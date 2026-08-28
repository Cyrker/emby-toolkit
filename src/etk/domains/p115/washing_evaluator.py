from __future__ import annotations

import ast
import json
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from etk.policy_defaults import DEFAULT_LANGUAGE_MAPPING

from .local_config_service import coerce_release_group_mapping
from .washing_priority import effective_washing_priority_config, washing_json_list


@dataclass(frozen=True, slots=True)
class WashingVersionSource:
    cache_id: str
    sha1: str
    pick_code: str
    file_name: str
    file_size: int
    local_path: str
    cache_snapshot: Mapping[str, Any]
    organize_target_cid: str
    mediainfo: object


@dataclass(frozen=True, slots=True)
class WashingMediaItem:
    tmdb_id: str
    item_type: str
    parent_series_tmdb_id: str
    season_number: int | None
    episode_number: int | None
    title: str
    original_language: str
    runtime_minutes: float
    average_episode_runtime_minutes: float
    parent_genres: object
    asset_details: object
    washing_snapshot: Mapping[str, Any]
    versions: tuple[WashingVersionSource, ...]
    source_fingerprint: str


@dataclass(frozen=True, slots=True)
class WashingSortingTarget:
    cid: str
    category_path: str


@dataclass(frozen=True, slots=True)
class WashingPolicySnapshot:
    config: Mapping[str, Any]
    groups: tuple[Mapping[str, Any], ...]
    language_mapping: tuple[Mapping[str, Any], ...]
    release_group_mapping: Mapping[str, tuple[str, ...]]
    sorting_targets: tuple[WashingSortingTarget, ...]
    local_root: str


@dataclass(frozen=True, slots=True)
class WashingCacheMutation:
    cache_id: str
    level: int | None
    snapshot: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class WashingOrganizeBackfill:
    cache_id: str
    pick_code: str
    original_name: str
    target_cid: str
    category_name: str


@dataclass(frozen=True, slots=True)
class WashingItemMutation:
    media_level: int | None
    media_snapshot: Mapping[str, Any]
    cache_mutations: tuple[WashingCacheMutation, ...]
    organize_backfills: tuple[WashingOrganizeBackfill, ...]
    stats: Mapping[str, int]


def build_washing_policy_snapshot(
    *,
    settings: Mapping[str, Any],
    groups: Sequence[Mapping[str, Any]],
    local_root: object,
    media_root_name: object,
) -> WashingPolicySnapshot:
    config, _migrated = effective_washing_priority_config(
        settings.get("p115_washing_priority_config"),
        settings.get("p115_rename_config"),
    )
    raw_language_mapping = settings.get("language_mapping")
    language_mapping = (
        raw_language_mapping
        if isinstance(raw_language_mapping, list) and raw_language_mapping
        else DEFAULT_LANGUAGE_MAPPING
    )
    release_mapping = coerce_release_group_mapping(settings.get("release_group_mapping"))
    targets = _sorting_targets(
        settings.get("p115_sorting_rules"),
        media_root_name=media_root_name,
    )
    return WashingPolicySnapshot(
        config=dict(config),
        groups=tuple(dict(group) for group in groups),
        language_mapping=tuple(dict(item) for item in language_mapping if isinstance(item, Mapping)),
        release_group_mapping={
            str(label): tuple(str(alias) for alias in aliases) for label, aliases in release_mapping.items()
        },
        sorting_targets=targets,
        local_root=str(local_root or ""),
    )


def washing_policy_needs_clean_version(policy: WashingPolicySnapshot) -> bool:
    if any(
        bool(rule.get("clean_version"))
        for group in policy.groups
        for rule in washing_json_list(group.get("priorities"))
        if isinstance(rule, Mapping)
    ):
        return True
    return any(
        bool(match.get("clean_version"))
        for slot in washing_json_list(policy.config.get("version_slots"))
        if isinstance(slot, Mapping)
        for match in [slot.get("match")]
        if isinstance(match, Mapping)
    )


def needs_remote_episode_runtime(
    item: WashingMediaItem,
    policy: WashingPolicySnapshot,
) -> bool:
    if (
        item.item_type != "Episode"
        or item.runtime_minutes > 0
        or not item.parent_series_tmdb_id
        or item.season_number is None
        or item.episode_number is None
    ):
        return False
    if not washing_policy_needs_clean_version(policy) or _has_animation_genre(item.parent_genres):
        return False
    for version in item.versions:
        if version.mediainfo in (None, "", [], {}):
            continue
        physical = _physical_runtime_minutes(version.mediainfo)
        if physical >= CLEAN_VERSION_SHORT_DRAMA_MINUTES:
            return True
    return False


def evaluate_washing_item(
    item: WashingMediaItem,
    policy: WashingPolicySnapshot,
    *,
    remote_episode_runtime_minutes: float | None = None,
    evaluated_at: datetime | None = None,
) -> WashingItemMutation:
    evaluated_at_text = _timestamp(evaluated_at)
    media_type = "movie" if item.item_type == "Movie" else "series"
    db_media_type = "Movie" if item.item_type == "Movie" else "Series"
    stats = {
        "version_count": len(item.versions),
        "evaluated_versions": 0,
        "missing_raw": 0,
        "missing_identity": 0,
        "no_priority_rules": 0,
        "backfilled_target_cid": 0,
    }
    if not item.versions:
        stats["missing_identity"] += 1
        return WashingItemMutation(
            media_level=None,
            media_snapshot={
                "reason": "缺少 SHA1/PC，无法重算洗版优先级",
                "media_type": media_type,
                "evaluated_at": evaluated_at_text,
            },
            cache_mutations=(),
            organize_backfills=(),
            stats=stats,
        )

    versions: list[dict[str, Any]] = []
    cache_mutations: list[WashingCacheMutation] = []
    organize_backfills: list[WashingOrganizeBackfill] = []
    row_snapshot = dict(item.washing_snapshot)
    slot_needs_clean = any(
        bool(match.get("clean_version"))
        for slot in washing_json_list(policy.config.get("version_slots"))
        if isinstance(slot, Mapping)
        for match in [slot.get("match")]
        if isinstance(match, Mapping)
    )

    for source in item.versions:
        cache_snapshot = dict(source.cache_snapshot)
        target_cid = str(
            _first_non_empty(
                source.organize_target_cid,
                cache_snapshot.get("target_cid"),
                row_snapshot.get("target_cid"),
                "",
            )
            or ""
        ).strip()
        inferred_target_cid, inferred_category_path = infer_sorting_target(
            source.local_path,
            policy,
        )
        identity = _washing_identity(item, source.file_name)
        level: int | None = None
        reason = "未计算"
        version_slot: dict[str, Any] | None = None

        if source.mediainfo in (None, "", [], {}):
            level = 0
            reason = "缺少媒体流信息，无法重算洗版优先级"
            stats["missing_raw"] += 1
        else:
            try:
                priorities = load_washing_priorities(policy.groups, db_media_type, target_cid)
                if (
                    not has_normal_washing_priorities(priorities)
                    and inferred_target_cid
                    and inferred_target_cid != target_cid
                ):
                    fallback = load_washing_priorities(
                        policy.groups,
                        db_media_type,
                        inferred_target_cid,
                    )
                    if has_normal_washing_priorities(fallback):
                        target_cid = inferred_target_cid
                        priorities = fallback

                priority_input = _build_priority_input(
                    source.mediainfo,
                    file_name=source.file_name,
                    file_size=source.file_size,
                    original_lang=item.original_language,
                    media_type=media_type,
                    tmdb_id=item.parent_series_tmdb_id or item.tmdb_id,
                    season_num=item.season_number,
                    episode_num=item.episode_number,
                    need_clean_version_check=(
                        any(bool(rule.get("clean_version")) for rule in priorities if isinstance(rule, Mapping))
                        or slot_needs_clean
                    ),
                )
                normalized = normalize_washing_info(
                    priority_input,
                    language_mapping=policy.language_mapping,
                    release_group_mapping=policy.release_group_mapping,
                    item=item,
                    remote_episode_runtime_minutes=remote_episode_runtime_minutes,
                )
                version_slot = resolve_washing_version_slot(
                    normalized,
                    policy.config,
                    language_mapping=policy.language_mapping,
                    release_group_mapping=policy.release_group_mapping,
                )
                if version_slot is None:
                    level = 0
                    reason = "未命中任何多版本槽位"
                    stats["evaluated_versions"] += 1
                elif not has_normal_washing_priorities(priorities):
                    level = None
                    if inferred_target_cid and target_cid != inferred_target_cid:
                        reason = (
                            f"原目标分类CID({target_cid or '-'})只命中全局/排除规则，"
                            f"local_path 推导分类CID({inferred_target_cid})后仍未命中普通优先级规则"
                        )
                    elif target_cid:
                        reason = f"目标分类CID({target_cid})未命中普通洗版优先级规则"
                    else:
                        reason = "缺少整理目标分类CID，且 local_path 未命中分类规则"
                    stats["no_priority_rules"] += 1
                else:
                    level, reason = washing_level(
                        normalized,
                        priorities,
                        language_mapping=policy.language_mapping,
                        release_group_mapping=policy.release_group_mapping,
                    )
                    if inferred_target_cid and target_cid == inferred_target_cid and inferred_category_path:
                        reason = f"{reason}（分类路径: {inferred_category_path}）"
                    stats["evaluated_versions"] += 1
            except Exception as exc:
                level = 0
                reason = f"重算异常: {exc}"

        if (
            inferred_target_cid
            and target_cid == inferred_target_cid
            and source.organize_target_cid != inferred_target_cid
            and (source.cache_id or source.pick_code)
        ):
            organize_backfills.append(
                WashingOrganizeBackfill(
                    cache_id=source.cache_id,
                    pick_code=source.pick_code,
                    original_name=source.file_name or item.title or source.cache_id or source.pick_code,
                    target_cid=inferred_target_cid,
                    category_name=inferred_category_path,
                )
            )
            stats["backfilled_target_cid"] += 1

        version = {
            "fid": source.cache_id,
            "sha1": source.sha1,
            "size": source.file_size,
            "level": level,
            "reason": reason,
            "identity": identity,
            "file_name": source.file_name,
            "pick_code": source.pick_code,
            "media_type": media_type,
            "target_cid": target_cid,
            "version_slot": version_slot,
            "evaluated_at": evaluated_at_text,
        }
        versions.append(version)
        if source.cache_id:
            cache_mutations.append(
                WashingCacheMutation(
                    cache_id=source.cache_id,
                    level=level,
                    snapshot={
                        "reason": reason,
                        "target_cid": target_cid or None,
                        "media_type": media_type,
                        "version_slot": version_slot,
                        "identity": identity,
                        "evaluated_at": evaluated_at_text,
                    },
                )
            )

    best = sorted(versions, key=_best_sort_key)[0] if versions else {}
    slot_levels: dict[str, dict[str, Any]] = {}
    for version in versions:
        raw_slot = version.get("version_slot")
        slot: Mapping[str, Any] = raw_slot if isinstance(raw_slot, Mapping) else {}
        slot_id = str(slot.get("id") or "__single__")
        current = slot_levels.get(slot_id)
        if current and _best_sort_key(current) <= _best_sort_key(version):
            continue
        slot_levels[slot_id] = {
            "id": slot_id,
            "name": slot.get("name") or ("主版本" if slot_id == "__single__" else slot_id),
            "suffix": slot.get("suffix") or "",
            "level": version.get("level"),
            "reason": version.get("reason"),
            "sha1": version.get("sha1"),
        }

    media_snapshot = {
        "versions": versions,
        "slot_levels": slot_levels,
        "reason": best.get("reason") if best else "无有效版本",
        "sha1": best.get("sha1") if best else None,
        "target_cid": best.get("target_cid") if best else None,
        "media_type": media_type,
        "version_slot": best.get("version_slot") if best else {},
        "evaluated_at": evaluated_at_text,
    }
    return WashingItemMutation(
        media_level=best.get("level") if best else None,
        media_snapshot=media_snapshot,
        cache_mutations=tuple(cache_mutations),
        organize_backfills=tuple(organize_backfills),
        stats=stats,
    )


def normalize_washing_info(
    info: object,
    *,
    language_mapping: Sequence[Mapping[str, Any]],
    release_group_mapping: Mapping[str, Sequence[str]],
    item: WashingMediaItem,
    remote_episode_runtime_minutes: float | None,
) -> dict[str, Any]:
    normalized: dict[str, Any] = {
        "resolution": "unknown",
        "source": "unknown",
        "codec": "unknown",
        "effect": "sdr",
        "release_group": "",
        "original_lang": "",
        "audio_langs": set(),
        "sub_langs": set(),
        "size_gb": 0.0,
        "has_effect_subtitle": False,
        "is_clean_version": None,
        "clean_version_detail": {},
    }
    if not info:
        return normalized
    parsed = safe_parse_jsonish(info)
    if parsed is None:
        parsed = info
    media_source = _extract_media_source_info(parsed)
    streams = _extract_media_streams(parsed)
    video = _extract_video_stream(parsed)
    raw_etk = parsed.get("_etk") if isinstance(parsed, Mapping) else None
    etk: Mapping[str, Any] = raw_etk if isinstance(raw_etk, Mapping) else {}

    width = _first_non_empty(
        video.get("Width"),
        media_source.get("Width"),
        parsed.get("width") if isinstance(parsed, Mapping) else 0,
        parsed.get("Width") if isinstance(parsed, Mapping) else 0,
        0,
    )
    height = _first_non_empty(
        video.get("Height"),
        media_source.get("Height"),
        parsed.get("height") if isinstance(parsed, Mapping) else 0,
        parsed.get("Height") if isinstance(parsed, Mapping) else 0,
        0,
    )
    width_int, height_int = _integer(width), _integer(height)
    if width_int > 0 or height_int > 0:
        normalized["resolution"] = _resolution_tier(width_int, height_int)

    raw_codec = str(
        _first_non_empty(
            video.get("Codec"),
            media_source.get("Codec"),
            parsed.get("codec") if isinstance(parsed, Mapping) else "",
            parsed.get("video_codec") if isinstance(parsed, Mapping) else "",
            parsed.get("codec_name") if isinstance(parsed, Mapping) else "",
            "",
        )
        or ""
    ).lower()
    if raw_codec in {"hevc", "h265", "x265"}:
        normalized["codec"] = "hevc"
    elif raw_codec in {"h264", "avc", "x264"}:
        normalized["codec"] = "h264"
    else:
        normalized["codec"] = raw_codec or "unknown"

    raw_filename = str(
        _first_non_empty(
            parsed.get("path") if isinstance(parsed, Mapping) else "",
            parsed.get("Path") if isinstance(parsed, Mapping) else "",
            parsed.get("filename") if isinstance(parsed, Mapping) else "",
            parsed.get("name") if isinstance(parsed, Mapping) else "",
            media_source.get("Path"),
            "",
        )
        or ""
    )
    raw_source = ""
    if isinstance(parsed, Mapping):
        raw_source = str(
            _first_non_empty(
                etk.get("quality_source"),
                parsed.get("quality_source"),
                parsed.get("source"),
                parsed.get("quality"),
                parsed.get("quality_display"),
                parsed.get("resourceType"),
                parsed.get("resource_type"),
                "",
            )
            or ""
        )
    normalized["source"] = normalize_quality_source(raw_source) if raw_source else extract_quality_source(raw_filename)
    if not normalized["source"]:
        normalized["source"] = "unknown"
    normalized["effect"] = standardized_effect(raw_filename.lower(), video or media_source)

    release_group = extract_release_group(raw_filename, release_group_mapping)
    if not release_group and isinstance(parsed, Mapping):
        for key in ("release_group", "release_group_alias", "group", "release_group_raw"):
            value = parsed.get(key)
            values = value if isinstance(value, list) else [value]
            for candidate in values:
                release_group = normalize_release_group(candidate, release_group_mapping)
                if release_group:
                    break
            if release_group:
                break
    normalized["release_group"] = release_group

    audio_languages: set[str] = set()
    subtitle_languages: set[str] = set()
    if streams:
        audio_languages.update(_detected_languages(streams, "Audio", language_mapping=language_mapping))
        subtitle_languages.update(_detected_languages(streams, "Subtitle", language_mapping=language_mapping))
    if isinstance(parsed, Mapping):
        for track in safe_parse_list(parsed.get("audio_tracks")):
            if isinstance(track, Mapping):
                language = track.get("language") or track.get("Language")
                if language:
                    audio_languages.add(normalize_language(language, language_mapping))
        for track in safe_parse_list(parsed.get("subtitles")):
            if isinstance(track, Mapping):
                language = track.get("language") or track.get("Language")
                if language:
                    subtitle_languages.add(normalize_language(language, language_mapping))
        for key in ("audio_langs", "audio_languages_raw"):
            for language in safe_parse_list(parsed.get(key)):
                if language:
                    audio_languages.add(normalize_language(language, language_mapping))
        for key in ("sub_langs", "subtitle_languages_raw"):
            for language in safe_parse_list(parsed.get(key)):
                if language:
                    subtitle_languages.add(normalize_language(language, language_mapping))
    normalized["audio_langs"] = {normalize_language(value, language_mapping) for value in audio_languages if value}
    normalized["sub_langs"] = {normalize_language(value, language_mapping) for value in subtitle_languages if value}
    normalized["has_effect_subtitle"] = _has_effect_subtitle(parsed, streams)

    size_bytes = _first_non_empty(
        media_source.get("Size"),
        parsed.get("size_bytes") if isinstance(parsed, Mapping) else 0,
        parsed.get("_file_size") if isinstance(parsed, Mapping) else 0,
        parsed.get("Size") if isinstance(parsed, Mapping) else 0,
        0,
    )
    normalized["size_gb"] = _integer(size_bytes) / (1024**3)
    raw_original = ""
    if isinstance(parsed, Mapping):
        raw_original = str(
            _first_non_empty(
                parsed.get("_original_lang"),
                parsed.get("original_lang"),
                parsed.get("lang_code"),
                "",
            )
            or ""
        )
    normalized["original_lang"] = normalize_language(raw_original, language_mapping)
    normalized["has_external_subtitle"] = (
        parsed.get("has_external_subtitle", False) if isinstance(parsed, Mapping) else False
    )
    if isinstance(parsed, Mapping) and parsed.get("_need_clean_version_check"):
        detail = clean_version_result(
            item,
            parsed,
            remote_episode_runtime_minutes=remote_episode_runtime_minutes,
        )
        normalized["is_clean_version"] = detail.get("is_clean") if detail.get("checked") else None
        normalized["clean_version_detail"] = detail
    return normalized


def clean_version_result(
    item: WashingMediaItem,
    info: object,
    *,
    remote_episode_runtime_minutes: float | None,
) -> dict[str, Any]:
    if item.item_type != "Episode":
        return {"checked": False, "is_clean": False, "reason": "non_series"}
    if not item.parent_series_tmdb_id or item.season_number is None or item.episode_number is None:
        return {"checked": False, "is_clean": False, "reason": "missing_identity"}
    if _has_animation_genre(item.parent_genres):
        return {"checked": False, "is_clean": False, "reason": "animation_genre_skipped"}
    physical = _physical_runtime_minutes(info)
    if physical <= 0:
        return {"checked": False, "is_clean": False, "reason": "missing_physical_runtime"}
    if physical < CLEAN_VERSION_SHORT_DRAMA_MINUTES:
        return {"checked": False, "is_clean": False, "reason": "short_drama_skipped"}

    official = item.runtime_minutes
    source = "local_media_metadata"
    if official <= 0 and remote_episode_runtime_minutes is not None and remote_episode_runtime_minutes > 0:
        official = float(remote_episode_runtime_minutes)
        source = "tmdb_realtime"
    if official <= 0 and item.average_episode_runtime_minutes > 0:
        official = item.average_episode_runtime_minutes
        source = "local_media_metadata_average"
    if official <= 0:
        return {
            "checked": False,
            "is_clean": False,
            "reason": "missing_official_runtime",
            "physical_runtime_minutes": round(physical, 2),
        }

    delta = official - physical
    ratio = physical / official if official > 0 else 1.0
    is_clean = delta >= CLEAN_VERSION_MIN_DELTA_MINUTES and ratio <= CLEAN_VERSION_MAX_RUNTIME_RATIO
    return {
        "checked": True,
        "is_clean": bool(is_clean),
        "reason": "runtime_shorter" if is_clean else "runtime_not_short_enough",
        "runtime_source": source,
        "official_runtime_minutes": round(official, 2),
        "physical_runtime_minutes": round(physical, 2),
        "delta_minutes": round(delta, 2),
        "runtime_ratio": round(ratio, 4),
        "min_delta_minutes": CLEAN_VERSION_MIN_DELTA_MINUTES,
        "max_runtime_ratio": CLEAN_VERSION_MAX_RUNTIME_RATIO,
        "is_short_drama": False,
    }


def washing_level(
    normalized: Mapping[str, Any],
    priorities: Sequence[Mapping[str, Any]],
    *,
    language_mapping: Sequence[Mapping[str, Any]] = (),
    release_group_mapping: Mapping[str, Sequence[str]] | None = None,
) -> tuple[int, str]:
    release_mapping = release_group_mapping or {}
    failures: list[str] = []
    normal_index = 0
    for rule in priorities:
        excluded = bool(rule.get("is_exclude", False))
        group_name = rule.get("_group_name", "未知规则组")
        if not excluded:
            normal_index += 1
        matched, reason = match_washing_priority(
            normalized,
            rule,
            language_mapping=language_mapping,
            release_group_mapping=release_mapping,
        )
        if matched:
            if excluded:
                return -1, f"规则组[{group_name}] {reason}"
            return normal_index, f"规则组[{group_name}] -> 优先级 {normal_index}"
        if not excluded:
            failures.append(f"规则组[{group_name}]-优先级{normal_index}[{reason}]")
    if not failures:
        return 0, "未配置任何有效的普通优先级规则"
    return 0, "\n".join(failures)


def match_washing_priority(
    normalized: Mapping[str, Any],
    rule: Mapping[str, Any],
    *,
    language_mapping: Sequence[Mapping[str, Any]] = (),
    release_group_mapping: Mapping[str, Sequence[str]] | None = None,
) -> tuple[bool, str]:
    release_mapping = release_group_mapping or {}
    excluded = bool(rule.get("is_exclude", False))
    exempt_original = bool(rule.get("exempt_original_lang", False))

    required_resolution = _rule_iterable(rule.get("resolution"))
    if required_resolution:
        values = [str(value).lower().strip() for value in required_resolution]
        current = str(normalized.get("resolution") or "")
        matched = any(value == current or (value in {"4k", "2160p"} and current in {"4k", "2160p"}) for value in values)
        if excluded and matched:
            return True, f"命中排除条件: 分辨率 ({current})"
        if not excluded and not matched:
            return False, f"分辨率未命中 ({current})"

    required_codec = _rule_iterable(rule.get("codec"))
    if required_codec:
        values = [str(value).lower().strip() for value in required_codec]
        current = str(normalized.get("codec") or "")
        matched = any(
            value in current
            or current in value
            or (value in {"hevc", "h265"} and ("hevc" in current or "h265" in current))
            or (value in {"avc", "h264"} and ("avc" in current or "h264" in current))
            for value in values
        )
        if excluded and matched:
            return True, f"命中排除条件: 编码 ({current})"
        if not excluded and not matched:
            return False, f"编码未命中 ({current})"

    required_effect = _rule_iterable(rule.get("effect"))
    if required_effect:
        values = [str(value).lower().strip() for value in required_effect]
        current = str(normalized.get("effect") or "")
        matched = current in values
        if excluded and matched:
            return True, f"命中排除条件: 特效 ({current})"
        if not excluded and not matched:
            return False, f"特效未命中 ({current})"

    required_source = rule.get("source") or rule.get("quality") or []
    if required_source:
        values = required_source if isinstance(required_source, list) else [required_source]
        required_tokens = {token for value in values for token in quality_source_tokens(value) if token}
        current_tokens = quality_source_tokens(normalized.get("source"))
        matched = bool(current_tokens and required_tokens and current_tokens & required_tokens)
        if excluded and matched:
            return True, f"命中排除条件: 来源 ({normalized.get('source')})"
        if not excluded and not matched:
            return False, f"来源未命中 ({'/'.join(str(value) for value in values)})"

    required_groups = rule.get("release_group") or rule.get("release_groups") or []
    if required_groups:
        values = required_groups if isinstance(required_groups, list) else [required_groups]
        required = {normalize_release_group(value, release_mapping) for value in values if str(value or "").strip()}
        required.discard("")
        current = normalize_release_group(
            normalized.get("release_group"),
            release_mapping,
        )
        matched = bool(current and current in required)
        if excluded and matched:
            return True, f"命中排除条件: 发布组 ({current})"
        if not excluded and not matched:
            return False, f"发布组未命中 ({current or '未识别'})"

    original_language = str(normalized.get("original_lang") or "")
    required_audio = _rule_iterable(rule.get("audio"))
    if required_audio:
        required = {normalize_language(value, language_mapping) for value in required_audio if value}
        if exempt_original:
            required = {value for value in required if value != original_language}
        if required:
            available = {str(value) for value in normalized.get("audio_langs", set())}
            matches = [value for value in available if value in required]
            if not available and not excluded:
                return False, "未提取到音轨语言"
            if excluded and matches:
                return True, f"命中排除条件: 音轨 ({', '.join(matches)})"
            if not excluded and not matches:
                return False, "缺少必须的音轨"

    required_subtitle = _rule_iterable(rule.get("subtitle"))
    if required_subtitle:
        required = {normalize_language(value, language_mapping) for value in required_subtitle if value}
        if exempt_original:
            required = {value for value in required if value != original_language}
        if required:
            available = {str(value) for value in normalized.get("sub_langs", set())}
            matches = [value for value in available if value in required]
            if excluded and matches:
                return True, f"命中排除条件: 字幕 ({', '.join(matches)})"
            if not excluded and not matches and not normalized.get("has_external_subtitle", False):
                return False, "缺少必须的字幕"

    if rule.get("subtitle_effect") or rule.get("effect_subtitle"):
        has_effect = bool(normalized.get("has_effect_subtitle"))
        if excluded and has_effect:
            return True, "命中排除条件: 特效字幕"
        if not excluded and not has_effect:
            return False, "缺少特效字幕"

    minimum = rule.get("min_size_gb")
    maximum = rule.get("max_size_gb")
    size = float(normalized.get("size_gb") or 0)
    if excluded:
        if minimum and size > 0 and size <= float(minimum):
            return True, f"命中排除条件: 体积过小 ({size:.1f}GB <= {minimum}GB)"
        if maximum and size > 0 and size >= float(maximum):
            return True, f"命中排除条件: 体积过大 ({size:.1f}GB >= {maximum}GB)"
    else:
        if minimum and size > 0 and size < float(minimum):
            return False, "体积过小"
        if maximum and size > 0 and size > float(maximum):
            return False, "体积过大"

    if rule.get("clean_version"):
        clean = normalized.get("is_clean_version")
        detail = normalized.get("clean_version_detail")
        detail = detail if isinstance(detail, Mapping) else {}
        if clean is None and not excluded:
            return False, f"无法判断纯净版 ({detail.get('reason') or 'unknown'})"
        if clean and excluded:
            return True, "命中排除条件: 纯净版"
        if clean is False and not excluded:
            return False, "未命中纯净版"

    if excluded:
        return False, "未命中任何排除条件"
    return True, "匹配成功"


def resolve_washing_version_slot(
    normalized: Mapping[str, Any],
    config: Mapping[str, Any],
    *,
    language_mapping: Sequence[Mapping[str, Any]] = (),
    release_group_mapping: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any] | None:
    release_mapping = release_group_mapping or {}
    slots = washing_json_list(config.get("version_slots"))
    if not config.get("version_slots_enabled") or not slots:
        return {"enabled": False, "id": "__single__", "name": "主版本", "suffix": ""}
    fallback: dict[str, Any] | None = None
    for slot in slots:
        if not isinstance(slot, Mapping):
            continue
        slot_info = {
            "enabled": True,
            "id": str(slot.get("id") or "").strip(),
            "name": str(slot.get("name") or "未命名槽位").strip(),
            "suffix": str(slot.get("suffix") or slot.get("name") or "").strip(),
        }
        if slot.get("fallback"):
            if fallback is None:
                fallback = slot_info
            continue
        raw_match = slot.get("match")
        match: Mapping[str, Any] = raw_match if isinstance(raw_match, Mapping) else {}
        if not _slot_has_conditions(match):
            continue
        matched, _reason = match_washing_priority(
            normalized,
            dict(match),
            language_mapping=language_mapping,
            release_group_mapping=release_mapping,
        )
        if matched:
            return slot_info
    return fallback


def load_washing_priorities(
    groups: Sequence[Mapping[str, Any]],
    media_type: str,
    target_cid: str,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for group in groups:
        if str(group.get("media_type") or "") not in {media_type, "All"}:
            continue
        target_cids = washing_json_list(group.get("target_cids"))
        if target_cids and str(target_cid) not in target_cids:
            continue
        group_name = group.get("name", "未知规则组")
        for raw_rule in washing_json_list(group.get("priorities")):
            if not isinstance(raw_rule, Mapping):
                continue
            rule = dict(raw_rule)
            rule["_group_name"] = group_name
            result.append(rule)
    return result


def has_normal_washing_priorities(priorities: object) -> bool:
    return isinstance(priorities, list) and any(
        isinstance(rule, Mapping) and not bool(rule.get("is_exclude", False)) for rule in priorities
    )


def infer_sorting_target(
    local_path: object,
    policy: WashingPolicySnapshot,
) -> tuple[str, str]:
    path = _normalize_rule_path(local_path)
    if not path:
        return "", ""
    root = _normalize_rule_path(policy.local_root)
    if root:
        path_cf = path.casefold()
        root_cf = root.casefold()
        if path_cf == root_cf:
            path = ""
        elif path_cf.startswith(root_cf + "/"):
            path = path[len(root) :].strip("/")
    if not path:
        return "", ""
    path_cf = path.casefold()
    for target in policy.sorting_targets:
        category_cf = target.category_path.casefold()
        if path_cf == category_cf or path_cf.startswith(category_cf + "/"):
            return target.cid, target.category_path
    return "", ""


def safe_parse_jsonish(value: object) -> object | None:
    if value is None or isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except Exception:
            try:
                return ast.literal_eval(text)
            except Exception:
                return None
    return None


def safe_parse_list(value: object) -> list[Any]:
    parsed = safe_parse_jsonish(value)
    return parsed if isinstance(parsed, list) else []


def normalize_quality_source(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    lower = text.lower().replace("_", "-").replace(" ", "-")
    aliases = {
        "bluray原盘": "BluRay原盘",
        "blu-ray原盘": "BluRay原盘",
        "blu-ray-原盘": "BluRay原盘",
        "bluray-原盘": "BluRay原盘",
        "bdmv": "BluRay原盘",
        "bdiso": "BluRay原盘",
        "iso": "BluRay原盘",
        "remux": "Remux",
        "bdremux": "Remux",
        "blu-ray-remux": "Remux",
        "bluray-remux": "Remux",
        "uhd-bluray": "UHD BluRay",
        "uhd-blu-ray": "UHD BluRay",
        "ultra-hd-bluray": "UHD BluRay",
        "ultra-hd-blu-ray": "UHD BluRay",
        "bluray": "BluRay",
        "blu-ray": "BluRay",
        "bdrip": "BluRay",
        "brrip": "BluRay",
        "bd": "BluRay",
        "uhd": "UHD",
        "ultra-hd": "UHD",
        "web-dl": "WEB-DL",
        "webdl": "WEB-DL",
        "web": "WEB-DL",
        "web-dlrip": "WEB-DL",
        "webrip": "WEBRip",
        "web-rip": "WEBRip",
        "hdtv": "HDTV",
        "dvd": "DVDRip",
        "dvdrip": "DVDRip",
        "dvd-rip": "DVDRip",
    }
    return aliases.get(lower, text)


def quality_source_tokens(value: object) -> set[str]:
    normalized = normalize_quality_source(value)
    if not normalized:
        return set()
    lower = normalized.lower()
    result = {lower}
    if lower == "uhd bluray":
        result.update({"uhd", "bluray"})
    if lower == "bluray原盘":
        result.add("原盘")
    if lower == "remux":
        result.add("remux")
    return result


def extract_quality_source(file_name: object) -> str:
    name = str(file_name or "")
    if not name.strip():
        return "未知"
    upper = name.upper()
    lower = name.lower()
    extension = os.path.splitext(name)[1].lower()
    if extension == ".iso":
        return "BluRay原盘"
    if re.search(r"(?<![A-Z0-9])REMUX(?![A-Z0-9])", upper):
        return "Remux"
    has_uhd = bool(re.search(r"(?<![A-Z0-9])UHD(?![A-Z0-9])", upper))
    has_bluray = bool(re.search(r"(?<![A-Z0-9])BLU[-_. ]?RAY(?![A-Z0-9])", upper))
    if has_uhd and has_bluray:
        return "UHD BluRay"
    if has_uhd:
        return "UHD"
    if has_bluray or re.search(r"(?<![A-Z0-9])(?:BDRIP|BRRIP)(?![A-Z0-9])", upper):
        return "BluRay"
    if re.search(r"(?<![A-Z0-9])WEB[-_. ]?DL(?![A-Z0-9])", upper) or "webdl" in lower:
        return "WEB-DL"
    if re.search(r"(?<![A-Z0-9])WEB[-_. ]?RIP(?![A-Z0-9])", upper) or "webrip" in lower:
        return "WEBRip"
    if re.search(r"(?<![A-Z0-9])HDTV(?![A-Z0-9])", upper):
        return "HDTV"
    if re.search(r"(?<![A-Z0-9])DVD[-_. ]?RIP(?![A-Z0-9])", upper):
        return "DVDRip"
    return "未知"


def standardized_effect(path_lower: object, video_stream: Mapping[str, Any]) -> str:
    path = str(path_lower or "").lower()
    if video_stream:
        subtype = str(video_stream.get("ExtendedVideoSubType") or "").lower()
        subtype_description = str(video_stream.get("ExtendedVideoSubTypeDescription") or "").lower()
        extended_type = str(video_stream.get("ExtendedVideoType") or "").lower()
        video_range = str(video_stream.get("VideoRange") or "").lower()
        display = str(video_stream.get("DisplayTitle") or "").lower()
        profile = str(video_stream.get("Profile") or "").lower()
        codec = str(video_stream.get("Codec") or "").lower()
        transfer = str(video_stream.get("ColorTransfer") or "").lower()
        if subtype in {"doviprofile81", "doviprofile8", "dvhe.08", "dvh1.08"}:
            return "dovi_p8"
        if subtype in {"doviprofile76", "doviprofile7", "dvhe.07", "dvh1.07"}:
            return "dovi_p7"
        if subtype in {"doviprofile5", "dvhe.05", "dvh1.05"}:
            return "dovi_p5"
        combined = " ".join(
            (subtype, subtype_description, extended_type, video_range, display, profile, codec, transfer)
        )
        if "profile 8.1" in combined or "hdr10 compatible" in combined:
            return "dovi_p8"
        if "profile 7" in combined:
            return "dovi_p7"
        if "profile 5" in combined:
            return "dovi_p5"
        has_dv = any(value in combined for value in ("dovi", "dolbyvision", "dolby vision"))
        has_hdr10_plus = any(value in combined for value in ("hdr10+", "hdr10plus"))
        has_hdr = has_hdr10_plus or any(value in combined for value in ("hdr10", "hdr", "smpte2084"))
        if has_dv and has_hdr:
            return "dovi_p8"
        if has_dv:
            return "dovi_other"
        if has_hdr10_plus:
            return "hdr10+"
        if has_hdr:
            return "hdr"
    if ("dovi" in path or "dolbyvision" in path or "dv" in path) and "hdr" in path:
        return "dovi_p8"
    if any(value in path for value in ("dovi p7", "dovi.p7", "dv.p7", "profile 7", "profile7")):
        return "dovi_p7"
    if any(value in path for value in ("dovi p5", "dovi.p5", "dv.p5", "profile 5", "profile5")):
        return "dovi_p5"
    if "dovi" in path or "dolbyvision" in path:
        return "dovi_other"
    if "hdr10+" in path or "hdr10plus" in path:
        return "hdr10+"
    if "hdr" in path:
        return "hdr"
    return "sdr"


def normalize_language(value: object, mapping: Sequence[Mapping[str, Any]]) -> str:
    text = str(value or "").lower().strip()
    if not text:
        return ""
    if text in {
        "guo",
        "guoyu",
        "chs",
        "zh-cn",
        "zh-sg",
        "zh-hans",
        "cmn",
        "mandarin",
        "taiwan",
        "台湾",
        "臺灣",
        "国语",
        "普通话",
        "中文",
        "简体",
        "简中",
    }:
        return "chi"
    if text in {
        "cht",
        "zh-hk",
        "zh-tw",
        "hk",
        "tw",
        "cantonese",
        "粤语",
        "繁体",
        "繁中",
        "粤配",
        "粤英双语",
        "港配",
        "粤语配音",
        "广东话",
    }:
        return "yue"
    for item in mapping:
        label = str(item.get("label") or "").lower().strip()
        code = str(item.get("value") or "").lower().strip()
        aliases = [str(alias).lower().strip() for alias in _language_aliases(item.get("aliases"))]
        if text == code or text == label or text in aliases:
            three_letter = next((alias for alias in aliases if len(alias) == 3 and alias.isalpha()), "")
            return three_letter or code
    return text


def normalize_release_group(
    value: object,
    mapping: Mapping[str, Sequence[str]],
) -> str:
    text = str(value or "").strip(" ._-")
    if not text:
        return ""
    text = re.sub(
        r"\.(?:mkv|mp4|ts|avi|mov|wmv|strm|nfo|json)$",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip(" ._-")
    for label, aliases in mapping.items():
        if text.lower() == str(label).lower():
            return str(label)
        for alias in aliases:
            try:
                if re.fullmatch(str(alias), text, re.IGNORECASE):
                    return str(label)
            except re.error:
                continue
    return text


def extract_release_group(file_name: object, mapping: Mapping[str, Sequence[str]]) -> str:
    text = str(file_name or "").strip()
    if not text:
        return ""
    stem = os.path.splitext(text)[0].strip(" ._-")
    token = stem.rsplit("@", 1)[-1] if "@" in stem else re.split(r"\s*[·-]\s*", stem)[-1]
    token = str(token or "").strip(" ._-")
    if not re.search(r"[A-Za-z]", token):
        return ""
    normalized = normalize_release_group(token, mapping)
    return normalized if normalized and normalized != token else ""


CLEAN_VERSION_MIN_DELTA_MINUTES = 1.5
CLEAN_VERSION_MAX_RUNTIME_RATIO = 0.985
CLEAN_VERSION_SHORT_DRAMA_MINUTES = 25.0


def _build_priority_input(
    raw: object,
    *,
    file_name: str,
    file_size: int,
    original_lang: str,
    media_type: str,
    tmdb_id: str,
    season_num: int | None,
    episode_num: int | None,
    need_clean_version_check: bool,
) -> dict[str, Any]:
    parsed = safe_parse_jsonish(raw)
    if parsed is None:
        parsed = raw
    if isinstance(parsed, list):
        info = dict(parsed[0]) if parsed and isinstance(parsed[0], Mapping) else {}
    elif isinstance(parsed, Mapping):
        info = dict(parsed)
    else:
        info = {}
    if file_name:
        info["filename"] = file_name
    if file_size:
        info["_file_size"] = int(file_size)
    if original_lang:
        info["_original_lang"] = original_lang
    if media_type:
        info["_media_type"] = media_type
    if tmdb_id:
        info["_tmdb_id"] = tmdb_id
    if season_num is not None:
        info["_season_num"] = season_num
    if episode_num is not None:
        info["_episode_num"] = episode_num
    if need_clean_version_check:
        info["_need_clean_version_check"] = True
    return info


def _extract_media_source_info(info: object) -> dict[str, Any]:
    parsed = safe_parse_jsonish(info)
    if isinstance(parsed, list):
        if not parsed or not isinstance(parsed[0], Mapping):
            return {}
        first = parsed[0]
        source = first.get("MediaSourceInfo")
        return dict(source) if isinstance(source, Mapping) else dict(first)
    if isinstance(parsed, Mapping):
        source = parsed.get("MediaSourceInfo")
        return dict(source) if isinstance(source, Mapping) else dict(parsed)
    return {}


def _extract_media_streams(info: object) -> list[dict[str, Any]]:
    source = _extract_media_source_info(info)
    streams = source.get("MediaStreams", [])
    return [dict(stream) for stream in streams if isinstance(stream, Mapping)] if isinstance(streams, list) else []


def _extract_video_stream(info: object) -> dict[str, Any]:
    source = _extract_media_source_info(info)
    for stream in _extract_media_streams(info):
        if str(stream.get("Type") or "").lower() == "video":
            return stream
    return source if str(source.get("Type") or "").lower() == "video" else {}


def _detected_languages(
    streams: Sequence[Mapping[str, Any]],
    stream_type: str,
    *,
    language_mapping: Sequence[Mapping[str, Any]],
) -> set[str]:
    keyword_map = _language_keyword_map(language_mapping)
    detected: set[str] = set()
    for stream in streams:
        if stream.get("Type") != stream_type:
            continue
        stream_languages: set[str] = set()
        title = f"{stream.get('Title') or ''} {stream.get('DisplayTitle') or ''}".lower().strip()
        if title:
            for key, keywords in keyword_map.items():
                normalized_key = key.replace("sub_", "")
                for keyword in keywords:
                    candidate = keyword.lower()
                    if not candidate:
                        continue
                    if _contains_chinese(candidate):
                        matched = candidate in title
                    else:
                        matched = bool(
                            re.search(
                                rf"(?<![a-z0-9]){re.escape(candidate)}(?![a-z0-9])",
                                title,
                            )
                        )
                    if matched:
                        stream_languages.add(normalized_key)
                        break
        raw_language = str(stream.get("Language") or "").lower().strip()
        if raw_language:
            normalized = normalize_language(raw_language, language_mapping)
            if normalized:
                stream_languages.add(normalized)
        if "yue" in stream_languages and "chi" in stream_languages:
            if not any(value in title for value in ("guo", "国", "mandarin")):
                stream_languages.remove("chi")
        detected.update(stream_languages)
    return detected


def _language_keyword_map(mapping: Sequence[Mapping[str, Any]]) -> dict[str, list[str]]:
    aliases_by_label = {
        str(item.get("label") or ""): _language_aliases(item.get("aliases"))
        for item in mapping
        if isinstance(item, Mapping)
    }
    result = {
        "chi": aliases_by_label.get("国语", []) + ["国配", "公映", "台配", "京译", "上译", "央译"],
        "yue": aliases_by_label.get("粤语", []) + ["粤配", "港配", "粤英双语"],
        "eng": aliases_by_label.get("英语", []),
        "jpn": aliases_by_label.get("日语", []),
        "kor": aliases_by_label.get("韩语", []),
        "sub_chi": aliases_by_label.get("国语", [])
        + ["CHS", "SC", "GB", "ZHS", "ZH-CN", "ZH-HANS", "简体", "简中", "Simplified"],
        "sub_yue": aliases_by_label.get("粤语", [])
        + ["CHT", "TC", "BIG5", "ZHT", "ZH-TW", "ZH-HK", "ZH-HANT", "繁体", "繁中", "Traditional"],
        "sub_eng": aliases_by_label.get("英语", []) + ["英字"],
        "sub_jpn": aliases_by_label.get("日语", []) + ["日字"],
        "sub_kor": aliases_by_label.get("韩语", []) + ["韩字"],
    }
    for item in mapping:
        label = str(item.get("label") or "")
        if label in {"国语", "粤语", "英语", "日语", "韩语", "无语言"}:
            continue
        aliases = _language_aliases(item.get("aliases"))
        code = next(
            (value.lower() for value in aliases if len(value) == 3 and value.isalpha()),
            str(item.get("value") or "").lower().strip(),
        )
        if code and aliases:
            result[code] = aliases
            result[f"sub_{code}"] = aliases
    return result


def _has_effect_subtitle(parsed: object, streams: Sequence[Mapping[str, Any]]) -> bool:
    for stream in streams:
        if str(stream.get("Type") or "").lower() != "subtitle":
            continue
        if any(_effect_subtitle_text(stream.get(key)) for key in ("Title", "DisplayTitle", "title", "display_title")):
            return True
    if not isinstance(parsed, Mapping):
        return False
    for key in ("subtitles", "subtitle_tracks", "subtitle_list", "subtitle"):
        value = parsed.get(key)
        if _effect_subtitle_text(value):
            return True
        for item in safe_parse_list(value):
            if isinstance(item, Mapping):
                if any(
                    _effect_subtitle_text(item.get(field))
                    for field in ("Title", "DisplayTitle", "title", "display_title", "display", "name")
                ):
                    return True
            elif _effect_subtitle_text(item):
                return True
    return False


def _effect_subtitle_text(value: object) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    if "特效" in text:
        return True
    normalized = text
    for value in ("_", "-", ".", "/", "(", ")"):
        normalized = normalized.replace(value, " ")
    return bool({"effect", "effects", "tx"} & set(normalized.split()))


def _physical_runtime_minutes(info: object) -> float:
    parsed = safe_parse_jsonish(info)
    if isinstance(parsed, list) and parsed:
        parsed = parsed[0]
    if not isinstance(parsed, Mapping):
        return 0.0
    source = _extract_media_source_info(parsed)
    candidates = [
        _minutes(source.get("RunTimeTicks"), 600_000_000.0),
        _minutes(parsed.get("RunTimeTicks"), 600_000_000.0),
    ]
    raw_format = parsed.get("format")
    fmt: Mapping[str, Any] = raw_format if isinstance(raw_format, Mapping) else {}
    candidates.append(_minutes(fmt.get("duration"), 60.0))
    raw_streams = parsed.get("streams")
    streams: list[Any] = raw_streams if isinstance(raw_streams, list) else []
    candidates.extend(_minutes(stream.get("duration"), 60.0) for stream in streams if isinstance(stream, Mapping))
    return next((value for value in candidates if value > 0), 0.0)


def _has_animation_genre(value: object) -> bool:
    genres = safe_parse_jsonish(value)
    if isinstance(genres, Mapping):
        genres = genres.get("genres") or [genres]
    values = genres if isinstance(genres, list) else [genres]
    for item in values:
        if isinstance(item, Mapping):
            if str(item.get("id") or "") == "16":
                return True
            text = f"{item.get('name') or ''} {item.get('zh') or ''} {item.get('en') or ''}".lower()
        else:
            text = str(item or "").lower()
        if any(token in text for token in ("animation", "动画", "動漫", "动漫", "anime", "アニメ")):
            return True
    return False


def _sorting_targets(value: object, *, media_root_name: object) -> tuple[WashingSortingTarget, ...]:
    raw_rules = washing_json_list(value)
    root_name = _normalize_rule_path(media_root_name)
    result: list[WashingSortingTarget] = []
    seen: set[tuple[str, str]] = set()
    for rule in raw_rules:
        if not isinstance(rule, Mapping) or rule.get("enabled") is False:
            continue
        cid = str(rule.get("cid") or "").strip()
        if not cid or cid == "0":
            continue
        path = _normalize_rule_path(rule.get("category_path") or rule.get("dir_name") or rule.get("name"))
        if root_name:
            path_cf = path.casefold()
            root_cf = root_name.casefold()
            if path_cf == root_cf:
                path = ""
            elif path_cf.startswith(root_cf + "/"):
                path = path[len(root_name) :].strip("/")
        if not path:
            continue
        key = cid, path.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(WashingSortingTarget(cid, path))
    result.sort(key=lambda target: len(target.category_path), reverse=True)
    return tuple(result)


def _washing_identity(item: WashingMediaItem, file_name: str) -> dict[str, Any]:
    media_type = "movie" if item.item_type == "Movie" else "series"
    values = {
        "title": item.title,
        "tmdb_id": item.tmdb_id,
        "file_name": file_name,
        "item_type": item.item_type,
        "media_type": media_type,
        "parent_series_tmdb_id": item.parent_series_tmdb_id or None,
        "season_number": item.season_number,
        "episode_number": item.episode_number,
    }
    return {key: value for key, value in values.items() if value not in (None, "", [], {})}


def _slot_has_conditions(match: Mapping[str, Any]) -> bool:
    if any(match.get(key) for key in ("resolution", "source", "codec", "effect", "audio", "subtitle", "release_group")):
        return True
    return bool(
        match.get("subtitle_effect")
        or match.get("clean_version")
        or match.get("min_size_gb") not in (None, "")
        or match.get("max_size_gb") not in (None, "")
    )


def _best_sort_key(value: Mapping[str, Any]) -> tuple[int, int]:
    level = value.get("level")
    if isinstance(level, int) and level > 0:
        return 0, level
    if level is None:
        return 2, 999_999
    return 1, abs(int(level or 0))


def _resolution_tier(width: int, height: int) -> str:
    if width >= 3800 or height >= 2100:
        return "4k"
    if width >= 1800 or height >= 1000:
        return "1080p"
    if width >= 1200 or height >= 700:
        return "720p"
    if width >= 700 or height >= 480:
        return "480p"
    return "未知"


def _language_aliases(value: object) -> list[str]:
    if value is None:
        return []
    values = value.split(",") if isinstance(value, str) else value if isinstance(value, list) else [value]
    return [str(item).strip() for item in values if str(item).strip()]


def _normalize_rule_path(value: object) -> str:
    return re.sub(r"/+", "/", str(value or "").strip().replace("\\", "/")).strip("/")


def _contains_chinese(value: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", value))


def _rule_iterable(value: object) -> list[Any]:
    if value in (None, "", [], {}):
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        return list(value)
    try:
        return list(value)  # type: ignore[arg-type]
    except TypeError:
        return [value]


def _integer(value: object, default: int = 0) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _minutes(value: object, divisor: float) -> float:
    if value in (None, "", 0, "0"):
        return 0.0
    try:
        return float(value) / divisor  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _first_non_empty(*values: object) -> object:
    return next((value for value in values if value not in (None, "", [], {})), None)


def _timestamp(value: datetime | None) -> str:
    current = value or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    return current.astimezone(UTC).isoformat().replace("+00:00", "Z")


__all__ = [
    "WashingCacheMutation",
    "WashingItemMutation",
    "WashingMediaItem",
    "WashingOrganizeBackfill",
    "WashingPolicySnapshot",
    "WashingSortingTarget",
    "WashingVersionSource",
    "build_washing_policy_snapshot",
    "clean_version_result",
    "evaluate_washing_item",
    "extract_quality_source",
    "has_normal_washing_priorities",
    "infer_sorting_target",
    "load_washing_priorities",
    "match_washing_priority",
    "needs_remote_episode_runtime",
    "normalize_language",
    "normalize_quality_source",
    "normalize_release_group",
    "normalize_washing_info",
    "quality_source_tokens",
    "resolve_washing_version_slot",
    "safe_parse_jsonish",
    "safe_parse_list",
    "standardized_effect",
    "washing_level",
    "washing_policy_needs_clean_version",
]
