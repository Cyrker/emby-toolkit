from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from functools import cmp_to_key
from typing import Any, Protocol
from uuid import UUID

from etk.platform import Settings
from etk.policy_defaults import DEFAULT_RATING_MAPPING

from .cleanup import CLEANUP_CONFIG_SETTING, canonical_cleanup_config

RATING_MAPPING_SETTING = "rating_mapping"

_DEFAULT_SCAN_RULES: tuple[Mapping[str, Any], ...] = (
    {"id": "runtime", "enabled": True},
    {
        "id": "effect",
        "enabled": True,
        "priority": ["dovi_p8", "dovi_p7", "dovi_p5", "dovi_other", "hdr10+", "hdr", "sdr"],
    },
    {
        "id": "resolution",
        "enabled": True,
        "priority": ["4k", "1080p", "720p", "480p"],
    },
    {"id": "bit_depth", "enabled": True},
    {"id": "bitrate", "enabled": True},
    {
        "id": "codec",
        "enabled": True,
        "priority": ["AV1", "HEVC", "H.264", "VP9"],
    },
    {
        "id": "quality",
        "enabled": True,
        "priority": ["remux", "blu-ray", "web-dl", "hdtv"],
    },
    {"id": "subtitle", "enabled": True, "priority": "desc"},
    {"id": "frame_rate", "enabled": False},
    {"id": "filesize", "enabled": True},
    {"id": "date_added", "enabled": True, "priority": "asc"},
)


@dataclass(frozen=True, slots=True)
class CleanupScanPolicy:
    rules: tuple[Mapping[str, Any], ...]
    library_ids: tuple[str, ...]
    keep_one_per_resolution: bool
    rating_mapping: Mapping[str, tuple[Mapping[str, Any], ...]]


@dataclass(frozen=True, slots=True)
class CleanupScanItem:
    tmdb_id: str
    item_type: str
    title: str
    original_language: str
    countries: object
    asset_details: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True, slots=True)
class CleanupScanSnapshot:
    user_id: str | None
    policy: CleanupScanPolicy
    visible_movie_ids: tuple[str, ...]
    visible_series_ids: tuple[str, ...]
    items: tuple[CleanupScanItem, ...]
    source_fingerprint: str


@dataclass(frozen=True, slots=True)
class CleanupScanEntry:
    tmdb_id: str
    item_type: str
    versions: tuple[Mapping[str, Any], ...]
    best_version: str | tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CleanupScanReconcileResult:
    deleted_pending: int
    upserted_items: int


@dataclass(frozen=True, slots=True)
class CleanupScanResult:
    message: str
    stats: Mapping[str, Any]


class CleanupScanRepository(Protocol):
    def load_snapshot(self, *, user_id: str | None) -> CleanupScanSnapshot: ...

    def reconcile(
        self,
        snapshot: CleanupScanSnapshot,
        entries: Sequence[CleanupScanEntry],
        *,
        job_id: UUID | None,
        cancellation_check: Callable[[], None],
    ) -> CleanupScanReconcileResult: ...


class CleanupEmbyInventory(Protocol):
    def existing_item_ids(self, item_ids: Sequence[str]) -> frozenset[str]: ...


class CleanupScanProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


class CleanupScanService:
    """Processor-free duplicate scan with authoritative Emby readback and atomic publication."""

    def __init__(
        self,
        repository: CleanupScanRepository,
        inventory: CleanupEmbyInventory,
        settings: Callable[[], Settings],
    ) -> None:
        self._repository = repository
        self._inventory = inventory
        self._settings = settings

    def execute(
        self,
        progress: CleanupScanProgress,
        *,
        requested_by_user_id: object = None,
        execution_job_id: object = None,
    ) -> CleanupScanResult:
        requested_user = str(requested_by_user_id or "").strip()
        configured_user = str(self._settings().emby_user_id or "").strip()
        user_id = requested_user or configured_user or None
        job_id = _optional_uuid(execution_job_id, "cleanup scan execution_job_id")
        progress.checkpoint()
        progress.update(1, "  ➜ [媒体去重] 正在计算用户可见扫描范围")
        snapshot = self._repository.load_snapshot(user_id=user_id)
        visible_total = len(snapshot.visible_movie_ids) + len(snapshot.visible_series_ids)
        stats: dict[str, Any] = {
            "requested_by_user_id": user_id,
            "visible_movies": len(snapshot.visible_movie_ids),
            "visible_series": len(snapshot.visible_series_ids),
            "scanned_items": 0,
            "checked_emby_ids": 0,
            "missing_emby_ids": 0,
            "indexed_items": 0,
            "deleted_pending": 0,
            "upserted_items": 0,
            "authoritative_emby_read": False,
            "atomic_reconcile": False,
        }
        if visible_total == 0:
            message = "扫描中止：当前用户视角下没有可见的媒体项。"
            progress.update(100, message)
            return CleanupScanResult(message, stats)

        candidate_ids = tuple(
            dict.fromkeys(
                str(version.get("emby_item_id") or "").strip()
                for item in snapshot.items
                for version in item.asset_details
                if str(version.get("emby_item_id") or "").strip()
            )
        )
        progress.checkpoint()
        existing_ids = self._inventory.existing_item_ids(candidate_ids)
        unexpected = existing_ids - set(candidate_ids)
        if unexpected:
            raise ValueError("cleanup Emby inventory returned identities outside the requested set")
        stats["checked_emby_ids"] = len(candidate_ids)
        stats["missing_emby_ids"] = len(set(candidate_ids) - existing_ids)
        stats["authoritative_emby_read"] = True

        entries: list[CleanupScanEntry] = []
        total_items = len(snapshot.items)
        for index, item in enumerate(snapshot.items, start=1):
            progress.checkpoint()
            stats["scanned_items"] += 1
            entry = evaluate_cleanup_scan_item(
                item,
                existing_item_ids=existing_ids,
                policy=snapshot.policy,
            )
            if entry is not None:
                entries.append(entry)
            current = 10 + int(index / max(total_items, 1) * 80)
            progress.update(min(current, 90), f"({index}/{total_items}) 正在分析: {item.title or '未知媒体'}")

        progress.checkpoint()
        effect_label = "cleanup-index-publication"
        _record_effect(progress, "record_started_effect", effect_label)
        try:
            reconciled = self._repository.reconcile(
                snapshot,
                entries,
                job_id=job_id,
                cancellation_check=progress.checkpoint,
            )
        except Exception:
            _record_effect(progress, "record_abandoned_effect", effect_label)
            raise
        _record_effect(progress, "record_committed_effect", effect_label)
        stats["indexed_items"] = len(entries)
        stats["deleted_pending"] = reconciled.deleted_pending
        stats["upserted_items"] = reconciled.upserted_items
        stats["atomic_reconcile"] = True
        message = f"扫描完成！共发现 {len(entries)} 组需要清理的多版本媒体。"
        progress.update(100, message)
        return CleanupScanResult(message, stats)


def effective_cleanup_scan_policy(values: Mapping[str, Any]) -> CleanupScanPolicy:
    config = canonical_cleanup_config(values.get(CLEANUP_CONFIG_SETTING))

    raw_rules = config["rules"] or _DEFAULT_SCAN_RULES
    rules = [deepcopy(dict(rule)) for rule in raw_rules]

    raw_libraries = config["library_ids"]
    library_ids = tuple(dict.fromkeys(str(value).strip() for value in raw_libraries if str(value).strip()))

    raw_rating_mapping = values.get(RATING_MAPPING_SETTING) or DEFAULT_RATING_MAPPING
    if not isinstance(raw_rating_mapping, Mapping):
        raise TypeError("rating_mapping must be an object")
    rating_mapping: dict[str, tuple[Mapping[str, Any], ...]] = {}
    for country, raw_entries in raw_rating_mapping.items():
        if not isinstance(raw_entries, list):
            raise TypeError("rating_mapping country entries must be arrays")
        entries: list[Mapping[str, Any]] = []
        for entry in raw_entries:
            if not isinstance(entry, Mapping):
                raise TypeError("rating_mapping entry must be an object")
            entries.append(deepcopy(dict(entry)))
        rating_mapping[str(country)] = tuple(entries)
    return CleanupScanPolicy(
        rules=tuple(rules),
        library_ids=library_ids,
        keep_one_per_resolution=config["keep_one_per_res"],
        rating_mapping=rating_mapping,
    )


def _optional_uuid(value: object, field: str) -> UUID | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return value if isinstance(value, UUID) else UUID(str(value))
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError(f"{field} must be a UUID") from exc


def _record_effect(progress: CleanupScanProgress, method_name: str, label: str) -> None:
    callback = getattr(progress, method_name, None)
    if callable(callback):
        callback(label)


def evaluate_cleanup_scan_item(
    item: CleanupScanItem,
    *,
    existing_item_ids: frozenset[str],
    policy: CleanupScanPolicy,
) -> CleanupScanEntry | None:
    versions_by_id: dict[str, Mapping[str, Any]] = {}
    for raw_version in item.asset_details:
        if not isinstance(raw_version, Mapping):
            raise TypeError("cleanup asset version must be an object")
        emby_id = str(raw_version.get("emby_item_id") or "").strip()
        if not emby_id or emby_id not in existing_item_ids:
            continue
        versions_by_id[emby_id] = dict(raw_version)
    versions = list(versions_by_id.values())
    if len(versions) < 2:
        return None

    chinese_media = _is_chinese_media(item.original_language, item.countries)
    if policy.keep_one_per_resolution:
        groups: dict[object, list[Mapping[str, Any]]] = defaultdict(list)
        for version in versions:
            groups[_version_properties(version).get("resolution", "unknown")].append(version)
        best_ids: list[str] = []
        for group_versions in groups.values():
            best_id = _best_version_id(group_versions, policy.rules, chinese_media=chinese_media)
            if best_id and best_id not in best_ids:
                best_ids.append(best_id)
        if len(best_ids) == len(versions):
            return None
        best_version: str | tuple[str, ...] = tuple(best_ids)
    else:
        best_version = _best_version_id(versions, policy.rules, chinese_media=chinese_media)
        if not best_version:
            return None

    frontend_versions = tuple(_frontend_version(version) for version in versions)
    return CleanupScanEntry(
        tmdb_id=item.tmdb_id,
        item_type=item.item_type,
        versions=frontend_versions,
        best_version=best_version,
    )


def _version_properties(version: Mapping[str, Any]) -> dict[str, Any]:
    subtitle_languages = version.get("subtitle_languages_raw", [])
    subtitle_count = len(subtitle_languages)
    if subtitle_count == 0:
        raw_subtitles = version.get("subtitles", [])
        if raw_subtitles:
            subtitle_count = len(raw_subtitles)
    quality = (
        str(version.get("quality_display", "未知")).lower().replace("bluray", "blu-ray").replace("webdl", "web-dl")
    )
    effect_raw = version.get("effect_display", "SDR")
    if isinstance(effect_raw, list):
        effect_raw = effect_raw[0] if effect_raw else "SDR"
    raw_id = version.get("emby_item_id")
    integer_id = int(raw_id) if raw_id and str(raw_id).isdigit() else 0
    return {
        "id": version.get("emby_item_id"),
        "path": version.get("path"),
        "quality": quality,
        "resolution": version.get("resolution_display", "未知"),
        "effect": str(effect_raw).lower(),
        "codec": version.get("codec_display", "未知"),
        "filesize": version.get("size_bytes") or 0,
        "video_bitrate_mbps": version.get("video_bitrate_mbps") or 0,
        "bit_depth": version.get("bit_depth") or 8,
        "frame_rate": version.get("frame_rate") or 0,
        "runtime_minutes": version.get("runtime_minutes") or 0,
        "date_added": version.get("date_added_to_library") or "",
        "int_id": integer_id,
        "subtitle_count": subtitle_count,
        "subtitle_languages": subtitle_languages,
    }


def _best_version_id(
    versions: Sequence[Mapping[str, Any]],
    rules: Sequence[Mapping[str, Any]],
    *,
    chinese_media: bool,
) -> str:
    properties = [_version_properties(version) for version in versions if version]
    ordered = sorted(
        properties,
        key=cmp_to_key(lambda left, right: _compare_versions(left, right, rules, chinese_media=chinese_media)),
        reverse=True,
    )
    return str(ordered[0].get("id") or "") if ordered else ""


def _compare_versions(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    rules: Sequence[Mapping[str, Any]],
    *,
    chinese_media: bool,
) -> int:
    for rule in rules:
        if not rule.get("enabled"):
            continue
        rule_type = rule.get("id")
        preference = rule.get("priority", "desc")
        result = 0
        if rule_type == "bitrate":
            left_value = left.get("video_bitrate_mbps") or 0
            right_value = right.get("video_bitrate_mbps") or 0
            if abs(left_value - right_value) > 1.0:
                result = 1 if (left_value < right_value if preference == "asc" else left_value > right_value) else -1
        elif rule_type == "bit_depth":
            left_value = left.get("bit_depth") or 8
            right_value = right.get("bit_depth") or 8
            if left_value != right_value:
                result = 1 if (left_value < right_value if preference == "asc" else left_value > right_value) else -1
        elif rule_type == "frame_rate":
            left_value = left.get("frame_rate") or 0
            right_value = right.get("frame_rate") or 0
            if abs(left_value - right_value) > 2.0:
                result = 1 if (left_value < right_value if preference == "asc" else left_value > right_value) else -1
        elif rule_type == "runtime":
            left_value = left.get("runtime_minutes") or 0
            right_value = right.get("runtime_minutes") or 0
            if abs(left_value - right_value) > 2:
                result = 1 if (left_value < right_value if preference == "asc" else left_value > right_value) else -1
        elif rule_type == "filesize":
            left_value = left.get("filesize") or 0
            right_value = right.get("filesize") or 0
            if left_value != right_value:
                result = 1 if (left_value < right_value if preference == "asc" else left_value > right_value) else -1
        elif rule_type in {"resolution", "quality", "effect", "codec"}:
            left_value = left.get(str(rule_type))
            right_value = right.get(str(rule_type))
            priority = _priority_values(rule.get("priority", []))
            if rule_type == "resolution":
                priority = [_normalize_resolution(value) for value in priority]
                left_value, right_value = _normalize_resolution(left_value), _normalize_resolution(right_value)
            elif rule_type == "quality":
                priority = [
                    str(value).lower().replace("bluray", "blu-ray").replace("webdl", "web-dl") for value in priority
                ]
            elif rule_type == "effect":
                priority = [str(value).lower().replace(" ", "_") for value in priority]
            else:
                priority = [_normalize_codec(value) for value in priority]
                left_value, right_value = _normalize_codec(left_value), _normalize_codec(right_value)
            left_index = priority.index(left_value) if left_value in priority else 999
            right_index = priority.index(right_value) if right_value in priority else 999
            if left_index != right_index:
                result = 1 if left_index < right_index else -1
        elif rule_type == "subtitle":
            if chinese_media:
                continue
            chinese_codes = {"chi", "zho", "zh", "yue", "chs", "cht", "zh-cn", "zh-tw", "zh-hk"}
            left_languages = [str(value).lower() for value in left.get("subtitle_languages", [])]
            right_languages = [str(value).lower() for value in right.get("subtitle_languages", [])]
            left_has_chinese = any(value in chinese_codes for value in left_languages)
            right_has_chinese = any(value in chinese_codes for value in right_languages)
            if left_has_chinese != right_has_chinese:
                result = 1 if left_has_chinese else -1
        elif rule_type == "date_added":
            left_date, right_date = left.get("date_added"), right.get("date_added")
            if left_date and right_date and left_date != right_date:
                result = 1 if (left_date < right_date if preference == "asc" else left_date > right_date) else -1
            else:
                left_id, right_id = int(left.get("int_id") or 0), int(right.get("int_id") or 0)
                if left_id != right_id:
                    result = 1 if (left_id < right_id if preference == "asc" else left_id > right_id) else -1
        if result != 0:
            return result
    return 0


def _frontend_version(version: Mapping[str, Any]) -> Mapping[str, Any]:
    properties = _version_properties(version)
    return {
        "id": version.get("emby_item_id"),
        "path": version.get("path"),
        "filesize": version.get("size_bytes", 0),
        "quality": properties.get("quality"),
        "resolution": properties.get("resolution"),
        "effect": properties.get("effect"),
        "video_bitrate_mbps": properties.get("video_bitrate_mbps"),
        "bit_depth": properties.get("bit_depth"),
        "frame_rate": properties.get("frame_rate"),
        "runtime_minutes": properties.get("runtime_minutes"),
        "codec": properties.get("codec"),
        "subtitle_count": properties.get("subtitle_count"),
        "subtitle_languages": properties.get("subtitle_languages"),
    }


def _is_chinese_media(original_language: object, countries: object) -> bool:
    language = str(original_language or "").lower()
    raw_countries = countries
    if isinstance(raw_countries, str):
        try:
            raw_countries = json.loads(raw_countries)
        except (TypeError, ValueError):
            raw_countries = []
    country_values = raw_countries if isinstance(raw_countries, list) else []
    return language in {"zh", "cn", "zh-cn", "zh-tw", "zh-hk", "yue", "cmn"} or any(
        value in ("CN", "HK", "TW", "MO") for value in country_values
    )


def _priority_values(value: object) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, Sequence) and not isinstance(value, bytes):
        return list(value)
    try:
        return list(value)  # type: ignore[arg-type]
    except TypeError:
        return [value]


def _normalize_resolution(value: object) -> str:
    normalized = str(value).lower()
    return "4k" if normalized == "2160p" else normalized


def _normalize_codec(value: object) -> str:
    normalized = str(value).upper()
    if normalized in {"H265", "X265"}:
        return "HEVC"
    if normalized in {"H264", "X264", "AVC"}:
        return "H.264"
    return normalized


def cleanup_scan_setting_keys() -> tuple[str, ...]:
    return (
        CLEANUP_CONFIG_SETTING,
        RATING_MAPPING_SETTING,
    )


__all__ = [
    "CleanupEmbyInventory",
    "CleanupScanEntry",
    "CleanupScanItem",
    "CleanupScanPolicy",
    "CleanupScanProgress",
    "CleanupScanReconcileResult",
    "CleanupScanRepository",
    "CleanupScanResult",
    "CleanupScanService",
    "CleanupScanSnapshot",
    "cleanup_scan_setting_keys",
    "effective_cleanup_scan_policy",
    "evaluate_cleanup_scan_item",
]
