"""Processor-free refresh of the resubscribe decision index.

The cache task is deliberately kept separate from the resubscribe execution
workflow.  It reads one repeatable PostgreSQL snapshot, evaluates the local
rules, and publishes the complete index in the same transaction.  No provider
or filesystem effect is part of this owner; those effects belong to the later
resubscribe operation runtime.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.tasks import TaskOutcome

logger = logging.getLogger(__name__)

RESUBSCRIBE_CACHE_TASK_KEY = "update-resubscribe-cache"
_PRESERVED_INDEX_STATUSES = frozenset({"subscribed", "auto_subscribed", "ignored", "indeterminate"})


class ResubscribeCacheProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


class ResubscribeCacheRepository(Protocol):
    def refresh_index(self, checkpoint: Callable[[], None]) -> Mapping[str, int]: ...


class NativeResubscribeCacheTaskService:
    """Run the local cache refresh without constructing a legacy processor."""

    def __init__(self, repository: ResubscribeCacheRepository) -> None:
        self._repository = repository

    def execute(
        self,
        task_key: str,
        progress: ResubscribeCacheProgress,
        parameters: Mapping[str, Any],
    ) -> TaskOutcome:
        del parameters
        if task_key != RESUBSCRIBE_CACHE_TASK_KEY:
            raise LookupError(f"unsupported resubscribe cache task key: {task_key!r}")
        progress.update(5, "正在刷新媒体整理索引...")
        progress.checkpoint()
        stats = dict(self._repository.refresh_index(progress.checkpoint))
        progress.update(100, "媒体洗版状态刷新完成。")
        return TaskOutcome.succeeded("媒体洗版状态刷新完成。", stats)


class SqlAlchemyResubscribeCacheRepository:
    """Evaluate resubscribe rules against one frozen local media snapshot."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def refresh_index(self, checkpoint: Callable[[], None]) -> Mapping[str, int]:
        with self._unit_of_work() as uow:
            # The scan and the later purge must observe one database snapshot.
            # A concurrent index/media writer either waits for the table lock or
            # causes PostgreSQL to abort this transaction for a safe retry.
            uow.session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            rules = tuple(
                dict(row)
                for row in uow.session.execute(
                    text("SELECT * FROM resubscribe_rules WHERE enabled IS TRUE ORDER BY sort_order, id")
                )
                .mappings()
                .all()
            )
            checkpoint()
            if not rules:
                uow.session.execute(text("LOCK TABLE resubscribe_index IN SHARE ROW EXCLUSIVE MODE"))
                deleted = uow.session.execute(text("DELETE FROM resubscribe_index"))
                uow.commit()
                return {
                    "candidates": 0,
                    "matched": 0,
                    "purged": int(getattr(deleted, "rowcount", 0) or 0),
                    "failed": 0,
                }

            candidates = self._load_candidates(uow, checkpoint)
            if not candidates:
                # Match the legacy safety behaviour: an empty media snapshot
                # does not prove that previously published rows are stale.
                uow.commit()
                return {"candidates": 0, "matched": 0, "purged": 0, "failed": 0}

            existing_statuses = {
                (str(row["tmdb_id"]), str(row["item_type"]), int(row["season_number"])): str(row["status"])
                for row in uow.session.execute(
                    text("SELECT tmdb_id, item_type, season_number, status FROM resubscribe_index")
                )
                .mappings()
                .all()
            }
            rows, missing_updates, keep_keys = self._evaluate(rules, candidates, existing_statuses, checkpoint)
            checkpoint()

            # Prevent a concurrent UI status update or another cache run from
            # being silently deleted by the stale scan snapshot.
            uow.session.execute(text("LOCK TABLE resubscribe_index IN SHARE ROW EXCLUSIVE MODE"))
            for tmdb_id, missing in missing_updates.items():
                uow.session.execute(
                    text(
                        """
                        UPDATE media_metadata
                        SET watchlist_missing_info_json = CAST(:missing AS jsonb),
                            last_updated_at = NOW()
                        WHERE tmdb_id = :tmdb_id AND item_type = 'Season'
                        """
                    ),
                    {
                        "tmdb_id": tmdb_id,
                        "missing": json.dumps({"missing_episodes": list(missing)}, ensure_ascii=False),
                    },
                )

            for row in rows:
                uow.session.execute(
                    text(
                        """
                        INSERT INTO resubscribe_index (
                            tmdb_id, item_type, season_number, status, reason, matched_rule_id, last_checked_at
                        ) VALUES (:tmdb_id, :item_type, :season_number, :status, :reason, :matched_rule_id, NOW())
                        ON CONFLICT (tmdb_id, item_type, season_number) DO UPDATE SET
                            status = CASE
                                WHEN resubscribe_index.status IN (
                                    'subscribed', 'auto_subscribed', 'ignored', 'indeterminate'
                                )
                                    THEN resubscribe_index.status
                                ELSE EXCLUDED.status
                            END,
                            reason = EXCLUDED.reason,
                            matched_rule_id = EXCLUDED.matched_rule_id,
                            last_checked_at = NOW()
                        """
                    ),
                    row,
                )

            existing = uow.session.execute(
                text("SELECT tmdb_id, item_type, season_number FROM resubscribe_index")
            ).mappings()
            stale = [
                {
                    "tmdb_id": str(row["tmdb_id"]),
                    "item_type": str(row["item_type"]),
                    "season_number": int(row["season_number"]),
                }
                for row in existing
                if (str(row["tmdb_id"]), str(row["item_type"]), int(row["season_number"])) not in keep_keys
            ]
            for row in stale:
                uow.session.execute(
                    text(
                        """
                        DELETE FROM resubscribe_index
                        WHERE tmdb_id = :tmdb_id
                          AND item_type = :item_type
                          AND season_number = :season_number
                        """
                    ),
                    row,
                )
            uow.commit()
            return {
                "candidates": len(candidates),
                "matched": len(rows),
                "purged": len(stale),
                "failed": 0,
            }

    @staticmethod
    def _load_candidates(
        uow: SqlAlchemyUnitOfWork,
        checkpoint: Callable[[], None],
    ) -> tuple[dict[str, Any], ...]:
        rows = (
            uow.session.execute(
                text(
                    """
                    SELECT media.tmdb_id, media.parent_series_tmdb_id, media.item_type,
                           media.season_number, media.title, media.original_title,
                           media.original_language, media.release_year, media.rating,
                           media.custom_rating, media.official_rating_json, media.runtime_minutes,
                           media.date_added, media.release_date, media.genres_json, media.countries_json,
                           media.keywords_json, media.tags_json, media.production_companies_json,
                           media.networks_json, media.actors_json, media.directors_json,
                           media.asset_details_json, media.emby_item_ids_json,
                           media.watchlist_missing_info_json, media.in_library,
                           media.watching_status, media.force_ended, media.total_episodes,
                           media.total_episodes_locked,
                           series.title AS parent_title,
                           series.original_language AS parent_original_language,
                           series.original_title AS parent_original_title,
                           series.rating AS parent_rating,
                           series.watching_status AS parent_watching_status
                    FROM media_metadata AS media
                    LEFT JOIN media_metadata AS series
                      ON media.item_type = 'Season'
                     AND series.tmdb_id = media.parent_series_tmdb_id
                     AND series.item_type = 'Series'
                    WHERE media.item_type IN ('Movie', 'Season')
                      AND COALESCE(media.force_ended, FALSE) IS FALSE
                      AND (
                          media.item_type = 'Movie' AND media.in_library IS TRUE
                          OR media.item_type = 'Season' AND EXISTS (
                              SELECT 1
                              FROM media_metadata AS episode
                              WHERE episode.parent_series_tmdb_id = media.parent_series_tmdb_id
                                AND episode.item_type = 'Episode'
                                AND episode.season_number = media.season_number
                                AND episode.in_library IS TRUE
                          )
                      )
                      AND COALESCE(series.watching_status, '') NOT IN ('Watching', 'Paused', 'Pending')
                    ORDER BY media.tmdb_id, media.item_type, media.season_number
                    """
                )
            )
            .mappings()
            .all()
        )
        checkpoint()
        episode_rows = (
            uow.session.execute(
                text(
                    """
                    SELECT parent_series_tmdb_id, season_number, episode_number,
                           asset_details_json, emby_item_ids_json
                    FROM media_metadata
                    WHERE item_type = 'Episode' AND in_library IS TRUE
                    ORDER BY parent_series_tmdb_id, season_number, episode_number NULLS LAST, tmdb_id
                    """
                )
            )
            .mappings()
            .all()
        )
        episodes: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
        for row in episode_rows:
            parent = str(row.get("parent_series_tmdb_id") or "").strip()
            season = _positive_int(row.get("season_number"))
            if parent and season > 0:
                episodes[(parent, season)].append(dict(row))

        candidates: list[dict[str, Any]] = []
        for raw in rows:
            candidate = dict(raw)
            item_type = str(candidate.get("item_type") or "")
            if item_type == "Movie":
                candidate["_identity"] = (str(candidate.get("tmdb_id") or ""), "Movie", -1)
                candidate["_assets"] = _asset_objects(candidate.get("asset_details_json"))
                candidate["_multi_version"] = len(_json_sequence(candidate.get("emby_item_ids_json"))) > 1
            elif item_type == "Season":
                parent = str(candidate.get("parent_series_tmdb_id") or "").strip()
                season_number = _positive_int(candidate.get("season_number"))
                season_episodes = episodes.get((parent, season_number), [])
                if not parent or season_number <= 0 or not season_episodes:
                    continue
                candidate["_identity"] = (parent, "Season", season_number)
                candidate["_episodes"] = tuple(season_episodes)
                candidate["_assets"] = _first_episode_assets(season_episodes)
                candidate["_multi_version"] = any(
                    len(_json_sequence(row.get("emby_item_ids_json"))) > 1 for row in season_episodes
                )
                missing = _missing_episode_numbers(
                    int(candidate.get("total_episodes") or 0) if bool(candidate.get("total_episodes_locked")) else 0,
                    [row.get("episode_number") for row in season_episodes],
                )
                candidate["_missing_episodes"] = missing
                candidate["_has_gaps"] = bool(missing)
                candidate["_in_progress"] = str(candidate.get("parent_watching_status") or "").casefold() in {
                    "watching",
                    "paused",
                    "pending",
                }
                # Season rows often do not carry their own language/title/rating;
                # use the parent only for rule evaluation, never for identity.
                for field in ("title", "original_title", "original_language", "rating"):
                    if candidate.get(field) in (None, ""):
                        candidate[field] = candidate.get(f"parent_{field}")
            else:
                continue
            candidates.append(candidate)
        return tuple(candidates)

    @classmethod
    def _evaluate(
        cls,
        rules: Sequence[Mapping[str, Any]],
        candidates: Sequence[Mapping[str, Any]],
        existing_statuses: Mapping[tuple[str, str, int], str],
        checkpoint: Callable[[], None],
    ) -> tuple[list[dict[str, Any]], dict[str, tuple[int, ...]], set[tuple[str, str, int]]]:
        processed: set[str] = set()
        matched_rows: list[dict[str, Any]] = []
        missing_updates: dict[str, tuple[int, ...]] = {}
        keep_keys: set[tuple[str, str, int]] = set()
        for rule_index, rule in enumerate(rules):
            checkpoint()
            for candidate in candidates:
                identity = candidate.get("_identity")
                if not isinstance(identity, tuple) or len(identity) != 3:
                    continue
                logical_id = f"{identity[1]}:{identity[0]}"
                if logical_id in processed or not _scope_matches(candidate, rule):
                    continue
                # Legacy rule priority is first matching scope, even when the
                # physical file later turns out to be unusable.
                processed.add(logical_id)
                if identity[1] == "Season":
                    missing_updates[str(candidate["tmdb_id"])] = tuple(candidate.get("_missing_episodes", ()))
                if bool(candidate.get("_multi_version")) or not candidate.get("_assets"):
                    continue
                reason = _match_reason(candidate, rule)
                if reason is None:
                    continue
                key = (str(identity[0]), str(identity[1]), int(identity[2]))
                previous = str(existing_statuses.get(key) or "")
                status = previous if previous in _PRESERVED_INDEX_STATUSES else "needed"
                matched_rows.append(
                    {
                        "tmdb_id": key[0],
                        "item_type": key[1],
                        "season_number": key[2],
                        "status": status,
                        "reason": reason,
                        "matched_rule_id": _positive_int(rule.get("id")) or None,
                    }
                )
                keep_keys.add(key)
            if rule_index % 10 == 0:
                checkpoint()
        return matched_rows, missing_updates, keep_keys


def _scope_matches(candidate: Mapping[str, Any], rule: Mapping[str, Any]) -> bool:
    raw_rules = rule.get("scope_rules")
    if not isinstance(raw_rules, list) or not raw_rules:
        return False
    used = False
    for raw in raw_rules:
        if not isinstance(raw, Mapping):
            return False
        field = str(raw.get("field") or "").strip().casefold()
        expected = raw.get("value", raw.get("values"))
        if not field or _empty_scope_value(expected):
            continue
        used = True
        actual = _scope_values(candidate, field)
        operator = str(raw.get("operator") or "in").strip().casefold()
        if not _scope_operator_matches(field, actual, expected, operator):
            return False
    return used


def _scope_values(candidate: Mapping[str, Any], field: str) -> tuple[Any, ...]:
    assets = tuple(candidate.get("_assets", ()))
    if field in {"title", "name"}:
        return (candidate.get("title"),)
    if field in {"original_title", "original_name"}:
        return (candidate.get("original_title"),)
    if field in {"original_language", "language"}:
        return (candidate.get("original_language"),)
    if field in {"release_year", "year", "rating", "runtime", "runtime_minutes"}:
        key = (
            "runtime_minutes"
            if field in {"runtime", "runtime_minutes"}
            else "release_year"
            if field in {"year"}
            else field
        )
        values: list[Any] = [candidate.get(key)]
        if field in {"runtime", "runtime_minutes"}:
            values.extend(asset.get("runtime_minutes") or asset.get("runtime") for asset in assets)
        return tuple(value for value in values if value not in (None, ""))
    if field in {"date_added", "release_date"}:
        return (candidate.get(field),)
    if field in {"genres", "genre"}:
        return _object_values(candidate.get("genres_json"))
    if field in {"countries", "country"}:
        return _object_values(candidate.get("countries_json"))
    if field in {"tags", "tag"}:
        return _object_values(candidate.get("tags_json"))
    if field in {"keywords", "keyword"}:
        return _object_values(candidate.get("keywords_json"))
    if field in {"studios", "studio"}:
        return _object_values(candidate.get("production_companies_json")) + _object_values(
            candidate.get("networks_json")
        )
    if field in {"actors", "directors"}:
        return _object_values(candidate.get(f"{field}_json"))
    if field in {"library", "libraries"}:
        values: list[Any] = []
        for asset in assets:
            values.append(asset.get("source_library_id"))
        return tuple(value for value in values if value not in (None, ""))
    if field in {"resolution", "quality", "effect", "codec"}:
        key = {
            "resolution": "resolution_display",
            "quality": "quality_display",
            "effect": "effect_display",
            "codec": "codec_display",
        }[field]
        return tuple(asset.get(key) for asset in assets if asset.get(key) not in (None, ""))
    if field == "audio_lang":
        return tuple(asset.get("audio_display") for asset in assets if asset.get("audio_display") not in (None, ""))
    if field == "is_in_progress":
        return (bool(candidate.get("_in_progress")),)
    if field == "unified_rating":
        official = candidate.get("official_rating_json")
        if isinstance(official, Mapping):
            return (candidate.get("custom_rating") or official.get("US"),)
        return (candidate.get("custom_rating"),)
    return ()


def _scope_operator_matches(field: str, actual: Sequence[Any], expected: Any, operator: str) -> bool:
    expected_values = _flatten_values(expected)
    if not expected_values:
        return True
    if operator in {"is_none_of", "not_in", "not-in", "exclude", "neq", "!=", "does_not_contain"}:
        if operator == "does_not_contain":
            return not any(
                str(value or "").casefold().find(str(item).casefold()) >= 0
                for value in actual
                for item in expected_values
            )
        return not any(_equal(value, item) for value in actual for item in expected_values)
    if operator in {"contains", "like", "includes"}:
        if field in {"title", "name", "original_title", "original_name", "audio_lang"}:
            return any(
                str(item).casefold() in str(value or "").casefold() for value in actual for item in expected_values
            )
        return any(_equal(value, item) for value in actual for item in expected_values)
    if operator in {"starts_with", "startswith"}:
        return any(
            str(value or "").casefold().startswith(str(item).casefold()) for value in actual for item in expected_values
        )
    if operator in {"ends_with", "endswith"}:
        return any(
            str(value or "").casefold().endswith(str(item).casefold()) for value in actual for item in expected_values
        )
    if operator in {"gte", ">=", "min", "lte", "<=", "max"}:
        predicate = (
            (lambda left, right: left >= right)
            if operator in {"gte", ">=", "min"}
            else (lambda left, right: left <= right)
        )
        return any(_numeric_compare(value, item, predicate) for value in actual for item in expected_values)
    if operator in {"is", "eq", "equals", "is_primary"}:
        values = actual[:1] if operator == "is_primary" else actual
        return any(_equal(value, item) for value in values for item in expected_values)
    if operator == "in_last_days":
        return _date_window(actual, expected_values, older=False)
    if operator == "not_in_last_days":
        return _date_window(actual, expected_values, older=True)
    return any(_equal(value, item) for value in actual for item in expected_values)


def _match_reason(candidate: Mapping[str, Any], rule: Mapping[str, Any]) -> str | None:
    raw_assets = candidate.get("_assets")
    if not isinstance(raw_assets, Sequence) or not raw_assets or not isinstance(raw_assets[0], Mapping):
        return None
    details = dict(raw_assets[0])
    reasons: list[str] = []
    if bool(rule.get("filter_rating_enabled")):
        rating = _float(candidate.get("rating"), 0.0)
        minimum = _float(rule.get("filter_rating_min"), 0.0)
        if not (rating == 0 and bool(rule.get("filter_rating_ignore_zero"))) and rating < minimum:
            if str(rule.get("rule_type") or "resubscribe") != "delete":
                return None
            reasons.append(f"评分过低({rating:g})")
    if bool(rule.get("resubscribe_resolution_enabled")):
        threshold = _float(rule.get("resubscribe_resolution_threshold"), 1920.0)
        if _resolution_tier(details.get("resolution_display")) < _resolution_tier_for_width(threshold):
            reasons.append("分辨率不达标")
    if bool(rule.get("resubscribe_quality_enabled")):
        required = _string_set(rule.get("resubscribe_quality_include"))
        if required and _quality_tier(details.get("quality_display")) < max(_quality_tier(value) for value in required):
            reasons.append("质量不符")
    if bool(rule.get("resubscribe_effect_enabled")):
        required = _string_set(rule.get("resubscribe_effect_include"))
        if required and _effect_tier(details.get("effect_display")) < max(_effect_tier(value) for value in required):
            reasons.append("特效不达标")
    if bool(rule.get("resubscribe_codec_enabled")):
        required = _string_set(rule.get("resubscribe_codec_include"))
        if required and _codec_tier(details.get("codec_display")) < max(_codec_tier(value) for value in required):
            reasons.append("编码不符")
    if bool(rule.get("resubscribe_filesize_enabled")):
        size = _float(details.get("size_bytes"), 0.0)
        threshold = _float(rule.get("resubscribe_filesize_threshold_gb"), 10.0)
        if size > 0:
            value = size / (1024**3)
            operator = str(rule.get("resubscribe_filesize_operator") or "lt").casefold()
            if (operator == "gt" and value > threshold) or (operator != "gt" and value < threshold):
                reasons.append(f"文件 {'>' if operator == 'gt' else '<'} {threshold:g} GB")
    if bool(rule.get("resubscribe_audio_enabled")):
        audio = _string_set(details.get("audio_languages_raw"))
        for language in _string_set(rule.get("resubscribe_audio_missing_languages")):
            if not _language_exempt(candidate, language) and not _audio_present(audio, language):
                reasons.append(f"缺{language}音轨")
    if bool(rule.get("resubscribe_subtitle_enabled")):
        audio = _string_set(details.get("audio_languages_raw"))
        subtitles = _string_set(details.get("subtitle_languages_raw"))
        display = str(details.get("subtitle_display") or "").casefold()
        for language in _string_set(rule.get("resubscribe_subtitle_missing_languages")):
            if _language_exempt(candidate, language):
                continue
            if bool(rule.get("resubscribe_subtitle_skip_if_audio_exists")) and _audio_present(audio, language):
                continue
            if not _subtitle_present(subtitles, display, language):
                reasons.append(f"缺{language}字幕")
    if str(candidate.get("item_type") or "") == "Season":
        if bool(rule.get("filter_missing_episodes_enabled")):
            if not bool(candidate.get("_has_gaps")):
                return None
            reasons.append("存在缺失集数")
        if bool(rule.get("consistency_check_enabled")) and not _season_consistent(candidate, rule):
            reasons.append("季包一致性校验未通过")
    if not reasons:
        return None
    return "; ".join(dict.fromkeys(reasons))


def _season_consistent(candidate: Mapping[str, Any], rule: Mapping[str, Any]) -> bool:
    episodes = candidate.get("_episodes")
    if not isinstance(episodes, Sequence) or not episodes:
        return False
    expected = _positive_int(candidate.get("total_episodes")) if bool(candidate.get("total_episodes_locked")) else 0
    if expected and len(episodes) < expected:
        return False
    signatures: list[tuple[str, str, str, str]] = []
    codes: list[str] = []
    for row in episodes:
        if not isinstance(row, Mapping):
            return False
        assets = _asset_objects(row.get("asset_details_json"))
        if not assets:
            return False
        asset = assets[0]
        resolution = _known(asset.get("resolution_display") or asset.get("resolution"))
        codec = _known(asset.get("codec_display") or asset.get("video_codec"))
        if not resolution or not codec:
            return False
        signatures.append(
            (
                resolution,
                codec,
                _known(asset.get("release_group_raw") or asset.get("release_group")),
                _known(asset.get("effect_display") or asset.get("effect")),
            )
        )
        codes.append(
            _known(asset.get("file_sha1") or asset.get("sha1") or asset.get("file_pickcode") or asset.get("pick_code"))
        )
    if not signatures or len({value[0] for value in signatures}) != 1 or len({value[1] for value in signatures}) != 1:
        return False
    if bool(rule.get("consistency_must_match_group")) and (
        len({value[2] for value in signatures}) != 1 or not all(value[2] for value in signatures)
    ):
        return False
    if bool(rule.get("consistency_must_match_code")) and (len(set(codes)) != 1 or not all(codes)):
        return False
    if any(value[3] for value in signatures):
        return len({value[3] for value in signatures}) == 1 and all(value[3] for value in signatures)
    return True


def _first_episode_assets(rows: Sequence[Mapping[str, Any]]) -> tuple[Mapping[str, Any], ...]:
    for row in rows:
        assets = _asset_objects(row.get("asset_details_json"))
        if assets:
            return assets
    return ()


def _asset_objects(value: Any) -> tuple[Mapping[str, Any], ...]:
    parsed = _parse_json(value)
    if isinstance(parsed, Mapping):
        for key in ("items", "files", "assets", "asset_details"):
            nested = parsed.get(key)
            if isinstance(nested, list):
                parsed = nested
                break
        else:
            parsed = [parsed]
    if not isinstance(parsed, list):
        return ()
    return tuple(dict(item) for item in parsed if isinstance(item, Mapping))


def _object_values(value: Any) -> tuple[Any, ...]:
    parsed = _parse_json(value)
    if isinstance(parsed, Mapping):
        return tuple(
            value for key in ("id", "tmdb_id", "name", "value") if (value := parsed.get(key)) not in (None, "")
        )
    if isinstance(parsed, Sequence) and not isinstance(parsed, str | bytes):
        values: list[Any] = []
        for item in parsed:
            values.extend(_object_values(item))
        return tuple(values)
    return () if parsed in (None, "") else (parsed,)


def _flatten_values(value: Any) -> tuple[Any, ...]:
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        values: list[Any] = []
        for item in value:
            values.extend(_flatten_values(item))
        return tuple(values)
    return () if value in (None, "") else (value,)


def _json_sequence(value: Any) -> tuple[Any, ...]:
    parsed = _parse_json(value)
    return tuple(parsed) if isinstance(parsed, Sequence) and not isinstance(parsed, str | bytes) else ()


def _parse_json(value: Any) -> Any:
    if isinstance(value, str) and value.lstrip().startswith(("[", "{")):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return value
    return value


def _empty_scope_value(value: Any) -> bool:
    return (
        value is None
        or value == ""
        or (isinstance(value, Sequence) and not isinstance(value, str | bytes) and not value)
    )


def _equal(left: Any, right: Any) -> bool:
    return str(left or "").strip().casefold() == str(right or "").strip().casefold() and str(left or "").strip() != ""


def _numeric_compare(left: Any, right: Any, predicate: Callable[[float, float], bool]) -> bool:
    try:
        return predicate(float(left), float(right))
    except (TypeError, ValueError):
        return False


def _date_window(actual: Sequence[Any], expected: Sequence[Any], *, older: bool) -> bool:
    days = _positive_int(expected[0]) if expected else 0
    if days <= 0:
        return False
    today = datetime.now(UTC).date()
    for value in actual:
        parsed = _date_value(value)
        if parsed is None:
            continue
        in_window = parsed >= today - timedelta(days=days)
        if (not in_window) if older else in_window:
            return True
    return False


def _date_value(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        return date.fromisoformat(normalized[:10])
    except ValueError:
        return None


def _positive_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        result = int(value)
    except (TypeError, ValueError):
        return 0
    return result if result > 0 else 0


def _float(value: Any, default: float) -> float:
    if isinstance(value, bool):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _string_set(value: Any) -> frozenset[str]:
    return frozenset(str(item).strip().casefold() for item in _flatten_values(value) if str(item).strip())


def _known(value: Any) -> str:
    normalized = str(value or "").strip().casefold()
    return "" if normalized in {"", "unknown", "未知", "无"} else normalized


def _resolution_tier(value: Any) -> int:
    normalized = str(value or "").strip().casefold()
    if normalized in {"4k", "2160p", "uhd"}:
        return 4
    if normalized in {"1080p", "fhd"}:
        return 3
    if normalized in {"720p", "hd"}:
        return 2
    if normalized in {"480p", "sd"}:
        return 1
    return 1


def _resolution_tier_for_width(value: float) -> int:
    if value >= 3800:
        return 4
    if value >= 1900:
        return 3
    if value >= 1200:
        return 2
    return 1


def _quality_tier(value: Any) -> int:
    normalized = str(value or "").strip().casefold().replace("_", "-")
    for name, tier in (
        ("remux", 6),
        ("blu-ray", 5),
        ("bluray", 5),
        ("web-dl", 4),
        ("webdl", 4),
        ("webrip", 3),
        ("hdtv", 2),
        ("dvdrip", 1),
    ):
        if name in normalized:
            return tier
    return 0


def _codec_tier(value: Any) -> int:
    normalized = str(value or "").strip().casefold()
    if any(alias in normalized for alias in ("hevc", "h265", "x265")):
        return 2
    if any(alias in normalized for alias in ("h264", "avc", "x264")):
        return 1
    return 0


def _effect_tier(value: Any) -> int:
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return max((_effect_tier(item) for item in value), default=0)
    normalized = str(value or "sdr").strip().casefold().replace(" ", "_")
    normalized = {"杜比视界": "dovi_other", "dolby_vision": "dovi_other", "hdr": "hdr"}.get(normalized, normalized)
    return {
        "dovi_p8": 7,
        "dovi_p7": 6,
        "dovi_p5": 5,
        "dovi_other": 4,
        "hdr10+": 3,
        "hdr10plus": 3,
        "hdr": 2,
        "sdr": 1,
    }.get(normalized, 0)


def _audio_present(audio: frozenset[str], language: str) -> bool:
    normalized = language.casefold()
    aliases = {
        "zh": {"zh", "chi", "cmn", "yue", "cantonese"},
        "chi": {"chi", "zh", "cmn"},
        "yue": {"yue", "zh-yue", "cantonese"},
    }
    return bool(audio & aliases.get(normalized, {normalized}))


def _subtitle_present(subtitles: frozenset[str], display: str, language: str) -> bool:
    normalized = language.casefold()
    if normalized == "zh":
        return bool({"简体", "繁体", "chi", "yue"} & subtitles) or "简体" in display or "繁体" in display
    if normalized == "chi":
        return "chi" in subtitles or "简体" in display
    if normalized == "yue":
        return "yue" in subtitles or "繁体" in display
    return normalized in subtitles


def _language_exempt(candidate: Mapping[str, Any], language: str) -> bool:
    normalized = language.casefold()
    if normalized not in {"zh", "chi", "yue", "jpn", "kor", "eng"}:
        return False
    original = str(candidate.get("original_language") or "").casefold()
    mapped = {"zh": "chi", "cn": "chi", "cmn": "chi", "hk": "yue", "en": "eng", "ja": "jpn", "ko": "kor"}.get(
        original, original
    )
    targets = {"zh", "chi", "yue"} if normalized == "zh" else {normalized}
    if mapped in targets:
        return True
    title = str(candidate.get("original_title") or "")
    return normalized in {"zh", "chi", "yue", "jpn", "kor"} and sum("\u4e00" <= char <= "\u9fff" for char in title) >= 2


def _missing_episode_numbers(expected_count: int, collected: Sequence[Any]) -> tuple[int, ...]:
    expected = _positive_int(expected_count)
    if expected <= 0:
        return ()
    numbers = {_positive_int(value) for value in collected if _positive_int(value) > 0}
    return tuple(number for number in range(1, expected + 1) if number not in numbers)


__all__ = [
    "NativeResubscribeCacheTaskService",
    "RESUBSCRIBE_CACHE_TASK_KEY",
    "ResubscribeCacheProgress",
    "ResubscribeCacheRepository",
    "SqlAlchemyResubscribeCacheRepository",
]
