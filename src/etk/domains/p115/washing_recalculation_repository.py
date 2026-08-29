from __future__ import annotations

import hashlib
import json
import logging
import os
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork

from .washing_evaluator import (
    WashingItemMutation,
    WashingMediaItem,
    WashingVersionSource,
)
from .washing_priority import normalize_washing_priority_recalculate_scope

logger = logging.getLogger(__name__)


class WashingRecalculationDrift(RuntimeError):
    """The source projection changed after evaluation and must not be overwritten."""


class SqlAlchemyP115WashingRecalculationRepository:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def load_policy_source(self) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        keys = (
            "p115_washing_priority_config",
            "p115_rename_config",
            "language_mapping",
            "release_group_mapping",
            "p115_sorting_rules",
        )
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                    SELECT
                        COALESCE(
                            (
                                SELECT jsonb_object_agg(setting_key, value_json)
                                FROM app_settings
                                WHERE setting_key = ANY(CAST(:keys AS text[]))
                            ),
                            '{}'::jsonb
                        ) AS settings,
                        COALESCE(
                            (
                                SELECT jsonb_agg(to_jsonb(groups) ORDER BY groups.sort_order ASC)
                                FROM (
                                    SELECT id, name, media_type, target_cids, priorities, sort_order
                                    FROM washing_priority_groups
                                    ORDER BY sort_order ASC
                                ) groups
                            ),
                            '[]'::jsonb
                        ) AS groups
                    """
                    ),
                    {"keys": list(keys)},
                )
                .mappings()
                .one()
            )
        settings = row.get("settings")
        groups = row.get("groups")
        return (
            dict(settings) if isinstance(settings, Mapping) else {},
            [dict(group) for group in groups if isinstance(group, Mapping)] if isinstance(groups, list) else [],
        )

    def list_candidates(
        self,
        *,
        item_types: Sequence[str],
        affected_scope: object,
        limit: int | None,
    ) -> list[tuple[str, str]]:
        allowed = tuple(str(value) for value in item_types if str(value) in {"Movie", "Episode"})
        if not allowed:
            return []
        params: dict[str, Any] = {"item_types": list(allowed)}
        scope_sql = ""
        if affected_scope is not None:
            normalized = normalize_washing_priority_recalculate_scope(affected_scope)
            clauses: list[str] = []
            for media_type, item_type in (("Movie", "Movie"), ("Series", "Episode")):
                if item_type not in allowed:
                    continue
                entry = normalized.get(media_type)
                if not isinstance(entry, Mapping):
                    continue
                if entry.get("all"):
                    clauses.append(f"mm.item_type = :scope_{media_type.lower()}_type")
                    params[f"scope_{media_type.lower()}_type"] = item_type
                    continue
                target_cids = entry.get("target_cids")
                if not isinstance(target_cids, list) or not target_cids:
                    continue
                prefix = media_type.lower()
                params[f"scope_{prefix}_type"] = item_type
                params[f"scope_{prefix}_cids"] = [str(value) for value in target_cids]
                clauses.append(
                    f"""
                    (
                        mm.item_type = :scope_{prefix}_type
                        AND (
                            COALESCE(mm.washing_snapshot_json, '{{}}'::jsonb)->>'target_cid'
                                = ANY(CAST(:scope_{prefix}_cids AS text[]))
                            OR EXISTS (
                                SELECT 1
                                FROM jsonb_array_elements(
                                    CASE
                                        WHEN jsonb_typeof(
                                            COALESCE(mm.washing_snapshot_json, '{{}}'::jsonb)->'versions'
                                        ) = 'array'
                                        THEN COALESCE(mm.washing_snapshot_json, '{{}}'::jsonb)->'versions'
                                        ELSE '[]'::jsonb
                                    END
                                ) version
                                WHERE version->>'target_cid'
                                    = ANY(CAST(:scope_{prefix}_cids AS text[]))
                            )
                            OR EXISTS (
                                SELECT 1
                                FROM jsonb_array_elements_text(
                                    CASE
                                        WHEN jsonb_typeof(mm.file_pickcode_json) = 'array'
                                        THEN mm.file_pickcode_json
                                        ELSE '[]'::jsonb
                                    END
                                ) pc(value)
                                JOIN p115_organize_records record ON record.pick_code = pc.value
                                WHERE record.target_cid
                                    = ANY(CAST(:scope_{prefix}_cids AS text[]))
                            )
                        )
                    )
                    """
                )
            scope_sql = f"AND ({' OR '.join(clauses)})" if clauses else "AND FALSE"
        limit_sql = ""
        if limit is not None and limit > 0:
            params["limit"] = int(limit)
            limit_sql = "LIMIT :limit"
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    f"""
                    SELECT mm.tmdb_id, mm.item_type
                    FROM media_metadata mm
                    WHERE mm.in_library = TRUE
                      AND mm.item_type = ANY(CAST(:item_types AS text[]))
                      {scope_sql}
                    ORDER BY mm.item_type ASC, COALESCE(mm.title, '') ASC, mm.tmdb_id ASC
                    {limit_sql}
                    """
                ),
                params,
            ).mappings()
            return [(str(row["tmdb_id"]), str(row["item_type"])) for row in rows]

    def load_item(self, tmdb_id: str, item_type: str) -> WashingMediaItem | None:
        with self._unit_of_work() as uow:
            return self._load_item(uow, tmdb_id, item_type, for_update=False)

    def persist_item(
        self,
        item: WashingMediaItem,
        mutation: WashingItemMutation,
    ) -> int:
        with self._unit_of_work() as uow:
            current = self._load_item(
                uow,
                item.tmdb_id,
                item.item_type,
                for_update=True,
            )
            if current is None:
                raise WashingRecalculationDrift("media item disappeared during washing recalculation")
            if current.source_fingerprint != item.source_fingerprint:
                raise WashingRecalculationDrift("media item changed during washing recalculation")

            for cache in mutation.cache_mutations:
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_filesystem_cache
                        SET washing_level = :level,
                            washing_snapshot_json = CAST(:snapshot AS jsonb),
                            updated_at = NOW()
                        WHERE id = :cache_id
                        """
                    ),
                    {
                        "cache_id": cache.cache_id,
                        "level": cache.level,
                        "snapshot": _json(cache.snapshot),
                    },
                )

            backfilled = 0
            for backfill in mutation.organize_backfills:
                try:
                    with uow.session.begin_nested():
                        if self._backfill_target_cid(uow, current, backfill):
                            backfilled += 1
                except Exception as exc:
                    logger.debug(
                        "Unable to backfill washing target CID for tmdb=%s cache=%s: %s",
                        item.tmdb_id,
                        backfill.cache_id,
                        exc,
                        exc_info=True,
                    )

            uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET washing_level = :level,
                        washing_snapshot_json = CAST(:snapshot AS jsonb),
                        last_updated_at = NOW()
                    WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                    """
                ),
                {
                    "tmdb_id": item.tmdb_id,
                    "item_type": item.item_type,
                    "level": mutation.media_level,
                    "snapshot": _json(mutation.media_snapshot),
                },
            )
            uow.commit()
            return backfilled

    def _load_item(
        self,
        uow: SqlAlchemyUnitOfWork,
        tmdb_id: str,
        item_type: str,
        *,
        for_update: bool,
    ) -> WashingMediaItem | None:
        lock_sql = "FOR UPDATE OF mm" if for_update else ""
        row = (
            uow.session.execute(
                text(
                    f"""
                SELECT
                    mm.tmdb_id,
                    mm.item_type,
                    mm.parent_series_tmdb_id,
                    mm.season_number,
                    mm.episode_number,
                    mm.title,
                    COALESCE(
                        NULLIF(BTRIM(mm.original_language), ''),
                        NULLIF(BTRIM(parent.original_language), '')
                    )
                        AS original_language,
                    mm.runtime_minutes,
                    mm.file_sha1_json,
                    mm.file_pickcode_json,
                    mm.asset_details_json,
                    mm.washing_snapshot_json,
                    parent.genres_json AS parent_genres_json,
                    CASE
                        WHEN mm.item_type = 'Episode' THEN (
                            SELECT AVG(peer.runtime_minutes)
                            FROM media_metadata peer
                            WHERE peer.item_type = 'Episode'
                              AND peer.parent_series_tmdb_id = mm.parent_series_tmdb_id
                              AND peer.season_number = mm.season_number
                              AND peer.in_library = TRUE
                              AND peer.runtime_minutes IS NOT NULL
                              AND peer.runtime_minutes > 0
                        )
                        ELSE NULL
                    END AS average_episode_runtime
                FROM media_metadata mm
                LEFT JOIN LATERAL (
                    SELECT p.original_language, p.genres_json
                    FROM media_metadata p
                    WHERE mm.item_type = 'Episode'
                      AND p.item_type = 'Series'
                      AND p.tmdb_id = mm.parent_series_tmdb_id
                    ORDER BY p.in_library DESC, p.last_updated_at DESC NULLS LAST
                    LIMIT 1
                ) parent ON TRUE
                WHERE mm.tmdb_id = :tmdb_id
                  AND mm.item_type = :item_type
                  AND mm.in_library = TRUE
                {lock_sql}
                """
                ),
                {"tmdb_id": str(tmdb_id), "item_type": str(item_type)},
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None

        sha1s = _clean_array(row.get("file_sha1_json"))
        pick_codes = _clean_array(row.get("file_pickcode_json"))
        version_count = max(len(sha1s), len(pick_codes))
        versions: list[WashingVersionSource] = []
        for index in range(version_count):
            sha1 = sha1s[index].upper() if index < len(sha1s) else ""
            pick_code = pick_codes[index] if index < len(pick_codes) else ""
            cache = self._load_cache(
                uow,
                sha1=sha1,
                pick_code=pick_code,
                for_update=for_update,
            )
            if cache is not None:
                sha1 = sha1 or str(cache.get("sha1") or "").strip().upper()
                pick_code = pick_code or str(cache.get("pick_code") or "").strip()
            file_name = str((cache or {}).get("name") or "") or _asset_file_name(
                row.get("asset_details_json"),
                index,
            )
            file_size = _strict_integer((cache or {}).get("size") or 0)
            organize_target = self._load_organize_target(
                uow,
                cache_id=str((cache or {}).get("id") or ""),
                pick_code=pick_code,
            )
            mediainfo = self._load_mediainfo(uow, sha1) if sha1 else None
            versions.append(
                WashingVersionSource(
                    cache_id=str((cache or {}).get("id") or ""),
                    sha1=sha1,
                    pick_code=pick_code,
                    file_name=file_name,
                    file_size=file_size,
                    local_path=str((cache or {}).get("local_path") or ""),
                    cache_snapshot=_mapping((cache or {}).get("washing_snapshot_json")),
                    organize_target_cid=organize_target,
                    mediainfo=mediainfo,
                )
            )

        payload = {
            "tmdb_id": str(row.get("tmdb_id") or ""),
            "item_type": str(row.get("item_type") or ""),
            "parent_series_tmdb_id": str(row.get("parent_series_tmdb_id") or ""),
            "season_number": _optional_integer(row.get("season_number")),
            "episode_number": _optional_integer(row.get("episode_number")),
            "title": str(row.get("title") or ""),
            "original_language": str(row.get("original_language") or ""),
            "runtime_minutes": _float(row.get("runtime_minutes")),
            "average_episode_runtime_minutes": _float(row.get("average_episode_runtime")),
            "parent_genres": row.get("parent_genres_json"),
            "asset_details": row.get("asset_details_json"),
            "washing_snapshot": _mapping(row.get("washing_snapshot_json")),
            "versions": [
                {
                    "cache_id": version.cache_id,
                    "sha1": version.sha1,
                    "pick_code": version.pick_code,
                    "file_name": version.file_name,
                    "file_size": version.file_size,
                    "local_path": version.local_path,
                    "cache_snapshot": dict(version.cache_snapshot),
                    "organize_target_cid": version.organize_target_cid,
                    "mediainfo": version.mediainfo,
                }
                for version in versions
            ],
        }
        return WashingMediaItem(
            tmdb_id=payload["tmdb_id"],
            item_type=payload["item_type"],
            parent_series_tmdb_id=payload["parent_series_tmdb_id"],
            season_number=payload["season_number"],
            episode_number=payload["episode_number"],
            title=payload["title"],
            original_language=payload["original_language"],
            runtime_minutes=payload["runtime_minutes"],
            average_episode_runtime_minutes=payload["average_episode_runtime_minutes"],
            parent_genres=payload["parent_genres"],
            asset_details=payload["asset_details"],
            washing_snapshot=payload["washing_snapshot"],
            versions=tuple(versions),
            source_fingerprint=_fingerprint(payload),
        )

    @staticmethod
    def _load_cache(
        uow: SqlAlchemyUnitOfWork,
        *,
        sha1: str,
        pick_code: str,
        for_update: bool,
    ) -> dict[str, Any] | None:
        clauses: list[str] = []
        params: dict[str, Any] = {}
        if pick_code:
            clauses.append("pick_code = :pick_code")
            params["pick_code"] = pick_code
        if sha1:
            clauses.append("UPPER(sha1) = :sha1")
            params["sha1"] = sha1.upper()
        if not clauses:
            return None
        order = (
            "CASE WHEN pick_code = :pick_code THEN 0 ELSE 1 END, updated_at DESC NULLS LAST"
            if pick_code
            else "updated_at DESC NULLS LAST"
        )
        lock_sql = "FOR UPDATE" if for_update else ""
        row = (
            uow.session.execute(
                text(
                    f"""
                SELECT id, parent_id, name, sha1, pick_code, local_path, size,
                       washing_level, washing_snapshot_json
                FROM p115_filesystem_cache
                WHERE {" OR ".join(clauses)}
                ORDER BY {order}
                LIMIT 1
                {lock_sql}
                """
                ),
                params,
            )
            .mappings()
            .one_or_none()
        )
        return dict(row) if row is not None else None

    @staticmethod
    def _load_mediainfo(uow: SqlAlchemyUnitOfWork, sha1: str) -> object:
        return uow.session.scalar(
            text(
                """
                SELECT mediainfo_json
                FROM p115_mediainfo_cache
                WHERE sha1 = :sha1 OR UPPER(sha1) = :sha1
                LIMIT 1
                """
            ),
            {"sha1": sha1.upper()},
        )

    @staticmethod
    def _load_organize_target(
        uow: SqlAlchemyUnitOfWork,
        *,
        cache_id: str,
        pick_code: str,
    ) -> str:
        clauses: list[str] = []
        params: dict[str, Any] = {}
        if pick_code:
            clauses.append("pick_code = :pick_code")
            params["pick_code"] = pick_code
        if cache_id:
            clauses.append("file_id = :file_id")
            params["file_id"] = cache_id
        if not clauses:
            return ""
        value = uow.session.scalar(
            text(
                f"""
                SELECT target_cid
                FROM p115_organize_records
                WHERE ({" OR ".join(clauses)})
                  AND target_cid IS NOT NULL
                  AND target_cid <> ''
                ORDER BY processed_at DESC NULLS LAST, id DESC
                LIMIT 1
                """
            ),
            params,
        )
        return str(value or "").strip()

    @staticmethod
    def _backfill_target_cid(
        uow: SqlAlchemyUnitOfWork,
        item: WashingMediaItem,
        backfill: Any,
    ) -> bool:
        clauses: list[str] = []
        params: dict[str, Any] = {
            "target_cid": backfill.target_cid,
            "category_name": backfill.category_name or None,
            "tmdb_id": item.tmdb_id or None,
            "media_type": "movie" if item.item_type == "Movie" else "tv",
            "season_number": item.season_number,
        }
        if backfill.pick_code:
            clauses.append("pick_code = :pick_code")
            params["pick_code"] = backfill.pick_code
        if backfill.cache_id:
            clauses.append("file_id = :file_id")
            params["file_id"] = backfill.cache_id
        if clauses:
            updated = uow.session.scalar(
                text(
                    f"""
                    UPDATE p115_organize_records
                    SET target_cid = :target_cid,
                        category_name = COALESCE(NULLIF(category_name, ''), :category_name),
                        tmdb_id = COALESCE(tmdb_id, :tmdb_id),
                        media_type = COALESCE(media_type, :media_type),
                        season_number = COALESCE(season_number, :season_number)
                    WHERE {" OR ".join(clauses)}
                    RETURNING id
                    """
                ),
                params,
            )
            if updated is not None:
                return True
        if not backfill.cache_id:
            return False
        inserted = uow.session.scalar(
            text(
                """
                INSERT INTO p115_organize_records (
                    file_id,
                    pick_code,
                    original_name,
                    status,
                    tmdb_id,
                    media_type,
                    target_cid,
                    category_name,
                    renamed_name,
                    processed_at,
                    season_number
                )
                VALUES (
                    :file_id,
                    :pick_code,
                    :original_name,
                    'success',
                    :tmdb_id,
                    :media_type,
                    :target_cid,
                    :category_name,
                    :renamed_name,
                    NOW(),
                    :season_number
                )
                ON CONFLICT (file_id) DO UPDATE SET
                    target_cid = EXCLUDED.target_cid,
                    category_name = COALESCE(
                        NULLIF(p115_organize_records.category_name, ''),
                        EXCLUDED.category_name
                    ),
                    tmdb_id = COALESCE(p115_organize_records.tmdb_id, EXCLUDED.tmdb_id),
                    media_type = COALESCE(p115_organize_records.media_type, EXCLUDED.media_type),
                    season_number = COALESCE(
                        p115_organize_records.season_number,
                        EXCLUDED.season_number
                    )
                RETURNING id
                """
            ),
            {
                **params,
                "file_id": backfill.cache_id,
                "pick_code": backfill.pick_code or None,
                "original_name": backfill.original_name,
                "renamed_name": backfill.original_name or None,
            },
        )
        return inserted is not None


def _clean_array(value: object) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            value = []
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if item not in (None, "", [], {})]


def _asset_file_name(value: object, index: int) -> str:
    assets = value
    if isinstance(assets, str):
        try:
            assets = json.loads(assets)
        except Exception:
            assets = []
    if isinstance(assets, Mapping):
        assets = [assets]
    if not isinstance(assets, list) or not assets:
        return ""
    candidates: list[object] = []
    if index < len(assets):
        candidates.append(assets[index])
    candidates.extend(assets)
    for raw_asset in candidates:
        if not isinstance(raw_asset, Mapping):
            continue
        name = raw_asset.get("file_name") or raw_asset.get("FileName") or raw_asset.get("name") or raw_asset.get("Name")
        if name:
            return str(name)
        path = raw_asset.get("path") or raw_asset.get("Path") or raw_asset.get("file_path") or raw_asset.get("FilePath")
        if path:
            return os.path.basename(str(path).replace("\\", "/"))
    return ""


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            value = {}
    return dict(value) if isinstance(value, Mapping) else {}


def _strict_integer(value: object) -> int:
    return int(value)  # type: ignore[arg-type]


def _optional_integer(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _float(value: object) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _fingerprint(value: Mapping[str, Any]) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return hashlib.sha256(payload).hexdigest()


__all__ = [
    "SqlAlchemyP115WashingRecalculationRepository",
    "WashingRecalculationDrift",
]
