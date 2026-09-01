from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork


@dataclass(frozen=True, slots=True)
class SubscriptionMediaKey:
    tmdb_id: str
    item_type: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> SubscriptionMediaKey:
        tmdb_id = str(value.get("tmdb_id") or "").strip()
        item_type = str(value.get("item_type") or "").strip()
        if not tmdb_id or not item_type:
            raise ValueError("tmdb_id and item_type are required")
        return cls(tmdb_id, item_type)


class SqlAlchemySubscriptionMediaProjection:
    """P5-owned media_metadata projection and guarded status mutations."""

    def list_subscriptions(self, uow: SqlAlchemyUnitOfWork) -> list[dict[str, Any]]:
        rows = (
            uow.session.execute(
                text(
                    """
                    WITH active_seasons AS (
                        SELECT DISTINCT parent_series_tmdb_id
                        FROM media_metadata
                        WHERE item_type = 'Season'
                          AND subscription_status IN (
                            'REQUESTED','WANTED','PENDING_RELEASE','IGNORED','SUBSCRIBED','PAUSED'
                          )
                    )
                    SELECT
                        media.tmdb_id,
                        media.item_type,
                        media.season_number,
                        CASE
                            WHEN media.item_type = 'Season'
                                THEN COALESCE(parent.title, '未知剧集') || ' 第 ' || media.season_number || ' 季 '
                            ELSE media.title
                        END AS title,
                        media.release_date,
                        media.poster_path,
                        media.subscription_status,
                        media.ignore_reason,
                        media.subscription_sources_json,
                        media.first_requested_at,
                        media.last_subscribed_at,
                        media.paused_until,
                        CASE
                            WHEN media.item_type = 'Series' THEN media.tmdb_id
                            WHEN media.item_type = 'Season' THEN media.parent_series_tmdb_id
                            ELSE NULL
                        END AS series_tmdb_id
                    FROM media_metadata AS media
                    LEFT JOIN media_metadata AS parent
                      ON media.parent_series_tmdb_id = parent.tmdb_id
                     AND parent.item_type = 'Series'
                    LEFT JOIN active_seasons
                      ON media.tmdb_id = active_seasons.parent_series_tmdb_id
                    WHERE media.subscription_status IN (
                        'REQUESTED','WANTED','PENDING_RELEASE','IGNORED','SUBSCRIBED','PAUSED'
                    )
                      AND (media.item_type <> 'Series' OR active_seasons.parent_series_tmdb_id IS NULL)
                    ORDER BY media.first_requested_at DESC
                    """
                )
            )
            .mappings()
            .all()
        )
        return [{key: _jsonable(value) for key, value in dict(row).items()} for row in rows]

    def lock_item(
        self,
        uow: SqlAlchemyUnitOfWork,
        key: SubscriptionMediaKey,
    ) -> Mapping[str, Any] | None:
        row = (
            uow.session.execute(
                text(
                    """
                    SELECT
                        tmdb_id,
                        item_type,
                        in_library,
                        subscription_status,
                        subscription_sources_json,
                        ignore_reason,
                        parent_series_tmdb_id,
                        season_number,
                        title,
                        first_requested_at,
                        last_subscribed_at,
                        last_updated_at
                    FROM media_metadata
                    WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                    FOR UPDATE
                    """
                ),
                {"tmdb_id": key.tmdb_id, "item_type": key.item_type},
            )
            .mappings()
            .first()
        )
        return dict(row) if row is not None else None

    def lock_season_key(
        self,
        uow: SqlAlchemyUnitOfWork,
        *,
        parent_series_tmdb_id: str,
        season_number: int,
    ) -> SubscriptionMediaKey | None:
        tmdb_id = uow.session.scalar(
            text(
                """
                SELECT tmdb_id
                FROM media_metadata
                WHERE item_type = 'Season'
                  AND parent_series_tmdb_id = :parent_series_tmdb_id
                  AND season_number = :season_number
                ORDER BY last_updated_at DESC NULLS LAST, tmdb_id
                FOR UPDATE
                LIMIT 1
                """
            ),
            {
                "parent_series_tmdb_id": parent_series_tmdb_id,
                "season_number": int(season_number),
            },
        )
        return SubscriptionMediaKey(str(tmdb_id), "Season") if tmdb_id is not None else None

    def ensure_metadata(
        self,
        uow: SqlAlchemyUnitOfWork,
        key: SubscriptionMediaKey,
        record: Mapping[str, Any],
    ) -> None:
        uow.session.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, original_title, release_date,
                    release_year, poster_path, backdrop_path, overview,
                    season_number, parent_series_tmdb_id,
                    last_synced_at, last_updated_at
                ) VALUES (
                    :tmdb_id, :item_type, :title, :original_title, :release_date,
                    :release_year, :poster_path, :backdrop_path, :overview,
                    :season_number, :parent_series_tmdb_id,
                    NOW(), NOW()
                )
                ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                    title = EXCLUDED.title,
                    original_title = EXCLUDED.original_title,
                    release_date = EXCLUDED.release_date,
                    release_year = EXCLUDED.release_year,
                    poster_path = EXCLUDED.poster_path,
                    backdrop_path = EXCLUDED.backdrop_path,
                    overview = EXCLUDED.overview,
                    season_number = EXCLUDED.season_number,
                    parent_series_tmdb_id = EXCLUDED.parent_series_tmdb_id,
                    last_synced_at = NOW(),
                    last_updated_at = NOW()
                """
            ),
            {
                "tmdb_id": key.tmdb_id,
                "item_type": key.item_type,
                "title": record.get("title"),
                "original_title": record.get("original_title"),
                "release_date": record.get("release_date") or None,
                "release_year": record.get("release_year"),
                "poster_path": record.get("poster_path"),
                "backdrop_path": record.get("backdrop_path"),
                "overview": record.get("overview"),
                "season_number": record.get("season_number"),
                "parent_series_tmdb_id": record.get("parent_series_tmdb_id"),
            },
        )

    def set_status(
        self,
        uow: SqlAlchemyUnitOfWork,
        key: SubscriptionMediaKey,
        status: str,
        request: Mapping[str, Any],
        source: Mapping[str, Any] | None,
        ignore_reason: str | None,
    ) -> int:
        if status == "NONE":
            return self.reset_item(uow, key)
        params = _status_params(key, status, request, source, ignore_reason)
        if status == "WANTED" and bool(request.get("force_unignore")):
            result = uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET subscription_status = 'WANTED',
                        subscription_sources_json = CASE
                            WHEN subscription_sources_json @> CAST(:source_json AS jsonb)
                                THEN subscription_sources_json
                            ELSE subscription_sources_json || CAST(:source_json AS jsonb)
                        END,
                        ignore_reason = NULL,
                        last_updated_at = NOW()
                    WHERE tmdb_id = :tmdb_id
                      AND item_type = :item_type
                      AND subscription_status = 'IGNORED'
                    """
                ),
                params,
            )
            return int(getattr(result, "rowcount", 0) or 0)
        sql = {
            "WANTED": _WANTED_SQL,
            "PENDING_RELEASE": _PENDING_RELEASE_SQL,
            "SUBSCRIBED": _SUBSCRIBED_SQL,
            "IGNORED": _IGNORED_SQL,
        }[status]
        result = uow.session.execute(text(sql), params)
        return int(getattr(result, "rowcount", 0) or 0)

    def reset_item(self, uow: SqlAlchemyUnitOfWork, key: SubscriptionMediaKey) -> int:
        result = uow.session.execute(
            text(
                """
                UPDATE media_metadata
                SET subscription_status = 'NONE',
                    subscription_sources_json = '[]'::jsonb,
                    ignore_reason = NULL,
                    last_updated_at = NOW()
                WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                """
            ),
            {"tmdb_id": key.tmdb_id, "item_type": key.item_type},
        )
        return int(getattr(result, "rowcount", 0) or 0)

    def delete_item(self, uow: SqlAlchemyUnitOfWork, key: SubscriptionMediaKey) -> int:
        result = uow.session.execute(
            text("DELETE FROM media_metadata WHERE tmdb_id = :tmdb_id AND item_type = :item_type"),
            {"tmdb_id": key.tmdb_id, "item_type": key.item_type},
        )
        return int(getattr(result, "rowcount", 0) or 0)


def _status_params(
    key: SubscriptionMediaKey,
    status: str,
    request: Mapping[str, Any],
    source: Mapping[str, Any] | None,
    ignore_reason: str | None,
) -> dict[str, Any]:
    return {
        "tmdb_id": key.tmdb_id,
        "item_type": key.item_type,
        "status": status,
        "source_json": json.dumps([dict(source)] if source is not None else [], ensure_ascii=False),
        "source_type": str(source.get("type") or "") if source is not None else "",
        "ignore_reason": ignore_reason,
        "title": request.get("title"),
        "original_title": request.get("original_title"),
        "release_date": request.get("release_date") or None,
        "release_year": request.get("release_year"),
        "poster_path": request.get("poster_path"),
        "backdrop_path": request.get("backdrop_path"),
        "season_number": request.get("season_number"),
        "parent_series_tmdb_id": request.get("parent_series_tmdb_id"),
        "overview": request.get("overview"),
    }


_INSERT_COLUMNS = """
    tmdb_id, item_type, subscription_status, subscription_sources_json,
    ignore_reason, first_requested_at, last_subscribed_at, title,
    original_title, release_date, release_year, poster_path, backdrop_path,
    season_number, parent_series_tmdb_id, overview, last_updated_at
"""
_INSERT_VALUES = """
    :tmdb_id, :item_type, :status, CAST(:source_json AS jsonb),
    :ignore_reason, NOW(), CASE WHEN :status = 'SUBSCRIBED' THEN NOW() ELSE NULL END,
    :title, :original_title, :release_date, :release_year, :poster_path,
    :backdrop_path, :season_number, :parent_series_tmdb_id, :overview, NOW()
"""
_APPEND_SOURCE = """
    CASE
        WHEN CAST(:source_json AS jsonb) = '[]'::jsonb THEN media_metadata.subscription_sources_json
        WHEN media_metadata.subscription_sources_json @> CAST(:source_json AS jsonb)
            THEN media_metadata.subscription_sources_json
        ELSE media_metadata.subscription_sources_json || CAST(:source_json AS jsonb)
    END
"""

_WANTED_SQL = f"""
    INSERT INTO media_metadata ({_INSERT_COLUMNS}) VALUES ({_INSERT_VALUES})
    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
        subscription_status = 'WANTED',
        subscription_sources_json = {_APPEND_SOURCE},
        first_requested_at = COALESCE(media_metadata.first_requested_at, EXCLUDED.first_requested_at),
        ignore_reason = NULL,
        parent_series_tmdb_id = COALESCE(EXCLUDED.parent_series_tmdb_id, media_metadata.parent_series_tmdb_id),
        poster_path = COALESCE(EXCLUDED.poster_path, media_metadata.poster_path),
        backdrop_path = COALESCE(EXCLUDED.backdrop_path, media_metadata.backdrop_path),
        release_year = COALESCE(EXCLUDED.release_year, media_metadata.release_year),
        last_updated_at = NOW()
    WHERE media_metadata.subscription_status NOT IN ('SUBSCRIBED', 'PAUSED')
      AND (media_metadata.in_library = FALSE OR :source_type IN ('resubscribe','gap_scan','revived_season'))
      AND (
        CAST(:source_json AS jsonb) = '[]'::jsonb
        OR NOT (media_metadata.subscription_sources_json @> CAST(:source_json AS jsonb))
        OR :source_type IN ('resubscribe','gap_scan','revived_season')
        OR (
            media_metadata.subscription_status = 'IGNORED'
            AND (
                media_metadata.ignore_reason IS NULL
                OR media_metadata.ignore_reason NOT IN ('手动忽略', '订阅超时')
            )
        )
      )
"""

_PENDING_RELEASE_SQL = f"""
    INSERT INTO media_metadata ({_INSERT_COLUMNS}) VALUES ({_INSERT_VALUES})
    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
        subscription_status = 'PENDING_RELEASE',
        subscription_sources_json = {_APPEND_SOURCE},
        first_requested_at = COALESCE(media_metadata.first_requested_at, EXCLUDED.first_requested_at),
        ignore_reason = NULL,
        parent_series_tmdb_id = COALESCE(EXCLUDED.parent_series_tmdb_id, media_metadata.parent_series_tmdb_id),
        poster_path = COALESCE(EXCLUDED.poster_path, media_metadata.poster_path),
        backdrop_path = COALESCE(EXCLUDED.backdrop_path, media_metadata.backdrop_path),
        release_year = COALESCE(EXCLUDED.release_year, media_metadata.release_year),
        last_updated_at = NOW()
    WHERE media_metadata.in_library = FALSE
      AND media_metadata.subscription_status NOT IN ('SUBSCRIBED', 'WANTED', 'PAUSED')
      AND NOT (
        media_metadata.subscription_status = 'IGNORED'
        AND COALESCE(media_metadata.ignore_reason, '') IN ('手动忽略', '订阅超时')
      )
      AND (
        CAST(:source_json AS jsonb) = '[]'::jsonb
        OR NOT (media_metadata.subscription_sources_json @> CAST(:source_json AS jsonb))
        OR :source_type = 'revived_season'
      )
"""

_SUBSCRIBED_SQL = f"""
    INSERT INTO media_metadata ({_INSERT_COLUMNS}) VALUES ({_INSERT_VALUES})
    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
        subscription_status = 'SUBSCRIBED',
        subscription_sources_json = {_APPEND_SOURCE},
        first_requested_at = COALESCE(media_metadata.first_requested_at, EXCLUDED.first_requested_at),
        last_subscribed_at = NOW(),
        ignore_reason = NULL,
        parent_series_tmdb_id = COALESCE(EXCLUDED.parent_series_tmdb_id, media_metadata.parent_series_tmdb_id),
        poster_path = COALESCE(EXCLUDED.poster_path, media_metadata.poster_path),
        backdrop_path = COALESCE(EXCLUDED.backdrop_path, media_metadata.backdrop_path),
        release_year = COALESCE(EXCLUDED.release_year, media_metadata.release_year),
        last_updated_at = NOW()
    WHERE (
        media_metadata.in_library = FALSE
        OR media_metadata.item_type = 'Season'
        OR :source_type IN ('resubscribe','gap_scan')
        OR media_metadata.subscription_status = 'WANTED'
      )
      AND (
        CAST(:source_json AS jsonb) = '[]'::jsonb
        OR NOT (media_metadata.subscription_sources_json @> CAST(:source_json AS jsonb))
        OR media_metadata.subscription_status <> 'SUBSCRIBED'
      )
"""

_IGNORED_SQL = f"""
    INSERT INTO media_metadata ({_INSERT_COLUMNS}) VALUES ({_INSERT_VALUES})
    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
        subscription_status = 'IGNORED',
        subscription_sources_json = {_APPEND_SOURCE},
        ignore_reason = EXCLUDED.ignore_reason,
        parent_series_tmdb_id = COALESCE(EXCLUDED.parent_series_tmdb_id, media_metadata.parent_series_tmdb_id),
        last_updated_at = NOW()
    WHERE CAST(:source_json AS jsonb) = '[]'::jsonb
       OR NOT (media_metadata.subscription_sources_json @> CAST(:source_json AS jsonb))
       OR media_metadata.subscription_status <> 'IGNORED'
"""


def _jsonable(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value


__all__ = ["SqlAlchemySubscriptionMediaProjection", "SubscriptionMediaKey"]
