from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from sqlalchemy import text

from etk.domains.media.subscription_projection import (
    SqlAlchemySubscriptionMediaProjection,
    SubscriptionMediaKey,
)
from etk.platform.db import SqlAlchemyUnitOfWork


class SqlAlchemyAutoSubscriptionRepository:
    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        media: SqlAlchemySubscriptionMediaProjection,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._media = media

    def promote_pending_to_wanted(self) -> int:
        with self._unit_of_work() as uow:
            result = uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET subscription_status = 'WANTED',
                        last_synced_at = NOW(),
                        last_updated_at = NOW()
                    WHERE subscription_status = 'PENDING_RELEASE'
                      AND release_date <= NOW()
                    """
                )
            )
            count = int(getattr(result, "rowcount", 0) or 0)
            uow.commit()
            return count

    def movies_to_revive(self) -> Sequence[Mapping[str, Any]]:
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, title
                        FROM media_metadata
                        WHERE item_type = 'Movie'
                          AND subscription_status = 'PAUSED'
                          AND paused_until <= NOW()
                        ORDER BY paused_until, tmdb_id
                        """
                    )
                )
                .mappings()
                .all()
            )
            return tuple(dict(row) for row in rows)

    def movies_to_pause(self, *, search_window_days: int) -> Sequence[Mapping[str, Any]]:
        days = max(int(search_window_days), 1)
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, title
                        FROM media_metadata
                        WHERE item_type = 'Movie'
                          AND subscription_status = 'SUBSCRIBED'
                          AND in_library = FALSE
                          AND last_subscribed_at < NOW() - make_interval(days => :days)
                        ORDER BY last_subscribed_at, tmdb_id
                        """
                    ),
                    {"days": days},
                )
                .mappings()
                .all()
            )
            return tuple(dict(row) for row in rows)

    def mark_movie_revived(self, tmdb_id: str) -> None:
        with self._unit_of_work() as uow:
            uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET subscription_status = 'SUBSCRIBED',
                        last_subscribed_at = NOW(),
                        paused_until = NULL,
                        last_updated_at = NOW()
                    WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'
                    """
                ),
                {"tmdb_id": tmdb_id},
            )
            uow.commit()

    def mark_movie_paused(self, tmdb_id: str, *, pause_days: int) -> None:
        days = max(int(pause_days), 1)
        with self._unit_of_work() as uow:
            uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET subscription_status = 'PAUSED',
                        paused_until = NOW() + make_interval(days => :days)
                            + (random() * INTERVAL '2 days'),
                        last_updated_at = NOW()
                    WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'
                    """
                ),
                {"tmdb_id": tmdb_id, "days": days},
            )
            uow.commit()

    def worklist(self) -> Sequence[Mapping[str, Any]]:
        with self._unit_of_work() as uow:
            rows = uow.session.execute(text(_WORKLIST_SQL)).mappings().all()
            return tuple(dict(row) for row in rows)

    def series_title(self, tmdb_id: str) -> str | None:
        if not str(tmdb_id or "").strip():
            return None
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text(
                    """
                    SELECT title
                    FROM media_metadata
                    WHERE tmdb_id = :tmdb_id AND item_type = 'Series'
                    LIMIT 1
                    """
                ),
                {"tmdb_id": str(tmdb_id)},
            )
            return str(value) if value else None

    def mark_subscribed(self, item: Mapping[str, Any]) -> None:
        key = SubscriptionMediaKey.from_mapping(item)
        with self._unit_of_work() as uow:
            self._media.set_status(uow, key, "SUBSCRIBED", item, None, None)
            uow.commit()


_WORKLIST_SQL = """
WITH target_media AS (
    SELECT
        tmdb_id,
        item_type,
        title,
        release_date,
        release_year,
        poster_path,
        backdrop_path,
        overview,
        parent_series_tmdb_id,
        season_number,
        NULL::integer AS episode_number,
        subscription_status,
        watching_status,
        subscription_sources_json,
        in_library,
        first_requested_at,
        last_subscribed_at
    FROM media_metadata
    WHERE (item_type = 'Movie' AND subscription_status IN ('WANTED', 'SUBSCRIBED'))
       OR (item_type = 'Season' AND subscription_status IN ('WANTED', 'SUBSCRIBED'))
)
SELECT
    target.tmdb_id,
    target.item_type,
    target.title,
    target.release_date,
    target.release_year,
    target.poster_path,
    target.backdrop_path,
    target.overview,
    target.parent_series_tmdb_id,
    target.season_number,
    target.episode_number,
    target.subscription_status,
    target.watching_status,
    target.subscription_sources_json,
    target.first_requested_at,
    target.last_subscribed_at,
    target.in_library,
    COALESCE(
        jsonb_agg(episode.episode_number ORDER BY episode.episode_number)
            FILTER (WHERE episode.episode_number IS NOT NULL),
        '[]'::jsonb
    ) AS missing_episode_numbers,
    COUNT(episode.episode_number)::integer AS missing_episode_count
FROM target_media AS target
LEFT JOIN media_metadata AS episode
  ON target.item_type = 'Season'
 AND episode.item_type = 'Episode'
 AND episode.parent_series_tmdb_id = COALESCE(target.parent_series_tmdb_id, target.tmdb_id)
 AND COALESCE(episode.season_number, -1) = COALESCE(target.season_number, -1)
 AND COALESCE(episode.in_library, FALSE) = FALSE
 AND episode.episode_number IS NOT NULL
 AND (episode.release_date IS NULL OR episode.release_date <= CURRENT_DATE)
GROUP BY
    target.tmdb_id, target.item_type, target.title, target.release_date, target.release_year,
    target.poster_path, target.backdrop_path, target.overview, target.parent_series_tmdb_id,
    target.season_number, target.episode_number, target.subscription_status,
    target.watching_status, target.subscription_sources_json, target.first_requested_at,
    target.last_subscribed_at, target.in_library
HAVING target.subscription_status = 'WANTED'
    OR (target.item_type = 'Movie' AND COALESCE(target.in_library, FALSE) = FALSE)
    OR (
        target.item_type = 'Season'
        AND (COUNT(episode.episode_number) > 0 OR COALESCE(target.in_library, FALSE) = FALSE)
    )
ORDER BY
    CASE
        WHEN target.subscription_status = 'WANTED' THEN 0
        WHEN target.item_type = 'Season' AND target.subscription_status = 'SUBSCRIBED' THEN 0
        ELSE 1
    END,
    COALESCE(target.first_requested_at, target.last_subscribed_at, NOW()) ASC
"""


__all__ = ["SqlAlchemyAutoSubscriptionRepository"]
