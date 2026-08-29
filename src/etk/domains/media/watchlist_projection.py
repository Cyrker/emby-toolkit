from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork

from .subscription_projection import SqlAlchemySubscriptionMediaProjection, SubscriptionMediaKey


class SqlAlchemyWatchlistMediaProjectionRepository:
    """P5-owned PostgreSQL projection used by the watchlist use case."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        subscription_projection: SqlAlchemySubscriptionMediaProjection | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._subscription_projection = subscription_projection or SqlAlchemySubscriptionMediaProjection()

    def list_missing_old_seasons(self) -> tuple[Mapping[str, Any], ...]:
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        WITH series_max_season AS (
                            SELECT parent_series_tmdb_id, MAX(season_number) AS max_seq
                            FROM media_metadata
                            WHERE item_type = 'Season'
                              AND season_number > 0
                              AND in_library = TRUE
                            GROUP BY parent_series_tmdb_id
                        )
                        SELECT
                            season.tmdb_id,
                            season.item_type,
                            season.title,
                            season.original_title,
                            season.season_number,
                            season.parent_series_tmdb_id,
                            season.release_date,
                            season.poster_path,
                            season.overview,
                            parent.title AS series_title
                        FROM media_metadata AS season
                        JOIN series_max_season AS maximum
                          ON season.parent_series_tmdb_id = maximum.parent_series_tmdb_id
                        LEFT JOIN media_metadata AS parent
                          ON season.parent_series_tmdb_id = parent.tmdb_id
                         AND parent.item_type = 'Series'
                        WHERE season.item_type = 'Season'
                          AND season.season_number > 0
                          AND season.in_library = FALSE
                          AND season.season_number < maximum.max_seq
                          AND season.subscription_status != 'IGNORED'
                          AND parent.watching_status IS NOT NULL
                          AND parent.watching_status != 'NONE'
                        """
                    )
                )
                .mappings()
                .all()
            )
        return tuple(dict(row) for row in rows)

    def mark_old_seasons_wanted(
        self,
        seasons: Sequence[Mapping[str, Any]],
        *,
        source: Mapping[str, Any],
        checkpoint: Callable[[], None],
    ) -> int:
        if not seasons:
            return 0
        with self._unit_of_work() as uow:
            affected = 0
            for index, season in enumerate(seasons):
                if index % 50 == 0:
                    checkpoint()
                affected += self._subscription_projection.set_status(
                    uow,
                    SubscriptionMediaKey(str(season["tmdb_id"]), "Season"),
                    "WANTED",
                    season,
                    source,
                    None,
                )
            checkpoint()
            uow.commit()
        return affected

    def list_watchlist_items(self) -> tuple[Mapping[str, Any], ...]:
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        WITH active_series AS (
                            SELECT
                                tmdb_id,
                                title,
                                watching_status,
                                watchlist_last_checked_at,
                                watchlist_next_episode_json,
                                watchlist_missing_info_json,
                                emby_item_ids_json,
                                watchlist_tmdb_status,
                                total_episodes,
                                first_requested_at
                            FROM media_metadata
                            WHERE item_type = 'Series'
                              AND watching_status != 'NONE'
                        ),
                        episode_stats AS (
                            SELECT
                                parent_series_tmdb_id,
                                season_number,
                                COUNT(*) FILTER (WHERE in_library = TRUE) AS collected_count,
                                COUNT(*) AS total_count
                            FROM media_metadata
                            WHERE item_type = 'Episode'
                              AND parent_series_tmdb_id IN (
                                  SELECT tmdb_id FROM active_series
                              )
                            GROUP BY parent_series_tmdb_id, season_number
                        ),
                        series_stats AS (
                            SELECT
                                parent_series_tmdb_id,
                                COUNT(*) FILTER (
                                    WHERE in_library = TRUE
                                ) AS series_collected_count
                            FROM media_metadata
                            WHERE item_type = 'Episode'
                              AND parent_series_tmdb_id IN (
                                  SELECT tmdb_id FROM active_series
                              )
                            GROUP BY parent_series_tmdb_id
                        ),
                        latest_seasons AS (
                            SELECT
                                parent_series_tmdb_id,
                                MAX(season_number) AS max_season_number
                            FROM media_metadata
                            WHERE item_type = 'Season'
                              AND parent_series_tmdb_id IN (
                                  SELECT tmdb_id FROM active_series
                              )
                            GROUP BY parent_series_tmdb_id
                        )
                        SELECT
                            s.tmdb_id,
                            'Season' AS item_type,
                            p.title || ' 第 ' || s.season_number || ' 季' AS item_name,
                            s.season_number,
                            p.tmdb_id AS parent_tmdb_id,
                            s.release_date AS release_year,
                            s.watching_status AS status,
                            p.watching_status AS series_status,
                            COALESCE(s.force_ended, FALSE) AS force_ended,
                            p.watchlist_last_checked_at AS last_checked_at,
                            p.watchlist_next_episode_json AS next_episode_to_air_json,
                            p.watchlist_missing_info_json AS missing_info_json,
                            p.emby_item_ids_json,
                            p.watchlist_tmdb_status AS tmdb_status,
                            COALESCE(es.collected_count, 0) AS collected_count,
                            COALESCE(
                                NULLIF(s.total_episodes, 0),
                                COALESCE(es.total_count, 0)
                            ) AS total_count,
                            COALESCE(
                                ss.series_collected_count,
                                0
                            ) AS series_collected_count,
                            p.total_episodes AS series_total_episodes,
                            s.total_episodes_locked
                        FROM media_metadata AS s
                        JOIN active_series AS p
                          ON s.parent_series_tmdb_id = p.tmdb_id
                        LEFT JOIN episode_stats AS es
                          ON s.parent_series_tmdb_id = es.parent_series_tmdb_id
                         AND s.season_number = es.season_number
                        LEFT JOIN series_stats AS ss
                          ON s.parent_series_tmdb_id = ss.parent_series_tmdb_id
                        LEFT JOIN latest_seasons AS ls
                          ON s.parent_series_tmdb_id = ls.parent_series_tmdb_id
                        WHERE s.item_type = 'Season'
                          AND s.season_number > 0
                          AND s.watching_status != 'NONE'
                          AND COALESCE(es.collected_count, 0) > 0
                          AND (
                              s.watching_status != 'Completed'
                              OR s.in_library = TRUE
                              OR COALESCE(s.force_ended, FALSE) = TRUE
                          )
                          AND (
                              s.total_episodes = 0
                              OR COALESCE(es.collected_count, 0) < s.total_episodes
                              OR s.season_number = ls.max_season_number
                              OR p.watching_status IN ('Completed', 'Paused')
                              OR s.watching_status = 'Completed'
                          )
                        ORDER BY p.first_requested_at DESC, s.season_number ASC
                        """
                    )
                )
                .mappings()
                .all()
            )
        return tuple(dict(row) for row in rows)

    def add_watchlist_item(self, tmdb_id: str, item_name: str) -> None:
        with self._unit_of_work() as uow:
            uow.session.execute(
                text(
                    """
                    INSERT INTO media_metadata (
                        tmdb_id,
                        item_type,
                        title,
                        watching_status,
                        force_ended,
                        paused_until
                    )
                    VALUES (
                        :tmdb_id,
                        'Series',
                        :item_name,
                        'NONE',
                        FALSE,
                        NULL
                    )
                    ON CONFLICT (tmdb_id, item_type)
                    DO UPDATE SET
                        watching_status = 'NONE',
                        force_ended = FALSE,
                        paused_until = NULL
                    """
                ),
                {"tmdb_id": tmdb_id, "item_name": item_name},
            )
            uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET watching_status = 'NONE'
                    WHERE parent_series_tmdb_id = :tmdb_id
                      AND item_type = 'Episode'
                    """
                ),
                {"tmdb_id": tmdb_id},
            )
            uow.session.execute(
                text(
                    """
                    WITH local_counts AS (
                        SELECT
                            season_number,
                            COUNT(*) FILTER (
                                WHERE in_library = TRUE
                            ) AS local_count
                        FROM media_metadata
                        WHERE parent_series_tmdb_id = :tmdb_id
                          AND item_type = 'Episode'
                          AND season_number > 0
                        GROUP BY season_number
                    )
                    UPDATE media_metadata AS season
                    SET watching_status = CASE
                        WHEN COALESCE(season.force_ended, FALSE)
                        THEN 'Completed'
                        WHEN COALESCE(season.total_episodes, 0) > 0
                         AND COALESCE(
                             (
                                 SELECT local_count
                                 FROM local_counts
                                 WHERE local_counts.season_number = season.season_number
                             ),
                             0
                         ) >= season.total_episodes
                        THEN 'Completed'
                        ELSE 'Watching'
                    END
                    WHERE season.parent_series_tmdb_id = :tmdb_id
                      AND season.item_type = 'Season'
                      AND season.season_number > 0
                    """
                ),
                {"tmdb_id": tmdb_id},
            )
            uow.commit()

    def update_watchlist_status(self, tmdb_id: str, new_status: str) -> bool:
        reset_flags = new_status in {"Watching", "Pending"}
        with self._unit_of_work() as uow:
            updated = uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET watching_status = :new_status,
                        force_ended = CASE
                            WHEN :reset_flags THEN FALSE
                            ELSE force_ended
                        END,
                        paused_until = CASE
                            WHEN :reset_flags THEN NULL
                            ELSE paused_until
                        END,
                        watchlist_last_checked_at = NOW()
                    WHERE tmdb_id = :tmdb_id
                      AND item_type = 'Series'
                    """
                ),
                {
                    "tmdb_id": tmdb_id,
                    "new_status": new_status,
                    "reset_flags": reset_flags,
                },
            )
            updated_count = _rowcount(updated)
            uow.commit()
        return updated_count > 0

    def watchlist_delete_context(self, tmdb_id: str) -> Mapping[str, Any] | None:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT title, emby_item_ids_json
                        FROM media_metadata
                        WHERE tmdb_id = :tmdb_id
                          AND item_type = 'Series'
                        LIMIT 1
                        """
                    ),
                    {"tmdb_id": tmdb_id},
                )
                .mappings()
                .first()
            )
        if row is None:
            return None
        return {
            "tmdb_id": tmdb_id,
            "title": str(row.get("title") or tmdb_id),
            "emby_ids": _normalize_emby_ids(row.get("emby_item_ids_json")),
        }

    def remove_watchlist_item(self, tmdb_id: str) -> bool:
        with self._unit_of_work() as uow:
            removed = uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET watching_status = 'NONE',
                        paused_until = NULL,
                        force_ended = FALSE,
                        watchlist_last_checked_at = NULL,
                        watchlist_tmdb_status = NULL,
                        watchlist_next_episode_json = NULL,
                        watchlist_missing_info_json = NULL,
                        subscription_status = 'NONE',
                        subscription_sources_json = '[]'::jsonb,
                        ignore_reason = NULL
                    WHERE (
                        tmdb_id = :tmdb_id
                        AND item_type = 'Series'
                    )
                       OR parent_series_tmdb_id = :tmdb_id
                    """
                ),
                {"tmdb_id": tmdb_id},
            )
            removed_count = _rowcount(removed)
            uow.commit()
        return removed_count > 0

    def watchlist_item_name(self, tmdb_id: str) -> str | None:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT title
                        FROM media_metadata
                        WHERE tmdb_id = :tmdb_id
                          AND item_type = 'Series'
                        """
                    ),
                    {"tmdb_id": tmdb_id},
                )
                .mappings()
                .first()
            )
        if row is None:
            return None
        title = row.get("title")
        return str(title) if title is not None else None

    def force_end_watchlist_seasons(self, tmdb_ids: Sequence[Any]) -> int:
        normalized_ids = _string_ids(tmdb_ids)
        if not normalized_ids:
            return 0
        with self._unit_of_work() as uow:
            updated = uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET watching_status = 'Completed',
                        force_ended = TRUE,
                        paused_until = NULL,
                        watchlist_last_checked_at = NOW()
                    WHERE tmdb_id = ANY(CAST(:tmdb_ids AS text[]))
                      AND item_type = 'Season'
                      AND season_number > 0
                    """
                ),
                {"tmdb_ids": normalized_ids},
            )
            updated_count = _rowcount(updated)
            uow.commit()
        return updated_count

    def batch_update_watchlist_status(
        self,
        item_ids: Sequence[Any],
        new_status: str,
    ) -> int:
        normalized_ids = _string_ids(item_ids)
        if not normalized_ids:
            return 0
        reset_flags = new_status == "Watching"
        with self._unit_of_work() as uow:
            updated = uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET watching_status = :new_status,
                        force_ended = CASE
                            WHEN :reset_flags THEN FALSE
                            ELSE force_ended
                        END,
                        paused_until = CASE
                            WHEN :reset_flags THEN NULL
                            ELSE paused_until
                        END,
                        watchlist_last_checked_at = NOW()
                    WHERE tmdb_id = ANY(CAST(:item_ids AS text[]))
                      AND item_type = 'Series'
                    """
                ),
                {
                    "item_ids": normalized_ids,
                    "new_status": new_status,
                    "reset_flags": reset_flags,
                },
            )
            updated_count = _rowcount(updated)
            if reset_flags:
                updated_seasons = uow.session.execute(
                    text(
                        """
                        UPDATE media_metadata
                        SET watching_status = 'Watching',
                            force_ended = FALSE,
                            paused_until = NULL,
                            watchlist_last_checked_at = NOW()
                        WHERE parent_series_tmdb_id = ANY(
                            CAST(:item_ids AS text[])
                        )
                          AND item_type = 'Season'
                          AND season_number > 0
                          AND force_ended = TRUE
                        """
                    ),
                    {"item_ids": normalized_ids},
                )
                updated_count += _rowcount(updated_seasons)
            uow.commit()
        return updated_count

    def batch_remove_watchlist_items(self, tmdb_ids: Sequence[Any]) -> int:
        normalized_ids = _string_ids(tmdb_ids)
        if not normalized_ids:
            return 0
        with self._unit_of_work() as uow:
            removed = uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET watching_status = 'NONE',
                        paused_until = NULL,
                        force_ended = FALSE,
                        watchlist_last_checked_at = NULL,
                        watchlist_tmdb_status = NULL,
                        watchlist_next_episode_json = NULL,
                        watchlist_missing_info_json = NULL,
                        subscription_status = 'NONE',
                        subscription_sources_json = '[]'::jsonb,
                        ignore_reason = NULL
                    WHERE (
                        tmdb_id = ANY(CAST(:tmdb_ids AS text[]))
                        AND item_type = 'Series'
                    )
                       OR parent_series_tmdb_id = ANY(
                           CAST(:tmdb_ids AS text[])
                       )
                    """
                ),
                {"tmdb_ids": normalized_ids},
            )
            removed_count = _rowcount(removed)
            uow.commit()
        return removed_count

    def update_watchlist_total_episodes(
        self,
        tmdb_id: Any,
        total_episodes: Any,
        item_type: Any,
    ) -> None:
        with self._unit_of_work() as uow:
            uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET total_episodes = :total_episodes,
                        total_episodes_locked = TRUE
                    WHERE tmdb_id = :tmdb_id
                      AND item_type = :item_type
                    """
                ),
                {
                    "tmdb_id": tmdb_id,
                    "total_episodes": total_episodes,
                    "item_type": item_type,
                },
            )
            uow.commit()


def _normalize_emby_ids(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    normalized = str(value).strip()
    return [normalized] if normalized else []


def _string_ids(values: Sequence[Any]) -> list[str]:
    return [str(value) for value in values]


def _rowcount(result: Any) -> int:
    return max(int(getattr(result, "rowcount", 0) or 0), 0)


__all__ = ["SqlAlchemyWatchlistMediaProjectionRepository"]
