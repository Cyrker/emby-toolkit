from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, RootModel
from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause

from etk.platform.db import SqlAlchemyUnitOfWork

_MEDIA_TYPES = {"movie": "Movie", "series": "Series", "episode": "Episode"}
_REVIEW_FILTER_FIELDS = frozenset({"query", "reason"})


class DatabaseStatusResponse(BaseModel):
    status: str
    data: dict[str, Any] | list[Any]


class DatabaseTableListResponse(RootModel[list[str]]):
    pass


class ReviewItemResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    item_id: str | int
    item_name: str
    failed_at: Any = None
    reason: str | None = None
    item_type: str | None = None
    score: Any = None


class ReviewItemsResponse(BaseModel):
    items: list[ReviewItemResponse]
    total_items: int
    total_pages: int
    current_page: int
    per_page: int
    query: str
    reason: str


class DatabaseReadRepository(Protocol):
    def core_stats(self) -> Mapping[str, Any]: ...

    def library_stats(self) -> Mapping[str, Any]: ...

    def system_stats(self) -> Mapping[str, Any]: ...

    def release_group_rankings(self, limit: int) -> list[dict[str, Any]]: ...

    def historical_release_group_rankings(self, limit: int) -> list[dict[str, Any]]: ...

    def table_names(self) -> list[str]: ...

    def review_items(
        self,
        *,
        page: int,
        per_page: int,
        query: str,
        reason: str,
    ) -> tuple[list[dict[str, Any]], int]: ...

    def review_reasons(self) -> list[str]: ...


class SqlAlchemyDatabaseReadRepository:
    """PostgreSQL read model for P3 database dashboards and review queries."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        *,
        timezone: str = "Asia/Shanghai",
    ) -> None:
        self._unit_of_work = unit_of_work
        self._timezone = str(timezone or "Asia/Shanghai")

    def core_stats(self) -> Mapping[str, Any]:
        statement = text(
            """
            SELECT
                (
                    SELECT COUNT(*) FROM media_metadata
                    WHERE item_type IN (:movie_type, :series_type)
                ) AS media_cached_total,
                (SELECT COUNT(*) FROM p115_mediainfo_cache) AS mediainfo_backed_up_total,
                (
                    SELECT COALESCE(SUM(hit_count), 0) FROM p115_mediainfo_cache
                ) AS mediainfo_hits_total
            """
        )
        return self._single_row(
            statement,
            {
                "movie_type": _MEDIA_TYPES["movie"],
                "series_type": _MEDIA_TYPES["series"],
            },
        )

    def library_stats(self) -> Mapping[str, Any]:
        counts = self._single_row(
            text(
                """
                SELECT
                    (
                        SELECT COUNT(*) FROM media_metadata
                        WHERE item_type = :movie_type AND in_library = TRUE
                    ) AS media_movies_in_library,
                    (
                        SELECT COUNT(*) FROM media_metadata
                        WHERE item_type = :series_type AND in_library = TRUE
                    ) AS media_series_in_library,
                    (
                        SELECT COUNT(*) FROM media_metadata
                        WHERE item_type = :episode_type AND in_library = TRUE
                    ) AS media_episodes_in_library
                """
            ),
            {
                "movie_type": _MEDIA_TYPES["movie"],
                "series_type": _MEDIA_TYPES["series"],
                "episode_type": _MEDIA_TYPES["episode"],
            },
        )
        data = dict(counts)
        data["resolution_stats"] = self._all_rows(
            text(
                """
                SELECT
                    jsonb_array_elements(asset_details_json) ->> 'resolution_display' AS resolution,
                    COUNT(*) AS count
                FROM media_metadata
                WHERE
                    in_library = TRUE
                    AND item_type IN (:movie_type, :episode_type)
                    AND asset_details_json IS NOT NULL
                    AND jsonb_array_length(asset_details_json) > 0
                GROUP BY resolution
                ORDER BY count DESC
                """
            ),
            {
                "movie_type": _MEDIA_TYPES["movie"],
                "episode_type": _MEDIA_TYPES["episode"],
            },
        )
        data["weekly_added_stats"] = self._weekly_library_additions(days=7)
        return data

    def system_stats(self) -> Mapping[str, Any]:
        return self._single_row(
            text(
                """
                SELECT
                    (SELECT COUNT(*) FROM translation_cache) AS translation_cache_count,
                    (SELECT COUNT(*) FROM processed_log) AS processed_log_count,
                    (SELECT COUNT(*) FROM failed_log) AS failed_log_count
                """
            )
        )

    def release_group_rankings(self, limit: int) -> list[dict[str, Any]]:
        return self._all_rows(
            text(
                """
                SELECT release_group, COUNT(*) AS count
                FROM (
                    SELECT
                        jsonb_array_elements_text(asset -> 'release_group_raw') AS release_group,
                        (
                            (asset ->> 'date_added_to_library')::timestamp AT TIME ZONE 'UTC'
                        ) AS asset_added_at_utc
                    FROM (
                        SELECT jsonb_array_elements(asset_details_json) AS asset
                        FROM media_metadata
                        WHERE
                            in_library = TRUE
                            AND asset_details_json IS NOT NULL
                            AND jsonb_array_length(asset_details_json) > 0
                            AND asset_details_json::text LIKE :like_pattern
                    ) AS assets
                ) AS release_groups
                WHERE
                    release_group IS NOT NULL
                    AND release_group != ''
                    AND (asset_added_at_utc AT TIME ZONE :timezone)::date =
                        (NOW() AT TIME ZONE :timezone)::date
                GROUP BY release_group
                ORDER BY count DESC
                LIMIT :limit
                """
            ),
            {
                "like_pattern": "%date_added_to_library%",
                "timezone": self._timezone,
                "limit": limit,
            },
        )

    def historical_release_group_rankings(self, limit: int) -> list[dict[str, Any]]:
        return self._all_rows(
            text(
                """
                SELECT release_group, COUNT(*) AS count
                FROM (
                    SELECT jsonb_array_elements_text(asset -> 'release_group_raw') AS release_group
                    FROM (
                        SELECT jsonb_array_elements(asset_details_json) AS asset
                        FROM media_metadata
                        WHERE
                            in_library = TRUE
                            AND asset_details_json IS NOT NULL
                            AND jsonb_array_length(asset_details_json) > 0
                            AND asset_details_json::text LIKE :like_pattern
                    ) AS assets
                ) AS release_groups
                WHERE release_group IS NOT NULL AND release_group != ''
                GROUP BY release_group
                ORDER BY count DESC
                LIMIT :limit
                """
            ),
            {"like_pattern": "%date_added_to_library%", "limit": limit},
        )

    def table_names(self) -> list[str]:
        values: list[str] = []
        with self._unit_of_work() as uow:
            result = uow.session.scalars(
                text(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = :schema AND table_type = :table_type
                    ORDER BY table_name
                    """
                ),
                {"schema": "public", "table_type": "BASE TABLE"},
            )
            values = [str(value) for value in result]
        return values

    def review_items(
        self,
        *,
        page: int,
        per_page: int,
        query: str,
        reason: str,
    ) -> tuple[list[dict[str, Any]], int]:
        filters = {"query": query, "reason": reason}
        if not set(filters).issubset(_REVIEW_FILTER_FIELDS):
            raise ValueError("unsupported review filter")

        query_pattern = f"%{query}%" if query else None
        reason_pattern = f"{reason}%" if reason else None
        parameters: dict[str, Any] = {
            "query_pattern": query_pattern,
            "reason_pattern": reason_pattern,
        }
        offset = (page - 1) * per_page

        with self._unit_of_work() as uow:
            total_value = uow.session.scalar(
                text(
                    """
                    SELECT COUNT(*) AS total
                    FROM failed_log
                    WHERE
                        (
                            CAST(:query_pattern AS TEXT) IS NULL
                            OR item_name ILIKE CAST(:query_pattern AS TEXT)
                        )
                        AND (
                            CAST(:reason_pattern AS TEXT) IS NULL
                            OR reason LIKE CAST(:reason_pattern AS TEXT)
                        )
                    """
                ),
                parameters,
            )
            items = uow.session.execute(
                text(
                    """
                    SELECT item_id, item_name, failed_at, reason, item_type, score
                    FROM failed_log
                    WHERE
                        (
                            CAST(:query_pattern AS TEXT) IS NULL
                            OR item_name ILIKE CAST(:query_pattern AS TEXT)
                        )
                        AND (
                            CAST(:reason_pattern AS TEXT) IS NULL
                            OR reason LIKE CAST(:reason_pattern AS TEXT)
                        )
                    ORDER BY failed_at DESC
                    LIMIT :limit OFFSET :offset
                    """
                ),
                {**parameters, "limit": per_page, "offset": offset},
            )
            rows = [dict(row) for row in items.mappings().all()]
        return rows, int(total_value or 0)

    def review_reasons(self) -> list[str]:
        with self._unit_of_work() as uow:
            raw_reasons = uow.session.scalars(
                text("SELECT DISTINCT reason FROM failed_log WHERE reason IS NOT NULL")
            ).all()

        reasons: set[str] = set()
        for value in raw_reasons:
            if not value:
                continue
            reason = str(value).strip()
            if reason.startswith("缺失媒体信息"):
                reasons.add("缺失媒体信息")
            elif reason.startswith("处理评分"):
                reasons.add("处理评分")
            else:
                reasons.add(reason)
        return sorted(reasons)

    def _weekly_library_additions(self, *, days: int) -> list[dict[str, Any]]:
        bounded_days = max(1, min(int(days or 7), 31))
        return self._all_rows(
            text(
                """
                WITH date_range AS (
                    SELECT generate_series(
                        (NOW() AT TIME ZONE :timezone)::date - (CAST(:days AS INTEGER) - 1),
                        (NOW() AT TIME ZONE :timezone)::date,
                        interval '1 day'
                    )::date AS stat_date
                ),
                raw_assets AS (
                    SELECT
                        metadata.item_type,
                        asset ->> 'date_added_to_library' AS added_text
                    FROM media_metadata AS metadata
                    CROSS JOIN LATERAL jsonb_array_elements(metadata.asset_details_json) AS asset
                    WHERE
                        metadata.in_library = TRUE
                        AND metadata.item_type IN (:movie_type, :episode_type)
                        AND metadata.asset_details_json IS NOT NULL
                        AND jsonb_typeof(metadata.asset_details_json) = 'array'
                        AND jsonb_array_length(metadata.asset_details_json) > 0
                        AND metadata.asset_details_json::text LIKE :like_pattern
                        AND asset ? 'date_added_to_library'
                        AND NULLIF(asset ->> 'date_added_to_library', '') IS NOT NULL
                ),
                asset_rows AS (
                    SELECT
                        (
                            CASE
                                WHEN added_text ~ '(Z|[+-][0-9]{2}:?[0-9]{2})$'
                                    THEN added_text::timestamptz
                                ELSE added_text::timestamp AT TIME ZONE 'UTC'
                            END
                            AT TIME ZONE :timezone
                        )::date AS stat_date,
                        item_type
                    FROM raw_assets
                    WHERE added_text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                )
                SELECT
                    dates.stat_date::text AS date,
                    to_char(dates.stat_date, 'MM-DD') AS date_label,
                    COALESCE(COUNT(assets.stat_date), 0)::int AS count,
                    COALESCE(
                        COUNT(assets.stat_date) FILTER (WHERE assets.item_type = :movie_type), 0
                    )::int AS movies,
                    COALESCE(
                        COUNT(assets.stat_date) FILTER (WHERE assets.item_type = :episode_type), 0
                    )::int AS episodes
                FROM date_range AS dates
                LEFT JOIN asset_rows AS assets ON assets.stat_date = dates.stat_date
                GROUP BY dates.stat_date
                ORDER BY dates.stat_date ASC
                """
            ),
            {
                "timezone": self._timezone,
                "days": bounded_days,
                "movie_type": _MEDIA_TYPES["movie"],
                "episode_type": _MEDIA_TYPES["episode"],
                "like_pattern": "%date_added_to_library%",
            },
        )

    def _single_row(
        self,
        statement: TextClause,
        parameters: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        with self._unit_of_work() as uow:
            row = uow.session.execute(statement, dict(parameters or {})).mappings().first()
            result = dict(row) if row is not None else {}
        return result

    def _all_rows(
        self,
        statement: TextClause,
        parameters: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        with self._unit_of_work() as uow:
            rows = uow.session.execute(statement, dict(parameters or {})).mappings().all()
            result = [dict(row) for row in rows]
        return result


class DatabaseReadService:
    """Stable response-facing read model over an injectable database repository."""

    def __init__(self, repository: DatabaseReadRepository) -> None:
        self._repository = repository

    def core_stats(self) -> dict[str, Any]:
        return dict(self._repository.core_stats())

    def library_stats(self) -> dict[str, Any]:
        raw = self._repository.library_stats()
        return {
            "movies_in_library": raw.get("media_movies_in_library", 0),
            "series_in_library": raw.get("media_series_in_library", 0),
            "episodes_in_library": raw.get("media_episodes_in_library", 0),
            "resolution_stats": raw.get("resolution_stats", []),
            "weekly_added_stats": raw.get("weekly_added_stats", []),
        }

    def system_stats(self) -> dict[str, Any]:
        raw = self._repository.system_stats()
        return {
            "translation_cache_count": raw.get("translation_cache_count", 0),
            "processed_log_count": raw.get("processed_log_count", 0),
            "failed_log_count": raw.get("failed_log_count", 0),
        }

    def rankings(self) -> dict[str, Any]:
        return {
            "release_group_ranking": self._repository.release_group_rankings(5),
            "historical_release_group_ranking": self._repository.historical_release_group_rankings(5),
        }

    def table_names(self) -> list[str]:
        return self._repository.table_names()

    def review_items(
        self,
        *,
        page: int,
        per_page: int,
        query: str,
        reason: str,
    ) -> dict[str, Any]:
        items, total = self._repository.review_items(
            page=page,
            per_page=per_page,
            query=query,
            reason=reason,
        )
        total_pages = (total + per_page - 1) // per_page if total > 0 else 0
        return {
            "items": items,
            "total_items": total,
            "total_pages": total_pages,
            "current_page": page,
            "per_page": per_page,
            "query": query,
            "reason": reason,
        }

    def review_reasons(self) -> list[str]:
        return self._repository.review_reasons()
