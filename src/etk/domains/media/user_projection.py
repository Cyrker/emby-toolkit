from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from typing import Any

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork


class SqlAlchemyUserMediaProjection:
    """P5 media owner projection for P8 portal and local media-search reads."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def request_history(
        self,
        user_id: str,
        *,
        page: int,
        page_size: int,
        status: str,
    ) -> tuple[list[dict[str, Any]], int]:
        source_filter = json.dumps([{"type": "user_request", "user_id": user_id}], ensure_ascii=False)
        condition = {
            "completed": "metadata.in_library = TRUE",
            "pending": "metadata.in_library = FALSE AND metadata.subscription_status = 'REQUESTED'",
            "processing": (
                "metadata.in_library = FALSE AND metadata.subscription_status "
                "IN ('WANTED', 'SUBSCRIBED', 'PAUSED', 'PENDING_RELEASE')"
            ),
            "failed": ("metadata.in_library = FALSE AND metadata.subscription_status IN ('IGNORED', 'NONE')"),
        }.get(status)
        filters = ["metadata.subscription_sources_json @> CAST(:source_filter AS jsonb)"]
        if condition is not None:
            filters.append(condition)
        where_sql = " AND ".join(filters)
        parameters = {"source_filter": source_filter}
        with self._unit_of_work() as uow:
            total = int(
                uow.session.scalar(
                    text(f"SELECT COUNT(*) FROM media_metadata AS metadata WHERE {where_sql}"),
                    parameters,
                )
                or 0
            )
            rows = uow.session.execute(
                text(
                    f"""
                    SELECT metadata.tmdb_id, metadata.item_type,
                           CASE
                               WHEN metadata.item_type = 'Season' AND parent.title IS NOT NULL
                                   THEN parent.title || ' 第 ' || metadata.season_number || ' 季'
                               ELSE metadata.title
                           END AS title,
                           metadata.subscription_status AS status,
                           metadata.in_library,
                           metadata.first_requested_at AS requested_at,
                           metadata.ignore_reason AS notes
                    FROM media_metadata AS metadata
                    LEFT JOIN media_metadata AS parent
                      ON metadata.parent_series_tmdb_id = parent.tmdb_id
                     AND parent.item_type = 'Series'
                    WHERE {where_sql}
                    ORDER BY metadata.first_requested_at DESC
                    LIMIT :limit OFFSET :offset
                    """
                ),
                {
                    **parameters,
                    "limit": page_size,
                    "offset": (page - 1) * page_size,
                },
            ).mappings()
            items = [dict(row) for row in rows]
        for item in items:
            if item.get("in_library"):
                item["status"] = "completed"
        return items, total

    def request_stats(self, user_id: str) -> dict[str, int]:
        source_filter = json.dumps([{"type": "user_request", "user_id": user_id}], ensure_ascii=False)
        stats = {"total": 0, "completed": 0, "processing": 0, "pending": 0, "failed": 0}
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    """
                    SELECT in_library, subscription_status, COUNT(*) AS count
                    FROM media_metadata
                    WHERE subscription_sources_json @> CAST(:source_filter AS jsonb)
                    GROUP BY in_library, subscription_status
                    """
                ),
                {"source_filter": source_filter},
            ).mappings()
            for row in rows:
                count = int(row["count"] or 0)
                stats["total"] += count
                if row["in_library"]:
                    stats["completed"] += count
                elif row["subscription_status"] == "REQUESTED":
                    stats["pending"] += count
                elif row["subscription_status"] in {"WANTED", "SUBSCRIBED", "PENDING_RELEASE", "PAUSED"}:
                    stats["processing"] += count
                elif row["subscription_status"] in {"IGNORED", "NONE"}:
                    stats["failed"] += count
        return stats

    def dashboard_aggregation(self, emby_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
        unique_ids = tuple(dict.fromkeys(str(value).strip() for value in emby_ids if str(value).strip()))
        if not unique_ids:
            return {}
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    """
                    WITH input_ids AS (
                        SELECT unnest(CAST(:emby_ids AS text[])) AS emby_id
                    )
                    SELECT input.emby_id AS input_emby_id,
                           CASE WHEN media.item_type = 'Episode'
                               THEN COALESCE(parent.title, media.title)
                               ELSE media.title END AS title,
                           CASE WHEN media.item_type = 'Episode'
                               THEN parent.poster_path ELSE media.poster_path END AS poster_path,
                           CASE WHEN media.item_type = 'Episode'
                               THEN COALESCE(parent.tmdb_id, media.tmdb_id)
                               ELSE media.tmdb_id END AS tmdb_id,
                           CASE WHEN media.item_type = 'Episode'
                               THEN 'Series' ELSE 'Movie' END AS target_type,
                           CASE WHEN media.item_type = 'Episode'
                               THEN parent.emby_item_ids_json ->> 0
                               ELSE media.emby_item_ids_json ->> 0 END AS target_emby_id
                    FROM input_ids AS input
                    JOIN media_metadata AS media
                      ON media.emby_item_ids_json @> to_jsonb(input.emby_id)
                    LEFT JOIN media_metadata AS parent
                      ON media.parent_series_tmdb_id = parent.tmdb_id
                     AND parent.item_type = 'Series'
                    WHERE media.item_type IN ('Movie','Episode')
                    """
                ),
                {"emby_ids": list(unique_ids)},
            ).mappings()
            result: dict[str, dict[str, Any]] = {}
            for row in rows:
                if not row["tmdb_id"]:
                    continue
                result[str(row["input_emby_id"])] = {
                    "id": row["tmdb_id"],
                    "name": row["title"],
                    "poster_path": row["poster_path"],
                    "type": row["target_type"],
                    "emby_id": row["target_emby_id"],
                }
            return result

    def search_library(self, query: str) -> dict[str, Any]:
        search_term = f"%{query}%"
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    """
                    SELECT metadata.tmdb_id, metadata.item_type, metadata.title,
                           metadata.release_year, item_ids.emby_id,
                           asset.value ->> 'resolution_display' AS resolution,
                           asset.value ->> 'quality_display' AS quality
                    FROM media_metadata AS metadata
                    JOIN LATERAL jsonb_array_elements_text(
                        CASE
                            WHEN jsonb_typeof(metadata.emby_item_ids_json) = 'array'
                                THEN metadata.emby_item_ids_json
                            ELSE '[]'::jsonb
                        END
                    ) WITH ORDINALITY AS item_ids(emby_id, ordinal) ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT values.value
                        FROM jsonb_array_elements(
                            CASE
                                WHEN jsonb_typeof(metadata.asset_details_json) = 'array'
                                    THEN metadata.asset_details_json
                                ELSE '[]'::jsonb
                            END
                        ) WITH ORDINALITY AS values(value, ordinal)
                        WHERE values.ordinal = item_ids.ordinal
                    ) AS asset ON TRUE
                    WHERE metadata.title ILIKE :search_term
                       OR metadata.original_title ILIKE :search_term
                    LIMIT 50
                    """
                ),
                {"search_term": search_term},
            ).mappings()
            values = [dict(row) for row in rows]
        items = []
        for row in values:
            if not row.get("emby_id"):
                continue
            version = f" [{row['resolution']} {row['quality']}]" if row.get("resolution") else ""
            items.append(
                {
                    "item_id": row["emby_id"],
                    "item_name": f"{row.get('title') or ''}{version}",
                    "item_type": row.get("item_type"),
                    "failed_at": None,
                    "error_message": f"本地数据库搜索结果 (年份: {row.get('release_year')})",
                    "score": None,
                    "provider_ids": {"Tmdb": row.get("tmdb_id")},
                }
            )
        return {"items": items, "total_items": len(items)}

    def search_studios(self, query: str, *, limit: int = 20) -> list[str]:
        with self._unit_of_work() as uow:
            rows = uow.session.scalars(
                text(
                    """
                    WITH studio_values AS (
                        SELECT jsonb_array_elements(
                            CASE
                                WHEN jsonb_typeof(production_companies_json) = 'array'
                                    THEN production_companies_json
                                ELSE '[]'::jsonb
                            END
                        ) AS value
                        FROM media_metadata
                        WHERE in_library IS TRUE
                        UNION ALL
                        SELECT jsonb_array_elements(
                            CASE
                                WHEN jsonb_typeof(networks_json) = 'array'
                                    THEN networks_json
                                ELSE '[]'::jsonb
                            END
                        ) AS value
                        FROM media_metadata
                        WHERE in_library IS TRUE
                    )
                    SELECT DISTINCT BTRIM(
                        CASE
                            WHEN jsonb_typeof(value) = 'string' THEN value #>> '{}'
                            WHEN jsonb_typeof(value) = 'object' THEN value ->> 'name'
                            ELSE NULL
                        END
                    ) AS name
                    FROM studio_values
                    """
                )
            ).all()
        normalized = sorted({str(value).strip() for value in rows if str(value or "").strip()})
        search = query.casefold()
        starts = [value for value in normalized if value.casefold().startswith(search)]
        contains = [
            value for value in normalized if not value.casefold().startswith(search) and search in value.casefold()
        ]
        return (starts + contains)[: max(0, int(limit))]


__all__ = ["SqlAlchemyUserMediaProjection"]
