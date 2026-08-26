from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork


class SqlAlchemyCleanupMediaProjection:
    """P5-owned read projection for pending cleanup rows and media metadata."""

    def list_pending_tasks(self, uow: SqlAlchemyUnitOfWork) -> tuple[Mapping[str, Any], ...]:
        rows = (
            uow.session.execute(
                text(
                    """
                    SELECT
                        t.id,
                        t.tmdb_id,
                        t.item_type,
                        t.versions_info_json,
                        t.best_version_json,
                        m.title AS item_name,
                        m.season_number,
                        m.episode_number,
                        m.parent_series_tmdb_id,
                        parent.title AS parent_series_name
                    FROM cleanup_index AS t
                    JOIN media_metadata AS m
                      ON t.tmdb_id = m.tmdb_id
                     AND t.item_type = m.item_type
                    LEFT JOIN media_metadata AS parent
                      ON m.parent_series_tmdb_id = parent.tmdb_id
                     AND parent.item_type = 'Series'
                    WHERE t.status = 'pending'
                    ORDER BY
                        parent.title,
                        m.season_number,
                        m.episode_number,
                        m.title
                    """
                )
            )
            .mappings()
            .all()
        )
        return tuple(dict(row) for row in rows)

    def version_deletion_snapshot(
        self,
        uow: SqlAlchemyUnitOfWork,
        emby_id: str,
    ) -> Mapping[str, Any] | None:
        row = (
            uow.session.execute(
                text(
                    """
                    SELECT tmdb_id, item_type, title, asset_details_json, file_pickcode_json
                    FROM media_metadata
                    WHERE jsonb_typeof(asset_details_json) = 'array'
                      AND EXISTS (
                          SELECT 1
                          FROM jsonb_array_elements(asset_details_json) AS asset(value)
                          WHERE COALESCE(asset.value->>'emby_item_id', asset.value->>'id') = :emby_id
                      )
                    ORDER BY tmdb_id, item_type
                    LIMIT 1
                    FOR UPDATE
                    """
                ),
                {"emby_id": emby_id},
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        return {
            "tmdb_id": str(row["tmdb_id"]),
            "item_type": str(row["item_type"]),
            "title": row["title"],
            "asset_details": row["asset_details_json"],
            "file_pickcodes": row["file_pickcode_json"],
        }


__all__ = ["SqlAlchemyCleanupMediaProjection"]
