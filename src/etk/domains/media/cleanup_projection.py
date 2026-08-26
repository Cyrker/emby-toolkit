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


__all__ = ["SqlAlchemyCleanupMediaProjection"]
