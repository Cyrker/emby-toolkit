from __future__ import annotations

import json
from collections.abc import Mapping
from copy import deepcopy
from typing import Any, cast

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork


class SqlAlchemyActorSubscriptionProjection:
    """Media-owned projection used inside the actor repository transaction."""

    def tracked_media(
        self,
        uow: SqlAlchemyUnitOfWork,
        subscription_id: int,
    ) -> list[dict[str, Any]]:
        source_filter = json.dumps([{"type": "actor_subscription", "id": subscription_id}])
        rows = (
            uow.session.execute(
                text(
                    """
                    SELECT
                        m.tmdb_id AS tmdb_media_id,
                        m.item_type AS media_type,
                        m.title,
                        m.release_date,
                        m.poster_path,
                        m.subscription_status AS status,
                        m.emby_item_ids_json,
                        m.in_library,
                        m.ignore_reason,
                        m.season_number,
                        p.title AS parent_title
                    FROM media_metadata m
                    LEFT JOIN media_metadata p
                      ON m.parent_series_tmdb_id = p.tmdb_id
                     AND p.item_type = 'Series'
                    WHERE m.subscription_sources_json @> CAST(:source_filter AS jsonb)
                    ORDER BY m.release_date DESC
                    """
                ),
                {"source_filter": source_filter},
            )
            .mappings()
            .all()
        )
        return [dict(cast(Mapping[str, Any], row)) for row in rows]

    def reset_changed_configuration(
        self,
        uow: SqlAlchemyUnitOfWork,
        subscription_id: int,
    ) -> None:
        source_filter = json.dumps([{"type": "actor_subscription", "id": subscription_id}])
        rows = (
            uow.session.execute(
                text(
                    """
                    SELECT tmdb_id, item_type, subscription_sources_json
                    FROM media_metadata
                    WHERE subscription_status = 'IGNORED'
                      AND subscription_sources_json @> CAST(:source_filter AS jsonb)
                    """
                ),
                {"source_filter": source_filter},
            )
            .mappings()
            .all()
        )
        for row in rows:
            media = dict(row)
            remaining = [
                source
                for source in _json_array(media.get("subscription_sources_json"))
                if not _is_actor_source(source, subscription_id)
            ]
            uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET subscription_sources_json = CAST(:sources AS jsonb),
                        subscription_status = CASE WHEN :empty THEN 'NONE' ELSE subscription_status END
                    WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                    """
                ),
                {
                    "sources": json.dumps(remaining, ensure_ascii=False),
                    "empty": not remaining,
                    "tmdb_id": media["tmdb_id"],
                    "item_type": media["item_type"],
                },
            )

    def remove_subscription(
        self,
        uow: SqlAlchemyUnitOfWork,
        subscription_id: int,
    ) -> tuple[Mapping[str, Any], ...]:
        source_filter = json.dumps([{"type": "actor_subscription", "id": subscription_id}])
        rows = (
            uow.session.execute(
                text(
                    """
                    SELECT tmdb_id, item_type, season_number, subscription_sources_json, in_library
                    FROM media_metadata
                    WHERE subscription_sources_json @> CAST(:source_filter AS jsonb)
                    """
                ),
                {"source_filter": source_filter},
            )
            .mappings()
            .all()
        )
        cancellations: list[Mapping[str, Any]] = []
        for row in rows:
            media = dict(row)
            remaining = [
                source
                for source in _json_array(media.get("subscription_sources_json"))
                if not _is_actor_source(source, subscription_id)
            ]
            real_other_sources = [
                source
                for source in remaining
                if isinstance(source, Mapping)
                and (source.get("name") or source.get("type") in {"actor_subscription", "user_request"})
            ]
            if real_other_sources:
                uow.session.execute(
                    text(
                        """
                        UPDATE media_metadata
                        SET subscription_sources_json = CAST(:sources AS jsonb)
                        WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                        """
                    ),
                    {
                        "sources": json.dumps(remaining, ensure_ascii=False),
                        "tmdb_id": media["tmdb_id"],
                        "item_type": media["item_type"],
                    },
                )
                continue

            cancellations.append(
                {
                    "tmdb_id": str(media["tmdb_id"]),
                    "item_type": str(media["item_type"]),
                    "season_number": media.get("season_number"),
                }
            )
            if media.get("in_library"):
                uow.session.execute(
                    text(
                        """
                        UPDATE media_metadata
                        SET subscription_sources_json = '[]'::jsonb,
                            subscription_status = 'NONE'
                        WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                        """
                    ),
                    {"tmdb_id": media["tmdb_id"], "item_type": media["item_type"]},
                )
            else:
                uow.session.execute(
                    text(
                        """
                        DELETE FROM media_metadata
                        WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                        """
                    ),
                    {"tmdb_id": media["tmdb_id"], "item_type": media["item_type"]},
                )
        return tuple(cancellations)


def _is_actor_source(source: object, subscription_id: int) -> bool:
    return (
        isinstance(source, Mapping)
        and source.get("type") == "actor_subscription"
        and source.get("id") == subscription_id
    )


def _json_array(value: object) -> list[Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return []
    return deepcopy(value) if isinstance(value, list) else []


__all__ = ["SqlAlchemyActorSubscriptionProjection"]
