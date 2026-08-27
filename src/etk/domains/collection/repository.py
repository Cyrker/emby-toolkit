from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from sqlalchemy import bindparam, text
from sqlalchemy.exc import IntegrityError

from etk.platform.db import SqlAlchemyUnitOfWork


class CollectionNameConflictError(RuntimeError):
    pass


class SqlAlchemyCollectionRepository:
    """Own the P8 collection tables while preserving their baseline schema."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def list_custom(self) -> list[dict[str, Any]]:
        values: list[dict[str, Any]] = []
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text("SELECT * FROM custom_collections ORDER BY sort_order ASC, id ASC")
            ).mappings()
            values = [dict(row) for row in rows]
        return values

    def get_custom(self, collection_id: int) -> dict[str, Any] | None:
        value: dict[str, Any] | None = None
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text("SELECT * FROM custom_collections WHERE id = :collection_id"),
                    {"collection_id": collection_id},
                )
                .mappings()
                .first()
            )
            value = dict(row) if row is not None else None
        return value

    def create_custom(
        self,
        *,
        name: str,
        collection_type: str,
        definition: Any,
        allowed_user_ids: Sequence[str] | None,
    ) -> dict[str, Any]:
        value: dict[str, Any] | None = None
        try:
            with self._unit_of_work() as uow:
                row = (
                    uow.session.execute(
                        text(
                            """
                        INSERT INTO custom_collections (
                            name, type, definition_json, allowed_user_ids
                        ) VALUES (
                            :name, :type, CAST(:definition AS jsonb), CAST(:allowed_user_ids AS jsonb)
                        )
                        RETURNING *
                        """
                        ),
                        {
                            "name": name,
                            "type": collection_type,
                            "definition": json.dumps(definition, ensure_ascii=False),
                            "allowed_user_ids": json.dumps(list(allowed_user_ids), ensure_ascii=False)
                            if allowed_user_ids is not None
                            else None,
                        },
                    )
                    .mappings()
                    .one()
                )
                uow.commit()
                value = dict(row)
        except IntegrityError as exc:
            raise CollectionNameConflictError(name) from exc
        if value is None:
            raise RuntimeError("custom collection insert did not return a row")
        return value

    def update_custom(
        self,
        collection_id: int,
        *,
        name: str,
        collection_type: str,
        definition: Any,
        status: str,
        allowed_user_ids: Sequence[str] | None,
    ) -> dict[str, Any] | None:
        value: dict[str, Any] | None = None
        try:
            with self._unit_of_work() as uow:
                row = (
                    uow.session.execute(
                        text(
                            """
                        UPDATE custom_collections
                        SET name = :name,
                            type = :type,
                            definition_json = CAST(:definition AS jsonb),
                            status = :status,
                            allowed_user_ids = CAST(:allowed_user_ids AS jsonb)
                        WHERE id = :collection_id
                        RETURNING *
                        """
                        ),
                        {
                            "collection_id": collection_id,
                            "name": name,
                            "type": collection_type,
                            "definition": json.dumps(definition, ensure_ascii=False),
                            "status": status,
                            "allowed_user_ids": json.dumps(list(allowed_user_ids), ensure_ascii=False)
                            if allowed_user_ids is not None
                            else None,
                        },
                    )
                    .mappings()
                    .first()
                )
                uow.commit()
                value = dict(row) if row is not None else None
        except IntegrityError as exc:
            raise CollectionNameConflictError(name) from exc
        return value

    def delete_custom(self, collection_id: int) -> dict[str, Any] | None:
        value: dict[str, Any] | None = None
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text("DELETE FROM custom_collections WHERE id = :collection_id RETURNING *"),
                    {"collection_id": collection_id},
                )
                .mappings()
                .first()
            )
            uow.commit()
            value = dict(row) if row is not None else None
        return value

    def update_custom_order(self, ordered_ids: Sequence[Any]) -> None:
        normalized_ids = [int(value) for value in ordered_ids]
        lock_ids = tuple(sorted(set(normalized_ids)))
        with self._unit_of_work() as uow:
            if lock_ids:
                tuple(
                    uow.session.scalars(
                        text(
                            """
                            SELECT id
                            FROM custom_collections
                            WHERE id IN :collection_ids
                            ORDER BY id
                            FOR UPDATE
                            """
                        ).bindparams(bindparam("collection_ids", expanding=True)),
                        {"collection_ids": lock_ids},
                    )
                )
            for sort_order, collection_id in enumerate(normalized_ids):
                uow.session.execute(
                    text("UPDATE custom_collections SET sort_order = :sort_order WHERE id = :collection_id"),
                    {"sort_order": sort_order, "collection_id": collection_id},
                )
            uow.commit()

    def list_native(self, *, active_only: bool = True) -> list[dict[str, Any]]:
        predicate = "WHERE is_active IS TRUE" if active_only else ""
        values: list[dict[str, Any]] = []
        with self._unit_of_work() as uow:
            rows = uow.session.execute(text(f"SELECT * FROM collections_info {predicate} ORDER BY name")).mappings()
            values = [dict(row) for row in rows]
        return values

    def get_native(self, tmdb_collection_id: str) -> dict[str, Any] | None:
        value: dict[str, Any] | None = None
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text("SELECT * FROM collections_info WHERE tmdb_collection_id = :tmdb_collection_id"),
                    {"tmdb_collection_id": str(tmdb_collection_id)},
                )
                .mappings()
                .first()
            )
            value = dict(row) if row is not None else None
        return value

    def upsert_native(self, details: Mapping[str, Any], existing_emby_id: str | None = None) -> dict[str, Any]:
        raw_parts = details.get("parts")
        parts = raw_parts if isinstance(raw_parts, list) else []
        tmdb_id = str(details.get("id") or "")
        value: dict[str, Any] | None = None
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                    INSERT INTO collections_info (
                        tmdb_collection_id, emby_collection_id, name, overview,
                        poster_path, backdrop_path, metadata_schema_version,
                        is_active, all_tmdb_ids_json, last_checked_at
                    ) VALUES (
                        :tmdb_id, :emby_id, :name, :overview,
                        :poster, :backdrop, 1,
                        :is_active, CAST(:ids AS jsonb), NOW()
                    )
                    ON CONFLICT (tmdb_collection_id) DO UPDATE SET
                        emby_collection_id = COALESCE(EXCLUDED.emby_collection_id, collections_info.emby_collection_id),
                        name = EXCLUDED.name,
                        overview = COALESCE(EXCLUDED.overview, collections_info.overview),
                        poster_path = EXCLUDED.poster_path,
                        backdrop_path = COALESCE(EXCLUDED.backdrop_path, collections_info.backdrop_path),
                        metadata_schema_version = 1,
                        is_active = collections_info.is_active OR EXCLUDED.is_active,
                        all_tmdb_ids_json = EXCLUDED.all_tmdb_ids_json,
                        last_checked_at = NOW()
                    RETURNING *
                    """
                    ),
                    {
                        "tmdb_id": tmdb_id,
                        "emby_id": existing_emby_id,
                        "name": details.get("name"),
                        "overview": details.get("overview"),
                        "poster": details.get("poster_path"),
                        "backdrop": details.get("backdrop_path"),
                        "is_active": bool(existing_emby_id),
                        "ids": json.dumps(
                            [str(item.get("id")) for item in parts if isinstance(item, Mapping) and item.get("id")],
                            ensure_ascii=False,
                        ),
                    },
                )
                .mappings()
                .one()
            )
            uow.commit()
            value = dict(row)
        if value is None:
            raise RuntimeError("native collection upsert did not return a row")
        return value

    def deactivate_native(self, emby_collection_id: str) -> bool:
        with self._unit_of_work() as uow:
            result = uow.session.execute(
                text(
                    """
                    UPDATE collections_info
                    SET emby_collection_id = NULL, is_active = FALSE, last_checked_at = NOW()
                    WHERE emby_collection_id = :emby_collection_id
                    """
                ),
                {"emby_collection_id": emby_collection_id},
            )
            uow.commit()
            changed = bool(int(getattr(result, "rowcount", 0) or 0))
        return changed


__all__ = ["CollectionNameConflictError", "SqlAlchemyCollectionRepository"]
