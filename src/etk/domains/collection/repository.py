from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from sqlalchemy import bindparam, text
from sqlalchemy.exc import IntegrityError

from etk.domains.media.subscription_projection import (
    SqlAlchemySubscriptionMediaProjection,
    SubscriptionMediaKey,
)
from etk.platform.db import SqlAlchemyUnitOfWork


class CollectionNameConflictError(RuntimeError):
    pass


class CollectionMatchConflictError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class CustomMatchCandidate:
    collection_id: int
    collection_name: str
    old_tmdb_id: str | None
    old_title: str | None
    item_type: str
    season_number: int | None
    collection_digest: bytes


@dataclass(frozen=True, slots=True)
class CollectionCorrectionSnapshot:
    collection_tmdb_id: str
    season_number: int | None
    target_key: SubscriptionMediaKey
    target_record: Mapping[str, Any]
    target_status: str
    corrected_item: Mapping[str, Any]
    parent_key: SubscriptionMediaKey | None = None
    parent_record: Mapping[str, Any] | None = None


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

    def inspect_custom_match(
        self,
        collection_id: int,
        *,
        old_tmdb_id: str | None,
        old_title: str | None,
    ) -> CustomMatchCandidate | None:
        normalized_old_id = str(old_tmdb_id or "").strip() or None
        normalized_old_title = str(old_title or "").strip() or None
        if normalized_old_id is None and normalized_old_title is None:
            raise ValueError("old_tmdb_id or old_title is required")
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT name, definition_json, generated_media_info_json
                        FROM custom_collections
                        WHERE id = :collection_id
                        """
                    ),
                    {"collection_id": collection_id},
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
        definition, items = _collection_documents(dict(row))
        target = _locate_match(items, normalized_old_id, normalized_old_title)
        if target is None:
            return None
        current_id = str(target.get("tmdb_id") or "").strip() or None
        current_season = target.get("season")
        return CustomMatchCandidate(
            collection_id=int(collection_id),
            collection_name=str(row.get("name") or ""),
            old_tmdb_id=current_id,
            old_title=normalized_old_title,
            item_type=_collection_item_type(target),
            season_number=int(current_season) if current_season is not None else None,
            collection_digest=_collection_digest(str(row.get("name") or ""), definition, items),
        )

    def apply_custom_match(
        self,
        candidate: CustomMatchCandidate,
        snapshot: CollectionCorrectionSnapshot,
    ) -> dict[str, Any]:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT name, definition_json, generated_media_info_json
                        FROM custom_collections
                        WHERE id = :collection_id
                        FOR UPDATE
                        """
                    ),
                    {"collection_id": candidate.collection_id},
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise CollectionMatchConflictError("custom collection changed before correction commit")
            definition, items = _collection_documents(dict(row))
            if _collection_digest(str(row.get("name") or ""), definition, items) != candidate.collection_digest:
                raise CollectionMatchConflictError("custom collection changed before correction commit")
            target = _locate_match(items, candidate.old_tmdb_id, candidate.old_title)
            if target is None:
                raise CollectionMatchConflictError("custom collection item changed before correction commit")
            target_season = target.get("season")
            if (
                _collection_item_type(target) != candidate.item_type
                or (int(target_season) if target_season is not None else None) != candidate.season_number
            ):
                raise CollectionMatchConflictError("custom collection item identity changed before correction commit")

            target["tmdb_id"] = snapshot.collection_tmdb_id
            if snapshot.season_number is None:
                target.pop("season", None)
            else:
                target["season"] = snapshot.season_number

            raw_corrections = definition.get("corrections")
            corrections = deepcopy(dict(raw_corrections)) if isinstance(raw_corrections, Mapping) else {}
            correction_key = candidate.old_tmdb_id or f"title:{candidate.old_title}"
            correction: dict[str, Any] = {"tmdb_id": snapshot.collection_tmdb_id}
            if snapshot.season_number is not None:
                correction["season"] = snapshot.season_number
            corrections[correction_key] = correction
            definition["corrections"] = corrections

            media = SqlAlchemySubscriptionMediaProjection()
            if snapshot.parent_key is not None and snapshot.parent_record is not None:
                media.ensure_metadata(uow, snapshot.parent_key, snapshot.parent_record)
            media.ensure_metadata(uow, snapshot.target_key, snapshot.target_record)
            old_key: SubscriptionMediaKey | None = None
            if candidate.old_tmdb_id is not None and candidate.season_number is not None:
                old_key = media.lock_season_key(
                    uow,
                    parent_series_tmdb_id=candidate.old_tmdb_id,
                    season_number=candidate.season_number,
                )
            elif candidate.old_tmdb_id is not None:
                old_key = SubscriptionMediaKey(candidate.old_tmdb_id, candidate.item_type)
            if old_key is not None and old_key != snapshot.target_key:
                media.set_status(
                    uow,
                    old_key,
                    "IGNORED",
                    {},
                    None,
                    f"修正为 {snapshot.collection_tmdb_id}",
                )
            media.set_status(
                uow,
                snapshot.target_key,
                snapshot.target_status,
                snapshot.target_record,
                {
                    "type": "collection_correction",
                    "id": candidate.collection_id,
                    "name": str(row.get("name") or ""),
                },
                None,
            )

            uow.session.execute(
                text(
                    """
                    UPDATE custom_collections
                    SET definition_json = CAST(:definition AS jsonb),
                        generated_media_info_json = CAST(:items AS jsonb)
                    WHERE id = :collection_id
                    """
                ),
                {
                    "collection_id": candidate.collection_id,
                    "definition": json.dumps(definition, ensure_ascii=False),
                    "items": json.dumps(items, ensure_ascii=False),
                },
            )
            uow.commit()
            return deepcopy(dict(snapshot.corrected_item))

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

    def delete_missing_native(self, tmdb_collection_id: str) -> tuple[str, ...]:
        normalized_tmdb_id = str(tmdb_collection_id or "").strip()
        if not normalized_tmdb_id:
            raise ValueError("tmdb_collection_id must not be empty")
        with self._unit_of_work() as uow:
            native = (
                uow.session.execute(
                    text(
                        """
                        DELETE FROM collections_info
                        WHERE tmdb_collection_id = :tmdb_collection_id
                        RETURNING poster_path, backdrop_path
                        """
                    ),
                    {"tmdb_collection_id": normalized_tmdb_id},
                )
                .mappings()
                .one_or_none()
            )
            policy = (
                uow.session.execute(
                    text(
                        """
                        DELETE FROM media_image_policy_cache
                        WHERE item_type = 'BoxSet'
                          AND tmdb_id = :tmdb_collection_id
                          AND season_number = -1
                          AND episode_number = -1
                        RETURNING images_json
                        """
                    ),
                    {"tmdb_collection_id": normalized_tmdb_id},
                )
                .mappings()
                .one_or_none()
            )
            sources: list[str] = []
            if native is not None:
                sources.extend(
                    str(value).strip()
                    for value in (native.get("poster_path"), native.get("backdrop_path"))
                    if str(value or "").strip()
                )
            raw_images = policy.get("images_json") if policy is not None else None
            if isinstance(raw_images, list):
                sources.extend(
                    str(item.get("source_url") or "").strip()
                    for item in raw_images
                    if isinstance(item, Mapping) and str(item.get("source_url") or "").strip()
                )
            uow.commit()
            return tuple(dict.fromkeys(sources))


def _collection_documents(row: Mapping[str, Any]) -> tuple[dict[str, Any], list[Any]]:
    raw_definition = row.get("definition_json")
    raw_items = row.get("generated_media_info_json")
    definition = deepcopy(dict(raw_definition)) if isinstance(raw_definition, Mapping) else {}
    items = deepcopy(list(raw_items)) if isinstance(raw_items, list) else []
    return definition, items


def _locate_match(
    items: Sequence[Any],
    old_tmdb_id: str | None,
    old_title: str | None,
) -> dict[str, Any] | None:
    for item in items:
        if not isinstance(item, dict):
            continue
        current_id = str(item.get("tmdb_id") or "").strip()
        if old_tmdb_id is not None and current_id == old_tmdb_id:
            return item
        if (
            old_tmdb_id is None
            and old_title is not None
            and not current_id
            and str(item.get("title") or "") == old_title
        ):
            return item
    return None


def _collection_item_type(item: Mapping[str, Any]) -> str:
    return "Series" if str(item.get("media_type") or "Movie").strip() == "Series" else "Movie"


def _collection_digest(
    name: str,
    definition: Mapping[str, Any],
    items: Sequence[Any],
) -> bytes:
    payload = json.dumps(
        {"name": name, "definition": dict(definition), "items": list(items)},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(payload).digest()


__all__ = [
    "CollectionCorrectionSnapshot",
    "CollectionMatchConflictError",
    "CollectionNameConflictError",
    "CustomMatchCandidate",
    "SqlAlchemyCollectionRepository",
]
