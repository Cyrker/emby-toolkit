from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork

from .operation_repository import PostgresCollectionOperationRepository
from .operations import (
    CollectionOperation,
    CollectionOperationIntent,
    CollectionOperationKind,
    CollectionOperationScopeBusyError,
)
from .refresh import NativeCollectionReference, publish_native_collection_in_uow

logger = logging.getLogger(__name__)

_ACTIVATION_LOCK = "native-collection-refresh"
_ACTIVATION_PRINCIPAL = "collection-plugin:activation"


class CollectionActivationConflictError(RuntimeError):
    pass


class CollectionActivationNotFoundError(LookupError):
    pass


class CollectionActivationProviderRejectedError(RuntimeError):
    pass


class CollectionActivationProviderUnavailableError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class CollectionActivationReservation:
    operation: CollectionOperation
    operation_created: bool
    changed: bool
    already_active: bool


class CollectionActivationRepository(Protocol):
    def reserve(
        self,
        *,
        tmdb_collection_id: str,
        emby_collection_id: str,
        name: str,
    ) -> CollectionActivationReservation | None: ...


class CollectionActivationDetailsProvider(Protocol):
    def collection_details(self, tmdb_collection_id: str) -> Mapping[str, Any]: ...


class CollectionActivationCompletion(Protocol):
    def complete(
        self,
        operation_id: UUID,
        claim_token: UUID,
        details: Mapping[str, Any],
        *,
        today: date,
    ) -> CollectionOperation: ...


class PostgresCollectionActivationRepository:
    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        operations: PostgresCollectionOperationRepository | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._operations = operations or PostgresCollectionOperationRepository(unit_of_work)

    def reserve(
        self,
        *,
        tmdb_collection_id: str,
        emby_collection_id: str,
        name: str,
    ) -> CollectionActivationReservation | None:
        tmdb_id = _tmdb_id(tmdb_collection_id)
        emby_id = _bounded_text(emby_collection_id, "emby_collection_id", maximum=512)
        requested_name = _optional_text(name, maximum=512)
        with self._unit_of_work() as uow:
            _lock_refresh(uow)
            current = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_collection_id, emby_collection_id, name, is_active
                        FROM collections_info
                        WHERE tmdb_collection_id = :tmdb_collection_id
                        FOR UPDATE
                        """
                    ),
                    {"tmdb_collection_id": tmdb_id},
                )
                .mappings()
                .one_or_none()
            )
            if current is None:
                return None

            conflicting_tmdb_id = uow.session.scalar(
                text(
                    """
                    SELECT tmdb_collection_id
                    FROM collections_info
                    WHERE emby_collection_id = :emby_collection_id
                      AND tmdb_collection_id <> :tmdb_collection_id
                    FOR UPDATE
                    """
                ),
                {
                    "emby_collection_id": emby_id,
                    "tmdb_collection_id": tmdb_id,
                },
            )
            if conflicting_tmdb_id is not None:
                raise CollectionActivationConflictError("Emby collection is already bound to another TMDb collection")

            current_emby_id = str(current.get("emby_collection_id") or "").strip()
            if current_emby_id and current_emby_id != emby_id:
                raise CollectionActivationConflictError("TMDb collection is already bound to another Emby collection")

            effective_name = requested_name or str(current.get("name") or "").strip() or tmdb_id
            already_active = bool(current.get("is_active")) and current_emby_id == emby_id
            name_changed = str(current.get("name") or "").strip() != effective_name
            changed = not already_active or name_changed
            intent = CollectionOperationIntent.create(
                kind=CollectionOperationKind.ACTIVATION_REFRESH,
                scope_key=f"emby-collection:{emby_id}",
                principal_id=_ACTIVATION_PRINCIPAL,
                request={
                    "tmdb_collection_id": tmdb_id,
                    "emby_collection_id": emby_id,
                    "name": effective_name,
                },
                idempotency_key=f"{tmdb_id}:{emby_id}:{effective_name}",
            )
            try:
                reserved = self._operations.reserve_in_uow(uow, intent)
            except CollectionOperationScopeBusyError as exc:
                raise CollectionActivationConflictError(str(exc)) from exc

            updated = uow.session.execute(
                text(
                    """
                    UPDATE collections_info
                    SET emby_collection_id = :emby_collection_id,
                        name = :name,
                        is_active = TRUE,
                        last_checked_at = NOW()
                    WHERE tmdb_collection_id = :tmdb_collection_id
                    """
                ),
                {
                    "tmdb_collection_id": tmdb_id,
                    "emby_collection_id": emby_id,
                    "name": effective_name,
                },
            )
            if int(getattr(updated, "rowcount", 0) or 0) != 1:
                raise RuntimeError("collection activation lost its locked metadata row")
            uow.commit()
        return CollectionActivationReservation(
            operation=reserved.operation,
            operation_created=reserved.created,
            changed=changed,
            already_active=already_active and not name_changed,
        )

    def complete(
        self,
        operation_id: UUID,
        claim_token: UUID,
        details: Mapping[str, Any],
        *,
        today: date,
    ) -> CollectionOperation:
        with self._unit_of_work() as uow:
            _lock_refresh(uow)
            operation = self._operations.require_live_claim_in_uow(uow, operation_id, claim_token)
            if operation.kind is not CollectionOperationKind.ACTIVATION_REFRESH:
                raise ValueError("collection operation is not an activation refresh")
            tmdb_id = _tmdb_id(operation.request.get("tmdb_collection_id"))
            emby_id = _bounded_text(
                operation.request.get("emby_collection_id"),
                "emby_collection_id",
                maximum=512,
            )
            current = (
                uow.session.execute(
                    text(
                        """
                        SELECT name, emby_collection_id, is_active
                        FROM collections_info
                        WHERE tmdb_collection_id = :tmdb_collection_id
                        FOR UPDATE
                        """
                    ),
                    {"tmdb_collection_id": tmdb_id},
                )
                .mappings()
                .one_or_none()
            )
            if (
                current is None
                or not bool(current.get("is_active"))
                or str(current.get("emby_collection_id") or "").strip() != emby_id
            ):
                raise ValueError("collection activation binding changed before publication")

            auto_subscribe = _auto_subscribe_enabled(uow)
            reference = NativeCollectionReference(
                emby_collection_id=emby_id,
                tmdb_collection_id=tmdb_id,
                name=str(current.get("name") or operation.request.get("name") or tmdb_id).strip(),
                library_id="",
            )
            published = publish_native_collection_in_uow(
                uow,
                reference,
                details,
                auto_subscribe=auto_subscribe,
                today=today,
            )
            terminal = self._operations.mark_confirmed_in_uow(
                uow,
                operation_id,
                claim_token,
                state={
                    "tmdb_collection_id": tmdb_id,
                    "emby_collection_id": emby_id,
                    "auto_subscribe": auto_subscribe,
                },
                result={
                    "tmdb_collection_id": tmdb_id,
                    "emby_collection_id": emby_id,
                    "movie_count": published.movie_count,
                    "subscription_candidates": published.subscription_candidates,
                },
            )
            uow.commit()
            return terminal


class CollectionActivationService:
    def __init__(self, repository: CollectionActivationRepository) -> None:
        self._repository = repository

    def activate(self, payload: Any) -> tuple[dict[str, Any], int]:
        if not isinstance(payload, Mapping):
            return _error("invalid collection activation request", 400)
        try:
            tmdb_id = _tmdb_id(payload.get("tmdb_collection_id"))
            emby_id = _bounded_text(payload.get("emby_collection_id"), "emby_collection_id", maximum=512)
            name = _optional_text(payload.get("name"), maximum=512)
        except (TypeError, ValueError):
            return _error("invalid collection activation request", 400)

        try:
            reservation = self._repository.reserve(
                tmdb_collection_id=tmdb_id,
                emby_collection_id=emby_id,
                name=name,
            )
        except CollectionActivationConflictError:
            return _error("collection activation conflict", 409)
        except Exception:
            logger.exception("Collection activation transaction failed")
            return _error("collection activation failed", 500)
        if reservation is None:
            return _error("collection metadata cache not found", 404)
        return (
            {
                "ok": True,
                "changed": reservation.changed,
                "already_active": reservation.already_active,
                "operation_id": str(reservation.operation.id),
                "operation_status": reservation.operation.status.value,
                "operation_created": reservation.operation_created,
            },
            200,
        )


def _error(message: str, status: int) -> tuple[dict[str, Any], int]:
    return {"ok": False, "error": message}, status


def _lock_refresh(uow: SqlAlchemyUnitOfWork) -> None:
    uow.session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
        {"lock_key": _ACTIVATION_LOCK},
    )


def _auto_subscribe_enabled(uow: SqlAlchemyUnitOfWork) -> bool:
    value = uow.session.scalar(
        text("SELECT value_json FROM app_settings WHERE setting_key = 'native_collections_config'")
    )
    return bool(value.get("auto_sub_enabled")) if isinstance(value, Mapping) else False


def _tmdb_id(value: object) -> str:
    normalized = str(value or "").strip()
    if not normalized.isdigit() or len(normalized) > 32:
        raise ValueError("tmdb_collection_id is invalid")
    return normalized


def _bounded_text(value: object, field: str, *, maximum: int) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > maximum:
        raise ValueError(f"{field} is invalid")
    return normalized


def _optional_text(value: object, *, maximum: int) -> str:
    normalized = str(value or "").strip()
    if len(normalized) > maximum:
        raise ValueError("name is invalid")
    return normalized


__all__ = [
    "CollectionActivationCompletion",
    "CollectionActivationConflictError",
    "CollectionActivationDetailsProvider",
    "CollectionActivationNotFoundError",
    "CollectionActivationProviderRejectedError",
    "CollectionActivationProviderUnavailableError",
    "CollectionActivationRepository",
    "CollectionActivationReservation",
    "CollectionActivationService",
    "PostgresCollectionActivationRepository",
]
