from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlparse
from uuid import UUID

from etk.platform.jobs.types import UnitOfWorkFactory

from .operation_repository import PostgresCollectionOperationRepository
from .operations import (
    CollectionOperationIntent,
    CollectionOperationKind,
    CollectionOperationReservation,
    CollectionOperationStatus,
    collection_image_digest,
)

COLLECTION_NATIVE_DELETE = CollectionOperationKind.NATIVE_DELETE.value
COLLECTION_CUSTOM_DELETE = CollectionOperationKind.CUSTOM_DELETE.value
COLLECTION_CUSTOM_EMPTY = COLLECTION_CUSTOM_DELETE
COLLECTION_MISSING_CLEANUP = CollectionOperationKind.MISSING_CLEANUP.value
COLLECTION_IMAGE_ARCHIVE = CollectionOperationKind.IMAGE_ARCHIVE.value
COLLECTION_REMOVAL_SURVIVAL_CHECK = CollectionOperationKind.REMOVAL_SURVIVAL_CHECK.value


@dataclass(frozen=True, slots=True)
class CollectionEffectSubmission:
    effect_id: UUID
    status: str
    created: bool
    related_effect_ids: tuple[UUID, ...] = ()


@dataclass(frozen=True, slots=True)
class RemoteCollectionState:
    collection_id: str
    member_ids: tuple[str, ...]
    tmdb_collection_id: str | None = None
    name: str | None = None


class CollectionRemoteRejected(RuntimeError):
    pass


class CollectionRemoteIndeterminate(RuntimeError):
    pass


class CollectionRemoteEffectsPort(Protocol):
    def read_collection(self, collection_id: str) -> RemoteCollectionState | None: ...

    def find_collection_by_name(self, name: str) -> RemoteCollectionState | None: ...

    def create_collection(self, name: str, member_ids: Sequence[str]) -> RemoteCollectionState: ...

    def add_members(self, collection_id: str, member_ids: Sequence[str]) -> None: ...

    def remove_members(self, collection_id: str, member_ids: Sequence[str]) -> None: ...

    def list_native_collections(self, tmdb_collection_id: str) -> Sequence[RemoteCollectionState]: ...

    def empty_collection(self, state: RemoteCollectionState) -> None: ...

    def delete_collection(self, collection_id: str) -> None: ...


class CollectionAssetEffectsPort(Protocol):
    def read_archived_image(self, source_url: str) -> Mapping[str, Any] | None: ...

    def archive_image(self, source_url: str) -> Mapping[str, Any] | None: ...

    def cleanup_placeholder(self, tmdb_id: str) -> bool: ...

    def discard_unreferenced_sources(self, source_urls: Sequence[str]) -> int: ...


class CollectionDeletionSuppressionPort(Protocol):
    def suppress(self, collection_id: str, *, ttl_seconds: float) -> None: ...

    def is_suppressed(self, collection_id: str) -> bool: ...


class PostgresCollectionEffectGateway:
    """Reserve P8 Collection operations without calling external providers."""

    def __init__(
        self,
        operations: PostgresCollectionOperationRepository | UnitOfWorkFactory,
    ) -> None:
        self._operations = (
            operations
            if isinstance(operations, PostgresCollectionOperationRepository)
            else PostgresCollectionOperationRepository(operations)
        )

    def stage_native_delete(
        self,
        emby_collection_id: str,
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> CollectionEffectSubmission:
        collection_id = _required_text(emby_collection_id, "emby_collection_id")
        reservation = self._operations.reserve(
            CollectionOperationIntent.create(
                kind=CollectionOperationKind.NATIVE_DELETE,
                scope_key=f"emby-collection:{collection_id}",
                principal_id=principal_id,
                request={
                    "emby_collection_id": collection_id,
                    "reason": "admin_delete",
                },
                idempotency_key=idempotency_key,
            )
        )
        return _submission(reservation)

    def stage_custom_delete(
        self,
        collection_id: int,
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> CollectionEffectSubmission | None:
        reservation = self._operations.reserve_custom_delete(
            collection_id=int(collection_id),
            principal_id=principal_id,
            idempotency_key=idempotency_key,
        )
        return _submission(reservation) if reservation is not None else None

    def stage_missing_cleanup(self, tmdb_collection_id: str) -> CollectionEffectSubmission:
        tmdb_id = _required_text(tmdb_collection_id, "tmdb_collection_id")
        reservation = self._operations.reserve(
            CollectionOperationIntent.create(
                kind=CollectionOperationKind.MISSING_CLEANUP,
                scope_key=f"tmdb-collection:{tmdb_id}:missing",
                principal_id="collection-system:tmdb-missing",
                request={
                    "tmdb_collection_id": tmdb_id,
                    "trigger": "explicit_tmdb_404",
                },
                idempotency_key=f"tmdb-404:{tmdb_id}",
            )
        )
        return _submission(reservation)

    def stage_image_archives(self, payload: Mapping[str, Any]) -> tuple[CollectionEffectSubmission, ...]:
        tmdb_id = str(payload.get("tmdb_id") or "").strip()
        images = payload.get("images")
        if not tmdb_id or not isinstance(images, Mapping):
            return ()
        slots = {
            str(key): str(value).strip()
            for key, value in images.items()
            if key in {"primary", "backdrop", "logo", "thumb"} and _remote_url(value)
        }
        if not slots:
            return ()
        metadata_digest = collection_image_digest(slots)
        reservation = self._operations.reserve(
            CollectionOperationIntent.create(
                kind=CollectionOperationKind.IMAGE_ARCHIVE,
                scope_key=f"tmdb-collection-image:{tmdb_id}:{metadata_digest}",
                principal_id="collection-system:image-archive",
                request={
                    "tmdb_collection_id": tmdb_id,
                    "metadata_digest": metadata_digest,
                    "images": slots,
                },
                idempotency_key=f"{tmdb_id}:{metadata_digest}",
            )
        )
        return (_submission(reservation),)

    def stage_removal_survival_check(
        self,
        emby_collection_id: str,
        *,
        event_identity: str,
    ) -> CollectionEffectSubmission:
        collection_id = _required_text(emby_collection_id, "emby_collection_id")
        event_id = _required_text(event_identity, "event_identity")
        reservation = self._operations.reserve(
            CollectionOperationIntent.create(
                kind=CollectionOperationKind.REMOVAL_SURVIVAL_CHECK,
                scope_key=f"emby-collection:{collection_id}",
                principal_id="collection-system:removal-event",
                request={
                    "emby_collection_id": collection_id,
                    "event_identity": event_id,
                },
                idempotency_key=event_id,
            )
        )
        return _submission(reservation)


def _submission(reservation: CollectionOperationReservation) -> CollectionEffectSubmission:
    operation = reservation.operation
    status = "pending" if operation.status is CollectionOperationStatus.READY else operation.status.value
    return CollectionEffectSubmission(
        operation.id,
        status,
        reservation.created,
        reservation.related_effect_ids,
    )


def _remote_url(value: Any) -> bool:
    parsed = urlparse(str(value or "").strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _required_text(value: Any, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return normalized


__all__ = [
    "COLLECTION_CUSTOM_DELETE",
    "COLLECTION_CUSTOM_EMPTY",
    "COLLECTION_IMAGE_ARCHIVE",
    "COLLECTION_MISSING_CLEANUP",
    "COLLECTION_NATIVE_DELETE",
    "COLLECTION_REMOVAL_SURVIVAL_CHECK",
    "CollectionAssetEffectsPort",
    "CollectionDeletionSuppressionPort",
    "CollectionEffectSubmission",
    "CollectionRemoteEffectsPort",
    "CollectionRemoteIndeterminate",
    "CollectionRemoteRejected",
    "PostgresCollectionEffectGateway",
    "RemoteCollectionState",
]
