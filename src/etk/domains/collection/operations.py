from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from etk.platform.jobs.json_payload import canonical_json, normalize_durable_json_object

COLLECTION_OPERATION_TOPIC = "collection.operation.requested"
COLLECTION_OPERATION_AGGREGATE_TYPE = "collection-operation"
COLLECTION_OPERATION_TOPICS = (COLLECTION_OPERATION_TOPIC,)


class CollectionOperationKind(StrEnum):
    NATIVE_DELETE = "collection.native.delete"
    CUSTOM_DELETE = "collection.custom.delete"
    MISSING_CLEANUP = "collection.missing.cleanup"
    IMAGE_ARCHIVE = "collection.image.archive"
    REMOVAL_SURVIVAL_CHECK = "collection.removal.survival-check"


class CollectionOperationStatus(StrEnum):
    READY = "ready"
    CLAIMED = "claimed"
    REQUESTING = "requesting"
    AWAITING_READBACK = "awaiting_readback"
    RETRYABLE = "retryable"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    INDETERMINATE = "indeterminate"

    @property
    def terminal(self) -> bool:
        return self in {
            CollectionOperationStatus.CONFIRMED,
            CollectionOperationStatus.REJECTED,
            CollectionOperationStatus.INDETERMINATE,
        }


class CollectionOperationStep(StrEnum):
    RESERVED = "reserved"
    READ_REMOTE = "read_remote"
    REMOVE_MEMBERS = "remove_members"
    DELETE_SHELL = "delete_shell"
    DISCOVER_MISSING_TARGETS = "discover_missing_targets"
    ARCHIVE_IMAGES = "archive_images"
    CLEANUP_PLACEHOLDERS = "cleanup_placeholders"
    CLEANUP_LOCAL = "cleanup_local"
    SURVIVAL_READBACK = "survival_readback"


@dataclass(frozen=True, slots=True)
class CollectionOperationIntent:
    id: UUID
    operation_key: str
    request_hash: bytes
    kind: CollectionOperationKind
    scope_key: str
    request: Mapping[str, Any]
    available_at: datetime

    @classmethod
    def create(
        cls,
        *,
        kind: CollectionOperationKind | str,
        scope_key: str,
        principal_id: str,
        request: Mapping[str, Any],
        idempotency_key: str | None,
        available_at: datetime | None = None,
    ) -> CollectionOperationIntent:
        normalized_kind = kind if isinstance(kind, CollectionOperationKind) else CollectionOperationKind(kind)
        normalized_scope = _required_text(scope_key, "scope_key")
        normalized_principal = _required_text(principal_id, "principal_id")
        normalized_request = normalize_durable_json_object(dict(request), field="collection operation request")
        request_hash = hashlib.sha256(
            canonical_json(normalized_request, field="collection operation request").encode("utf-8")
        ).digest()
        explicit = str(idempotency_key or "").strip()
        occurrence = explicit or str(uuid4())
        operation_key = collection_operation_key(
            kind=normalized_kind,
            principal_id=normalized_principal,
            idempotency_key=occurrence,
        )
        return cls(
            id=uuid5(NAMESPACE_URL, f"etk:collection-operation:{operation_key}"),
            operation_key=operation_key,
            request_hash=request_hash,
            kind=normalized_kind,
            scope_key=normalized_scope,
            request=normalized_request,
            available_at=_utc(available_at),
        )


@dataclass(frozen=True, slots=True)
class CollectionOperation:
    id: UUID
    sequence: int
    operation_key: str
    request_hash: bytes
    kind: CollectionOperationKind
    scope_key: str
    request: Mapping[str, Any]
    status: CollectionOperationStatus
    current_step: CollectionOperationStep
    state_revision: int
    attempts: int
    available_at: datetime
    claimed_by: str | None
    claim_token: UUID | None
    claimed_at: datetime | None
    lease_expires_at: datetime | None
    step_started_at: datetime | None
    state: Mapping[str, Any]
    result: Mapping[str, Any] | None
    error: Mapping[str, Any] | None
    finished_at: datetime | None
    created_at: datetime
    updated_at: datetime

    @property
    def terminal(self) -> bool:
        return self.status.terminal


@dataclass(frozen=True, slots=True)
class CollectionOperationReservation:
    operation: CollectionOperation
    created: bool
    related_effect_ids: tuple[UUID, ...] = ()


class CollectionOperationError(RuntimeError):
    pass


class CollectionOperationConflictError(CollectionOperationError):
    pass


class CollectionOperationScopeBusyError(CollectionOperationError):
    pass


class CollectionOperationIndeterminateBlockerError(CollectionOperationScopeBusyError):
    def __init__(self, blocker_id: UUID) -> None:
        self.blocker_id = blocker_id
        super().__init__(f"collection operation is blocked by indeterminate operation {blocker_id}")


class CollectionOperationStateError(CollectionOperationError):
    pass


def _required_text(value: object, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"collection operation {field} must not be empty")
    return normalized


def collection_operation_key(
    *,
    kind: CollectionOperationKind | str,
    principal_id: str,
    idempotency_key: str,
) -> str:
    normalized_kind = kind if isinstance(kind, CollectionOperationKind) else CollectionOperationKind(kind)
    normalized_principal = _required_text(principal_id, "principal_id")
    normalized_idempotency = _required_text(idempotency_key, "idempotency_key")
    return hashlib.sha256(
        f"{normalized_kind.value}\0{normalized_principal}\0{normalized_idempotency}".encode()
    ).hexdigest()


def collection_image_digest(images: Mapping[str, Any]) -> str:
    normalized = {str(key): str(value).strip() for key, value in images.items() if str(value or "").strip()}
    return hashlib.sha256(canonical_json(normalized, field="collection image metadata").encode()).hexdigest()


def _utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = [
    "COLLECTION_OPERATION_AGGREGATE_TYPE",
    "COLLECTION_OPERATION_TOPIC",
    "COLLECTION_OPERATION_TOPICS",
    "CollectionOperation",
    "CollectionOperationConflictError",
    "CollectionOperationError",
    "CollectionOperationIntent",
    "CollectionOperationIndeterminateBlockerError",
    "CollectionOperationKind",
    "CollectionOperationReservation",
    "CollectionOperationScopeBusyError",
    "CollectionOperationStateError",
    "CollectionOperationStatus",
    "CollectionOperationStep",
    "collection_image_digest",
    "collection_operation_key",
]
