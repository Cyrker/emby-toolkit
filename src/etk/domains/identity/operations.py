from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from etk.platform.jobs.json_payload import canonical_json, normalize_durable_json_object

IDENTITY_OPERATION_TOPIC = "identity.operation.requested"
IDENTITY_OPERATION_AGGREGATE_TYPE = "identity-operation"
IDENTITY_OPERATION_TOPICS = frozenset({IDENTITY_OPERATION_TOPIC})


class IdentityOperationKind(StrEnum):
    REGISTER = "register"
    ASSIGN_TEMPLATE = "assign_template"
    SET_STATUS = "set_status"
    EXPIRE_USER = "expire_user"
    DELETE_USER = "delete_user"
    UPLOAD_AVATAR = "upload_avatar"


class IdentityOperationStatus(StrEnum):
    READY = "ready"
    CLAIMED = "claimed"
    REQUESTING = "requesting"
    AWAITING_READBACK = "awaiting_readback"
    RETRYABLE = "retryable"
    COMPENSATING = "compensating"
    REMOTE_CONFIRMED = "remote_confirmed"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    COMPENSATED = "compensated"
    INDETERMINATE = "indeterminate"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in {
            IdentityOperationStatus.CONFIRMED,
            IdentityOperationStatus.REJECTED,
            IdentityOperationStatus.COMPENSATED,
            IdentityOperationStatus.INDETERMINATE,
            IdentityOperationStatus.CANCELLED,
        }


class IdentityOperationStep(StrEnum):
    RESERVED = "reserved"
    PREFLIGHT_USERNAME = "preflight_username"
    CREATE = "create"
    PASSWORD = "password"
    POLICY = "policy"
    CONFIGURATION = "configuration"
    LOCAL_COMMIT = "local_commit"
    DELETE_REMOTE_USER = "delete_remote_user"
    WRITE = "write"
    UPLOAD_AVATAR = "upload_avatar"
    READBACK = "readback"


@dataclass(frozen=True, slots=True)
class IdentityOperationIntent:
    id: UUID
    operation_key: str
    request_hash: bytes
    kind: IdentityOperationKind
    scope_key: str
    request: Mapping[str, Any]
    available_at: datetime

    @classmethod
    def create(
        cls,
        *,
        kind: IdentityOperationKind | str,
        scope_key: str,
        request: Mapping[str, Any],
        available_at: datetime | None = None,
        idempotency_key: str | None = None,
    ) -> IdentityOperationIntent:
        normalized_kind = kind if isinstance(kind, IdentityOperationKind) else IdentityOperationKind(kind)
        normalized_scope = str(scope_key or "").strip()
        if not normalized_scope:
            raise ValueError("identity operation scope_key must not be empty")
        normalized_request = normalize_durable_json_object(dict(request), field="identity operation request")
        request_bytes = canonical_json(normalized_request, field="identity operation request").encode("utf-8")
        request_hash = hashlib.sha256(request_bytes).digest()
        normalized_idempotency = None if idempotency_key is None else str(idempotency_key).strip()
        if idempotency_key is not None and not normalized_idempotency:
            raise ValueError("identity operation idempotency_key must not be empty")
        operation_key = identity_operation_key(
            kind=normalized_kind,
            scope_key=normalized_scope,
            request_hash=request_hash,
            idempotency_key=normalized_idempotency,
        )
        timestamp = _utc(available_at)
        return cls(
            id=uuid5(NAMESPACE_URL, f"etk:identity-operation:{operation_key}"),
            operation_key=operation_key,
            request_hash=request_hash,
            kind=normalized_kind,
            scope_key=normalized_scope,
            request=normalized_request,
            available_at=timestamp,
        )


@dataclass(frozen=True, slots=True)
class IdentityOperation:
    id: UUID
    sequence: int
    operation_key: str
    request_hash: bytes
    kind: IdentityOperationKind
    scope_key: str
    request: Mapping[str, Any]
    status: IdentityOperationStatus
    current_step: IdentityOperationStep
    state_revision: int
    attempts: int
    available_at: datetime
    claimed_by: str | None
    claim_token: UUID | None
    claimed_at: datetime | None
    lease_expires_at: datetime | None
    cancel_requested_at: datetime | None
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
class IdentityOperationReservation:
    operation: IdentityOperation
    created: bool


@dataclass(frozen=True, slots=True)
class IdentityAvatarArtifact:
    id: UUID
    operation_id: UUID
    owner_user_id: str
    sha256: bytes
    size_bytes: int
    content_type: str
    content: bytes


def identity_operation_key(
    *,
    kind: IdentityOperationKind | str,
    scope_key: str,
    request_hash: bytes | None = None,
    idempotency_key: str | None = None,
) -> str:
    normalized_kind = kind if isinstance(kind, IdentityOperationKind) else IdentityOperationKind(kind)
    normalized_scope = str(scope_key or "").strip()
    if not normalized_scope:
        raise ValueError("identity operation scope_key must not be empty")
    normalized_idempotency = None if idempotency_key is None else str(idempotency_key).strip()
    if idempotency_key is not None and not normalized_idempotency:
        raise ValueError("identity operation idempotency_key must not be empty")
    if normalized_idempotency is not None:
        identity_component = {
            "idempotency_key_hash": hashlib.sha256(normalized_idempotency.encode("utf-8")).hexdigest()
        }
    else:
        if request_hash is None or len(request_hash) != 32:
            raise ValueError("identity operation request_hash must contain 32 bytes")
        identity_component = {"request_hash": request_hash.hex()}
    identity = {
        "kind": normalized_kind.value,
        "scope_key": normalized_scope,
        **identity_component,
    }
    return hashlib.sha256(canonical_json(identity, field="identity operation identity").encode("utf-8")).hexdigest()


def _utc(value: datetime | None) -> datetime:
    timestamp = value or datetime.now(UTC)
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("identity operation timestamp must be timezone-aware")
    return timestamp.astimezone(UTC)


__all__ = [
    "IDENTITY_OPERATION_AGGREGATE_TYPE",
    "IDENTITY_OPERATION_TOPIC",
    "IDENTITY_OPERATION_TOPICS",
    "IdentityOperation",
    "IdentityAvatarArtifact",
    "IdentityOperationIntent",
    "IdentityOperationKind",
    "IdentityOperationReservation",
    "IdentityOperationStatus",
    "IdentityOperationStep",
    "identity_operation_key",
]
