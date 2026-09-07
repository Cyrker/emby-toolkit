from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from etk.platform.jobs.json_payload import canonical_json, normalize_durable_json_object

SHARING_OPERATION_TOPIC = "sharing.operation.requested"
SHARING_OPERATION_AGGREGATE_TYPE = "sharing-operation"
SHARING_OPERATION_TOPICS = (SHARING_OPERATION_TOPIC,)


class SharingOperationKind(StrEnum):
    REGISTER_DEVICE = "register_device"
    REGISTER_SOURCE = "register_source"
    DISPLAY_METADATA = "display_metadata"
    TRANSFER = "transfer"
    SHARE_REQUEST = "share_request"
    VIRTUAL_IMPORT = "virtual_import"
    HDHIVE_CHECKIN = "hdhive_checkin"
    TELEGRAM_DELIVERY = "telegram_delivery"
    PROXY_PLAY = "proxy_play"


class SharingOperationStatus(StrEnum):
    READY = "ready"
    CLAIMED = "claimed"
    REQUESTING = "requesting"
    AWAITING_READBACK = "awaiting_readback"
    RETRYABLE = "retryable"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    INDETERMINATE = "indeterminate"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in {
            SharingOperationStatus.CONFIRMED,
            SharingOperationStatus.REJECTED,
            SharingOperationStatus.INDETERMINATE,
            SharingOperationStatus.CANCELLED,
        }


@dataclass(frozen=True, slots=True)
class SharingOperationIntent:
    id: UUID
    operation_key: str
    request_hash: bytes
    kind: SharingOperationKind
    scope_key: str
    request: Mapping[str, Any]
    available_at: datetime

    @classmethod
    def create(
        cls,
        *,
        kind: SharingOperationKind | str,
        scope_key: str,
        principal_id: str,
        request: Mapping[str, Any],
        idempotency_key: str | None,
        available_at: datetime | None = None,
    ) -> SharingOperationIntent:
        normalized_kind = kind if isinstance(kind, SharingOperationKind) else SharingOperationKind(kind)
        normalized_scope = _required_text(scope_key, "scope_key")
        normalized_principal = _required_text(principal_id, "principal_id")
        normalized_request = normalize_durable_json_object(dict(request), field="sharing operation request")
        request_hash = hashlib.sha256(
            canonical_json(normalized_request, field="sharing operation request").encode("utf-8")
        ).digest()
        occurrence = str(idempotency_key or "").strip() or str(uuid4())
        operation_key = sharing_operation_key(
            kind=normalized_kind,
            principal_id=normalized_principal,
            idempotency_key=occurrence,
        )
        return cls(
            id=uuid5(NAMESPACE_URL, f"etk:sharing-operation:{operation_key}"),
            operation_key=operation_key,
            request_hash=request_hash,
            kind=normalized_kind,
            scope_key=normalized_scope,
            request=normalized_request,
            available_at=_utc(available_at),
        )


@dataclass(frozen=True, slots=True)
class SharingOperation:
    id: UUID
    sequence: int
    operation_key: str
    request_hash: bytes
    kind: SharingOperationKind
    scope_key: str
    request: Mapping[str, Any]
    status: SharingOperationStatus
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
    cancel_requested_at: datetime | None
    finished_at: datetime | None
    created_at: datetime
    updated_at: datetime

    @property
    def terminal(self) -> bool:
        return self.status.terminal


class SharingOperationError(RuntimeError):
    pass


class SharingOperationConflictError(SharingOperationError):
    pass


class SharingOperationScopeBusyError(SharingOperationError):
    pass


class SharingOperationStateError(SharingOperationError):
    pass


class SharingOperationPreconditionError(SharingOperationStateError):
    pass


def sharing_operation_key(
    *,
    kind: SharingOperationKind | str,
    principal_id: str,
    idempotency_key: str,
) -> str:
    normalized_kind = kind if isinstance(kind, SharingOperationKind) else SharingOperationKind(kind)
    return hashlib.sha256(
        f"{normalized_kind.value}\0{_required_text(principal_id, 'principal_id')}\0"
        f"{_required_text(idempotency_key, 'idempotency_key')}".encode()
    ).hexdigest()


def _required_text(value: object, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"sharing operation {field} must not be empty")
    return normalized


def _utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = [
    "SHARING_OPERATION_AGGREGATE_TYPE",
    "SHARING_OPERATION_TOPIC",
    "SHARING_OPERATION_TOPICS",
    "SharingOperation",
    "SharingOperationConflictError",
    "SharingOperationError",
    "SharingOperationIntent",
    "SharingOperationKind",
    "SharingOperationScopeBusyError",
    "SharingOperationStateError",
    "SharingOperationStatus",
    "sharing_operation_key",
]
