from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

SUBSCRIPTION_OPERATION_TOPIC = "subscription.operation.requested"
SUBSCRIPTION_OPERATION_AGGREGATE_TYPE = "subscription-operation"
SUBSCRIPTION_OPERATION_TOPICS = frozenset({SUBSCRIPTION_OPERATION_TOPIC})

_FORBIDDEN_DURABLE_KEYS = frozenset(
    {
        "access_token",
        "api_key",
        "apikey",
        "authorization",
        "cookie",
        "cookies",
        "password",
        "secret",
        "token",
    }
)

_FORBIDDEN_DURABLE_KEY_SUFFIXES = (
    "_api_key",
    "_cookie",
    "_password",
    "_secret",
    "_token",
)


class SubscriptionOperationKind(StrEnum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    RESUBSCRIBE = "resubscribe"
    RECONCILE = "reconcile"


class SubscriptionOperationStatus(StrEnum):
    READY = "ready"
    CLAIMED = "claimed"
    REQUESTING = "requesting"
    AWAITING_READBACK = "awaiting_readback"
    REMOTE_CONFIRMED = "remote_confirmed"
    RETRYABLE = "retryable"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    INDETERMINATE = "indeterminate"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in {
            SubscriptionOperationStatus.CONFIRMED,
            SubscriptionOperationStatus.REJECTED,
            SubscriptionOperationStatus.INDETERMINATE,
            SubscriptionOperationStatus.CANCELLED,
        }


@dataclass(frozen=True, slots=True)
class SubscriptionQuotaRequirement:
    quota_date: date
    daily_cap: int
    units: int = 1
    ttl_seconds: float = 3600

    def __post_init__(self) -> None:
        if isinstance(self.daily_cap, bool) or not isinstance(self.daily_cap, int) or self.daily_cap < 0:
            raise ValueError("daily_cap must be a non-negative integer")
        if isinstance(self.units, bool) or not isinstance(self.units, int) or self.units < 1:
            raise ValueError("quota units must be a positive integer")
        if not math.isfinite(float(self.ttl_seconds)) or float(self.ttl_seconds) <= 0:
            raise ValueError("quota reservation TTL must be positive and finite")


@dataclass(frozen=True, slots=True)
class SubscriptionOperationIntent:
    id: UUID
    operation_key: str
    request_hash: bytes
    kind: SubscriptionOperationKind
    scope_key: str
    request: Mapping[str, Any]
    available_at: datetime
    quota: SubscriptionQuotaRequirement | None = None

    @classmethod
    def create(
        cls,
        *,
        kind: SubscriptionOperationKind | str,
        scope_key: str,
        request: Mapping[str, Any],
        available_at: datetime | None = None,
        quota: SubscriptionQuotaRequirement | None = None,
        idempotency_key: str | None = None,
    ) -> SubscriptionOperationIntent:
        normalized_kind = kind if isinstance(kind, SubscriptionOperationKind) else SubscriptionOperationKind(kind)
        normalized_scope = str(scope_key or "").strip()
        if not normalized_scope:
            raise ValueError("subscription operation scope_key must not be empty")
        normalized_request = _json_object(request)
        request_bytes = canonical_json(normalized_request).encode()
        request_hash = hashlib.sha256(request_bytes).digest()
        identity_key = None if idempotency_key is None else str(idempotency_key).strip()
        if idempotency_key is not None and not identity_key:
            raise ValueError("subscription operation idempotency_key must not be empty")
        identity_fields = (
            {
                "kind": normalized_kind.value,
                "idempotency_key_hash": hashlib.sha256(identity_key.encode()).hexdigest(),
                "scope_key": normalized_scope,
            }
            if identity_key is not None
            else {
                "kind": normalized_kind.value,
                "request_hash": request_hash.hex(),
                "scope_key": normalized_scope,
            }
        )
        identity = canonical_json(identity_fields)
        operation_key = hashlib.sha256(identity.encode()).hexdigest()
        timestamp = _utc(available_at)
        return cls(
            id=uuid5(NAMESPACE_URL, f"etk:subscription-operation:{operation_key}"),
            operation_key=operation_key,
            request_hash=request_hash,
            kind=normalized_kind,
            scope_key=normalized_scope,
            request=normalized_request,
            available_at=timestamp,
            quota=quota,
        )


@dataclass(frozen=True, slots=True)
class SubscriptionOperation:
    id: UUID
    sequence: int
    operation_key: str
    request_hash: bytes
    kind: SubscriptionOperationKind
    scope_key: str
    request: Mapping[str, Any]
    status: SubscriptionOperationStatus
    state_revision: int
    attempts: int
    available_at: datetime
    claimed_by: str | None
    claim_token: UUID | None
    claimed_at: datetime | None
    lease_expires_at: datetime | None
    cancel_requested_at: datetime | None
    provider_request_started_at: datetime | None
    provider_receipt: Mapping[str, Any] | None
    readback: Mapping[str, Any] | None
    result: Mapping[str, Any] | None
    error: Mapping[str, Any] | None
    finished_at: datetime | None
    created_at: datetime
    updated_at: datetime

    @property
    def terminal(self) -> bool:
        return self.status.terminal


@dataclass(frozen=True, slots=True)
class SubscriptionOperationReservation:
    operation: SubscriptionOperation
    created: bool
    quota_reservation_id: UUID | None = None


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def safe_durable_mapping(value: Mapping[str, Any], *, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field} must be a mapping")
    normalized = _json_value(value)
    if not isinstance(normalized, dict):
        raise AssertionError(f"normalized {field} must be an object")
    _reject_durable_secrets(normalized, field=field)
    return normalized


def _json_object(value: Mapping[str, Any]) -> dict[str, Any]:
    return safe_durable_mapping(value, field="subscription operation request")


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("subscription operation request must contain finite numbers")
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("subscription operation request keys must be strings")
            normalized[key] = _json_value(item)
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_value(item) for item in value]
    raise ValueError(f"unsupported subscription operation request value: {type(value).__name__}")


def _reject_durable_secrets(value: Any, *, field: str) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized_key = str(key).strip().casefold().replace("-", "_")
            if normalized_key in _FORBIDDEN_DURABLE_KEYS or normalized_key.endswith(_FORBIDDEN_DURABLE_KEY_SUFFIXES):
                raise ValueError(f"{field} cannot persist secret field {key!r}")
            _reject_durable_secrets(item, field=field)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            _reject_durable_secrets(item, field=field)


def _utc(value: datetime | None) -> datetime:
    timestamp = value or datetime.now(UTC)
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("subscription operation timestamp must be timezone-aware")
    return timestamp.astimezone(UTC)


__all__ = [
    "SUBSCRIPTION_OPERATION_AGGREGATE_TYPE",
    "SUBSCRIPTION_OPERATION_TOPIC",
    "SUBSCRIPTION_OPERATION_TOPICS",
    "SubscriptionOperation",
    "SubscriptionOperationIntent",
    "SubscriptionOperationKind",
    "SubscriptionOperationReservation",
    "SubscriptionOperationStatus",
    "SubscriptionQuotaRequirement",
    "canonical_json",
    "safe_durable_mapping",
]
