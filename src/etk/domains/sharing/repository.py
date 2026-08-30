from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs.json_payload import canonical_json, normalize_durable_json_object

from .operations import (
    SHARING_OPERATION_AGGREGATE_TYPE,
    SHARING_OPERATION_TOPIC,
    SharingOperation,
    SharingOperationConflictError,
    SharingOperationIntent,
    SharingOperationScopeBusyError,
    SharingOperationStateError,
    SharingOperationStatus,
)

_ACTIVE_STATUSES = (
    "ready",
    "claimed",
    "requesting",
    "awaiting_readback",
    "retryable",
    "indeterminate",
)
_CLAIMABLE_STATUSES = ("ready", "retryable", "awaiting_readback")
_TERMINAL_STATUSES = ("confirmed", "rejected", "indeterminate")
DEFAULT_SHARING_OPERATION_MAX_ATTEMPTS = 4


class PostgresSharingOperationRepository:
    """Durable sharing effects; claims and Outbox rows share explicit UoWs."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def reserve(self, intent: SharingOperationIntent) -> SharingOperation:
        with self._unit_of_work() as uow:
            operation = self.reserve_in_uow(uow, intent)
            uow.commit()
            return operation

    def reserve_in_uow(self, uow: SqlAlchemyUnitOfWork, intent: SharingOperationIntent) -> SharingOperation:
        request = normalize_durable_json_object(dict(intent.request), field="sharing operation request")
        request_hash = bytes.fromhex(_sha256_json(request))
        if request_hash != bytes(intent.request_hash):
            raise ValueError("sharing operation request_hash does not match request")
        existing = (
            uow.session.execute(
                text("SELECT * FROM sharing_operations WHERE operation_key = :operation_key FOR UPDATE"),
                {"operation_key": intent.operation_key},
            )
            .mappings()
            .one_or_none()
        )
        if existing is not None:
            current = _operation(existing)
            if (
                current.request_hash != request_hash
                or current.kind is not intent.kind
                or current.scope_key != intent.scope_key
            ):
                raise SharingOperationConflictError("sharing operation identity changed for idempotency key")
            return current
        active = (
            uow.session.execute(
                text(
                    """
                    SELECT * FROM sharing_operations
                    WHERE scope_key = :scope_key AND status = ANY(CAST(:statuses AS text[]))
                    ORDER BY event_sequence DESC
                    LIMIT 1 FOR UPDATE
                    """
                ),
                {"scope_key": intent.scope_key, "statuses": list(_ACTIVE_STATUSES)},
            )
            .mappings()
            .one_or_none()
        )
        if active is not None:
            raise SharingOperationScopeBusyError(
                f"sharing scope {intent.scope_key!r} is already owned by {active['id']}"
            )
        row = (
            uow.session.execute(
                text(
                    """
                    INSERT INTO sharing_operations(
                        id, operation_key, request_hash, kind, scope_key, request_json,
                        status, available_at
                    ) VALUES (
                        :id, :operation_key, :request_hash, :kind, :scope_key,
                        CAST(:request_json AS jsonb), 'ready', :available_at
                    ) RETURNING *
                    """
                ),
                {
                    "id": intent.id,
                    "operation_key": intent.operation_key,
                    "request_hash": bytes(intent.request_hash),
                    "kind": intent.kind.value,
                    "scope_key": intent.scope_key,
                    "request_json": canonical_json(request, field="sharing operation request"),
                    "available_at": intent.available_at,
                },
            )
            .mappings()
            .one()
        )
        operation = _operation(row)
        self._stage_outbox(uow, operation)
        return operation

    def read(self, operation_id: UUID) -> SharingOperation | None:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text("SELECT * FROM sharing_operations WHERE id = :id"),
                    {"id": operation_id},
                )
                .mappings()
                .one_or_none()
            )
            return _operation(row) if row is not None else None

    def claim_by_id(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> SharingOperation | None:
        worker = _required(worker_id, "worker_id")
        lease = _positive_seconds(lease_seconds)
        timestamp = _aware(now)
        token = uuid4()
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE sharing_operations
                        SET status = 'claimed', attempts = attempts + 1,
                            claimed_by = :worker_id, claim_token = :claim_token,
                            claimed_at = :now,
                            lease_expires_at = :lease_expires_at,
                            state_revision = state_revision + 1, updated_at = :now
                        WHERE id = :id
                          AND status = ANY(CAST(:statuses AS text[]))
                          AND available_at <= :now
                          AND (claim_token IS NULL OR lease_expires_at <= :now)
                        RETURNING *
                        """
                    ),
                    {
                        "id": operation_id,
                        "worker_id": worker,
                        "claim_token": token,
                        "now": timestamp,
                        "lease_expires_at": timestamp + timedelta(seconds=lease),
                        "statuses": list(_CLAIMABLE_STATUSES),
                    },
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            operation = _operation(row)
            self._complete_outbox(uow, operation.id)
            uow.commit()
            return operation

    def mark_requesting(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        now: datetime | None = None,
    ) -> SharingOperation:
        return self._claimed_update(
            operation_id,
            claim_token,
            "status = 'requesting', step_started_at = :now",
            {"now": _aware(now)},
            allowed=(SharingOperationStatus.CLAIMED,),
        )

    def mark_awaiting_readback(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
        retry_after_seconds: float,
        now: datetime | None = None,
    ) -> SharingOperation:
        delay = _nonnegative_seconds(retry_after_seconds)
        return self._claimed_update(
            operation_id,
            claim_token,
            "status = 'awaiting_readback', available_at = :available_at, step_started_at = COALESCE(:now, NOW()), "
            "error_json = CAST(:error_json AS jsonb), claimed_by = NULL, claim_token = NULL, "
            "claimed_at = NULL, lease_expires_at = NULL",
            {
                "now": _aware(now),
                "available_at": _aware(now) + timedelta(seconds=delay),
                "error_json": canonical_json(
                    normalize_durable_json_object(dict(error), field="sharing readback error")
                ),
            },
            allowed=(
                SharingOperationStatus.CLAIMED,
                SharingOperationStatus.REQUESTING,
                SharingOperationStatus.AWAITING_READBACK,
            ),
            stage=True,
        )

    def mark_retryable(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
        retry_after_seconds: float,
        now: datetime | None = None,
    ) -> SharingOperation:
        delay = _nonnegative_seconds(retry_after_seconds)
        return self._claimed_update(
            operation_id,
            claim_token,
            "status = 'retryable', available_at = :available_at, error_json = CAST(:error_json AS jsonb), "
            "claimed_by = NULL, claim_token = NULL, claimed_at = NULL, lease_expires_at = NULL, step_started_at = NULL",
            {
                "now": _aware(now),
                "available_at": _aware(now) + timedelta(seconds=delay),
                "error_json": canonical_json(normalize_durable_json_object(dict(error), field="sharing retry error")),
            },
            allowed=(
                SharingOperationStatus.CLAIMED,
                SharingOperationStatus.REQUESTING,
                SharingOperationStatus.AWAITING_READBACK,
            ),
            stage=True,
        )

    def mark_confirmed(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        result: Mapping[str, Any],
        now: datetime | None = None,
    ) -> SharingOperation:
        return self._claimed_update(
            operation_id,
            claim_token,
            "status = 'confirmed', result_json = CAST(:result_json AS jsonb), error_json = NULL, "
            "finished_at = :now, step_started_at = NULL, claimed_by = NULL, claim_token = NULL, "
            "claimed_at = NULL, lease_expires_at = NULL",
            {
                "result_json": canonical_json(normalize_durable_json_object(dict(result), field="sharing result")),
                "now": _aware(now),
            },
            allowed=(
                SharingOperationStatus.CLAIMED,
                SharingOperationStatus.REQUESTING,
                SharingOperationStatus.AWAITING_READBACK,
            ),
            complete=True,
        )

    def mark_rejected(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
        now: datetime | None = None,
    ) -> SharingOperation:
        return self._claimed_update(
            operation_id,
            claim_token,
            "status = 'rejected', error_json = CAST(:error_json AS jsonb), finished_at = :now, "
            "step_started_at = NULL, claimed_by = NULL, claim_token = NULL, claimed_at = NULL, lease_expires_at = NULL",
            {
                "error_json": canonical_json(normalize_durable_json_object(dict(error), field="sharing rejection")),
                "now": _aware(now),
            },
            allowed=(
                SharingOperationStatus.CLAIMED,
                SharingOperationStatus.REQUESTING,
                SharingOperationStatus.AWAITING_READBACK,
            ),
            complete=True,
        )

    def mark_indeterminate(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
        now: datetime | None = None,
    ) -> SharingOperation:
        return self._claimed_update(
            operation_id,
            claim_token,
            "status = 'indeterminate', error_json = CAST(:error_json AS jsonb), finished_at = :now, "
            "step_started_at = NULL, claimed_by = NULL, claim_token = NULL, claimed_at = NULL, lease_expires_at = NULL",
            {
                "error_json": canonical_json(
                    normalize_durable_json_object(dict(error), field="sharing indeterminate error")
                ),
                "now": _aware(now),
            },
            allowed=(
                SharingOperationStatus.CLAIMED,
                SharingOperationStatus.REQUESTING,
                SharingOperationStatus.AWAITING_READBACK,
            ),
            complete=True,
        )

    def expire_stale_claims(
        self,
        *,
        limit: int = 100,
        max_attempts: int = DEFAULT_SHARING_OPERATION_MAX_ATTEMPTS,
        now: datetime | None = None,
    ) -> int:
        bounded = max(1, min(int(limit), 1000))
        attempt_limit = _attempt_limit(max_attempts)
        timestamp = _aware(now)
        with self._unit_of_work() as uow:
            result = uow.session.execute(
                text(
                    """
                        WITH stale AS (
                            SELECT id, attempts
                            FROM sharing_operations
                            WHERE status = 'claimed'
                              AND step_started_at IS NULL
                              AND lease_expires_at <= :now
                            ORDER BY event_sequence ASC LIMIT :limit FOR UPDATE SKIP LOCKED
                        )
                        UPDATE sharing_operations AS operation
                        SET status = CASE
                                WHEN stale.attempts >= :max_attempts THEN 'rejected'
                                ELSE 'retryable'
                            END,
                            available_at = CASE
                                WHEN stale.attempts < :max_attempts THEN :now
                                ELSE operation.available_at
                            END,
                            finished_at = CASE
                                WHEN stale.attempts >= :max_attempts THEN :now
                                ELSE operation.finished_at
                            END,
                            claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                            lease_expires_at = NULL,
                            step_started_at = NULL,
                            error_json = jsonb_build_object(
                                'type', CASE
                                    WHEN stale.attempts >= :max_attempts
                                        THEN 'sharing_claim_recovery_exhausted'
                                    ELSE 'sharing_claim_expired'
                                END,
                                'provider_may_have_been_called', FALSE,
                                'attempts', stale.attempts
                            ),
                            state_revision = operation.state_revision + 1, updated_at = :now
                        FROM stale
                        WHERE operation.id = stale.id
                        RETURNING operation.*
                        """
                ),
                {
                    "now": timestamp,
                    "limit": bounded,
                    "max_attempts": attempt_limit,
                },
            )
            rows = result.mappings().all()
            for row in rows:
                operation = _operation(row)
                if operation.status is SharingOperationStatus.RETRYABLE:
                    self._stage_outbox(uow, operation)
                else:
                    self._complete_outbox(uow, operation.id)
            count = len(rows)
            uow.commit()
            return count

    def requeue_stale_readback_claims(
        self,
        *,
        limit: int = 100,
        max_attempts: int = DEFAULT_SHARING_OPERATION_MAX_ATTEMPTS,
        now: datetime | None = None,
    ) -> int:
        bounded = max(1, min(int(limit), 1000))
        attempt_limit = _attempt_limit(max_attempts)
        timestamp = _aware(now)
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        WITH stale AS (
                            SELECT id, attempts
                            FROM sharing_operations
                            WHERE status IN ('claimed', 'requesting')
                              AND step_started_at IS NOT NULL
                              AND lease_expires_at <= :now
                            ORDER BY event_sequence ASC
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE sharing_operations AS operation
                        SET status = CASE
                                WHEN stale.attempts >= :max_attempts THEN 'indeterminate'
                                ELSE 'awaiting_readback'
                            END,
                            available_at = :now,
                            finished_at = CASE
                                WHEN stale.attempts >= :max_attempts THEN :now
                                ELSE operation.finished_at
                            END,
                            claimed_by = NULL,
                            claim_token = NULL,
                            claimed_at = NULL,
                            lease_expires_at = NULL,
                            error_json = jsonb_build_object(
                                'type', CASE
                                    WHEN stale.attempts >= :max_attempts
                                        THEN 'authoritative_readback_exhausted'
                                    ELSE 'sharing_request_claim_expired'
                                END,
                                'provider_may_have_been_called', TRUE,
                                'authoritative_readback_required', TRUE,
                                'attempts', stale.attempts
                            ),
                            state_revision = operation.state_revision + 1,
                            updated_at = :now
                        FROM stale
                        WHERE operation.id = stale.id
                        RETURNING operation.*
                        """
                    ),
                    {
                        "now": timestamp,
                        "limit": bounded,
                        "max_attempts": attempt_limit,
                    },
                )
                .mappings()
                .all()
            )
            for row in rows:
                operation = _operation(row)
                if operation.status is SharingOperationStatus.AWAITING_READBACK:
                    self._stage_outbox(uow, operation)
                else:
                    self._complete_outbox(uow, operation.id)
            uow.commit()
            return len(rows)

    def reject_dead_dispatches(self, *, limit: int = 100) -> int:
        bounded = max(1, min(int(limit), 1000))
        with self._unit_of_work() as uow:
            result = uow.session.execute(
                text(
                    """
                        WITH dead AS (
                            SELECT operation.id, operation.status
                            FROM sharing_operations AS operation
                            JOIN outbox_events AS outbox
                              ON outbox.aggregate_id = operation.id::text
                            WHERE outbox.topic = :topic
                              AND outbox.aggregate_type = :aggregate_type
                              AND outbox.status = 'dead'
                              AND operation.status IN ('ready','retryable','awaiting_readback')
                              AND operation.claim_token IS NULL
                            ORDER BY outbox.created_at ASC
                            LIMIT :limit
                            FOR UPDATE OF operation SKIP LOCKED
                        )
                        UPDATE sharing_operations AS operation
                        SET status = CASE
                                WHEN dead.status = 'awaiting_readback' THEN 'indeterminate'
                                ELSE 'rejected'
                            END,
                            finished_at = NOW(),
                            claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                            lease_expires_at = NULL,
                            error_json = jsonb_build_object(
                                'type', 'sharing_dispatch_dead',
                                'provider_may_have_been_called', dead.status = 'awaiting_readback'
                            ),
                            state_revision = operation.state_revision + 1,
                            updated_at = NOW()
                        FROM dead
                        WHERE operation.id = dead.id
                        """
                ),
                {
                    "topic": SHARING_OPERATION_TOPIC,
                    "aggregate_type": SHARING_OPERATION_AGGREGATE_TYPE,
                    "limit": bounded,
                },
            )
            count = int(getattr(result, "rowcount", 0) or 0)
            uow.commit()
            return count

    def _claimed_update(
        self,
        operation_id: UUID,
        claim_token: UUID,
        assignments: str,
        params: Mapping[str, Any],
        *,
        allowed: tuple[SharingOperationStatus, ...],
        stage: bool = False,
        complete: bool = False,
    ) -> SharingOperation:
        token = claim_token
        transition_at = _aware(params.get("now"))
        allowed_values = [status.value for status in allowed]
        with self._unit_of_work() as uow:
            current = (
                uow.session.execute(
                    text("SELECT * FROM sharing_operations WHERE id = :id FOR UPDATE"),
                    {"id": operation_id},
                )
                .mappings()
                .one_or_none()
            )
            if current is None:
                raise SharingOperationStateError("sharing operation was not found")
            current_operation = _operation(current)
            if (
                current_operation.status.value not in allowed_values
                or current_operation.claim_token != token
                or current_operation.lease_expires_at is None
                or current_operation.lease_expires_at <= transition_at
            ):
                raise SharingOperationStateError("sharing operation claim is no longer valid")
            values = {"id": operation_id, "claim_token": token, **dict(params), "now": transition_at}
            row = (
                uow.session.execute(
                    text(
                        f"""
                        UPDATE sharing_operations
                        SET {assignments}, state_revision = state_revision + 1, updated_at = COALESCE(:now, NOW())
                        WHERE id = :id AND claim_token = :claim_token AND lease_expires_at > :now
                        RETURNING *
                        """
                    ),
                    values,
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise SharingOperationStateError("sharing operation transition lost its claim")
            operation = _operation(row)
            if stage and operation.status in {
                SharingOperationStatus.RETRYABLE,
                SharingOperationStatus.AWAITING_READBACK,
            }:
                self._stage_outbox(uow, operation)
            if complete:
                self._complete_outbox(uow, operation.id)
            uow.commit()
            return operation

    @staticmethod
    def _stage_outbox(uow: SqlAlchemyUnitOfWork, operation: SharingOperation) -> None:
        uow.session.execute(
            text(
                """
                INSERT INTO outbox_events(
                    id, event_key, topic, aggregate_type, aggregate_id, payload_version,
                    payload_json, status, available_at, max_publish_attempts
                ) VALUES(
                    :id, :event_key, :topic, :aggregate_type, :aggregate_id, 1,
                    CAST(:payload_json AS jsonb), 'pending', :available_at, 20
                ) ON CONFLICT(event_key) DO UPDATE SET
                    status = 'pending',
                    available_at = EXCLUDED.available_at,
                    claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                    processed_at = NULL, published_at = NULL, updated_at = NOW()
                WHERE outbox_events.status <> 'dead'
                """
            ),
            {
                "id": uuid4(),
                "event_key": f"sharing-operation:{operation.id}",
                "topic": SHARING_OPERATION_TOPIC,
                "aggregate_type": SHARING_OPERATION_AGGREGATE_TYPE,
                "aggregate_id": str(operation.id),
                "payload_json": json.dumps(
                    {
                        "operation_id": str(operation.id),
                        "sequence": operation.sequence,
                        "operation_key": operation.operation_key,
                        "request_hash": operation.request_hash.hex(),
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "available_at": operation.available_at,
            },
        )

    @staticmethod
    def _complete_outbox(uow: SqlAlchemyUnitOfWork, operation_id: UUID) -> None:
        uow.session.execute(
            text(
                """
                UPDATE outbox_events
                SET status = 'processed', processed_at = COALESCE(processed_at, NOW()),
                    claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                    updated_at = NOW()
                WHERE topic = :topic AND aggregate_type = :aggregate_type
                  AND aggregate_id = :aggregate_id AND status <> 'dead'
                """
            ),
            {
                "topic": SHARING_OPERATION_TOPIC,
                "aggregate_type": SHARING_OPERATION_AGGREGATE_TYPE,
                "aggregate_id": str(operation_id),
            },
        )


def _operation(row: Mapping[Any, Any]) -> SharingOperation:
    return SharingOperation(
        id=UUID(str(row["id"])),
        sequence=int(row.get("event_sequence") or 0),
        operation_key=str(row["operation_key"]),
        request_hash=bytes(row["request_hash"]),
        kind=_enum_value(row.get("kind")),
        scope_key=str(row["scope_key"]),
        request=dict(row.get("request_json") or {}),
        status=SharingOperationStatus(str(row["status"])),
        state_revision=int(row.get("state_revision") or 0),
        attempts=int(row.get("attempts") or 0),
        available_at=_aware(row.get("available_at")),
        claimed_by=str(row["claimed_by"]) if row.get("claimed_by") is not None else None,
        claim_token=UUID(str(row["claim_token"])) if row.get("claim_token") else None,
        claimed_at=_nullable_time(row.get("claimed_at")),
        lease_expires_at=_nullable_time(row.get("lease_expires_at")),
        step_started_at=_nullable_time(row.get("step_started_at")),
        state=dict(row.get("state_json") or {}),
        result=dict(row["result_json"]) if isinstance(row.get("result_json"), Mapping) else None,
        error=dict(row["error_json"]) if isinstance(row.get("error_json"), Mapping) else None,
        finished_at=_nullable_time(row.get("finished_at")),
        created_at=_aware(row.get("created_at")),
        updated_at=_aware(row.get("updated_at")),
    )


def _enum_value(value: Any):
    from .operations import SharingOperationKind

    return SharingOperationKind(str(value))


def _sha256_json(value: Mapping[str, Any]) -> str:
    import hashlib

    return hashlib.sha256(canonical_json(value, field="sharing operation request").encode("utf-8")).hexdigest()


def _required(value: Any, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"sharing operation {field} must not be empty")
    return normalized


def _positive_seconds(value: float) -> float:
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise ValueError("sharing operation lease_seconds must be positive and finite")
    return result


def _nonnegative_seconds(value: float) -> float:
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError("sharing operation retry_after_seconds must be non-negative and finite")
    return result


def _attempt_limit(value: int) -> int:
    if isinstance(value, bool) or int(value) < 2:
        raise ValueError("sharing operation max_attempts must be at least two")
    return int(value)


def _aware(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return datetime.now(UTC)


def _nullable_time(value: Any) -> datetime | None:
    return _aware(value) if value is not None else None


__all__ = ["DEFAULT_SHARING_OPERATION_MAX_ATTEMPTS", "PostgresSharingOperationRepository"]
