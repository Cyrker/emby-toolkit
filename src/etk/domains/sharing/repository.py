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

from .ledger import apply_contribution_in_uow, normalize_contribution_idempotency_key
from .operations import (
    SHARING_OPERATION_AGGREGATE_TYPE,
    SHARING_OPERATION_TOPIC,
    SharingOperation,
    SharingOperationConflictError,
    SharingOperationIntent,
    SharingOperationPreconditionError,
    SharingOperationScopeBusyError,
    SharingOperationStateError,
    SharingOperationStatus,
)
from .source_registration import source_registration_state_hash

_ACTIVE_STATUSES = (
    "ready",
    "claimed",
    "requesting",
    "awaiting_readback",
    "retryable",
    "indeterminate",
)
_CLAIMABLE_STATUSES = ("ready", "retryable", "awaiting_readback")
DEFAULT_SHARING_OPERATION_MAX_ATTEMPTS = 4


class PostgresSharingOperationRepository:
    """Durable sharing effects; claims and Outbox rows share explicit UoWs."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def reserve(self, intent: SharingOperationIntent) -> SharingOperation:
        with self._unit_of_work() as uow:
            operation, _replayed = self._reserve_in_uow(uow, intent)
            uow.commit()
            return operation

    def reserve_with_replay(self, intent: SharingOperationIntent) -> tuple[SharingOperation, bool]:
        with self._unit_of_work() as uow:
            operation, replayed = self._reserve_in_uow(uow, intent)
            uow.commit()
            return operation, replayed

    def reserve_in_uow(self, uow: SqlAlchemyUnitOfWork, intent: SharingOperationIntent) -> SharingOperation:
        operation, _replayed = self._reserve_in_uow(uow, intent)
        return operation

    def reserve_in_uow_with_replay(
        self,
        uow: SqlAlchemyUnitOfWork,
        intent: SharingOperationIntent,
    ) -> tuple[SharingOperation, bool]:
        return self._reserve_in_uow(uow, intent)

    def reserve_prerequisite_chain(
        self,
        *,
        dependent_intent: SharingOperationIntent,
        prerequisite_intent: SharingOperationIntent,
    ) -> tuple[SharingOperation, SharingOperation, bool]:
        blocked_by = str(dependent_intent.request.get("blocked_by_operation_id") or "").strip()
        continuation = prerequisite_intent.request.get("local_continuation")
        if blocked_by != str(prerequisite_intent.id):
            raise ValueError("sharing dependent operation prerequisite identity is invalid")
        if (
            not isinstance(continuation, Mapping)
            or str(continuation.get("type") or "").strip() != "release_dependent_operation"
            or str(continuation.get("operation_id") or "").strip() != str(dependent_intent.id)
        ):
            raise ValueError("sharing prerequisite continuation identity is invalid")
        with self._unit_of_work() as uow:
            dependent, replayed = self._reserve_in_uow(uow, dependent_intent, stage_outbox=False)
            prerequisite, _prerequisite_replayed = self._reserve_in_uow(uow, prerequisite_intent)
            if prerequisite.status is SharingOperationStatus.CONFIRMED:
                _release_dependent_operation(uow, prerequisite, continuation)
            elif prerequisite.terminal:
                _terminalize_dependent_operation(uow, prerequisite, continuation)
            uow.commit()
            return dependent, prerequisite, replayed

    def _reserve_in_uow(
        self,
        uow: SqlAlchemyUnitOfWork,
        intent: SharingOperationIntent,
        *,
        stage_outbox: bool = True,
    ) -> tuple[SharingOperation, bool]:
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
            return current, True
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
        if stage_outbox:
            self._stage_outbox(uow, operation)
        return operation, False

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

    def validate_prewrite(self, operation: SharingOperation) -> None:
        with self._unit_of_work() as uow:
            _validate_operation_prewrite(uow, operation, lock=False)

    def request_cancel(
        self,
        operation_id: UUID,
        *,
        now: datetime | None = None,
    ) -> SharingOperation | None:
        timestamp = _aware(now)
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text("SELECT * FROM sharing_operations WHERE id = :id FOR UPDATE"),
                    {"id": operation_id},
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            current = _operation(row)
            if current.terminal:
                return current
            cancellable_before_request = current.step_started_at is None and current.status in {
                SharingOperationStatus.READY,
                SharingOperationStatus.RETRYABLE,
                SharingOperationStatus.CLAIMED,
            }
            if cancellable_before_request:
                cancelled = (
                    uow.session.execute(
                        text(
                            """
                            UPDATE sharing_operations
                            SET status = 'cancelled', cancel_requested_at = COALESCE(cancel_requested_at, :now),
                                error_json = jsonb_build_object(
                                    'type', 'cancelled_before_provider_request',
                                    'provider_may_have_been_called', FALSE
                                ),
                                result_json = NULL, finished_at = :now, step_started_at = NULL,
                                claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                                lease_expires_at = NULL, state_revision = state_revision + 1,
                                updated_at = :now
                            WHERE id = :id
                            RETURNING *
                            """
                        ),
                        {"id": operation_id, "now": timestamp},
                    )
                    .mappings()
                    .one()
                )
                operation = _operation(cancelled)
                _apply_escrow_compensation(uow, current)
                _apply_terminal_local_continuation(uow, operation)
                self._complete_outbox(uow, operation.id)
            else:
                requested = (
                    uow.session.execute(
                        text(
                            """
                            UPDATE sharing_operations
                            SET cancel_requested_at = COALESCE(cancel_requested_at, :now),
                                state_revision = state_revision + 1, updated_at = :now
                            WHERE id = :id
                            RETURNING *
                            """
                        ),
                        {"id": operation_id, "now": timestamp},
                    )
                    .mappings()
                    .one()
                )
                operation = _operation(requested)
            uow.commit()
            return operation

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
            validate_prewrite=True,
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
                    _apply_escrow_compensation(uow, operation)
                    _apply_terminal_local_continuation(uow, operation)
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
                    _apply_terminal_local_continuation(uow, operation)
                    self._complete_outbox(uow, operation.id)
            uow.commit()
            return len(rows)

    def reject_dead_dispatches(self, *, limit: int = 100) -> int:
        bounded = max(1, min(int(limit), 1000))
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
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
                        RETURNING operation.*
                        """
                    ),
                    {
                        "topic": SHARING_OPERATION_TOPIC,
                        "aggregate_type": SHARING_OPERATION_AGGREGATE_TYPE,
                        "limit": bounded,
                    },
                )
                .mappings()
                .all()
            )
            for row in rows:
                operation = _operation(row)
                if operation.status is SharingOperationStatus.REJECTED:
                    _apply_escrow_compensation(uow, operation)
                _apply_terminal_local_continuation(uow, operation)
            uow.commit()
            return len(rows)

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
        validate_prewrite: bool = False,
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
            if validate_prewrite:
                try:
                    _validate_operation_prewrite(uow, current_operation, lock=True)
                except SharingOperationStateError as exc:
                    raise SharingOperationPreconditionError(str(exc)) from exc
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
                if operation.status is SharingOperationStatus.CONFIRMED:
                    _apply_confirmed_local_continuation(uow, operation)
                elif operation.status is SharingOperationStatus.REJECTED:
                    _apply_escrow_compensation(uow, operation)
                    _apply_terminal_local_continuation(uow, operation)
                elif operation.status in {
                    SharingOperationStatus.INDETERMINATE,
                    SharingOperationStatus.CANCELLED,
                }:
                    _apply_terminal_local_continuation(uow, operation)
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


def _validate_operation_prewrite(
    uow: SqlAlchemyUnitOfWork,
    operation: SharingOperation,
    *,
    lock: bool,
) -> None:
    continuation = operation.request.get("local_continuation")
    if not isinstance(continuation, Mapping):
        return
    if str(continuation.get("type") or "").strip() != "confirm_local_source_registration":
        return
    _validate_source_registration_preconditions(uow, continuation, lock=lock)


def _apply_confirmed_local_continuation(
    uow: SqlAlchemyUnitOfWork,
    operation: SharingOperation,
) -> None:
    continuation = operation.request.get("local_continuation")
    if continuation is None:
        return
    if not isinstance(continuation, Mapping):
        raise SharingOperationStateError("sharing local continuation must be an object")
    continuation_type = str(continuation.get("type") or "").strip()
    if continuation_type == "enable_shared_resource":
        _enable_shared_resource(uow)
        return
    if continuation_type in {"disable_local_source", "delete_local_source"}:
        _apply_local_source_lifecycle(uow, operation, continuation, delete=continuation_type == "delete_local_source")
        return
    if continuation_type == "release_dependent_operation":
        _release_dependent_operation(uow, operation, continuation)
        return
    if continuation_type == "confirm_local_source_registration":
        _apply_source_registration_confirmation(uow, operation, continuation)
        return
    raise SharingOperationStateError(f"sharing local continuation type {continuation_type!r} is unsupported")


def _apply_terminal_local_continuation(
    uow: SqlAlchemyUnitOfWork,
    operation: SharingOperation,
) -> None:
    continuation = operation.request.get("local_continuation")
    if not isinstance(continuation, Mapping):
        return
    continuation_type = str(continuation.get("type") or "").strip()
    if continuation_type == "release_dependent_operation":
        _terminalize_dependent_operation(uow, operation, continuation)
    elif (
        continuation_type == "confirm_local_source_registration"
        and operation.status is not SharingOperationStatus.CANCELLED
    ):
        _record_source_registration_failure(uow, operation, continuation)


def _release_dependent_operation(
    uow: SqlAlchemyUnitOfWork,
    prerequisite: SharingOperation,
    continuation: Mapping[str, Any],
) -> None:
    dependent_id = _uuid(continuation.get("operation_id"), "dependent operation id")
    row = (
        uow.session.execute(
            text("SELECT * FROM sharing_operations WHERE id = :id FOR UPDATE"),
            {"id": dependent_id},
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise SharingOperationStateError("sharing dependent operation was not found")
    dependent = _operation(row)
    if str(dependent.request.get("blocked_by_operation_id") or "").strip() != str(prerequisite.id):
        raise SharingOperationStateError("sharing dependent operation prerequisite changed")
    if dependent.terminal:
        return
    if dependent.status is not SharingOperationStatus.READY or dependent.claim_token is not None:
        return
    PostgresSharingOperationRepository._stage_outbox(uow, dependent)


def _terminalize_dependent_operation(
    uow: SqlAlchemyUnitOfWork,
    prerequisite: SharingOperation,
    continuation: Mapping[str, Any],
) -> None:
    dependent_id = _uuid(continuation.get("operation_id"), "dependent operation id")
    row = (
        uow.session.execute(
            text("SELECT * FROM sharing_operations WHERE id = :id FOR UPDATE"),
            {"id": dependent_id},
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise SharingOperationStateError("sharing dependent operation was not found")
    dependent = _operation(row)
    if str(dependent.request.get("blocked_by_operation_id") or "").strip() != str(prerequisite.id):
        raise SharingOperationStateError("sharing dependent operation prerequisite changed")
    if dependent.terminal:
        return
    status = {
        SharingOperationStatus.REJECTED: SharingOperationStatus.REJECTED,
        SharingOperationStatus.INDETERMINATE: SharingOperationStatus.INDETERMINATE,
        SharingOperationStatus.CANCELLED: SharingOperationStatus.CANCELLED,
    }.get(prerequisite.status)
    if status is None:
        raise SharingOperationStateError("sharing prerequisite is not terminal")
    error = {
        "type": f"sharing_prerequisite_{status.value}",
        "prerequisite_operation_id": str(prerequisite.id),
        "provider_may_have_been_called": status is SharingOperationStatus.INDETERMINATE,
    }
    updated = (
        uow.session.execute(
            text(
                """
                UPDATE sharing_operations
                SET status = :status, error_json = CAST(:error_json AS jsonb), result_json = NULL,
                    finished_at = NOW(), claimed_by = NULL, claim_token = NULL,
                    claimed_at = NULL, lease_expires_at = NULL, step_started_at = NULL,
                    state_revision = state_revision + 1, updated_at = NOW()
                WHERE id = :id AND status = 'ready' AND claim_token IS NULL
                RETURNING *
                """
            ),
            {
                "id": dependent.id,
                "status": status.value,
                "error_json": canonical_json(error, field="sharing prerequisite error"),
            },
        )
        .mappings()
        .one_or_none()
    )
    if updated is None:
        raise SharingOperationStateError("sharing dependent operation terminal transition failed")
    terminal = _operation(updated)
    _apply_terminal_local_continuation(uow, terminal)
    PostgresSharingOperationRepository._complete_outbox(uow, terminal.id)


def _apply_source_registration_confirmation(
    uow: SqlAlchemyUnitOfWork,
    operation: SharingOperation,
    continuation: Mapping[str, Any],
) -> None:
    source, files = _validate_source_registration_preconditions(uow, continuation, lock=True)
    source_id = _positive_id(continuation.get("source_id"), "source id")
    expected_center_id = str(continuation.get("expected_center_source_id") or "").strip()
    provider_result = operation.result.get("provider_result") if isinstance(operation.result, Mapping) else None
    provider_result = dict(provider_result) if isinstance(provider_result, Mapping) else {}
    center_source_id = str(provider_result.get("source_id") or expected_center_id).strip()
    if not center_source_id:
        raise SharingOperationStateError("sharing provider confirmation omitted source id")
    receipt = normalize_durable_json_object(
        {
            "operation_id": str(operation.id),
            "provider_result": provider_result,
            "authoritative_readback": bool((operation.result or {}).get("authoritative_readback")),
        },
        field="sharing source confirmation receipt",
    )
    result = uow.session.execute(
        text(
            """
            UPDATE shared_rapid_sources
            SET center_source_id = :center_source_id,
                status = 'active', center_status = 'reported',
                last_error = NULL, reported_at = NOW(), disabled_at = NULL,
                raw_json = COALESCE(raw_json, '{}'::jsonb)
                    || jsonb_build_object('native_registration', CAST(:receipt AS jsonb)),
                updated_at = NOW()
            WHERE id = :id
            """
        ),
        {
            "id": source_id,
            "center_source_id": center_source_id,
            "receipt": canonical_json(receipt, field="sharing source confirmation receipt"),
        },
    )
    if int(getattr(result, "rowcount", 0) or 0) != 1:
        raise SharingOperationStateError("sharing local source registration was not applied")
    if files:
        uow.session.execute(
            text(
                """
                UPDATE shared_rapid_source_files
                SET raw_ffprobe_uploaded = TRUE,
                    raw_json = COALESCE(raw_json, '{}'::jsonb)
                        || jsonb_build_object('registration_operation_id', CAST(:operation_id AS text)),
                    updated_at = NOW()
                WHERE local_source_id = :id
                """
            ),
            {"id": source_id, "operation_id": str(operation.id)},
        )
    else:
        _insert_confirmed_source_file(uow, source_id, continuation, operation)


def _validate_source_registration_preconditions(
    uow: SqlAlchemyUnitOfWork,
    continuation: Mapping[str, Any],
    *,
    lock: bool,
) -> tuple[Mapping[str, Any], list[Mapping[str, Any]]]:
    source_id = _positive_id(continuation.get("source_id"), "source id")
    source_query = (
        "SELECT * FROM shared_rapid_sources WHERE id = :id FOR UPDATE"
        if lock
        else ("SELECT * FROM shared_rapid_sources WHERE id = :id")
    )
    source = uow.session.execute(text(source_query), {"id": source_id}).mappings().one_or_none()
    if source is None:
        raise SharingOperationStateError("sharing local source no longer exists")
    files_query = """
        SELECT * FROM shared_rapid_source_files
        WHERE local_source_id = :id
        ORDER BY id ASC
    """
    if lock:
        files_query += " FOR UPDATE"
    files = list(uow.session.execute(text(files_query), {"id": source_id}).mappings().all())
    expected_hash = _required(continuation.get("expected_source_state_hash"), "source state hash")
    current_hash = source_registration_state_hash(dict(source), [dict(value) for value in files])
    if current_hash != expected_hash:
        raise SharingOperationStateError("sharing local source changed before registration confirmation")
    expected_center_id = str(continuation.get("expected_center_source_id") or "").strip()
    if str(source.get("center_source_id") or "").strip() != expected_center_id:
        raise SharingOperationStateError("sharing center source identity changed before confirmation")
    source_kind = _required(continuation.get("source_kind"), "source kind")
    if str(source.get("source_kind") or "").strip().lower() != source_kind:
        raise SharingOperationStateError("sharing local source kind changed before confirmation")
    _validate_source_registration_holder(uow, continuation, lock=lock)
    return dict(source), [dict(value) for value in files]


def _validate_source_registration_holder(
    uow: SqlAlchemyUnitOfWork,
    continuation: Mapping[str, Any],
    *,
    lock: bool,
) -> None:
    holder = continuation.get("local_sign_holder")
    if holder is None:
        return
    if not isinstance(holder, Mapping):
        raise SharingOperationStateError("sharing local sign-holder snapshot is invalid")
    account_id = _required(holder.get("account_id"), "sign-holder account id")
    if not account_id.isdigit():
        raise SharingOperationStateError("sharing local sign-holder account id is invalid")
    pick_code = _required(holder.get("pick_code"), "sign-holder pick code")
    sha1 = _required(holder.get("sha1"), "sign-holder sha1").upper()
    holder_query = """
        SELECT id
        FROM p115_filesystem_cache
        WHERE account_id = :account_id
          AND (entry_kind = 'file' OR entry_kind IS NULL)
          AND pick_code = :pick_code
          AND upper(sha1) = :sha1
        LIMIT 1
    """
    if lock:
        holder_query += " FOR KEY SHARE"
    found = uow.session.execute(
        text(holder_query),
        {"account_id": account_id, "pick_code": pick_code, "sha1": sha1},
    ).scalar_one_or_none()
    if found is None:
        raise SharingOperationStateError("sharing local sign-holder changed before registration confirmation")


def _insert_confirmed_source_file(
    uow: SqlAlchemyUnitOfWork,
    source_id: int,
    continuation: Mapping[str, Any],
    operation: SharingOperation,
) -> None:
    file_record = continuation.get("file_record")
    if not isinstance(file_record, Mapping):
        raise SharingOperationStateError("sharing source file snapshot is missing")
    sha1 = _required(file_record.get("sha1"), "source file sha1").upper()
    file_name = _required(file_record.get("file_name"), "source file name")
    uow.session.execute(
        text(
            """
            INSERT INTO shared_rapid_source_files(
                local_source_id, fid, pick_code, sha1, preid, size, file_name,
                relative_path, tmdb_id, item_type, season_number, episode_number,
                raw_ffprobe_uploaded, media_signature_json, rapid_meta_json, raw_json
            ) VALUES (
                :local_source_id, :fid, :pick_code, :sha1, :preid, :size, :file_name,
                :relative_path, :tmdb_id, :item_type, :season_number, :episode_number,
                TRUE, CAST(:media_signature_json AS jsonb), CAST(:rapid_meta_json AS jsonb),
                jsonb_build_object('registration_operation_id', CAST(:operation_id AS text))
            )
            """
        ),
        {
            "local_source_id": source_id,
            "fid": file_record.get("fid"),
            "pick_code": file_record.get("pick_code"),
            "sha1": sha1,
            "preid": file_record.get("preid"),
            "size": int(file_record.get("size") or 0),
            "file_name": file_name,
            "relative_path": file_record.get("relative_path"),
            "tmdb_id": file_record.get("tmdb_id"),
            "item_type": file_record.get("item_type"),
            "season_number": file_record.get("season_number"),
            "episode_number": file_record.get("episode_number"),
            "media_signature_json": canonical_json(
                dict(file_record.get("media_signature_json") or {}),
                field="sharing source file signature",
            ),
            "rapid_meta_json": canonical_json(
                dict(file_record.get("rapid_meta_json") or {}),
                field="sharing source file rapid metadata",
            ),
            "operation_id": str(operation.id),
        },
    )


def _record_source_registration_failure(
    uow: SqlAlchemyUnitOfWork,
    operation: SharingOperation,
    continuation: Mapping[str, Any],
) -> None:
    source_id = _positive_id(continuation.get("source_id"), "source id")
    source = (
        uow.session.execute(
            text("SELECT * FROM shared_rapid_sources WHERE id = :id FOR UPDATE"),
            {"id": source_id},
        )
        .mappings()
        .one_or_none()
    )
    if source is None:
        return
    files = (
        uow.session.execute(
            text("SELECT * FROM shared_rapid_source_files WHERE local_source_id = :id ORDER BY id ASC"),
            {"id": source_id},
        )
        .mappings()
        .all()
    )
    expected_hash = str(continuation.get("expected_source_state_hash") or "").strip()
    current_hash = source_registration_state_hash(dict(source), [dict(value) for value in files])
    if expected_hash and current_hash != expected_hash:
        return
    error = operation.error if isinstance(operation.error, Mapping) else {}
    message = str(error.get("message") or error.get("type") or f"共享登记{operation.status.value}").strip()
    uow.session.execute(
        text("UPDATE shared_rapid_sources SET last_error = :message, updated_at = NOW() WHERE id = :id"),
        {"id": source_id, "message": message[:1000]},
    )


def _enable_shared_resource(uow: SqlAlchemyUnitOfWork) -> None:
    uow.session.execute(
        text(
            """
            INSERT INTO app_settings(setting_key, value_json, last_updated_at)
            VALUES ('shared_resource_config', '{}'::jsonb, NOW())
            ON CONFLICT (setting_key) DO NOTHING
            """
        )
    )
    current = uow.session.scalar(
        text(
            """
            SELECT value_json
            FROM app_settings
            WHERE setting_key = 'shared_resource_config'
            FOR UPDATE
            """
        )
    )
    payload = dict(current) if isinstance(current, Mapping) else {}
    payload["p115_shared_resource_enabled"] = True
    uow.session.execute(
        text(
            """
            UPDATE app_settings
            SET value_json = CAST(:value_json AS jsonb), last_updated_at = NOW()
            WHERE setting_key = 'shared_resource_config'
            """
        ),
        {"value_json": json.dumps(payload, ensure_ascii=False)},
    )


def _apply_local_source_lifecycle(
    uow: SqlAlchemyUnitOfWork,
    operation: SharingOperation,
    continuation: Mapping[str, Any],
    *,
    delete: bool,
) -> None:
    source_id = continuation.get("source_id")
    if isinstance(source_id, bool):
        raise SharingOperationStateError("sharing local source id is invalid")
    try:
        normalized_id = int(str(source_id).strip())
    except (TypeError, ValueError) as exc:
        raise SharingOperationStateError("sharing local source id is invalid") from exc
    if normalized_id < 1:
        raise SharingOperationStateError("sharing local source id is invalid")
    expected_kind = _required(continuation.get("expected_source_kind"), "local source kind")
    expected_center_id = _required(continuation.get("expected_center_source_id"), "local center source id")
    row = (
        uow.session.execute(
            text("SELECT * FROM shared_rapid_sources WHERE id = :id FOR UPDATE"),
            {"id": normalized_id},
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise SharingOperationStateError("sharing local source no longer exists")
    if (
        str(row.get("source_kind") or "").strip() != expected_kind
        or str(row.get("center_source_id") or "").strip() != expected_center_id
    ):
        raise SharingOperationStateError("sharing local source identity changed before confirmation")
    if delete:
        uow.session.execute(
            text("DELETE FROM shared_rapid_source_files WHERE local_source_id = :id"),
            {"id": normalized_id},
        )
        result = uow.session.execute(
            text("DELETE FROM shared_rapid_sources WHERE id = :id"),
            {"id": normalized_id},
        )
    else:
        result = uow.session.execute(
            text(
                """
                UPDATE shared_rapid_sources
                SET status = 'disabled', center_status = 'disabled',
                    disabled_at = COALESCE(disabled_at, NOW()),
                    raw_json = COALESCE(raw_json, '{}'::jsonb)
                        || jsonb_build_object('lifecycle_operation_id', CAST(:operation_id AS text)),
                    updated_at = NOW()
                WHERE id = :id
                """
            ),
            {"id": normalized_id, "operation_id": str(operation.id)},
        )
    if int(getattr(result, "rowcount", 0) or 0) != 1:
        raise SharingOperationStateError("sharing local source continuation was not applied")


def _apply_escrow_compensation(
    uow: SqlAlchemyUnitOfWork,
    operation: SharingOperation,
) -> None:
    compensation = operation.request.get("ledger_compensation")
    if compensation is None:
        return
    if not isinstance(compensation, Mapping):
        raise SharingOperationStateError("sharing ledger compensation must be an object")
    if str(compensation.get("type") or "").strip() != "release_transfer_escrow":
        raise SharingOperationStateError("sharing ledger compensation type is unsupported")
    account_id = _required(compensation.get("account_id"), "ledger compensation account_id")
    debit_key = _required(compensation.get("debit_idempotency_key"), "ledger compensation debit key")
    reference = _required(compensation.get("reference"), "ledger compensation reference")
    amount = compensation.get("amount")
    if isinstance(amount, bool):
        raise SharingOperationStateError("sharing ledger compensation amount is invalid")
    try:
        normalized_amount = int(str(amount).strip())
    except (TypeError, ValueError) as exc:
        raise SharingOperationStateError("sharing ledger compensation amount is invalid") from exc
    if normalized_amount < 1:
        raise SharingOperationStateError("sharing ledger compensation amount is invalid")
    debit = (
        uow.session.execute(
            text(
                """
                SELECT delta, reason, reference, operation_id
                FROM sharing_credit_ledger
                WHERE account_id = :account_id AND idempotency_key = :idempotency_key
                FOR UPDATE
                """
            ),
            {
                "account_id": account_id,
                "idempotency_key": normalize_contribution_idempotency_key(debit_key),
            },
        )
        .mappings()
        .one_or_none()
    )
    if (
        debit is None
        or int(debit["delta"]) != -normalized_amount
        or str(debit["reason"]) != "transfer_escrow"
        or str(debit["reference"] or "") != reference
        or str(debit["operation_id"] or "") != str(operation.id)
    ):
        raise SharingOperationStateError("sharing ledger compensation has no matching transfer escrow debit")
    apply_contribution_in_uow(
        uow,
        account_id,
        normalized_amount,
        idempotency_key=f"compensation:{debit_key}",
        reason="transfer_escrow_released",
        reference=reference,
        operation_id=operation.id,
        metadata={"compensates": debit_key, "source_id": reference},
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
        cancel_requested_at=_nullable_time(row.get("cancel_requested_at")),
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


def _uuid(value: Any, field: str) -> UUID:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError) as exc:
        raise SharingOperationStateError(f"sharing {field} is invalid") from exc


def _positive_id(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise SharingOperationStateError(f"sharing {field} is invalid")
    try:
        normalized = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise SharingOperationStateError(f"sharing {field} is invalid") from exc
    if normalized < 1:
        raise SharingOperationStateError(f"sharing {field} is invalid")
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
