from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork

from .operations import (
    SUBSCRIPTION_OPERATION_AGGREGATE_TYPE,
    SUBSCRIPTION_OPERATION_TOPIC,
    SubscriptionOperation,
    SubscriptionOperationIntent,
    SubscriptionOperationKind,
    SubscriptionOperationReservation,
    SubscriptionOperationStatus,
    canonical_json,
    safe_durable_mapping,
)
from .runtime_repository import (
    PostgresSubscriptionRuntimeRepository,
    SubscriptionQuotaStateError,
    SubscriptionQuotaStatus,
    _lock_quota_date,
    _quota_key,
    _quota_reservation,
    _reserve_quota_in_uow,
)


class SubscriptionOperationStateError(RuntimeError):
    pass


class SubscriptionOperationClaimLostError(SubscriptionOperationStateError):
    pass


class PostgresSubscriptionOperationRepository:
    """Durable MoviePilot effect intent with quota and Outbox atomicity."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def reserve(self, intent: SubscriptionOperationIntent) -> SubscriptionOperationReservation:
        with self._unit_of_work() as uow:
            reservation = self.reserve_in_uow(uow, intent)
            uow.commit()
        return reservation

    @staticmethod
    def reserve_in_uow(
        uow: SqlAlchemyUnitOfWork,
        intent: SubscriptionOperationIntent,
    ) -> SubscriptionOperationReservation:
        return _reserve_operation_in_uow(uow, intent)

    def read(self, operation_id: UUID) -> SubscriptionOperation | None:
        row: Mapping[Any, Any] | None = None
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text("SELECT * FROM subscription_operations WHERE id = :id"),
                    {"id": operation_id},
                )
                .mappings()
                .first()
            )
        return _operation(row) if row is not None else None

    def claim_by_id(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> SubscriptionOperation | None:
        worker = str(worker_id or "").strip()
        if not worker:
            raise ValueError("subscription operation worker_id must not be empty")
        lease = float(lease_seconds)
        if not math.isfinite(lease) or lease <= 0:
            raise ValueError("subscription operation lease_seconds must be positive and finite")
        current_time = _utc(now)
        claim_token = uuid4()
        lease_expires_at = current_time + timedelta(seconds=lease)
        with self._unit_of_work() as uow:
            current_row = (
                uow.session.execute(
                    text("SELECT * FROM subscription_operations WHERE id = :id FOR UPDATE"),
                    {"id": operation_id},
                )
                .mappings()
                .first()
            )
            if current_row is None:
                return None
            current = _operation(current_row)
            if (
                current.status
                not in {
                    SubscriptionOperationStatus.READY,
                    SubscriptionOperationStatus.RETRYABLE,
                    SubscriptionOperationStatus.AWAITING_READBACK,
                }
                or current.available_at > current_time
                or (current.cancel_requested_at is not None and current.provider_request_started_at is None)
            ):
                return None
            if not _operation_quota_is_reserved(uow, operation_id):
                return None
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE subscription_operations
                        SET status = 'claimed',
                            state_revision = state_revision + 1,
                            attempts = attempts + 1,
                            claimed_by = :worker_id,
                            claim_token = :claim_token,
                            claimed_at = :now,
                            lease_expires_at = :lease_expires_at,
                            error_json = NULL,
                            updated_at = :now
                        WHERE id = :id AND state_revision = :state_revision
                        RETURNING *
                        """
                    ),
                    {
                        "id": operation_id,
                        "state_revision": current.state_revision,
                        "worker_id": worker,
                        "claim_token": claim_token,
                        "now": current_time,
                        "lease_expires_at": lease_expires_at,
                    },
                )
                .mappings()
                .first()
            )
            if row is None:
                return None
            _extend_operation_quota(uow, operation_id, lease_expires_at)
            _complete_outbox(uow, operation_id)
            uow.commit()
        return _operation(row) if row is not None else None

    def mark_requesting(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        now: datetime | None = None,
    ) -> SubscriptionOperation:
        current_time = _utc(now)
        with self._unit_of_work() as uow:
            current = _require_live_claim(
                uow,
                operation_id,
                claim_token,
                allowed=(SubscriptionOperationStatus.CLAIMED,),
                now=current_time,
            )
            if current.provider_request_started_at is not None:
                raise SubscriptionOperationStateError(
                    "subscription operation is in readback-only recovery and cannot resend the provider request"
                )
            row = _claimed_update(
                uow,
                operation_id,
                claim_token,
                """
                status = 'requesting',
                provider_request_started_at = COALESCE(provider_request_started_at, :now)
                """,
                {"now": current_time},
            )
            uow.commit()
        return _operation(row)

    def mark_awaiting_readback(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any] | None = None,
        retry_after_seconds: float = 0,
        now: datetime | None = None,
    ) -> SubscriptionOperation:
        delay = _nonnegative_seconds(retry_after_seconds, "retry_after_seconds")
        current_time = _utc(now)
        available_at = current_time + timedelta(seconds=delay)
        with self._unit_of_work() as uow:
            current = _require_live_claim(
                uow,
                operation_id,
                claim_token,
                allowed=(
                    SubscriptionOperationStatus.CLAIMED,
                    SubscriptionOperationStatus.REQUESTING,
                    SubscriptionOperationStatus.REMOTE_CONFIRMED,
                ),
                now=current_time,
                allow_cancel_requested=True,
            )
            if current.provider_request_started_at is None:
                raise SubscriptionOperationStateError(
                    "subscription operation cannot enter readback recovery before the provider request checkpoint"
                )
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE subscription_operations
                        SET status = 'awaiting_readback',
                            state_revision = state_revision + 1,
                            available_at = :available_at,
                            claimed_by = NULL,
                            claim_token = NULL,
                            claimed_at = NULL,
                            lease_expires_at = NULL,
                            error_json = CAST(:error_json AS jsonb),
                            updated_at = :now
                        WHERE id = :id AND claim_token = :claim_token
                        RETURNING *
                        """
                    ),
                    {
                        "id": operation_id,
                        "claim_token": claim_token,
                        "available_at": available_at,
                        "error_json": _mapping_json(error, field="subscription operation readback error"),
                        "now": current_time,
                    },
                )
                .mappings()
                .one()
            )
            operation = _operation(row)
            _extend_operation_quota(uow, operation_id, available_at + timedelta(seconds=1))
            _stage_outbox(uow, operation)
            uow.commit()
        return operation

    def mark_remote_confirmed(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        receipt: Mapping[str, Any],
        now: datetime | None = None,
    ) -> SubscriptionOperation:
        current_time = _utc(now)
        with self._unit_of_work() as uow:
            current = _require_live_claim(
                uow,
                operation_id,
                claim_token,
                allowed=(
                    SubscriptionOperationStatus.CLAIMED,
                    SubscriptionOperationStatus.REQUESTING,
                ),
                now=current_time,
                allow_cancel_requested=True,
            )
            if current.provider_request_started_at is None:
                raise SubscriptionOperationStateError(
                    "subscription operation cannot confirm a remote result before the provider request checkpoint"
                )
            row = _claimed_update(
                uow,
                operation_id,
                claim_token,
                "status = 'remote_confirmed', provider_receipt_json = CAST(:receipt_json AS jsonb)",
                {"receipt_json": _mapping_json(receipt, field="subscription operation provider receipt")},
            )
            uow.commit()
        return _operation(row)

    def mark_retryable(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
        retry_after_seconds: float,
        now: datetime | None = None,
    ) -> SubscriptionOperation:
        delay = _nonnegative_seconds(retry_after_seconds, "retry_after_seconds")
        current_time = _utc(now)
        available_at = current_time + timedelta(seconds=delay)
        with self._unit_of_work() as uow:
            current = _require_live_claim(
                uow,
                operation_id,
                claim_token,
                allowed=(SubscriptionOperationStatus.CLAIMED,),
                now=current_time,
            )
            if current.provider_request_started_at is not None:
                raise SubscriptionOperationClaimLostError(
                    "subscription operation provider request started; only authoritative readback recovery is allowed"
                )
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE subscription_operations
                        SET status = 'retryable',
                            state_revision = state_revision + 1,
                            available_at = :available_at,
                            claimed_by = NULL,
                            claim_token = NULL,
                            claimed_at = NULL,
                            lease_expires_at = NULL,
                            error_json = CAST(:error_json AS jsonb),
                            updated_at = :now
                        WHERE id = :id AND claim_token = :claim_token
                        RETURNING *
                        """
                    ),
                    {
                        "id": operation_id,
                        "claim_token": claim_token,
                        "available_at": available_at,
                        "error_json": _mapping_json(error, field="subscription operation retry error"),
                        "now": current_time,
                    },
                )
                .mappings()
                .one()
            )
            operation = _operation(row)
            _extend_operation_quota(uow, operation_id, available_at + timedelta(seconds=1))
            _stage_outbox(uow, operation)
            uow.commit()
        assert current.id == operation.id
        return operation

    def mark_confirmed(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        result: Mapping[str, Any],
        readback: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> SubscriptionOperation:
        return self._mark_terminal(
            operation_id,
            claim_token,
            status=SubscriptionOperationStatus.CONFIRMED,
            result=result,
            readback=readback,
            error=None,
            quota_target=SubscriptionQuotaStatus.COMMITTED,
            now=_utc(now),
        )

    def mark_cancelled_before_write(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        evidence: Mapping[str, Any],
        now: datetime | None = None,
    ) -> SubscriptionOperation:
        return self._mark_terminal(
            operation_id,
            claim_token,
            status=SubscriptionOperationStatus.CANCELLED,
            result=None,
            readback=evidence,
            error={"type": "cancelled_before_provider_write"},
            quota_target=SubscriptionQuotaStatus.RELEASED,
            now=_utc(now),
        )

    def mark_rejected(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
        now: datetime | None = None,
    ) -> SubscriptionOperation:
        return self._mark_terminal(
            operation_id,
            claim_token,
            status=SubscriptionOperationStatus.REJECTED,
            result=None,
            readback=None,
            error=error,
            quota_target=SubscriptionQuotaStatus.RELEASED,
            now=_utc(now),
        )

    def mark_indeterminate(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
        readback: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> SubscriptionOperation:
        return self._mark_terminal(
            operation_id,
            claim_token,
            status=SubscriptionOperationStatus.INDETERMINATE,
            result=None,
            readback=readback,
            error=error,
            quota_target=SubscriptionQuotaStatus.COMMITTED,
            now=_utc(now),
        )

    def is_cancel_requested(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        now: datetime | None = None,
    ) -> bool:
        current_time = _utc(now)
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT cancel_requested_at, claim_token, lease_expires_at
                        FROM subscription_operations
                        WHERE id = :id
                        """
                    ),
                    {"id": operation_id},
                )
                .mappings()
                .first()
            )
        return (
            row is None
            or row["claim_token"] != claim_token
            or row["lease_expires_at"] is None
            or row["lease_expires_at"] <= current_time
            or row["cancel_requested_at"] is not None
        )

    def request_cancel(
        self,
        operation_id: UUID,
        *,
        now: datetime | None = None,
    ) -> SubscriptionOperation:
        current_time = _utc(now)
        with self._unit_of_work() as uow:
            current_row = (
                uow.session.execute(
                    text("SELECT * FROM subscription_operations WHERE id = :id FOR UPDATE"),
                    {"id": operation_id},
                )
                .mappings()
                .first()
            )
            if current_row is None:
                raise SubscriptionOperationStateError("subscription operation was not found")
            current = _operation(current_row)
            if current.terminal:
                return current
            if current.provider_request_started_at is None:
                row = (
                    uow.session.execute(
                        text(
                            """
                            UPDATE subscription_operations
                            SET status = 'cancelled',
                                state_revision = state_revision + 1,
                                claimed_by = NULL,
                                claim_token = NULL,
                                claimed_at = NULL,
                                lease_expires_at = NULL,
                                cancel_requested_at = COALESCE(cancel_requested_at, :now),
                                error_json = CAST(:error_json AS jsonb),
                                finished_at = :now,
                                updated_at = :now
                            WHERE id = :id
                            RETURNING *
                            """
                        ),
                        {
                            "id": operation_id,
                            "now": current_time,
                            "error_json": _mapping_json(
                                {"type": "cancel_requested_before_provider_request"},
                                field="subscription operation cancellation error",
                            ),
                        },
                    )
                    .mappings()
                    .one()
                )
                _transition_operation_quota(
                    uow,
                    operation_id,
                    SubscriptionQuotaStatus.RELEASED,
                    current_time,
                )
                _complete_outbox(uow, operation_id)
            else:
                row = (
                    uow.session.execute(
                        text(
                            """
                            UPDATE subscription_operations
                            SET cancel_requested_at = COALESCE(cancel_requested_at, :now),
                                state_revision = state_revision + 1,
                                updated_at = :now
                            WHERE id = :id
                            RETURNING *
                            """
                        ),
                        {"id": operation_id, "now": current_time},
                    )
                    .mappings()
                    .one()
                )
            operation = _operation(row)
            uow.commit()
        return operation

    def expire_stale_claims(
        self,
        *,
        limit: int = 100,
        retry_delay_seconds: float = 0,
        now: datetime | None = None,
    ) -> int:
        bounded_limit = _positive_int(limit, "limit")
        delay = _nonnegative_seconds(retry_delay_seconds, "retry_delay_seconds")
        current_time = _utc(now)
        available_at = current_time + timedelta(seconds=delay)
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT *
                        FROM subscription_operations
                        WHERE status IN ('claimed','requesting','remote_confirmed')
                          AND claim_token IS NOT NULL
                          AND lease_expires_at <= :now
                        ORDER BY lease_expires_at, event_sequence
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED
                        """
                    ),
                    {"now": current_time, "limit": bounded_limit},
                )
                .mappings()
                .all()
            )
            for stale in rows:
                previous = _operation(stale)
                provider_started = previous.provider_request_started_at is not None
                cancelled = previous.cancel_requested_at is not None and not provider_started
                if cancelled:
                    target = SubscriptionOperationStatus.CANCELLED
                    error = {"type": "cancel_requested_before_provider_request"}
                elif provider_started:
                    target = SubscriptionOperationStatus.AWAITING_READBACK
                    error = {
                        "type": "claim_lease_expired",
                        "recovered_from": previous.status.value,
                        "remote_request_may_have_started": True,
                    }
                else:
                    target = SubscriptionOperationStatus.RETRYABLE
                    error = {
                        "type": "claim_lease_expired",
                        "recovered_from": previous.status.value,
                        "remote_request_may_have_started": False,
                    }
                updated = (
                    uow.session.execute(
                        text(
                            """
                            UPDATE subscription_operations
                            SET status = :status,
                                state_revision = state_revision + 1,
                                available_at = :available_at,
                                claimed_by = NULL,
                                claim_token = NULL,
                                claimed_at = NULL,
                                lease_expires_at = NULL,
                                error_json = CAST(:error_json AS jsonb),
                                finished_at = CASE WHEN :status = 'cancelled' THEN :now ELSE NULL END,
                                updated_at = :now
                            WHERE id = :id AND claim_token = :claim_token
                            RETURNING *
                            """
                        ),
                        {
                            "id": previous.id,
                            "claim_token": previous.claim_token,
                            "status": target.value,
                            "available_at": available_at,
                            "error_json": _mapping_json(
                                error,
                                field="subscription operation stale-claim error",
                            ),
                            "now": current_time,
                        },
                    )
                    .mappings()
                    .one()
                )
                operation = _operation(updated)
                if cancelled:
                    _transition_operation_quota(
                        uow,
                        operation.id,
                        SubscriptionQuotaStatus.RELEASED,
                        current_time,
                    )
                    _complete_outbox(uow, operation.id)
                else:
                    _extend_operation_quota(uow, operation.id, available_at + timedelta(seconds=1))
                    _stage_outbox(uow, operation)
            uow.commit()
        return len(rows)

    def reject_dead_dispatches(
        self,
        *,
        limit: int = 100,
        now: datetime | None = None,
    ) -> int:
        bounded_limit = _positive_int(limit, "limit")
        current_time = _utc(now)
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT operation.*
                        FROM subscription_operations AS operation
                        JOIN outbox_events AS event
                          ON event.event_key = :topic || ':' || operation.id::text
                         AND event.topic = :topic
                        WHERE event.status = 'dead'
                          AND operation.status IN ('ready','retryable','awaiting_readback')
                          AND operation.claim_token IS NULL
                        ORDER BY operation.event_sequence
                        LIMIT :limit
                        FOR UPDATE OF operation SKIP LOCKED
                        """
                    ),
                    {"topic": SUBSCRIPTION_OPERATION_TOPIC, "limit": bounded_limit},
                )
                .mappings()
                .all()
            )
            for row in rows:
                operation = _operation(row)
                remote_may_exist = operation.provider_request_started_at is not None
                status = (
                    SubscriptionOperationStatus.INDETERMINATE
                    if remote_may_exist
                    else SubscriptionOperationStatus.REJECTED
                )
                updated = uow.session.execute(
                    text(
                        """
                            UPDATE subscription_operations
                            SET status = :status,
                                state_revision = state_revision + 1,
                                error_json = CAST(:error_json AS jsonb),
                                finished_at = :now,
                                updated_at = :now
                            WHERE id = :id AND state_revision = :state_revision
                            RETURNING id
                            """
                    ),
                    {
                        "id": operation.id,
                        "state_revision": operation.state_revision,
                        "status": status.value,
                        "error_json": _mapping_json(
                            {
                                "type": "subscription_operation_dispatch_dead",
                                "remote_request_may_have_started": remote_may_exist,
                            },
                            field="subscription operation dead-dispatch error",
                        ),
                        "now": current_time,
                    },
                ).first()
                if updated is None:
                    continue
                _transition_operation_quota(
                    uow,
                    operation.id,
                    (SubscriptionQuotaStatus.COMMITTED if remote_may_exist else SubscriptionQuotaStatus.RELEASED),
                    current_time,
                )
            uow.commit()
        return len(rows)

    def _mark_terminal(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        status: SubscriptionOperationStatus,
        result: Mapping[str, Any] | None,
        readback: Mapping[str, Any] | None,
        error: Mapping[str, Any] | None,
        quota_target: SubscriptionQuotaStatus,
        now: datetime,
    ) -> SubscriptionOperation:
        with self._unit_of_work() as uow:
            current = _require_live_claim(
                uow,
                operation_id,
                claim_token,
                allowed=(
                    SubscriptionOperationStatus.CLAIMED,
                    SubscriptionOperationStatus.REQUESTING,
                    SubscriptionOperationStatus.AWAITING_READBACK,
                    SubscriptionOperationStatus.REMOTE_CONFIRMED,
                ),
                now=now,
                allow_cancel_requested=True,
            )
            if status is SubscriptionOperationStatus.CANCELLED and current.provider_request_started_at is not None:
                raise SubscriptionOperationStateError(
                    "subscription operation provider request started; cancellation requires authoritative readback"
                )
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE subscription_operations
                        SET status = :status,
                            state_revision = state_revision + 1,
                            claimed_by = NULL,
                            claim_token = NULL,
                            claimed_at = NULL,
                            lease_expires_at = NULL,
                            readback_json = CAST(:readback_json AS jsonb),
                            result_json = CAST(:result_json AS jsonb),
                            error_json = CAST(:error_json AS jsonb),
                            finished_at = :now,
                            updated_at = :now
                        WHERE id = :id AND claim_token = :claim_token
                        RETURNING *
                        """
                    ),
                    {
                        "id": operation_id,
                        "claim_token": claim_token,
                        "status": status.value,
                        "readback_json": _mapping_json(
                            readback,
                            field="subscription operation readback",
                        ),
                        "result_json": _mapping_json(
                            result,
                            field="subscription operation result",
                        ),
                        "error_json": _mapping_json(
                            error,
                            field="subscription operation terminal error",
                        ),
                        "now": now,
                    },
                )
                .mappings()
                .one()
            )
            _transition_operation_quota(uow, operation_id, quota_target, now)
            _complete_outbox(uow, operation_id)
            uow.commit()
        return _operation(row)


def _reserve_operation_in_uow(
    uow: SqlAlchemyUnitOfWork,
    intent: SubscriptionOperationIntent,
) -> SubscriptionOperationReservation:
    request_json = canonical_json(dict(intent.request))
    row = (
        uow.session.execute(
            text(
                """
                INSERT INTO subscription_operations (
                    id,
                    operation_key,
                    request_hash,
                    kind,
                    scope_key,
                    request_json,
                    status,
                    state_revision,
                    attempts,
                    available_at,
                    created_at,
                    updated_at
                ) VALUES (
                    :id,
                    :operation_key,
                    :request_hash,
                    :kind,
                    :scope_key,
                    CAST(:request_json AS jsonb),
                    'ready',
                    0,
                    0,
                    :available_at,
                    NOW(),
                    NOW()
                )
                ON CONFLICT (operation_key) DO NOTHING
                RETURNING *
                """
            ),
            {
                "id": intent.id,
                "operation_key": intent.operation_key,
                "request_hash": intent.request_hash,
                "kind": intent.kind.value,
                "scope_key": intent.scope_key,
                "request_json": request_json,
                "available_at": intent.available_at,
            },
        )
        .mappings()
        .first()
    )
    created = row is not None
    if row is None:
        row = (
            uow.session.execute(
                text(
                    """
                    SELECT *
                    FROM subscription_operations
                    WHERE operation_key = :operation_key
                    FOR UPDATE
                    """
                ),
                {"operation_key": intent.operation_key},
            )
            .mappings()
            .one()
        )
    operation = _operation(row)
    _assert_same_intent(operation, intent)

    quota_row = (
        uow.session.execute(
            text(
                """
                SELECT *
                FROM subscription_quota_reservations
                WHERE operation_id = :operation_id
                FOR UPDATE
                """
            ),
            {"operation_id": operation.id},
        )
        .mappings()
        .first()
    )
    quota_reservation_id: UUID | None = None
    if intent.quota is None:
        if quota_row is not None:
            raise SubscriptionOperationStateError(
                "subscription operation quota requirement cannot be removed on replay"
            )
    elif quota_row is not None:
        quota = intent.quota
        reservation = _quota_reservation(quota_row)
        expected_key = _quota_key(intent.operation_key, quota.quota_date)
        if reservation.reservation_key != expected_key or reservation.units != quota.units:
            raise SubscriptionOperationStateError(
                "subscription operation quota requirement conflicts with its original reservation"
            )
        quota_reservation_id = reservation.id
    elif not created:
        raise SubscriptionOperationStateError("subscription operation quota requirement cannot be added on replay")
    else:
        quota = intent.quota
        current_time = datetime.now(UTC)
        reservation = _reserve_quota_in_uow(
            uow,
            reservation_key=_quota_key(intent.operation_key, quota.quota_date),
            quota_date=quota.quota_date,
            daily_cap=quota.daily_cap,
            units=quota.units,
            expires_at=max(current_time, intent.available_at) + timedelta(seconds=float(quota.ttl_seconds)),
            operation_id=operation.id,
            now=current_time,
        )
        quota_reservation_id = reservation.id
    if created:
        _stage_outbox(uow, operation)
    return SubscriptionOperationReservation(operation, created, quota_reservation_id)


def _require_live_claim(
    uow: SqlAlchemyUnitOfWork,
    operation_id: UUID,
    claim_token: UUID,
    *,
    allowed: Sequence[SubscriptionOperationStatus],
    now: datetime,
    allow_cancel_requested: bool = False,
) -> SubscriptionOperation:
    row = (
        uow.session.execute(
            text(
                """
                SELECT *, lease_expires_at > :now AS lease_active
                FROM subscription_operations
                WHERE id = :id
                FOR UPDATE
                """
            ),
            {"id": operation_id, "now": now},
        )
        .mappings()
        .first()
    )
    if row is None:
        raise SubscriptionOperationClaimLostError("subscription operation was not found")
    operation = _operation(row)
    if operation.status not in set(allowed) or operation.claim_token != claim_token or not bool(row["lease_active"]):
        raise SubscriptionOperationClaimLostError("subscription operation lost its active claim")
    if operation.cancel_requested_at is not None and not allow_cancel_requested:
        raise SubscriptionOperationClaimLostError("subscription operation cancellation was requested")
    return operation


def _claimed_update(
    uow: SqlAlchemyUnitOfWork,
    operation_id: UUID,
    claim_token: UUID,
    assignments: str,
    values: Mapping[str, Any],
) -> Mapping[Any, Any]:
    return (
        uow.session.execute(
            text(
                f"""
                UPDATE subscription_operations
                SET {assignments},
                    state_revision = state_revision + 1,
                    updated_at = NOW()
                WHERE id = :id AND claim_token = :claim_token
                RETURNING *
                """
            ),
            {"id": operation_id, "claim_token": claim_token, **dict(values)},
        )
        .mappings()
        .one()
    )


def _transition_operation_quota(
    uow: SqlAlchemyUnitOfWork,
    operation_id: UUID,
    target: SubscriptionQuotaStatus,
    now: datetime,
) -> None:
    quota_day = uow.session.scalar(
        text("SELECT quota_date FROM subscription_quota_reservations WHERE operation_id = :operation_id"),
        {"operation_id": operation_id},
    )
    if quota_day is None:
        return
    _lock_quota_date(uow, quota_day)
    row = (
        uow.session.execute(
            text(
                """
                SELECT *
                FROM subscription_quota_reservations
                WHERE operation_id = :operation_id
                FOR UPDATE
                """
            ),
            {"operation_id": operation_id},
        )
        .mappings()
        .one()
    )
    reservation = _quota_reservation(row)
    if reservation.status is target:
        return
    if reservation.status is not SubscriptionQuotaStatus.RESERVED:
        raise SubscriptionQuotaStateError(
            f"quota reservation cannot transition from {reservation.status.value} to {target.value}"
        )
    timestamp_column = "committed_at" if target is SubscriptionQuotaStatus.COMMITTED else "released_at"
    uow.session.execute(
        text(
            f"""
            UPDATE subscription_quota_reservations
            SET status = :status,
                state_revision = state_revision + 1,
                {timestamp_column} = :now,
                updated_at = :now
            WHERE id = :id AND status = 'reserved'
            """
        ),
        {"id": reservation.id, "status": target.value, "now": now},
    )
    cap = PostgresSubscriptionRuntimeRepository._projected_cap_for_date(
        uow,
        quota_day,
        fallback=reservation.cap_snapshot,
    )
    availability = PostgresSubscriptionRuntimeRepository._availability_in_uow(uow, quota_day, cap)
    PostgresSubscriptionRuntimeRepository._sync_legacy_projection(
        uow,
        quota_day,
        cap,
        availability.reserved_units,
        availability.committed_units,
    )


def _operation_quota_is_reserved(
    uow: SqlAlchemyUnitOfWork,
    operation_id: UUID,
) -> bool:
    quota_day = uow.session.scalar(
        text("SELECT quota_date FROM subscription_quota_reservations WHERE operation_id = :operation_id"),
        {"operation_id": operation_id},
    )
    if quota_day is None:
        return True
    _lock_quota_date(uow, quota_day)
    status = uow.session.scalar(
        text(
            """
            SELECT status
            FROM subscription_quota_reservations
            WHERE operation_id = :operation_id
            FOR UPDATE
            """
        ),
        {"operation_id": operation_id},
    )
    return status == SubscriptionQuotaStatus.RESERVED.value


def _extend_operation_quota(
    uow: SqlAlchemyUnitOfWork,
    operation_id: UUID,
    expires_at: datetime,
) -> None:
    quota_day = uow.session.scalar(
        text("SELECT quota_date FROM subscription_quota_reservations WHERE operation_id = :operation_id"),
        {"operation_id": operation_id},
    )
    if quota_day is None:
        return
    _lock_quota_date(uow, quota_day)
    uow.session.execute(
        text(
            """
            UPDATE subscription_quota_reservations
            SET expires_at = GREATEST(expires_at, :expires_at),
                updated_at = CASE WHEN expires_at < :expires_at THEN NOW() ELSE updated_at END
            WHERE operation_id = :operation_id AND status = 'reserved'
            """
        ),
        {"operation_id": operation_id, "expires_at": expires_at},
    )


def _stage_outbox(uow: SqlAlchemyUnitOfWork, operation: SubscriptionOperation) -> None:
    payload = {
        "operation_id": str(operation.id),
        "sequence": operation.sequence,
        "operation_key": operation.operation_key,
        "request_hash": operation.request_hash.hex(),
    }
    uow.session.execute(
        text(
            """
            INSERT INTO outbox_events (
                id, event_key, topic, aggregate_type, aggregate_id,
                payload_version, payload_json, status, available_at
            ) VALUES (
                :id, :event_key, :topic, :aggregate_type, :aggregate_id,
                1, CAST(:payload_json AS jsonb), 'pending', :available_at
            )
            ON CONFLICT (event_key) DO UPDATE SET
                status = 'pending', available_at = EXCLUDED.available_at,
                published_at = NULL, processed_at = NULL,
                claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                last_error = NULL, updated_at = NOW()
            WHERE outbox_events.status <> 'dead'
            """
        ),
        {
            "id": uuid4(),
            "event_key": f"{SUBSCRIPTION_OPERATION_TOPIC}:{operation.id}",
            "topic": SUBSCRIPTION_OPERATION_TOPIC,
            "aggregate_type": SUBSCRIPTION_OPERATION_AGGREGATE_TYPE,
            "aggregate_id": str(operation.id),
            "payload_json": canonical_json(payload),
            "available_at": operation.available_at,
        },
    )


def _complete_outbox(uow: SqlAlchemyUnitOfWork, operation_id: UUID) -> None:
    uow.session.execute(
        text(
            """
            UPDATE outbox_events
            SET status = 'processed',
                processed_at = COALESCE(processed_at, NOW()),
                claimed_by = NULL,
                claim_token = NULL,
                claimed_at = NULL,
                updated_at = NOW()
            WHERE event_key = :event_key AND status <> 'dead'
            """
        ),
        {"event_key": f"{SUBSCRIPTION_OPERATION_TOPIC}:{operation_id}"},
    )


def _assert_same_intent(
    operation: SubscriptionOperation,
    intent: SubscriptionOperationIntent,
) -> None:
    if (
        operation.id != intent.id
        or operation.request_hash != intent.request_hash
        or operation.kind is not intent.kind
        or operation.scope_key != intent.scope_key
    ):
        raise SubscriptionOperationStateError("subscription operation idempotency identity conflict")


def _mapping_json(value: Mapping[str, Any] | None, *, field: str) -> str | None:
    if value is None:
        return None
    return canonical_json(safe_durable_mapping(value, field=field))


def _operation(row: Mapping[Any, Any]) -> SubscriptionOperation:
    return SubscriptionOperation(
        id=row["id"],
        sequence=int(row["event_sequence"]),
        operation_key=str(row["operation_key"]),
        request_hash=bytes(row["request_hash"]),
        kind=SubscriptionOperationKind(str(row["kind"])),
        scope_key=str(row["scope_key"]),
        request=dict(row["request_json"]),
        status=SubscriptionOperationStatus(str(row["status"])),
        state_revision=int(row["state_revision"]),
        attempts=int(row["attempts"]),
        available_at=row["available_at"],
        claimed_by=row["claimed_by"],
        claim_token=row["claim_token"],
        claimed_at=row["claimed_at"],
        lease_expires_at=row["lease_expires_at"],
        cancel_requested_at=row["cancel_requested_at"],
        provider_request_started_at=row["provider_request_started_at"],
        provider_receipt=dict(row["provider_receipt_json"]) if row["provider_receipt_json"] is not None else None,
        readback=dict(row["readback_json"]) if row["readback_json"] is not None else None,
        result=dict(row["result_json"]) if row["result_json"] is not None else None,
        error=dict(row["error_json"]) if row["error_json"] is not None else None,
        finished_at=row["finished_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _utc(value: datetime | None) -> datetime:
    timestamp = value or datetime.now(UTC)
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("subscription operation timestamp must be timezone-aware")
    return timestamp.astimezone(UTC)


def _positive_int(value: int, name: str) -> int:
    normalized = int(value)
    if normalized < 1:
        raise ValueError(f"{name} must be positive")
    return normalized


def _nonnegative_seconds(value: float, name: str) -> float:
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{name} must be non-negative and finite")
    return normalized


__all__ = [
    "PostgresSubscriptionOperationRepository",
    "SubscriptionOperationClaimLostError",
    "SubscriptionOperationStateError",
]
