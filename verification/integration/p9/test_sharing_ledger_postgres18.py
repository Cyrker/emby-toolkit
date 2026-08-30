from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event
from typing import cast
from uuid import UUID

import pytest
from sqlalchemy import text

from etk.domains.sharing import (
    InsufficientContributionError,
    PostgresContributionLedger,
    SharingDeliveryOutcome,
    SharingLedgerService,
    SharingOperation,
    SharingOperationExecutor,
)
from etk.domains.sharing.operations import (
    SharingOperationIntent,
    SharingOperationKind,
    SharingOperationStateError,
)
from etk.domains.sharing.repository import PostgresSharingOperationRepository


class _Delivery:
    def __init__(
        self,
        requests: list[SharingDeliveryOutcome],
        *,
        readbacks: list[SharingDeliveryOutcome] | None = None,
        supports_readback: bool = True,
        checkpoint: bool = True,
    ) -> None:
        self.requests = list(requests)
        self.readbacks = list(readbacks or [])
        self.supports_readback = supports_readback
        self.checkpoint = checkpoint
        self.request_calls = 0
        self.readback_calls = 0

    def supports_authoritative_readback(self, operation: SharingOperation) -> bool:
        del operation
        return self.supports_readback

    def request(
        self,
        operation: SharingOperation,
        *,
        checkpoint: Callable[[], None],
    ) -> SharingDeliveryOutcome:
        del operation
        self.request_calls += 1
        if self.checkpoint:
            checkpoint()
        return self.requests.pop(0)

    def readback(self, operation: SharingOperation) -> SharingDeliveryOutcome:
        del operation
        self.readback_calls += 1
        return self.readbacks.pop(0)


class _MalformedDelivery(_Delivery):
    def request(
        self,
        operation: SharingOperation,
        *,
        checkpoint: Callable[[], None],
    ) -> SharingDeliveryOutcome:
        del operation
        self.request_calls += 1
        checkpoint()
        return cast(SharingDeliveryOutcome, object())


class _BlockingDelivery(_Delivery):
    def __init__(self) -> None:
        super().__init__([])
        self.entered = Event()
        self.release = Event()

    def request(
        self,
        operation: SharingOperation,
        *,
        checkpoint: Callable[[], None],
    ) -> SharingDeliveryOutcome:
        del operation
        self.request_calls += 1
        checkpoint()
        self.entered.set()
        if not self.release.wait(5):
            raise TimeoutError("concurrent sharing delivery test was not released")
        return SharingDeliveryOutcome.confirmed({"receipt_id": "concurrent"})


def _sharing_operation(
    repository: PostgresSharingOperationRepository,
    suffix: str,
    *,
    available_at: datetime | None = None,
) -> SharingOperation:
    return repository.reserve(
        SharingOperationIntent.create(
            kind=SharingOperationKind.TRANSFER,
            scope_key=f"source:executor:{suffix}",
            principal_id=f"device-executor-{suffix}",
            request={"source_id": f"executor-{suffix}", "amount": 1},
            idempotency_key=f"executor-{suffix}",
            available_at=available_at,
        )
    )


def test_postgres_concurrent_debit_and_replay_are_atomic(p9_runtime) -> None:
    ledger = PostgresContributionLedger(p9_runtime.unit_of_work)
    ledger.ensure_account("device-1", 100)

    def debit(index: int) -> bool:
        try:
            ledger.debit(
                "device-1",
                10,
                idempotency_key=f"debit-{index}",
                reason="share_request_escrow",
            )
            return True
        except Exception:
            return False

    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(debit, range(20)))

    assert sum(results) == 10
    assert ledger.balance("device-1") == 0
    replay = ledger.debit(
        "device-1",
        10,
        idempotency_key="debit-0",
        reason="share_request_escrow",
    )
    assert replay.delta == -10
    assert len(ledger.entries("device-1", limit=100)) == 10


def test_postgres_transfer_operation_ledger_and_outbox_share_one_uow(p9_runtime) -> None:
    ledger = PostgresContributionLedger(p9_runtime.unit_of_work)
    operations = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    ledger.ensure_account("device-transfer", 5)
    service = SharingLedgerService(
        ledger,
        operations,
        unit_of_work=p9_runtime.unit_of_work,
    )

    result = service.reserve_transfer(
        account_id="device-transfer",
        source_id="source-transfer",
        amount=3,
        principal_id="principal-transfer",
        idempotency_key="transfer-uow",
    )

    assert result.status_code == 202
    assert ledger.balance("device-transfer") == 2
    operation_id = UUID(result.payload["operation_id"])
    operation = operations.read(operation_id)
    assert operation is not None
    assert operation.request == {
        "provider": "shared_center",
        "action": "acquire_transfer_lease",
        "payload": {
            "account_id": "device-transfer",
            "source_id": "source-transfer",
            "amount": 3,
        },
    }
    with p9_runtime.engine.connect() as connection:
        outbox = connection.execute(
            text("SELECT status FROM outbox_events WHERE aggregate_id = :operation_id"),
            {"operation_id": str(operation_id)},
        ).scalar_one()
    assert outbox == "pending"


def test_postgres_transfer_reservation_rolls_back_when_ledger_rejects(p9_runtime) -> None:
    ledger = PostgresContributionLedger(p9_runtime.unit_of_work)
    operations = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    service = SharingLedgerService(
        ledger,
        operations,
        unit_of_work=p9_runtime.unit_of_work,
    )

    with pytest.raises(InsufficientContributionError):
        service.reserve_transfer(
            account_id="device-no-credit",
            source_id="source-no-credit",
            amount=1,
            principal_id="principal-no-credit",
            idempotency_key="transfer-no-credit",
        )

    with p9_runtime.engine.connect() as connection:
        counts = connection.execute(
            text(
                """
                SELECT
                    (SELECT COUNT(*) FROM sharing_operations
                     WHERE scope_key = 'account:device-no-credit:source:source-no-credit'),
                    (SELECT COUNT(*) FROM sharing_credit_ledger
                     WHERE account_id = 'device-no-credit'),
                    (SELECT COUNT(*) FROM outbox_events
                     WHERE aggregate_type = 'sharing-operation'
                       AND aggregate_id IN (
                           SELECT id::text FROM sharing_operations
                           WHERE scope_key = 'account:device-no-credit:source:source-no-credit'
                       ))
                """
            )
        ).one()
    assert tuple(counts) == (0, 0, 0)


def test_postgres_sharing_operation_stages_identifier_only_outbox(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = repository.reserve(
        SharingOperationIntent.create(
            kind=SharingOperationKind.TRANSFER,
            scope_key="source:source-1",
            principal_id="device-1",
            request={"source_id": "source-1", "amount": 5},
            idempotency_key="transfer-1",
        )
    )
    replay = repository.reserve(
        SharingOperationIntent.create(
            kind=SharingOperationKind.TRANSFER,
            scope_key="source:source-1",
            principal_id="device-1",
            request={"source_id": "source-1", "amount": 5},
            idempotency_key="transfer-1",
        )
    )
    assert replay == operation
    with p9_runtime.engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT payload_json
                FROM outbox_events
                WHERE aggregate_id = :operation_id
                """
            ),
            {"operation_id": str(operation.id)},
        ).scalar_one()
    assert set(row) == {"operation_id", "operation_key", "request_hash", "sequence"}


def test_postgres_sharing_operation_outbox_claim_retry_expiry_and_terminal_lifecycle(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = repository.reserve(
        SharingOperationIntent.create(
            kind=SharingOperationKind.TRANSFER,
            scope_key="source:lifecycle",
            principal_id="device-lifecycle",
            request={"source_id": "lifecycle", "amount": 1},
            idempotency_key="lifecycle-1",
        )
    )

    def outbox_status() -> tuple[str, object]:
        with p9_runtime.engine.connect() as connection:
            return connection.execute(
                text("SELECT status, processed_at FROM outbox_events WHERE aggregate_id = :operation_id"),
                {"operation_id": str(operation.id)},
            ).one()

    status, processed_at = outbox_status()
    assert status == "pending"
    assert processed_at is None

    claimed = repository.claim_by_id(operation.id, worker_id="worker-lifecycle", lease_seconds=60)
    assert claimed is not None
    assert claimed.claim_token is not None
    status, processed_at = outbox_status()
    assert status == "processed"
    assert processed_at is not None

    retried = repository.mark_retryable(
        operation.id,
        claimed.claim_token,
        error={"type": "temporary"},
        retry_after_seconds=0,
    )
    assert retried.status.value == "retryable"
    status, processed_at = outbox_status()
    assert status == "pending"
    assert processed_at is None

    reclaimed = repository.claim_by_id(operation.id, worker_id="worker-lifecycle-2", lease_seconds=1)
    assert reclaimed is not None
    assert reclaimed.claim_token is not None
    expired_at = (reclaimed.lease_expires_at or datetime.now(UTC)) + timedelta(seconds=1)
    assert repository.expire_stale_claims(now=expired_at) == 1
    expired = repository.read(operation.id)
    assert expired is not None
    assert expired.error == {
        "type": "sharing_claim_expired",
        "provider_may_have_been_called": False,
        "attempts": 2,
    }
    status, processed_at = outbox_status()
    assert status == "pending"
    assert processed_at is None

    terminal = repository.claim_by_id(operation.id, worker_id="worker-lifecycle-3", lease_seconds=60, now=expired_at)
    assert terminal is not None
    assert terminal.claim_token is not None
    repository.mark_indeterminate(
        operation.id,
        terminal.claim_token,
        error={"type": "provider_unknown"},
    )
    status, processed_at = outbox_status()
    assert status == "processed"
    assert processed_at is not None


def test_postgres_stale_requesting_sharing_operation_is_not_replayed(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    claim_time = datetime.now(UTC) - timedelta(seconds=5)
    operation = _sharing_operation(
        repository,
        "requesting-expiry",
        available_at=claim_time,
    )
    claimed = repository.claim_by_id(
        operation.id,
        worker_id="worker-requesting-expiry",
        lease_seconds=1,
        now=claim_time,
    )
    assert claimed is not None
    assert claimed.claim_token is not None
    repository.mark_requesting(operation.id, claimed.claim_token, now=claim_time)

    expired_at = datetime.now(UTC)
    assert repository.expire_stale_claims(now=expired_at) == 0
    assert repository.requeue_stale_readback_claims(now=expired_at) == 1

    delivery = _Delivery([], supports_readback=False)
    execution = SharingOperationExecutor(repository, delivery).execute(
        operation.id,
        worker_id="replacement-worker",
        lease_seconds=60,
    )
    assert execution is not None
    assert execution.operation.status.value == "indeterminate"
    assert execution.operation.error == {
        "type": "authoritative_readback_unavailable",
        "provider_may_have_been_called": True,
    }
    assert delivery.request_calls == 0
    assert delivery.readback_calls == 0
    with p9_runtime.engine.connect() as connection:
        outbox = connection.execute(
            text(
                """
                SELECT status, processed_at
                FROM outbox_events
                WHERE aggregate_id = :operation_id
                """
            ),
            {"operation_id": str(operation.id)},
        ).one()
    assert outbox[0] == "processed"
    assert outbox[1] is not None


def test_postgres_sharing_executor_confirms_only_after_provider_checkpoint(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "confirmed")
    delivery = _Delivery([SharingDeliveryOutcome.confirmed({"receipt_id": "receipt-1"})])

    execution = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0).execute(
        operation.id,
        worker_id="sharing-worker",
        lease_seconds=60,
    )

    assert execution is not None
    assert execution.operation.status.value == "confirmed"
    assert execution.operation.result == {"receipt_id": "receipt-1"}
    assert delivery.request_calls == 1
    assert delivery.readback_calls == 0


def test_postgres_sharing_executor_recovers_checkpointed_unknown_without_resend(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "readback")
    delivery = _Delivery(
        [SharingDeliveryOutcome.indeterminate({"type": "transport_unknown"})],
        readbacks=[SharingDeliveryOutcome.confirmed({"receipt_id": "receipt-recovered"})],
    )
    executor = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0)

    first = executor.execute(operation.id, worker_id="sharing-worker-1", lease_seconds=60)
    second = executor.execute(operation.id, worker_id="sharing-worker-2", lease_seconds=60)

    assert first is not None
    assert first.operation.status.value == "awaiting_readback"
    assert second is not None
    assert second.operation.status.value == "confirmed"
    assert second.operation.result == {"receipt_id": "receipt-recovered"}
    assert delivery.request_calls == 1
    assert delivery.readback_calls == 1


def test_postgres_sharing_executor_fences_unknown_write_without_readback(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "no-readback")
    delivery = _Delivery(
        [SharingDeliveryOutcome.indeterminate({"type": "unlock_unknown"})],
        supports_readback=False,
    )

    execution = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0).execute(
        operation.id,
        worker_id="sharing-worker",
        lease_seconds=60,
    )

    assert execution is not None
    assert execution.operation.status.value == "indeterminate"
    assert execution.operation.error == {
        "type": "unlock_unknown",
        "authoritative_readback": False,
        "provider_may_have_been_called": True,
    }
    assert delivery.request_calls == 1
    assert repository.claim_by_id(operation.id, worker_id="replacement", lease_seconds=60) is None


def test_postgres_sharing_executor_rearms_only_a_proven_prewrite_failure(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "prewrite")
    delivery = _Delivery(
        [SharingDeliveryOutcome.retryable({"type": "preflight_unavailable"})],
        checkpoint=False,
    )

    execution = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0).execute(
        operation.id,
        worker_id="sharing-worker",
        lease_seconds=60,
    )

    assert execution is not None
    assert execution.operation.status.value == "retryable"
    assert execution.operation.step_started_at is None
    with p9_runtime.engine.connect() as connection:
        status = connection.execute(
            text("SELECT status FROM outbox_events WHERE aggregate_id = :operation_id"),
            {"operation_id": str(operation.id)},
        ).scalar_one()
    assert status == "pending"


def test_postgres_sharing_executor_rejects_confirmation_without_checkpoint(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "missing-checkpoint")
    delivery = _Delivery([SharingDeliveryOutcome.confirmed()], checkpoint=False)

    execution = SharingOperationExecutor(repository, delivery).execute(
        operation.id,
        worker_id="sharing-worker",
        lease_seconds=60,
    )

    assert execution is not None
    assert execution.operation.status.value == "indeterminate"
    assert execution.operation.error == {
        "type": "provider_checkpoint_missing",
        "provider_may_have_been_called": True,
    }


def test_postgres_sharing_executor_recovers_expired_readback_capable_request(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    claim_time = datetime.now(UTC) - timedelta(seconds=5)
    operation = _sharing_operation(repository, "expired-readback", available_at=claim_time)
    claimed = repository.claim_by_id(
        operation.id,
        worker_id="stale-worker",
        lease_seconds=1,
        now=claim_time,
    )
    assert claimed is not None
    assert claimed.claim_token is not None
    requesting = repository.mark_requesting(operation.id, claimed.claim_token, now=claim_time)
    expired_at = datetime.now(UTC)

    assert (
        repository.requeue_stale_readback_claims(
            now=expired_at,
        )
        == 1
    )
    recovered = repository.read(operation.id)
    assert recovered is not None
    assert recovered.status.value == "awaiting_readback"
    assert recovered.step_started_at == requesting.step_started_at
    assert recovered.error == {
        "type": "sharing_request_claim_expired",
        "provider_may_have_been_called": True,
        "authoritative_readback_required": True,
        "attempts": 1,
    }

    delivery = _Delivery(
        [],
        readbacks=[SharingDeliveryOutcome.confirmed({"receipt_id": "after-crash"})],
    )
    execution = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0).execute(
        operation.id,
        worker_id="replacement-worker",
        lease_seconds=60,
    )

    assert execution is not None
    assert execution.operation.status.value == "confirmed"
    assert execution.operation.result == {"receipt_id": "after-crash"}
    assert delivery.request_calls == 0
    assert delivery.readback_calls == 1


def test_postgres_sharing_executor_malformed_checkpointed_outcome_fails_to_readback(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "malformed-outcome")
    delivery = _MalformedDelivery([])

    execution = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0).execute(
        operation.id,
        worker_id="sharing-worker",
        lease_seconds=60,
    )

    assert execution is not None
    assert execution.operation.status.value == "awaiting_readback"
    assert execution.operation.step_started_at is not None
    assert execution.operation.error == {
        "type": "invalid_checkpointed_delivery",
        "error_class": "TypeError",
    }


def test_postgres_sharing_executor_recovers_when_checkpoint_commit_ack_is_lost(
    p9_runtime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "checkpoint-ack-lost")
    delivery = _Delivery(
        [SharingDeliveryOutcome.confirmed({"receipt_id": "unused"})],
        readbacks=[SharingDeliveryOutcome.confirmed({"receipt_id": "readback-confirmed"})],
    )
    original_mark_requesting = repository.mark_requesting

    def commit_then_lose_ack(
        operation_id: UUID,
        claim_token: UUID,
        *,
        now: datetime | None = None,
    ) -> SharingOperation:
        original_mark_requesting(operation_id, claim_token, now=now)
        raise ConnectionError("checkpoint commit acknowledgement was lost")

    monkeypatch.setattr(repository, "mark_requesting", commit_then_lose_ack)
    executor = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0)

    first = executor.execute(operation.id, worker_id="request-worker", lease_seconds=60)
    second = executor.execute(operation.id, worker_id="readback-worker", lease_seconds=60)

    assert first is not None
    assert first.operation.status.value == "awaiting_readback"
    assert first.operation.step_started_at is not None
    assert second is not None
    assert second.operation.status.value == "confirmed"
    assert second.operation.result == {"receipt_id": "readback-confirmed"}
    assert delivery.request_calls == 1
    assert delivery.readback_calls == 1


def test_postgres_sharing_executor_duplicate_workers_call_provider_once(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "concurrent")
    delivery = _BlockingDelivery()
    executor = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first_future = pool.submit(
            executor.execute,
            operation.id,
            worker_id="sharing-worker-1",
            lease_seconds=60,
        )
        assert delivery.entered.wait(5)
        second = pool.submit(
            executor.execute,
            operation.id,
            worker_id="sharing-worker-2",
            lease_seconds=60,
        ).result(timeout=5)
        delivery.release.set()
        first = first_future.result(timeout=5)

    assert first is not None
    assert first.claimed is True
    assert first.operation.status.value == "confirmed"
    assert second is not None
    assert second.claimed is False
    assert second.operation.status.value == "requesting"
    assert delivery.request_calls == 1
    with p9_runtime.engine.connect() as connection:
        outbox = connection.execute(
            text("SELECT status, processed_at FROM outbox_events WHERE aggregate_id = :operation_id"),
            {"operation_id": str(operation.id)},
        ).one()
    assert outbox[0] == "processed"
    assert outbox[1] is not None


def test_postgres_stale_readback_claim_never_rearms_provider_request(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "stale-readback-claim")
    delivery = _Delivery(
        [SharingDeliveryOutcome.indeterminate({"type": "write_unknown"})],
        readbacks=[SharingDeliveryOutcome.retryable({"type": "readback_unavailable"})],
    )
    executor = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0)
    first = executor.execute(operation.id, worker_id="request-worker", lease_seconds=60)
    assert first is not None
    assert first.operation.status.value == "awaiting_readback"

    readback_claim = repository.claim_by_id(
        operation.id,
        worker_id="readback-worker",
        lease_seconds=1,
    )
    assert readback_claim is not None
    assert readback_claim.step_started_at is not None
    expired_at = (readback_claim.lease_expires_at or datetime.now(UTC)) + timedelta(seconds=1)
    assert repository.expire_stale_claims(now=expired_at) == 0
    assert repository.requeue_stale_readback_claims(now=expired_at, max_attempts=2) == 1

    terminal = repository.read(operation.id)
    assert terminal is not None
    assert terminal.status.value == "indeterminate"
    assert terminal.step_started_at is not None
    assert terminal.error == {
        "type": "authoritative_readback_exhausted",
        "provider_may_have_been_called": True,
        "authoritative_readback_required": True,
        "attempts": 2,
    }
    replay = executor.execute(operation.id, worker_id="replacement-worker", lease_seconds=60)
    assert replay is not None
    assert replay.claimed is False
    assert replay.operation.status.value == "indeterminate"
    assert delivery.request_calls == 1
    assert delivery.readback_calls == 0


def test_postgres_sharing_stale_claim_recovery_has_attempt_ceiling(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    claim_time = datetime.now(UTC)
    operation = _sharing_operation(repository, "attempt-ceiling", available_at=claim_time)

    first = repository.claim_by_id(
        operation.id,
        worker_id="crashing-worker-1",
        lease_seconds=1,
        now=claim_time,
    )
    assert first is not None
    first_expiry = (first.lease_expires_at or claim_time) + timedelta(seconds=1)
    assert repository.expire_stale_claims(now=first_expiry, max_attempts=2) == 1
    retryable = repository.read(operation.id)
    assert retryable is not None
    assert retryable.status.value == "retryable"

    second = repository.claim_by_id(
        operation.id,
        worker_id="crashing-worker-2",
        lease_seconds=1,
        now=first_expiry,
    )
    assert second is not None
    second_expiry = (second.lease_expires_at or first_expiry) + timedelta(seconds=1)
    assert repository.expire_stale_claims(now=second_expiry, max_attempts=2) == 1
    exhausted = repository.read(operation.id)
    assert exhausted is not None
    assert exhausted.status.value == "rejected"
    assert exhausted.error == {
        "type": "sharing_claim_recovery_exhausted",
        "provider_may_have_been_called": False,
        "attempts": 2,
    }


def test_postgres_sharing_transition_rejects_an_expired_claim_token(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    claim_time = datetime.now(UTC) - timedelta(seconds=5)
    operation = _sharing_operation(repository, "expired-token", available_at=claim_time)
    claimed = repository.claim_by_id(
        operation.id,
        worker_id="expired-worker",
        lease_seconds=1,
        now=claim_time,
    )
    assert claimed is not None
    assert claimed.claim_token is not None
    expired_at = claim_time + timedelta(seconds=2)

    with pytest.raises(SharingOperationStateError, match="claim is no longer valid"):
        repository.mark_requesting(operation.id, claimed.claim_token, now=expired_at)

    stale = repository.read(operation.id)
    assert stale is not None
    assert stale.status.value == "claimed"
    assert stale.step_started_at is None
    assert repository.expire_stale_claims(now=expired_at) == 1
    recovered = repository.read(operation.id)
    assert recovered is not None
    assert recovered.status.value == "retryable"


def test_postgres_dead_sharing_dispatch_does_not_terminate_active_claim(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "dead-active")
    with p9_runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE outbox_events SET status = 'dead' WHERE aggregate_id = :operation_id"),
            {"operation_id": str(operation.id)},
        )
    claimed = repository.claim_by_id(operation.id, worker_id="active-worker", lease_seconds=60)
    assert claimed is not None
    assert claimed.claim_token is not None

    assert repository.reject_dead_dispatches() == 0
    active = repository.read(operation.id)
    assert active is not None
    assert active.status.value == "claimed"
    assert active.claim_token == claimed.claim_token

    repository.mark_retryable(
        operation.id,
        claimed.claim_token,
        error={"type": "prewrite_failure"},
        retry_after_seconds=0,
    )
    assert repository.reject_dead_dispatches() == 1
    rejected = repository.read(operation.id)
    assert rejected is not None
    assert rejected.status.value == "rejected"
    assert rejected.claim_token is None
    assert rejected.error == {
        "type": "sharing_dispatch_dead",
        "provider_may_have_been_called": False,
    }
