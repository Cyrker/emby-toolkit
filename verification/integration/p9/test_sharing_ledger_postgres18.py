from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

from sqlalchemy import text

from etk.domains.sharing import PostgresContributionLedger
from etk.domains.sharing.operations import SharingOperationIntent, SharingOperationKind
from etk.domains.sharing.repository import PostgresSharingOperationRepository


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
