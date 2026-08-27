from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

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
