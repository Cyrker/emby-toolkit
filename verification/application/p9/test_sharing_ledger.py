from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from etk.domains.sharing import (
    InMemoryContributionLedger,
    InsufficientContributionError,
    LedgerIdempotencyConflictError,
    SharingLedgerService,
)
from etk.domains.sharing.operations import SharingOperationIntent, SharingOperationKind
from etk.platform.jobs import InvalidJsonPayloadError


def test_debit_is_idempotent_and_conflicting_reuse_fails() -> None:
    ledger = InMemoryContributionLedger({"device-1": 25})

    first = ledger.debit("device-1", 10, idempotency_key="transfer-1", reason="escrow")
    replay = ledger.debit("device-1", 10, idempotency_key="transfer-1", reason="escrow")

    assert replay == first
    assert ledger.balance("device-1") == 15
    assert len(ledger.entries("device-1")) == 1

    with pytest.raises(LedgerIdempotencyConflictError):
        ledger.debit("device-1", 5, idempotency_key="transfer-1", reason="escrow")


def test_concurrent_debits_are_serialized_by_account_lock() -> None:
    ledger = InMemoryContributionLedger({"device-1": 100})

    def debit(index: int):
        try:
            return ledger.debit(
                "device-1",
                10,
                idempotency_key=f"transfer-{index}",
                reason="escrow",
            )
        except InsufficientContributionError:
            return None

    with ThreadPoolExecutor(max_workers=20) as executor:
        entries = list(executor.map(debit, range(20)))

    assert sum(entry is not None for entry in entries) == 10
    assert ledger.balance("device-1") == 0
    assert len(ledger.entries("device-1", limit=100)) == 10


def test_compensation_is_a_distinct_idempotent_credit() -> None:
    ledger = InMemoryContributionLedger({"device-1": 20})
    service = SharingLedgerService(ledger)

    debit = service.debit(
        "device-1",
        10,
        idempotency_key="share-request-1",
        reason="share_request_escrow",
    )
    compensation = service.compensate(
        "device-1",
        10,
        original_idempotency_key="share-request-1",
        reason="share_request_refund",
    )
    replay = service.compensate(
        "device-1",
        10,
        original_idempotency_key="share-request-1",
        reason="share_request_refund",
    )

    assert debit.delta == -10
    assert compensation.delta == 10
    assert replay == compensation
    assert ledger.balance("device-1") == 20


def test_durable_operation_and_ledger_reject_secret_payloads() -> None:
    with pytest.raises(InvalidJsonPayloadError):
        SharingOperationIntent.create(
            kind=SharingOperationKind.TRANSFER,
            scope_key="source:1",
            principal_id="device-1",
            request={"source_id": "1", "api_key": "not-persisted"},
            idempotency_key="request-1",
        )


def test_service_reserve_transfer_returns_a_stable_ledger_reference() -> None:
    ledger = InMemoryContributionLedger({"device-1": 30})
    result = SharingLedgerService(ledger).reserve_transfer(
        account_id="device-1",
        source_id="source-1",
        amount=5,
        principal_id="user-1",
        idempotency_key="transfer-1",
    )

    assert result.status_code == 200
    assert result.payload["success"] is True
    assert result.payload["credit"] == 25
    assert result.payload["ledger_entry_id"]
