from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from etk.platform.jobs.json_payload import normalize_durable_json_object

from .ledger import (
    ContributionLedger,
    ContributionLedgerEntry,
)
from .operations import SharingOperationIntent, SharingOperationKind


@dataclass(frozen=True, slots=True)
class SharingHttpResult:
    payload: Any
    status_code: int = 200


class SharingLedgerService:
    """Application-level contribution use cases.

    The service deliberately keeps provider calls out of the transaction. A
    caller first reserves a durable operation, then a Worker performs the
    external effect and records a confirmed/rejected/indeterminate outcome.
    Ledger mutations themselves are atomic at the repository boundary.
    """

    def __init__(self, ledger: ContributionLedger, operations: Any | None = None) -> None:
        self._ledger = ledger
        self._operations = operations

    def balance(self, account_id: str) -> SharingHttpResult:
        account = _account(account_id)
        return SharingHttpResult({"success": True, "account_id": account, "credit": self._ledger.balance(account)})

    def ledger(self, account_id: str, *, limit: int = 100) -> SharingHttpResult:
        account = _account(account_id)
        items = [entry_to_json(entry) for entry in self._ledger.entries(account, limit=limit)]
        return SharingHttpResult(
            {
                "success": True,
                "account_id": account,
                "items": items,
                "count": len(items),
                "credit": self._ledger.balance(account),
            }
        )

    def debit(
        self,
        account_id: str,
        amount: int,
        *,
        idempotency_key: str,
        reason: str,
        reference: str | None = None,
        operation_id: UUID | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> ContributionLedgerEntry:
        return self._ledger.debit(
            _account(account_id),
            amount,
            idempotency_key=idempotency_key,
            reason=reason,
            reference=reference,
            operation_id=operation_id,
            metadata=metadata,
        )

    def compensate(
        self,
        account_id: str,
        amount: int,
        *,
        original_idempotency_key: str,
        reason: str = "compensation",
        reference: str | None = None,
        operation_id: UUID | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> ContributionLedgerEntry:
        key = str(original_idempotency_key or "").strip()
        if not key:
            raise ValueError("original_idempotency_key must not be empty")
        return self._ledger.credit(
            _account(account_id),
            amount,
            idempotency_key=f"compensation:{key}",
            reason=reason,
            reference=reference or key,
            operation_id=operation_id,
            metadata={"compensates": key, **dict(metadata or {})},
        )

    def reserve_transfer(
        self,
        *,
        account_id: str,
        source_id: str,
        amount: int,
        principal_id: str,
        idempotency_key: str,
        request: Mapping[str, Any] | None = None,
    ) -> SharingHttpResult:
        """Reserve a transfer and atomically debit its escrow when supported.

        ``PostgresSharingOperationRepository`` exposes ``reserve_and_debit``
        in later adapter wiring. The fallback path is intentionally limited to
        a deterministic ledger debit so Fake/Stub tests can exercise the same
        idempotency semantics without a database.
        """

        normalized_source = _required(source_id, "source_id")
        normalized_request = normalize_durable_json_object(
            {
                "account_id": _account(account_id),
                "source_id": normalized_source,
                "amount": amount,
                **dict(request or {}),
            },
            field="sharing transfer request",
        )
        operation_id: UUID | None = None
        operation: Any | None = None
        if self._operations is not None:
            intent = SharingOperationIntent.create(
                kind=SharingOperationKind.TRANSFER,
                scope_key=f"account:{normalized_request['account_id']}:source:{normalized_source}",
                principal_id=_required(principal_id, "principal_id"),
                request=normalized_request,
                idempotency_key=idempotency_key,
            )
            operation = self._operations.reserve(intent)
            if operation is None:
                raise RuntimeError("sharing operation reservation returned no operation")
            operation_id = operation.id
        entry = self.debit(
            account_id,
            amount,
            idempotency_key=f"transfer:{idempotency_key}",
            reason="transfer_escrow",
            reference=normalized_source,
            operation_id=operation_id,
            metadata={"source_id": normalized_source},
        )
        operation_status = getattr(operation, "status", "reserved") if operation is not None else "reserved"
        return SharingHttpResult(
            {
                "success": True,
                "operation_id": str(operation_id) if operation_id else None,
                "status": str(operation_status),
                "credit": entry.balance_after,
                "ledger_entry_id": str(entry.id),
            },
            202 if operation is not None else 200,
        )


def entry_to_json(entry: ContributionLedgerEntry) -> dict[str, Any]:
    return {
        "id": str(entry.id),
        "event_sequence": entry.sequence,
        "account_id": entry.account_id,
        "idempotency_key": entry.idempotency_key,
        "delta": entry.delta,
        "balance_after": entry.balance_after,
        "reason": entry.reason,
        "ref_id": entry.reference,
        "reference": entry.reference,
        "operation_id": str(entry.operation_id) if entry.operation_id else None,
        "raw_json": dict(entry.metadata),
        "created_at": entry.created_at.isoformat(),
    }


def _account(value: object) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError("account_id must not be empty")
    return normalized


def _required(value: object, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return normalized


__all__ = [
    "SharingHttpResult",
    "SharingLedgerService",
    "entry_to_json",
]
