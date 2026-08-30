from __future__ import annotations

import hashlib
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs.json_payload import canonical_json, normalize_durable_json_object


@dataclass(frozen=True, slots=True)
class ContributionLedgerEntry:
    id: UUID
    sequence: int
    account_id: str
    idempotency_key: str
    delta: int
    balance_after: int
    reason: str
    reference: str | None
    operation_id: UUID | None
    metadata: Mapping[str, Any]
    created_at: datetime


class ContributionLedgerError(RuntimeError):
    pass


class InsufficientContributionError(ContributionLedgerError):
    def __init__(self, account_id: str, requested: int, available: int) -> None:
        self.account_id = account_id
        self.requested = requested
        self.available = available
        super().__init__(f"account {account_id!r} has {available} contribution points; {requested} required")


class LedgerIdempotencyConflictError(ContributionLedgerError):
    pass


class ContributionLedger(Protocol):
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
    ) -> ContributionLedgerEntry: ...

    def credit(
        self,
        account_id: str,
        amount: int,
        *,
        idempotency_key: str,
        reason: str,
        reference: str | None = None,
        operation_id: UUID | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> ContributionLedgerEntry: ...

    def balance(self, account_id: str) -> int: ...

    def entries(self, account_id: str, *, limit: int = 100) -> Sequence[ContributionLedgerEntry]: ...


class InMemoryContributionLedger:
    """Deterministic Fake used by local verification; the lock models a DB row lock."""

    def __init__(self, initial_balances: Mapping[str, int] | None = None) -> None:
        self._lock = threading.RLock()
        self._balances = {
            str(key): _nonnegative_int(value, "initial balance") for key, value in (initial_balances or {}).items()
        }
        self._entries: dict[str, list[ContributionLedgerEntry]] = {}
        self._by_key: dict[tuple[str, str], ContributionLedgerEntry] = {}

    def ensure_account(self, account_id: str, initial_balance: int = 0) -> int:
        account = _account_id(account_id)
        initial = _nonnegative_int(initial_balance, "initial balance")
        with self._lock:
            return self._balances.setdefault(account, initial)

    def balance(self, account_id: str) -> int:
        with self._lock:
            return self._balances.get(_account_id(account_id), 0)

    def debit(self, account_id: str, amount: int, **kwargs: Any) -> ContributionLedgerEntry:
        return self._apply(account_id, -_positive_int(amount), **kwargs)

    def credit(self, account_id: str, amount: int, **kwargs: Any) -> ContributionLedgerEntry:
        return self._apply(account_id, _positive_int(amount), **kwargs)

    def _apply(
        self,
        account_id: str,
        delta: int,
        *,
        idempotency_key: str,
        reason: str,
        reference: str | None = None,
        operation_id: UUID | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> ContributionLedgerEntry:
        account = _account_id(account_id)
        key = _key(idempotency_key)
        normalized_reason = _reason(reason)
        normalized_metadata = _metadata(metadata)
        request_hash = _entry_hash(delta, normalized_reason, reference, operation_id, normalized_metadata)
        with self._lock:
            existing = self._by_key.get((account, key))
            if existing is not None:
                if (
                    _entry_hash(
                        existing.delta,
                        existing.reason,
                        existing.reference,
                        existing.operation_id,
                        existing.metadata,
                    )
                    != request_hash
                ):
                    raise LedgerIdempotencyConflictError("ledger idempotency key was reused with different intent")
                return existing
            current = self._balances.get(account, 0)
            if delta < 0 and current < -delta:
                raise InsufficientContributionError(account, -delta, current)
            after = current + delta
            entry = ContributionLedgerEntry(
                id=uuid5(NAMESPACE_URL, f"etk:sharing-ledger:{account}:{key}"),
                sequence=sum(len(items) for items in self._entries.values()) + 1,
                account_id=account,
                idempotency_key=key,
                delta=delta,
                balance_after=after,
                reason=normalized_reason,
                reference=_optional_reference(reference),
                operation_id=operation_id,
                metadata=normalized_metadata,
                created_at=datetime.now(UTC),
            )
            self._balances[account] = after
            self._entries.setdefault(account, []).append(entry)
            self._by_key[(account, key)] = entry
            return entry

    def entries(self, account_id: str, *, limit: int = 100) -> Sequence[ContributionLedgerEntry]:
        bounded = max(1, min(int(limit), 1000))
        with self._lock:
            return tuple(reversed(self._entries.get(_account_id(account_id), ())[-bounded:]))


class PostgresContributionLedger:
    """Append-only Ledger with the account row as the concurrency fence."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def ensure_account(self, account_id: str, initial_balance: int = 0) -> int:
        account = _account_id(account_id)
        initial = _nonnegative_int(initial_balance, "initial balance")
        with self._unit_of_work() as uow:
            uow.session.execute(
                text(
                    """
                    INSERT INTO sharing_credit_accounts(account_id, balance)
                    VALUES (:account_id, :balance)
                    ON CONFLICT (account_id) DO NOTHING
                    """
                ),
                {"account_id": account, "balance": initial},
            )
            balance = int(
                uow.session.scalar(
                    text("SELECT balance FROM sharing_credit_accounts WHERE account_id = :account_id"),
                    {"account_id": account},
                )
                or 0
            )
            uow.commit()
            return balance

    def balance(self, account_id: str) -> int:
        account = _account_id(account_id)
        with self._unit_of_work() as uow:
            return int(
                uow.session.scalar(
                    text("SELECT balance FROM sharing_credit_accounts WHERE account_id = :account_id"),
                    {"account_id": account},
                )
                or 0
            )

    def debit(self, account_id: str, amount: int, **kwargs: Any) -> ContributionLedgerEntry:
        return self._apply(account_id, -_positive_int(amount), **kwargs)

    def credit(self, account_id: str, amount: int, **kwargs: Any) -> ContributionLedgerEntry:
        return self._apply(account_id, _positive_int(amount), **kwargs)

    def debit_in_uow(
        self,
        uow: SqlAlchemyUnitOfWork,
        account_id: str,
        amount: int,
        **kwargs: Any,
    ) -> ContributionLedgerEntry:
        return self._apply_in_uow(uow, account_id, -_positive_int(amount), **kwargs)

    def _apply(
        self,
        account_id: str,
        delta: int,
        *,
        idempotency_key: str,
        reason: str,
        reference: str | None = None,
        operation_id: UUID | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> ContributionLedgerEntry:
        with self._unit_of_work() as uow:
            entry = self._apply_in_uow(
                uow,
                account_id,
                delta,
                idempotency_key=idempotency_key,
                reason=reason,
                reference=reference,
                operation_id=operation_id,
                metadata=metadata,
            )
            uow.commit()
            return entry

    @staticmethod
    def _apply_in_uow(
        uow: SqlAlchemyUnitOfWork,
        account_id: str,
        delta: int,
        *,
        idempotency_key: str,
        reason: str,
        reference: str | None = None,
        operation_id: UUID | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> ContributionLedgerEntry:
        account = _account_id(account_id)
        key = _key(idempotency_key)
        normalized_reason = _reason(reason)
        normalized_reference = _optional_reference(reference)
        normalized_metadata = _metadata(metadata)
        intent_hash = _entry_hash(delta, normalized_reason, normalized_reference, operation_id, normalized_metadata)
        uow.session.execute(
            text(
                """
                INSERT INTO sharing_credit_accounts(account_id, balance)
                VALUES (:account_id, 0)
                ON CONFLICT (account_id) DO NOTHING
                """
            ),
            {"account_id": account},
        )
        row = (
            uow.session.execute(
                text(
                    "SELECT account_id, balance FROM sharing_credit_accounts WHERE account_id = :account_id FOR UPDATE"
                ),
                {"account_id": account},
            )
            .mappings()
            .one()
        )
        existing = (
            uow.session.execute(
                text("SELECT * FROM sharing_credit_ledger WHERE account_id = :account_id AND idempotency_key = :key"),
                {"account_id": account, "key": key},
            )
            .mappings()
            .one_or_none()
        )
        if existing is not None:
            existing_entry = _entry(existing)
            if (
                _entry_hash(
                    existing_entry.delta,
                    existing_entry.reason,
                    existing_entry.reference,
                    existing_entry.operation_id,
                    existing_entry.metadata,
                )
                != intent_hash
            ):
                raise LedgerIdempotencyConflictError("ledger idempotency key was reused with different intent")
            return existing_entry
        current = int(row["balance"] or 0)
        if delta < 0 and current < -delta:
            raise InsufficientContributionError(account, -delta, current)
        after = current + delta
        values = {
            "id": uuid5(NAMESPACE_URL, f"etk:sharing-ledger:{account}:{key}"),
            "account_id": account,
            "idempotency_key": key,
            "delta": delta,
            "balance_after": after,
            "reason": normalized_reason,
            "reference": normalized_reference,
            "operation_id": operation_id,
            "metadata": canonical_json(normalized_metadata, field="ledger metadata"),
        }
        inserted = (
            uow.session.execute(
                text(
                    """
                    INSERT INTO sharing_credit_ledger(
                        id, account_id, idempotency_key, delta, balance_after,
                        reason, reference, operation_id, metadata
                    ) VALUES (
                        :id, :account_id, :idempotency_key, :delta, :balance_after,
                        :reason, :reference, :operation_id, CAST(:metadata AS jsonb)
                    ) RETURNING *
                    """
                ),
                values,
            )
            .mappings()
            .one()
        )
        uow.session.execute(
            text(
                """
                UPDATE sharing_credit_accounts
                SET balance = :balance, revision = revision + 1, updated_at = NOW()
                WHERE account_id = :account_id
                """
            ),
            {"account_id": account, "balance": after},
        )
        return _entry(inserted)

    def entries(self, account_id: str, *, limit: int = 100) -> Sequence[ContributionLedgerEntry]:
        account = _account_id(account_id)
        bounded = max(1, min(int(limit), 1000))
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT * FROM sharing_credit_ledger
                        WHERE account_id = :account_id
                        ORDER BY event_sequence DESC
                        LIMIT :limit
                        """
                    ),
                    {"account_id": account, "limit": bounded},
                )
                .mappings()
                .all()
            )
        return tuple(_entry(row) for row in rows)


def _entry(row: Mapping[Any, Any]) -> ContributionLedgerEntry:
    return ContributionLedgerEntry(
        id=UUID(str(row["id"])),
        sequence=int(row.get("event_sequence") or 0),
        account_id=str(row["account_id"]),
        idempotency_key=str(row["idempotency_key"]),
        delta=int(row["delta"]),
        balance_after=int(row["balance_after"]),
        reason=str(row["reason"]),
        reference=str(row["reference"]) if row.get("reference") is not None else None,
        operation_id=UUID(str(row["operation_id"])) if row.get("operation_id") else None,
        metadata=dict(row.get("metadata") or {}),
        created_at=_aware(row.get("created_at")),
    )


def _entry_hash(
    delta: int,
    reason: str,
    reference: str | None,
    operation_id: UUID | None,
    metadata: Mapping[str, Any],
) -> bytes:
    payload = {
        "delta": delta,
        "reason": reason,
        "reference": reference,
        "operation_id": str(operation_id) if operation_id else None,
        "metadata": dict(metadata),
    }
    return hashlib.sha256(canonical_json(payload, field="ledger intent").encode("utf-8")).digest()


def _metadata(value: Mapping[str, Any] | None) -> dict[str, Any]:
    return normalize_durable_json_object(dict(value or {}), field="ledger metadata")


def _account_id(value: object) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > 255:
        raise ValueError("ledger account_id must be a non-empty string of at most 255 characters")
    return normalized


def _key(value: object) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError("ledger idempotency_key must not be empty")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _reason(value: object) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > 255:
        raise ValueError("ledger reason must be a non-empty string of at most 255 characters")
    return normalized


def _optional_reference(value: object) -> str | None:
    normalized = str(value or "").strip()
    return normalized[:255] or None


def _positive_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("contribution amount must be a positive integer")
    return value


def _nonnegative_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _aware(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return datetime.now(UTC)


__all__ = [
    "ContributionLedger",
    "ContributionLedgerEntry",
    "ContributionLedgerError",
    "InMemoryContributionLedger",
    "InsufficientContributionError",
    "LedgerIdempotencyConflictError",
    "PostgresContributionLedger",
]
