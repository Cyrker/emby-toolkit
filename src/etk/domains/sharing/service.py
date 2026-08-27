from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from etk.integrations.http_policy import ExternalAdapterError, ProviderDisposition, ProviderResponse
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
    headers: Mapping[str, str] = field(default_factory=dict)


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
        """Reserve a transfer and atomically debit its escrow when supported."""

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


class SharingService(SharingLedgerService):
    """Native sharing application service over the typed provider ports.

    Read-only provider calls are retried by their adapter.  Mutations are
    intentionally delegated once to the adapter; an indeterminate provider
    result is surfaced as a durable-looking ``202`` response instead of being
    replayed by the request thread.
    """

    def __init__(self, ledger: ContributionLedger, center: Any, operations: Any | None = None) -> None:
        super().__init__(ledger, operations=operations)
        self._center = center

    def credit_balance(self, account_id: str) -> SharingHttpResult:
        return self.balance(account_id)

    def credit_ledger(self, account_id: str, *, limit: int = 100) -> SharingHttpResult:
        return self.ledger(account_id, limit=limit)

    def list_sources(self, **query: Any) -> SharingHttpResult:
        response = self._provider_call(self._center.list_sources, **query)
        return _provider_http_result(response)

    def register_device(self, *, name: str, server_id_hash: str) -> SharingHttpResult:
        response = self._provider_call(self._center.register_device, name=name, server_id_hash=server_id_hash)
        return _provider_http_result(response)

    def device_status(self) -> SharingHttpResult:
        response = self._provider_call(self._center.device_status)
        return _provider_http_result(response)

    def health(self) -> SharingHttpResult:
        response = self._provider_call(self._center.health)
        return _provider_http_result(response)

    def register_source(
        self,
        source_kind: str,
        payload: Mapping[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> SharingHttpResult:
        del idempotency_key
        normalized_kind = str(source_kind or "").strip().lower()
        normalized_payload = normalize_durable_json_object(dict(payload), field="sharing source payload")
        response = self._provider_call(self._center.register_source, normalized_kind, normalized_payload)
        return _provider_http_result(response, accepted_status=201)

    def acquire_transfer_lease(self, payload: Mapping[str, Any]) -> SharingHttpResult:
        response = self._provider_call(
            self._center.acquire_transfer_lease,
            normalize_durable_json_object(dict(payload), field="sharing transfer lease"),
        )
        return _provider_http_result(response, accepted_status=202)

    def report_transfer(self, payload: Mapping[str, Any]) -> SharingHttpResult:
        response = self._provider_call(
            self._center.report_transfer, normalize_durable_json_object(dict(payload), field="sharing transfer report")
        )
        return _provider_http_result(response)

    def quote_share_request(self, payload: Mapping[str, Any]) -> SharingHttpResult:
        response = self._provider_call(
            self._center.quote_share_request,
            normalize_durable_json_object(dict(payload), field="sharing quote request"),
        )
        return _provider_http_result(response)

    def list_share_requests(self, **query: Any) -> SharingHttpResult:
        response = self._provider_call(self._center.list_share_requests, **query)
        return _provider_http_result(response)

    def create_share_request(self, payload: Mapping[str, Any]) -> SharingHttpResult:
        response = self._provider_call(
            self._center.create_share_request, normalize_durable_json_object(dict(payload), field="sharing request")
        )
        return _provider_http_result(response, accepted_status=202)

    def operation_status(self, operation_id: str) -> SharingHttpResult:
        try:
            parsed = UUID(str(operation_id))
        except (TypeError, ValueError, AttributeError):
            return SharingHttpResult({"success": False, "message": "共享操作不存在"}, 404)
        if self._operations is None:
            return SharingHttpResult({"success": False, "message": "共享操作不可用"}, 503)
        operation = self._operations.read(parsed)
        if operation is None:
            return SharingHttpResult({"success": False, "message": "共享操作不存在"}, 404)
        payload = {
            "success": True,
            "operation": {
                "id": str(operation.id),
                "operation_key": operation.operation_key,
                "kind": operation.kind.value,
                "scope_key": operation.scope_key,
                "status": operation.status.value,
                "attempts": operation.attempts,
                "state_revision": operation.state_revision,
                "available_at": operation.available_at.isoformat(),
                "finished_at": operation.finished_at.isoformat() if operation.finished_at else None,
                "result": dict(operation.result or {}),
                "error": dict(operation.error or {}),
            },
        }
        return SharingHttpResult(payload)

    @staticmethod
    def _provider_call(call: Any, *args: Any, **kwargs: Any) -> ProviderResponse[Any]:
        """Map bounded adapter failures to an HTTP-safe typed outcome."""

        try:
            response = call(*args, **kwargs)
        except ExternalAdapterError as exc:
            return ProviderResponse(exc.disposition, exc.status_code)
        if not isinstance(response, ProviderResponse):
            raise TypeError("sharing provider returned an invalid response")
        return response


def _provider_http_result(response: ProviderResponse[Any], *, accepted_status: int = 200) -> SharingHttpResult:
    """Map a typed adapter outcome to the legacy JSON envelope."""

    payload = dict(response.payload) if isinstance(response.payload, Mapping) else {}
    if response.disposition is ProviderDisposition.CONFIRMED:
        payload.setdefault("success", True)
        status = response.status_code or accepted_status
        if not 200 <= status < 300:
            status = 200
        return SharingHttpResult(payload, status)
    if response.disposition is ProviderDisposition.NOT_FOUND:
        return SharingHttpResult({"success": False, "message": "共享中心资源不存在", **payload}, 404)
    if response.disposition is ProviderDisposition.REJECTED:
        status = response.status_code if response.status_code and 400 <= response.status_code < 500 else 400
        return SharingHttpResult({"success": False, "message": "共享中心拒绝了请求", **payload}, status)
    if response.disposition is ProviderDisposition.INDETERMINATE:
        headers = _retry_header(response)
        return SharingHttpResult(
            {"success": False, "status": "indeterminate", "operation_status": "indeterminate", **payload},
            202,
            headers,
        )
    headers = _retry_header(response)
    return SharingHttpResult(
        {"success": False, "status": "retryable", "message": "共享中心暂时不可用", **payload},
        503,
        headers,
    )


def _retry_header(response: ProviderResponse[Any]) -> dict[str, str]:
    value = response.retry_after_seconds
    return {"Retry-After": str(max(0, int(value)))} if value is not None else {}


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
    "SharingService",
    "entry_to_json",
]
