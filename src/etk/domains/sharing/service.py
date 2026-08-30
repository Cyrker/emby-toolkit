from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, cast
from uuid import UUID

from etk.integrations.http_policy import ExternalAdapterError, ProviderDisposition, ProviderResponse
from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs.json_payload import normalize_durable_json_object

from .configuration import SharingConfigurationService, SharingServerIdentity
from .ledger import (
    ContributionLedger,
    ContributionLedgerEntry,
)
from .operations import SharingOperationIntent, SharingOperationKind

_DISPLAY_TAG_FALLBACK = [
    {"label": "已完结", "value": "completed_certified"},
    {"label": "连载中", "value": "ongoing"},
    {"label": "短剧", "value": "short_drama"},
    {"label": "纯净版", "value": "clean_version"},
    {"label": "片头", "value": "intro"},
    {"label": "原盘", "value": "original_disc"},
    {"label": "国语", "value": "mandarin_audio"},
    {"label": "中字", "value": "chinese_subtitle"},
    {"label": "特效", "value": "effect_subtitle"},
]


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

    def __init__(
        self,
        ledger: ContributionLedger,
        operations: Any | None = None,
        *,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork] | None = None,
    ) -> None:
        self._ledger = ledger
        self._operations = operations
        self._unit_of_work = unit_of_work

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
        intent: SharingOperationIntent | None = None
        if self._operations is not None:
            intent = SharingOperationIntent.create(
                kind=SharingOperationKind.TRANSFER,
                scope_key=f"account:{normalized_request['account_id']}:source:{normalized_source}",
                principal_id=_required(principal_id, "principal_id"),
                request={
                    "provider": "shared_center",
                    "action": "acquire_transfer_lease",
                    "payload": normalized_request,
                },
                idempotency_key=idempotency_key,
            )
        operations = self._operations
        reserve_in_uow = getattr(operations, "reserve_in_uow", None)
        debit_in_uow = getattr(self._ledger, "debit_in_uow", None)
        if (
            intent is not None
            and self._unit_of_work is not None
            and callable(reserve_in_uow)
            and callable(debit_in_uow)
        ):
            with self._unit_of_work() as uow:
                operation = reserve_in_uow(uow, intent)
                if operation is None:
                    raise RuntimeError("sharing operation reservation returned no operation")
                operation_id = operation.id
                entry = cast(
                    ContributionLedgerEntry,
                    debit_in_uow(
                        uow,
                        account_id,
                        amount,
                        idempotency_key=f"transfer:{idempotency_key}",
                        reason="transfer_escrow",
                        reference=normalized_source,
                        operation_id=operation_id,
                        metadata={"source_id": normalized_source},
                    ),
                )
                uow.commit()
        else:
            if intent is not None and operations is not None:
                operation = operations.reserve(intent)
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

    def __init__(
        self,
        ledger: ContributionLedger,
        center: Any,
        operations: Any | None = None,
        *,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork] | None = None,
        configuration: SharingConfigurationService | None = None,
        identity: SharingServerIdentity | None = None,
    ) -> None:
        super().__init__(ledger, operations=operations, unit_of_work=unit_of_work)
        self._center = center
        self._configuration = configuration
        self._identity = identity

    def configuration(self) -> SharingHttpResult:
        if self._configuration is None:
            return SharingHttpResult({"success": False, "message": "共享资源配置不可用"}, 503)
        return SharingHttpResult({"success": True, "data": self._configuration.load()})

    def save_configuration(self, payload: Mapping[str, Any]) -> SharingHttpResult:
        if self._configuration is None:
            return SharingHttpResult({"success": False, "message": "共享资源配置不可用"}, 503)
        saved = self._configuration.save(payload)
        return SharingHttpResult(
            {
                "success": True,
                "message": "共享资源配置已保存（Rapid v2：中心不存 CK、不创建 115 分享）",
                "data": saved,
            }
        )

    def credit_balance(self, account_id: str) -> SharingHttpResult:
        return self.balance(account_id)

    def credit_ledger(self, account_id: str, *, limit: int = 100) -> SharingHttpResult:
        return self.ledger(account_id, limit=limit)

    def list_sources(self, **query: Any) -> SharingHttpResult:
        # Older fakes and compatibility adapters expose the un-decorated
        # list_sources name; native adapters implement the display contract.
        call = getattr(self._center, "list_display_sources", None) or getattr(self._center, "list_sources", None)
        if call is None:
            return SharingHttpResult({"success": False, "message": "共享中心资源列表不可用"}, 503)
        response = self._provider_call(call, **query)
        response = _map_confirmed_payload(response, _display_list_payload)
        return _provider_http_result(response)

    def display_tags(self) -> SharingHttpResult:
        call = getattr(self._center, "list_display_tags", None)
        if call is None:
            return SharingHttpResult({"success": True, "items": list(_DISPLAY_TAG_FALLBACK)})
        response = self._provider_call(call)
        if response.disposition is not ProviderDisposition.CONFIRMED:
            return SharingHttpResult({"success": True, "items": list(_DISPLAY_TAG_FALLBACK)})
        payload = dict(response.payload) if isinstance(response.payload, Mapping) else {}
        items = [dict(item) for item in payload.get("items", []) if isinstance(item, Mapping)]
        return SharingHttpResult({"success": True, "items": items or list(_DISPLAY_TAG_FALLBACK)})

    def display_home(self, **query: Any) -> SharingHttpResult:
        call = getattr(self._center, "list_display_home", None)
        if call is None:
            return SharingHttpResult({"success": False, "message": "共享中心首页不可用"}, 503)
        response = self._provider_call(call, **query)
        return _provider_http_result(_map_confirmed_payload(response, _display_home_payload))

    def display_children(self, **query: Any) -> SharingHttpResult:
        call = getattr(self._center, "list_display_children", None)
        if call is None:
            return SharingHttpResult({"success": False, "message": "共享中心子项不可用"}, 503)
        response = self._provider_call(call, **query)
        return _provider_http_result(_map_confirmed_payload(response, _display_children_payload))

    def display_detail(self, **query: Any) -> SharingHttpResult:
        call = getattr(self._center, "display_detail", None)
        if call is None:
            return SharingHttpResult({"success": False, "message": "共享中心详情不可用"}, 503)
        response = self._provider_call(call, **query)
        return _provider_http_result(_map_confirmed_payload(response, _display_detail_payload))

    def register_device(self, *, name: str) -> SharingHttpResult:
        if self._identity is None:
            return SharingHttpResult({"success": False, "message": "Emby ServerID 不可用"}, 400)
        try:
            server_id_hash = self._identity.server_id_hash()
        except ExternalAdapterError as exc:
            return _provider_http_result(ProviderResponse(exc.disposition, exc.status_code))
        response = self._provider_call(self._center.register_device, name=name, server_id_hash=server_id_hash)
        if response.disposition is not ProviderDisposition.CONFIRMED:
            return _provider_http_result(response)
        payload = dict(response.payload) if isinstance(response.payload, Mapping) else {}
        configuration_saved = False
        saved: dict[str, Any] | None = None
        if self._configuration is not None:
            try:
                saved = self._configuration.enable()
                configuration_saved = True
            except Exception:
                # The provider mutation is already confirmed. Do not turn a
                # local projection failure into a retryable registration.
                pass
        return SharingHttpResult(
            {
                "success": True,
                "message": (
                    "共享资源中心已按 ServerID 重新连接，监听已刷新"
                    if configuration_saved
                    else "共享资源中心已连接，但本地启用状态保存失败，请重新保存配置"
                ),
                "data": saved,
                "device": payload,
                "configuration_saved": configuration_saved,
            }
        )

    def device_status(self) -> SharingHttpResult:
        response = self._provider_call(self._center.device_status)
        local_hash = ""
        if self._identity is not None:
            try:
                local_hash = self._identity.server_id_hash()
            except ExternalAdapterError:
                pass
        if response.disposition is not ProviderDisposition.CONFIRMED:
            result = _provider_http_result(response)
            payload = dict(result.payload) if isinstance(result.payload, Mapping) else {}
            payload.setdefault("data", {"local_server_id_hash": local_hash})
            return SharingHttpResult(payload, result.status_code, result.headers)
        payload = dict(response.payload) if isinstance(response.payload, Mapping) else {}
        device = payload.get("device")
        if isinstance(device, Mapping):
            normalized_device = dict(device)
            metadata = normalized_device.get("share_heartbeat_meta_json")
            if isinstance(metadata, Mapping):
                normalized_device.setdefault("forced_offline", metadata.get("forced_offline"))
                normalized_device.setdefault(
                    "forced_offline_reason",
                    metadata.get("forced_offline_reason") or metadata.get("ban_reason") or "",
                )
                normalized_device.setdefault(
                    "ban_reason",
                    metadata.get("ban_reason") or metadata.get("forced_offline_reason") or "",
                )
                normalized_device.setdefault("banned_by", metadata.get("banned_by") or "")
                normalized_device.setdefault("banned_at", metadata.get("banned_at"))
            payload["device"] = normalized_device
        data = {**payload, "local_server_id_hash": local_hash}
        return SharingHttpResult({"success": True, "data": data, **data})

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


def _map_confirmed_payload(
    response: ProviderResponse[Any],
    transform: Callable[[Mapping[str, Any]], dict[str, Any]],
) -> ProviderResponse[Any]:
    if response.disposition is not ProviderDisposition.CONFIRMED or not isinstance(response.payload, Mapping):
        return response
    return ProviderResponse(
        response.disposition,
        response.status_code,
        transform(response.payload),
        response.retry_after_seconds,
    )


def _display_list_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result["items"] = [
        _decorate_display_row(item, strip_children=True)
        for item in payload.get("items", [])
        if isinstance(item, Mapping)
    ]
    return result


def _display_home_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    sections: list[dict[str, Any]] = []
    for section in payload.get("sections", []):
        if not isinstance(section, Mapping):
            continue
        normalized = dict(section)
        normalized["items"] = [
            _decorate_display_row(item, strip_children=True)
            for item in section.get("items", [])
            if isinstance(item, Mapping)
        ]
        sections.append(normalized)
    result["sections"] = sections
    result["items"] = []
    if "cache_hit" in result:
        result["center_cache_hit"] = bool(result["cache_hit"])
        result["cache_hit"] = bool(result["cache_hit"])
    return result


def _display_children_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    for key in ("items", "children", "pack_items", "parents", "seasons", "resources", "versions"):
        value = payload.get(key)
        if isinstance(value, list):
            result[key] = [_decorate_display_row(item) for item in value if isinstance(item, Mapping)]
    return result


def _display_detail_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    for key in ("resources", "versions", "items", "seasons"):
        value = payload.get(key)
        if isinstance(value, list):
            result[key] = [_decorate_display_row(item, detail=True) for item in value if isinstance(item, Mapping)]
    result["children"] = []
    result["pack_items"] = []
    return {"data": result, **result}


def _decorate_display_row(
    value: Mapping[str, Any],
    *,
    strip_children: bool = False,
    detail: bool = False,
) -> dict[str, Any]:
    row = dict(value)
    nested_keys = ("versions",) if strip_children else ("versions", "children", "pack_items", "resources")
    kind = str(row.get("source_kind") or row.get("lazy_children_kind") or "").casefold()
    item_type = str(row.get("display_type") or row.get("item_type") or "").casefold()
    is_pack = kind in {
        "season_hub",
        "logical_season",
    } or item_type in {"pack", "season", "series"}
    for key in nested_keys:
        nested = row.get(key)
        if detail and is_pack and key in {"children", "pack_items"}:
            row.pop(key, None)
        elif isinstance(nested, list):
            row[key] = [_decorate_display_row(item, detail=detail) for item in nested if isinstance(item, Mapping)]

    completed = _completed_certified(row)
    row["is_completed_certified"] = completed
    row["is_completed"] = completed
    if completed:
        metadata = _flag_metadata(row, "is_completed_certified", "completed_certified_meta_json")
        row["completed_certified_meta_json"] = metadata or {
            "is_completed_certified": True,
            "certified_by": "logical_season_pool",
            "status": row.get("status"),
        }
    else:
        row.pop("completed_certified_meta_json", None)

    short_direct = _direct_flag_state(row, "is_short_drama", "short_drama_meta_json")
    if short_direct is False:
        row["is_short_drama"] = False
        row["short_drama_meta_json"] = _json_mapping(row.get("short_drama_meta_json")) or {
            "is_short_drama": False,
            "manual_override": True,
        }
    else:
        metadata = _flag_metadata(row, "is_short_drama", "short_drama_meta_json")
        if metadata:
            row["is_short_drama"] = True
            row["short_drama_meta_json"] = metadata

    for flag, metadata_key in (("is_animation", "animation_meta_json"),):
        metadata = _flag_metadata(row, flag, metadata_key)
        if metadata:
            row[flag] = True
            row[metadata_key] = metadata

    if row.get("is_ongoing_hub") or str(row.get("source_kind") or "").casefold() == "season_hub":
        row["version_summary"] = {}
    else:
        row["version_summary"] = _version_summary(row)
    if not row.get("size") and row.get("total_size"):
        row["size"] = row["total_size"]

    if strip_children:
        children: list[Mapping[str, Any]] = []
        for key in ("children", "pack_items"):
            nested = row.pop(key, None)
            if isinstance(nested, list):
                children.extend(item for item in nested if isinstance(item, Mapping))
        if children:
            count = _display_count(row.get("children_count"), len(children))
            row.update(
                {
                    "has_children": True,
                    "children_loaded": False,
                    "children_count": count,
                    "child_count": _display_count(row.get("child_count"), count),
                    "pack_item_count": _display_count(row.get("pack_item_count"), count),
                }
            )
            row.setdefault("lazy_children_kind", "season_hub" if is_pack else "logical_season")
    return row


def _completed_certified(row: Mapping[str, Any]) -> bool:
    kind = str(row.get("source_kind") or "").casefold()
    status = str(row.get("status") or "").casefold()
    logical_complete = bool(row.get("logical_pool_complete") or row.get("pool_complete") or status == "pool_complete")
    if kind == "season_hub":
        return False
    if kind == "logical_season":
        return logical_complete
    if row.get("is_ongoing_hub") and not logical_complete:
        return False
    return logical_complete or bool(_flag_metadata(row, "is_completed_certified", "completed_certified_meta_json"))


def _direct_flag_state(row: Mapping[str, Any], flag: str, metadata_key: str) -> bool | None:
    for container_key in (
        "",
        metadata_key,
        "version_summary",
        "summary_json",
        "media_signature_json",
        "raw_summary_json",
        "rapid_meta_json",
    ):
        container = row if not container_key else _json_mapping(row.get(container_key))
        if flag in container:
            state = _bool_state(container.get(flag))
            if state is not None:
                return state
        metadata = _json_mapping(container.get(metadata_key))
        if flag in metadata:
            state = _bool_state(metadata.get(flag))
            if state is not None:
                return state
    return None


def _flag_metadata(row: Mapping[str, Any], flag: str, metadata_key: str) -> dict[str, Any]:
    for candidate in _nested_display_rows(row):
        for container_key in (
            "",
            "version_summary",
            "summary_json",
            "media_signature_json",
            "raw_summary_json",
            "rapid_meta_json",
            metadata_key,
        ):
            container = candidate if not container_key else _json_mapping(candidate.get(container_key))
            metadata = _json_mapping(container.get(metadata_key))
            if _bool_value(container.get(flag)) or _bool_value(metadata.get(flag)):
                normalized = dict(metadata)
                normalized.setdefault(flag, True)
                return normalized
    return {}


def _nested_display_rows(row: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    result: list[Mapping[str, Any]] = [row]
    for key in ("versions", "children", "pack_items", "resources"):
        value = row.get(key)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, Mapping):
                    result.extend(_nested_display_rows(item))
    return result


def _version_summary(row: Mapping[str, Any]) -> dict[str, Any]:
    raw = _json_mapping(row.get("raw_summary_json") or row.get("summary_json") or row.get("version_summary"))
    signature = _json_mapping(row.get("media_signature_json") or row.get("media_signature"))
    summary = dict(raw)
    summary.update(signature)
    codec = _first_text(summary.get("video_codec"), summary.get("codec_display"), summary.get("codec"))
    summary.update(
        {
            "resolution": _first_text(summary.get("resolution"), summary.get("resolution_display")),
            "effect": _first_text(summary.get("effect"), summary.get("effect_display"), summary.get("effect_key")),
            "video_codec": codec,
            "codec": codec,
            "bit_depth": _first_text(summary.get("bit_depth")),
            "fps": _first_text(summary.get("fps"), summary.get("frame_rate")),
            "audio_list": _track_values(summary.get("audio_list"), summary.get("audios"), summary.get("audio")),
            "subtitle_list": _track_values(
                summary.get("subtitle_list"), summary.get("subtitles"), summary.get("subtitle")
            ),
        }
    )
    return {key: value for key, value in summary.items() if value not in (None, "", [], {})}


def _track_values(*values: Any) -> list[Any]:
    result: list[Any] = []
    for value in values:
        items = value if isinstance(value, list) else ([] if value in (None, "", {}) else [value])
        for item in items:
            if item not in result:
                result.append(item)
    return result


def _first_text(*values: object) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _bool_value(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().casefold() in {"1", "true", "yes", "y", "on", "启用", "开启", "是"}


def _bool_state(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().casefold()
    if text in {"1", "true", "yes", "y", "on", "启用", "开启", "是"}:
        return True
    if text in {"0", "false", "no", "n", "off", "停用", "关闭", "否"}:
        return False
    return None


def _json_mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _display_count(value: object, default: int) -> int:
    try:
        return max(0, int(str(value).strip())) if value not in (None, "") else max(0, int(default))
    except (TypeError, ValueError, OverflowError):
        return max(0, int(default))


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
