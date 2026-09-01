from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, cast
from uuid import UUID

from etk.integrations.http_policy import ExternalAdapterError, ProviderDisposition, ProviderResponse
from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs.json_payload import canonical_json, normalize_durable_json_object

from .configuration import SharingConfigurationService, SharingServerIdentity
from .ledger import (
    ContributionLedger,
    ContributionLedgerEntry,
)
from .operations import (
    SharingOperation,
    SharingOperationConflictError,
    SharingOperationIntent,
    SharingOperationKind,
    SharingOperationScopeBusyError,
    SharingOperationStatus,
)
from .source_registration import (
    SharingSourceRegistrationError,
    SharingSourceRegistrationSkipped,
    prepare_source_registration,
)
from .virtual_import import VirtualImportService, virtual_import_state_hash

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
        debit_key = f"transfer:{idempotency_key}"
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
                    "ledger_compensation": {
                        "type": "release_transfer_escrow",
                        "account_id": normalized_request["account_id"],
                        "amount": amount,
                        "debit_idempotency_key": debit_key,
                        "reference": normalized_source,
                    },
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
                        idempotency_key=debit_key,
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
                idempotency_key=debit_key,
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

    Read-only provider calls are retried by their adapter. Provider mutations
    are reserved as durable operations and return a queryable ``202`` handle;
    the request thread never performs the external write.
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
        projection: Any | None = None,
        tmdb: Any | None = None,
        virtual_imports: VirtualImportService | None = None,
    ) -> None:
        super().__init__(ledger, operations=operations, unit_of_work=unit_of_work)
        self._center = center
        self._configuration = configuration
        self._identity = identity
        self._projection = projection
        self._tmdb = tmdb
        self._virtual_imports = virtual_imports

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

    def summary(self, account_id: str) -> SharingHttpResult:
        if self._projection is None:
            return SharingHttpResult({"success": False, "message": "共享资源本地投影不可用"}, 503)
        account = _account(account_id)
        data = dict(self._projection.summary())
        data["credit"] = {"account_id": account, "credit": self._ledger.balance(account)}
        return SharingHttpResult({"success": True, "data": data})

    def refresh_credit(self, account_id: str) -> SharingHttpResult:
        account = _account(account_id)
        data = {"account_id": account, "credit": self._ledger.balance(account), "source": "local_ledger"}
        return SharingHttpResult({"success": True, "message": "贡献点已按本地 Ledger 刷新", "data": data})

    def local_sources(self, **query: Any) -> SharingHttpResult:
        if self._projection is None:
            return SharingHttpResult({"success": False, "message": "共享资源本地投影不可用"}, 503)
        return SharingHttpResult({"success": True, **self._projection.list_local_sources(**query)})

    def local_source_check(self, source_id: int) -> SharingHttpResult:
        if self._projection is None:
            return SharingHttpResult({"success": False, "message": "共享资源本地投影不可用"}, 503)
        item = self._projection.get_local_source(source_id)
        if item is None:
            return SharingHttpResult({"success": False, "message": "本地共享源不存在"}, 404)
        return SharingHttpResult(
            {
                "success": True,
                "message": "Rapid v2 本地源无需 115 分享审核",
                "item": item,
            }
        )

    def report_local_source(
        self,
        source_id: int,
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> SharingHttpResult:
        return self._stage_local_source_registration(
            source_id,
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            mode="report",
        )

    def reregister_local_source(
        self,
        source_id: int,
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> SharingHttpResult:
        return self._stage_local_source_registration(
            source_id,
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            mode="reregister",
        )

    def reregister_local_sources(
        self,
        source_ids: Sequence[int],
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> SharingHttpResult:
        normalized_ids = list(dict.fromkeys(int(value) for value in source_ids if int(value) > 0))
        if not normalized_ids:
            raise ValueError("source_ids must not be empty")
        if len(normalized_ids) > 200:
            raise ValueError("source_ids exceeds 200 items")
        items: list[dict[str, Any]] = []
        operation_ids: list[str] = []
        missing: list[int] = []
        failed: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        seen_identity: set[tuple[Any, ...]] = set()
        for source_id in normalized_ids:
            item_key = None
            if idempotency_key:
                digest = hashlib.sha256(f"{idempotency_key}\0{source_id}".encode()).hexdigest()
                item_key = f"batch:{digest}"
            result = self._stage_local_source_registration(
                source_id,
                principal_id=principal_id,
                idempotency_key=item_key,
                mode="reregister",
                seen_identity=seen_identity,
            )
            payload = dict(result.payload) if isinstance(result.payload, Mapping) else {}
            if result.status_code == 404:
                missing.append(source_id)
                continue
            if payload.get("skipped") is True:
                skipped.append({"id": source_id, **payload})
                continue
            operation_id = str(payload.get("operation_id") or "").strip()
            status = str(payload.get("status") or "").strip().casefold()
            if status in {"rejected", "indeterminate", "cancelled"}:
                failed.append(
                    {
                        "id": source_id,
                        "operation_id": operation_id or None,
                        "status": status,
                        "message": payload.get("message") or "重新登记操作未确认",
                    }
                )
                continue
            if operation_id:
                operation_ids.append(operation_id)
                items.append({"id": source_id, **payload})
                continue
            if result.status_code >= 400:
                failed.append(
                    {
                        "id": source_id,
                        "status": status or "failed",
                        "message": payload.get("message") or "重新登记任务提交失败",
                    }
                )
                continue
            items.append({"id": source_id, **payload})
        accepted = len(operation_ids)
        parts = [f"已提交 {accepted} 个重新登记任务"]
        if skipped:
            parts.append(f"{len(skipped)} 个不可秒传源已跳过")
        if missing:
            parts.append(f"{len(missing)} 个不存在")
        if failed:
            parts.append(f"{len(failed)} 个提交失败")
        if operation_ids:
            status_code = 202
        elif failed or missing or skipped:
            status_code = 400
        else:
            status_code = 200
        return SharingHttpResult(
            {
                "success": bool(operation_ids) and not failed,
                "accepted": bool(operation_ids),
                "partial": bool(operation_ids and (failed or missing or skipped)),
                "message": "，".join(parts),
                "operation_ids": operation_ids,
                "items": items,
                "missing": missing,
                "skipped": skipped,
                "failed": failed,
            },
            status_code,
        )

    def _stage_local_source_registration(
        self,
        source_id: int,
        *,
        principal_id: str,
        idempotency_key: str | None,
        mode: str,
        seen_identity: set[tuple[Any, ...]] | None = None,
    ) -> SharingHttpResult:
        if self._projection is None:
            return SharingHttpResult({"success": False, "message": "共享资源本地投影不可用"}, 503)
        if self._operations is None:
            return SharingHttpResult({"success": False, "message": "共享操作运行时不可用"}, 503)
        snapshot_reader = getattr(self._projection, "source_registration_snapshot", None)
        reserve_chain = getattr(self._operations, "reserve_prerequisite_chain", None)
        if not callable(snapshot_reader) or not callable(reserve_chain):
            return SharingHttpResult({"success": False, "message": "共享源登记运行时不可用"}, 503)
        normalized_id = int(source_id)
        if normalized_id < 1:
            raise ValueError("source_id must be positive")
        snapshot = snapshot_reader(normalized_id)
        if snapshot is None:
            return SharingHttpResult({"success": False, "message": "本地共享源不存在"}, 404)
        if not isinstance(snapshot, Mapping):
            raise TypeError("sharing source snapshot must be an object")
        source = snapshot.get("source")
        files = snapshot.get("files")
        raw_by_sha1 = snapshot.get("raw_by_sha1")
        formatted_by_sha1 = snapshot.get("formatted_by_sha1")
        if not isinstance(source, Mapping) or not isinstance(files, Sequence) or isinstance(files, (str, bytes)):
            raise TypeError("sharing source snapshot is invalid")
        adult_reason = str(snapshot.get("adult_block_reason") or "").strip()
        if adult_reason:
            label = source.get("title") or source.get("tmdb_id") or normalized_id
            return SharingHttpResult(
                {
                    "success": False,
                    "adult_blocked": True,
                    "reason": adult_reason,
                    "source_id": normalized_id,
                    "message": f"成人资源不参与共享登记：{label}",
                },
                200 if mode == "report" else 400,
            )
        try:
            prepared = prepare_source_registration(
                source,
                [value for value in files if isinstance(value, Mapping)],
                raw_by_sha1=raw_by_sha1 if isinstance(raw_by_sha1, Mapping) else {},
                formatted_by_sha1=formatted_by_sha1 if isinstance(formatted_by_sha1, Mapping) else {},
            )
        except SharingSourceRegistrationSkipped as exc:
            payload = {
                "success": mode == "reregister",
                "skipped": True,
                "reason": exc.reason,
                "source_id": normalized_id,
                "message": exc.message,
            }
            return SharingHttpResult(payload, 200)
        except SharingSourceRegistrationError as exc:
            payload = {"success": False, "source_id": normalized_id, "message": str(exc)}
            return SharingHttpResult(payload, 200 if mode == "report" else 400)

        provider_payload = dict(prepared.provider_payload)
        identity = (
            prepared.source_kind,
            provider_payload.get("tmdb_id"),
            provider_payload.get("season_number"),
            provider_payload.get("episode_number") if prepared.source_kind == "episode" else None,
            provider_payload.get("sha1") if prepared.source_kind == "movie" else None,
        )
        if seen_identity is not None:
            if identity in seen_identity:
                return SharingHttpResult(
                    {
                        "success": True,
                        "skipped": True,
                        "reason": "duplicate_media_identity",
                        "source_id": normalized_id,
                        "message": "同一媒体身份已在本批次提交，已跳过重复源",
                    }
                )
            seen_identity.add(identity)

        principal = _required(principal_id, "principal_id")
        scope_key = f"local-source-registration:{normalized_id}"
        occurrence = _operation_occurrence(
            idempotency_key,
            kind=SharingOperationKind.REGISTER_SOURCE,
            principal_id=principal,
            scope_key=scope_key,
            request={
                "mode": mode,
                "source_id": normalized_id,
                "source_state_hash": prepared.source_state_hash,
            },
        )
        raw_scope = f"local-source-raw:{normalized_id}:{provider_payload['sha1']}"
        raw_seed = SharingOperationIntent.create(
            kind=SharingOperationKind.REGISTER_SOURCE,
            scope_key=raw_scope,
            principal_id=principal,
            idempotency_key=f"{occurrence}:raw",
            request={
                "provider": "shared_center",
                "action": "upload_raw_ffprobe",
                "payload": dict(prepared.raw_payload),
                "readback": dict(prepared.raw_readback),
            },
        )
        dependent_request = {
            "provider": "shared_center",
            "action": "register_source",
            "payload": {"source_kind": prepared.source_kind, "body": provider_payload},
            "readback": dict(prepared.provider_readback),
            "local_continuation": dict(prepared.local_continuation),
            "blocked_by_operation_id": str(raw_seed.id),
            "registration_mode": mode,
        }
        dependent_intent = SharingOperationIntent.create(
            kind=SharingOperationKind.REGISTER_SOURCE,
            scope_key=scope_key,
            principal_id=principal,
            idempotency_key=f"{occurrence}:register",
            request=dependent_request,
        )
        prerequisite_intent = SharingOperationIntent.create(
            kind=SharingOperationKind.REGISTER_SOURCE,
            scope_key=raw_scope,
            principal_id=principal,
            idempotency_key=f"{occurrence}:raw",
            request={
                "provider": "shared_center",
                "action": "upload_raw_ffprobe",
                "payload": dict(prepared.raw_payload),
                "readback": dict(prepared.raw_readback),
                "local_continuation": {
                    "type": "release_dependent_operation",
                    "operation_id": str(dependent_intent.id),
                },
            },
        )
        if prerequisite_intent.id != raw_seed.id:
            raise AssertionError("sharing raw prerequisite identity drifted")
        try:
            operation, prerequisite, replayed = cast(
                Callable[..., tuple[SharingOperation, SharingOperation, bool]],
                reserve_chain,
            )(
                dependent_intent=dependent_intent,
                prerequisite_intent=prerequisite_intent,
            )
        except (SharingOperationConflictError, SharingOperationScopeBusyError):
            return SharingHttpResult(
                {
                    "success": False,
                    "status": "conflict",
                    "source_id": normalized_id,
                    "message": "同一共享源登记操作仍在处理中",
                },
                409,
            )
        operation = cast(SharingOperation, operation)
        prerequisite = cast(SharingOperation, prerequisite)
        status = operation.status
        payload: dict[str, Any] = {
            "success": status not in {SharingOperationStatus.REJECTED, SharingOperationStatus.INDETERMINATE},
            "accepted": not status.terminal,
            "source_id": normalized_id,
            "operation_id": str(operation.id),
            "prerequisite_operation_id": str(prerequisite.id),
            "status": status.value,
            "idempotency_key": occurrence,
            "replayed": bool(replayed),
            "message": "共享源重新登记任务已提交" if mode == "reregister" else "共享源中心登记任务已提交",
        }
        if status is SharingOperationStatus.CONFIRMED:
            payload.update(message="共享源登记已确认", result=dict(operation.result or {}))
            return SharingHttpResult(payload)
        if status is SharingOperationStatus.REJECTED:
            payload.update(message="共享中心拒绝了登记操作", error=dict(operation.error or {}))
            return SharingHttpResult(payload, 409)
        if status is SharingOperationStatus.INDETERMINATE:
            payload.update(message="共享源登记结果不确定，请人工核对", error=dict(operation.error or {}))
        if status is SharingOperationStatus.CANCELLED:
            payload.update(success=False, message="共享源登记操作已取消", error=dict(operation.error or {}))
            return SharingHttpResult(payload, 409)
        return SharingHttpResult(payload, 202)

    def disable_local_source(
        self,
        source_id: int,
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> SharingHttpResult:
        return self._local_source_lifecycle(
            source_id,
            delete=False,
            principal_id=principal_id,
            idempotency_key=idempotency_key,
        )

    def delete_local_source(
        self,
        source_id: int,
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> SharingHttpResult:
        return self._local_source_lifecycle(
            source_id,
            delete=True,
            principal_id=principal_id,
            idempotency_key=idempotency_key,
        )

    def disable_local_sources(
        self,
        source_ids: Sequence[int],
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> SharingHttpResult:
        return self._local_source_lifecycle_batch(
            source_ids,
            delete=False,
            principal_id=principal_id,
            idempotency_key=idempotency_key,
        )

    def delete_local_sources(
        self,
        source_ids: Sequence[int],
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> SharingHttpResult:
        return self._local_source_lifecycle_batch(
            source_ids,
            delete=True,
            principal_id=principal_id,
            idempotency_key=idempotency_key,
        )

    def _local_source_lifecycle(
        self,
        source_id: int,
        *,
        delete: bool,
        principal_id: str,
        idempotency_key: str | None,
    ) -> SharingHttpResult:
        if self._projection is None:
            return SharingHttpResult({"success": False, "message": "共享资源本地投影不可用"}, 503)
        normalized_id = int(source_id)
        if normalized_id < 1:
            raise ValueError("source_id must be positive")
        item = self._projection.get_local_source(normalized_id)
        if item is None:
            return SharingHttpResult({"success": False, "message": "本地共享源不存在"}, 404)
        if not _source_requires_center_disable(item):
            mutation = self._projection.delete_local_source if delete else self._projection.disable_local_source
            saved = mutation(
                normalized_id,
                expected_center_source_id=str(item.get("center_source_id") or ""),
                expected_status=str(item.get("status") or ""),
                expected_center_status=str(item.get("center_status") or ""),
            )
            if saved is not None:
                return SharingHttpResult(
                    {
                        "success": True,
                        "accepted": False,
                        "status": "confirmed",
                        "source_id": normalized_id,
                        "item": saved,
                        "message": "已删除本地共享记录" if delete else "已停用本地共享源",
                    }
                )
            item = self._projection.get_local_source(normalized_id)
            if item is None:
                return SharingHttpResult({"success": False, "message": "本地共享源不存在"}, 404)
            if not _source_requires_center_disable(item):
                return SharingHttpResult(
                    {"success": False, "message": "本地共享源状态已变化，请刷新后重试"},
                    409,
                )
        source_kind = _required(item.get("source_kind"), "source_kind")
        center_source_id = _required(item.get("center_source_id"), "center_source_id")
        result = self._stage_provider_operation(
            kind=SharingOperationKind.REGISTER_SOURCE,
            scope_key=f"local-source-lifecycle:{normalized_id}",
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            request={
                "provider": "shared_center",
                "action": "disable_source",
                "payload": {
                    "source_kind": source_kind,
                    "source_id": center_source_id,
                    "message": "local delete" if delete else "local disabled",
                },
                "local_continuation": {
                    "type": "delete_local_source" if delete else "disable_local_source",
                    "source_id": normalized_id,
                    "expected_source_kind": source_kind,
                    "expected_center_source_id": center_source_id,
                },
            },
            accepted_message=(
                "中心取消登记任务已提交，确认后删除本地共享源" if delete else "中心停用任务已提交，确认后停用本地共享源"
            ),
        )
        payload: dict[str, Any] = dict(result.payload) if isinstance(result.payload, Mapping) else {"success": False}
        payload["source_id"] = normalized_id
        return SharingHttpResult(payload, result.status_code, result.headers)

    def _local_source_lifecycle_batch(
        self,
        source_ids: Sequence[int],
        *,
        delete: bool,
        principal_id: str,
        idempotency_key: str | None,
    ) -> SharingHttpResult:
        normalized_ids = list(dict.fromkeys(int(value) for value in source_ids if int(value) > 0))
        if not normalized_ids:
            raise ValueError("source_ids must not be empty")
        if len(normalized_ids) > 200:
            raise ValueError("source_ids exceeds 200 items")
        results: list[dict[str, Any]] = []
        operation_ids: list[str] = []
        missing: list[int] = []
        failed: list[dict[str, Any]] = []
        completed = 0
        for source_id in normalized_ids:
            item_key = f"{idempotency_key}:{source_id}" if idempotency_key else None
            result = self._local_source_lifecycle(
                source_id,
                delete=delete,
                principal_id=principal_id,
                idempotency_key=item_key,
            )
            payload = dict(result.payload) if isinstance(result.payload, Mapping) else {}
            if result.status_code == 404:
                missing.append(source_id)
                continue
            if result.status_code >= 400:
                failed.append({"id": source_id, "message": payload.get("message") or "共享源操作失败"})
                continue
            operation_id = str(payload.get("operation_id") or "").strip()
            status = str(payload.get("status") or "").strip().casefold()
            if (
                operation_id
                and payload.get("accepted") is True
                and status
                not in {
                    "confirmed",
                    "rejected",
                    "indeterminate",
                    "cancelled",
                }
            ):
                operation_ids.append(operation_id)
            elif status == "confirmed":
                completed += 1
            elif operation_id or status in {"rejected", "indeterminate", "cancelled"}:
                failed.append(
                    {
                        "id": source_id,
                        "operation_id": operation_id or None,
                        "status": status or "failed",
                        "message": payload.get("message") or "共享源操作未确认",
                    }
                )
                continue
            else:
                completed += 1
            results.append(payload)
        accepted = len(operation_ids)
        action_text = "删除" if delete else "停用"
        parts = [f"已确认 {completed} 个，已提交 {accepted} 个{action_text}任务"]
        if missing:
            parts.append(f"{len(missing)} 个不存在")
        if failed:
            parts.append(f"{len(failed)} 个提交失败")
        status_code = 202 if operation_ids else (409 if failed and not results else 200)
        return SharingHttpResult(
            {
                "success": not failed,
                "accepted": bool(operation_ids),
                "partial": bool(failed and results),
                "message": "，".join(parts),
                "operation_ids": operation_ids,
                "items": results,
                "missing": missing,
                "failed": failed,
            },
            status_code,
        )

    def virtual_imports(self, **query: Any) -> SharingHttpResult:
        if self._projection is None:
            return SharingHttpResult({"success": False, "message": "虚拟入库投影不可用"}, 503)
        return SharingHttpResult({"success": True, **self._projection.list_virtual_imports(**query)})

    def import_center_source(
        self,
        payload: Mapping[str, Any],
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> SharingHttpResult:
        if self._virtual_imports is None:
            return SharingHttpResult({"success": False, "message": "共享虚拟入库服务不可用"}, 503)
        source_value = payload.get("source") if isinstance(payload.get("source"), Mapping) else payload
        if not isinstance(source_value, Mapping):
            raise ValueError("source must be an object")
        mode = str(payload.get("mode") or "rapid").strip().casefold() or "rapid"
        if mode not in {"rapid", "share"}:
            return SharingHttpResult({"success": False, "message": "共享池转存模式必须是 rapid 或 share"}, 400)
        expected_channel_id = _share_channel_id(source_value)
        source = self._virtual_imports.freeze(_without_share_credentials(source_value))
        if mode == "share" and source.source_kind != "logical_season":
            return SharingHttpResult(
                {"success": False, "message": "115 分享转存仅支持 logical_season 资源"},
                400,
            )
        config = self._configuration.load() if self._configuration is not None else {}
        force_real = _bool_value(payload.get("force_real", False))
        if _bool_value(config.get("p115_shared_virtual_import_enabled", False)) and not force_real:
            result = self._stage_provider_operation(
                kind=SharingOperationKind.VIRTUAL_IMPORT,
                scope_key=f"virtual-import:{source.source_kind}:{source.source_id}",
                principal_id=principal_id,
                idempotency_key=idempotency_key,
                request={
                    "provider": "virtual_import",
                    "action": "create",
                    "payload": {
                        "source_kind": source.source_kind,
                        "source_id": source.source_id,
                        "source": source.source,
                        "source_state_hash": source.state_hash,
                    },
                },
                accepted_message="虚拟入库任务已提交",
            )
            return _with_data(result, ok=True, accepted=True, virtual=True)
        request = {
            "provider": "p115",
            "action": "transfer_logical_season_share" if mode == "share" else "transfer_center_source",
            "payload": {
                "source_kind": source.source_kind,
                "source_id": source.source_id,
                "source": source.source,
                "source_state_hash": source.state_hash,
                "mode": mode,
                **({"expected_channel_id": expected_channel_id} if expected_channel_id else {}),
            },
            "local_continuation": {
                "type": "finalize_center_transfer",
                "source_kind": source.source_kind,
                "source_id": source.source_id,
            },
        }
        result = self._stage_provider_operation(
            kind=SharingOperationKind.TRANSFER,
            scope_key=(
                f"center-share-transfer:{source.source_kind}:{source.source_id}"
                if mode == "share"
                else f"center-transfer:{source.source_kind}:{source.source_id}"
            ),
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            request=request,
            accepted_message="共享中心分享转存任务已提交" if mode == "share" else "共享中心转存任务已提交",
        )
        return _with_data(result, ok=True, accepted=True)

    def delete_virtual_import(
        self,
        virtual_id: int,
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> SharingHttpResult:
        if self._virtual_imports is None:
            return SharingHttpResult({"success": False, "message": "共享虚拟入库服务不可用"}, 503)
        replay = self._replay_virtual_import_deletion(
            virtual_id,
            principal_id=principal_id,
            idempotency_key=idempotency_key,
        )
        if replay is not None:
            return replay
        item = self._virtual_imports.get(virtual_id)
        if item is None:
            return SharingHttpResult({"success": False, "message": "虚拟入库记录不存在"}, 404)
        state_hash = virtual_import_state_hash(item)
        result = self._stage_provider_operation(
            kind=SharingOperationKind.VIRTUAL_IMPORT,
            scope_key=f"virtual-import:{virtual_id}",
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            request={
                "provider": "virtual_import",
                "action": "delete",
                "payload": {"virtual_id": virtual_id, "state_hash": state_hash},
                "local_continuation": {
                    "type": "finalize_virtual_import_deletion",
                    "virtual_id": virtual_id,
                    "expected_state_hash": state_hash,
                },
            },
            accepted_message="已提交虚拟入库辞退任务",
        )
        return _with_data(result, item=item)

    def promote_virtual_import(
        self,
        virtual_id: int,
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> SharingHttpResult:
        if self._virtual_imports is None:
            return SharingHttpResult({"success": False, "message": "共享虚拟入库服务不可用"}, 503)
        item = self._virtual_imports.get(virtual_id)
        if item is None:
            return SharingHttpResult({"success": False, "message": "虚拟入库记录不存在"}, 404)
        source = item.get("source_payload_json")
        if not isinstance(source, Mapping) or not source:
            return SharingHttpResult({"success": False, "message": "虚拟记录缺少中心源 payload，无法正式入库"}, 400)
        frozen = self._virtual_imports.freeze(source)
        state_hash = virtual_import_state_hash(item)
        request = {
            "provider": "p115",
            "action": "transfer_center_source",
            "payload": {
                "source_kind": frozen.source_kind,
                "source_id": frozen.source_id,
                "source": frozen.source,
                "source_state_hash": frozen.state_hash,
                "mode": "rapid",
                "virtual_id": virtual_id,
                "virtual_state_hash": state_hash,
            },
            "local_continuation": {
                "type": "finalize_center_transfer",
                "source_kind": frozen.source_kind,
                "source_id": frozen.source_id,
                "virtual_id": virtual_id,
                "expected_state_hash": state_hash,
            },
        }
        result = self._stage_provider_operation(
            kind=SharingOperationKind.VIRTUAL_IMPORT,
            scope_key=f"virtual-import:{virtual_id}",
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            request=request,
            accepted_message="已提交转正，等待正式入库完成",
        )
        return _with_data(result, item=item)

    def search_shareable_media(self, keyword: str, *, limit: int) -> SharingHttpResult:
        if self._projection is None:
            return SharingHttpResult({"success": False, "message": "共享资源媒体投影不可用"}, 503)
        return SharingHttpResult(
            {"success": True, "items": self._projection.search_shareable_media(keyword, limit=limit)}
        )

    def validate_manual_source(self, payload: Mapping[str, Any]) -> SharingHttpResult:
        if self._projection is None:
            return SharingHttpResult({"success": False, "message": "共享资源媒体投影不可用"}, 503)
        result = self._projection.validate_manual_source(payload)
        return SharingHttpResult(
            {
                "success": True,
                "message": result.get("message") or "预校验完成",
                "data": result,
                "files": result.get("files") or [],
                "missing_raw": result.get("missing_raw") or [],
            }
        )

    @staticmethod
    def share_request_param_options() -> SharingHttpResult:
        values = {
            "resolution": ["4K", "1080p", "720p", "480p"],
            "codec": ["HEVC", "H.264", "AV1", "VP9"],
            "effect": ["DoVi P8", "DoVi P7", "DoVi P5", "DoVi", "HDR10+", "HDR", "SDR"],
            "audio": ["国语", "粤语", "英语", "日语", "韩语"],
            "subtitle": ["简体", "繁体", "英文", "日文", "韩文", "无"],
        }
        data = {key: [{"label": value, "value": value} for value in options] for key, options in values.items()}
        data["frame_rate"] = [
            {"label": label, "value": value}
            for label, value in (("≥ 60 fps", "60"), ("≥ 50 fps", "50"), ("≥ 30 fps", "30"), ("24 fps", "24"))
        ]
        return SharingHttpResult({"success": True, "data": data})

    def search_share_request_tmdb(self, keyword: str, *, media_type: str) -> SharingHttpResult:
        normalized_keyword = str(keyword or "").strip()
        if not normalized_keyword:
            return SharingHttpResult({"success": True, "items": []})
        if self._tmdb is None:
            return SharingHttpResult({"success": False, "message": "TMDb 搜索不可用"}, 503)
        configured = getattr(self._tmdb, "daily_theme_configured", None)
        if callable(configured) and not bool(configured()):
            return SharingHttpResult({"success": False, "message": "未配置 TMDb API Key"}, 400)
        normalized_type = "tv" if str(media_type or "").strip().lower() in {"tv", "series"} else "movie"
        try:
            payload = self._tmdb.search(query=normalized_keyword, media_type=normalized_type, page=1)
        except (RuntimeError, TypeError, ValueError):
            return SharingHttpResult({"success": False, "message": "TMDb 搜索失败"}, 503)
        if not isinstance(payload, Mapping):
            return SharingHttpResult({"success": False, "message": "TMDb 搜索失败"}, 503)
        raw_rows = payload.get("results")
        rows = raw_rows if isinstance(raw_rows, list) else []
        items = [_share_request_tmdb_item(row, normalized_type) for row in rows[:20] if isinstance(row, Mapping)]
        return SharingHttpResult({"success": True, "items": items})

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

    def register_device(
        self,
        *,
        name: str,
        principal_id: str,
        idempotency_key: str | None,
    ) -> SharingHttpResult:
        if self._identity is None:
            return SharingHttpResult({"success": False, "message": "Emby ServerID 不可用"}, 400)
        try:
            server_id_hash = self._identity.server_id_hash()
        except ExternalAdapterError as exc:
            return _provider_http_result(ProviderResponse(exc.disposition, exc.status_code))
        return self._stage_provider_operation(
            kind=SharingOperationKind.REGISTER_DEVICE,
            scope_key=f"device-registration:{server_id_hash}",
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            request={
                "provider": "shared_center",
                "action": "register_device",
                "payload": {
                    "name": str(name or "ETK Device").strip() or "ETK Device",
                    "server_id_hash": server_id_hash,
                },
                "readback": {
                    "provider": "shared_center",
                    "action": "device_status",
                    "query": {},
                    "match_key": "device.server_id_hash",
                    "match_value": server_id_hash,
                },
                "local_continuation": {"type": "enable_shared_resource"},
            },
            accepted_message="共享资源中心连接任务已提交，等待后台确认",
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
        principal_id: str,
        idempotency_key: str | None = None,
    ) -> SharingHttpResult:
        normalized_kind = str(source_kind or "").strip().lower()
        normalized_payload = normalize_durable_json_object(dict(payload), field="sharing source payload")
        return self._stage_provider_operation(
            kind=SharingOperationKind.REGISTER_SOURCE,
            scope_key=_identity_scope(
                f"source-registration:{normalized_kind}",
                normalized_payload,
                ("source_id", "root_fid", "pick_code", "sha1", "tmdb_id", "season_number"),
            ),
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            request={
                "provider": "shared_center",
                "action": "register_source",
                "payload": {"source_kind": normalized_kind, "body": normalized_payload},
            },
            accepted_message="共享资源登记任务已提交，等待后台确认",
        )

    def acquire_transfer_lease(
        self,
        payload: Mapping[str, Any],
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> SharingHttpResult:
        normalized = normalize_durable_json_object(dict(payload), field="sharing transfer lease")
        scope_key = _identity_scope(
            "transfer-lease",
            normalized,
            ("lease_id", "transfer_id", "source_id", "root_fid"),
        )
        occurrence = _operation_occurrence(
            idempotency_key,
            kind=SharingOperationKind.TRANSFER,
            principal_id=_required(principal_id, "principal_id"),
            scope_key=scope_key,
            request={"provider": "shared_center", "action": "acquire_transfer_lease", "payload": normalized},
        )
        amount = _positive_optional_int(normalized.get("amount") or normalized.get("cost"))
        source_id = str(normalized.get("source_id") or "").strip()
        if amount is not None and source_id:
            try:
                reserved = self.reserve_transfer(
                    account_id=str(normalized.get("account_id") or principal_id),
                    source_id=source_id,
                    amount=amount,
                    principal_id=principal_id,
                    idempotency_key=occurrence,
                    request=normalized,
                )
            except (SharingOperationConflictError, SharingOperationScopeBusyError):
                return SharingHttpResult(
                    {"success": False, "status": "conflict", "message": "同一转存租约仍在处理中"},
                    409,
                )
            return SharingHttpResult(
                {
                    **dict(reserved.payload),
                    "message": "转存租约任务已提交，等待后台确认",
                    "idempotency_key": occurrence,
                },
                reserved.status_code,
                reserved.headers,
            )
        return self._stage_provider_operation(
            kind=SharingOperationKind.TRANSFER,
            scope_key=scope_key,
            principal_id=principal_id,
            idempotency_key=occurrence,
            request={"provider": "shared_center", "action": "acquire_transfer_lease", "payload": normalized},
            accepted_message="转存租约任务已提交，等待后台确认",
        )

    def report_transfer(
        self,
        payload: Mapping[str, Any],
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> SharingHttpResult:
        normalized = normalize_durable_json_object(dict(payload), field="sharing transfer report")
        return self._stage_provider_operation(
            kind=SharingOperationKind.TRANSFER,
            scope_key=_identity_scope(
                "transfer-report",
                normalized,
                ("report_id", "transfer_id", "lease_id", "source_id"),
            ),
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            request={"provider": "shared_center", "action": "report_transfer", "payload": normalized},
            accepted_message="转存结果上报任务已提交，等待后台确认",
        )

    def quote_share_request(self, payload: Mapping[str, Any]) -> SharingHttpResult:
        response = self._provider_call(
            self._center.quote_share_request,
            normalize_durable_json_object(dict(payload), field="sharing quote request"),
        )
        return _provider_http_result(response)

    def list_share_requests(self, **query: Any) -> SharingHttpResult:
        response = self._provider_call(self._center.list_share_requests, **query)
        return _provider_http_result(response)

    def create_share_request(
        self,
        payload: Mapping[str, Any],
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> SharingHttpResult:
        normalized = normalize_durable_json_object(dict(payload), field="sharing request")
        return self._stage_provider_operation(
            kind=SharingOperationKind.SHARE_REQUEST,
            scope_key=_identity_scope(
                "share-request",
                normalized,
                (
                    "group_id",
                    "request_id",
                    "tmdb_id",
                    "media_type",
                    "target_type",
                    "season_number",
                    "episode_number",
                    "episode_numbers",
                ),
            ),
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            request={"provider": "shared_center", "action": "create_share_request", "payload": normalized},
            accepted_message="求共享任务已提交，等待后台确认",
        )

    def co_request_share_request(
        self,
        group_id: str,
        payload: Mapping[str, Any],
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> SharingHttpResult:
        return self._stage_share_request_participant_operation(
            "co_request_share_request",
            group_id,
            payload,
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            accepted_message="同求任务已提交，等待后台确认",
        )

    def cancel_share_request(
        self,
        group_id: str,
        payload: Mapping[str, Any],
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> SharingHttpResult:
        return self._stage_share_request_participant_operation(
            "cancel_share_request",
            group_id,
            payload,
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            accepted_message="取消求共享任务已提交，等待后台确认",
        )

    def _stage_share_request_participant_operation(
        self,
        action: str,
        group_id: str,
        payload: Mapping[str, Any],
        *,
        principal_id: str,
        idempotency_key: str | None,
        accepted_message: str,
    ) -> SharingHttpResult:
        normalized_group = _required(group_id, "group_id")
        if len(normalized_group) > 200:
            raise ValueError("group_id is too long")
        normalized_principal = _required(principal_id, "principal_id")
        normalized = normalize_durable_json_object(dict(payload), field="share-request participant payload")
        return self._stage_provider_operation(
            kind=SharingOperationKind.SHARE_REQUEST,
            scope_key=f"share-request-participant:{normalized_group}:{normalized_principal}",
            principal_id=normalized_principal,
            idempotency_key=idempotency_key,
            request={
                "provider": "shared_center",
                "action": action,
                "payload": {"group_id": normalized_group, "body": normalized},
            },
            accepted_message=accepted_message,
        )

    def _stage_provider_operation(
        self,
        *,
        kind: SharingOperationKind,
        scope_key: str,
        principal_id: str,
        idempotency_key: str | None,
        request: Mapping[str, Any],
        accepted_message: str,
    ) -> SharingHttpResult:
        if self._operations is None:
            return SharingHttpResult({"success": False, "message": "共享操作运行时不可用"}, 503)
        normalized_principal = _required(principal_id, "principal_id")
        occurrence = _operation_occurrence(
            idempotency_key,
            kind=kind,
            principal_id=normalized_principal,
            scope_key=scope_key,
            request=request,
        )
        intent = SharingOperationIntent.create(
            kind=kind,
            scope_key=scope_key,
            principal_id=normalized_principal,
            request=request,
            idempotency_key=occurrence,
        )
        try:
            reserve_with_replay = getattr(self._operations, "reserve_with_replay", None)
            if callable(reserve_with_replay):
                operation, replayed = cast(
                    Callable[[SharingOperationIntent], tuple[Any, bool]],
                    reserve_with_replay,
                )(intent)
            else:
                operation = self._operations.reserve(intent)
                replayed = operation.status is not SharingOperationStatus.READY or operation.attempts > 0
        except (SharingOperationConflictError, SharingOperationScopeBusyError):
            return SharingHttpResult(
                {"success": False, "status": "conflict", "message": "同一共享操作仍在处理中"},
                409,
            )
        return self._provider_operation_result(
            operation,
            replayed=replayed,
            accepted_message=accepted_message,
            idempotency_key=occurrence,
        )

    def stage_provider_operation(
        self,
        *,
        kind: SharingOperationKind,
        scope_key: str,
        principal_id: str,
        idempotency_key: str | None,
        request: Mapping[str, Any],
        accepted_message: str,
    ) -> SharingHttpResult:
        """Stage one allow-listed provider action for Worker-only execution."""

        return self._stage_provider_operation(
            kind=kind,
            scope_key=scope_key,
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            request=request,
            accepted_message=accepted_message,
        )

    def _replay_virtual_import_deletion(
        self,
        virtual_id: int,
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> SharingHttpResult | None:
        occurrence = str(idempotency_key or "").strip()
        reader = getattr(self._operations, "read_by_occurrence", None)
        if not occurrence or not callable(reader):
            return None
        operation = cast(
            SharingOperation | None,
            reader(
                kind=SharingOperationKind.VIRTUAL_IMPORT,
                principal_id=_required(principal_id, "principal_id"),
                idempotency_key=occurrence,
            ),
        )
        if operation is None:
            return None
        request: Mapping[str, Any] = operation.request if isinstance(operation.request, Mapping) else {}
        payload_value = request.get("payload")
        payload: Mapping[str, Any] = payload_value if isinstance(payload_value, Mapping) else {}
        if (
            operation.scope_key != f"virtual-import:{virtual_id}"
            or request.get("provider") != "virtual_import"
            or request.get("action") != "delete"
            or payload.get("virtual_id") != virtual_id
        ):
            return SharingHttpResult(
                {"success": False, "status": "conflict", "message": "幂等键已用于其他共享操作"},
                409,
            )
        return self._provider_operation_result(
            operation,
            replayed=True,
            accepted_message="已提交虚拟入库辞退任务",
            idempotency_key=occurrence,
        )

    @staticmethod
    def _provider_operation_result(
        operation: SharingOperation,
        *,
        replayed: bool,
        accepted_message: str,
        idempotency_key: str,
    ) -> SharingHttpResult:
        status = operation.status
        payload: dict[str, Any] = {
            "success": status not in {SharingOperationStatus.REJECTED, SharingOperationStatus.INDETERMINATE},
            "accepted": not status.terminal,
            "message": accepted_message,
            "operation_id": str(operation.id),
            "status": status.value,
            "idempotency_key": idempotency_key,
            "replayed": replayed,
        }
        if status is SharingOperationStatus.CONFIRMED:
            payload["message"] = "共享操作已确认"
            payload["result"] = dict(operation.result or {})
            return SharingHttpResult(payload)
        if status is SharingOperationStatus.REJECTED:
            payload["message"] = "共享中心拒绝了操作"
            payload["error"] = dict(operation.error or {})
            return SharingHttpResult(payload, 409)
        if status is SharingOperationStatus.INDETERMINATE:
            payload["message"] = "共享操作结果不确定，请人工核对后再处理"
            payload["error"] = dict(operation.error or {})
        return SharingHttpResult(payload, 202)

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
                "cancel_requested_at": (
                    operation.cancel_requested_at.isoformat() if operation.cancel_requested_at else None
                ),
                "finished_at": operation.finished_at.isoformat() if operation.finished_at else None,
                "result": dict(operation.result or {}),
                "error": dict(operation.error or {}),
            },
        }
        return SharingHttpResult(payload)

    def cancel_operation(self, operation_id: str) -> SharingHttpResult:
        try:
            parsed = UUID(str(operation_id))
        except (TypeError, ValueError, AttributeError):
            return SharingHttpResult({"success": False, "message": "共享操作不存在"}, 404)
        if self._operations is None:
            return SharingHttpResult({"success": False, "message": "共享操作不可用"}, 503)
        request_cancel = getattr(self._operations, "request_cancel", None)
        if not callable(request_cancel):
            return SharingHttpResult({"success": False, "message": "共享操作取消不可用"}, 503)
        operation = cast(Callable[[UUID], SharingOperation | None], request_cancel)(parsed)
        if operation is None:
            return SharingHttpResult({"success": False, "message": "共享操作不存在"}, 404)
        payload = {
            "operation_id": str(operation.id),
            "status": operation.status.value,
            "cancel_requested_at": (
                operation.cancel_requested_at.isoformat() if operation.cancel_requested_at else None
            ),
        }
        if operation.status is SharingOperationStatus.CANCELLED:
            return SharingHttpResult({"success": True, "message": "共享操作已在远端请求前取消", **payload})
        if operation.status.terminal:
            return SharingHttpResult(
                {"success": False, "message": "共享操作已终结，不能取消", **payload},
                409,
            )
        return SharingHttpResult(
            {
                "success": True,
                "accepted": True,
                "message": "共享操作已请求取消；远端请求已进入确认边界，将继续权威回读",
                **payload,
            },
            202,
        )

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


_SHARE_CHANNEL_CONTAINER_KEYS = frozenset(
    {
        "share_channel",
        "logical_season_share_channel",
        "completed_share_channel",
        "logical_share_channel",
    }
)
_SHARE_CREDENTIAL_KEYS = frozenset(
    {
        "share_code",
        "receive_code",
        "share_url",
    }
)


def _share_channel_id(source: Mapping[str, Any]) -> str:
    candidates = [source]
    logical_group = source.get("logical_group")
    if isinstance(logical_group, Mapping):
        candidates.append(logical_group)
    for candidate in candidates:
        for key in _SHARE_CHANNEL_CONTAINER_KEYS:
            value = candidate.get(key)
            if isinstance(value, Mapping):
                channel_id = str(value.get("channel_id") or "").strip()
                if channel_id:
                    return channel_id[:255]
        channel_id = str(candidate.get("share_channel_id") or "").strip()
        if channel_id:
            return channel_id[:255]
    return ""


def _without_share_credentials(value: Mapping[str, Any]) -> dict[str, Any]:
    def scrub(current: Any) -> Any:
        if isinstance(current, Mapping):
            result: dict[str, Any] = {}
            for raw_key, item in current.items():
                key = str(raw_key)
                normalized = key.strip().casefold().replace("-", "_")
                if normalized in _SHARE_CHANNEL_CONTAINER_KEYS or normalized in _SHARE_CREDENTIAL_KEYS:
                    continue
                result[key] = scrub(item)
            return result
        if isinstance(current, list):
            return [scrub(item) for item in current]
        return current

    return cast(dict[str, Any], scrub(value))


def _display_count(value: object, default: int) -> int:
    try:
        return max(0, int(str(value).strip())) if value not in (None, "") else max(0, int(default))
    except (TypeError, ValueError, OverflowError):
        return max(0, int(default))


def _share_request_tmdb_item(row: Mapping[str, Any], media_type: str) -> dict[str, Any]:
    if media_type == "movie":
        release = str(row.get("release_date") or "")
        title = _first_text(row.get("title"), row.get("name"), row.get("original_title"))
    else:
        release = str(row.get("first_air_date") or "")
        title = _first_text(row.get("name"), row.get("title"), row.get("original_name"))
    release_year = _display_count(release[:4], 0) or None if len(release) >= 4 else None
    return {
        "tmdb_id": str(row.get("id") or ""),
        "media_type": "movie" if media_type == "movie" else "tv",
        "title": title,
        "release_year": release_year,
        "poster_path": row.get("poster_path") or "",
        "overview": row.get("overview") or "",
    }


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


def _scope_digest(value: Mapping[str, Any]) -> str:
    normalized = normalize_durable_json_object(dict(value), field="sharing operation scope")
    return hashlib.sha256(canonical_json(normalized, field="sharing operation scope").encode("utf-8")).hexdigest()


def _identity_scope(prefix: str, value: Mapping[str, Any], fields: tuple[str, ...]) -> str:
    identity = {field: value[field] for field in fields if field in value and value[field] not in (None, "", [], {})}
    return f"{prefix}:{_scope_digest(identity or value)}"


def _source_requires_center_disable(value: Mapping[str, Any]) -> bool:
    if not str(value.get("center_source_id") or "").strip():
        return False
    status = str(value.get("status") or "").strip().casefold()
    center_status = str(value.get("center_status") or "").strip().casefold()
    inactive = {"disabled", "cancelled", "canceled", "deleted"}
    dirty = {"dirty_raw", "dirty_summary", "dirty_meta"}
    if status in inactive or center_status in inactive | dirty:
        return False
    return status in {"active", "available", "updating", "pending", ""} or center_status in {
        "reported",
        "partial",
        "local",
        "pending",
        "",
    }


def _operation_occurrence(
    value: str | None,
    *,
    kind: SharingOperationKind,
    principal_id: str,
    scope_key: str,
    request: Mapping[str, Any],
) -> str:
    supplied = str(value or "").strip()
    if supplied:
        if len(supplied) > 256:
            raise ValueError("sharing operation idempotency key is too long")
        return supplied
    normalized = normalize_durable_json_object(dict(request), field="sharing operation occurrence")
    material = "\0".join(
        (
            kind.value,
            principal_id,
            scope_key,
            canonical_json(normalized, field="sharing operation occurrence"),
        )
    )
    return f"auto:{hashlib.sha256(material.encode('utf-8')).hexdigest()}"


def _positive_optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        raise ValueError("sharing transfer amount must be a positive integer")
    try:
        result = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError("sharing transfer amount must be a positive integer") from exc
    if result < 1:
        raise ValueError("sharing transfer amount must be a positive integer")
    return result


def _required(value: object, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return normalized


def _with_data(result: SharingHttpResult, **values: Any) -> SharingHttpResult:
    """Merge small route-facing hints without changing the operation payload."""

    payload = dict(result.payload) if isinstance(result.payload, Mapping) else {"result": result.payload}
    current = payload.get("data")
    data = dict(current) if isinstance(current, Mapping) else {}
    data.update(values)
    payload["data"] = data
    return SharingHttpResult(payload, result.status_code, result.headers)


__all__ = [
    "SharingHttpResult",
    "SharingLedgerService",
    "SharingService",
    "entry_to_json",
]
