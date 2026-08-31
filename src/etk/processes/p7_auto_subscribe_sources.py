from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, cast

from etk.domains.sharing.execution import (
    SharingDeliveryOutcome,
    SharingOperationDeliveryPort,
    SharingOperationExecutor,
)
from etk.domains.sharing.operations import (
    SharingOperation,
    SharingOperationConflictError,
    SharingOperationIntent,
    SharingOperationKind,
    SharingOperationScopeBusyError,
    SharingOperationStatus,
    sharing_operation_key,
)
from etk.domains.sharing.repository import PostgresSharingOperationRepository
from etk.domains.sharing.virtual_import import VirtualImportService
from etk.domains.subscription.auto_subscribe import (
    AutoSourceDisposition,
    AutoSourceResult,
    AutoSubscriptionInterrupted,
    AutoSubscriptionProgress,
)
from etk.integrations import (
    HDHivePort,
    ProviderDisposition,
    ProviderResponse,
    SharedCenterPort,
    TelegramUserBotIdempotencyConflictError,
)
from etk.integrations.http_policy import ExternalAdapterError
from etk.platform import Settings
from etk.platform.jobs import PersistentExecutionControlError, normalize_json_object
from etk.platform.jobs.json_payload import canonical_json


class TelegramSearchPort(Protocol):
    def enqueue_once(
        self,
        command_type: str,
        payload: Mapping[str, Any] | None = None,
        *,
        idempotency_key: str,
        result_owner_key: str | None = None,
    ) -> str: ...

    def wait(
        self,
        command_id: str,
        *,
        timeout_seconds: float = 45,
        poll_seconds: float = 0.1,
    ) -> Mapping[str, Any]: ...

    def get_command(self, command_id: str) -> Mapping[str, Any] | None: ...

    def consume_resource_secret(self, reference: str, owner_key: str) -> Mapping[str, Any]: ...


class P115ResourceTransferPort(Protocol):
    def freeze_identity(self) -> Mapping[str, Any]: ...

    def transfer(
        self,
        resource: Mapping[str, Any],
        *,
        expected_identity: Mapping[str, Any],
    ) -> ProviderResponse[dict[str, Any]]: ...


class P115CookieClientPort(Protocol):
    def request(self, url: str, *, method: str, data: Mapping[str, Any]) -> Any: ...

    def offline_add_urls(self, payload: Mapping[str, Any]) -> Any: ...


@dataclass(frozen=True, slots=True)
class _TelegramCandidate:
    command_id: str
    index: int
    digest: str


class PersistentOrderedAutoSubscriptionSources:
    """Discover an ordered source and execute only a durable transfer operation."""

    def __init__(
        self,
        *,
        center: SharedCenterPort,
        hdhive: HDHivePort,
        telegram: TelegramSearchPort,
        operations: PostgresSharingOperationRepository,
        executor: SharingOperationExecutor,
        virtual_imports: VirtualImportService,
        shared_configuration: Callable[[], Mapping[str, Any] | None],
        telegram_configuration: Callable[[], Mapping[str, Any] | None],
        worker_id: str,
        lease_seconds: float,
    ) -> None:
        normalized_worker = str(worker_id or "").strip()
        if not normalized_worker:
            raise ValueError("auto-subscribe source worker_id must not be empty")
        if float(lease_seconds) <= 0:
            raise ValueError("auto-subscribe source lease_seconds must be positive")
        self._center = center
        self._hdhive = hdhive
        self._telegram = telegram
        self._operations = operations
        self._executor = executor
        self._virtual_imports = virtual_imports
        self._shared_configuration = shared_configuration
        self._telegram_configuration = telegram_configuration
        self._worker_id = normalized_worker
        self._lease_seconds = float(lease_seconds)

    @staticmethod
    def replay_safe(source_type: str) -> bool:
        return str(source_type or "").strip() in {"shared_pool", "hdhive", "tg_channel"}

    def attempt(
        self,
        source_type: str,
        context: Mapping[str, Any],
        progress: AutoSubscriptionProgress,
    ) -> AutoSourceResult:
        source = str(source_type or "").strip()
        occurrence = _required_text(context.get("operation_occurrence"), "operation occurrence")
        if source == "shared_pool":
            return self._shared_pool(context, occurrence=occurrence, progress=progress)
        if source == "hdhive":
            return self._hdhive_source(context, occurrence=occurrence, progress=progress)
        if source == "tg_channel":
            return self._telegram_source(context, occurrence=occurrence, progress=progress)
        return AutoSourceResult.miss()

    def _shared_pool(
        self,
        context: Mapping[str, Any],
        *,
        occurrence: str,
        progress: AutoSubscriptionProgress,
    ) -> AutoSourceResult:
        progress.checkpoint()
        try:
            response = self._center.list_sources(
                tmdb_id=_lookup_tmdb_id(context),
                item_type=_center_item_type(context),
                limit=200,
                offset=0,
            )
        except ExternalAdapterError as exc:
            return _read_error("shared_pool", exc.disposition)
        except Exception:
            return AutoSourceResult(AutoSourceDisposition.RETRYABLE, "shared_pool")
        candidate = _provider_candidate(response, context, keys=("items", "sources", "results", "data"))
        if isinstance(candidate, AutoSourceResult):
            return candidate
        try:
            frozen = self._virtual_imports.freeze(candidate)
        except (TypeError, ValueError):
            return AutoSourceResult(AutoSourceDisposition.REJECTED, "shared_pool")
        try:
            configuration = self._shared_configuration()
        except Exception:
            return AutoSourceResult(AutoSourceDisposition.RETRYABLE, "shared_pool")
        shared_config = dict(configuration) if isinstance(configuration, Mapping) else {}
        virtual = bool(shared_config.get("p115_shared_virtual_import_enabled", False))
        if virtual:
            kind = SharingOperationKind.VIRTUAL_IMPORT
            scope_key = f"virtual-import:{frozen.source_kind}:{frozen.source_id}"
            request = {
                "provider": "virtual_import",
                "action": "create",
                "payload": {
                    "source_kind": frozen.source_kind,
                    "source_id": frozen.source_id,
                    "source": frozen.source,
                    "source_state_hash": frozen.state_hash,
                },
            }
            action_type = "共享资源虚拟入库"
        else:
            kind = SharingOperationKind.TRANSFER
            scope_key = f"center-transfer:{frozen.source_kind}:{frozen.source_id}"
            request = {
                "provider": "p115",
                "action": "transfer_center_source",
                "payload": {
                    "source_kind": frozen.source_kind,
                    "source_id": frozen.source_id,
                    "source": frozen.source,
                    "source_state_hash": frozen.state_hash,
                    "mode": "rapid",
                },
                "local_continuation": {
                    "type": "finalize_center_transfer",
                    "source_kind": frozen.source_kind,
                    "source_id": frozen.source_id,
                },
            }
            action_type = "共享资源秒传"
        return self._stage_and_execute(
            kind=kind,
            scope_key=scope_key,
            occurrence=f"{occurrence}:shared_pool",
            request=request,
            action_type=action_type,
            progress=progress,
        )

    def _hdhive_source(
        self,
        context: Mapping[str, Any],
        *,
        occurrence: str,
        progress: AutoSubscriptionProgress,
    ) -> AutoSourceResult:
        progress.checkpoint()
        media_type = "movie" if str(context.get("item_type") or "") == "Movie" else "tv"
        season = _positive_int(context.get("season_number"))
        try:
            response = self._hdhive.get_resources(
                _lookup_tmdb_id(context),
                media_type,
                season=season,
            )
        except ExternalAdapterError as exc:
            return _read_error("hdhive", exc.disposition)
        except Exception:
            return AutoSourceResult(AutoSourceDisposition.RETRYABLE, "hdhive")
        candidate = _provider_candidate(
            response,
            context,
            keys=("resources", "items", "results", "data"),
            hdhive=True,
        )
        if isinstance(candidate, AutoSourceResult):
            return candidate
        slug = str(candidate.get("slug") or candidate.get("resource_id") or "").strip()
        if not slug:
            return AutoSourceResult(AutoSourceDisposition.REJECTED, "hdhive")
        slug_digest = hashlib.sha256(slug.encode("utf-8")).hexdigest()
        return self._stage_and_execute(
            kind=SharingOperationKind.TRANSFER,
            scope_key=f"auto-subscribe:hdhive:{slug_digest}",
            occurrence=f"{occurrence}:hdhive",
            request={
                "provider": "auto_subscription",
                "action": "hdhive_transfer",
                "payload": {
                    "slug": slug,
                    "slug_hash": slug_digest,
                    "tmdb_id": _lookup_tmdb_id(context),
                    "media_type": media_type,
                    "season_number": season,
                },
            },
            action_type="影巢",
            progress=progress,
        )

    def _telegram_source(
        self,
        context: Mapping[str, Any],
        *,
        occurrence: str,
        progress: AutoSubscriptionProgress,
    ) -> AutoSourceResult:
        progress.checkpoint()
        media_type = "movie" if str(context.get("item_type") or "") == "Movie" else "tv"
        search_payload = {
            "query": _required_text(context.get("title"), "title"),
            "media_type": media_type,
            "tmdb_id": _lookup_tmdb_id(context),
            "year": str(context.get("year") or ""),
            "limit": 10,
            "timeout": 30,
            "include_tmdb_query": True,
            "strict_title_match": True,
        }
        operation_occurrence = f"{occurrence}:tg_channel"
        operation_owner_key = sharing_operation_key(
            kind=SharingOperationKind.TRANSFER,
            principal_id="worker:auto-subscribe",
            idempotency_key=operation_occurrence,
        )
        try:
            command_id = self._telegram.enqueue_once(
                "search",
                search_payload,
                idempotency_key=f"{occurrence}:tg_channel:search",
                result_owner_key=operation_owner_key,
            )
        except TelegramUserBotIdempotencyConflictError:
            return _indeterminate("频道", "telegram_search_request_changed")
        except Exception:
            return AutoSourceResult(AutoSourceDisposition.RETRYABLE, "tg_channel")
        progress.checkpoint()
        try:
            command = self._telegram.wait(command_id, timeout_seconds=45, poll_seconds=0.1)
        except Exception:
            return AutoSourceResult(AutoSourceDisposition.RETRYABLE, "tg_channel")
        result = command.get("result")
        result = dict(result) if isinstance(result, Mapping) else {}
        if result.get("ok") is not True:
            return AutoSourceResult(AutoSourceDisposition.REJECTED, "tg_channel")
        search_result_ref = _required_hash(result.get("search_result_ref"), "Telegram search result reference")
        search_result_hash = _required_hash(result.get("search_result_hash"), "Telegram search result hash")
        try:
            raw_result = self._telegram.consume_resource_secret(search_result_ref, operation_owner_key)
        except Exception:
            return _indeterminate("频道", "telegram_search_result_unavailable")
        if _candidate_digest(raw_result) != search_result_hash:
            return _indeterminate("频道", "telegram_search_result_changed")
        try:
            telegram_configuration = self._telegram_configuration()
        except Exception:
            return AutoSourceResult(AutoSourceDisposition.RETRYABLE, "tg_channel")
        block_configuration = dict(telegram_configuration) if isinstance(telegram_configuration, Mapping) else {}
        candidates = [
            candidate
            for candidate in _candidate_rows(raw_result, ("results", "items", "data"))
            if not _telegram_candidate_blocked(candidate, block_configuration)
        ]
        selected = _select_candidate(candidates, context, telegram=True)
        if selected is None:
            return AutoSourceResult.miss()
        index, candidate = selected
        digest = _candidate_digest(candidate)
        identity = _TelegramCandidate(command_id, index, digest)
        return self._stage_and_execute(
            kind=SharingOperationKind.TRANSFER,
            scope_key=f"auto-subscribe:telegram:{digest}",
            occurrence=operation_occurrence,
            request={
                "provider": "auto_subscription",
                "action": "telegram_transfer",
                "payload": {
                    "command_id": identity.command_id,
                    "candidate_index": identity.index,
                    "candidate_hash": identity.digest,
                    "search_result_ref": search_result_ref,
                    "search_result_hash": search_result_hash,
                },
            },
            action_type="频道",
            progress=progress,
        )

    def _stage_and_execute(
        self,
        *,
        kind: SharingOperationKind,
        scope_key: str,
        occurrence: str,
        request: Mapping[str, Any],
        action_type: str,
        progress: AutoSubscriptionProgress,
    ) -> AutoSourceResult:
        intent = SharingOperationIntent.create(
            kind=kind,
            scope_key=scope_key,
            principal_id="worker:auto-subscribe",
            request=request,
            idempotency_key=occurrence,
        )
        try:
            operation, _replayed = self._operations.reserve_with_replay(intent)
        except SharingOperationScopeBusyError:
            return AutoSourceResult(
                AutoSourceDisposition.PENDING,
                action_type,
                {"action": action_type, "error_type": "sharing_operation_scope_busy", "retryable": True},
            )
        except SharingOperationConflictError:
            return _indeterminate(action_type, "sharing_operation_request_changed")
        except Exception as exc:
            return AutoSourceResult(
                AutoSourceDisposition.INDETERMINATE,
                action_type,
                {"action": action_type, "error_type": type(exc).__name__, "indeterminate": True},
            )
        self._checkpoint_parent(progress, operation.id)
        execution = self._executor.execute(
            operation.id,
            worker_id=f"{self._worker_id}:auto-subscribe",
            lease_seconds=self._lease_seconds,
            before_request=lambda: self._checkpoint_parent(progress, operation.id),
        )
        current = execution.operation if execution is not None else self._operations.read(operation.id)
        if current is None:
            return _indeterminate(action_type, "sharing_operation_missing")
        if current.status is SharingOperationStatus.CONFIRMED:
            return AutoSourceResult.confirmed(action_type)
        if current.status in {SharingOperationStatus.REJECTED, SharingOperationStatus.CANCELLED}:
            return AutoSourceResult(AutoSourceDisposition.REJECTED, action_type)
        if current.status in {
            SharingOperationStatus.READY,
            SharingOperationStatus.CLAIMED,
            SharingOperationStatus.REQUESTING,
            SharingOperationStatus.AWAITING_READBACK,
            SharingOperationStatus.RETRYABLE,
        }:
            return AutoSourceResult(
                AutoSourceDisposition.PENDING,
                action_type,
                {
                    "action": action_type,
                    "error_type": f"sharing_operation_{current.status.value}",
                    "retryable": True,
                },
            )
        return _indeterminate(action_type, f"sharing_operation_{current.status.value}")

    def _checkpoint_parent(self, progress: AutoSubscriptionProgress, operation_id: Any) -> None:
        try:
            progress.checkpoint()
        except Exception as exc:
            try:
                self._operations.request_cancel(operation_id)
            except Exception as cancel_exc:
                raise AutoSubscriptionInterrupted(
                    "auto-subscribe interruption could not cancel its reserved sharing operation"
                ) from cancel_exc
            if isinstance(exc, PersistentExecutionControlError):
                raise
            raise AutoSubscriptionInterrupted(
                "auto-subscribe execution was interrupted before the sharing provider request"
            ) from exc


class AutoSubscriptionTransferOperationDelivery(SharingOperationDeliveryPort):
    """Execute one HDHive or Telegram transfer without persisting raw resource links."""

    def __init__(
        self,
        *,
        repository: PostgresSharingOperationRepository,
        hdhive: HDHivePort,
        telegram: TelegramSearchPort,
        p115: P115ResourceTransferPort,
    ) -> None:
        self._repository = repository
        self._hdhive = hdhive
        self._telegram = telegram
        self._p115 = p115

    @staticmethod
    def supports(operation: SharingOperation) -> bool:
        request = operation.request if isinstance(operation.request, Mapping) else {}
        return str(request.get("provider") or "") == "auto_subscription" and str(request.get("action") or "") in {
            "hdhive_transfer",
            "telegram_transfer",
        }

    def supports_authoritative_readback(self, operation: SharingOperation) -> bool:
        del operation
        return False

    def request(
        self,
        operation: SharingOperation,
        *,
        checkpoint: Callable[[], None],
    ) -> SharingDeliveryOutcome:
        action, payload = _operation_payload(operation)
        if operation.claim_token is None:
            raise RuntimeError("auto-subscribe transfer operation is not claimed")
        identity = dict(self._p115.freeze_identity())
        identity.update(
            schema_version=1,
            kind="auto_subscription_transfer",
            action=action,
            candidate_hash=str(payload.get("candidate_hash") or payload.get("slug_hash") or ""),
            phase="prepared",
        )
        self._repository.save_prewrite_state(operation.id, operation.claim_token, identity)
        resource: Mapping[str, Any] | None = None
        if action == "telegram_transfer":
            resource = self._telegram_resource(operation, payload)
            if resource is None:
                return SharingDeliveryOutcome.rejected({"type": "telegram_candidate_unavailable"})
        checkpoint()
        if action == "hdhive_transfer":
            response = self._hdhive.unlock(_required_text(payload.get("slug"), "HDHive slug"))
            if response.disposition is ProviderDisposition.REJECTED:
                return SharingDeliveryOutcome.rejected({"type": "hdhive_unlock_rejected"})
            if response.disposition is not ProviderDisposition.CONFIRMED or not isinstance(response.payload, Mapping):
                return SharingDeliveryOutcome.indeterminate({"type": "hdhive_unlock_not_confirmed"})
            resource = _resource_from_payload(response.payload)
            if resource is None:
                return SharingDeliveryOutcome.indeterminate({"type": "hdhive_unlock_resource_invalid"})
        assert resource is not None
        try:
            transferred = self._p115.transfer(resource, expected_identity=identity)
        except Exception as exc:
            return SharingDeliveryOutcome.indeterminate(
                {"type": "p115_resource_transfer_error", "error_class": type(exc).__name__}
            )
        if transferred.disposition is ProviderDisposition.CONFIRMED:
            return SharingDeliveryOutcome.confirmed(
                {
                    "provider_response_confirmed": True,
                    "resource_kind": str(
                        (transferred.payload or {}).get("resource_kind") or resource.get("resource_kind") or ""
                    ),
                }
            )
        if transferred.disposition is ProviderDisposition.REJECTED:
            return SharingDeliveryOutcome.rejected({"type": "p115_resource_transfer_rejected"})
        return SharingDeliveryOutcome.indeterminate({"type": "p115_resource_transfer_not_confirmed"})

    def readback(self, operation: SharingOperation) -> SharingDeliveryOutcome:
        del operation
        return SharingDeliveryOutcome.indeterminate({"type": "authoritative_readback_unavailable"})

    def _telegram_resource(
        self,
        operation: SharingOperation,
        payload: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        reference = _required_hash(payload.get("search_result_ref"), "Telegram search result reference")
        expected_hash = _required_hash(payload.get("search_result_hash"), "Telegram search result hash")
        result = self._telegram.consume_resource_secret(reference, operation.operation_key)
        if _candidate_digest(result) != expected_hash:
            return None
        candidates = _candidate_rows(result, ("results", "items", "data"))
        index = _nonnegative_int(payload.get("candidate_index"))
        if index is None or index >= len(candidates):
            return None
        candidate = candidates[index]
        if _candidate_digest(candidate) != str(payload.get("candidate_hash") or ""):
            return None
        return _resource_from_payload(candidate)


class NativeP115ResourceTransferGateway:
    def __init__(
        self,
        credentials: Any,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[str, str], P115CookieClientPort] | None = None,
    ) -> None:
        self._credentials = credentials
        self._settings = settings
        self._client_factory = client_factory or _default_p115_client

    def freeze_identity(self) -> Mapping[str, Any]:
        snapshot = self._credentials.load()
        return self._identity(snapshot)

    def _identity(self, snapshot: Any) -> dict[str, Any]:
        if not snapshot.has_cookie:
            raise RuntimeError("115 Cookie is required for cloud resource transfer")
        account_id = _required_text(snapshot.account_id, "115 account id")
        target_cid = _required_text(self._settings().p115_save_path_cid, "115 target cid")
        credential_hash = hashlib.sha256(f"{snapshot.app_type}\0{snapshot.cookie}".encode()).hexdigest()
        return {
            "account_id": account_id,
            "target_cid": target_cid,
            "app_type": str(snapshot.app_type or "web"),
            "credential_hash": credential_hash,
        }

    def transfer(
        self,
        resource: Mapping[str, Any],
        *,
        expected_identity: Mapping[str, Any],
    ) -> ProviderResponse[dict[str, Any]]:
        snapshot = self._credentials.load()
        current = self._identity(snapshot)
        for field in ("account_id", "target_cid", "app_type", "credential_hash"):
            if current.get(field) != expected_identity.get(field):
                raise RuntimeError(f"115 resource transfer {field} changed after checkpoint")
        client = self._client_factory(snapshot.cookie, str(snapshot.app_type or "web"))
        kind = str(resource.get("resource_kind") or "")
        if kind == "p115_share":
            share_code = _required_text(resource.get("share_code"), "115 share code")
            receive_code = str(resource.get("receive_code") or "").strip()
            response = client.request(
                "https://webapi.115.com/share/receive",
                method="POST",
                data={"share_code": share_code, "receive_code": receive_code, "cid": current["target_cid"]},
            )
        elif kind == "p115_offline":
            urls = _offline_urls(resource.get("urls"))
            if not urls:
                raise ValueError("115 offline resource has no supported URL")
            request: dict[str, Any] = {"wp_path_id": current["target_cid"]}
            request.update({f"url[{index}]": url for index, url in enumerate(urls)})
            response = client.offline_add_urls(request)
        else:
            return ProviderResponse(ProviderDisposition.REJECTED, 400, {"resource_kind": kind})
        payload = _response_object(response)
        outcome = _p115_outcome(payload)
        disposition = {
            "confirmed": ProviderDisposition.CONFIRMED,
            "rejected": ProviderDisposition.REJECTED,
        }.get(outcome, ProviderDisposition.INDETERMINATE)
        return ProviderResponse(disposition, 200, {"resource_kind": kind})


def _operation_payload(operation: SharingOperation) -> tuple[str, dict[str, Any]]:
    request = operation.request if isinstance(operation.request, Mapping) else {}
    action = _required_text(request.get("action"), "auto-subscribe transfer action")
    payload = request.get("payload")
    if not isinstance(payload, Mapping):
        raise ValueError("auto-subscribe transfer payload must be an object")
    return action, dict(payload)


def _provider_candidate(
    response: ProviderResponse[dict[str, Any]],
    context: Mapping[str, Any],
    *,
    keys: Sequence[str],
    hdhive: bool = False,
) -> Mapping[str, Any] | AutoSourceResult:
    if response.disposition in {ProviderDisposition.NOT_FOUND, ProviderDisposition.REJECTED}:
        return AutoSourceResult(AutoSourceDisposition.REJECTED, "hdhive" if hdhive else "shared_pool")
    if response.disposition is not ProviderDisposition.CONFIRMED or not isinstance(response.payload, Mapping):
        return AutoSourceResult(AutoSourceDisposition.RETRYABLE, "hdhive" if hdhive else "shared_pool")
    rows = _candidate_rows(response.payload, keys)
    selected = _select_candidate(rows, context, hdhive=hdhive)
    return selected[1] if selected is not None else AutoSourceResult.miss()


def _candidate_rows(payload: Mapping[str, Any], keys: Sequence[str]) -> list[dict[str, Any]]:
    queue: list[Any] = [payload]
    seen: set[int] = set()
    while queue:
        value = queue.pop(0)
        if id(value) in seen:
            continue
        seen.add(id(value))
        if isinstance(value, Sequence) and not isinstance(value, str | bytes):
            rows = [dict(item) for item in value if isinstance(item, Mapping)]
            if rows:
                return rows
        if isinstance(value, Mapping):
            for key in keys:
                nested = value.get(key)
                if isinstance(nested, Mapping | list | tuple):
                    queue.append(nested)
    return []


def _select_candidate(
    rows: Sequence[Mapping[str, Any]],
    context: Mapping[str, Any],
    *,
    hdhive: bool = False,
    telegram: bool = False,
) -> tuple[int, Mapping[str, Any]] | None:
    matches: list[tuple[tuple[Any, ...], int, Mapping[str, Any]]] = []
    expected_tmdb = _lookup_tmdb_id(context)
    expected_title = _normalized_title(context.get("title"))
    expected_year = str(context.get("year") or "")
    season = _positive_int(context.get("season_number"))
    for index, row in enumerate(rows):
        row_tmdb = str(row.get("tmdb_id") or row.get("tmdbid") or "").strip()
        if row_tmdb and row_tmdb != expected_tmdb:
            continue
        if not row_tmdb and telegram:
            title = _normalized_title(row.get("title") or row.get("name"))
            if expected_title and (not title or expected_title not in title and title not in expected_title):
                continue
            if expected_year and expected_year not in _candidate_text(row):
                continue
        season_level = _season_level(row, season)
        if season is not None and season_level < 0:
            continue
        if hdhive and not str(row.get("slug") or row.get("resource_id") or "").strip():
            continue
        if telegram and _resource_from_payload(row) is None:
            continue
        score = (
            -season_level,
            _safe_int(row.get("unlock_points") or row.get("_effective_points")),
            0 if str(row.get("pan_type") or "115").casefold() == "115" else 1,
            -_size_value(row.get("share_size") or row.get("size") or row.get("_size_gb")),
            str(row.get("source_id") or row.get("slug") or row.get("message_id") or index),
        )
        matches.append((score, index, row))
    if not matches:
        return None
    _score, index, row = min(matches, key=lambda item: item[0])
    return index, row


def _resource_from_payload(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    values: list[Mapping[str, Any]] = [payload]
    for key in ("data", "result", "resource", "unlock"):
        nested = payload.get(key)
        if isinstance(nested, Mapping):
            values.append(nested)
    for value in values:
        urls = _offline_urls(value.get("urls") or value.get("url") or value.get("magnet_url"))
        if urls:
            return {"resource_kind": "p115_offline", "urls": urls}
        combined = "\n".join(str(value.get(key) or "") for key in ("target_link", "full_url", "url", "text"))
        offline = _offline_urls(combined)
        if offline:
            return {"resource_kind": "p115_offline", "urls": offline}
        match = re.search(r"(?i)(?:115\.com|115cdn\.com|anxia\.com)/s/([a-z0-9]+)", combined)
        share_code = str(value.get("share_code") or (match.group(1) if match else "")).strip()
        if share_code:
            return {
                "resource_kind": "p115_share",
                "share_code": share_code,
                "receive_code": str(value.get("receive_code") or value.get("access_code") or "").strip(),
            }
    return None


def _read_error(action: str, disposition: ProviderDisposition) -> AutoSourceResult:
    if disposition in {ProviderDisposition.REJECTED, ProviderDisposition.NOT_FOUND}:
        return AutoSourceResult(AutoSourceDisposition.REJECTED, action)
    return AutoSourceResult(AutoSourceDisposition.RETRYABLE, action)


def _indeterminate(action: str, error_type: str) -> AutoSourceResult:
    return AutoSourceResult(
        AutoSourceDisposition.INDETERMINATE,
        action,
        {"action": action, "error_type": error_type, "indeterminate": True},
    )


def _lookup_tmdb_id(context: Mapping[str, Any]) -> str:
    item_type = str(context.get("item_type") or "")
    value = (
        context.get("tmdb_id")
        if item_type == "Movie"
        else context.get("parent_series_tmdb_id") or context.get("tmdb_id")
    )
    return _required_text(value, "tmdb_id")


def _center_item_type(context: Mapping[str, Any]) -> str:
    return "Movie" if str(context.get("item_type") or "") == "Movie" else "Season"


def _candidate_digest(candidate: Mapping[str, Any]) -> str:
    normalized = normalize_json_object(dict(candidate), field="auto-subscribe source candidate")
    encoded = canonical_json(normalized, field="auto-subscribe source candidate").encode()
    return hashlib.sha256(encoded).hexdigest()


def _candidate_text(value: Mapping[str, Any]) -> str:
    return "\n".join(str(value.get(key) or "") for key in ("title", "name", "text", "remark", "year"))


def _telegram_candidate_blocked(candidate: Mapping[str, Any], configuration: Mapping[str, Any]) -> bool:
    rules = configuration.get("block_keywords")
    if not isinstance(rules, Sequence) or isinstance(rules, str | bytes):
        return False
    text = str(candidate.get("text") or candidate.get("remark") or candidate.get("title") or "")
    username = str(candidate.get("source_username") or "").strip().casefold().lstrip("@")
    chat_id = str(candidate.get("source_chat_id") or candidate.get("chat_id") or "").strip()
    for value in rules:
        if isinstance(value, str):
            pattern = value.strip()
            channel = ""
        elif isinstance(value, Mapping):
            pattern = str(value.get("pattern") or "").strip()
            channel = str(value.get("channel") or "").strip().casefold()
        else:
            continue
        if not pattern or not _telegram_channel_matches(channel, username, chat_id):
            continue
        try:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        except re.error:
            return True
    return False


def _telegram_channel_matches(target: str, username: str, chat_id: str) -> bool:
    normalized = str(target or "").strip().casefold().lstrip("@")
    if not normalized:
        return True
    target_id = normalized[4:] if normalized.startswith("-100") else normalized
    current_id = chat_id[4:] if chat_id.startswith("-100") else chat_id
    return username == normalized or chat_id == normalized or current_id == target_id


def _normalized_title(value: object) -> str:
    return re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", str(value or "").casefold())


def _season_level(value: Mapping[str, Any], expected: int | None) -> int:
    if expected is None:
        return 0
    explicit: set[int] = set()
    direct = _positive_int(value.get("season_number") or value.get("season"))
    if direct is not None:
        explicit.add(direct)
    text = _candidate_text(value)
    for pattern in (r"\bS\s*(\d{1,2})\b", r"\bSeason\s*(\d{1,2})\b", r"第\s*(\d{1,2})\s*季"):
        explicit.update(int(match.group(1)) for match in re.finditer(pattern, text, re.IGNORECASE))
    if explicit:
        return 2 if expected in explicit else -1
    return 1 if expected == 1 else -1


def _offline_urls(value: object) -> list[str]:
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        candidates = [str(item or "").strip() for item in value]
    else:
        text = str(value or "")
        candidates = re.findall(r"(?i)(magnet:\?[^\s]+|ed2k://\|file\|.*?\|/)", text)
        if text.strip().startswith(("magnet:?", "ed2k://")):
            candidates.insert(0, text.strip())
    result: list[str] = []
    for item in candidates:
        if item.startswith(("magnet:?", "ed2k://")) and item not in result:
            result.append(item)
    return result


def _response_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    json_method = getattr(value, "json", None)
    if callable(json_method):
        decoded = json_method()
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def _p115_outcome(value: Mapping[str, Any]) -> str:
    state = value.get("state")
    if state is True or (not isinstance(state, bool) and str(state).strip().casefold() in {"1", "true"}):
        return "confirmed"
    message = str(value.get("error_msg") or value.get("error") or value.get("message") or "").casefold()
    deterministic = (
        "参数错误",
        "格式错误",
        "链接无效",
        "分享不存在",
        "分享已失效",
        "提取码错误",
        "invalid parameter",
        "invalid link",
        "not found",
        "unsupported",
    )
    return "rejected" if any(marker in message for marker in deterministic) else "indeterminate"


def _default_p115_client(cookie: str, app_type: str) -> P115CookieClientPort:
    from p115client import P115Client

    return cast(P115CookieClientPort, P115Client(cookie, app=app_type))


def _required_text(value: object, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"auto-subscribe source {field} must not be empty")
    return normalized


def _required_hash(value: object, field: str) -> str:
    normalized = str(value or "").strip().casefold()
    if not re.fullmatch(r"[0-9a-f]{64}", normalized):
        raise ValueError(f"auto-subscribe source {field} must be a SHA256 reference")
    return normalized


def _positive_int(value: object) -> int | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        parsed = int(cast(Any, value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _nonnegative_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(cast(Any, value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _safe_int(value: object) -> int:
    try:
        return int(cast(Any, value or 0))
    except (TypeError, ValueError):
        return 0


def _size_value(value: object) -> float:
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)
    match = re.search(r"(\d+(?:\.\d+)?)\s*(TB|GB|MB|KB|B)?", str(value or ""), re.IGNORECASE)
    if not match:
        return 0.0
    number = float(match.group(1))
    unit = str(match.group(2) or "GB").upper()
    return number * {"TB": 1024.0, "GB": 1.0, "MB": 1 / 1024, "KB": 1 / 1024**2, "B": 1 / 1024**3}[unit]


__all__ = [
    "AutoSubscriptionTransferOperationDelivery",
    "NativeP115ResourceTransferGateway",
    "P115ResourceTransferPort",
    "PersistentOrderedAutoSubscriptionSources",
    "TelegramSearchPort",
]
