from __future__ import annotations

import hashlib
import logging
import os
import re
import socket
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import unquote
from uuid import UUID, uuid4

import httpx
from sqlalchemy import text

from etk.compatibility.ingestion_monitor import PersistentIngestionBinding, PersistentIngestionIngress
from etk.domains.ingestion import (
    INGESTION_MONITOR_TOPIC,
    BindingStatus,
    FileObservation,
    IngestionMonitorStateError,
    MonitorClassification,
    MonitorEventType,
    MonitorItemProjection,
    MonitorItemStatus,
    MonitorPolicy,
    MonitorSource,
    PendingBindingProjection,
    PostgresIngestionMonitorLedger,
    SqlAlchemyIngestionRepository,
    normalize_media_path,
    valid_etk_strm_content,
)
from etk.domains.ingestion.effects import EMBY_PATHS_CHANGED_EFFECT, stage_ingestion_path_notification
from etk.domains.media.bridge_client import emby_authorization_revision
from etk.domains.media.editing_effects import BridgeMediaEffectDelivery, MediaInfoBridgeGateway
from etk.domains.media.effect_execution import PersistentMediaEffectExecutor
from etk.domains.media.effects import (
    MediaEffectConflictError,
    MediaEffectStatus,
    PostgresMediaEffectLedger,
)
from etk.domains.operations.jobs import JobSubmissionService, TaskBusyError
from etk.domains.p115.bridge_compatibility import p115_item_matches
from etk.domains.p115.media_info_adapters import (
    ControlledProcessOutput,
    ControlledProcessProfile,
    ControlledProcessRunner,
)
from etk.domains.p115.media_info_formatter import (
    P115MediaInfoPolicySnapshot,
    format_raw_ffprobe_mediainfo,
)
from etk.platform import Settings
from etk.platform.jobs import OutboxClaim, PostgresJobStore, PublishBatchResult
from etk.platform.jobs.types import UnitOfWorkFactory
from etk.platform.observability.redaction import redact_sensitive

from .celery_app import EXECUTE_INGESTION_MONITOR_TASK, create_celery_app
from .runtime_config import TaskRuntimeConfig

INGESTION_MONITOR_TOPICS = (INGESTION_MONITOR_TOPIC,)
INGESTION_CLAIM_LEASE_SECONDS = 420
INGESTION_MAX_ATTEMPTS = 3
INGESTION_RETRY_SECONDS = 10
_POLL_DELAYS = (0, 0.25, 0.5, 1, 2, 4, 8)
_P115_PLAY_PATH = re.compile(r"/api/p115/play/([^/?#]+)", re.IGNORECASE)
_P115_VIRTUAL_PLAY_PATH = re.compile(
    r"/api/p115/virtual-play/[^/?#]+/([A-Fa-f0-9]{40})(?:[/?#]|$)",
    re.IGNORECASE,
)
_FFPROBE_STDOUT_LIMIT = 16 * 1024 * 1024
_FFPROBE_STDERR_LIMIT = 256 * 1024
_FFPROBE_TIMEOUT_SECONDS = 300.0


class IngestionPreparationPort(Protocol):
    def prepare(self, item: MonitorItemProjection) -> bool: ...

    def poll(self, binding: PendingBindingProjection) -> Mapping[str, Any] | None: ...


class IngestionFingerprintPort(Protocol):
    def extract(self, path: str) -> tuple[str | None, str | None]: ...


class IngestionBindingDeliveryPort(Protocol):
    def deliver(
        self,
        binding: PendingBindingProjection,
        item: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...


class P115MediaInfoCacheReader(Protocol):
    def find_by_sha1(self, sha1: str, *, record_hit: bool = False) -> Any: ...


class P115IngestionCredentialStore(Protocol):
    def load(self) -> Any: ...


class P115IngestionFingerprintRepository(Protocol):
    def resolve_sha1_by_pick_code(self, pick_code: str, *, account_id: str) -> str | None: ...


class P115IngestionPolicyProvider(Protocol):
    def snapshot(self) -> P115MediaInfoPolicySnapshot: ...


class PersistentIngestionIngressGateway(PersistentIngestionIngress):
    """Translate compatibility Watchdog/scan events into durable monitor intents."""

    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        ledger: PostgresIngestionMonitorLedger,
        event_key_factory: Callable[[], str] | None = None,
    ) -> None:
        self._settings = settings
        self._ledger = ledger
        self._event_key_factory = event_key_factory or (lambda: uuid4().hex)

    def record_event(
        self,
        event_type: str,
        path: str,
        *,
        related_path: str | None = None,
        source: str = "watchdog",
        source_event_key: str | None = None,
    ) -> object:
        settings = self._settings()
        policy = MonitorPolicy.create(
            enabled=settings.monitor_enabled,
            extensions=settings.monitor_extensions,
            exclude_paths=settings.monitor_exclude_dirs,
            refresh_delay_seconds=settings.monitor_exclude_refresh_delay,
        )
        normalized_event = str(event_type or "").strip().casefold()
        normalized_source = str(source or "").strip().casefold()
        source_mapping = {
            "watchdog": MonitorSource.WATCHDOG,
            "scan": MonitorSource.SCAN,
            "active": MonitorSource.ACTIVE,
            "compatibility-active": MonitorSource.ACTIVE,
        }
        try:
            monitor_source = source_mapping[normalized_source]
        except KeyError as exc:
            raise ValueError(f"unsupported ingestion event source: {source!r}") from exc
        root = _matching_monitor_root(path, related_path, settings.monitor_paths)
        raw_event_key = str(source_event_key or self._event_key_factory()).strip()
        if not raw_event_key:
            raise ValueError("ingestion source event key must not be empty")
        digest = hashlib.sha256(raw_event_key.encode("utf-8")).hexdigest()
        producer_key = f"{monitor_source.value}:{normalized_event}:{digest}"

        if normalized_event in {"created", "upsert"}:
            intents = policy.created(
                path,
                producer_key=producer_key,
                monitor_root=root,
                source=monitor_source,
            )
        elif normalized_event == "modified":
            intents = policy.modified(path)
        elif normalized_event == "deleted":
            intents = policy.deleted(
                path,
                producer_key=producer_key,
                monitor_root=root,
                source=monitor_source,
            )
        elif normalized_event == "moved":
            if not str(related_path or "").strip():
                raise ValueError("moved ingestion event requires a source path")
            intents = policy.moved(
                related_path,
                path,
                producer_key=producer_key,
                monitor_root=root,
                source=monitor_source,
            )
        else:
            raise ValueError(f"unsupported ingestion event type: {event_type!r}")
        return self._ledger.record(intents) if intents else ()


logger = logging.getLogger(__name__)


class IngestionBindingRejected(RuntimeError):
    pass


class IngestionBindingIndeterminate(RuntimeError):
    pass


class IngestionBindingInProgress(RuntimeError):
    pass


class NativeIngestionBindingDelivery(IngestionBindingDeliveryPort):
    """Confirm MediaInfo once, then submit the bounded native reprocess operation."""

    def __init__(
        self,
        *,
        jobs: Callable[[], JobSubmissionService],
        uow_factory: UnitOfWorkFactory,
        bridge: MediaInfoBridgeGateway,
        media_info: P115MediaInfoCacheReader,
        media_effects: PersistentMediaEffectExecutor | None = None,
        lease_seconds: float | Callable[[], float] = 90,
    ) -> None:
        self._jobs = jobs
        self._bridge = bridge
        self._media_info = media_info
        self._media_effect_ledger = PostgresMediaEffectLedger(uow_factory)
        self._media_effects = media_effects or PersistentMediaEffectExecutor(
            self._media_effect_ledger,
            BridgeMediaEffectDelivery(bridge),
            worker_id="ingestion-binding:media-effect",
            lease_seconds=lease_seconds,
        )

    def deliver(
        self,
        binding: PendingBindingProjection,
        item: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        idempotency_scope = "ingestion-binding"
        idempotency_key = str(binding.id)
        effect_key = f"p6:ingestion-binding:emby.mediainfo.apply:{binding.id}"
        jobs = self._jobs()
        submitted_job = jobs.get_idempotent(scope=idempotency_scope, key=idempotency_key)
        effect = self._media_effect_ledger.read(effect_key)
        if submitted_job is not None:
            if effect is None or effect.status is not MediaEffectStatus.CONFIRMED:
                raise IngestionBindingIndeterminate("ingestion Job exists without a confirmed MediaInfo effect")
            staged_item_id = str(effect.payload.get("item_id") or "").strip()
            if not staged_item_id:
                raise IngestionBindingIndeterminate("confirmed MediaInfo effect has no Emby item identity")
            return {
                "mediainfo_effect_id": str(effect.id),
                "job_id": str(submitted_job.job.id),
                "job_reused": True,
                "item_id": staged_item_id,
            }

        if effect is None:
            item_id = str(item.get("Id") or "").strip()
            item_type = str(item.get("Type") or "").strip()
            series_id = str(item.get("SeriesId") or "").strip()
            if not item_id or item_type not in {"Movie", "Episode"}:
                raise IngestionBindingRejected("Emby binding item identity is invalid")
            if item_type == "Episode" and not series_id:
                raise IngestionBindingRejected("episode binding has no SeriesId")

            if binding.media_info is not None:
                mediainfo: object = binding.media_info
            elif binding.sha1:
                record = self._media_info.find_by_sha1(binding.sha1, record_hit=True)
                mediainfo = getattr(record, "formatted", None)
                if not mediainfo:
                    raise IngestionBindingRejected("pending binding MediaInfo cache was not found")
            else:
                raise IngestionBindingRejected("pending binding has no media information identity")

            payload = {
                "item_id": item_id,
                "item_type": item_type,
                "item_name": str(item.get("Name") or item_id),
                "series_id": series_id,
                "sha1": binding.sha1 or "",
                "binding_id": str(binding.id),
                "emby_authorization_revision": self._bridge.authorization_revision(),
                "mediainfo": mediainfo,
                "drop_conflicting_external_streams": True,
            }
            try:
                effect = self._media_effect_ledger.reserve(
                    effect_key=effect_key,
                    effect_type="emby.mediainfo.apply",
                    aggregate_type="ingestion_binding",
                    aggregate_id=str(binding.id),
                    payload=payload,
                ).effect
            except MediaEffectConflictError:
                effect = self._media_effect_ledger.read(effect_key)
                if effect is None:
                    raise

        delivered = self._media_effects.execute(effect.id)
        if delivered.status is MediaEffectStatus.REJECTED:
            raise IngestionBindingRejected("Emby media information injection was rejected")
        if delivered.status is MediaEffectStatus.INDETERMINATE:
            raise IngestionBindingIndeterminate("Emby media information injection outcome is unknown")
        if delivered.status is not MediaEffectStatus.CONFIRMED:
            raise IngestionBindingInProgress("Emby media information injection is still in progress")

        staged_item_id = str(delivered.payload.get("item_id") or "").strip()
        staged_item_type = str(delivered.payload.get("item_type") or "").strip()
        staged_series_id = str(delivered.payload.get("series_id") or "").strip()
        staged_item_name = str(delivered.payload.get("item_name") or staged_item_id).strip()
        if not staged_item_id or staged_item_type not in {"Movie", "Episode"}:
            raise IngestionBindingIndeterminate("durable MediaInfo effect has no resumable Emby item identity")

        if staged_item_type == "Episode":
            if not staged_series_id:
                raise IngestionBindingIndeterminate("durable Episode MediaInfo effect has no SeriesId")
        submitted = jobs.submit_native_operation(
            "media.reprocess-item",
            {
                "item_id": staged_item_id,
                "restore_media_info": False,
                "notify_library": True,
            },
            display_name=f"主动入库: {staged_item_name}",
            idempotency_scope=idempotency_scope,
            idempotency_key=idempotency_key,
        )
        logger.info(
            "主动入库已提交自动刮削：媒体=%s，Emby ID=%s，任务=%s",
            staged_item_name,
            staged_item_id,
            submitted.projection.job.id,
        )
        return {
            "mediainfo_effect_id": str(effect.id),
            "job_id": str(submitted.projection.job.id),
            "job_reused": submitted.reused,
            "item_id": staged_item_id,
        }


@dataclass(frozen=True, slots=True)
class _BindingContext:
    item: MonitorItemProjection
    fingerprint: Callable[[str], tuple[str | None, str | None]]


class PersistentPendingBindingGateway(PersistentIngestionBinding):
    """Single-use pending binding adapter shared by Worker polling and Bridge callbacks."""

    def __init__(
        self,
        *,
        ledger: PostgresIngestionMonitorLedger,
        delivery: IngestionBindingDeliveryPort,
        claimant_id: str | None = None,
        lease_seconds: float = 90,
    ) -> None:
        self._ledger = ledger
        self._delivery = delivery
        self._claimant_id = str(claimant_id or f"ingestion-binding:{socket.gethostname()}").strip()
        self._lease_seconds = float(lease_seconds)
        if not self._claimant_id or self._lease_seconds <= 0:
            raise ValueError("pending binding claimant and lease must be valid")
        self._context: ContextVar[_BindingContext | None] = ContextVar("etk_ingestion_binding_context", default=None)
        self._reserved: ContextVar[PendingBindingProjection | None] = ContextVar(
            "etk_ingestion_reserved_binding",
            default=None,
        )

    @contextmanager
    def monitor_item(
        self,
        item: MonitorItemProjection,
        *,
        fingerprint: Callable[[str], tuple[str | None, str | None]],
    ):
        context_token: Token[_BindingContext | None] = self._context.set(_BindingContext(item, fingerprint))
        reserved_token: Token[PendingBindingProjection | None] = self._reserved.set(None)
        try:
            yield self
        finally:
            self._reserved.reset(reserved_token)
            self._context.reset(context_token)

    @property
    def reserved_binding(self) -> PendingBindingProjection | None:
        return self._reserved.get()

    def enqueue_binding(
        self,
        path: str,
        *,
        mediainfo: Mapping[str, Any] | None = None,
    ) -> PendingBindingProjection:
        context = self._context.get()
        if context is None:
            raise RuntimeError("persistent binding enqueue requires a claimed monitor item")
        existing = self._ledger.binding_for_monitor_item(context.item.id)
        if existing is not None:
            self._reserved.set(existing)
            return existing
        pick_code, sha1 = context.fingerprint(path)
        reservation = self._ledger.reserve_binding(
            binding_key=f"monitor:{context.item.id}",
            monitor_item_id=context.item.id,
            path=path,
            sha1=sha1,
            pick_code=pick_code,
            media_info=mediainfo,
        )
        self._reserved.set(reservation.binding)
        return reservation.binding

    def replay_pending_binding(
        self,
        *,
        emby_item_id: str,
        sha1: str,
        expected_pick_code: str = "",
    ) -> Mapping[str, Any] | None:
        binding = self._ledger.lookup_for_item_ready(
            sha1=sha1,
            pick_code=expected_pick_code or None,
            emby_item_id=str(emby_item_id or "").strip(),
        )
        if (
            binding is not None
            and binding.status is BindingStatus.CONFIRMED
            and binding.emby_item_id == str(emby_item_id or "").strip()
        ):
            return {"ok": True, "skipped": True, "reason": "already_completed"}
        return None

    def accept_pending_binding(
        self,
        item: Mapping[str, Any],
        *,
        sha1: str,
        expected_pick_code: str = "",
    ) -> Mapping[str, Any] | None:
        item_id = str(item.get("Id") or "").strip()
        path = _item_path(item)
        binding = self._ledger.lookup_for_item_ready(
            path=path or None,
            sha1=sha1 or None,
            pick_code=expected_pick_code or None,
            emby_item_id=item_id,
        )
        if binding is None:
            return None
        if binding.status is BindingStatus.CONFIRMED and binding.emby_item_id != item_id:
            return None
        terminal = _binding_terminal_outcome(binding, item_id)
        if terminal is not None:
            return terminal
        if binding.status is BindingStatus.CLAIMED:
            return {"ok": False, "skipped": False, "retryable": True, "reason": "binding_in_progress"}
        claimed = self._ledger.claim_binding_by_id(
            binding.id,
            worker_id=self._claimant_id,
            lease_seconds=self._lease_seconds,
        )
        if claimed is None:
            current = self._ledger.read_binding(binding.id)
            if current is None:
                return None
            return _binding_terminal_outcome(current, item_id) or {
                "ok": False,
                "skipped": False,
                "retryable": True,
                "reason": "binding_in_progress",
            }
        assert claimed.claim_token is not None
        try:
            delivery_result = self._delivery.deliver(claimed, item)
        except IngestionBindingRejected as exc:
            error = _safe_error("binding_rejected", exc)
            self._ledger.mark_binding_rejected(claimed.id, claimed.claim_token, error=error)
            return {"ok": False, "skipped": False, "retryable": False, "reason": "binding_rejected"}
        except IngestionBindingIndeterminate as exc:
            error = _safe_error("binding_delivery_indeterminate", exc)
            self._ledger.mark_binding_indeterminate(claimed.id, claimed.claim_token, error=error)
            return {"ok": False, "skipped": False, "retryable": False, "reason": "binding_indeterminate"}
        except (IngestionBindingInProgress, TaskBusyError) as exc:
            # 串行队列忙是等待条件，不能耗尽业务失败次数后丢弃同批其他分集。
            error = _safe_error("binding_delivery_in_progress", exc)
            self._ledger.mark_binding_retryable(
                claimed.id,
                claimed.claim_token,
                error=error,
                retry_after_seconds=INGESTION_RETRY_SECONDS,
            )
            return {"ok": False, "skipped": False, "retryable": True, "reason": "binding_in_progress"}
        except Exception as exc:
            error = _safe_error("binding_delivery_failed", exc)
            if claimed.attempts >= INGESTION_MAX_ATTEMPTS:
                self._ledger.mark_binding_rejected(claimed.id, claimed.claim_token, error=error)
                return {"ok": False, "skipped": False, "retryable": False, "reason": "binding_retry_exhausted"}
            self._ledger.mark_binding_retryable(
                claimed.id,
                claimed.claim_token,
                error=error,
                retry_after_seconds=INGESTION_RETRY_SECONDS,
            )
            return {"ok": False, "skipped": False, "retryable": True, "reason": "binding_delivery_failed"}

        confirmed_item_id = str(delivery_result.get("item_id") or item_id).strip()
        if not confirmed_item_id:
            raise IngestionMonitorStateError("binding delivery returned no resumable Emby item identity")
        result = {"ok": True, "accepted": True, "item_id": confirmed_item_id}
        self._ledger.mark_binding_confirmed(
            claimed.id,
            claimed.claim_token,
            emby_item_id=confirmed_item_id,
            result=result,
        )
        return result

    def reject_unresolved(self, binding: PendingBindingProjection, *, reason: str) -> bool:
        current = self._ledger.read_binding(binding.id)
        if current is None or current.status.terminal or current.status is BindingStatus.CLAIMED:
            return False
        claimed = self._ledger.claim_binding_by_id(
            current.id,
            worker_id=self._claimant_id,
            lease_seconds=self._lease_seconds,
        )
        if claimed is None or claimed.claim_token is None:
            return False
        self._ledger.mark_binding_rejected(
            claimed.id,
            claimed.claim_token,
            error={"type": str(reason or "binding_unresolved")},
        )
        return True


class NativeIngestionFingerprintGateway(IngestionFingerprintPort):
    """Resolve the account-bound 115 identity without loading a Processor."""

    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        credentials: P115IngestionCredentialStore,
        repository: P115IngestionFingerprintRepository,
    ) -> None:
        self._settings = settings
        self._credentials = credentials
        self._repository = repository

    def extract(self, path: str) -> tuple[str | None, str | None]:
        if not self._settings().monitor_sha1_pc_search:
            return None, None
        value = str(path or "").strip()
        if not value:
            return None, None
        sample = value
        if not value.startswith(("http://", "https://")):
            if not value.casefold().endswith(".strm") or not os.path.isfile(value):
                return None, None
            try:
                with open(value, encoding="utf-8", errors="ignore") as stream:
                    sample = stream.read(2048).strip()
            except OSError:
                return None, None

        play = _P115_PLAY_PATH.search(sample)
        virtual = _P115_VIRTUAL_PLAY_PATH.search(sample)
        pick_code = unquote(play.group(1)).strip() if play else ""
        sha1 = virtual.group(1).upper() if virtual else ""
        if pick_code and not sha1:
            account_id = str(getattr(self._credentials.load(), "account_id", "") or "").strip()
            if account_id:
                sha1 = (
                    str(
                        self._repository.resolve_sha1_by_pick_code(
                            pick_code,
                            account_id=account_id,
                        )
                        or ""
                    )
                    .strip()
                    .upper()
                )
        return pick_code or None, sha1 or None


class NativeIngestionPreparationGateway(IngestionPreparationPort):
    """Prepare a stable file and perform bounded Emby path readback."""

    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        pending_bindings: PersistentPendingBindingGateway,
        processes: ControlledProcessRunner,
        policies: P115IngestionPolicyProvider,
        client_factory: Callable[[], httpx.Client] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._settings = settings
        self._pending_bindings = pending_bindings
        self._processes = processes
        self._policies = policies
        self._client_factory = client_factory or self._default_client
        self._sleep = sleep

    def prepare(self, item: MonitorItemProjection) -> bool:
        if item.classification is MonitorClassification.STRM:
            self._pending_bindings.enqueue_binding(item.path)
            return True
        if item.classification is MonitorClassification.PHYSICAL:
            logger.info("新文件已稳定，正在获取媒体流信息：%s", item.path)
            outcome = self._processes.run(
                ControlledProcessProfile.FFPROBE,
                (
                    "-hide_banner",
                    "-v",
                    "error",
                    "-print_format",
                    "json",
                    "-show_format",
                    "-show_streams",
                    "-show_chapters",
                    item.path,
                ),
                max_stdin_bytes=0,
                max_stdout_bytes=_FFPROBE_STDOUT_LIMIT,
                max_stderr_bytes=_FFPROBE_STDERR_LIMIT,
                timeout_seconds=_FFPROBE_TIMEOUT_SECONDS,
                output=ControlledProcessOutput.JSON,
            )
            raw = outcome.parsed_output
            if not outcome.succeeded or not isinstance(raw, Mapping):
                logger.warning("媒体流探测失败：%s", item.path)
                return False
            formatted = format_raw_ffprobe_mediainfo(
                raw,
                file_name=os.path.basename(item.path),
                file_size=os.path.getsize(item.path),
                policy=self._policies.snapshot(),
            )
            if not formatted:
                return False
            self._pending_bindings.enqueue_binding(item.path, mediainfo=dict(formatted[0]))
            logger.info("媒体流信息已准备，等待 Emby 路径绑定：%s", item.path)
            return True
        raise ValueError(f"unsupported ingestion preparation classification: {item.classification.value}")

    def poll(self, binding: PendingBindingProjection) -> Mapping[str, Any] | None:
        settings = self._settings()
        base_url = str(settings.emby_server_url or "").strip().rstrip("/")
        token = settings.emby_api_key.get_secret_value().strip()
        user_id = str(settings.emby_user_id or "").strip()
        if not base_url or not token:
            raise RuntimeError("ingestion polling requires configured Emby authorization")
        expected_path = normalize_media_path(binding.path)
        had_response = False
        last_error: Exception | None = None
        with self._client_factory() as client:
            for delay in _POLL_DELAYS:
                if delay:
                    self._sleep(delay)
                params = {
                    "Path": binding.path,
                    "Recursive": "true",
                    "Fields": (
                        "Path,MediaSources,ProviderIds,SeriesId,SeasonId,ParentId,"
                        "IndexNumber,ParentIndexNumber,Type,Name"
                    ),
                    "EnableTotalRecordCount": "false",
                }
                if user_id:
                    params["UserId"] = user_id
                try:
                    response = client.get(
                        f"{base_url}/Items",
                        params=params,
                        headers={"Accept": "application/json", "X-Emby-Token": token},
                    )
                    response.raise_for_status()
                    payload: Any = response.json()
                except (httpx.HTTPError, TypeError, ValueError) as exc:
                    last_error = exc
                    continue
                if not isinstance(payload, Mapping) or not isinstance(payload.get("Items"), list):
                    raise RuntimeError("Emby ingestion path query returned an invalid response")
                had_response = True
                for raw_item in payload["Items"]:
                    if not isinstance(raw_item, Mapping):
                        continue
                    item = dict(raw_item)
                    if (
                        normalize_media_path(item.get("Path")) != expected_path
                        or not str(item.get("Id") or "").strip()
                        or str(item.get("Type") or "").strip() not in {"Movie", "Episode"}
                    ):
                        continue
                    if binding.path.casefold().endswith(".strm") and not p115_item_matches(
                        item,
                        binding.sha1 or "",
                        binding.pick_code or "",
                    ):
                        continue
                    return item
        if not had_response and last_error is not None:
            raise RuntimeError("Emby ingestion path query failed") from last_error
        return None

    def _default_client(self) -> httpx.Client:
        return httpx.Client(timeout=max(1.0, float(self._settings().emby_api_timeout)), trust_env=False)


class CeleryIngestionMonitorPublisher:
    def __init__(self, config: TaskRuntimeConfig, *, celery_app: Any | None = None) -> None:
        self._config = config
        self._app = celery_app or create_celery_app(config)

    def publish(self, claim: OutboxClaim) -> None:
        event = claim.event
        if event.topic != INGESTION_MONITOR_TOPIC or event.aggregate_type != "ingestion_monitor_item":
            raise ValueError("ingestion publisher received an unsupported Outbox event")
        if event.payload_version != 1:
            raise ValueError("ingestion monitor Outbox payload version is unsupported")
        payload = event.payload
        item_id = _uuid(payload.get("item_id"), "item_id")
        if event.aggregate_id != str(item_id):
            raise ValueError("ingestion monitor Outbox aggregate does not match item_id")
        if int(payload.get("event_sequence") or 0) <= 0:
            raise ValueError("ingestion monitor Outbox event_sequence is invalid")
        if not str(payload.get("producer_key") or "").strip():
            raise ValueError("ingestion monitor Outbox producer_key is missing")
        request_hash = str(payload.get("request_hash") or "").strip()
        if len(request_hash) != 64:
            raise ValueError("ingestion monitor Outbox request_hash is invalid")
        try:
            bytes.fromhex(request_hash)
        except ValueError as exc:
            raise ValueError("ingestion monitor Outbox request_hash is invalid") from exc
        self._app.send_task(
            EXECUTE_INGESTION_MONITOR_TASK,
            kwargs={"item_id": str(item_id)},
            task_id=str(item_id),
            queue=self._config.queue_name,
            routing_key=self._config.queue_name,
            serializer="json",
        )


@dataclass(frozen=True, slots=True)
class IngestionCoordinatorResult:
    suspended: int
    resumed: int
    expired_claims: int
    expired_bindings: int
    sampled: int
    rejected_dead: int
    publish: PublishBatchResult


class IngestionMonitorCoordinator:
    def __init__(
        self,
        *,
        store: PostgresJobStore,
        ledger: PostgresIngestionMonitorLedger,
        publisher: CeleryIngestionMonitorPublisher,
        config: TaskRuntimeConfig,
        publisher_id: str,
    ) -> None:
        self._store = store
        self._ledger = ledger
        self._publisher = publisher
        self._config = config
        self._publisher_id = str(publisher_id or "").strip()
        self._enabled: bool | None = None
        if not self._publisher_id:
            raise ValueError("ingestion monitor publisher id must not be empty")

    def synchronize_enabled(self, enabled: bool) -> tuple[int, int]:
        normalized = bool(enabled)
        if self._enabled is normalized and normalized:
            return 0, 0
        if normalized:
            result = (0, self._ledger.resume_suspended())
        else:
            result = (self._ledger.suspend_active(), 0)
        self._enabled = normalized
        return result

    def reset_enabled_state(self) -> None:
        self._enabled = None

    def tick(self, settings: Settings) -> IngestionCoordinatorResult:
        limit = self._config.outbox_batch_size
        expired_claims = self._ledger.expire_stale_claims(limit=limit, retry_delay_seconds=1)
        expired_bindings = self._ledger.expire_stale_bindings(limit=limit, retry_delay_seconds=1)
        suspended, resumed = self.synchronize_enabled(settings.monitor_enabled)
        if not settings.monitor_enabled:
            return IngestionCoordinatorResult(
                suspended=suspended,
                resumed=resumed,
                expired_claims=expired_claims,
                expired_bindings=expired_bindings,
                sampled=0,
                rejected_dead=0,
                publish=PublishBatchResult(claimed=0, published=0, failed=0, dead=0),
            )
        sampled = 0
        for item in self._ledger.due_for_stability(limit=limit):
            try:
                self._ledger.record_stability_sample(item.id, _observe(item))
            except IngestionMonitorStateError:
                continue
            sampled += 1
        self._store.requeue_stale_published(
            limit=limit,
            processed_timeout_seconds=max(self._config.lease_seconds * 2, 60),
            topics=INGESTION_MONITOR_TOPICS,
        )
        rejected_dead = self._ledger.reject_dead_dispatches(limit=limit)
        publish = self._store.publish_outbox_batch(
            self._publisher,
            publisher_id=self._publisher_id,
            limit=limit,
            claim_timeout_seconds=self._config.outbox_claim_timeout_seconds,
            failure_backoff_seconds=2,
            topics=INGESTION_MONITOR_TOPICS,
        )
        if publish.dead:
            rejected_dead += self._ledger.reject_dead_dispatches(limit=limit)
        return IngestionCoordinatorResult(
            suspended=suspended,
            resumed=resumed,
            expired_claims=expired_claims,
            expired_bindings=expired_bindings,
            sampled=sampled,
            rejected_dead=rejected_dead,
            publish=publish,
        )


class IngestionMonitorExecutor:
    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        ledger: PostgresIngestionMonitorLedger,
        settings: Callable[[], Settings],
        preparation: IngestionPreparationPort,
        pending_bindings: PersistentPendingBindingGateway,
        media_effects: Any,
        fingerprints: IngestionFingerprintPort,
        worker_id: str,
        lease_seconds: float = INGESTION_CLAIM_LEASE_SECONDS,
    ) -> None:
        self._uow_factory = uow_factory
        self._ledger = ledger
        self._settings = settings
        self._preparation = preparation
        self._pending_bindings = pending_bindings
        self._media_effects = media_effects
        self._fingerprints = fingerprints
        self._worker_id = str(worker_id or "").strip()
        self._lease_seconds = float(lease_seconds)
        if not self._worker_id or self._lease_seconds <= 0:
            raise ValueError("ingestion monitor Worker identity and lease must be valid")

    def execute(self, item_id: UUID) -> MonitorItemProjection | None:
        claimed = self._ledger.claim_by_id(
            item_id,
            worker_id=self._worker_id,
            lease_seconds=self._lease_seconds,
        )
        if claimed is None:
            return self._ledger.read(item_id)
        assert claimed.claim_token is not None
        if claimed.classification in {MonitorClassification.DELETED, MonitorClassification.REFRESH_ONLY}:
            return self._complete_with_notification(claimed, result={"path_notified": True})

        if not os.path.isfile(claimed.path):
            return self._reject(claimed, "stable_file_missing")
        if claimed.classification is MonitorClassification.STRM:
            try:
                with open(claimed.path, "rb") as stream:
                    content = stream.read(2048)
            except OSError as exc:
                return self._retry_or_reject(claimed, "strm_read_failed", exc)
            if not valid_etk_strm_content(content):
                return self._reject(claimed, "invalid_etk_strm")

        try:
            with self._pending_bindings.monitor_item(claimed, fingerprint=self._fingerprints.extract) as binding_scope:
                prepared = self._preparation.prepare(claimed)
                binding = binding_scope.reserved_binding
        except Exception as exc:
            return self._retry_or_reject(claimed, "media_preparation_failed", exc)
        if not prepared or binding is None:
            return self._retry_or_reject(claimed, "media_preparation_incomplete")

        effect_id = self._reserve_notification(claimed)
        delivered = self._media_effects.execute(effect_id)
        if delivered.status is MediaEffectStatus.REJECTED:
            return self._terminalize_notification_failure(
                claimed,
                binding,
                binding_status=BindingStatus.REJECTED,
                error_type="path_notification_rejected",
                details=delivered.error,
            )
        if delivered.status is MediaEffectStatus.INDETERMINATE:
            return self._terminalize_notification_failure(
                claimed,
                binding,
                binding_status=BindingStatus.INDETERMINATE,
                error_type="path_notification_indeterminate",
                details={"effect_id": str(delivered.id), **dict(delivered.error or {})},
            )
        if delivered.status is not MediaEffectStatus.CONFIRMED:
            return self._retry(claimed, "path_notification_in_progress")

        try:
            emby_item = self._preparation.poll(binding)
        except Exception as exc:
            reconciled = self._reconcile_binding_state(claimed, binding, effect_id)
            if reconciled is not None:
                return reconciled
            if claimed.attempts >= INGESTION_MAX_ATTEMPTS:
                if not self._pending_bindings.reject_unresolved(binding, reason="emby_item_poll_failed"):
                    reconciled = self._reconcile_binding_state(claimed, binding, effect_id)
                    if reconciled is not None:
                        return reconciled
                    return self._retry(claimed, "binding_state_changed")
            return self._retry_or_reject(claimed, "emby_item_poll_failed", exc)
        if emby_item is None:
            reconciled = self._reconcile_binding_state(claimed, binding, effect_id)
            if reconciled is not None:
                return reconciled
            if claimed.attempts >= INGESTION_MAX_ATTEMPTS:
                if not self._pending_bindings.reject_unresolved(binding, reason="emby_item_not_found"):
                    reconciled = self._reconcile_binding_state(claimed, binding, effect_id)
                    if reconciled is not None:
                        return reconciled
                    return self._retry(claimed, "binding_state_changed")
            return self._retry_or_reject(claimed, "emby_item_not_found")
        outcome = self._pending_bindings.accept_pending_binding(
            emby_item,
            sha1=binding.sha1 or "",
            expected_pick_code=binding.pick_code or "",
        )
        if outcome is None:
            return self._retry_or_reject(claimed, "pending_binding_not_found")
        if outcome.get("ok") is not True:
            reason = str(outcome.get("reason") or "binding_rejected")
            if outcome.get("retryable"):
                return self._retry(claimed, reason)
            if reason == "binding_indeterminate":
                return self._indeterminate(claimed, reason)
            return self._reject(claimed, reason)
        confirmed_binding = self._ledger.read_binding(binding.id)
        return self._complete_with_notification(
            claimed,
            result={
                "binding_id": str(binding.id),
                "emby_item_id": str(
                    outcome.get("item_id")
                    or (confirmed_binding.emby_item_id if confirmed_binding is not None else "")
                    or ""
                ),
                "notification_effect_id": str(effect_id),
            },
            confirmed_effect_id=effect_id,
        )

    def _reserve_notification(self, claimed: MonitorItemProjection) -> UUID:
        with self._uow_factory() as uow:
            current = _owned_claim(
                SqlAlchemyIngestionRepository(uow.session).item(claimed.id, for_update=True),
                claimed,
            )
            effect_id = uow.session.scalar(
                text(
                    """
                    SELECT id
                    FROM media_effects
                    WHERE aggregate_type = 'ingestion_monitor'
                      AND aggregate_id = :aggregate_id
                      AND effect_type = :effect_type
                    ORDER BY created_at, id
                    LIMIT 1
                    FOR UPDATE
                    """
                ),
                {"aggregate_id": str(current.id), "effect_type": EMBY_PATHS_CHANGED_EFFECT},
            )
            if effect_id is None:
                effect_id = stage_ingestion_path_notification(
                    uow.session,
                    current,
                    authorization_revision=emby_authorization_revision(self._settings()),
                ).effect.id
            uow.commit()
            return UUID(str(effect_id))

    def _reconcile_binding_state(
        self,
        claimed: MonitorItemProjection,
        binding: PendingBindingProjection,
        effect_id: UUID,
    ) -> MonitorItemProjection | None:
        current = self._ledger.read_binding(binding.id)
        if current is None or current.status in {BindingStatus.PENDING, BindingStatus.RETRYABLE}:
            return None
        if current.status is BindingStatus.CONFIRMED:
            return self._complete_with_notification(
                claimed,
                result={
                    "binding_id": str(current.id),
                    "emby_item_id": str(current.emby_item_id or ""),
                    "notification_effect_id": str(effect_id),
                },
                confirmed_effect_id=effect_id,
            )
        if current.status is BindingStatus.INDETERMINATE:
            return self._indeterminate(claimed, "binding_indeterminate", current.error)
        if current.status is BindingStatus.REJECTED:
            return self._reject(claimed, "binding_rejected", current.error)
        if current.status is BindingStatus.CLAIMED:
            return self._retry(claimed, "binding_in_progress")
        raise IngestionMonitorStateError(f"unsupported pending binding status: {current.status.value}")

    def _complete_with_notification(
        self,
        claimed: MonitorItemProjection,
        *,
        result: Mapping[str, Any],
        confirmed_effect_id: UUID | None = None,
    ) -> MonitorItemProjection:
        with self._uow_factory() as uow:
            repository = SqlAlchemyIngestionRepository(uow.session)
            current = _owned_claim(repository.item(claimed.id, for_update=True), claimed)
            if confirmed_effect_id is None:
                effect_id = stage_ingestion_path_notification(
                    uow.session,
                    current,
                    authorization_revision=emby_authorization_revision(self._settings()),
                ).effect.id
            else:
                effect_status = uow.session.scalar(
                    text(
                        """
                        SELECT status
                        FROM media_effects
                        WHERE id = :effect_id
                          AND aggregate_type = 'ingestion_monitor'
                          AND aggregate_id = :aggregate_id
                          AND effect_type = :effect_type
                        FOR UPDATE
                        """
                    ),
                    {
                        "effect_id": confirmed_effect_id,
                        "aggregate_id": str(current.id),
                        "effect_type": EMBY_PATHS_CHANGED_EFFECT,
                    },
                )
                if effect_status != MediaEffectStatus.CONFIRMED.value:
                    raise IngestionMonitorStateError("ingestion notification effect is not confirmed")
                effect_id = confirmed_effect_id
            assert current.claim_token is not None
            transition = repository.mark_confirmed(
                current.id,
                current.claim_token,
                result={**dict(result), "notification_effect_id": str(effect_id)},
            )
            uow.commit()
            return transition.item

    def _terminalize_notification_failure(
        self,
        claimed: MonitorItemProjection,
        binding: PendingBindingProjection,
        *,
        binding_status: BindingStatus,
        error_type: str,
        details: Mapping[str, Any] | None,
    ) -> MonitorItemProjection:
        error: dict[str, Any] = {"type": error_type}
        if details:
            safe = redact_sensitive(dict(details))
            if isinstance(safe, Mapping):
                error["details"] = dict(safe)
        with self._uow_factory() as uow:
            repository = SqlAlchemyIngestionRepository(uow.session)
            current = _owned_claim(repository.item(claimed.id, for_update=True), claimed)
            assert current.claim_token is not None
            repository.terminalize_binding_for_monitor_claim(
                binding.id,
                monitor_item_id=current.id,
                monitor_claim_token=current.claim_token,
                status=binding_status,
                error=error,
            )
            transition = (
                repository.mark_indeterminate(current.id, current.claim_token, error=error)
                if binding_status is BindingStatus.INDETERMINATE
                else repository.mark_rejected(current.id, current.claim_token, error=error)
            )
            uow.commit()
            return transition.item

    def _retry_or_reject(
        self,
        claimed: MonitorItemProjection,
        error_type: str,
        exc: BaseException | None = None,
    ) -> MonitorItemProjection:
        assert claimed.claim_token is not None
        error = _safe_error(error_type, exc) if exc is not None else {"type": error_type}
        if claimed.attempts >= INGESTION_MAX_ATTEMPTS:
            logger.error("自动入库重试已耗尽：%s；原因=%s", claimed.path, error_type)
            return self._ledger.mark_rejected(claimed.id, claimed.claim_token, error=error).item
        return self._retry(claimed, error_type, error=error)

    def _retry(
        self,
        claimed: MonitorItemProjection,
        error_type: str,
        *,
        error: Mapping[str, Any] | None = None,
    ) -> MonitorItemProjection:
        assert claimed.claim_token is not None
        logger.warning("自动入库等待重试：%s；原因=%s，已尝试=%s", claimed.path, error_type, claimed.attempts)
        return self._ledger.mark_retryable(
            claimed.id,
            claimed.claim_token,
            error=dict(error) if error is not None else {"type": error_type},
            retry_after_seconds=INGESTION_RETRY_SECONDS,
        ).item

    def _indeterminate(
        self,
        claimed: MonitorItemProjection,
        error_type: str,
        details: Mapping[str, Any] | None = None,
    ) -> MonitorItemProjection:
        assert claimed.claim_token is not None
        error: dict[str, Any] = {"type": error_type}
        if details:
            safe = redact_sensitive(dict(details))
            if isinstance(safe, Mapping):
                error["details"] = dict(safe)
        return self._ledger.mark_indeterminate(claimed.id, claimed.claim_token, error=error).item

    def _reject(
        self,
        claimed: MonitorItemProjection,
        error_type: str,
        details: Mapping[str, Any] | None = None,
    ) -> MonitorItemProjection:
        assert claimed.claim_token is not None
        logger.error("自动入库已拒绝：%s；原因=%s", claimed.path, error_type)
        error: dict[str, Any] = {"type": error_type}
        if details:
            safe = redact_sensitive(dict(details))
            if isinstance(safe, Mapping):
                error["details"] = dict(safe)
        return self._ledger.mark_rejected(claimed.id, claimed.claim_token, error=error).item


def _matching_monitor_root(path: object, related_path: object | None, roots: Sequence[str]) -> str:
    candidates = sorted(
        {str(root or "").strip() for root in roots if str(root or "").strip()},
        key=lambda value: len(normalize_media_path(value)),
        reverse=True,
    )
    path_keys = [normalize_media_path(path)]
    if related_path is not None and str(related_path).strip():
        path_keys.append(normalize_media_path(related_path))
    for root in candidates:
        root_key = normalize_media_path(root)
        if any(key == root_key or key.startswith(f"{root_key}/") for key in path_keys):
            return os.path.normpath(root)
    raise ValueError("ingestion event path is outside configured monitor roots")


def _item_path(item: Mapping[str, Any]) -> str:
    path = str(item.get("Path") or "").strip()
    if path:
        return path
    sources = item.get("MediaSources")
    if isinstance(sources, Sequence) and not isinstance(sources, (str, bytes)):
        for source in sources:
            if isinstance(source, Mapping) and str(source.get("Path") or "").strip():
                return str(source.get("Path") or "").strip()
    return ""


def _binding_terminal_outcome(binding: PendingBindingProjection, item_id: str) -> Mapping[str, Any] | None:
    if binding.status is BindingStatus.CONFIRMED:
        if binding.emby_item_id != item_id:
            return None
        return {"ok": True, "skipped": True, "reason": "already_completed"}
    if binding.status is BindingStatus.REJECTED:
        return {"ok": False, "skipped": False, "retryable": False, "reason": "binding_rejected"}
    if binding.status is BindingStatus.INDETERMINATE:
        return {"ok": False, "skipped": False, "retryable": False, "reason": "binding_indeterminate"}
    return None


def _observe(item: MonitorItemProjection) -> FileObservation:
    if item.event_type is MonitorEventType.DELETED:
        return FileObservation(False)
    try:
        stat = os.stat(item.path)
    except OSError:
        return FileObservation(False)
    return FileObservation(True, size=stat.st_size, mtime_ns=stat.st_mtime_ns)


def _owned_claim(
    current: MonitorItemProjection | None,
    expected: MonitorItemProjection,
) -> MonitorItemProjection:
    if (
        current is None
        or current.status is not MonitorItemStatus.CLAIMED
        or current.claim_token != expected.claim_token
    ):
        raise IngestionMonitorStateError("ingestion monitor claim is no longer owned by this Worker")
    assert current.claim_token is not None
    return current


def _safe_error(error_type: str, exc: BaseException) -> dict[str, Any]:
    return {
        "type": error_type,
        "error_type": type(exc).__name__,
        "message": str(redact_sensitive(str(exc)))[:1000],
    }


def _uuid(value: object, field: str) -> UUID:
    try:
        return UUID(str(value or ""))
    except ValueError as exc:
        raise ValueError(f"ingestion monitor Outbox {field} is invalid") from exc


__all__ = [
    "CeleryIngestionMonitorPublisher",
    "INGESTION_CLAIM_LEASE_SECONDS",
    "INGESTION_MONITOR_TOPICS",
    "IngestionBindingDeliveryPort",
    "IngestionFingerprintPort",
    "IngestionBindingIndeterminate",
    "IngestionBindingRejected",
    "IngestionCoordinatorResult",
    "IngestionMonitorCoordinator",
    "IngestionMonitorExecutor",
    "IngestionPreparationPort",
    "NativeIngestionBindingDelivery",
    "NativeIngestionFingerprintGateway",
    "NativeIngestionPreparationGateway",
    "PersistentIngestionIngressGateway",
    "PersistentPendingBindingGateway",
]
