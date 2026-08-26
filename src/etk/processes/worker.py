from __future__ import annotations

import logging
import os
import signal
import socket
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any
from uuid import UUID

from etk.bootstrap.app_factory import _create_default_container
from etk.bootstrap.runtime_supervisor import RuntimeRole, RuntimeSupervisor
from etk.compatibility.background import PersistentBackgroundGateway
from etk.compatibility.flask_app import LazyCompatibilityApplication, create_flask_app
from etk.compatibility.ingestion_monitor import (
    clear_persistent_ingestion_binding,
    clear_persistent_ingestion_ingress,
    install_persistent_ingestion_binding,
    install_persistent_ingestion_ingress,
)
from etk.compatibility.intro_detection import (
    clear_persistent_intro_detection_gateway,
    install_persistent_intro_detection_gateway,
)
from etk.compatibility.legacy_jobs import LegacyTaskAdapter
from etk.compatibility.media_effects import clear_persistent_media_effect_gateway
from etk.compatibility.persistent_runtime_state import PostgresRuntimeStateBackend
from etk.compatibility.persistent_tasks import PersistentTaskManagerGateway
from etk.compatibility.playback import (
    NativePlaybackCompatibilityGateway,
    clear_persistent_playback_gateway,
    install_persistent_playback_gateway,
)
from etk.compatibility.upload_monitor import (
    clear_persistent_upload_source_cleanup,
    install_persistent_upload_source_cleanup,
)
from etk.domains.ingestion import MonitorItemProjection, PostgresIngestionMonitorLedger
from etk.domains.media.bridge_client import HttpxEmbyBridgeClient
from etk.domains.media.compatibility import LegacyP5MediaEventGateway
from etk.domains.media.editing_effects import BridgeMediaEffectDelivery, PersistentMediaInfoCommandGateway
from etk.domains.media.effect_execution import PersistentMediaEffectExecutor, RoutedMediaEffectDelivery
from etk.domains.media.effects import MediaEffectProjection, PostgresMediaEffectLedger
from etk.domains.media.event_effects import P5MediaEventEffectDelivery
from etk.domains.media.final_refresh_effects import (
    EmbyMetadataRefreshEffectDelivery,
    clear_final_refresh_settings_provider,
    install_final_refresh_settings_provider,
)
from etk.domains.media.image_editing_compatibility import LegacyMediaImageEditingGateway
from etk.domains.media.image_editing_effects import ManualImageEffectDelivery
from etk.domains.operations.jobs import (
    JobSubmissionService,
    import_legacy_operation,
)
from etk.domains.p115 import (
    AtomicP115LocalStrmLibraryGateway,
    ChromaprintProcessAdapter,
    EmbyAuthorizationRevisionAdapter,
    EmbyPathChangeEffectDelivery,
    HttpxEmbyPathChangeGateway,
    IntroDetectionExecutionService,
    IntroDetectionOperation,
    IntroDetectionSubmissionService,
    LegacyP7IntroPolicyAdapter,
    LegacyP7IntroRuntimeSource,
    LegacyP9IntroChaptersChangedHook,
    LegacyP115CredentialStore,
    LegacyP115DirectLinkGateway,
    LegacyP115DirectoryTreeGateway,
    LegacyP115OrganizeMutationGateway,
    LegacyP115PlaybackProvider,
    LegacyP115PlaybackSourceGateway,
    LegacyP115PlaybackUserGateway,
    LegacyP115StrmSyncGateway,
    NativeIntroDetectionBatchEngine,
    NativeP115IntroDetectionGateway,
    P115DirectorySyncService,
    P115FingerprintRepairService,
    P115IntroBackfillTaskService,
    P115IntroFingerprintAdapter,
    P115OrganizeExecutionService,
    P115PlaybackConfigService,
    P115PlaybackService,
    P115StrmMediaInfoEnrichmentService,
    P115StrmSyncService,
    PostgresP115IntroDetectionRepository,
    PostgresP115PlaybackRepository,
    PostgresP115StrmSyncCoordinator,
    SqlAlchemyP115DirectoryCacheRepository,
    SqlAlchemyP115MediaInfoRepository,
    SqlAlchemyP115OrganizeExecutionRepository,
    SqlAlchemyP115OrganizePlanRepository,
    SqlAlchemyP115RenameConfigRepository,
    SqlAlchemyP115SortingRulesRepository,
    SqlAlchemyP115StrmSyncRepository,
    legacy_account_bound_direct_urls,
)
from etk.domains.p115.iso_probe import NativeP115IsoProbeGateway
from etk.domains.p115.media_info_adapters import (
    ControlledProcessRunner,
    LegacyP115DirectUrlGateway,
    LegacyP115FingerprintRemoteGateway,
    LegacyP115MediaInfoPolicyProvider,
    LocalP115FingerprintHintGateway,
)
from etk.domains.p115.media_info_restore import HttpxEmbyMediaInfoQueryGateway, P115MediaInfoRestoreService
from etk.domains.p115.media_probe import P115MediaProbeService
from etk.domains.p115.playback import PlaybackOperation
from etk.domains.p115.upload_adapters import (
    LegacyP115UploadProjection,
    LegacyP115UploadProvider,
    NativeP115UploadSourceCleanupGateway,
)
from etk.domains.p115.upload_monitor import P115UploadMonitorConfigService, UploadOperation
from etk.domains.p115.upload_repository import PostgresP115UploadMonitorRepository
from etk.domains.subscription.moviepilot_configured import ConfiguredMoviePilotGateway
from etk.domains.subscription.operation_repository import PostgresSubscriptionOperationRepository
from etk.domains.subscription.operations import SubscriptionOperation
from etk.domains.subscription.provider_execution import SubscriptionOperationExecutor
from etk.domains.system import SqlAlchemyAppSettingsRepository
from etk.platform.jobs import (
    AttemptClaimResult,
    AttemptStatus,
    InvalidJsonPayloadError,
    InvalidStateTransitionError,
    JobProjection,
    JobStatus,
    PersistentEffectIndeterminate,
    PersistentExecutionCancelled,
    PersistentExecutionInterruptedAfterEffect,
    PersistentExecutionTimedOut,
    PostgresExecutionGuard,
    PostgresJobStore,
    normalize_json_object,
)
from etk.platform.observability.redaction import redact_sensitive
from etk.platform.tasks import ProcessorIdentity, TaskOutcome, TaskOutcomeStatus, TaskRegistry
from etk.platform.tasks.catalog import (
    P4_NATIVE_TASK_KEYS,
    P5_NATIVE_TASK_KEYS,
    P6_INGESTION_NATIVE_TASK_KEYS,
    P6_NATIVE_TASK_KEYS,
    P6_P115_NATIVE_TASK_KEYS,
)

from .p5_media_tasks import LegacyP5MediaCompatibilityGateway, P5MediaTaskRouter
from .p6_ingestion import (
    INGESTION_CLAIM_LEASE_SECONDS,
    IngestionMonitorExecutor,
    LegacyIngestionBindingDelivery,
    LegacyIngestionPreparationGateway,
    PersistentIngestionIngressGateway,
    PersistentPendingBindingGateway,
)
from .p6_ingestion_tasks import P6IngestionScanService, P6IngestionTaskRouter
from .p6_intro_detection import P115IntroDetectionOperationExecutor
from .p6_p115_tasks import P6P115TaskRouter
from .p6_playback import P115PlaybackOperationExecutor
from .p6_upload_monitor import P115UploadOperationExecutor
from .p7_subscription import SUBSCRIPTION_OPERATION_CLAIM_LEASE_SECONDS
from .runtime_config import TaskRuntimeConfig

logger = logging.getLogger(__name__)

_FORCE_INCREMENTAL_TASKS = frozenset({"role-translation", "enrich-aliases", "populate-metadata", "restore_mediainfo"})
_HIGH_CHAIN = "task-chain-high-freq"
_LOW_CHAIN = "task-chain-low-freq"
_WORKFLOW_DEPTH_LIMIT = 8


class _CooperativeTimeout(RuntimeError):
    pass


class _TaskReportedFailure(RuntimeError):
    pass


class WorkerShutdownRequested(RuntimeError):
    """Reject a delivery that raced with process shutdown."""


class _ExecutionControl:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cancel_requested = False
        self._timed_out = False
        self._inflight_effects: list[str] = []
        self._committed_effects: list[str] = []

    def request_cancel(self) -> None:
        with self._lock:
            self._cancel_requested = True

    def request_timeout(self) -> None:
        with self._lock:
            self._timed_out = True

    def record_started_effect(self, label: str) -> None:
        normalized = str(label or "external-effect").strip()[:200] or "external-effect"
        with self._lock:
            if normalized not in self._inflight_effects:
                self._inflight_effects.append(normalized)

    def record_committed_effect(self, label: str) -> None:
        normalized = str(label or "external-effect").strip()[:200] or "external-effect"
        with self._lock:
            if normalized in self._inflight_effects:
                self._inflight_effects.remove(normalized)
            if normalized not in self._committed_effects:
                self._committed_effects.append(normalized)

    def record_abandoned_effect(self, label: str) -> None:
        normalized = str(label or "external-effect").strip()[:200] or "external-effect"
        with self._lock:
            if normalized in self._inflight_effects:
                self._inflight_effects.remove(normalized)

    def checkpoint(self) -> None:
        state = self.state()
        if not state["cancel_requested"] and not state["timed_out"]:
            return
        if state["committed_effects"]:
            reason = "deadline" if state["timed_out"] else "cancellation"
            raise PersistentExecutionInterruptedAfterEffect(
                f"{reason} was observed after confirmed effects: {', '.join(state['committed_effects'])}"
            )
        if state["inflight_effects"]:
            raise PersistentEffectIndeterminate(
                "execution control was observed while external effects were in flight: "
                + ", ".join(state["inflight_effects"])
            )
        if state["timed_out"]:
            raise PersistentExecutionTimedOut("persistent operation exceeded its cooperative deadline")
        raise PersistentExecutionCancelled("persistent operation cancellation was requested")

    def state(self) -> dict[str, Any]:
        with self._lock:
            return {
                "cancel_requested": self._cancel_requested,
                "timed_out": self._timed_out,
                "inflight_effects": list(self._inflight_effects),
                "committed_effects": list(self._committed_effects),
            }


class RuntimeLegacyTaskResolver:
    """Resolve executable state only inside an initialized Worker process."""

    def resolve(self, task_key: str, processor: ProcessorIdentity):
        import extensions

        try:
            task_function, _display_name, registered_processor = extensions.TASK_REGISTRY[task_key]
        except (KeyError, TypeError, ValueError) as exc:
            raise LookupError(f"legacy task registry does not contain {task_key!r}") from exc
        if str(registered_processor) != processor.value:
            raise RuntimeError(f"legacy task {task_key!r} processor identity changed at runtime")
        resolved_processor = _processor(processor)
        if resolved_processor is None:
            raise RuntimeError(f"{processor.value} processor is unavailable")
        return task_function, resolved_processor


@dataclass(slots=True)
class _ExecutionProgress:
    store: PostgresJobStore
    job_id: UUID
    attempt_id: UUID
    worker_id: str
    lease_seconds: int
    control: _ExecutionControl
    accept_succeeded_terminal_update: bool = False
    _reported_failures: list[str] = field(default_factory=list, init=False, repr=False)
    _failure_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def update(self, progress: int, message: str) -> None:
        normalized_progress = None if progress < 0 else max(0, min(100, int(progress)))
        if progress < 0:
            with self._failure_lock:
                self._reported_failures.append(str(message))
        try:
            self.store.heartbeat(
                job_id=self.job_id,
                attempt_id=self.attempt_id,
                worker_id=self.worker_id,
                progress=normalized_progress,
                message=str(message),
                log_entry={
                    "timestamp": datetime.now(UTC).isoformat(),
                    "level": "error" if progress < 0 else "info",
                    "message": str(message),
                },
                lease_seconds=self.lease_seconds,
            )
        except InvalidStateTransitionError:
            if not self._is_succeeded_terminal_update():
                raise

    def _is_succeeded_terminal_update(self) -> bool:
        if not self.accept_succeeded_terminal_update:
            return False
        projection = self.store.get_job(self.job_id)
        attempt = None if projection is None else projection.current_attempt
        return bool(
            projection is not None
            and projection.job.status is JobStatus.SUCCEEDED
            and attempt is not None
            and attempt.id == self.attempt_id
            and attempt.status is AttemptStatus.SUCCEEDED
        )

    def raise_if_reported_failure(self) -> None:
        with self._failure_lock:
            if not self._reported_failures:
                return
            latest = self._reported_failures[-1]
        raise _TaskReportedFailure(latest or "legacy task reported failure")

    def checkpoint(self) -> None:
        self.control.checkpoint()

    def record_started_effect(self, label: str) -> None:
        self.control.record_started_effect(label)

    def record_committed_effect(self, label: str) -> None:
        self.control.record_committed_effect(label)

    def record_abandoned_effect(self, label: str) -> None:
        self.control.record_abandoned_effect(label)

    def control_state(self) -> dict[str, Any]:
        return {"persistent": True, "job_id": str(self.job_id), **self.control.state()}


class _HeartbeatMonitor:
    def __init__(
        self,
        *,
        store: PostgresJobStore,
        projection: JobProjection,
        worker_id: str,
        config: TaskRuntimeConfig,
        processor: Any,
        control: _ExecutionControl,
        terminate_process: Callable[[], None],
    ) -> None:
        self._store = store
        self._projection = projection
        self._worker_id = worker_id
        self._config = config
        self._processor = processor
        self._control = control
        self._terminate_process = terminate_process
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="etk-job-heartbeat", daemon=True)
        self.cancel_requested = False
        self.timed_out = False
        self.error: BaseException | None = None
        self._started_monotonic = time.monotonic()
        self._control_requested_at: float | None = None
        self.stop_reason: str | None = None
        self.terminalized = False

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=max(1.0, self._config.heartbeat_seconds * 2))
        if self._thread.is_alive():
            raise RuntimeError("job heartbeat monitor did not stop")
        self._signal_timeout_if_due()
        if self.error is not None:
            raise RuntimeError("job heartbeat monitor failed") from self.error

    def _run(self) -> None:
        try:
            next_heartbeat = self._started_monotonic + self._config.heartbeat_seconds
            while True:
                now = time.monotonic()
                self._signal_process_shutdown(now)
                self._signal_timeout_if_due(now)
                if self._terminate_if_grace_expired(now):
                    return
                wake_at = next_heartbeat
                timeout = self._projection.job.timeout_seconds
                if timeout is not None and self.stop_reason is None:
                    wake_at = min(wake_at, self._started_monotonic + timeout)
                if self._control_requested_at is not None:
                    wake_at = min(
                        wake_at,
                        self._control_requested_at + self._config.job_termination_grace_seconds,
                    )
                if self._stop.wait(max(0.0, wake_at - now)):
                    return

                now = time.monotonic()
                self._signal_process_shutdown(now)
                self._signal_timeout_if_due(now)
                if self._terminate_if_grace_expired(now):
                    return
                if now < next_heartbeat:
                    continue

                result = self._store.heartbeat(
                    job_id=self._projection.job.id,
                    attempt_id=self._projection.current_attempt.id,
                    worker_id=self._worker_id,
                    lease_seconds=self._config.lease_seconds,
                )
                parent_cancel_requested = False
                parent_job_id = self._projection.job.parent_job_id
                if parent_job_id is not None:
                    parent = self._store.get_job(parent_job_id)
                    parent_cancel_requested = bool(
                        parent is not None and parent.job.status in {JobStatus.CANCEL_REQUESTED, JobStatus.CANCELLED}
                    )
                if result.cancel_requested or parent_cancel_requested:
                    self._request_control("cancel", now)
                next_heartbeat = now + self._config.heartbeat_seconds
        except BaseException as exc:
            self.error = exc
            _signal_stop(self._processor)

    def _signal_timeout_if_due(self, now: float | None = None) -> None:
        timeout = self._projection.job.timeout_seconds
        if timeout is None or self.stop_reason is not None:
            return
        observed_at = now if now is not None else time.monotonic()
        if observed_at - self._started_monotonic < timeout:
            return
        self._request_control("timeout", observed_at)

    def _signal_process_shutdown(self, now: float) -> None:
        if not worker_shutdown_requested() or self.stop_reason is not None:
            return
        self._store.request_cancel(self._projection.job.id)
        self._request_control("cancel", now)

    def _request_control(self, reason: str, now: float) -> None:
        if self.stop_reason is not None:
            return
        self.stop_reason = reason
        self._control_requested_at = now
        if reason == "timeout":
            self.timed_out = True
            self._control.request_timeout()
        else:
            self.cancel_requested = True
            self._control.request_cancel()
        _signal_stop(self._processor)

    def _terminate_if_grace_expired(self, now: float) -> bool:
        requested_at = self._control_requested_at
        if requested_at is None:
            return False
        if now - requested_at < self._config.job_termination_grace_seconds:
            return False
        self._terminalize_controlled_attempt()
        self.terminalized = True
        self._stop.set()
        self._terminate_process()
        return True

    def _terminalize_controlled_attempt(self) -> None:
        projection = self._projection
        attempt = projection.current_attempt
        control_state = self._control.state()
        effect_failure = _effect_interruption_failure(control_state, self.stop_reason or "cancellation")
        if effect_failure is not None:
            safe_error = _error_summary(effect_failure)
            self._store.fail_attempt(
                job_id=projection.job.id,
                attempt_id=attempt.id,
                worker_id=self._worker_id,
                error_type=type(effect_failure).__name__,
                error_message=str(safe_error["message"]),
                error_details={**safe_error, "control": control_state},
                retryable=False,
                honor_cancel_request=False,
            )
            return
        if self.stop_reason == "timeout":
            retryable = attempt.attempt_no < projection.job.max_attempts
            self._store.timeout_attempt(
                job_id=projection.job.id,
                attempt_id=attempt.id,
                worker_id=self._worker_id,
                error_message="job exceeded its execution deadline and did not stop within the grace period",
                error_details={"control": control_state},
                retryable=retryable,
                next_attempt_at=_retry_at(attempt.attempt_no) if retryable else None,
                honor_cancel_request=False,
            )
            return
        self._store.cancel_attempt(
            job_id=projection.job.id,
            attempt_id=attempt.id,
            worker_id=self._worker_id,
            reason="job did not stop within the cancellation grace period",
        )


class WorkerRuntime:
    def __init__(
        self,
        *,
        config: TaskRuntimeConfig | None = None,
        sleep: Callable[[float], None] = time.sleep,
        terminate_process: Callable[[], None] | None = None,
    ) -> None:
        self.config = config or TaskRuntimeConfig.from_environ()
        self._sleep = sleep
        self._terminate_process = terminate_process or _terminate_current_worker_process
        self._lock = threading.RLock()
        self._started = False
        self._container: Any = None
        self._config_adapter: Any = None
        self._last_settings: dict[str, Any] | None = None
        self._supervisor: RuntimeSupervisor | None = None
        self._store: PostgresJobStore | None = None
        self._registry: TaskRegistry | None = None
        self._service: JobSubmissionService | None = None
        self._adapter: LegacyTaskAdapter | None = None
        self._media_effects: PersistentMediaEffectExecutor | None = None
        self._p5_media_tasks = P5MediaTaskRouter(LegacyP5MediaCompatibilityGateway())
        self._p6_p115_tasks: P6P115TaskRouter | None = None
        self._p6_ingestion_tasks: P6IngestionTaskRouter | None = None
        self._ingestion_executor: IngestionMonitorExecutor | None = None
        self._ingestion_binding: PersistentPendingBindingGateway | None = None
        self._ingestion_ingress: PersistentIngestionIngressGateway | None = None
        self._p115_upload_executor: P115UploadOperationExecutor | None = None
        self._p115_playback_executor: P115PlaybackOperationExecutor | None = None
        self._p115_intro_executor: P115IntroDetectionOperationExecutor | None = None
        self._subscription_executor: SubscriptionOperationExecutor | None = None
        self._playback_gateway: NativePlaybackCompatibilityGateway | None = None
        self._intro_detection_gateway: NativeP115IntroDetectionGateway | None = None
        self._p115_upload_source_cleanup: NativeP115UploadSourceCleanupGateway | None = None
        self._gateway: PersistentTaskManagerGateway | None = None
        self._background_gateway: PersistentBackgroundGateway | None = None
        self._final_refresh_settings_provider: Callable[[], Any] | None = None
        self._worker_id = f"{socket.gethostname()}:{os.getpid()}"
        self._workflow_depth = threading.local()
        self._active_executions: dict[UUID, tuple[Any, _ExecutionControl]] = {}
        self._active_ingestion_executions = 0
        self._active_p115_upload_executions = 0
        self._active_p115_playback_executions = 0
        self._active_p115_intro_executions = 0
        self._active_subscription_executions = 0
        self._idle = threading.Event()
        self._idle.set()
        self._shutdown_requested = threading.Event()

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            os.environ["ETK_PROCESS_ROLE"] = "worker"
            container, config_adapter = _create_default_container(None)
            supervisor = RuntimeSupervisor()
            background_gateway: PersistentBackgroundGateway | None = None
            submission_gateway_installed = False
            media_effect_gateway_installed = False
            ingestion_binding_gateway_installed = False
            ingestion_ingress_gateway_installed = False
            final_refresh_settings_provider_installed = False
            upload_source_cleanup_installed = False
            playback_gateway_installed = False
            intro_detection_gateway_installed = False
            ingestion_binding: PersistentPendingBindingGateway | None = None
            upload_source_cleanup: NativeP115UploadSourceCleanupGateway | None = None
            intro_detection_gateway: NativeP115IntroDetectionGateway | None = None
            try:
                settings_provider = config_adapter.load
                database = container.database
                store = PostgresJobStore(
                    lambda: database.unit_of_work(),
                    execution_guard=PostgresExecutionGuard(database.raw_pool),
                )
                registry = TaskRegistry.from_fixture(_task_fixture())
                service = JobSubmissionService(store, registry)
                app_settings = SqlAlchemyAppSettingsRepository(database.unit_of_work)
                subscription_executor = SubscriptionOperationExecutor(
                    PostgresSubscriptionOperationRepository(database.unit_of_work),
                    ConfiguredMoviePilotGateway(app_settings),
                )
                ingestion_ledger = PostgresIngestionMonitorLedger(database.unit_of_work)
                upload_repository = PostgresP115UploadMonitorRepository(database.unit_of_work)
                upload_config = P115UploadMonitorConfigService(
                    upload_repository,
                    lambda: config_adapter.load().monitor_extensions,
                )
                upload_source_cleanup = NativeP115UploadSourceCleanupGateway(
                    upload_repository,
                    upload_config.get,
                )
                ingestion_ingress = PersistentIngestionIngressGateway(
                    settings=settings_provider,
                    ledger=ingestion_ledger,
                )
                p6_ingestion_tasks = P6IngestionTaskRouter(
                    P6IngestionScanService(
                        settings=settings_provider,
                        ingress=ingestion_ingress,
                        known_paths=ingestion_ledger,
                    )
                )
                p115_credentials = LegacyP115CredentialStore()
                playback_repository = PostgresP115PlaybackRepository(database.unit_of_work)
                playback_config = P115PlaybackConfigService(
                    PostgresRuntimeStateBackend(database.raw_pool.connection),
                    LegacyP115PlaybackUserGateway(),
                )
                playback_executor = P115PlaybackOperationExecutor(
                    repository=playback_repository,
                    provider=LegacyP115PlaybackProvider(playback_config),
                    config=playback_config,
                    worker_id=f"{self._worker_id}:p115-playback",
                )
                playback_service = P115PlaybackService(
                    config=playback_config,
                    repository=playback_repository,
                    sources=LegacyP115PlaybackSourceGateway(),
                    direct_links=LegacyP115DirectLinkGateway(playback_config),
                    executor=playback_executor,
                )
                playback_gateway = NativePlaybackCompatibilityGateway(lambda: playback_service)
                p115_mediainfo_repository = SqlAlchemyP115MediaInfoRepository(database.unit_of_work)
                bridge_client = HttpxEmbyBridgeClient(settings_provider)
                p115_fingerprint_repair = P115FingerprintRepairService(
                    credentials=p115_credentials,
                    repository=p115_mediainfo_repository,
                    hints=LocalP115FingerprintHintGateway(),
                    remote=LegacyP115FingerprintRemoteGateway(),
                    local_root=lambda: str(config_adapter.load().local_strm_root or ""),
                )
                processes = ControlledProcessRunner()
                intro_repository = PostgresP115IntroDetectionRepository(database.unit_of_work)
                intro_source = LegacyP7IntroRuntimeSource(intro_repository)
                intro_detection_gateway = NativeP115IntroDetectionGateway(
                    submission=IntroDetectionSubmissionService(intro_repository),
                    source=intro_source,
                    account_id=lambda: p115_credentials.load().account_id,
                )
                intro_fingerprints = P115IntroFingerprintAdapter(
                    legacy_account_bound_direct_urls(
                        p115_credentials,
                        LegacyP115DirectUrlGateway(),
                    ),
                    ChromaprintProcessAdapter(processes),
                )
                intro_executor = P115IntroDetectionOperationExecutor(
                    repository=intro_repository,
                    service=IntroDetectionExecutionService(
                        NativeIntroDetectionBatchEngine(
                            intro_repository,
                            intro_fingerprints,
                            EmbyAuthorizationRevisionAdapter(bridge_client),
                            chapters_changed=LegacyP9IntroChaptersChangedHook(),
                        ),
                        LegacyP7IntroPolicyAdapter(intro_source),
                    ),
                    worker_id=f"{self._worker_id}:p115-intro",
                )
                mediainfo_policies = LegacyP115MediaInfoPolicyProvider()
                media_probe = P115MediaProbeService(
                    direct_urls=LegacyP115DirectUrlGateway(),
                    processes=processes,
                    iso=NativeP115IsoProbeGateway(processes),
                    policies=mediainfo_policies,
                )
                strm_mediainfo = P115StrmMediaInfoEnrichmentService(
                    credentials=p115_credentials,
                    repository=p115_mediainfo_repository,
                    probe=media_probe,
                    policies=mediainfo_policies,
                )
                p6_p115_tasks = P6P115TaskRouter(
                    P115DirectorySyncService(
                        settings=app_settings,
                        credentials=p115_credentials,
                        directories=LegacyP115DirectoryTreeGateway(),
                        repository=SqlAlchemyP115DirectoryCacheRepository(database.unit_of_work),
                    ),
                    P115OrganizeExecutionService(
                        plans=SqlAlchemyP115OrganizePlanRepository(database.unit_of_work),
                        repository=SqlAlchemyP115OrganizeExecutionRepository(database.unit_of_work),
                        mutations=LegacyP115OrganizeMutationGateway(),
                    ),
                    P115StrmSyncService(
                        settings=config_adapter.load,
                        credentials=p115_credentials,
                        sorting_rules=SqlAlchemyP115SortingRulesRepository(database.unit_of_work),
                        rename_config=SqlAlchemyP115RenameConfigRepository(database.unit_of_work),
                        remote=LegacyP115StrmSyncGateway(config_adapter.load),
                        libraries=AtomicP115LocalStrmLibraryGateway(),
                        repository=SqlAlchemyP115StrmSyncRepository(database.unit_of_work),
                        coordinator=PostgresP115StrmSyncCoordinator(database.engine),
                        mediainfo=strm_mediainfo,
                    ),
                    p115_fingerprint_repair,
                    playback_service,
                    P115IntroBackfillTaskService(intro_detection_gateway),
                )
                event_compatibility = LazyCompatibilityApplication(
                    partial(
                        create_flask_app,
                        supervisor=supervisor,
                        blueprint_specs=(("routes.webhook", "webhook_bp"),),
                    )
                )
                media_effects = PersistentMediaEffectExecutor(
                    PostgresMediaEffectLedger(database.unit_of_work),
                    RoutedMediaEffectDelivery(
                        [
                            BridgeMediaEffectDelivery(bridge_client),
                            EmbyMetadataRefreshEffectDelivery(settings_provider),
                            ManualImageEffectDelivery(LegacyMediaImageEditingGateway()),
                            P5MediaEventEffectDelivery(LegacyP5MediaEventGateway(event_compatibility)),
                            EmbyPathChangeEffectDelivery(
                                settings_provider,
                                HttpxEmbyPathChangeGateway(),
                            ),
                        ]
                    ),
                    worker_id=f"{self._worker_id}:media-effect",
                    lease_seconds=lambda: max(float(config_adapter.load().emby_api_timeout) + 30, 90),
                )
                ingestion_binding = PersistentPendingBindingGateway(
                    ledger=ingestion_ledger,
                    delivery=LegacyIngestionBindingDelivery(
                        jobs=lambda: service,
                        uow_factory=database.unit_of_work,
                        bridge=bridge_client,
                        media_effects=media_effects,
                    ),
                    claimant_id=f"{self._worker_id}:binding",
                    lease_seconds=INGESTION_CLAIM_LEASE_SECONDS,
                )
                ingestion_executor = IngestionMonitorExecutor(
                    uow_factory=database.unit_of_work,
                    ledger=ingestion_ledger,
                    settings=settings_provider,
                    preparation=LegacyIngestionPreparationGateway(settings_provider),
                    pending_bindings=ingestion_binding,
                    media_effects=media_effects,
                    processor=lambda: _processor(ProcessorIdentity.MEDIA),
                    worker_id=f"{self._worker_id}:ingestion",
                    lease_seconds=INGESTION_CLAIM_LEASE_SECONDS,
                )

                def submit_upload_screenshot(operation: UploadOperation, _path: str) -> str:
                    submitted = service.submit_task(
                        "restore-nfo-and-images",
                        {},
                        trigger_type="upload-monitor",
                        idempotency_scope="p115-upload-screenshot",
                        idempotency_key=str(operation.id),
                        max_attempts=3,
                        silent=True,
                    )
                    return str(submitted.projection.job.id)

                p115_upload_executor = P115UploadOperationExecutor(
                    repository=upload_repository,
                    provider=LegacyP115UploadProvider(),
                    projection=LegacyP115UploadProjection(
                        ingestion=ingestion_ingress,
                        submit_screenshot=submit_upload_screenshot,
                    ),
                    worker_id=f"{self._worker_id}:p115-upload",
                )
                p5_media_tasks = P5MediaTaskRouter(
                    LegacyP5MediaCompatibilityGateway(),
                    P115MediaInfoRestoreService(
                        credentials=p115_credentials,
                        repository=p115_mediainfo_repository,
                        fingerprints=p115_fingerprint_repair,
                        emby=HttpxEmbyMediaInfoQueryGateway(settings_provider),
                        probe=media_probe,
                        policies=mediainfo_policies,
                        commands=PersistentMediaInfoCommandGateway(
                            database.unit_of_work,
                            bridge_client,
                            executor=media_effects,
                        ),
                    ),
                )
                supervisor.start(role=RuntimeRole.WORKER, background=False)
                adapter = LegacyTaskAdapter(registry, RuntimeLegacyTaskResolver())
                gateway = PersistentTaskManagerGateway(service)
                background_gateway = PersistentBackgroundGateway(lambda: service)
                from etk.compatibility.background import install_persistent_background_gateway
                from etk.compatibility.media_effects import install_persistent_media_effect_gateway
                from task_manager import install_persistent_submission_gateway

                install_persistent_submission_gateway(gateway)
                submission_gateway_installed = True
                install_persistent_background_gateway(background_gateway)
                install_final_refresh_settings_provider(settings_provider)
                final_refresh_settings_provider_installed = True
                install_persistent_media_effect_gateway(media_effects)
                media_effect_gateway_installed = True
                install_persistent_ingestion_binding(ingestion_binding)
                ingestion_binding_gateway_installed = True
                install_persistent_ingestion_ingress(ingestion_ingress)
                ingestion_ingress_gateway_installed = True
                install_persistent_upload_source_cleanup(upload_source_cleanup)
                upload_source_cleanup_installed = True
                install_persistent_playback_gateway(playback_gateway)
                playback_gateway_installed = True
                install_persistent_intro_detection_gateway(intro_detection_gateway)
                intro_detection_gateway_installed = True
            except BaseException:
                try:
                    if intro_detection_gateway_installed and intro_detection_gateway is not None:
                        clear_persistent_intro_detection_gateway(intro_detection_gateway)
                    if playback_gateway_installed:
                        clear_persistent_playback_gateway(playback_gateway)
                    if upload_source_cleanup_installed and upload_source_cleanup is not None:
                        clear_persistent_upload_source_cleanup(upload_source_cleanup)
                    if ingestion_ingress_gateway_installed:
                        clear_persistent_ingestion_ingress(ingestion_ingress)
                    if ingestion_binding_gateway_installed and ingestion_binding is not None:
                        clear_persistent_ingestion_binding(ingestion_binding)
                    if media_effect_gateway_installed:
                        clear_persistent_media_effect_gateway(media_effects)
                    if final_refresh_settings_provider_installed:
                        clear_final_refresh_settings_provider(settings_provider)
                    if background_gateway is not None:
                        from etk.compatibility.background import clear_persistent_background_gateway

                        clear_persistent_background_gateway(background_gateway)
                    if submission_gateway_installed:
                        from task_manager import clear_persistent_submission_gateway

                        clear_persistent_submission_gateway(gateway)
                    supervisor.stop()
                finally:
                    container.close()
                raise
            self._container = container
            self._config_adapter = config_adapter
            self._last_settings = container.settings.to_runtime_dict()
            self._supervisor = supervisor
            self._store = store
            self._registry = registry
            self._service = service
            self._adapter = adapter
            self._media_effects = media_effects
            self._p5_media_tasks = p5_media_tasks
            self._p6_p115_tasks = p6_p115_tasks
            self._p6_ingestion_tasks = p6_ingestion_tasks
            self._ingestion_executor = ingestion_executor
            self._ingestion_binding = ingestion_binding
            self._ingestion_ingress = ingestion_ingress
            self._p115_upload_executor = p115_upload_executor
            self._p115_playback_executor = playback_executor
            self._p115_intro_executor = intro_executor
            self._subscription_executor = subscription_executor
            self._playback_gateway = playback_gateway
            self._intro_detection_gateway = intro_detection_gateway
            self._p115_upload_source_cleanup = upload_source_cleanup
            self._gateway = gateway
            self._background_gateway = background_gateway
            self._final_refresh_settings_provider = settings_provider
            self._started = True

    def request_shutdown(self) -> None:
        self._shutdown_requested.set()
        with self._lock:
            store = self._store
            active = tuple(self._active_executions.items())
        for job_id, (processor, control) in active:
            if store is not None:
                try:
                    store.request_cancel(job_id)
                except Exception:
                    logger.exception("Unable to persist cooperative Worker shutdown", extra={"job_id": str(job_id)})
            control.request_cancel()
            _signal_stop(processor)

    def close(self) -> None:
        self.request_shutdown()
        if not self._idle.wait(self.config.worker_shutdown_timeout_seconds):
            raise RuntimeError("active Worker execution did not stop before the shutdown timeout")
        with self._lock:
            if not self._started:
                return
            errors: list[Exception] = []
            if self._intro_detection_gateway is not None:
                try:
                    clear_persistent_intro_detection_gateway(self._intro_detection_gateway)
                except Exception as exc:
                    errors.append(exc)
            if self._playback_gateway is not None:
                try:
                    clear_persistent_playback_gateway(self._playback_gateway)
                except Exception as exc:
                    errors.append(exc)
            if self._p115_upload_source_cleanup is not None:
                try:
                    clear_persistent_upload_source_cleanup(self._p115_upload_source_cleanup)
                except Exception as exc:
                    errors.append(exc)
            if self._ingestion_ingress is not None:
                try:
                    clear_persistent_ingestion_ingress(self._ingestion_ingress)
                except Exception as exc:
                    errors.append(exc)
            if self._ingestion_binding is not None:
                try:
                    clear_persistent_ingestion_binding(self._ingestion_binding)
                except Exception as exc:
                    errors.append(exc)
            if self._media_effects is not None:
                try:
                    clear_persistent_media_effect_gateway(self._media_effects)
                except Exception as exc:
                    errors.append(exc)
            if self._final_refresh_settings_provider is not None:
                try:
                    clear_final_refresh_settings_provider(self._final_refresh_settings_provider)
                except Exception as exc:
                    errors.append(exc)
            if self._background_gateway is not None:
                try:
                    from etk.compatibility.background import clear_persistent_background_gateway

                    clear_persistent_background_gateway(self._background_gateway)
                except Exception as exc:
                    errors.append(exc)
            if self._gateway is not None:
                try:
                    from task_manager import clear_persistent_submission_gateway

                    clear_persistent_submission_gateway(self._gateway)
                except Exception as exc:
                    errors.append(exc)
            if self._supervisor is not None:
                try:
                    self._supervisor.stop()
                except Exception as exc:
                    errors.append(exc)
            if self._container is not None:
                try:
                    self._container.close()
                except Exception as exc:
                    errors.append(exc)
            self._container = None
            self._config_adapter = None
            self._last_settings = None
            self._supervisor = None
            self._store = None
            self._registry = None
            self._service = None
            self._adapter = None
            self._media_effects = None
            self._p6_p115_tasks = None
            self._p6_ingestion_tasks = None
            self._ingestion_executor = None
            self._ingestion_binding = None
            self._ingestion_ingress = None
            self._p115_upload_executor = None
            self._p115_playback_executor = None
            self._p115_intro_executor = None
            self._subscription_executor = None
            self._playback_gateway = None
            self._intro_detection_gateway = None
            self._p115_upload_source_cleanup = None
            self._gateway = None
            self._background_gateway = None
            self._final_refresh_settings_provider = None
            self._active_executions.clear()
            self._active_ingestion_executions = 0
            self._active_p115_upload_executions = 0
            self._active_p115_playback_executions = 0
            self._active_p115_intro_executions = 0
            self._active_subscription_executions = 0
            self._idle.set()
            self._started = False
            if errors:
                raise ExceptionGroup("Worker runtime shutdown failed", errors)

    def execute_media_effect(self, *, effect_id: str) -> MediaEffectProjection:
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown is in progress")
        self.start()
        try:
            parsed_effect_id = UUID(str(effect_id))
        except ValueError as exc:
            raise ValueError("media effect id must be a UUID") from exc
        with self._lock:
            executor = self._media_effects
        if executor is None:
            raise RuntimeError("Worker media effect executor is not initialized")
        return executor.execute(parsed_effect_id)

    def execute_ingestion_monitor(self, *, item_id: str) -> MonitorItemProjection | None:
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown is in progress")
        try:
            parsed_item_id = UUID(str(item_id))
        except ValueError as exc:
            raise ValueError("ingestion monitor item id must be a UUID") from exc
        self.start()
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown started during runtime initialization")
        with self._lock:
            executor = self._ingestion_executor
            if executor is None:
                raise RuntimeError("Worker ingestion monitor executor is not initialized")
            self._active_ingestion_executions += 1
            self._idle.clear()
        try:
            return executor.execute(parsed_item_id)
        finally:
            with self._lock:
                self._active_ingestion_executions -= 1
                if (
                    not self._active_executions
                    and self._active_ingestion_executions == 0
                    and self._active_p115_upload_executions == 0
                    and self._active_p115_playback_executions == 0
                    and self._active_p115_intro_executions == 0
                    and self._active_subscription_executions == 0
                ):
                    self._idle.set()

    def execute_p115_upload_operation(self, *, operation_id: str) -> UploadOperation | None:
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown is in progress")
        try:
            parsed_operation_id = UUID(str(operation_id))
        except ValueError as exc:
            raise ValueError("P115 upload operation id must be a UUID") from exc
        self.start()
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown started during runtime initialization")
        with self._lock:
            executor = self._p115_upload_executor
            if executor is None:
                raise RuntimeError("Worker P115 upload executor is not initialized")
            self._active_p115_upload_executions += 1
            self._idle.clear()
        try:
            return executor.execute(parsed_operation_id)
        finally:
            with self._lock:
                self._active_p115_upload_executions -= 1
                if (
                    not self._active_executions
                    and self._active_ingestion_executions == 0
                    and self._active_p115_upload_executions == 0
                    and self._active_p115_playback_executions == 0
                    and self._active_p115_intro_executions == 0
                    and self._active_subscription_executions == 0
                ):
                    self._idle.set()

    def execute_p115_playback_operation(self, *, operation_id: str) -> PlaybackOperation | None:
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown is in progress")
        try:
            parsed_operation_id = UUID(str(operation_id))
        except ValueError as exc:
            raise ValueError("P115 playback operation id must be a UUID") from exc
        self.start()
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown started during runtime initialization")
        with self._lock:
            executor = self._p115_playback_executor
            if executor is None:
                raise RuntimeError("Worker P115 playback executor is not initialized")
            self._active_p115_playback_executions += 1
            self._idle.clear()
        try:
            return executor.execute(parsed_operation_id)
        finally:
            with self._lock:
                self._active_p115_playback_executions -= 1
                if (
                    not self._active_executions
                    and self._active_ingestion_executions == 0
                    and self._active_p115_upload_executions == 0
                    and self._active_p115_playback_executions == 0
                    and self._active_p115_intro_executions == 0
                    and self._active_subscription_executions == 0
                ):
                    self._idle.set()

    def execute_p115_intro_detection_operation(
        self,
        *,
        operation_id: str,
    ) -> IntroDetectionOperation | None:
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown is in progress")
        try:
            parsed_operation_id = UUID(str(operation_id))
        except ValueError as exc:
            raise ValueError("P115 intro detection operation id must be a UUID") from exc
        self.start()
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown started during runtime initialization")
        with self._lock:
            executor = self._p115_intro_executor
            if executor is None:
                raise RuntimeError("Worker P115 intro detection executor is not initialized")
            self._active_p115_intro_executions += 1
            self._idle.clear()
        try:
            return executor.execute(parsed_operation_id)
        finally:
            with self._lock:
                self._active_p115_intro_executions -= 1
                if (
                    not self._active_executions
                    and self._active_ingestion_executions == 0
                    and self._active_p115_upload_executions == 0
                    and self._active_p115_playback_executions == 0
                    and self._active_p115_intro_executions == 0
                    and self._active_subscription_executions == 0
                ):
                    self._idle.set()

    def execute_subscription_operation(self, *, operation_id: str) -> SubscriptionOperation | None:
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown is in progress")
        try:
            parsed_operation_id = UUID(str(operation_id))
        except ValueError as exc:
            raise ValueError("subscription operation id must be a UUID") from exc
        self.start()
        if self._shutdown_requested.is_set() or worker_shutdown_requested():
            raise WorkerShutdownRequested("Worker shutdown started during runtime initialization")
        with self._lock:
            executor = self._subscription_executor
            if executor is None:
                raise RuntimeError("Worker subscription operation executor is not initialized")
            self._active_subscription_executions += 1
            self._idle.clear()
        try:
            execution = executor.execute(
                parsed_operation_id,
                worker_id=f"{self._worker_id}:subscription",
                lease_seconds=SUBSCRIPTION_OPERATION_CLAIM_LEASE_SECONDS,
            )
            return execution.operation if execution is not None else None
        finally:
            with self._lock:
                self._active_subscription_executions -= 1
                if (
                    not self._active_executions
                    and self._active_ingestion_executions == 0
                    and self._active_p115_upload_executions == 0
                    and self._active_p115_playback_executions == 0
                    and self._active_p115_intro_executions == 0
                    and self._active_subscription_executions == 0
                ):
                    self._idle.set()

    def execute(self, *, event_id: str, job_id: str, attempt_id: str) -> None:
        del event_id
        if worker_shutdown_requested() or self._shutdown_requested.is_set():
            raise WorkerShutdownRequested("Worker shutdown is already in progress")
        self.start()
        if worker_shutdown_requested() or self._shutdown_requested.is_set():
            raise WorkerShutdownRequested("Worker shutdown started during runtime initialization")
        store = self._require_store()
        parsed_job_id = UUID(job_id)
        parsed_attempt_id = UUID(attempt_id)
        with store.execution_guard(parsed_job_id) as acquired:
            if not acquired:
                logger.info("Duplicate delivery skipped because Job execution guard is held", extra={"job_id": job_id})
                return
            if self._shutdown_requested.is_set():
                raise WorkerShutdownRequested("Worker shutdown started before Job claim")
            if worker_shutdown_requested():
                raise WorkerShutdownRequested("process shutdown started before Job claim")
            pending = store.get_job(parsed_job_id)
            if pending is not None and pending.job.parent_job_id is None:
                self._refresh_settings()
            claim = store.claim_attempt(
                job_id=parsed_job_id,
                attempt_id=parsed_attempt_id,
                worker_id=self._worker_id,
                lease_seconds=self.config.lease_seconds,
            )
            if not claim.claimed:
                logger.info("Duplicate or stale delivery skipped", extra={"job_id": job_id, "attempt_id": attempt_id})
                return
            self._execute_claimed(claim)

    def _execute_claimed(self, claim: AttemptClaimResult) -> None:
        store = self._require_store()
        projection = claim.projection
        job = projection.job
        attempt = projection.current_attempt
        registry = self._registry
        adapter = self._adapter
        if registry is None or adapter is None:
            raise RuntimeError("Worker task registry is not initialized")
        try:
            definition = registry.require(job.task_key)
        except KeyError:
            definition = None
        store.start_attempt(
            job_id=job.id,
            attempt_id=attempt.id,
            worker_id=self._worker_id,
            lease_seconds=self.config.lease_seconds,
        )
        envelope = _request_envelope(job.request_payload)
        processor_identity = ProcessorIdentity(envelope["processor_type"])
        processor = (
            None
            if (
                definition is not None
                and (
                    definition.key in (P4_NATIVE_TASK_KEYS | P6_NATIVE_TASK_KEYS)
                    or definition.key == "restore_mediainfo"
                )
            )
            or envelope.get("call_style") == "direct"
            else _processor(processor_identity)
        )
        is_direct = envelope.get("call_style") == "direct"
        control = _ExecutionControl()
        _clear_stop(processor)
        self._register_execution(job.id, processor, control)
        monitor = _HeartbeatMonitor(
            store=store,
            projection=projection,
            worker_id=self._worker_id,
            config=self.config,
            processor=processor,
            control=control,
            terminate_process=self._terminate_process,
        )
        progress = _ExecutionProgress(
            store=store,
            job_id=job.id,
            attempt_id=attempt.id,
            worker_id=self._worker_id,
            lease_seconds=self.config.lease_seconds,
            control=control,
            accept_succeeded_terminal_update=job.task_key == "scan-organize-115",
        )
        try:
            monitor.start()
        except BaseException:
            _clear_stop(processor)
            self._unregister_execution(job.id)
            raise
        result: Any = None
        failure: Exception | None = None
        p4_terminalization_reported = False
        try:
            if definition is not None and definition.key in P4_NATIVE_TASK_KEYS:
                result = self._execute_chain(projection)
            elif definition is not None and definition.key in P5_NATIVE_TASK_KEYS:
                message = registry.create_message(
                    definition.key,
                    envelope["parameters"],
                    payload_version=int(envelope["payload_version"]),
                    metadata={"job_id": str(job.id), "attempt_id": str(attempt.id)},
                )
                from task_manager import persistent_execution

                with persistent_execution(progress):
                    result = self._p5_media_tasks.execute(message, processor, progress)
                if not isinstance(result, TaskOutcome):
                    raise TypeError(f"P5 native task {definition.key!r} did not return TaskOutcome")
            elif definition is not None and definition.key in P6_NATIVE_TASK_KEYS:
                metadata = {
                    "job_id": str(job.id),
                    "attempt_id": str(attempt.id),
                    "worker_id": self._worker_id,
                }
                requested_by_user_id = str(getattr(job, "requested_by_user_id", None) or "").strip()
                if requested_by_user_id:
                    metadata["requested_by_user_id"] = requested_by_user_id
                message = registry.create_message(
                    definition.key,
                    envelope["parameters"],
                    payload_version=int(envelope["payload_version"]),
                    metadata=metadata,
                )
                if definition.key in P6_INGESTION_NATIVE_TASK_KEYS:
                    if self._p6_ingestion_tasks is None:
                        raise RuntimeError("P6 ingestion task router is unavailable")
                    result = self._p6_ingestion_tasks.execute(message, progress)
                elif definition.key in P6_P115_NATIVE_TASK_KEYS:
                    if self._p6_p115_tasks is None:
                        raise RuntimeError("P6 115 task router is unavailable")
                    result = self._p6_p115_tasks.execute(message, progress)
                else:
                    raise LookupError(f"P6 native task {definition.key!r} has no task router")
                if not isinstance(result, TaskOutcome):
                    raise TypeError(f"P6 native task {definition.key!r} did not return TaskOutcome")
            elif definition is not None:
                message = registry.create_message(
                    definition.key,
                    envelope["parameters"],
                    payload_version=int(envelope["payload_version"]),
                    metadata={"job_id": str(job.id), "attempt_id": str(attempt.id)},
                )
                from task_manager import persistent_execution

                with persistent_execution(progress):
                    result = adapter.execute(message)
            else:
                result = self._execute_importable_operation(envelope, processor, progress)
            p4_terminalization_reported = _p4_terminalized_native_outcome(job.task_key, result)
            progress.raise_if_reported_failure()
            _raise_for_reported_failure(job.task_key, result)
        except Exception as exc:
            failure = exc
        finally:
            try:
                monitor.close()
            except Exception as exc:
                failure = failure or exc

        try:
            if monitor.terminalized:
                return
            latest = store.get_job(job.id)
            if p4_terminalization_reported and _p4_terminalization_matches_outcome(
                latest,
                attempt_id=attempt.id,
                result=result,
            ):
                return
            control_state = control.state()
            cancellation_requested = bool(
                monitor.cancel_requested
                or worker_shutdown_requested()
                or (latest is not None and latest.job.status is JobStatus.CANCEL_REQUESTED)
            )
            control_reason = monitor.stop_reason
            if control_reason is None and (
                isinstance(failure, PersistentExecutionTimedOut)
                or isinstance(failure, _CooperativeTimeout)
                or monitor.timed_out
            ):
                control_reason = "timeout"
            if control_reason is None and (
                isinstance(failure, PersistentExecutionCancelled)
                or cancellation_requested
                or (not is_direct and failure is None and _stop_requested(processor))
            ):
                control_reason = "cancel"

            if control_reason is not None:
                effect_failure = _effect_interruption_failure(control_state, control_reason)
                if effect_failure is not None:
                    safe_error = _error_summary(effect_failure)
                    store.fail_attempt(
                        job_id=job.id,
                        attempt_id=attempt.id,
                        worker_id=self._worker_id,
                        error_type=type(effect_failure).__name__,
                        error_message=str(safe_error["message"]),
                        error_details={**safe_error, "control": control_state},
                        retryable=False,
                        honor_cancel_request=False,
                    )
                    return
                if control_reason == "cancel":
                    store.cancel_attempt(
                        job_id=job.id,
                        attempt_id=attempt.id,
                        worker_id=self._worker_id,
                    )
                    return
                retryable = attempt.attempt_no < job.max_attempts
                store.timeout_attempt(
                    job_id=job.id,
                    attempt_id=attempt.id,
                    worker_id=self._worker_id,
                    error_message=str(failure or "job exceeded its cooperative timeout"),
                    error_details={"control": control_state},
                    retryable=retryable,
                    next_attempt_at=_retry_at(attempt.attempt_no) if retryable else None,
                    honor_cancel_request=False,
                )
                return

            if (
                is_direct
                and failure is not None
                and not isinstance(
                    failure,
                    (
                        PersistentEffectIndeterminate,
                        PersistentExecutionInterruptedAfterEffect,
                    ),
                )
            ):
                failure = _effect_interruption_failure(control_state, "failure") or failure
            if isinstance(
                failure,
                (PersistentExecutionInterruptedAfterEffect, PersistentEffectIndeterminate),
            ):
                safe_error = _error_summary(failure)
                if isinstance(result, TaskOutcome):
                    safe_error["task_outcome"] = result.to_result()
                store.fail_attempt(
                    job_id=job.id,
                    attempt_id=attempt.id,
                    worker_id=self._worker_id,
                    error_type=type(failure).__name__,
                    error_message=str(safe_error["message"]),
                    error_details={**safe_error, "control": control.state()},
                    retryable=False,
                    honor_cancel_request=not is_direct,
                )
                return
            if failure is not None:
                retryable = attempt.attempt_no < job.max_attempts and not (
                    is_direct and (monitor.timed_out or cancellation_requested)
                )
                safe_error = _error_summary(failure)
                if isinstance(result, TaskOutcome):
                    safe_error["task_outcome"] = result.to_result()
                store.fail_attempt(
                    job_id=job.id,
                    attempt_id=attempt.id,
                    worker_id=self._worker_id,
                    error_type=type(failure).__name__,
                    error_message=str(safe_error["message"]),
                    error_details=safe_error,
                    retryable=retryable,
                    next_attempt_at=_retry_at(attempt.attempt_no) if retryable else None,
                    honor_cancel_request=not is_direct,
                )
                if not retryable:
                    logger.error(
                        "Job execution failed",
                        exc_info=(type(failure), failure, failure.__traceback__),
                        extra={"job_id": str(job.id)},
                    )
                return
            store.succeed_attempt(
                job_id=job.id,
                attempt_id=attempt.id,
                worker_id=self._worker_id,
                result=_result_object(result),
                honor_cancel_request=not is_direct,
            )
        finally:
            _clear_stop(processor)
            self._unregister_execution(job.id)

    def _execute_importable_operation(
        self,
        envelope: Mapping[str, Any],
        processor: Any,
        progress: _ExecutionProgress,
    ) -> Any:
        operation = envelope.get("legacy_operation")
        if not isinstance(operation, Mapping):
            raise LookupError("Job task key is not present in the frozen task registry")
        task_function = import_legacy_operation(
            str(operation.get("module") or ""),
            str(operation.get("qualname") or ""),
        )
        args = envelope.get("positional_parameters", [])
        if not isinstance(args, list):
            raise InvalidJsonPayloadError("positional_parameters must be a JSON array")
        from task_manager import persistent_execution

        with persistent_execution(progress):
            if envelope.get("call_style") == "direct":
                progress.checkpoint()
                return task_function(*args, **envelope["parameters"])
            return task_function(processor, *args, **envelope["parameters"])

    def _execute_chain(self, parent: JobProjection) -> dict[str, Any]:
        depth = int(getattr(self._workflow_depth, "value", 0))
        if depth >= _WORKFLOW_DEPTH_LIMIT:
            raise RuntimeError("task-chain recursion depth exceeded")
        self._workflow_depth.value = depth + 1
        try:
            return self._execute_chain_at_depth(parent)
        finally:
            self._workflow_depth.value = depth

    def _execute_chain_at_depth(self, parent: JobProjection) -> dict[str, Any]:
        store = self._require_store()
        service = self._service
        if service is None or self._supervisor is None:
            raise RuntimeError("Worker runtime is not initialized")
        config = self._supervisor.config
        if parent.job.task_key == _HIGH_CHAIN:
            sequence = config.get("task_chain_sequence", [])
            max_minutes = int(config.get("task_chain_max_runtime_minutes") or 0)
        elif parent.job.task_key == _LOW_CHAIN:
            sequence = config.get("task_chain_low_freq_sequence", [])
            max_minutes = int(config.get("task_chain_low_freq_max_runtime_minutes") or 0)
        else:
            raise LookupError(f"unknown native chain task {parent.job.task_key!r}")
        if not isinstance(sequence, list):
            raise TypeError("task chain sequence must be a list")
        deadline = time.monotonic() + max_minutes * 60 if max_minutes > 0 else None
        outcomes: list[dict[str, Any]] = []
        for step, raw_key in enumerate(sequence, 1):
            task_key = str(raw_key)
            remaining_seconds = None if deadline is None else int(deadline - time.monotonic())
            if remaining_seconds is not None and remaining_seconds < 1:
                raise _CooperativeTimeout("task chain exceeded its maximum runtime")
            latest_parent = store.get_job(parent.job.id)
            if latest_parent is not None and latest_parent.job.status is JobStatus.CANCEL_REQUESTED:
                return {"status": "cancel_requested", "children": outcomes}
            try:
                registry = self._registry
                if registry is None:
                    raise RuntimeError("Worker task registry is not initialized")
                registry.require(task_key)
            except KeyError:
                outcomes.append({"task_key": task_key, "status": "skipped", "reason": "unknown task"})
                continue
            parameters = {"force_full_update": False} if task_key in _FORCE_INCREMENTAL_TASKS else {}
            submitted = service.submit_task(
                task_key,
                parameters,
                trigger_type="workflow",
                parent_job_id=parent.job.id,
                workflow_step=step,
                idempotency_scope="task-chain-child",
                idempotency_key=f"{parent.job.id}:{step}:{task_key}",
                max_attempts=1,
                timeout_seconds=remaining_seconds,
                silent=True,
                dispatch=False,
            )
            child = submitted.projection
            self.execute(
                event_id="inline-workflow",
                job_id=str(child.job.id),
                attempt_id=str(child.current_attempt.id),
            )
            completed = store.get_job(child.job.id)
            if completed is None:
                raise RuntimeError("workflow child disappeared after execution")
            if not completed.job.status.terminal:
                raise RuntimeError("workflow child did not complete during inline execution")
            outcomes.append(
                {
                    "job_id": str(completed.job.id),
                    "task_key": task_key,
                    "status": completed.job.status.value,
                }
            )
            if completed.job.status is JobStatus.TIMED_OUT:
                raise _CooperativeTimeout(f"workflow child {task_key!r} exceeded the remaining deadline")
            if completed.job.status is JobStatus.CANCELLED:
                latest_parent = store.get_job(parent.job.id)
                if latest_parent is not None and latest_parent.job.status is JobStatus.CANCEL_REQUESTED:
                    return {"status": "cancel_requested", "children": outcomes}
                raise RuntimeError(f"workflow child {task_key!r} was cancelled independently")
            store.heartbeat(
                job_id=parent.job.id,
                attempt_id=parent.current_attempt.id,
                worker_id=self._worker_id,
                progress=int(step / max(len(sequence), 1) * 100),
                message=f"任务链进度 {step}/{len(sequence)}: {task_key}",
                lease_seconds=self.config.lease_seconds,
            )
            self._sleep(3.0 if completed.job.status is JobStatus.FAILED else 1.0)
        return {"status": "completed", "children": outcomes}

    def _refresh_settings(self) -> None:
        with self._lock:
            adapter = self._config_adapter
            supervisor = self._supervisor
            if adapter is None or supervisor is None:
                raise RuntimeError("Worker runtime configuration is not initialized")
            settings = adapter.load()
            values = settings.to_runtime_dict()
            if values == self._last_settings:
                return
            supervisor.stop()
            supervisor.start(role=RuntimeRole.WORKER, background=False)
            self._last_settings = values

    def _register_execution(self, job_id: UUID, processor: Any, control: _ExecutionControl) -> None:
        with self._lock:
            self._active_executions[job_id] = (processor, control)
            self._idle.clear()

    def _unregister_execution(self, job_id: UUID) -> None:
        with self._lock:
            self._active_executions.pop(job_id, None)
            if (
                not self._active_executions
                and self._active_ingestion_executions == 0
                and self._active_p115_upload_executions == 0
                and self._active_p115_playback_executions == 0
                and self._active_p115_intro_executions == 0
                and self._active_subscription_executions == 0
            ):
                self._idle.set()

    def _require_store(self) -> PostgresJobStore:
        if self._store is None:
            raise RuntimeError("Worker runtime is not started")
        return self._store


_RUNTIME_LOCK = threading.Lock()
_RUNTIME: WorkerRuntime | None = None
_PROCESS_SHUTDOWN_REQUESTED = False


def get_worker_runtime() -> WorkerRuntime:
    global _RUNTIME
    if worker_shutdown_requested():
        raise WorkerShutdownRequested("Worker shutdown is already in progress")
    with _RUNTIME_LOCK:
        if _RUNTIME is None:
            _RUNTIME = WorkerRuntime()
        return _RUNTIME


def close_worker_runtime() -> None:
    global _RUNTIME
    with _RUNTIME_LOCK:
        runtime, _RUNTIME = _RUNTIME, None
    if runtime is not None:
        runtime.close()


def request_worker_shutdown() -> None:
    """Signal-safe sticky shutdown request used by Celery's POSIX handler."""

    global _PROCESS_SHUTDOWN_REQUESTED
    _PROCESS_SHUTDOWN_REQUESTED = True


def worker_shutdown_requested() -> bool:
    return _PROCESS_SHUTDOWN_REQUESTED


def _task_fixture() -> Path:
    return Path(__file__).resolve().parents[3] / "verification" / "fixtures" / "tasks" / "ordered-task-catalog.json"


def _request_envelope(value: Mapping[str, Any]) -> dict[str, Any]:
    payload = normalize_json_object(dict(value), field="request_payload")
    parameters = payload.get("parameters")
    if not isinstance(parameters, dict):
        raise InvalidJsonPayloadError("request_payload.parameters must be a JSON object")
    processor = ProcessorIdentity(str(payload.get("processor_type") or ""))
    version = payload.get("payload_version")
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise InvalidJsonPayloadError("request_payload.payload_version must be a positive integer")
    payload["parameters"] = parameters
    payload["processor_type"] = processor.value
    payload["payload_version"] = version
    return payload


def _processor(identity: ProcessorIdentity) -> Any:
    import extensions

    return {
        ProcessorIdentity.MEDIA: extensions.media_processor_instance,
        ProcessorIdentity.WATCHLIST: extensions.watchlist_processor_instance,
        ProcessorIdentity.ACTOR: extensions.actor_subscription_processor_instance,
    }[identity]


def _signal_stop(processor: Any) -> None:
    if processor is not None:
        processor.signal_stop()


def _clear_stop(processor: Any) -> None:
    if processor is not None:
        processor.clear_stop_signal()


def _stop_requested(processor: Any) -> bool:
    return bool(processor is not None and processor.is_stop_requested())


def _retry_at(attempt_no: int) -> datetime:
    delay_seconds = min(300, 2 ** max(0, attempt_no - 1))
    return datetime.now(UTC) + timedelta(seconds=delay_seconds)


def _effect_interruption_failure(control_state: Mapping[str, Any], reason: str) -> Exception | None:
    inflight = list(control_state.get("inflight_effects") or ())
    if inflight:
        return PersistentEffectIndeterminate(
            f"{reason} interrupted external effects with an unknown outcome: {', '.join(map(str, inflight))}"
        )
    committed = list(control_state.get("committed_effects") or ())
    if committed:
        return PersistentExecutionInterruptedAfterEffect(
            f"{reason} was observed after confirmed effects: {', '.join(map(str, committed))}"
        )
    return None


def _terminate_current_worker_process() -> None:
    os.kill(os.getpid(), signal.SIGKILL)


def _result_object(value: Any) -> dict[str, Any]:
    if isinstance(value, TaskOutcome):
        return value.to_result()
    if value is None:
        return {}
    if isinstance(value, Mapping):
        try:
            return normalize_json_object(dict(value), field="result")
        except InvalidJsonPayloadError:
            pass
    safe = redact_sensitive(value)
    try:
        return normalize_json_object({"value": safe}, field="result")
    except InvalidJsonPayloadError:
        return {"value_repr": str(safe)[:2000]}


def _error_summary(error: Exception) -> dict[str, Any]:
    message = redact_sensitive(str(error))
    return {
        "type": type(error).__name__,
        "message": str(message)[:2000],
    }


def _p4_terminalized_native_outcome(task_key: str, result: Any) -> bool:
    return (
        task_key == "scan-organize-115"
        and isinstance(result, TaskOutcome)
        and result.details.get("p4_terminalized") is True
    )


def _p4_terminalization_matches_outcome(
    projection: JobProjection | None,
    *,
    attempt_id: UUID,
    result: Any,
) -> bool:
    if projection is None or not isinstance(result, TaskOutcome):
        return False
    expected_job_status = JobStatus.SUCCEEDED if result.status.successful else JobStatus.FAILED
    expected_attempt_status = AttemptStatus.SUCCEEDED if result.status.successful else AttemptStatus.FAILED
    return (
        projection.job.status is expected_job_status
        and projection.current_attempt.id == attempt_id
        and projection.current_attempt.status is expected_attempt_status
    )


def _raise_for_reported_failure(task_key: str, result: Any) -> None:
    if isinstance(result, TaskOutcome):
        if result.status.successful:
            return
        raise _TaskReportedFailure(result.message or f"task reported {result.status.value}")
    if not isinstance(result, Mapping):
        return
    status = str(result.get("status") or "").strip().lower()
    if result.get("skipped") is True or status in {TaskOutcomeStatus.SKIPPED.value, "not_applicable"}:
        return
    reported_failure = result.get("ok") is False or status in {
        TaskOutcomeStatus.FAILED.value,
        TaskOutcomeStatus.PARTIAL.value,
        "indeterminate",
    }
    if not reported_failure:
        return
    message = str(result.get("message") or f"task {task_key!r} reported an unsuccessful outcome")
    raise _TaskReportedFailure(message)


__all__ = [
    "RuntimeLegacyTaskResolver",
    "WorkerShutdownRequested",
    "WorkerRuntime",
    "close_worker_runtime",
    "get_worker_runtime",
    "request_worker_shutdown",
    "worker_shutdown_requested",
]
