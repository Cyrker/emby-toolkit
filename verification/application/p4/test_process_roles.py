from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest
from apscheduler.jobstores.base import JobLookupError
from celery.exceptions import Reject

from etk.domains.operations.jobs import JobSubmissionService
from etk.platform.jobs import OutboxClaim, OutboxEventRecord, OutboxStatus, PublishBatchResult
from etk.processes import coordinator
from etk.processes import worker as worker_module
from etk.processes.celery_app import (
    EXECUTE_INGESTION_MONITOR_TASK,
    EXECUTE_JOB_TASK,
    EXECUTE_P115_PLAYBACK_OPERATION_TASK,
    EXECUTE_P115_UPLOAD_OPERATION_TASK,
    create_celery_app,
)
from etk.processes.coordinator import (
    CeleryOutboxPublisher,
    CoordinatorLeadership,
    CoordinatorRuntime,
    PersistentScheduleManager,
)
from etk.processes.runtime_config import TaskRuntimeConfig
from etk.processes.worker import WorkerRuntime, WorkerShutdownRequested, _raise_for_reported_failure, _worker_data_root

REPO_ROOT = Path(__file__).resolve().parents[3]
EVENT_ID = UUID("00000000-0000-0000-0000-000000000010")
JOB_ID = UUID("00000000-0000-0000-0000-000000000011")
ATTEMPT_ID = UUID("00000000-0000-0000-0000-000000000012")
CLAIM_TOKEN = UUID("00000000-0000-0000-0000-000000000013")
NOW = datetime(2026, 8, 23, tzinfo=UTC)
RUNTIME_ENVIRONMENT = (
    "ETK_REDIS_URL",
    "ETK_TASK_QUEUE",
    "ETK_JOB_HEARTBEAT_SECONDS",
    "ETK_JOB_LEASE_SECONDS",
    "ETK_COORDINATOR_INTERVAL_SECONDS",
    "ETK_OUTBOX_BATCH_SIZE",
    "ETK_OUTBOX_CLAIM_TIMEOUT_SECONDS",
    "ETK_WORKER_CONCURRENCY",
    "ETK_WORKER_SHUTDOWN_TIMEOUT_SECONDS",
    "ETK_JOB_TERMINATION_GRACE_SECONDS",
    "ETK_COORDINATOR_SCHEDULES_ENABLED",
)


def test_worker_data_root_matches_the_application_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("APP_DATA_DIR", raising=False)
    monkeypatch.delenv("CONFIG_DIR", raising=False)

    assert _worker_data_root(SimpleNamespace()) == REPO_ROOT / "local_data"


class FakeSendApp:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def send_task(self, name: str, **options: Any) -> None:
        self.calls.append((name, options))


class LockBackend:
    def __init__(self) -> None:
        self.owner: int | None = None
        self.operations: list[tuple[int, str, tuple[int, int]]] = []

    def execute(self, connection_id: int, statement: str, parameters: tuple[int, int]) -> tuple[bool]:
        self.operations.append((connection_id, statement, parameters))
        if "pg_try_advisory_lock" in statement:
            if self.owner is None:
                self.owner = connection_id
                return (True,)
            return (self.owner == connection_id,)
        if "pg_advisory_unlock" in statement:
            released = self.owner == connection_id
            if released:
                self.owner = None
            return (released,)
        if "FROM pg_locks" in statement:
            return (self.owner == connection_id,)
        raise AssertionError(f"unexpected SQL: {statement}")


class FakeCursor:
    def __init__(self, row: tuple[bool]) -> None:
        self._row = row

    def fetchone(self) -> tuple[bool]:
        return self._row


class FakeLockConnection:
    def __init__(self, backend: LockBackend, connection_id: int) -> None:
        self.backend = backend
        self.connection_id = connection_id
        self.commits = 0

    def execute(self, statement: str, parameters: tuple[int, int]) -> FakeCursor:
        return FakeCursor(self.backend.execute(self.connection_id, statement, parameters))

    def commit(self) -> None:
        self.commits += 1


class FakeLease:
    def __init__(self, pool: FakeRawPool, connection: FakeLockConnection) -> None:
        self.pool = pool
        self.connection = connection
        self.exited = False

    def __enter__(self) -> FakeLockConnection:
        return self.connection

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        del exc_type, exc, traceback
        self.exited = True
        self.pool.exited_connection_ids.append(self.connection.connection_id)


class FakeRawPool:
    def __init__(self) -> None:
        self.backend = LockBackend()
        self.next_connection_id = 1
        self.leases: list[FakeLease] = []
        self.exited_connection_ids: list[int] = []

    def connection(self) -> FakeLease:
        connection = FakeLockConnection(self.backend, self.next_connection_id)
        self.next_connection_id += 1
        lease = FakeLease(self, connection)
        self.leases.append(lease)
        return lease


class FakeScheduler:
    running = False

    def __init__(self) -> None:
        self.jobs: list[dict[str, Any]] = []

    def remove_job(self, job_id: str) -> None:
        raise JobLookupError(job_id)

    def add_job(self, function: Any, *, trigger: Any, **options: Any) -> None:
        self.jobs.append({"function": function, "trigger": trigger, **options})


class RecordingCoordinatorStore:
    def __init__(self) -> None:
        self.enqueue_options: dict[str, Any] | None = None
        self.recover_options: dict[str, Any] | None = None
        self.requeue_options: dict[str, Any] | None = None
        self.publish_options: dict[str, Any] | None = None
        self.publisher: Any = None

    def enqueue_due_retries(self, **options: Any) -> None:
        self.enqueue_options = options

    def recover_stale_attempts(self, **options: Any) -> None:
        self.recover_options = options

    def requeue_stale_published(self, **options: Any) -> None:
        self.requeue_options = options

    def publish_outbox_batch(self, publisher: Any, **options: Any) -> PublishBatchResult:
        self.publisher = publisher
        self.publish_options = options
        return PublishBatchResult(claimed=0, published=0, failed=0, dead=0)


def _clean_runtime_environment(monkeypatch) -> None:
    for name in RUNTIME_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)


def _claim(payload: dict[str, Any] | None = None) -> OutboxClaim:
    event = OutboxEventRecord(
        id=EVENT_ID,
        event_key=f"job.dispatch:{ATTEMPT_ID}",
        topic="job.dispatch.requested",
        aggregate_type="job_attempt",
        aggregate_id=str(ATTEMPT_ID),
        payload_version=1,
        payload=payload
        or {
            "event_id": str(EVENT_ID),
            "job_id": str(JOB_ID),
            "attempt_id": str(ATTEMPT_ID),
        },
        status=OutboxStatus.PUBLISHING,
        available_at=NOW,
        claimed_by="fixture-coordinator",
        claim_token=CLAIM_TOKEN,
        claimed_at=NOW,
        published_at=None,
        processed_at=None,
        publish_attempts=1,
        max_publish_attempts=20,
        last_error=None,
        created_at=NOW,
        updated_at=NOW,
    )
    return OutboxClaim(event=event, claim_token=CLAIM_TOKEN)


def test_cli_help_exposes_every_process_role_without_materializing_runtime(tmp_path: Path) -> None:
    environment = os.environ.copy()
    environment.update(
        APP_DATA_DIR=str(tmp_path / "data"),
        HOME=str(tmp_path / "home"),
        PYTHONDONTWRITEBYTECODE="1",
    )
    commands = (
        "api",
        "worker",
        "coordinator",
        "worker-health",
        "coordinator-health",
        "proxy",
        "generate-nginx-config",
    )

    top = subprocess.run(
        [sys.executable, "main.py", "--help"],
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert top.returncode == 0, top.stderr
    assert "usage: etk" in top.stdout
    for command in commands:
        assert command in top.stdout
        result = subprocess.run(
            [sys.executable, "main.py", command, "--help"],
            cwd=REPO_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert f"usage: etk {command}" in result.stdout

    assert not (tmp_path / "data").exists()
    assert not (tmp_path / "home").exists()


def test_runtime_config_parses_process_environment(monkeypatch) -> None:
    _clean_runtime_environment(monkeypatch)
    values = {
        "ETK_REDIS_URL": "rediss://fixture.invalid:6380/4",
        "ETK_TASK_QUEUE": "fixture.serial",
        "ETK_JOB_HEARTBEAT_SECONDS": "7",
        "ETK_JOB_LEASE_SECONDS": "35",
        "ETK_COORDINATOR_INTERVAL_SECONDS": "0.25",
        "ETK_OUTBOX_BATCH_SIZE": "23",
        "ETK_OUTBOX_CLAIM_TIMEOUT_SECONDS": "44",
        "ETK_WORKER_CONCURRENCY": "2",
        "ETK_WORKER_SHUTDOWN_TIMEOUT_SECONDS": "19",
        "ETK_JOB_TERMINATION_GRACE_SECONDS": "2.5",
        "ETK_COORDINATOR_SCHEDULES_ENABLED": "off",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)

    assert TaskRuntimeConfig.from_environ() == TaskRuntimeConfig(
        broker_url="rediss://fixture.invalid:6380/4",
        queue_name="fixture.serial",
        heartbeat_seconds=7,
        lease_seconds=35,
        coordinator_interval_seconds=0.25,
        outbox_batch_size=23,
        outbox_claim_timeout_seconds=44,
        worker_concurrency=2,
        worker_shutdown_timeout_seconds=19,
        job_termination_grace_seconds=2.5,
        schedules_enabled=False,
    )


@pytest.mark.parametrize(
    ("environment", "message"),
    [
        ({"ETK_REDIS_URL": "https://fixture.invalid"}, "must use redis"),
        ({"ETK_TASK_QUEUE": "   "}, "must not be empty"),
        ({"ETK_JOB_HEARTBEAT_SECONDS": "zero"}, "must be an integer"),
        ({"ETK_OUTBOX_BATCH_SIZE": "0"}, "must be positive"),
        ({"ETK_COORDINATOR_INTERVAL_SECONDS": "nan"}, "must be positive"),
        ({"ETK_COORDINATOR_SCHEDULES_ENABLED": "sometimes"}, "must be a boolean"),
        (
            {"ETK_JOB_HEARTBEAT_SECONDS": "30", "ETK_JOB_LEASE_SECONDS": "30"},
            "lease must be longer",
        ),
    ],
)
def test_runtime_config_rejects_invalid_transport_and_limits(
    monkeypatch,
    environment: dict[str, str],
    message: str,
) -> None:
    _clean_runtime_environment(monkeypatch)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match=message):
        TaskRuntimeConfig.from_environ()


def test_celery_transport_is_json_only_resultless_and_single_prefetch() -> None:
    config = TaskRuntimeConfig(
        broker_url="redis://fixture.invalid:6379/7",
        queue_name="fixture.serial",
        heartbeat_seconds=5,
        lease_seconds=30,
    )
    app = create_celery_app(config)
    task = app.tasks[EXECUTE_JOB_TASK]

    assert app.conf.accept_content == ["json"]
    assert app.conf.task_serializer == "json"
    assert app.conf.result_backend is None
    assert app.conf.task_ignore_result is True
    assert app.conf.task_acks_late is True
    assert app.conf.task_reject_on_worker_lost is True
    assert app.conf.worker_prefetch_multiplier == 1
    assert app.conf.task_default_queue == "fixture.serial"
    assert [queue.name for queue in app.conf.task_queues] == ["fixture.serial"]
    assert task.serializer == "json"
    assert task.ignore_result is True
    assert task.acks_late is True
    assert task.reject_on_worker_lost is True


def test_worker_shutdown_request_is_sticky_without_taking_runtime_lock(monkeypatch) -> None:
    monkeypatch.setattr(worker_module, "_PROCESS_SHUTDOWN_REQUESTED", False)
    completed = Event()

    def request() -> None:
        worker_module.request_worker_shutdown()
        completed.set()

    requester = Thread(target=request, daemon=True)
    with worker_module._RUNTIME_LOCK:
        requester.start()
        assert completed.wait(0.5), "shutdown signal path blocked on the runtime lock"
    requester.join(1)

    assert not requester.is_alive()
    assert worker_module.worker_shutdown_requested()


def test_worker_startup_shutdown_race_does_not_claim_a_delivery(monkeypatch) -> None:
    monkeypatch.setattr(worker_module, "_PROCESS_SHUTDOWN_REQUESTED", False)
    runtime = WorkerRuntime(config=TaskRuntimeConfig(), terminate_process=lambda: None)
    entered_start = Event()
    release_start = Event()
    failures: list[BaseException] = []

    def blocked_start() -> None:
        entered_start.set()
        assert release_start.wait(2), "test did not release Worker startup"

    monkeypatch.setattr(runtime, "start", blocked_start)

    def execute() -> None:
        try:
            runtime.execute(event_id=str(EVENT_ID), job_id=str(JOB_ID), attempt_id=str(ATTEMPT_ID))
        except BaseException as exc:
            failures.append(exc)

    delivery = Thread(target=execute, daemon=True)
    delivery.start()
    assert entered_start.wait(1)
    worker_module.request_worker_shutdown()
    release_start.set()
    delivery.join(1)

    assert not delivery.is_alive()
    assert len(failures) == 1
    assert isinstance(failures[0], WorkerShutdownRequested)
    assert runtime._store is None


def test_legacy_compatibility_environment_cannot_restore_worker_profile(monkeypatch) -> None:
    import task_manager
    from etk.compatibility import background

    events: list[str] = []
    monkeypatch.setenv("ETK_ENABLE_LEGACY_COMPATIBILITY", "1")
    database = SimpleNamespace(
        unit_of_work=lambda: None,
        raw_pool=SimpleNamespace(connection=lambda: None),
        engine=object(),
    )
    container = SimpleNamespace(
        database=database,
        settings=SimpleNamespace(to_runtime_dict=lambda: {}),
        close=lambda: events.append("container.close"),
    )
    config_adapter = SimpleNamespace(load=lambda: SimpleNamespace(emby_api_timeout=30))

    task_manager.clear_persistent_submission_gateway()
    monkeypatch.setattr(worker_module, "_create_default_container", lambda _settings: (container, config_adapter))
    watchlist_services: list[Any] = []
    original_watchlist_factory = worker_module._native_watchlist_task_service

    def record_watchlist_service(**kwargs: Any) -> Any:
        service = original_watchlist_factory(**kwargs)
        watchlist_services.append(service)
        return service

    monkeypatch.setattr(worker_module, "_native_watchlist_task_service", record_watchlist_service)

    def fail_background_install(_gateway: object) -> None:
        raise RuntimeError("fixture background install failed")

    monkeypatch.setattr(
        background,
        "install_persistent_background_gateway",
        fail_background_install,
    )
    runtime = WorkerRuntime(config=TaskRuntimeConfig())

    runtime.start()

    assert task_manager._persistent_gateway is None
    assert len(watchlist_services) == 1
    assert type(watchlist_services[0]).__name__ == "NativeWatchlistTaskService"
    assert type(watchlist_services[0]._effects._intro).__name__ == "_DeferredWatchlistIntroGateway"
    state = cast(Any, runtime)
    assert type(state._ingestion_executor._preparation).__name__ == "NativeIngestionPreparationGateway"
    assert type(state._ingestion_executor._fingerprints).__name__ == "NativeIngestionFingerprintGateway"
    assert type(state._p115_upload_executor._provider).__name__ == "NativeP115UploadProvider"
    assert type(state._p115_upload_executor._projection).__name__ == "NativeP115UploadProjection"
    assert events == []

    runtime.close()

    assert task_manager._persistent_gateway is None
    assert events == ["container.close"]


def test_native_worker_start_wires_processor_free_execution_graph(monkeypatch) -> None:
    events: list[str] = []
    monkeypatch.delenv("ETK_ENABLE_LEGACY_COMPATIBILITY", raising=False)
    database = SimpleNamespace(
        unit_of_work=lambda: None,
        raw_pool=SimpleNamespace(connection=lambda: None),
        engine=object(),
    )
    container = SimpleNamespace(
        database=database,
        settings=SimpleNamespace(to_runtime_dict=lambda: {"profile": "native"}),
        close=lambda: events.append("container.close"),
    )
    config_adapter = SimpleNamespace(load=lambda: SimpleNamespace(emby_api_timeout=30))

    monkeypatch.setattr(worker_module, "_create_default_container", lambda _settings: (container, config_adapter))
    runtime = WorkerRuntime(config=TaskRuntimeConfig())

    runtime.start()

    state = cast(Any, runtime)
    assert worker_module.os.environ["ETK_PROCESS_ROLE"] == "worker"
    assert type(state._media_effects).__name__ == "PersistentMediaEffectExecutor"
    assert state._media_effects._delivery.effect_types == {
        "emby.chapters.update",
        "emby.cover.apply",
        "emby.library.paths_changed",
        "emby.event.image.update",
        "emby.event.metadata.update",
        "emby.image.manual_update",
        "emby.mediainfo.apply",
        "emby.studio-image.reconcile",
        "emby.tags.apply",
        "daily-theme.replenishment",
        "sharing.intro.export",
        "moviepilot.webhook.assistant",
        "moviepilot.webhook.transfer",
    }
    assert type(state._p5_media_tasks).__name__ == "P5MediaTaskRouter"
    assert state._p5_media_tasks._compatibility is None
    assert type(state._p5_media_tasks._restore_mediainfo).__name__ == "P115MediaInfoRestoreService"
    assert type(state._p5_media_tasks._restore_mediainfo._probe._direct_urls).__name__ == ("P115SdkDirectUrlGateway")
    assert type(state._p6_p115_tasks).__name__ == "P6P115TaskRouter"
    assert type(state._p6_p115_tasks._organize_record_tmdb_backfill).__name__ == (
        "P115OrganizeRecordTmdbBackfillService"
    )
    assert type(state._p6_p115_tasks._directory_sync).__name__ == "P115DirectorySyncService"
    assert type(state._p6_p115_tasks._directory_sync._directories).__name__ == ("HttpxP115DirectoryTreeGateway")
    assert type(state._p6_p115_tasks._strm_sync).__name__ == "P115StrmSyncService"
    assert type(state._p6_p115_tasks._strm_sync._remote).__name__ == "HttpxP115StrmSyncGateway"
    assert type(state._p6_p115_tasks._fingerprint_repair).__name__ == "P115FingerprintRepairService"
    assert type(state._p6_p115_tasks._fingerprint_repair._remote).__name__ == ("HttpxP115FingerprintRemoteGateway")
    assert type(state._p6_p115_tasks._intro_backfill).__name__ == "P115IntroBackfillTaskService"
    assert type(state._p6_p115_tasks._intro_backfill._gateway._source).__name__ == "PostgresP115IntroRuntimeSource"
    assert type(state._p115_intro_executor).__name__ == "P115IntroDetectionOperationExecutor"
    assert type(state._p6_p115_tasks._organize_execution).__name__ == "P115OrganizeExecutionService"
    assert type(state._p6_p115_tasks._organize_execution._mutations).__name__ == ("HttpxP115OrganizeMutationGateway")
    assert type(state._p6_p115_tasks._organize_reconciliation).__name__ == "P115OrganizeExecutionService"
    assert type(state._p6_p115_tasks._organize_reconciliation._mutations).__name__ == (
        "HttpxP115OrganizeReadbackGateway"
    )
    assert type(state._p6_ingestion_tasks).__name__ == "P6IngestionTaskRouter"
    assert type(state._ingestion_executor).__name__ == "IngestionMonitorExecutor"
    assert type(state._ingestion_executor._preparation).__name__ == "NativeIngestionPreparationGateway"
    assert type(state._ingestion_executor._fingerprints).__name__ == "NativeIngestionFingerprintGateway"
    assert type(state._ingestion_binding._delivery).__name__ == "NativeIngestionBindingDelivery"
    assert type(state._p115_upload_executor).__name__ == "P115UploadOperationExecutor"
    assert type(state._p115_upload_executor._provider).__name__ == "NativeP115UploadProvider"
    assert type(state._p115_upload_executor._projection).__name__ == "NativeP115UploadProjection"
    assert type(state._p115_upload_executor._projection._repository).__name__ == (
        "SqlAlchemyP115UploadProjectionRepository"
    )
    native_handlers = state._native_operations._handlers
    assert {
        "identity.post-authorization",
        "system.restart",
        "p115.deep-delete.execute",
        "media.reprocess-item",
        "media.cast-update",
        "media.metadata-event-sync",
        "p115.temp-cleanup",
        "media.plugin-update-check",
    } <= set(native_handlers)
    deep_delete = native_handlers["p115.deep-delete.execute"]
    assert type(deep_delete).__name__ == "NativeP115DeepDeleteOperation"
    assert type(deep_delete._service._p115).__name__ == "NativeP115DeleteReadbackGateway"
    assert type(deep_delete._service._upload_sources).__name__ == "NativeP115UploadSourceCleanupGateway"
    assert type(deep_delete._service._media).__name__ == "NativeP115MediaCleanupReceiptGateway"
    assert type(deep_delete._service._shared).__name__ == "NativeP115SharedCleanupGateway"
    assert type(state._p7_subscription_tasks).__name__ == "P7SubscriptionTaskRouter"
    assert not hasattr(state._p7_subscription_tasks, "_compatibility")
    assert type(state._p7_subscription_tasks._assistant).__name__ == "SubscribeAssistantMaintenanceService"
    assert type(state._p7_subscription_tasks._assistant._gateway).__name__ == (
        "NativeSubscribeAssistantMaintenanceGateway"
    )
    assert type(state._p7_subscription_tasks._washing_recalculation).__name__ == (
        "P115WashingPriorityRecalculationService"
    )
    assert type(state._p7_subscription_tasks._old_season_backfill).__name__ == "OldSeasonBackfillService"
    assert type(state._p7_subscription_tasks._auto_subscribe).__name__ == "AutoSubscribeService"
    assert type(state._p7_subscription_tasks._auto_subscribe._sources).__name__ == (
        "PersistentOrderedAutoSubscriptionSources"
    )
    assert type(state._p7_subscription_tasks._auto_subscribe._sources._telegram).__name__ == (
        "DurableTelegramUserBotAdapter"
    )
    assert type(state._p7_subscription_tasks._auto_subscribe._repository).__name__ == (
        "SqlAlchemyAutoSubscriptionRepository"
    )
    assert type(state._p7_subscription_tasks._watchlist_tasks).__name__ == "NativeWatchlistTaskService"
    assert type(state._p7_subscription_tasks._watchlist_tasks._effects).__name__ == "NativeWatchlistTaskEffects"
    assert type(state._p7_subscription_tasks._watchlist_tasks._effects._intro).__name__ == (
        "_DeferredWatchlistIntroGateway"
    )
    assert type(state._p8_effect_tasks).__name__ == "P8EffectTaskRouter"
    assert state._p8_effect_tasks._compatibility is None
    assert type(state._p8_effect_tasks._cleanup_scan).__name__ == "CleanupScanService"
    assert type(state._p8_effect_tasks._daily_theme).__name__ == "DailyThemeService"
    assert type(state._p8_effect_tasks._daily_theme._repository).__name__ == ("SqlAlchemyDailyThemeRepository")
    assert type(state._p8_effect_tasks._cover_tasks).__name__ == "NativeCoverTaskService"
    assert type(state._p8_effect_tasks._cover_tasks._renderer).__name__ == "ConfiguredCoverRenderer"
    assert type(state._p8_effect_tasks._cover_tasks._effects).__name__ == "PersistentCoverTaskEffects"
    assert type(state._p9_sharing_tasks).__name__ == "P9SharingTaskRouter"
    assert state._p9_sharing_tasks._compatibility is None
    assert type(state._subscription_executor).__name__ == "SubscriptionOperationExecutor"
    assert type(state._identity_executor).__name__ == "IdentityOperationExecutor"
    assert type(state._database_maintenance_executor).__name__ == "DatabaseMaintenanceExecutor"
    assert events == []

    runtime.close()

    assert events == ["container.close"]
    assert state._media_effects is None
    assert state._p5_media_tasks._compatibility is None
    assert state._p5_media_tasks._restore_mediainfo is None
    assert state._p6_p115_tasks is None
    assert state._p6_ingestion_tasks is None
    assert state._ingestion_executor is None
    assert state._ingestion_binding is None
    assert state._ingestion_ingress is None
    assert state._p115_upload_executor is None
    assert state._native_operations is None
    assert state._p7_subscription_tasks is None
    assert state._p8_effect_tasks is None
    assert state._p9_sharing_tasks is None
    assert state._subscription_executor is None
    assert state._identity_executor is None
    assert state._database_maintenance_executor is None


def test_native_worker_start_failure_closes_container(monkeypatch) -> None:
    events: list[str] = []
    monkeypatch.delenv("ETK_ENABLE_LEGACY_COMPATIBILITY", raising=False)
    database = SimpleNamespace(
        unit_of_work=lambda: None,
        raw_pool=SimpleNamespace(connection=lambda: None),
        engine=object(),
    )
    container = SimpleNamespace(
        database=database,
        settings=SimpleNamespace(to_runtime_dict=lambda: {}),
        close=lambda: events.append("container.close"),
    )
    config_adapter = SimpleNamespace(load=lambda: SimpleNamespace(emby_api_timeout=30))

    monkeypatch.setattr(worker_module, "_create_default_container", lambda _settings: (container, config_adapter))
    monkeypatch.setattr(
        worker_module,
        "PostgresJobStore",
        lambda *_args, **_options: (_ for _ in ()).throw(RuntimeError("fixture native setup failed")),
    )

    with pytest.raises(RuntimeError, match="fixture native setup failed"):
        WorkerRuntime(config=TaskRuntimeConfig()).start()

    assert events == ["container.close"]


def test_celery_rejects_and_requeues_delivery_after_sticky_shutdown(monkeypatch) -> None:
    monkeypatch.setattr(worker_module, "_PROCESS_SHUTDOWN_REQUESTED", True)
    task = create_celery_app(TaskRuntimeConfig()).tasks[EXECUTE_JOB_TASK]

    with pytest.raises(Reject) as exc_info:
        task.run(event_id=str(EVENT_ID), job_id=str(JOB_ID), attempt_id=str(ATTEMPT_ID))

    assert exc_info.value.requeue is True


def test_celery_forwards_ingestion_item_to_the_worker_runtime(monkeypatch) -> None:
    calls: list[str] = []
    runtime = SimpleNamespace(execute_ingestion_monitor=lambda *, item_id: calls.append(item_id))
    monkeypatch.setattr(worker_module, "_PROCESS_SHUTDOWN_REQUESTED", False)
    monkeypatch.setattr(worker_module, "get_worker_runtime", lambda: runtime)
    task = create_celery_app(TaskRuntimeConfig()).tasks[EXECUTE_INGESTION_MONITOR_TASK]

    task.run(item_id=str(EVENT_ID))

    assert calls == [str(EVENT_ID)]


def test_celery_forwards_p115_upload_operation_to_the_worker_runtime(monkeypatch) -> None:
    calls: list[str] = []
    runtime = SimpleNamespace(execute_p115_upload_operation=lambda *, operation_id: calls.append(operation_id))
    monkeypatch.setattr(worker_module, "_PROCESS_SHUTDOWN_REQUESTED", False)
    monkeypatch.setattr(worker_module, "get_worker_runtime", lambda: runtime)
    task = create_celery_app(TaskRuntimeConfig()).tasks[EXECUTE_P115_UPLOAD_OPERATION_TASK]

    task.run(operation_id=str(EVENT_ID))

    assert calls == [str(EVENT_ID)]


def test_celery_registers_and_forwards_p115_playback_operation(monkeypatch) -> None:
    calls: list[str] = []
    runtime = SimpleNamespace(execute_p115_playback_operation=lambda *, operation_id: calls.append(operation_id))
    monkeypatch.setattr(worker_module, "_PROCESS_SHUTDOWN_REQUESTED", False)
    monkeypatch.setattr(worker_module, "get_worker_runtime", lambda: runtime)
    config = TaskRuntimeConfig(queue_name="fixture.serial")
    app = create_celery_app(config)
    task = app.tasks[EXECUTE_P115_PLAYBACK_OPERATION_TASK]

    task.run(operation_id=str(EVENT_ID))

    assert EXECUTE_P115_PLAYBACK_OPERATION_TASK == "etk.p115.playback.execute"
    assert app.conf.task_routes[EXECUTE_P115_PLAYBACK_OPERATION_TASK] == {"queue": "fixture.serial"}
    assert calls == [str(EVENT_ID)]


def test_celery_requeues_p115_upload_delivery_after_sticky_shutdown(monkeypatch) -> None:
    monkeypatch.setattr(worker_module, "_PROCESS_SHUTDOWN_REQUESTED", True)
    task = create_celery_app(TaskRuntimeConfig()).tasks[EXECUTE_P115_UPLOAD_OPERATION_TASK]

    with pytest.raises(Reject) as exc_info:
        task.run(operation_id=str(EVENT_ID))

    assert exc_info.value.requeue is True


def test_celery_requeues_p115_playback_delivery_after_sticky_shutdown(monkeypatch) -> None:
    monkeypatch.setattr(worker_module, "_PROCESS_SHUTDOWN_REQUESTED", True)
    task = create_celery_app(TaskRuntimeConfig()).tasks[EXECUTE_P115_PLAYBACK_OPERATION_TASK]

    with pytest.raises(Reject) as exc_info:
        task.run(operation_id=str(EVENT_ID))

    assert exc_info.value.requeue is True


@pytest.mark.parametrize("fail", [False, True])
def test_worker_playback_execution_restores_active_counter_and_idle(monkeypatch, fail: bool) -> None:
    entered = Event()
    release = Event()
    completed: list[UUID] = []
    failures: list[BaseException] = []

    class PlaybackExecutor:
        def execute(self, operation_id: UUID) -> None:
            entered.set()
            assert release.wait(2), "test did not release playback execution"
            if fail:
                raise RuntimeError("fixture playback failure")
            completed.append(operation_id)

    monkeypatch.setattr(worker_module, "_PROCESS_SHUTDOWN_REQUESTED", False)
    runtime = WorkerRuntime(config=TaskRuntimeConfig())
    cast(Any, runtime)._started = True
    cast(Any, runtime)._p115_playback_executor = PlaybackExecutor()

    def execute() -> None:
        try:
            runtime.execute_p115_playback_operation(operation_id=str(EVENT_ID))
        except BaseException as exc:
            failures.append(exc)

    delivery = Thread(target=execute, daemon=True)
    delivery.start()
    assert entered.wait(1)
    assert cast(Any, runtime)._active_p115_playback_executions == 1
    assert not cast(Any, runtime)._idle.is_set()

    release.set()
    delivery.join(1)

    assert not delivery.is_alive()
    assert cast(Any, runtime)._active_p115_playback_executions == 0
    assert cast(Any, runtime)._idle.is_set()
    if fail:
        assert len(failures) == 1
        assert isinstance(failures[0], RuntimeError)
        assert completed == []
    else:
        assert failures == []
        assert completed == [EVENT_ID]


def test_worker_playback_completion_does_not_mark_idle_while_another_execution_is_active(monkeypatch) -> None:
    monkeypatch.setattr(worker_module, "_PROCESS_SHUTDOWN_REQUESTED", False)
    runtime = WorkerRuntime(config=TaskRuntimeConfig())
    cast(Any, runtime)._started = True
    cast(Any, runtime)._p115_playback_executor = SimpleNamespace(execute=lambda _operation_id: None)
    cast(Any, runtime)._active_p115_upload_executions = 1
    cast(Any, runtime)._idle.clear()

    runtime.execute_p115_playback_operation(operation_id=str(EVENT_ID))

    assert cast(Any, runtime)._active_p115_playback_executions == 0
    assert not cast(Any, runtime)._idle.is_set()


def test_worker_rejects_playback_before_start_after_shutdown_request(monkeypatch) -> None:
    monkeypatch.setattr(worker_module, "_PROCESS_SHUTDOWN_REQUESTED", False)
    runtime = WorkerRuntime(config=TaskRuntimeConfig())
    runtime.request_shutdown()

    with pytest.raises(WorkerShutdownRequested, match="shutdown is in progress"):
        runtime.execute_p115_playback_operation(operation_id=str(EVENT_ID))

    assert cast(Any, runtime)._active_p115_playback_executions == 0
    assert cast(Any, runtime)._p115_playback_executor is None


def test_worker_shutdown_clears_native_p115_upload_executor() -> None:
    runtime = WorkerRuntime(config=TaskRuntimeConfig())
    cast(Any, runtime)._started = True
    cast(Any, runtime)._p115_upload_executor = object()

    runtime.close()

    assert cast(Any, runtime)._p115_upload_executor is None
    assert cast(Any, runtime)._started is False


def test_worker_shutdown_clears_native_p115_playback_executor() -> None:
    runtime = WorkerRuntime(config=TaskRuntimeConfig())
    cast(Any, runtime)._started = True
    cast(Any, runtime)._p115_playback_executor = object()

    runtime.close()

    assert cast(Any, runtime)._p115_playback_executor is None
    assert cast(Any, runtime)._started is False


def test_coordinator_leader_starts_only_native_schedules_and_listener(monkeypatch) -> None:
    events: list[str] = []

    class Schedules:
        def __init__(self, _service: object) -> None:
            pass

        def start(self) -> None:
            events.append("schedules.start")

        def stop(self) -> None:
            events.append("schedules.stop")

    manager = SimpleNamespace(
        apply_listener_state=lambda: events.append("listener.apply"),
        stop_listener=lambda: events.append("listener.stop"),
    )
    runtime = CoordinatorRuntime.__new__(CoordinatorRuntime)
    runtime._service = cast(Any, object())
    runtime._schedules = None
    cast(Any, runtime)._native_telegram_search_manager = manager
    runtime._refresh_settings = lambda *, force=False: events.append(f"refresh:{force}")  # type: ignore[method-assign]

    monkeypatch.setattr(coordinator, "PersistentScheduleManager", Schedules)

    runtime._become_native_leader()
    runtime._shutdown_native_leader()

    assert events == [
        "schedules.start",
        "refresh:True",
        "listener.apply",
        "listener.stop",
        "schedules.stop",
    ]


def test_system_update_unsuccessful_result_is_not_marked_as_worker_success() -> None:
    with pytest.raises(RuntimeError, match="fixture daemon unavailable"):
        _raise_for_reported_failure(
            "system-auto-update",
            {"ok": False, "status": "error", "message": "fixture daemon unavailable"},
        )

    _raise_for_reported_failure("system-auto-update", {"ok": True, "status": "up_to_date"})
    with pytest.raises(RuntimeError, match="domain-specific result"):
        _raise_for_reported_failure("populate-metadata", {"ok": False, "message": "domain-specific result"})


def test_worker_refreshes_native_settings_snapshot_at_the_next_root_job_boundary() -> None:
    class SettingsSnapshot:
        def __init__(self, values: dict[str, Any]) -> None:
            self.values = values

        def to_runtime_dict(self) -> dict[str, Any]:
            return dict(self.values)

    class ConfigAdapter:
        def load(self) -> SettingsSnapshot:
            return SettingsSnapshot({"task_chain_sequence": ["populate-metadata"]})

    runtime = WorkerRuntime(config=TaskRuntimeConfig())
    cast(Any, runtime)._config_adapter = ConfigAdapter()
    cast(Any, runtime)._last_settings = {"task_chain_sequence": []}

    runtime._refresh_settings()
    runtime._refresh_settings()

    assert cast(Any, runtime)._last_settings == {"task_chain_sequence": ["populate-metadata"]}


def test_coordinator_runtime_context_reads_only_native_settings_owners(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values: dict[str, Any] = {
        "shared_resource_config": {"p115_shared_resource_enabled": True},
        "p115_temp_dir_config": {"cleanup_cron": "15 */2 * * *"},
        "tg_userbot_config": {"enabled": True},
        "p115_upload_monitor_config": {
            "enabled": True,
            "mappings": [
                {
                    "local_dir": "/fixture/upload",
                    "target_cid": "123",
                }
            ],
        },
        "emby_server_id_cache": "fixture-server",
    }
    loads: list[tuple[str, ...]] = []
    unit_of_work = object()

    class Repository:
        def __init__(self, candidate: object) -> None:
            assert candidate is unit_of_work

        def load_many(self, keys: tuple[str, ...]) -> dict[str, Any]:
            loads.append(keys)
            return {key: values.get(key) for key in keys}

    runtime = CoordinatorRuntime(config=TaskRuntimeConfig())
    runtime._container = SimpleNamespace(database=SimpleNamespace(unit_of_work=unit_of_work))
    runtime._config_adapter = SimpleNamespace(
        load=lambda: SimpleNamespace(monitor_extensions=[".mkv", ".srt"]),
    )
    monkeypatch.setattr(coordinator, "SqlAlchemyAppSettingsRepository", Repository)

    context = runtime._load_runtime_context()

    assert context["_shared_resource_enabled"] is True
    assert context["_emby_server_id"] == "fixture-server"
    assert context["_p115_temp_cleanup_cron"] == "15 */2 * * *"
    assert context["_tg_userbot_config"] == {"enabled": True}
    assert len(context["_p115_upload_monitor_revision"]) == 64
    assert loads == [
        (
            "shared_resource_config",
            "p115_temp_dir_config",
            "tg_userbot_config",
            "p115_upload_monitor_config",
            "emby_server_id_cache",
        )
    ]


def test_coordinator_cron_normalizes_legacy_day_of_week_before_scheduling() -> None:
    assert coordinator._fix_cron_dow("0 6 * * 1-5") == "0 6 * * mon-fri"
    assert coordinator._fix_cron_dow("30 7 * * 0,7") == "30 7 * * sun,sun"
    assert coordinator._fix_cron_dow("invalid cron") == "invalid cron"

    manager = PersistentScheduleManager(cast(JobSubmissionService, object()))
    scheduler = FakeScheduler()
    cast(Any, manager)._scheduler = scheduler
    changed = manager.sync(
        {
            "task_chain_enabled": True,
            "task_chain_cron": "0 6 * * 1-5",
            "task_chain_sequence": ["populate-metadata"],
            "task_chain_low_freq_enabled": False,
            "_shared_resource_enabled": False,
            "_p115_temp_cleanup_cron": "15 */2 * * 0",
        },
        schedules_enabled=True,
    )

    assert changed
    high = next(job for job in scheduler.jobs if job["id"] == "high_freq_task_chain_job")
    assert str(high["trigger"].fields[4]) == "mon-fri"
    assert {job["id"] for job in scheduler.jobs} == {
        "high_freq_task_chain_job",
        "daily_theme_job",
        "play_pool_daily_speedtest_job",
        "p115_temp_dir_cleanup_job",
        "etk_plugin_update_startup_job",
    }
    cleanup = next(job for job in scheduler.jobs if job["id"] == "p115_temp_dir_cleanup_job")
    assert str(cleanup["trigger"].fields[4]) == "sun"


def test_coordinator_schedules_one_processor_free_plan() -> None:
    manager = PersistentScheduleManager(cast(JobSubmissionService, object()))
    scheduler = FakeScheduler()
    cast(Any, manager)._scheduler = scheduler

    assert manager.sync(
        {
            "task_chain_enabled": True,
            "task_chain_cron": "0 6 * * *",
            "task_chain_sequence": ["populate-metadata"],
            "task_chain_low_freq_enabled": True,
            "task_chain_low_freq_cron": "0 7 * * *",
            "task_chain_low_freq_sequence": ["check-watchlist"],
            "_shared_resource_enabled": True,
            "_emby_server_id": "fixture-server",
            "_p115_temp_cleanup_cron": "15 */2 * * *",
        },
        schedules_enabled=True,
    )

    assert {job["id"] for job in scheduler.jobs} == {
        "high_freq_task_chain_job",
        "low_freq_task_chain_job",
        "daily_theme_job",
        "shared_share_status_sync_job",
        "play_pool_daily_speedtest_job",
        "p115_temp_dir_cleanup_job",
        "etk_plugin_update_startup_job",
    }
    task_keys = {job["kwargs"]["task_key"] for job in scheduler.jobs if "task_key" in job["kwargs"]}
    assert task_keys == {
        "task-chain-high-freq",
        "task-chain-low-freq",
        "update-daily-theme",
        "shared-share-status-sync",
        "play-pool-daily-speedtest",
    }
    operation_keys = {job["kwargs"]["operation_key"] for job in scheduler.jobs if "operation_key" in job["kwargs"]}
    assert operation_keys == {
        "p115.temp-cleanup",
        "media.plugin-update-check",
    }


def test_coordinator_submits_scheduled_operations_by_native_key() -> None:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    service = SimpleNamespace(
        is_busy=lambda: False,
        submit_native_operation=lambda *args, **kwargs: calls.append((args, kwargs)),
    )
    manager = PersistentScheduleManager(cast(Any, service))

    manager._submit_operation(
        operation_key="p115.temp-cleanup",
        name="115播放临时目录清理",
        schedule_id="p115_temp_dir_cleanup_job",
        parameters={"older_than_hours": 3},
    )

    assert len(calls) == 1
    args, options = calls[0]
    assert args == ("p115.temp-cleanup", {"older_than_hours": 3})
    assert options["display_name"] == "115播放临时目录清理"
    assert options["trigger_type"] == "schedule"
    assert options["idempotency_scope"] == "schedule:p115_temp_dir_cleanup_job"
    assert options["max_attempts"] == 1


def test_schedule_sync_validates_the_full_plan_before_replacing_jobs() -> None:
    manager = PersistentScheduleManager(cast(JobSubmissionService, object()))
    scheduler = FakeScheduler()
    cast(Any, manager)._scheduler = scheduler
    values = {
        "task_chain_enabled": True,
        "task_chain_cron": "0 6 * * 1-5",
        "task_chain_sequence": ["populate-metadata"],
        "task_chain_low_freq_enabled": False,
        "_shared_resource_enabled": False,
    }

    assert manager.sync(values, schedules_enabled=True)
    before = list(scheduler.jobs)
    invalid = dict(values, task_chain_cron="not-a-cron")

    with pytest.raises(ValueError):
        manager.sync(invalid, schedules_enabled=True)

    assert scheduler.jobs == before


def test_two_coordinator_leadership_ports_hold_only_one_session_lock() -> None:
    pool = FakeRawPool()
    first = CoordinatorLeadership(pool)
    second = CoordinatorLeadership(pool)

    assert first.try_acquire()
    assert first.try_acquire()
    assert first.validate()
    assert not second.try_acquire()
    assert first.acquired
    assert not second.acquired
    assert pool.backend.owner == 1
    assert pool.exited_connection_ids == [2]

    first.release()
    assert pool.backend.owner is None
    assert second.try_acquire()
    assert pool.backend.owner == 3
    second.release()

    assert pool.backend.owner is None
    parameters = {operation[2] for operation in pool.backend.operations}
    assert len(parameters) == 1
    assert all(
        "pg_try_advisory_lock" in operation[1]
        or "pg_advisory_unlock" in operation[1]
        or "FROM pg_locks" in operation[1]
        for operation in pool.backend.operations
    )


def test_coordinator_discards_a_session_that_no_longer_owns_the_lock() -> None:
    pool = FakeRawPool()
    first = CoordinatorLeadership(pool)
    second = CoordinatorLeadership(pool)

    assert first.try_acquire()
    pool.backend.owner = None

    assert not first.validate()
    assert not first.acquired
    assert pool.exited_connection_ids == [1]
    assert second.try_acquire()


def test_outbox_publisher_uses_attempt_id_as_deterministic_celery_task_id(monkeypatch) -> None:
    fake_app = FakeSendApp()
    monkeypatch.setattr(coordinator, "create_celery_app", lambda _config: fake_app)
    config = TaskRuntimeConfig(queue_name="fixture.serial")
    publisher = CeleryOutboxPublisher(config)

    publisher.publish(_claim())

    assert fake_app.calls == [
        (
            EXECUTE_JOB_TASK,
            {
                "kwargs": {
                    "event_id": str(EVENT_ID),
                    "job_id": str(JOB_ID),
                    "attempt_id": str(ATTEMPT_ID),
                },
                "task_id": str(ATTEMPT_ID),
                "queue": "fixture.serial",
                "routing_key": "fixture.serial",
                "serializer": "json",
            },
        )
    ]

    media_claim = _claim()
    media_claim = replace(
        media_claim,
        event=replace(
            media_claim.event,
            topic="media.effect.requested",
            aggregate_type="media_effect",
        ),
    )
    with pytest.raises(ValueError, match="only supports job.dispatch.requested"):
        publisher.publish(media_claim)

    incomplete = _claim()
    incomplete = replace(incomplete, event=replace(incomplete.event, payload={"event_id": str(EVENT_ID)}))
    with pytest.raises(ValueError, match="payload is incomplete"):
        publisher.publish(incomplete)

    assert len(fake_app.calls) == 1


def test_coordinator_tick_filters_recovery_and_publish_to_job_dispatch_topic(monkeypatch) -> None:
    store = RecordingCoordinatorStore()
    runtime = CoordinatorRuntime.__new__(CoordinatorRuntime)
    runtime.config = TaskRuntimeConfig(
        lease_seconds=30,
        outbox_batch_size=7,
        outbox_claim_timeout_seconds=11,
    )
    runtime._store = cast(Any, store)
    runtime._publisher = cast(Any, object())
    runtime._publisher_id = "fixture-coordinator"
    monkeypatch.setattr(runtime, "_refresh_settings", lambda: None)
    monkeypatch.setattr(runtime, "_process_native_telegram_search_commands", lambda: None)

    runtime._tick()

    assert store.enqueue_options == {"limit": 7}
    assert store.recover_options == {"limit": 7, "retry_delay_seconds": 1}
    assert store.requeue_options == {
        "limit": 7,
        "processed_timeout_seconds": 60,
        "topics": ("job.dispatch.requested",),
    }
    assert store.publish_options == {
        "publisher_id": "fixture-coordinator",
        "limit": 7,
        "claim_timeout_seconds": 11,
        "failure_backoff_seconds": 2,
        "topics": ("job.dispatch.requested",),
    }
    assert store.publisher is runtime._publisher


def test_native_coordinator_tick_consumes_telegram_search_commands(monkeypatch) -> None:
    store = RecordingCoordinatorStore()
    runtime = CoordinatorRuntime.__new__(CoordinatorRuntime)
    runtime.config = TaskRuntimeConfig(
        lease_seconds=30,
        outbox_batch_size=7,
        outbox_claim_timeout_seconds=11,
    )
    runtime._store = cast(Any, store)
    runtime._publisher = cast(Any, object())
    runtime._publisher_id = "fixture-native-coordinator"
    runtime._config_adapter = cast(Any, SimpleNamespace(load=lambda: SimpleNamespace()))
    events: list[str] = []
    monkeypatch.setattr(runtime, "_refresh_settings", lambda: None)
    monkeypatch.setattr(
        runtime,
        "_process_native_telegram_search_commands",
        lambda: events.append("telegram-search"),
    )

    runtime._tick()

    assert events == ["telegram-search"]
    assert store.publish_options is not None
    assert store.publish_options["topics"] == ("job.dispatch.requested",)
