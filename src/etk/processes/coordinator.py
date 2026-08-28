from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import socket
import sys
import threading
import time
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from etk.bootstrap.app_factory import _create_default_container
from etk.bootstrap.runtime_supervisor import RuntimeRole, RuntimeSupervisor
from etk.compatibility.ingestion_monitor import (
    clear_persistent_ingestion_ingress,
    install_persistent_ingestion_ingress,
)
from etk.compatibility.persistent_tasks import PersistentTaskManagerGateway
from etk.compatibility.scheduled_operations import (
    check_etk_plugin_update,
    cleanup_p115_temp_videos,
    verify_pro_status,
)
from etk.compatibility.upload_monitor import (
    clear_persistent_upload_monitor_ingress,
    install_persistent_upload_monitor_ingress,
)
from etk.domains.collection.operation_repository import PostgresCollectionOperationRepository
from etk.domains.identity.operation_repository import PostgresIdentityOperationRepository
from etk.domains.ingestion import PostgresIngestionMonitorLedger
from etk.domains.media.effects import PostgresMediaEffectLedger
from etk.domains.operations.database_maintenance import PostgresDatabaseMaintenanceRepository
from etk.domains.operations.jobs import JobSubmissionService, TaskBusyError
from etk.domains.p115.intro_detection_repository import PostgresP115IntroDetectionRepository
from etk.domains.p115.playback_repository import PostgresP115PlaybackRepository
from etk.domains.p115.upload_monitor import (
    P115UploadMonitorConfigService,
    P115UploadMonitorIngressService,
    normalize_upload_monitor_config,
    upload_monitor_config_revision,
)
from etk.domains.p115.upload_repository import PostgresP115UploadMonitorRepository
from etk.domains.subscription.cleanup_runtime_repository import SqlAlchemyCleanupExecutionRepository
from etk.domains.subscription.operation_repository import PostgresSubscriptionOperationRepository
from etk.domains.subscription.runtime_repository import PostgresSubscriptionRuntimeRepository
from etk.platform.jobs import OutboxClaim, PostgresJobStore
from etk.platform.tasks import TaskRegistry

from .celery_app import EXECUTE_JOB_TASK, create_celery_app
from .p5_media_effects import CeleryMediaEffectPublisher, MediaEffectOutboxDispatcher
from .p6_ingestion import (
    CeleryIngestionMonitorPublisher,
    IngestionMonitorCoordinator,
    PersistentIngestionIngressGateway,
)
from .p6_intro_detection import CeleryP115IntroDetectionPublisher, P115IntroDetectionCoordinator
from .p6_playback import CeleryP115PlaybackPublisher, P115PlaybackCoordinator
from .p6_upload_monitor import CeleryP115UploadPublisher, P115UploadMonitorCoordinator
from .p7_subscription import CelerySubscriptionOperationPublisher, SubscriptionOperationCoordinator
from .p8_collection_operations import (
    CeleryCollectionOperationPublisher,
    CollectionOperationCoordinator,
)
from .p8_database_maintenance import (
    CeleryDatabaseMaintenancePublisher,
    DatabaseMaintenanceCoordinator,
)
from .p8_identity import CeleryIdentityOperationPublisher, IdentityOperationCoordinator
from .runtime_config import TaskRuntimeConfig

logger = logging.getLogger(__name__)

_COORDINATOR_LOCK_NAMESPACE = 0x45544B50
_COORDINATOR_LOCK_KEY = 4
_HIGH_JOB = "high_freq_task_chain_job"
_LOW_JOB = "low_freq_task_chain_job"
_DAILY_THEME_JOB = "daily_theme_job"
_SHARED_SYNC_JOB = "shared_share_status_sync_job"
_PLAY_POOL_JOB = "play_pool_daily_speedtest_job"
_P115_TEMP_CLEANUP_JOB = "p115_temp_dir_cleanup_job"
_PRO_STATUS_JOB = "pro_status_check_job"
_PLUGIN_UPDATE_JOB = "etk_plugin_update_startup_job"
_JOB_DISPATCH_TOPIC = "job.dispatch.requested"
_JOB_DISPATCH_TOPICS = (_JOB_DISPATCH_TOPIC,)
_MANAGED_SCHEDULE_IDS = frozenset(
    {
        _HIGH_JOB,
        _LOW_JOB,
        _DAILY_THEME_JOB,
        _SHARED_SYNC_JOB,
        _PLAY_POOL_JOB,
        _P115_TEMP_CLEANUP_JOB,
        _PRO_STATUS_JOB,
        _PLUGIN_UPDATE_JOB,
    }
)
_SCHEDULED_OPERATIONS: dict[str, Callable[..., Any]] = {
    "check-etk-plugin-update": check_etk_plugin_update,
    "cleanup-p115-temp-videos": cleanup_p115_temp_videos,
    "verify-pro-status": verify_pro_status,
}


class CeleryOutboxPublisher:
    def __init__(self, config: TaskRuntimeConfig) -> None:
        self._config = config
        self._app = create_celery_app(config)

    def publish(self, claim: OutboxClaim) -> None:
        if claim.event.topic != _JOB_DISPATCH_TOPIC or claim.event.aggregate_type != "job_attempt":
            raise ValueError("Celery outbox publisher only supports job.dispatch.requested job_attempt events")
        payload = claim.event.payload
        event_id = str(payload.get("event_id") or "")
        job_id = str(payload.get("job_id") or "")
        attempt_id = str(payload.get("attempt_id") or "")
        if not event_id or not job_id or not attempt_id:
            raise ValueError("job dispatch outbox payload is incomplete")
        self._app.send_task(
            EXECUTE_JOB_TASK,
            kwargs={"event_id": event_id, "job_id": job_id, "attempt_id": attempt_id},
            task_id=attempt_id,
            queue=self._config.queue_name,
            routing_key=self._config.queue_name,
            serializer="json",
        )


class CoordinatorLeadership:
    """Session-scoped PostgreSQL advisory leadership using one dedicated lease."""

    def __init__(self, raw_pool: Any) -> None:
        self._raw_pool = raw_pool
        self._lease: AbstractContextManager[Any] | None = None
        self._connection: Any = None
        self.acquired = False

    def try_acquire(self) -> bool:
        if self.acquired:
            return True
        lease = self._raw_pool.connection()
        connection = lease.__enter__()
        try:
            row = connection.execute(
                "SELECT pg_try_advisory_lock(%s, %s)",
                (_COORDINATOR_LOCK_NAMESPACE, _COORDINATOR_LOCK_KEY),
            ).fetchone()
            connection.commit()
            acquired = bool(row and row[0])
        except BaseException:
            lease.__exit__(*sys.exc_info())
            raise
        if not acquired:
            lease.__exit__(None, None, None)
            return False
        self._lease = lease
        self._connection = connection
        self.acquired = True
        return True

    def validate(self) -> bool:
        if not self.acquired or self._connection is None:
            return False
        try:
            row = self._connection.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_locks
                    WHERE locktype = 'advisory'
                      AND pid = pg_backend_pid()
                      AND classid::bigint = %s
                      AND objid::bigint = %s
                      AND objsubid = 2
                      AND granted
                )
                """,
                (_COORDINATOR_LOCK_NAMESPACE, _COORDINATOR_LOCK_KEY),
            ).fetchone()
            self._connection.commit()
        except BaseException:
            self._discard_lease(sys.exc_info())
            return False
        if row and bool(row[0]):
            return True
        self._discard_lease((None, None, None))
        return False

    def _discard_lease(self, exception: tuple[Any, Any, Any]) -> None:
        lease = self._lease
        self._lease = None
        self._connection = None
        self.acquired = False
        if lease is not None:
            lease.__exit__(*exception)

    def release(self) -> None:
        lease, connection = self._lease, self._connection
        self._lease = None
        self._connection = None
        if lease is None:
            self.acquired = False
            return
        try:
            if self.acquired:
                connection.execute(
                    "SELECT pg_advisory_unlock(%s, %s)",
                    (_COORDINATOR_LOCK_NAMESPACE, _COORDINATOR_LOCK_KEY),
                )
                connection.commit()
        finally:
            self.acquired = False
            lease.__exit__(None, None, None)


class PersistentScheduleManager:
    """APScheduler computes fire times; callbacks only create persistent Jobs."""

    def __init__(self, service: JobSubmissionService) -> None:
        self._service = service
        self._scheduler = BackgroundScheduler(
            timezone="Asia/Shanghai",
            job_defaults={"misfire_grace_time": 300, "coalesce": True, "max_instances": 1},
        )
        self._fingerprint: str | None = None
        self._startup_operation_scheduled = False

    @property
    def running(self) -> bool:
        return bool(self._scheduler.running)

    def start(self) -> None:
        if not self._scheduler.running:
            self._scheduler.start()

    def stop(self) -> None:
        if self._scheduler.running:
            self._scheduler.shutdown(wait=True)

    def sync(self, values: Mapping[str, Any], *, schedules_enabled: bool) -> bool:
        relevant = {
            key: values.get(key)
            for key in (
                "task_chain_enabled",
                "task_chain_cron",
                "task_chain_sequence",
                "task_chain_low_freq_enabled",
                "task_chain_low_freq_cron",
                "task_chain_low_freq_sequence",
                "_shared_resource_enabled",
                "_emby_server_id",
                "_p115_temp_cleanup_cron",
            )
        }
        relevant["schedules_enabled"] = schedules_enabled
        fingerprint = json.dumps(relevant, ensure_ascii=False, sort_keys=True, default=str)
        if fingerprint == self._fingerprint:
            return False
        plan = self._build_plan(values) if schedules_enabled else {}
        for job_id, (function, trigger, name, kwargs) in plan.items():
            self._scheduler.add_job(
                function,
                trigger=trigger,
                id=job_id,
                name=name,
                kwargs=kwargs,
                replace_existing=True,
            )
        removable = set(_MANAGED_SCHEDULE_IDS.difference(plan))
        if schedules_enabled and self._startup_operation_scheduled:
            removable.discard(_PLUGIN_UPDATE_JOB)
        for job_id in removable:
            self._remove(job_id)
        self._startup_operation_scheduled = self._startup_operation_scheduled or _PLUGIN_UPDATE_JOB in plan
        if not schedules_enabled:
            self._startup_operation_scheduled = False
        self._fingerprint = fingerprint
        return True

    def _build_plan(
        self,
        values: Mapping[str, Any],
    ) -> dict[str, tuple[Callable[..., Any], Any, str, dict[str, Any]]]:
        plan: dict[str, tuple[Callable[..., Any], Any, str, dict[str, Any]]] = {}

        def add_task(job_id: str, task_key: str, expression: str, *, name: str | None = None) -> None:
            plan[job_id] = (
                self._submit,
                CronTrigger.from_crontab(_fix_cron_dow(expression), timezone="Asia/Shanghai"),
                name or task_key,
                {"task_key": task_key, "schedule_id": job_id},
            )

        for job_id, task_key, enabled_key, cron_key, sequence_key in (
            (_HIGH_JOB, "task-chain-high-freq", "task_chain_enabled", "task_chain_cron", "task_chain_sequence"),
            (
                _LOW_JOB,
                "task-chain-low-freq",
                "task_chain_low_freq_enabled",
                "task_chain_low_freq_cron",
                "task_chain_low_freq_sequence",
            ),
        ):
            expression = str(values.get(cron_key) or "").strip()
            sequence = values.get(sequence_key)
            if _boolish(values.get(enabled_key, False)) and expression and isinstance(sequence, list) and sequence:
                add_task(job_id, task_key, expression)

        add_task(_DAILY_THEME_JOB, "update-daily-theme", "5 0 * * *")
        add_task(_PLAY_POOL_JOB, "play-pool-daily-speedtest", "45 12 * * *")

        if _boolish(values.get("_shared_resource_enabled", False)):
            server_id = str(values.get("_emby_server_id") or "").strip()
            base_minute = int(hashlib.md5(server_id.encode()).hexdigest(), 16) % 10 if server_id else 0
            plan[_SHARED_SYNC_JOB] = (
                self._submit,
                CronTrigger(minute=f"{base_minute}/10", timezone="Asia/Shanghai", jitter=90),
                "共享分享状态同步",
                {"task_key": "shared-share-status-sync", "schedule_id": _SHARED_SYNC_JOB},
            )

        cleanup_cron = str(values.get("_p115_temp_cleanup_cron") or "").strip()
        if cleanup_cron:
            plan[_P115_TEMP_CLEANUP_JOB] = self._operation_spec(
                "cleanup-p115-temp-videos",
                "115播放临时目录清理",
                CronTrigger.from_crontab(_fix_cron_dow(cleanup_cron), timezone="Asia/Shanghai"),
                _P115_TEMP_CLEANUP_JOB,
                {"older_than_hours": 3},
            )
        plan[_PRO_STATUS_JOB] = self._operation_spec(
            "verify-pro-status",
            "Pro状态检查",
            CronTrigger.from_crontab("0 3 * * *", timezone="Asia/Shanghai"),
            _PRO_STATUS_JOB,
            {},
        )
        if not self._startup_operation_scheduled:
            plan[_PLUGIN_UPDATE_JOB] = self._operation_spec(
                "check-etk-plugin-update",
                "ETK MediaInfo Bridge 更新检查",
                DateTrigger(run_date=datetime.now(UTC) + timedelta(seconds=30)),
                _PLUGIN_UPDATE_JOB,
                {},
            )
        return plan

    def _operation_spec(
        self,
        operation_key: str,
        name: str,
        trigger: Any,
        schedule_id: str,
        parameters: Mapping[str, Any],
    ) -> tuple[Callable[..., Any], Any, str, dict[str, Any]]:
        return (
            self._submit_operation,
            trigger,
            name,
            {
                "operation_key": operation_key,
                "name": name,
                "schedule_id": schedule_id,
                "parameters": dict(parameters),
            },
        )

    def _submit(self, *, task_key: str, schedule_id: str) -> None:
        if self._service.is_busy():
            logger.info(
                "Scheduled Job skipped because the compatibility serial queue is busy", extra={"task_key": task_key}
            )
            return
        minute = datetime.now(UTC).replace(second=0, microsecond=0).isoformat()
        try:
            self._service.submit_task(
                task_key,
                {},
                trigger_type="schedule",
                idempotency_scope=f"schedule:{schedule_id}",
                idempotency_key=minute,
                max_attempts=3,
                reject_when_busy=True,
                silent=True,
            )
        except TaskBusyError:
            logger.info("Scheduled Job lost the serial busy race", extra={"task_key": task_key})
        except Exception:
            logger.exception("Scheduled Job submission failed", extra={"task_key": task_key})

    def _submit_operation(
        self,
        *,
        operation_key: str,
        name: str,
        schedule_id: str,
        parameters: Mapping[str, Any],
    ) -> None:
        operation = _SCHEDULED_OPERATIONS[operation_key]
        if self._service.is_busy():
            logger.info("Scheduled operation skipped because the serial queue is busy", extra={"name": name})
            return
        minute = datetime.now(UTC).replace(second=0, microsecond=0).isoformat()
        try:
            self._service.submit_legacy_callable(
                operation,
                name,
                "media",
                (),
                parameters,
                silent=True,
                trigger_type="schedule",
                idempotency_scope=f"schedule:{schedule_id}",
                idempotency_key=minute,
                max_attempts=3,
            )
        except TaskBusyError:
            logger.info("Scheduled operation lost the serial busy race", extra={"name": name})
        except Exception:
            logger.exception("Scheduled operation submission failed", extra={"name": name})

    def _remove(self, job_id: str) -> None:
        try:
            self._scheduler.remove_job(job_id)
        except JobLookupError:
            pass


class CoordinatorRuntime:
    def __init__(
        self,
        *,
        config: TaskRuntimeConfig | None = None,
        health_file: Path | None = None,
    ) -> None:
        self.config = config or TaskRuntimeConfig.from_environ()
        self.health_file = health_file or Path(
            os.environ.get("ETK_COORDINATOR_HEALTH_FILE", "/tmp/etk-coordinator-health.json")
        )
        self._stop = threading.Event()
        self._container: Any = None
        self._config_adapter: Any = None
        self._supervisor: RuntimeSupervisor | None = None
        self._leadership: CoordinatorLeadership | None = None
        self._store: PostgresJobStore | None = None
        self._service: JobSubmissionService | None = None
        self._gateway: PersistentTaskManagerGateway | None = None
        self._schedules: PersistentScheduleManager | None = None
        self._publisher = CeleryOutboxPublisher(self.config)
        self._media_effect_publisher = CeleryMediaEffectPublisher(self.config)
        self._media_effect_dispatcher: MediaEffectOutboxDispatcher | None = None
        self._ingestion_publisher = CeleryIngestionMonitorPublisher(self.config)
        self._ingestion_coordinator: IngestionMonitorCoordinator | None = None
        self._ingestion_ingress: PersistentIngestionIngressGateway | None = None
        self._p115_upload_publisher = CeleryP115UploadPublisher(self.config)
        self._p115_upload_coordinator: P115UploadMonitorCoordinator | None = None
        self._p115_upload_ingress: P115UploadMonitorIngressService | None = None
        self._p115_playback_publisher = CeleryP115PlaybackPublisher(self.config)
        self._p115_playback_coordinator: P115PlaybackCoordinator | None = None
        self._p115_intro_publisher = CeleryP115IntroDetectionPublisher(self.config)
        self._p115_intro_coordinator: P115IntroDetectionCoordinator | None = None
        self._subscription_publisher = CelerySubscriptionOperationPublisher(self.config)
        self._subscription_coordinator: SubscriptionOperationCoordinator | None = None
        self._collection_operation_publisher = CeleryCollectionOperationPublisher(self.config)
        self._collection_operation_coordinator: CollectionOperationCoordinator | None = None
        self._identity_publisher = CeleryIdentityOperationPublisher(self.config)
        self._identity_coordinator: IdentityOperationCoordinator | None = None
        self._database_maintenance_publisher = CeleryDatabaseMaintenancePublisher(self.config)
        self._database_maintenance_coordinator: DatabaseMaintenanceCoordinator | None = None
        self._cleanup_execution_repository: SqlAlchemyCleanupExecutionRepository | None = None
        self._publisher_id = f"{socket.gethostname()}:{os.getpid()}"
        self._last_settings_refresh = 0.0
        self._last_settings: Any = None
        self._last_runtime_context: dict[str, Any] | None = None

    def request_stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        os.environ["ETK_PROCESS_ROLE"] = "coordinator"
        if not _legacy_compatibility_enabled():
            self._run_native_only()
            return
        container, config_adapter = _create_default_container(None)
        database = container.database
        cleanup_execution_repository = SqlAlchemyCleanupExecutionRepository(database.unit_of_work)
        store = PostgresJobStore(
            lambda: database.unit_of_work(),
            cancellation_policy=cleanup_execution_repository,
        )
        registry = TaskRegistry.from_fixture(_task_fixture())
        service = JobSubmissionService(store, registry)
        leadership = CoordinatorLeadership(database.raw_pool)
        self._container = container
        self._config_adapter = config_adapter
        self._store = store
        self._media_effect_dispatcher = MediaEffectOutboxDispatcher(
            store,
            PostgresMediaEffectLedger(database.unit_of_work),
            self._media_effect_publisher,
            self.config,
            publisher_id=f"{self._publisher_id}:media-effect",
        )
        ingestion_ledger = PostgresIngestionMonitorLedger(database.unit_of_work)
        self._ingestion_ingress = PersistentIngestionIngressGateway(
            settings=config_adapter.load,
            ledger=ingestion_ledger,
        )
        self._ingestion_coordinator = IngestionMonitorCoordinator(
            store=store,
            ledger=ingestion_ledger,
            publisher=self._ingestion_publisher,
            config=self.config,
            publisher_id=f"{self._publisher_id}:ingestion",
        )
        upload_repository = PostgresP115UploadMonitorRepository(database.unit_of_work)
        upload_config = P115UploadMonitorConfigService(
            upload_repository,
            lambda: config_adapter.load().monitor_extensions,
        )
        self._p115_upload_ingress = P115UploadMonitorIngressService(
            config=upload_config,
            repository=upload_repository,
        )
        self._p115_upload_coordinator = P115UploadMonitorCoordinator(
            store=store,
            repository=upload_repository,
            config_service=upload_config,
            publisher=self._p115_upload_publisher,
            config=self.config,
            publisher_id=f"{self._publisher_id}:p115-upload",
        )
        playback_repository = PostgresP115PlaybackRepository(database.unit_of_work)
        self._p115_playback_coordinator = P115PlaybackCoordinator(
            store=store,
            repository=playback_repository,
            publisher=self._p115_playback_publisher,
            config=self.config,
            publisher_id=f"{self._publisher_id}:p115-playback",
        )
        intro_repository = PostgresP115IntroDetectionRepository(database.unit_of_work)
        self._p115_intro_coordinator = P115IntroDetectionCoordinator(
            store=store,
            repository=intro_repository,
            publisher=self._p115_intro_publisher,
            config=self.config,
            publisher_id=f"{self._publisher_id}:p115-intro",
        )
        self._subscription_coordinator = SubscriptionOperationCoordinator(
            store=store,
            repository=PostgresSubscriptionOperationRepository(database.unit_of_work),
            runtime_repository=PostgresSubscriptionRuntimeRepository(database.unit_of_work),
            publisher=self._subscription_publisher,
            config=self.config,
            publisher_id=f"{self._publisher_id}:subscription",
        )
        self._collection_operation_coordinator = CollectionOperationCoordinator(
            store=store,
            repository=PostgresCollectionOperationRepository(database.unit_of_work),
            publisher=self._collection_operation_publisher,
            config=self.config,
            publisher_id=f"{self._publisher_id}:collection-operation",
        )
        self._identity_coordinator = IdentityOperationCoordinator(
            store=store,
            repository=PostgresIdentityOperationRepository(database.unit_of_work),
            publisher=self._identity_publisher,
            config=self.config,
            publisher_id=f"{self._publisher_id}:identity",
        )
        self._database_maintenance_coordinator = DatabaseMaintenanceCoordinator(
            store=store,
            repository=PostgresDatabaseMaintenanceRepository(database.unit_of_work),
            publisher=self._database_maintenance_publisher,
            config=self.config,
            publisher_id=f"{self._publisher_id}:database-maintenance",
        )
        self._cleanup_execution_repository = cleanup_execution_repository
        self._service = service
        self._leadership = leadership
        self._write_health("standby")
        try:
            while not self._stop.is_set():
                if leadership.acquired and not leadership.validate():
                    logger.warning("Coordinator leadership lease was lost")
                    self._shutdown_leader()
                    self._write_health("standby")
                    self._stop.wait(self.config.coordinator_interval_seconds)
                    continue
                if not leadership.acquired:
                    if not leadership.try_acquire():
                        self._write_health("standby")
                        self._stop.wait(self.config.coordinator_interval_seconds)
                        continue
                    self._become_leader()
                try:
                    self._tick()
                except Exception:
                    logger.exception("Coordinator iteration failed")
                    self._write_health("degraded")
                else:
                    self._write_health("leader")
                self._stop.wait(self.config.coordinator_interval_seconds)
        finally:
            errors: list[Exception] = []
            for cleanup in (self._shutdown_leader, leadership.release, container.close):
                try:
                    cleanup()
                except Exception as exc:
                    errors.append(exc)
            try:
                self._write_health("stopped")
            except Exception as exc:
                errors.append(exc)
            if errors:
                raise ExceptionGroup("Coordinator shutdown failed", errors)

    def _run_native_only(self) -> None:
        """Keep a native heartbeat without starting legacy scheduler/daemon code."""

        if self.config.schedules_enabled:
            logger.warning("Coordinator schedules are disabled in the native runtime")
        try:
            while not self._stop.is_set():
                self._write_health("standby")
                self._stop.wait(self.config.coordinator_interval_seconds)
        finally:
            self._write_health("stopped")

    def _become_leader(self) -> None:
        if (
            self._service is None
            or self._ingestion_ingress is None
            or self._ingestion_coordinator is None
            or self._p115_upload_ingress is None
            or self._p115_upload_coordinator is None
        ):
            raise RuntimeError("Coordinator services are not initialized")
        supervisor = RuntimeSupervisor()
        self._supervisor = supervisor
        try:
            self._p115_upload_ingress.ensure_config()
            install_persistent_ingestion_ingress(self._ingestion_ingress)
            install_persistent_upload_monitor_ingress(self._p115_upload_ingress)
            gateway = PersistentTaskManagerGateway(self._service)
            self._gateway = gateway
            from task_manager import install_persistent_submission_gateway

            install_persistent_submission_gateway(gateway)
            settings = self._config_adapter.load() if self._config_adapter is not None else None
            if settings is None:
                raise RuntimeError("Coordinator settings are not initialized")
            self._ingestion_coordinator.synchronize_enabled(settings.monitor_enabled)
            # The UserBot can submit an incoming transfer as soon as its listener starts.
            supervisor.start(role=RuntimeRole.COORDINATOR, background=True)
            if not self._p115_upload_ingress.enabled():
                self._p115_upload_ingress.apply_config()
            schedules = PersistentScheduleManager(self._service)
            self._schedules = schedules
            schedules.start()
            self._refresh_settings(force=True)
        except Exception:
            self._shutdown_leader()
            raise

    def _shutdown_leader(self) -> None:
        errors: list[Exception] = []
        if self._schedules is not None:
            try:
                self._schedules.stop()
            except Exception as exc:
                errors.append(exc)
        if self._supervisor is not None:
            try:
                self._supervisor.stop()
            except Exception as exc:
                errors.append(exc)
        if self._ingestion_ingress is not None:
            try:
                clear_persistent_ingestion_ingress(self._ingestion_ingress)
            except Exception as exc:
                errors.append(exc)
        if self._p115_upload_ingress is not None:
            try:
                clear_persistent_upload_monitor_ingress(self._p115_upload_ingress)
            except Exception as exc:
                errors.append(exc)
        if self._gateway is not None:
            try:
                from task_manager import clear_persistent_submission_gateway

                clear_persistent_submission_gateway(self._gateway)
            except Exception as exc:
                errors.append(exc)
        ingestion = getattr(self, "_ingestion_coordinator", None)
        reset_enabled = getattr(ingestion, "reset_enabled_state", None)
        if callable(reset_enabled):
            reset_enabled()
        self._schedules = None
        self._gateway = None
        self._supervisor = None
        if errors:
            raise ExceptionGroup("Coordinator leader shutdown failed", errors)

    def _tick(self) -> None:
        store = self._store
        if store is None:
            raise RuntimeError("Coordinator store is not initialized")
        self._refresh_settings()
        self._process_telegram_commands()
        cleanup = getattr(self, "_cleanup_execution_repository", None)
        if cleanup is not None:
            cleanup.protect_stale_post_effect_cancellations()
        store.enqueue_due_retries(limit=self.config.outbox_batch_size)
        store.recover_stale_attempts(limit=self.config.outbox_batch_size, retry_delay_seconds=1)
        if cleanup is not None:
            cleanup.reconcile_terminal_jobs()
        store.requeue_stale_published(
            limit=self.config.outbox_batch_size,
            processed_timeout_seconds=max(self.config.lease_seconds * 2, 60),
            topics=_JOB_DISPATCH_TOPICS,
        )
        result = store.publish_outbox_batch(
            self._publisher,
            publisher_id=self._publisher_id,
            limit=self.config.outbox_batch_size,
            claim_timeout_seconds=self.config.outbox_claim_timeout_seconds,
            failure_backoff_seconds=2,
            topics=_JOB_DISPATCH_TOPICS,
        )
        if result.failed or result.dead:
            logger.warning(
                "Outbox batch completed with publish failures",
                extra={"failed": result.failed, "dead": result.dead},
            )
            raise RuntimeError("Outbox dispatch is degraded")
        media_dispatcher = getattr(self, "_media_effect_dispatcher", None)
        if media_dispatcher is not None:
            media_result = media_dispatcher.tick()
            if media_result.expired_claims:
                logger.warning(
                    "Expired Media Effect claims were marked indeterminate",
                    extra={"expired_claims": media_result.expired_claims},
                )
            if media_result.rejected_dead_effects:
                logger.error(
                    "Media Effect dispatch exhausted without provider execution",
                    extra={"rejected_effects": media_result.rejected_dead_effects},
                )
            if media_result.publish.failed or media_result.publish.dead:
                logger.warning(
                    "Media Effect Outbox batch completed with publish failures",
                    extra={
                        "failed": media_result.publish.failed,
                        "dead": media_result.publish.dead,
                    },
                )
                raise RuntimeError("Media Effect Outbox dispatch is degraded")

        ingestion = getattr(self, "_ingestion_coordinator", None)
        settings = getattr(self, "_last_settings", None)
        if ingestion is not None and settings is not None:
            ingestion_result = ingestion.tick(settings)
            if ingestion_result.rejected_dead:
                logger.error(
                    "Ingestion monitor dispatch exhausted without Worker execution",
                    extra={"rejected_items": ingestion_result.rejected_dead},
                )
            if ingestion_result.publish.failed or ingestion_result.publish.dead:
                raise RuntimeError("Ingestion monitor Outbox dispatch is degraded")

        upload = getattr(self, "_p115_upload_coordinator", None)
        if upload is not None:
            upload_result = upload.tick()
            if upload_result.rejected_dead:
                logger.error(
                    "P115 upload dispatch exhausted without Worker execution",
                    extra={"rejected_operations": upload_result.rejected_dead},
                )
            if upload_result.publish.failed or upload_result.publish.dead:
                raise RuntimeError("P115 upload Outbox dispatch is degraded")

        playback = getattr(self, "_p115_playback_coordinator", None)
        if playback is not None:
            playback_result = playback.tick()
            if playback_result.rejected_dead:
                logger.error(
                    "P115 playback dispatch exhausted without Worker execution",
                    extra={"rejected_operations": playback_result.rejected_dead},
                )
            if playback_result.publish.failed or playback_result.publish.dead:
                raise RuntimeError("P115 playback Outbox dispatch is degraded")

        intro = getattr(self, "_p115_intro_coordinator", None)
        if intro is not None:
            intro_result = intro.tick()
            if intro_result.expired_claims:
                logger.warning(
                    "Expired P115 intro detection claims were requeued",
                    extra={"expired_operations": intro_result.expired_claims},
                )
            if intro_result.rejected_dead:
                logger.error(
                    "P115 intro detection dispatch exhausted without Worker execution",
                    extra={"rejected_operations": intro_result.rejected_dead},
                )
            if intro_result.publish.failed or intro_result.publish.dead:
                raise RuntimeError("P115 intro detection Outbox dispatch is degraded")

        subscription = getattr(self, "_subscription_coordinator", None)
        if subscription is not None:
            subscription_result = subscription.tick()
            if subscription_result.expired_claims:
                logger.warning(
                    "Expired subscription operation claims were recovered",
                    extra={"expired_operations": subscription_result.expired_claims},
                )
            if subscription_result.rejected_dead:
                logger.error(
                    "Subscription operation dispatch exhausted without Worker execution",
                    extra={"rejected_operations": subscription_result.rejected_dead},
                )
            if subscription_result.publish.failed or subscription_result.publish.dead:
                raise RuntimeError("Subscription operation Outbox dispatch is degraded")

        identity = getattr(self, "_identity_coordinator", None)
        if identity is not None:
            identity_result = identity.tick()
            if identity_result.expired_claims:
                logger.warning(
                    "Expired identity operation claims were recovered",
                    extra={"expired_operations": identity_result.expired_claims},
                )
            if identity_result.purged_avatar_artifacts:
                logger.info(
                    "Expired identity avatar artifacts were purged",
                    extra={"purged_artifacts": identity_result.purged_avatar_artifacts},
                )
            if identity_result.rejected_dead:
                logger.error(
                    "Identity operation dispatch exhausted without Worker execution",
                    extra={"rejected_operations": identity_result.rejected_dead},
                )
            if identity_result.publish.failed or identity_result.publish.dead:
                raise RuntimeError("Identity operation Outbox dispatch is degraded")

        collection = getattr(self, "_collection_operation_coordinator", None)
        if collection is not None:
            collection_result = collection.tick()
            if collection_result.expired_claims:
                logger.warning(
                    "Expired collection operation claims were recovered",
                    extra={"expired_operations": collection_result.expired_claims},
                )
            if collection_result.rejected_dead:
                logger.error(
                    "Collection operation dispatch exhausted without Worker execution",
                    extra={"rejected_operations": collection_result.rejected_dead},
                )
            if collection_result.publish.failed or collection_result.publish.dead:
                raise RuntimeError("Collection operation Outbox dispatch is degraded")

        maintenance = getattr(self, "_database_maintenance_coordinator", None)
        if maintenance is not None:
            maintenance_result = maintenance.tick()
            if maintenance_result.expired_claims:
                logger.warning(
                    "Expired database maintenance claims were recovered",
                    extra={"expired_operations": maintenance_result.expired_claims},
                )
            if maintenance_result.purged_artifacts:
                logger.info(
                    "Expired database backup artifacts were purged",
                    extra={"purged_artifacts": maintenance_result.purged_artifacts},
                )
            if maintenance_result.rejected_dead:
                logger.error(
                    "Database maintenance dispatch exhausted without Worker execution",
                    extra={"rejected_operations": maintenance_result.rejected_dead},
                )
            if maintenance_result.publish.failed or maintenance_result.publish.dead:
                raise RuntimeError("Database maintenance Outbox dispatch is degraded")

    def _process_telegram_commands(self) -> None:
        from etk.compatibility.tg_control_bridge import TelegramCommandExecutor, TelegramControlBridge
        from handler.tg_userbot import TGUserBotManager

        executor = TelegramCommandExecutor(
            TelegramControlBridge(),
            owner_id=self._publisher_id,
            lease_seconds=max(60, int(self.config.coordinator_interval_seconds * 3)),
        )
        executor.process_pending(TGUserBotManager.get_instance(), limit=8)

    def _refresh_settings(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_settings_refresh < 5:
            return
        self._last_settings_refresh = now
        adapter = self._config_adapter
        if adapter is None:
            return
        settings = adapter.load()
        runtime_context = self._load_runtime_context()
        settings_changed = (
            self._last_settings is not None and settings.to_runtime_dict() != self._last_settings.to_runtime_dict()
        )
        runtime_context_changed = (
            self._last_runtime_context is not None and runtime_context != self._last_runtime_context
        )
        if settings_changed or runtime_context_changed:
            # Recreate only the Coordinator-owned compatibility adapters; the
            # API and Worker consume their own immutable snapshots.
            if self._supervisor is not None:
                self._supervisor.stop()
                self._supervisor.start(role=RuntimeRole.COORDINATOR, background=True)
                if self._p115_upload_ingress is not None and not self._p115_upload_ingress.enabled():
                    self._p115_upload_ingress.apply_config()
        self._last_settings = settings
        self._last_runtime_context = runtime_context
        if self._schedules is not None:
            schedule_values = settings.to_runtime_dict()
            schedule_values.update(runtime_context)
            self._schedules.sync(
                schedule_values,
                schedules_enabled=self.config.schedules_enabled,
            )

    @staticmethod
    def _load_runtime_context() -> dict[str, Any]:
        import config_manager
        import constants
        import extensions
        from database import settings_db
        from handler.p115_temp_dir import get_temp_dir_config

        shared = settings_db.get_shared_resource_config() or {}
        temp = get_temp_dir_config() or {}
        tg_userbot = settings_db.get_setting("tg_userbot_config") or {}
        upload_config = normalize_upload_monitor_config(
            settings_db.get_setting("p115_upload_monitor_config") or {},
            default_extensions=config_manager.APP_CONFIG.get(
                constants.CONFIG_OPTION_MONITOR_EXTENSIONS,
                constants.DEFAULT_MONITOR_EXTENSIONS,
            ),
        )
        return {
            "_shared_resource_enabled": shared.get("p115_shared_resource_enabled", False),
            "_emby_server_id": str(getattr(extensions, "EMBY_SERVER_ID", "") or "").strip(),
            "_p115_temp_cleanup_cron": str(temp.get("cleanup_cron") or "").strip(),
            "_tg_userbot_config": tg_userbot if isinstance(tg_userbot, dict) else {},
            "_p115_upload_monitor_revision": upload_monitor_config_revision(upload_config),
        }

    def _write_health(self, state: str) -> None:
        payload = {
            "state": state,
            "pid": os.getpid(),
            "updated_at": datetime.now(UTC).isoformat(),
        }
        self.health_file.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.health_file.with_suffix(f"{self.health_file.suffix}.{os.getpid()}.tmp")
        temporary.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        temporary.replace(self.health_file)


def coordinator_health(path: Path, *, max_age_seconds: float = 10) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        updated_at = datetime.fromisoformat(str(payload["updated_at"]))
        age = (datetime.now(UTC) - updated_at.astimezone(UTC)).total_seconds()
        return payload.get("state") in {"leader", "standby"} and 0 <= age <= max_age_seconds
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        return False


def _legacy_compatibility_enabled() -> bool:
    return os.environ.get("ETK_ENABLE_LEGACY_COMPATIBILITY", "").strip().casefold() in {"1", "true", "yes", "on"}


def _task_fixture() -> Path:
    return Path(__file__).resolve().parents[3] / "verification" / "fixtures" / "tasks" / "ordered-task-catalog.json"


def _fix_cron_dow(expression: str) -> str:
    parts = expression.strip().split()
    if len(parts) != 5:
        return expression
    dow = parts[4]
    if dow != "*":
        mapping = {
            "7": "sun",
            "6": "sat",
            "5": "fri",
            "4": "thu",
            "3": "wed",
            "2": "tue",
            "1": "mon",
            "0": "sun",
        }
        for number, name in mapping.items():
            dow = re.sub(rf"\b{number}\b", name, dow)
        parts[4] = dow
    return " ".join(parts)


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().casefold() in {"1", "true", "yes", "on", "enabled", "开启", "是"}


__all__ = [
    "CeleryOutboxPublisher",
    "CoordinatorLeadership",
    "CoordinatorRuntime",
    "PersistentScheduleManager",
    "coordinator_health",
]
