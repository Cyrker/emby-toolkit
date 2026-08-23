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
from etk.compatibility.persistent_tasks import PersistentTaskManagerGateway
from etk.compatibility.scheduled_operations import (
    check_etk_plugin_update,
    cleanup_p115_temp_videos,
    verify_pro_status,
)
from etk.domains.operations.jobs import JobSubmissionService, TaskBusyError
from etk.platform.jobs import OutboxClaim, PostgresJobStore
from etk.platform.tasks import TaskRegistry

from .celery_app import EXECUTE_JOB_TASK, create_celery_app
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
        self._publisher_id = f"{socket.gethostname()}:{os.getpid()}"
        self._last_settings_refresh = 0.0
        self._last_settings: Any = None
        self._last_runtime_context: dict[str, Any] | None = None

    def request_stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        os.environ["ETK_PROCESS_ROLE"] = "coordinator"
        container, config_adapter = _create_default_container(None)
        database = container.database
        store = PostgresJobStore(lambda: database.unit_of_work())
        registry = TaskRegistry.from_fixture(_task_fixture())
        service = JobSubmissionService(store, registry)
        leadership = CoordinatorLeadership(database.raw_pool)
        self._container = container
        self._config_adapter = config_adapter
        self._store = store
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

    def _become_leader(self) -> None:
        if self._service is None:
            raise RuntimeError("Coordinator service is not initialized")
        supervisor = RuntimeSupervisor()
        self._supervisor = supervisor
        try:
            gateway = PersistentTaskManagerGateway(self._service)
            self._gateway = gateway
            from task_manager import install_persistent_submission_gateway

            install_persistent_submission_gateway(gateway)
            # The UserBot can submit an incoming transfer as soon as its listener starts.
            supervisor.start(role=RuntimeRole.COORDINATOR, background=True)
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
        store.enqueue_due_retries(limit=self.config.outbox_batch_size)
        store.recover_stale_attempts(limit=self.config.outbox_batch_size, retry_delay_seconds=1)
        store.requeue_stale_published(
            limit=self.config.outbox_batch_size,
            processed_timeout_seconds=max(self.config.lease_seconds * 2, 60),
        )
        result = store.publish_outbox_batch(
            self._publisher,
            publisher_id=self._publisher_id,
            limit=self.config.outbox_batch_size,
            claim_timeout_seconds=self.config.outbox_claim_timeout_seconds,
            failure_backoff_seconds=2,
        )
        if result.failed or result.dead:
            logger.warning(
                "Outbox batch completed with publish failures",
                extra={"failed": result.failed, "dead": result.dead},
            )
            raise RuntimeError("Outbox dispatch is degraded")

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
        import extensions
        from database import settings_db
        from handler.p115_temp_dir import get_temp_dir_config

        shared = settings_db.get_shared_resource_config() or {}
        temp = get_temp_dir_config() or {}
        tg_userbot = settings_db.get_setting("tg_userbot_config") or {}
        return {
            "_shared_resource_enabled": shared.get("p115_shared_resource_enabled", False),
            "_emby_server_id": str(getattr(extensions, "EMBY_SERVER_ID", "") or "").strip(),
            "_p115_temp_cleanup_cron": str(temp.get("cleanup_cron") or "").strip(),
            "_tg_userbot_config": tg_userbot if isinstance(tg_userbot, dict) else {},
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
