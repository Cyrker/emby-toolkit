from __future__ import annotations

import hashlib
import importlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast
from uuid import UUID

from etk.platform.jobs import (
    ActiveJobConflictError,
    IdempotencyConflictError,
    JobProjection,
    JobStatus,
    JobStoreProtocol,
    ResumeJobResult,
    normalize_json_object,
)
from etk.platform.tasks import ProcessorIdentity, TaskDefinition, TaskRegistry


@dataclass(frozen=True, slots=True)
class SubmittedJob:
    projection: JobProjection
    reused: bool
    resumed: bool = False


class UnknownTaskError(LookupError):
    pass


class TaskBusyError(RuntimeError):
    pass


class UnsupportedLegacyOperationError(ValueError):
    pass


class ApprovedExecutionRequiredError(ValueError):
    pass


class JobSubmissionService:
    """Application boundary for every API, schedule, and compatibility submission."""

    def __init__(self, store: JobStoreProtocol, registry: TaskRegistry) -> None:
        self._store = store
        self._registry = registry
        self._definitions_by_function: dict[str, tuple[TaskDefinition, ...]] = {}
        for definition in registry.definitions():
            self._definitions_by_function.setdefault(definition.legacy_function, ())
            self._definitions_by_function[definition.legacy_function] += (definition,)

    @property
    def store(self) -> JobStoreProtocol:
        return self._store

    @property
    def registry(self) -> TaskRegistry:
        return self._registry

    def submit_task(
        self,
        task_key: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        trigger_type: str = "manual",
        requested_by_user_id: str | None = None,
        idempotency_key: str | None = None,
        idempotency_scope: str | None = None,
        parent_job_id: UUID | None = None,
        workflow_step: int | None = None,
        max_attempts: int = 1,
        timeout_seconds: int | None = None,
        trace_id: str | None = None,
        silent: bool = False,
        reject_when_busy: bool = False,
        dispatch: bool = True,
        queue_name: str = "legacy.serial",
    ) -> SubmittedJob:
        try:
            definition = self._registry.require(task_key)
        except (KeyError, LookupError) as exc:
            raise UnknownTaskError(task_key) from exc
        if definition.key == "scan-organize-115":
            raise ApprovedExecutionRequiredError("scan-organize-115 必须通过 immutable Plan 审批执行，禁止普通任务提交")
        if reject_when_busy and self._store.has_active_job():
            raise TaskBusyError("another job is active")
        payload = _task_payload(definition, parameters, silent=silent)
        try:
            result = self._store.create_job(
                task_key=definition.key,
                payload=payload,
                queue_name=queue_name,
                trigger_type=trigger_type,
                max_attempts=max_attempts,
                timeout_seconds=timeout_seconds,
                requested_by_user_id=requested_by_user_id,
                parent_job_id=parent_job_id,
                workflow_step=workflow_step,
                trace_id=trace_id,
                idempotency_key=idempotency_key,
                idempotency_scope=idempotency_scope,
                dispatch=dispatch,
            )
        except ActiveJobConflictError as exc:
            raise TaskBusyError("another job is active") from exc
        return SubmittedJob(result.projection, result.reused)

    def submit_legacy_callable(
        self,
        task_function: Callable[..., Any],
        task_name: str,
        processor_type: ProcessorIdentity | str,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
        *,
        silent: bool,
        trigger_type: str = "system",
        idempotency_key: str | None = None,
        idempotency_scope: str | None = None,
        max_attempts: int = 1,
        queue_when_busy: bool = False,
    ) -> SubmittedJob:
        processor = ProcessorIdentity(processor_type)
        definition = self._match_definition(task_function, task_name, processor)
        if definition is not None:
            if args:
                raise UnsupportedLegacyOperationError(
                    f"catalog task {definition.key!r} cannot persist positional arguments"
                )
            return self.submit_task(
                definition.key,
                kwargs,
                trigger_type=trigger_type,
                silent=silent,
                reject_when_busy=not queue_when_busy,
                idempotency_key=idempotency_key,
                idempotency_scope=idempotency_scope,
                max_attempts=max_attempts,
                queue_name="legacy.backlog" if queue_when_busy else "legacy.serial",
            )

        module = str(getattr(task_function, "__module__", "") or "").strip()
        qualname = str(getattr(task_function, "__qualname__", "") or "").strip()
        if not module or not qualname or "<locals>" in qualname or "<lambda>" in qualname:
            raise UnsupportedLegacyOperationError(
                f"task {task_name!r} must use an explicit persistent operation adapter"
            )
        operation_key = _operation_key(module, qualname)
        payload = {
            "payload_version": 1,
            "parameters": normalize_json_object(dict(kwargs), field="parameters"),
            "positional_parameters": _normalize_json_list(args),
            "processor_type": processor.value,
            "display_name": str(task_name),
            "silent": bool(silent),
            "legacy_operation": {"module": module, "qualname": qualname},
        }
        if not queue_when_busy and self._store.has_active_job():
            raise TaskBusyError("another job is active")
        try:
            result = self._store.create_job(
                task_key=operation_key,
                payload=payload,
                queue_name="legacy.backlog" if queue_when_busy else "legacy.serial",
                trigger_type=trigger_type,
                max_attempts=max_attempts,
                idempotency_key=idempotency_key,
                idempotency_scope=idempotency_scope,
            )
        except ActiveJobConflictError as exc:
            raise TaskBusyError("another job is active") from exc
        return SubmittedJob(result.projection, result.reused)

    def submit_background_operation(
        self,
        operation: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
        *,
        scheduled_for: datetime | None = None,
        max_attempts: int = 1,
        timeout_seconds: int | None = None,
        idempotency_key: str | None = None,
        idempotency_scope: str | None = None,
    ) -> SubmittedJob:
        module, qualname = _importable_identity(operation)
        payload = {
            "payload_version": 1,
            "parameters": normalize_json_object(dict(kwargs), field="parameters"),
            "positional_parameters": _normalize_json_list(args),
            "processor_type": ProcessorIdentity.MEDIA.value,
            "display_name": str(getattr(operation, "__name__", "background operation")),
            "silent": True,
            "call_style": "direct",
            "legacy_operation": {"module": module, "qualname": qualname},
        }
        result = self._store.create_job(
            task_key=_operation_key(module, qualname, prefix="background-operation"),
            payload=payload,
            queue_name="legacy.background",
            trigger_type="system",
            scheduled_for=scheduled_for,
            max_attempts=max_attempts,
            timeout_seconds=timeout_seconds,
            idempotency_key=idempotency_key,
            idempotency_scope=idempotency_scope,
        )
        return SubmittedJob(result.projection, result.reused)

    def is_busy(self) -> bool:
        return self._store.has_active_job()

    def current(self) -> JobProjection | None:
        return self._store.get_current_job()

    def get(self, job_id: UUID) -> JobProjection | None:
        return self._store.get_job(job_id)

    def get_idempotent(self, *, scope: str, key: str) -> JobProjection | None:
        return self._store.get_idempotent_job(scope=scope, key=key)

    def resume_terminal(self, job_id: UUID) -> SubmittedJob:
        resume = getattr(self._store, "resume_terminal_job", None)
        if not callable(resume):
            raise RuntimeError("persistent Job Store does not support explicit resume")
        result = cast(ResumeJobResult, resume(job_id))
        return SubmittedJob(
            projection=result.projection,
            reused=True,
            resumed=bool(result.resumed),
        )

    def cancel_current(self) -> JobProjection | None:
        return self._store.request_cancel_current()

    def cancel(self, job_id: UUID) -> JobProjection:
        return self._store.request_cancel(job_id)

    def legacy_status(self) -> dict[str, Any]:
        return project_legacy_status(self._store.get_current_job())

    def _match_definition(
        self,
        task_function: Callable[..., Any],
        task_name: str,
        processor: ProcessorIdentity,
    ) -> TaskDefinition | None:
        function_name = str(getattr(task_function, "__name__", "") or "")
        candidates = tuple(
            definition
            for definition in self._definitions_by_function.get(function_name, ())
            if definition.processor is processor
        )
        if len(candidates) == 1:
            return candidates[0]
        normalized_name = str(task_name or "").strip()
        for definition in candidates:
            if definition.name == normalized_name:
                return definition
        return None


class LegacyProcessorReadiness:
    """Compatibility health/signalling port; it never executes a task."""

    def media_ready(self) -> bool:
        import extensions

        return extensions.media_processor_instance is not None

    def any_ready(self) -> bool:
        return bool(self._processors())

    def signal_stop_all(self) -> None:
        for processor in self._processors():
            processor.signal_stop()

    @staticmethod
    def _processors() -> tuple[Any, ...]:
        import extensions

        return tuple(
            processor
            for processor in (
                extensions.media_processor_instance,
                extensions.watchlist_processor_instance,
                extensions.actor_subscription_processor_instance,
            )
            if processor is not None
        )


class PersistentRuntimeStatusReader:
    def __init__(self, service: JobSubmissionService) -> None:
        self._service = service

    def read_status(self) -> Mapping[str, Any]:
        return self._service.legacy_status()


def project_legacy_status(projection: JobProjection | None) -> dict[str, Any]:
    if projection is None:
        return {
            "is_running": False,
            "current_action": "无",
            "progress": 0,
            "message": "等待任务",
            "last_action": None,
            "logs": [],
        }
    job = projection.job
    display_name = str(job.request_payload.get("display_name") or job.task_key)
    active = job.status in {
        JobStatus.QUEUED,
        JobStatus.RUNNING,
        JobStatus.RETRY_WAIT,
        JobStatus.CANCEL_REQUESTED,
    }
    message = job.progress_message or _default_status_message(job.status, display_name)
    return {
        "is_running": active,
        "current_action": display_name if active else "无",
        "progress": int(job.progress),
        "message": message,
        "last_action": display_name,
        "logs": [dict(entry) for entry in job.logs],
    }


def serialize_projection(projection: JobProjection) -> dict[str, Any]:
    job = projection.job
    attempt = projection.current_attempt
    return {
        "job_id": str(job.id),
        "task_key": job.task_key,
        "status": job.status.value,
        "progress": job.progress,
        "message": job.progress_message,
        "logs": [dict(entry) for entry in job.logs],
        "attempt": {
            "attempt_id": str(attempt.id),
            "number": attempt.attempt_no,
            "status": attempt.status.value,
            "worker_id": attempt.worker_id,
            "last_heartbeat_at": _iso(attempt.last_heartbeat_at),
        },
        "parent_job_id": str(job.parent_job_id) if job.parent_job_id else None,
        "workflow_step": job.workflow_step,
        "cancel_requested_at": _iso(job.cancel_requested_at),
        "started_at": _iso(job.started_at),
        "finished_at": _iso(job.finished_at),
        "result": job.result,
        "error": job.error_summary,
        "created_at": _iso(job.created_at),
        "updated_at": _iso(job.updated_at),
    }


def import_legacy_operation(module: str, qualname: str) -> Callable[..., Any]:
    if not module or not qualname or "<" in qualname:
        raise UnsupportedLegacyOperationError("legacy operation is not importable")
    value: Any = importlib.import_module(module)
    for segment in qualname.split("."):
        value = getattr(value, segment)
    if not callable(value):
        raise UnsupportedLegacyOperationError(f"{module}:{qualname} is not callable")
    return value


def _task_payload(
    definition: TaskDefinition,
    parameters: Mapping[str, Any] | None,
    *,
    silent: bool,
) -> dict[str, Any]:
    normalized = normalize_json_object(dict(parameters or {}), field="parameters")
    return {
        "payload_version": definition.payload_version,
        "parameters": normalized,
        "processor_type": definition.processor.value,
        "display_name": definition.name,
        "silent": bool(silent),
    }


def _operation_key(module: str, qualname: str, *, prefix: str = "legacy-operation") -> str:
    identity = f"{module}:{qualname}"
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20]
    return f"{prefix}:{digest}"


def _importable_identity(function: Callable[..., Any]) -> tuple[str, str]:
    module = str(getattr(function, "__module__", "") or "").strip()
    qualname = str(getattr(function, "__qualname__", "") or "").strip()
    if not module or not qualname or "<locals>" in qualname or "<lambda>" in qualname:
        raise UnsupportedLegacyOperationError("background operation must be a top-level importable callable")
    return module, qualname


def _normalize_json_list(values: tuple[Any, ...]) -> list[Any]:
    encoded = json.dumps(list(values), allow_nan=False, ensure_ascii=False, separators=(",", ":"))
    decoded = json.loads(encoded)
    if not isinstance(decoded, list):
        raise UnsupportedLegacyOperationError("positional parameters must be JSON values")
    return decoded


def _default_status_message(status: JobStatus, display_name: str) -> str:
    messages = {
        JobStatus.QUEUED: f"{display_name} 已提交，等待任务线程启动...",
        JobStatus.RUNNING: f"{display_name} 处理中...",
        JobStatus.RETRY_WAIT: f"{display_name} 等待重试...",
        JobStatus.CANCEL_REQUESTED: "已发送停止任务请求。",
        JobStatus.SUCCEEDED: "处理完成。",
        JobStatus.FAILED: "未知结束状态",
        JobStatus.TIMED_OUT: "任务因超过最大运行时长而停止。",
        JobStatus.CANCELLED: "任务已成功中断。",
    }
    return messages[status]


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


__all__ = [
    "ApprovedExecutionRequiredError",
    "IdempotencyConflictError",
    "JobSubmissionService",
    "LegacyProcessorReadiness",
    "PersistentRuntimeStatusReader",
    "SubmittedJob",
    "TaskBusyError",
    "UnknownTaskError",
    "UnsupportedLegacyOperationError",
    "import_legacy_operation",
    "project_legacy_status",
    "serialize_projection",
]
