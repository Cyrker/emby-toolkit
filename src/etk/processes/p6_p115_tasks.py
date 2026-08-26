from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol
from uuid import UUID

from etk.domains.p115 import P115ExecutionRunResult, P115ExecutionStatus, P115TaskProgress
from etk.domains.p115.playback import task_outcome_from_daily_result
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcome
from etk.platform.tasks.catalog import P6_P115_NATIVE_TASK_KEYS


class _DirectorySyncUseCase(Protocol):
    def execute(self, progress: P115TaskProgress) -> TaskOutcome: ...


class _ReconciliationResult(Protocol):
    def to_dict(self) -> dict[str, Any]: ...


class _StrmSyncUseCase(Protocol):
    def execute(self, progress: P115TaskProgress, *, home_video_only: bool = False) -> TaskOutcome: ...


class _FingerprintRepairUseCase(Protocol):
    def execute(self, progress: P115TaskProgress) -> TaskOutcome: ...


class _PlaybackMaintenanceUseCase(Protocol):
    def daily_speedtest(self, progress: P115TaskProgress) -> Mapping[str, Any]: ...


class _IntroBackfillUseCase(Protocol):
    def execute(self, progress: P115TaskProgress) -> TaskOutcome: ...


class _OrganizeExecutionUseCase(Protocol):
    def execute(
        self,
        execution_id: UUID,
        progress: P115TaskProgress,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str | None = None,
    ) -> P115ExecutionRunResult: ...

    def reconcile(
        self,
        execution_id: UUID,
        approved_intent_id: str,
        *,
        actor: str | None = None,
    ) -> _ReconciliationResult: ...


class P6P115TaskRouter:
    """Validate P6 task envelopes and dispatch native 115 use cases."""

    def __init__(
        self,
        directory_sync: _DirectorySyncUseCase,
        organize_execution: _OrganizeExecutionUseCase,
        strm_sync: _StrmSyncUseCase | None = None,
        fingerprint_repair: _FingerprintRepairUseCase | None = None,
        playback: _PlaybackMaintenanceUseCase | None = None,
        intro_backfill: _IntroBackfillUseCase | None = None,
    ) -> None:
        self._directory_sync = directory_sync
        self._organize_execution = organize_execution
        self._strm_sync = strm_sync
        self._fingerprint_repair = fingerprint_repair
        self._playback = playback
        self._intro_backfill = intro_backfill

    def execute(self, message: TaskMessage, progress: P115TaskProgress) -> TaskOutcome:
        if not isinstance(message, TaskMessage):
            raise TypeError("P6 115 task router requires a TaskMessage")
        if message.task_key not in P6_P115_NATIVE_TASK_KEYS:
            raise LookupError(f"unknown P6 115 task key: {message.task_key}")
        expected_processor = (
            ProcessorIdentity.WATCHLIST if message.task_key == "intro-fingerprint-backfill" else ProcessorIdentity.MEDIA
        )
        if message.processor is not expected_processor:
            raise ValueError(f"task {message.task_key!r} requires the {expected_processor.value} processor identity")
        if message.payload.version != 1:
            raise ValueError(f"task {message.task_key!r} payload version is not supported")

        if message.task_key == "sync-115-directory-tree":
            return self._execute_directory_sync(message, progress)
        if message.task_key == "scan-organize-115":
            return self._execute_organize(message, progress)
        if message.task_key == "manual-correct-organize-records":
            return self._execute_reconciliation(message)
        if message.task_key in {"full-sync-strm", "monitor-115-life-events"}:
            return self._execute_strm_sync(message, progress)
        if message.task_key == "repair-p115-fingerprints":
            return self._execute_fingerprint_repair(message, progress)
        if message.task_key == "play-pool-daily-speedtest":
            return self._execute_playback_speedtest(message, progress)
        if message.task_key == "intro-fingerprint-backfill":
            return self._execute_intro_backfill(message, progress)
        raise LookupError(f"unknown P6 115 task key: {message.task_key}")

    def _execute_directory_sync(self, message: TaskMessage, progress: P115TaskProgress) -> TaskOutcome:
        if message.payload.data:
            raise ValueError(f"task {message.task_key!r} does not accept parameters")

        result = self._directory_sync.execute(progress)
        return TaskOutcome(
            result.status,
            result.message,
            self._details(message.task_key, result.details),
        )

    def _execute_organize(self, message: TaskMessage, progress: P115TaskProgress) -> TaskOutcome:
        _require_exact_fields(message.task_key, message.payload.data, {"execution_id"})
        execution_id = _uuid_field(message.payload.data, "execution_id", container="payload")
        job_id = _uuid_field(message.metadata, "job_id", container="metadata")
        attempt_id = _uuid_field(message.metadata, "attempt_id", container="metadata")
        worker_id = _required_text_field(message.metadata, "worker_id", container="metadata", maximum=255)

        result = self._organize_execution.execute(
            execution_id,
            progress,
            job_id=job_id,
            attempt_id=attempt_id,
            worker_id=worker_id,
        )
        details = self._details(
            message.task_key,
            {
                **result.details,
                "p4_terminalized": result.p4_terminalized,
            },
        )
        factory = TaskOutcome.succeeded if result.status is P115ExecutionStatus.CONFIRMED else TaskOutcome.failed
        return factory(result.message, details)

    def _execute_reconciliation(self, message: TaskMessage) -> TaskOutcome:
        _require_exact_fields(
            message.task_key,
            message.payload.data,
            {"execution_id", "approved_reconciliation_intent_id"},
        )
        execution_id = _uuid_field(message.payload.data, "execution_id", container="payload")
        approved_intent_id = _required_text_field(
            message.payload.data,
            "approved_reconciliation_intent_id",
            container="payload",
            maximum=255,
        )
        actor = _optional_text_field(
            message.metadata,
            "requested_by_user_id",
            container="metadata",
            maximum=255,
        )
        result = self._organize_execution.reconcile(execution_id, approved_intent_id, actor=actor)
        return TaskOutcome.succeeded(
            "115 整理对账完成",
            self._details(
                message.task_key,
                {
                    **result.to_dict(),
                    "p4_terminalized": False,
                },
            ),
        )

    def _execute_strm_sync(self, message: TaskMessage, progress: P115TaskProgress) -> TaskOutcome:
        home_video_only = message.task_key == "monitor-115-life-events"
        if home_video_only and message.payload.data:
            raise ValueError(f"task {message.task_key!r} does not accept parameters")
        if not home_video_only:
            unsupported = set(message.payload.data) - {"home_video_only"}
            if unsupported:
                raise ValueError(f"task {message.task_key!r} has unsupported parameters: {sorted(unsupported)!r}")
            if "home_video_only" in message.payload.data:
                raw_home_video_only = message.payload.data["home_video_only"]
                if not isinstance(raw_home_video_only, bool):
                    raise ValueError(f"task {message.task_key!r} parameter 'home_video_only' must be a boolean")
                home_video_only = raw_home_video_only
        if self._strm_sync is None:
            raise RuntimeError("P6 STRM sync use case is unavailable")
        result = self._strm_sync.execute(
            progress,
            home_video_only=home_video_only,
        )
        return TaskOutcome(
            result.status,
            result.message,
            self._details(message.task_key, result.details),
        )

    def _execute_fingerprint_repair(self, message: TaskMessage, progress: P115TaskProgress) -> TaskOutcome:
        if message.payload.data:
            raise ValueError(f"task {message.task_key!r} does not accept parameters")
        if self._fingerprint_repair is None:
            raise RuntimeError("P6 fingerprint repair use case is unavailable")
        result = self._fingerprint_repair.execute(progress)
        return TaskOutcome(
            result.status,
            result.message,
            self._details(message.task_key, result.details),
        )

    def _execute_playback_speedtest(self, message: TaskMessage, progress: P115TaskProgress) -> TaskOutcome:
        if message.payload.data:
            raise ValueError(f"task {message.task_key!r} does not accept parameters")
        if self._playback is None:
            raise RuntimeError("P6 playback maintenance use case is unavailable")
        result = task_outcome_from_daily_result(self._playback.daily_speedtest(progress))
        return TaskOutcome(
            result.status,
            result.message,
            self._details(message.task_key, result.details),
        )

    def _execute_intro_backfill(self, message: TaskMessage, progress: P115TaskProgress) -> TaskOutcome:
        if message.payload.data:
            raise ValueError(f"task {message.task_key!r} does not accept parameters")
        if self._intro_backfill is None:
            raise RuntimeError("P6 intro detection backfill use case is unavailable")
        result = self._intro_backfill.execute(progress)
        return TaskOutcome(
            result.status,
            result.message,
            self._details(message.task_key, result.details),
        )

    def _details(self, task_key: str, details: Mapping[str, Any]) -> dict[str, Any]:
        return {
            **details,
            "task_key": task_key,
            "native_handler": type(self).__name__,
            "compatibility_fallback": False,
        }


def _require_exact_fields(task_key: str, values: Mapping[str, Any], expected: set[str]) -> None:
    actual = set(values)
    if actual == expected:
        return
    missing = sorted(expected - actual)
    unsupported = sorted(actual - expected)
    raise ValueError(
        f"task {task_key!r} requires exactly {sorted(expected)!r}; missing={missing!r}, unsupported={unsupported!r}"
    )


def _uuid_field(values: Mapping[str, Any], field: str, *, container: str) -> UUID:
    value = values.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"task {container} field {field!r} must be a UUID string")
    try:
        return UUID(value.strip())
    except ValueError as exc:
        raise ValueError(f"task {container} field {field!r} must be a UUID string") from exc


def _required_text_field(
    values: Mapping[str, Any],
    field: str,
    *,
    container: str,
    maximum: int,
) -> str:
    value = values.get(field)
    if not isinstance(value, str):
        raise ValueError(f"task {container} field {field!r} must be a non-empty string")
    normalized = value.strip()
    if not normalized or len(normalized) > maximum:
        raise ValueError(f"task {container} field {field!r} must be a 1-{maximum} character string")
    return normalized


def _optional_text_field(
    values: Mapping[str, Any],
    field: str,
    *,
    container: str,
    maximum: int,
) -> str | None:
    if values.get(field) is None:
        return None
    return _required_text_field(values, field, container=container, maximum=maximum)


__all__ = ["P6P115TaskRouter"]
