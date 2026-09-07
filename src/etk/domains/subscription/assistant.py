from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs import normalize_json_object

ASSISTANT_MAINTENANCE_STATE_PREFIX = "maintenance:"
ASSISTANT_MAINTENANCE_RETENTION_DAYS = 30
ASSISTANT_SNAPSHOT_CLAIM_LEASE_SECONDS = 300
ASSISTANT_MAINTENANCE_PHASES = (
    "delete_records_cleanup",
    "snapshot_cleanup",
    "washing_timeout",
    "history_reconcile",
    "moviepilot_subscription_sync",
    "no_download_check",
    "download_check",
    "snapshot_verify",
)

_PHASE_STAT_KEYS = {
    "delete_records_cleanup": "delete_records_cleaned",
    "snapshot_cleanup": "snapshots_cleaned",
    "washing_timeout": "washing_timeouts",
    "history_reconcile": "history_reconciled",
    "moviepilot_subscription_sync": "mp_subscriptions_synced",
    "no_download_check": "no_download_checked",
    "download_check": "download_checked",
    "snapshot_verify": "snapshots_checked",
}


class AssistantMaintenanceClaimLostError(RuntimeError):
    pass


class AssistantMaintenanceProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


class AssistantMaintenanceSession(Protocol):
    def phases(self) -> Sequence[str]: ...

    def run_phase(self, phase: str, checkpoint: Callable[[], None]) -> int: ...


class AssistantMaintenanceGateway(Protocol):
    def begin(
        self,
        processor: object | None,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
    ) -> AssistantMaintenanceSession: ...


@dataclass(frozen=True, slots=True)
class AssistantMaintenanceRun:
    job_id: UUID
    phases: tuple[str, ...]
    completed_phases: tuple[str, ...]
    results: Mapping[str, int]
    completed: bool
    current_attempt_id: UUID

    @property
    def stats(self) -> dict[str, int]:
        stats = {
            "released_pending": 0,
            "download_checked": 0,
            "no_download_checked": 0,
            "mp_subscriptions_synced": 0,
            "washing_timeouts": 0,
            "history_reconciled": 0,
            "snapshots_checked": 0,
            "snapshots_cleaned": 0,
            "delete_records_cleaned": 0,
        }
        for phase, count in self.results.items():
            stat_key = _PHASE_STAT_KEYS.get(phase)
            if stat_key is not None:
                stats[stat_key] = int(count)
        return stats


class SqlAlchemyAssistantMaintenanceRepository:
    """Fence maintenance phase checkpoints to the current persistent Job attempt."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def load_or_create(
        self,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        phases: Sequence[str],
    ) -> AssistantMaintenanceRun:
        normalized_phases = _phases(phases)
        with self._unit_of_work() as uow:
            self._require_current_attempt(uow, job_id, attempt_id, worker_id)
            state_key = _state_key(job_id)
            uow.session.execute(
                text(
                    """
                    DELETE FROM subscribe_assistant_state
                    WHERE subscribe_assistant_state.state_key LIKE :state_prefix
                      AND subscribe_assistant_state.state_key <> :state_key
                      AND subscribe_assistant_state.updated_at
                          < NOW() - make_interval(days => :retention_days)
                      AND NULLIF(subscribe_assistant_state.state_json->>'job_id', '') IS NOT NULL
                      AND (
                          EXISTS (
                              SELECT 1
                              FROM job_runs AS retained_job
                              WHERE retained_job.id::text
                                    = subscribe_assistant_state.state_json->>'job_id'
                                AND retained_job.status IN (
                                    'succeeded', 'failed', 'timed_out', 'cancelled'
                                )
                                AND COALESCE(retained_job.finished_at, retained_job.updated_at)
                                    < NOW() - make_interval(days => :retention_days)
                          )
                          OR NOT EXISTS (
                              SELECT 1
                              FROM job_runs AS retained_job
                              WHERE retained_job.id::text
                                    = subscribe_assistant_state.state_json->>'job_id'
                          )
                      )
                    """
                ),
                {
                    "state_prefix": f"{ASSISTANT_MAINTENANCE_STATE_PREFIX}%",
                    "state_key": state_key,
                    "retention_days": ASSISTANT_MAINTENANCE_RETENTION_DAYS,
                },
            )
            uow.session.execute(
                text(
                    """
                    INSERT INTO subscribe_assistant_state (state_key, state_json, updated_at)
                    VALUES (:state_key, CAST(:state_json AS jsonb), NOW())
                    ON CONFLICT (state_key) DO NOTHING
                    """
                ),
                {
                    "state_key": state_key,
                    "state_json": _state_json(job_id, attempt_id, normalized_phases, (), {}, False),
                },
            )
            row = self._locked_state(uow, state_key)
            run = _run(row, job_id, attempt_id)
            if run.phases != normalized_phases:
                normalized_phases = run.phases
            self._write_state(
                uow,
                state_key,
                _state_json(
                    job_id,
                    attempt_id,
                    normalized_phases,
                    run.completed_phases,
                    run.results,
                    run.completed,
                ),
            )
            uow.commit()
        return AssistantMaintenanceRun(
            job_id,
            normalized_phases,
            run.completed_phases,
            dict(run.results),
            run.completed,
            attempt_id,
        )

    def checkpoint_phase(
        self,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        phase: str,
        count: int,
    ) -> AssistantMaintenanceRun:
        normalized_phase = str(phase or "").strip()
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError("assistant maintenance phase count must be a non-negative integer")
        with self._unit_of_work() as uow:
            self._require_current_attempt(uow, job_id, attempt_id, worker_id)
            state_key = _state_key(job_id)
            run = _run(self._locked_state(uow, state_key), job_id, attempt_id)
            if normalized_phase not in run.phases:
                raise ValueError(f"assistant maintenance phase {normalized_phase!r} is not in the frozen plan")
            completed = list(run.completed_phases)
            results = dict(run.results)
            if normalized_phase not in completed:
                phase_index = run.phases.index(normalized_phase)
                if tuple(completed) != run.phases[:phase_index]:
                    raise AssistantMaintenanceClaimLostError(
                        "assistant maintenance phases are not checkpointed in order"
                    )
                completed.append(normalized_phase)
                results[normalized_phase] = count
            self._write_state(
                uow,
                state_key,
                _state_json(job_id, attempt_id, run.phases, completed, results, False),
            )
            uow.commit()
        return AssistantMaintenanceRun(job_id, run.phases, tuple(completed), results, False, attempt_id)

    def complete(self, job_id: UUID, attempt_id: UUID, worker_id: str) -> AssistantMaintenanceRun:
        with self._unit_of_work() as uow:
            self._require_current_attempt(uow, job_id, attempt_id, worker_id)
            state_key = _state_key(job_id)
            run = _run(self._locked_state(uow, state_key), job_id, attempt_id)
            if run.completed_phases != run.phases:
                raise RuntimeError("assistant maintenance cannot complete before every frozen phase")
            self._write_state(
                uow,
                state_key,
                _state_json(job_id, attempt_id, run.phases, run.completed_phases, run.results, True),
            )
            uow.commit()
        return AssistantMaintenanceRun(
            job_id,
            run.phases,
            run.completed_phases,
            dict(run.results),
            True,
            attempt_id,
        )

    @staticmethod
    def _require_current_attempt(
        uow: SqlAlchemyUnitOfWork,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
    ) -> None:
        normalized_worker = str(worker_id or "").strip()
        if not normalized_worker:
            raise ValueError("assistant maintenance worker_id must not be empty")
        job = (
            uow.session.execute(
                text(
                    """
                    SELECT id, status, current_attempt
                    FROM job_runs
                    WHERE id = :job_id
                    FOR UPDATE
                    """
                ),
                {"job_id": job_id},
            )
            .mappings()
            .one_or_none()
        )
        attempt = (
            uow.session.execute(
                text(
                    """
                    SELECT id, job_id, attempt_no, status, worker_id,
                           lease_expires_at > NOW() AS lease_active
                    FROM job_attempts
                    WHERE id = :attempt_id
                    FOR UPDATE
                    """
                ),
                {"attempt_id": attempt_id},
            )
            .mappings()
            .one_or_none()
        )
        owned = bool(
            job is not None
            and attempt is not None
            and attempt["job_id"] == job_id
            and int(attempt["attempt_no"]) == int(job["current_attempt"])
            and str(job["status"]) in {"running", "cancel_requested"}
            and str(attempt["status"]) == "running"
            and str(attempt["worker_id"] or "") == normalized_worker
            and attempt["lease_active"] is True
        )
        if not owned:
            raise AssistantMaintenanceClaimLostError("assistant maintenance attempt is no longer current")

    @staticmethod
    def _locked_state(uow: SqlAlchemyUnitOfWork, state_key: str) -> Mapping[str, Any]:
        row = (
            uow.session.execute(
                text(
                    """
                    SELECT state_json
                    FROM subscribe_assistant_state
                    WHERE state_key = :state_key
                    FOR UPDATE
                    """
                ),
                {"state_key": state_key},
            )
            .mappings()
            .one_or_none()
        )
        if row is None or not isinstance(row["state_json"], Mapping):
            raise RuntimeError("assistant maintenance state is missing or invalid")
        return dict(row["state_json"])

    @staticmethod
    def _write_state(uow: SqlAlchemyUnitOfWork, state_key: str, state_json: str) -> None:
        updated = uow.session.execute(
            text(
                """
                UPDATE subscribe_assistant_state
                SET state_json = CAST(:state_json AS jsonb), updated_at = NOW()
                WHERE state_key = :state_key
                """
            ),
            {"state_key": state_key, "state_json": state_json},
        )
        if int(getattr(updated, "rowcount", 0) or 0) != 1:
            raise RuntimeError("assistant maintenance state update was lost")


class SubscribeAssistantMaintenanceService:
    def __init__(
        self,
        repository: SqlAlchemyAssistantMaintenanceRepository,
        gateway: AssistantMaintenanceGateway,
    ) -> None:
        self._repository = repository
        self._gateway = gateway

    def execute(
        self,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        processor: object | None,
        progress: AssistantMaintenanceProgress,
    ) -> AssistantMaintenanceRun:
        session = self._gateway.begin(
            processor,
            job_id=job_id,
            attempt_id=attempt_id,
            worker_id=worker_id,
        )
        run = self._repository.load_or_create(job_id, attempt_id, worker_id, session.phases())
        if run.completed:
            return run
        completed = set(run.completed_phases)
        total = max(len(run.phases), 1)
        for index, phase in enumerate(run.phases, 1):
            if phase in completed:
                continue
            progress.checkpoint()
            progress.update(max(1, int((index - 1) / total * 90)), f"订阅助手巡检：{phase}")
            count = session.run_phase(phase, progress.checkpoint)
            run = self._repository.checkpoint_phase(job_id, attempt_id, worker_id, phase, count)
            completed.add(phase)
        progress.update(100, "订阅助手巡检完成")
        return self._repository.complete(job_id, attempt_id, worker_id)


def _state_key(job_id: UUID) -> str:
    return f"{ASSISTANT_MAINTENANCE_STATE_PREFIX}{job_id}"


def _phases(values: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(str(value or "").strip() for value in values)
    if not normalized or any(not value for value in normalized) or len(set(normalized)) != len(normalized):
        raise ValueError("assistant maintenance phases must be unique non-empty names")
    unsupported = set(normalized) - set(ASSISTANT_MAINTENANCE_PHASES)
    if unsupported:
        raise ValueError(f"unsupported assistant maintenance phases: {sorted(unsupported)!r}")
    expected_order = tuple(phase for phase in ASSISTANT_MAINTENANCE_PHASES if phase in normalized)
    if normalized != expected_order:
        raise ValueError("assistant maintenance phases must use the frozen order")
    return normalized


def _state_json(
    job_id: UUID,
    attempt_id: UUID,
    phases: Sequence[str],
    completed_phases: Sequence[str],
    results: Mapping[str, int],
    completed: bool,
) -> str:
    value = {
        "schema_version": 1,
        "job_id": str(job_id),
        "phases": list(phases),
        "completed_phases": list(completed_phases),
        "results": {str(key): int(count) for key, count in results.items()},
        "completed": bool(completed),
        "current_attempt_id": str(attempt_id),
        "updated_at": datetime.now(UTC).isoformat(),
    }
    return json.dumps(
        normalize_json_object(value, field="assistant maintenance state"),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _run(
    value: Mapping[str, Any],
    job_id: UUID,
    attempt_id: UUID,
) -> AssistantMaintenanceRun:
    if int(value.get("schema_version") or 0) != 1 or str(value.get("job_id") or "") != str(job_id):
        raise RuntimeError("assistant maintenance state identity is invalid")
    phases_value = value.get("phases")
    completed_value = value.get("completed_phases")
    results_value = value.get("results")
    if (
        not isinstance(phases_value, list)
        or not isinstance(completed_value, list)
        or not isinstance(results_value, Mapping)
    ):
        raise RuntimeError("assistant maintenance state shape is invalid")
    phases = _phases(tuple(str(item) for item in phases_value))
    completed = tuple(str(item) for item in completed_value)
    if completed != phases[: len(completed)]:
        raise RuntimeError("assistant maintenance completed phases are invalid")
    results: dict[str, int] = {}
    for phase, raw_count in results_value.items():
        if phase not in completed or isinstance(raw_count, bool) or not isinstance(raw_count, int) or raw_count < 0:
            raise RuntimeError("assistant maintenance phase result is invalid")
        results[str(phase)] = raw_count
    return AssistantMaintenanceRun(
        job_id,
        phases,
        completed,
        results,
        bool(value.get("completed")),
        attempt_id,
    )


__all__ = [
    "ASSISTANT_MAINTENANCE_RETENTION_DAYS",
    "ASSISTANT_MAINTENANCE_PHASES",
    "ASSISTANT_MAINTENANCE_STATE_PREFIX",
    "ASSISTANT_SNAPSHOT_CLAIM_LEASE_SECONDS",
    "AssistantMaintenanceClaimLostError",
    "AssistantMaintenanceGateway",
    "AssistantMaintenanceProgress",
    "AssistantMaintenanceRun",
    "AssistantMaintenanceSession",
    "SqlAlchemyAssistantMaintenanceRepository",
    "SubscribeAssistantMaintenanceService",
]
