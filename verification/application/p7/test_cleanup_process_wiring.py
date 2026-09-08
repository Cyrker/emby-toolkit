from __future__ import annotations

from collections.abc import Iterator
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

import task_manager
from etk.domains.subscription.cleanup_execution import CleanupExecutionService, cleanup_job_max_attempts
from etk.domains.subscription.cleanup_repository import SqlAlchemyCleanupRepository
from etk.platform.jobs import JobStatus
from etk.processes.p7_cleanup import clear_cleanup_executor, execute_cleanup_job, install_cleanup_executor
from etk.processes.p7_resubscribe_cleanup import PersistentResubscribeCleanupGateway
from etk.processes.runtime_config import TaskRuntimeConfig
from etk.processes.worker import WorkerRuntime, _ExecutionControl, _ExecutionProgress, _HeartbeatMonitor


class _PersistentProgress:
    def __init__(self, job_id: UUID, attempt_id: UUID, worker_id: str) -> None:
        self.job_id = job_id
        self.attempt_id = attempt_id
        self.worker_id = worker_id

    def update(self, progress: int, message: str) -> None:
        del progress, message

    def checkpoint(self) -> None:
        return None

    def record_started_effect(self, label: str) -> None:
        del label

    def record_committed_effect(self, label: str) -> None:
        del label

    def record_abandoned_effect(self, label: str) -> None:
        del label

    def control_state(self) -> dict[str, Any]:
        return {"cancel_requested": False, "timed_out": False}


class _CleanupExecutor:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def execute(self, **values: Any) -> dict[str, Any]:
        progress = values.pop("progress")
        self.calls.append({**values, "control": dict(progress.control_state())})
        return {"ok": True, "status": "confirmed", "durable_effect_started": True}


class _AttemptStore:
    def __init__(self) -> None:
        self.failed: list[dict[str, Any]] = []
        self.cancelled: list[dict[str, Any]] = []

    def fail_attempt(self, **values: Any) -> None:
        self.failed.append(dict(values))

    def cancel_attempt(self, **values: Any) -> None:
        self.cancelled.append(dict(values))


class _CleanupStagingRepository:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def stage_version_deletion(self, emby_id: str, **values: Any) -> Any:
        self.calls.append({"emby_id": emby_id, **values})
        return SimpleNamespace(
            job_id=uuid4(),
            execution_key="a" * 64,
            item_count=1,
            reused=False,
            resumed=False,
        )


class _ParentCancellationStore:
    def __init__(self, child: Any, parent: Any, cancelled: Any) -> None:
        self.child = child
        self.parent = parent
        self.cancelled = cancelled
        self.cancel_calls: list[UUID] = []
        self.claim_calls: list[dict[str, Any]] = []

    def execution_guard(self, job_id: UUID) -> Any:
        assert job_id == self.child.job.id
        return nullcontext(True)

    def get_job(self, job_id: UUID) -> Any:
        return self.parent if job_id == self.parent.job.id else self.child

    def request_cancel(self, job_id: UUID) -> Any:
        self.cancel_calls.append(job_id)
        return self.cancelled

    def claim_attempt(self, **values: Any) -> Any:
        self.claim_calls.append(dict(values))
        return SimpleNamespace(claimed=True, projection=self.cancelled)


@pytest.fixture(autouse=True)
def _cleanup_executor_state() -> Iterator[None]:
    clear_cleanup_executor()
    yield
    clear_cleanup_executor()


def test_cleanup_direct_callable_requires_and_forwards_persistent_identity() -> None:
    with pytest.raises(RuntimeError, match="persistent Worker context"):
        execute_cleanup_job(str(uuid4()), "a" * 64)

    executor = _CleanupExecutor()
    install_cleanup_executor(cast(CleanupExecutionService, executor))
    job_id = uuid4()
    attempt_id = uuid4()
    progress = _PersistentProgress(job_id, attempt_id, "worker-a")
    with task_manager.persistent_execution(progress):
        with pytest.raises(RuntimeError, match="does not match"):
            execute_cleanup_job(str(uuid4()), "a" * 64)
        result = execute_cleanup_job(str(job_id), "a" * 64)

    assert result["status"] == "confirmed"
    assert executor.calls == [
        {
            "job_id": job_id,
            "execution_key": "a" * 64,
            "attempt_id": attempt_id,
            "worker_id": "worker-a",
            "control": {"cancel_requested": False, "timed_out": False},
        }
    ]


def test_resubscribe_cleanup_stages_a_child_from_explicit_native_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _CleanupStagingRepository()
    gateway = PersistentResubscribeCleanupGateway(cast(SqlAlchemyCleanupRepository, repository))
    job_id = uuid4()
    progress = _PersistentProgress(job_id, uuid4(), "worker-a")
    monkeypatch.setattr(
        task_manager,
        "current_persistent_execution_identity",
        lambda: (_ for _ in ()).throw(AssertionError("native cleanup imported legacy execution state")),
    )

    result = gateway.stage_version_deletion("emby-old", progress=progress)

    assert result["item_count"] == 1
    assert repository.calls == [
        {
            "emby_id": "emby-old",
            "parent_job_id": job_id,
            "parent_attempt_id": progress.attempt_id,
            "parent_worker_id": "worker-a",
        }
    ]


def test_resubscribe_cleanup_preserves_compatibility_execution_binding() -> None:
    repository = _CleanupStagingRepository()
    gateway = PersistentResubscribeCleanupGateway(cast(SqlAlchemyCleanupRepository, repository))
    progress = _PersistentProgress(uuid4(), uuid4(), "worker-a")

    with task_manager.persistent_execution(progress):
        result = gateway.stage_version_deletion("emby-old")

    assert result["item_count"] == 1
    assert repository.calls[0]["parent_job_id"] == progress.job_id


def test_cleanup_job_attempt_capacity_covers_each_item_recovery_budget() -> None:
    assert cleanup_job_max_attempts(1) == 11
    assert cleanup_job_max_attempts(2) == 21
    assert cleanup_job_max_attempts(2, recovery_budget=1) == 5


def test_hard_cancel_after_cleanup_checkpoint_schedules_readback_recovery() -> None:
    store = _AttemptStore()
    job_id = uuid4()
    attempt_id = uuid4()
    recovered: list[UUID] = []
    monitor = _HeartbeatMonitor(
        store=cast(Any, store),
        projection=cast(
            Any,
            SimpleNamespace(
                job=SimpleNamespace(
                    id=job_id,
                    max_attempts=11,
                    timeout_seconds=3600,
                    parent_job_id=None,
                ),
                current_attempt=SimpleNamespace(id=attempt_id, attempt_no=1),
            ),
        ),
        worker_id="worker-a",
        config=TaskRuntimeConfig(),
        processor=None,
        control=_ExecutionControl(),
        terminate_process=lambda: None,
        prepare_post_effect_cancellation=lambda value: recovered.append(value) is None,
    )
    monitor.stop_reason = "cancel"

    monitor._terminalize_controlled_attempt()

    assert recovered == [job_id]
    assert store.cancelled == []
    assert len(store.failed) == 1
    assert store.failed[0]["job_id"] == job_id
    assert store.failed[0]["attempt_id"] == attempt_id
    assert store.failed[0]["retryable"] is True
    assert store.failed[0]["honor_cancel_request"] is False


@pytest.mark.parametrize(
    ("cancelled_status", "expected_claims"),
    [(JobStatus.CANCELLED, 0), (JobStatus.QUEUED, 1)],
)
def test_parent_cancelled_child_continues_only_when_recovery_policy_preserves_queue(
    cancelled_status: JobStatus,
    expected_claims: int,
) -> None:
    child_id = uuid4()
    parent_id = uuid4()
    attempt_id = uuid4()
    child = SimpleNamespace(
        job=SimpleNamespace(id=child_id, parent_job_id=parent_id, status=JobStatus.QUEUED),
        current_attempt=SimpleNamespace(id=attempt_id),
    )
    parent = SimpleNamespace(
        job=SimpleNamespace(id=parent_id, parent_job_id=None, status=JobStatus.CANCELLED),
        current_attempt=SimpleNamespace(id=uuid4()),
    )
    cancelled = SimpleNamespace(
        job=SimpleNamespace(id=child_id, parent_job_id=parent_id, status=cancelled_status),
        current_attempt=SimpleNamespace(id=attempt_id),
    )
    store = _ParentCancellationStore(child, parent, cancelled)
    runtime = WorkerRuntime()
    state = cast(Any, runtime)
    state._started = True
    state._store = store
    executions: list[Any] = []
    state._execute_claimed = executions.append

    runtime.execute(event_id=str(uuid4()), job_id=str(child_id), attempt_id=str(attempt_id))

    assert store.cancel_calls == [child_id]
    assert len(store.claim_calls) == expected_claims
    assert len(executions) == expected_claims


def test_recovery_cleanup_reaches_native_executor_before_parent_cancel_checkpoint() -> None:
    executor = _CleanupExecutor()
    child_id = uuid4()
    parent_id = uuid4()
    attempt_id = uuid4()
    child = SimpleNamespace(
        job=SimpleNamespace(id=child_id, parent_job_id=parent_id, status=JobStatus.RUNNING),
        current_attempt=SimpleNamespace(id=attempt_id),
    )
    parent = SimpleNamespace(
        job=SimpleNamespace(id=parent_id, parent_job_id=None, status=JobStatus.CANCELLED),
        current_attempt=SimpleNamespace(id=uuid4()),
    )
    store = _ParentCancellationStore(child, parent, child)
    runtime = WorkerRuntime()
    cast(Any, runtime)._p7_cleanup_executor = executor
    cast(Any, runtime)._worker_id = "worker-a"
    progress = _ExecutionProgress(
        store=cast(Any, store),
        job_id=child_id,
        attempt_id=attempt_id,
        worker_id="worker-a",
        lease_seconds=60,
        control=_ExecutionControl(),
        task_key="p7-cleanup-execution",
    )

    result = runtime._execute_cleanup_task(
        {
            "call_style": "direct",
            "native_task": "p7-cleanup-execution",
            "positional_parameters": [],
            "parameters": {"job_id": str(child_id), "execution_key": "a" * 64},
        },
        job_id=child_id,
        attempt_id=attempt_id,
        progress=progress,
    )

    assert result["status"] == "confirmed"
    assert len(executor.calls) == 1
