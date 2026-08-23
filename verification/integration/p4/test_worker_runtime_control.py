from __future__ import annotations

import time
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
from typing import Any, cast

import pytest

from etk.domains.operations.jobs import JobSubmissionService
from etk.platform.jobs import AttemptStatus, JobStatus, PostgresJobStore
from etk.platform.tasks import TaskOutcome, TaskRegistry
from etk.processes import worker as worker_module
from etk.processes.runtime_config import TaskRuntimeConfig
from etk.processes.worker import WorkerRuntime

REPO_ROOT = Path(__file__).resolve().parents[3]
TASK_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "tasks" / "ordered-task-catalog.json"
_UNCOOPERATIVE_RELEASE = Event()


def _direct_control_operation(marker: str, mode: str) -> dict[str, object]:
    import task_manager

    path = Path(marker)
    path.with_suffix(".started").write_text("started", encoding="utf-8")
    if mode.startswith("ignore-control"):
        if mode == "ignore-control-inflight":
            task_manager.record_persistent_effect_started("fixture-provider-write")
        elif mode == "ignore-control-committed":
            task_manager.record_persistent_effect_committed("fixture-provider-write")
        assert _UNCOOPERATIVE_RELEASE.wait(10), "fixture operation was not released"
        return {"ok": True, "ignored_control": True}
    if mode == "finite-overrun":
        time.sleep(1.1)
        return {"ok": True, "overran": True}
    if mode == "committed-checkpoint":
        task_manager.record_persistent_effect_committed("fixture-provider-write")
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        state = task_manager.persistent_execution_control_state()
        if state["cancel_requested"] or state["timed_out"]:
            path.with_suffix(".observed").write_text("observed", encoding="utf-8")
            if mode == "complete-after-control":
                return {"ok": True, "control_observed": True}
            task_manager.persistent_execution_checkpoint()
        time.sleep(0.02)
    raise TimeoutError("fixture direct operation did not receive execution control")


class CooperativeProcessor:
    def __init__(self) -> None:
        self.stop = Event()

    def signal_stop(self) -> None:
        self.stop.set()

    def clear_stop_signal(self) -> None:
        self.stop.clear()

    def is_stop_requested(self) -> bool:
        return self.stop.is_set()


class BlockingAdapter:
    def __init__(self, processor: CooperativeProcessor, *, return_after: float | None = None) -> None:
        self.processor = processor
        self.return_after = return_after
        self.started = Event()
        self.stop_observed = Event()

    def execute(self, _message: object) -> dict[str, bool]:
        self.started.set()
        if self.return_after is not None:
            time.sleep(self.return_after)
        else:
            assert self.processor.stop.wait(10), "operation did not receive a cooperative stop"
        if self.processor.is_stop_requested():
            self.stop_observed.set()
        return {"stop_observed": self.stop_observed.is_set()}


def _runtime(
    store: PostgresJobStore,
    adapter: BlockingAdapter,
    *,
    heartbeat_seconds: int,
    supervisor_config: dict[str, Any] | None = None,
    termination_grace_seconds: float = 1,
    terminate_process: Any = None,
) -> tuple[WorkerRuntime, JobSubmissionService]:
    class NativeMediaRouter:
        @staticmethod
        def execute(message: object, _processor: object) -> TaskOutcome:
            return TaskOutcome.succeeded("fixture native media task completed", adapter.execute(message))

    registry = TaskRegistry.from_fixture(TASK_FIXTURE)
    service = JobSubmissionService(store, registry)
    runtime = WorkerRuntime(
        config=TaskRuntimeConfig(
            heartbeat_seconds=heartbeat_seconds,
            lease_seconds=max(30, heartbeat_seconds * 3),
            job_termination_grace_seconds=termination_grace_seconds,
        ),
        sleep=lambda _seconds: None,
        terminate_process=terminate_process or (lambda: None),
    )
    state = cast(Any, runtime)
    state._started = True
    state._store = store
    state._registry = registry
    state._service = service
    state._adapter = adapter
    state._p5_media_tasks = NativeMediaRouter()
    state._supervisor = SimpleNamespace(config=supervisor_config or {})
    state._refresh_settings = lambda: None
    return runtime, service


def test_deadline_shorter_than_heartbeat_still_times_out_the_attempt(
    p4_store: PostgresJobStore,
    monkeypatch,
) -> None:
    processor = CooperativeProcessor()
    adapter = BlockingAdapter(processor, return_after=1.2)
    runtime, service = _runtime(p4_store, adapter, heartbeat_seconds=5)
    monkeypatch.setattr(worker_module, "_processor", lambda _identity: processor)
    submitted = service.submit_task("populate-metadata", {}, timeout_seconds=1)

    runtime.execute(
        event_id="inline-test",
        job_id=str(submitted.projection.job.id),
        attempt_id=str(submitted.projection.current_attempt.id),
    )

    completed = p4_store.get_job(submitted.projection.job.id)
    assert completed is not None
    assert adapter.stop_observed.is_set()
    assert completed.job.status is JobStatus.TIMED_OUT
    assert completed.current_attempt.status is AttemptStatus.TIMED_OUT


def test_cancelling_native_workflow_parent_stops_the_running_inline_child(
    p4_store: PostgresJobStore,
    monkeypatch,
) -> None:
    processor = CooperativeProcessor()
    adapter = BlockingAdapter(processor)
    runtime, service = _runtime(
        p4_store,
        adapter,
        heartbeat_seconds=1,
        supervisor_config={
            "task_chain_sequence": ["populate-metadata"],
            "task_chain_max_runtime_minutes": 0,
        },
    )
    monkeypatch.setattr(worker_module, "_processor", lambda _identity: processor)
    parent = service.submit_task("task-chain-high-freq", {})
    execution = Thread(
        target=runtime.execute,
        kwargs={
            "event_id": "inline-test",
            "job_id": str(parent.projection.job.id),
            "attempt_id": str(parent.projection.current_attempt.id),
        },
        daemon=True,
    )
    execution.start()
    assert adapter.started.wait(5), "workflow child did not start"

    requested = p4_store.request_cancel(parent.projection.job.id)
    assert requested.job.status is JobStatus.CANCEL_REQUESTED
    assert adapter.stop_observed.wait(3), "workflow child did not observe parent cancellation"
    execution.join(3)
    assert not execution.is_alive()

    completed_parent = p4_store.get_job(parent.projection.job.id)
    children = p4_store.list_children(parent.projection.job.id)
    assert completed_parent is not None
    assert len(children) == 1
    assert children[0].job.status is JobStatus.CANCELLED
    assert children[0].current_attempt.status is AttemptStatus.CANCELLED
    assert completed_parent.job.status is JobStatus.CANCELLED
    assert completed_parent.current_attempt.status is AttemptStatus.CANCELLED


def test_direct_operation_late_success_without_effects_does_not_override_cancel(
    p4_store: PostgresJobStore,
    tmp_path: Path,
) -> None:
    processor = CooperativeProcessor()
    adapter = BlockingAdapter(processor, return_after=0)
    runtime, service = _runtime(p4_store, adapter, heartbeat_seconds=1)
    marker = tmp_path / "direct-success"
    submitted = service.submit_background_operation(
        _direct_control_operation,
        (str(marker), "complete-after-control"),
        {},
        max_attempts=3,
    )
    execution = Thread(
        target=runtime.execute,
        kwargs={
            "event_id": "inline-test",
            "job_id": str(submitted.projection.job.id),
            "attempt_id": str(submitted.projection.current_attempt.id),
        },
        daemon=True,
    )
    execution.start()
    assert _wait_for_path(marker.with_suffix(".started"))
    requested = p4_store.request_cancel(submitted.projection.job.id)
    assert requested.job.status is JobStatus.CANCEL_REQUESTED
    execution.join(5)
    assert not execution.is_alive()

    completed = p4_store.get_job(submitted.projection.job.id)
    assert completed is not None
    assert completed.job.status is JobStatus.CANCELLED
    assert completed.current_attempt.status is AttemptStatus.CANCELLED
    assert completed.job.result is None


def test_direct_operation_stops_at_pre_effect_checkpoint(
    p4_store: PostgresJobStore,
    tmp_path: Path,
) -> None:
    processor = CooperativeProcessor()
    adapter = BlockingAdapter(processor, return_after=0)
    runtime, service = _runtime(p4_store, adapter, heartbeat_seconds=1)
    marker = tmp_path / "direct-cancel"
    submitted = service.submit_background_operation(
        _direct_control_operation,
        (str(marker), "checkpoint"),
        {},
        max_attempts=3,
    )
    execution = Thread(
        target=runtime.execute,
        kwargs={
            "event_id": "inline-test",
            "job_id": str(submitted.projection.job.id),
            "attempt_id": str(submitted.projection.current_attempt.id),
        },
        daemon=True,
    )
    execution.start()
    assert _wait_for_path(marker.with_suffix(".started"))
    p4_store.request_cancel(submitted.projection.job.id)
    execution.join(5)
    assert not execution.is_alive()

    completed = p4_store.get_job(submitted.projection.job.id)
    assert completed is not None
    assert completed.job.status is JobStatus.CANCELLED
    assert completed.current_attempt.status is AttemptStatus.CANCELLED


def test_direct_operation_stops_at_pre_effect_deadline(
    p4_store: PostgresJobStore,
    tmp_path: Path,
) -> None:
    processor = CooperativeProcessor()
    adapter = BlockingAdapter(processor, return_after=0)
    runtime, service = _runtime(p4_store, adapter, heartbeat_seconds=5)
    marker = tmp_path / "direct-timeout"
    submitted = service.submit_background_operation(
        _direct_control_operation,
        (str(marker), "checkpoint"),
        {},
        max_attempts=1,
        timeout_seconds=1,
    )

    runtime.execute(
        event_id="inline-test",
        job_id=str(submitted.projection.job.id),
        attempt_id=str(submitted.projection.current_attempt.id),
    )

    completed = p4_store.get_job(submitted.projection.job.id)
    assert completed is not None
    assert completed.job.status is JobStatus.TIMED_OUT
    assert completed.current_attempt.status is AttemptStatus.TIMED_OUT
    assert marker.with_suffix(".observed").read_text(encoding="utf-8") == "observed"


def test_direct_operation_interrupted_after_effect_fails_without_retry(
    p4_store: PostgresJobStore,
    tmp_path: Path,
) -> None:
    processor = CooperativeProcessor()
    adapter = BlockingAdapter(processor, return_after=0)
    runtime, service = _runtime(p4_store, adapter, heartbeat_seconds=1)
    marker = tmp_path / "direct-indeterminate"
    submitted = service.submit_background_operation(
        _direct_control_operation,
        (str(marker), "committed-checkpoint"),
        {},
        max_attempts=3,
    )
    execution = Thread(
        target=runtime.execute,
        kwargs={
            "event_id": "inline-test",
            "job_id": str(submitted.projection.job.id),
            "attempt_id": str(submitted.projection.current_attempt.id),
        },
        daemon=True,
    )
    execution.start()
    assert _wait_for_path(marker.with_suffix(".started"))
    p4_store.request_cancel(submitted.projection.job.id)
    execution.join(5)
    assert not execution.is_alive()

    completed = p4_store.get_job(submitted.projection.job.id)
    assert completed is not None
    assert completed.job.status is JobStatus.FAILED
    assert completed.current_attempt.status is AttemptStatus.FAILED
    assert completed.current_attempt.retryable is False
    assert completed.current_attempt.error_type == "PersistentExecutionInterruptedAfterEffect"


def test_direct_finite_overrun_without_effects_is_timed_out_not_succeeded(
    p4_store: PostgresJobStore,
    tmp_path: Path,
) -> None:
    processor = CooperativeProcessor()
    adapter = BlockingAdapter(processor, return_after=0)
    runtime, service = _runtime(
        p4_store,
        adapter,
        heartbeat_seconds=5,
        termination_grace_seconds=1,
    )
    marker = tmp_path / "finite-overrun"
    submitted = service.submit_background_operation(
        _direct_control_operation,
        (str(marker), "finite-overrun"),
        {},
        max_attempts=1,
        timeout_seconds=1,
    )

    runtime.execute(
        event_id="inline-test",
        job_id=str(submitted.projection.job.id),
        attempt_id=str(submitted.projection.current_attempt.id),
    )

    completed = p4_store.get_job(submitted.projection.job.id)
    assert completed is not None
    assert completed.job.status is JobStatus.TIMED_OUT
    assert completed.current_attempt.status is AttemptStatus.TIMED_OUT
    assert completed.job.result is None


def test_uncooperative_pre_effect_timeout_is_fenced_then_terminated_and_retryable(
    p4_store: PostgresJobStore,
    tmp_path: Path,
) -> None:
    _UNCOOPERATIVE_RELEASE.clear()
    terminated = Event()
    processor = CooperativeProcessor()
    adapter = BlockingAdapter(processor, return_after=0)
    runtime, service = _runtime(
        p4_store,
        adapter,
        heartbeat_seconds=5,
        termination_grace_seconds=0.05,
        terminate_process=terminated.set,
    )
    marker = tmp_path / "uncooperative-timeout"
    submitted = service.submit_background_operation(
        _direct_control_operation,
        (str(marker), "ignore-control"),
        {},
        max_attempts=2,
        timeout_seconds=1,
    )
    execution = Thread(
        target=runtime.execute,
        kwargs={
            "event_id": "inline-test",
            "job_id": str(submitted.projection.job.id),
            "attempt_id": str(submitted.projection.current_attempt.id),
        },
        daemon=True,
    )
    try:
        execution.start()
        assert _wait_for_path(marker.with_suffix(".started"))
        assert terminated.wait(3), "attempt terminator was not called"

        completed = p4_store.get_job(submitted.projection.job.id)
        assert completed is not None
        assert completed.job.status is JobStatus.RETRY_WAIT
        assert completed.current_attempt.status is AttemptStatus.TIMED_OUT
        assert completed.current_attempt.retryable is True
        assert completed.current_attempt.lease_expires_at is None
    finally:
        _UNCOOPERATIVE_RELEASE.set()
        execution.join(3)
    assert not execution.is_alive()


@pytest.mark.parametrize(
    ("mode", "error_type"),
    [
        ("ignore-control-inflight", "PersistentEffectIndeterminate"),
        ("ignore-control-committed", "PersistentExecutionInterruptedAfterEffect"),
    ],
)
def test_uncooperative_effect_timeout_is_non_retryable_before_termination(
    mode: str,
    error_type: str,
    p4_store: PostgresJobStore,
    tmp_path: Path,
) -> None:
    _UNCOOPERATIVE_RELEASE.clear()
    terminated = Event()
    processor = CooperativeProcessor()
    adapter = BlockingAdapter(processor, return_after=0)
    runtime, service = _runtime(
        p4_store,
        adapter,
        heartbeat_seconds=5,
        termination_grace_seconds=0.05,
        terminate_process=terminated.set,
    )
    marker = tmp_path / mode
    submitted = service.submit_background_operation(
        _direct_control_operation,
        (str(marker), mode),
        {},
        max_attempts=3,
        timeout_seconds=1,
    )
    execution = Thread(
        target=runtime.execute,
        kwargs={
            "event_id": "inline-test",
            "job_id": str(submitted.projection.job.id),
            "attempt_id": str(submitted.projection.current_attempt.id),
        },
        daemon=True,
    )
    try:
        execution.start()
        assert _wait_for_path(marker.with_suffix(".started"))
        assert terminated.wait(3), "attempt terminator was not called"

        completed = p4_store.get_job(submitted.projection.job.id)
        assert completed is not None
        assert completed.job.status is JobStatus.FAILED
        assert completed.current_attempt.status is AttemptStatus.FAILED
        assert completed.current_attempt.retryable is False
        assert completed.current_attempt.error_type == error_type
        assert completed.current_attempt.lease_expires_at is None
    finally:
        _UNCOOPERATIVE_RELEASE.set()
        execution.join(3)
    assert not execution.is_alive()


def test_uncooperative_cancel_is_persisted_before_terminating_worker(
    p4_store: PostgresJobStore,
    tmp_path: Path,
) -> None:
    _UNCOOPERATIVE_RELEASE.clear()
    terminated = Event()
    processor = CooperativeProcessor()
    adapter = BlockingAdapter(processor, return_after=0)
    runtime, service = _runtime(
        p4_store,
        adapter,
        heartbeat_seconds=1,
        termination_grace_seconds=0.05,
        terminate_process=terminated.set,
    )
    marker = tmp_path / "uncooperative-cancel"
    submitted = service.submit_background_operation(
        _direct_control_operation,
        (str(marker), "ignore-control"),
        {},
        max_attempts=3,
    )
    execution = Thread(
        target=runtime.execute,
        kwargs={
            "event_id": "inline-test",
            "job_id": str(submitted.projection.job.id),
            "attempt_id": str(submitted.projection.current_attempt.id),
        },
        daemon=True,
    )
    try:
        execution.start()
        assert _wait_for_path(marker.with_suffix(".started"))
        p4_store.request_cancel(submitted.projection.job.id)
        assert terminated.wait(3), "attempt terminator was not called"

        completed = p4_store.get_job(submitted.projection.job.id)
        assert completed is not None
        assert completed.job.status is JobStatus.CANCELLED
        assert completed.current_attempt.status is AttemptStatus.CANCELLED
        assert completed.current_attempt.lease_expires_at is None
    finally:
        _UNCOOPERATIVE_RELEASE.set()
        execution.join(3)
    assert not execution.is_alive()


def test_uncooperative_process_shutdown_is_persisted_before_terminating_worker(
    p4_store: PostgresJobStore,
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(worker_module, "_PROCESS_SHUTDOWN_REQUESTED", False)
    _UNCOOPERATIVE_RELEASE.clear()
    terminated = Event()
    processor = CooperativeProcessor()
    adapter = BlockingAdapter(processor, return_after=0)
    runtime, service = _runtime(
        p4_store,
        adapter,
        heartbeat_seconds=1,
        termination_grace_seconds=0.05,
        terminate_process=terminated.set,
    )
    marker = tmp_path / "uncooperative-shutdown"
    submitted = service.submit_background_operation(
        _direct_control_operation,
        (str(marker), "ignore-control"),
        {},
        max_attempts=3,
    )
    execution = Thread(
        target=runtime.execute,
        kwargs={
            "event_id": "inline-test",
            "job_id": str(submitted.projection.job.id),
            "attempt_id": str(submitted.projection.current_attempt.id),
        },
        daemon=True,
    )
    try:
        execution.start()
        assert _wait_for_path(marker.with_suffix(".started"))
        worker_module.request_worker_shutdown()
        assert terminated.wait(3), "attempt terminator was not called"

        completed = p4_store.get_job(submitted.projection.job.id)
        assert completed is not None
        assert completed.job.status is JobStatus.CANCELLED
        assert completed.current_attempt.status is AttemptStatus.CANCELLED
        assert completed.current_attempt.lease_expires_at is None
    finally:
        _UNCOOPERATIVE_RELEASE.set()
        execution.join(3)
    assert not execution.is_alive()


def _wait_for_path(path: Path, timeout: float = 5) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return True
        time.sleep(0.02)
    return path.exists()
