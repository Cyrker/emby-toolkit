from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

from etk.compatibility.legacy_jobs import LegacyTaskAdapter
from etk.platform.jobs import JobStatus
from etk.platform.tasks import ProcessorIdentity, TaskHandlerKind, TaskOutcome, TaskRegistry
from etk.processes.runtime_config import TaskRuntimeConfig
from etk.processes.worker import (
    WorkerRuntime,
    _ExecutionControl,
    _ExecutionProgress,
    _raise_for_reported_failure,
    _result_object,
    _TaskReportedFailure,
)


class HeartbeatStore:
    def __init__(self) -> None:
        self.calls = []

    def heartbeat(self, **values):
        self.calls.append(values)
        return None


class ExecutionStore(HeartbeatStore):
    def __init__(self, projection) -> None:
        super().__init__()
        self.projection = projection
        self.terminals: list[tuple[str, dict]] = []

    def start_attempt(self, **_values):
        return self.projection

    def get_job(self, _job_id):
        return self.projection

    def timeout_attempt(self, **values):
        self.terminals.append(("timeout", values))
        return self.projection

    def cancel_attempt(self, **values):
        self.terminals.append(("cancel", values))
        return self.projection

    def fail_attempt(self, **values):
        self.terminals.append(("failed", values))
        return self.projection

    def succeed_attempt(self, **values):
        self.terminals.append(("succeeded", values))
        return self.projection


_UNCOOPERATIVE_STARTED = Event()
_UNCOOPERATIVE_RELEASE = Event()


def _finite_overrun_operation() -> dict[str, bool]:
    import time

    time.sleep(0.05)
    return {"ok": True}


def _uncooperative_operation(effect: str) -> dict[str, bool]:
    import task_manager

    if effect == "inflight":
        task_manager.record_persistent_effect_started("fixture-provider-write")
    elif effect == "committed":
        task_manager.record_persistent_effect_committed("fixture-provider-write")
    _UNCOOPERATIVE_STARTED.set()
    assert _UNCOOPERATIVE_RELEASE.wait(2), "test did not release the operation"
    return {"ok": True}


REPO_ROOT = Path(__file__).resolve().parents[3]
TASK_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "tasks" / "ordered-task-catalog.json"


def _direct_projection(operation: Callable[..., object], *, parameters: dict, timeout_seconds: float):
    module = str(operation.__module__)
    qualname = str(operation.__qualname__)
    job = SimpleNamespace(
        id=UUID("00000000-0000-0000-0000-000000000211"),
        task_key=f"background-operation:{module}:{qualname}",
        request_payload={
            "payload_version": 1,
            "parameters": parameters,
            "positional_parameters": [],
            "processor_type": "media",
            "display_name": qualname,
            "silent": True,
            "call_style": "direct",
            "legacy_operation": {"module": module, "qualname": qualname},
        },
        timeout_seconds=timeout_seconds,
        max_attempts=3,
        parent_job_id=None,
        status=JobStatus.RUNNING,
    )
    attempt = SimpleNamespace(
        id=UUID("00000000-0000-0000-0000-000000000212"),
        attempt_no=1,
    )
    return SimpleNamespace(job=job, current_attempt=attempt)


def _direct_runtime(
    projection,
    *,
    terminate_process: Callable[[], None],
    termination_grace_seconds: float = 0.01,
):
    class MissingRegistry:
        @staticmethod
        def require(_task_key: str):
            raise KeyError(_task_key)

    store = ExecutionStore(projection)
    runtime = WorkerRuntime(
        config=TaskRuntimeConfig(
            heartbeat_seconds=1,
            lease_seconds=30,
            job_termination_grace_seconds=termination_grace_seconds,
        ),
        terminate_process=terminate_process,
    )
    state = cast(Any, runtime)
    state._store = store
    state._registry = MissingRegistry()
    state._adapter = object()
    return runtime, store


def test_negative_legacy_progress_becomes_persistent_failure_evidence() -> None:
    store = HeartbeatStore()
    progress = _ExecutionProgress(
        store=store,  # type: ignore[arg-type]
        job_id=UUID("00000000-0000-0000-0000-000000000201"),
        attempt_id=UUID("00000000-0000-0000-0000-000000000202"),
        worker_id="fixture-worker",
        lease_seconds=30,
        control=_ExecutionControl(),
    )

    progress.update(-1, "fixture task failed")

    with pytest.raises(_TaskReportedFailure, match="fixture task failed"):
        progress.raise_if_reported_failure()
    assert store.calls[0]["progress"] is None


def test_structured_task_outcomes_have_unambiguous_terminal_meaning() -> None:
    succeeded = TaskOutcome.succeeded("done", {"count": 2})
    skipped = TaskOutcome.skipped("nothing to do")
    partial = TaskOutcome.partial("one item failed", {"failed": 1})
    failed = TaskOutcome.failed("provider unavailable")

    _raise_for_reported_failure("fixture", succeeded)
    _raise_for_reported_failure("fixture", skipped)
    with pytest.raises(_TaskReportedFailure, match="one item failed"):
        _raise_for_reported_failure("fixture", partial)
    with pytest.raises(_TaskReportedFailure, match="provider unavailable"):
        _raise_for_reported_failure("fixture", failed)

    assert _result_object(succeeded) == {
        "ok": True,
        "status": "succeeded",
        "message": "done",
        "details": {"count": 2},
    }
    assert _result_object(skipped)["status"] == "skipped"


def test_mapping_failure_is_generic_and_explicit_skip_is_not_failure() -> None:
    with pytest.raises(_TaskReportedFailure, match="fixture failed"):
        _raise_for_reported_failure("any-catalog-key", {"ok": False, "message": "fixture failed"})
    _raise_for_reported_failure(
        "any-catalog-key",
        {"ok": False, "skipped": True, "message": "business filter skipped"},
    )


def test_finite_direct_overrun_without_effects_cannot_be_marked_succeeded() -> None:
    projection = _direct_projection(_finite_overrun_operation, parameters={}, timeout_seconds=0.01)
    runtime, store = _direct_runtime(
        projection,
        terminate_process=lambda: None,
        termination_grace_seconds=1,
    )

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == ["timeout"]
    assert store.terminals[0][1]["retryable"] is True
    assert store.terminals[0][1]["honor_cancel_request"] is False


@pytest.mark.parametrize(
    ("effect", "terminal", "error_type"),
    [
        ("none", "timeout", None),
        ("inflight", "failed", "PersistentEffectIndeterminate"),
        ("committed", "failed", "PersistentExecutionInterruptedAfterEffect"),
    ],
)
def test_uncooperative_direct_timeout_is_fenced_before_injected_termination(
    effect: str,
    terminal: str,
    error_type: str | None,
) -> None:
    _UNCOOPERATIVE_STARTED.clear()
    _UNCOOPERATIVE_RELEASE.clear()
    termination_observed = Event()
    projection = _direct_projection(
        _uncooperative_operation,
        parameters={"effect": effect},
        timeout_seconds=0.01,
    )
    runtime, store = _direct_runtime(
        projection,
        terminate_process=lambda: termination_observed.set(),
    )
    failures: list[BaseException] = []

    def execute() -> None:
        try:
            runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]
        except BaseException as exc:
            failures.append(exc)

    execution = Thread(target=execute, daemon=True)
    try:
        execution.start()
        assert _UNCOOPERATIVE_STARTED.wait(1)
        assert termination_observed.wait(1)
        assert store.terminals and store.terminals[0][0] == terminal
        if error_type is not None:
            assert store.terminals[0][1]["error_type"] == error_type
            assert store.terminals[0][1]["retryable"] is False
        assert all(status != "succeeded" for status, _values in store.terminals)
    finally:
        _UNCOOPERATIVE_RELEASE.set()
        execution.join(1)

    assert not execution.is_alive()
    assert failures == []


def test_every_legacy_catalog_key_routes_negative_failure_evidence_to_worker_terminal_contract() -> None:
    import task_manager

    registry = TaskRegistry.from_fixture(TASK_FIXTURE)

    class Resolver:
        @staticmethod
        def resolve(
            task_key: str,
            processor: ProcessorIdentity,
        ) -> tuple[Callable[..., object], object]:
            del task_key, processor

            def fail(_runtime_processor, **_parameters):
                task_manager.update_status_from_thread(-1, "fixture catalog failure")

            return fail, object()

    adapter = LegacyTaskAdapter(registry, Resolver())
    checked = []
    for index, definition in enumerate(registry.definitions(), start=1):
        if definition.handler_kind is TaskHandlerKind.NATIVE:
            continue
        progress = _ExecutionProgress(
            store=HeartbeatStore(),  # type: ignore[arg-type]
            job_id=UUID(int=index),
            attempt_id=UUID(int=1000 + index),
            worker_id="fixture-worker",
            lease_seconds=30,
            control=_ExecutionControl(),
        )
        message = registry.create_message(definition.key, {})
        with task_manager.persistent_execution(progress):
            adapter.execute(message)
        with pytest.raises(_TaskReportedFailure, match="fixture catalog failure"):
            progress.raise_if_reported_failure()
        checked.append(definition.key)

    assert len(checked) == 42
