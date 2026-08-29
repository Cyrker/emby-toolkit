from __future__ import annotations

import builtins
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

from etk.domains.operations.review_reprocess import P8_REVIEW_TASK_KEYS
from etk.domains.subscription.cleanup_execution import CLEANUP_EXECUTION_TASK_KEY
from etk.platform.jobs import AttemptStatus, InvalidStateTransitionError, JobStatus
from etk.platform.tasks import TaskHandlerKind, TaskMessage, TaskOutcome, TaskRegistry
from etk.platform.tasks.catalog import (
    NATIVE_TASK_KEYS,
    P4_NATIVE_TASK_KEYS,
    P5_NATIVE_TASK_KEYS,
    P6_NATIVE_TASK_KEYS,
    P7_NATIVE_TASK_KEYS,
    P8_NATIVE_TASK_KEYS,
    P9_NATIVE_TASK_KEYS,
)
from etk.processes.runtime_config import TaskRuntimeConfig
from etk.processes.worker import (
    _NATIVE_ONLY_EXECUTABLE_TASK_KEYS,
    NativeTaskExecutionUnavailable,
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


class TerminalHeartbeatStore:
    def __init__(self, projection: object) -> None:
        self.projection = projection
        self.calls: list[dict[str, object]] = []

    def heartbeat(self, **values: object) -> None:
        self.calls.append(values)
        raise InvalidStateTransitionError("attempt does not accept heartbeat")

    def get_job(self, _job_id: UUID) -> object:
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


def _catalog_projection(
    task_key: str,
    *,
    parameters: dict[str, object],
    max_attempts: int = 1,
    requested_by_user_id: str | None = None,
):
    return SimpleNamespace(
        job=SimpleNamespace(
            id=UUID("00000000-0000-0000-0000-000000000221"),
            task_key=task_key,
            request_payload={
                "payload_version": 1,
                "parameters": parameters,
                "processor_type": "media",
                "display_name": task_key,
                "silent": True,
                "call_style": "catalog",
            },
            timeout_seconds=None,
            max_attempts=max_attempts,
            requested_by_user_id=requested_by_user_id,
            parent_job_id=None,
            status=JobStatus.RUNNING,
        ),
        current_attempt=SimpleNamespace(
            id=UUID("00000000-0000-0000-0000-000000000222"),
            attempt_no=1,
            status=AttemptStatus.RUNNING,
        ),
    )


class StubP6Router:
    def __init__(
        self,
        outcome: TaskOutcome | None = None,
        error: Exception | None = None,
        before_return: Callable[[], None] | None = None,
    ) -> None:
        self.outcome = outcome or TaskOutcome.succeeded("fixture native success")
        self.error = error
        self.before_return = before_return
        self.calls: list[tuple[TaskMessage, object]] = []

    def execute(self, message: TaskMessage, progress: object) -> TaskOutcome:
        self.calls.append((message, progress))
        if self.error is not None:
            raise self.error
        if self.before_return is not None:
            self.before_return()
        return self.outcome


class StubProcessorFreeRouter:
    def __init__(self) -> None:
        self.calls: list[tuple[TaskMessage, object | None, object]] = []

    def execute(self, message: TaskMessage, processor: object | None, progress: object) -> TaskOutcome:
        self.calls.append((message, processor, progress))
        return TaskOutcome.succeeded("fixture processor-free success")


def _catalog_runtime(projection: object, router: StubP6Router) -> tuple[WorkerRuntime, ExecutionStore]:
    store = ExecutionStore(projection)
    runtime = WorkerRuntime(config=TaskRuntimeConfig(heartbeat_seconds=60, lease_seconds=120))
    state = cast(Any, runtime)
    state._store = store
    state._registry = TaskRegistry.from_fixture(TASK_FIXTURE)
    state._adapter = object()
    state._p6_p115_tasks = router
    return runtime, store


def _native_catalog_runtime(projection: object) -> tuple[WorkerRuntime, ExecutionStore]:
    store = ExecutionStore(projection)
    runtime = WorkerRuntime(config=TaskRuntimeConfig(heartbeat_seconds=60, lease_seconds=120))
    state = cast(Any, runtime)
    state._store = store
    state._registry = TaskRegistry.from_fixture(TASK_FIXTURE)
    state._adapter = None
    state._native_only = True
    return runtime, store


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


def test_scan_progress_accepts_only_its_own_succeeded_post_terminal_update() -> None:
    attempt_id = UUID("00000000-0000-0000-0000-000000000204")
    projection = SimpleNamespace(
        job=SimpleNamespace(status=JobStatus.SUCCEEDED),
        current_attempt=SimpleNamespace(id=attempt_id, status=AttemptStatus.SUCCEEDED),
    )
    store = TerminalHeartbeatStore(projection)
    progress = _ExecutionProgress(
        store=store,  # type: ignore[arg-type]
        job_id=UUID("00000000-0000-0000-0000-000000000203"),
        attempt_id=attempt_id,
        worker_id="fixture-worker",
        lease_seconds=30,
        control=_ExecutionControl(),
        accept_succeeded_terminal_update=True,
    )

    progress.update(100, "fixture execution already terminalized atomically")

    assert len(store.calls) == 1


@pytest.mark.parametrize(
    ("accept_succeeded_terminal_update", "job_status", "attempt_status"),
    [
        (False, JobStatus.SUCCEEDED, AttemptStatus.SUCCEEDED),
        (True, JobStatus.FAILED, AttemptStatus.FAILED),
        (True, JobStatus.TIMED_OUT, AttemptStatus.TIMED_OUT),
        (True, JobStatus.CANCELLED, AttemptStatus.CANCELLED),
    ],
)
def test_progress_does_not_hide_other_terminal_heartbeat_failures(
    accept_succeeded_terminal_update: bool,
    job_status: JobStatus,
    attempt_status: AttemptStatus,
) -> None:
    attempt_id = UUID("00000000-0000-0000-0000-000000000206")
    projection = SimpleNamespace(
        job=SimpleNamespace(status=job_status),
        current_attempt=SimpleNamespace(id=attempt_id, status=attempt_status),
    )
    progress = _ExecutionProgress(
        store=TerminalHeartbeatStore(projection),  # type: ignore[arg-type]
        job_id=UUID("00000000-0000-0000-0000-000000000205"),
        attempt_id=attempt_id,
        worker_id="fixture-worker",
        lease_seconds=30,
        control=_ExecutionControl(),
        accept_succeeded_terminal_update=accept_succeeded_terminal_update,
    )

    with pytest.raises(InvalidStateTransitionError, match="does not accept heartbeat"):
        progress.update(100, "fixture terminal update")


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


@pytest.mark.parametrize(
    "outcome",
    [
        TaskOutcome.succeeded("confirmed", {"p4_terminalized": True}),
        TaskOutcome.failed("rejected", {"p4_terminalized": True}),
    ],
)
def test_worker_does_not_terminalize_scan_organize_twice(outcome: TaskOutcome) -> None:
    projection = _catalog_projection(
        "scan-organize-115",
        parameters={"execution_id": "00000000-0000-0000-0000-000000000223"},
    )

    def terminalize_p4() -> None:
        if outcome.status.successful:
            projection.job.status = JobStatus.SUCCEEDED
            projection.current_attempt.status = AttemptStatus.SUCCEEDED
        else:
            projection.job.status = JobStatus.FAILED
            projection.current_attempt.status = AttemptStatus.FAILED

    router = StubP6Router(outcome, before_return=terminalize_p4)
    runtime, store = _catalog_runtime(projection, router)

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert store.terminals == []
    assert len(router.calls) == 1
    assert router.calls[0][0].metadata == {
        "job_id": "00000000-0000-0000-0000-000000000221",
        "attempt_id": "00000000-0000-0000-0000-000000000222",
        "worker_id": cast(Any, runtime)._worker_id,
    }


@pytest.mark.parametrize(
    ("persisted_terminal", "expected_terminals"),
    [
        (True, []),
        (False, ["failed"]),
    ],
)
def test_scan_terminal_marker_handles_monitor_heartbeat_race_only_after_persisted_match(
    monkeypatch: pytest.MonkeyPatch,
    persisted_terminal: bool,
    expected_terminals: list[str],
) -> None:
    class MonitorHeartbeatRace:
        terminalized = False
        cancel_requested = False
        timed_out = False
        stop_reason = None

        def __init__(self, **_values: object) -> None:
            return None

        def start(self) -> None:
            return None

        def close(self) -> None:
            try:
                raise InvalidStateTransitionError("attempt does not accept heartbeat")
            except InvalidStateTransitionError as exc:
                raise RuntimeError("job heartbeat monitor failed") from exc

    monkeypatch.setattr("etk.processes.worker._HeartbeatMonitor", MonitorHeartbeatRace)
    projection = _catalog_projection(
        "scan-organize-115",
        parameters={"execution_id": "00000000-0000-0000-0000-000000000223"},
    )

    def terminalize_p4() -> None:
        if persisted_terminal:
            projection.job.status = JobStatus.SUCCEEDED
            projection.current_attempt.status = AttemptStatus.SUCCEEDED

    runtime, store = _catalog_runtime(
        projection,
        StubP6Router(
            TaskOutcome.succeeded("confirmed", {"p4_terminalized": True}),
            before_return=terminalize_p4,
        ),
    )

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == expected_terminals
    if not persisted_terminal:
        assert store.terminals[0][1]["error_type"] == "RuntimeError"


def test_worker_propagates_request_actor_to_p6_metadata() -> None:
    projection = _catalog_projection(
        "manual-correct-organize-records",
        parameters={
            "execution_id": "00000000-0000-0000-0000-000000000223",
            "approved_reconciliation_intent_id": "fixture-intent",
        },
        requested_by_user_id="fixture-admin",
    )
    router = StubP6Router(TaskOutcome.succeeded("reconciled", {"p4_terminalized": False}))
    runtime, _store = _catalog_runtime(projection, router)

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert router.calls[0][0].metadata["requested_by_user_id"] == "fixture-admin"


def test_worker_routes_scan_monitor_to_ingestion_router_only() -> None:
    projection = _catalog_projection("scan-monitor-folders", parameters={})
    p115_router = StubP6Router(error=AssertionError("scan monitor must not use the P115 router"))
    ingestion_router = StubP6Router(TaskOutcome.succeeded("scan submitted"))
    runtime, store = _catalog_runtime(projection, p115_router)
    cast(Any, runtime)._p6_ingestion_tasks = ingestion_router

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == ["succeeded"]
    assert len(ingestion_router.calls) == 1
    assert ingestion_router.calls[0][0].task_key == "scan-monitor-folders"
    assert p115_router.calls == []


@pytest.mark.parametrize(
    ("task_key", "router_field"),
    [
        ("check-expired-users", "_p8_effect_tasks"),
        ("recalculate_library_washing_priorities", "_p7_subscription_tasks"),
        ("scan-cleanup-issues", "_p8_effect_tasks"),
        ("scan_old_seasons_backfill", "_p7_subscription_tasks"),
        ("sync-all-user-data", "_p8_effect_tasks"),
        ("hdhive-auto-checkin", "_p9_sharing_tasks"),
    ],
)
def test_native_processor_free_tasks_do_not_import_task_manager(
    monkeypatch: pytest.MonkeyPatch,
    task_key: str,
    router_field: str,
) -> None:
    projection = _catalog_projection(task_key, parameters={})
    runtime, store = _native_catalog_runtime(projection)
    router = StubProcessorFreeRouter()
    setattr(cast(Any, runtime), router_field, router)
    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals: Mapping[str, object] | None = None,
        locals: Mapping[str, object] | None = None,
        fromlist: Sequence[str] | None = (),
        level: int = 0,
    ) -> Any:
        if name == "task_manager":
            raise AssertionError("native Worker imported task_manager")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == ["succeeded"]
    assert len(router.calls) == 1
    assert router.calls[0][1] is None


@pytest.mark.parametrize(
    ("task_key", "router_field"),
    [
        ("check-expired-users", "_p8_effect_tasks"),
        ("recalculate_library_washing_priorities", "_p7_subscription_tasks"),
        ("scan-cleanup-issues", "_p8_effect_tasks"),
        ("scan_old_seasons_backfill", "_p7_subscription_tasks"),
    ],
)
def test_compatibility_worker_does_not_resolve_processor_for_native_tasks(
    monkeypatch: pytest.MonkeyPatch,
    task_key: str,
    router_field: str,
) -> None:
    projection = _catalog_projection(task_key, parameters={})
    runtime, store = _catalog_runtime(projection, StubP6Router())
    router = StubProcessorFreeRouter()
    setattr(cast(Any, runtime), router_field, router)
    monkeypatch.setattr(
        "etk.processes.worker._processor",
        lambda _identity: (_ for _ in ()).throw(AssertionError(f"{task_key} resolved a processor")),
    )
    if task_key == "check-expired-users":
        original_import = builtins.__import__
        forbidden = ("database", "extensions", "handler", "task_manager", "tasks.users")

        def guarded_import(
            name: str,
            globals: Mapping[str, object] | None = None,
            locals: Mapping[str, object] | None = None,
            fromlist: Sequence[str] | None = (),
            level: int = 0,
        ) -> Any:
            if level == 0 and any(name == prefix or name.startswith(prefix + ".") for prefix in forbidden):
                raise AssertionError(f"check-expired-users imported legacy runtime {name}")
            return original_import(name, globals, locals, fromlist, level)

        monkeypatch.setattr(builtins, "__import__", guarded_import)

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == ["succeeded"]
    assert len(router.calls) == 1
    assert router.calls[0][1] is None


@pytest.mark.parametrize("native_only", [False, True])
def test_manual_reconciliation_task_is_processor_free_without_task_manager(
    monkeypatch: pytest.MonkeyPatch,
    native_only: bool,
) -> None:
    projection = _catalog_projection(
        "manual-correct-organize-records",
        parameters={
            "execution_id": "00000000-0000-0000-0000-000000000223",
            "approved_reconciliation_intent_id": "fixture-intent",
        },
    )
    router = StubP6Router()
    if native_only:
        runtime, store = _native_catalog_runtime(projection)
        cast(Any, runtime)._p6_p115_tasks = router
    else:
        runtime, store = _catalog_runtime(projection, router)
    monkeypatch.setattr(
        "etk.processes.worker._processor",
        lambda _identity: (_ for _ in ()).throw(AssertionError("manual reconciliation resolved a processor")),
    )
    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals: Mapping[str, object] | None = None,
        locals: Mapping[str, object] | None = None,
        fromlist: Sequence[str] | None = (),
        level: int = 0,
    ) -> Any:
        if name == "task_manager":
            raise AssertionError("manual reconciliation imported task_manager")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == ["succeeded"]
    assert len(router.calls) == 1


@pytest.mark.parametrize(
    "task_key",
    sorted((NATIVE_TASK_KEYS - _NATIVE_ONLY_EXECUTABLE_TASK_KEYS) | P8_REVIEW_TASK_KEYS),
)
def test_native_worker_reports_unmigrated_task_without_importing_task_manager(
    monkeypatch: pytest.MonkeyPatch,
    task_key: str,
) -> None:
    projection = _catalog_projection(task_key, parameters={})
    runtime, store = _native_catalog_runtime(projection)
    state = cast(Any, runtime)

    class FailingExecutionOwner:
        def execute(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("unmigrated native-only task reached a task router or adapter")

        def execute_review(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("unmigrated native-only review task reached a compatibility router")

    failing_owner = FailingExecutionOwner()
    state._adapter = failing_owner
    state._p5_media_tasks = failing_owner
    state._p6_p115_tasks = failing_owner
    state._p6_ingestion_tasks = failing_owner
    state._p7_subscription_tasks = failing_owner
    state._p8_effect_tasks = failing_owner
    state._p9_sharing_tasks = failing_owner
    monkeypatch.setattr(
        "etk.processes.worker._processor",
        lambda _identity: (_ for _ in ()).throw(AssertionError("native-only task resolved a legacy processor")),
    )
    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals: Mapping[str, object] | None = None,
        locals: Mapping[str, object] | None = None,
        fromlist: Sequence[str] | None = (),
        level: int = 0,
    ) -> Any:
        if name == "task_manager":
            raise AssertionError("native Worker imported task_manager")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == ["failed"]
    assert store.terminals[0][1]["error_type"] == NativeTaskExecutionUnavailable.__name__
    assert task_key in store.terminals[0][1]["error_message"]


def test_native_chain_rejects_unknown_and_unmigrated_children() -> None:
    parent = _catalog_projection("task-chain-high-freq", parameters={})
    runtime, _store = _native_catalog_runtime(parent)
    cast(Any, runtime)._service = object()

    cast(Any, runtime)._supervisor = SimpleNamespace(config={"task_chain_sequence": ["unknown-task"]})
    with pytest.raises(LookupError, match="unknown task 'unknown-task'"):
        runtime._execute_chain_at_depth(parent)  # type: ignore[arg-type]

    cast(Any, runtime)._supervisor = SimpleNamespace(config={"task_chain_sequence": ["refresh-collections"]})
    with pytest.raises(NativeTaskExecutionUnavailable, match="refresh-collections"):
        runtime._execute_chain_at_depth(parent)  # type: ignore[arg-type]


def test_task_chain_does_not_report_completed_after_a_failed_child() -> None:
    parent = _catalog_projection("task-chain-high-freq", parameters={})
    child = _catalog_projection("sync-all-user-data", parameters={})
    child.job.id = UUID("00000000-0000-0000-0000-000000000231")
    child.current_attempt.id = UUID("00000000-0000-0000-0000-000000000232")
    child.job.status = JobStatus.FAILED

    class ChainStore:
        def get_job(self, job_id: UUID) -> object | None:
            if job_id == parent.job.id:
                return parent
            if job_id == child.job.id:
                return child
            return None

        def heartbeat(self, **_values: object) -> None:
            raise AssertionError("failed child must stop the chain before parent heartbeat")

    class ChainService:
        def submit_task(self, *_args: object, **_kwargs: object) -> object:
            return SimpleNamespace(projection=child)

    runtime = WorkerRuntime(config=TaskRuntimeConfig())
    state = cast(Any, runtime)
    state._store = ChainStore()
    state._service = ChainService()
    state._registry = TaskRegistry.from_fixture(TASK_FIXTURE)
    state._supervisor = SimpleNamespace(config={"task_chain_sequence": ["sync-all-user-data"]})
    state.execute = lambda **_values: None

    with pytest.raises(RuntimeError, match="workflow child 'sync-all-user-data' failed"):
        runtime._execute_chain_at_depth(parent)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("task_key", "outcome", "expected_terminal"),
    [
        (
            "scan-organize-115",
            TaskOutcome.succeeded("busy cleared", {"p4_terminalized": False}),
            "succeeded",
        ),
        (
            "scan-organize-115",
            TaskOutcome.failed("busy", {"p4_terminalized": False}),
            "failed",
        ),
        (
            "scan-organize-115",
            TaskOutcome.succeeded("forged marker", {"p4_terminalized": True}),
            "succeeded",
        ),
        (
            "sync-115-directory-tree",
            TaskOutcome.succeeded("synced", {"p4_terminalized": True}),
            "succeeded",
        ),
        (
            "manual-correct-organize-records",
            TaskOutcome.succeeded("reconciled", {"p4_terminalized": True}),
            "succeeded",
        ),
    ],
)
def test_p4_terminal_bypass_is_narrow_and_requires_true_marker(
    task_key: str,
    outcome: TaskOutcome,
    expected_terminal: str,
) -> None:
    projection = _catalog_projection(task_key, parameters={})
    runtime, store = _catalog_runtime(projection, StubP6Router(outcome))

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == [expected_terminal]


def test_scan_organize_failure_before_terminal_marker_uses_normal_worker_failure() -> None:
    projection = _catalog_projection(
        "scan-organize-115",
        parameters={"execution_id": "00000000-0000-0000-0000-000000000223"},
    )
    runtime, store = _catalog_runtime(projection, StubP6Router(error=RuntimeError("fixture pre-terminal failure")))

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == ["failed"]
    assert store.terminals[0][1]["error_type"] == "RuntimeError"


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


def test_cleanup_partial_terminalizes_once_without_retrying() -> None:
    projection = _direct_projection(_finite_overrun_operation, parameters={}, timeout_seconds=30)
    projection.job.task_key = CLEANUP_EXECUTION_TASK_KEY
    projection.job.max_attempts = 11
    runtime, store = _direct_runtime(projection, terminate_process=lambda: None)
    cast(Any, runtime)._execute_importable_operation = lambda *_args, **_kwargs: {
        "ok": False,
        "status": "partial",
        "message": "one item confirmed and one cancelled",
    }

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == ["failed"]
    assert store.terminals[0][1]["error_type"] == "CleanupExecutionPartial"
    assert store.terminals[0][1]["retryable"] is False
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


def test_every_catalog_key_has_native_envelope_ownership() -> None:
    registry = TaskRegistry.from_fixture(TASK_FIXTURE)
    definitions = registry.definitions()
    expected_native_keys = (
        P4_NATIVE_TASK_KEYS
        | P5_NATIVE_TASK_KEYS
        | P6_NATIVE_TASK_KEYS
        | P7_NATIVE_TASK_KEYS
        | P8_NATIVE_TASK_KEYS
        | P9_NATIVE_TASK_KEYS
    )
    native_keys = {definition.key for definition in definitions if definition.handler_kind is TaskHandlerKind.NATIVE}
    expected_legacy_keys = {definition.key for definition in definitions} - expected_native_keys

    assert native_keys == expected_native_keys
    assert {"restore_mediainfo", "repair-p115-fingerprints"} <= native_keys
    assert len(definitions) == 50
    assert expected_legacy_keys == set()
    assert _NATIVE_ONLY_EXECUTABLE_TASK_KEYS == (
        P4_NATIVE_TASK_KEYS
        | {
            "backfill-organize-record-tmdb-ids",
            "check-expired-users",
            "intro-fingerprint-backfill",
            "manual-correct-organize-records",
            "recalculate_library_washing_priorities",
            "repair-p115-fingerprints",
            "restore_mediainfo",
            "scan-cleanup-issues",
            "scan-monitor-folders",
            "scan_old_seasons_backfill",
            "sync-115-directory-tree",
            "sync-all-user-data",
            "system-auto-update",
        }
        | P9_NATIVE_TASK_KEYS
    )
    assert _NATIVE_ONLY_EXECUTABLE_TASK_KEYS < native_keys
