from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

from etk.platform.jobs import JobStatus
from etk.platform.tasks import TaskMessage, TaskOutcome, TaskRegistry
from etk.processes import worker as worker_module
from etk.processes.runtime_config import TaskRuntimeConfig
from etk.processes.worker import WorkerRuntime

REPO_ROOT = Path(__file__).resolve().parents[3]
TASK_FIXTURE = REPO_ROOT / "verification/fixtures/tasks/ordered-task-catalog.json"


class ExecutionStore:
    def __init__(self, projection: Any) -> None:
        self.projection = projection
        self.terminals: list[tuple[str, dict[str, Any]]] = []

    def start_attempt(self, **_values: Any) -> Any:
        return self.projection

    def get_job(self, _job_id: UUID) -> Any:
        return self.projection

    def heartbeat(self, **_values: Any) -> Any:
        return SimpleNamespace(cancel_requested=False)

    def succeed_attempt(self, **values: Any) -> Any:
        self.terminals.append(("succeeded", values))
        return self.projection

    def fail_attempt(self, **values: Any) -> Any:
        self.terminals.append(("failed", values))
        return self.projection

    def timeout_attempt(self, **values: Any) -> Any:
        self.terminals.append(("timeout", values))
        return self.projection

    def cancel_attempt(self, **values: Any) -> Any:
        self.terminals.append(("cancelled", values))
        return self.projection


class FailingLegacyAdapter:
    def execute(self, _message: object) -> object:
        raise AssertionError("P6 native task must not execute through LegacyTaskAdapter")


class FakeP6Router:
    def __init__(self, outcome: TaskOutcome | None = None) -> None:
        self.outcome = outcome or TaskOutcome.succeeded("directory sync complete", {"cached": 2})
        self.calls: list[tuple[TaskMessage, object]] = []

    def execute(self, message: TaskMessage, progress: object) -> TaskOutcome:
        self.calls.append((message, progress))
        return self.outcome


def _projection(task_key: str = "sync-115-directory-tree") -> Any:
    return SimpleNamespace(
        job=SimpleNamespace(
            id=UUID("00000000-0000-0000-0000-000000000601"),
            task_key=task_key,
            request_payload={
                "payload_version": 1,
                "parameters": {},
                "processor_type": "media",
                "display_name": "同步网盘目录",
                "silent": True,
                "call_style": "catalog",
            },
            timeout_seconds=None,
            max_attempts=1,
            parent_job_id=None,
            status=JobStatus.RUNNING,
        ),
        current_attempt=SimpleNamespace(
            id=UUID("00000000-0000-0000-0000-000000000602"),
            attempt_no=1,
        ),
    )


@pytest.mark.parametrize(
    "task_key",
    [
        "sync-115-directory-tree",
        "full-sync-strm",
        "monitor-115-life-events",
        "play-pool-daily-speedtest",
    ],
)
def test_worker_executes_p6_sync_without_resolving_legacy_processor(
    monkeypatch: pytest.MonkeyPatch,
    task_key: str,
) -> None:
    monkeypatch.setattr(
        worker_module,
        "_processor",
        lambda _identity: (_ for _ in ()).throw(AssertionError("native P6 sync must not resolve a legacy processor")),
    )
    projection = _projection(task_key)
    store = ExecutionStore(projection)
    router = FakeP6Router()
    runtime = WorkerRuntime(config=TaskRuntimeConfig(heartbeat_seconds=60, lease_seconds=120))
    state = cast(Any, runtime)
    state._store = store
    state._registry = TaskRegistry.from_fixture(TASK_FIXTURE)
    state._adapter = FailingLegacyAdapter()
    state._p6_p115_tasks = router

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == ["succeeded"]
    assert store.terminals[0][1]["result"] == {
        "ok": True,
        "status": "succeeded",
        "message": "directory sync complete",
        "details": {"cached": 2},
    }
    assert len(router.calls) == 1
    assert router.calls[0][0].task_key == task_key


def test_worker_persists_partial_directory_sync_details_on_failed_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(worker_module, "_processor", lambda _identity: None)
    projection = _projection()
    store = ExecutionStore(projection)
    router = FakeP6Router(
        TaskOutcome.partial(
            "one root failed",
            {"roots_completed": 1, "roots_failed": 1, "errors": [{"cid": "root-b"}]},
        )
    )
    runtime = WorkerRuntime(config=TaskRuntimeConfig(heartbeat_seconds=60, lease_seconds=120))
    state = cast(Any, runtime)
    state._store = store
    state._registry = TaskRegistry.from_fixture(TASK_FIXTURE)
    state._adapter = FailingLegacyAdapter()
    state._p6_p115_tasks = router

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == ["failed"]
    assert store.terminals[0][1]["error_details"]["task_outcome"] == {
        "ok": False,
        "status": "partial",
        "message": "one root failed",
        "details": {
            "roots_completed": 1,
            "roots_failed": 1,
            "errors": [{"cid": "root-b"}],
        },
    }
