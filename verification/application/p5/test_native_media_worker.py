from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import task_manager
from etk.platform.jobs import JobStatus
from etk.platform.tasks import TaskMessage, TaskOutcome, TaskRegistry
from etk.processes.runtime_config import TaskRuntimeConfig
from etk.processes.worker import WorkerRuntime

REPO_ROOT = Path(__file__).resolve().parents[3]
TASK_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "tasks" / "ordered-task-catalog.json"


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


class FakeNativeRouter:
    def __init__(self, outcome: TaskOutcome, *, report_failure: bool = False) -> None:
        self.outcome = outcome
        self.report_failure = report_failure
        self.calls: list[tuple[TaskMessage, object]] = []

    def execute(self, message: TaskMessage, processor: object, _progress: object) -> TaskOutcome:
        self.calls.append((message, processor))
        if self.report_failure:
            task_manager.update_status_from_thread(-1, "native compatibility reported failure")
        return self.outcome


def _projection(task_key: str, parameters: dict[str, Any]) -> Any:
    return SimpleNamespace(
        job=SimpleNamespace(
            id=UUID("00000000-0000-0000-0000-000000000501"),
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
            max_attempts=1,
            parent_job_id=None,
            status=JobStatus.RUNNING,
        ),
        current_attempt=SimpleNamespace(
            id=UUID("00000000-0000-0000-0000-000000000502"),
            attempt_no=1,
        ),
    )


def _runtime(
    router: FakeNativeRouter,
    projection: Any,
) -> tuple[WorkerRuntime, ExecutionStore]:
    runtime = WorkerRuntime(config=TaskRuntimeConfig(heartbeat_seconds=60, lease_seconds=120))
    store = ExecutionStore(projection)
    state = cast(Any, runtime)
    state._store = store
    state._registry = TaskRegistry.from_fixture(TASK_FIXTURE)
    state._p5_media_tasks = router
    return runtime, store


def test_worker_executes_targeted_backfill_through_p5_native_router() -> None:
    projection = _projection(
        "backfill-media-metadata",
        {"targets": [{"tmdb_id": "100", "media_type": "movie"}]},
    )
    router = FakeNativeRouter(TaskOutcome.succeeded("targeted backfill complete", {"count": 1}))
    runtime, store = _runtime(router, projection)

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == ["succeeded"]
    assert store.terminals[0][1]["result"] == {
        "ok": True,
        "status": "succeeded",
        "message": "targeted backfill complete",
        "details": {"count": 1},
    }
    message, observed_processor = router.calls[0]
    assert message.payload.data == {"targets": [{"tmdb_id": "100", "media_type": "movie"}]}
    assert observed_processor is None


def test_worker_converts_failed_native_outcome_to_failed_attempt() -> None:
    projection = _projection("populate-metadata", {})
    router = FakeNativeRouter(TaskOutcome.failed("metadata failed"))
    runtime, store = _runtime(router, projection)

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == ["failed"]
    assert store.terminals[0][1]["error_type"] == "_TaskReportedFailure"
    assert store.terminals[0][1]["error_message"] == "metadata failed"


def test_native_reprocess_partial_persists_completion_and_retry_details() -> None:
    from etk.processes.native_operations import NativeOperationRouter

    projection = _projection("native-operation:media.reprocess-item", {"item_id": "3"})
    projection.job.request_payload.update(call_style="direct", native_operation="media.reprocess-item")
    result = {"status": "partial", "message": "资料待补齐", "details": {
        "metadata": {"incomplete": [{"item_id": "3", "missing": ["中文简介"]}]},
        "metadata_retry": {"status": "scheduled"},
    }}
    runtime, store = _runtime(FakeNativeRouter(TaskOutcome.succeeded()), projection)
    runtime._native_operations = NativeOperationRouter(
        {"media.reprocess-item": SimpleNamespace(execute=lambda *_: result)}
    )
    runtime._execute_claimed(SimpleNamespace(projection=projection))
    assert store.terminals[0][0] == "failed"
    assert store.terminals[0][1]["error_details"]["task_outcome"] == result


def test_worker_ignores_removed_legacy_thread_status_when_native_outcome_succeeds() -> None:
    projection = _projection("restore-nfo-and-images", {})
    router = FakeNativeRouter(
        TaskOutcome.succeeded("compatibility returned None"),
        report_failure=True,
    )
    runtime, store = _runtime(router, projection)

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == ["succeeded"]
    assert store.terminals[0][1]["result"]["status"] == "succeeded"
