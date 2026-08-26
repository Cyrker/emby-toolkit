from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

from etk.compatibility.legacy_jobs import LegacyTaskAdapter
from etk.platform.jobs import AttemptStatus, JobStatus
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcome, TaskRegistry
from etk.processes import p5_media_tasks, worker
from etk.processes.p5_media_tasks import LegacyP5MediaCompatibilityGateway, P5MediaTaskRouter
from etk.processes.runtime_config import TaskRuntimeConfig
from etk.processes.worker import WorkerRuntime

TASK_FIXTURE = Path(__file__).resolve().parents[4] / "verification" / "fixtures" / "tasks" / "ordered-task-catalog.json"
JOB_ID = UUID("00000000-0000-0000-0000-000000000661")
ATTEMPT_ID = UUID("00000000-0000-0000-0000-000000000662")


class _Store:
    def __init__(self, projection: object) -> None:
        self.projection = projection
        self.heartbeats: list[dict[str, object]] = []
        self.terminals: list[tuple[str, dict[str, object]]] = []

    def start_attempt(self, **_values: object) -> object:
        return self.projection

    def get_job(self, _job_id: UUID) -> object:
        return self.projection

    def heartbeat(self, **values: object) -> None:
        self.heartbeats.append(values)

    def succeed_attempt(self, **values: object) -> object:
        self.terminals.append(("succeeded", values))
        return self.projection

    def fail_attempt(self, **values: object) -> object:
        self.terminals.append(("failed", values))
        return self.projection

    def timeout_attempt(self, **values: object) -> object:
        self.terminals.append(("timeout", values))
        return self.projection

    def cancel_attempt(self, **values: object) -> object:
        self.terminals.append(("cancelled", values))
        return self.projection


class _NoopMonitor:
    terminalized = False
    cancel_requested = False
    timed_out = False
    stop_reason = None

    def __init__(self, **_values: object) -> None:
        return None

    def start(self) -> None:
        return None

    def close(self) -> None:
        return None


class _Resolver:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ProcessorIdentity]] = []

    def resolve(self, task_key: str, processor: ProcessorIdentity) -> tuple[object, object]:
        self.calls.append((task_key, processor))
        raise AssertionError("LegacyTaskAdapter must not execute a native P5 task")


class _Restore:
    def __init__(self) -> None:
        self.calls: list[tuple[object, str, bool]] = []

    def execute(
        self,
        progress: object,
        *,
        operation_key: str,
        force_full_update: bool = False,
    ) -> TaskOutcome:
        self.calls.append((progress, operation_key, force_full_update))
        cast(Any, progress).update(37, "native restore progress")
        return TaskOutcome.succeeded("native restore complete", {"restored": 1})


class _Compatibility:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object, Mapping[str, Any]]] = []

    def execute(
        self,
        task_key: str,
        processor: object,
        parameters: Mapping[str, Any],
    ) -> object:
        self.calls.append((task_key, processor, dict(parameters)))
        return True


class _CapturingRouter(P5MediaTaskRouter):
    def __init__(self, compatibility: object, restore: object | None = None) -> None:
        super().__init__(cast(Any, compatibility), cast(Any, restore))
        self.calls: list[tuple[TaskMessage, object | None, object | None]] = []

    def execute(
        self,
        message: TaskMessage,
        processor: object | None,
        progress: object | None = None,
    ) -> TaskOutcome:
        self.calls.append((message, processor, progress))
        return super().execute(message, processor, cast(Any, progress))


class _Processor:
    def __init__(self) -> None:
        self.clear_calls = 0

    def clear_stop_signal(self) -> None:
        self.clear_calls += 1

    def signal_stop(self) -> None:
        raise AssertionError("processor stop was not expected")

    def is_stop_requested(self) -> bool:
        return False


def _projection(task_key: str, parameters: Mapping[str, object]) -> object:
    return SimpleNamespace(
        job=SimpleNamespace(
            id=JOB_ID,
            task_key=task_key,
            request_payload={
                "payload_version": 1,
                "parameters": dict(parameters),
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
            id=ATTEMPT_ID,
            attempt_no=1,
            status=AttemptStatus.RUNNING,
        ),
    )


def _runtime(
    projection: object,
    router: P5MediaTaskRouter,
) -> tuple[WorkerRuntime, _Store, _Resolver]:
    registry = TaskRegistry.from_fixture(TASK_FIXTURE)
    resolver = _Resolver()
    store = _Store(projection)
    runtime = WorkerRuntime(config=TaskRuntimeConfig(heartbeat_seconds=60, lease_seconds=120))
    state = cast(Any, runtime)
    state._store = store
    state._registry = registry
    state._adapter = LegacyTaskAdapter(registry, cast(Any, resolver))
    state._p5_media_tasks = router
    return runtime, store, resolver


def test_worker_restore_mediainfo_is_native_without_processor_or_legacy_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(worker, "_HeartbeatMonitor", _NoopMonitor)
    processor_resolutions: list[ProcessorIdentity] = []
    legacy_imports: list[str] = []

    def reject_processor(identity: ProcessorIdentity) -> object:
        processor_resolutions.append(identity)
        raise AssertionError("restore_mediainfo must not resolve Processor")

    def reject_legacy_import(module_name: str) -> object:
        legacy_imports.append(module_name)
        raise AssertionError("tasks.media.task_restore_mediainfo must not be imported")

    monkeypatch.setattr(worker, "_processor", reject_processor)
    monkeypatch.setattr(p5_media_tasks, "import_module", reject_legacy_import)
    restore = _Restore()
    router = _CapturingRouter(LegacyP5MediaCompatibilityGateway(), restore)
    projection = _projection("restore_mediainfo", {"force_full_update": True})
    runtime, store, resolver = _runtime(projection, router)

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == ["succeeded"]
    assert processor_resolutions == []
    assert resolver.calls == []
    assert legacy_imports == []
    assert len(router.calls) == 1
    message, processor, progress = router.calls[0]
    assert processor is None
    assert progress is restore.calls[0][0]
    assert message.metadata == {"job_id": str(JOB_ID), "attempt_id": str(ATTEMPT_ID)}
    assert restore.calls[0][1:] == (f"restore_mediainfo:{JOB_ID}", True)
    assert store.heartbeats[0]["progress"] == 37
    result = cast(dict[str, object], store.terminals[0][1]["result"])
    assert result["details"] == {
        "restored": 1,
        "task_key": "restore_mediainfo",
        "native_handler": "_Restore",
        "envelope_owner": "_CapturingRouter",
        "compatibility_fallback": False,
    }


def test_worker_other_p5_tasks_keep_processor_and_compatibility_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(worker, "_HeartbeatMonitor", _NoopMonitor)
    processor = _Processor()
    processor_resolutions: list[ProcessorIdentity] = []

    def resolve_processor(identity: ProcessorIdentity) -> _Processor:
        processor_resolutions.append(identity)
        return processor

    monkeypatch.setattr(worker, "_processor", resolve_processor)
    compatibility = _Compatibility()
    router = _CapturingRouter(compatibility)
    projection = _projection("populate-metadata", {"batch_size": 5, "force_full_update": False})
    runtime, store, resolver = _runtime(projection, router)

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == ["succeeded"]
    assert processor_resolutions == [ProcessorIdentity.MEDIA]
    assert resolver.calls == []
    assert len(router.calls) == 1
    assert router.calls[0][1] is processor
    assert router.calls[0][2] is not None
    assert compatibility.calls == [
        (
            "populate-metadata",
            processor,
            {"batch_size": 5, "force_full_update": False},
        )
    ]
    assert processor.clear_calls == 2
