from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from etk.compatibility import background
from etk.compatibility.background import (
    PersistentBackgroundGateway,
    PersistentBackgroundHandle,
    clear_persistent_background_gateway,
    install_persistent_background_gateway,
    spawn,
    spawn_later,
)
from etk.compatibility.persistent_tasks import PersistentTaskManagerGateway
from etk.domains.operations.jobs import JobSubmissionService, UnsupportedLegacyOperationError
from etk.platform.jobs import InvalidJsonPayloadError, JobStatus
from etk.platform.tasks import TaskRegistry

REPO_ROOT = Path(__file__).resolve().parents[3]
TASK_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "tasks" / "ordered-task-catalog.json"
JOB_ID = UUID("00000000-0000-0000-0000-000000000099")


def fixture_background_operation(value: dict[str, object], *, enabled: bool = True) -> dict[str, object]:
    return {"value": value, "enabled": enabled}


class CaptureStore:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def create_job(self, **values: object):
        self.calls.append(values)
        projection = SimpleNamespace(job=SimpleNamespace(id=JOB_ID))
        return SimpleNamespace(projection=projection, reused=False)


class FakeBackgroundService:
    def __init__(self) -> None:
        self.calls: list[tuple[object, tuple[object, ...], dict[str, object], datetime | None]] = []
        self.cancelled: list[UUID] = []
        self.lookups: list[tuple[str, str]] = []

    def submit_background_operation(
        self,
        function: object,
        args: tuple[object, ...],
        kwargs: dict[str, object],
        *,
        scheduled_for: datetime | None,
        max_attempts: int = 1,
        timeout_seconds: int | None = None,
        idempotency_key: str | None = None,
        idempotency_scope: str | None = None,
    ):
        del max_attempts, timeout_seconds, idempotency_key, idempotency_scope
        self.calls.append((function, args, kwargs, scheduled_for))
        return SimpleNamespace(projection=SimpleNamespace(job=SimpleNamespace(id=JOB_ID)))

    def get(self, job_id: UUID):
        assert job_id == JOB_ID
        return SimpleNamespace(job=SimpleNamespace(status=JobStatus.SUCCEEDED))

    def get_idempotent(self, *, scope: str, key: str):
        self.lookups.append((scope, key))
        if key == "missing":
            return None
        return SimpleNamespace(job=SimpleNamespace(id=JOB_ID))

    def cancel(self, job_id: UUID) -> None:
        self.cancelled.append(job_id)


def test_background_operation_persists_only_importable_strict_json_payloads() -> None:
    store = CaptureStore()
    service = JobSubmissionService(store, TaskRegistry.from_fixture(TASK_FIXTURE))  # type: ignore[arg-type]
    scheduled_for = datetime(2026, 8, 23, tzinfo=UTC)

    submitted = service.submit_background_operation(
        fixture_background_operation,
        ({"id": "fixture"},),
        {"enabled": False},
        scheduled_for=scheduled_for,
    )

    assert submitted.projection.job.id == JOB_ID
    call = store.calls[0]
    assert call["queue_name"] == "legacy.background"
    assert call["scheduled_for"] == scheduled_for
    assert call["max_attempts"] == 1
    assert call["timeout_seconds"] is None
    assert call["payload"] == {
        "payload_version": 1,
        "parameters": {"enabled": False},
        "positional_parameters": [{"id": "fixture"}],
        "processor_type": "media",
        "display_name": "fixture_background_operation",
        "silent": True,
        "call_style": "direct",
        "legacy_operation": {
            "module": __name__,
            "qualname": "fixture_background_operation",
        },
    }

    with pytest.raises(UnsupportedLegacyOperationError, match="top-level importable"):
        service.submit_background_operation(lambda: None, (), {})
    with pytest.raises(InvalidJsonPayloadError):
        service.submit_background_operation(fixture_background_operation, (), {"bad": object()})

    service.submit_background_operation(
        fixture_background_operation,
        ({"id": "retryable"},),
        {},
        max_attempts=5,
        timeout_seconds=900,
        idempotency_key="fixture-token",
        idempotency_scope="fixture-background",
    )
    policy_call = store.calls[-1]
    assert policy_call["max_attempts"] == 5
    assert policy_call["timeout_seconds"] == 900
    assert policy_call["idempotency_key"] == "fixture-token"
    assert policy_call["idempotency_scope"] == "fixture-background"


def test_api_spawn_and_delay_use_the_persistent_gateway_without_dispatcher(monkeypatch) -> None:
    service = FakeBackgroundService()
    gateway = PersistentBackgroundGateway(lambda: service)
    monkeypatch.setenv("ETK_PROCESS_ROLE", "api")
    install_persistent_background_gateway(gateway)
    try:
        immediate = spawn(fixture_background_operation, {"id": "one"}, enabled=False)
        delayed = spawn_later(3, fixture_background_operation, {"id": "two"})
        recovered = gateway.lookup(idempotency_scope="fixture", idempotency_key="known")
        missing = gateway.lookup(idempotency_scope="fixture", idempotency_key="missing")

        assert isinstance(immediate, PersistentBackgroundHandle)
        assert isinstance(delayed, PersistentBackgroundHandle)
        assert isinstance(recovered, PersistentBackgroundHandle)
        assert recovered.job_id == JOB_ID
        assert missing is None
        assert service.lookups == [("fixture", "known"), ("fixture", "missing")]
        assert immediate.ready()
        immediate.kill()
        assert service.cancelled == [JOB_ID]
        assert service.calls[0][3] is None
        assert service.calls[1][3] is not None
        assert background._DISPATCHER is None
    finally:
        clear_persistent_background_gateway(gateway)

    with pytest.raises(RuntimeError, match="requires the persistent Job gateway"):
        spawn(fixture_background_operation, {"id": "three"})


def test_task_manager_gateway_forwards_retry_and_idempotency_policy() -> None:
    class CaptureLegacyService:
        def __init__(self) -> None:
            self.values = {}

        def submit_legacy_callable(self, *args, **kwargs):
            self.values = {"args": args, **kwargs}
            return SimpleNamespace()

        def is_busy(self) -> bool:
            return False

        def legacy_status(self) -> dict[str, object]:
            return {}

    service = CaptureLegacyService()
    gateway = PersistentTaskManagerGateway(service)  # type: ignore[arg-type]

    assert gateway.submit(
        fixture_background_operation,
        "Telegram resource action",
        "media",
        (),
        {"value": {"id": "fixture"}},
        silent=True,
        max_attempts=3,
        idempotency_key="update:1",
        idempotency_scope="telegram-resource-action",
    )
    assert service.values["max_attempts"] == 3
    assert service.values["idempotency_key"] == "update:1"
    assert service.values["idempotency_scope"] == "telegram-resource-action"
    assert service.values["queue_when_busy"] is True
