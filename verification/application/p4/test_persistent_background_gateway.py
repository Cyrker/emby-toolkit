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
from etk.platform.jobs import JobStatus
from etk.platform.tasks import TaskRegistry

REPO_ROOT = Path(__file__).resolve().parents[3]
TASK_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "tasks" / "ordered-task-catalog.json"
JOB_ID = UUID("00000000-0000-0000-0000-000000000099")


def fixture_background_operation(value: dict[str, object], *, enabled: bool = True) -> dict[str, object]:
    return {"value": value, "enabled": enabled}


def task_populate_metadata_cache() -> None:
    raise AssertionError("submission mapping must not execute the catalog function")


class CaptureStore:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def create_job(self, **values: object):
        self.calls.append(values)
        projection = SimpleNamespace(job=SimpleNamespace(id=JOB_ID))
        return SimpleNamespace(projection=projection, reused=False)

    @staticmethod
    def has_active_job() -> bool:
        return False


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
        raise UnsupportedLegacyOperationError("dynamic background callable submission is disabled")

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


def test_background_operation_submission_is_permanently_disabled() -> None:
    store = CaptureStore()
    service = JobSubmissionService(store, TaskRegistry.from_fixture(TASK_FIXTURE))  # type: ignore[arg-type]
    scheduled_for = datetime(2026, 8, 23, tzinfo=UTC)

    with pytest.raises(UnsupportedLegacyOperationError, match="dynamic background callable submission is disabled"):
        service.submit_background_operation(
            fixture_background_operation,
            ({"id": "fixture"},),
            {"enabled": False},
            scheduled_for=scheduled_for,
            max_attempts=5,
            timeout_seconds=900,
            idempotency_key="fixture-token",
            idempotency_scope="fixture-background",
        )

    assert store.calls == []


def test_legacy_callable_submission_only_maps_known_catalog_functions() -> None:
    store = CaptureStore()
    service = JobSubmissionService(store, TaskRegistry.from_fixture(TASK_FIXTURE))  # type: ignore[arg-type]

    submitted = service.submit_legacy_callable(
        task_populate_metadata_cache,
        "同步媒体数据",
        "media",
        (),
        {"force_full_update": False},
        silent=False,
        queue_when_busy=True,
    )

    assert submitted.projection.job.id == JOB_ID
    assert store.calls[0]["task_key"] == "populate-metadata"
    payload = store.calls[0]["payload"]
    assert payload == {
        "payload_version": 1,
        "parameters": {"force_full_update": False},
        "processor_type": "media",
        "display_name": "同步媒体数据",
        "silent": False,
    }
    assert isinstance(payload, dict)
    assert "legacy_operation" not in payload

    with pytest.raises(UnsupportedLegacyOperationError, match="dynamic callable submission is disabled"):
        service.submit_legacy_callable(
            fixture_background_operation,
            "unknown",
            "media",
            (),
            {},
            silent=True,
        )
    assert len(store.calls) == 1


def test_api_spawn_and_delay_propagate_disabled_dynamic_submission(monkeypatch) -> None:
    service = FakeBackgroundService()
    gateway = PersistentBackgroundGateway(lambda: service)
    monkeypatch.setenv("ETK_PROCESS_ROLE", "api")
    install_persistent_background_gateway(gateway)
    try:
        with pytest.raises(UnsupportedLegacyOperationError, match="dynamic background callable submission is disabled"):
            spawn(fixture_background_operation, {"id": "one"}, enabled=False)
        with pytest.raises(UnsupportedLegacyOperationError, match="dynamic background callable submission is disabled"):
            spawn_later(3, fixture_background_operation, {"id": "two"})
        recovered = gateway.lookup(idempotency_scope="fixture", idempotency_key="known")
        missing = gateway.lookup(idempotency_scope="fixture", idempotency_key="missing")

        assert isinstance(recovered, PersistentBackgroundHandle)
        assert recovered.job_id == JOB_ID
        assert missing is None
        assert service.lookups == [("fixture", "known"), ("fixture", "missing")]
        assert recovered.ready()
        recovered.kill()
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


def test_task_manager_gateway_submits_catalog_key_without_legacy_callable() -> None:
    class CaptureCatalogService:
        def __init__(self) -> None:
            self.values: dict[str, object] = {}

        def submit_task(self, *args: object, **kwargs: object) -> SimpleNamespace:
            self.values = {"args": args, **kwargs}
            return SimpleNamespace()

    service = CaptureCatalogService()
    gateway = PersistentTaskManagerGateway(service)  # type: ignore[arg-type]

    assert gateway.submit_key(
        "manual_subscribe_batch",
        "立即订阅",
        "media",
        {"subscribe_requests": [{"tmdb_id": "101", "item_type": "Movie"}]},
        silent=False,
        max_attempts=2,
        idempotency_key="portal:request-1",
        idempotency_scope="portal-manual-subscription",
    )

    assert service.values["args"] == (
        "manual_subscribe_batch",
        {"subscribe_requests": [{"tmdb_id": "101", "item_type": "Movie"}]},
    )
    assert service.values["trigger_type"] == "system"
    assert service.values["queue_name"] == "legacy.backlog"
    assert service.values["max_attempts"] == 2
    assert service.values["idempotency_key"] == "portal:request-1"
    assert service.values["idempotency_scope"] == "portal-manual-subscription"
