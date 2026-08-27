from __future__ import annotations

import threading
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

import pytest

from etk.domains.collection.operation_execution import CollectionOperationExecutor
from etk.domains.collection.operations import (
    COLLECTION_OPERATION_AGGREGATE_TYPE,
    COLLECTION_OPERATION_TOPIC,
    COLLECTION_OPERATION_TOPICS,
)
from etk.platform.jobs import OutboxClaim, OutboxEventRecord, OutboxStatus, PublishBatchResult
from etk.processes.celery_app import EXECUTE_COLLECTION_OPERATION_TASK, create_celery_app
from etk.processes.coordinator import CoordinatorRuntime
from etk.processes.p8_collection_operations import (
    CeleryCollectionOperationPublisher,
    CollectionOperationCoordinator,
    CollectionOperationCoordinatorResult,
    collection_operation_claim_lease_seconds,
)
from etk.processes.runtime_config import TaskRuntimeConfig
from etk.processes.worker import WorkerRuntime

_EVENT_ID = UUID("00000000-0000-0000-0000-000000000091")
_OPERATION_ID = UUID("00000000-0000-0000-0000-000000000092")
_CLAIM_TOKEN = UUID("00000000-0000-0000-0000-000000000093")
_NOW = datetime(2026, 8, 27, tzinfo=UTC)


class RecordingCeleryApp:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def send_task(self, name: str, **options: Any) -> None:
        self.calls.append((name, options))


class RecordingRepository:
    def __init__(self, reject_results: list[int]) -> None:
        self.reject_results = reject_results
        self.expire_calls: list[dict[str, Any]] = []
        self.reject_limits: list[int] = []

    def expire_stale_claims(self, **options: Any) -> int:
        self.expire_calls.append(options)
        return 2

    def reject_dead_dispatches(self, *, limit: int) -> int:
        self.reject_limits.append(limit)
        return self.reject_results.pop(0)


class RecordingStore:
    def __init__(self, publish: PublishBatchResult) -> None:
        self.publish = publish
        self.requeue_options: dict[str, Any] | None = None
        self.publish_options: dict[str, Any] | None = None
        self.publisher: Any = None

    def requeue_stale_published(self, **options: Any) -> None:
        self.requeue_options = options

    def publish_outbox_batch(self, publisher: Any, **options: Any) -> PublishBatchResult:
        self.publisher = publisher
        self.publish_options = options
        return self.publish


class CleanCoordinatorStore:
    def enqueue_due_retries(self, **_options: Any) -> None:
        return None

    def recover_stale_attempts(self, **_options: Any) -> None:
        return None

    def requeue_stale_published(self, **_options: Any) -> None:
        return None

    def publish_outbox_batch(self, _publisher: Any, **_options: Any) -> PublishBatchResult:
        return PublishBatchResult(claimed=0, published=0, failed=0, dead=0)


def _claim() -> OutboxClaim:
    event = OutboxEventRecord(
        id=_EVENT_ID,
        event_key=f"{COLLECTION_OPERATION_TOPIC}:{_OPERATION_ID}",
        topic=COLLECTION_OPERATION_TOPIC,
        aggregate_type=COLLECTION_OPERATION_AGGREGATE_TYPE,
        aggregate_id=str(_OPERATION_ID),
        payload_version=1,
        payload={
            "operation_id": str(_OPERATION_ID),
            "sequence": 9,
            "operation_key": "a" * 64,
            "request_hash": "b" * 64,
        },
        status=OutboxStatus.PUBLISHING,
        available_at=_NOW,
        claimed_by="fixture-collection-coordinator",
        claim_token=_CLAIM_TOKEN,
        claimed_at=_NOW,
        published_at=None,
        processed_at=None,
        publish_attempts=1,
        max_publish_attempts=20,
        last_error=None,
        created_at=_NOW,
        updated_at=_NOW,
    )
    return OutboxClaim(event=event, claim_token=_CLAIM_TOKEN)


def test_collection_publisher_uses_operation_uuid_as_celery_task_id() -> None:
    app = RecordingCeleryApp()
    publisher = CeleryCollectionOperationPublisher(
        TaskRuntimeConfig(queue_name="fixture.serial"),
        celery_app=app,
    )

    publisher.publish(_claim())

    assert app.calls == [
        (
            EXECUTE_COLLECTION_OPERATION_TASK,
            {
                "kwargs": {"operation_id": str(_OPERATION_ID)},
                "task_id": str(_OPERATION_ID),
                "queue": "fixture.serial",
                "routing_key": "fixture.serial",
                "serializer": "json",
            },
        )
    ]


@pytest.mark.parametrize(
    ("event_change", "message"),
    [
        ({"topic": "job.dispatch.requested"}, "unsupported Outbox event"),
        ({"aggregate_id": str(_EVENT_ID)}, "aggregate does not match"),
        ({"payload_version": 2}, "payload version is unsupported"),
        ({"payload": {"operation_id": str(_OPERATION_ID), "sequence": 0}}, "sequence is invalid"),
    ],
)
def test_collection_publisher_rejects_drifted_identity(
    event_change: dict[str, Any],
    message: str,
) -> None:
    app = RecordingCeleryApp()
    claim = _claim()
    drifted = replace(claim, event=replace(claim.event, **event_change))

    with pytest.raises(ValueError, match=message):
        CeleryCollectionOperationPublisher(TaskRuntimeConfig(), celery_app=app).publish(drifted)

    assert app.calls == []


def test_collection_coordinator_recovers_claims_and_dead_dispatches() -> None:
    publish = PublishBatchResult(claimed=4, published=2, failed=1, dead=1)
    repository = RecordingRepository([2, 3])
    store = RecordingStore(publish)
    publisher = cast(Any, object())
    config = TaskRuntimeConfig(
        lease_seconds=20,
        outbox_batch_size=7,
        outbox_claim_timeout_seconds=11,
    )
    coordinator = CollectionOperationCoordinator(
        store=cast(Any, store),
        repository=cast(Any, repository),
        publisher=publisher,
        config=config,
        publisher_id="fixture-collection-coordinator",
    )

    result = coordinator.tick()

    assert result == CollectionOperationCoordinatorResult(
        expired_claims=2,
        rejected_dead=5,
        publish=publish,
    )
    assert repository.expire_calls == [{"limit": 7, "retry_delay_seconds": 1}]
    assert repository.reject_limits == [7, 7]
    assert store.requeue_options == {
        "limit": 7,
        "processed_timeout_seconds": 60,
        "topics": COLLECTION_OPERATION_TOPICS,
    }
    assert store.publish_options == {
        "publisher_id": "fixture-collection-coordinator",
        "limit": 7,
        "claim_timeout_seconds": 11,
        "failure_backoff_seconds": 2,
        "topics": COLLECTION_OPERATION_TOPICS,
    }
    assert store.publisher is publisher


def test_celery_app_registers_json_only_collection_operation_task() -> None:
    app = create_celery_app(TaskRuntimeConfig(queue_name="fixture.serial"))

    task = app.tasks[EXECUTE_COLLECTION_OPERATION_TASK]

    assert task.serializer == "json"
    assert task.acks_late is True
    assert task.reject_on_worker_lost is True
    assert app.conf.task_routes[EXECUTE_COLLECTION_OPERATION_TASK] == {"queue": "fixture.serial"}


def test_coordinator_runtime_fails_closed_on_collection_dispatch_degradation(monkeypatch) -> None:
    runtime = CoordinatorRuntime.__new__(CoordinatorRuntime)
    runtime.config = TaskRuntimeConfig()
    runtime._store = cast(Any, CleanCoordinatorStore())
    runtime._publisher = cast(Any, object())
    runtime._publisher_id = "fixture-coordinator"
    runtime._collection_operation_coordinator = cast(
        Any,
        type(
            "CollectionCoordinator",
            (),
            {
                "tick": lambda _self: CollectionOperationCoordinatorResult(
                    expired_claims=0,
                    rejected_dead=0,
                    publish=PublishBatchResult(claimed=1, published=0, failed=1, dead=0),
                )
            },
        )(),
    )
    monkeypatch.setattr(runtime, "_refresh_settings", lambda: None)
    monkeypatch.setattr(runtime, "_process_telegram_commands", lambda: None)

    with pytest.raises(RuntimeError, match="Collection operation Outbox dispatch is degraded"):
        runtime._tick()


@pytest.mark.parametrize("raises", [False, True])
def test_worker_collection_execution_maintains_active_counter_and_idle(monkeypatch, raises: bool) -> None:
    calls: list[tuple[UUID, bool]] = []

    class Executor:
        def execute(self, operation_id: UUID) -> Any:
            calls.append((operation_id, runtime._idle.is_set()))
            if raises:
                raise RuntimeError("fixture execution failed")
            return "projection"

    runtime = WorkerRuntime.__new__(WorkerRuntime)
    runtime._shutdown_requested = threading.Event()
    runtime._lock = threading.RLock()
    runtime._idle = threading.Event()
    runtime._idle.set()
    runtime._collection_operation_executor = cast(Any, Executor())
    runtime._active_executions = {}
    runtime._active_ingestion_executions = 0
    runtime._active_p115_upload_executions = 0
    runtime._active_p115_playback_executions = 0
    runtime._active_p115_intro_executions = 0
    runtime._active_subscription_executions = 0
    runtime._active_collection_operation_executions = 0
    runtime._active_identity_executions = 0
    monkeypatch.setattr(runtime, "start", lambda: None)
    monkeypatch.setattr("etk.processes.worker.worker_shutdown_requested", lambda: False)

    if raises:
        with pytest.raises(RuntimeError, match="fixture execution failed"):
            runtime.execute_collection_operation(operation_id=str(_OPERATION_ID))
    else:
        assert runtime.execute_collection_operation(operation_id=str(_OPERATION_ID)) == "projection"

    assert calls == [(_OPERATION_ID, False)]
    assert runtime._active_collection_operation_executions == 0
    assert runtime._idle.is_set()


def test_collection_operation_lease_covers_bounded_provider_calls() -> None:
    assert collection_operation_claim_lease_seconds(60) == 390
    assert collection_operation_claim_lease_seconds(1) == 90


def test_collection_executor_reloads_claim_lease_for_each_delivery() -> None:
    leases: list[float] = []
    configured = iter((120.0, 240.0))

    class Repository:
        @staticmethod
        def read(_operation_id: UUID) -> object:
            return object()

        def claim_by_id(
            self,
            _operation_id: UUID,
            *,
            worker_id: str,
            lease_seconds: float,
        ) -> None:
            del worker_id
            leases.append(lease_seconds)
            return None

    executor = CollectionOperationExecutor(
        cast(Any, Repository()),
        cast(Any, object()),
        cast(Any, object()),
        cast(Any, object()),
        worker_id="fixture-collection-worker",
        lease_seconds=lambda: next(configured),
    )

    executor.execute(_OPERATION_ID)
    executor.execute(_OPERATION_ID)

    assert leases == [120.0, 240.0]
