from __future__ import annotations

import threading
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest
from celery.exceptions import Reject

from etk.domains.sharing.execution import SharingDeliveryDisposition
from etk.domains.sharing.operation_delivery import SharingProviderOperationDelivery
from etk.domains.sharing.operations import (
    SHARING_OPERATION_AGGREGATE_TYPE,
    SHARING_OPERATION_TOPIC,
    SHARING_OPERATION_TOPICS,
)
from etk.integrations import ProviderDisposition, ProviderResponse
from etk.platform.jobs import OutboxClaim, OutboxEventRecord, OutboxStatus, PublishBatchResult
from etk.processes.celery_app import EXECUTE_SHARING_OPERATION_TASK, create_celery_app
from etk.processes.p9_sharing_operations import (
    SHARING_OPERATION_CLAIM_LEASE_SECONDS,
    CelerySharingOperationPublisher,
    SharingOperationCoordinator,
    SharingOperationCoordinatorResult,
)
from etk.processes.runtime_config import TaskRuntimeConfig
from etk.processes.worker import WorkerRuntime

_EVENT_ID = UUID("00000000-0000-0000-0000-000000000091")
_OPERATION_ID = UUID("00000000-0000-0000-0000-000000000092")
_CLAIM_TOKEN = UUID("00000000-0000-0000-0000-000000000093")
_NOW = datetime(2026, 8, 30, tzinfo=UTC)


class RecordingCeleryApp:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def send_task(self, name: str, **options: Any) -> None:
        self.calls.append((name, options))


class RecordingRepository:
    def __init__(self, reject_results: list[int]) -> None:
        self.reject_results = reject_results
        self.calls: list[tuple[str, int]] = []

    def expire_stale_claims(self, *, limit: int) -> int:
        self.calls.append(("expire", limit))
        return 2

    def requeue_stale_readback_claims(self, *, limit: int) -> int:
        self.calls.append(("readback", limit))
        return 3

    def reject_dead_dispatches(self, *, limit: int) -> int:
        self.calls.append(("dead", limit))
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


class FakeSharedCenter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def acquire_transfer_lease(self, payload: dict[str, Any]) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("lease", payload))
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"lease_id": "lease-1"})

    register_device = acquire_transfer_lease
    register_source = acquire_transfer_lease
    report_transfer = acquire_transfer_lease
    share_sync_heartbeat = acquire_transfer_lease
    run_maintenance = acquire_transfer_lease
    create_share_request = acquire_transfer_lease

    def list_sources(self, **query: Any) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("readback", query))
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {"items": [{"source_id": "source-1"}]},
        )

    list_share_requests = list_sources

    def device_status(self) -> ProviderResponse[dict[str, Any]]:
        return self.list_sources()


class FakeHDHive:
    def unlock(self, slug: str) -> ProviderResponse[dict[str, Any]]:
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {
                "url": "https://115.com/s/private-share",
                "access_code": "private-code",
                "slug": slug,
            },
        )

    checkin = unlock
    create_subscription = unlock
    delete_subscription = unlock

    def check_subscription(self, *_args: Any) -> ProviderResponse[dict[str, Any]]:
        return ProviderResponse(ProviderDisposition.NOT_FOUND, 404)

    get_resources = check_subscription


def _claim() -> OutboxClaim:
    event = OutboxEventRecord(
        id=_EVENT_ID,
        event_key=f"{SHARING_OPERATION_TOPIC}:{_OPERATION_ID}",
        topic=SHARING_OPERATION_TOPIC,
        aggregate_type=SHARING_OPERATION_AGGREGATE_TYPE,
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
        claimed_by="fixture-sharing-coordinator",
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


def test_sharing_publisher_uses_operation_uuid_as_celery_task_id() -> None:
    app = RecordingCeleryApp()
    publisher = CelerySharingOperationPublisher(
        TaskRuntimeConfig(queue_name="fixture.serial"),
        celery_app=app,
    )

    publisher.publish(_claim())

    assert app.calls == [
        (
            EXECUTE_SHARING_OPERATION_TASK,
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
def test_sharing_publisher_rejects_drifted_identity(
    event_change: dict[str, Any],
    message: str,
) -> None:
    app = RecordingCeleryApp()
    publisher = CelerySharingOperationPublisher(TaskRuntimeConfig(), celery_app=app)
    claim = _claim()

    with pytest.raises(ValueError, match=message):
        publisher.publish(replace(claim, event=replace(claim.event, **event_change)))

    assert app.calls == []


def test_sharing_coordinator_recovers_disjoint_claims_and_dead_dispatches() -> None:
    publish = PublishBatchResult(claimed=4, published=2, failed=1, dead=1)
    repository = RecordingRepository([2, 3])
    store = RecordingStore(publish)
    publisher = cast(Any, object())
    config = TaskRuntimeConfig(
        lease_seconds=20,
        outbox_batch_size=7,
        outbox_claim_timeout_seconds=11,
    )
    coordinator = SharingOperationCoordinator(
        store=cast(Any, store),
        repository=cast(Any, repository),
        publisher=publisher,
        config=config,
        publisher_id="fixture-sharing-coordinator",
    )

    result = coordinator.tick()

    assert result == SharingOperationCoordinatorResult(
        expired_claims=2,
        expired_readbacks=3,
        rejected_dead=5,
        publish=publish,
    )
    assert repository.calls == [("expire", 7), ("readback", 7), ("dead", 7), ("dead", 7)]
    topics = tuple(SHARING_OPERATION_TOPICS)
    assert store.requeue_options == {
        "limit": 7,
        "processed_timeout_seconds": 60,
        "topics": topics,
    }
    assert store.publish_options == {
        "publisher_id": "fixture-sharing-coordinator",
        "limit": 7,
        "claim_timeout_seconds": 11,
        "failure_backoff_seconds": 2,
        "topics": topics,
    }
    assert store.publisher is publisher


def test_sharing_delivery_checkpoints_immediately_before_provider_mutation() -> None:
    center = FakeSharedCenter()
    delivery = SharingProviderOperationDelivery(
        shared_center=cast(Any, center),
        hdhive=cast(Any, FakeHDHive()),
    )
    checkpoints: list[int] = []
    operation = cast(
        Any,
        SimpleNamespace(
            request={
                "provider": "shared_center",
                "action": "acquire_transfer_lease",
                "payload": {"source_id": "source-1"},
            }
        ),
    )

    outcome = delivery.request(operation, checkpoint=lambda: checkpoints.append(len(center.calls)))

    assert checkpoints == [0]
    assert center.calls == [("lease", {"source_id": "source-1"})]
    assert outcome.disposition is SharingDeliveryDisposition.CONFIRMED


def test_sharing_delivery_rejects_invalid_payload_before_provider_checkpoint() -> None:
    delivery = SharingProviderOperationDelivery(
        shared_center=cast(Any, FakeSharedCenter()),
        hdhive=cast(Any, FakeHDHive()),
    )
    checkpoints: list[bool] = []
    operation = cast(
        Any,
        SimpleNamespace(
            request={
                "provider": "shared_center",
                "action": "register_device",
                "payload": {"name": "Fixture Device"},
            }
        ),
    )

    outcome = delivery.request(operation, checkpoint=lambda: checkpoints.append(True))

    assert checkpoints == []
    assert outcome.disposition is SharingDeliveryDisposition.REJECTED


def test_sharing_delivery_authoritative_readback_requires_expected_identity() -> None:
    center = FakeSharedCenter()
    delivery = SharingProviderOperationDelivery(
        shared_center=cast(Any, center),
        hdhive=cast(Any, FakeHDHive()),
    )
    operation = cast(
        Any,
        SimpleNamespace(
            request={
                "provider": "shared_center",
                "action": "acquire_transfer_lease",
                "payload": {"source_id": "source-1"},
                "readback": {
                    "provider": "shared_center",
                    "action": "list_sources",
                    "query": {"source_id": "source-1"},
                    "match_key": "source_id",
                    "match_value": "source-1",
                },
            }
        ),
    )

    assert delivery.supports_authoritative_readback(operation) is True
    assert delivery.readback(operation).disposition is SharingDeliveryDisposition.CONFIRMED


def test_sharing_delivery_never_persists_hdhive_unlock_credentials() -> None:
    delivery = SharingProviderOperationDelivery(
        shared_center=cast(Any, FakeSharedCenter()),
        hdhive=cast(Any, FakeHDHive()),
    )
    operation = cast(
        Any,
        SimpleNamespace(
            request={
                "provider": "hdhive",
                "action": "unlock",
                "payload": {"slug": "fixture-resource"},
            }
        ),
    )

    outcome = delivery.request(operation, checkpoint=lambda: None)

    assert outcome.disposition is SharingDeliveryDisposition.CONFIRMED
    serialized = repr(dict(outcome.result or {}))
    assert "private-share" not in serialized
    assert "private-code" not in serialized
    assert len(str((outcome.result or {}).get("provider_result_hash") or "")) == 64


def test_sharing_delivery_does_not_confirm_readback_without_identity_match() -> None:
    center = FakeSharedCenter()
    delivery = SharingProviderOperationDelivery(
        shared_center=cast(Any, center),
        hdhive=cast(Any, FakeHDHive()),
    )
    operation = cast(
        Any,
        SimpleNamespace(
            request={
                "provider": "shared_center",
                "action": "acquire_transfer_lease",
                "payload": {"source_id": "source-1"},
                "readback": {
                    "provider": "shared_center",
                    "action": "list_sources",
                    "query": {"source_id": "source-1"},
                },
            }
        ),
    )

    assert delivery.supports_authoritative_readback(operation) is False
    assert delivery.readback(operation).disposition is SharingDeliveryDisposition.INDETERMINATE


def test_sharing_delivery_matches_identity_in_items_response() -> None:
    class Center(FakeSharedCenter):
        def list_sources(self, **query: Any) -> ProviderResponse[dict[str, Any]]:
            self.calls.append(("readback", query))
            return ProviderResponse(
                ProviderDisposition.CONFIRMED,
                200,
                {"items": [{"source_id": "source-1"}]},
            )

    center = Center()
    delivery = SharingProviderOperationDelivery(
        shared_center=cast(Any, center),
        hdhive=cast(Any, FakeHDHive()),
    )
    operation = cast(
        Any,
        SimpleNamespace(
            request={
                "provider": "shared_center",
                "action": "acquire_transfer_lease",
                "payload": {"source_id": "source-1"},
                "readback": {
                    "provider": "shared_center",
                    "action": "list_sources",
                    "query": {"source_id": "source-1"},
                    "match_key": "source_id",
                    "match_value": "source-1",
                },
            }
        ),
    )

    assert delivery.supports_authoritative_readback(operation) is True
    assert delivery.readback(operation).disposition is SharingDeliveryDisposition.CONFIRMED


def test_sharing_delivery_does_not_treat_nested_request_echo_as_authoritative_state() -> None:
    class Center(FakeSharedCenter):
        def list_sources(self, **query: Any) -> ProviderResponse[dict[str, Any]]:
            self.calls.append(("readback", query))
            return ProviderResponse(
                ProviderDisposition.CONFIRMED,
                200,
                {"request": {"source_id": "source-1"}, "items": []},
            )

    delivery = SharingProviderOperationDelivery(
        shared_center=cast(Any, Center()),
        hdhive=cast(Any, FakeHDHive()),
    )
    operation = cast(
        Any,
        SimpleNamespace(
            request={
                "provider": "shared_center",
                "action": "acquire_transfer_lease",
                "payload": {"source_id": "source-1"},
                "readback": {
                    "provider": "shared_center",
                    "action": "list_sources",
                    "query": {"source_id": "source-1"},
                    "match_key": "source_id",
                    "match_value": "source-1",
                },
            }
        ),
    )

    assert delivery.readback(operation).disposition is SharingDeliveryDisposition.RETRYABLE


def test_celery_app_registers_json_only_sharing_operation_task() -> None:
    app = create_celery_app(TaskRuntimeConfig(queue_name="fixture.serial"))

    task = app.tasks[EXECUTE_SHARING_OPERATION_TASK]

    assert task.serializer == "json"
    assert task.acks_late is True
    assert task.reject_on_worker_lost is True
    assert app.conf.task_routes[EXECUTE_SHARING_OPERATION_TASK] == {"queue": "fixture.serial"}


def test_celery_sharing_operation_requeues_when_worker_shutdown_has_started(monkeypatch) -> None:
    app = create_celery_app(TaskRuntimeConfig(queue_name="fixture.serial"))
    monkeypatch.setattr("etk.processes.worker.worker_shutdown_requested", lambda: True)

    with pytest.raises(Reject) as raised:
        app.tasks[EXECUTE_SHARING_OPERATION_TASK].run(operation_id=str(_OPERATION_ID))

    assert raised.value.requeue is True


@pytest.mark.parametrize("raises", [False, True])
def test_worker_sharing_execution_maintains_active_counter_and_idle(monkeypatch, raises: bool) -> None:
    calls: list[tuple[UUID, str, float, bool]] = []

    class Executor:
        def execute(self, operation_id: UUID, *, worker_id: str, lease_seconds: float) -> Any:
            calls.append((operation_id, worker_id, lease_seconds, runtime._idle.is_set()))
            if raises:
                raise RuntimeError("fixture execution failed")
            return SimpleNamespace(operation="projection")

    runtime = WorkerRuntime.__new__(WorkerRuntime)
    runtime._shutdown_requested = threading.Event()
    runtime._lock = threading.RLock()
    runtime._idle = threading.Event()
    runtime._idle.set()
    runtime._worker_id = "fixture-worker"
    runtime._sharing_operation_executor = cast(Any, Executor())
    runtime._active_executions = {}
    runtime._active_ingestion_executions = 0
    runtime._active_p115_upload_executions = 0
    runtime._active_p115_playback_executions = 0
    runtime._active_p115_intro_executions = 0
    runtime._active_subscription_executions = 0
    runtime._active_collection_operation_executions = 0
    runtime._active_identity_executions = 0
    runtime._active_database_maintenance_executions = 0
    runtime._active_sharing_operation_executions = 0
    runtime._active_media_effect_executions = 0
    monkeypatch.setattr(runtime, "start", lambda: None)
    monkeypatch.setattr("etk.processes.worker.worker_shutdown_requested", lambda: False)

    if raises:
        with pytest.raises(RuntimeError, match="fixture execution failed"):
            runtime.execute_sharing_operation(operation_id=str(_OPERATION_ID))
    else:
        assert runtime.execute_sharing_operation(operation_id=str(_OPERATION_ID)) == "projection"

    assert calls == [
        (
            _OPERATION_ID,
            "fixture-worker:sharing",
            SHARING_OPERATION_CLAIM_LEASE_SECONDS,
            False,
        )
    ]
    assert runtime._active_sharing_operation_executions == 0
    assert runtime._idle.is_set()
