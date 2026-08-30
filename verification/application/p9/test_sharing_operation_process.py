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
from etk.integrations.http_policy import ExternalAdapterError
from etk.platform.jobs import OutboxClaim, OutboxEventRecord, OutboxStatus, PublishBatchResult
from etk.processes.celery_app import EXECUTE_SHARING_OPERATION_TASK, create_celery_app
from etk.processes.coordinator import CoordinatorRuntime
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

    def validate_configuration(self) -> None:
        return None

    def register_device(self, *, name: str, server_id_hash: str) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("register_device", {"name": name, "server_id_hash": server_id_hash}))
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {"device_id": "device-1", "token": "must-not-persist"},
        )

    def register_source(self, source_kind: str, payload: dict[str, Any]) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("register_source", {"source_kind": source_kind, "payload": payload}))
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"source_id": "source-1"})

    report_transfer = acquire_transfer_lease
    share_sync_heartbeat = acquire_transfer_lease
    run_maintenance = acquire_transfer_lease

    def create_share_request(self, payload: dict[str, Any]) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("create_share_request", payload))
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {"group_id": "group-1", "token": "must-not-persist"},
        )

    def co_request_share_request(self, group_id: str, payload: dict[str, Any]) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("co_request_share_request", {"group_id": group_id, "payload": payload}))
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"group_id": group_id, "joined_by_me": True})

    def cancel_share_request(self, group_id: str, payload: dict[str, Any]) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("cancel_share_request", {"group_id": group_id, "payload": payload}))
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"group_id": group_id, "joined_by_me": False})

    def list_sources(self, **query: Any) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("readback", query))
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {"items": [{"source_id": "source-1"}]},
        )

    list_share_requests = list_sources

    def device_status(self) -> ProviderResponse[dict[str, Any]]:
        self.calls.append(("device_status", {}))
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {"device": {"id": "device-1", "server_id_hash": "a" * 64}},
        )


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


def test_native_coordinator_tick_dispatches_without_legacy_runtime_hooks() -> None:
    calls: list[str] = []

    class Store:
        def enqueue_due_retries(self, **_options: Any) -> None:
            calls.append("enqueue")

        def recover_stale_attempts(self, **_options: Any) -> None:
            calls.append("recover")

        def requeue_stale_published(self, **_options: Any) -> None:
            calls.append("requeue")

        def publish_outbox_batch(self, _publisher: Any, **_options: Any) -> PublishBatchResult:
            calls.append("publish")
            return PublishBatchResult(claimed=0, published=0, failed=0, dead=0)

    class Sharing:
        def tick(self) -> SharingOperationCoordinatorResult:
            calls.append("sharing")
            return SharingOperationCoordinatorResult(
                expired_claims=0,
                expired_readbacks=0,
                rejected_dead=0,
                publish=PublishBatchResult(claimed=0, published=0, failed=0, dead=0),
            )

    settings = SimpleNamespace(monitor_enabled=False)
    runtime = CoordinatorRuntime.__new__(CoordinatorRuntime)
    runtime.config = TaskRuntimeConfig()
    runtime._store = cast(Any, Store())
    runtime._publisher = cast(Any, object())
    runtime._publisher_id = "fixture-native-coordinator"
    runtime._config_adapter = SimpleNamespace(load=lambda: calls.append("settings") or settings)
    runtime._cleanup_execution_repository = None
    runtime._sharing_operation_coordinator = cast(Any, Sharing())
    runtime._refresh_settings = lambda **_options: (_ for _ in ()).throw(  # type: ignore[method-assign]
        AssertionError("native Coordinator called compatibility settings refresh")
    )
    runtime._process_telegram_commands = lambda: (_ for _ in ()).throw(  # type: ignore[method-assign]
        AssertionError("native Coordinator called legacy Telegram runtime")
    )

    runtime._tick(compatibility=False)

    assert runtime._last_settings is settings
    assert calls == ["settings", "enqueue", "recover", "requeue", "publish", "sharing"]


def test_native_coordinator_closes_container_when_startup_initialization_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    health: list[str] = []

    class FailingContainer:
        closed = False

        @property
        def database(self) -> Any:
            raise RuntimeError("fixture native initialization failed")

        def close(self) -> None:
            self.closed = True

    container = FailingContainer()
    monkeypatch.setattr(
        "etk.processes.coordinator._create_default_container",
        lambda _data_dir: (container, object()),
    )
    runtime = CoordinatorRuntime.__new__(CoordinatorRuntime)
    runtime.config = TaskRuntimeConfig()
    runtime._write_health = health.append  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="fixture native initialization failed"):
        runtime._run_native_only()

    assert container.closed is True
    assert health == ["stopped"]


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


def test_sharing_delivery_rejects_shared_center_configuration_before_checkpoint() -> None:
    class MisconfiguredCenter(FakeSharedCenter):
        def validate_configuration(self) -> None:
            raise ExternalAdapterError(
                "fixture Shared Center URL is unavailable",
                disposition=ProviderDisposition.REJECTED,
                system="shared_center",
                operation="configuration",
            )

    center = MisconfiguredCenter()
    delivery = SharingProviderOperationDelivery(
        shared_center=cast(Any, center),
        hdhive=cast(Any, FakeHDHive()),
    )
    checkpoints: list[bool] = []
    operation = cast(
        Any,
        SimpleNamespace(
            request={
                "provider": "shared_center",
                "action": "report_transfer",
                "payload": {"transfer_id": "transfer-1", "result": "ok"},
            }
        ),
    )

    outcome = delivery.request(operation, checkpoint=lambda: checkpoints.append(True))

    assert outcome.disposition is SharingDeliveryDisposition.REJECTED
    assert checkpoints == []
    assert center.calls == []


def test_sharing_delivery_matches_nested_device_identity_and_keeps_safe_receipt() -> None:
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
                "action": "register_device",
                "payload": {"name": "Fixture Device", "server_id_hash": "a" * 64},
                "readback": {
                    "provider": "shared_center",
                    "action": "device_status",
                    "query": {},
                    "match_key": "device.server_id_hash",
                    "match_value": "a" * 64,
                },
            }
        ),
    )

    requested = delivery.request(operation, checkpoint=lambda: None)
    readback = delivery.readback(operation)

    assert requested.disposition is SharingDeliveryDisposition.CONFIRMED
    assert requested.result is not None
    assert requested.result["provider_result"] == {"device_id": "device-1"}
    assert "must-not-persist" not in repr(requested.result)
    assert delivery.supports_authoritative_readback(operation) is True
    assert readback.disposition is SharingDeliveryDisposition.CONFIRMED


def test_sharing_delivery_does_not_confirm_wrong_nested_device_identity() -> None:
    delivery = SharingProviderOperationDelivery(
        shared_center=cast(Any, FakeSharedCenter()),
        hdhive=cast(Any, FakeHDHive()),
    )
    operation = cast(
        Any,
        SimpleNamespace(
            request={
                "provider": "shared_center",
                "action": "register_device",
                "payload": {"name": "Fixture Device", "server_id_hash": "b" * 64},
                "readback": {
                    "provider": "shared_center",
                    "action": "device_status",
                    "query": {},
                    "match_key": "device.server_id_hash",
                    "match_value": "b" * 64,
                },
            }
        ),
    )

    assert delivery.readback(operation).disposition is SharingDeliveryDisposition.RETRYABLE


def test_sharing_delivery_keeps_share_request_group_id_without_secret_fields() -> None:
    delivery = SharingProviderOperationDelivery(
        shared_center=cast(Any, FakeSharedCenter()),
        hdhive=cast(Any, FakeHDHive()),
    )
    operation = cast(
        Any,
        SimpleNamespace(
            request={
                "provider": "shared_center",
                "action": "create_share_request",
                "payload": {"tmdb_id": "101"},
            }
        ),
    )

    outcome = delivery.request(operation, checkpoint=lambda: None)

    assert outcome.disposition is SharingDeliveryDisposition.CONFIRMED
    assert outcome.result is not None
    assert outcome.result["provider_result"] == {"group_id": "group-1"}
    assert "must-not-persist" not in repr(outcome.result)


@pytest.mark.parametrize(
    ("action", "expected_call"),
    [
        ("co_request_share_request", "co_request_share_request"),
        ("cancel_share_request", "cancel_share_request"),
    ],
)
def test_sharing_delivery_dispatches_share_request_participant_mutations(
    action: str,
    expected_call: str,
) -> None:
    center = FakeSharedCenter()
    delivery = SharingProviderOperationDelivery(
        shared_center=cast(Any, center),
        hdhive=cast(Any, FakeHDHive()),
    )
    checkpoints: list[bool] = []
    operation = cast(
        Any,
        SimpleNamespace(
            request={
                "provider": "shared_center",
                "action": action,
                "payload": {"group_id": "group-1", "body": {"reason": "fixture"}},
            }
        ),
    )

    outcome = delivery.request(operation, checkpoint=lambda: checkpoints.append(True))

    assert outcome.disposition is SharingDeliveryDisposition.CONFIRMED
    assert checkpoints == [True]
    assert center.calls == [(expected_call, {"group_id": "group-1", "payload": {"reason": "fixture"}})]
    assert delivery.supports_authoritative_readback(operation) is False


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
