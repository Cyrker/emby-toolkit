from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

import pytest

from etk.domains.p115.playback import PLAYBACK_AGGREGATE_TYPE, PLAYBACK_TOPIC, PLAYBACK_TOPICS
from etk.platform.jobs import OutboxClaim, OutboxEventRecord, OutboxStatus, PublishBatchResult
from etk.processes.celery_app import EXECUTE_P115_PLAYBACK_OPERATION_TASK
from etk.processes.coordinator import CoordinatorRuntime
from etk.processes.p6_playback import (
    CeleryP115PlaybackPublisher,
    P115PlaybackCoordinator,
    P115PlaybackCoordinatorResult,
)
from etk.processes.runtime_config import TaskRuntimeConfig

_EVENT_ID = UUID("00000000-0000-0000-0000-000000000061")
_OPERATION_ID = UUID("00000000-0000-0000-0000-000000000062")
_CLAIM_TOKEN = UUID("00000000-0000-0000-0000-000000000063")
_NOW = datetime(2026, 8, 26, tzinfo=UTC)


class RecordingCeleryApp:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def send_task(self, name: str, **options: Any) -> None:
        self.calls.append((name, options))


class RecordingPlaybackRepository:
    def __init__(self, *, reject_results: list[int] | None = None) -> None:
        self.reject_results = list(reject_results or [0])
        self.expire_claim_limits: list[int] = []
        self.expire_lease_limits: list[int] = []
        self.expire_qr_limits: list[int] = []
        self.reject_limits: list[int] = []

    def expire_stale_claims(self, *, limit: int) -> int:
        self.expire_claim_limits.append(limit)
        return 2

    def expire_leases(self, *, limit: int) -> tuple[object, ...]:
        self.expire_lease_limits.append(limit)
        return (object(), object(), object())

    def expire_qr_sessions(self, *, limit: int) -> int:
        self.expire_qr_limits.append(limit)
        return 4

    def reject_dead_dispatches(self, *, limit: int) -> int:
        self.reject_limits.append(limit)
        return self.reject_results.pop(0)


class RecordingPlaybackStore:
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
        event_key=f"{PLAYBACK_TOPIC}:{_OPERATION_ID}",
        topic=PLAYBACK_TOPIC,
        aggregate_type=PLAYBACK_AGGREGATE_TYPE,
        aggregate_id=str(_OPERATION_ID),
        payload_version=1,
        payload={
            "operation_id": str(_OPERATION_ID),
            "sequence": 7,
            "operation_key": "a" * 64,
            "request_hash": "b" * 64,
        },
        status=OutboxStatus.PUBLISHING,
        available_at=_NOW,
        claimed_by="fixture-playback-coordinator",
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


def test_playback_publisher_uses_operation_uuid_as_celery_task_id() -> None:
    app = RecordingCeleryApp()
    publisher = CeleryP115PlaybackPublisher(
        TaskRuntimeConfig(queue_name="fixture.serial"),
        celery_app=app,
    )

    publisher.publish(_claim())

    assert app.calls == [
        (
            EXECUTE_P115_PLAYBACK_OPERATION_TASK,
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
def test_playback_publisher_rejects_drifted_outbox_identity(
    event_change: dict[str, Any],
    message: str,
) -> None:
    app = RecordingCeleryApp()
    publisher = CeleryP115PlaybackPublisher(TaskRuntimeConfig(), celery_app=app)
    claim = _claim()
    claim = replace(claim, event=replace(claim.event, **event_change))

    with pytest.raises(ValueError, match=message):
        publisher.publish(claim)

    assert app.calls == []


def test_playback_coordinator_recovers_claims_leases_and_dead_dispatches() -> None:
    publish = PublishBatchResult(claimed=4, published=2, failed=1, dead=1)
    repository = RecordingPlaybackRepository(reject_results=[2, 3])
    store = RecordingPlaybackStore(publish)
    publisher = cast(Any, object())
    config = TaskRuntimeConfig(
        lease_seconds=20,
        outbox_batch_size=7,
        outbox_claim_timeout_seconds=11,
    )
    coordinator = P115PlaybackCoordinator(
        store=cast(Any, store),
        repository=repository,
        publisher=publisher,
        config=config,
        publisher_id="fixture-playback-coordinator",
    )

    result = coordinator.tick()

    assert result == P115PlaybackCoordinatorResult(
        expired_claims=2,
        expired_leases=3,
        expired_qr_sessions=4,
        rejected_dead=5,
        publish=publish,
    )
    assert repository.expire_claim_limits == [7]
    assert repository.expire_lease_limits == [7]
    assert repository.expire_qr_limits == [7]
    assert repository.reject_limits == [7, 7]
    assert store.requeue_options == {
        "limit": 7,
        "processed_timeout_seconds": 60,
        "topics": PLAYBACK_TOPICS,
    }
    assert store.publish_options == {
        "publisher_id": "fixture-playback-coordinator",
        "limit": 7,
        "claim_timeout_seconds": 11,
        "failure_backoff_seconds": 2,
        "topics": PLAYBACK_TOPICS,
    }
    assert store.publisher is publisher


@pytest.mark.parametrize(
    "publish",
    [
        PublishBatchResult(claimed=1, published=0, failed=1, dead=0),
        PublishBatchResult(claimed=1, published=0, failed=0, dead=1),
    ],
)
def test_coordinator_runtime_fails_closed_on_playback_dispatch_degradation(
    monkeypatch,
    publish: PublishBatchResult,
) -> None:
    runtime = CoordinatorRuntime.__new__(CoordinatorRuntime)
    runtime.config = TaskRuntimeConfig()
    runtime._store = cast(Any, CleanCoordinatorStore())
    runtime._publisher = cast(Any, object())
    runtime._publisher_id = "fixture-coordinator"
    runtime._p115_playback_coordinator = cast(
        Any,
        type(
            "PlaybackCoordinator",
            (),
            {
                "tick": lambda _self: P115PlaybackCoordinatorResult(
                    expired_claims=0,
                    expired_leases=0,
                    expired_qr_sessions=0,
                    rejected_dead=0,
                    publish=publish,
                )
            },
        )(),
    )
    monkeypatch.setattr(runtime, "_refresh_settings", lambda: None)
    monkeypatch.setattr(runtime, "_process_native_telegram_search_commands", lambda: None)

    with pytest.raises(RuntimeError, match="P115 playback Outbox dispatch is degraded"):
        runtime._tick()


def test_coordinator_runtime_accepts_clean_playback_dispatch(monkeypatch) -> None:
    runtime = CoordinatorRuntime.__new__(CoordinatorRuntime)
    runtime.config = TaskRuntimeConfig()
    runtime._store = cast(Any, CleanCoordinatorStore())
    runtime._publisher = cast(Any, object())
    runtime._publisher_id = "fixture-coordinator"
    runtime._p115_playback_coordinator = cast(
        Any,
        type(
            "PlaybackCoordinator",
            (),
            {
                "tick": lambda _self: P115PlaybackCoordinatorResult(
                    expired_claims=0,
                    expired_leases=0,
                    expired_qr_sessions=0,
                    rejected_dead=1,
                    publish=PublishBatchResult(claimed=1, published=1, failed=0, dead=0),
                )
            },
        )(),
    )
    monkeypatch.setattr(runtime, "_refresh_settings", lambda: None)
    monkeypatch.setattr(runtime, "_process_native_telegram_search_commands", lambda: None)

    runtime._tick()
