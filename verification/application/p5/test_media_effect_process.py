from __future__ import annotations

import threading
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from etk.domains.media.effect_execution import (
    MediaEffectExecutionUnavailable,
    PersistentMediaEffectExecutor,
)
from etk.domains.media.effects import (
    MEDIA_EFFECT_TOPIC,
    MediaEffectProjection,
    MediaEffectStatus,
)
from etk.platform.jobs import OutboxClaim, OutboxEventRecord, OutboxStatus, PublishBatchResult
from etk.processes import worker as worker_module
from etk.processes.celery_app import (
    EXECUTE_JOB_TASK,
    EXECUTE_MEDIA_EFFECT_TASK,
    create_celery_app,
)
from etk.processes.p5_media_effects import (
    CeleryMediaEffectPublisher,
    MediaEffectOutboxDispatcher,
)
from etk.processes.runtime_config import TaskRuntimeConfig
from etk.processes.worker import WorkerRuntime, WorkerShutdownRequested

EFFECT_ID = UUID("11111111-1111-4111-8111-111111111111")
EVENT_ID = UUID("22222222-2222-4222-8222-222222222222")


class FakeCelery:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def send_task(self, name: str, **options: Any) -> None:
        self.calls.append((name, options))


class FakeLedger:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.reject_results = [2, 3]

    def expire_stale_claims(self, **options: Any) -> int:
        self.calls.append(("expire", options))
        return 1

    def requeue_replay_safe_stale_claims(self, **options: Any) -> int:
        self.calls.append(("requeue-safe", options))
        return 5

    def requeue_stale_published(self, **options: Any) -> int:
        self.calls.append(("requeue", options))
        return 4

    def reject_dead_dispatches(self, **options: Any) -> int:
        self.calls.append(("reject", options))
        return self.reject_results.pop(0)


class FakeStore:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, dict[str, Any]]] = []

    def publish_outbox_batch(self, publisher: Any, **options: Any) -> PublishBatchResult:
        self.calls.append((publisher, options))
        return PublishBatchResult(claimed=1, published=0, failed=0, dead=1)


class FakeExecutor:
    def __init__(self) -> None:
        self.ids: list[UUID] = []

    def execute(self, effect_id: UUID) -> object:
        self.ids.append(effect_id)
        return object()


class FakeExecutionLedger:
    def __init__(self, projection: MediaEffectProjection) -> None:
        self.projection = projection
        self.claims: list[UUID] = []

    def read_by_id(self, effect_id: UUID) -> MediaEffectProjection | None:
        assert effect_id == self.projection.id
        return self.projection

    def claim_by_id(self, effect_id: UUID, **_kwargs: Any) -> MediaEffectProjection | None:
        self.claims.append(effect_id)
        return self.projection


class NarrowDelivery:
    effect_types = frozenset({"emby.mediainfo.apply"})

    def deliver(self, effect: MediaEffectProjection) -> dict[str, object]:
        del effect
        raise AssertionError("unsupported effect must be rejected before delivery")


def _config() -> TaskRuntimeConfig:
    return TaskRuntimeConfig(
        broker_url="redis://fixture.invalid:6379/0",
        queue_name="fixture.serial",
        heartbeat_seconds=5,
        lease_seconds=30,
        outbox_batch_size=7,
        outbox_claim_timeout_seconds=11,
        schedules_enabled=False,
    )


def _claim(*, topic: str = MEDIA_EFFECT_TOPIC) -> OutboxClaim:
    now = datetime.now(UTC)
    event = OutboxEventRecord(
        id=EVENT_ID,
        event_key="media.effect.requested:fixture",
        topic=topic,
        aggregate_type="media_effect",
        aggregate_id=str(EFFECT_ID),
        payload_version=1,
        payload={
            "effect_id": str(EFFECT_ID),
            "effect_key": "fixture-effect",
            "effect_type": "emby.mediainfo.apply",
            "aggregate_type": "media_asset",
            "aggregate_id": "fixture-item:fixture-sha1",
            "request_hash": "ab" * 32,
        },
        status=OutboxStatus.PUBLISHING,
        available_at=now,
        claimed_by="fixture-publisher",
        claim_token=uuid4(),
        claimed_at=now,
        published_at=None,
        processed_at=None,
        publish_attempts=1,
        max_publish_attempts=20,
        last_error=None,
        created_at=now,
        updated_at=now,
    )
    return OutboxClaim(event=event, claim_token=event.claim_token or uuid4())


def _effect_projection(
    *,
    status: MediaEffectStatus = MediaEffectStatus.PENDING,
    effect_type: str = "emby.metadata.refresh",
) -> MediaEffectProjection:
    now = datetime.now(UTC)
    return MediaEffectProjection(
        id=EFFECT_ID,
        effect_key="fixture-effect",
        effect_type=effect_type,
        aggregate_type="media_asset",
        aggregate_id="fixture-item:fixture-sha1",
        payload_version=1,
        payload={"item_id": "fixture-item"},
        request_hash=b"a" * 32,
        status=status,
        attempts=0,
        available_at=now,
        claimed_by=None,
        claim_token=None,
        claimed_at=None,
        lease_expires_at=None,
        last_attempt_at=None,
        result=None,
        error=None,
        finished_at=now if status.terminal else None,
        created_at=now,
        updated_at=now,
    )


def test_media_effect_publisher_validates_envelope_and_uses_deterministic_task_id() -> None:
    app = FakeCelery()
    publisher = CeleryMediaEffectPublisher(_config(), celery_app=app)

    publisher.publish(_claim())

    assert app.calls == [
        (
            EXECUTE_MEDIA_EFFECT_TASK,
            {
                "kwargs": {"effect_id": str(EFFECT_ID)},
                "task_id": str(EFFECT_ID),
                "queue": "fixture.serial",
                "routing_key": "fixture.serial",
                "serializer": "json",
            },
        )
    ]
    with pytest.raises(ValueError, match="only supports media.effect.requested"):
        publisher.publish(_claim(topic="job.dispatch.requested"))
    assert len(app.calls) == 1


def test_media_effect_dispatcher_recovers_before_publish_and_finalizes_new_dead_rows() -> None:
    ledger = FakeLedger()
    store = FakeStore()
    publisher = cast(Any, object())
    dispatcher = MediaEffectOutboxDispatcher(
        cast(Any, store),
        cast(Any, ledger),
        publisher,
        _config(),
        publisher_id="fixture-coordinator:media",
        replay_safe_effect_types=("daily-theme.replenishment",),
    )

    result = dispatcher.tick()

    assert ledger.calls == [
        (
            "requeue-safe",
            {"effect_types": ("daily-theme.replenishment",), "limit": 7},
        ),
        ("expire", {"limit": 7}),
        ("requeue", {"limit": 7, "processed_timeout_seconds": 60}),
        ("reject", {"limit": 7}),
        ("reject", {"limit": 7}),
    ]
    assert result.requeued_replay_safe_claims == 5
    assert store.calls == [
        (
            publisher,
            {
                "publisher_id": "fixture-coordinator:media",
                "limit": 7,
                "claim_timeout_seconds": 11,
                "failure_backoff_seconds": 2,
                "topics": (MEDIA_EFFECT_TOPIC,),
            },
        )
    ]
    assert result.expired_claims == 1
    assert result.requeued_events == 4
    assert result.rejected_dead_effects == 5
    assert result.publish.dead == 1


def test_narrow_executor_rejects_unsupported_pending_effect_before_claim() -> None:
    ledger = FakeExecutionLedger(_effect_projection())
    executor = PersistentMediaEffectExecutor(
        cast(Any, ledger),
        NarrowDelivery(),
        worker_id="fixture-worker:media",
    )

    with pytest.raises(MediaEffectExecutionUnavailable, match="unavailable in this Worker profile"):
        executor.execute(EFFECT_ID)

    assert ledger.claims == []
    assert ledger.projection.status is MediaEffectStatus.PENDING


@pytest.mark.parametrize("status", [MediaEffectStatus.RETRYABLE, MediaEffectStatus.CLAIMED])
def test_narrow_executor_rejects_unsupported_replayable_effect_without_changing_claim_state(
    status: MediaEffectStatus,
) -> None:
    projection = _effect_projection(status=status)
    if status is MediaEffectStatus.RETRYABLE:
        projection = replace(projection, attempts=2)
    else:
        projection = replace(
            projection,
            attempts=2,
            claimed_by="fixture-worker",
            claim_token=UUID("33333333-3333-4333-8333-333333333333"),
            claimed_at=datetime.now(UTC),
            lease_expires_at=datetime.now(UTC),
        )
    ledger = FakeExecutionLedger(projection)
    executor = PersistentMediaEffectExecutor(
        cast(Any, ledger),
        NarrowDelivery(),
        worker_id="fixture-worker:media",
    )

    with pytest.raises(MediaEffectExecutionUnavailable, match="unavailable in this Worker profile"):
        executor.execute(EFFECT_ID)

    assert ledger.claims == []
    assert ledger.projection == projection


@pytest.mark.parametrize(
    "status",
    [MediaEffectStatus.CONFIRMED, MediaEffectStatus.REJECTED, MediaEffectStatus.INDETERMINATE],
)
def test_narrow_executor_returns_terminal_unsupported_effect_without_claim(status: MediaEffectStatus) -> None:
    projection = replace(_effect_projection(), status=status, finished_at=datetime.now(UTC))
    ledger = FakeExecutionLedger(projection)
    executor = PersistentMediaEffectExecutor(
        cast(Any, ledger),
        NarrowDelivery(),
        worker_id="fixture-worker:media",
    )

    assert executor.execute(EFFECT_ID) is projection
    assert ledger.claims == []


def test_celery_app_registers_job_and_media_effect_tasks_on_the_serial_queue() -> None:
    app = create_celery_app(_config())

    assert EXECUTE_JOB_TASK in app.tasks
    assert EXECUTE_MEDIA_EFFECT_TASK in app.tasks
    assert app.conf.task_routes[EXECUTE_JOB_TASK] == {"queue": "fixture.serial"}
    assert app.conf.task_routes[EXECUTE_MEDIA_EFFECT_TASK] == {"queue": "fixture.serial"}


def test_worker_runtime_routes_media_effect_id_and_honors_sticky_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(worker_module, "_PROCESS_SHUTDOWN_REQUESTED", False)
    runtime = WorkerRuntime(config=_config())
    executor = FakeExecutor()
    runtime._started = True
    runtime._media_effects = cast(Any, executor)

    runtime.execute_media_effect(effect_id=str(EFFECT_ID))

    assert executor.ids == [EFFECT_ID]
    with pytest.raises(ValueError, match="must be a UUID"):
        runtime.execute_media_effect(effect_id="not-a-uuid")
    runtime._shutdown_requested.set()
    with pytest.raises(WorkerShutdownRequested, match="shutdown"):
        runtime.execute_media_effect(effect_id=str(EFFECT_ID))

    runtime._shutdown_requested.clear()

    def start_and_request_shutdown() -> None:
        runtime._shutdown_requested.set()

    monkeypatch.setattr(runtime, "start", start_and_request_shutdown)
    with pytest.raises(WorkerShutdownRequested, match="shutdown started"):
        runtime.execute_media_effect(effect_id=str(EFFECT_ID))
    assert executor.ids == [EFFECT_ID]


class BlockingMediaEffectExecutor(FakeExecutor):
    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()

    def execute(self, effect_id: UUID) -> object:
        result = super().execute(effect_id)
        self.started.set()
        assert self.release.wait(2)
        return result


def test_worker_runtime_close_waits_for_media_effect_execution() -> None:
    runtime = WorkerRuntime(config=replace(_config(), worker_shutdown_timeout_seconds=1))
    executor = BlockingMediaEffectExecutor()
    runtime._started = True
    runtime._media_effects = cast(Any, executor)

    execution = threading.Thread(
        target=runtime.execute_media_effect,
        kwargs={"effect_id": str(EFFECT_ID)},
        daemon=True,
    )
    execution.start()
    assert executor.started.wait(1)
    assert not runtime._idle.is_set()

    closed = threading.Event()

    def close_runtime() -> None:
        runtime.close()
        closed.set()

    closer = threading.Thread(target=close_runtime, daemon=True)
    closer.start()
    assert runtime._shutdown_requested.wait(1)
    assert not closed.wait(0.05)

    executor.release.set()
    execution.join(1)
    closer.join(1)

    assert not execution.is_alive()
    assert not closer.is_alive()
    assert closed.is_set()
    assert runtime._active_media_effect_executions == 0
