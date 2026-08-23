from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from etk.domains.media.effects import MEDIA_EFFECT_TOPIC
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
    )

    result = dispatcher.tick()

    assert ledger.calls == [
        ("expire", {"limit": 7}),
        ("requeue", {"limit": 7, "processed_timeout_seconds": 60}),
        ("reject", {"limit": 7}),
        ("reject", {"limit": 7}),
    ]
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
