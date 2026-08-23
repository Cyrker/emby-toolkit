from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import text

from etk.platform.db import DatabaseRuntime
from etk.platform.jobs import (
    AttemptStatus,
    JobStatus,
    OutboxClaim,
    OutboxEventRecord,
    OutboxStatus,
    PostgresJobStore,
)

JOB_DISPATCH_TOPIC = "job.dispatch.requested"
MEDIA_EFFECT_TOPIC = "media.effect.requested"
UNKNOWN_TOPIC = "fixture.unknown"


class RecordingPublisher:
    def __init__(self) -> None:
        self.events: list[OutboxEventRecord] = []

    def publish(self, claim: OutboxClaim) -> None:
        self.events.append(claim.event)


def _insert_non_job_event(
    runtime: DatabaseRuntime,
    *,
    topic: str,
    status: str = "pending",
    aggregate_type: str = "media_effect",
    aggregate_id: str = "fixture-effect",
    claim_token: UUID | None = None,
    max_publish_attempts: int = 20,
) -> UUID:
    event_id = uuid4()
    publishing = status == "publishing"
    now = datetime.now(UTC)
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO outbox_events (
                    id, event_key, topic, aggregate_type, aggregate_id,
                    payload_version, payload_json, status, available_at,
                    claimed_by, claim_token, claimed_at, published_at,
                    publish_attempts, max_publish_attempts
                ) VALUES (
                    :id, :event_key, :topic, :aggregate_type, :aggregate_id,
                    1, '{"effect_id":"fixture-effect"}'::jsonb, :status, NOW(),
                    :claimed_by, :claim_token, :claimed_at, :published_at,
                    :publish_attempts, :max_publish_attempts
                )
                """
            ),
            {
                "id": event_id,
                "event_key": f"{topic}:{event_id.hex}",
                "topic": topic,
                "aggregate_type": aggregate_type,
                "aggregate_id": aggregate_id,
                "status": status,
                "claimed_by": "fixture-publisher" if publishing else None,
                "claim_token": claim_token if publishing else None,
                "claimed_at": now if publishing else None,
                "published_at": now - timedelta(minutes=10) if status == "published" else None,
                "publish_attempts": 1 if publishing else 0,
                "max_publish_attempts": max_publish_attempts,
            },
        )
    return event_id


def test_job_topic_publish_batch_leaves_media_and_unknown_events_pending(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
) -> None:
    p4_store.create_job(task_key="topic-filter-job", payload={})
    media_event_id = _insert_non_job_event(p4_runtime, topic=MEDIA_EFFECT_TOPIC)
    unknown_event_id = _insert_non_job_event(p4_runtime, topic=UNKNOWN_TOPIC)
    publisher = RecordingPublisher()

    result = p4_store.publish_outbox_batch(
        publisher,
        publisher_id="job-only-publisher",
        topics=(JOB_DISPATCH_TOPIC,),
    )

    assert result.claimed == 1
    assert result.published == 1
    assert result.failed == 0
    assert result.dead == 0
    assert [event.topic for event in publisher.events] == [JOB_DISPATCH_TOPIC]
    with p4_runtime.engine.connect() as connection:
        non_job_rows = {
            row["id"]: row
            for row in connection.execute(
                text(
                    """
                    SELECT id, status, publish_attempts, claimed_by, claim_token, claimed_at
                    FROM outbox_events
                    WHERE id IN (:media_event_id, :unknown_event_id)
                    """
                ),
                {
                    "media_event_id": media_event_id,
                    "unknown_event_id": unknown_event_id,
                },
            ).mappings()
        }

    assert set(non_job_rows) == {media_event_id, unknown_event_id}
    for row in non_job_rows.values():
        assert row["status"] == "pending"
        assert row["publish_attempts"] == 0
        assert row["claimed_by"] is None
        assert row["claim_token"] is None
        assert row["claimed_at"] is None


def test_outbox_topic_filter_is_opt_in_and_default_claim_behavior_is_preserved(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
) -> None:
    media_event_id = _insert_non_job_event(p4_runtime, topic=MEDIA_EFFECT_TOPIC)

    assert p4_store.claim_outbox(publisher_id="empty-filter", topics=()) == ()
    claims = p4_store.claim_outbox(publisher_id="unfiltered-publisher")

    assert len(claims) == 1
    assert claims[0].event.id == media_event_id
    assert claims[0].event.topic == MEDIA_EFFECT_TOPIC
    assert claims[0].event.status is OutboxStatus.PUBLISHING
    assert claims[0].event.publish_attempts == 1


def test_job_topic_recovery_does_not_requeue_stale_media_or_unknown_events(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
) -> None:
    p4_store.create_job(task_key="topic-recovery-job", payload={})
    claim = p4_store.claim_outbox(
        publisher_id="job-recovery-publisher",
        topics=(JOB_DISPATCH_TOPIC,),
    )[0]
    assert p4_store.mark_outbox_published(claim.event.id, claim.claim_token)
    media_event_id = _insert_non_job_event(p4_runtime, topic=MEDIA_EFFECT_TOPIC, status="published")
    unknown_event_id = _insert_non_job_event(p4_runtime, topic=UNKNOWN_TOPIC, status="published")
    with p4_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE outbox_events
                SET published_at = NOW() - INTERVAL '10 minutes'
                WHERE id = :event_id
                """
            ),
            {"event_id": claim.event.id},
        )

    recovery = p4_store.requeue_stale_published(
        processed_timeout_seconds=60,
        topics=(JOB_DISPATCH_TOPIC,),
    )

    assert recovery.requeued_events == 1
    with p4_runtime.engine.connect() as connection:
        statuses = {
            row["id"]: row["status"]
            for row in connection.execute(
                text(
                    """
                    SELECT id, status
                    FROM outbox_events
                    WHERE id IN (:job_event_id, :media_event_id, :unknown_event_id)
                    """
                ),
                {
                    "job_event_id": claim.event.id,
                    "media_event_id": media_event_id,
                    "unknown_event_id": unknown_event_id,
                },
            ).mappings()
        }

    assert statuses == {
        claim.event.id: "pending",
        media_event_id: "published",
        unknown_event_id: "published",
    }


def test_dead_non_job_event_cannot_fail_a_job_even_with_job_attempt_aggregate(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
) -> None:
    created = p4_store.create_job(task_key="unrelated-job", payload={})
    claim_token = uuid4()
    media_event_id = _insert_non_job_event(
        p4_runtime,
        topic=MEDIA_EFFECT_TOPIC,
        status="publishing",
        aggregate_type="job_attempt",
        aggregate_id=str(created.attempt.id),
        claim_token=claim_token,
        max_publish_attempts=1,
    )

    status = p4_store.mark_outbox_failed(
        media_event_id,
        claim_token,
        error="unsupported media publisher",
        backoff_seconds=0,
    )

    assert status is OutboxStatus.DEAD
    projection = p4_store.get_job(created.job.id)
    assert projection is not None
    assert projection.job.status is JobStatus.QUEUED
    assert projection.current_attempt.status is AttemptStatus.CREATED
    with p4_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT status FROM outbox_events WHERE id = :event_id"),
                {"event_id": media_event_id},
            )
            == "dead"
        )
