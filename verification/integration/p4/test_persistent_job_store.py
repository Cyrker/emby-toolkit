from __future__ import annotations

from datetime import UTC, datetime, timedelta
from threading import Barrier, Event, Thread
from uuid import uuid4

import pytest
from sqlalchemy import text

from etk.platform.db import DatabaseRuntime
from etk.platform.jobs import (
    ActiveJobConflictError,
    AttemptStatus,
    IdempotencyConflictError,
    InvalidJsonPayloadError,
    InvalidStateTransitionError,
    JobStatus,
    OutboxStatus,
    PostgresJobStore,
)
from etk.processes.coordinator import CoordinatorLeadership


def test_migration_creates_four_tables_and_constraints(p4_runtime: DatabaseRuntime) -> None:
    with p4_runtime.engine.connect() as connection:
        tables = set(
            connection.scalars(
                text(
                    """
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name IN ('job_runs', 'job_attempts', 'outbox_events', 'idempotency_records')
                    """
                )
            )
        )
        constraints = set(
            connection.scalars(
                text(
                    """
                    SELECT conname FROM pg_constraint
                    WHERE conname IN (
                        'ck_job_runs_terminal_finished',
                        'ck_job_attempts_terminal_finished',
                        'uq_idempotency_records_scope_key_hash',
                        'uq_job_attempts_job_attempt_no'
                    )
                    """
                )
            )
        )

    assert tables == {"job_runs", "job_attempts", "outbox_events", "idempotency_records"}
    assert constraints == {
        "ck_job_runs_terminal_finished",
        "ck_job_attempts_terminal_finished",
        "uq_idempotency_records_scope_key_hash",
        "uq_job_attempts_job_attempt_no",
    }


def test_create_is_atomic_and_idempotency_reuses_only_the_same_request(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
) -> None:
    created = p4_store.create_job(
        task_key="populate-metadata",
        payload={"force_full_update": False},
        max_attempts=2,
        idempotency_key="request-1",
    )
    reused = p4_store.create_job(
        task_key="populate-metadata",
        payload={"force_full_update": False},
        max_attempts=2,
        idempotency_key="request-1",
    )

    assert not created.reused
    assert reused.reused
    assert reused.job.id == created.job.id
    assert reused.attempt.id == created.attempt.id
    assert created.job.status is JobStatus.QUEUED
    assert created.attempt.status is AttemptStatus.CREATED
    recovered = p4_store.get_idempotent_job(
        scope="job.create:populate-metadata",
        key="request-1",
    )
    assert recovered is not None and recovered.job.id == created.job.id
    assert (
        p4_store.get_idempotent_job(
            scope="job.create:populate-metadata",
            key="missing",
        )
        is None
    )

    with p4_runtime.engine.connect() as connection:
        counts = connection.execute(
            text(
                """
                SELECT
                    (SELECT count(*) FROM job_runs),
                    (SELECT count(*) FROM job_attempts),
                    (SELECT count(*) FROM outbox_events),
                    (SELECT count(*) FROM idempotency_records)
                """
            )
        ).one()
    assert counts == (1, 1, 1, 1)

    with pytest.raises(IdempotencyConflictError):
        p4_store.create_job(
            task_key="populate-metadata",
            payload={"force_full_update": True},
            max_attempts=2,
            idempotency_key="request-1",
        )

    with pytest.raises(InvalidJsonPayloadError):
        p4_store.create_job(task_key="populate-metadata", payload={"bad": lambda: None})
    with p4_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM job_runs")) == 1


def test_concurrent_same_idempotency_key_commits_one_job(p4_store: PostgresJobStore) -> None:
    barrier = Barrier(2)
    results = []
    errors: list[BaseException] = []

    def submit() -> None:
        try:
            barrier.wait(timeout=2)
            results.append(
                p4_store.create_job(
                    task_key="populate-metadata",
                    payload={"force_full_update": False},
                    idempotency_key="concurrent-key",
                )
            )
        except BaseException as exc:
            errors.append(exc)

    threads = [Thread(target=submit, daemon=True) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5)

    assert not errors
    assert len(results) == 2
    assert {result.job.id for result in results} == {results[0].job.id}
    assert sorted(result.reused for result in results) == [False, True]


def test_current_busy_children_cancel_and_terminal_log_retention(p4_store: PostgresJobStore) -> None:
    parent = p4_store.create_job(task_key="task-chain-high-freq", payload={}, max_attempts=1)
    with pytest.raises(ActiveJobConflictError):
        p4_store.create_job(task_key="second-root", payload={})
    child = p4_store.create_job(
        task_key="populate-metadata",
        payload={},
        parent_job_id=parent.job.id,
        workflow_step=1,
    )

    assert p4_store.has_active_job()
    assert p4_store.get_current_job().job.id == parent.job.id  # type: ignore[union-attr]
    assert [item.job.id for item in p4_store.list_children(parent.job.id)] == [child.job.id]

    for index in range(5):
        p4_store.append_log(parent.job.id, {"index": index, "message": f"line-{index}"}, max_entries=3)
    cancelled = p4_store.request_cancel(parent.job.id)

    assert cancelled.job.status is JobStatus.CANCELLED
    assert cancelled.current_attempt.status is AttemptStatus.CANCELLED
    assert [entry["index"] for entry in cancelled.logs] == [2, 3, 4]
    retained = p4_store.get_job(parent.job.id)
    assert retained is not None and retained.logs == cancelled.logs

    p4_store.request_cancel(child.job.id)
    assert not p4_store.has_active_job()
    assert p4_store.get_current_job() is not None
    assert p4_store.request_cancel_current().job.status.terminal  # type: ignore[union-attr]


def test_inline_workflow_child_has_no_broker_dispatch(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
) -> None:
    parent = p4_store.create_job(task_key="task-chain-high-freq", payload={})
    child = p4_store.create_job(
        task_key="populate-metadata",
        payload={},
        parent_job_id=parent.job.id,
        workflow_step=1,
        dispatch=False,
    )

    with p4_runtime.engine.connect() as connection:
        dispatched_attempts = set(connection.scalars(text("SELECT aggregate_id FROM outbox_events")))

    assert dispatched_attempts == {str(parent.attempt.id)}
    assert str(child.attempt.id) not in dispatched_attempts
    with pytest.raises(ValueError, match="only workflow children"):
        p4_store.create_job(task_key="undispatched-root", payload={}, dispatch=False)


def test_root_backlog_claims_are_serialized_across_workers(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
) -> None:
    first = p4_store.create_job(task_key="first", payload={})
    second = p4_store.create_job(task_key="second", payload={}, queue_name="legacy.backlog")
    recorder = RecordingPublisher()
    assert p4_store.publish_outbox_batch(recorder, publisher_id="publisher-serial").published == 2

    first_claim = p4_store.claim_attempt(
        job_id=first.job.id,
        attempt_id=first.attempt.id,
        worker_id="worker-one",
        lease_seconds=30,
    )
    blocked_claim = p4_store.claim_attempt(
        job_id=second.job.id,
        attempt_id=second.attempt.id,
        worker_id="worker-two",
        lease_seconds=30,
    )

    assert first_claim.claimed
    assert not blocked_claim.claimed
    with p4_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT status FROM outbox_events WHERE aggregate_id = :attempt_id"),
                {"attempt_id": str(second.attempt.id)},
            )
            == "pending"
        )

    p4_store.cancel_attempt(
        job_id=first.job.id,
        attempt_id=first.attempt.id,
        worker_id="worker-one",
        reason="fixture complete",
    )
    second_claim = p4_store.claim_attempt(
        job_id=second.job.id,
        attempt_id=second.attempt.id,
        worker_id="worker-two",
        lease_seconds=30,
    )
    assert second_claim.claimed


def test_coordinator_detects_a_terminated_leadership_session(p4_runtime: DatabaseRuntime) -> None:
    first = CoordinatorLeadership(p4_runtime.raw_pool)
    second = CoordinatorLeadership(p4_runtime.raw_pool)
    assert first.try_acquire()
    assert not second.try_acquire()

    backend_pid = first._connection.execute("SELECT pg_backend_pid()").fetchone()[0]  # type: ignore[union-attr]
    with p4_runtime.engine.begin() as connection:
        assert connection.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": backend_pid})

    assert not first.validate()
    assert not first.acquired
    assert second.try_acquire()
    second.release()


def test_claim_marks_pending_dispatch_processed_and_duplicate_is_noop(p4_store: PostgresJobStore) -> None:
    created = p4_store.create_job(task_key="populate-metadata", payload={})

    first = p4_store.claim_attempt(
        job_id=created.job.id,
        attempt_id=created.attempt.id,
        worker_id="worker-1",
        lease_seconds=30,
    )
    duplicate = p4_store.claim_attempt(
        job_id=created.job.id,
        attempt_id=created.attempt.id,
        worker_id="worker-2",
        lease_seconds=30,
    )

    assert first.claimed
    assert first.projection.current_attempt.status is AttemptStatus.CLAIMED
    assert not duplicate.claimed
    claims = p4_store.claim_outbox(publisher_id="publisher", limit=10)
    assert claims == ()

    p4_store.start_attempt(
        job_id=created.job.id,
        attempt_id=created.attempt.id,
        worker_id="worker-1",
        lease_seconds=30,
    )
    heartbeat = p4_store.heartbeat(
        job_id=created.job.id,
        attempt_id=created.attempt.id,
        worker_id="worker-1",
        lease_seconds=30,
        progress=25,
        message="working",
        log_entry={"level": "info", "message": "working"},
    )
    assert heartbeat.accepted and not heartbeat.cancel_requested
    assert heartbeat.projection.job.progress == 25

    finished = p4_store.succeed_attempt(
        job_id=created.job.id,
        attempt_id=created.attempt.id,
        worker_id="worker-1",
        result={"processed": 3},
    )
    assert finished.job.status is JobStatus.SUCCEEDED
    assert finished.current_attempt.status is AttemptStatus.SUCCEEDED
    assert finished.job.result == {"processed": 3}


def test_running_cancel_is_persistent_and_wins_terminal_race(p4_store: PostgresJobStore) -> None:
    created = p4_store.create_job(task_key="populate-metadata", payload={})
    p4_store.claim_attempt(
        job_id=created.job.id,
        attempt_id=created.attempt.id,
        worker_id="worker-1",
        lease_seconds=30,
    )
    p4_store.start_attempt(
        job_id=created.job.id,
        attempt_id=created.attempt.id,
        worker_id="worker-1",
        lease_seconds=30,
    )

    requested = p4_store.request_cancel(created.job.id)
    heartbeat = p4_store.heartbeat(
        job_id=created.job.id,
        attempt_id=created.attempt.id,
        worker_id="worker-1",
        lease_seconds=30,
    )
    raced_success = p4_store.succeed_attempt(
        job_id=created.job.id,
        attempt_id=created.attempt.id,
        worker_id="worker-1",
        result={"would_have_succeeded": True},
    )

    assert requested.job.status is JobStatus.CANCEL_REQUESTED
    assert heartbeat.cancel_requested
    assert raced_success.job.status is JobStatus.CANCELLED
    assert raced_success.current_attempt.status is AttemptStatus.CANCELLED


def test_advisory_guard_allows_only_one_execution_session(p4_store: PostgresJobStore) -> None:
    job_id = uuid4()
    entered = Event()
    release = Event()
    result: list[bool] = []

    def holder() -> None:
        with p4_store.execution_guard(job_id) as acquired:
            result.append(acquired)
            entered.set()
            assert release.wait(2)

    thread = Thread(target=holder, daemon=True)
    thread.start()
    assert entered.wait(2)
    with p4_store.execution_guard(job_id) as acquired:
        result.append(acquired)
    release.set()
    thread.join(2)

    assert result == [True, False]
    with p4_store.execution_guard(job_id) as acquired:
        assert acquired


def test_failure_retry_due_and_timeout_terminal(p4_store: PostgresJobStore) -> None:
    created = p4_store.create_job(task_key="populate-metadata", payload={}, max_attempts=2)
    p4_store.claim_attempt(
        job_id=created.job.id,
        attempt_id=created.attempt.id,
        worker_id="worker-1",
        lease_seconds=30,
    )
    p4_store.start_attempt(
        job_id=created.job.id,
        attempt_id=created.attempt.id,
        worker_id="worker-1",
        lease_seconds=30,
    )
    waiting = p4_store.fail_attempt(
        job_id=created.job.id,
        attempt_id=created.attempt.id,
        worker_id="worker-1",
        error_type="temporary",
        error_message="retry me",
        retryable=True,
        next_attempt_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    assert waiting.job.status is JobStatus.RETRY_WAIT

    attempts = p4_store.enqueue_due_retries()
    assert len(attempts) == 1 and attempts[0].attempt_no == 2
    current = p4_store.get_job(created.job.id)
    assert current is not None and current.job.status is JobStatus.QUEUED

    p4_store.claim_attempt(
        job_id=current.job.id,
        attempt_id=current.current_attempt.id,
        worker_id="worker-2",
        lease_seconds=30,
    )
    timed_out = p4_store.timeout_attempt(
        job_id=current.job.id,
        attempt_id=current.current_attempt.id,
        worker_id="worker-2",
        retryable=True,
    )
    assert timed_out.job.status is JobStatus.TIMED_OUT
    assert timed_out.current_attempt.status is AttemptStatus.TIMED_OUT


def test_terminal_job_resume_is_concurrent_safe_and_preserves_idempotent_identity(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
) -> None:
    created = p4_store.create_job(
        task_key="identity-post-authorization",
        payload={"revision": "fixture"},
        queue_name="legacy.background",
        idempotency_scope="identity-post-authorization",
        idempotency_key="fixture-revision",
    )
    p4_store.claim_attempt(
        job_id=created.job.id,
        attempt_id=created.attempt.id,
        worker_id="worker-1",
        lease_seconds=30,
    )
    p4_store.start_attempt(
        job_id=created.job.id,
        attempt_id=created.attempt.id,
        worker_id="worker-1",
        lease_seconds=30,
    )
    failed = p4_store.fail_attempt(
        job_id=created.job.id,
        attempt_id=created.attempt.id,
        worker_id="worker-1",
        error_type="fixture",
        error_message="explicit resume required",
        retryable=False,
    )
    assert failed.job.status is JobStatus.FAILED

    barrier = Barrier(2)
    results = []
    errors: list[BaseException] = []

    def resume() -> None:
        try:
            barrier.wait(timeout=2)
            results.append(p4_store.resume_terminal_job(created.job.id))
        except BaseException as exc:
            errors.append(exc)

    threads = [Thread(target=resume, daemon=True) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5)

    assert errors == []
    assert len(results) == 2
    assert sorted(result.resumed for result in results) == [False, True]
    assert {result.projection.job.id for result in results} == {created.job.id}
    assert {result.projection.current_attempt.attempt_no for result in results} == {2}
    recovered = p4_store.get_idempotent_job(
        scope="identity-post-authorization",
        key="fixture-revision",
    )
    assert recovered is not None
    assert recovered.job.id == created.job.id
    assert recovered.job.status is JobStatus.QUEUED
    assert recovered.job.max_attempts == 2

    with p4_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT count(*) FROM job_attempts WHERE job_id = :job_id"),
                {"job_id": created.job.id},
            )
            == 2
        )


def test_stale_attempt_recovery_marks_lost_and_requeues(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
) -> None:
    created = p4_store.create_job(task_key="populate-metadata", payload={}, max_attempts=2)
    p4_store.claim_attempt(
        job_id=created.job.id,
        attempt_id=created.attempt.id,
        worker_id="doomed-worker",
        lease_seconds=30,
    )
    with p4_runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE job_attempts SET lease_expires_at = NOW() - interval '1 second' WHERE id = :id"),
            {"id": created.attempt.id},
        )

    recovery = p4_store.recover_stale_attempts()
    projection = p4_store.get_job(created.job.id)

    assert recovery.lost_attempts == 1
    assert recovery.requeued_jobs == 1
    assert projection is not None
    assert projection.job.status is JobStatus.RETRY_WAIT
    assert projection.current_attempt.status is AttemptStatus.LOST


def test_stale_native_operation_is_indeterminate_and_never_requeued(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
) -> None:
    created = p4_store.create_job(
        task_key="native-operation:p115.quick-deploy",
        payload={"native_operation": "p115.quick-deploy", "parameters": {}},
        max_attempts=2,
    )
    p4_store.claim_attempt(
        job_id=created.job.id,
        attempt_id=created.attempt.id,
        worker_id="lost-native-worker",
        lease_seconds=30,
    )
    with p4_runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE job_attempts SET lease_expires_at = NOW() - interval '1 second' WHERE id = :id"),
            {"id": created.attempt.id},
        )

    recovery = p4_store.recover_stale_attempts()
    projection = p4_store.get_job(created.job.id)

    assert recovery.lost_attempts == 1
    assert recovery.requeued_jobs == 0
    assert recovery.failed_jobs == 1
    assert projection is not None
    assert projection.job.status is JobStatus.FAILED
    assert projection.current_attempt.status is AttemptStatus.LOST
    assert projection.current_attempt.retryable is False
    assert projection.current_attempt.error_type == "PersistentEffectIndeterminate"
    assert projection.job.error_summary == {
        "type": "PersistentEffectIndeterminate",
        "message": "native operation worker lease expired after execution started; external effect outcome is unknown",
        "retry_exhausted": True,
    }

    with pytest.raises(InvalidStateTransitionError, match="cannot be resumed"):
        p4_store.resume_terminal_job(created.job.id)

    with p4_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT count(*) FROM job_attempts WHERE job_id = :job_id"),
                {"job_id": created.job.id},
            )
            == 1
        )
        assert (
            connection.scalar(
                text("SELECT count(*) FROM outbox_events WHERE aggregate_id = :attempt_id"),
                {"attempt_id": str(created.attempt.id)},
            )
            == 1
        )


class FailingPublisher:
    def publish(self, claim) -> None:
        del claim
        raise ConnectionError("redis unavailable")


class RecordingPublisher:
    def __init__(self) -> None:
        self.events = []

    def publish(self, claim) -> None:
        self.events.append(claim.event.id)


def test_dead_dispatch_fails_the_job_and_releases_the_serial_queue(p4_store: PostgresJobStore) -> None:
    created = p4_store.create_job(
        task_key="redis-permanently-unavailable",
        payload={},
        max_publish_attempts=1,
    )

    result = p4_store.publish_outbox_batch(
        FailingPublisher(),
        publisher_id="publisher-dead",
        failure_backoff_seconds=0,
    )

    assert result.dead == 1
    failed = p4_store.get_job(created.job.id)
    assert failed is not None
    assert failed.job.status is JobStatus.FAILED
    assert failed.current_attempt.status is AttemptStatus.FAILED
    assert failed.job.error_summary == {"type": "dispatch_failed", "message": "redis unavailable"}
    assert not p4_store.has_active_job()
    assert p4_store.create_job(task_key="replacement", payload={}).job.status is JobStatus.QUEUED


def test_outbox_skip_locked_claim_failure_and_publish_recovery(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
) -> None:
    p4_store.create_job(task_key="one", payload={}, queue_name="fixture.one")
    p4_store.create_job(task_key="two", payload={}, queue_name="fixture.two")

    first = p4_store.claim_outbox(publisher_id="publisher-1", limit=1)
    second = p4_store.claim_outbox(publisher_id="publisher-2", limit=10)
    assert len(first) == 1 and len(second) == 1
    assert first[0].event.id != second[0].event.id

    assert (
        p4_store.mark_outbox_failed(
            first[0].event.id,
            first[0].claim_token,
            error="redis unavailable",
            backoff_seconds=0,
        )
        is OutboxStatus.PENDING
    )
    assert p4_store.mark_outbox_published(second[0].event.id, second[0].claim_token)

    failed = p4_store.publish_outbox_batch(
        FailingPublisher(),
        publisher_id="publisher-3",
        failure_backoff_seconds=0,
    )
    assert failed.claimed == 1 and failed.failed == 1

    recorder = RecordingPublisher()
    published = p4_store.publish_outbox_batch(
        recorder,
        publisher_id="publisher-4",
        failure_backoff_seconds=0,
    )
    assert published.published == 1
    assert len(recorder.events) == 1

    with p4_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE outbox_events
                SET published_at = NOW() - interval '10 minutes'
                WHERE status = 'published' AND processed_at IS NULL
                """
            )
        )
    recovery = p4_store.requeue_stale_published(processed_timeout_seconds=60)
    assert recovery.requeued_events >= 1


def test_busy_root_defers_an_outbox_event_that_is_still_publishing(
    p4_store: PostgresJobStore,
    p4_runtime: DatabaseRuntime,
) -> None:
    running = p4_store.create_job(task_key="running-root", payload={})
    queued = p4_store.create_job(
        task_key="queued-root",
        payload={},
        queue_name="legacy.backlog",
    )

    first_claim = p4_store.claim_outbox(publisher_id="publisher-running", limit=1)
    assert len(first_claim) == 1
    assert first_claim[0].event.aggregate_id == str(running.attempt.id)
    claimed_running = p4_store.claim_attempt(
        job_id=running.job.id,
        attempt_id=running.attempt.id,
        worker_id="worker-running",
        lease_seconds=30,
    )
    assert claimed_running.claimed
    p4_store.start_attempt(
        job_id=running.job.id,
        attempt_id=running.attempt.id,
        worker_id="worker-running",
        lease_seconds=30,
    )

    publishing_claim = p4_store.claim_outbox(publisher_id="publisher-queued", limit=1)
    assert len(publishing_claim) == 1
    assert publishing_claim[0].event.aggregate_id == str(queued.attempt.id)

    deferred = p4_store.claim_attempt(
        job_id=queued.job.id,
        attempt_id=queued.attempt.id,
        worker_id="worker-queued",
        lease_seconds=30,
    )

    assert not deferred.claimed
    assert not p4_store.mark_outbox_published(
        publishing_claim[0].event.id,
        publishing_claim[0].claim_token,
    )
    with p4_runtime.engine.connect() as connection:
        event = connection.execute(
            text(
                """
                SELECT status, claim_token, claimed_by, claimed_at,
                       available_at > NOW() AS delayed
                FROM outbox_events
                WHERE id = :event_id
                """
            ),
            {"event_id": publishing_claim[0].event.id},
        ).one()
    assert event == ("pending", None, None, None, True)
