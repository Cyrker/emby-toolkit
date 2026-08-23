from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import Session

from .types import (
    AttemptRecord,
    AttemptStatus,
    JobProjection,
    JobRecord,
    JobStatus,
    OutboxEventRecord,
    OutboxStatus,
)


class SqlAlchemyJobRepository:
    """PostgreSQL job ledger operations; transaction ownership stays with the UoW."""

    def __init__(self, session: Session):
        self.session = session

    def insert_job(self, values: Mapping[str, Any]) -> None:
        self.session.execute(
            text(
                """
                INSERT INTO job_runs (
                    id, task_key, trigger_type, queue_name, payload_version,
                    request_payload_json, requested_by_user_id, parent_job_id,
                    workflow_step, status, max_attempts, current_attempt,
                    timeout_seconds, scheduled_for, trace_id
                ) VALUES (
                    :id, :task_key, :trigger_type, :queue_name, :payload_version,
                    CAST(:request_payload_json AS jsonb), :requested_by_user_id, :parent_job_id,
                    :workflow_step, 'queued', :max_attempts, 1,
                    :timeout_seconds, COALESCE(:scheduled_for, NOW()), :trace_id
                )
                """
            ),
            dict(values),
        )

    def insert_attempt(self, *, attempt_id: UUID, job_id: UUID, attempt_no: int) -> None:
        self.session.execute(
            text(
                """
                INSERT INTO job_attempts (id, job_id, attempt_no, status, celery_task_id)
                VALUES (:id, :job_id, :attempt_no, 'created', :celery_task_id)
                """
            ),
            {
                "id": attempt_id,
                "job_id": job_id,
                "attempt_no": attempt_no,
                "celery_task_id": str(attempt_id),
            },
        )

    def insert_dispatch_event(
        self,
        *,
        event_id: UUID,
        job_id: UUID,
        attempt_id: UUID,
        available_at: datetime | None = None,
        max_publish_attempts: int = 20,
    ) -> None:
        payload = json.dumps(
            {"event_id": str(event_id), "job_id": str(job_id), "attempt_id": str(attempt_id)},
            separators=(",", ":"),
            sort_keys=True,
        )
        self.session.execute(
            text(
                """
                INSERT INTO outbox_events (
                    id, event_key, topic, aggregate_type, aggregate_id,
                    payload_version, payload_json, status, available_at,
                    max_publish_attempts
                ) VALUES (
                    :id, :event_key, 'job.dispatch.requested', 'job_attempt', :aggregate_id,
                    1, CAST(:payload_json AS jsonb), 'pending', COALESCE(:available_at, NOW()),
                    :max_publish_attempts
                )
                """
            ),
            {
                "id": event_id,
                "event_key": f"job.dispatch:{attempt_id}",
                "aggregate_id": str(attempt_id),
                "payload_json": payload,
                "available_at": available_at,
                "max_publish_attempts": max_publish_attempts,
            },
        )

    def insert_idempotency(
        self,
        *,
        record_id: UUID,
        scope: str,
        key_hash: bytes,
        request_hash: bytes,
        job_id: UUID,
        attempt_id: UUID,
        result_json: str,
    ) -> None:
        self.session.execute(
            text(
                """
                INSERT INTO idempotency_records (
                    id, scope, key_hash, request_hash, status, job_id, attempt_id,
                    result_json, completed_at
                ) VALUES (
                    :id, :scope, :key_hash, :request_hash, 'completed', :job_id, :attempt_id,
                    CAST(:result_json AS jsonb), NOW()
                )
                """
            ),
            {
                "id": record_id,
                "scope": scope,
                "key_hash": key_hash,
                "request_hash": request_hash,
                "job_id": job_id,
                "attempt_id": attempt_id,
                "result_json": result_json,
            },
        )

    def get_idempotency(
        self,
        *,
        scope: str,
        key_hash: bytes,
        for_update: bool = False,
    ) -> RowMapping | None:
        suffix = " FOR UPDATE" if for_update else ""
        return (
            self.session.execute(
                text(
                    """
                SELECT id, scope, key_hash, request_hash, status, job_id, attempt_id,
                       result_json, completed_at, expires_at
                FROM idempotency_records
                WHERE scope = :scope AND key_hash = :key_hash
                """
                    + suffix
                ),
                {"scope": scope, "key_hash": key_hash},
            )
            .mappings()
            .one_or_none()
        )

    def get_job_row(self, job_id: UUID, *, for_update: bool = False) -> RowMapping | None:
        suffix = " FOR UPDATE" if for_update else ""
        return (
            self.session.execute(
                text("SELECT * FROM job_runs WHERE id = :job_id" + suffix),
                {"job_id": job_id},
            )
            .mappings()
            .one_or_none()
        )

    def get_attempt_row(self, attempt_id: UUID, *, for_update: bool = False) -> RowMapping | None:
        suffix = " FOR UPDATE" if for_update else ""
        return (
            self.session.execute(
                text("SELECT * FROM job_attempts WHERE id = :attempt_id" + suffix),
                {"attempt_id": attempt_id},
            )
            .mappings()
            .one_or_none()
        )

    def get_current_attempt_row(self, job_id: UUID, attempt_no: int, *, for_update: bool = False) -> RowMapping | None:
        suffix = " FOR UPDATE" if for_update else ""
        return (
            self.session.execute(
                text(
                    """
                SELECT * FROM job_attempts
                WHERE job_id = :job_id AND attempt_no = :attempt_no
                """
                    + suffix
                ),
                {"job_id": job_id, "attempt_no": attempt_no},
            )
            .mappings()
            .one_or_none()
        )

    def projection(self, job_id: UUID) -> JobProjection | None:
        job = self.get_job_row(job_id)
        if job is None:
            return None
        attempt = self.get_current_attempt_row(job_id, int(job["current_attempt"]))
        if attempt is None:
            return None
        return _projection(job, attempt)

    def current_projection(self) -> JobProjection | None:
        job = (
            self.session.execute(
                text(
                    """
                SELECT * FROM job_runs
                ORDER BY
                    CASE WHEN status IN ('queued', 'running', 'retry_wait', 'cancel_requested') THEN 0 ELSE 1 END,
                    CASE WHEN status IN ('queued', 'running', 'retry_wait', 'cancel_requested') THEN created_at END ASC,
                    CASE
                        WHEN status NOT IN ('queued', 'running', 'retry_wait', 'cancel_requested') THEN finished_at
                    END DESC NULLS LAST,
                    created_at DESC
                LIMIT 1
                """
                )
            )
            .mappings()
            .one_or_none()
        )
        if job is None:
            return None
        attempt = self.get_current_attempt_row(job["id"], int(job["current_attempt"]))
        return _projection(job, attempt) if attempt is not None else None

    def has_active_job(self) -> bool:
        return bool(
            self.session.scalar(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM job_runs
                        WHERE status IN ('queued', 'running', 'retry_wait', 'cancel_requested')
                    )
                    """
                )
            )
        )

    def acquire_serial_claim_lock(self, namespace: int, key: int) -> None:
        self.session.execute(
            text("SELECT pg_advisory_xact_lock(:namespace, :key)"),
            {"namespace": namespace, "key": key},
        )

    def has_other_running_root(self, job_id: UUID) -> bool:
        return bool(
            self.session.scalar(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM job_runs
                        WHERE id <> :job_id
                          AND parent_job_id IS NULL
                          AND status IN ('running', 'cancel_requested')
                    )
                    """
                ),
                {"job_id": job_id},
            )
        )

    def defer_dispatch(self, attempt_id: UUID, *, delay_seconds: float) -> bool:
        result = self.session.execute(
            text(
                """
                UPDATE outbox_events
                SET status = 'pending', available_at = NOW() + make_interval(secs => :delay_seconds),
                    published_at = NULL, processed_at = NULL,
                    claim_token = NULL, claimed_by = NULL, claimed_at = NULL, updated_at = NOW()
                WHERE topic = 'job.dispatch.requested'
                  AND aggregate_type = 'job_attempt'
                  AND aggregate_id = :attempt_id
                  AND status IN ('pending', 'publishing', 'published')
                RETURNING id
                """
            ),
            {"attempt_id": str(attempt_id), "delay_seconds": delay_seconds},
        ).scalar_one_or_none()
        return result is not None

    def list_children(self, parent_job_id: UUID) -> tuple[JobProjection, ...]:
        rows = (
            self.session.execute(
                text(
                    """
                SELECT * FROM job_runs
                WHERE parent_job_id = :parent_job_id
                ORDER BY workflow_step ASC NULLS LAST, created_at ASC
                """
                ),
                {"parent_job_id": parent_job_id},
            )
            .mappings()
            .all()
        )
        result: list[JobProjection] = []
        for job in rows:
            attempt = self.get_current_attempt_row(job["id"], int(job["current_attempt"]))
            if attempt is not None:
                result.append(_projection(job, attempt))
        return tuple(result)

    def mark_job_running(self, job_id: UUID) -> None:
        self.session.execute(
            text(
                """
                UPDATE job_runs
                SET status = 'running', started_at = COALESCE(started_at, NOW()),
                    next_attempt_at = NULL, updated_at = NOW()
                WHERE id = :job_id
                """
            ),
            {"job_id": job_id},
        )

    def mark_job_cancel_requested(self, job_id: UUID) -> None:
        self.session.execute(
            text(
                """
                UPDATE job_runs
                SET status = 'cancel_requested', cancel_requested_at = COALESCE(cancel_requested_at, NOW()),
                    updated_at = NOW()
                WHERE id = :job_id
                """
            ),
            {"job_id": job_id},
        )

    def mark_job_terminal(
        self,
        job_id: UUID,
        status: JobStatus,
        *,
        result_json: str | None = None,
        error_summary_json: str | None = None,
    ) -> None:
        self.session.execute(
            text(
                """
                UPDATE job_runs
                SET status = :status, progress = CASE WHEN :status = 'succeeded' THEN 100 ELSE progress END,
                    result_json = CAST(:result_json AS jsonb),
                    error_summary_json = CAST(:error_summary_json AS jsonb),
                    finished_at = NOW(), next_attempt_at = NULL, updated_at = NOW()
                WHERE id = :job_id
                """
            ),
            {
                "job_id": job_id,
                "status": status.value,
                "result_json": result_json,
                "error_summary_json": error_summary_json,
            },
        )

    def mark_job_retry_wait(self, job_id: UUID, *, next_attempt_at: datetime | None, error_summary_json: str) -> None:
        self.session.execute(
            text(
                """
                UPDATE job_runs
                SET status = 'retry_wait', next_attempt_at = COALESCE(:next_attempt_at, NOW()),
                    error_summary_json = CAST(:error_summary_json AS jsonb), updated_at = NOW()
                WHERE id = :job_id
                """
            ),
            {
                "job_id": job_id,
                "next_attempt_at": next_attempt_at,
                "error_summary_json": error_summary_json,
            },
        )

    def mark_job_queued_for_retry(self, job_id: UUID, *, attempt_no: int) -> None:
        self.session.execute(
            text(
                """
                UPDATE job_runs
                SET status = 'queued', current_attempt = :attempt_no, scheduled_for = NOW(),
                    next_attempt_at = NULL, updated_at = NOW()
                WHERE id = :job_id
                """
            ),
            {"job_id": job_id, "attempt_no": attempt_no},
        )

    def mark_terminal_job_resumed(
        self,
        job_id: UUID,
        *,
        attempt_no: int,
        max_attempts: int,
    ) -> bool:
        resumed = self.session.execute(
            text(
                """
                UPDATE job_runs
                SET status = 'queued', progress = 0, progress_message = '',
                    current_attempt = :attempt_no, max_attempts = :max_attempts,
                    scheduled_for = NOW(), next_attempt_at = NULL,
                    cancel_requested_at = NULL, finished_at = NULL,
                    result_json = NULL, error_summary_json = NULL, updated_at = NOW()
                WHERE id = :job_id
                  AND status IN ('failed', 'timed_out', 'cancelled')
                RETURNING id
                """
            ),
            {
                "job_id": job_id,
                "attempt_no": attempt_no,
                "max_attempts": max_attempts,
            },
        ).scalar_one_or_none()
        return resumed is not None

    def mark_attempt_claimed(self, attempt_id: UUID, *, worker_id: str, lease_seconds: float) -> None:
        self.session.execute(
            text(
                """
                UPDATE job_attempts
                SET status = 'claimed', worker_id = :worker_id, claimed_at = NOW(),
                    last_heartbeat_at = NOW(),
                    lease_expires_at = NOW() + make_interval(secs => :lease_seconds),
                    updated_at = NOW()
                WHERE id = :attempt_id
                """
            ),
            {"attempt_id": attempt_id, "worker_id": worker_id, "lease_seconds": lease_seconds},
        )

    def mark_attempt_running(self, attempt_id: UUID, *, worker_id: str, lease_seconds: float) -> None:
        self.session.execute(
            text(
                """
                UPDATE job_attempts
                SET status = 'running', started_at = COALESCE(started_at, NOW()),
                    last_heartbeat_at = NOW(),
                    lease_expires_at = NOW() + make_interval(secs => :lease_seconds),
                    updated_at = NOW()
                WHERE id = :attempt_id AND worker_id = :worker_id
                """
            ),
            {"attempt_id": attempt_id, "worker_id": worker_id, "lease_seconds": lease_seconds},
        )

    def update_heartbeat(
        self,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        lease_seconds: float,
        progress: int | None,
        message: str | None,
    ) -> None:
        self.session.execute(
            text(
                """
                UPDATE job_attempts
                SET last_heartbeat_at = NOW(),
                    lease_expires_at = NOW() + make_interval(secs => :lease_seconds),
                    updated_at = NOW()
                WHERE id = :attempt_id AND worker_id = :worker_id
                """
            ),
            {"attempt_id": attempt_id, "worker_id": worker_id, "lease_seconds": lease_seconds},
        )
        self.session.execute(
            text(
                """
                UPDATE job_runs
                SET progress = COALESCE(:progress, progress),
                    progress_message = COALESCE(:message, progress_message),
                    updated_at = NOW()
                WHERE id = :job_id
                """
            ),
            {"job_id": job_id, "progress": progress, "message": message},
        )

    def mark_attempt_terminal(
        self,
        attempt_id: UUID,
        status: AttemptStatus,
        *,
        retryable: bool | None,
        error_type: str | None,
        error_message: str | None,
        error_details_json: str | None,
        result_json: str | None,
    ) -> None:
        self.session.execute(
            text(
                """
                UPDATE job_attempts
                SET status = :status, retryable = :retryable,
                    error_type = :error_type, error_message = :error_message,
                    error_details_json = CAST(:error_details_json AS jsonb),
                    result_json = CAST(:result_json AS jsonb),
                    finished_at = NOW(), lease_expires_at = NULL, updated_at = NOW()
                WHERE id = :attempt_id
                """
            ),
            {
                "attempt_id": attempt_id,
                "status": status.value,
                "retryable": retryable,
                "error_type": error_type,
                "error_message": error_message,
                "error_details_json": error_details_json,
                "result_json": result_json,
            },
        )

    def mark_dispatch_processed(self, attempt_id: UUID) -> None:
        self.session.execute(
            text(
                """
                UPDATE outbox_events
                SET status = 'processed', processed_at = COALESCE(processed_at, NOW()),
                    claim_token = NULL, claimed_by = NULL, claimed_at = NULL, updated_at = NOW()
                WHERE event_key = :event_key
                  AND status IN ('pending', 'publishing', 'published')
                """
            ),
            {"event_key": f"job.dispatch:{attempt_id}"},
        )

    def cancel_dispatch(self, attempt_id: UUID) -> None:
        self.session.execute(
            text(
                """
                UPDATE outbox_events
                SET status = 'cancelled', claim_token = NULL, claimed_by = NULL, claimed_at = NULL,
                    updated_at = NOW()
                WHERE event_key = :event_key
                  AND status IN ('pending', 'publishing', 'published')
                """
            ),
            {"event_key": f"job.dispatch:{attempt_id}"},
        )

    def append_log(self, job_id: UUID, *, entry_json: str, max_entries: int) -> bool:
        result = self.session.execute(
            text(
                """
                UPDATE job_runs
                SET logs_json = (
                    SELECT COALESCE(jsonb_agg(item ORDER BY ordinal), '[]'::jsonb)
                    FROM (
                        SELECT item, ordinal
                        FROM jsonb_array_elements(
                            job_runs.logs_json || jsonb_build_array(CAST(:entry_json AS jsonb))
                        ) WITH ORDINALITY AS expanded(item, ordinal)
                        ORDER BY ordinal DESC
                        LIMIT :max_entries
                    ) AS recent
                ),
                updated_at = NOW()
                WHERE id = :job_id
                RETURNING id
                """
            ),
            {"job_id": job_id, "entry_json": entry_json, "max_entries": max_entries},
        ).scalar_one_or_none()
        return result is not None

    def due_retry_rows(self, *, limit: int) -> Sequence[RowMapping]:
        return (
            self.session.execute(
                text(
                    """
                SELECT * FROM job_runs
                WHERE status = 'retry_wait' AND next_attempt_at <= NOW()
                ORDER BY next_attempt_at, created_at
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
                """
                ),
                {"limit": limit},
            )
            .mappings()
            .all()
        )

    def stale_attempt_rows(self, *, limit: int) -> Sequence[RowMapping]:
        return (
            self.session.execute(
                text(
                    """
                SELECT
                    a.id AS attempt_id, a.job_id, a.attempt_no, a.worker_id,
                    j.status AS job_status, j.max_attempts, j.current_attempt
                FROM job_attempts AS a
                JOIN job_runs AS j ON j.id = a.job_id
                WHERE a.status IN ('claimed', 'running')
                  AND a.lease_expires_at < NOW()
                  AND j.status IN ('running', 'cancel_requested')
                  AND j.current_attempt = a.attempt_no
                ORDER BY a.lease_expires_at, a.created_at
                LIMIT :limit
                FOR UPDATE OF a, j SKIP LOCKED
                """
                ),
                {"limit": limit},
            )
            .mappings()
            .all()
        )

    def claim_outbox(
        self,
        *,
        publisher_id: str,
        claim_token: UUID,
        limit: int,
        claim_timeout_seconds: float,
        topics: Sequence[str] | None = None,
    ) -> tuple[OutboxEventRecord, ...]:
        rows = (
            self.session.execute(
                text(
                    """
                WITH candidates AS (
                    SELECT id
                    FROM outbox_events
                    WHERE (
                        (status = 'pending' AND available_at <= NOW())
                        OR (
                            status = 'publishing'
                            AND claimed_at < NOW() - make_interval(secs => :claim_timeout_seconds)
                        )
                    )
                      AND (
                        CAST(:topics AS text[]) IS NULL
                        OR topic = ANY(CAST(:topics AS text[]))
                      )
                    ORDER BY available_at, created_at
                    LIMIT :limit
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE outbox_events AS event
                SET status = 'publishing', claimed_by = :publisher_id,
                    claim_token = :claim_token, claimed_at = NOW(),
                    publish_attempts = publish_attempts + 1, updated_at = NOW()
                FROM candidates
                WHERE event.id = candidates.id
                RETURNING event.*
                """
                ),
                {
                    "publisher_id": publisher_id,
                    "claim_token": claim_token,
                    "claim_timeout_seconds": claim_timeout_seconds,
                    "limit": limit,
                    "topics": list(topics) if topics is not None else None,
                },
            )
            .mappings()
            .all()
        )
        return tuple(_outbox_record(row) for row in rows)

    def mark_outbox_published(self, event_id: UUID, claim_token: UUID) -> bool:
        result = self.session.execute(
            text(
                """
                UPDATE outbox_events
                SET status = 'published', published_at = NOW(), last_error = NULL,
                    claim_token = NULL, claimed_by = NULL, claimed_at = NULL, updated_at = NOW()
                WHERE id = :event_id AND status = 'publishing' AND claim_token = :claim_token
                RETURNING id
                """
            ),
            {"event_id": event_id, "claim_token": claim_token},
        ).scalar_one_or_none()
        return result is not None

    def mark_outbox_failed(
        self,
        event_id: UUID,
        claim_token: UUID,
        *,
        error: str,
        backoff_seconds: float,
    ) -> OutboxStatus | None:
        row = (
            self.session.execute(
                text(
                    """
                UPDATE outbox_events
                SET status = CASE WHEN publish_attempts >= max_publish_attempts THEN 'dead' ELSE 'pending' END,
                    available_at = CASE
                        WHEN publish_attempts >= max_publish_attempts THEN available_at
                        ELSE NOW() + make_interval(secs => :backoff_seconds)
                    END,
                    last_error = :error, claim_token = NULL, claimed_by = NULL, claimed_at = NULL,
                    updated_at = NOW()
                WHERE id = :event_id AND status = 'publishing' AND claim_token = :claim_token
                RETURNING status
                """
                ),
                {
                    "event_id": event_id,
                    "claim_token": claim_token,
                    "error": error,
                    "backoff_seconds": backoff_seconds,
                },
            )
            .mappings()
            .one_or_none()
        )
        return OutboxStatus(row["status"]) if row is not None else None

    def fail_dead_dispatch(self, event_id: UUID, *, error_message: str, error_summary_json: str) -> bool:
        result = self.session.execute(
            text(
                """
                WITH dead_dispatch AS (
                    SELECT aggregate_id::uuid AS attempt_id
                    FROM outbox_events
                    WHERE id = :event_id
                      AND topic = 'job.dispatch.requested'
                      AND aggregate_type = 'job_attempt'
                      AND status = 'dead'
                ), failed_attempt AS (
                    UPDATE job_attempts AS attempt
                    SET status = 'failed', retryable = FALSE,
                        error_type = 'dispatch_failed', error_message = :error_message,
                        error_details_json = CAST(:error_summary_json AS jsonb),
                        finished_at = NOW(), updated_at = NOW()
                    FROM dead_dispatch
                    WHERE attempt.id = dead_dispatch.attempt_id
                      AND attempt.status = 'created'
                    RETURNING attempt.job_id
                )
                UPDATE job_runs AS job
                SET status = 'failed', error_summary_json = CAST(:error_summary_json AS jsonb),
                    finished_at = NOW(), updated_at = NOW()
                FROM failed_attempt
                WHERE job.id = failed_attempt.job_id AND job.status = 'queued'
                RETURNING job.id
                """
            ),
            {
                "event_id": event_id,
                "error_message": error_message,
                "error_summary_json": error_summary_json,
            },
        ).scalar_one_or_none()
        return result is not None

    def requeue_stale_published(
        self,
        *,
        limit: int,
        processed_timeout_seconds: float,
        topics: Sequence[str] | None = None,
    ) -> int:
        rows = self.session.execute(
            text(
                """
                WITH candidates AS (
                    SELECT id
                    FROM outbox_events
                    WHERE status = 'published' AND processed_at IS NULL
                      AND published_at < NOW() - make_interval(secs => :processed_timeout_seconds)
                      AND (
                        CAST(:topics AS text[]) IS NULL
                        OR topic = ANY(CAST(:topics AS text[]))
                      )
                    ORDER BY published_at, created_at
                    LIMIT :limit
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE outbox_events AS event
                SET status = 'pending', available_at = NOW(), updated_at = NOW()
                FROM candidates
                WHERE event.id = candidates.id
                RETURNING event.id
                """
            ),
            {
                "processed_timeout_seconds": processed_timeout_seconds,
                "limit": limit,
                "topics": list(topics) if topics is not None else None,
            },
        ).all()
        return len(rows)


def _projection(job: Mapping[Any, Any], attempt: Mapping[Any, Any]) -> JobProjection:
    return JobProjection(job=_job_record(job), current_attempt=_attempt_record(attempt))


def _job_record(row: Mapping[Any, Any]) -> JobRecord:
    logs = row.get("logs_json") or []
    return JobRecord(
        id=row["id"],
        task_key=row["task_key"],
        trigger_type=row["trigger_type"],
        queue_name=row["queue_name"],
        payload_version=int(row["payload_version"]),
        request_payload=dict(row["request_payload_json"] or {}),
        requested_by_user_id=row["requested_by_user_id"],
        parent_job_id=row["parent_job_id"],
        workflow_step=row["workflow_step"],
        status=JobStatus(row["status"]),
        progress=int(row["progress"]),
        progress_message=row["progress_message"],
        logs=tuple(dict(item) for item in logs),
        max_attempts=int(row["max_attempts"]),
        current_attempt=int(row["current_attempt"]),
        timeout_seconds=row["timeout_seconds"],
        scheduled_for=row["scheduled_for"],
        next_attempt_at=row["next_attempt_at"],
        cancel_requested_at=row["cancel_requested_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        result=dict(row["result_json"]) if row["result_json"] is not None else None,
        error_summary=dict(row["error_summary_json"]) if row["error_summary_json"] is not None else None,
        trace_id=row["trace_id"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _attempt_record(row: Mapping[Any, Any]) -> AttemptRecord:
    return AttemptRecord(
        id=row["id"],
        job_id=row["job_id"],
        attempt_no=int(row["attempt_no"]),
        status=AttemptStatus(row["status"]),
        celery_task_id=row["celery_task_id"],
        worker_id=row["worker_id"],
        claimed_at=row["claimed_at"],
        started_at=row["started_at"],
        last_heartbeat_at=row["last_heartbeat_at"],
        lease_expires_at=row["lease_expires_at"],
        finished_at=row["finished_at"],
        retryable=row["retryable"],
        error_type=row["error_type"],
        error_message=row["error_message"],
        error_details=dict(row["error_details_json"]) if row["error_details_json"] is not None else None,
        result=dict(row["result_json"]) if row["result_json"] is not None else None,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _outbox_record(row: Mapping[Any, Any]) -> OutboxEventRecord:
    return OutboxEventRecord(
        id=row["id"],
        event_key=row["event_key"],
        topic=row["topic"],
        aggregate_type=row["aggregate_type"],
        aggregate_id=row["aggregate_id"],
        payload_version=int(row["payload_version"]),
        payload=dict(row["payload_json"]),
        status=OutboxStatus(row["status"]),
        available_at=row["available_at"],
        claimed_by=row["claimed_by"],
        claim_token=row["claim_token"],
        claimed_at=row["claimed_at"],
        published_at=row["published_at"],
        processed_at=row["processed_at"],
        publish_attempts=int(row["publish_attempts"]),
        max_publish_attempts=int(row["max_publish_attempts"]),
        last_error=row["last_error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
