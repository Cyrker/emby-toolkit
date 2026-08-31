from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.exc import IntegrityError

from etk.platform.db.retry import database_sqlstate
from etk.platform.observability.redaction import redact_sensitive

from .json_payload import canonical_json, canonical_json_hash, normalize_json_object
from .repository import SqlAlchemyJobRepository
from .types import (
    ActiveJobConflictError,
    AttemptClaimResult,
    AttemptNotFoundError,
    AttemptRecord,
    AttemptStatus,
    CreateJobResult,
    ExecutionGuard,
    ExecutionGuardUnavailableError,
    HeartbeatResult,
    IdempotencyConflictError,
    InvalidStateTransitionError,
    JobCancellationPolicy,
    JobNotFoundError,
    JobProjection,
    JobStatus,
    OutboxClaim,
    OutboxPublisher,
    OutboxStatus,
    PublishBatchResult,
    RecoveryResult,
    ResumeJobResult,
    UnitOfWorkFactory,
)

_SERIAL_CLAIM_LOCK_NAMESPACE = 0x45544B50
_SERIAL_CLAIM_LOCK_KEY = 5


class PostgresJobStore:
    """Synchronous PostgreSQL source of truth for P4 jobs and delivery state."""

    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        execution_guard: ExecutionGuard | None = None,
        cancellation_policy: JobCancellationPolicy | None = None,
    ):
        self._uow_factory = uow_factory
        self._execution_guard = execution_guard
        self._cancellation_policy = cancellation_policy

    def create_job(
        self,
        *,
        task_key: str,
        payload: Mapping[str, Any],
        queue_name: str = "legacy.serial",
        trigger_type: str = "manual",
        max_attempts: int = 1,
        timeout_seconds: int | None = None,
        scheduled_for: datetime | None = None,
        requested_by_user_id: str | None = None,
        parent_job_id: UUID | None = None,
        workflow_step: int | None = None,
        trace_id: str | None = None,
        idempotency_key: str | None = None,
        idempotency_scope: str | None = None,
        max_publish_attempts: int = 20,
        dispatch: bool = True,
    ) -> CreateJobResult:
        task_key = _nonempty(task_key, "task_key")
        queue_name = _nonempty(queue_name, "queue_name")
        trigger_type = _nonempty(trigger_type, "trigger_type")
        if trigger_type not in {"manual", "schedule", "workflow", "system", "monitor"}:
            raise ValueError("trigger_type is not supported")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if timeout_seconds is not None and timeout_seconds < 1:
            raise ValueError("timeout_seconds must be positive")
        if workflow_step is not None and workflow_step < 1:
            raise ValueError("workflow_step must be positive")
        if (parent_job_id is None) != (workflow_step is None):
            raise ValueError("parent_job_id and workflow_step must be provided together")
        if max_publish_attempts < 1:
            raise ValueError("max_publish_attempts must be at least 1")
        if not dispatch and parent_job_id is None:
            raise ValueError("only workflow children may disable broker dispatch")

        normalized_payload = normalize_json_object(payload, field="payload")
        request_envelope = {
            "task_key": task_key,
            "queue_name": queue_name,
            "trigger_type": trigger_type,
            "payload": normalized_payload,
            "max_attempts": max_attempts,
            "timeout_seconds": timeout_seconds,
            "scheduled_for": scheduled_for.isoformat() if scheduled_for is not None else None,
            "requested_by_user_id": requested_by_user_id,
            "parent_job_id": str(parent_job_id) if parent_job_id is not None else None,
            "workflow_step": workflow_step,
            "dispatch": dispatch,
        }
        request_hash = canonical_json_hash(
            _idempotency_request_envelope(task_key, request_envelope),
            field="job request",
        )
        scope: str | None = None
        key_hash: bytes | None = None
        if idempotency_key is not None:
            clean_key = _nonempty(idempotency_key, "idempotency_key")
            scope = _nonempty(idempotency_scope or f"job.create:{task_key}", "idempotency_scope")
            key_hash = hashlib.sha256(f"{scope}\0{clean_key}".encode()).digest()
            existing = self._read_idempotent(scope, key_hash, request_hash)
            if existing is not None:
                return existing
        elif idempotency_scope is not None:
            raise ValueError("idempotency_scope requires idempotency_key")

        job_id = uuid4()
        attempt_id = uuid4()
        event_id = uuid4()
        try:
            with self._uow_factory() as uow:
                repository = SqlAlchemyJobRepository(uow.session)
                repository.insert_job(
                    {
                        "id": job_id,
                        "task_key": task_key,
                        "trigger_type": trigger_type,
                        "queue_name": queue_name,
                        "payload_version": 1,
                        "request_payload_json": canonical_json(normalized_payload),
                        "requested_by_user_id": requested_by_user_id,
                        "parent_job_id": parent_job_id,
                        "workflow_step": workflow_step,
                        "max_attempts": max_attempts,
                        "timeout_seconds": timeout_seconds,
                        "scheduled_for": scheduled_for,
                        "trace_id": trace_id,
                    }
                )
                repository.insert_attempt(attempt_id=attempt_id, job_id=job_id, attempt_no=1)
                if dispatch:
                    repository.insert_dispatch_event(
                        event_id=event_id,
                        job_id=job_id,
                        attempt_id=attempt_id,
                        available_at=scheduled_for,
                        max_publish_attempts=max_publish_attempts,
                    )
                if scope is not None and key_hash is not None:
                    repository.insert_idempotency(
                        record_id=uuid4(),
                        scope=scope,
                        key_hash=key_hash,
                        request_hash=request_hash,
                        job_id=job_id,
                        attempt_id=attempt_id,
                        result_json=json.dumps({"job_id": str(job_id)}, separators=(",", ":")),
                    )
                projection = _require_projection(repository.projection(job_id), job_id)
                uow.commit()
                return CreateJobResult(projection=projection, reused=False)
        except IntegrityError as exc:
            active_job_conflict = _is_active_root_serial_unique_violation(exc)
            if (
                scope is not None
                and key_hash is not None
                and (active_job_conflict or _is_idempotency_unique_violation(exc))
            ):
                existing = self._read_idempotent(scope, key_hash, request_hash)
                if existing is not None:
                    return existing
            if active_job_conflict:
                raise ActiveJobConflictError("an active root Job already owns the serial queue") from exc
            raise

    def get_job(self, job_id: UUID) -> JobProjection | None:
        with self._uow_factory() as uow:
            return SqlAlchemyJobRepository(uow.session).projection(job_id)

    def resume_terminal_job(
        self,
        job_id: UUID,
        *,
        max_publish_attempts: int = 20,
    ) -> ResumeJobResult:
        """Append one explicit attempt to a failed terminal Job under a row lock."""
        _positive_integer(max_publish_attempts, "max_publish_attempts")
        try:
            with self._uow_factory() as uow:
                repository = SqlAlchemyJobRepository(uow.session)
                job = _require_job(repository.get_job_row(job_id, for_update=True), job_id)
                status = JobStatus(job["status"])
                if status not in {JobStatus.FAILED, JobStatus.TIMED_OUT, JobStatus.CANCELLED}:
                    return ResumeJobResult(
                        projection=_require_projection(repository.projection(job_id), job_id),
                        resumed=False,
                    )
                if str(job["task_key"]).startswith("native-operation:"):
                    raise InvalidStateTransitionError(
                        "native operation Jobs cannot be resumed after a terminal outcome"
                    )
                current_attempt = _require_attempt(
                    repository.get_current_attempt_row(
                        job_id,
                        int(job["current_attempt"]),
                        for_update=True,
                    ),
                    job_id,
                )
                if not AttemptStatus(current_attempt["status"]).terminal:
                    raise InvalidStateTransitionError("terminal Job has a non-terminal current attempt")

                attempt_no = int(job["current_attempt"]) + 1
                max_attempts = max(int(job["max_attempts"]), attempt_no)
                attempt_id = uuid4()
                repository.insert_attempt(
                    attempt_id=attempt_id,
                    job_id=job_id,
                    attempt_no=attempt_no,
                )
                repository.insert_dispatch_event(
                    event_id=uuid4(),
                    job_id=job_id,
                    attempt_id=attempt_id,
                    max_publish_attempts=max_publish_attempts,
                )
                if not repository.mark_terminal_job_resumed(
                    job_id,
                    attempt_no=attempt_no,
                    max_attempts=max_attempts,
                ):
                    raise InvalidStateTransitionError("Job is no longer resumable")
                projection = _require_projection(repository.projection(job_id), job_id)
                uow.commit()
                return ResumeJobResult(projection=projection, resumed=True)
        except IntegrityError as exc:
            if _is_active_root_serial_unique_violation(exc):
                raise ActiveJobConflictError("an active root Job already owns the serial queue") from exc
            raise

    def get_idempotent_job(self, *, scope: str, key: str) -> JobProjection | None:
        normalized_scope = _nonempty(scope, "scope")
        normalized_key = _nonempty(key, "key")
        key_hash = hashlib.sha256(f"{normalized_scope}\0{normalized_key}".encode()).digest()
        with self._uow_factory() as uow:
            repository = SqlAlchemyJobRepository(uow.session)
            existing = repository.get_idempotency(scope=normalized_scope, key_hash=key_hash)
            if existing is None:
                return None
            return _require_projection(repository.projection(existing["job_id"]), existing["job_id"])

    def get_current_job(self) -> JobProjection | None:
        with self._uow_factory() as uow:
            return SqlAlchemyJobRepository(uow.session).current_projection()

    def has_active_job(self) -> bool:
        with self._uow_factory() as uow:
            return SqlAlchemyJobRepository(uow.session).has_active_job()

    def list_children(self, parent_job_id: UUID) -> Sequence[JobProjection]:
        with self._uow_factory() as uow:
            return SqlAlchemyJobRepository(uow.session).list_children(parent_job_id)

    def request_cancel_current(self) -> JobProjection | None:
        current = self.get_current_job()
        if current is None or current.job.status.terminal:
            return current
        return self.request_cancel(current.job.id)

    def request_cancel(self, job_id: UUID) -> JobProjection:
        with self._uow_factory() as uow:
            repository = SqlAlchemyJobRepository(uow.session)
            job = _require_job(repository.get_job_row(job_id, for_update=True), job_id)
            status = JobStatus(job["status"])
            attempt = _require_attempt(
                repository.get_current_attempt_row(job_id, int(job["current_attempt"]), for_update=True),
                job_id,
            )
            if status.terminal:
                return _require_projection(repository.projection(job_id), job_id)
            if status in {JobStatus.QUEUED, JobStatus.RETRY_WAIT}:
                if self._cancellation_policy is not None and self._cancellation_policy.preserve_recovery_on_cancel(
                    uow,
                    job=job,
                    attempt=attempt,
                ):
                    projection = _require_projection(repository.projection(job_id), job_id)
                    uow.commit()
                    return projection
                if AttemptStatus(attempt["status"]) is AttemptStatus.CREATED:
                    repository.mark_attempt_terminal(
                        attempt["id"],
                        AttemptStatus.CANCELLED,
                        retryable=False,
                        error_type="cancelled",
                        error_message="cancelled before execution",
                        error_details_json=None,
                        result_json=None,
                    )
                    repository.cancel_dispatch(attempt["id"])
                repository.mark_job_terminal(job_id, JobStatus.CANCELLED)
            elif status is JobStatus.RUNNING:
                repository.mark_job_cancel_requested(job_id)
            projection = _require_projection(repository.projection(job_id), job_id)
            uow.commit()
            return projection

    def execution_guard(self, job_id: UUID) -> AbstractContextManager[bool]:
        if self._execution_guard is None:
            raise ExecutionGuardUnavailableError("no PostgreSQL execution guard was configured")
        return self._execution_guard.acquire(job_id)

    def claim_attempt(
        self,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        lease_seconds: float,
    ) -> AttemptClaimResult:
        worker_id = _nonempty(worker_id, "worker_id")
        _positive(lease_seconds, "lease_seconds")
        with self._uow_factory() as uow:
            repository = SqlAlchemyJobRepository(uow.session)
            job = _require_job(repository.get_job_row(job_id, for_update=True), job_id)
            attempt = _require_attempt(repository.get_attempt_row(attempt_id, for_update=True), attempt_id)
            if attempt["job_id"] != job_id:
                raise InvalidStateTransitionError("attempt does not belong to job")
            can_claim = (
                JobStatus(job["status"]) is JobStatus.QUEUED
                and AttemptStatus(attempt["status"]) is AttemptStatus.CREATED
                and int(job["current_attempt"]) == int(attempt["attempt_no"])
            )
            if not can_claim:
                return AttemptClaimResult(
                    claimed=False,
                    projection=_require_projection(repository.projection(job_id), job_id),
                )
            if job["parent_job_id"] is None:
                repository.acquire_serial_claim_lock(_SERIAL_CLAIM_LOCK_NAMESPACE, _SERIAL_CLAIM_LOCK_KEY)
                if repository.has_other_running_root(job_id):
                    repository.defer_dispatch(attempt_id, delay_seconds=1)
                    projection = _require_projection(repository.projection(job_id), job_id)
                    uow.commit()
                    return AttemptClaimResult(claimed=False, projection=projection)
            repository.mark_attempt_claimed(attempt_id, worker_id=worker_id, lease_seconds=lease_seconds)
            repository.mark_job_running(job_id)
            # Inline workflow execution and broker delivery share this path. Marking
            # the event processed here prevents a pending publisher from redelivering it.
            repository.mark_dispatch_processed(attempt_id)
            projection = _require_projection(repository.projection(job_id), job_id)
            uow.commit()
            return AttemptClaimResult(claimed=True, projection=projection)

    def start_attempt(
        self,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        lease_seconds: float,
    ) -> JobProjection:
        worker_id = _nonempty(worker_id, "worker_id")
        _positive(lease_seconds, "lease_seconds")
        with self._uow_factory() as uow:
            repository = SqlAlchemyJobRepository(uow.session)
            job, attempt = _lock_owned_attempt(repository, job_id, attempt_id, worker_id)
            if JobStatus(job["status"]) not in {JobStatus.RUNNING, JobStatus.CANCEL_REQUESTED}:
                raise InvalidStateTransitionError("job is not running")
            if AttemptStatus(attempt["status"]) is not AttemptStatus.CLAIMED:
                raise InvalidStateTransitionError("attempt must be claimed before it starts")
            repository.mark_attempt_running(attempt_id, worker_id=worker_id, lease_seconds=lease_seconds)
            projection = _require_projection(repository.projection(job_id), job_id)
            uow.commit()
            return projection

    def heartbeat(
        self,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        lease_seconds: float,
        progress: int | None = None,
        message: str | None = None,
        log_entry: Mapping[str, Any] | None = None,
        max_log_entries: int = 200,
    ) -> HeartbeatResult:
        worker_id = _nonempty(worker_id, "worker_id")
        _positive(lease_seconds, "lease_seconds")
        if progress is not None and not 0 <= progress <= 100:
            raise ValueError("progress must be between 0 and 100")
        if max_log_entries < 1:
            raise ValueError("max_log_entries must be positive")
        normalized_log = _normalized_log_entry(log_entry, field="log_entry") if log_entry is not None else None
        with self._uow_factory() as uow:
            repository = SqlAlchemyJobRepository(uow.session)
            job, attempt = _lock_owned_attempt(repository, job_id, attempt_id, worker_id)
            if AttemptStatus(attempt["status"]) not in {AttemptStatus.CLAIMED, AttemptStatus.RUNNING}:
                raise InvalidStateTransitionError("attempt does not accept heartbeat")
            repository.update_heartbeat(
                job_id=job_id,
                attempt_id=attempt_id,
                worker_id=worker_id,
                lease_seconds=lease_seconds,
                progress=progress,
                message=message,
            )
            if normalized_log is not None:
                repository.append_log(
                    job_id,
                    entry_json=canonical_json(normalized_log, field="log_entry"),
                    max_entries=max_log_entries,
                )
            projection = _require_projection(repository.projection(job_id), job_id)
            uow.commit()
            return HeartbeatResult(
                accepted=True,
                cancel_requested=JobStatus(job["status"]) is JobStatus.CANCEL_REQUESTED,
                projection=projection,
            )

    def append_log(self, job_id: UUID, entry: Mapping[str, Any], *, max_entries: int = 200) -> JobProjection:
        if max_entries < 1:
            raise ValueError("max_entries must be positive")
        normalized = _normalized_log_entry(entry, field="log entry")
        with self._uow_factory() as uow:
            repository = SqlAlchemyJobRepository(uow.session)
            if not repository.append_log(
                job_id,
                entry_json=canonical_json(normalized, field="log entry"),
                max_entries=max_entries,
            ):
                raise JobNotFoundError(f"job {job_id} was not found")
            projection = _require_projection(repository.projection(job_id), job_id)
            uow.commit()
            return projection

    def succeed_attempt(
        self,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        result: Mapping[str, Any] | None = None,
        honor_cancel_request: bool = True,
    ) -> JobProjection:
        return self._finish_attempt(
            job_id=job_id,
            attempt_id=attempt_id,
            worker_id=worker_id,
            desired_status=AttemptStatus.SUCCEEDED,
            result=result or {},
            retryable=False,
            honor_cancel_request=honor_cancel_request,
        )

    def fail_attempt(
        self,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        error_type: str,
        error_message: str,
        error_details: Mapping[str, Any] | None = None,
        retryable: bool,
        next_attempt_at: datetime | None = None,
        honor_cancel_request: bool = True,
    ) -> JobProjection:
        return self._finish_attempt(
            job_id=job_id,
            attempt_id=attempt_id,
            worker_id=worker_id,
            desired_status=AttemptStatus.FAILED,
            retryable=retryable,
            error_type=_nonempty(error_type, "error_type"),
            error_message=str(error_message),
            error_details=error_details or {},
            next_attempt_at=next_attempt_at,
            honor_cancel_request=honor_cancel_request,
        )

    def timeout_attempt(
        self,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        error_message: str = "job deadline exceeded",
        error_details: Mapping[str, Any] | None = None,
        retryable: bool,
        next_attempt_at: datetime | None = None,
        honor_cancel_request: bool = True,
    ) -> JobProjection:
        return self._finish_attempt(
            job_id=job_id,
            attempt_id=attempt_id,
            worker_id=worker_id,
            desired_status=AttemptStatus.TIMED_OUT,
            retryable=retryable,
            error_type="timeout",
            error_message=error_message,
            error_details=error_details or {},
            next_attempt_at=next_attempt_at,
            honor_cancel_request=honor_cancel_request,
        )

    def cancel_attempt(
        self,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        reason: str = "cancel requested",
    ) -> JobProjection:
        return self._finish_attempt(
            job_id=job_id,
            attempt_id=attempt_id,
            worker_id=worker_id,
            desired_status=AttemptStatus.CANCELLED,
            retryable=False,
            error_type="cancelled",
            error_message=reason,
            error_details={},
        )

    def enqueue_due_retries(self, *, limit: int = 100, max_publish_attempts: int = 20) -> tuple[AttemptRecord, ...]:
        _positive_integer(limit, "limit")
        _positive_integer(max_publish_attempts, "max_publish_attempts")
        created: list[AttemptRecord] = []
        with self._uow_factory() as uow:
            repository = SqlAlchemyJobRepository(uow.session)
            for job in repository.due_retry_rows(limit=limit):
                attempt_no = int(job["current_attempt"]) + 1
                if attempt_no > int(job["max_attempts"]):
                    repository.mark_job_terminal(
                        job["id"],
                        JobStatus.FAILED,
                        error_summary_json=canonical_json({"type": "retry_budget_exhausted"}),
                    )
                    continue
                attempt_id = uuid4()
                repository.insert_attempt(attempt_id=attempt_id, job_id=job["id"], attempt_no=attempt_no)
                repository.insert_dispatch_event(
                    event_id=uuid4(),
                    job_id=job["id"],
                    attempt_id=attempt_id,
                    max_publish_attempts=max_publish_attempts,
                )
                repository.mark_job_queued_for_retry(job["id"], attempt_no=attempt_no)
                attempt = repository.get_attempt_row(attempt_id)
                assert attempt is not None
                projection = repository.projection(job["id"])
                assert projection is not None
                created.append(projection.current_attempt)
            uow.commit()
        return tuple(created)

    def recover_stale_attempts(
        self,
        *,
        limit: int = 100,
        retry_delay_seconds: float = 0,
    ) -> RecoveryResult:
        _positive_integer(limit, "limit")
        if retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must not be negative")
        lost = requeued = failed = cancelled = 0
        with self._uow_factory() as uow:
            repository = SqlAlchemyJobRepository(uow.session)
            rows = repository.stale_attempt_rows(limit=limit)
            for row in rows:
                lost += 1
                job_was_cancelled = JobStatus(row["job_status"]) is JobStatus.CANCEL_REQUESTED
                native_operation, identity_error = _native_operation_recovery_identity(row)
                can_retry = (
                    not job_was_cancelled
                    and not native_operation
                    and identity_error is None
                    and int(row["attempt_no"]) < int(row["max_attempts"])
                )
                error_type = (
                    "NativeOperationIdentityMismatch"
                    if identity_error is not None
                    else ("PersistentEffectIndeterminate" if native_operation else "worker_lost")
                )
                error_message = (
                    identity_error
                    if identity_error is not None
                    else (
                        "native operation worker lease expired after execution started; "
                        "external effect outcome is unknown"
                        if native_operation
                        else "worker heartbeat lease expired"
                    )
                )
                repository.mark_attempt_terminal(
                    row["attempt_id"],
                    AttemptStatus.LOST,
                    retryable=can_retry,
                    error_type=error_type,
                    error_message=error_message,
                    error_details_json=canonical_json({"worker_id": row["worker_id"]}),
                    result_json=None,
                )
                if job_was_cancelled and not native_operation:
                    repository.mark_job_terminal(row["job_id"], JobStatus.CANCELLED)
                    cancelled += 1
                elif can_retry:
                    next_attempt_at = None
                    if retry_delay_seconds:
                        next_attempt_at = _database_now_plus(uow.session, retry_delay_seconds)
                    repository.mark_job_retry_wait(
                        row["job_id"],
                        next_attempt_at=next_attempt_at,
                        error_summary_json=canonical_json({"type": "worker_lost"}),
                    )
                    requeued += 1
                else:
                    repository.mark_job_terminal(
                        row["job_id"],
                        JobStatus.FAILED,
                        error_summary_json=canonical_json(
                            {
                                "type": error_type,
                                "message": error_message,
                                "retry_exhausted": True,
                            }
                        ),
                    )
                    failed += 1
            uow.commit()
        return RecoveryResult(
            lost_attempts=lost,
            requeued_jobs=requeued,
            failed_jobs=failed,
            cancelled_jobs=cancelled,
        )

    def claim_outbox(
        self,
        *,
        publisher_id: str,
        limit: int = 100,
        claim_timeout_seconds: float = 60,
        topics: Sequence[str] | None = None,
    ) -> tuple[OutboxClaim, ...]:
        publisher_id = _nonempty(publisher_id, "publisher_id")
        _positive_integer(limit, "limit")
        _positive(claim_timeout_seconds, "claim_timeout_seconds")
        normalized_topics = _normalized_topics(topics)
        claim_token = uuid4()
        with self._uow_factory() as uow:
            events = SqlAlchemyJobRepository(uow.session).claim_outbox(
                publisher_id=publisher_id,
                claim_token=claim_token,
                limit=limit,
                claim_timeout_seconds=claim_timeout_seconds,
                topics=normalized_topics,
            )
            claims = tuple(OutboxClaim(event=event, claim_token=claim_token) for event in events)
            uow.commit()
            return claims

    def mark_outbox_published(self, event_id: UUID, claim_token: UUID) -> bool:
        with self._uow_factory() as uow:
            updated = SqlAlchemyJobRepository(uow.session).mark_outbox_published(event_id, claim_token)
            uow.commit()
            return updated

    def mark_outbox_failed(
        self,
        event_id: UUID,
        claim_token: UUID,
        *,
        error: str,
        backoff_seconds: float,
    ) -> OutboxStatus | None:
        if backoff_seconds < 0:
            raise ValueError("backoff_seconds must not be negative")
        safe_error = str(redact_sensitive(str(error)))[:8000]
        with self._uow_factory() as uow:
            repository = SqlAlchemyJobRepository(uow.session)
            status = repository.mark_outbox_failed(
                event_id,
                claim_token,
                error=safe_error,
                backoff_seconds=backoff_seconds,
            )
            if status is OutboxStatus.DEAD:
                repository.fail_dead_dispatch(
                    event_id,
                    error_message=safe_error,
                    error_summary_json=canonical_json(
                        {"type": "dispatch_failed", "message": safe_error},
                        field="dispatch failure",
                    ),
                )
            uow.commit()
            return status

    def requeue_stale_published(
        self,
        *,
        limit: int = 100,
        processed_timeout_seconds: float = 300,
        topics: Sequence[str] | None = None,
    ) -> RecoveryResult:
        _positive_integer(limit, "limit")
        _positive(processed_timeout_seconds, "processed_timeout_seconds")
        normalized_topics = _normalized_topics(topics)
        with self._uow_factory() as uow:
            count = SqlAlchemyJobRepository(uow.session).requeue_stale_published(
                limit=limit,
                processed_timeout_seconds=processed_timeout_seconds,
                topics=normalized_topics,
            )
            uow.commit()
            return RecoveryResult(requeued_events=count)

    def publish_outbox_batch(
        self,
        publisher: OutboxPublisher,
        *,
        publisher_id: str,
        limit: int = 100,
        claim_timeout_seconds: float = 60,
        failure_backoff_seconds: float = 5,
        topics: Sequence[str] | None = None,
    ) -> PublishBatchResult:
        claims = self.claim_outbox(
            publisher_id=publisher_id,
            limit=limit,
            claim_timeout_seconds=claim_timeout_seconds,
            topics=topics,
        )
        published = failed = dead = 0
        # claim_outbox committed before this loop. No broker call occurs inside a DB UoW.
        for claim in claims:
            try:
                publisher.publish(claim)
            except Exception as exc:
                status = self.mark_outbox_failed(
                    claim.event.id,
                    claim.claim_token,
                    error=str(exc),
                    backoff_seconds=failure_backoff_seconds,
                )
                if status is OutboxStatus.DEAD:
                    dead += 1
                elif status is not None:
                    failed += 1
            else:
                if self.mark_outbox_published(claim.event.id, claim.claim_token):
                    published += 1
        return PublishBatchResult(claimed=len(claims), published=published, failed=failed, dead=dead)

    def _finish_attempt(
        self,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        desired_status: AttemptStatus,
        retryable: bool,
        result: Mapping[str, Any] | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
        error_details: Mapping[str, Any] | None = None,
        next_attempt_at: datetime | None = None,
        honor_cancel_request: bool = True,
    ) -> JobProjection:
        worker_id = _nonempty(worker_id, "worker_id")
        normalized_result = normalize_json_object(result, field="result") if result is not None else None
        normalized_error = (
            _normalized_log_entry(error_details, field="error_details") if error_details is not None else None
        )
        safe_error_message = str(redact_sensitive(error_message)) if error_message is not None else None
        with self._uow_factory() as uow:
            repository = SqlAlchemyJobRepository(uow.session)
            job, attempt = _lock_owned_attempt(repository, job_id, attempt_id, worker_id)
            attempt_status = AttemptStatus(attempt["status"])
            if attempt_status not in {AttemptStatus.CLAIMED, AttemptStatus.RUNNING}:
                raise InvalidStateTransitionError("attempt is not active")

            job_status = JobStatus(job["status"])
            actual_status = (
                AttemptStatus.CANCELLED
                if honor_cancel_request and job_status is JobStatus.CANCEL_REQUESTED
                else desired_status
            )
            actual_retryable = retryable and actual_status is not AttemptStatus.CANCELLED
            repository.mark_attempt_terminal(
                attempt_id,
                actual_status,
                retryable=actual_retryable,
                error_type="cancelled" if actual_status is AttemptStatus.CANCELLED else error_type,
                error_message="cancel requested" if actual_status is AttemptStatus.CANCELLED else safe_error_message,
                error_details_json=canonical_json(normalized_error) if normalized_error is not None else None,
                result_json=canonical_json(normalized_result) if normalized_result is not None else None,
            )

            if actual_status is AttemptStatus.CANCELLED:
                repository.mark_job_terminal(job_id, JobStatus.CANCELLED)
            elif actual_status is AttemptStatus.SUCCEEDED:
                repository.mark_job_terminal(
                    job_id,
                    JobStatus.SUCCEEDED,
                    result_json=canonical_json(normalized_result or {}),
                )
            elif actual_retryable and int(attempt["attempt_no"]) < int(job["max_attempts"]):
                repository.mark_job_retry_wait(
                    job_id,
                    next_attempt_at=next_attempt_at,
                    error_summary_json=canonical_json(
                        {"type": error_type or actual_status.value, "message": safe_error_message or ""}
                    ),
                )
            else:
                terminal_job_status = (
                    JobStatus.TIMED_OUT if actual_status is AttemptStatus.TIMED_OUT else JobStatus.FAILED
                )
                repository.mark_job_terminal(
                    job_id,
                    terminal_job_status,
                    error_summary_json=canonical_json(
                        {"type": error_type or actual_status.value, "message": safe_error_message or ""}
                    ),
                )
            projection = _require_projection(repository.projection(job_id), job_id)
            uow.commit()
            return projection

    def _read_idempotent(self, scope: str, key_hash: bytes, request_hash: bytes) -> CreateJobResult | None:
        with self._uow_factory() as uow:
            repository = SqlAlchemyJobRepository(uow.session)
            existing = repository.get_idempotency(scope=scope, key_hash=key_hash)
            if existing is None:
                return None
            stored_hash = bytes(existing["request_hash"])
            if stored_hash != request_hash:
                raise IdempotencyConflictError("idempotency key was already used with a different request")
            projection = _require_projection(repository.projection(existing["job_id"]), existing["job_id"])
            return CreateJobResult(projection=projection, reused=True)


def _lock_owned_attempt(
    repository: SqlAlchemyJobRepository,
    job_id: UUID,
    attempt_id: UUID,
    worker_id: str,
) -> tuple[Mapping[Any, Any], Mapping[Any, Any]]:
    job = _require_job(repository.get_job_row(job_id, for_update=True), job_id)
    attempt = _require_attempt(repository.get_attempt_row(attempt_id, for_update=True), attempt_id)
    if attempt["job_id"] != job_id:
        raise InvalidStateTransitionError("attempt does not belong to job")
    if attempt["worker_id"] != worker_id:
        raise InvalidStateTransitionError("attempt belongs to another worker")
    if int(job["current_attempt"]) != int(attempt["attempt_no"]):
        raise InvalidStateTransitionError("attempt is no longer current")
    return job, attempt


def _require_job(row: Mapping[Any, Any] | None, job_id: UUID) -> Mapping[Any, Any]:
    if row is None:
        raise JobNotFoundError(f"job {job_id} was not found")
    return row


def _require_attempt(row: Mapping[Any, Any] | None, attempt_id: object) -> Mapping[Any, Any]:
    if row is None:
        raise AttemptNotFoundError(f"attempt {attempt_id} was not found")
    return row


def _require_projection(projection: JobProjection | None, job_id: UUID) -> JobProjection:
    if projection is None:
        raise JobNotFoundError(f"job {job_id} has no current projection")
    return projection


def _nonempty(value: str, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return normalized


def _native_operation_recovery_identity(row: Mapping[Any, Any]) -> tuple[bool, str | None]:
    """Classify stale work without allowing an identity mismatch to retry."""

    task_key = str(row.get("task_key") or "").strip()
    payload_value: object = row.get("request_payload_json")
    if isinstance(payload_value, str):
        try:
            payload_value = json.loads(payload_value)
        except (TypeError, ValueError):
            payload_value = None
    payload = payload_value if isinstance(payload_value, Mapping) else {}
    native_value = payload.get("native_operation")
    native_key = str(native_value or "").strip().lower() if isinstance(native_value, str) else ""
    has_native_field = "native_operation" in payload
    has_native_task = task_key.startswith("native-operation:")
    native_indicated = has_native_field or has_native_task

    if not native_indicated:
        return False, None
    if has_native_field and (not isinstance(native_value, str) or not native_key):
        return True, "native operation identity is missing or invalid"
    if native_key and task_key != f"native-operation:{native_key}":
        return True, "native operation payload does not match task key"
    if has_native_task and task_key != f"native-operation:{native_key}":
        return True, "native operation task key does not match payload identity"
    return True, None


def _idempotency_request_envelope(task_key: str, request_envelope: Mapping[str, Any]) -> dict[str, Any]:
    """Exclude only execution-time fencing from quick-deploy replay identity."""

    if task_key != "native-operation:p115.quick-deploy":
        return dict(request_envelope)
    payload = request_envelope.get("payload")
    if not isinstance(payload, Mapping):
        return dict(request_envelope)
    parameters = payload.get("parameters")
    if not isinstance(parameters, Mapping) or "dynamic_config_revision" not in parameters:
        return dict(request_envelope)
    replay_payload = dict(payload)
    replay_parameters = dict(parameters)
    replay_parameters.pop("dynamic_config_revision", None)
    replay_payload["parameters"] = replay_parameters
    replay_envelope = dict(request_envelope)
    replay_envelope["payload"] = replay_payload
    return replay_envelope


def _positive(value: float, field: str) -> None:
    if value <= 0:
        raise ValueError(f"{field} must be positive")


def _positive_integer(value: int, field: str) -> None:
    if value < 1:
        raise ValueError(f"{field} must be positive")


def _normalized_topics(topics: Sequence[str] | None) -> tuple[str, ...] | None:
    if topics is None:
        return None
    if isinstance(topics, (str, bytes)):
        raise ValueError("topics must be a sequence of topic names")
    return tuple(dict.fromkeys(_nonempty(topic, "topic") for topic in topics))


def _is_idempotency_unique_violation(exc: IntegrityError) -> bool:
    return _unique_constraint_name(exc) == "uq_idempotency_records_scope_key_hash"


def _is_active_root_serial_unique_violation(exc: IntegrityError) -> bool:
    return _unique_constraint_name(exc) == "uq_job_runs_single_active_root_serial"


def _unique_constraint_name(exc: IntegrityError) -> str | None:
    if database_sqlstate(exc) != "23505":
        return None
    diagnostic = getattr(getattr(exc, "orig", None), "diag", None)
    value = getattr(diagnostic, "constraint_name", None)
    return str(value) if value is not None else None


def _database_now_plus(session: Any, seconds: float) -> datetime:
    from sqlalchemy import text

    value = session.scalar(text("SELECT NOW() + make_interval(secs => :seconds)"), {"seconds": seconds})
    assert isinstance(value, datetime)
    return value


def _normalized_log_entry(value: Mapping[str, Any], *, field: str) -> dict[str, Any]:
    normalized = normalize_json_object(value, field=field)
    redacted = redact_sensitive(normalized)
    assert isinstance(redacted, dict)
    return normalize_json_object(redacted, field=field)
