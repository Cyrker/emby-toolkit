from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Literal, Protocol
from uuid import UUID

from sqlalchemy.orm import Session


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    CANCEL_REQUESTED = "cancel_requested"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in TERMINAL_JOB_STATUSES


TERMINAL_JOB_STATUSES = frozenset(
    {
        JobStatus.SUCCEEDED,
        JobStatus.FAILED,
        JobStatus.TIMED_OUT,
        JobStatus.CANCELLED,
    }
)
ACTIVE_JOB_STATUSES = frozenset(
    {
        JobStatus.QUEUED,
        JobStatus.RUNNING,
        JobStatus.RETRY_WAIT,
        JobStatus.CANCEL_REQUESTED,
    }
)


class AttemptStatus(StrEnum):
    CREATED = "created"
    CLAIMED = "claimed"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    LOST = "lost"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in TERMINAL_ATTEMPT_STATUSES


TERMINAL_ATTEMPT_STATUSES = frozenset(
    {
        AttemptStatus.SUCCEEDED,
        AttemptStatus.FAILED,
        AttemptStatus.TIMED_OUT,
        AttemptStatus.LOST,
        AttemptStatus.CANCELLED,
    }
)


class OutboxStatus(StrEnum):
    PENDING = "pending"
    PUBLISHING = "publishing"
    PUBLISHED = "published"
    PROCESSED = "processed"
    CANCELLED = "cancelled"
    DEAD = "dead"


class IdempotencyStatus(StrEnum):
    RESERVED = "reserved"
    COMPLETED = "completed"
    FAILED = "failed"
    INDETERMINATE = "indeterminate"


class JobStoreError(RuntimeError):
    pass


class ActiveJobConflictError(JobStoreError):
    pass


class JobNotFoundError(JobStoreError):
    pass


class AttemptNotFoundError(JobStoreError):
    pass


class InvalidStateTransitionError(JobStoreError):
    pass


class IdempotencyConflictError(JobStoreError):
    pass


class InvalidJsonPayloadError(ValueError, JobStoreError):
    pass


class ExecutionGuardUnavailableError(JobStoreError):
    pass


JsonObject = dict[str, Any]


@dataclass(frozen=True, slots=True)
class JobRecord:
    id: UUID
    task_key: str
    trigger_type: str
    queue_name: str
    payload_version: int
    request_payload: JsonObject
    requested_by_user_id: str | None
    parent_job_id: UUID | None
    workflow_step: int | None
    status: JobStatus
    progress: int
    progress_message: str
    logs: tuple[JsonObject, ...]
    max_attempts: int
    current_attempt: int
    timeout_seconds: int | None
    scheduled_for: datetime
    next_attempt_at: datetime | None
    cancel_requested_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    result: JsonObject | None
    error_summary: JsonObject | None
    trace_id: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    id: UUID
    job_id: UUID
    attempt_no: int
    status: AttemptStatus
    celery_task_id: str
    worker_id: str | None
    claimed_at: datetime | None
    started_at: datetime | None
    last_heartbeat_at: datetime | None
    lease_expires_at: datetime | None
    finished_at: datetime | None
    retryable: bool | None
    error_type: str | None
    error_message: str | None
    error_details: JsonObject | None
    result: JsonObject | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class OutboxEventRecord:
    id: UUID
    event_key: str
    topic: str
    aggregate_type: str
    aggregate_id: str
    payload_version: int
    payload: JsonObject
    status: OutboxStatus
    available_at: datetime
    claimed_by: str | None
    claim_token: UUID | None
    claimed_at: datetime | None
    published_at: datetime | None
    processed_at: datetime | None
    publish_attempts: int
    max_publish_attempts: int
    last_error: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class JobProjection:
    job: JobRecord
    current_attempt: AttemptRecord

    @property
    def logs(self) -> tuple[JsonObject, ...]:
        return self.job.logs


@dataclass(frozen=True, slots=True)
class CreateJobResult:
    projection: JobProjection
    reused: bool

    @property
    def job(self) -> JobRecord:
        return self.projection.job

    @property
    def attempt(self) -> AttemptRecord:
        return self.projection.current_attempt


@dataclass(frozen=True, slots=True)
class ResumeJobResult:
    projection: JobProjection
    resumed: bool


@dataclass(frozen=True, slots=True)
class AttemptClaimResult:
    claimed: bool
    projection: JobProjection


@dataclass(frozen=True, slots=True)
class HeartbeatResult:
    accepted: bool
    cancel_requested: bool
    projection: JobProjection


@dataclass(frozen=True, slots=True)
class OutboxClaim:
    event: OutboxEventRecord
    claim_token: UUID


@dataclass(frozen=True, slots=True)
class PublishBatchResult:
    claimed: int
    published: int
    failed: int
    dead: int


@dataclass(frozen=True, slots=True)
class RecoveryResult:
    lost_attempts: int = 0
    requeued_jobs: int = 0
    failed_jobs: int = 0
    cancelled_jobs: int = 0
    requeued_events: int = 0


class UnitOfWork(Protocol):
    @property
    def session(self) -> Session: ...

    def __enter__(self) -> UnitOfWork: ...

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> Literal[False]: ...

    def commit(self) -> None: ...


type UnitOfWorkFactory = Callable[[], UnitOfWork]


class JobCancellationPolicy(Protocol):
    def preserve_recovery_on_cancel(
        self,
        uow: UnitOfWork,
        *,
        job: Mapping[str, Any],
        attempt: Mapping[str, Any],
    ) -> bool: ...


class ExecutionGuard(Protocol):
    def acquire(self, job_id: UUID) -> AbstractContextManager[bool]: ...


class OutboxPublisher(Protocol):
    def publish(self, claim: OutboxClaim) -> None: ...


class JobStoreProtocol(Protocol):
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
    ) -> CreateJobResult: ...

    def get_job(self, job_id: UUID) -> JobProjection | None: ...

    def get_idempotent_job(self, *, scope: str, key: str) -> JobProjection | None: ...

    def get_current_job(self) -> JobProjection | None: ...

    def has_active_job(self) -> bool: ...

    def list_children(self, parent_job_id: UUID) -> Sequence[JobProjection]: ...

    def request_cancel(self, job_id: UUID) -> JobProjection: ...

    def request_cancel_current(self) -> JobProjection | None: ...

    def execution_guard(self, job_id: UUID) -> AbstractContextManager[bool]: ...

    def claim_attempt(
        self,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        lease_seconds: float,
    ) -> AttemptClaimResult: ...

    def start_attempt(
        self,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        lease_seconds: float,
    ) -> JobProjection: ...

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
    ) -> HeartbeatResult: ...

    def append_log(
        self,
        job_id: UUID,
        entry: Mapping[str, Any],
        *,
        max_entries: int = 200,
    ) -> JobProjection: ...

    def succeed_attempt(
        self,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        result: Mapping[str, Any] | None = None,
        honor_cancel_request: bool = True,
    ) -> JobProjection: ...

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
    ) -> JobProjection: ...

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
    ) -> JobProjection: ...

    def cancel_attempt(
        self,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        reason: str = "cancel requested",
    ) -> JobProjection: ...

    def enqueue_due_retries(
        self,
        *,
        limit: int = 100,
        max_publish_attempts: int = 20,
    ) -> tuple[AttemptRecord, ...]: ...

    def recover_stale_attempts(
        self,
        *,
        limit: int = 100,
        retry_delay_seconds: float = 0,
    ) -> RecoveryResult: ...

    def claim_outbox(
        self,
        *,
        publisher_id: str,
        limit: int = 100,
        claim_timeout_seconds: float = 60,
        topics: Sequence[str] | None = None,
    ) -> tuple[OutboxClaim, ...]: ...

    def mark_outbox_published(self, event_id: UUID, claim_token: UUID) -> bool: ...

    def mark_outbox_failed(
        self,
        event_id: UUID,
        claim_token: UUID,
        *,
        error: str,
        backoff_seconds: float,
    ) -> OutboxStatus | None: ...

    def requeue_stale_published(
        self,
        *,
        limit: int = 100,
        processed_timeout_seconds: float = 300,
        topics: Sequence[str] | None = None,
    ) -> RecoveryResult: ...

    def publish_outbox_batch(
        self,
        publisher: OutboxPublisher,
        *,
        publisher_id: str,
        limit: int = 100,
        claim_timeout_seconds: float = 60,
        failure_backoff_seconds: float = 5,
        topics: Sequence[str] | None = None,
    ) -> PublishBatchResult: ...
