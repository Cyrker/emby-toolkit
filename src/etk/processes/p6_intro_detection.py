from __future__ import annotations

import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from etk.domains.p115.intro_detection import (
    BATCH_CONTINUATION_DELAY_SECONDS,
    INTRO_DETECTION_AGGREGATE_TYPE,
    INTRO_DETECTION_TOPIC,
    INTRO_DETECTION_TOPICS,
    IntroBatchDisposition,
    IntroBatchResult,
    IntroDetectionCancelled,
    IntroDetectionClaimLostError,
    IntroDetectionExecutionService,
    IntroDetectionOperation,
    IntroDetectionOperationStatus,
    IntroDetectionPhase,
    IntroDetectionStateError,
)
from etk.platform.jobs import OutboxClaim, PostgresJobStore, PublishBatchResult
from etk.platform.observability.redaction import redact_sensitive

from .celery_app import create_celery_app
from .runtime_config import TaskRuntimeConfig

EXECUTE_P115_INTRO_DETECTION_OPERATION_TASK = "etk.p115.intro_detection.execute"
P115_INTRO_DETECTION_CLAIM_LEASE_SECONDS = 1200
P115_INTRO_DETECTION_MAX_ATTEMPTS = 6
P115_INTRO_DETECTION_RETRY_SECONDS = 15


class IntroDetectionProcessRepository(Protocol):
    def read(self, operation_id: UUID) -> IntroDetectionOperation | None: ...

    def claim_by_id(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> IntroDetectionOperation | None: ...

    def heartbeat(self, operation_id: UUID, claim_token: UUID, *, lease_seconds: float) -> bool: ...

    def is_cancel_requested(self, operation_id: UUID, claim_token: UUID) -> bool: ...

    def schedule_continuation(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        phase: IntroDetectionPhase,
        progress: Mapping[str, Any],
        delay_seconds: float,
    ) -> Any: ...

    def mark_retryable(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        phase: IntroDetectionPhase,
        progress: Mapping[str, Any],
        error: Mapping[str, Any] | None,
        retry_after_seconds: float,
    ) -> Any: ...

    def mark_confirmed(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        phase: IntroDetectionPhase,
        progress: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> Any: ...

    def mark_rejected(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        phase: IntroDetectionPhase,
        progress: Mapping[str, Any],
        error: Mapping[str, Any],
    ) -> Any: ...

    def mark_cancelled(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        phase: IntroDetectionPhase,
        progress: Mapping[str, Any],
    ) -> Any: ...

    def expire_stale_claims(self, *, limit: int = 100) -> int: ...

    def reject_dead_dispatches(self, *, limit: int = 100) -> int: ...


class CeleryP115IntroDetectionPublisher:
    def __init__(self, config: TaskRuntimeConfig, *, celery_app: Any | None = None) -> None:
        self._config = config
        self._app = celery_app or create_celery_app(config)

    def publish(self, claim: OutboxClaim) -> None:
        event = claim.event
        if event.topic != INTRO_DETECTION_TOPIC or event.aggregate_type != INTRO_DETECTION_AGGREGATE_TYPE:
            raise ValueError("P115 intro publisher received an unsupported Outbox event")
        if event.payload_version != 1:
            raise ValueError("P115 intro Outbox payload version is unsupported")
        payload = event.payload
        operation_id = _uuid(payload.get("operation_id"), "operation_id")
        if event.aggregate_id != str(operation_id):
            raise ValueError("P115 intro Outbox aggregate does not match operation_id")
        if int(payload.get("sequence") or 0) <= 0:
            raise ValueError("P115 intro Outbox sequence is invalid")
        operation_key = _digest(payload.get("operation_key"), "operation_key")
        request_hash = _digest(payload.get("request_hash"), "request_hash")
        del operation_key, request_hash
        self._app.send_task(
            EXECUTE_P115_INTRO_DETECTION_OPERATION_TASK,
            kwargs={"operation_id": str(operation_id)},
            task_id=str(operation_id),
            queue=self._config.queue_name,
            routing_key=self._config.queue_name,
            serializer="json",
        )


@dataclass(frozen=True, slots=True)
class P115IntroDetectionCoordinatorResult:
    expired_claims: int
    rejected_dead: int
    publish: PublishBatchResult


class P115IntroDetectionCoordinator:
    def __init__(
        self,
        *,
        store: PostgresJobStore,
        repository: IntroDetectionProcessRepository,
        publisher: CeleryP115IntroDetectionPublisher,
        config: TaskRuntimeConfig,
        publisher_id: str,
    ) -> None:
        self._store = store
        self._repository = repository
        self._publisher = publisher
        self._config = config
        self._publisher_id = str(publisher_id or "").strip()
        if not self._publisher_id:
            raise ValueError("P115 intro publisher id must not be empty")

    def tick(self) -> P115IntroDetectionCoordinatorResult:
        limit = self._config.outbox_batch_size
        expired_claims = self._repository.expire_stale_claims(limit=limit)
        self._store.requeue_stale_published(
            limit=limit,
            processed_timeout_seconds=max(self._config.lease_seconds * 2, 60),
            topics=INTRO_DETECTION_TOPICS,
        )
        rejected_dead = self._repository.reject_dead_dispatches(limit=limit)
        publish = self._store.publish_outbox_batch(
            self._publisher,
            publisher_id=self._publisher_id,
            limit=limit,
            claim_timeout_seconds=self._config.outbox_claim_timeout_seconds,
            failure_backoff_seconds=2,
            topics=INTRO_DETECTION_TOPICS,
        )
        if publish.dead:
            rejected_dead += self._repository.reject_dead_dispatches(limit=limit)
        return P115IntroDetectionCoordinatorResult(expired_claims, rejected_dead, publish)


class P115IntroDetectionOperationExecutor:
    def __init__(
        self,
        *,
        repository: IntroDetectionProcessRepository,
        service: IntroDetectionExecutionService,
        worker_id: str,
        lease_seconds: float = P115_INTRO_DETECTION_CLAIM_LEASE_SECONDS,
    ) -> None:
        self._repository = repository
        self._service = service
        self._worker_id = str(worker_id or "").strip()
        self._lease_seconds = float(lease_seconds)
        if not self._worker_id or self._lease_seconds <= 0:
            raise ValueError("P115 intro Worker identity and lease must be valid")

    def execute(self, operation_id: UUID) -> IntroDetectionOperation | None:
        current = self._repository.read(operation_id)
        if current is None or current.status.terminal:
            return current
        claimed = self._repository.claim_by_id(
            operation_id,
            worker_id=self._worker_id,
            lease_seconds=self._lease_seconds,
        )
        if claimed is None:
            return self._repository.read(operation_id)
        try:
            token = _claim_token(claimed)
        except IntroDetectionClaimLostError:
            return self._repository.read(operation_id)
        cancellation = _ClaimCancellation(
            self._repository,
            operation_id,
            token,
        )
        heartbeat = _ClaimHeartbeat(
            self._repository,
            operation_id,
            token,
            lease_seconds=self._lease_seconds,
            cancellation=cancellation,
        )
        heartbeat.start()
        try:
            return self._execute_claimed(operation_id, claimed, token, cancellation)
        finally:
            heartbeat.stop()

    def _execute_claimed(
        self,
        operation_id: UUID,
        claimed: IntroDetectionOperation,
        token: UUID,
        cancellation: _ClaimCancellation,
    ) -> IntroDetectionOperation | None:
        try:
            result = self._service.execute(claimed, cancel=cancellation)
        except IntroDetectionCancelled:
            result = None
            cancellation.mark_cancelled()
        except IntroDetectionClaimLostError:
            return self._repository.read(operation_id)
        except IntroDetectionStateError as exc:
            return self._handle_execution_failure(
                operation_id,
                token,
                exc,
                error_type="intro_detection_state_error",
            )
        except Exception as exc:
            return self._handle_execution_failure(
                operation_id,
                token,
                exc,
                error_type="intro_detection_execution_failed",
            )

        try:
            return self._finish_result(operation_id, token, cancellation, result)
        except IntroDetectionClaimLostError:
            return self._repository.read(operation_id)

    def _handle_execution_failure(
        self,
        operation_id: UUID,
        token: UUID,
        exc: BaseException,
        *,
        error_type: str,
    ) -> IntroDetectionOperation | None:
        current = self._claimed_snapshot(operation_id, token)
        if current is None:
            return self._repository.read(operation_id)
        error = _safe_error(error_type, exc)
        try:
            if current.attempts < P115_INTRO_DETECTION_MAX_ATTEMPTS:
                return self._repository.mark_retryable(
                    operation_id,
                    token,
                    phase=current.phase,
                    progress=current.progress,
                    error=error,
                    retry_after_seconds=_retry_delay(current.attempts),
                ).operation
            return self._repository.mark_rejected(
                operation_id,
                token,
                phase=current.phase,
                progress=current.progress,
                error={**error, "attempts_exhausted": True},
            ).operation
        except IntroDetectionClaimLostError:
            return self._repository.read(operation_id)

    def _finish_result(
        self,
        operation_id: UUID,
        token: UUID,
        cancellation: _ClaimCancellation,
        result: IntroBatchResult | None,
    ) -> IntroDetectionOperation | None:
        latest = self._claimed_snapshot(operation_id, token)
        if latest is None:
            return self._repository.read(operation_id)
        if result is None or cancellation():
            return self._repository.mark_cancelled(
                operation_id,
                token,
                phase=latest.phase,
                progress=latest.progress,
            ).operation
        progress = {**dict(latest.progress), **dict(result.progress)}
        if result.disposition is IntroBatchDisposition.CONFIRMED:
            return self._repository.mark_confirmed(
                operation_id,
                token,
                phase=result.phase,
                progress=progress,
                result=dict(result.result or {}),
            ).operation
        if result.disposition is IntroBatchDisposition.REJECTED:
            return self._repository.mark_rejected(
                operation_id,
                token,
                phase=result.phase,
                progress=progress,
                error=dict(result.error or {"type": "intro_detection_rejected"}),
            ).operation
        if result.disposition is IntroBatchDisposition.CANCELLED:
            return self._repository.mark_cancelled(
                operation_id,
                token,
                phase=result.phase,
                progress=progress,
            ).operation
        if result.disposition is IntroBatchDisposition.RETRYABLE:
            return self._repository.mark_retryable(
                operation_id,
                token,
                phase=result.phase,
                progress=progress,
                error=dict(result.error or {}),
                retry_after_seconds=result.retry_after_seconds,
            ).operation
        return self._repository.schedule_continuation(
            operation_id,
            token,
            phase=result.phase,
            progress=progress,
            delay_seconds=BATCH_CONTINUATION_DELAY_SECONDS,
        ).operation

    def _claimed_snapshot(self, operation_id: UUID, token: UUID) -> IntroDetectionOperation | None:
        current = self._repository.read(operation_id)
        if (
            current is None
            or current.status is not IntroDetectionOperationStatus.CLAIMED
            or current.claim_token != token
        ):
            return None
        return current


class _ClaimCancellation:
    def __init__(
        self,
        repository: IntroDetectionProcessRepository,
        operation_id: UUID,
        claim_token: UUID,
        *,
        poll_interval_seconds: float = 0.5,
    ) -> None:
        self._repository = repository
        self._operation_id = operation_id
        self._claim_token = claim_token
        self._poll_interval_seconds = poll_interval_seconds
        self._cancelled = threading.Event()
        self._next_poll = 0.0
        self._lock = threading.Lock()

    def __call__(self) -> bool:
        if self._cancelled.is_set():
            return True
        now = time.monotonic()
        with self._lock:
            if now < self._next_poll:
                return self._cancelled.is_set()
            self._next_poll = now + self._poll_interval_seconds
        if self._repository.is_cancel_requested(self._operation_id, self._claim_token):
            self._cancelled.set()
        return self._cancelled.is_set()

    def mark_cancelled(self) -> None:
        self._cancelled.set()


class _ClaimHeartbeat:
    def __init__(
        self,
        repository: IntroDetectionProcessRepository,
        operation_id: UUID,
        claim_token: UUID,
        *,
        lease_seconds: float,
        cancellation: _ClaimCancellation,
    ) -> None:
        self._repository = repository
        self._operation_id = operation_id
        self._claim_token = claim_token
        self._lease_seconds = lease_seconds
        self._cancellation = cancellation
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=f"etk-intro-heartbeat-{operation_id}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=1)

    def _run(self) -> None:
        interval = min(max(self._lease_seconds / 3, 1), 30)
        while not self._stop.wait(interval):
            if not self._repository.heartbeat(
                self._operation_id,
                self._claim_token,
                lease_seconds=self._lease_seconds,
            ):
                self._cancellation.mark_cancelled()
                return


def _claim_token(operation: IntroDetectionOperation) -> UUID:
    if operation.claim_token is None:
        raise IntroDetectionClaimLostError("claimed intro detection operation has no token")
    return operation.claim_token


def _retry_delay(attempts: int) -> float:
    return min(P115_INTRO_DETECTION_RETRY_SECONDS * (2 ** max(0, attempts - 1)), 300)


def _safe_error(error_type: str, exc: BaseException) -> dict[str, Any]:
    message = redact_sensitive(str(exc))
    return {"type": error_type, "message": str(message)[:500]}


def _uuid(value: object, field_name: str) -> UUID:
    try:
        return UUID(str(value or ""))
    except ValueError as exc:
        raise ValueError(f"P115 intro Outbox {field_name} is invalid") from exc


def _digest(value: object, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64:
        raise ValueError(f"P115 intro Outbox {field_name} is invalid")
    try:
        bytes.fromhex(normalized)
    except ValueError as exc:
        raise ValueError(f"P115 intro Outbox {field_name} is invalid") from exc
    return normalized


__all__ = [
    "CeleryP115IntroDetectionPublisher",
    "EXECUTE_P115_INTRO_DETECTION_OPERATION_TASK",
    "P115IntroDetectionCoordinator",
    "P115IntroDetectionCoordinatorResult",
    "P115IntroDetectionOperationExecutor",
]
