from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from etk.domains.p115.upload_monitor import (
    UPLOAD_MONITOR_TOPIC,
    UPLOAD_MONITOR_TOPICS,
    P115UploadMonitorConfigService,
    P115UploadProjection,
    P115UploadProvider,
    UploadedSource,
    UploadEventType,
    UploadOperation,
    UploadOperationStatus,
    UploadProjectionOutcome,
    UploadProviderDisposition,
    UploadProviderOutcome,
    observe_upload_file,
    uploaded_source_record_key,
)
from etk.platform.jobs import OutboxClaim, PostgresJobStore, PublishBatchResult

from .celery_app import EXECUTE_P115_UPLOAD_OPERATION_TASK, create_celery_app
from .runtime_config import TaskRuntimeConfig

P115_UPLOAD_CLAIM_LEASE_SECONDS = 3600
P115_UPLOAD_MAX_ATTEMPTS = 4
P115_UPLOAD_READBACK_ATTEMPTS = 3
P115_UPLOAD_RETRY_SECONDS = 15


class CeleryP115UploadPublisher:
    def __init__(self, config: TaskRuntimeConfig, *, celery_app: Any | None = None) -> None:
        self._config = config
        self._app = celery_app or create_celery_app(config)

    def publish(self, claim: OutboxClaim) -> None:
        event = claim.event
        if event.topic != UPLOAD_MONITOR_TOPIC or event.aggregate_type != "p115_upload_operation":
            raise ValueError("P115 upload publisher received an unsupported Outbox event")
        if event.payload_version != 1:
            raise ValueError("P115 upload Outbox payload version is unsupported")
        payload = event.payload
        operation_id = _uuid(payload.get("operation_id"), "operation_id")
        if event.aggregate_id != str(operation_id):
            raise ValueError("P115 upload Outbox aggregate does not match operation_id")
        if int(payload.get("sequence") or 0) <= 0:
            raise ValueError("P115 upload Outbox sequence is invalid")
        operation_key = str(payload.get("operation_key") or "").strip()
        request_hash = str(payload.get("request_hash") or "").strip()
        if len(operation_key) != 64 or len(request_hash) != 64:
            raise ValueError("P115 upload Outbox identity is invalid")
        try:
            bytes.fromhex(operation_key)
            bytes.fromhex(request_hash)
        except ValueError as exc:
            raise ValueError("P115 upload Outbox identity is invalid") from exc
        self._app.send_task(
            EXECUTE_P115_UPLOAD_OPERATION_TASK,
            kwargs={"operation_id": str(operation_id)},
            task_id=str(operation_id),
            queue=self._config.queue_name,
            routing_key=self._config.queue_name,
            serializer="json",
        )


@dataclass(frozen=True, slots=True)
class P115UploadCoordinatorResult:
    suspended: int
    resumed: int
    expired_claims: int
    sampled: int
    rejected_dead: int
    publish: PublishBatchResult


class P115UploadMonitorCoordinator:
    def __init__(
        self,
        *,
        store: PostgresJobStore,
        repository: Any,
        config_service: P115UploadMonitorConfigService,
        publisher: CeleryP115UploadPublisher,
        config: TaskRuntimeConfig,
        publisher_id: str,
    ) -> None:
        self._store = store
        self._repository = repository
        self._config_service = config_service
        self._publisher = publisher
        self._config = config
        self._publisher_id = str(publisher_id or "").strip()
        if not self._publisher_id:
            raise ValueError("P115 upload publisher id must not be empty")

    def tick(self) -> P115UploadCoordinatorResult:
        limit = self._config.outbox_batch_size
        _config, revision = self._config_service.snapshot()
        suspended, resumed = self._repository.synchronize_config(revision)
        expired = self._repository.expire_stale_claims(
            limit=limit,
            retry_delay_seconds=1,
        )
        sampled = 0
        for operation in self._repository.due_for_stability(limit=limit):
            observation = None
            try:
                observation = observe_upload_file(
                    operation.path,
                    str(operation.mapping["local_dir"]),
                )
            except (OSError, ValueError):
                pass
            self._repository.record_stability_sample(operation.id, observation)
            sampled += 1
        self._store.requeue_stale_published(
            limit=limit,
            processed_timeout_seconds=max(self._config.lease_seconds * 2, 60),
            topics=UPLOAD_MONITOR_TOPICS,
        )
        rejected_dead = self._repository.reject_dead_dispatches(limit=limit)
        publish = self._store.publish_outbox_batch(
            self._publisher,
            publisher_id=self._publisher_id,
            limit=limit,
            claim_timeout_seconds=self._config.outbox_claim_timeout_seconds,
            failure_backoff_seconds=2,
            topics=UPLOAD_MONITOR_TOPICS,
        )
        if publish.dead:
            rejected_dead += self._repository.reject_dead_dispatches(limit=limit)
        return P115UploadCoordinatorResult(
            suspended=suspended,
            resumed=resumed,
            expired_claims=expired,
            sampled=sampled,
            rejected_dead=rejected_dead,
            publish=publish,
        )


class P115UploadOperationExecutor:
    def __init__(
        self,
        *,
        repository: Any,
        provider: P115UploadProvider,
        projection: P115UploadProjection,
        worker_id: str,
        lease_seconds: float = P115_UPLOAD_CLAIM_LEASE_SECONDS,
    ) -> None:
        self._repository = repository
        self._provider = provider
        self._projection = projection
        self._worker_id = str(worker_id or "").strip()
        self._lease_seconds = float(lease_seconds)
        if not self._worker_id or self._lease_seconds <= 0:
            raise ValueError("P115 upload Worker identity and lease must be valid")

    def execute(self, operation_id: UUID) -> UploadOperation | None:
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
        if claimed.claim_token is None:
            raise RuntimeError("claimed P115 upload operation has no claim token")
        control = self._repository.control()
        if control is None or (
            claimed.status is UploadOperationStatus.CLAIMED
            and claimed.event_type is UploadEventType.UPSERT
            and claimed.config_revision != control.config_revision
        ):
            return self._repository.mark_superseded(
                claimed.id,
                claimed.claim_token,
                error={"type": "upload_config_revision_changed"},
            ).operation
        if claimed.event_type is UploadEventType.DELETED:
            return self._execute_delete(claimed)
        return self._execute_upload(claimed)

    def _execute_upload(self, claimed: UploadOperation) -> UploadOperation:
        token = _claim_token(claimed)
        operation = claimed
        if operation.remote_receipt is None:
            outcome = (
                self._provider.readback(operation)
                if operation.status is UploadOperationStatus.AWAITING_READBACK
                else self._provider.execute(operation, checkpoint=self._checkpoint(operation, token))
            )
            operation = self._handle_provider_outcome(operation, token, outcome)
            if operation.status is not UploadOperationStatus.REMOTE_CONFIRMED:
                return operation

        try:
            projection = self._projection_from_operation(operation)
            if projection is None:
                operation = self._repository.mark_projecting_local(operation.id, token).operation
                projection = self._projection.project(operation)
                projection_payload = {
                    "derived_paths": list(projection.derived_paths),
                    "ingestion_event_key": projection.ingestion_event_key,
                    "details": dict(projection.details),
                }
                operation = self._repository.mark_ingestion_pending(
                    operation.id,
                    token,
                    projection_payload,
                ).operation
            source_delete: Mapping[str, Any] = {}
            if str(operation.mapping.get("mode") or "") == "delete":
                source_delete = self._projection.delete_source(operation)
            receipt = dict(operation.remote_receipt or {})
            ownership = projection.details.get("derived_ownership")
            ownership = dict(ownership) if isinstance(ownership, Mapping) else {}
            completed = UploadedSource(
                record_key=uploaded_source_record_key(operation.mapping_id, operation.path_key),
                operation_id=operation.id,
                mapping_id=operation.mapping_id,
                config_revision=operation.config_revision,
                local_path=operation.path,
                path_key=operation.path_key,
                relative_path=operation.relative_path,
                target_cid=str(receipt.get("parent_id") or operation.mapping["target_cid"]),
                mode=str(operation.mapping.get("mode") or "keep"),
                fingerprint=_fingerprint(operation),
                remote_receipt=receipt,
                derived_paths=projection.derived_paths,
                derived_ownership=ownership,
                completed_at=datetime.now(UTC),
                operation_sequence=operation.sequence,
            )
            return self._repository.mark_confirmed(
                operation.id,
                token,
                result={
                    "remote_receipt": receipt,
                    "projection": {
                        "derived_paths": list(projection.derived_paths),
                        "ingestion_event_key": projection.ingestion_event_key,
                        "details": dict(projection.details),
                    },
                    "source_delete": dict(source_delete),
                },
                completed=completed,
            ).operation
        except Exception as exc:
            return self._retry_local_projection(operation, token, exc)

    def _execute_delete(self, claimed: UploadOperation) -> UploadOperation:
        token = _claim_token(claimed)
        source = _completed_source(claimed)
        operation = claimed
        if not bool((operation.remote_receipt or {}).get("deleted")):
            outcome = (
                self._provider.readback_delete(operation, source)
                if operation.status is UploadOperationStatus.AWAITING_READBACK
                else self._provider.delete(
                    operation,
                    source,
                    checkpoint=self._checkpoint(operation, token),
                )
            )
            operation = self._handle_provider_outcome(operation, token, outcome)
            if operation.status is not UploadOperationStatus.REMOTE_CONFIRMED:
                return operation
        try:
            current_source = self._repository.source_by_path(operation.mapping_id, operation.path_key)
            if not _same_uploaded_source(current_source, source):
                return self._repository.mark_confirmed(
                    operation.id,
                    token,
                    result={
                        "remote_receipt": dict(operation.remote_receipt or {}),
                        "local_projection": {
                            "skipped": True,
                            "reason": "completed_source_replaced_or_missing",
                        },
                        "source_record_removed": False,
                    },
                    completed=None,
                ).operation
            operation = self._repository.mark_projecting_local(operation.id, token).operation
            local = self._projection.delete_uploaded_source(operation, source)
            return self._repository.mark_confirmed(
                operation.id,
                token,
                result={
                    "remote_receipt": dict(operation.remote_receipt or {}),
                    "local_projection": dict(local),
                    "source_record_removed": True,
                },
                completed=None,
            ).operation
        except Exception as exc:
            return self._retry_local_projection(operation, token, exc)

    def _checkpoint(self, operation: UploadOperation, token: UUID):
        def persist(backend: str, session: Mapping[str, Any]) -> None:
            self._repository.checkpoint_request(
                operation.id,
                token,
                backend=backend,
                session=session,
                lease_seconds=self._lease_seconds,
            )

        return persist

    def _handle_provider_outcome(
        self,
        operation: UploadOperation,
        token: UUID,
        outcome: UploadProviderOutcome,
    ) -> UploadOperation:
        if (
            outcome.request_sent
            and outcome.session is not None
            and outcome.backend
            and operation.status in {UploadOperationStatus.CLAIMED, UploadOperationStatus.REQUESTING}
        ):
            self._repository.checkpoint_request(
                operation.id,
                token,
                backend=outcome.backend,
                session=outcome.session,
                lease_seconds=self._lease_seconds,
            )
        if outcome.disposition is UploadProviderDisposition.CONFIRMED:
            if outcome.receipt is None:
                raise RuntimeError("confirmed P115 upload outcome has no receipt")
            return self._repository.mark_remote_confirmed(operation.id, token, outcome.receipt).operation
        error = dict(outcome.error or {"type": "upload_provider_failure"})
        if outcome.disposition is UploadProviderDisposition.REJECTED:
            return self._repository.mark_rejected(operation.id, token, error=error).operation
        if not outcome.request_sent and outcome.disposition in {
            UploadProviderDisposition.RETRYABLE,
            UploadProviderDisposition.INDETERMINATE,
        }:
            if operation.attempts < P115_UPLOAD_MAX_ATTEMPTS:
                return self._repository.mark_retryable(
                    operation.id,
                    token,
                    error=error,
                    retry_after_seconds=P115_UPLOAD_RETRY_SECONDS,
                ).operation
            return self._repository.mark_rejected(
                operation.id,
                token,
                error={**error, "type": "upload_retry_budget_exhausted"},
            ).operation
        if operation.attempts < P115_UPLOAD_READBACK_ATTEMPTS:
            return self._repository.mark_awaiting_readback(
                operation.id,
                token,
                session=outcome.session,
                error=error,
                retry_after_seconds=P115_UPLOAD_RETRY_SECONDS,
            ).operation
        return self._repository.mark_indeterminate(operation.id, token, error=error).operation

    def _retry_local_projection(
        self,
        operation: UploadOperation,
        token: UUID,
        exc: Exception,
    ) -> UploadOperation:
        error = {"type": "upload_local_projection_failed", "message": str(exc)[:500]}
        if operation.attempts < P115_UPLOAD_MAX_ATTEMPTS:
            return self._repository.mark_retryable(
                operation.id,
                token,
                error=error,
                retry_after_seconds=P115_UPLOAD_RETRY_SECONDS,
            ).operation
        return self._repository.mark_indeterminate(operation.id, token, error=error).operation

    @staticmethod
    def _projection_from_operation(operation: UploadOperation) -> UploadProjectionOutcome | None:
        value = operation.projection
        if not isinstance(value, Mapping):
            return None
        paths = value.get("derived_paths")
        details = value.get("details")
        if not isinstance(paths, list) or not isinstance(details, Mapping):
            return None
        return UploadProjectionOutcome(
            derived_paths=tuple(str(path) for path in paths),
            ingestion_event_key=str(value.get("ingestion_event_key") or "") or None,
            details=dict(details),
        )


def _claim_token(operation: UploadOperation) -> UUID:
    if operation.claim_token is None:
        raise RuntimeError("P115 upload operation claim token is missing")
    return operation.claim_token


def _fingerprint(operation: UploadOperation):
    if operation.fingerprint is None:
        raise RuntimeError("P115 upload operation has no source fingerprint")
    return operation.fingerprint


def _completed_source(operation: UploadOperation) -> UploadedSource:
    if operation.completed_source is None:
        raise RuntimeError("delete P115 upload operation has no completed source snapshot")
    source = UploadedSource.from_snapshot(operation.completed_source)
    if source.mapping_id != operation.mapping_id or source.path_key != operation.path_key:
        raise RuntimeError("delete P115 upload operation completed source identity changed")
    return source


def _same_uploaded_source(current: UploadedSource | None, expected: UploadedSource) -> bool:
    return bool(
        current is not None
        and current.record_key == expected.record_key
        and current.operation_id == expected.operation_id
        and current.payload() == expected.payload()
    )


def _uuid(value: object, field: str) -> UUID:
    try:
        return UUID(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a UUID") from exc


__all__ = [
    "CeleryP115UploadPublisher",
    "P115UploadMonitorCoordinator",
    "P115UploadOperationExecutor",
]
