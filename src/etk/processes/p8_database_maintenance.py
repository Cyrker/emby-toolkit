from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from etk.domains.operations.database_maintenance import (
    DATABASE_MAINTENANCE_AGGREGATE_TYPE,
    DATABASE_MAINTENANCE_TOPIC,
    DATABASE_MAINTENANCE_TOPICS,
    PostgresDatabaseMaintenanceRepository,
)
from etk.platform.jobs import OutboxClaim, PostgresJobStore, PublishBatchResult

from .celery_app import EXECUTE_DATABASE_MAINTENANCE_TASK, create_celery_app
from .runtime_config import TaskRuntimeConfig

DATABASE_MAINTENANCE_CLAIM_LEASE_SECONDS = 300


class CeleryDatabaseMaintenancePublisher:
    def __init__(self, config: TaskRuntimeConfig, *, celery_app: Any | None = None) -> None:
        self._config = config
        self._app = celery_app or create_celery_app(config)

    def publish(self, claim: OutboxClaim) -> None:
        event = claim.event
        if event.topic != DATABASE_MAINTENANCE_TOPIC or event.aggregate_type != DATABASE_MAINTENANCE_AGGREGATE_TYPE:
            raise ValueError("database maintenance publisher received an unsupported Outbox event")
        if event.payload_version != 1:
            raise ValueError("database maintenance Outbox payload version is unsupported")
        payload = event.payload
        operation_id = _uuid(payload.get("operation_id"), "operation_id")
        artifact_value = payload.get("artifact_id")
        artifact_id = _uuid(artifact_value, "artifact_id") if artifact_value else None
        if event.aggregate_id != str(operation_id):
            raise ValueError("database maintenance Outbox aggregate does not match operation_id")
        if int(payload.get("sequence") or 0) <= 0:
            raise ValueError("database maintenance Outbox sequence is invalid")
        if not _sha256_hex(str(payload.get("operation_key") or "")):
            raise ValueError("database maintenance operation key is invalid")
        if not _sha256_hex(str(payload.get("request_hash") or "")):
            raise ValueError("database maintenance request hash is invalid")
        kwargs = {"operation_id": str(operation_id)}
        if artifact_id is not None:
            kwargs["artifact_id"] = str(artifact_id)
        self._app.send_task(
            EXECUTE_DATABASE_MAINTENANCE_TASK,
            kwargs=kwargs,
            task_id=str(operation_id),
            queue=self._config.queue_name,
            routing_key=self._config.queue_name,
            serializer="json",
        )


@dataclass(frozen=True, slots=True)
class DatabaseMaintenanceCoordinatorResult:
    expired_claims: int
    purged_artifacts: int
    rejected_dead: int
    publish: PublishBatchResult


class DatabaseMaintenanceCoordinator:
    def __init__(
        self,
        *,
        store: PostgresJobStore,
        repository: PostgresDatabaseMaintenanceRepository,
        publisher: CeleryDatabaseMaintenancePublisher,
        config: TaskRuntimeConfig,
        publisher_id: str,
    ) -> None:
        self._store = store
        self._repository = repository
        self._publisher = publisher
        self._config = config
        self._publisher_id = str(publisher_id or "").strip()
        if not self._publisher_id:
            raise ValueError("database maintenance publisher id must not be empty")

    def tick(self) -> DatabaseMaintenanceCoordinatorResult:
        limit = self._config.outbox_batch_size
        expired_claims = self._repository.expire_stale_claims(limit=limit, retry_delay_seconds=1)
        purged_artifacts = self._repository.purge_expired_artifacts(limit=limit)
        self._store.requeue_stale_published(
            limit=limit,
            processed_timeout_seconds=max(self._config.lease_seconds * 2, 60),
            topics=DATABASE_MAINTENANCE_TOPICS,
        )
        rejected_dead = self._repository.reject_dead_dispatches(limit=limit)
        publish = self._store.publish_outbox_batch(
            self._publisher,
            publisher_id=self._publisher_id,
            limit=limit,
            claim_timeout_seconds=self._config.outbox_claim_timeout_seconds,
            failure_backoff_seconds=2,
            topics=DATABASE_MAINTENANCE_TOPICS,
        )
        if publish.dead:
            rejected_dead += self._repository.reject_dead_dispatches(limit=limit)
        return DatabaseMaintenanceCoordinatorResult(expired_claims, purged_artifacts, rejected_dead, publish)


def _uuid(value: object, field: str) -> UUID:
    try:
        return UUID(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"database maintenance Outbox {field} is invalid") from exc


def _sha256_hex(value: str) -> bool:
    if len(value) != 64:
        return False
    try:
        bytes.fromhex(value)
    except ValueError:
        return False
    return True


__all__ = [
    "CeleryDatabaseMaintenancePublisher",
    "DATABASE_MAINTENANCE_CLAIM_LEASE_SECONDS",
    "DatabaseMaintenanceCoordinator",
    "DatabaseMaintenanceCoordinatorResult",
]
