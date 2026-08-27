from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from etk.domains.collection.operation_repository import PostgresCollectionOperationRepository
from etk.domains.collection.operations import (
    COLLECTION_OPERATION_AGGREGATE_TYPE,
    COLLECTION_OPERATION_TOPIC,
    COLLECTION_OPERATION_TOPICS,
)
from etk.platform.jobs import OutboxClaim, PostgresJobStore, PublishBatchResult

from .celery_app import EXECUTE_COLLECTION_OPERATION_TASK, create_celery_app
from .runtime_config import TaskRuntimeConfig

COLLECTION_OPERATION_CLAIM_LEASE_SECONDS = 90
COLLECTION_OPERATION_MAX_PROVIDER_CALLS_PER_STEP = 6


def collection_operation_claim_lease_seconds(emby_api_timeout: float) -> float:
    return max(
        float(emby_api_timeout) * COLLECTION_OPERATION_MAX_PROVIDER_CALLS_PER_STEP + 30,
        COLLECTION_OPERATION_CLAIM_LEASE_SECONDS,
    )


class CeleryCollectionOperationPublisher:
    def __init__(self, config: TaskRuntimeConfig, *, celery_app: Any | None = None) -> None:
        self._config = config
        self._app = celery_app or create_celery_app(config)

    def publish(self, claim: OutboxClaim) -> None:
        event = claim.event
        if event.topic != COLLECTION_OPERATION_TOPIC or event.aggregate_type != COLLECTION_OPERATION_AGGREGATE_TYPE:
            raise ValueError("collection operation publisher received an unsupported Outbox event")
        if event.payload_version != 1:
            raise ValueError("collection operation Outbox payload version is unsupported")
        payload = event.payload
        operation_id = _uuid(payload.get("operation_id"), "operation_id")
        if event.aggregate_id != str(operation_id):
            raise ValueError("collection operation Outbox aggregate does not match operation_id")
        if int(payload.get("sequence") or 0) <= 0:
            raise ValueError("collection operation Outbox sequence is invalid")
        if not _sha256_hex(str(payload.get("operation_key") or "")):
            raise ValueError("collection operation key is invalid")
        if not _sha256_hex(str(payload.get("request_hash") or "")):
            raise ValueError("collection operation request hash is invalid")
        self._app.send_task(
            EXECUTE_COLLECTION_OPERATION_TASK,
            kwargs={"operation_id": str(operation_id)},
            task_id=str(operation_id),
            queue=self._config.queue_name,
            routing_key=self._config.queue_name,
            serializer="json",
        )


@dataclass(frozen=True, slots=True)
class CollectionOperationCoordinatorResult:
    expired_claims: int
    rejected_dead: int
    publish: PublishBatchResult


class CollectionOperationCoordinator:
    def __init__(
        self,
        *,
        store: PostgresJobStore,
        repository: PostgresCollectionOperationRepository,
        publisher: CeleryCollectionOperationPublisher,
        config: TaskRuntimeConfig,
        publisher_id: str,
    ) -> None:
        self._store = store
        self._repository = repository
        self._publisher = publisher
        self._config = config
        self._publisher_id = str(publisher_id or "").strip()
        if not self._publisher_id:
            raise ValueError("collection operation publisher id must not be empty")

    def tick(self) -> CollectionOperationCoordinatorResult:
        limit = self._config.outbox_batch_size
        expired_claims = self._repository.expire_stale_claims(
            limit=limit,
            retry_delay_seconds=1,
        )
        self._store.requeue_stale_published(
            limit=limit,
            processed_timeout_seconds=max(self._config.lease_seconds * 2, 60),
            topics=COLLECTION_OPERATION_TOPICS,
        )
        rejected_dead = self._repository.reject_dead_dispatches(limit=limit)
        publish = self._store.publish_outbox_batch(
            self._publisher,
            publisher_id=self._publisher_id,
            limit=limit,
            claim_timeout_seconds=self._config.outbox_claim_timeout_seconds,
            failure_backoff_seconds=2,
            topics=COLLECTION_OPERATION_TOPICS,
        )
        if publish.dead:
            rejected_dead += self._repository.reject_dead_dispatches(limit=limit)
        return CollectionOperationCoordinatorResult(
            expired_claims=expired_claims,
            rejected_dead=rejected_dead,
            publish=publish,
        )


def _uuid(value: object, field: str) -> UUID:
    try:
        return UUID(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"collection operation Outbox {field} is invalid") from exc


def _sha256_hex(value: str) -> bool:
    if len(value) != 64:
        return False
    try:
        bytes.fromhex(value)
    except ValueError:
        return False
    return True


__all__ = [
    "COLLECTION_OPERATION_CLAIM_LEASE_SECONDS",
    "COLLECTION_OPERATION_MAX_PROVIDER_CALLS_PER_STEP",
    "CeleryCollectionOperationPublisher",
    "CollectionOperationCoordinator",
    "CollectionOperationCoordinatorResult",
    "collection_operation_claim_lease_seconds",
]
