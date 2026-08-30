from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from etk.domains.sharing.operations import (
    SHARING_OPERATION_AGGREGATE_TYPE,
    SHARING_OPERATION_TOPIC,
    SHARING_OPERATION_TOPICS,
)
from etk.platform.jobs import OutboxClaim, PostgresJobStore, PublishBatchResult

from .celery_app import EXECUTE_SHARING_OPERATION_TASK, create_celery_app
from .runtime_config import TaskRuntimeConfig

SHARING_OPERATION_CLAIM_LEASE_SECONDS = 90


class CelerySharingOperationPublisher:
    """Publish only validated identifier-bearing sharing Outbox events."""

    def __init__(self, config: TaskRuntimeConfig, *, celery_app: Any | None = None) -> None:
        self._config = config
        self._app = celery_app or create_celery_app(config)

    def publish(self, claim: OutboxClaim) -> None:
        event = claim.event
        if event.topic != SHARING_OPERATION_TOPIC or event.aggregate_type != SHARING_OPERATION_AGGREGATE_TYPE:
            raise ValueError("sharing operation publisher received an unsupported Outbox event")
        if event.payload_version != 1:
            raise ValueError("sharing operation Outbox payload version is unsupported")
        payload = event.payload
        operation_id = _uuid(payload.get("operation_id"), "operation_id")
        if event.aggregate_id != str(operation_id):
            raise ValueError("sharing operation Outbox aggregate does not match operation_id")
        if int(payload.get("sequence") or 0) <= 0:
            raise ValueError("sharing operation Outbox sequence is invalid")
        if not _sha256_hex(str(payload.get("operation_key") or "")):
            raise ValueError("sharing operation Outbox operation_key is invalid")
        if not _sha256_hex(str(payload.get("request_hash") or "")):
            raise ValueError("sharing operation Outbox request_hash is invalid")
        self._app.send_task(
            EXECUTE_SHARING_OPERATION_TASK,
            kwargs={"operation_id": str(operation_id)},
            task_id=str(operation_id),
            queue=self._config.queue_name,
            routing_key=self._config.queue_name,
            serializer="json",
        )


@dataclass(frozen=True, slots=True)
class SharingOperationCoordinatorResult:
    expired_claims: int
    expired_readbacks: int
    rejected_dead: int
    publish: PublishBatchResult


class SharingOperationCoordinator:
    def __init__(
        self,
        *,
        store: PostgresJobStore,
        repository: Any,
        publisher: CelerySharingOperationPublisher,
        config: TaskRuntimeConfig,
        publisher_id: str,
    ) -> None:
        self._store = store
        self._repository = repository
        self._publisher = publisher
        self._config = config
        self._publisher_id = str(publisher_id or "").strip()
        if not self._publisher_id:
            raise ValueError("sharing operation publisher id must not be empty")

    def tick(self) -> SharingOperationCoordinatorResult:
        limit = self._config.outbox_batch_size
        expired_claims = self._repository.expire_stale_claims(limit=limit)
        expired_readbacks = self._repository.requeue_stale_readback_claims(limit=limit)
        self._store.requeue_stale_published(
            limit=limit,
            processed_timeout_seconds=max(self._config.lease_seconds * 2, 60),
            topics=tuple(SHARING_OPERATION_TOPICS),
        )
        rejected_dead = self._repository.reject_dead_dispatches(limit=limit)
        publish = self._store.publish_outbox_batch(
            self._publisher,
            publisher_id=self._publisher_id,
            limit=limit,
            claim_timeout_seconds=self._config.outbox_claim_timeout_seconds,
            failure_backoff_seconds=2,
            topics=tuple(SHARING_OPERATION_TOPICS),
        )
        if publish.dead:
            rejected_dead += self._repository.reject_dead_dispatches(limit=limit)
        return SharingOperationCoordinatorResult(
            expired_claims=expired_claims,
            expired_readbacks=expired_readbacks,
            rejected_dead=rejected_dead,
            publish=publish,
        )


def _uuid(value: object, field: str) -> UUID:
    try:
        return UUID(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"sharing operation Outbox {field} is invalid") from exc


def _sha256_hex(value: str) -> bool:
    if len(value) != 64:
        return False
    try:
        bytes.fromhex(value)
    except ValueError:
        return False
    return True


__all__ = [
    "CelerySharingOperationPublisher",
    "SHARING_OPERATION_CLAIM_LEASE_SECONDS",
    "SharingOperationCoordinator",
    "SharingOperationCoordinatorResult",
]
