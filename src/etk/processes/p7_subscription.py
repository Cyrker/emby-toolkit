from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from etk.domains.subscription.operation_repository import PostgresSubscriptionOperationRepository
from etk.domains.subscription.operations import (
    SUBSCRIPTION_OPERATION_AGGREGATE_TYPE,
    SUBSCRIPTION_OPERATION_TOPIC,
    SUBSCRIPTION_OPERATION_TOPICS,
)
from etk.domains.subscription.runtime_repository import PostgresSubscriptionRuntimeRepository
from etk.platform.jobs import OutboxClaim, PostgresJobStore, PublishBatchResult

from .celery_app import EXECUTE_SUBSCRIPTION_OPERATION_TASK, create_celery_app
from .runtime_config import TaskRuntimeConfig

SUBSCRIPTION_OPERATION_CLAIM_LEASE_SECONDS = 90


class CelerySubscriptionOperationPublisher:
    def __init__(self, config: TaskRuntimeConfig, *, celery_app: Any | None = None) -> None:
        self._config = config
        self._app = celery_app or create_celery_app(config)

    def publish(self, claim: OutboxClaim) -> None:
        event = claim.event
        if event.topic != SUBSCRIPTION_OPERATION_TOPIC or event.aggregate_type != SUBSCRIPTION_OPERATION_AGGREGATE_TYPE:
            raise ValueError("subscription operation publisher received an unsupported Outbox event")
        if event.payload_version != 1:
            raise ValueError("subscription operation Outbox payload version is unsupported")
        payload = event.payload
        operation_id = _uuid(payload.get("operation_id"), "operation_id")
        if event.aggregate_id != str(operation_id):
            raise ValueError("subscription operation Outbox aggregate does not match operation_id")
        if int(payload.get("sequence") or 0) <= 0:
            raise ValueError("subscription operation Outbox sequence is invalid")
        operation_key = str(payload.get("operation_key") or "").strip()
        request_hash = str(payload.get("request_hash") or "").strip()
        if not _sha256_hex(operation_key) or not _sha256_hex(request_hash):
            raise ValueError("subscription operation Outbox identity is invalid")
        self._app.send_task(
            EXECUTE_SUBSCRIPTION_OPERATION_TASK,
            kwargs={"operation_id": str(operation_id)},
            task_id=str(operation_id),
            queue=self._config.queue_name,
            routing_key=self._config.queue_name,
            serializer="json",
        )


@dataclass(frozen=True, slots=True)
class SubscriptionOperationCoordinatorResult:
    expired_claims: int
    expired_quota_reservations: int
    rejected_dead: int
    publish: PublishBatchResult


class SubscriptionOperationCoordinator:
    def __init__(
        self,
        *,
        store: PostgresJobStore,
        repository: PostgresSubscriptionOperationRepository,
        runtime_repository: PostgresSubscriptionRuntimeRepository,
        publisher: CelerySubscriptionOperationPublisher,
        config: TaskRuntimeConfig,
        publisher_id: str,
    ) -> None:
        self._store = store
        self._repository = repository
        self._runtime_repository = runtime_repository
        self._publisher = publisher
        self._config = config
        self._publisher_id = str(publisher_id or "").strip()
        if not self._publisher_id:
            raise ValueError("subscription operation publisher id must not be empty")

    def tick(self) -> SubscriptionOperationCoordinatorResult:
        limit = self._config.outbox_batch_size
        expired_claims = self._repository.expire_stale_claims(
            limit=limit,
            retry_delay_seconds=1,
        )
        expired_quota = self._runtime_repository.expire_quota_reservations(limit=limit)
        self._store.requeue_stale_published(
            limit=limit,
            processed_timeout_seconds=max(self._config.lease_seconds * 2, 60),
            topics=tuple(SUBSCRIPTION_OPERATION_TOPICS),
        )
        rejected_dead = self._repository.reject_dead_dispatches(limit=limit)
        publish = self._store.publish_outbox_batch(
            self._publisher,
            publisher_id=self._publisher_id,
            limit=limit,
            claim_timeout_seconds=self._config.outbox_claim_timeout_seconds,
            failure_backoff_seconds=2,
            topics=tuple(SUBSCRIPTION_OPERATION_TOPICS),
        )
        if publish.dead:
            rejected_dead += self._repository.reject_dead_dispatches(limit=limit)
        return SubscriptionOperationCoordinatorResult(
            expired_claims=expired_claims,
            expired_quota_reservations=expired_quota,
            rejected_dead=rejected_dead,
            publish=publish,
        )


def _uuid(value: Any, field: str) -> UUID:
    try:
        return UUID(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"subscription operation Outbox {field} is invalid") from exc


def _sha256_hex(value: str) -> bool:
    if len(value) != 64:
        return False
    try:
        bytes.fromhex(value)
    except ValueError:
        return False
    return True


__all__ = [
    "CelerySubscriptionOperationPublisher",
    "SUBSCRIPTION_OPERATION_CLAIM_LEASE_SECONDS",
    "SubscriptionOperationCoordinator",
    "SubscriptionOperationCoordinatorResult",
]
