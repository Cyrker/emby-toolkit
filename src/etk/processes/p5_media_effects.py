from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from etk.domains.media.effects import MEDIA_EFFECT_TOPIC, PostgresMediaEffectLedger
from etk.platform.jobs import OutboxClaim, PostgresJobStore, PublishBatchResult

from .celery_app import EXECUTE_MEDIA_EFFECT_TASK, create_celery_app
from .runtime_config import TaskRuntimeConfig

MEDIA_EFFECT_TOPICS = (MEDIA_EFFECT_TOPIC,)


class CeleryMediaEffectPublisher:
    def __init__(self, config: TaskRuntimeConfig, *, celery_app: Any | None = None) -> None:
        self._config = config
        self._app = celery_app or create_celery_app(config)

    def publish(self, claim: OutboxClaim) -> None:
        event = claim.event
        if event.topic != MEDIA_EFFECT_TOPIC or event.aggregate_type != "media_effect":
            raise ValueError("Media Effect publisher only supports media.effect.requested events")
        if event.payload_version != 1:
            raise ValueError("media effect outbox payload version is unsupported")
        payload = event.payload
        effect_id = _uuid(payload.get("effect_id"), "effect_id")
        if event.aggregate_id != str(effect_id):
            raise ValueError("media effect outbox aggregate does not match effect_id")
        if not str(payload.get("effect_key") or "").strip():
            raise ValueError("media effect outbox effect_key is missing")
        request_hash = str(payload.get("request_hash") or "").strip()
        if len(request_hash) != 64:
            raise ValueError("media effect outbox request_hash is invalid")
        try:
            bytes.fromhex(request_hash)
        except ValueError as exc:
            raise ValueError("media effect outbox request_hash is invalid") from exc
        self._app.send_task(
            EXECUTE_MEDIA_EFFECT_TASK,
            kwargs={"effect_id": str(effect_id)},
            task_id=str(effect_id),
            queue=self._config.queue_name,
            routing_key=self._config.queue_name,
            serializer="json",
        )


@dataclass(frozen=True, slots=True)
class MediaEffectDispatchResult:
    requeued_replay_safe_claims: int
    expired_claims: int
    requeued_events: int
    rejected_dead_effects: int
    publish: PublishBatchResult


class MediaEffectOutboxDispatcher:
    def __init__(
        self,
        store: PostgresJobStore,
        ledger: PostgresMediaEffectLedger,
        publisher: CeleryMediaEffectPublisher,
        config: TaskRuntimeConfig,
        *,
        publisher_id: str,
        replay_safe_effect_types: Sequence[str] = (),
    ) -> None:
        self._store = store
        self._ledger = ledger
        self._publisher = publisher
        self._config = config
        self._publisher_id = str(publisher_id or "").strip()
        self._replay_safe_effect_types = tuple(
            dict.fromkeys(str(value or "").strip() for value in replay_safe_effect_types)
        )
        if not self._publisher_id:
            raise ValueError("media effect publisher id must not be empty")
        if any(not value for value in self._replay_safe_effect_types):
            raise ValueError("replay-safe media effect type must not be empty")

    def tick(self) -> MediaEffectDispatchResult:
        limit = self._config.outbox_batch_size
        requeued_replay_safe = (
            self._ledger.requeue_replay_safe_stale_claims(
                effect_types=self._replay_safe_effect_types,
                limit=limit,
            )
            if self._replay_safe_effect_types
            else 0
        )
        expired = self._ledger.expire_stale_claims(limit=limit)
        requeued = self._ledger.requeue_stale_published(
            limit=limit,
            processed_timeout_seconds=max(self._config.lease_seconds * 2, 60),
        )
        rejected = self._ledger.reject_dead_dispatches(limit=limit)
        publish = self._store.publish_outbox_batch(
            self._publisher,
            publisher_id=self._publisher_id,
            limit=limit,
            claim_timeout_seconds=self._config.outbox_claim_timeout_seconds,
            failure_backoff_seconds=2,
            topics=MEDIA_EFFECT_TOPICS,
        )
        if publish.dead:
            rejected += self._ledger.reject_dead_dispatches(limit=limit)
        return MediaEffectDispatchResult(
            requeued_replay_safe_claims=requeued_replay_safe,
            expired_claims=expired,
            requeued_events=requeued,
            rejected_dead_effects=rejected,
            publish=publish,
        )


def _uuid(value: object, field: str) -> UUID:
    try:
        return UUID(str(value or ""))
    except ValueError as exc:
        raise ValueError(f"media effect outbox {field} is invalid") from exc


__all__ = [
    "CeleryMediaEffectPublisher",
    "MEDIA_EFFECT_TOPICS",
    "MediaEffectDispatchResult",
    "MediaEffectOutboxDispatcher",
]
