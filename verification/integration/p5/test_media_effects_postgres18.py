from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etk.domains.media.effects import (
    MEDIA_EFFECT_TOPIC,
    MediaEffectConflictError,
    MediaEffectStateError,
    MediaEffectStatus,
    PostgresMediaEffectLedger,
    media_effect_event_key,
    media_effect_request_hash,
)
from etk.platform.db import DatabaseRuntime

EFFECT_TYPE = "emby.metadata.write"
AGGREGATE_TYPE = "media"
AGGREGATE_ID = "Movie:42"


def _ledger(runtime: DatabaseRuntime) -> PostgresMediaEffectLedger:
    return PostgresMediaEffectLedger(runtime.unit_of_work)


def _clear_effect_rows(runtime: DatabaseRuntime) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM outbox_events WHERE topic = :topic"),
            {"topic": MEDIA_EFFECT_TOPIC},
        )
        connection.execute(text("DELETE FROM media_effects"))


@pytest.fixture(autouse=True)
def clean_media_effect_rows(p5_runtime: DatabaseRuntime) -> Iterator[None]:
    _clear_effect_rows(p5_runtime)
    try:
        yield
    finally:
        _clear_effect_rows(p5_runtime)


def test_pg18_media_effect_migration_head_schema_and_constraints(p5_runtime: DatabaseRuntime) -> None:
    with p5_runtime.engine.connect() as connection:
        server_version = str(connection.scalar(text("SHOW server_version_num")))
        migration_head = connection.scalar(text("SELECT version_num FROM etk_migrations.alembic_version"))
        columns = set(
            connection.scalars(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = 'media_effects'
                    """
                )
            )
        )
        constraint_rows = (
            connection.execute(
                text(
                    """
                    SELECT conname, pg_get_constraintdef(oid) AS definition
                    FROM pg_constraint
                    WHERE conrelid = 'public.media_effects'::regclass
                    """
                )
            )
            .mappings()
            .all()
        )
        constraints = {str(row["conname"]): str(row["definition"]) for row in constraint_rows}
        indexes = set(
            connection.scalars(
                text(
                    """
                    SELECT indexname
                    FROM pg_indexes
                    WHERE schemaname = 'public' AND tablename = 'media_effects'
                    """
                )
            )
        )

    assert server_version.startswith("18")
    assert migration_head == "0003_p5_media_effects"
    assert columns == {
        "id",
        "effect_key",
        "effect_type",
        "aggregate_type",
        "aggregate_id",
        "payload_version",
        "payload_json",
        "request_hash",
        "status",
        "attempts",
        "available_at",
        "claimed_by",
        "claim_token",
        "claimed_at",
        "lease_expires_at",
        "last_attempt_at",
        "result_json",
        "error_json",
        "finished_at",
        "created_at",
        "updated_at",
    }
    assert {
        "media_effects_pkey",
        "uq_media_effects_effect_key",
        "ck_media_effects_status",
        "ck_media_effects_claim",
        "ck_media_effects_terminal_finished",
    } <= constraints.keys()
    assert "claimed_by IS NULL" in constraints["ck_media_effects_claim"]
    assert {
        "ix_media_effects_aggregate",
        "ix_media_effects_claimable",
        "ix_media_effects_claim_recovery",
        "ix_media_effects_status_updated",
    } <= indexes

    with pytest.raises(IntegrityError):
        with p5_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO media_effects (
                        id, effect_key, effect_type, aggregate_type, aggregate_id,
                        payload_json, request_hash, status, claimed_by
                    ) VALUES (
                        :id, 'invalid-partial-claim', :effect_type, :aggregate_type, :aggregate_id,
                        '{}'::jsonb, :request_hash, 'pending', 'orphan-worker'
                    )
                    """
                ),
                {
                    "id": uuid4(),
                    "effect_type": EFFECT_TYPE,
                    "aggregate_type": AGGREGATE_TYPE,
                    "aggregate_id": AGGREGATE_ID,
                    "request_hash": b"x" * 32,
                },
            )


def test_pg18_reserve_reuses_exact_request_and_persists_one_outbox(p5_runtime: DatabaseRuntime) -> None:
    ledger = _ledger(p5_runtime)
    effect_key = "emby.metadata:Movie:42:revision-7"
    payload = {"item_id": "emby-42", "metadata": {"name": "测试", "year": 2026}}

    first = ledger.reserve(
        effect_key=effect_key,
        effect_type=EFFECT_TYPE,
        aggregate_type=AGGREGATE_TYPE,
        aggregate_id=AGGREGATE_ID,
        payload=payload,
        max_publish_attempts=7,
    )
    reused = ledger.reserve(
        effect_key=effect_key,
        effect_type=EFFECT_TYPE,
        aggregate_type=AGGREGATE_TYPE,
        aggregate_id=AGGREGATE_ID,
        payload={"metadata": {"year": 2026, "name": "测试"}, "item_id": "emby-42"},
        max_publish_attempts=99,
    )

    assert first.created is True
    assert reused.created is False
    assert reused.effect == first.effect
    assert ledger.read(effect_key) == first.effect
    assert first.effect.status is MediaEffectStatus.PENDING
    assert first.effect.attempts == 0
    assert first.effect.payload == payload
    assert first.effect.request_hash == media_effect_request_hash(
        effect_type=EFFECT_TYPE,
        aggregate_type=AGGREGATE_TYPE,
        aggregate_id=AGGREGATE_ID,
        payload=payload,
    )
    assert first.effect.claim_token is None
    assert first.effect.finished_at is None

    with p5_runtime.engine.connect() as connection:
        effect_count = connection.scalar(text("SELECT count(*) FROM media_effects"))
        outbox_rows = (
            connection.execute(
                text(
                    """
                SELECT event_key, topic, aggregate_type, aggregate_id, payload_version,
                       payload_json, status, max_publish_attempts
                FROM outbox_events
                WHERE topic = :topic
                """
                ),
                {"topic": MEDIA_EFFECT_TOPIC},
            )
            .mappings()
            .all()
        )

    assert effect_count == 1
    assert len(outbox_rows) == 1
    outbox = outbox_rows[0]
    assert outbox["event_key"] == media_effect_event_key(effect_key)
    assert outbox["topic"] == MEDIA_EFFECT_TOPIC
    assert outbox["aggregate_type"] == "media_effect"
    assert outbox["aggregate_id"] == str(first.effect.id)
    assert outbox["payload_version"] == 1
    assert outbox["status"] == "pending"
    assert outbox["max_publish_attempts"] == 7
    assert outbox["payload_json"] == {
        "effect_id": str(first.effect.id),
        "effect_key": effect_key,
        "effect_type": EFFECT_TYPE,
        "aggregate_type": AGGREGATE_TYPE,
        "aggregate_id": AGGREGATE_ID,
        "request_hash": first.effect.request_hash.hex(),
    }


def test_pg18_outbox_conflict_rolls_back_effect_reservation(p5_runtime: DatabaseRuntime) -> None:
    ledger = _ledger(p5_runtime)
    effect_key = "atomic-conflict"
    event_key = media_effect_event_key(effect_key)
    with p5_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO outbox_events (
                    id, event_key, topic, aggregate_type, aggregate_id, payload_json
                ) VALUES (
                    :id, :event_key, :topic, 'media_effect', 'preexisting', '{}'::jsonb
                )
                """
            ),
            {"id": uuid4(), "event_key": event_key, "topic": MEDIA_EFFECT_TOPIC},
        )

    with pytest.raises(IntegrityError):
        ledger.reserve(
            effect_key=effect_key,
            effect_type=EFFECT_TYPE,
            aggregate_type=AGGREGATE_TYPE,
            aggregate_id=AGGREGATE_ID,
            payload={"item_id": "emby-42"},
        )

    with p5_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT count(*) FROM media_effects WHERE effect_key = :effect_key"),
                {"effect_key": effect_key},
            )
            == 0
        )
        assert (
            connection.scalar(
                text("SELECT count(*) FROM outbox_events WHERE event_key = :event_key"),
                {"event_key": event_key},
            )
            == 1
        )


def test_pg18_concurrent_reserve_creates_one_effect_and_one_outbox(p5_runtime: DatabaseRuntime) -> None:
    barrier = Barrier(2, timeout=10)
    effect_key = "concurrent-reservation"

    def reserve(_index: int) -> Any:
        barrier.wait()
        return _ledger(p5_runtime).reserve(
            effect_key=effect_key,
            effect_type=EFFECT_TYPE,
            aggregate_type=AGGREGATE_TYPE,
            aggregate_id=AGGREGATE_ID,
            payload={"item_id": "emby-42", "revision": 7},
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(reserve, range(2)))

    assert sorted(result.created for result in results) == [False, True]
    assert len({result.effect.id for result in results}) == 1
    with p5_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM media_effects")) == 1
        assert (
            connection.scalar(
                text("SELECT count(*) FROM outbox_events WHERE topic = :topic"),
                {"topic": MEDIA_EFFECT_TOPIC},
            )
            == 1
        )


def test_pg18_same_key_with_different_request_fails_closed(p5_runtime: DatabaseRuntime) -> None:
    ledger = _ledger(p5_runtime)
    effect_key = "conflicting-reservation"
    original = ledger.reserve(
        effect_key=effect_key,
        effect_type=EFFECT_TYPE,
        aggregate_type=AGGREGATE_TYPE,
        aggregate_id=AGGREGATE_ID,
        payload={"revision": 1},
    )

    with pytest.raises(MediaEffectConflictError):
        ledger.reserve(
            effect_key=effect_key,
            effect_type=EFFECT_TYPE,
            aggregate_type=AGGREGATE_TYPE,
            aggregate_id=AGGREGATE_ID,
            payload={"revision": 2},
        )

    persisted = ledger.read(effect_key)
    assert persisted == original.effect
    assert persisted is not None and persisted.payload == {"revision": 1}
    with p5_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM media_effects")) == 1
        assert (
            connection.scalar(
                text("SELECT count(*) FROM outbox_events WHERE topic = :topic"),
                {"topic": MEDIA_EFFECT_TOPIC},
            )
            == 1
        )


def test_pg18_expired_claim_becomes_indeterminate_without_replay(p5_runtime: DatabaseRuntime) -> None:
    ledger = _ledger(p5_runtime)
    effect_key = "claim-recovery"
    ledger.reserve(
        effect_key=effect_key,
        effect_type=EFFECT_TYPE,
        aggregate_type=AGGREGATE_TYPE,
        aggregate_id=AGGREGATE_ID,
        payload={"item_id": "emby-42"},
    )

    first_claims = ledger.claim(worker_id="worker-1", lease_seconds=60)
    assert len(first_claims) == 1
    first = first_claims[0]
    assert first.status is MediaEffectStatus.CLAIMED
    assert first.claimed_by == "worker-1"
    assert first.claim_token is not None
    assert first.attempts == 1
    assert ledger.claim(worker_id="worker-2", lease_seconds=60) == ()

    with p5_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE media_effects
                SET claimed_at = NOW() - INTERVAL '2 minutes',
                    lease_expires_at = NOW() - INTERVAL '1 minute'
                WHERE effect_key = :effect_key
                """
            ),
            {"effect_key": effect_key},
        )

    assert ledger.claim(worker_id="worker-2", lease_seconds=60) == ()
    assert ledger.expire_stale_claims() == 1
    expired = ledger.read(effect_key)
    assert expired is not None
    assert expired.status is MediaEffectStatus.INDETERMINATE
    assert expired.attempts == 1
    assert expired.claim_token is None
    assert expired.error == {
        "type": "claim_lease_expired",
        "message": "执行器租约过期，外部结果未知，禁止自动重放",
    }
    assert ledger.claim(worker_id="worker-2", lease_seconds=60) == ()

    assert first.claim_token is not None
    with pytest.raises(MediaEffectStateError):
        ledger.mark_confirmed(effect_key, first.claim_token, result={"ok": True})
    with pytest.raises(MediaEffectStateError):
        ledger.mark_rejected(effect_key, first.claim_token, error={"reason": "stale"})
    with pytest.raises(MediaEffectStateError):
        ledger.mark_indeterminate(effect_key, first.claim_token, error={"reason": "stale"})
    with pytest.raises(MediaEffectStateError):
        ledger.mark_retryable(effect_key, first.claim_token, error={"reason": "stale"})
    with p5_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT status FROM outbox_events WHERE topic = :topic"),
                {"topic": MEDIA_EFFECT_TOPIC},
            )
            == "processed"
        )


def test_pg18_retryable_effect_is_fenced_and_can_be_confirmed(p5_runtime: DatabaseRuntime) -> None:
    ledger = _ledger(p5_runtime)
    effect_key = "retryable-effect"
    ledger.reserve(
        effect_key=effect_key,
        effect_type=EFFECT_TYPE,
        aggregate_type=AGGREGATE_TYPE,
        aggregate_id=AGGREGATE_ID,
        payload={"item_id": "emby-42"},
    )
    second = ledger.claim(worker_id="worker-2", lease_seconds=60)[0]
    assert second.claim_token is not None

    retryable = ledger.mark_retryable(
        effect_key,
        second.claim_token,
        error={"reason": "provider-confirmed-no-effect"},
        retry_after_seconds=60,
    )
    assert retryable.changed is True
    assert retryable.effect.status is MediaEffectStatus.RETRYABLE
    assert retryable.effect.claim_token is None
    assert retryable.effect.error == {"reason": "provider-confirmed-no-effect"}
    assert ledger.claim(worker_id="worker-3", lease_seconds=60) == ()
    with pytest.raises(MediaEffectStateError):
        ledger.mark_retryable(effect_key, second.claim_token, error={"reason": "provider-confirmed-no-effect"})

    with p5_runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE media_effects SET available_at = NOW() - INTERVAL '1 second' WHERE effect_key = :effect_key"),
            {"effect_key": effect_key},
        )

    third = ledger.claim(worker_id="worker-3", lease_seconds=60)[0]
    assert third.attempts == 2
    assert third.claim_token is not None
    confirmed = ledger.mark_confirmed(effect_key, third.claim_token, result={"provider_id": "result-42"})
    duplicate = ledger.mark_confirmed(effect_key, third.claim_token, result={"provider_id": "result-42"})

    assert confirmed.changed is True
    assert duplicate.changed is False
    assert duplicate.effect.status is MediaEffectStatus.CONFIRMED
    assert duplicate.effect.finished_at is not None
    assert duplicate.effect.result == {"provider_id": "result-42"}
    assert duplicate.effect.error is None
    assert duplicate.effect.attempts == 2
    assert ledger.claim(worker_id="worker-4", lease_seconds=60) == ()
    assert ledger.read(effect_key) == duplicate.effect


def test_pg18_media_outbox_recovery_requeues_only_no_effect_and_rejects_dead_dispatch(
    p5_runtime: DatabaseRuntime,
) -> None:
    ledger = _ledger(p5_runtime)
    published = ledger.reserve(
        effect_key="published-no-effect",
        effect_type=EFFECT_TYPE,
        aggregate_type=AGGREGATE_TYPE,
        aggregate_id=f"{AGGREGATE_ID}:published",
        payload={"item_id": "emby-published"},
    ).effect
    dead = ledger.reserve(
        effect_key="dead-no-effect",
        effect_type=EFFECT_TYPE,
        aggregate_type=AGGREGATE_TYPE,
        aggregate_id=f"{AGGREGATE_ID}:dead",
        payload={"item_id": "emby-dead"},
    ).effect
    with p5_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE outbox_events
                SET status = 'published', published_at = NOW() - INTERVAL '10 minutes'
                WHERE aggregate_id = :effect_id
                """
            ),
            {"effect_id": str(published.id)},
        )
        connection.execute(
            text(
                """
                UPDATE outbox_events
                SET status = 'dead', publish_attempts = max_publish_attempts,
                    last_error = 'fixture broker unavailable'
                WHERE aggregate_id = :effect_id
                """
            ),
            {"effect_id": str(dead.id)},
        )

    assert ledger.requeue_stale_published(processed_timeout_seconds=60) == 1
    assert ledger.reject_dead_dispatches() == 1
    with p5_runtime.engine.connect() as connection:
        states = {
            str(row["aggregate_id"]): str(row["status"])
            for row in connection.execute(
                text("SELECT aggregate_id, status FROM outbox_events WHERE topic = :topic"),
                {"topic": MEDIA_EFFECT_TOPIC},
            ).mappings()
        }
    assert states[str(published.id)] == "pending"
    assert states[str(dead.id)] == "dead"
    rejected = ledger.read_by_id(dead.id)
    assert rejected is not None
    assert rejected.status is MediaEffectStatus.REJECTED
    assert rejected.error == {
        "type": "effect_dispatch_exhausted",
        "message": "媒体副作用消息投递失败，未调用外部提供方",
    }


def test_pg18_rejected_and_indeterminate_effects_are_terminal_without_replay(
    p5_runtime: DatabaseRuntime,
) -> None:
    ledger = _ledger(p5_runtime)
    for effect_key in ("rejected-effect", "indeterminate-effect"):
        ledger.reserve(
            effect_key=effect_key,
            effect_type=EFFECT_TYPE,
            aggregate_type=AGGREGATE_TYPE,
            aggregate_id=f"{AGGREGATE_ID}:{effect_key}",
            payload={"effect_key": effect_key},
        )

    claims = {claim.effect_key: claim for claim in ledger.claim(worker_id="terminal-worker", lease_seconds=60, limit=2)}
    rejected_claim = claims["rejected-effect"]
    indeterminate_claim = claims["indeterminate-effect"]
    assert rejected_claim.claim_token is not None
    assert indeterminate_claim.claim_token is not None

    rejected = ledger.mark_rejected(
        rejected_claim.effect_key,
        rejected_claim.claim_token,
        error={"reason": "provider-rejected"},
    )
    indeterminate = ledger.mark_indeterminate(
        indeterminate_claim.effect_key,
        indeterminate_claim.claim_token,
        error={"reason": "provider-outcome-unknown"},
    )

    assert rejected.changed is True
    assert rejected.effect.status is MediaEffectStatus.REJECTED
    assert rejected.effect.finished_at is not None
    assert indeterminate.changed is True
    assert indeterminate.effect.status is MediaEffectStatus.INDETERMINATE
    assert indeterminate.effect.finished_at is not None
    assert (
        ledger.mark_rejected(
            rejected_claim.effect_key,
            rejected_claim.claim_token,
            error={"reason": "provider-rejected"},
        ).changed
        is False
    )
    assert (
        ledger.mark_indeterminate(
            indeterminate_claim.effect_key,
            indeterminate_claim.claim_token,
            error={"reason": "provider-outcome-unknown"},
        ).changed
        is False
    )
    with pytest.raises(MediaEffectStateError):
        ledger.mark_confirmed(indeterminate_claim.effect_key, indeterminate_claim.claim_token)
    with pytest.raises(MediaEffectStateError):
        ledger.mark_retryable(indeterminate_claim.effect_key, indeterminate_claim.claim_token)
    assert ledger.claim(worker_id="replay-worker", lease_seconds=60, limit=10) == ()
