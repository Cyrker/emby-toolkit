from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import text

from etk.domains.media.bridge_client import BridgeWriteResult
from etk.domains.media.editing_effects import BridgeMediaEffectDelivery
from etk.domains.media.effect_execution import (
    MediaEffectExecutionUnavailable,
    PersistentMediaEffectExecutor,
    RoutedMediaEffectDelivery,
)
from etk.domains.media.effects import MediaEffectStatus, PostgresMediaEffectLedger
from etk.platform.db import DatabaseRuntime
from etk.platform.jobs import PostgresJobStore
from etk.processes.p5_media_effects import CeleryMediaEffectPublisher, MediaEffectOutboxDispatcher
from etk.processes.runtime_config import TaskRuntimeConfig


class FakeCelery:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def send_task(self, name: str, **options: Any) -> None:
        self.calls.append((name, options))


class FakeBridge:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def authorization_revision(self) -> str:
        return "a" * 64

    def apply_mediainfo(
        self,
        item_id: str,
        raw: Any,
        *,
        drop_conflicting_external_streams: bool = False,
        expected_authorization_revision: str | None = None,
    ) -> BridgeWriteResult:
        assert expected_authorization_revision == self.authorization_revision()
        self.calls.append((item_id, raw))
        return BridgeWriteResult(
            "apply-mediainfo",
            200,
            {"Applied": True, "Drop": drop_conflicting_external_streams},
        )

    def update_chapters(self, item_id: str, **_options: Any) -> BridgeWriteResult:
        raise AssertionError(f"unexpected chapter delivery for {item_id}")


def test_pg18_media_effect_outbox_dispatches_once_and_worker_marks_it_processed(
    p5_runtime: DatabaseRuntime,
) -> None:
    ledger = PostgresMediaEffectLedger(p5_runtime.unit_of_work)
    effect = ledger.reserve(
        effect_key="p5:dispatch:fixture-effect",
        effect_type="emby.mediainfo.apply",
        aggregate_type="media_asset",
        aggregate_id="fixture-item:fixture-sha1",
        payload={
            "item_id": "fixture-item",
            "sha1": "A" * 40,
            "emby_authorization_revision": "a" * 64,
            "mediainfo": {
                "MediaSourceInfo": {
                    "MediaStreams": [{"Type": "Video", "Codec": "h264"}],
                }
            },
            "drop_conflicting_external_streams": False,
        },
    ).effect
    config = TaskRuntimeConfig(
        broker_url="redis://fixture.invalid:6379/0",
        queue_name="fixture.serial",
        heartbeat_seconds=5,
        lease_seconds=30,
        schedules_enabled=False,
    )
    fake_celery = FakeCelery()
    dispatcher = MediaEffectOutboxDispatcher(
        PostgresJobStore(p5_runtime.unit_of_work),
        ledger,
        CeleryMediaEffectPublisher(config, celery_app=fake_celery),
        config,
        publisher_id="fixture-coordinator:media",
    )

    dispatched = dispatcher.tick()

    assert dispatched.publish.published == 1
    assert len(fake_celery.calls) == 1
    with p5_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT status FROM outbox_events WHERE aggregate_id = :effect_id"), {"effect_id": str(effect.id)}
            )
            == "published"
        )

    bridge = FakeBridge()
    completed = PersistentMediaEffectExecutor(
        ledger,
        BridgeMediaEffectDelivery(bridge),
        worker_id="fixture-worker:media",
        lease_seconds=90,
    ).execute(effect.id)

    assert completed.status is MediaEffectStatus.CONFIRMED
    assert bridge.calls == [("fixture-item", effect.payload["mediainfo"])]
    with p5_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT status FROM outbox_events WHERE aggregate_id = :effect_id"), {"effect_id": str(effect.id)}
            )
            == "processed"
        )
    second = dispatcher.tick()
    assert second.publish.claimed == 0
    assert len(fake_celery.calls) == 1


def test_pg18_narrow_executor_leaves_unsupported_effect_pending_and_unclaimed(
    p5_runtime: DatabaseRuntime,
) -> None:
    ledger = PostgresMediaEffectLedger(p5_runtime.unit_of_work)
    effect = ledger.reserve(
        effect_key="p5:narrow-worker:unsupported-effect",
        effect_type="emby.metadata.refresh",
        aggregate_type="media_asset",
        aggregate_id="fixture-item",
        payload={"item_id": "fixture-item"},
    ).effect
    bridge = FakeBridge()
    executor = PersistentMediaEffectExecutor(
        ledger,
        RoutedMediaEffectDelivery([BridgeMediaEffectDelivery(bridge)]),
        worker_id="fixture-narrow-worker:media",
        lease_seconds=90,
    )

    with pytest.raises(MediaEffectExecutionUnavailable, match="unavailable in this Worker profile"):
        executor.execute(effect.id)

    with p5_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                    SELECT status, attempts, claimed_by, claim_token, claimed_at, lease_expires_at
                    FROM media_effects
                    WHERE id = :effect_id
                    """
                ),
                {"effect_id": effect.id},
            )
            .mappings()
            .one()
        )
    assert dict(row) == {
        "status": "pending",
        "attempts": 0,
        "claimed_by": None,
        "claim_token": None,
        "claimed_at": None,
        "lease_expires_at": None,
    }
    assert bridge.calls == []


def test_pg18_narrow_executor_preserves_unsupported_retryable_effect_and_schedule(
    p5_runtime: DatabaseRuntime,
) -> None:
    ledger = PostgresMediaEffectLedger(p5_runtime.unit_of_work)
    effect = ledger.reserve(
        effect_key="p5:narrow-worker:unsupported-retryable-effect",
        effect_type="emby.metadata.refresh",
        aggregate_type="media_asset",
        aggregate_id="fixture-retryable-item",
        payload={"item_id": "fixture-retryable-item"},
    ).effect
    claimed = ledger.claim_by_id(
        effect.id,
        worker_id="fixture-retryable-worker",
        lease_seconds=90,
    )
    assert claimed is not None and claimed.claim_token is not None
    retryable = ledger.mark_retryable(
        claimed.effect_key,
        claimed.claim_token,
        error={"reason": "fixture retry"},
        retry_after_seconds=600,
    ).effect
    bridge = FakeBridge()
    executor = PersistentMediaEffectExecutor(
        ledger,
        RoutedMediaEffectDelivery([BridgeMediaEffectDelivery(bridge)]),
        worker_id="fixture-narrow-worker:media",
        lease_seconds=90,
    )

    with pytest.raises(MediaEffectExecutionUnavailable, match="unavailable in this Worker profile"):
        executor.execute(effect.id)

    with p5_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                    SELECT status, attempts, available_at, claimed_by, claim_token,
                           claimed_at, lease_expires_at, error_json
                    FROM media_effects
                    WHERE id = :effect_id
                    """
                ),
                {"effect_id": effect.id},
            )
            .mappings()
            .one()
        )
    assert row["status"] == "retryable"
    assert row["attempts"] == retryable.attempts
    assert row["available_at"] == retryable.available_at
    assert row["claimed_by"] is None
    assert row["claim_token"] is None
    assert row["claimed_at"] is None
    assert row["lease_expires_at"] is None
    assert row["error_json"] == retryable.error
    assert bridge.calls == []


def test_pg18_narrow_executor_preserves_unsupported_claimed_effect(
    p5_runtime: DatabaseRuntime,
) -> None:
    ledger = PostgresMediaEffectLedger(p5_runtime.unit_of_work)
    effect = ledger.reserve(
        effect_key="p5:narrow-worker:unsupported-claimed-effect",
        effect_type="emby.metadata.refresh",
        aggregate_type="media_asset",
        aggregate_id="fixture-claimed-item",
        payload={"item_id": "fixture-claimed-item"},
    ).effect
    claimed = ledger.claim_by_id(
        effect.id,
        worker_id="fixture-claimed-worker",
        lease_seconds=90,
    )
    assert claimed is not None
    bridge = FakeBridge()
    executor = PersistentMediaEffectExecutor(
        ledger,
        RoutedMediaEffectDelivery([BridgeMediaEffectDelivery(bridge)]),
        worker_id="fixture-narrow-worker:media",
        lease_seconds=90,
    )

    with pytest.raises(MediaEffectExecutionUnavailable, match="unavailable in this Worker profile"):
        executor.execute(effect.id)

    with p5_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                    SELECT status, attempts, claimed_by, claim_token, claimed_at, lease_expires_at
                    FROM media_effects
                    WHERE id = :effect_id
                    """
                ),
                {"effect_id": effect.id},
            )
            .mappings()
            .one()
        )
    assert row["status"] == "claimed"
    assert row["attempts"] == claimed.attempts
    assert row["claimed_by"] == claimed.claimed_by
    assert row["claim_token"] == claimed.claim_token
    assert row["claimed_at"] == claimed.claimed_at
    assert row["lease_expires_at"] == claimed.lease_expires_at
    assert bridge.calls == []
