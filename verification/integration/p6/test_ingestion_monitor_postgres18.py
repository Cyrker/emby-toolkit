from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest
from sqlalchemy import inspect, text

from etk.domains.ingestion import (
    INGESTION_MONITOR_TOPIC,
    BindingStatus,
    FileObservation,
    IngestionMonitorStateError,
    MonitorItemStatus,
    MonitorPolicy,
    MonitorSource,
    PostgresIngestionMonitorLedger,
    StabilityOutcome,
    normalize_media_path,
)
from etk.domains.ingestion.effects import stage_ingestion_path_notification
from etk.domains.media.bridge_client import BridgeWriteIndeterminate, BridgeWriteResult, emby_authorization_revision
from etk.domains.media.editing_effects import BridgeMediaEffectDelivery
from etk.domains.media.effect_execution import (
    MediaEffectDeliveryIndeterminate,
    MediaEffectDeliveryRejected,
    PersistentMediaEffectExecutor,
)
from etk.domains.media.effects import MEDIA_EFFECT_TOPIC, PostgresMediaEffectLedger
from etk.platform import Settings
from etk.platform.db import DatabaseRuntime
from etk.processes.p6_ingestion import (
    IngestionBindingIndeterminate,
    IngestionMonitorExecutor,
    NativeIngestionBindingDelivery,
    PersistentPendingBindingGateway,
)


@pytest.fixture(autouse=True)
def clean_ingestion_state(p6_runtime: DatabaseRuntime):
    def clean() -> None:
        with p6_runtime.engine.begin() as connection:
            connection.execute(text("DELETE FROM ingestion_pending_bindings"))
            connection.execute(text("DELETE FROM ingestion_monitor_items"))
            connection.execute(
                text("DELETE FROM outbox_events WHERE topic = :topic"),
                {"topic": INGESTION_MONITOR_TOPIC},
            )
            connection.execute(
                text(
                    """
                    DELETE FROM outbox_events
                    WHERE topic = :topic
                      AND aggregate_id IN (
                          SELECT id::text
                          FROM media_effects
                          WHERE aggregate_type IN ('ingestion_monitor', 'ingestion_binding')
                      )
                    """
                ),
                {"topic": MEDIA_EFFECT_TOPIC},
            )
            connection.execute(
                text("DELETE FROM media_effects WHERE aggregate_type IN ('ingestion_monitor', 'ingestion_binding')")
            )
            connection.execute(text("DELETE FROM media_metadata WHERE tmdb_id LIKE 'p6-monitor-fixture-%'"))

    clean()
    yield
    clean()


def _ledger(runtime: DatabaseRuntime) -> PostgresIngestionMonitorLedger:
    return PostgresIngestionMonitorLedger(runtime.unit_of_work)


def _policy(*, enabled: bool = True, refresh_delay_seconds: int = 0) -> MonitorPolicy:
    return MonitorPolicy.create(
        enabled=enabled,
        extensions=(".mkv", ".strm", ".mp3"),
        exclude_paths=("/library/excluded",),
        refresh_delay_seconds=refresh_delay_seconds,
    )


def _created(path: str, producer_key: str, *, enabled: bool = True, seconds_ago: int = 10):
    return _policy(enabled=enabled).created(
        path,
        producer_key=producer_key,
        monitor_root="/library",
        observed_at=datetime.now(UTC) - timedelta(seconds=seconds_ago),
    )


def _confirm_notification(
    runtime: DatabaseRuntime,
    item_id: UUID,
):
    ledger = _ledger(runtime)
    ledger.mark_ready(item_id)
    claimed_item = ledger.claim_by_id(item_id, worker_id="fixture-monitor-worker", lease_seconds=60)
    assert claimed_item is not None and claimed_item.claim_token is not None
    settings = Settings(
        emby_server_url="http://emby.fixture.invalid",
        emby_api_key="fixture-token",
    )
    with runtime.unit_of_work() as uow:
        effect = stage_ingestion_path_notification(
            uow.session,
            claimed_item,
            authorization_revision=emby_authorization_revision(settings),
        ).effect
        uow.commit()
    effects = PostgresMediaEffectLedger(runtime.unit_of_work)
    effect_claim = effects.claim_by_id(effect.id, worker_id="fixture-effect-worker", lease_seconds=60)
    assert effect_claim is not None and effect_claim.claim_token is not None
    effects.mark_confirmed(effect_claim.effect_key, effect_claim.claim_token, result={"ok": True})
    return claimed_item


def test_migration_schema_constraints_and_indexes_are_present(p6_runtime: DatabaseRuntime) -> None:
    inspector = inspect(p6_runtime.engine)
    assert {"ingestion_monitor_items", "ingestion_pending_bindings"} <= set(inspector.get_table_names())
    monitor_constraints = {
        constraint["name"] for constraint in inspector.get_check_constraints("ingestion_monitor_items")
    }
    assert {
        "ck_ingestion_monitor_status",
        "ck_ingestion_monitor_claim",
        "ck_ingestion_monitor_terminal_finished",
        "ck_ingestion_monitor_event_classification",
        "ck_ingestion_monitor_refresh_delay",
    } <= monitor_constraints
    monitor_indexes = {index["name"] for index in inspector.get_indexes("ingestion_monitor_items")}
    assert {
        "uq_ingestion_monitor_unclaimed_path",
        "uq_ingestion_monitor_claimed_path",
        "ix_ingestion_monitor_claimable",
        "ix_ingestion_monitor_stability",
    } <= monitor_indexes
    media_effect_indexes = {index["name"] for index in inspector.get_indexes("media_effects")}
    assert "uq_ingestion_monitor_notification_effect" in media_effect_indexes


def test_active_ingestion_source_is_preserved_in_the_durable_projection(
    p6_runtime: DatabaseRuntime,
) -> None:
    intent = _policy().created(
        "/library/Active.mkv",
        producer_key="active:created:fixture",
        monitor_root="/library",
        source=MonitorSource.ACTIVE,
        observed_at=datetime.now(UTC) - timedelta(seconds=10),
    )[0]

    item = _ledger(p6_runtime).record((intent,))[0].item

    assert item.source is MonitorSource.ACTIVE
    assert _ledger(p6_runtime).read(item.id) == item


def test_refresh_only_delay_is_snapshotted_and_applied_after_stability(
    p6_runtime: DatabaseRuntime,
) -> None:
    ledger = _ledger(p6_runtime)
    sampled_at = datetime.now(UTC)
    intent = _policy(refresh_delay_seconds=17).created(
        "/library/excluded/Album.mp3",
        producer_key="refresh-delay-1",
        monitor_root="/library",
        observed_at=sampled_at - timedelta(seconds=10),
    )[0]
    item = ledger.record((intent,))[0].item

    for offset in range(4):
        item, outcome = ledger.record_stability_sample(
            item.id,
            FileObservation(True, size=100, mtime_ns=123),
            observed_at=sampled_at + timedelta(seconds=offset),
        )

    assert outcome is StabilityOutcome.READY
    assert item.status is MonitorItemStatus.READY
    assert item.refresh_delay_seconds == 17
    assert item.available_at == sampled_at + timedelta(seconds=20)
    with p6_runtime.engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT item.refresh_delay_seconds, event.available_at
                FROM ingestion_monitor_items AS item
                JOIN outbox_events AS event
                  ON event.aggregate_id = item.id::text
                WHERE item.id = :item_id AND event.topic = :topic
                """
            ),
            {"item_id": item.id, "topic": INGESTION_MONITOR_TOPIC},
        ).one()
    assert row == (17, item.available_at)


def test_same_path_coalesces_idempotently_and_claim_order_is_deterministic(
    p6_runtime: DatabaseRuntime,
) -> None:
    ledger = _ledger(p6_runtime)
    first = ledger.record(_created("/library/B.mkv", "created-b-1"))[0]
    replacement = ledger.record(_created("/library/B.mkv", "created-b-2"))[0]
    replay = ledger.record(_created("/library/B.mkv", "created-b-2"))[0]
    earliest = ledger.record(_created("/library/A.mkv", "created-a-1"))[0]

    assert first.created is True
    assert ledger.read(first.item.id).status is MonitorItemStatus.SUPERSEDED  # type: ignore[union-attr]
    assert replacement.superseded_ids == (first.item.id,)
    assert replay.created is False
    assert replay.item.id == replacement.item.id
    assert earliest.item.event_sequence > replacement.item.event_sequence

    ledger.mark_ready(replacement.item.id)
    ledger.mark_ready(earliest.item.id)
    claims = ledger.claim(worker_id="worker-1", lease_seconds=30, limit=2)

    assert [claim.id for claim in claims] == [replacement.item.id, earliest.item.id]
    assert [claim.event_sequence for claim in claims] == sorted(claim.event_sequence for claim in claims)
    assert all(claim.claim_token is not None for claim in claims)

    with p6_runtime.engine.connect() as connection:
        states = dict(
            connection.execute(
                text(
                    """
                    SELECT aggregate_id, status
                    FROM outbox_events
                    WHERE topic = :topic
                    ORDER BY created_at, id
                    """
                ),
                {"topic": INGESTION_MONITOR_TOPIC},
            ).all()
        )
    assert states == {str(replacement.item.id): "processed", str(earliest.item.id): "processed"}


def test_move_persists_correlated_source_delete_before_destination_upsert(
    p6_runtime: DatabaseRuntime,
) -> None:
    correlation_time = datetime.now(UTC) - timedelta(seconds=10)
    intents = _policy().moved(
        "/library/Old.mkv",
        "/library/New.mkv",
        producer_key="move-1",
        monitor_root="/library",
        observed_at=correlation_time,
    )

    source, destination = _ledger(p6_runtime).record(intents)

    assert source.item.event_type.value == "deleted"
    assert destination.item.event_type.value == "upsert"
    assert source.item.correlation_id == destination.item.correlation_id
    assert source.item.related_path == destination.item.path
    assert destination.item.related_path == source.item.path
    assert source.item.event_sequence < destination.item.event_sequence


def test_new_event_does_not_mutate_a_claimed_same_path_snapshot(p6_runtime: DatabaseRuntime) -> None:
    ledger = _ledger(p6_runtime)
    original = ledger.record(_created("/library/Movie.mkv", "created-original"))[0].item
    ledger.mark_ready(original.id)
    claimed = ledger.claim_by_id(original.id, worker_id="worker-1", lease_seconds=30)
    assert claimed is not None and claimed.claim_token is not None

    newer = ledger.record(_created("/library/Movie.mkv", "created-newer"))[0].item
    observed_original = ledger.read(original.id)

    assert observed_original is not None
    assert observed_original.status is MonitorItemStatus.CLAIMED
    assert observed_original.claim_token == claimed.claim_token
    assert newer.id != original.id
    assert newer.status is MonitorItemStatus.PENDING
    ledger.mark_confirmed(original.id, claimed.claim_token, result={"handled": True})


def test_stability_samples_and_ready_outbox_survive_repository_recreation(
    p6_runtime: DatabaseRuntime,
) -> None:
    ledger = _ledger(p6_runtime)
    item = ledger.record(_created("/library/Movie.mkv", "stable-created"))[0].item
    base = datetime.now(UTC)

    for index, expected_matches in enumerate((0, 1, 2)):
        current, outcome = _ledger(p6_runtime).record_stability_sample(
            item.id,
            FileObservation(True, size=2048, mtime_ns=index + 1),
            observed_at=base + timedelta(seconds=index),
        )
        assert current.stable_matches == expected_matches
        assert outcome is StabilityOutcome.WAITING

    ready, outcome = _ledger(p6_runtime).record_stability_sample(
        item.id,
        FileObservation(True, size=2048, mtime_ns=4),
        observed_at=base + timedelta(seconds=3),
    )
    assert outcome is StabilityOutcome.READY
    assert ready.status is MonitorItemStatus.READY
    assert ready.stable_matches == 3

    with p6_runtime.engine.connect() as connection:
        event = (
            connection.execute(
                text("SELECT event_key, status, payload_json FROM outbox_events WHERE aggregate_id = :item_id"),
                {"item_id": str(item.id)},
            )
            .mappings()
            .one()
        )
    assert event["event_key"] == f"{INGESTION_MONITOR_TOPIC}:{item.id}"
    assert event["status"] == "pending"
    assert event["payload_json"]["item_id"] == str(item.id)


def test_expired_claim_is_retryable_and_stale_token_is_fenced(p6_runtime: DatabaseRuntime) -> None:
    ledger = _ledger(p6_runtime)
    item = ledger.record(_created("/library/Retry.mkv", "retry-created"))[0].item
    ledger.mark_ready(item.id)
    first = ledger.claim_by_id(item.id, worker_id="worker-1", lease_seconds=30)
    assert first is not None and first.claim_token is not None

    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE ingestion_monitor_items
                SET claimed_at = NOW() - INTERVAL '2 minutes',
                    lease_expires_at = NOW() - INTERVAL '1 minute'
                WHERE id = :item_id
                """
            ),
            {"item_id": item.id},
        )

    assert _ledger(p6_runtime).expire_stale_claims() == 1
    recovered = ledger.read(item.id)
    assert recovered is not None
    assert recovered.status is MonitorItemStatus.RETRYABLE
    assert recovered.claim_token is None
    assert recovered.error == {"type": "claim_lease_expired"}

    with pytest.raises(IngestionMonitorStateError):
        ledger.mark_confirmed(item.id, first.claim_token, result={"stale": True})
    second = ledger.claim_by_id(item.id, worker_id="worker-2", lease_seconds=30)
    assert second is not None and second.claim_token is not None
    assert second.claim_token != first.claim_token


def test_expired_claim_is_superseded_when_a_newer_same_path_event_exists(
    p6_runtime: DatabaseRuntime,
) -> None:
    ledger = _ledger(p6_runtime)
    original = ledger.record(_created("/library/Replaced.mkv", "replace-original"))[0].item
    ledger.mark_ready(original.id)
    claimed = ledger.claim_by_id(original.id, worker_id="worker-1", lease_seconds=30)
    assert claimed is not None and claimed.claim_token is not None
    successor = ledger.record(_created("/library/Replaced.mkv", "replace-successor"))[0].item

    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE ingestion_monitor_items
                SET claimed_at = NOW() - INTERVAL '2 minutes',
                    lease_expires_at = NOW() - INTERVAL '1 minute'
                WHERE id = :item_id
                """
            ),
            {"item_id": original.id},
        )

    assert ledger.expire_stale_claims() == 1
    observed_original = ledger.read(original.id)
    observed_successor = ledger.read(successor.id)
    assert observed_original is not None
    assert observed_original.status is MonitorItemStatus.SUPERSEDED
    assert observed_original.error == {"type": "retry_superseded_by_newer_event"}
    assert observed_successor is not None
    assert observed_successor.status is MonitorItemStatus.PENDING


def test_disabled_backlog_suspends_and_resumes_without_loss(p6_runtime: DatabaseRuntime) -> None:
    ledger = _ledger(p6_runtime)
    initial = ledger.record(_created("/library/Disabled.mkv", "disabled-created", enabled=False))[0].item
    assert initial.status is MonitorItemStatus.SUSPENDED

    assert ledger.resume_suspended(quiet_window_seconds=0, stability_timeout_seconds=60) == 1
    resumed = ledger.read(initial.id)
    assert resumed is not None
    assert resumed.status is MonitorItemStatus.PENDING
    assert resumed.last_size is None
    assert resumed.stable_matches == 0

    assert ledger.suspend_active() == 1
    suspended = ledger.read(initial.id)
    assert suspended is not None and suspended.status is MonitorItemStatus.SUSPENDED


def test_pending_binding_can_be_accepted_and_replayed_after_runtime_recreation(
    p6_runtime: DatabaseRuntime,
) -> None:
    item = _ledger(p6_runtime).record(_created("/library/Binding.strm", "binding-created"))[0].item
    reservation = _ledger(p6_runtime).reserve_binding(
        binding_key=f"monitor:{item.id}",
        monitor_item_id=item.id,
        path=item.path,
        sha1="A" * 40,
        pick_code="pick-binding",
        media_info={"MediaSourceInfo": {"Container": "strm"}},
    )
    replayed_reservation = _ledger(p6_runtime).reserve_binding(
        binding_key=f"monitor:{item.id}",
        monitor_item_id=item.id,
        path=item.path,
        sha1="A" * 40,
        pick_code="pick-binding",
        media_info={"MediaSourceInfo": {"Container": "strm"}},
    )
    assert reservation.created is True
    assert replayed_reservation.created is False
    assert replayed_reservation.binding.id == reservation.binding.id

    restarted = _ledger(p6_runtime)
    pending = restarted.lookup_for_item_ready(
        path="/library/other-name.strm",
        sha1="A" * 40,
        pick_code="pick-binding",
    )
    assert pending is not None
    assert pending.status is BindingStatus.PENDING
    assert restarted.binding_for_monitor_item(item.id) == pending
    assert restarted.lookup_for_item_ready(pick_code="PICK-BINDING") == pending
    _confirm_notification(p6_runtime, item.id)
    claimed = restarted.claim_binding_by_id(pending.id, worker_id="item-ready-worker", lease_seconds=30)
    assert claimed is not None and claimed.claim_token is not None
    confirmed = restarted.mark_binding_confirmed(
        claimed.id,
        claimed.claim_token,
        emby_item_id="emby-item-901",
        result={"accepted": True},
    )
    assert confirmed.binding.status is BindingStatus.CONFIRMED

    callback_replay = _ledger(p6_runtime).lookup_for_item_ready(
        path=item.path,
        sha1="A" * 40,
        pick_code="pick-binding",
    )
    assert callback_replay is not None
    assert callback_replay.status is BindingStatus.CONFIRMED
    assert callback_replay.emby_item_id == "emby-item-901"


def test_confirmed_history_lookup_prefers_the_matching_emby_item_id(
    p6_runtime: DatabaseRuntime,
) -> None:
    ledger = _ledger(p6_runtime)
    confirmed_bindings = []
    for suffix, item_id in (("old", "101"), ("new", "102")):
        monitor = ledger.record(_created(f"/library/History-{suffix}.strm", f"history-{suffix}"))[0].item
        binding = ledger.reserve_binding(
            binding_key=f"monitor:{monitor.id}",
            monitor_item_id=monitor.id,
            path=monitor.path,
            sha1="B" * 40,
            pick_code="history-pick",
            media_info={"MediaSourceInfo": {"MediaStreams": []}},
        ).binding
        claimed_monitor = _confirm_notification(p6_runtime, monitor.id)
        claimed_binding = ledger.claim_binding_by_id(
            binding.id,
            worker_id=f"binding-{suffix}",
            lease_seconds=30,
        )
        assert claimed_binding is not None and claimed_binding.claim_token is not None
        confirmed = ledger.mark_binding_confirmed(
            claimed_binding.id,
            claimed_binding.claim_token,
            emby_item_id=item_id,
            result={"ok": True, "item_id": item_id},
        ).binding
        ledger.mark_confirmed(
            claimed_monitor.id,
            claimed_monitor.claim_token,
            result={"binding_id": str(confirmed.id)},
        )
        confirmed_bindings.append(confirmed)

    old = ledger.lookup_for_item_ready(
        sha1="B" * 40,
        pick_code="HISTORY-PICK",
        emby_item_id="101",
    )
    new = ledger.lookup_for_item_ready(
        sha1="B" * 40,
        pick_code="history-pick",
        emby_item_id="102",
    )

    assert old == confirmed_bindings[0]
    assert new == confirmed_bindings[1]


class _BindingBridge:
    def __init__(self, *, indeterminate: bool = False) -> None:
        self.indeterminate = indeterminate
        self.calls = 0
        self.revision = "d" * 64

    def authorization_revision(self) -> str:
        return self.revision

    def apply_mediainfo(self, item_id: str, raw: Any, **_options: Any) -> BridgeWriteResult:
        self.calls += 1
        if self.indeterminate:
            raise BridgeWriteIndeterminate("apply_mediainfo", "fixture response lost")
        return BridgeWriteResult("apply_mediainfo", 200, {"item_id": item_id, "raw": bool(raw)})


class _BindingJobs:
    def __init__(self) -> None:
        self.calls = 0
        self.projection: Any | None = None
        self.submissions: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def get_idempotent(self, *, scope: str, key: str) -> Any | None:
        assert scope == "ingestion-binding"
        assert key
        return self.projection

    def submit_native_operation(self, *_args: Any, **_options: Any) -> Any:
        self.calls += 1
        self.submissions.append((_args, dict(_options)))
        self.projection = SimpleNamespace(job=SimpleNamespace(id=UUID("00000000-0000-0000-0000-000000000978")))
        return SimpleNamespace(projection=self.projection, reused=False)


class _FailOnceBindingJobs(_BindingJobs):
    def __init__(self) -> None:
        super().__init__()
        self.failures = 1

    def submit_native_operation(self, *_args: Any, **_options: Any) -> Any:
        if self.failures:
            self.failures -= 1
            self.calls += 1
            self.submissions.append((_args, dict(_options)))
            raise RuntimeError("fixture crash before Job reservation")
        return super().submit_native_operation(*_args, **_options)


class _BindingMediaInfo:
    def __init__(self, value: object | None = None) -> None:
        self.value = value
        self.calls = 0

    def find_by_sha1(self, _sha1: str, *, record_hit: bool = False) -> Any:
        assert record_hit is True
        self.calls += 1
        return SimpleNamespace(formatted=self.value) if self.value is not None else None


def _binding_delivery_fixture(
    runtime: DatabaseRuntime,
    *,
    bridge: _BindingBridge,
    jobs: _BindingJobs | None = None,
    use_cached_mediainfo: bool = False,
    media_info: _BindingMediaInfo | None = None,
) -> tuple[NativeIngestionBindingDelivery, Any, _BindingJobs]:
    monitor = _ledger(runtime).record(_created("/library/Effect.strm", "binding-effect"))[0].item
    binding = (
        _ledger(runtime)
        .reserve_binding(
            binding_key=f"monitor:{monitor.id}",
            monitor_item_id=monitor.id,
            path=monitor.path,
            sha1="C" * 40,
            pick_code="effect-pick",
            media_info=None if use_cached_mediainfo else {"MediaSourceInfo": {"MediaStreams": []}},
        )
        .binding
    )
    jobs = jobs or _BindingJobs()
    effects = PersistentMediaEffectExecutor(
        PostgresMediaEffectLedger(runtime.unit_of_work),
        BridgeMediaEffectDelivery(bridge),
        worker_id="fixture-binding-effect",
        lease_seconds=60,
    )
    return (
        NativeIngestionBindingDelivery(
            jobs=lambda: cast(Any, jobs),
            uow_factory=runtime.unit_of_work,
            bridge=cast(Any, bridge),
            media_info=cast(Any, media_info or _BindingMediaInfo()),
            media_effects=effects,
        ),
        binding,
        jobs,
    )


def test_binding_mediainfo_effect_replays_without_a_second_external_write(
    p6_runtime: DatabaseRuntime,
) -> None:
    bridge = _BindingBridge()
    delivery, binding, jobs = _binding_delivery_fixture(p6_runtime, bridge=bridge)
    item = {"Id": "101", "Type": "Movie", "Name": "Fixture"}

    first = delivery.deliver(binding, item)
    bridge.revision = "e" * 64
    replay = delivery.deliver(binding, {**item, "Name": "Renamed after Job submit"})

    assert bridge.calls == 1
    assert jobs.calls == 1
    assert first["mediainfo_effect_id"] == replay["mediainfo_effect_id"]
    assert first["job_id"] == replay["job_id"]
    assert first["item_id"] == replay["item_id"] == "101"
    assert first["job_reused"] is False
    assert replay["job_reused"] is True


def test_binding_mediainfo_indeterminate_effect_is_terminal_and_never_replayed(
    p6_runtime: DatabaseRuntime,
) -> None:
    bridge = _BindingBridge(indeterminate=True)
    delivery, binding, jobs = _binding_delivery_fixture(p6_runtime, bridge=bridge)
    item = {"Id": "101", "Type": "Movie", "Name": "Fixture"}

    with pytest.raises(IngestionBindingIndeterminate, match="outcome is unknown"):
        delivery.deliver(binding, item)
    with pytest.raises(IngestionBindingIndeterminate, match="outcome is unknown"):
        delivery.deliver(binding, item)

    assert bridge.calls == 1
    assert jobs.calls == 0


def test_binding_crash_replay_reuses_durable_effect_snapshot_after_runtime_drift(
    p6_runtime: DatabaseRuntime,
) -> None:
    bridge = _BindingBridge()
    jobs = _FailOnceBindingJobs()
    cache = _BindingMediaInfo({"MediaSourceInfo": {"marker": "original"}})
    delivery, binding, _ = _binding_delivery_fixture(
        p6_runtime,
        bridge=bridge,
        jobs=jobs,
        use_cached_mediainfo=True,
        media_info=cache,
    )
    original_item = {
        "Id": "episode-101",
        "Type": "Episode",
        "Name": "Original episode name",
        "SeriesId": "series-original",
    }

    with pytest.raises(RuntimeError, match="crash before Job reservation"):
        delivery.deliver(binding, original_item)

    bridge.revision = "e" * 64
    cache.value = {"MediaSourceInfo": {"marker": "drifted"}}
    replay = delivery.deliver(
        binding,
        {
            **original_item,
            "Name": "Renamed after effect confirmation",
            "SeriesId": "series-drifted",
        },
    )

    effect = PostgresMediaEffectLedger(p6_runtime.unit_of_work).read(
        f"p6:ingestion-binding:emby.mediainfo.apply:{binding.id}"
    )
    assert effect is not None and effect.status.value == "confirmed"
    assert effect.payload["emby_authorization_revision"] == "d" * 64
    assert effect.payload["mediainfo"] == {"MediaSourceInfo": {"marker": "original"}}
    assert bridge.calls == 1
    assert cache.calls == 1
    assert jobs.calls == 2
    assert replay["item_id"] == "episode-101"
    successful_args, successful_options = jobs.submissions[-1]
    assert successful_args == (
        "media.reprocess-item",
        {
            "item_id": "episode-101",
            "restore_media_info": False,
        },
    )
    assert successful_options["display_name"] == "主动入库: Original episode name"
    assert successful_options["idempotency_scope"] == "ingestion-binding"
    assert successful_options["idempotency_key"] == str(binding.id)


class _Fingerprints:
    def extract(self, _path: str) -> tuple[str, str]:
        return "fixture-pick", "E" * 40


def test_deleted_monitor_item_stages_only_durable_emby_deleted_notification(
    p6_runtime: DatabaseRuntime,
) -> None:
    intent = _policy().deleted(
        "/library/Deleted.mkv",
        producer_key="deleted-1",
        monitor_root="/library",
        observed_at=datetime.now(UTC) - timedelta(seconds=10),
    )
    item = _ledger(p6_runtime).record(intent)[0].item
    _ledger(p6_runtime).mark_ready(item.id)
    settings = Settings(
        emby_server_url="http://emby.fixture.invalid",
        emby_api_key="fixture-token",
    )
    executor = IngestionMonitorExecutor(
        uow_factory=p6_runtime.unit_of_work,
        ledger=_ledger(p6_runtime),
        settings=lambda: settings,
        preparation=cast(Any, object()),
        pending_bindings=cast(Any, object()),
        media_effects=cast(Any, object()),
        fingerprints=cast(Any, object()),
        worker_id="delete-worker",
    )

    completed = executor.execute(item.id)

    assert completed is not None
    assert completed.status is MonitorItemStatus.CONFIRMED
    assert completed.result is not None
    effect_id = completed.result["notification_effect_id"]

    with p6_runtime.engine.connect() as connection:
        effect_row = (
            connection.execute(
                text(
                    """
                SELECT effect_type, aggregate_type, aggregate_id, payload_json
                FROM media_effects
                WHERE id = :effect_id
                """
                ),
                {"effect_id": effect_id},
            )
            .mappings()
            .one()
        )
    assert effect_row["effect_type"] == "emby.library.paths_changed"
    assert effect_row["aggregate_type"] == "ingestion_monitor"
    assert effect_row["aggregate_id"] == str(item.id)
    assert effect_row["payload_json"] == {
        "paths": ["/library/Deleted.mkv"],
        "update_type": "Deleted",
        "emby_authorization_revision": emby_authorization_revision(settings),
        "operation_revision": item.request_hash.hex(),
    }
    assert "deep-delete" not in str(effect_row["payload_json"])


class _NoopBindingDelivery:
    def deliver(self, _binding: Any, _item: Any) -> Any:
        raise AssertionError("notification failure must stop before binding delivery")


class _FlakyBindingDelivery:
    def __init__(self) -> None:
        self.calls = 0

    def deliver(self, _binding: Any, _item: Any) -> dict[str, Any]:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("fixture transient binding delivery failure")
        return {"job_id": "fixture-job"}


class _NotificationDelivery:
    effect_types = frozenset({"emby.library.paths_changed"})

    def __init__(self, outcome: str = "confirmed") -> None:
        self.outcome = outcome
        self.calls = 0

    def deliver(self, _effect: Any) -> dict[str, Any]:
        self.calls += 1
        if self.outcome == "rejected":
            raise MediaEffectDeliveryRejected("fixture notification rejected")
        if self.outcome == "indeterminate":
            raise MediaEffectDeliveryIndeterminate("fixture notification response lost")
        return {"ok": True}


class _ClaimingNotificationEffects:
    def __init__(self, ledger: PostgresMediaEffectLedger) -> None:
        self.ledger = ledger
        self.claimed: Any | None = None

    def execute(self, effect_id: UUID) -> Any:
        current = self.ledger.read_by_id(effect_id)
        assert current is not None
        if current.status.value == "pending":
            self.claimed = self.ledger.claim_by_id(
                effect_id,
                worker_id="competing-effect-worker",
                lease_seconds=60,
            )
            assert self.claimed is not None
            return self.claimed
        return current


class _ExecutorPreparation:
    def __init__(
        self,
        gateway: PersistentPendingBindingGateway,
        ledger: PostgresIngestionMonitorLedger,
        *,
        confirm_during_poll: bool = False,
        claim_during_poll: bool = False,
        raise_during_poll: bool = False,
        poll_item: Mapping[str, Any] | None = None,
    ) -> None:
        self.gateway = gateway
        self.ledger = ledger
        self.confirm_during_poll = confirm_during_poll
        self.claim_during_poll = claim_during_poll
        self.raise_during_poll = raise_during_poll
        self.poll_item = dict(poll_item) if poll_item is not None else None
        self.callback_claim: Any | None = None

    def prepare(self, item: Any) -> bool:
        self.gateway.enqueue_binding(
            item.path,
            mediainfo={"MediaSourceInfo": {"MediaStreams": []}},
        )
        return True

    def poll(self, binding: Any) -> Mapping[str, Any] | None:
        if self.confirm_during_poll or self.claim_during_poll:
            claimed = self.ledger.claim_binding_by_id(
                binding.id,
                worker_id="fixture-callback",
                lease_seconds=30,
            )
            assert claimed is not None and claimed.claim_token is not None
            self.callback_claim = claimed
            if self.confirm_during_poll:
                self.ledger.mark_binding_confirmed(
                    claimed.id,
                    claimed.claim_token,
                    emby_item_id="101",
                    result={"ok": True, "accepted": True, "item_id": "101"},
                )
        if self.raise_during_poll:
            raise RuntimeError("fixture Emby polling failure")
        return self.poll_item


def _physical_ingestion_executor(
    runtime: DatabaseRuntime,
    path: str,
    *,
    notification_outcome: str = "confirmed",
    confirm_during_poll: bool = False,
    claim_during_poll: bool = False,
    raise_during_poll: bool = False,
    poll_item: Mapping[str, Any] | None = None,
    binding_delivery: Any | None = None,
    media_effects: Any | None = None,
) -> tuple[
    IngestionMonitorExecutor,
    Any,
    PostgresIngestionMonitorLedger,
    _NotificationDelivery,
    _ExecutorPreparation,
]:
    ledger = _ledger(runtime)
    policy = MonitorPolicy.create(enabled=True, extensions=(".mkv",))
    item = ledger.record(
        policy.created(
            path,
            producer_key=f"physical-{notification_outcome}-{confirm_during_poll}",
            monitor_root=str(path.rsplit("/", 1)[0]),
            observed_at=datetime.now(UTC) - timedelta(seconds=10),
        )
    )[0].item
    ledger.mark_ready(item.id)
    pending = PersistentPendingBindingGateway(
        ledger=ledger,
        delivery=cast(Any, binding_delivery or _NoopBindingDelivery()),
        claimant_id="fixture-binding",
        lease_seconds=60,
    )
    preparation = _ExecutorPreparation(
        pending,
        ledger,
        confirm_during_poll=confirm_during_poll,
        claim_during_poll=claim_during_poll,
        raise_during_poll=raise_during_poll,
        poll_item=poll_item,
    )
    notification = _NotificationDelivery(notification_outcome)
    effects = media_effects or PersistentMediaEffectExecutor(
        PostgresMediaEffectLedger(runtime.unit_of_work),
        notification,
        worker_id="fixture-notification",
        lease_seconds=60,
    )
    settings = Settings(
        emby_server_url="http://emby.fixture.invalid",
        emby_api_key="fixture-token",
    )
    executor = IngestionMonitorExecutor(
        uow_factory=runtime.unit_of_work,
        ledger=ledger,
        settings=lambda: settings,
        preparation=preparation,
        pending_bindings=pending,
        media_effects=effects,
        fingerprints=_Fingerprints(),
        worker_id="fixture-ingestion",
    )
    return executor, item, ledger, notification, preparation


def _set_monitor_attempts(runtime: DatabaseRuntime, item_id: UUID, attempts: int) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE ingestion_monitor_items SET attempts = :attempts, available_at = NOW() WHERE id = :item_id"),
            {"attempts": attempts, "item_id": item_id},
        )


def _make_ingestion_retry_due(runtime: DatabaseRuntime, item_id: UUID, binding_id: UUID | None = None) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE ingestion_monitor_items SET available_at = NOW() WHERE id = :item_id"),
            {"item_id": item_id},
        )
        if binding_id is not None:
            connection.execute(
                text("UPDATE ingestion_pending_bindings SET available_at = NOW() WHERE id = :binding_id"),
                {"binding_id": binding_id},
            )


@pytest.mark.parametrize(
    ("notification_outcome", "terminal_status"),
    [
        ("rejected", BindingStatus.REJECTED),
        ("indeterminate", BindingStatus.INDETERMINATE),
    ],
)
def test_notification_failure_atomically_terminalizes_monitor_and_pending_binding(
    p6_runtime: DatabaseRuntime,
    tmp_path,
    notification_outcome: str,
    terminal_status: BindingStatus,
) -> None:
    path = tmp_path / f"{notification_outcome}.mkv"
    path.write_bytes(b"fixture")
    executor, item, ledger, notification, _preparation = _physical_ingestion_executor(
        p6_runtime,
        str(path),
        notification_outcome=notification_outcome,
    )

    completed = executor.execute(item.id)
    binding = ledger.binding_for_monitor_item(item.id)

    expected_monitor = (
        MonitorItemStatus.REJECTED if terminal_status is BindingStatus.REJECTED else MonitorItemStatus.INDETERMINATE
    )
    assert completed is not None and completed.status is expected_monitor
    assert binding is not None and binding.status is terminal_status
    assert binding.error == completed.error
    assert notification.calls == 1


def test_callback_confirmation_wins_even_when_worker_path_poll_returns_none(
    p6_runtime: DatabaseRuntime,
    tmp_path,
) -> None:
    path = tmp_path / "callback-wins.mkv"
    path.write_bytes(b"fixture")
    executor, item, ledger, notification, _preparation = _physical_ingestion_executor(
        p6_runtime,
        str(path),
        confirm_during_poll=True,
    )

    completed = executor.execute(item.id)
    binding = ledger.binding_for_monitor_item(item.id)

    assert completed is not None and completed.status is MonitorItemStatus.CONFIRMED
    assert binding is not None and binding.status is BindingStatus.CONFIRMED
    assert completed.result is not None
    assert completed.result["binding_id"] == str(binding.id)
    assert completed.result["emby_item_id"] == "101"
    assert notification.calls == 1


def test_poll_exception_callback_claim_race_cannot_split_terminal_states(
    p6_runtime: DatabaseRuntime,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "callback-claim-race.mkv"
    path.write_bytes(b"fixture")
    executor, item, ledger, _notification, preparation = _physical_ingestion_executor(
        p6_runtime,
        str(path),
        raise_during_poll=True,
    )
    _set_monitor_attempts(p6_runtime, item.id, 2)
    original_claim = ledger.claim_binding_by_id
    callback: dict[str, Any] = {}

    def race_claim(binding_id: UUID, *, worker_id: str, lease_seconds: float):
        if worker_id == "fixture-binding" and "claim" not in callback:
            callback["claim"] = original_claim(
                binding_id,
                worker_id="fixture-callback-race",
                lease_seconds=lease_seconds,
            )
        return original_claim(binding_id, worker_id=worker_id, lease_seconds=lease_seconds)

    monkeypatch.setattr(ledger, "claim_binding_by_id", race_claim)

    retrying = executor.execute(item.id)
    claimed_binding = callback.get("claim")

    assert retrying is not None and retrying.status is MonitorItemStatus.RETRYABLE
    assert retrying.attempts == 3
    assert claimed_binding is not None and claimed_binding.claim_token is not None
    assert ledger.read_binding(claimed_binding.id).status is BindingStatus.CLAIMED

    ledger.mark_binding_confirmed(
        claimed_binding.id,
        claimed_binding.claim_token,
        emby_item_id="101",
        result={"ok": True, "accepted": True, "item_id": "101"},
    )
    preparation.raise_during_poll = False
    _make_ingestion_retry_due(p6_runtime, item.id)

    completed = executor.execute(item.id)
    assert completed is not None and completed.status is MonitorItemStatus.CONFIRMED
    assert ledger.read_binding(claimed_binding.id).status is BindingStatus.CONFIRMED


def test_poll_exception_after_callback_confirmation_completes_the_monitor(
    p6_runtime: DatabaseRuntime,
    tmp_path,
) -> None:
    path = tmp_path / "callback-confirmed-before-poll-error.mkv"
    path.write_bytes(b"fixture")
    executor, item, ledger, _notification, _preparation = _physical_ingestion_executor(
        p6_runtime,
        str(path),
        confirm_during_poll=True,
        raise_during_poll=True,
    )
    _set_monitor_attempts(p6_runtime, item.id, 2)

    completed = executor.execute(item.id)
    binding = ledger.binding_for_monitor_item(item.id)

    assert completed is not None and completed.status is MonitorItemStatus.CONFIRMED
    assert binding is not None and binding.status is BindingStatus.CONFIRMED


def test_claimed_notification_effect_does_not_exhaust_monitor_retry_budget(
    p6_runtime: DatabaseRuntime,
    tmp_path,
) -> None:
    path = tmp_path / "notification-effect-claimed.mkv"
    path.write_bytes(b"fixture")
    effect_ledger = PostgresMediaEffectLedger(p6_runtime.unit_of_work)
    competing_effects = _ClaimingNotificationEffects(effect_ledger)
    executor, item, ledger, _notification, preparation = _physical_ingestion_executor(
        p6_runtime,
        str(path),
        media_effects=competing_effects,
    )
    _set_monitor_attempts(p6_runtime, item.id, 2)

    retrying = executor.execute(item.id)
    binding = ledger.binding_for_monitor_item(item.id)

    assert retrying is not None and retrying.status is MonitorItemStatus.RETRYABLE
    assert retrying.attempts == 3
    assert binding is not None and binding.status is BindingStatus.PENDING
    assert competing_effects.claimed is not None and competing_effects.claimed.claim_token is not None

    effect_ledger.mark_confirmed(
        competing_effects.claimed.effect_key,
        competing_effects.claimed.claim_token,
        result={"ok": True},
    )
    preparation.confirm_during_poll = True
    _make_ingestion_retry_due(p6_runtime, item.id)

    completed = executor.execute(item.id)
    assert completed is not None and completed.status is MonitorItemStatus.CONFIRMED
    assert ledger.read_binding(binding.id).status is BindingStatus.CONFIRMED


def test_binding_retry_budget_is_independent_from_monitor_attempts(
    p6_runtime: DatabaseRuntime,
    tmp_path,
) -> None:
    path = tmp_path / "binding-retry-budget.mkv"
    path.write_bytes(b"fixture")
    delivery = _FlakyBindingDelivery()
    executor, item, ledger, _notification, _preparation = _physical_ingestion_executor(
        p6_runtime,
        str(path),
        poll_item={"Id": "101", "Type": "Movie", "Path": str(path)},
        binding_delivery=delivery,
    )
    _set_monitor_attempts(p6_runtime, item.id, 2)

    retrying = executor.execute(item.id)
    binding = ledger.binding_for_monitor_item(item.id)

    assert retrying is not None and retrying.status is MonitorItemStatus.RETRYABLE
    assert retrying.attempts == 3
    assert binding is not None and binding.status is BindingStatus.RETRYABLE
    assert binding.attempts == 1

    _make_ingestion_retry_due(p6_runtime, item.id, binding.id)
    completed = executor.execute(item.id)
    confirmed_binding = ledger.read_binding(binding.id)

    assert completed is not None and completed.status is MonitorItemStatus.CONFIRMED
    assert confirmed_binding is not None and confirmed_binding.status is BindingStatus.CONFIRMED
    assert confirmed_binding.attempts == 2
    assert delivery.calls == 2


def test_known_asset_paths_use_native_exact_normalization(p6_runtime: DatabaseRuntime) -> None:
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (tmdb_id, item_type, asset_details_json)
                VALUES (
                    'p6-monitor-fixture-1',
                    'Movie',
                    '[{"path":"/Library/Movie.MKV/"},{"path":"C:\\\\Media\\\\Show.strm"}]'::jsonb
                )
                """
            )
        )

    assert _ledger(p6_runtime).known_asset_paths() == frozenset(
        {
            "/library/movie.mkv",
            "c:/media/show.strm",
        }
    )
    assert normalize_media_path("/Library/Movie.MKV/") == "/library/movie.mkv"
