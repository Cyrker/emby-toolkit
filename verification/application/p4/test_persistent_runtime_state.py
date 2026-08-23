from __future__ import annotations

import threading
from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from etk.compatibility import persistent_runtime_state as runtime_state
from etk.compatibility.persistent_runtime_state import (
    PendingLeaseActiveError,
    PersistentExpiringSet,
    PersistentRuntimeStateStore,
)
from etk.platform.jobs import InvalidJsonPayloadError, normalize_json_object


class MemoryRuntimeStateBackend:
    def __init__(self) -> None:
        self.now = datetime(2026, 8, 23, tzinfo=UTC)
        self.states: dict[str, dict[str, object]] = {}
        self._lock = threading.RLock()

    def mutate(self, setting_key, mutation):
        with self._lock:
            current = deepcopy(self.states.get(setting_key))
            next_state, result, should_write = mutation(current, self.now)
            if should_write:
                if next_state is None:
                    self.states.pop(setting_key, None)
                else:
                    self.states[setting_key] = normalize_json_object(
                        next_state,
                        field="fake runtime state",
                    )
            return deepcopy(result)

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def _mp_file(name: str, file_id: str, *, size: int = 100) -> dict[str, object]:
    return {
        "tmdb_id": "100",
        "media_type": "Series",
        "season_num": 1,
        "episode_num": 2,
        "name": name,
        "file_id": file_id,
        "parent_id": "parent-1",
        "size": size,
    }


def test_moviepilot_route_persists_deduplicated_batch_and_one_wake_claim(monkeypatch) -> None:
    from etk.compatibility import background
    from routes import webhook

    backend = MemoryRuntimeStateBackend()
    api_store = PersistentRuntimeStateStore(backend)
    monkeypatch.setattr(runtime_state, "_DEFAULT_STORE", api_store)
    monkeypatch.setattr(background, "persistent_background_gateway_installed", lambda: True)

    scheduled = []
    monkeypatch.setattr(
        webhook,
        "submit_persistent_background",
        lambda operation, **options: scheduled.append((operation, options)),
    )

    subtitle = _mp_file("S01E02.zh.srt", "subtitle-1")
    duplicate_subtitle = {**subtitle, "size": 110}
    video = _mp_file("S01E02.mkv", "video-1", size=1000)
    webhook._enqueue_mp_file(subtitle)
    webhook._enqueue_mp_file(duplicate_subtitle)
    webhook._enqueue_mp_file(video)

    assert [call[1]["delay"] for call in scheduled] == [7200, 7200, 5]
    assert all(call[0] is webhook._wake_moviepilot_batch for call in scheduled)
    assert all(len(call[1]["args"]) == 2 for call in scheduled)
    assert all(call[1]["max_attempts"] == webhook.PERSISTENT_COALESCING_MAX_ATTEMPTS for call in scheduled)
    assert all(call[1]["timeout_seconds"] == webhook.PERSISTENT_COALESCING_TIMEOUT_SECONDS for call in scheduled)
    assert all(call[1]["idempotency_scope"] == "coalesced-wake:moviepilot" for call in scheduled)

    processed = []
    monkeypatch.setattr(
        webhook,
        "_process_mp_files",
        lambda key, files, has_video=False: processed.append((key, files, has_video)),
    )

    # A different store instance sees the same state, as API/Worker do after restart.
    monkeypatch.setattr(runtime_state, "_DEFAULT_STORE", PersistentRuntimeStateStore(backend))
    assert scheduled[0][0](*scheduled[0][1]["args"]) is None  # early stale wake-up
    backend.advance(5)
    scheduled[-1][0](*scheduled[-1][1]["args"])
    scheduled[0][0](*scheduled[0][1]["args"])  # state was already uniquely finalized

    assert len(processed) == 1
    key, files, has_video = processed[0]
    assert key == ("100", "Series", 1, 2)
    assert has_video is True
    assert [item["file_id"] for item in files] == ["subtitle-1", "video-1"]
    assert files[0]["size"] == 110


def test_moviepilot_video_shortens_subtitle_deadline() -> None:
    backend = MemoryRuntimeStateBackend()
    store = PersistentRuntimeStateStore(backend)
    key = '["100","Series",1,2]'
    values = ["100", "Series", 1, 2]

    subtitle = store.merge_moviepilot_batch(
        key,
        values,
        _mp_file("S01E02.srt", "subtitle-1"),
        is_video=False,
    )
    backend.advance(30)
    video = store.merge_moviepilot_batch(
        key,
        values,
        _mp_file("S01E02.mkv", "video-1"),
        is_video=True,
    )

    assert datetime.fromisoformat(video.deadline_at.replace("Z", "+00:00")) < datetime.fromisoformat(
        subtitle.deadline_at.replace("Z", "+00:00")
    )
    assert video.delay_seconds == 5


def test_moviepilot_subtitle_arriving_during_video_processing_keeps_short_deadline() -> None:
    backend = MemoryRuntimeStateBackend()
    store = PersistentRuntimeStateStore(backend)
    key = '["100","Series",1,2]'
    video = store.merge_moviepilot_batch(
        key,
        ["100", "Series", 1, 2],
        _mp_file("S01E02.mkv", "video-1"),
        is_video=True,
    )
    backend.advance(5)
    claim = store.claim_pending(
        "moviepilot",
        key,
        video.version,
        lease_seconds=10,
    )
    assert claim is not None

    subtitle = store.merge_moviepilot_batch(
        key,
        ["100", "Series", 1, 2],
        _mp_file("S01E02.srt", "subtitle-1"),
        is_video=False,
    )

    assert subtitle.delay_seconds == 5
    assert store.finalize_pending("moviepilot", key, claim.owner_token)


def test_moviepilot_persistent_batch_surfaces_organizer_failure(monkeypatch) -> None:
    from routes import webhook

    class Organizer:
        forced_season = None

        def __init__(self, *_args) -> None:
            pass

        def get_target_cid(self, **_kwargs):
            return "target"

        def execute(self, *_args, **_kwargs):
            return False

    monkeypatch.setattr(webhook.P115Service, "get_client", lambda: object())
    monkeypatch.setattr(webhook, "SmartOrganizer", Organizer)
    monkeypatch.setattr(webhook, "_refresh_mp_file_info_from_115", lambda *_args: None)
    monkeypatch.setattr(webhook, "get_config", lambda: {})

    with pytest.raises(RuntimeError, match="未完全成功"):
        webhook._process_mp_files(
            ("100", "Series", 1, 2),
            [_mp_file("S01E02.mkv", "video-1")],
            True,
        )


def test_moviepilot_concurrent_mutations_keep_every_file() -> None:
    backend = MemoryRuntimeStateBackend()
    first_store = PersistentRuntimeStateStore(backend)
    second_store = PersistentRuntimeStateStore(backend)
    barrier = threading.Barrier(2)
    wakes = []

    def merge(store, file_info):
        barrier.wait(timeout=2)
        wakes.append(
            store.merge_moviepilot_batch(
                '["100","Series",1,2]',
                ["100", "Series", 1, 2],
                file_info,
                is_video=True,
            )
        )

    threads = [
        threading.Thread(
            target=merge,
            args=(first_store, _mp_file("S01E02.mkv", "video-1")),
            daemon=True,
        ),
        threading.Thread(
            target=merge,
            args=(second_store, _mp_file("S01E02.srt", "subtitle-1")),
            daemon=True,
        ),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(3)

    assert len(wakes) == 2
    backend.advance(5)
    claim = first_store.claim_pending(
        "moviepilot",
        '["100","Series",1,2]',
        max(wake.version for wake in wakes),
        lease_seconds=10,
    )
    assert claim is not None
    assert {item["file_id"] for item in claim.payload["files"]} == {"video-1", "subtitle-1"}
    assert first_store.finalize_pending("moviepilot", '["100","Series",1,2]', claim.owner_token)


def test_metadata_debounce_claims_latest_event_once_across_store_instances() -> None:
    backend = MemoryRuntimeStateBackend()
    worker_store = PersistentRuntimeStateStore(backend)
    api_store = PersistentRuntimeStateStore(backend)

    first = api_store.merge_metadata_update("item-1", "Old", delay_seconds=15)
    backend.advance(10)
    latest = api_store.merge_metadata_update("item-1", "Latest", delay_seconds=15)
    backend.advance(5)

    assert (
        worker_store.claim_pending(
            "emby-metadata-update",
            "item-1",
            first.version,
            lease_seconds=10,
        )
        is None
    )
    backend.advance(10)
    claim = worker_store.claim_pending(
        "emby-metadata-update",
        "item-1",
        latest.version,
        lease_seconds=10,
    )
    assert claim is not None
    assert claim.payload == {
        "item_id": "item-1",
        "item_name": "Latest",
    }
    assert worker_store.finalize_pending("emby-metadata-update", "item-1", claim.owner_token)
    assert (
        api_store.claim_pending(
            "emby-metadata-update",
            "item-1",
            latest.version,
            lease_seconds=10,
        )
        is None
    )


def test_active_episode_ids_are_atomically_unioned_under_concurrency() -> None:
    backend = MemoryRuntimeStateBackend()
    first_store = PersistentRuntimeStateStore(backend)
    second_store = PersistentRuntimeStateStore(backend)
    barrier = threading.Barrier(2)
    wakes = []

    def merge(store, name, episode_ids):
        barrier.wait(timeout=2)
        wakes.append(
            store.merge_active_emby_series(
                "series-1",
                name,
                episode_ids,
                delay_seconds=10,
            )
        )

    threads = [
        threading.Thread(target=merge, args=(first_store, "Series", ["e1", "e2"]), daemon=True),
        threading.Thread(target=merge, args=(second_store, "", ["e2", "e3"]), daemon=True),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(3)

    assert len(wakes) == 2
    backend.advance(10)
    claim = first_store.claim_pending(
        "active-emby-series-dispatch",
        "series-1",
        max(wake.version for wake in wakes),
        lease_seconds=10,
    )
    assert claim is not None
    assert claim.payload["episode_ids"] == ["e1", "e2", "e3"]
    assert claim.payload["series_name"] == "Series"
    assert first_store.finalize_pending(
        "active-emby-series-dispatch",
        "series-1",
        claim.owner_token,
    )


def test_processing_lease_owner_mismatch_and_expiry_reclaim() -> None:
    backend = MemoryRuntimeStateBackend()
    store = PersistentRuntimeStateStore(backend)
    wake = store.merge_metadata_update("item-1", "Latest", delay_seconds=1)
    backend.advance(1)

    first = store.claim_pending(
        "emby-metadata-update",
        "item-1",
        wake.version,
        lease_seconds=10,
    )
    assert first is not None
    assert not store.finalize_pending("emby-metadata-update", "item-1", "wrong-owner")
    assert not store.release_pending("emby-metadata-update", "item-1", "wrong-owner")
    with pytest.raises(PendingLeaseActiveError):
        store.claim_pending(
            "emby-metadata-update",
            "item-1",
            wake.version,
            lease_seconds=10,
        )

    backend.advance(11)
    reclaimed = store.claim_pending(
        "emby-metadata-update",
        "item-1",
        wake.version,
        lease_seconds=10,
    )
    assert reclaimed is not None
    assert reclaimed.owner_token != first.owner_token
    assert reclaimed.payload == first.payload
    assert not store.finalize_pending("emby-metadata-update", "item-1", first.owner_token)
    assert store.finalize_pending("emby-metadata-update", "item-1", reclaimed.owner_token)


def test_finalize_preserves_events_that_arrive_during_processing() -> None:
    backend = MemoryRuntimeStateBackend()
    store = PersistentRuntimeStateStore(backend)
    first_wake = store.merge_active_emby_series(
        "series-1",
        "Series",
        ["e1"],
        delay_seconds=1,
    )
    backend.advance(1)
    first_claim = store.claim_pending(
        "active-emby-series-dispatch",
        "series-1",
        first_wake.version,
        lease_seconds=10,
    )
    assert first_claim is not None

    second_wake = store.merge_active_emby_series(
        "series-1",
        "Series",
        ["e2"],
        delay_seconds=1,
    )
    assert store.finalize_pending(
        "active-emby-series-dispatch",
        "series-1",
        first_claim.owner_token,
    )
    backend.advance(1)
    second_claim = store.claim_pending(
        "active-emby-series-dispatch",
        "series-1",
        second_wake.version,
        lease_seconds=10,
    )
    assert second_claim is not None
    assert second_claim.payload["episode_ids"] == ["e2"]


def test_moviepilot_wake_requeues_payload_after_business_exception(monkeypatch) -> None:
    from routes import webhook

    backend = MemoryRuntimeStateBackend()
    store = PersistentRuntimeStateStore(backend)
    monkeypatch.setattr(runtime_state, "_DEFAULT_STORE", store)
    wake = store.merge_moviepilot_batch(
        '["100","Series",1,2]',
        ["100", "Series", 1, 2],
        _mp_file("S01E02.mkv", "video-1"),
        is_video=True,
        video_delay_seconds=1,
    )
    backend.advance(1)
    calls = []

    def process(*args):
        calls.append(args)
        if len(calls) == 1:
            raise RuntimeError("organizer failed")
        return True

    monkeypatch.setattr(webhook, "_process_mp_files", process)
    monkeypatch.setattr(webhook, "PERSISTENT_COALESCING_LEASE_SECONDS", 10)

    with pytest.raises(RuntimeError, match="organizer failed"):
        webhook._wake_moviepilot_batch(wake.batch_key, wake.version)
    assert webhook._wake_moviepilot_batch(wake.batch_key, wake.version) is True
    assert len(calls) == 2
    assert (
        store.claim_pending(
            "moviepilot",
            wake.batch_key,
            wake.version,
            lease_seconds=10,
        )
        is None
    )


def test_metadata_wake_requeues_until_task_submission_succeeds(monkeypatch) -> None:
    from routes import webhook

    backend = MemoryRuntimeStateBackend()
    store = PersistentRuntimeStateStore(backend)
    monkeypatch.setattr(runtime_state, "_DEFAULT_STORE", store)
    monkeypatch.setattr(webhook, "PERSISTENT_COALESCING_LEASE_SECONDS", 10)
    wake = store.merge_metadata_update("item-1", "Latest", delay_seconds=1)
    backend.advance(1)
    submitted = iter([False, True])
    monkeypatch.setattr(webhook.task_manager, "submit_task", lambda *_args, **_kwargs: next(submitted))

    with pytest.raises(RuntimeError, match="未持久分派"):
        webhook._wake_metadata_update(wake.batch_key, wake.version)
    assert webhook._wake_metadata_update(wake.batch_key, wake.version) is True
    assert (
        store.claim_pending(
            "emby-metadata-update",
            wake.batch_key,
            wake.version,
            lease_seconds=10,
        )
        is None
    )


def test_active_series_wake_requeues_until_dispatch_succeeds(monkeypatch) -> None:
    from routes import webhook

    backend = MemoryRuntimeStateBackend()
    store = PersistentRuntimeStateStore(backend)
    monkeypatch.setattr(runtime_state, "_DEFAULT_STORE", store)
    monkeypatch.setattr(webhook, "PERSISTENT_COALESCING_LEASE_SECONDS", 10)
    wake = store.merge_active_emby_series(
        "series-1",
        "Series",
        ["e1"],
        delay_seconds=1,
    )
    backend.advance(1)
    dispatched = iter([False, True])
    monkeypatch.setattr(
        webhook,
        "_dispatch_active_emby_series",
        lambda *_args, **_kwargs: next(dispatched),
    )

    with pytest.raises(RuntimeError, match="未持久分派"):
        webhook._wake_active_emby_series_dispatch(wake.batch_key, wake.version)
    assert webhook._wake_active_emby_series_dispatch(wake.batch_key, wake.version) is True
    assert (
        store.claim_pending(
            "active-emby-series-dispatch",
            wake.batch_key,
            wake.version,
            lease_seconds=10,
        )
        is None
    )


def test_ttl_marker_is_cross_instance_single_consumer_and_expires() -> None:
    backend = MemoryRuntimeStateBackend()
    worker_store = PersistentRuntimeStateStore(backend)
    api_store = PersistentRuntimeStateStore(backend)

    worker_store.put_ttl_marker("emby-user-policy-update", "user-1", 10)
    assert api_store.consume_ttl_marker("emby-user-policy-update", "user-1", 10)
    assert not worker_store.consume_ttl_marker("emby-user-policy-update", "user-1", 10)

    worker_store.put_ttl_marker("emby-user-policy-update", "user-2", 10)
    backend.advance(11)
    assert not api_store.consume_ttl_marker("emby-user-policy-update", "user-2", 10)

    worker_set = PersistentExpiringSet(
        "deleting-emby-collections",
        default_ttl_seconds=20,
        store=worker_store,
    )
    api_set = PersistentExpiringSet(
        "deleting-emby-collections",
        default_ttl_seconds=20,
        store=api_store,
    )
    worker_set.add_for("collection-1", 20)
    assert "collection-1" in api_set
    backend.advance(21)
    assert "collection-1" not in api_set


def test_reservation_is_cross_instance_owner_safe_releasable_and_expiring() -> None:
    backend = MemoryRuntimeStateBackend()
    api_store = PersistentRuntimeStateStore(backend)
    worker_store = PersistentRuntimeStateStore(backend)

    token = api_store.reserve_once("tmdb-missing-collection-cleanup", "100", 60)
    assert token
    assert worker_store.reserve_once("tmdb-missing-collection-cleanup", "100", 60) is None
    assert not worker_store.release_once("tmdb-missing-collection-cleanup", "100", "wrong-token")
    assert worker_store.release_once("tmdb-missing-collection-cleanup", "100", token)
    assert api_store.reserve_once("tmdb-missing-collection-cleanup", "100", 60)

    backend.advance(61)
    assert worker_store.reserve_once("tmdb-missing-collection-cleanup", "100", 60)


def test_delivery_ledger_deduplicates_success_and_releases_failed_claims() -> None:
    backend = MemoryRuntimeStateBackend()
    first_store = PersistentRuntimeStateStore(backend)
    second_store = PersistentRuntimeStateStore(backend)

    first_claim = first_store.claim_delivery("telegram-media-notification", "event-recipient", lease_seconds=30)
    assert first_claim is not None
    with pytest.raises(PendingLeaseActiveError):
        second_store.claim_delivery("telegram-media-notification", "event-recipient", lease_seconds=30)
    assert first_store.complete_delivery(
        "telegram-media-notification",
        "event-recipient",
        first_claim.owner_token,
        result={"transport": "message"},
    )
    assert second_store.claim_delivery("telegram-media-notification", "event-recipient", lease_seconds=30) is None

    retry_claim = first_store.claim_delivery("telegram-media-notification", "failed-recipient", lease_seconds=30)
    assert retry_claim is not None
    assert not second_store.release_delivery(
        "telegram-media-notification",
        "failed-recipient",
        "wrong-owner",
    )
    assert first_store.release_delivery(
        "telegram-media-notification",
        "failed-recipient",
        retry_claim.owner_token,
    )
    assert second_store.claim_delivery("telegram-media-notification", "failed-recipient", lease_seconds=30)


def test_delivery_ledger_reclaims_expired_inflight_owner() -> None:
    backend = MemoryRuntimeStateBackend()
    store = PersistentRuntimeStateStore(backend)
    first = store.claim_delivery("telegram-media-notification", "event-recipient", lease_seconds=10)
    assert first is not None

    backend.advance(11)
    reclaimed = store.claim_delivery("telegram-media-notification", "event-recipient", lease_seconds=10)

    assert reclaimed is not None
    assert reclaimed.owner_token != first.owner_token
    assert not store.complete_delivery(
        "telegram-media-notification",
        "event-recipient",
        first.owner_token,
    )
    assert store.complete_delivery(
        "telegram-media-notification",
        "event-recipient",
        reclaimed.owner_token,
    )


def test_runtime_state_rejects_non_json_moviepilot_payload() -> None:
    store = PersistentRuntimeStateStore(MemoryRuntimeStateBackend())
    with pytest.raises(InvalidJsonPayloadError):
        store.merge_moviepilot_batch(
            "batch",
            ["100", "Series", 1, 2],
            {"name": "episode.mkv", "callback": lambda: None},
            is_video=True,
        )


def test_tmdb_cleanup_submission_failure_releases_reservation(monkeypatch) -> None:
    from routes import tmdb_collections

    released = []
    monkeypatch.setattr(tmdb_collections, "reserve_once", lambda *args: "reservation-token")
    monkeypatch.setattr(
        tmdb_collections,
        "release_once",
        lambda *args: released.append(args) or True,
    )
    monkeypatch.setattr(
        tmdb_collections,
        "spawn",
        lambda *args: (_ for _ in ()).throw(RuntimeError("submission failed")),
    )

    with pytest.raises(RuntimeError, match="submission failed"):
        tmdb_collections._schedule_missing_collection_cleanup("100", None)

    assert released == [
        ("tmdb-missing-collection-cleanup", "100", "reservation-token"),
    ]
