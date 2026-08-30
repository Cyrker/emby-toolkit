from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

from sqlalchemy import text

from etk.processes.proxy import PostgresPlaybackLeaseStore

NOW = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)


def test_postgres_proxy_leases_enforce_user_concurrency_and_session_replay(p9_runtime) -> None:
    store = PostgresPlaybackLeaseStore(p9_runtime.unit_of_work)

    def acquire(index: int) -> bool:
        lease = store.acquire(
            "user-1",
            f"item-{index}",
            play_session_id=f"session-{index}",
            max_streams=2,
            now=NOW,
        )
        return lease is not None

    with ThreadPoolExecutor(max_workers=12) as executor:
        results = list(executor.map(acquire, range(12)))

    assert sum(results) == 2
    with p9_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM proxy_playback_leases WHERE user_id = 'user-1'")) == 2

    replay = store.acquire(
        "user-1",
        "item-0",
        play_session_id="session-0",
        max_streams=2,
        now=NOW + timedelta(seconds=5),
    )
    assert replay is not None
    with p9_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM proxy_playback_leases WHERE user_id = 'user-1'")) == 2


def test_postgres_proxy_leases_refresh_release_and_expiry_reclaim(p9_runtime) -> None:
    store = PostgresPlaybackLeaseStore(p9_runtime.unit_of_work)
    lease = store.acquire("user-2", "item-1", play_session_id="session-1", ttl_seconds=10, now=NOW)
    assert lease is not None
    assert store.refresh(
        user_id="user-2",
        item_id="item-1",
        play_session_id="session-1",
        ttl_seconds=20,
        now=NOW + timedelta(seconds=1),
    )
    assert store.reclaim_expired(now=NOW + timedelta(seconds=1)) == 0
    assert store.release(user_id="user-2", item_id="item-1", play_session_id="session-1") == 1
    assert store.release(user_id="user-2", item_id="item-1", play_session_id="session-1") == 0

    expired = store.acquire("user-2", "item-2", play_session_id="session-2", ttl_seconds=1, now=NOW)
    assert expired is not None
    assert store.reclaim_expired(now=NOW + timedelta(seconds=2)) == 1


def test_postgres_proxy_lease_refresh_matches_device_identity(p9_runtime) -> None:
    store = PostgresPlaybackLeaseStore(p9_runtime.unit_of_work)
    lease = store.acquire(
        "user-3",
        "item-1",
        device_id="device-1",
        client_signature="Fixture Client",
        ttl_seconds=10,
        now=NOW,
    )
    assert lease is not None
    assert store.refresh(
        user_id="user-3",
        item_id="item-1",
        device_id="device-1",
        client_signature="Fixture Client",
        ttl_seconds=20,
        now=NOW + timedelta(seconds=1),
    )
