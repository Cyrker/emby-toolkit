from __future__ import annotations

import threading
import time

import pytest
from sqlalchemy import text

from etk.compatibility.persistent_runtime_state import (
    PendingLeaseActiveError,
    PersistentRuntimeStateStore,
    PostgresRuntimeStateBackend,
)
from etk.compatibility.tg_resource_effects import TelegramResourceEffectStore
from etk.platform.db import DatabaseRuntime


def _stores(p4_runtime: DatabaseRuntime) -> tuple[PersistentRuntimeStateStore, PersistentRuntimeStateStore]:
    assert p4_runtime.raw_pool is not None
    backend = PostgresRuntimeStateBackend(p4_runtime.raw_pool.connection)
    with p4_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM app_settings WHERE setting_key LIKE 'runtime.%'"))
    return PersistentRuntimeStateStore(backend), PersistentRuntimeStateStore(backend)


def test_postgres_runtime_state_concurrent_merge_and_unique_finalize(p4_runtime: DatabaseRuntime) -> None:
    first_store, second_store = _stores(p4_runtime)
    barrier = threading.Barrier(2)
    wakes = []
    errors: list[BaseException] = []

    def merge(store: PersistentRuntimeStateStore, episode_ids: list[str]) -> None:
        try:
            barrier.wait(timeout=2)
            wakes.append(
                store.merge_active_emby_series(
                    "integration-series",
                    "Integration Series",
                    episode_ids,
                    delay_seconds=0.05,
                )
            )
        except BaseException as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=merge, args=(first_store, ["e1", "e2"]), daemon=True),
        threading.Thread(target=merge, args=(second_store, ["e2", "e3"]), daemon=True),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(3)

    assert not errors
    assert len(wakes) == 2
    time.sleep(0.1)
    latest_version = max(wake.version for wake in wakes)
    claim = second_store.claim_pending(
        "active-emby-series-dispatch",
        "integration-series",
        latest_version,
        lease_seconds=1,
    )
    assert claim is not None
    assert claim.payload["episode_ids"] == ["e1", "e2", "e3"]
    with pytest.raises(PendingLeaseActiveError):
        first_store.claim_pending(
            "active-emby-series-dispatch",
            "integration-series",
            latest_version,
            lease_seconds=1,
        )
    assert not first_store.finalize_pending(
        "active-emby-series-dispatch",
        "integration-series",
        "wrong-owner",
    )
    assert second_store.finalize_pending(
        "active-emby-series-dispatch",
        "integration-series",
        claim.owner_token,
    )
    assert (
        first_store.claim_pending(
            "active-emby-series-dispatch",
            "integration-series",
            latest_version,
            lease_seconds=1,
        )
        is None
    )


def test_postgres_processing_lease_expiry_allows_owner_safe_reclaim(p4_runtime: DatabaseRuntime) -> None:
    first_store, second_store = _stores(p4_runtime)
    wake = first_store.merge_metadata_update(
        "integration-item",
        "Integration Item",
        delay_seconds=0.01,
    )
    time.sleep(0.03)
    first_claim = first_store.claim_pending(
        "emby-metadata-update",
        "integration-item",
        wake.version,
        lease_seconds=0.05,
    )
    assert first_claim is not None
    assert not second_store.release_pending(
        "emby-metadata-update",
        "integration-item",
        "wrong-owner",
    )

    time.sleep(0.08)
    reclaimed = second_store.claim_pending(
        "emby-metadata-update",
        "integration-item",
        wake.version,
        lease_seconds=0.05,
    )
    assert reclaimed is not None
    assert reclaimed.owner_token != first_claim.owner_token
    assert reclaimed.payload == first_claim.payload
    assert not first_store.finalize_pending(
        "emby-metadata-update",
        "integration-item",
        first_claim.owner_token,
    )
    assert second_store.finalize_pending(
        "emby-metadata-update",
        "integration-item",
        reclaimed.owner_token,
    )


def test_postgres_delivery_claim_is_owner_safe_and_deduplicates_completion(p4_runtime: DatabaseRuntime) -> None:
    first_store, second_store = _stores(p4_runtime)
    claim = first_store.claim_delivery(
        "telegram-media-notification",
        "integration-event-recipient",
        lease_seconds=1,
    )
    assert claim is not None

    with pytest.raises(PendingLeaseActiveError):
        second_store.claim_delivery(
            "telegram-media-notification",
            "integration-event-recipient",
            lease_seconds=1,
        )
    assert not second_store.complete_delivery(
        "telegram-media-notification",
        "integration-event-recipient",
        "wrong-owner",
    )
    assert first_store.complete_delivery(
        "telegram-media-notification",
        "integration-event-recipient",
        claim.owner_token,
        result={"transport": "message"},
    )
    assert (
        second_store.claim_delivery(
            "telegram-media-notification",
            "integration-event-recipient",
            lease_seconds=1,
        )
        is None
    )


def test_postgres_telegram_resource_effect_survives_store_recreation(p4_runtime: DatabaseRuntime) -> None:
    assert p4_runtime.raw_pool is not None
    backend = PostgresRuntimeStateBackend(p4_runtime.raw_pool.connection)
    first = TelegramResourceEffectStore(backend)
    second = TelegramResourceEffectStore(backend)
    key = "a" * 64

    prepared, created = first.prepare(
        key,
        "p115_offline_add",
        {"source_hash": "b" * 64, "target_cid": "target-1"},
    )
    loaded = second.read(key)

    assert created is True
    assert loaded == prepared
    assert second.mark_succeeded(key, "p115_offline_add").status == "succeeded"
    completed = first.read(key)
    assert completed is not None
    assert completed.status == "succeeded"
