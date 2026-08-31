from __future__ import annotations

import json
import time
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from threading import Barrier, Event, Lock
from types import SimpleNamespace
from typing import Any, cast

import httpx
from pydantic import SecretStr
from sqlalchemy import text

from etk.domains.collection.refresh import (
    NativeCollectionReference,
    SqlAlchemyNativeCollectionRefreshRepository,
)
from etk.domains.media.auto_tagging import AUTO_TAGGING_EFFECT_TYPE
from etk.domains.media.auto_tagging_adapter import (
    HttpxAutoTaggingAdapter,
    PersistentAutoTaggingEffects,
    PostgresAutoTaggingItemLock,
)
from etk.domains.media.effect_execution import PersistentMediaEffectExecutor
from etk.domains.media.effects import MediaEffectProjection, MediaEffectStatus, PostgresMediaEffectLedger
from etk.domains.media.embeddings import SqlAlchemyMediaEmbeddingRepository
from etk.domains.media.studio_images_adapter import PersistentStudioImageEffects
from etk.platform import Settings
from etk.platform.db import DatabaseRuntime


class ReadOnlyEffectExecutor:
    def __init__(self, ledger: PostgresMediaEffectLedger) -> None:
        self._ledger = ledger

    def execute(self, effect_id: object) -> MediaEffectProjection:
        effect = self._ledger.read_by_id(effect_id)  # type: ignore[arg-type]
        assert effect is not None
        return effect

    def reserve_and_execute(self, **values: Any) -> MediaEffectProjection:
        return self._ledger.reserve(**values).effect


def test_pg18_embedding_repository_fences_changed_overview(p8_runtime: DatabaseRuntime) -> None:
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, overview, in_library, subscription_status
                ) VALUES
                    ('101', 'Movie', 'One', 'overview one', TRUE, 'NONE'),
                    ('102', 'Series', 'Two', 'overview two', TRUE, 'NONE'),
                    ('103', 'Movie', 'Offline', 'offline', FALSE, 'NONE')
                """
            )
        )
    repository = SqlAlchemyMediaEmbeddingRepository(p8_runtime.unit_of_work)

    candidates = repository.list_pending(after=None, limit=50)

    assert repository.count_pending() == 2
    assert [(value.tmdb_id, value.item_type) for value in candidates] == [
        ("101", "Movie"),
        ("102", "Series"),
    ]
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE media_metadata SET overview = 'changed' WHERE tmdb_id = '101' AND item_type = 'Movie'")
        )
    assert repository.store(candidates[0], [0.25, 0.75]) is False
    assert repository.store(candidates[1], [0.5, 0.5]) is True
    with p8_runtime.engine.connect() as connection:
        values = connection.execute(
            text(
                """
                SELECT tmdb_id, overview_embedding
                FROM media_metadata
                WHERE tmdb_id IN ('101', '102')
                ORDER BY tmdb_id
                """
            )
        ).all()
    assert values == [("101", None), ("102", [0.5, 0.5])]


def test_pg18_collection_refresh_publishes_atomically_and_preserves_duplicate_hard_ignore(
    p8_runtime: DatabaseRuntime,
) -> None:
    source = '[{"type":"native_collection","name":"Fixture Collection"}]'
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, release_date, in_library,
                    subscription_status, subscription_sources_json, ignore_reason
                ) VALUES
                    ('201', 'Movie', 'Hard ignored', '2025-01-01', FALSE,
                     'IGNORED', CAST(:source AS jsonb), '手动忽略'),
                    ('202', 'Movie', 'Soft ignored', '2025-01-01', FALSE,
                     'IGNORED', '[]'::jsonb, '临时忽略'),
                    ('204', 'Movie', 'Future hard ignored', '2027-01-01', FALSE,
                     'IGNORED', '[]'::jsonb, '手动忽略')
                """
            ),
            {"source": source},
        )
    repository = SqlAlchemyNativeCollectionRefreshRepository(p8_runtime.unit_of_work)
    reference = NativeCollectionReference("emby-box-1", "999", "Fixture Collection", "library-1")

    result = repository.publish(
        reference,
        {
            "id": 999,
            "name": "Fixture Collection",
            "poster_path": "/collection.jpg",
            "parts": [
                {
                    "id": 201,
                    "title": "Hard ignored",
                    "release_date": "2025-01-01",
                    "poster_path": "/201.jpg",
                },
                {
                    "id": 202,
                    "title": "Soft ignored",
                    "release_date": "2025-01-01",
                    "poster_path": "/202.jpg",
                },
                {
                    "id": 203,
                    "title": "Future",
                    "release_date": "2027-01-01",
                    "poster_path": "/203.jpg",
                },
                {
                    "id": 204,
                    "title": "Future hard ignored",
                    "release_date": "2027-01-01",
                    "poster_path": "/204.jpg",
                },
            ],
        },
        auto_subscribe=True,
        today=date(2026, 9, 1),
    )

    assert result.movie_count == 4
    assert result.subscription_candidates == 2
    with p8_runtime.engine.connect() as connection:
        rows = (
            connection.execute(
                text(
                    """
                    SELECT tmdb_id, subscription_status, subscription_sources_json, ignore_reason
                    FROM media_metadata
                    WHERE tmdb_id IN ('201', '202', '203', '204')
                    ORDER BY tmdb_id
                    """
                )
            )
            .mappings()
            .all()
        )
        collection = (
            connection.execute(
                text(
                    """
                    SELECT emby_collection_id, is_active, all_tmdb_ids_json
                    FROM collections_info
                    WHERE tmdb_collection_id = '999'
                    """
                )
            )
            .mappings()
            .one()
        )
    assert [(row["tmdb_id"], row["subscription_status"]) for row in rows] == [
        ("201", "IGNORED"),
        ("202", "WANTED"),
        ("203", "PENDING_RELEASE"),
        ("204", "IGNORED"),
    ]
    assert rows[0]["ignore_reason"] == "手动忽略"
    assert rows[1]["subscription_sources_json"] == [{"type": "native_collection", "name": "Fixture Collection"}]
    assert rows[3]["ignore_reason"] == "手动忽略"
    assert collection == {
        "emby_collection_id": "emby-box-1",
        "is_active": True,
        "all_tmdb_ids_json": ["201", "202", "203", "204"],
    }


def test_pg18_collection_refresh_deactivates_active_rows_without_emby_identity(
    p8_runtime: DatabaseRuntime,
) -> None:
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO collections_info (
                    tmdb_collection_id, emby_collection_id, name, is_active
                ) VALUES
                    ('ghost-null', NULL, 'Ghost', TRUE),
                    ('stale', 'emby-stale', 'Stale', TRUE),
                    ('current', 'emby-current', 'Current', TRUE)
                """
            )
        )

    changed = SqlAlchemyNativeCollectionRefreshRepository(p8_runtime.unit_of_work).reconcile_active(["emby-current"])

    assert changed == 2
    with p8_runtime.engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT tmdb_collection_id, emby_collection_id, is_active
                FROM collections_info
                ORDER BY tmdb_collection_id
                """
            )
        ).all()
    assert rows == [
        ("current", "emby-current", True),
        ("ghost-null", None, False),
        ("stale", None, False),
    ]


def test_pg18_auto_tagging_and_studio_effects_reuse_stable_reservations(
    p8_runtime: DatabaseRuntime,
) -> None:
    ledger = PostgresMediaEffectLedger(p8_runtime.unit_of_work)
    executor = cast(PersistentMediaEffectExecutor, ReadOnlyEffectExecutor(ledger))
    auto_tagging = PersistentAutoTaggingEffects(executor)
    studio_images = PersistentStudioImageEffects(executor)

    first_tag = auto_tagging.apply(
        effect_key="auto-tagging:job-1:item:item-1",
        item_id="item-1",
        operation="add",
        tags=["Fixture"],
    )
    reused_tag = auto_tagging.apply(
        effect_key="auto-tagging:job-1:item:item-1",
        item_id="item-1",
        operation="add",
        tags=["Fixture"],
    )
    first_image = studio_images.reconcile(
        effect_key="studio-images:job-1:studio:studio-1",
        item_id="studio-1",
        logo_path="/fixture.png",
        logo_source="company:1",
    )
    reused_image = studio_images.reconcile(
        effect_key="studio-images:job-1:studio:studio-1",
        item_id="studio-1",
        logo_path="/fixture.png",
        logo_source="network:1",
    )

    assert reused_tag.id == first_tag.id
    assert reused_image.id == first_image.id
    assert "logo_source" not in first_image.payload
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM media_effects")) == 2
        assert connection.scalar(text("SELECT COUNT(*) FROM outbox_events WHERE topic = 'media.effect.requested'")) == 2


def test_pg18_inline_media_effect_claim_precedes_outbox_execution(p8_runtime: DatabaseRuntime) -> None:
    entered = Event()
    release = Event()

    class BlockingDelivery:
        effect_types = frozenset({AUTO_TAGGING_EFFECT_TYPE})
        calls = 0

        def deliver(self, effect: MediaEffectProjection) -> Mapping[str, Any]:
            assert effect.effect_type == AUTO_TAGGING_EFFECT_TYPE
            self.calls += 1
            entered.set()
            assert release.wait(5)
            return {"readback": "confirmed"}

    ledger = PostgresMediaEffectLedger(p8_runtime.unit_of_work)
    delivery = BlockingDelivery()
    inline = PersistentMediaEffectExecutor(ledger, delivery, worker_id="inline-owner", lease_seconds=30)
    outbox = PersistentMediaEffectExecutor(ledger, delivery, worker_id="outbox-owner", lease_seconds=30)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            inline.reserve_and_execute,
            effect_key="auto-tagging:job-inline:item:item-1",
            effect_type=AUTO_TAGGING_EFFECT_TYPE,
            aggregate_type="emby_item",
            aggregate_id="item-1",
            payload={"item_id": "item-1", "operation": "add", "tags": ["Fixture"]},
        )
        assert entered.wait(5)
        with p8_runtime.engine.connect() as connection:
            effect_id = connection.scalar(
                text("SELECT id FROM media_effects WHERE effect_key = 'auto-tagging:job-inline:item:item-1'")
            )
        competing = outbox.execute(effect_id)
        assert competing.status is MediaEffectStatus.CLAIMED
        assert delivery.calls == 1
        release.set()
        terminal = future.result(timeout=5)

    assert terminal.status is MediaEffectStatus.CONFIRMED
    assert delivery.calls == 1
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text(
                    """
                SELECT status
                FROM outbox_events
                WHERE topic = 'media.effect.requested'
                  AND aggregate_id = :effect_id
                """
                ),
                {"effect_id": str(effect_id)},
            )
            == "processed"
        )


def test_pg18_auto_tagging_serializes_same_item_read_modify_write(
    p8_runtime: DatabaseRuntime,
) -> None:
    state_lock = Lock()
    start = Barrier(3)
    state: dict[str, list[str]] = {"tags": []}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            with state_lock:
                current = list(state["tags"])
            if "Fields" not in request.url.params:
                time.sleep(0.05)
            return httpx.Response(200, json={"Id": "item-1", "Tags": current, "LockedFields": []})
        if request.method == "POST":
            payload = json.loads(request.read())
            with state_lock:
                state["tags"] = list(payload["Tags"])
            return httpx.Response(204)
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    settings = Settings(
        emby_server_url="https://emby.example",
        emby_user_id="user-fixture",
        emby_api_key=SecretStr("emby-token"),
        emby_api_timeout=5,
    )
    item_lock = PostgresAutoTaggingItemLock(p8_runtime.engine)

    def apply(tag: str) -> Mapping[str, object]:
        adapter = HttpxAutoTaggingAdapter(
            lambda: settings,
            client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
            item_lock=item_lock,
        )
        start.wait()
        return adapter.deliver(
            cast(
                MediaEffectProjection,
                SimpleNamespace(payload={"item_id": "item-1", "operation": "add", "tags": [tag]}),
            )
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(apply, tag) for tag in ("A", "B")]
        start.wait()
        results = [future.result(timeout=5) for future in futures]

    assert all(result["readback"] == "confirmed" for result in results)
    assert set(state["tags"]) == {"A", "B"}
