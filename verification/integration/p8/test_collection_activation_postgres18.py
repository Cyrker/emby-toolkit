from __future__ import annotations

import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text

from etk.domains.collection.activation import (
    CollectionActivationConflictError,
    PostgresCollectionActivationRepository,
)
from etk.domains.collection.operation_execution import CollectionOperationExecutor
from etk.domains.collection.operation_repository import PostgresCollectionOperationRepository
from etk.domains.collection.operations import CollectionOperationStatus
from etk.domains.collection.refresh import NativeCollectionReference, SqlAlchemyNativeCollectionRefreshRepository

_NOW = datetime(2026, 9, 1, tzinfo=UTC)
_REPO_ROOT = Path(__file__).resolve().parents[3]


def _insert_collection(
    runtime: Any,
    tmdb_id: str,
    *,
    emby_id: str | None = None,
    active: bool = False,
    name: str = "Fixture Collection",
) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO collections_info (
                    tmdb_collection_id, emby_collection_id, name,
                    metadata_schema_version, is_active, all_tmdb_ids_json
                ) VALUES (
                    :tmdb_id, :emby_id, :name, 1, :active, '[]'::jsonb
                )
                """
            ),
            {
                "tmdb_id": tmdb_id,
                "emby_id": emby_id,
                "name": name,
                "active": active,
            },
        )


def test_collection_activation_reserves_binding_and_outbox_atomically_with_replay_and_conflicts(
    p8_runtime: Any,
) -> None:
    _insert_collection(p8_runtime, "25")
    _insert_collection(p8_runtime, "26", emby_id="emby-26", active=True)
    operations = PostgresCollectionOperationRepository(p8_runtime.unit_of_work)
    repository = PostgresCollectionActivationRepository(p8_runtime.unit_of_work, operations)

    first = repository.reserve(
        tmdb_collection_id="25",
        emby_collection_id="emby-25",
        name="Activated Fixture",
    )
    replay = repository.reserve(
        tmdb_collection_id="25",
        emby_collection_id="emby-25",
        name="Activated Fixture",
    )

    assert first is not None and replay is not None
    assert first.operation_created is True
    assert first.changed is True
    assert replay.operation.id == first.operation.id
    assert replay.operation_created is False
    assert replay.changed is False
    assert replay.already_active is True
    with p8_runtime.engine.connect() as connection:
        binding = connection.execute(
            text(
                """
                SELECT emby_collection_id, name, is_active
                FROM collections_info
                WHERE tmdb_collection_id = '25'
                """
            )
        ).one()
        operation = connection.execute(
            text("SELECT kind, status FROM collection_operations WHERE id = :id"),
            {"id": first.operation.id},
        ).one()
        outbox = connection.execute(
            text(
                """
                SELECT status
                FROM outbox_events
                WHERE aggregate_type = 'collection-operation'
                  AND aggregate_id = :operation_id
                """
            ),
            {"operation_id": str(first.operation.id)},
        ).one()
    assert binding == ("emby-25", "Activated Fixture", True)
    assert operation == ("collection.activation.refresh", "ready")
    assert outbox == ("pending",)

    with pytest.raises(CollectionActivationConflictError):
        repository.reserve(
            tmdb_collection_id="25",
            emby_collection_id="emby-replacement",
            name="Activated Fixture",
        )
    with pytest.raises(CollectionActivationConflictError):
        repository.reserve(
            tmdb_collection_id="25",
            emby_collection_id="emby-26",
            name="Activated Fixture",
        )
    assert (
        repository.reserve(
            tmdb_collection_id="999",
            emby_collection_id="emby-999",
            name="Missing",
        )
        is None
    )


def test_collection_activation_worker_atomically_publishes_movies_hard_ignore_and_terminal_outbox(
    p8_runtime: Any,
) -> None:
    _insert_collection(p8_runtime, "25")
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                VALUES ('native_collections_config', CAST(:value AS jsonb), NOW())
                """
            ),
            {"value": '{"auto_sub_enabled":true}'},
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, release_date, in_library,
                    subscription_status, subscription_sources_json, ignore_reason
                ) VALUES (
                    '2503', 'Movie', 'Hard ignored future', DATE '2027-01-03', FALSE,
                    'IGNORED', '[]'::jsonb, '手动忽略'
                )
                """
            )
        )
    operations = PostgresCollectionOperationRepository(p8_runtime.unit_of_work)
    activation = PostgresCollectionActivationRepository(p8_runtime.unit_of_work, operations)
    reserved = activation.reserve(
        tmdb_collection_id="25",
        emby_collection_id="emby-25",
        name="Activated Fixture",
    )
    assert reserved is not None

    class Provider:
        calls = 0

        def collection_details(self, tmdb_collection_id: str) -> dict[str, Any]:
            assert tmdb_collection_id == "25"
            self.calls += 1
            return {
                "id": 25,
                "name": "Provider Fixture",
                "overview": "Fixture overview",
                "poster_path": "/collection.jpg",
                "backdrop_path": "/collection-backdrop.jpg",
                "parts": [
                    {
                        "id": 2501,
                        "title": "Released",
                        "release_date": "2026-01-01",
                        "poster_path": "/2501.jpg",
                    },
                    {
                        "id": 2502,
                        "title": "Future",
                        "release_date": "2027-01-02",
                        "poster_path": "/2502.jpg",
                    },
                    {
                        "id": 2503,
                        "title": "Hard ignored future",
                        "release_date": "2027-01-03",
                        "poster_path": "/2503.jpg",
                    },
                ],
            }

    provider = Provider()
    executor = CollectionOperationExecutor(
        operations,
        worker_id="fixture-activation-worker",
        lease_seconds=90,
        activation_provider=provider,
        activation_completion=activation,
        clock=lambda: _NOW,
    )

    terminal = executor.execute(reserved.operation.id)

    assert terminal is not None and terminal.status is CollectionOperationStatus.CONFIRMED
    assert terminal.result == {
        "tmdb_collection_id": "25",
        "emby_collection_id": "emby-25",
        "movie_count": 3,
        "subscription_candidates": 2,
    }
    assert provider.calls == 1
    with p8_runtime.engine.connect() as connection:
        rows = (
            connection.execute(
                text(
                    """
                SELECT tmdb_id, subscription_status, subscription_sources_json, ignore_reason
                FROM media_metadata
                WHERE tmdb_id IN ('2501','2502','2503') AND item_type = 'Movie'
                ORDER BY tmdb_id
                """
                )
            )
            .mappings()
            .all()
        )
        collection = connection.execute(
            text(
                """
                SELECT emby_collection_id, is_active, all_tmdb_ids_json
                FROM collections_info
                WHERE tmdb_collection_id = '25'
                """
            )
        ).one()
        outbox_status = connection.scalar(
            text(
                """
                SELECT status
                FROM outbox_events
                WHERE aggregate_type = 'collection-operation'
                  AND aggregate_id = :operation_id
                """
            ),
            {"operation_id": str(reserved.operation.id)},
        )
    assert [(row["tmdb_id"], row["subscription_status"]) for row in rows] == [
        ("2501", "WANTED"),
        ("2502", "PENDING_RELEASE"),
        ("2503", "IGNORED"),
    ]
    assert rows[0]["subscription_sources_json"] == [{"type": "native_collection", "name": "Activated Fixture"}]
    assert rows[1]["subscription_sources_json"] == [{"type": "native_collection", "name": "Activated Fixture"}]
    assert rows[2]["subscription_sources_json"] == []
    assert rows[2]["ignore_reason"] == "手动忽略"
    assert collection == ("emby-25", True, ["2501", "2502", "2503"])
    assert outbox_status == "processed"


def test_collection_activation_recovers_committed_intent_after_worker_claim_loss(p8_runtime: Any) -> None:
    _insert_collection(p8_runtime, "25")
    operations = PostgresCollectionOperationRepository(p8_runtime.unit_of_work)
    activation = PostgresCollectionActivationRepository(p8_runtime.unit_of_work, operations)
    reserved = activation.reserve(
        tmdb_collection_id="25",
        emby_collection_id="emby-25",
        name="Activated Fixture",
    )
    assert reserved is not None
    claimed = operations.claim_by_id(
        reserved.operation.id,
        worker_id="crashed-worker",
        lease_seconds=0.001,
    )
    assert claimed is not None
    time.sleep(0.01)
    assert operations.expire_stale_claims(retry_delay_seconds=0) == 1

    class Provider:
        def collection_details(self, tmdb_collection_id: str) -> dict[str, Any]:
            assert tmdb_collection_id == "25"
            return {
                "id": 25,
                "parts": [
                    {
                        "id": 2501,
                        "title": "Recovered",
                        "release_date": "2026-01-01",
                        "poster_path": "/2501.jpg",
                    }
                ],
            }

    terminal = CollectionOperationExecutor(
        operations,
        worker_id="replacement-worker",
        lease_seconds=90,
        activation_provider=Provider(),
        activation_completion=activation,
        clock=lambda: _NOW,
    ).execute(reserved.operation.id)

    assert terminal is not None and terminal.status is CollectionOperationStatus.CONFIRMED
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT COUNT(*) FROM collection_operations WHERE id = :id"),
                {"id": reserved.operation.id},
            )
            == 1
        )
        assert (
            connection.scalar(
                text(
                    """
                SELECT COUNT(*)
                FROM outbox_events
                WHERE aggregate_type = 'collection-operation'
                  AND aggregate_id = :operation_id
                """
                ),
                {"operation_id": str(reserved.operation.id)},
            )
            == 1
        )


def test_collection_activation_and_full_refresh_share_the_same_advisory_lock(p8_runtime: Any) -> None:
    _insert_collection(p8_runtime, "25")
    real_operations = PostgresCollectionOperationRepository(p8_runtime.unit_of_work)
    activation_entered = threading.Event()
    release_activation = threading.Event()
    activation_finished = threading.Event()
    refresh_finished = threading.Event()
    errors: list[BaseException] = []

    class BlockingOperations:
        def reserve_in_uow(self, uow: Any, intent: Any) -> Any:
            activation_entered.set()
            if not release_activation.wait(5):
                raise TimeoutError("fixture did not release activation transaction")
            return real_operations.reserve_in_uow(uow, intent)

    activation = PostgresCollectionActivationRepository(
        p8_runtime.unit_of_work,
        cast(Any, BlockingOperations()),
    )
    refresh = SqlAlchemyNativeCollectionRefreshRepository(p8_runtime.unit_of_work)

    def activate() -> None:
        try:
            activation.reserve(
                tmdb_collection_id="25",
                emby_collection_id="emby-25",
                name="Activated Fixture",
            )
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)
        finally:
            activation_finished.set()

    def reconcile() -> None:
        try:
            refresh.reconcile_active(["emby-25"])
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)
        finally:
            refresh_finished.set()

    activation_thread = threading.Thread(target=activate)
    refresh_thread = threading.Thread(target=reconcile)
    activation_thread.start()
    assert activation_entered.wait(5)
    refresh_thread.start()
    assert not refresh_finished.wait(0.2)
    release_activation.set()
    assert activation_finished.wait(5)
    assert refresh_finished.wait(5)
    activation_thread.join(timeout=1)
    refresh_thread.join(timeout=1)

    assert errors == []
    with p8_runtime.engine.connect() as connection:
        assert connection.execute(
            text(
                """
                SELECT emby_collection_id, is_active
                FROM collections_info
                WHERE tmdb_collection_id = '25'
                """
            )
        ).one() == ("emby-25", True)


def test_stale_full_refresh_snapshot_cannot_deactivate_or_publish_over_new_activation(p8_runtime: Any) -> None:
    snapshot_at = datetime.now(UTC) - timedelta(hours=1)
    _insert_collection(p8_runtime, "25")
    activation = PostgresCollectionActivationRepository(p8_runtime.unit_of_work)
    reserved = activation.reserve(
        tmdb_collection_id="25",
        emby_collection_id="emby-25",
        name="Activated Fixture",
    )
    assert reserved is not None
    refresh = SqlAlchemyNativeCollectionRefreshRepository(p8_runtime.unit_of_work)

    assert refresh.reconcile_active([], observed_at=snapshot_at) == 0
    stale = refresh.publish(
        reference=NativeCollectionReference(
            tmdb_collection_id="25",
            emby_collection_id="stale-emby",
            name="Stale Fixture",
            library_id="library-stale",
        ),
        details={
            "id": 25,
            "parts": [
                {
                    "id": 2599,
                    "title": "Stale Movie",
                    "release_date": "2026-01-01",
                    "poster_path": "/2599.jpg",
                }
            ],
        },
        auto_subscribe=True,
        today=_NOW.date(),
        observed_at=snapshot_at,
    )

    assert stale.applied is False
    with p8_runtime.engine.connect() as connection:
        assert connection.execute(
            text(
                """
                SELECT emby_collection_id, name, is_active
                FROM collections_info
                WHERE tmdb_collection_id = '25'
                """
            )
        ).one() == ("emby-25", "Activated Fixture", True)
        assert (
            connection.scalar(
                text("SELECT COUNT(*) FROM media_metadata WHERE tmdb_id = '2599' AND item_type = 'Movie'")
            )
            == 0
        )


def test_collection_activation_revision_refuses_lossy_downgrade(p8_runtime: Any) -> None:
    _insert_collection(p8_runtime, "25")
    reserved = PostgresCollectionActivationRepository(p8_runtime.unit_of_work).reserve(
        tmdb_collection_id="25",
        emby_collection_id="emby-25",
        name="Activated Fixture",
    )
    assert reserved is not None

    with pytest.raises(RuntimeError, match="cannot downgrade while collection activation operations exist"):
        command.downgrade(Config(str(_REPO_ROOT / "alembic.ini")), "0024_p11_sharing_cancellation")

    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM etk_migrations.alembic_version")) == (
            "0027_p11_watchlist_display"
        )
    command.upgrade(Config(str(_REPO_ROOT / "alembic.ini")), "head")
