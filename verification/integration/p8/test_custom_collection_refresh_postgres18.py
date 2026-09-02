from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import httpx
import pytest
from alembic import command
from alembic.config import Config
from pydantic import SecretStr
from sqlalchemy import text

import etk.domains.collection.operation_repository as operation_repository_module
from etk.domains.collection.custom_task_adapters import (
    SqlAlchemyCustomCollectionLibraryQueryAdapter,
    SqlAlchemyCustomCollectionMediaIndexAdapter,
)
from etk.domains.collection.custom_tasks import CustomCollectionRefreshPlan
from etk.domains.collection.effects import (
    CollectionRemoteIndeterminate,
    CollectionRemoteRejected,
    RemoteCollectionState,
)
from etk.domains.collection.operation_execution import (
    CollectionOperationExecutor,
    CollectionOperationReconciler,
)
from etk.domains.collection.operation_repository import PostgresCollectionOperationRepository
from etk.domains.collection.operations import (
    COLLECTION_OPERATION_TOPIC,
    CollectionOperationConflictError,
    CollectionOperationIntent,
    CollectionOperationKind,
    CollectionOperationStatus,
)
from etk.domains.collection.repository import SqlAlchemyCollectionRepository
from etk.platform import Settings
from etk.platform.db import DatabaseRuntime
from etk.platform.jobs.json_payload import canonical_json

_REPO_ROOT = Path(__file__).resolve().parents[3]


class StatefulRemote:
    def __init__(self) -> None:
        self.collections: dict[str, RemoteCollectionState] = {}
        self.create_calls = 0
        self.add_calls = 0
        self.remove_calls = 0
        self.delete_calls = 0
        self.create_error_after_write: BaseException | None = None
        self.add_error_after_write: BaseException | None = None
        self.remove_error_after_write: BaseException | None = None
        self.delete_error_after_write: BaseException | None = None
        self.after_add: Callable[[], None] | None = None

    def read_collection(self, collection_id: str) -> RemoteCollectionState | None:
        return self.collections.get(collection_id)

    def find_collection_by_name(self, name: str) -> RemoteCollectionState | None:
        matches = [
            state for state in self.collections.values() if str(state.name or "").casefold() == str(name).casefold()
        ]
        if len(matches) > 1:
            raise CollectionRemoteRejected("ambiguous collection name")
        return matches[0] if matches else None

    def create_collection(self, name: str, member_ids: Sequence[str]) -> RemoteCollectionState:
        self.create_calls += 1
        state = RemoteCollectionState(
            f"created-{self.create_calls}",
            tuple(member_ids),
            name=name,
        )
        self.collections[state.collection_id] = state
        if self.create_error_after_write is not None:
            error = self.create_error_after_write
            self.create_error_after_write = None
            raise error
        return state

    def add_members(self, collection_id: str, member_ids: Sequence[str]) -> None:
        self.add_calls += 1
        current = self.collections[collection_id]
        self.collections[collection_id] = RemoteCollectionState(
            current.collection_id,
            tuple(sorted(set(current.member_ids) | set(member_ids))),
            current.tmdb_collection_id,
            current.name,
        )
        if self.after_add is not None:
            callback = self.after_add
            self.after_add = None
            callback()
        if self.add_error_after_write is not None:
            error = self.add_error_after_write
            self.add_error_after_write = None
            raise error

    def remove_members(self, collection_id: str, member_ids: Sequence[str]) -> None:
        self.remove_calls += 1
        current = self.collections[collection_id]
        self.collections[collection_id] = RemoteCollectionState(
            current.collection_id,
            tuple(sorted(set(current.member_ids) - set(member_ids))),
            current.tmdb_collection_id,
            current.name,
        )
        if self.remove_error_after_write is not None:
            error = self.remove_error_after_write
            self.remove_error_after_write = None
            raise error

    def list_native_collections(self, tmdb_collection_id: str) -> Sequence[RemoteCollectionState]:
        del tmdb_collection_id
        return ()

    def empty_collection(self, state: RemoteCollectionState) -> None:
        self.remove_members(state.collection_id, state.member_ids)

    def delete_collection(self, collection_id: str) -> None:
        self.delete_calls += 1
        self.collections.pop(collection_id, None)
        if self.delete_error_after_write is not None:
            error = self.delete_error_after_write
            self.delete_error_after_write = None
            raise error


def _create_custom(
    runtime: DatabaseRuntime,
    *,
    name: str,
    definition: Mapping[str, Any] | None = None,
    emby_collection_id: str | None = None,
) -> Mapping[str, Any]:
    repository = SqlAlchemyCollectionRepository(runtime.unit_of_work)
    row = repository.create_custom(
        name=name,
        collection_type="list",
        definition=dict(definition or {"item_type": ["Movie"]}),
        allowed_user_ids=None,
    )
    if emby_collection_id is not None:
        with runtime.engine.begin() as connection:
            connection.execute(
                text("UPDATE custom_collections SET emby_collection_id = :emby_id WHERE id = :collection_id"),
                {"emby_id": emby_collection_id, "collection_id": row["id"]},
            )
        row = repository.get_custom(int(row["id"])) or row
    return row


def _plan(
    row: Mapping[str, Any],
    *,
    desired: Sequence[str],
    delete_empty_source: bool = False,
    generated_items: Sequence[Mapping[str, Any]] | None = None,
    item_type: Sequence[str] = ("Movie",),
) -> CustomCollectionRefreshPlan:
    definition = dict(row.get("definition_json") or {})
    return CustomCollectionRefreshPlan(
        collection_id=int(row["id"]),
        collection_name=str(row["name"]),
        collection_type=str(row["type"]),
        definition_digest=hashlib.sha256(canonical_json(definition).encode()).hexdigest(),
        source_snapshot={"status": "empty" if not desired else "ready", "items": []},
        desired_member_ids=tuple(desired),
        generated_items=(
            tuple(generated_items) if generated_items is not None else tuple({"emby_id": value} for value in desired)
        ),
        item_type=tuple(item_type),
        in_library_count=len(desired),
        expected_emby_collection_id=str(row.get("emby_collection_id") or "").strip() or None,
        allow_empty=True,
        delete_empty_source=delete_empty_source,
    )


def _reserve(
    runtime: DatabaseRuntime,
    plan: CustomCollectionRefreshPlan,
    *,
    occurrence: str,
) -> Any:
    repository = PostgresCollectionOperationRepository(runtime.unit_of_work)
    return repository.reserve(
        CollectionOperationIntent.create(
            kind=CollectionOperationKind.CUSTOM_REFRESH,
            scope_key=(
                f"emby-collection:{plan.expected_emby_collection_id}"
                if plan.expected_emby_collection_id
                else f"custom-collection:{plan.collection_id}"
            ),
            principal_id="fixture:custom-refresh",
            request=plan.to_request(),
            idempotency_key=occurrence,
        )
    ).operation


def _executor(
    runtime: DatabaseRuntime,
    remote: StatefulRemote,
    *,
    readback_attempts: int = 4,
) -> CollectionOperationExecutor:
    return CollectionOperationExecutor(
        PostgresCollectionOperationRepository(runtime.unit_of_work),
        remote,
        worker_id="fixture-custom-refresh",
        lease_seconds=60,
        retry_delay_seconds=0,
        readback_attempts=readback_attempts,
    )


def _outbox_status(runtime: DatabaseRuntime, operation_id: object) -> str:
    with runtime.engine.connect() as connection:
        return str(
            connection.scalar(
                text("SELECT status FROM outbox_events WHERE event_key = :event_key"),
                {"event_key": f"{COLLECTION_OPERATION_TOPIC}:{operation_id}"},
            )
        )


def test_create_crash_recovers_by_name_without_resending(
    p8_runtime: DatabaseRuntime,
) -> None:
    row = _create_custom(p8_runtime, name="Create Recovery")
    plan = _plan(row, desired=("item-a", "item-b"))
    operation = _reserve(p8_runtime, plan, occurrence="create-recovery")
    remote = StatefulRemote()
    remote.create_error_after_write = CollectionRemoteIndeterminate("lost create response")
    executor = _executor(p8_runtime, remote)

    pending = executor.execute(operation.id)
    terminal = executor.execute(operation.id)

    assert pending is not None and pending.status is CollectionOperationStatus.AWAITING_READBACK
    assert terminal is not None and terminal.status is CollectionOperationStatus.CONFIRMED
    assert remote.create_calls == 1
    assert terminal.result is not None and terminal.result["recovered_by_readback"] is True
    projection = SqlAlchemyCollectionRepository(p8_runtime.unit_of_work).get_custom(int(row["id"]))
    assert projection is not None
    assert projection["emby_collection_id"] == "created-1"
    assert projection["generated_media_info_json"] == [{"emby_id": "item-a"}, {"emby_id": "item-b"}]
    assert _outbox_status(p8_runtime, operation.id) == "processed"


def test_remove_crash_recovers_then_adds_only_the_remaining_diff(
    p8_runtime: DatabaseRuntime,
) -> None:
    row = _create_custom(p8_runtime, name="Member Recovery", emby_collection_id="box-1")
    plan = _plan(row, desired=("item-b", "item-c"))
    operation = _reserve(p8_runtime, plan, occurrence="member-recovery")
    remote = StatefulRemote()
    remote.collections["box-1"] = RemoteCollectionState("box-1", ("item-a", "item-b"), name="Member Recovery")
    remote.remove_error_after_write = CollectionRemoteIndeterminate("lost remove response")
    executor = _executor(p8_runtime, remote)

    pending = executor.execute(operation.id)
    terminal = executor.execute(operation.id)

    assert pending is not None and pending.status is CollectionOperationStatus.AWAITING_READBACK
    assert terminal is not None and terminal.status is CollectionOperationStatus.CONFIRMED
    assert remote.remove_calls == 1
    assert remote.add_calls == 1
    assert set(remote.collections["box-1"].member_ids) == {"item-b", "item-c"}


def test_add_crash_recovers_without_resending(
    p8_runtime: DatabaseRuntime,
) -> None:
    row = _create_custom(p8_runtime, name="Add Recovery", emby_collection_id="box-add")
    plan = _plan(row, desired=("item-a", "item-b"))
    operation = _reserve(p8_runtime, plan, occurrence="add-recovery")
    remote = StatefulRemote()
    remote.collections["box-add"] = RemoteCollectionState("box-add", ("item-a",), name="Add Recovery")
    remote.add_error_after_write = CollectionRemoteIndeterminate("lost add response")
    executor = _executor(p8_runtime, remote)

    pending = executor.execute(operation.id)
    terminal = executor.execute(operation.id)

    assert pending is not None and pending.status is CollectionOperationStatus.AWAITING_READBACK
    assert terminal is not None and terminal.status is CollectionOperationStatus.CONFIRMED
    assert remote.add_calls == 1
    assert set(remote.collections["box-add"].member_ids) == {"item-a", "item-b"}


def test_delete_crash_recovers_without_resending(
    p8_runtime: DatabaseRuntime,
) -> None:
    row = _create_custom(p8_runtime, name="Delete Recovery", emby_collection_id="box-delete")
    plan = _plan(row, desired=(), delete_empty_source=True)
    operation = _reserve(p8_runtime, plan, occurrence="delete-recovery")
    remote = StatefulRemote()
    remote.collections["box-delete"] = RemoteCollectionState("box-delete", (), name="Delete Recovery")
    remote.delete_error_after_write = CollectionRemoteIndeterminate("lost delete response")
    executor = _executor(p8_runtime, remote)

    pending = executor.execute(operation.id)
    terminal = executor.execute(operation.id)

    assert pending is not None and pending.status is CollectionOperationStatus.AWAITING_READBACK
    assert terminal is not None and terminal.status is CollectionOperationStatus.CONFIRMED
    assert remote.delete_calls == 1
    projection = SqlAlchemyCollectionRepository(p8_runtime.unit_of_work).get_custom(int(row["id"]))
    assert projection is not None and projection["emby_collection_id"] is None


def test_ambiguous_name_rejects_before_create(
    p8_runtime: DatabaseRuntime,
) -> None:
    row = _create_custom(p8_runtime, name="Ambiguous")
    operation = _reserve(p8_runtime, _plan(row, desired=("item-a",)), occurrence="ambiguous-name")
    remote = StatefulRemote()
    remote.collections = {
        "box-a": RemoteCollectionState("box-a", (), name="Ambiguous"),
        "box-b": RemoteCollectionState("box-b", (), name="AMBIGUOUS"),
    }

    terminal = _executor(p8_runtime, remote).execute(operation.id)

    assert terminal is not None and terminal.status is CollectionOperationStatus.REJECTED
    assert remote.create_calls == 0


def test_same_occurrence_with_different_custom_plan_conflicts(
    p8_runtime: DatabaseRuntime,
) -> None:
    row = _create_custom(p8_runtime, name="Request Conflict")
    first = _plan(row, desired=("item-a",))
    _reserve(p8_runtime, first, occurrence="same-occurrence")

    with pytest.raises(CollectionOperationConflictError):
        _reserve(
            p8_runtime,
            _plan(row, desired=("item-b",)),
            occurrence="same-occurrence",
        )


def test_definition_drift_after_provider_write_is_indeterminate_and_not_replayed(
    p8_runtime: DatabaseRuntime,
) -> None:
    row = _create_custom(p8_runtime, name="Definition Drift", emby_collection_id="box-drift")
    plan = _plan(row, desired=("item-a", "item-b"))
    operation = _reserve(p8_runtime, plan, occurrence="definition-drift")
    remote = StatefulRemote()
    remote.collections["box-drift"] = RemoteCollectionState("box-drift", ("item-a",), name="Definition Drift")

    def drift_definition() -> None:
        with p8_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE custom_collections SET definition_json = CAST(:definition AS jsonb) "
                    "WHERE id = :collection_id"
                ),
                {"definition": '{"changed":true}', "collection_id": row["id"]},
            )

    remote.after_add = drift_definition
    executor = _executor(p8_runtime, remote)

    terminal = executor.execute(operation.id)
    replay = executor.execute(operation.id)

    assert terminal is not None and terminal.status is CollectionOperationStatus.INDETERMINATE
    assert replay is not None and replay.status is CollectionOperationStatus.INDETERMINATE
    assert remote.add_calls == 1
    projection = SqlAlchemyCollectionRepository(p8_runtime.unit_of_work).get_custom(int(row["id"]))
    assert projection is not None and projection["last_synced_at"] is None
    assert _outbox_status(p8_runtime, operation.id) == "processed"


def test_manual_reconciliation_confirms_an_indeterminate_create_atomically(
    p8_runtime: DatabaseRuntime,
) -> None:
    row = _create_custom(p8_runtime, name="Manual Reconcile")
    plan = _plan(row, desired=("item-a",))
    operation = _reserve(p8_runtime, plan, occurrence="manual-reconcile")
    remote = StatefulRemote()
    remote.create_error_after_write = CollectionRemoteIndeterminate("lost create response")

    indeterminate = _executor(p8_runtime, remote, readback_attempts=1).execute(operation.id)
    assert indeterminate is not None and indeterminate.status is CollectionOperationStatus.INDETERMINATE

    confirmed = CollectionOperationReconciler(
        PostgresCollectionOperationRepository(p8_runtime.unit_of_work),
        remote,
    ).reconcile(
        operation.id,
        expected_state_revision=indeterminate.state_revision,
        resolved_by="fixture-admin",
    )

    assert confirmed is not None and confirmed.status is CollectionOperationStatus.CONFIRMED
    assert confirmed.result is not None and confirmed.result["reconciled"] is True
    projection = SqlAlchemyCollectionRepository(p8_runtime.unit_of_work).get_custom(int(row["id"]))
    assert projection is not None and projection["emby_collection_id"] == "created-1"
    assert remote.create_calls == 1
    assert _outbox_status(p8_runtime, operation.id) == "processed"


def test_projection_failure_rolls_back_before_operation_terminalization(
    p8_runtime: DatabaseRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row = _create_custom(p8_runtime, name="Atomic Rollback", emby_collection_id="box-rollback")
    plan = _plan(row, desired=("item-a",))
    operation = _reserve(p8_runtime, plan, occurrence="atomic-rollback")
    remote = StatefulRemote()
    remote.collections["box-rollback"] = RemoteCollectionState("box-rollback", ("item-a",), name="Atomic Rollback")

    def fail_projection(uow: Any, **_values: Any) -> None:
        uow.session.execute(
            text("UPDATE custom_collections SET emby_collection_id = 'leaked' WHERE id = :collection_id"),
            {"collection_id": row["id"]},
        )
        raise ValueError("fixture projection failure")

    monkeypatch.setattr(
        operation_repository_module,
        "apply_custom_refresh_projection_in_uow",
        fail_projection,
    )

    terminal = _executor(p8_runtime, remote).execute(operation.id)

    assert terminal is not None and terminal.status is CollectionOperationStatus.REJECTED
    projection = SqlAlchemyCollectionRepository(p8_runtime.unit_of_work).get_custom(int(row["id"]))
    assert projection is not None and projection["emby_collection_id"] == "box-rollback"
    assert _outbox_status(p8_runtime, operation.id) == "processed"


def test_unexpected_projection_failure_after_provider_write_is_indeterminate(
    p8_runtime: DatabaseRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row = _create_custom(p8_runtime, name="Unexpected Projection", emby_collection_id="box-runtime")
    plan = _plan(row, desired=("item-a", "item-b"))
    operation = _reserve(p8_runtime, plan, occurrence="unexpected-projection")
    remote = StatefulRemote()
    remote.collections["box-runtime"] = RemoteCollectionState(
        "box-runtime",
        ("item-a",),
        name="Unexpected Projection",
    )

    def fail_projection(_uow: Any, **_values: Any) -> None:
        raise RuntimeError("fixture runtime projection failure")

    monkeypatch.setattr(
        operation_repository_module,
        "apply_custom_refresh_projection_in_uow",
        fail_projection,
    )

    terminal = _executor(p8_runtime, remote).execute(operation.id)
    replay = _executor(p8_runtime, remote).execute(operation.id)

    assert terminal is not None and terminal.status is CollectionOperationStatus.INDETERMINATE
    assert replay is not None and replay.status is CollectionOperationStatus.INDETERMINATE
    assert remote.add_calls == 1
    projection = SqlAlchemyCollectionRepository(p8_runtime.unit_of_work).get_custom(int(row["id"]))
    assert projection is not None and projection["last_synced_at"] is None
    assert _outbox_status(p8_runtime, operation.id) == "processed"


def test_custom_refresh_publishes_subscriptions_atomically_with_hard_ignore_and_season_identity(
    p8_runtime: DatabaseRuntime,
) -> None:
    row = _create_custom(p8_runtime, name="Atomic Subscriptions", emby_collection_id="box-subscriptions")
    generated = (
        {"tmdb_id": "701", "media_type": "Movie", "title": "Hard ignored", "emby_id": None},
        {"tmdb_id": "702", "media_type": "Movie", "title": "Soft ignored", "emby_id": None},
        {
            "tmdb_id": "703",
            "media_type": "Series",
            "title": "Season",
            "season": 1,
            "season_tmdb_id": "70301",
            "emby_id": None,
        },
    )
    plan = _plan(row, desired=(), generated_items=generated, item_type=("Movie", "Series"))
    operation = _reserve(p8_runtime, plan, occurrence="atomic-subscriptions")
    remote = StatefulRemote()
    remote.collections["box-subscriptions"] = RemoteCollectionState(
        "box-subscriptions",
        (),
        name="Atomic Subscriptions",
    )
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, subscription_status, ignore_reason
                ) VALUES
                    ('701', 'Movie', 'Hard ignored', 'IGNORED', '手动忽略'),
                    ('702', 'Movie', 'Soft ignored', 'IGNORED', '临时忽略')
                """
            )
        )

    terminal = _executor(p8_runtime, remote).execute(operation.id)

    assert terminal is not None and terminal.status is CollectionOperationStatus.CONFIRMED
    with p8_runtime.engine.connect() as connection:
        values = (
            connection.execute(
                text(
                    """
                SELECT tmdb_id, item_type, subscription_status, ignore_reason,
                       season_number, parent_series_tmdb_id, subscription_sources_json
                FROM media_metadata
                WHERE tmdb_id IN ('701','702','70301')
                ORDER BY tmdb_id
                """
                )
            )
            .mappings()
            .all()
        )
    assert [(value["tmdb_id"], value["subscription_status"], value["ignore_reason"]) for value in values] == [
        ("701", "IGNORED", "手动忽略"),
        ("702", "WANTED", None),
        ("70301", "WANTED", None),
    ]
    assert values[2]["item_type"] == "Season"
    assert values[2]["season_number"] == 1
    assert values[2]["parent_series_tmdb_id"] == "703"
    assert values[2]["subscription_sources_json"] == [
        {"type": "custom_collection", "id": int(row["id"]), "name": "Atomic Subscriptions"}
    ]
    assert _outbox_status(p8_runtime, operation.id) == "processed"


def test_media_index_rejects_stale_offline_emby_identity(
    p8_runtime: DatabaseRuntime,
) -> None:
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, in_library, emby_item_ids_json, subscription_status
                ) VALUES
                    ('801', 'Movie', 'Offline', FALSE, '["stale-item"]'::jsonb, 'NONE'),
                    ('802', 'Movie', 'Online', TRUE, '["live-item"]'::jsonb, 'NONE'),
                    ('80301', 'Season', 'Offline season', FALSE, '["stale-season"]'::jsonb, 'NONE')
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE media_metadata
                SET parent_series_tmdb_id = '803', season_number = 1
                WHERE tmdb_id = '80301' AND item_type = 'Season'
                """
            )
        )
    index = SqlAlchemyCustomCollectionMediaIndexAdapter(p8_runtime.unit_of_work)

    assert index.lookup(tmdb_id="801", media_type="Movie") is None
    assert index.lookup(tmdb_id="802", media_type="Movie") == "live-item"
    assert index.lookup(tmdb_id="803", media_type="Series", season=1) is None


def test_library_query_uses_only_the_selected_users_visible_emby_identity(
    p8_runtime: DatabaseRuntime,
) -> None:
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, in_library, emby_item_ids_json, subscription_status
                ) VALUES
                    ('811', 'Movie', 'Multi identity', TRUE, '["hidden","visible-a"]'::jsonb, 'NONE'),
                    ('812', 'Movie', 'User B', TRUE, '["visible-b"]'::jsonb, 'NONE')
                """
            )
        )
    settings = Settings(
        emby_server_url="https://emby.example",
        emby_user_id="user-a",
        emby_api_key=SecretStr("fixture-token"),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        user_id = request.url.path.split("/")[2]
        values = {"user-a": ["visible-a"], "user-b": ["visible-b"]}[user_id]
        return httpx.Response(
            200,
            json={"Items": [{"Id": value} for value in values], "TotalRecordCount": len(values)},
        )

    adapter = SqlAlchemyCustomCollectionLibraryQueryAdapter(
        p8_runtime.unit_of_work,
        lambda: settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )
    definition = {"item_type": ["Movie"], "rules": []}

    user_a = adapter.query_filter(definition, user_id="user-a", execution_key="fixture")
    user_b = adapter.query_filter(definition, user_id="user-b", execution_key="fixture")

    assert user_a.emby_ids == ("visible-a",)
    assert user_b.emby_ids == ("visible-b",)
    assert "hidden" not in user_a.emby_ids


def test_library_query_does_not_fall_back_to_all_media_when_emby_read_fails(
    p8_runtime: DatabaseRuntime,
) -> None:
    settings = Settings(
        emby_server_url="https://emby.example",
        emby_user_id="user-a",
        emby_api_key=SecretStr("fixture-token"),
    )

    def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("fixture timeout", request=request)

    adapter = SqlAlchemyCustomCollectionLibraryQueryAdapter(
        p8_runtime.unit_of_work,
        lambda: settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(timeout)),
    )

    with pytest.raises(RuntimeError, match="Emby user library query failed"):
        adapter.query_filter({"item_type": ["Movie"], "rules": []}, user_id="user-a")


def test_custom_refresh_revision_refuses_lossy_downgrade(
    p8_runtime: DatabaseRuntime,
) -> None:
    row = _create_custom(p8_runtime, name="Migration Guard")
    operation = _reserve(
        p8_runtime,
        _plan(row, desired=("item-a",)),
        occurrence="migration-guard",
    )
    assert operation.kind is CollectionOperationKind.CUSTOM_REFRESH

    with pytest.raises(RuntimeError, match="cannot downgrade while custom collection refresh operations exist"):
        command.downgrade(Config(str(_REPO_ROOT / "alembic.ini")), "0025_p11_collection_activation")

    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM etk_migrations.alembic_version")) == (
            "0026_p11_custom_refresh"
        )
