from __future__ import annotations

from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etk.domains.collection.effects import (
    CollectionRemoteIndeterminate,
    PostgresCollectionEffectGateway,
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
    CollectionOperationScopeBusyError,
    CollectionOperationStateError,
    CollectionOperationStatus,
    CollectionOperationStep,
    collection_operation_key,
)
from etk.domains.collection.repository import SqlAlchemyCollectionRepository
from etk.platform.db import DatabaseRuntime


class FakeRemote:
    def __init__(
        self,
        reads: Sequence[RemoteCollectionState | None | BaseException] = (),
        *,
        listed: Sequence[RemoteCollectionState] | BaseException = (),
    ) -> None:
        self.reads = list(reads)
        self.listed = listed
        self.read_calls: list[str] = []
        self.list_calls: list[str] = []
        self.empty_calls: list[str] = []
        self.delete_calls: list[str] = []
        self.empty_error: BaseException | None = None
        self.delete_error: BaseException | None = None

    def read_collection(self, collection_id: str) -> RemoteCollectionState | None:
        self.read_calls.append(collection_id)
        if not self.reads:
            raise AssertionError("unexpected collection readback")
        value = self.reads.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value

    def list_native_collections(self, tmdb_collection_id: str) -> Sequence[RemoteCollectionState]:
        self.list_calls.append(tmdb_collection_id)
        if isinstance(self.listed, BaseException):
            raise self.listed
        return tuple(self.listed)

    def empty_collection(self, state: RemoteCollectionState) -> None:
        self.empty_calls.append(state.collection_id)
        if self.empty_error is not None:
            raise self.empty_error

    def delete_collection(self, collection_id: str) -> None:
        self.delete_calls.append(collection_id)
        if self.delete_error is not None:
            raise self.delete_error


class FakeAssets:
    def __init__(self) -> None:
        self.archived: dict[str, Mapping[str, Any]] = {}
        self.archive_calls: list[str] = []
        self.cleanup_calls: list[str] = []
        self.cleanup_failures: dict[str, int] = {}
        self.discard_calls: list[tuple[str, ...]] = []
        self.discard_error: BaseException | None = None

    def read_archived_image(self, source_url: str) -> Mapping[str, Any] | None:
        return self.archived.get(source_url)

    def archive_image(self, source_url: str) -> Mapping[str, Any] | None:
        self.archive_calls.append(source_url)
        receipt = {
            "source_url": source_url,
            "content_hash": "a" * 64,
            "relative_path": f"aa/{len(self.archive_calls)}.jpg",
            "mime_type": "image/jpeg",
            "byte_size": 123,
        }
        self.archived[source_url] = receipt
        return receipt

    def cleanup_placeholder(self, tmdb_id: str) -> bool:
        self.cleanup_calls.append(tmdb_id)
        failures = self.cleanup_failures.get(tmdb_id, 0)
        if failures > 0:
            self.cleanup_failures[tmdb_id] = failures - 1
            raise RuntimeError("fixture placeholder cleanup failure")
        return True

    def discard_unreferenced_sources(self, source_urls: Sequence[str]) -> int:
        values = tuple(source_urls)
        self.discard_calls.append(values)
        if self.discard_error is not None:
            raise self.discard_error
        return len(values)


class FakeSuppression:
    def __init__(self) -> None:
        self.values: list[tuple[str, float]] = []

    def suppress(self, collection_id: str, *, ttl_seconds: float) -> None:
        self.values.append((collection_id, ttl_seconds))

    def is_suppressed(self, collection_id: str) -> bool:
        return any(value == collection_id for value, _ttl_seconds in self.values)


def _collection_repository(runtime: DatabaseRuntime) -> SqlAlchemyCollectionRepository:
    return SqlAlchemyCollectionRepository(runtime.unit_of_work)


def _operation_repository(runtime: DatabaseRuntime) -> PostgresCollectionOperationRepository:
    return PostgresCollectionOperationRepository(runtime.unit_of_work)


def _executor(
    runtime: DatabaseRuntime,
    remote: FakeRemote,
    assets: FakeAssets | None = None,
    suppression: FakeSuppression | None = None,
) -> CollectionOperationExecutor:
    return CollectionOperationExecutor(
        _operation_repository(runtime),
        remote,
        assets or FakeAssets(),
        suppression or FakeSuppression(),
        worker_id="collection-pg18",
        lease_seconds=60,
        retry_delay_seconds=0,
    )


def _expire_claim(runtime: DatabaseRuntime, operation_id: UUID) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE collection_operations
                SET claimed_at = NOW() - :claimed_age,
                    lease_expires_at = NOW() - :lease_age
                WHERE id = :operation_id
                """
            ),
            {
                "operation_id": operation_id,
                "claimed_age": timedelta(seconds=60),
                "lease_age": timedelta(seconds=30),
            },
        )


def test_collection_operation_schema_enforces_provider_checkpoint_and_scope_indexes(
    p8_runtime: DatabaseRuntime,
) -> None:
    with p8_runtime.engine.connect() as connection:
        indexes = set(
            connection.scalars(
                text(
                    """
                    SELECT indexname
                    FROM pg_indexes
                    WHERE schemaname = 'public' AND tablename = 'collection_operations'
                    """
                )
            )
        )
    with p8_runtime.engine.connect() as connection:
        transaction = connection.begin()
        with pytest.raises(IntegrityError):
            connection.execute(
                text(
                    """
                    INSERT INTO collection_operations (
                        id, operation_key, request_hash, kind, scope_key,
                        request_json, status, current_step, step_started_at
                    ) VALUES (
                        :id, :operation_key, :request_hash,
                        'collection.native.delete', 'emby-collection:invalid',
                        '{}'::jsonb, 'requesting', 'delete_shell', NOW()
                    )
                    """
                ),
                {
                    "id": uuid4(),
                    "operation_key": "c" * 64,
                    "request_hash": bytes.fromhex("d" * 64),
                },
            )
        transaction.rollback()

    assert {
        "ix_collection_operation_due",
        "ix_collection_operation_claim_recovery",
        "ix_collection_operation_scope_history",
        "uq_collection_operation_active_scope",
    }.issubset(indexes)


def test_custom_delete_reserves_operation_and_outbox_with_local_delete_atomically(
    p8_runtime: DatabaseRuntime,
) -> None:
    collections = _collection_repository(p8_runtime)
    collection = collections.create_custom(
        name="Delete Fixture",
        collection_type="list",
        definition={"source": "fixture"},
        allowed_user_ids=None,
    )
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE custom_collections
                SET emby_collection_id = 'emby-custom',
                    generated_media_info_json =
                        '[{"tmdb_id":"11"},{"tmdb_id":"12"},{"tmdb_id":"11"}]'::jsonb
                WHERE id = :collection_id
                """
            ),
            {"collection_id": collection["id"]},
        )

    gateway = PostgresCollectionEffectGateway(_operation_repository(p8_runtime))
    staged = gateway.stage_custom_delete(
        int(collection["id"]),
        principal_id="collection-admin:fixture",
        idempotency_key="delete-once",
    )

    assert staged is not None and staged.created is True
    assert staged.related_effect_ids == ()
    assert collections.get_custom(int(collection["id"])) is None
    with p8_runtime.engine.connect() as connection:
        operation = (
            connection.execute(
                text("SELECT kind, request_json, status FROM collection_operations WHERE id = :id"),
                {"id": staged.effect_id},
            )
            .mappings()
            .one()
        )
        outbox = (
            connection.execute(
                text("SELECT topic, aggregate_id, status FROM outbox_events"),
            )
            .mappings()
            .one()
        )
        media_effect_count = int(connection.scalar(text("SELECT COUNT(*) FROM media_effects")) or 0)
    assert operation["kind"] == CollectionOperationKind.CUSTOM_DELETE.value
    assert operation["status"] == "ready"
    assert operation["request_json"]["placeholder_tmdb_ids"] == ["11", "12"]
    assert outbox == {
        "topic": COLLECTION_OPERATION_TOPIC,
        "aggregate_id": str(staged.effect_id),
        "status": "pending",
    }
    assert media_effect_count == 0

    replay = gateway.stage_custom_delete(
        int(collection["id"]),
        principal_id="collection-admin:fixture",
        idempotency_key="delete-once",
    )
    assert replay is not None and replay.effect_id == staged.effect_id and replay.created is False

    other = collections.create_custom(
        name="Delete Conflict",
        collection_type="list",
        definition={},
        allowed_user_ids=None,
    )
    with pytest.raises(CollectionOperationConflictError):
        gateway.stage_custom_delete(
            int(other["id"]),
            principal_id="collection-admin:fixture",
            idempotency_key="delete-once",
        )
    assert collections.get_custom(int(other["id"])) is not None


def test_custom_delete_rolls_back_row_and_operation_when_outbox_insert_fails(
    p8_runtime: DatabaseRuntime,
) -> None:
    collections = _collection_repository(p8_runtime)
    collection = collections.create_custom(
        name="Atomic Delete",
        collection_type="list",
        definition={},
        allowed_user_ids=None,
    )
    operation_key = collection_operation_key(
        kind=CollectionOperationKind.CUSTOM_DELETE,
        principal_id="collection-admin:fixture",
        idempotency_key="atomic-conflict",
    )
    operation_id = uuid5(NAMESPACE_URL, f"etk:collection-operation:{operation_key}")
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO outbox_events (
                    id, event_key, topic, aggregate_type, aggregate_id,
                    payload_version, payload_json, status, available_at
                ) VALUES (
                    :id, :event_key, 'fixture.conflict', 'fixture', :aggregate_id,
                    1, '{}'::jsonb, 'pending', NOW()
                )
                """
            ),
            {
                "id": uuid4(),
                "event_key": f"{COLLECTION_OPERATION_TOPIC}:{operation_id}",
                "aggregate_id": str(operation_id),
            },
        )

    gateway = PostgresCollectionEffectGateway(_operation_repository(p8_runtime))
    with pytest.raises(IntegrityError):
        gateway.stage_custom_delete(
            int(collection["id"]),
            principal_id="collection-admin:fixture",
            idempotency_key="atomic-conflict",
        )

    assert collections.get_custom(int(collection["id"])) is not None
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT COUNT(*) FROM collection_operations WHERE id = :id"),
                {"id": operation_id},
            )
            == 0
        )


def test_custom_delete_remote_failure_keeps_local_delete_and_never_deletes_shell(
    p8_runtime: DatabaseRuntime,
) -> None:
    collections = _collection_repository(p8_runtime)
    collection = collections.create_custom(
        name="Best Effort",
        collection_type="list",
        definition={},
        allowed_user_ids=None,
    )
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE custom_collections
                SET emby_collection_id = 'custom-box',
                    generated_media_info_json = '[{"tmdb_id":"88"}]'::jsonb
                WHERE id = :id
                """
            ),
            {"id": collection["id"]},
        )
    staged = PostgresCollectionEffectGateway(_operation_repository(p8_runtime)).stage_custom_delete(
        int(collection["id"]),
        principal_id="collection-admin:fixture",
        idempotency_key="best-effort",
    )
    assert staged is not None
    remote = FakeRemote([CollectionRemoteIndeterminate("Emby unavailable")])
    assets = FakeAssets()
    suppression = FakeSuppression()

    result = _executor(p8_runtime, remote, assets, suppression).execute(staged.effect_id)

    assert result is not None and result.status is CollectionOperationStatus.CONFIRMED
    assert collections.get_custom(int(collection["id"])) is None
    assert remote.delete_calls == []
    assert assets.cleanup_calls == ["88"]
    assert result.result is not None and result.result["warnings"]


def test_custom_placeholder_cleanup_retries_without_repeating_completed_items(
    p8_runtime: DatabaseRuntime,
) -> None:
    collections = _collection_repository(p8_runtime)
    collection = collections.create_custom(
        name="Placeholder Retry",
        collection_type="list",
        definition={},
        allowed_user_ids=None,
    )
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE custom_collections
                SET generated_media_info_json =
                    '[{"tmdb_id":"11"},{"tmdb_id":"12"}]'::jsonb
                WHERE id = :id
                """
            ),
            {"id": collection["id"]},
        )
    staged = PostgresCollectionEffectGateway(_operation_repository(p8_runtime)).stage_custom_delete(
        int(collection["id"]),
        principal_id="collection-admin:fixture",
        idempotency_key="placeholder-retry",
    )
    assert staged is not None
    assets = FakeAssets()
    assets.cleanup_failures["12"] = 1
    executor = _executor(p8_runtime, FakeRemote(), assets)

    first = executor.execute(staged.effect_id)
    second = executor.execute(staged.effect_id)

    assert first is not None and first.status is CollectionOperationStatus.RETRYABLE
    assert first.state["cleaned_placeholder_tmdb_ids"] == ["11"]
    assert second is not None and second.status is CollectionOperationStatus.CONFIRMED
    assert assets.cleanup_calls == ["11", "12", "12"]


def test_native_delete_unknown_then_later_confirmation_uses_readback_without_second_write(
    p8_runtime: DatabaseRuntime,
) -> None:
    collections = _collection_repository(p8_runtime)
    collections.upsert_native(
        {"id": 101, "name": "Native Fixture", "parts": []},
        existing_emby_id="emby-native",
    )
    staged = PostgresCollectionEffectGateway(_operation_repository(p8_runtime)).stage_native_delete(
        "emby-native",
        principal_id="collection-admin:fixture",
        idempotency_key="native-delete",
    )
    state = RemoteCollectionState("emby-native", ("item-1",), "101")
    remote = FakeRemote([state, state, None])
    remote.delete_error = CollectionRemoteIndeterminate("timeout after provider request")
    suppression = FakeSuppression()
    executor = _executor(p8_runtime, remote, suppression=suppression)

    first = executor.execute(staged.effect_id)
    assert first is not None and first.status is CollectionOperationStatus.AWAITING_READBACK
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT status FROM outbox_events WHERE aggregate_id = :id"),
                {"id": str(staged.effect_id)},
            )
            == "pending"
        )
    second = executor.execute(staged.effect_id)

    assert second is not None and second.status is CollectionOperationStatus.CONFIRMED
    assert remote.empty_calls == ["emby-native"]
    assert remote.delete_calls == ["emby-native"]
    assert suppression.values == [
        ("emby-native", 10),
        ("emby-native", 10),
    ]
    assert collections.get_native("101")["is_active"] is False  # type: ignore[index]


def test_native_delete_readback_exhaustion_is_indeterminate_and_keeps_scope_fenced(
    p8_runtime: DatabaseRuntime,
) -> None:
    collections = _collection_repository(p8_runtime)
    collections.upsert_native(
        {"id": 202, "name": "Unknown Fixture", "parts": []},
        existing_emby_id="emby-unknown",
    )
    state = RemoteCollectionState("emby-unknown", (), "202")
    gateway = PostgresCollectionEffectGateway(_operation_repository(p8_runtime))
    staged = gateway.stage_native_delete(
        "emby-unknown",
        principal_id="collection-admin:fixture",
        idempotency_key="native-unknown",
    )
    remote = FakeRemote([state, state, state, state, state])
    remote.delete_error = CollectionRemoteIndeterminate("timeout after provider request")
    executor = _executor(p8_runtime, remote)

    result = executor.execute(staged.effect_id)
    while result is not None and not result.terminal:
        result = executor.execute(staged.effect_id)

    assert result is not None and result.status is CollectionOperationStatus.INDETERMINATE
    assert remote.delete_calls == ["emby-unknown"]
    assert collections.get_native("202")["is_active"] is True  # type: ignore[index]
    with pytest.raises(CollectionOperationScopeBusyError):
        gateway.stage_native_delete(
            "emby-unknown",
            principal_id="collection-admin:fixture",
            idempotency_key="another-delete",
        )
    reconciled = CollectionOperationReconciler(
        _operation_repository(p8_runtime),
        FakeRemote([None]),
    ).reconcile(
        result.id,
        expected_state_revision=result.state_revision,
        resolved_by="collection-admin:fixture",
    )
    assert reconciled is not None and reconciled.status is CollectionOperationStatus.CONFIRMED
    assert collections.get_native("202")["is_active"] is False  # type: ignore[index]
    follow_up = gateway.stage_native_delete(
        "emby-unknown",
        principal_id="collection-admin:fixture",
        idempotency_key="after-reconciliation",
    )
    assert follow_up.created is True


def test_native_indeterminate_present_reconciliation_is_revision_fenced_and_rejected(
    p8_runtime: DatabaseRuntime,
) -> None:
    collections = _collection_repository(p8_runtime)
    collections.upsert_native(
        {"id": 212, "name": "Present Reconcile", "parts": []},
        existing_emby_id="present-box",
    )
    operations = _operation_repository(p8_runtime)
    staged = PostgresCollectionEffectGateway(operations).stage_native_delete(
        "present-box",
        principal_id="collection-admin:fixture",
        idempotency_key="present-reconcile",
    )
    claim = operations.claim_by_id(staged.effect_id, worker_id="fixture", lease_seconds=30)
    assert claim is not None and claim.claim_token is not None
    indeterminate = operations.mark_indeterminate(
        staged.effect_id,
        claim.claim_token,
        state={"fixture": True},
        error={"type": "fixture_unknown"},
    )
    remote_state = RemoteCollectionState("present-box", (), "212")
    reconciler = CollectionOperationReconciler(operations, FakeRemote([remote_state]))

    with pytest.raises(CollectionOperationStateError):
        reconciler.reconcile(
            indeterminate.id,
            expected_state_revision=indeterminate.state_revision - 1,
            resolved_by="collection-admin:fixture",
        )
    reconciled = reconciler.reconcile(
        indeterminate.id,
        expected_state_revision=indeterminate.state_revision,
        resolved_by="collection-admin:fixture",
    )

    assert reconciled is not None and reconciled.status is CollectionOperationStatus.REJECTED
    assert reconciled.error is not None and reconciled.error["type"] == "provider_write_not_observed"
    assert collections.get_native("212")["is_active"] is True  # type: ignore[index]


def test_stale_prewrite_and_postcheckpoint_claims_recover_to_distinct_states(
    p8_runtime: DatabaseRuntime,
) -> None:
    gateway = PostgresCollectionEffectGateway(_operation_repository(p8_runtime))
    prewrite = gateway.stage_native_delete(
        "prewrite-box",
        principal_id="collection-admin:fixture",
        idempotency_key="prewrite",
    )
    postwrite = gateway.stage_native_delete(
        "postwrite-box",
        principal_id="collection-admin:fixture",
        idempotency_key="postwrite",
    )
    operations = _operation_repository(p8_runtime)
    pre_claim = operations.claim_by_id(prewrite.effect_id, worker_id="crashed-pre", lease_seconds=30)
    post_claim = operations.claim_by_id(postwrite.effect_id, worker_id="crashed-post", lease_seconds=30)
    assert pre_claim is not None and pre_claim.claim_token is not None
    assert post_claim is not None and post_claim.claim_token is not None
    operations.begin_provider_step(
        postwrite.effect_id,
        post_claim.claim_token,
        step=CollectionOperationStep.DELETE_SHELL,
        state={"emby_collection_id": "postwrite-box"},
    )
    _expire_claim(p8_runtime, prewrite.effect_id)
    _expire_claim(p8_runtime, postwrite.effect_id)

    assert operations.expire_stale_claims(retry_delay_seconds=0) == 2
    recovered_pre = operations.read(prewrite.effect_id)
    recovered_post = operations.read(postwrite.effect_id)
    assert recovered_pre is not None and recovered_pre.status is CollectionOperationStatus.RETRYABLE
    assert recovered_post is not None and recovered_post.status is CollectionOperationStatus.AWAITING_READBACK
    with p8_runtime.engine.connect() as connection:
        statuses = connection.execute(
            text("SELECT aggregate_id, status FROM outbox_events ORDER BY aggregate_id"),
        ).all()
    assert {status for _aggregate_id, status in statuses} == {"pending"}
    with pytest.raises(CollectionOperationStateError):
        operations.mark_confirmed(
            prewrite.effect_id,
            pre_claim.claim_token,
            state={},
            result={},
        )


def test_same_scope_concurrent_reservation_has_one_owner_and_two_workers_have_one_claim(
    p8_runtime: DatabaseRuntime,
) -> None:
    gateway = PostgresCollectionEffectGateway(_operation_repository(p8_runtime))
    barrier = Barrier(2)

    def reserve(key: str) -> UUID | type[BaseException]:
        barrier.wait(timeout=5)
        try:
            return gateway.stage_native_delete(
                "shared-box",
                principal_id="collection-admin:fixture",
                idempotency_key=key,
            ).effect_id
        except BaseException as exc:
            return type(exc)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(reserve, ("first", "second")))
    operation_ids = [value for value in results if isinstance(value, UUID)]
    errors = [value for value in results if isinstance(value, type)]
    assert len(operation_ids) == 1
    assert errors == [CollectionOperationScopeBusyError]

    operations = _operation_repository(p8_runtime)
    first_claim = operations.claim_by_id(operation_ids[0], worker_id="worker-a", lease_seconds=30)
    second_claim = operations.claim_by_id(operation_ids[0], worker_id="worker-b", lease_seconds=30)
    assert first_claim is not None and second_claim is None


def test_survival_404_deactivates_and_confirms_in_one_transaction(
    p8_runtime: DatabaseRuntime,
) -> None:
    collections = _collection_repository(p8_runtime)
    collections.upsert_native(
        {"id": 303, "name": "Survival", "parts": []},
        existing_emby_id="survival-box",
    )
    staged = PostgresCollectionEffectGateway(_operation_repository(p8_runtime)).stage_removal_survival_check(
        "survival-box",
        event_identity="event-303",
    )

    result = _executor(p8_runtime, FakeRemote([None])).execute(staged.effect_id)

    assert result is not None and result.status is CollectionOperationStatus.CONFIRMED
    assert result.result is not None and result.result["survived"] is False
    assert collections.get_native("303")["is_active"] is False  # type: ignore[index]
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT status FROM outbox_events WHERE aggregate_id = :id"),
                {"id": str(staged.effect_id)},
            )
            == "processed"
        )


def test_missing_cleanup_reserves_verified_child_and_cleans_local_state_before_confirmation(
    p8_runtime: DatabaseRuntime,
) -> None:
    collections = _collection_repository(p8_runtime)
    collections.upsert_native(
        {
            "id": 404,
            "name": "Missing",
            "parts": [],
            "poster_path": "/poster.jpg",
            "backdrop_path": "/backdrop.jpg",
        },
        existing_emby_id="missing-box",
    )
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_image_policy_cache (
                    item_type, tmdb_id, season_number, episode_number, policy_json, images_json
                ) VALUES (
                    'BoxSet', '404', -1, -1, '[]'::jsonb,
                    '[{"source_url":"https://images.example/policy.jpg"}]'::jsonb
                )
                """
            )
        )
    staged = PostgresCollectionEffectGateway(_operation_repository(p8_runtime)).stage_missing_cleanup("404")
    remote = FakeRemote(
        listed=(
            RemoteCollectionState("missing-box", (), "404"),
            RemoteCollectionState("wrong-box", (), "999"),
        )
    )
    assets = FakeAssets()

    result = _executor(p8_runtime, remote, assets).execute(staged.effect_id)

    assert result is not None and result.status is CollectionOperationStatus.CONFIRMED
    assert collections.get_native("404") is None
    assert result.result is not None and len(result.result["child_operation_ids"]) == 1
    assert assets.discard_calls == [
        (
            "https://image.tmdb.org/t/p/original/poster.jpg",
            "https://image.tmdb.org/t/p/original/backdrop.jpg",
            "https://images.example/policy.jpg",
        )
    ]
    child_id = UUID(result.result["child_operation_ids"][0])
    child = _operation_repository(p8_runtime).read(child_id)
    assert child is not None and child.kind is CollectionOperationKind.NATIVE_DELETE
    assert child.request["expected_tmdb_collection_id"] == "404"


def test_missing_cleanup_waits_when_verified_remote_scope_has_non_delete_owner(
    p8_runtime: DatabaseRuntime,
) -> None:
    collections = _collection_repository(p8_runtime)
    collections.upsert_native(
        {"id": 505, "name": "Busy Missing", "parts": []},
        existing_emby_id="busy-box",
    )
    gateway = PostgresCollectionEffectGateway(_operation_repository(p8_runtime))
    gateway.stage_removal_survival_check("busy-box", event_identity="busy-event")
    parent = gateway.stage_missing_cleanup("505")
    remote = FakeRemote(listed=(RemoteCollectionState("busy-box", (), "505"),))

    result = _executor(p8_runtime, remote).execute(parent.effect_id)

    assert result is not None and result.status is CollectionOperationStatus.RETRYABLE
    assert collections.get_native("505") is not None
    assert result.state.get("child_reservation_complete") is not True


def test_missing_cleanup_becomes_indeterminate_when_verified_child_is_indeterminate(
    p8_runtime: DatabaseRuntime,
) -> None:
    collections = _collection_repository(p8_runtime)
    collections.upsert_native(
        {"id": 515, "name": "Blocked Missing", "parts": []},
        existing_emby_id="blocked-box",
    )
    operations = _operation_repository(p8_runtime)
    child = operations.reserve(
        CollectionOperationIntent.create(
            kind=CollectionOperationKind.NATIVE_DELETE,
            scope_key="emby-collection:blocked-box",
            principal_id="collection-system:fixture",
            request={
                "emby_collection_id": "blocked-box",
                "expected_tmdb_collection_id": "515",
                "reason": "fixture",
            },
            idempotency_key="blocked-child",
        )
    ).operation
    claimed = operations.claim_by_id(child.id, worker_id="fixture-blocker", lease_seconds=30)
    assert claimed is not None and claimed.claim_token is not None
    blocker = operations.mark_indeterminate(
        child.id,
        claimed.claim_token,
        state={},
        error={"type": "fixture_indeterminate"},
    )
    parent = PostgresCollectionEffectGateway(operations).stage_missing_cleanup("515")
    remote = FakeRemote(listed=(RemoteCollectionState("blocked-box", (), "515"),))

    result = _executor(p8_runtime, remote).execute(parent.effect_id)

    assert result is not None and result.status is CollectionOperationStatus.INDETERMINATE
    assert result.error is not None
    assert result.error["blocker_operation_id"] == str(blocker.id)
    assert collections.get_native("515") is not None


def test_missing_cleanup_does_not_reuse_admin_delete_without_expected_tmdb_identity(
    p8_runtime: DatabaseRuntime,
) -> None:
    collections = _collection_repository(p8_runtime)
    collections.upsert_native(
        {"id": 525, "name": "Admin-owned Missing", "parts": []},
        existing_emby_id="admin-owned-box",
    )
    gateway = PostgresCollectionEffectGateway(_operation_repository(p8_runtime))
    gateway.stage_native_delete(
        "admin-owned-box",
        principal_id="collection-admin:fixture",
        idempotency_key="admin-owner",
    )
    parent = gateway.stage_missing_cleanup("525")
    remote = FakeRemote(listed=(RemoteCollectionState("admin-owned-box", (), "525"),))

    result = _executor(p8_runtime, remote).execute(parent.effect_id)

    assert result is not None and result.status is CollectionOperationStatus.RETRYABLE
    assert collections.get_native("525") is not None


def test_missing_local_asset_cleanup_exhaustion_is_indeterminate_not_confirmed(
    p8_runtime: DatabaseRuntime,
) -> None:
    collections = _collection_repository(p8_runtime)
    collections.upsert_native(
        {"id": 535, "name": "Cleanup Failure", "parts": [], "poster_path": "/poster.jpg"},
    )
    staged = PostgresCollectionEffectGateway(_operation_repository(p8_runtime)).stage_missing_cleanup("535")
    assets = FakeAssets()
    assets.discard_error = RuntimeError("fixture cache cleanup failure")
    executor = _executor(p8_runtime, FakeRemote(listed=()), assets)

    result = executor.execute(staged.effect_id)
    while result is not None and not result.terminal:
        result = executor.execute(staged.effect_id)

    assert result is not None and result.status is CollectionOperationStatus.INDETERMINATE
    assert result.error is not None and result.error["type"] == "missing_image_cleanup_exhausted"
    assert collections.get_native("535") is None


def test_image_archive_receipts_are_durable_and_recovery_reads_before_archive(
    p8_runtime: DatabaseRuntime,
) -> None:
    collections = _collection_repository(p8_runtime)
    collections.upsert_native(
        {
            "id": 606,
            "name": "Images",
            "parts": [],
            "poster_path": "/poster.jpg",
            "backdrop_path": "/backdrop.jpg",
        }
    )
    primary = "https://image.tmdb.org/t/p/original/poster.jpg"
    backdrop = "https://image.tmdb.org/t/p/original/backdrop.jpg"
    staged = PostgresCollectionEffectGateway(_operation_repository(p8_runtime)).stage_image_archives(
        {
            "tmdb_id": "606",
            "images": {
                "primary": primary,
                "backdrop": backdrop,
                "logo": None,
                "thumb": backdrop,
            },
        }
    )[0]
    operations = _operation_repository(p8_runtime)
    claim = operations.claim_by_id(staged.effect_id, worker_id="crashed-image", lease_seconds=30)
    assert claim is not None and claim.claim_token is not None
    operations.begin_provider_step(
        staged.effect_id,
        claim.claim_token,
        step=CollectionOperationStep.ARCHIVE_IMAGES,
        state={"receipts": {}, "pending_slot": "primary", "pending_source": primary},
    )
    _expire_claim(p8_runtime, staged.effect_id)
    assert operations.expire_stale_claims(retry_delay_seconds=0) == 1
    assets = FakeAssets()
    assets.archived[primary] = {
        "source_url": primary,
        "content_hash": "b" * 64,
        "relative_path": "bb/recovered.jpg",
        "mime_type": "image/jpeg",
        "byte_size": 321,
    }

    result = _executor(p8_runtime, FakeRemote(), assets).execute(staged.effect_id)

    assert result is not None and result.status is CollectionOperationStatus.CONFIRMED
    assert primary not in assets.archive_calls
    assert assets.archive_calls == [backdrop]
    assert result.result is not None
    assert result.result["stale_snapshot"] is False
    assert result.result["receipts"]["primary"]["content_sha256"] == "b" * 64
    assert result.result["receipts"]["thumb"]["content_sha256"] == "a" * 64


def test_stale_image_cleanup_exhaustion_is_indeterminate_and_does_not_rearchive(
    p8_runtime: DatabaseRuntime,
) -> None:
    collections = _collection_repository(p8_runtime)
    collections.upsert_native(
        {
            "id": 616,
            "name": "Stale Images",
            "parts": [],
            "poster_path": "/poster.jpg",
        }
    )
    primary = "https://image.tmdb.org/t/p/original/poster.jpg"
    staged = PostgresCollectionEffectGateway(_operation_repository(p8_runtime)).stage_image_archives(
        {"tmdb_id": "616", "images": {"primary": primary}}
    )[0]
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE collections_info
                SET poster_path = '/changed.jpg'
                WHERE tmdb_collection_id = '616'
                """
            )
        )
    assets = FakeAssets()
    assets.discard_error = RuntimeError("fixture stale cleanup failure")
    executor = _executor(p8_runtime, FakeRemote(), assets)

    result = executor.execute(staged.effect_id)
    while result is not None and not result.terminal:
        result = executor.execute(staged.effect_id)

    assert result is not None and result.status is CollectionOperationStatus.INDETERMINATE
    assert result.error is not None and result.error["type"] == "stale_image_cleanup_exhausted"
    assert result.state["stale_snapshot"] is True
    assert assets.archive_calls == [primary]
    assert len(assets.discard_calls) == 12
