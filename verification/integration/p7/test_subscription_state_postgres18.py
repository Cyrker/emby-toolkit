from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etk.domains.identity.repository import SqlAlchemyIdentityRepository
from etk.domains.media.subscription_projection import SqlAlchemySubscriptionMediaProjection
from etk.domains.subscription.core import (
    PostgresCoreSubscriptionOperationStager,
    SqlAlchemySubscriptionCoreRepository,
    SubscriptionCoreService,
    SubscriptionStateConflictError,
)
from etk.domains.subscription.moviepilot import (
    MoviePilotProviderError,
    MoviePilotProviderErrorKind,
    MoviePilotQueryDisposition,
    MoviePilotQueryResult,
    MoviePilotWriteOutcome,
    MoviePilotWriteResult,
)
from etk.domains.subscription.operation_repository import PostgresSubscriptionOperationRepository
from etk.domains.subscription.provider_execution import SubscriptionOperationExecutor
from etk.domains.system.repository import SqlAlchemyAppSettingsRepository
from etk.platform.db import DatabaseRuntime
from etk.platform.jobs import PostgresJobStore


class _Gateway:
    def __init__(self, *, reject_delete: bool = False) -> None:
        self.reject_delete = reject_delete
        self.deletes: list[tuple[str, int | None]] = []
        self.queries: list[tuple[str, int | None]] = []

    def query_subscription(self, tmdb_id: int | str, *, season: int | None = None) -> MoviePilotQueryResult:
        self.queries.append((str(tmdb_id), season))
        return MoviePilotQueryResult(MoviePilotQueryDisposition.EMPTY, None)

    def create_subscription(self, payload: Mapping[str, Any]) -> MoviePilotWriteResult:
        del payload
        raise AssertionError("unexpected create")

    def update_subscription(self, payload: Mapping[str, Any]) -> MoviePilotWriteResult:
        del payload
        raise AssertionError("unexpected update")

    def update_subscription_status(
        self,
        tmdb_id: int | str,
        status: str,
        *,
        season: int | None = None,
        total_episodes: int | None = None,
    ) -> MoviePilotWriteResult:
        del tmdb_id, status, season, total_episodes
        raise AssertionError("unexpected status update")

    def delete_subscription(self, tmdb_id: int | str, *, season: int | None = None) -> MoviePilotWriteResult:
        self.deletes.append((str(tmdb_id), season))
        if self.reject_delete:
            raise MoviePilotProviderError(
                MoviePilotProviderErrorKind.REJECTED,
                "delete-subscription",
                "fixture rejection",
                status_code=400,
                outcome=MoviePilotWriteOutcome.REJECTED,
            )
        return MoviePilotWriteResult("delete-subscription", 204)


class _Availability:
    @staticmethod
    def status() -> Mapping[str, bool]:
        return {
            "mp_configured": True,
            "hdhive_configured": False,
            "tg_userbot_configured": False,
            "shared_pool_configured": False,
            "cloud_search_configured": False,
        }


def _settings(runtime: DatabaseRuntime) -> SqlAlchemyAppSettingsRepository:
    return SqlAlchemyAppSettingsRepository(runtime.unit_of_work)


def _repository(runtime: DatabaseRuntime) -> SqlAlchemySubscriptionCoreRepository:
    return SqlAlchemySubscriptionCoreRepository(
        runtime.unit_of_work,
        SqlAlchemySubscriptionMediaProjection(),
        PostgresCoreSubscriptionOperationStager(),
        _settings(runtime),
    )


def _enable_moviepilot(runtime: DatabaseRuntime) -> None:
    _settings(runtime).save("mp_config", {"moviepilot_url": "https://moviepilot.invalid"})


def _insert_media(
    runtime: DatabaseRuntime,
    *,
    tmdb_id: str,
    item_type: str = "Movie",
    status: str = "SUBSCRIBED",
    in_library: bool = False,
    sources: list[dict[str, Any]] | None = None,
    parent_series_tmdb_id: str | None = None,
    season_number: int | None = None,
    title: str | None = None,
) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM cleanup_index WHERE tmdb_id = :tmdb_id AND item_type = :item_type"),
            {"tmdb_id": tmdb_id, "item_type": item_type},
        )
        connection.execute(
            text("DELETE FROM resubscribe_index WHERE tmdb_id = :tmdb_id AND item_type = :item_type"),
            {"tmdb_id": tmdb_id, "item_type": item_type},
        )
        connection.execute(
            text("DELETE FROM media_metadata WHERE tmdb_id = :tmdb_id AND item_type = :item_type"),
            {"tmdb_id": tmdb_id, "item_type": item_type},
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, in_library, subscription_status,
                    subscription_sources_json, parent_series_tmdb_id, season_number,
                    first_requested_at, last_subscribed_at
                ) VALUES (
                    :tmdb_id, :item_type, :title, :in_library, :status,
                    CAST(:sources AS jsonb), :parent_series_tmdb_id, :season_number,
                    NOW(), CASE WHEN :status = 'SUBSCRIBED' THEN NOW() ELSE NULL END
                )
                """
            ),
            {
                "tmdb_id": tmdb_id,
                "item_type": item_type,
                "title": title or f"Fixture {tmdb_id}",
                "in_library": in_library,
                "status": status,
                "sources": json.dumps(sources or [{"type": "manual_subscribe"}]),
                "parent_series_tmdb_id": parent_series_tmdb_id,
                "season_number": season_number,
            },
        )


def test_release_commits_final_state_operation_outbox_and_no_quota_for_one_generation(
    p7_runtime: DatabaseRuntime,
) -> None:
    _enable_moviepilot(p7_runtime)
    _insert_media(p7_runtime, tmdb_id="p7-state-901")
    repository = _repository(p7_runtime)

    first = repository.transition(
        {"tmdb_id": "p7-state-901", "item_type": "Movie", "new_status": "NONE"},
        "NONE",
    )
    second = repository.transition(
        {
            "tmdb_id": "p7-state-901",
            "item_type": "Movie",
            "new_status": "IGNORED",
            "source": {"type": "manual_ignore", "opaque": {"kept": True}},
        },
        "IGNORED",
    )

    assert first.operation_id is not None
    assert second.operation_id is None
    with p7_runtime.engine.connect() as connection:
        state = (
            connection.execute(
                text(
                    """
                SELECT
                    (SELECT subscription_status FROM media_metadata
                     WHERE tmdb_id = 'p7-state-901' AND item_type = 'Movie') AS media_status,
                    (SELECT status FROM subscription_operations WHERE id = :operation_id) AS operation_status,
                    (SELECT status FROM outbox_events
                     WHERE aggregate_type = 'subscription-operation'
                       AND aggregate_id = CAST(:operation_id AS text)) AS outbox_status,
                    (SELECT COUNT(*) FROM subscription_quota_reservations
                     WHERE operation_id = :operation_id) AS quota_count,
                    (SELECT COUNT(*) FROM subscription_operations
                     WHERE scope_key LIKE :scope_pattern) AS operation_count
                """
                ),
                {
                    "operation_id": first.operation_id,
                    "scope_pattern": "subscription-state:movie:p7-state-901:%:delete",
                },
            )
            .mappings()
            .one()
        )
    assert dict(state) == {
        "media_status": "IGNORED",
        "operation_status": "ready",
        "outbox_status": "pending",
        "quota_count": 0,
        "operation_count": 1,
    }


def test_season_identity_and_opaque_ignored_state_are_frozen_before_provider_execution(
    p7_runtime: DatabaseRuntime,
) -> None:
    _enable_moviepilot(p7_runtime)
    _insert_media(p7_runtime, tmdb_id="p7-series-902", item_type="Series", status="NONE")
    _insert_media(
        p7_runtime,
        tmdb_id="p7-season-local-902",
        item_type="Season",
        parent_series_tmdb_id="p7-series-902",
        season_number=3,
    )
    source = {
        "type": "user_request",
        "user_id": "fixture-user-902",
        "opaque": {"channel": "manual"},
    }
    transition = _repository(p7_runtime).transition(
        {
            "tmdb_id": "p7-season-local-902",
            "item_type": "Season",
            "new_status": "IGNORED",
            "source": source,
            "ignore_reason": "fixture ignore",
        },
        "IGNORED",
    )
    assert transition.operation_id is not None
    with p7_runtime.engine.connect() as connection:
        before = (
            connection.execute(
                text(
                    """
                SELECT subscription_status, subscription_sources_json, ignore_reason
                FROM media_metadata
                WHERE tmdb_id = 'p7-season-local-902' AND item_type = 'Season'
                """
                )
            )
            .mappings()
            .one()
        )
    assert before["subscription_status"] == "IGNORED"
    assert source in before["subscription_sources_json"]
    assert before["ignore_reason"] == "fixture ignore"

    gateway = _Gateway()
    execution = SubscriptionOperationExecutor(
        PostgresSubscriptionOperationRepository(p7_runtime.unit_of_work),
        gateway,
        readback_delay_seconds=0,
    ).execute(
        transition.operation_id,
        worker_id="fixture-subscription-state",
        lease_seconds=30,
        now=datetime.now(UTC) + timedelta(seconds=1),
    )

    assert execution is not None and execution.operation.status.value == "confirmed"
    assert gateway.deletes == [("p7-series-902", 3)]
    with p7_runtime.engine.connect() as connection:
        after = connection.scalar(
            text(
                """
                SELECT subscription_status FROM media_metadata
                WHERE tmdb_id = 'p7-season-local-902' AND item_type = 'Season'
                """
            )
        )
    assert after == "IGNORED"


def test_provider_rejection_does_not_revert_committed_local_desired_state(
    p7_runtime: DatabaseRuntime,
) -> None:
    _enable_moviepilot(p7_runtime)
    _insert_media(p7_runtime, tmdb_id="p7-state-903")
    transition = _repository(p7_runtime).transition(
        {"tmdb_id": "p7-state-903", "item_type": "Movie", "new_status": "NONE"},
        "NONE",
    )
    assert transition.operation_id is not None

    execution = SubscriptionOperationExecutor(
        PostgresSubscriptionOperationRepository(p7_runtime.unit_of_work),
        _Gateway(reject_delete=True),
    ).execute(
        transition.operation_id,
        worker_id="fixture-subscription-state",
        lease_seconds=30,
        now=datetime.now(UTC) + timedelta(seconds=1),
    )

    assert execution is not None and execution.operation.status.value == "rejected"
    with p7_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text(
                    """
                SELECT subscription_status FROM media_metadata
                WHERE tmdb_id = 'p7-state-903' AND item_type = 'Movie'
                """
                )
            )
            == "NONE"
        )


def test_batch_delete_atomically_stages_provider_delete_resets_online_and_purges_offline(
    p7_runtime: DatabaseRuntime,
) -> None:
    _enable_moviepilot(p7_runtime)
    _insert_media(p7_runtime, tmdb_id="p7-batch-904", status="SUBSCRIBED", in_library=False)
    _insert_media(p7_runtime, tmdb_id="p7-batch-905", status="WANTED", in_library=True)
    _insert_media(p7_runtime, tmdb_id="p7-batch-906", status="WANTED", in_library=False)
    with p7_runtime.engine.begin() as connection:
        for tmdb_id in ("p7-batch-904", "p7-batch-905", "p7-batch-906"):
            connection.execute(
                text(
                    """
                    INSERT INTO cleanup_index (tmdb_id, item_type, status)
                    VALUES (:tmdb_id, 'Movie', 'pending')
                    """
                ),
                {"tmdb_id": tmdb_id},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO resubscribe_index (tmdb_id, item_type, season_number, status)
                    VALUES (:tmdb_id, 'Movie', -1, 'needed')
                    """
                ),
                {"tmdb_id": tmdb_id},
            )

    result = _repository(p7_runtime).batch_delete(
        [
            {"tmdb_id": "p7-batch-904", "item_type": "Movie"},
            {"tmdb_id": "p7-batch-905", "item_type": "Movie"},
            {"tmdb_id": "p7-batch-906", "item_type": "Movie"},
            {"tmdb_id": "p7-batch-906", "item_type": "Movie"},
        ]
    )

    assert result.deleted_count == 2
    assert result.reset_count == 1
    assert result.operation_count == 1
    with p7_runtime.engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT tmdb_id, subscription_status
                FROM media_metadata
                WHERE tmdb_id IN ('p7-batch-904','p7-batch-905','p7-batch-906')
                ORDER BY tmdb_id
                """
            )
        ).all()
        operation_id = connection.scalar(
            text(
                """
                SELECT id FROM subscription_operations
                WHERE scope_key LIKE :scope_pattern
                """
            ),
            {"scope_pattern": "subscription-state:movie:p7-batch-904:%:delete"},
        )
        cleanup_ids = tuple(
            connection.execute(
                text("SELECT tmdb_id FROM cleanup_index WHERE tmdb_id LIKE 'p7-batch-%' ORDER BY tmdb_id")
            ).scalars()
        )
        resubscribe_ids = tuple(
            connection.execute(
                text("SELECT tmdb_id FROM resubscribe_index WHERE tmdb_id LIKE 'p7-batch-%' ORDER BY tmdb_id")
            ).scalars()
        )
    assert rows == [("p7-batch-905", "NONE")]
    assert cleanup_ids == ("p7-batch-905",)
    assert resubscribe_ids == ("p7-batch-905",)
    assert operation_id is not None

    execution = SubscriptionOperationExecutor(
        PostgresSubscriptionOperationRepository(p7_runtime.unit_of_work),
        _Gateway(),
        readback_delay_seconds=0,
    ).execute(
        operation_id,
        worker_id="fixture-subscription-batch-delete",
        lease_seconds=30,
        now=datetime.now(UTC) + timedelta(seconds=1),
    )
    assert execution is not None and execution.operation.status.value == "confirmed"
    with p7_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT COUNT(*) FROM subscription_quota_reservations WHERE operation_id = :id"),
                {"id": operation_id},
            )
            == 0
        )


def test_media_mutation_failure_rolls_back_operation_and_outbox(p7_runtime: DatabaseRuntime) -> None:
    _enable_moviepilot(p7_runtime)
    _insert_media(p7_runtime, tmdb_id="p7-state-907")
    constraint = "verification_p7_none_atomic"
    with p7_runtime.engine.connect() as connection:
        before_operations = int(connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) or 0)
        before_outbox = int(connection.scalar(text("SELECT COUNT(*) FROM outbox_events")) or 0)
    with p7_runtime.engine.begin() as connection:
        connection.execute(text(f"ALTER TABLE media_metadata DROP CONSTRAINT IF EXISTS {constraint}"))
        connection.execute(
            text(
                f"""
                ALTER TABLE media_metadata ADD CONSTRAINT {constraint}
                CHECK (subscription_status <> 'NONE') NOT VALID
                """
            )
        )
    try:
        with pytest.raises(IntegrityError):
            _repository(p7_runtime).transition(
                {"tmdb_id": "p7-state-907", "item_type": "Movie", "new_status": "NONE"},
                "NONE",
            )
        with p7_runtime.engine.connect() as connection:
            assert (
                connection.scalar(text("SELECT subscription_status FROM media_metadata WHERE tmdb_id = 'p7-state-907'"))
                == "SUBSCRIBED"
            )
            assert (
                int(connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) or 0) == before_operations
            )
            assert int(connection.scalar(text("SELECT COUNT(*) FROM outbox_events")) or 0) == before_outbox
    finally:
        with p7_runtime.engine.begin() as connection:
            connection.execute(text(f"ALTER TABLE media_metadata DROP CONSTRAINT IF EXISTS {constraint}"))


def test_batch_delete_rejects_active_cleanup_claim_without_local_changes(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_media(p7_runtime, tmdb_id="p7-batch-908", status="WANTED", in_library=False)
    with p7_runtime.engine.begin() as connection:
        cleanup_id = connection.scalar(
            text(
                """
                INSERT INTO cleanup_index (tmdb_id, item_type, status)
                VALUES ('p7-batch-908', 'Movie', 'pending')
                RETURNING id
                """
            )
        )
    job = PostgresJobStore(p7_runtime.unit_of_work).create_job(
        task_key="execute-cleanup",
        payload={"fixture": True},
        max_attempts=1,
    )
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO cleanup_execution_items (
                    id, execution_key, job_id, cleanup_index_id, item_key, snapshot_json, status
                ) VALUES (
                    :id, :execution_key, :job_id, :cleanup_index_id, :item_key, '{}'::jsonb, 'queued'
                )
                """
            ),
            {
                "id": uuid4(),
                "execution_key": "9" * 64,
                "job_id": job.job.id,
                "cleanup_index_id": cleanup_id,
                "item_key": f"cleanup:{cleanup_id}",
            },
        )

    with pytest.raises(SubscriptionStateConflictError):
        _repository(p7_runtime).batch_delete([{"tmdb_id": "p7-batch-908", "item_type": "Movie"}])

    with p7_runtime.engine.connect() as connection:
        assert (
            connection.scalar(text("SELECT subscription_status FROM media_metadata WHERE tmdb_id = 'p7-batch-908'"))
            == "WANTED"
        )
        assert (
            connection.scalar(
                text("SELECT COUNT(*) FROM cleanup_index WHERE id = :id"),
                {"id": cleanup_id},
            )
            == 1
        )


def test_status_guards_and_pending_release_keep_distinct_business_semantics(
    p7_runtime: DatabaseRuntime,
) -> None:
    _insert_media(p7_runtime, tmdb_id="p7-guard-909", status="SUBSCRIBED")
    _insert_media(p7_runtime, tmdb_id="p7-guard-910", status="PAUSED")
    repository = _repository(p7_runtime)

    with pytest.raises(SubscriptionStateConflictError):
        repository.transition(
            {"tmdb_id": "p7-guard-909", "item_type": "Movie", "new_status": "WANTED"},
            "WANTED",
        )
    with pytest.raises(SubscriptionStateConflictError):
        repository.transition(
            {"tmdb_id": "p7-guard-910", "item_type": "Movie", "new_status": "PENDING_RELEASE"},
            "PENDING_RELEASE",
        )
    pending = repository.transition(
        {
            "tmdb_id": "p7-pending-911",
            "item_type": "Movie",
            "new_status": "PENDING_RELEASE",
            "title": "Pending Fixture",
        },
        "PENDING_RELEASE",
    )

    assert pending.operation_id is None
    with p7_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                SELECT subscription_status, subscription_sources_json
                FROM media_metadata WHERE tmdb_id = 'p7-pending-911' AND item_type = 'Movie'
                """
                )
            )
            .mappings()
            .one()
        )
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) == 0
    assert dict(row) == {"subscription_status": "PENDING_RELEASE", "subscription_sources_json": []}


def test_new_subscription_generation_creates_a_new_delete_operation(p7_runtime: DatabaseRuntime) -> None:
    _enable_moviepilot(p7_runtime)
    _insert_media(p7_runtime, tmdb_id="p7-generation-912")
    repository = _repository(p7_runtime)

    first = repository.transition(
        {"tmdb_id": "p7-generation-912", "item_type": "Movie", "new_status": "NONE"},
        "NONE",
    )
    repository.transition(
        {
            "tmdb_id": "p7-generation-912",
            "item_type": "Movie",
            "new_status": "SUBSCRIBED",
            "source": {"type": "manual_subscribe", "generation": 2},
        },
        "SUBSCRIBED",
    )
    second = repository.transition(
        {"tmdb_id": "p7-generation-912", "item_type": "Movie", "new_status": "NONE"},
        "NONE",
    )

    assert first.operation_id is not None and second.operation_id is not None
    assert first.operation_id != second.operation_id
    with p7_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text(
                    """
                SELECT COUNT(*) FROM subscription_operations
                WHERE kind = 'delete' AND scope_key LIKE :scope_pattern
                """
                ),
                {"scope_pattern": "subscription-state:movie:p7-generation-912:%:delete"},
            )
            == 2
        )


def test_management_service_enriches_user_without_mutating_opaque_projection(
    p7_runtime: DatabaseRuntime,
) -> None:
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO emby_users (id, name, is_administrator)
                VALUES ('fixture-user-913', 'Fixture User', FALSE)
                ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
                """
            )
        )
    sources = [
        {"type": "manual_custom", "nested": {"value": 1}},
        {"type": "user_request", "user_id": "fixture-user-913", "opaque": "kept"},
    ]
    _insert_media(p7_runtime, tmdb_id="p7-state-913", status="WANTED", sources=sources)
    service = SubscriptionCoreService(
        _repository(p7_runtime),
        _Availability(),
        SqlAlchemyIdentityRepository(p7_runtime.unit_of_work),
    )

    result = service.list_subscriptions()
    assert result.status_code == 200
    item = next(value for value in result.payload if value["tmdb_id"] == "p7-state-913")
    assert item["subscription_sources_json"] == [
        {"type": "manual_custom", "nested": {"value": 1}},
        {
            "type": "user_request",
            "user_id": "fixture-user-913",
            "opaque": "kept",
            "user": "Fixture User",
        },
    ]
    with p7_runtime.engine.connect() as connection:
        stored = connection.scalar(
            text("SELECT subscription_sources_json FROM media_metadata WHERE tmdb_id = 'p7-state-913'")
        )
    assert stored == sources
