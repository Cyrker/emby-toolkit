from __future__ import annotations

from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text

from etk.domains.media.subscription_projection import (
    SqlAlchemySubscriptionMediaProjection,
    SubscriptionMediaKey,
)
from etk.domains.sharing.execution import SharingDeliveryOutcome, SharingOperationExecutor
from etk.domains.sharing.operations import SharingOperationKind, SharingOperationStatus, sharing_operation_key
from etk.domains.sharing.repository import PostgresSharingOperationRepository
from etk.domains.subscription.auto_subscribe import (
    AutoSourceDisposition,
    AutoSubscriptionClaimDisposition,
    AutoSubscriptionExecution,
)
from etk.domains.subscription.auto_subscribe_repository import SqlAlchemyAutoSubscriptionRepository
from etk.integrations import ProviderDisposition, ProviderResponse
from etk.platform.db import DatabaseRuntime, SqlAlchemyUnitOfWork
from etk.platform.jobs import PersistentExecutionCancelled
from etk.processes.p7_auto_subscribe_sources import PersistentOrderedAutoSubscriptionSources


class _FailingSubscriptionMediaProjection(SqlAlchemySubscriptionMediaProjection):
    def set_status(
        self,
        uow: SqlAlchemyUnitOfWork,
        key: SubscriptionMediaKey,
        status: str,
        request: Mapping[str, Any],
        source: Mapping[str, Any] | None,
        ignore_reason: str | None,
    ) -> int:
        super().set_status(uow, key, status, request, source, ignore_reason)
        raise RuntimeError("injected projection failure")


def test_auto_subscription_worklist_and_lifecycle_projection(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = SqlAlchemyAutoSubscriptionRepository(
        p7_runtime.unit_of_work,
        SqlAlchemySubscriptionMediaProjection(),
    )
    fixture_ids = ["990000", "990001", "990002", "990003", "990010", "990011", "990012"]
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM media_metadata WHERE tmdb_id = ANY(CAST(:ids AS text[]))"),
            {"ids": fixture_ids},
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata(
                    tmdb_id, item_type, title, release_date, subscription_status,
                    in_library, first_requested_at, last_subscribed_at,
                    parent_series_tmdb_id, season_number, episode_number,
                    subscription_sources_json, last_updated_at
                ) VALUES
                    ('990000', 'Series', 'Fixture Series', '2020-01-01', 'NONE', FALSE,
                     NOW() - INTERVAL '20 days', NULL, NULL, NULL, NULL, '[]'::jsonb, NOW()),
                    ('990001', 'Movie', 'Pending Movie', CURRENT_DATE - 1, 'PENDING_RELEASE', FALSE,
                     NOW() - INTERVAL '10 days', NULL, NULL, NULL, NULL, '[]'::jsonb, NOW()),
                    ('990002', 'Movie', 'Wanted Movie', '2020-01-01', 'WANTED', FALSE,
                     NOW() - INTERVAL '9 days', NULL, NULL, NULL, NULL, '[]'::jsonb, NOW()),
                    ('990003', 'Movie', 'Subscribed Movie', '2020-01-01', 'SUBSCRIBED', FALSE,
                     NOW() - INTERVAL '8 days', NOW() - INTERVAL '8 days', NULL, NULL, NULL,
                     '[]'::jsonb, NOW()),
                    ('990010', 'Season', 'Fixture S02', '2020-01-01', 'SUBSCRIBED', TRUE,
                     NOW() - INTERVAL '7 days', NOW() - INTERVAL '7 days', '990000', 2, NULL,
                     '[]'::jsonb, NOW()),
                    ('990011', 'Episode', 'Fixture S02E01', CURRENT_DATE - 1, 'NONE', FALSE,
                     NULL, NULL, '990000', 2, 1, '[]'::jsonb, NOW()),
                    ('990012', 'Episode', 'Fixture S02E02', CURRENT_DATE + 1, 'NONE', FALSE,
                     NULL, NULL, '990000', 2, 2, '[]'::jsonb, NOW())
                """
            )
        )

    assert repository.promote_pending_to_wanted() == 1
    worklist = [dict(item) for item in repository.worklist()]

    assert [(item["tmdb_id"], item["item_type"]) for item in worklist] == [
        ("990001", "Movie"),
        ("990002", "Movie"),
        ("990010", "Season"),
        ("990003", "Movie"),
    ]
    season = next(item for item in worklist if item["tmdb_id"] == "990010")
    assert season["missing_episode_numbers"] == [1]
    assert season["missing_episode_count"] == 1
    assert repository.series_title("990000") == "Fixture Series"

    execution = AutoSubscriptionExecution("job-1", "attempt-1", "worker-1")
    pause_item = next(item for item in repository.movies_to_pause(search_window_days=1) if item["tmdb_id"] == "990003")
    pause_claim = repository.claim(pause_item, phase="pause", execution=execution)
    repository.begin_source(
        pause_claim,
        source_type="mp",
        replay_safe=True,
        execution=execution,
    )
    repository.complete_movie_maintenance(
        pause_claim,
        tmdb_id="990003",
        action="pause",
        pause_days=7,
    )
    with p7_runtime.engine.connect() as connection:
        paused = (
            connection.execute(
                text(
                    """
                    SELECT subscription_status, paused_until > NOW() AS future_pause
                    FROM media_metadata
                    WHERE tmdb_id = '990003' AND item_type = 'Movie'
                    """
                )
            )
            .mappings()
            .one()
        )
    assert dict(paused) == {"subscription_status": "PAUSED", "future_pause": True}

    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE media_metadata
                SET paused_until = NOW() - INTERVAL '1 minute'
                WHERE tmdb_id = '990003' AND item_type = 'Movie'
                """
            )
        )
    assert [item["tmdb_id"] for item in repository.movies_to_revive()] == ["990003"]
    revive_item = repository.movies_to_revive()[0]
    revive_claim = repository.claim(revive_item, phase="revive", execution=execution)
    repository.begin_source(
        revive_claim,
        source_type="mp",
        replay_safe=True,
        execution=execution,
    )
    repository.complete_movie_maintenance(
        revive_claim,
        tmdb_id="990003",
        action="revive",
    )
    subscribe_claim = repository.claim(worklist[1], phase="subscribe", execution=execution)
    repository.begin_source(
        subscribe_claim,
        source_type="mp",
        replay_safe=True,
        execution=execution,
    )
    repository.complete_subscription(
        subscribe_claim,
        worklist[1],
        action_type="MP",
        mark_subscribed=True,
    )

    with p7_runtime.engine.connect() as connection:
        rows = {
            row["tmdb_id"]: dict(row)
            for row in connection.execute(
                text(
                    """
                    SELECT tmdb_id, subscription_status, paused_until
                    FROM media_metadata
                    WHERE tmdb_id IN ('990002', '990003')
                    ORDER BY tmdb_id
                    """
                )
            ).mappings()
        }
    assert rows["990002"]["subscription_status"] == "SUBSCRIBED"
    assert rows["990003"]["subscription_status"] == "SUBSCRIBED"
    assert rows["990003"]["paused_until"] is None


def test_auto_subscription_pause_query_uses_bounded_window(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = SqlAlchemyAutoSubscriptionRepository(
        p7_runtime.unit_of_work,
        SqlAlchemySubscriptionMediaProjection(),
    )
    with p7_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM media_metadata WHERE tmdb_id = '990020'"))
        connection.execute(
            text(
                """
                INSERT INTO media_metadata(
                    tmdb_id, item_type, title, release_date, subscription_status,
                    in_library, last_subscribed_at, subscription_sources_json, last_updated_at
                ) VALUES (
                    '990020', 'Movie', 'Pause Candidate', '2020-01-01', 'SUBSCRIBED',
                    FALSE, NOW() - INTERVAL '2 days', '[]'::jsonb, NOW()
                )
                """
            )
        )

    assert [item["tmdb_id"] for item in repository.movies_to_pause(search_window_days=1)] == ["990020"]
    assert repository.movies_to_pause(search_window_days=3) == ()


def test_auto_subscription_item_claim_fences_concurrent_jobs_and_recovers_replay_safe_source(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = SqlAlchemyAutoSubscriptionRepository(
        p7_runtime.unit_of_work,
        SqlAlchemySubscriptionMediaProjection(),
    )
    item = {
        "tmdb_id": "990030",
        "item_type": "Movie",
        "title": "Claimed Movie",
        "subscription_status": "WANTED",
        "first_requested_at": datetime(2026, 8, 30, tzinfo=UTC),
    }
    with p7_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM media_metadata WHERE tmdb_id = '990030'"))
        connection.execute(
            text(
                """
                INSERT INTO media_metadata(
                    tmdb_id, item_type, title, subscription_status, in_library,
                    first_requested_at, subscription_sources_json, last_updated_at
                ) VALUES (
                    '990030', 'Movie', 'Claimed Movie', 'WANTED', FALSE,
                    :first_requested_at, '[]'::jsonb, NOW()
                )
                """
            ),
            {"first_requested_at": item["first_requested_at"]},
        )

    executions = (
        AutoSubscriptionExecution("job-a", "attempt-a", "worker-a"),
        AutoSubscriptionExecution("job-b", "attempt-b", "worker-b"),
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = tuple(
            pool.map(
                lambda execution: repository.claim(item, phase="subscribe", execution=execution),
                executions,
            )
        )
    assert {claim.disposition for claim in claims} == {
        AutoSubscriptionClaimDisposition.ACQUIRED,
        AutoSubscriptionClaimDisposition.BUSY,
    }
    claimed_index = next(index for index, claim in enumerate(claims) if claim.acquired)
    claimed = claims[claimed_index]
    claimed_execution = executions[claimed_index]
    replacement_execution = executions[1 - claimed_index]
    with pytest.raises(RuntimeError, match="execution identity changed"):
        repository.begin_source(
            claimed,
            source_type="mp",
            replay_safe=True,
            execution=replacement_execution,
        )
    repository.begin_source(
        claimed,
        source_type="mp",
        replay_safe=True,
        execution=claimed_execution,
    )
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE subscription_runtime_states
                SET state_json = jsonb_set(
                    state_json,
                    '{lease_expires_at}',
                    to_jsonb(CAST(:expired AS text))
                )
                WHERE state_key = :state_key
                """
            ),
            {
                "state_key": claimed.state_key,
                "expired": (datetime.now(UTC) - timedelta(minutes=1)).isoformat(),
            },
        )

    with pytest.raises(RuntimeError, match="no longer valid"):
        repository.finish_source(claimed, disposition=AutoSourceDisposition.CONFIRMED)

    recovered = repository.claim(item, phase="subscribe", execution=replacement_execution)
    assert recovered.disposition is AutoSubscriptionClaimDisposition.ACQUIRED
    assert recovered.claim_token != claimed.claim_token
    with pytest.raises(RuntimeError, match="no longer valid"):
        repository.finish_source(claimed, disposition=AutoSourceDisposition.CONFIRMED)

    repository.begin_source(
        recovered,
        source_type="mp",
        replay_safe=True,
        execution=replacement_execution,
    )
    repository.complete_subscription(
        recovered,
        item,
        action_type="MP",
        mark_subscribed=True,
    )
    replay = repository.claim(item, phase="subscribe", execution=claimed_execution)
    assert replay.disposition is AutoSubscriptionClaimDisposition.REPLAYED
    with p7_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                SELECT subscription_status,
                       state_json ->> 'status' AS operation_status,
                       state_json ->> 'projection_applied' AS projection_applied
                FROM media_metadata
                JOIN subscription_runtime_states ON state_key = :state_key
                WHERE tmdb_id = '990030' AND item_type = 'Movie'
                """
                ),
                {"state_key": recovered.state_key},
            )
            .mappings()
            .one()
        )
    assert dict(row) == {
        "subscription_status": "SUBSCRIBED",
        "operation_status": "confirmed",
        "projection_applied": "true",
    }


def test_auto_subscription_projection_failure_rolls_back_receipt_and_can_recover(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = SqlAlchemyAutoSubscriptionRepository(
        p7_runtime.unit_of_work,
        _FailingSubscriptionMediaProjection(),
    )
    item = {
        "tmdb_id": "990032",
        "item_type": "Movie",
        "title": "Recover Projection",
        "subscription_status": "WANTED",
        "first_requested_at": datetime(2026, 8, 30, tzinfo=UTC),
    }
    with p7_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM media_metadata WHERE tmdb_id = '990032'"))

    first = AutoSubscriptionExecution("job-a", "attempt-a", "worker-a")
    claim = repository.claim(item, phase="subscribe", execution=first)
    repository.begin_source(
        claim,
        source_type="mp",
        replay_safe=True,
        execution=first,
    )
    with pytest.raises(RuntimeError, match="injected projection failure"):
        repository.complete_subscription(
            claim,
            item,
            action_type="MP",
            mark_subscribed=True,
        )

    with p7_runtime.engine.connect() as connection:
        state = (
            connection.execute(
                text(
                    """
                    SELECT state_json ->> 'status' AS status,
                           state_json ->> 'projection_applied' AS projection_applied
                    FROM subscription_runtime_states
                    WHERE state_key = :state_key
                    """
                ),
                {"state_key": claim.state_key},
            )
            .mappings()
            .one()
        )
    assert dict(state) == {"status": "requesting", "projection_applied": "false"}

    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata(
                    tmdb_id, item_type, title, subscription_status, in_library,
                    first_requested_at, subscription_sources_json, last_updated_at
                ) VALUES (
                    '990032', 'Movie', 'Recover Projection', 'WANTED', FALSE,
                    :first_requested_at, '[]'::jsonb, NOW()
                )
                """
            ),
            {"first_requested_at": item["first_requested_at"]},
        )
        connection.execute(
            text(
                """
                UPDATE subscription_runtime_states
                SET state_json = jsonb_set(
                    state_json,
                    '{lease_expires_at}',
                    to_jsonb(CAST(:expired AS text))
                )
                WHERE state_key = :state_key
                """
            ),
            {
                "state_key": claim.state_key,
                "expired": (datetime.now(UTC) - timedelta(minutes=1)).isoformat(),
            },
        )

    repository = SqlAlchemyAutoSubscriptionRepository(
        p7_runtime.unit_of_work,
        SqlAlchemySubscriptionMediaProjection(),
    )
    second = AutoSubscriptionExecution("job-b", "attempt-b", "worker-b")
    recovered = repository.claim(item, phase="subscribe", execution=second)
    assert recovered.disposition is AutoSubscriptionClaimDisposition.ACQUIRED
    repository.begin_source(
        recovered,
        source_type="mp",
        replay_safe=True,
        execution=second,
    )
    repository.complete_subscription(
        recovered,
        item,
        action_type="MP",
        mark_subscribed=True,
    )

    with p7_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                    SELECT media.subscription_status,
                           state.state_json ->> 'status' AS operation_status,
                           state.state_json ->> 'projection_applied' AS projection_applied
                    FROM media_metadata AS media
                    JOIN subscription_runtime_states AS state ON state.state_key = :state_key
                    WHERE media.tmdb_id = '990032' AND media.item_type = 'Movie'
                    """
                ),
                {"state_key": recovered.state_key},
            )
            .mappings()
            .one()
        )
    assert dict(row) == {
        "subscription_status": "SUBSCRIBED",
        "operation_status": "confirmed",
        "projection_applied": "true",
    }


def test_auto_subscription_expired_non_replay_safe_source_is_indeterminate(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = SqlAlchemyAutoSubscriptionRepository(
        p7_runtime.unit_of_work,
        SqlAlchemySubscriptionMediaProjection(),
    )
    item = {
        "tmdb_id": "990031",
        "item_type": "Movie",
        "title": "Unknown Source",
        "subscription_status": "WANTED",
    }
    first = AutoSubscriptionExecution("job-a", "attempt-a", "worker-a")
    claim = repository.claim(item, phase="subscribe", execution=first)
    repository.begin_source(
        claim,
        source_type="shared_pool",
        replay_safe=False,
        execution=first,
    )
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE subscription_runtime_states
                SET state_json = jsonb_set(
                    state_json,
                    '{lease_expires_at}',
                    to_jsonb(CAST(:expired AS text))
                )
                WHERE state_key = :state_key
                """
            ),
            {
                "state_key": claim.state_key,
                "expired": (datetime.now(UTC) - timedelta(minutes=1)).isoformat(),
            },
        )

    recovered = repository.claim(
        item,
        phase="subscribe",
        execution=AutoSubscriptionExecution("job-b", "attempt-b", "worker-b"),
    )
    assert recovered.disposition is AutoSubscriptionClaimDisposition.INDETERMINATE
    assert recovered.error == {
        "action": "shared_pool",
        "error_type": "expired_non_replay_safe_source_claim",
        "indeterminate": True,
    }


def test_auto_subscription_source_candidate_drift_reuses_one_postgres_operation_identity(
    p7_runtime: DatabaseRuntime,
) -> None:
    class HDHive:
        slug = "fixture-slug-1"

        def get_resources(self, *args: Any, **kwargs: Any) -> ProviderResponse[dict[str, Any]]:
            del args, kwargs
            return ProviderResponse(
                ProviderDisposition.CONFIRMED,
                200,
                {"resources": [{"tmdb_id": "990040", "slug": self.slug}]},
            )

    class Delivery:
        calls = 0

        def supports_authoritative_readback(self, operation: object) -> bool:
            del operation
            return False

        def request(self, operation: object, *, checkpoint: Any) -> SharingDeliveryOutcome:
            del operation
            self.calls += 1
            checkpoint()
            return SharingDeliveryOutcome.confirmed({"fixture": True})

        def readback(self, operation: object) -> SharingDeliveryOutcome:
            del operation
            return SharingDeliveryOutcome.indeterminate({"type": "fixture_readback_unavailable"})

    class Progress:
        def update(self, progress: int, message: str) -> None:
            del progress, message

        def checkpoint(self) -> None:
            return None

    occurrence = f"postgres18:{uuid4()}"
    operations = PostgresSharingOperationRepository(p7_runtime.unit_of_work)
    delivery = Delivery()
    executor = SharingOperationExecutor(operations, delivery, max_attempts=3, retry_delay_seconds=0)
    hdhive = HDHive()
    sources = PersistentOrderedAutoSubscriptionSources(
        center=SimpleNamespace(),  # type: ignore[arg-type]
        hdhive=hdhive,  # type: ignore[arg-type]
        telegram=SimpleNamespace(),  # type: ignore[arg-type]
        operations=operations,
        executor=executor,
        virtual_imports=SimpleNamespace(),  # type: ignore[arg-type]
        shared_configuration=lambda: {},
        telegram_configuration=lambda: {},
        worker_id="postgres18-worker",
        lease_seconds=30,
    )
    context = {
        "operation_occurrence": occurrence,
        "tmdb_id": "990040",
        "item_type": "Movie",
        "title": "Postgres Fixture",
    }

    first = sources.attempt("hdhive", context, Progress())
    hdhive.slug = "fixture-slug-2"
    drifted = sources.attempt("hdhive", context, Progress())

    operation_key = sharing_operation_key(
        kind=SharingOperationKind.TRANSFER,
        principal_id="worker:auto-subscribe",
        idempotency_key=f"{occurrence}:hdhive",
    )
    with p7_runtime.engine.connect() as connection:
        rows = (
            connection.execute(
                text(
                    """
                SELECT status, request_json
                FROM sharing_operations
                WHERE operation_key = :operation_key
                """
                ),
                {"operation_key": operation_key},
            )
            .mappings()
            .all()
        )

    assert first.disposition.value == "confirmed"
    assert drifted.disposition.value == "indeterminate"
    assert delivery.calls == 1
    assert len(rows) == 1
    assert rows[0]["status"] == SharingOperationStatus.CONFIRMED.value
    assert rows[0]["request_json"]["payload"]["slug"] == "fixture-slug-1"


def test_pending_sharing_operation_replays_to_confirm_parent_projection(
    p7_runtime: DatabaseRuntime,
) -> None:
    class HDHive:
        def get_resources(self, *args: Any, **kwargs: Any) -> ProviderResponse[dict[str, Any]]:
            del args, kwargs
            return ProviderResponse(
                ProviderDisposition.CONFIRMED,
                200,
                {"resources": [{"tmdb_id": "990041", "slug": "fixture-pending-slug"}]},
            )

    class Delivery:
        calls = 0

        def supports_authoritative_readback(self, operation: object) -> bool:
            del operation
            return False

        def request(self, operation: object, *, checkpoint: Any) -> SharingDeliveryOutcome:
            del operation
            self.calls += 1
            if self.calls == 1:
                return SharingDeliveryOutcome.retryable({"type": "fixture_prewrite_retry"})
            checkpoint()
            return SharingDeliveryOutcome.confirmed({"fixture": True})

        def readback(self, operation: object) -> SharingDeliveryOutcome:
            del operation
            return SharingDeliveryOutcome.indeterminate({"type": "fixture_readback_unavailable"})

    class Progress:
        def update(self, progress: int, message: str) -> None:
            del progress, message

        def checkpoint(self) -> None:
            return None

    item = {
        "tmdb_id": "990041",
        "item_type": "Movie",
        "title": "Pending Replay",
        "subscription_status": "WANTED",
        "first_requested_at": datetime(2026, 8, 31, tzinfo=UTC),
    }
    with p7_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM media_metadata WHERE tmdb_id = '990041'"))
        connection.execute(
            text(
                """
                INSERT INTO media_metadata(
                    tmdb_id, item_type, title, subscription_status, in_library,
                    first_requested_at, subscription_sources_json, last_updated_at
                ) VALUES (
                    '990041', 'Movie', 'Pending Replay', 'WANTED', FALSE,
                    :first_requested_at, '[]'::jsonb, NOW()
                )
                """
            ),
            {"first_requested_at": item["first_requested_at"]},
        )

    parent = SqlAlchemyAutoSubscriptionRepository(
        p7_runtime.unit_of_work,
        SqlAlchemySubscriptionMediaProjection(),
    )
    operations = PostgresSharingOperationRepository(p7_runtime.unit_of_work)
    delivery = Delivery()
    sources = PersistentOrderedAutoSubscriptionSources(
        center=SimpleNamespace(),  # type: ignore[arg-type]
        hdhive=HDHive(),  # type: ignore[arg-type]
        telegram=SimpleNamespace(),  # type: ignore[arg-type]
        operations=operations,
        executor=SharingOperationExecutor(operations, delivery, max_attempts=3, retry_delay_seconds=0),
        virtual_imports=SimpleNamespace(),  # type: ignore[arg-type]
        shared_configuration=lambda: {},
        telegram_configuration=lambda: {},
        worker_id="postgres18-worker",
        lease_seconds=30,
    )
    first_execution = AutoSubscriptionExecution("job-a", "attempt-a", "worker-a", lease_seconds=30)
    first_claim = parent.claim(item, phase="subscribe", execution=first_execution)
    parent.begin_source(
        first_claim,
        source_type="hdhive",
        replay_safe=True,
        execution=first_execution,
    )
    context = {**item, "operation_occurrence": first_claim.occurrence}

    pending = sources.attempt("hdhive", context, Progress())
    assert pending.disposition is AutoSourceDisposition.PENDING
    parent.finish_source(first_claim, disposition=pending.disposition)
    parent.release(first_claim)

    second_execution = AutoSubscriptionExecution("job-b", "attempt-b", "worker-b", lease_seconds=30)
    second_claim = parent.claim(item, phase="subscribe", execution=second_execution)
    assert second_claim.disposition is AutoSubscriptionClaimDisposition.ACQUIRED
    parent.begin_source(
        second_claim,
        source_type="hdhive",
        replay_safe=True,
        execution=second_execution,
    )
    confirmed = sources.attempt("hdhive", context, Progress())
    assert confirmed.disposition is AutoSourceDisposition.CONFIRMED
    parent.complete_subscription(
        second_claim,
        item,
        action_type="影巢",
        mark_subscribed=True,
    )

    with p7_runtime.engine.connect() as connection:
        media_status = connection.scalar(
            text("SELECT subscription_status FROM media_metadata WHERE tmdb_id = '990041'"),
        )
        operation_rows = connection.scalar(
            text(
                """
                SELECT COUNT(*)
                FROM sharing_operations
                WHERE operation_key = :operation_key
                """
            ),
            {
                "operation_key": sharing_operation_key(
                    kind=SharingOperationKind.TRANSFER,
                    principal_id="worker:auto-subscribe",
                    idempotency_key=f"{first_claim.occurrence}:hdhive",
                )
            },
        )
    assert media_status == "SUBSCRIBED"
    assert operation_rows == 1
    assert delivery.calls == 2


def test_parent_cancellation_cancels_reserved_sharing_operation_before_provider_write(
    p7_runtime: DatabaseRuntime,
) -> None:
    class HDHive:
        def get_resources(self, *args: Any, **kwargs: Any) -> ProviderResponse[dict[str, Any]]:
            del args, kwargs
            return ProviderResponse(
                ProviderDisposition.CONFIRMED,
                200,
                {"resources": [{"tmdb_id": "990042", "slug": "fixture-cancel-slug"}]},
            )

    class Delivery:
        provider_called = False

        def supports_authoritative_readback(self, operation: object) -> bool:
            del operation
            return False

        def request(self, operation: object, *, checkpoint: Any) -> SharingDeliveryOutcome:
            del operation
            checkpoint()
            self.provider_called = True
            return SharingDeliveryOutcome.confirmed({"fixture": True})

        def readback(self, operation: object) -> SharingDeliveryOutcome:
            del operation
            return SharingDeliveryOutcome.indeterminate({"type": "fixture_readback_unavailable"})

    class Progress:
        checkpoints = 0

        def update(self, progress: int, message: str) -> None:
            del progress, message

        def checkpoint(self) -> None:
            self.checkpoints += 1
            if self.checkpoints == 3:
                raise PersistentExecutionCancelled("cancelled fixture")

    occurrence = f"postgres18-cancel:{uuid4()}"
    operations = PostgresSharingOperationRepository(p7_runtime.unit_of_work)
    delivery = Delivery()
    sources = PersistentOrderedAutoSubscriptionSources(
        center=SimpleNamespace(),  # type: ignore[arg-type]
        hdhive=HDHive(),  # type: ignore[arg-type]
        telegram=SimpleNamespace(),  # type: ignore[arg-type]
        operations=operations,
        executor=SharingOperationExecutor(operations, delivery, max_attempts=3, retry_delay_seconds=0),
        virtual_imports=SimpleNamespace(),  # type: ignore[arg-type]
        shared_configuration=lambda: {},
        telegram_configuration=lambda: {},
        worker_id="postgres18-worker",
        lease_seconds=30,
    )
    context = {
        "operation_occurrence": occurrence,
        "tmdb_id": "990042",
        "item_type": "Movie",
        "title": "Cancelled Fixture",
    }

    with pytest.raises(PersistentExecutionCancelled):
        sources.attempt("hdhive", context, Progress())

    operation = operations.read_by_occurrence(
        kind=SharingOperationKind.TRANSFER,
        principal_id="worker:auto-subscribe",
        idempotency_key=f"{occurrence}:hdhive",
    )
    assert operation is not None
    assert operation.status is SharingOperationStatus.CANCELLED
    assert operation.step_started_at is None
    assert delivery.provider_called is False
    with p7_runtime.engine.connect() as connection:
        outbox_status = connection.scalar(
            text("SELECT status FROM outbox_events WHERE aggregate_id = :operation_id"),
            {"operation_id": str(operation.id)},
        )
    assert outbox_status == "processed"
