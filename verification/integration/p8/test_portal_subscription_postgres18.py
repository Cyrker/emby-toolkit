from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Barrier
from typing import Any, cast

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from etk.domains.subscription.moviepilot import (
    MoviePilotQueryDisposition,
    MoviePilotQueryResult,
    MoviePilotWriteResult,
)
from etk.domains.subscription.operation_repository import PostgresSubscriptionOperationRepository
from etk.domains.subscription.portal import (
    PortalMediaRecord,
    PortalProviderIntent,
    PortalSubscriptionConflictError,
    PortalSubscriptionRepository,
)
from etk.domains.subscription.provider_execution import SubscriptionOperationExecutor
from etk.domains.subscription.runtime_repository import SubscriptionQuotaExhaustedError
from etk.domains.system import SqlAlchemyAppSettingsRepository
from etk.platform.db import DatabaseRuntime

NOW = datetime(2026, 8, 27, 12, 0, tzinfo=UTC)


class ConfirmingMoviePilotGateway:
    def __init__(self, query_results: list[MoviePilotQueryResult] | None = None) -> None:
        self.writes: list[dict[str, Any]] = []
        self.query_results = list(query_results or [])

    def query_subscription(
        self,
        tmdb_id: int | str,
        *,
        season: int | None = None,
    ) -> MoviePilotQueryResult:
        if self.query_results:
            return self.query_results.pop(0)
        return MoviePilotQueryResult(
            MoviePilotQueryDisposition.FOUND,
            {"tmdbid": int(tmdb_id), "season": season},
        )

    def create_subscription(self, payload: dict[str, Any]) -> MoviePilotWriteResult:
        self.writes.append(dict(payload))
        return MoviePilotWriteResult("create-subscription", 201)


class FailingPreflightMoviePilotGateway:
    @staticmethod
    def query_subscription(
        tmdb_id: int | str,
        *,
        season: int | None = None,
    ) -> MoviePilotQueryResult:
        del tmdb_id, season
        raise RuntimeError("fixture preflight unavailable")

    @staticmethod
    def create_subscription(payload: dict[str, Any]) -> MoviePilotWriteResult:
        raise AssertionError(f"provider write must not run: {payload!r}")


def _repository(runtime: DatabaseRuntime) -> PortalSubscriptionRepository:
    settings = SqlAlchemyAppSettingsRepository(runtime.unit_of_work)
    return PortalSubscriptionRepository(
        runtime.unit_of_work,
        settings,
        clock=lambda: NOW,
    )


def _settings(runtime: DatabaseRuntime, *, cap: int = 3) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json)
                VALUES
                    ('mp_config', CAST(:mp_config AS jsonb)),
                    ('recommendation_pool', '[{"id":"101"},{"id":"202"}]'::jsonb)
                ON CONFLICT (setting_key) DO UPDATE SET value_json = EXCLUDED.value_json
                """
            ),
            {"mp_config": f'{{"moviepilot_url":"https://mp.fixture.invalid","resubscribe_daily_cap":{cap}}}'},
        )


def _movie() -> PortalMediaRecord:
    return PortalMediaRecord(
        tmdb_id="101",
        item_type="Movie",
        title="Fixture Movie",
        original_title="Fixture Original",
        release_date="2026-01-01",
        poster_path="/fixture.jpg",
        overview="Fixture overview",
    )


def test_portal_media_operation_quota_outbox_and_pool_commit_atomically(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime)
    repository = _repository(p8_runtime)

    result = repository.submit(
        user_id="fixture-user",
        records=(_movie(),),
        target_status="WANTED",
        provider_intents=(PortalProviderIntent("101", "Fixture Movie", "Movie"),),
        consume_quota=True,
        recommendation_tmdb_id="101",
        request_item_type="Movie",
        request_tmdb_id="101",
        request_season_number=None,
    )

    assert len(result.operation_ids) == 1
    assert result.recommendation_revision == 1
    assert result.recommendation_pool_size == 1
    operation_id = result.operation_ids[0]
    with p8_runtime.engine.connect() as connection:
        media = (
            connection.execute(
                text(
                    """
                    SELECT subscription_status, subscription_sources_json
                    FROM media_metadata WHERE tmdb_id = '101' AND item_type = 'Movie'
                    """
                )
            )
            .mappings()
            .one()
        )
        assert media["subscription_status"] == "WANTED"
        assert media["subscription_sources_json"] == [{"type": "user_request", "user_id": "fixture-user"}]
        assert (
            connection.scalar(
                text("SELECT status FROM subscription_operations WHERE id = :id"),
                {"id": operation_id},
            )
            == "ready"
        )
        assert (
            connection.scalar(
                text(
                    """
                SELECT status FROM outbox_events
                WHERE event_key = :event_key
                """
                ),
                {"event_key": f"subscription.operation.requested:{operation_id}"},
            )
            == "pending"
        )
        assert (
            connection.scalar(
                text(
                    """
                SELECT status FROM subscription_quota_reservations
                WHERE operation_id = :operation_id
                """
                ),
                {"operation_id": operation_id},
            )
            == "reserved"
        )
        assert connection.scalar(
            text("SELECT value_json FROM app_settings WHERE setting_key = 'recommendation_pool'")
        ) == [{"id": "202"}]
        publication = connection.scalar(
            text("SELECT value_json FROM app_settings WHERE setting_key = 'recommendation_publication_state'")
        )
        assert publication["mutation"] == "portal_removal"
        assert publication["removed_tmdb_id"] == "101"
        assert publication["pool_size"] == 1
        assert publication["revision"] == 1
        replenishment = (
            connection.execute(
                text(
                    """
                    SELECT id, status, payload_json
                    FROM media_effects
                    WHERE effect_key = 'daily-theme:replenishment:publication:1'
                    """
                )
            )
            .mappings()
            .one()
        )
        assert replenishment["status"] == "pending"
        assert replenishment["payload_json"] == {"publication_revision": 1}
        assert (
            connection.scalar(
                text(
                    """
                    SELECT status FROM outbox_events
                    WHERE topic = 'media.effect.requested'
                      AND aggregate_id = :effect_id
                    """
                ),
                {"effect_id": str(replenishment["id"])},
            )
            == "pending"
        )


def test_confirmed_portal_provider_operation_projects_subscribed_and_commits_quota(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime)
    portal = _repository(p8_runtime)
    submission = portal.submit(
        user_id="fixture-user",
        records=(_movie(),),
        target_status="WANTED",
        provider_intents=(PortalProviderIntent("101", "Fixture Movie", "Movie"),),
        consume_quota=True,
        recommendation_tmdb_id=None,
        request_item_type="Movie",
        request_tmdb_id="101",
        request_season_number=None,
    )
    operation_repository = PostgresSubscriptionOperationRepository(p8_runtime.unit_of_work)
    gateway = ConfirmingMoviePilotGateway(
        [
            MoviePilotQueryResult(MoviePilotQueryDisposition.EMPTY, None),
            MoviePilotQueryResult(MoviePilotQueryDisposition.FOUND, {"tmdbid": 101}),
        ]
    )
    executor = SubscriptionOperationExecutor(
        operation_repository,
        cast(Any, gateway),
        readback_delay_seconds=0,
    )

    execution = executor.execute(
        submission.operation_ids[0],
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )

    assert execution is not None
    assert execution.operation.status.value == "confirmed"
    assert len(gateway.writes) == 1
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text(
                    """
                    SELECT subscription_status FROM media_metadata
                    WHERE tmdb_id = '101' AND item_type = 'Movie'
                    """
                )
            )
            == "SUBSCRIBED"
        )
        assert (
            connection.scalar(
                text(
                    """
                    SELECT status FROM subscription_quota_reservations
                    WHERE operation_id = :operation_id
                    """
                ),
                {"operation_id": submission.operation_ids[0]},
            )
            == "committed"
        )


def test_portal_upsert_preserves_stronger_subscription_states(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime)
    repository = _repository(p8_runtime)
    base_values = {
        "user_id": "fixture-user",
        "provider_intents": (),
        "consume_quota": False,
        "recommendation_tmdb_id": None,
        "request_item_type": "Movie",
        "request_tmdb_id": "101",
        "request_season_number": None,
    }
    repository.submit(records=(_movie(),), target_status="WANTED", **base_values)
    for protected_status in ("SUBSCRIBED", "PAUSED"):
        with p8_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE media_metadata SET subscription_status = :status
                    WHERE tmdb_id = '101' AND item_type = 'Movie'
                    """
                ),
                {"status": protected_status},
            )
        repository.submit(records=(_movie(),), target_status="WANTED", **base_values)
        with p8_runtime.engine.connect() as connection:
            assert (
                connection.scalar(
                    text(
                        """
                        SELECT subscription_status FROM media_metadata
                        WHERE tmdb_id = '101' AND item_type = 'Movie'
                        """
                    )
                )
                == protected_status
            )

    parent = PortalMediaRecord("202", "Series", "Fixture Series", None, None, None, None)
    season = PortalMediaRecord(
        "2101",
        "Season",
        "Fixture Series 第 1 季",
        None,
        "2027-01-01",
        None,
        None,
        "202",
        1,
    )
    repository.submit(
        user_id="fixture-user",
        records=(parent,),
        target_status="WANTED",
        provider_intents=(),
        consume_quota=False,
        recommendation_tmdb_id=None,
        request_item_type="Series",
        request_tmdb_id="202",
        request_season_number=None,
    )
    repository.submit(
        user_id="fixture-user",
        records=(parent, season),
        target_status="PENDING_RELEASE",
        provider_intents=(),
        consume_quota=False,
        recommendation_tmdb_id=None,
        request_item_type="Season",
        request_tmdb_id="202",
        request_season_number=1,
    )
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text(
                    """
                    SELECT subscription_status FROM media_metadata
                    WHERE tmdb_id = '202' AND item_type = 'Series'
                    """
                )
            )
            == "WANTED"
        )


def test_portal_upsert_does_not_revive_hard_ignored_media(
    p8_runtime: DatabaseRuntime,
) -> None:
    records: list[tuple[PortalMediaRecord, str, str]] = []
    for index, target_status in enumerate(("REQUESTED", "PENDING_RELEASE"), 1):
        for reason in ("手动忽略", "订阅超时"):
            tmdb_id = f"hard-ignore-{index}-{reason}"
            record = PortalMediaRecord(
                tmdb_id,
                "Movie",
                f"Hard ignored {target_status} {reason}",
                None,
                "2027-01-01",
                None,
                None,
            )
            records.append((record, target_status, reason))
            with p8_runtime.engine.begin() as connection:
                connection.execute(
                    text(
                        """
                        INSERT INTO media_metadata (
                            tmdb_id, item_type, title, release_date, in_library,
                            subscription_status, subscription_sources_json, ignore_reason
                        ) VALUES (
                            :tmdb_id, 'Movie', :title, '2027-01-01', FALSE,
                            'IGNORED', '[{"type":"existing"}]'::jsonb, :ignore_reason
                        )
                        """
                    ),
                    {"tmdb_id": tmdb_id, "title": record.title, "ignore_reason": reason},
                )

    repository = _repository(p8_runtime)
    base_values = {
        "user_id": "fixture-user",
        "provider_intents": (),
        "consume_quota": False,
        "recommendation_tmdb_id": None,
        "request_item_type": "Movie",
        "request_tmdb_id": "hard-ignore-request",
        "request_season_number": None,
    }
    for record, target_status, _reason in records:
        repository.submit(records=(record,), target_status=target_status, **base_values)

    with p8_runtime.engine.connect() as connection:
        rows = (
            connection.execute(
                text(
                    """
                    SELECT tmdb_id, subscription_status, subscription_sources_json, ignore_reason
                    FROM media_metadata
                    WHERE tmdb_id LIKE 'hard-ignore-%'
                    ORDER BY tmdb_id
                    """
                )
            )
            .mappings()
            .all()
        )

    assert len(rows) == 4
    assert all(row["subscription_status"] == "IGNORED" for row in rows)
    assert all(row["subscription_sources_json"] == [{"type": "existing"}] for row in rows)
    assert {row["ignore_reason"] for row in rows} == {"手动忽略", "订阅超时"}


def test_outbox_failure_rolls_back_media_quota_operation_and_pool(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime)
    repository = _repository(p8_runtime)
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE FUNCTION p8_reject_portal_outbox() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    IF NEW.topic = 'subscription.operation.requested' THEN
                        RAISE EXCEPTION 'fixture portal outbox rejection';
                    END IF;
                    RETURN NEW;
                END;
                $$
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TRIGGER p8_reject_portal_outbox
                BEFORE INSERT ON outbox_events
                FOR EACH ROW EXECUTE FUNCTION p8_reject_portal_outbox()
                """
            )
        )
    try:
        with pytest.raises(DBAPIError):
            repository.submit(
                user_id="fixture-user",
                records=(_movie(),),
                target_status="WANTED",
                provider_intents=(PortalProviderIntent("101", "Fixture Movie", "Movie"),),
                consume_quota=True,
                recommendation_tmdb_id="101",
                request_item_type="Movie",
                request_tmdb_id="101",
                request_season_number=None,
            )
        with p8_runtime.engine.connect() as connection:
            assert connection.scalar(text("SELECT COUNT(*) FROM media_metadata")) == 0
            assert connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) == 0
            assert connection.scalar(text("SELECT COUNT(*) FROM subscription_quota_reservations")) == 0
            assert connection.scalar(text("SELECT COUNT(*) FROM media_effects")) == 0
            assert connection.scalar(
                text("SELECT value_json FROM app_settings WHERE setting_key = 'recommendation_pool'")
            ) == [{"id": "101"}, {"id": "202"}]
    finally:
        with p8_runtime.engine.begin() as connection:
            connection.execute(text("DROP TRIGGER IF EXISTS p8_reject_portal_outbox ON outbox_events"))
            connection.execute(text("DROP FUNCTION IF EXISTS p8_reject_portal_outbox()"))


def test_series_quota_exhaustion_rolls_back_every_season(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime, cap=1)
    repository = _repository(p8_runtime)
    records = (
        PortalMediaRecord("202", "Series", "Fixture Series", None, None, None, None),
        PortalMediaRecord(
            "2101",
            "Season",
            "Fixture Series 第 1 季",
            None,
            "2025-01-01",
            None,
            None,
            parent_series_tmdb_id="202",
            season_number=1,
        ),
        PortalMediaRecord(
            "2102",
            "Season",
            "Fixture Series 第 2 季",
            None,
            "2026-01-01",
            None,
            None,
            parent_series_tmdb_id="202",
            season_number=2,
        ),
    )

    with pytest.raises(SubscriptionQuotaExhaustedError):
        repository.submit(
            user_id="fixture-user",
            records=records,
            target_status="WANTED",
            provider_intents=(
                PortalProviderIntent("202", "Fixture Series", "Season", 1),
                PortalProviderIntent("202", "Fixture Series", "Season", 2),
            ),
            consume_quota=True,
            recommendation_tmdb_id=None,
            request_item_type="Series",
            request_tmdb_id="202",
            request_season_number=None,
        )

    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM media_metadata")) == 0
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) == 0
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_quota_reservations")) == 0


def test_replay_reuses_operation_and_source_without_duplicate_quota(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime)
    repository = _repository(p8_runtime)
    values = {
        "user_id": "fixture-user",
        "records": (_movie(),),
        "target_status": "WANTED",
        "provider_intents": (PortalProviderIntent("101", "Fixture Movie", "Movie"),),
        "consume_quota": True,
        "recommendation_tmdb_id": "101",
        "request_item_type": "Movie",
        "request_tmdb_id": "101",
        "request_season_number": None,
    }

    first = repository.submit(**values)
    second = repository.submit(**values)

    assert first.operation_ids == second.operation_ids
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) == 1
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_quota_reservations")) == 1
        assert (
            connection.scalar(
                text("SELECT COUNT(*) FROM outbox_events WHERE topic = 'subscription.operation.requested'")
            )
            == 1
        )
        assert connection.scalar(text("SELECT COUNT(*) FROM outbox_events WHERE topic = 'media.effect.requested'")) == 1
        assert (
            connection.scalar(
                text(
                    """
                SELECT jsonb_array_length(subscription_sources_json)
                FROM media_metadata WHERE tmdb_id = '101' AND item_type = 'Movie'
                """
                )
            )
            == 1
        )


def test_fresh_portal_occurrence_creates_new_operation_after_terminal_rejection(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime)
    repository = _repository(p8_runtime)
    values = {
        "user_id": "fixture-user",
        "records": (_movie(),),
        "target_status": "WANTED",
        "provider_intents": (PortalProviderIntent("101", "Fixture Movie", "Movie"),),
        "consume_quota": True,
        "recommendation_tmdb_id": "101",
        "request_item_type": "Movie",
        "request_tmdb_id": "101",
        "request_season_number": None,
    }
    first = repository.submit(**values)
    operation_repository = PostgresSubscriptionOperationRepository(p8_runtime.unit_of_work)
    claimed = operation_repository.claim_by_id(
        first.operation_ids[0],
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )
    assert claimed is not None and claimed.claim_token is not None
    rejected = operation_repository.mark_rejected(
        claimed.id,
        claimed.claim_token,
        error={"type": "fixture_rejection"},
        now=NOW,
    )
    assert rejected.status.value == "rejected"

    second = repository.submit(**values)

    assert second.operation_ids != first.operation_ids
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) == 2
        assert (
            connection.scalar(text("SELECT COUNT(*) FROM subscription_quota_reservations WHERE status = 'released'"))
            == 1
        )
        assert (
            connection.scalar(text("SELECT COUNT(*) FROM subscription_quota_reservations WHERE status = 'reserved'"))
            == 1
        )


def test_cancelled_portal_operation_allows_a_fresh_unkeyed_generation(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime)
    repository = _repository(p8_runtime)
    values = {
        "user_id": "fixture-user",
        "records": (_movie(),),
        "target_status": "WANTED",
        "provider_intents": (PortalProviderIntent("101", "Fixture Movie", "Movie"),),
        "consume_quota": True,
        "recommendation_tmdb_id": "101",
        "request_item_type": "Movie",
        "request_tmdb_id": "101",
        "request_season_number": None,
    }
    first = repository.submit(**values)
    operation_repository = PostgresSubscriptionOperationRepository(p8_runtime.unit_of_work)
    claimed = operation_repository.claim_by_id(
        first.operation_ids[0],
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )
    assert claimed is not None and claimed.claim_token is not None
    cancelled = operation_repository.mark_cancelled_before_write(
        claimed.id,
        claimed.claim_token,
        evidence={"provider_write_invoked": False},
        now=NOW,
    )
    assert cancelled.status.value == "cancelled"

    second = repository.submit(**values)

    assert second.operation_ids != first.operation_ids
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) == 2
        assert (
            connection.scalar(text("SELECT COUNT(*) FROM subscription_quota_reservations WHERE status = 'released'"))
            == 1
        )
        assert (
            connection.scalar(text("SELECT COUNT(*) FROM subscription_quota_reservations WHERE status = 'reserved'"))
            == 1
        )


def test_indeterminate_portal_operation_fences_a_fresh_request(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime)
    repository = _repository(p8_runtime)
    values = {
        "user_id": "fixture-user",
        "records": (_movie(),),
        "target_status": "WANTED",
        "provider_intents": (PortalProviderIntent("101", "Fixture Movie", "Movie"),),
        "consume_quota": True,
        "recommendation_tmdb_id": "101",
        "request_item_type": "Movie",
        "request_tmdb_id": "101",
        "request_season_number": None,
    }
    first = repository.submit(**values)
    operation_repository = PostgresSubscriptionOperationRepository(p8_runtime.unit_of_work)
    claimed = operation_repository.claim_by_id(
        first.operation_ids[0],
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )
    assert claimed is not None and claimed.claim_token is not None
    indeterminate = operation_repository.mark_indeterminate(
        claimed.id,
        claimed.claim_token,
        error={"type": "fixture_unknown"},
        now=NOW,
    )
    assert indeterminate.status.value == "indeterminate"

    with pytest.raises(PortalSubscriptionConflictError):
        repository.submit(**values)

    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) == 1
        assert (
            connection.scalar(
                text("SELECT status FROM subscription_quota_reservations WHERE operation_id = :id"),
                {"id": first.operation_ids[0]},
            )
            == "committed"
        )


def test_same_idempotency_key_reuses_original_series_snapshot_despite_provider_drift(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime)
    repository = _repository(p8_runtime)
    first_records = (
        PortalMediaRecord("202", "Series", "Original Series", None, None, None, None),
        PortalMediaRecord("2101", "Season", "Original S1", None, None, None, None, "202", 1),
        PortalMediaRecord("2102", "Season", "Original S2", None, None, None, None, "202", 2),
    )
    first = repository.submit(
        user_id="fixture-user",
        records=first_records,
        target_status="WANTED",
        provider_intents=(
            PortalProviderIntent("202", "Original Series", "Season", 1),
            PortalProviderIntent("202", "Original Series", "Season", 2),
        ),
        consume_quota=True,
        recommendation_tmdb_id=None,
        request_item_type="Series",
        request_tmdb_id="202",
        request_season_number=None,
        idempotency_key="series-request",
    )

    replay = repository.submit(
        user_id="fixture-user",
        records=(
            PortalMediaRecord("202", "Series", "Renamed Series", None, None, None, None),
            PortalMediaRecord("2102", "Season", "Renamed S2", None, None, None, None, "202", 2),
            PortalMediaRecord("2101", "Season", "Renamed S1", None, None, None, None, "202", 1),
        ),
        target_status="WANTED",
        provider_intents=(
            PortalProviderIntent("202", "Renamed Series", "Season", 2),
            PortalProviderIntent("202", "Renamed Series", "Season", 1),
        ),
        consume_quota=True,
        recommendation_tmdb_id=None,
        request_item_type="Series",
        request_tmdb_id="202",
        request_season_number=None,
        idempotency_key="series-request",
    )

    assert replay.operation_ids == first.operation_ids
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) == 2
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_quota_reservations")) == 2
        assert connection.scalar(text("SELECT COUNT(*) FROM outbox_events")) == 2


def test_same_idempotency_key_with_different_caller_intent_conflicts(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime)
    repository = _repository(p8_runtime)
    repository.submit(
        user_id="fixture-user",
        records=(_movie(),),
        target_status="WANTED",
        provider_intents=(PortalProviderIntent("101", "Fixture Movie", "Movie"),),
        consume_quota=True,
        recommendation_tmdb_id="101",
        request_item_type="Movie",
        request_tmdb_id="101",
        request_season_number=None,
        idempotency_key="shared-key",
    )

    with pytest.raises(PortalSubscriptionConflictError):
        repository.submit(
            user_id="fixture-user",
            records=(PortalMediaRecord("102", "Movie", "Different Movie", None, None, None, None),),
            target_status="WANTED",
            provider_intents=(PortalProviderIntent("102", "Different Movie", "Movie"),),
            consume_quota=True,
            recommendation_tmdb_id="102",
            request_item_type="Movie",
            request_tmdb_id="102",
            request_season_number=None,
            idempotency_key="shared-key",
        )

    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) == 1
        assert connection.scalar(text("SELECT COUNT(*) FROM media_metadata WHERE tmdb_id = '102'")) == 0


def test_different_explicit_keys_alias_one_active_provider_scope_with_separate_receipts(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime)
    repository = _repository(p8_runtime)
    values = {
        "user_id": "fixture-user",
        "records": (_movie(),),
        "target_status": "WANTED",
        "provider_intents": (PortalProviderIntent("101", "Fixture Movie", "Movie"),),
        "consume_quota": True,
        "recommendation_tmdb_id": "101",
        "request_item_type": "Movie",
        "request_tmdb_id": "101",
        "request_season_number": None,
    }
    first = repository.submit(**values, idempotency_key="scope-owner")

    second = repository.submit(**values, idempotency_key="different-request")

    assert second.operation_ids == first.operation_ids
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) == 1
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_quota_reservations")) == 1
        assert (
            connection.scalar(
                text("SELECT COUNT(*) FROM outbox_events WHERE topic = 'subscription.operation.requested'")
            )
            == 1
        )
        assert connection.scalar(text("SELECT COUNT(*) FROM outbox_events WHERE topic = 'media.effect.requested'")) == 1
        assert connection.scalar(text("SELECT COUNT(*) FROM portal_subscription_requests")) == 2
        assert (
            connection.scalar(
                text("SELECT id FROM subscription_operations WHERE id = :id"),
                {"id": first.operation_ids[0]},
            )
            == first.operation_ids[0]
        )


def test_explicit_key_receipt_covers_local_only_success_and_conflicts_on_new_intent(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime)
    repository = _repository(p8_runtime)
    values = {
        "user_id": "fixture-user",
        "records": (_movie(),),
        "target_status": "REQUESTED",
        "provider_intents": (),
        "consume_quota": False,
        "recommendation_tmdb_id": None,
        "request_item_type": "Movie",
        "request_tmdb_id": "101",
        "request_season_number": None,
        "idempotency_key": "local-only-request",
    }

    first = repository.submit(**values)
    replay = repository.submit(**values)

    assert first.operation_ids == replay.operation_ids == ()
    with pytest.raises(PortalSubscriptionConflictError):
        repository.submit(
            **{
                **values,
                "records": (PortalMediaRecord("102", "Movie", "Other", None, None, None, None),),
                "request_tmdb_id": "102",
            }
        )
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM portal_subscription_requests")) == 1
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) == 0


def test_explicit_key_replay_returns_first_accepted_http_outcome_after_config_drift(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime)
    repository = _repository(p8_runtime)
    base = {
        "user_id": "fixture-user",
        "records": (_movie(),),
        "consume_quota": False,
        "recommendation_tmdb_id": None,
        "request_item_type": "Movie",
        "request_tmdb_id": "101",
        "request_season_number": None,
        "idempotency_key": "http-outcome-replay",
    }
    first = repository.submit(
        **base,
        target_status="WANTED",
        provider_intents=(),
        response_payload={"status": "WANTED", "message": "MoviePilot 未配置"},
    )

    replay = repository.submit(
        **base,
        target_status="WANTED",
        provider_intents=(PortalProviderIntent("101", "Fixture Movie", "Movie"),),
        response_payload={"status": "approved", "message": "系统将自动处理"},
    )

    assert (
        first.response
        == replay.response
        == {
            "status": "WANTED",
            "message": "MoviePilot 未配置",
        }
    )
    assert replay.replayed is True
    assert replay.operation_ids == ()
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) == 0


def test_fresh_key_after_confirmation_creates_preflight_operation_without_duplicate_write(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime)
    repository = _repository(p8_runtime)
    values = {
        "user_id": "fixture-user",
        "records": (_movie(),),
        "target_status": "WANTED",
        "provider_intents": (PortalProviderIntent("101", "Fixture Movie", "Movie"),),
        "consume_quota": True,
        "recommendation_tmdb_id": None,
        "request_item_type": "Movie",
        "request_tmdb_id": "101",
        "request_season_number": None,
    }
    first = repository.submit(**values, idempotency_key="first-confirmed-request")
    operation_repository = PostgresSubscriptionOperationRepository(p8_runtime.unit_of_work)
    gateway = ConfirmingMoviePilotGateway(
        [
            MoviePilotQueryResult(MoviePilotQueryDisposition.EMPTY, None),
            MoviePilotQueryResult(MoviePilotQueryDisposition.FOUND, {"tmdbid": 101}),
            MoviePilotQueryResult(MoviePilotQueryDisposition.FOUND, {"tmdbid": 101}),
        ]
    )
    executor = SubscriptionOperationExecutor(
        operation_repository,
        cast(Any, gateway),
        readback_delay_seconds=0,
    )
    first_execution = executor.execute(
        first.operation_ids[0],
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )
    assert first_execution is not None and first_execution.operation.status.value == "confirmed"

    second = repository.submit(**values, idempotency_key="fresh-confirmed-request")
    second_execution = executor.execute(
        second.operation_ids[0],
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )

    assert second.operation_ids != first.operation_ids
    assert second_execution is not None and second_execution.operation.status.value == "confirmed"
    assert len(gateway.writes) == 1
    assert second_execution.operation.result == {
        "action": "create",
        "provider_write_invoked": False,
    }


def test_portal_preflight_exhaustion_releases_quota_and_allows_fresh_generation(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime)
    repository = _repository(p8_runtime)
    values = {
        "user_id": "fixture-user",
        "records": (_movie(),),
        "target_status": "WANTED",
        "provider_intents": (PortalProviderIntent("101", "Fixture Movie", "Movie"),),
        "consume_quota": True,
        "recommendation_tmdb_id": None,
        "request_item_type": "Movie",
        "request_tmdb_id": "101",
        "request_season_number": None,
    }
    first = repository.submit(**values)
    operation_repository = PostgresSubscriptionOperationRepository(p8_runtime.unit_of_work)
    executor = SubscriptionOperationExecutor(
        operation_repository,
        cast(Any, FailingPreflightMoviePilotGateway()),
        max_attempts=2,
        readback_delay_seconds=0,
    )

    first_attempt = executor.execute(
        first.operation_ids[0],
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )
    exhausted = executor.execute(
        first.operation_ids[0],
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )
    second = repository.submit(**values)

    assert first_attempt is not None and first_attempt.operation.status.value == "retryable"
    assert exhausted is not None and exhausted.operation.status.value == "rejected"
    assert second.operation_ids != first.operation_ids
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text(
                    """
                    SELECT status FROM subscription_quota_reservations
                    WHERE operation_id = :operation_id
                    """
                ),
                {"operation_id": first.operation_ids[0]},
            )
            == "released"
        )


def test_review_only_request_cannot_downgrade_an_approved_subscription(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime)
    repository = _repository(p8_runtime)
    repository.submit(
        user_id="admin-user",
        records=(_movie(),),
        target_status="WANTED",
        provider_intents=(),
        consume_quota=False,
        recommendation_tmdb_id=None,
        request_item_type="Movie",
        request_tmdb_id="101",
        request_season_number=None,
    )

    repository.submit(
        user_id="review-user",
        records=(_movie(),),
        target_status="REQUESTED",
        provider_intents=(),
        consume_quota=False,
        recommendation_tmdb_id=None,
        request_item_type="Movie",
        request_tmdb_id="101",
        request_season_number=None,
    )

    with p8_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    "SELECT subscription_status, subscription_sources_json "
                    "FROM media_metadata WHERE tmdb_id = '101' AND item_type = 'Movie'"
                )
            )
            .mappings()
            .one()
        )
        assert row["subscription_status"] == "WANTED"
        assert row["subscription_sources_json"] == [
            {"type": "user_request", "user_id": "admin-user"},
            {"type": "user_request", "user_id": "review-user"},
        ]
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) == 0


def test_concurrent_unkeyed_double_submit_reuses_one_operation_and_quota(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime)
    repository = _repository(p8_runtime)
    barrier = Barrier(2)
    values = {
        "user_id": "fixture-user",
        "records": (_movie(),),
        "target_status": "WANTED",
        "provider_intents": (PortalProviderIntent("101", "Fixture Movie", "Movie"),),
        "consume_quota": True,
        "recommendation_tmdb_id": "101",
        "request_item_type": "Movie",
        "request_tmdb_id": "101",
        "request_season_number": None,
    }

    def submit() -> tuple[object, ...]:
        barrier.wait()
        return tuple(repository.submit(**values).operation_ids)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(lambda _index: submit(), range(2)))

    assert results[0] == results[1]
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) == 1
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_quota_reservations")) == 1
        assert (
            connection.scalar(
                text("SELECT COUNT(*) FROM outbox_events WHERE topic = 'subscription.operation.requested'")
            )
            == 1
        )
        assert connection.scalar(text("SELECT COUNT(*) FROM outbox_events WHERE topic = 'media.effect.requested'")) == 1


def test_concurrent_series_and_season_requests_serialize_overlapping_provider_scope(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime)
    repository = _repository(p8_runtime)
    barrier = Barrier(2)
    series_values = {
        "user_id": "fixture-user",
        "records": (
            PortalMediaRecord("202", "Series", "Fixture Series", None, None, None, None),
            PortalMediaRecord("2101", "Season", "Fixture S1", None, None, None, None, "202", 1),
            PortalMediaRecord("2102", "Season", "Fixture S2", None, None, None, None, "202", 2),
        ),
        "target_status": "WANTED",
        "provider_intents": (
            PortalProviderIntent("202", "Fixture Series", "Season", 1),
            PortalProviderIntent("202", "Fixture Series", "Season", 2),
        ),
        "consume_quota": True,
        "recommendation_tmdb_id": None,
        "request_item_type": "Series",
        "request_tmdb_id": "202",
        "request_season_number": None,
    }
    season_values = {
        "user_id": "fixture-user",
        "records": (
            PortalMediaRecord("202", "Series", "Fixture Series", None, None, None, None),
            PortalMediaRecord("2101", "Season", "Fixture S1", None, None, None, None, "202", 1),
        ),
        "target_status": "WANTED",
        "provider_intents": (PortalProviderIntent("202", "Fixture Series", "Season", 1),),
        "consume_quota": True,
        "recommendation_tmdb_id": None,
        "request_item_type": "Season",
        "request_tmdb_id": "202",
        "request_season_number": 1,
    }

    def submit(values: dict[str, Any]) -> tuple[object, ...]:
        barrier.wait()
        return tuple(repository.submit(**values).operation_ids)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = (pool.submit(submit, series_values), pool.submit(submit, season_values))
        results = tuple(future.result() for future in futures)

    assert len(set(results[0]) | set(results[1])) == 2
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) == 2
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_quota_reservations")) == 2
        assert connection.scalar(text("SELECT COUNT(*) FROM outbox_events")) == 2


def test_concurrent_different_users_share_one_global_provider_operation(
    p8_runtime: DatabaseRuntime,
) -> None:
    _settings(p8_runtime)
    repository = _repository(p8_runtime)
    barrier = Barrier(2)

    def submit(user_id: str) -> tuple[object, ...]:
        barrier.wait()
        return tuple(
            repository.submit(
                user_id=user_id,
                records=(_movie(),),
                target_status="WANTED",
                provider_intents=(PortalProviderIntent("101", "Fixture Movie", "Movie"),),
                consume_quota=True,
                recommendation_tmdb_id=None,
                request_item_type="Movie",
                request_tmdb_id="101",
                request_season_number=None,
            ).operation_ids
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(submit, ("fixture-user-a", "fixture-user-b")))

    assert results[0] == results[1]
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_operations")) == 1
        assert connection.scalar(text("SELECT COUNT(*) FROM subscription_quota_reservations")) == 1
        assert connection.scalar(text("SELECT COUNT(*) FROM outbox_events")) == 1
        sources = connection.scalar(
            text("SELECT subscription_sources_json FROM media_metadata WHERE tmdb_id = '101' AND item_type = 'Movie'")
        )
        assert sorted(source["user_id"] for source in sources) == ["fixture-user-a", "fixture-user-b"]
