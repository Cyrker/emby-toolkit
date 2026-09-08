from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text

from etk.domains.media.subscription_projection import SqlAlchemySubscriptionMediaProjection
from etk.domains.media.watchlist_projection import SqlAlchemyWatchlistMediaProjectionRepository
from etk.domains.subscription.manual_batch import (
    ManualSubscriptionBatchService,
    SqlAlchemyManualSubscriptionRepository,
)
from etk.domains.subscription.manual_batch_adapters import ManualSubscriptionQuotaLedger
from etk.domains.subscription.moviepilot import (
    MoviePilotQueryDisposition,
    MoviePilotQueryResult,
    MoviePilotWriteResult,
)
from etk.domains.subscription.operation_repository import PostgresSubscriptionOperationRepository
from etk.domains.subscription.provider_execution import SubscriptionOperationExecutor
from etk.domains.subscription.runtime_repository import PostgresSubscriptionRuntimeRepository
from etk.domains.system import SqlAlchemyAppSettingsRepository
from etk.platform.db import DatabaseRuntime
from etk.platform.jobs import PostgresJobStore
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcomeStatus
from etk.processes.p7_subscription_effects import PersistentMoviePilotEffectGateway
from etk.processes.p7_subscription_tasks import P7SubscriptionTaskRouter


class _Tmdb:
    def __init__(
        self,
        *,
        movies: Mapping[int, Mapping[str, Any]] | None = None,
        series: Mapping[int, Mapping[str, Any]] | None = None,
    ) -> None:
        self.movies = dict(movies or {})
        self.series = dict(series or {})

    def movie(self, tmdb_id: int) -> Mapping[str, Any]:
        return self.movies.get(tmdb_id, {})

    def tv(self, tmdb_id: int) -> Mapping[str, Any]:
        return self.series.get(tmdb_id, {})

    def season(self, tmdb_id: int, season_number: int) -> Mapping[str, Any]:
        del tmdb_id, season_number
        return {}


class _MoviePilot:
    def __init__(self) -> None:
        self.writes: list[dict[str, Any]] = []
        self.queries: list[tuple[str, int | None]] = []

    def query_subscription(self, tmdb_id: int | str, *, season: int | None = None) -> MoviePilotQueryResult:
        self.queries.append((str(tmdb_id), season))
        return MoviePilotQueryResult(
            MoviePilotQueryDisposition.FOUND,
            {"id": len(self.queries), "tmdbid": int(tmdb_id), "season": season, "state": "R"},
        )

    def create_subscription(self, payload: Mapping[str, Any]) -> MoviePilotWriteResult:
        self.writes.append(dict(payload))
        return MoviePilotWriteResult("create-subscription", 201)


class _Progress:
    def __init__(self) -> None:
        self.updates: list[tuple[int, str]] = []
        self.checkpoints = 0

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        self.checkpoints += 1


def _router(
    runtime: DatabaseRuntime,
    tmdb: _Tmdb,
    moviepilot: _MoviePilot,
    *,
    cap: int,
) -> P7SubscriptionTaskRouter:
    settings = SqlAlchemyAppSettingsRepository(runtime.unit_of_work)
    settings.save_many(
        {
            "mp_config": {"resubscribe_daily_cap": cap},
            "watchlist_config": {},
        }
    )
    operations = PostgresSubscriptionOperationRepository(runtime.unit_of_work)
    executor = SubscriptionOperationExecutor(
        operations,
        moviepilot,  # type: ignore[arg-type]
        max_attempts=2,
        readback_delay_seconds=0,
    )
    effects = PersistentMoviePilotEffectGateway(
        operations,
        executor,
        settings,
        sleep=lambda _seconds: None,
    )
    service = ManualSubscriptionBatchService(
        SqlAlchemyManualSubscriptionRepository(
            runtime.unit_of_work,
            SqlAlchemySubscriptionMediaProjection(),
        ),
        settings,
        tmdb,
        effects,
        ManualSubscriptionQuotaLedger(PostgresSubscriptionRuntimeRepository(runtime.unit_of_work)),
        SqlAlchemyWatchlistMediaProjectionRepository(runtime.unit_of_work),
        clock=lambda: datetime(2026, 8, 30, 4, tzinfo=UTC),
    )
    return P7SubscriptionTaskRouter(None, manual_batch=service)


def _message(runtime: DatabaseRuntime, requests: list[Mapping[str, Any]]) -> TaskMessage:
    jobs = PostgresJobStore(runtime.unit_of_work)
    created = jobs.create_job(
        task_key="manual_subscribe_batch",
        payload={"subscribe_requests": requests},
        max_attempts=1,
    ).projection
    worker_id = "fixture-worker"
    claim = jobs.claim_attempt(
        job_id=created.job.id,
        attempt_id=created.current_attempt.id,
        worker_id=worker_id,
        lease_seconds=60,
    )
    assert claim.claimed is True
    jobs.start_attempt(
        job_id=created.job.id,
        attempt_id=created.current_attempt.id,
        worker_id=worker_id,
        lease_seconds=60,
    )
    return TaskMessage.create(
        task_key="manual_subscribe_batch",
        processor=ProcessorIdentity.MEDIA,
        payload={"subscribe_requests": requests},
        metadata={
            "job_id": str(created.job.id),
            "attempt_id": str(created.current_attempt.id),
            "worker_id": worker_id,
        },
    )


def test_manual_movie_confirmation_and_replay_are_ledger_idempotent(
    p7_runtime: DatabaseRuntime,
) -> None:
    with p7_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM media_metadata WHERE tmdb_id = '880001' AND item_type = 'Movie'"))
    tmdb = _Tmdb(
        movies={
            880001: {
                "title": "Fixture Movie",
                "release_dates": {"results": [{"release_dates": [{"type": 4, "release_date": "2026-08-01"}]}]},
            }
        }
    )
    moviepilot = _MoviePilot()
    router = _router(p7_runtime, tmdb, moviepilot, cap=2)
    message = _message(
        p7_runtime, [{"tmdb_id": "880001", "item_type": "Movie", "title": "Fixture Movie", "user_id": "user-1"}]
    )

    first = router.execute(message, None, _Progress())
    replay = router.execute(message, None, _Progress())

    assert first.status is TaskOutcomeStatus.SUCCEEDED
    assert replay.status is TaskOutcomeStatus.SUCCEEDED
    assert len(moviepilot.writes) == 1
    with p7_runtime.engine.connect() as connection:
        state = (
            connection.execute(
                text(
                    """
                    SELECT
                        (SELECT subscription_status FROM media_metadata
                         WHERE tmdb_id = '880001' AND item_type = 'Movie') AS media_status,
                        (SELECT COUNT(*) FROM subscription_operations) AS operations,
                        (SELECT COUNT(*) FROM subscription_quota_reservations) AS quota_rows,
                        (SELECT status FROM subscription_quota_reservations LIMIT 1) AS quota_status,
                        (SELECT COUNT(*) FROM outbox_events
                         WHERE topic = 'subscription.operation.requested') AS outbox_rows,
                        (SELECT status FROM outbox_events
                         WHERE topic = 'subscription.operation.requested' LIMIT 1) AS outbox_status
                    """
                )
            )
            .mappings()
            .one()
        )
    assert dict(state) == {
        "media_status": "SUBSCRIBED",
        "operations": 1,
        "quota_rows": 1,
        "quota_status": "committed",
        "outbox_rows": 1,
        "outbox_status": "processed",
    }


def test_manual_series_keeps_first_season_when_second_exhausts_quota(
    p7_runtime: DatabaseRuntime,
) -> None:
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                DELETE FROM media_metadata
                WHERE tmdb_id IN ('880101', '8801011', '8801012')
                   OR parent_series_tmdb_id = '880101'
                """
            )
        )
    tmdb = _Tmdb(
        series={
            880101: {
                "name": "Fixture Series",
                "status": "Returning Series",
                "seasons": [
                    {"id": 8801011, "season_number": 1, "air_date": "2025-01-01", "name": "S1"},
                    {"id": 8801012, "season_number": 2, "air_date": "2026-01-01", "name": "S2"},
                ],
            }
        }
    )
    moviepilot = _MoviePilot()
    router = _router(p7_runtime, tmdb, moviepilot, cap=1)

    outcome = router.execute(
        _message(
            p7_runtime,
            [{"tmdb_id": "880101", "item_type": "Series", "title": "Fixture Series"}],
        ),
        None,
        _Progress(),
    )

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["processed"] == 1
    assert outcome.details["quota_exhausted"] is True
    assert len(moviepilot.writes) == 1
    with p7_runtime.engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT tmdb_id, subscription_status
                FROM media_metadata
                WHERE tmdb_id IN ('8801011', '8801012')
                ORDER BY tmdb_id
                """
            )
        ).all()
        counts = (
            connection.execute(
                text(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM subscription_operations) AS operations,
                        (SELECT COUNT(*) FROM subscription_quota_reservations) AS quota_rows,
                        (SELECT COUNT(*) FROM outbox_events
                         WHERE topic = 'subscription.operation.requested') AS outbox_rows
                    """
                )
            )
            .mappings()
            .one()
        )
    assert rows == [("8801011", "SUBSCRIBED")]
    assert dict(counts) == {"operations": 1, "quota_rows": 1, "outbox_rows": 1}
