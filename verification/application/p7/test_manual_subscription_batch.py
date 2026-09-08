from __future__ import annotations

import builtins
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from etk.domains.media.subscription_projection import SubscriptionMediaKey
from etk.domains.subscription.manual_batch import ManualSubscriptionBatchService
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcomeStatus
from etk.processes.p7_subscription_effects import (
    PersistentMoviePilotEffectGateway,
    bind_moviepilot_execution,
)
from etk.processes.p7_subscription_tasks import P7SubscriptionTaskRouter


class _Settings:
    def __init__(self, values: Mapping[str, Any] | None = None) -> None:
        self.values = dict(values or {})

    def load(self, key: str) -> Any:
        return self.values.get(key, {})

    def save(self, key: str, value: Any) -> None:
        self.values[key] = value

    def delete(self, key: str) -> bool:
        return self.values.pop(key, None) is not None


class _Tmdb:
    def __init__(self) -> None:
        self.movies: dict[int, Mapping[str, Any]] = {}
        self.series: dict[int, Mapping[str, Any]] = {}
        self.seasons: dict[tuple[int, int], Mapping[str, Any]] = {}
        self.calls: list[tuple[Any, ...]] = []

    def movie(self, tmdb_id: int) -> Mapping[str, Any]:
        self.calls.append(("movie", tmdb_id))
        return self.movies.get(tmdb_id, {})

    def tv(self, tmdb_id: int) -> Mapping[str, Any]:
        self.calls.append(("tv", tmdb_id))
        return self.series.get(tmdb_id, {})

    def season(self, tmdb_id: int, season_number: int) -> Mapping[str, Any]:
        self.calls.append(("season", tmdb_id, season_number))
        return self.seasons.get((tmdb_id, season_number), {})


class _Effects:
    def __init__(self, creates: list[bool] | None = None, updates: list[bool] | None = None) -> None:
        self.creates = list(creates or [])
        self.updates = list(updates or [])
        self.create_calls: list[tuple[dict[str, Any], bool, bool]] = []
        self.update_calls: list[dict[str, Any]] = []

    def create_subscription(
        self,
        payload: Mapping[str, Any],
        *,
        consume_quota: bool,
        apply_local_continuation: bool = True,
    ) -> bool:
        self.create_calls.append((dict(payload), consume_quota, apply_local_continuation))
        return self.creates.pop(0) if self.creates else True

    def update_subscription_status(
        self,
        tmdb_id: int | str,
        *,
        season: int | None,
        status: str,
        total_episodes: int | None,
    ) -> bool:
        self.update_calls.append(
            {
                "tmdb_id": str(tmdb_id),
                "season": season,
                "status": status,
                "total_episodes": total_episodes,
            }
        )
        return self.updates.pop(0) if self.updates else True


class _Quota:
    def __init__(self, values: list[int] | None = None) -> None:
        self.values = list(values or [10])
        self.calls: list[tuple[date, int]] = []

    def remaining(self, *, quota_date: date, daily_cap: int) -> int:
        self.calls.append((quota_date, daily_cap))
        return self.values.pop(0) if len(self.values) > 1 else self.values[0]


class _Repository:
    def __init__(self) -> None:
        self.media_rows: dict[tuple[str, str], Mapping[str, Any]] = {}
        self.series_titles: dict[str, str] = {}
        self.season_keys: dict[tuple[str, int], SubscriptionMediaKey] = {}
        self.statuses: list[tuple[dict[str, Any], str, Mapping[str, Any] | None]] = []

    def media(self, tmdb_id: str, item_type: str) -> Mapping[str, Any] | None:
        return self.media_rows.get((tmdb_id, item_type))

    def series_title(self, tmdb_id: str) -> str | None:
        return self.series_titles.get(tmdb_id)

    def season_key(self, parent_series_tmdb_id: str, season_number: int) -> SubscriptionMediaKey | None:
        return self.season_keys.get((parent_series_tmdb_id, season_number))

    def set_status(
        self,
        record: Mapping[str, Any],
        status: str,
        source: Mapping[str, Any] | None,
    ) -> None:
        self.statuses.append((dict(record), status, dict(source) if source is not None else None))


class _Watchlist:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def add_watchlist_item(self, tmdb_id: str, item_name: str) -> None:
        self.calls.append((tmdb_id, item_name))


class _Progress:
    def __init__(self, *, fail_checkpoint: int | None = None) -> None:
        self.updates: list[tuple[int, str]] = []
        self.checkpoints = 0
        self.fail_checkpoint = fail_checkpoint

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        self.checkpoints += 1
        if self.fail_checkpoint == self.checkpoints:
            raise RuntimeError("cancelled fixture")


def _service(
    *,
    settings: Mapping[str, Any] | None = None,
    tmdb: _Tmdb | None = None,
    effects: _Effects | None = None,
    quota: _Quota | None = None,
    repository: _Repository | None = None,
    watchlist: _Watchlist | None = None,
) -> tuple[ManualSubscriptionBatchService, _Tmdb, _Effects, _Quota, _Repository, _Watchlist]:
    tmdb_gateway = tmdb or _Tmdb()
    effect_gateway = effects or _Effects()
    quota_gateway = quota or _Quota()
    media_repository = repository or _Repository()
    watchlist_gateway = watchlist or _Watchlist()
    return (
        ManualSubscriptionBatchService(
            media_repository,
            _Settings(settings),
            tmdb_gateway,
            effect_gateway,
            quota_gateway,
            watchlist_gateway,
            clock=lambda: datetime(2026, 8, 30, 4, tzinfo=UTC),
        ),
        tmdb_gateway,
        effect_gateway,
        quota_gateway,
        media_repository,
        watchlist_gateway,
    )


def _released_movie(title: str = "Fixture") -> dict[str, Any]:
    return {
        "title": title,
        "release_dates": {"results": [{"release_dates": [{"type": 4, "release_date": "2026-08-01"}]}]},
    }


def test_empty_invalid_and_quota_exhausted_batches_keep_legacy_counters() -> None:
    service, _tmdb, effects, quota, repository, _watchlist = _service(quota=_Quota([0]))
    progress = _Progress()

    empty = service.execute([], progress)
    result = service.execute(
        [
            {},
            {"tmdb_id": "1", "item_type": "Unknown", "title": "Unknown"},
            {"tmdb_id": "2", "item_type": "Movie", "title": "Stopped"},
        ],
        progress,
    )

    assert empty.status == "succeeded"
    assert empty.requested == 0
    assert result.status == "succeeded"
    assert result.processed == 0
    assert result.business_skipped == 2
    assert result.quota_exhausted is True
    assert result.provider_failures == ()
    assert len(quota.calls) == 1
    assert effects.create_calls == []
    assert repository.statuses == []


def test_movie_release_policy_skip_and_confirmed_provider_projection() -> None:
    tmdb = _Tmdb()
    tmdb.movies[1] = {"title": "Future", "release_dates": {"results": []}}
    tmdb.movies[2] = _released_movie("Released")
    service, _tmdb, effects, _quota, repository, _watchlist = _service(tmdb=tmdb)

    result = service.execute(
        [
            {"tmdb_id": "1", "item_type": "Movie", "title": "Future"},
            {"tmdb_id": "2", "item_type": "Movie", "title": "Released", "user_id": "user-1"},
        ],
        _Progress(),
    )

    assert result.status == "succeeded"
    assert result.processed == 1
    assert result.business_skipped == 1
    assert effects.create_calls == [({"name": "Released", "tmdbid": 2, "type": "电影"}, True, False)]
    assert repository.statuses == [
        (
            {
                "tmdb_id": "2",
                "item_type": "Movie",
                "title": "Released",
                "original_title": None,
                "release_date": None,
                "poster_path": None,
                "backdrop_path": None,
                "overview": None,
            },
            "SUBSCRIBED",
            {"type": "user_request", "user_id": "user-1"},
        )
    ]


def test_movie_provider_rejection_is_non_retryable_failed_outcome() -> None:
    tmdb = _Tmdb()
    tmdb.movies[101] = _released_movie()
    service, _tmdb, _effects, _quota, repository, _watchlist = _service(
        tmdb=tmdb,
        effects=_Effects([False]),
    )

    result = service.execute(
        [{"tmdb_id": "101", "item_type": "Movie", "title": "Fixture"}],
        _Progress(),
    )

    assert result.status == "failed"
    assert result.processed == 0
    assert result.provider_failures == ({"action": "create", "tmdb_id": "101", "item_type": "Movie", "season": None},)
    assert repository.statuses == []


def test_season_parent_resolution_tg_tracking_skips_provider_but_updates_local() -> None:
    repository = _Repository()
    repository.media_rows[("season-id", "Season")] = {
        "tmdb_id": "season-id",
        "item_type": "Season",
        "title": "Fixture S02",
        "parent_series_tmdb_id": "101",
        "season_number": 2,
    }
    repository.series_titles["101"] = "Fixture"
    repository.season_keys[("101", 2)] = SubscriptionMediaKey("season-id", "Season")
    tmdb = _Tmdb()
    tmdb.series[101] = {"name": "Fixture", "status": "Returning Series"}
    service, _tmdb, effects, _quota, repository, _watchlist = _service(
        settings={"watchlist_config": {"tg_channel_tracking": True}},
        tmdb=tmdb,
        repository=repository,
    )

    result = service.execute(
        [{"tmdb_id": "season-id", "item_type": "Season", "title": "Fixture S02"}],
        _Progress(),
    )

    assert result.processed == 1
    assert effects.create_calls == []
    assert repository.statuses[0][0]["tmdb_id"] == "season-id"
    assert repository.statuses[0][0]["parent_series_tmdb_id"] == "101"
    assert repository.statuses[0][1] == "SUBSCRIBED"


def test_invalid_season_identity_and_empty_series_details_are_business_skips() -> None:
    service, tmdb, effects, quota, repository, watchlist = _service()

    result = service.execute(
        [
            {"tmdb_id": "202", "item_type": "Season", "season_number": 2, "title": "Missing parent"},
            {
                "tmdb_id": "season-id",
                "item_type": "Season",
                "parent_series_tmdb_id": "101",
                "season_number": 0,
                "title": "Invalid season",
            },
            {"tmdb_id": "303", "item_type": "Series", "title": "Missing details"},
        ],
        _Progress(),
    )

    assert result.status == "succeeded"
    assert result.processed == 0
    assert result.business_skipped == 3
    assert result.provider_failures == ()
    assert len(quota.calls) == 3
    assert tmdb.calls == [("tv", 303)]
    assert effects.create_calls == []
    assert repository.statuses == []
    assert watchlist.calls == []


def test_series_future_and_auto_pending_seasons_preserve_partial_result() -> None:
    tmdb = _Tmdb()
    tmdb.series[101] = {
        "name": "Fixture",
        "status": "Returning Series",
        "poster_path": "/series.jpg",
        "backdrop_path": "/backdrop.jpg",
        "seasons": [
            {"id": 1001, "season_number": 1, "air_date": "2030-01-01", "name": "S1"},
            {
                "id": 1002,
                "season_number": 2,
                "air_date": "2026-08-20",
                "episode_count": 1,
                "name": "S2",
            },
        ],
    }
    effects = _Effects([True], [False])
    service, _tmdb, effects, _quota, repository, watchlist = _service(
        settings={
            "watchlist_config": {
                "auto_pending": {
                    "enabled": True,
                    "days": 30,
                    "episodes": 1,
                    "default_total_episodes": 99,
                }
            }
        },
        tmdb=tmdb,
        effects=effects,
    )

    result = service.execute(
        [{"tmdb_id": "101", "item_type": "Series", "title": "Fixture"}],
        _Progress(),
    )

    assert result.status == "partial"
    assert result.processed == 1
    assert result.provider_failures == (
        {"action": "update_status", "tmdb_id": "101", "item_type": "Season", "season": 2},
    )
    assert watchlist.calls == [("101", "Fixture")]
    assert effects.create_calls == [({"name": "Fixture", "tmdbid": 101, "type": "电视剧", "season": 2}, True, False)]
    assert effects.update_calls == [{"tmdb_id": "101", "season": 2, "status": "P", "total_episodes": 99}]
    assert [(status[0]["tmdb_id"], status[1]) for status in repository.statuses] == [("1001", "PENDING_RELEASE")]


def test_cancellation_before_first_item_prevents_all_effects() -> None:
    service, _tmdb, effects, _quota, repository, _watchlist = _service()

    with pytest.raises(RuntimeError, match="cancelled fixture"):
        service.execute(
            [{"tmdb_id": "1", "item_type": "Movie", "title": "Fixture"}],
            _Progress(fail_checkpoint=1),
        )

    assert effects.create_calls == []
    assert repository.statuses == []


def test_router_executes_manual_batch_without_processor_or_compatibility() -> None:
    service, tmdb, _effects, _quota, _repository, _watchlist = _service()
    tmdb.movies[101] = _released_movie()
    router = P7SubscriptionTaskRouter(None, manual_batch=service)
    message = TaskMessage.create(
        task_key="manual_subscribe_batch",
        processor=ProcessorIdentity.MEDIA,
        payload={"subscribe_requests": [{"tmdb_id": "101", "item_type": "Movie", "title": "Fixture"}]},
        metadata={
            "job_id": "00000000-0000-0000-0000-000000000101",
            "attempt_id": "00000000-0000-0000-0000-000000000102",
            "worker_id": "fixture-worker",
        },
    )

    outcome = router.execute(message, None, _Progress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["compatibility_fallback"] is False
    assert outcome.details["processed"] == 1
    assert outcome.details["retryable"] is False


def test_explicit_moviepilot_binding_does_not_import_task_manager(monkeypatch) -> None:
    gateway = PersistentMoviePilotEffectGateway(
        object(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        _Settings({"mp_config": {"resubscribe_daily_cap": 2}}),
    )
    intents: list[Any] = []
    monkeypatch.setattr(gateway, "_execute", lambda intent: intents.append(intent) is None or True)
    original_import = builtins.__import__

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "task_manager":
            raise AssertionError("native manual batch imported task_manager")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    with bind_moviepilot_execution(
        job_id="00000000-0000-0000-0000-000000000201",
        attempt_id="00000000-0000-0000-0000-000000000202",
        worker_id="fixture-worker",
        task_key="manual_subscribe_batch",
        checkpoint=lambda: None,
    ):
        assert gateway.create_subscription(
            {"name": "Fixture", "tmdbid": 101, "type": "电影"},
            consume_quota=True,
            apply_local_continuation=False,
        )

    assert len(intents) == 1
    assert intents[0].request["origin"] == {
        "task_key": "manual_subscribe_batch",
        "job_id": "00000000-0000-0000-0000-000000000201",
    }
    assert "local_continuation" not in intents[0].request


def test_user_portal_submits_manual_batch_by_task_key_without_legacy_callable() -> None:
    source = (Path(__file__).resolve().parents[3] / "routes" / "user_portal.py").read_text(encoding="utf-8")

    assert "from tasks.subscriptions import task_manual_subscribe_batch" not in source
    assert "task_function=task_manual_subscribe_batch" not in source
    assert 'submit_persistent_task_key(\n            "manual_subscribe_batch"' in source
    assert "req_item['series_tmdb_id'] = series_tmdb_id" in source


@pytest.mark.parametrize(
    "payload,expected_exception",
    [
        ({}, TypeError),
        ({"subscribe_requests": {}}, TypeError),
        ({"subscribe_requests": ["bad"]}, TypeError),
        ({"subscribe_requests": [], "extra": True}, ValueError),
    ],
)
def test_router_rejects_invalid_manual_batch_envelopes(
    payload: Mapping[str, Any],
    expected_exception: type[Exception],
) -> None:
    service, *_rest = _service()
    router = P7SubscriptionTaskRouter(None, manual_batch=service)
    message = TaskMessage.create(
        task_key="manual_subscribe_batch",
        processor=ProcessorIdentity.MEDIA,
        payload=payload,
        metadata={
            "job_id": "00000000-0000-0000-0000-000000000101",
            "attempt_id": "00000000-0000-0000-0000-000000000102",
            "worker_id": "fixture-worker",
        },
    )

    with pytest.raises(expected_exception):
        router.execute(message, None, _Progress())
