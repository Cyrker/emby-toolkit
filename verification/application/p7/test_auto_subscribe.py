from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from typing import Any

import pytest

from etk.domains.subscription.auto_subscribe import (
    AutoSourceDisposition,
    AutoSourceResult,
    AutoSubscribeService,
    AutoSubscriptionItemResult,
)


class _Settings:
    def __init__(self, values: Mapping[str, Any] | None = None) -> None:
        self.values = dict(values or {})

    def load(self, key: str) -> Any:
        return self.values.get(key, {})


class _Repository:
    def __init__(self) -> None:
        self.promoted = 0
        self.promotion_calls = 0
        self.revive: list[Mapping[str, Any]] = []
        self.pause: list[Mapping[str, Any]] = []
        self.items: list[Mapping[str, Any]] = []
        self.series_titles: dict[str, str] = {}
        self.revived: list[str] = []
        self.paused: list[tuple[str, int]] = []
        self.subscribed: list[dict[str, Any]] = []

    def promote_pending_to_wanted(self) -> int:
        self.promotion_calls += 1
        return self.promoted

    def movies_to_revive(self) -> Sequence[Mapping[str, Any]]:
        return self.revive

    def movies_to_pause(self, *, search_window_days: int) -> Sequence[Mapping[str, Any]]:
        assert search_window_days > 0
        return self.pause

    def mark_movie_revived(self, tmdb_id: str) -> None:
        self.revived.append(tmdb_id)

    def mark_movie_paused(self, tmdb_id: str, *, pause_days: int) -> None:
        self.paused.append((tmdb_id, pause_days))

    def worklist(self) -> Sequence[Mapping[str, Any]]:
        return self.items

    def series_title(self, tmdb_id: str) -> str | None:
        return self.series_titles.get(tmdb_id)

    def mark_subscribed(self, item: Mapping[str, Any]) -> None:
        self.subscribed.append(dict(item))


class _Tmdb:
    def __init__(self) -> None:
        self.movies: dict[int, Mapping[str, Any]] = {}
        self.calls: list[int] = []

    def movie(self, tmdb_id: int) -> Mapping[str, Any]:
        self.calls.append(tmdb_id)
        return self.movies.get(tmdb_id, {})


class _Quota:
    def __init__(self, values: list[int] | None = None) -> None:
        self.values = list(values or [10])
        self.calls: list[tuple[date, int]] = []

    def remaining(self, *, quota_date: date, daily_cap: int) -> int:
        self.calls.append((quota_date, daily_cap))
        return self.values.pop(0) if len(self.values) > 1 else self.values[0]


class _MoviePilot:
    def __init__(self) -> None:
        self.status_results: list[bool] = []
        self.create_results: list[bool] = []
        self.item_results: list[AutoSubscriptionItemResult] = []
        self.status_calls: list[tuple[str, int | None, str, int | None]] = []
        self.create_calls: list[tuple[str, str]] = []
        self.item_calls: list[dict[str, Any]] = []

    def update_status(
        self,
        tmdb_id: str,
        *,
        season: int | None,
        status: str,
        total_episodes: int | None = None,
    ) -> bool:
        self.status_calls.append((tmdb_id, season, status, total_episodes))
        return self.status_results.pop(0) if self.status_results else True

    def create_movie(self, tmdb_id: str, title: str) -> bool:
        self.create_calls.append((tmdb_id, title))
        return self.create_results.pop(0) if self.create_results else True

    def subscribe_item(self, item: Mapping[str, Any], progress: Any) -> AutoSubscriptionItemResult:
        progress.checkpoint()
        self.item_calls.append(dict(item))
        return self.item_results.pop(0) if self.item_results else AutoSubscriptionItemResult(True)


class _Sources:
    def __init__(self, results: Mapping[str, Sequence[AutoSourceResult | Exception]] | None = None) -> None:
        self.results = {key: list(value) for key, value in dict(results or {}).items()}
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def attempt(self, source_type: str, context: Mapping[str, Any], progress: Any) -> AutoSourceResult:
        progress.checkpoint()
        self.calls.append((source_type, dict(context)))
        values = self.results.get(source_type, [])
        result = values.pop(0) if values else AutoSourceResult.miss()
        if isinstance(result, Exception):
            raise result
        return result


class _Notifications:
    def __init__(self) -> None:
        self.successes: list[tuple[str, list[str]]] = []
        self.rejections: list[tuple[str, list[str]]] = []
        self.summaries: list[tuple[list[dict[str, Any]], bool]] = []

    def success(self, user_id: str, titles: Sequence[str]) -> None:
        self.successes.append((user_id, list(titles)))

    def rejected(self, user_id: str, titles: Sequence[str]) -> None:
        self.rejections.append((user_id, list(titles)))

    def summary(self, details: Sequence[Mapping[str, Any]], *, quota_exhausted: bool) -> None:
        self.summaries.append(([dict(item) for item in details], quota_exhausted))


class _Progress:
    def __init__(self, *, fail_checkpoint: int | None = None) -> None:
        self.updates: list[tuple[int, str]] = []
        self.checkpoints = 0
        self.fail_checkpoint = fail_checkpoint

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        self.checkpoints += 1
        if self.checkpoints == self.fail_checkpoint:
            raise RuntimeError("cancelled fixture")


def _service(
    *,
    repository: _Repository | None = None,
    settings: Mapping[str, Any] | None = None,
    tmdb: _Tmdb | None = None,
    quota: _Quota | None = None,
    moviepilot: _MoviePilot | None = None,
    sources: _Sources | None = None,
    notifications: _Notifications | None = None,
) -> tuple[
    AutoSubscribeService,
    _Repository,
    _Tmdb,
    _Quota,
    _MoviePilot,
    _Sources,
    _Notifications,
]:
    repository = repository or _Repository()
    tmdb = tmdb or _Tmdb()
    quota = quota or _Quota()
    moviepilot = moviepilot or _MoviePilot()
    sources = sources or _Sources()
    notifications = notifications or _Notifications()
    return (
        AutoSubscribeService(
            repository,
            _Settings(settings),
            tmdb,
            quota,
            moviepilot,
            sources,
            notifications,
            clock=lambda: datetime(2026, 8, 30, 4, tzinfo=UTC),
            sleep=lambda _seconds: None,
        ),
        repository,
        tmdb,
        quota,
        moviepilot,
        sources,
        notifications,
    )


def _released_movie() -> Mapping[str, Any]:
    return {"release_dates": {"results": [{"release_dates": [{"type": 4}]}]}}


def _user_source(user_id: str) -> list[dict[str, str]]:
    return [{"type": "user_request", "user_id": user_id}]


def test_empty_worklist_preserves_revive_pause_results_and_failures() -> None:
    repository = _Repository()
    repository.promoted = 2
    repository.revive = [
        {"tmdb_id": "1", "title": "Revive confirmed"},
        {"tmdb_id": "2", "title": "Revive fallback rejected"},
    ]
    repository.pause = [{"tmdb_id": "3", "title": "Pause fallback"}]
    moviepilot = _MoviePilot()
    moviepilot.status_results = [True, False, False, True]
    moviepilot.create_results = [False, True]
    service, *_rest = _service(repository=repository, moviepilot=moviepilot)

    result = service.execute(_Progress())

    assert result.status == "partial"
    assert result.requested == 0
    assert result.processed == 2
    assert result.promoted == 2
    assert result.revived == 1
    assert result.paused == 1
    assert result.provider_failures == ({"action": "revive", "tmdb_id": "2", "item_type": "Movie", "season": None},)
    assert repository.revived == ["1"]
    assert repository.paused == [("3", 7)]
    assert moviepilot.create_calls == [("2", "Revive fallback rejected"), ("3", "Pause fallback")]


@pytest.mark.parametrize(
    "mp_config",
    [
        {"movie_search_window_days": 0, "movie_pause_days": 7},
        {"movie_search_window_days": 1, "movie_pause_days": -1},
    ],
)
def test_zero_or_negative_maintenance_config_disables_revive_and_pause(
    mp_config: Mapping[str, Any],
) -> None:
    repository = _Repository()
    repository.revive = [{"tmdb_id": "1", "title": "Revive"}]
    repository.pause = [{"tmdb_id": "2", "title": "Pause"}]
    service, _repository, _tmdb, _quota, moviepilot, _sources, _notifications = _service(
        repository=repository,
        settings={"mp_config": mp_config},
    )

    result = service.execute(_Progress())

    assert result.processed == 0
    assert result.revived == 0
    assert result.paused == 0
    assert moviepilot.status_calls == []


def test_source_order_release_guard_nonwanted_mp_fence_and_notifications() -> None:
    repository = _Repository()
    repository.items = [
        {
            "tmdb_id": "10",
            "item_type": "Movie",
            "title": "Unreleased",
            "subscription_status": "WANTED",
            "subscription_sources_json": _user_source("u1"),
        },
        {
            "tmdb_id": "11",
            "item_type": "Movie",
            "title": "Shared",
            "subscription_status": "WANTED",
            "subscription_sources_json": _user_source("u1"),
        },
        {
            "tmdb_id": "season-id",
            "item_type": "Season",
            "title": "Season row",
            "parent_series_tmdb_id": "20",
            "season_number": 2,
            "subscription_status": "SUBSCRIBED",
            "missing_episode_numbers": [1, 3],
        },
    ]
    repository.series_titles["20"] = "Series"
    tmdb = _Tmdb()
    tmdb.movies[10] = {"release_dates": {"results": []}}
    tmdb.movies[11] = _released_movie()
    sources = _Sources(
        {
            "shared_pool": [
                AutoSourceResult.confirmed("共享资源秒传"),
                AutoSourceResult.miss(),
            ],
            "hdhive": [AutoSourceResult.confirmed("影巢")],
        }
    )
    service, _repository, _tmdb, _quota, moviepilot, sources, notifications = _service(
        repository=repository,
        settings={"subscription_strategy_config": {"subscription_sources": ["shared_pool", "hdhive", "mp"]}},
        tmdb=tmdb,
        sources=sources,
    )

    result = service.execute(_Progress())

    assert result.status == "succeeded"
    assert result.requested == 3
    assert result.processed == 2
    assert result.business_skipped == 1
    assert moviepilot.item_calls == []
    assert [call[0] for call in sources.calls] == ["shared_pool", "shared_pool", "hdhive"]
    assert sources.calls[-1][1]["title"] == "Series"
    assert sources.calls[-1][1]["missing_episode_numbers"] == [1, 3]
    assert [item["tmdb_id"] for item in repository.subscribed] == ["11"]
    assert notifications.rejections == [("u1", ["Unreleased(原因: 不满足发行日期延迟订阅)"])]
    assert notifications.successes == [("u1", ["Shared"])]


def test_quota_exhaustion_skips_entire_wanted_item_but_keeps_later_recheck() -> None:
    repository = _Repository()
    repository.items = [
        {
            "tmdb_id": "31",
            "item_type": "Movie",
            "title": "Quota stopped",
            "subscription_status": "WANTED",
        },
        {
            "tmdb_id": "32",
            "item_type": "Movie",
            "title": "Recheck",
            "subscription_status": "SUBSCRIBED",
        },
    ]
    tmdb = _Tmdb()
    tmdb.movies[31] = _released_movie()
    sources = _Sources({"shared_pool": [AutoSourceResult.confirmed("共享资源秒传")]})
    service, _repository, _tmdb, quota, moviepilot, sources, notifications = _service(
        repository=repository,
        settings={"subscription_strategy_config": {"subscription_sources": ["shared_pool", "mp"]}},
        tmdb=tmdb,
        quota=_Quota([0]),
        sources=sources,
    )

    result = service.execute(_Progress())

    assert result.status == "succeeded"
    assert result.processed == 1
    assert result.quota_exhausted is True
    assert len(quota.calls) == 1
    assert [call[1]["tmdb_id"] for call in sources.calls] == ["32"]
    assert moviepilot.item_calls == []
    assert notifications.summaries[0][1] is True


def test_indeterminate_source_stops_fallback_and_is_reported() -> None:
    repository = _Repository()
    repository.items = [
        {
            "tmdb_id": "41",
            "item_type": "Movie",
            "title": "Unknown transfer",
            "subscription_status": "WANTED",
        }
    ]
    tmdb = _Tmdb()
    tmdb.movies[41] = _released_movie()
    sources = _Sources(
        {
            "hdhive": [
                AutoSourceResult(
                    AutoSourceDisposition.INDETERMINATE,
                    "影巢",
                    {"action": "hdhive", "tmdb_id": "41", "indeterminate": True},
                )
            ]
        }
    )
    service, _repository, _tmdb, _quota, moviepilot, sources, _notifications = _service(
        repository=repository,
        settings={"subscription_strategy_config": {"subscription_sources": ["hdhive", "mp"]}},
        tmdb=tmdb,
        sources=sources,
    )

    result = service.execute(_Progress())

    assert result.status == "failed"
    assert result.processed == 0
    assert result.provider_failures == ({"action": "hdhive", "tmdb_id": "41", "indeterminate": True},)
    assert moviepilot.item_calls == []


def test_unknown_source_exception_is_indeterminate_and_stops_fallback() -> None:
    repository = _Repository()
    repository.items = [
        {
            "tmdb_id": "42",
            "item_type": "Movie",
            "title": "Unknown source exception",
            "subscription_status": "WANTED",
        }
    ]
    tmdb = _Tmdb()
    tmdb.movies[42] = _released_movie()
    sources = _Sources({"hdhive": [RuntimeError("unknown mutation result")]})
    service, _repository, _tmdb, _quota, moviepilot, _sources, _notifications = _service(
        repository=repository,
        settings={"subscription_strategy_config": {"subscription_sources": ["hdhive", "mp"]}},
        tmdb=tmdb,
        sources=sources,
    )

    result = service.execute(_Progress())

    assert result.status == "failed"
    assert result.provider_failures[0]["error_type"] == "RuntimeError"
    assert result.provider_failures[0]["indeterminate"] is True
    assert moviepilot.item_calls == []


def test_moviepilot_rejection_falls_through_and_is_discarded_after_later_success() -> None:
    repository = _Repository()
    repository.items = [
        {
            "tmdb_id": "43",
            "item_type": "Movie",
            "title": "Fallback item",
            "subscription_status": "WANTED",
        }
    ]
    tmdb = _Tmdb()
    tmdb.movies[43] = _released_movie()
    moviepilot = _MoviePilot()
    moviepilot.item_results = [
        AutoSubscriptionItemResult(
            False,
            ({"action": "create", "tmdb_id": "43", "item_type": "Movie", "season": None},),
        )
    ]
    sources = _Sources({"shared_pool": [AutoSourceResult.confirmed("共享资源秒传")]})
    service, _repository, _tmdb, _quota, _moviepilot, sources, _notifications = _service(
        repository=repository,
        settings={"subscription_strategy_config": {"subscription_sources": ["mp", "shared_pool"]}},
        tmdb=tmdb,
        moviepilot=moviepilot,
        sources=sources,
    )

    result = service.execute(_Progress())

    assert result.status == "succeeded"
    assert result.processed == 1
    assert result.provider_failures == ()
    assert [call[0] for call in sources.calls] == ["shared_pool"]
    assert [item["tmdb_id"] for item in repository.subscribed] == ["43"]


def test_moviepilot_partial_counts_item_and_cancellation_stops_before_source() -> None:
    repository = _Repository()
    repository.items = [
        {
            "tmdb_id": "51",
            "item_type": "Movie",
            "title": "MP partial",
            "subscription_status": "WANTED",
        }
    ]
    tmdb = _Tmdb()
    tmdb.movies[51] = _released_movie()
    moviepilot = _MoviePilot()
    moviepilot.item_results = [
        AutoSubscriptionItemResult(
            True,
            ({"action": "update_status", "tmdb_id": "51", "item_type": "Movie", "season": None},),
        )
    ]
    service, *_rest = _service(
        repository=repository,
        settings={"subscription_strategy_config": {"subscription_sources": ["mp"]}},
        tmdb=tmdb,
        moviepilot=moviepilot,
    )

    result = service.execute(_Progress())

    assert result.status == "partial"
    assert result.processed == 1
    assert repository.subscribed == []

    cancelled_repository = _Repository()
    cancelled_repository.items = list(repository.items)
    cancelled, *_rest = _service(repository=cancelled_repository, tmdb=tmdb, moviepilot=moviepilot)
    with pytest.raises(RuntimeError, match="cancelled fixture"):
        cancelled.execute(_Progress(fail_checkpoint=1))
    assert cancelled_repository.promotion_calls == 0


def test_moviepilot_local_projection_is_not_applied_twice() -> None:
    repository = _Repository()
    repository.items = [
        {
            "tmdb_id": "55",
            "item_type": "Movie",
            "title": "MP confirmed",
            "subscription_status": "WANTED",
        }
    ]
    tmdb = _Tmdb()
    tmdb.movies[55] = _released_movie()
    moviepilot = _MoviePilot()
    moviepilot.item_results = [AutoSubscriptionItemResult(True, local_projection_applied=True)]
    service, *_rest = _service(
        repository=repository,
        settings={"subscription_strategy_config": {"subscription_sources": ["mp"]}},
        tmdb=tmdb,
        moviepilot=moviepilot,
    )

    result = service.execute(_Progress())

    assert result.processed == 1
    assert repository.subscribed == []


def test_request_delay_has_checkpoints_on_both_sides() -> None:
    repository = _Repository()
    repository.items = [
        {"tmdb_id": "61", "item_type": "Episode", "title": "E1", "subscription_status": "NONE"},
        {"tmdb_id": "62", "item_type": "Episode", "title": "E2", "subscription_status": "NONE"},
    ]
    progress = _Progress()
    service, *_rest = _service(
        repository=repository,
        settings={"mp_config": {"resubscribe_delay_seconds": 1}},
    )

    result = service.execute(progress)

    assert result.processed == 0
    assert progress.checkpoints == 4
