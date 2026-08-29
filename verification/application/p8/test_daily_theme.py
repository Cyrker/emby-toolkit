from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

from etk.domains.media.daily_theme import DailyThemePublishResult, DailyThemeService, DailyThemeSnapshot
from etk.domains.media.daily_theme_effects import DailyThemeReplenishmentEffectDelivery
from etk.domains.media.effect_execution import MediaEffectDeliveryRetryable
from etk.domains.media.effects import MediaEffectProjection
from etk.platform.tasks import TaskOutcomeStatus


class FakeProvider:
    def __init__(self) -> None:
        self.configured = True
        self.pages: dict[int, Mapping[str, Any] | None | BaseException] = {}
        self.details: dict[int, Mapping[str, Any] | None | BaseException] = {}
        self.discover_calls: list[dict[str, Any]] = []
        self.detail_calls: list[int] = []

    def daily_theme_configured(self) -> bool:
        return self.configured

    def discover_movie(self, payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
        values = dict(payload)
        self.discover_calls.append(values)
        result = self.pages.get(int(values["page"]), {"results": []})
        if isinstance(result, BaseException):
            raise result
        return deepcopy(result)

    def movie_details(self, movie_id: int) -> Mapping[str, Any] | None:
        self.detail_calls.append(movie_id)
        result = self.details.get(movie_id)
        if isinstance(result, BaseException):
            raise result
        return deepcopy(result)


class FakeRepository:
    def __init__(self) -> None:
        self.snapshot = DailyThemeSnapshot(
            values={
                "keyword_mapping": [{"label": "科幻", "ids": [11, 22]}],
                "recommendation_pool": [{"id": 900}],
                "recommendation_theme_index": -1,
                "recommendation_pool_page": 3,
                "recommendation_publication_state": {"revision": 4},
            },
            fingerprint="fixture-fingerprint",
            publication_revision=4,
        )
        self.excluded: set[str] = set()
        self.names: dict[int, str] = {7: "张三"}
        self.actor_calls: list[tuple[int, ...]] = []
        self.published_revision: int | None = 5
        self.publish_calls: list[dict[str, Any]] = []
        self.receipts: dict[str, dict[str, Any]] = {}

    def load_snapshot(self) -> DailyThemeSnapshot:
        return self.snapshot

    def load_receipt(self, publication_id: str) -> Mapping[str, Any] | None:
        return deepcopy(self.receipts.get(publication_id))

    def excluded_movie_ids(self, tmdb_ids: Sequence[str]) -> set[str]:
        assert tuple(tmdb_ids)
        return set(self.excluded)

    def actor_names(self, actor_ids: Sequence[int]) -> Mapping[int, str]:
        self.actor_calls.append(tuple(actor_ids))
        return dict(self.names)

    def publish(
        self,
        *,
        expected_fingerprint: str,
        pool: Sequence[Mapping[str, Any]],
        theme_index: int,
        pool_page: int,
        metadata: Mapping[str, Any],
    ) -> DailyThemePublishResult | None:
        self.publish_calls.append(
            {
                "expected_fingerprint": expected_fingerprint,
                "pool": [dict(item) for item in pool],
                "theme_index": theme_index,
                "pool_page": pool_page,
                "metadata": dict(metadata),
            }
        )
        if self.published_revision is None:
            return None
        receipt = {**dict(metadata), "revision": self.published_revision}
        self.receipts[str(metadata["publication_id"])] = receipt
        return DailyThemePublishResult(
            revision=self.published_revision,
            receipt=receipt,
        )


class FakeDefaults:
    def keyword_mapping(self) -> Sequence[Mapping[str, Any]]:
        return [{"label": "默认", "ids": [99]}]


class FakeProgress:
    def __init__(self) -> None:
        self.updates: list[tuple[int, str]] = []
        self.checkpoints = 0

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        self.checkpoints += 1


def _movie(movie_id: int, *, title: str = "中文电影", overview: str = "简介", poster: str = "/p.jpg") -> dict[str, Any]:
    return {
        "id": movie_id,
        "title": title,
        "overview": overview,
        "poster_path": poster,
    }


def _details(movie_id: int) -> dict[str, Any]:
    return {
        "id": movie_id,
        "title": f"电影 {movie_id}",
        "overview": "详情",
        "poster_path": f"/{movie_id}.jpg",
        "release_date": "2026-08-30",
        "vote_average": 8.5,
        "credits": {
            "cast": [
                {
                    "id": 7,
                    "name": "Actor",
                    "profile_path": "/actor.jpg",
                    "character": "角色",
                }
            ]
        },
    }


def _service(provider: FakeProvider, repository: FakeRepository) -> DailyThemeService:
    return DailyThemeService(
        provider,
        repository,
        FakeDefaults(),
        clock=lambda: datetime(2026, 8, 30, 1, 2, 3, tzinfo=UTC),
    )


def _execute(
    service: DailyThemeService,
    progress: FakeProgress | None = None,
    *,
    publication_id: str = "00000000-0000-0000-0000-000000000294",
):
    return service.execute(
        progress or FakeProgress(),
        publication_id=publication_id,
    )


def test_daily_theme_filters_candidates_and_publishes_exact_legacy_shape() -> None:
    provider = FakeProvider()
    repository = FakeRepository()
    provider.pages[1] = {
        "results": [
            _movie(1),
            _movie(2, title="English"),
            _movie(3, overview=""),
            _movie(4, poster=""),
            _movie(5),
            _movie(1),
        ]
    }
    provider.pages[2] = {"results": []}
    provider.details[1] = _details(1)
    repository.excluded.add("5")
    progress = FakeProgress()

    outcome = _execute(_service(provider, repository), progress)

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details == {
        "publication": "committed",
        "published": True,
        "publication_id": "00000000-0000-0000-0000-000000000294",
        "mode": "daily",
        "theme_index": 0,
        "theme_label": "科幻",
        "pool_page": 1,
        "pages_scanned": 1,
        "pool_size": 1,
        "added_count": 1,
        "empty_result": False,
        "retained_previous_pool": False,
        "publication_revision": 5,
        "publication_conflict": False,
        "detail_failures": 0,
    }
    assert provider.discover_calls == [
        {
            "with_keywords": "11|22",
            "sort_by": "popularity.desc",
            "page": 1,
            "include_adult": False,
        },
        {
            "with_keywords": "11|22",
            "sort_by": "popularity.desc",
            "page": 2,
            "include_adult": False,
        },
    ]
    assert provider.detail_calls == [1]
    assert repository.actor_calls == [(7,)]
    publication = repository.publish_calls[0]
    assert publication["expected_fingerprint"] == "fixture-fingerprint"
    assert publication["pool"] == [
        {
            "id": 1,
            "title": "电影 1",
            "overview": "详情",
            "poster_path": "/1.jpg",
            "release_date": "2026-08-30",
            "vote_average": 8.5,
            "cast": [
                {
                    "id": 7,
                    "name": "Actor",
                    "name_cn": "张三",
                    "profile_path": "/actor.jpg",
                    "character": "角色",
                }
            ],
            "media_type": "movie",
        }
    ]
    assert publication["metadata"] == {
        "publication_id": "00000000-0000-0000-0000-000000000294",
        "mode": "daily",
        "source_revision": 4,
        "source_fingerprint": "fixture-fingerprint",
        "theme_index": 0,
        "theme_label": "科幻",
        "pool_page": 1,
        "pool_size": 1,
        "pages_scanned": 1,
        "added_count": 1,
        "published_at": "2026-08-30T01:02:03+00:00",
    }
    assert progress.updates[-1][0] == 100


def test_provider_failure_after_a_successful_page_retains_the_previous_publication() -> None:
    provider = FakeProvider()
    repository = FakeRepository()
    provider.pages[1] = {"results": [_movie(1)]}
    provider.pages[2] = TimeoutError("fixture timeout")
    provider.details[1] = _details(1)

    outcome = _execute(_service(provider, repository))

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert outcome.details["reason_code"] == "provider_unavailable"
    assert outcome.details["published"] is False
    assert outcome.details["retained_previous_pool"] is True
    assert outcome.details["pool_size"] == 1
    assert outcome.details["expected_revision"] == 4
    assert outcome.details["retryable"] is True
    assert repository.publish_calls == []


def test_detail_failure_is_all_or_nothing_and_configuration_failure_is_not_retryable() -> None:
    provider = FakeProvider()
    repository = FakeRepository()
    provider.pages[1] = {"results": [_movie(1)]}
    provider.details[1] = None

    failed = _execute(_service(provider, repository))
    provider.configured = False
    not_configured = _execute(_service(provider, repository))

    assert failed.status is TaskOutcomeStatus.FAILED
    assert failed.details["reason_code"] == "provider_unavailable"
    assert failed.details["retryable"] is True
    assert not_configured.status is TaskOutcomeStatus.FAILED
    assert not_configured.details["reason_code"] == "provider_not_configured"
    assert not_configured.details["retryable"] is False
    assert repository.publish_calls == []


def test_valid_empty_result_is_published_and_stale_plan_is_skipped() -> None:
    provider = FakeProvider()
    repository = FakeRepository()
    provider.pages[1] = {"results": []}

    empty = _execute(_service(provider, repository))
    repository.published_revision = None
    stale = _execute(
        _service(provider, repository),
        publication_id="00000000-0000-0000-0000-000000000295",
    )

    assert empty.status is TaskOutcomeStatus.SUCCEEDED
    assert empty.details["empty_result"] is True
    assert empty.details["pool_size"] == 0
    assert repository.publish_calls[0]["pool"] == []
    assert repository.publish_calls[0]["pool_page"] == 0
    assert stale.status is TaskOutcomeStatus.SKIPPED
    assert stale.details["reason_code"] == "publication_conflict"
    assert stale.details["published"] is False
    assert stale.details["retryable"] is False


def test_empty_mapping_uses_defaults_but_malformed_mapping_fails_closed() -> None:
    provider = FakeProvider()
    repository = FakeRepository()
    repository.snapshot = DailyThemeSnapshot(
        values={**repository.snapshot.values, "keyword_mapping": []},
        fingerprint=repository.snapshot.fingerprint,
        publication_revision=4,
    )
    provider.pages[1] = {"results": []}

    defaulted = _execute(_service(provider, repository))
    repository.snapshot = DailyThemeSnapshot(
        values={**repository.snapshot.values, "keyword_mapping": ["invalid"]},
        fingerprint=repository.snapshot.fingerprint,
        publication_revision=4,
    )
    invalid = _execute(
        _service(provider, repository),
        publication_id="00000000-0000-0000-0000-000000000296",
    )

    assert defaulted.status is TaskOutcomeStatus.SUCCEEDED
    assert provider.discover_calls[0]["with_keywords"] == "99"
    assert invalid.status is TaskOutcomeStatus.FAILED
    assert invalid.details["reason_code"] == "invalid_mapping"
    assert invalid.details["retryable"] is False


def test_same_job_replay_recovers_committed_publication_without_provider_or_database_writes() -> None:
    provider = FakeProvider()
    repository = FakeRepository()
    repository.receipts["00000000-0000-0000-0000-000000000294"] = {
        "revision": 5,
        "publication_id": "00000000-0000-0000-0000-000000000294",
        "theme_index": 0,
        "theme_label": "科幻",
        "pool_page": 2,
        "pool_size": 10,
    }
    provider.configured = False

    outcome = _execute(_service(provider, repository))

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["publication"] == "already_committed"
    assert outcome.details["publication_revision"] == 5
    assert outcome.details["replayed"] is True
    assert provider.discover_calls == []
    assert provider.detail_calls == []
    assert repository.publish_calls == []


def test_replenishment_keeps_the_current_theme_and_atomically_appends_the_next_page() -> None:
    provider = FakeProvider()
    repository = FakeRepository()
    repository.snapshot = DailyThemeSnapshot(
        values={
            **repository.snapshot.values,
            "recommendation_pool": [{"id": value} for value in range(1, 5)],
            "recommendation_theme_index": 0,
            "recommendation_pool_page": 2,
        },
        fingerprint="replenish-fingerprint",
        publication_revision=4,
    )
    provider.pages[3] = {"results": [_movie(4), _movie(5)]}
    provider.details[5] = _details(5)

    outcome = _service(provider, repository).execute(
        FakeProgress(),
        publication_id="00000000-0000-0000-0000-000000000297",
        mode="replenish",
    )

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["mode"] == "replenish"
    assert outcome.details["theme_index"] == 0
    assert outcome.details["pool_page"] == 3
    assert outcome.details["pool_size"] == 5
    assert outcome.details["added_count"] == 1
    assert provider.discover_calls[0]["page"] == 3
    assert [item["id"] for item in repository.publish_calls[0]["pool"]] == [1, 2, 3, 4, 5]


def test_replenishment_valid_empty_page_resets_page_but_sufficient_pool_is_skipped() -> None:
    provider = FakeProvider()
    repository = FakeRepository()
    repository.snapshot = DailyThemeSnapshot(
        values={
            **repository.snapshot.values,
            "recommendation_pool": [{"id": 1}],
            "recommendation_theme_index": 0,
            "recommendation_pool_page": 5,
        },
        fingerprint="replenish-empty-fingerprint",
        publication_revision=4,
    )
    provider.pages[6] = {"results": []}

    empty = _service(provider, repository).execute(
        FakeProgress(),
        publication_id="00000000-0000-0000-0000-000000000298",
        mode="replenish",
    )
    repository.snapshot = DailyThemeSnapshot(
        values={
            **repository.snapshot.values,
            "recommendation_pool": [{"id": value} for value in range(1, 6)],
        },
        fingerprint="replenish-full-fingerprint",
        publication_revision=4,
    )
    sufficient = _service(provider, repository).execute(
        FakeProgress(),
        publication_id="00000000-0000-0000-0000-000000000299",
        mode="replenish",
    )

    assert empty.status is TaskOutcomeStatus.SUCCEEDED
    assert empty.details["pool_page"] == 0
    assert empty.details["added_count"] == 0
    assert repository.publish_calls[0]["pool_page"] == 0
    assert sufficient.status is TaskOutcomeStatus.SKIPPED
    assert sufficient.details["reason_code"] == "pool_sufficient"
    assert len(provider.discover_calls) == 1


def test_replenishment_effect_executes_with_its_durable_effect_identity() -> None:
    provider = FakeProvider()
    repository = FakeRepository()
    repository.snapshot = DailyThemeSnapshot(
        values={
            **repository.snapshot.values,
            "recommendation_pool": [{"id": 1}],
            "recommendation_theme_index": 0,
            "recommendation_pool_page": 2,
        },
        fingerprint="effect-replenish-fingerprint",
        publication_revision=4,
    )
    provider.pages[3] = {"results": []}
    effect_id = UUID("00000000-0000-0000-0000-000000000300")
    effect = cast(
        MediaEffectProjection,
        SimpleNamespace(
            id=effect_id,
            effect_type="daily-theme.replenishment",
            payload={"publication_revision": 4},
        ),
    )

    result = DailyThemeReplenishmentEffectDelivery(_service(provider, repository)).deliver(effect)

    assert result["status"] == "succeeded"
    assert result["trigger_publication_revision"] == 4
    assert repository.publish_calls[0]["metadata"]["publication_id"] == str(effect_id)
    assert repository.publish_calls[0]["metadata"]["mode"] == "replenish"


def test_replenishment_effect_maps_retryable_provider_failure_to_effect_retry() -> None:
    provider = FakeProvider()
    repository = FakeRepository()
    repository.snapshot = DailyThemeSnapshot(
        values={
            **repository.snapshot.values,
            "recommendation_pool": [{"id": 1}],
            "recommendation_theme_index": 0,
            "recommendation_pool_page": 2,
        },
        fingerprint="effect-retry-fingerprint",
        publication_revision=4,
    )
    provider.pages[3] = TimeoutError("fixture timeout")
    effect = cast(
        MediaEffectProjection,
        SimpleNamespace(
            id=UUID("00000000-0000-0000-0000-000000000303"),
            effect_type="daily-theme.replenishment",
            payload={"publication_revision": 4},
        ),
    )

    with pytest.raises(MediaEffectDeliveryRetryable):
        DailyThemeReplenishmentEffectDelivery(_service(provider, repository)).deliver(effect)

    assert repository.publish_calls == []
