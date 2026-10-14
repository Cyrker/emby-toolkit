from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest

from etk.domains.subscription.watchlist_task_execution import (
    NativeWatchlistMoviePilotGateway,
    NativeWatchlistP115ReclassificationGateway,
    NativeWatchlistSharedMetadataGateway,
    NativeWatchlistTaskEffects,
    NativeWatchlistTaskService,
)
from etk.platform.jobs import PersistentExecutionCancelled
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcome, TaskOutcomeStatus
from etk.processes.p7_subscription_effects import PersistentMoviePilotEffectGateway
from etk.processes.p7_subscription_tasks import P7SubscriptionTaskRouter


class _Progress:
    def __init__(self) -> None:
        self.checkpoints = 0
        self.updates: list[tuple[int, str]] = []

    def checkpoint(self) -> None:
        self.checkpoints += 1

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def success_reported(self) -> bool:
        return any(progress == 100 for progress, _message in self.updates)


class _Repository:
    def __init__(self, candidates: Sequence[Mapping[str, Any]]) -> None:
        self.candidates = tuple(dict(value) for value in candidates)
        self.imported: tuple[Mapping[str, Any], ...] = ()
        self.refreshes: list[tuple[str, tuple[Mapping[str, Any], ...]]] = []

    def list_active_series(self, tmdb_id: str | None = None) -> Sequence[Mapping[str, Any]]:
        return tuple(value for value in self.candidates if tmdb_id is None or value.get("tmdb_id") == tmdb_id)

    def list_completed_series(self, library_ids: Sequence[str] = ()) -> Sequence[Mapping[str, Any]]:
        del library_ids
        return self.candidates

    def list_all_completed_series(self) -> Sequence[Mapping[str, Any]]:
        return self.candidates

    def import_library_series(self, items: Sequence[Mapping[str, Any]]) -> int:
        self.imported = tuple(items)
        return len(self.imported)

    def refresh_series(
        self,
        tmdb_id: str,
        details: Mapping[str, Any],
        *,
        season_details: Sequence[Mapping[str, Any]] = (),
    ) -> bool:
        del details
        self.refreshes.append((tmdb_id, tuple(dict(value) for value in season_details)))
        return True


class _Metadata:
    def __init__(self, values: Mapping[str, Mapping[str, Any] | BaseException]) -> None:
        self.values = dict(values)

    def series_details(self, tmdb_id: str) -> Mapping[str, Any]:
        value = self.values[tmdb_id]
        if isinstance(value, BaseException):
            raise value
        return dict(value)

    def season_details(self, tmdb_id: str, season_number: int) -> Mapping[str, Any]:
        return {
            "season_number": season_number,
            "episodes": [{"id": f"{tmdb_id}-s{season_number}-e1", "episode_number": 1}],
        }


class _Libraries:
    def __init__(self, values: Sequence[Mapping[str, Any]] = ()) -> None:
        self.values = tuple(values)
        self.calls: list[tuple[str, ...]] = []

    def list_series(self, library_ids: Sequence[str]) -> Sequence[Mapping[str, Any]]:
        self.calls.append(tuple(library_ids))
        return self.values


class _Effects:
    def __init__(self, failure: BaseException | None = None) -> None:
        self.failure = failure
        self.calls: list[dict[str, Any]] = []

    def publish_refresh(
        self,
        task_key: str,
        tmdb_id: str,
        candidate: Mapping[str, Any],
        details: Mapping[str, Any],
        season_details: Sequence[Mapping[str, Any]],
        parameters: Mapping[str, Any],
        checkpoint: Callable[[], None],
    ) -> Mapping[str, Any]:
        checkpoint()
        if self.failure is not None:
            raise self.failure
        self.calls.append(
            {
                "task_key": task_key,
                "tmdb_id": tmdb_id,
                "candidate": dict(candidate),
                "details": dict(details),
                "seasons": tuple(season_details),
                "parameters": dict(parameters),
            }
        )
        return {"staged": 1}


def _series_details(*, status: str = "Returning Series", air_date: str = "2026-01-01") -> dict[str, Any]:
    return {
        "name": "Fixture",
        "status": status,
        "first_air_date": "2025-01-01",
        "last_air_date": air_date,
        "last_episode_to_air": {"season_number": 1, "episode_number": 1, "air_date": air_date},
        "seasons": [{"id": 101, "season_number": 1, "episode_count": 10, "air_date": "2025-01-01"}],
    }


def test_watchlist_process_continues_after_one_provider_failure() -> None:
    repository = _Repository([{"tmdb_id": "1"}, {"tmdb_id": "2"}])
    effects = _Effects()
    service = NativeWatchlistTaskService(
        repository,
        _Metadata({"1": RuntimeError("provider failed"), "2": _series_details()}),
        _Libraries(),
        effects=effects,
    )

    outcome = service.execute("process-watchlist", _Progress(), {})

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details["processed"] == 2
    assert outcome.details["updated"] == 1
    assert outcome.details["failed"] == 1
    assert repository.refreshes[0][0] == "2"
    assert effects.calls[0]["task_key"] == "process-watchlist"


def test_watchlist_empty_scan_is_an_explicit_skip() -> None:
    service = NativeWatchlistTaskService(_Repository(()), _Metadata({}), _Libraries())

    outcome = service.execute("process-watchlist", _Progress(), {})

    assert outcome.status is TaskOutcomeStatus.SKIPPED
    assert outcome.details == {"processed": 0, "failed": 0, "imported": 0}


def test_add_all_imports_the_selected_library_before_calibration() -> None:
    repository = _Repository([{"tmdb_id": "1", "watchlist_tmdb_status": None}])
    libraries = _Libraries([{"Type": "Series", "Id": "emby-1", "ProviderIds": {"Tmdb": "1"}}])
    service = NativeWatchlistTaskService(
        repository,
        _Metadata({"1": _series_details()}),
        libraries,
        library_ids=lambda: ("library-a",),
    )

    outcome = service.execute("add-all-series-to-watchlist", _Progress(), {})

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["imported"] == 1
    assert libraries.calls == [("library-a",)]


def test_completed_refresh_skips_an_unchanged_authoritative_snapshot() -> None:
    details = _series_details(status="Ended")
    repository = _Repository(
        [
            {
                "tmdb_id": "1",
                "watchlist_tmdb_status": "Ended",
                "last_air_date": details["last_air_date"],
                "last_episode_to_air_json": details["last_episode_to_air"],
            }
        ]
    )
    service = NativeWatchlistTaskService(repository, _Metadata({"1": details}), _Libraries())

    outcome = service.execute("refresh_completed_series", _Progress(), {})

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["skipped"] == 1
    assert repository.refreshes == []


def test_inner_persistent_cancellation_is_not_downgraded_to_an_item_failure() -> None:
    service = NativeWatchlistTaskService(
        _Repository([{"tmdb_id": "1"}]),
        _Metadata({"1": _series_details()}),
        _Libraries(),
        effects=_Effects(PersistentExecutionCancelled("fixture cancellation")),
    )

    with pytest.raises(PersistentExecutionCancelled, match="fixture cancellation"):
        service.execute("process-watchlist", _Progress(), {})


class _Settings:
    def __init__(self, values: Mapping[str, Any]) -> None:
        self.values = dict(values)

    def load(self, key: str) -> Any:
        return self.values.get(key)


class _MoviePilotProvider:
    def __init__(self, subscriptions: Sequence[Mapping[str, Any]] = ()) -> None:
        self.subscriptions = [dict(value) for value in subscriptions]

    def list_subscriptions(self) -> Any:
        return SimpleNamespace(subscriptions=tuple(self.subscriptions))

    def query_subscription(self, tmdb_id: int | str, *, season: int | None = None) -> Any:
        found = next(
            (
                value
                for value in self.subscriptions
                if str(value.get("tmdbid")) == str(tmdb_id) and value.get("season") == season
            ),
            None,
        )
        return SimpleNamespace(found=found is not None, subscription=found)


class _MoviePilotEffects:
    def __init__(self, provider: _MoviePilotProvider) -> None:
        self.provider = provider
        self.created: list[dict[str, Any]] = []
        self.statuses: list[tuple[str, int | None, str, int | None]] = []
        self.updated: list[dict[str, Any]] = []
        self.deleted: list[int] = []

    def create_subscription(
        self,
        payload: Mapping[str, Any],
        *,
        consume_quota: bool,
        apply_local_continuation: bool = True,
    ) -> bool:
        assert consume_quota is True
        assert apply_local_continuation is False
        created = {"id": 11, **dict(payload), "state": "N"}
        self.created.append(created)
        self.provider.subscriptions.append(created)
        return True

    def update_subscription_status(
        self,
        tmdb_id: int | str,
        *,
        season: int | None,
        status: str,
        total_episodes: int | None,
    ) -> bool:
        self.statuses.append((str(tmdb_id), season, status, total_episodes))
        return True

    def update_subscription(self, payload: Mapping[str, Any]) -> bool:
        self.updated.append(dict(payload))
        return True

    def delete_subscription_by_id(self, subscription_id: int | str) -> bool:
        self.deleted.append(int(subscription_id))
        return True


class _TestMoviePilotGateway(NativeWatchlistMoviePilotGateway):
    def __init__(self, settings: _Settings, provider: _MoviePilotProvider, effects: _MoviePilotEffects) -> None:
        super().__init__(cast(Any, None), settings, provider, effects, clock=lambda: datetime(2026, 9, 8, tzinfo=UTC))
        self.context: dict[str, Any] = {
            "series": {"title": "Fixture", "watching_status": "Watching"},
            "seasons": {
                1: {
                    "season_number": 1,
                    "total_episodes": 10,
                    "local_count": 3,
                    "local_max_episode": 3,
                    "local_episode_numbers": [1, 2, 3],
                    "virtual": False,
                }
            },
        }
        self.full_washing: list[tuple[str, int, int]] = []
        self.snapshots = 0

    def _local_context(self, tmdb_id: str) -> dict[str, Any]:
        del tmdb_id
        return self.context

    def _recent_manual_state_change(self, subscribe_id: int) -> bool:
        del subscribe_id
        return False

    def _remember_expected_update(self, subscribe_id: int, state: str, total: int | None) -> None:
        del subscribe_id, state, total

    def _mark_full_washing(
        self,
        tmdb_id: str,
        season: int,
        subscribe_id: int,
        subscription: Mapping[str, Any],
    ) -> None:
        del subscription
        self.full_washing.append((tmdb_id, season, subscribe_id))

    def _upsert_snapshot(self, *args: Any, **kwargs: Any) -> bool:
        del args, kwargs
        self.snapshots += 1
        return True

    def _season_consistency(self, tmdb_id: str, season: int, expected: int) -> bool:
        del tmdb_id, season, expected
        return False


class _ConsistentMoviePilotGateway(_TestMoviePilotGateway):
    def __init__(self, settings: _Settings, provider: _MoviePilotProvider, effects: _MoviePilotEffects) -> None:
        super().__init__(settings, provider, effects)
        self.delete_state: list[tuple[str, str, int]] = []

    def _season_consistency(self, tmdb_id: str, season: int, expected: int) -> bool:
        del tmdb_id, season, expected
        return True

    def _mark_episode_washing_delete_pending(
        self,
        tmdb_id: str,
        season: int,
        subscribe_id: int,
        configuration: Mapping[str, Any],
    ) -> None:
        del configuration
        self.delete_state.append(("pending", tmdb_id, subscribe_id))

    def _clear_episode_washing_delete_pending(self, tmdb_id: str, season: int, subscribe_id: int) -> None:
        del season
        self.delete_state.append(("cleared", tmdb_id, subscribe_id))

    def _finalize_episode_washing_delete(
        self,
        tmdb_id: str,
        season: int,
        subscribe_id: int,
        subscription: Mapping[str, Any],
    ) -> None:
        del season, subscription
        self.delete_state.append(("finalized", tmdb_id, subscribe_id))


def _moviepilot_settings(**assistant: Any) -> _Settings:
    return _Settings(
        {
            "watchlist_config": {"auto_pending": {"enabled": False}},
            "mp_config": {
                "moviepilot_url": "https://moviepilot.invalid",
                "subscribe_assistant": {"enabled": True, "pending_enhanced_enabled": False, **assistant},
            },
            "subscription_strategy_config": {"subscription_sources": ["mp"]},
        }
    )


def test_moviepilot_sync_creates_latest_season_from_the_next_local_episode() -> None:
    provider = _MoviePilotProvider()
    effects = _MoviePilotEffects(provider)
    gateway = _TestMoviePilotGateway(_moviepilot_settings(best_version_type="no"), provider, effects)

    first = gateway.sync_series("100", {}, _series_details(), (), {}, lambda: None)
    second = gateway.sync_series("100", {}, _series_details(), (), {}, lambda: None)

    assert first["provider_disposition"] == "empty"
    assert first["created"] == 1
    assert effects.created[0]["start_episode"] == 4
    assert second["created"] == 0
    assert len(effects.created) == 1


def test_moviepilot_sync_updates_pending_state_and_fake_total() -> None:
    provider = _MoviePilotProvider([{"id": 11, "tmdbid": 100, "season": 1, "state": "R"}])
    effects = _MoviePilotEffects(provider)
    settings = _moviepilot_settings(
        pending_enhanced_enabled=True,
        auto_tv_pending_days=30,
        auto_tv_pending_episodes=5,
        pending_fake_total_episodes=88,
    )
    gateway = _TestMoviePilotGateway(settings, provider, effects)
    details = _series_details()
    details["seasons"][0]["air_date"] = "2026-09-01"

    result = gateway.sync_series("100", {}, details, (), {}, lambda: None)

    assert result["updated"] == 1
    assert effects.statuses == [("100", 1, "P", 88)]


def test_moviepilot_completed_flow_stages_snapshot_and_full_washing() -> None:
    provider = _MoviePilotProvider([{"id": 11, "tmdbid": 100, "season": 1, "state": "R"}])
    effects = _MoviePilotEffects(provider)
    gateway = _TestMoviePilotGateway(_moviepilot_settings(best_version_type="completed_full"), provider, effects)
    gateway.context["series"]["watching_status"] = "Completed"
    gateway.context["seasons"][1]["local_count"] = 10
    details = _series_details(status="Ended")

    result = gateway.sync_series("100", {}, details, (), {}, lambda: None)

    assert result["snapshots"] == 1
    assert result["full_washing"] == 1
    assert effects.updated[0]["best_version_full"] == 1
    assert gateway.full_washing == [("100", 1, 11)]


def test_consistent_episode_washing_suppresses_restore_before_delete_and_finalizes() -> None:
    provider = _MoviePilotProvider([{"id": 11, "tmdbid": 100, "season": 1, "state": "R"}])
    effects = _MoviePilotEffects(provider)
    gateway = _ConsistentMoviePilotGateway(
        _moviepilot_settings(best_version_type="tv_episode", best_version_episode_to_full=False),
        provider,
        effects,
    )
    gateway.context["series"]["watching_status"] = "Completed"
    gateway.context["seasons"][1]["local_count"] = 10
    details = _series_details(status="Ended")

    result = gateway.sync_series("100", {}, details, (), {}, lambda: None)

    assert result["full_washing"] == 0
    assert effects.deleted == [11]
    assert gateway.delete_state == [("pending", "100", 11), ("finalized", "100", 11)]


class _P115Plan:
    def __init__(self) -> None:
        self.calls: list[Mapping[str, Any]] = []

    def create(self, raw_payload: object) -> Any:
        assert isinstance(raw_payload, Mapping)
        self.calls.append(dict(raw_payload))
        return SimpleNamespace(status_code=201, payload={"success": True, "data": {"status": "ready", "plan_id": "p1"}})


class _NoopP115Plan(_P115Plan):
    def create(self, raw_payload: object) -> Any:
        assert isinstance(raw_payload, Mapping)
        self.calls.append(dict(raw_payload))
        return SimpleNamespace(
            status_code=200,
            payload={
                "success": True,
                "data": {
                    "status": "blocked",
                    "plan_id": "p1",
                    "errors": [{"code": "NO_ELIGIBLE_MEDIA"}],
                },
            },
        )


class _P115Executions:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def prepare(self, plan_id: str, approved_intent_id: str, **kwargs: Any) -> Any:
        del kwargs
        self.calls.append((plan_id, approved_intent_id))
        execution = SimpleNamespace(execution_id="e1", plan_id=plan_id, job_id="j1")
        return SimpleNamespace(execution=execution, created=True)


class _P115ExecutionRepository:
    def find_by_intent(self, approved_intent_id: str) -> Any:
        del approved_intent_id
        return None


class _TestP115Gateway(NativeWatchlistP115ReclassificationGateway):
    def __init__(self, plans: _P115Plan, executions: _P115Executions) -> None:
        super().__init__(cast(Any, None), plans, executions, _P115ExecutionRepository())

    def _series_status(self, tmdb_id: str) -> str:
        del tmdb_id
        return "Completed"

    def _records(self, tmdb_id: str) -> tuple[Mapping[str, Any], ...]:
        del tmdb_id
        return (
            {"file_id": "direct", "parent_id": "10", "category_name": "MP直出", "season_number": 1},
            {"file_id": "direct-category", "parent_id": "10", "category_name": "MP直出/电视剧"},
            {"file_id": "direct-unmatched", "parent_id": "10", "category_name": " MP直出/未匹配分类 "},
            {
                "file_id": "move",
                "parent_id": "10",
                "category_name": "连载",
                "season_number": 1,
                "original_name": "Fixture.S01E03.mkv",
            },
        )


def test_p115_reclassification_excludes_moviepilot_direct_records() -> None:
    plans = _P115Plan()
    executions = _P115Executions()
    gateway = _TestP115Gateway(plans, executions)

    result = gateway.stage_reclassification(
        "100",
        {"watching_status": "Watching"},
        {"name": "Fixture"},
        lambda: None,
    )

    assert result["mp_direct_skipped"] == 3
    assert plans.calls[0]["source_file_ids"] == ["move"]
    assert plans.calls[0]["recognition_hints"]["move"]["episode_number"] == 3
    assert executions.calls[0][0] == "p1"


def test_p115_reclassification_treats_already_classified_media_as_a_noop() -> None:
    plans = _NoopP115Plan()
    executions = _P115Executions()
    gateway = _TestP115Gateway(plans, executions)

    result = gateway.stage_reclassification(
        "100",
        {"watching_status": "Watching"},
        {"name": "Fixture"},
        lambda: None,
    )

    assert result["unchanged_groups"] == 1
    assert result["prepared"] == []
    assert executions.calls == []


class _SharingOperations:
    def __init__(self) -> None:
        self.intents: list[Any] = []

    def reserve_with_replay(self, intent: Any) -> tuple[Any, bool]:
        replayed = any(existing.operation_key == intent.operation_key for existing in self.intents)
        self.intents.append(intent)
        return SimpleNamespace(id=intent.id, status=SimpleNamespace(value="ready")), replayed


class _TestSharedGateway(NativeWatchlistSharedMetadataGateway):
    def _metadata_context(self, tmdb_id: str) -> Mapping[str, Any]:
        del tmdb_id
        return {
            "series": {"title": "Fixture", "watching_status": "Watching"},
            "seasons": [
                {
                    "season_number": 1,
                    "watching_status": "Watching",
                    "total_episodes": 10,
                    "total_episodes_locked": True,
                }
            ],
        }


def test_shared_center_metadata_uses_one_stable_durable_operation() -> None:
    operations = _SharingOperations()
    gateway = _TestSharedGateway(
        cast(Any, None),
        _Settings(
            {
                "shared_resource_config": {"p115_shared_resource_enabled": True},
                "watchlist_config": {},
            }
        ),
        operations,
        lambda: "server-hash",
    )
    seasons = ({"season_number": 1, "episodes": [{"season_number": 1, "episode_number": 1, "runtime": 45}]},)

    first = gateway.stage("100", _series_details(), seasons, skip_logical_share_dispatch=True)
    second = gateway.stage("100", _series_details(), seasons, skip_logical_share_dispatch=True)

    assert first["replayed"] is False
    assert second["replayed"] is True
    assert operations.intents[0].operation_key == operations.intents[1].operation_key
    payload = operations.intents[0].request["payload"]
    assert payload["skip_logical_share_dispatch"] is True
    assert payload["items"][0]["display_meta_json"]["item_type"] == "Episode"
    assert {item["item_type"] for item in payload["items"][0]["display_meta_items_json"]} == {
        "Series",
        "Season",
        "Episode",
    }


class _Intro:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], bool]] = []

    def enqueue_imported_episode_ids(
        self,
        episode_ids: Sequence[Any],
        *,
        allow_playback_followup: bool = False,
    ) -> Mapping[str, Any]:
        self.calls.append((tuple(str(value) for value in episode_ids), allow_playback_followup))
        return {"queued": 1}


class _CompositeEffects(NativeWatchlistTaskEffects):
    def _stage_emby_refresh(self, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
        del args, kwargs
        return {"staged": 1}


def test_targeted_watchlist_refresh_reserves_intro_followup() -> None:
    intro = _Intro()
    effects = _CompositeEffects(cast(Any, None), cast(Any, None), intro=intro)

    result = effects.publish_refresh(
        "process-watchlist",
        "100",
        {},
        _series_details(),
        (),
        {"tmdb_id": "100", "new_episode_ids": ["e1", "e1", "e2"]},
        lambda: None,
    )

    assert result["intro"] == {"queued": 1}
    assert intro.calls == [(("e1", "e2"), True)]


class _BoundWatchlistOwner:
    def __init__(self, effects: PersistentMoviePilotEffectGateway) -> None:
        self._effects = effects
        self.identity: Mapping[str, str] | None = None

    def execute(self, task_key: str, progress: Any, parameters: Mapping[str, Any]) -> TaskOutcome:
        del task_key, progress, parameters
        self.identity = self._effects._identity()
        return TaskOutcome.succeeded("fixture")


def test_router_binds_stable_moviepilot_execution_identity_for_watchlist_owner() -> None:
    effects = PersistentMoviePilotEffectGateway(cast(Any, None), cast(Any, None), cast(Any, None))
    owner = _BoundWatchlistOwner(effects)
    router = P7SubscriptionTaskRouter(
        assistant=None,
        moviepilot_effects=effects,
        watchlist_tasks=owner,
    )
    job_id = uuid4()
    attempt_id = uuid4()

    outcome = router.execute(
        TaskMessage.create(
            task_key="process-watchlist",
            processor=ProcessorIdentity.WATCHLIST,
            metadata={
                "job_id": str(job_id),
                "attempt_id": str(attempt_id),
                "worker_id": "watchlist-worker",
            },
        ),
        processor=None,
        progress=_Progress(),
    )

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert owner.identity == {
        "job_id": str(job_id),
        "attempt_id": str(attempt_id),
        "worker_id": "watchlist-worker",
        "task_key": "process-watchlist",
    }


class _ForbiddenEffect:
    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"refresh_completed_series reached {name}")


def test_completed_refresh_only_stages_emby_and_skips_cross_system_effects() -> None:
    effects = _CompositeEffects(
        cast(Any, None),
        cast(Any, None),
        moviepilot=cast(Any, _ForbiddenEffect()),
        p115=cast(Any, _ForbiddenEffect()),
        sharing=cast(Any, _ForbiddenEffect()),
    )

    result = effects.publish_refresh(
        "refresh_completed_series",
        "100",
        {},
        _series_details(status="Ended"),
        (),
        {},
        lambda: None,
    )

    assert result == {"tmdb_id": "100", "emby": {"staged": 1}}
