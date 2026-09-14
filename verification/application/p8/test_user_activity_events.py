from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from etk.domains.identity.user_activity import (
    P8UserEventGateway,
    PolicyPropagationGuard,
    UserActivityMutation,
    UserActivityRepository,
    UserEventApplication,
    UserEventEffectIntent,
    UserMediaTargetResolver,
)
from etk.domains.media.models import ServiceResult

NOW = datetime(2026, 8, 27, 9, 0, tzinfo=UTC)


class FakeRepository:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def apply(
        self,
        *,
        event_key: str,
        event_name: str,
        payload_digest: str,
        aggregate_id: str,
        result: ServiceResult,
        mutation: UserActivityMutation | None,
        intents: list[UserEventEffectIntent],
    ) -> UserEventApplication:
        self.calls.append(
            {
                "event_key": event_key,
                "event_name": event_name,
                "payload_digest": payload_digest,
                "aggregate_id": aggregate_id,
                "result": result,
                "mutation": mutation,
                "intents": list(intents),
            }
        )
        return UserEventApplication(result, True)


class FakeResolver:
    def __init__(self) -> None:
        self.values = {"episode-1": "series-1"}
        self.calls: list[tuple[str, str]] = []
        self.error: BaseException | None = None

    def series_id_for_episode(self, user_id: str, episode_id: str) -> str | None:
        self.calls.append((user_id, episode_id))
        if self.error is not None:
            raise self.error
        return self.values.get(episode_id)


class FakePolicyGuard:
    def __init__(self) -> None:
        self.suppressed = False
        self.calls: list[tuple[str, str]] = []

    def consume(self, user_id: str, event_key: str) -> bool:
        self.calls.append((user_id, event_key))
        return self.suppressed


def _gateway() -> tuple[P8UserEventGateway, FakeRepository, FakeResolver, FakePolicyGuard]:
    repository = FakeRepository()
    resolver = FakeResolver()
    guard = FakePolicyGuard()
    return (
        P8UserEventGateway(
            repository=cast(UserActivityRepository, repository),
            resolver=cast(UserMediaTargetResolver, resolver),
            policy_guard=cast(PolicyPropagationGuard, guard),
            clock=lambda: NOW,
        ),
        repository,
        resolver,
        guard,
    )


def _event(
    name: str,
    *,
    item_type: str = "Series",
    item_id: str = "item-1",
    user_data: dict[str, Any] | None = None,
    playback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "_etk_source": "ETKMediaInfoBridge",
        "Event": name,
        "EventId": f"fixture-{name}-{item_type}-{item_id}-{user_data}-{playback}",
        "User": {"Id": "user-1", "Name": "Fixture User"},
        "Item": {"Id": item_id, "Type": item_type, "RunTimeTicks": 1000},
        "UserData": user_data or {},
        "PlaybackInfo": playback or {},
        "Session": {"DeviceId": "device-1", "DeviceName": "Fixture Device"},
        "PlaySessionId": "session-1",
        "UnrelatedSecret": "must-not-be-persisted",
    }


def _handle(gateway: P8UserEventGateway, payload: dict[str, Any]) -> ServiceResult:
    return gateway.handle(payload, host_url="https://fixture.invalid", remote_addr="127.0.0.1")


def test_favorite_event_name_only_forces_exact_favorite() -> None:
    gateway, repository, _resolver, _guard = _gateway()
    marked = _handle(gateway, _event("item.markfavorite"))
    marked_mutation = repository.calls[-1]["mutation"]
    unmarked = _handle(gateway, _event("item.unmarkfavorite"))
    unmarked_mutation = repository.calls[-1]["mutation"]

    assert marked.payload == {"status": "exact_favorite_updated"}
    assert marked_mutation.exact_favorite is True
    assert marked_mutation.is_favorite is None
    assert [intent.kind for intent in repository.calls[0]["intents"]] == ["favorite-intro-detection"]
    assert unmarked.payload == {"status": "exact_favorite_updated"}
    assert unmarked_mutation.exact_favorite is False
    assert unmarked_mutation.is_favorite is None
    assert repository.calls[1]["intents"] == []


def test_played_and_rate_events_are_partial_patches_not_name_forced() -> None:
    gateway, repository, _resolver, _guard = _gateway()
    ignored = _handle(gateway, _event("item.markplayed"))
    played = _handle(gateway, _event("item.markplayed", user_data={"Played": True}))
    played_mutation = repository.calls[-1]["mutation"]
    unplayed = _handle(gateway, _event("item.markunplayed", user_data={"Played": False}))
    unplayed_mutation = repository.calls[-1]["mutation"]
    rated = _handle(gateway, _event("item.rate", user_data={"IsFavorite": True}))
    rated_mutation = repository.calls[-1]["mutation"]

    assert ignored.payload == {"status": "event_ignored_no_updatable_data"}
    assert played.payload == {"status": "user_data_updated"}
    assert played_mutation.played is True
    assert played_mutation.playback_position_ticks == 0
    assert played_mutation.last_played_date == NOW
    assert unplayed.payload == {"status": "user_data_updated"}
    assert unplayed_mutation.played is False
    assert unplayed_mutation.playback_position_ticks is None
    assert unplayed_mutation.last_played_date is None
    assert rated.payload == {"status": "user_data_updated"}
    assert rated_mutation.exact_favorite is True
    assert rated_mutation.is_favorite is True
    assert not hasattr(rated_mutation, "rating")


def test_item_type_matrix_preserves_exact_and_series_aggregate_identity() -> None:
    gateway, repository, resolver, _guard = _gateway()
    _handle(gateway, _event("item.rate", item_type="Movie", user_data={"Played": True}))
    movie = repository.calls[-1]["mutation"]
    _handle(gateway, _event("item.markfavorite", item_type="Season"))
    season = repository.calls[-1]["mutation"]
    _handle(
        gateway,
        _event(
            "item.rate",
            item_type="Episode",
            item_id="episode-1",
            user_data={"Played": True, "IsFavorite": True},
        ),
    )
    episode = repository.calls[-1]["mutation"]
    unsupported = _handle(gateway, _event("item.rate", item_type="Video"))

    assert (movie.aggregate_item_id, movie.exact_favorite) == ("item-1", None)
    assert (season.aggregate_item_id, season.exact_favorite) == (None, True)
    assert (episode.aggregate_item_id, episode.source_item_id) == ("series-1", "episode-1")
    assert resolver.calls == [("user-1", "episode-1")]
    assert unsupported.payload == {"status": "event_ignored_no_updatable_data"}


def test_playback_stop_and_halfway_freeze_threshold_state_and_intents() -> None:
    gateway, repository, _resolver, _guard = _gateway()
    stopped = _handle(
        gateway,
        _event(
            "playback.stop",
            playback={"PositionTicks": 900, "PlayedToCompletion": True},
        ),
    )
    stop_call = repository.calls[-1]
    before_halfway = _handle(
        gateway,
        _event(
            "playback.halfway",
            item_type="Episode",
            item_id="episode-1",
            playback={"PositionTicks": 499, "RunTimeTicks": 1000},
        ),
    )
    at_halfway = _handle(
        gateway,
        _event(
            "playback.halfway",
            item_type="Episode",
            item_id="episode-1",
            playback={"PlaybackPositionTicks": 500, "RunTimeTicks": 1000},
        ),
    )
    halfway_call = repository.calls[-1]
    non_episode = _handle(gateway, _event("playback.halfway", item_type="Movie"))

    assert stopped.payload == {"status": "user_data_updated"}
    assert stop_call["mutation"].played is True
    assert stop_call["mutation"].playback_position_ticks == 0
    assert [intent.kind for intent in stop_call["intents"]] == [
        "playback-cleanup",
        "proxy-concurrency-release",
        "telegram-playback-notification",
    ]
    snapshot = stop_call["intents"][0].payload["event_snapshot"]
    assert snapshot["UserId"] == "user-1"
    assert snapshot["ItemId"] == "item-1"
    assert snapshot["PlaySessionId"] == "session-1"
    assert snapshot["DeviceId"] == "device-1"
    assert snapshot["remote_addr"] == "127.0.0.1"
    assert "UnrelatedSecret" not in snapshot
    assert before_halfway.payload == {"status": "event_ignored_before_halfway"}
    assert at_halfway.status_code == 200
    assert at_halfway.payload == {"status": "user_data_updated"}
    assert halfway_call["mutation"].playback_position_ticks is None
    assert [intent.kind for intent in halfway_call["intents"]] == ["halfway-intro-detection"]
    assert non_episode.payload == {"status": "event_ignored_non_episode_halfway"}


def test_policy_event_stages_refresh_and_conditionally_template_sync() -> None:
    gateway, repository, _resolver, guard = _gateway()
    submitted = _handle(gateway, _event("user.policyupdated"))
    submitted_intents = [intent.kind for intent in repository.calls[-1]["intents"]]
    guard.suppressed = True
    suppressed = _handle(
        gateway,
        {
            **_event("user.policyupdated"),
            "EventId": "fixture-policy-suppressed",
        },
    )
    suppressed_intents = [intent.kind for intent in repository.calls[-1]["intents"]]
    missing = _handle(
        gateway,
        {
            "Event": "user.policyupdated",
            "EventId": "fixture-policy-missing-user",
            "User": {},
        },
    )

    assert submitted.status_code == 202
    assert submitted.payload == {"status": "auto_sync_task_submitted"}
    assert submitted_intents == ["refresh-user-policy", "sync-template-policy"]
    assert suppressed.status_code == 200
    assert suppressed.payload == {"status": "event_ignored_system_triggered"}
    assert suppressed_intents == ["refresh-user-policy"]
    assert missing.payload == {"status": "event_ignored_no_user_id"}


def test_episode_resolver_transport_failure_is_not_silently_treated_as_not_found() -> None:
    gateway, repository, resolver, _guard = _gateway()
    resolver.error = RuntimeError("fixture Emby timeout")

    result = _handle(
        gateway,
        _event(
            "item.rate",
            item_type="Episode",
            item_id="episode-1",
            user_data={"Played": True},
        ),
    )

    assert result.status_code == 500
    assert result.payload == {"status": "error_updating_user_data"}
    assert repository.calls == []
