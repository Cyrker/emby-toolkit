from __future__ import annotations

import math

import pytest

from etk.domains.subscription.models import (
    MoviePilotDecisionReason,
    MoviePilotState,
    MoviePilotStatePolicy,
    SeasonStatePolicy,
    SeasonTransitionReason,
    SeasonWatchState,
    SeriesStatePolicy,
    SeriesTransitionReason,
    SeriesWatchState,
    SubscriptionSource,
    SubscriptionSources,
    SubscriptionSourceType,
)


@pytest.mark.parametrize("tmdb_status", ["Ended", "Canceled", " ended ", "CANCELED"])
def test_tmdb_terminal_status_completes_series(tmdb_status: str) -> None:
    result = SeriesStatePolicy.transition(SeriesWatchState.WATCHING, tmdb_status=tmdb_status)

    assert result.current_state is SeriesWatchState.COMPLETED
    assert result.target_state is SeriesWatchState.COMPLETED
    assert result.reason is SeriesTransitionReason.TMDB_TERMINAL
    assert result.allowed is True
    assert result.changed is True
    assert result.newly_completed is True
    assert result.stable_terminal is False


@pytest.mark.parametrize(
    "previous",
    [SeriesWatchState.NONE, SeriesWatchState.WATCHING, SeriesWatchState.PAUSED, SeriesWatchState.PENDING],
)
def test_non_terminal_tmdb_status_uses_current_series_watching_policy(previous: SeriesWatchState) -> None:
    result = SeriesStatePolicy.transition(previous, tmdb_status="Returning Series")

    assert result.current_state is SeriesWatchState.WATCHING
    assert result.reason is SeriesTransitionReason.TMDB_ACTIVE
    assert result.allowed is True
    assert result.newly_completed is False


def test_force_ended_is_a_stable_terminal_and_newly_completed_is_edge_triggered() -> None:
    first = SeriesStatePolicy.transition(
        SeriesWatchState.WATCHING,
        tmdb_status="Returning Series",
        force_ended=True,
    )
    replay = SeriesStatePolicy.transition(
        first.current_state,
        tmdb_status="Returning Series",
        force_ended=True,
        revival_signal=True,
    )

    assert first.current_state is SeriesWatchState.COMPLETED
    assert first.reason is SeriesTransitionReason.FORCE_ENDED
    assert first.stable_terminal is True
    assert first.newly_completed is True
    assert replay.current_state is SeriesWatchState.COMPLETED
    assert replay.changed is False
    assert replay.newly_completed is False
    assert replay.revived is False
    assert replay.stable_terminal is True


def test_ordinary_completed_requires_an_explicit_revival_signal() -> None:
    blocked = SeriesStatePolicy.transition(SeriesWatchState.COMPLETED, tmdb_status="Returning Series")
    revived = SeriesStatePolicy.transition(
        SeriesWatchState.COMPLETED,
        tmdb_status="Returning Series",
        revival_signal=True,
    )

    assert blocked.target_state is SeriesWatchState.WATCHING
    assert blocked.current_state is SeriesWatchState.COMPLETED
    assert blocked.reason is SeriesTransitionReason.REVIVAL_REQUIRED
    assert blocked.allowed is False
    assert blocked.changed is False
    assert revived.current_state is SeriesWatchState.WATCHING
    assert revived.reason is SeriesTransitionReason.REVIVAL
    assert revived.allowed is True
    assert revived.changed is True
    assert revived.revived is True


def test_revival_signal_cannot_override_tmdb_terminal_status() -> None:
    result = SeriesStatePolicy.transition(
        SeriesWatchState.COMPLETED,
        tmdb_status="Ended",
        revival_signal=True,
    )

    assert result.current_state is SeriesWatchState.COMPLETED
    assert result.reason is SeriesTransitionReason.TMDB_TERMINAL
    assert result.revived is False


@pytest.mark.parametrize(
    ("local_count", "total_episodes", "expected"),
    [
        (12, 12, SeasonWatchState.COMPLETED),
        (13, 12, SeasonWatchState.COMPLETED),
        (11, 12, SeasonWatchState.WATCHING),
        (99, 0, SeasonWatchState.WATCHING),
    ],
)
def test_season_state_uses_only_known_total_and_local_count(
    local_count: int,
    total_episodes: int,
    expected: SeasonWatchState,
) -> None:
    result = SeasonStatePolicy.transition(
        SeasonWatchState.NONE,
        local_count=local_count,
        total_episodes=total_episodes,
    )

    assert result.current_state is expected
    assert result.newly_completed is (expected is SeasonWatchState.COMPLETED)


def test_season_force_end_is_stable_but_ordinary_completion_is_recomputed() -> None:
    ordinary = SeasonStatePolicy.transition(
        SeasonWatchState.COMPLETED,
        local_count=11,
        total_episodes=12,
    )
    forced = SeasonStatePolicy.transition(
        SeasonWatchState.WATCHING,
        local_count=0,
        total_episodes=0,
        force_ended=True,
    )

    assert ordinary.current_state is SeasonWatchState.WATCHING
    assert ordinary.reason is SeasonTransitionReason.LOCAL_INCOMPLETE
    assert ordinary.stable_terminal is False
    assert forced.current_state is SeasonWatchState.COMPLETED
    assert forced.reason is SeasonTransitionReason.FORCE_ENDED
    assert forced.stable_terminal is True


def test_series_and_season_completion_inputs_are_isolated() -> None:
    series = SeriesStatePolicy.transition(SeriesWatchState.WATCHING, tmdb_status="Returning Series")
    season = SeasonStatePolicy.transition(SeasonWatchState.WATCHING, local_count=12, total_episodes=12)

    assert series.current_state is SeriesWatchState.WATCHING
    assert season.current_state is SeasonWatchState.COMPLETED
    with pytest.raises(ValueError, match="unknown season watch state"):
        SeasonStatePolicy.transition("Paused", local_count=0, total_episodes=12)


@pytest.mark.parametrize(
    ("local_count", "total_episodes", "field"),
    [(-1, 12, "local_count"), (0, -1, "total_episodes")],
)
def test_season_counts_reject_malformed_values(local_count: int, total_episodes: int, field: str) -> None:
    with pytest.raises(ValueError, match=field):
        SeasonStatePolicy.transition(
            SeasonWatchState.WATCHING,
            local_count=local_count,
            total_episodes=total_episodes,
        )


@pytest.mark.parametrize(
    ("series_state", "expected"),
    [
        (SeriesWatchState.NONE, MoviePilotState.RUNNING),
        (SeriesWatchState.WATCHING, MoviePilotState.RUNNING),
        (SeriesWatchState.PAUSED, MoviePilotState.PAUSED),
        (SeriesWatchState.PENDING, MoviePilotState.PENDING),
        (SeriesWatchState.COMPLETED, MoviePilotState.RUNNING),
    ],
)
def test_moviepilot_state_mapping_preserves_legacy_defaults(
    series_state: SeriesWatchState,
    expected: MoviePilotState,
) -> None:
    assert MoviePilotStatePolicy.decide(series_state).state is expected


def test_moviepilot_completed_guard_and_new_subscription_are_explicit_inputs() -> None:
    guarded = MoviePilotStatePolicy.decide(
        SeriesWatchState.COMPLETED,
        completion_guard_pending=True,
    )
    created = MoviePilotStatePolicy.decide(SeriesWatchState.WATCHING, new_subscription=True)
    paused_created = MoviePilotStatePolicy.decide(SeriesWatchState.PAUSED, new_subscription=True)

    assert guarded.state is MoviePilotState.PENDING
    assert guarded.reason is MoviePilotDecisionReason.COMPLETED_GUARD
    assert created.state is MoviePilotState.NEW
    assert created.reason is MoviePilotDecisionReason.NEW_SUBSCRIPTION
    assert paused_created.state is MoviePilotState.PAUSED


def test_known_subscription_source_types_are_frozen_to_the_legacy_contract() -> None:
    assert {source_type.value for source_type in SubscriptionSourceType} == {
        "actor_subscription",
        "moviepilot",
        "revived_season",
        "scan_old_seasons_backfill",
        "resubscribe",
        "gap_scan",
    }


def test_source_canonicalization_deduplicates_exact_replays_and_preserves_metadata() -> None:
    raw = [
        {
            "type": "actor_subscription",
            "person_id": 42,
            "name": "演员",
            "id": 7,
            "audit": {"labels": ["主演", "电影"], "enabled": True},
        },
        {
            "audit": {"enabled": True, "labels": ["主演", "电影"]},
            "id": 7,
            "name": "演员",
            "person_id": 42,
            "type": "actor_subscription",
        },
        {"type": "moviepilot", "subscribe_id": 11, "reason": "history"},
    ]

    sources = SubscriptionSources.from_raw(raw)

    assert len(sources) == 2
    assert sources.to_json_value() == [
        {
            "type": "actor_subscription",
            "audit": {"enabled": True, "labels": ["主演", "电影"]},
            "id": 7,
            "name": "演员",
            "person_id": 42,
        },
        {"type": "moviepilot", "reason": "history", "subscribe_id": 11},
    ]
    assert SubscriptionSources.from_raw(sources.to_json_value()) == sources
    assert sources.merge(sources) == sources


def test_source_canonicalization_drops_malformed_entries_deterministically() -> None:
    raw = [
        None,
        "actor_subscription",
        {},
        {"type": "unknown", "reason": "not contracted"},
        {"type": "gap_scan", 1: "non-string-key"},
        {"type": "resubscribe", "rule": {"not-json"}},
        {"type": "moviepilot", "score": math.nan},
        {"type": "gap_scan", "reason": "missing_episode"},
    ]

    sources = SubscriptionSources.from_raw(raw)

    assert sources.to_json_value() == [{"type": "gap_scan", "reason": "missing_episode"}]
    assert SubscriptionSources.from_raw({"type": "gap_scan"}) == SubscriptionSources()
    assert SubscriptionSources.from_raw("[]") == SubscriptionSources()


def test_source_identity_keeps_distinct_audit_metadata() -> None:
    sources = SubscriptionSources.from_raw(
        [
            {"type": "actor_subscription", "id": 7, "name": "旧名"},
            {"type": "actor_subscription", "id": 7, "name": "新名"},
        ]
    )

    assert len(sources) == 2


def test_source_diff_is_directional_deterministic_and_idempotent() -> None:
    current = SubscriptionSources.from_raw(
        [
            {"type": "actor_subscription", "id": 7, "name": "演员"},
            {"type": "moviepilot", "subscribe_id": 11, "reason": "history"},
        ]
    )
    desired = SubscriptionSources.from_raw(
        [
            {"reason": "history", "subscribe_id": 11, "type": "moviepilot"},
            {"type": "revived_season", "reason": "watchlist_revival", "item_id": "100"},
        ]
    )

    diff = current.diff(desired)

    assert [source.source_type for source in diff.added] == [SubscriptionSourceType.REVIVED_SEASON]
    assert [source.source_type for source in diff.removed] == [SubscriptionSourceType.ACTOR_SUBSCRIPTION]
    assert [source.source_type for source in diff.unchanged] == [SubscriptionSourceType.MOVIEPILOT]
    assert diff.changed is True
    assert desired.diff(desired).changed is False
    assert current.merge(SubscriptionSources(diff.added)).diff(desired).removed == diff.removed
    assert (
        current.canonical_json()
        == SubscriptionSources.from_raw(list(reversed(current.to_json_value()))).canonical_json()
    )


def test_source_value_object_rejects_unknown_or_non_json_metadata() -> None:
    with pytest.raises(ValueError, match="unknown subscription source type"):
        SubscriptionSource("manual")
    with pytest.raises(ValueError, match="cannot contain 'type'"):
        SubscriptionSource(SubscriptionSourceType.GAP_SCAN, {"type": "other"})
    with pytest.raises(ValueError, match="unsupported"):
        SubscriptionSource(SubscriptionSourceType.GAP_SCAN, {"bad": object()})
