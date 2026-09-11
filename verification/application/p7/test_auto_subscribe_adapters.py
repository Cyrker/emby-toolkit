from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

import pytest

from etk.domains.subscription.auto_subscribe import AutoSourceDisposition, AutoSourceError
from etk.domains.subscription.auto_subscribe_adapters import (
    AutoSubscriptionMoviePilotGateway,
    CallableOrderedAutoSubscriptionSources,
    TelegramAutoSubscriptionNotifications,
)
from etk.domains.subscription.runtime_repository import SubscriptionQuotaExhaustedError
from etk.integrations import ProviderDisposition, ProviderResponse


class _Progress:
    def __init__(self) -> None:
        self.checkpoints = 0

    def update(self, progress: int, message: str) -> None:
        del progress, message

    def checkpoint(self) -> None:
        self.checkpoints += 1


class _Settings:
    def __init__(self, values: Mapping[str, Any] | None = None) -> None:
        self.values = dict(values or {})

    def load(self, key: str) -> Any:
        return self.values.get(key, {})


class _Tmdb:
    def __init__(self, details: Mapping[str, Any] | None = None) -> None:
        self.details = dict(details or {})
        self.calls: list[int] = []

    def tv(self, tmdb_id: int) -> Mapping[str, Any]:
        self.calls.append(tmdb_id)
        return self.details


class _Effects:
    def __init__(self) -> None:
        self.create_results: list[bool | Exception] = []
        self.status_results: list[bool] = []
        self.creates: list[tuple[dict[str, Any], bool, bool]] = []
        self.statuses: list[tuple[str, int | None, str, int | None]] = []

    def create_subscription(
        self,
        payload: Mapping[str, Any],
        *,
        consume_quota: bool,
        apply_local_continuation: bool = True,
    ) -> bool:
        self.creates.append((dict(payload), consume_quota, apply_local_continuation))
        result = self.create_results.pop(0) if self.create_results else True
        if isinstance(result, Exception):
            raise result
        return result

    def update_subscription_status(
        self,
        tmdb_id: int | str,
        *,
        season: int | None,
        status: str,
        total_episodes: int | None,
    ) -> bool:
        self.statuses.append((str(tmdb_id), season, status, total_episodes))
        return self.status_results.pop(0) if self.status_results else True


def _gateway(
    *,
    effects: _Effects | None = None,
    settings: Mapping[str, Any] | None = None,
    details: Mapping[str, Any] | None = None,
) -> tuple[AutoSubscriptionMoviePilotGateway, _Effects, _Tmdb]:
    effects = effects or _Effects()
    tmdb = _Tmdb(details)
    return (
        AutoSubscriptionMoviePilotGateway(
            effects,
            _Settings(settings),
            tmdb,
            clock=lambda: datetime(2026, 8, 30, 4, tzinfo=UTC),
        ),
        effects,
        tmdb,
    )


def test_moviepilot_adapter_movie_payload_and_quota_failure() -> None:
    gateway, effects, _tmdb = _gateway()

    result = gateway.subscribe_item(
        {
            "tmdb_id": "42",
            "item_type": "Movie",
            "title": "Fixture Movie",
            "operation_occurrence": "movie-occurrence",
        },
        _Progress(),
    )

    assert result.processed is True
    assert result.provider_failures == ()
    assert effects.creates == [({"name": "Fixture Movie", "tmdbid": 42, "type": "电影"}, True, False)]

    effects.create_results = [SubscriptionQuotaExhaustedError("quota")]
    exhausted = gateway.subscribe_item(
        {
            "tmdb_id": "43",
            "item_type": "Movie",
            "title": "Quota Movie",
            "operation_occurrence": "quota-occurrence",
        },
        _Progress(),
    )
    assert exhausted.processed is False
    assert exhausted.quota_exhausted is True
    assert exhausted.provider_failures == ()


def test_moviepilot_adapter_pending_season_requires_confirmed_status_update() -> None:
    gateway, effects, tmdb = _gateway(
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
        details={
            "status": "Returning Series",
            "seasons": [{"season_number": 2, "air_date": "2026-08-20", "episode_count": 1}],
        },
    )
    effects.status_results = [False]

    result = gateway.subscribe_item(
        {
            "tmdb_id": "season-row",
            "parent_series_tmdb_id": "100",
            "season_number": 2,
            "item_type": "Season",
            "title": "Fixture Series",
            "operation_occurrence": "season-occurrence",
        },
        _Progress(),
    )

    assert tmdb.calls == [100]
    assert effects.creates == [
        (
            {"name": "Fixture Series", "tmdbid": 100, "type": "电视剧", "season": 2},
            True,
            False,
        )
    ]
    assert effects.statuses == [("100", 2, "P", 99)]
    assert result.processed is False
    assert result.provider_failures == (
        {"action": "update_status", "tmdb_id": "100", "item_type": "Season", "season": 2},
    )


def test_moviepilot_adapter_completed_wash_flags_and_ignores_retired_tg_tracking() -> None:
    gateway, effects, _tmdb = _gateway(
        settings={"mp_config": {"subscribe_assistant": {"best_version_type": "completed_full"}}},
        details={"status": "Ended", "seasons": [{"season_number": 1, "episode_count": 8}]},
    )
    completed = gateway.subscribe_item(
        {
            "tmdb_id": "season-row",
            "parent_series_tmdb_id": "200",
            "season_number": 1,
            "item_type": "Season",
            "title": "Completed Series",
            "operation_occurrence": "completed-occurrence",
        },
        _Progress(),
    )
    assert completed.processed is True
    assert effects.creates[0][0]["best_version"] == 1
    assert effects.creates[0][0]["best_version_full"] == 1

    gateway, effects, _tmdb = _gateway(
        settings={"watchlist_config": {"tg_channel_tracking": True}},
        details={"status": "Returning Series", "seasons": [{"season_number": 3}]},
    )
    historical_tg = gateway.subscribe_item(
        {
            "tmdb_id": "season-row",
            "parent_series_tmdb_id": "201",
            "season_number": 3,
            "item_type": "Season",
            "title": "Tracking Series",
            "operation_occurrence": "tracking-occurrence",
        },
        _Progress(),
    )
    assert historical_tg.processed is True
    assert historical_tg.action_type == "MP"
    assert len(effects.creates) == 1


def test_callable_sources_normalize_results_and_fail_closed() -> None:
    calls: list[dict[str, Any]] = []

    def confirmed(context: Mapping[str, Any]) -> bool:
        calls.append(dict(context))
        return True

    sources = CallableOrderedAutoSubscriptionSources(
        {
            "confirmed": confirmed,
            "retryable": lambda _context: {"status": "retryable"},
            "pending": lambda _context: {"status": "pending"},
            "rejected": lambda _context: ProviderResponse(ProviderDisposition.REJECTED, 400),
            "unknown": lambda _context: (_ for _ in ()).throw(RuntimeError("unknown")),
        },
        replay_safe_sources=("confirmed", "rejected"),
    )
    progress = _Progress()
    context = {"tmdb_id": "1", "item_type": "Movie"}

    assert sources.replay_safe("confirmed") is True
    assert sources.replay_safe("retryable") is False
    assert sources.attempt("missing", context, progress).disposition is AutoSourceDisposition.MISS
    assert sources.attempt("confirmed", context, progress).disposition is AutoSourceDisposition.CONFIRMED
    assert sources.attempt("retryable", context, progress).disposition is AutoSourceDisposition.RETRYABLE
    assert sources.attempt("pending", context, progress).disposition is AutoSourceDisposition.PENDING
    assert sources.attempt("rejected", context, progress).disposition is AutoSourceDisposition.REJECTED
    with pytest.raises(AutoSourceError) as raised:
        sources.attempt("unknown", context, progress)
    assert raised.value.disposition is AutoSourceDisposition.INDETERMINATE
    assert calls == [context]


class _Directory:
    def user_chat_id(self, user_id: str) -> str | None:
        return {"u1": "chat-user"}.get(user_id)

    def admin_chat_ids(self) -> Sequence[str]:
        return ("chat-admin",)


class _Telegram:
    def __init__(self, dispositions: Sequence[ProviderDisposition] = ()) -> None:
        self.dispositions = list(dispositions)
        self.messages: list[tuple[str, str, bool]] = []

    def send_message(
        self,
        chat_id: str,
        text: str,
        *,
        disable_notification: bool = False,
        reply_markup: Mapping[str, Any] | None = None,
    ) -> ProviderResponse[dict[str, Any]]:
        del reply_markup
        self.messages.append((chat_id, text, disable_notification))
        disposition = self.dispositions.pop(0) if self.dispositions else ProviderDisposition.CONFIRMED
        return ProviderResponse(disposition, 200, {})


def test_notifications_use_native_directory_and_reject_unconfirmed_delivery() -> None:
    telegram = _Telegram()
    notifications = TelegramAutoSubscriptionNotifications(_Directory(), telegram)  # type: ignore[arg-type]

    notifications.success("u1", ["Movie"])
    notifications.rejected("missing", ["Ignored"])
    notifications.summary([{"action": "MP", "title": "Movie"}], quota_exhausted=True)

    assert [item[0] for item in telegram.messages] == ["chat-user", "chat-admin"]
    assert telegram.messages[-1][2] is True
    assert "配额已用尽" in telegram.messages[-1][1]

    rejected = TelegramAutoSubscriptionNotifications(
        _Directory(),
        _Telegram([ProviderDisposition.REJECTED]),  # type: ignore[arg-type]
    )
    with pytest.raises(RuntimeError, match="not confirmed"):
        rejected.success("u1", ["Movie"])
