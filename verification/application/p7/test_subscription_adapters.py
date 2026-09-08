from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import httpx
from pydantic import SecretStr

from etk.domains.subscription.adapters import (
    ConfiguredSubscriptionAvailabilityGateway,
    HttpxEmbyItemDeletionGateway,
    NativeWatchlistExternalDeleteGateway,
    StaticAssistantDefaults,
)
from etk.domains.subscription.moviepilot import MoviePilotListResult, MoviePilotWriteResult
from etk.domains.subscription.watchlist import WatchlistDeleteContext
from etk.platform import Settings


class FakeSettingsRepository:
    def __init__(self, value: object) -> None:
        self.value = value

    def load(self, key: str) -> Any:
        del key
        return self.value

    def save(self, key: str, value: Any) -> None:
        del key, value
        raise AssertionError("unexpected save")

    def delete(self, key: str) -> bool:
        del key
        raise AssertionError("unexpected delete")


class DictSettingsRepository:
    def __init__(self, values: Mapping[str, Any]) -> None:
        self.values = dict(values)

    def load(self, key: str) -> Any:
        return self.values.get(key)

    def save(self, key: str, value: Any) -> None:
        del key, value
        raise AssertionError("unexpected save")

    def delete(self, key: str) -> bool:
        del key
        raise AssertionError("unexpected delete")


class FakeMoviePilot:
    def __init__(self, subscriptions: tuple[Mapping[str, Any], ...]) -> None:
        self.subscriptions = subscriptions
        self.deleted: list[str] = []

    def list_subscriptions(self) -> MoviePilotListResult:
        return MoviePilotListResult(self.subscriptions)

    def delete_subscription_by_id(self, subscription_id: int | str) -> MoviePilotWriteResult:
        self.deleted.append(str(subscription_id))
        return MoviePilotWriteResult("delete-subscription-by-id", 204)


class FakeEmby:
    def __init__(self, failed: set[str] | None = None) -> None:
        self.failed = failed or set()
        self.deleted: list[str] = []

    def delete_item(self, item_id: str) -> bool:
        self.deleted.append(item_id)
        return item_id not in self.failed


def test_watchlist_external_delete_filters_moviepilot_and_deletes_all_emby_items() -> None:
    moviepilot = FakeMoviePilot(
        (
            {"id": 1, "tmdbid": "1399"},
            {"id": 2, "tmdbid": "1399", "season": 2},
            {"id": 3, "tmdbid": "550"},
        )
    )
    emby = FakeEmby()
    gateway = NativeWatchlistExternalDeleteGateway(
        FakeSettingsRepository({"moviepilot_url": "https://mp.invalid"}),
        moviepilot,
        emby,
    )

    result = gateway.delete(WatchlistDeleteContext("1399", "Fixture Series", ("emby-1", "emby-2")))

    assert result == {
        "success": True,
        "item": {"tmdb_id": "1399", "title": "Fixture Series", "emby_ids": ["emby-1", "emby-2"]},
        "mp_cancelled": True,
        "mp_deleted": 2,
        "emby_deleted": 2,
    }
    assert moviepilot.deleted == ["1", "2"]
    assert emby.deleted == ["emby-1", "emby-2"]


def test_watchlist_external_delete_stops_local_cleanup_on_emby_failure() -> None:
    gateway = NativeWatchlistExternalDeleteGateway(
        FakeSettingsRepository({}),
        FakeMoviePilot(()),
        FakeEmby({"emby-2"}),
    )

    result = gateway.delete(WatchlistDeleteContext("1399", "Fixture Series", ("emby-1", "emby-2")))

    assert result["success"] is False
    assert result["status"] == 500
    assert result["failed_emby_ids"] == ["emby-2"]


def test_httpx_emby_delete_uses_configured_token_and_treats_missing_item_as_done() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["token"] = request.headers.get("X-Emby-Token")
        return httpx.Response(404)

    settings = Settings(
        emby_server_url="https://emby.invalid/",
        emby_api_key=SecretStr("fixture-token"),
        emby_user_id="fixture-user",
    )
    gateway = HttpxEmbyItemDeletionGateway(
        lambda: settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )

    assert gateway.delete_item("item / 1") is True
    assert captured == {
        "method": "POST",
        "url": "https://emby.invalid/Items/item%20%2F%201/Delete?UserId=fixture-user",
        "token": "fixture-token",
    }


def test_subscription_availability_uses_injected_probes_and_degrades_probe_failures() -> None:
    repository = DictSettingsRepository(
        {
            "mp_config": {"moviepilot_url": "https://mp.invalid"},
            "tg_userbot_config": {"enabled": True, "channels": ["fixture"]},
        }
    )
    gateway = ConfiguredSubscriptionAvailabilityGateway(
        repository,
        hdhive_probe=lambda: True,
        shared_pool_probe=lambda: False,
    )

    assert gateway.status() == {
        "mp_configured": True,
        "hdhive_configured": True,
        "tg_userbot_configured": True,
        "shared_pool_configured": False,
        "cloud_search_configured": True,
    }
    assert gateway.available_sources() == ("mp", "hdhive", "tg_channel")

    def failed_probe() -> bool:
        raise RuntimeError("fixture probe failure")

    degraded = ConfiguredSubscriptionAvailabilityGateway(
        repository,
        hdhive_probe=failed_probe,
        shared_pool_probe=failed_probe,
    )
    assert degraded.status()["hdhive_configured"] is False
    assert degraded.status()["shared_pool_configured"] is False


def test_assistant_defaults_adapter_returns_isolated_native_defaults() -> None:
    first = dict(StaticAssistantDefaults.defaults())
    second = dict(StaticAssistantDefaults.defaults())

    assert first["enabled"] is True
    assert first["guard_mode"] == "balanced"
    assert first["delete_exclude_tags"] == ["H&R"]
    assert first["tracker_keywords"] == [
        "torrent not registered with this tracker",
        "torrent banned",
    ]
    first["delete_exclude_tags"].append("fixture")
    assert second["delete_exclude_tags"] == ["H&R"]
