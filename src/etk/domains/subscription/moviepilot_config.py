from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import Any, Protocol

MOVIEPILOT_CONFIG_USE_CASE_PROVIDER = "subscription.moviepilot-config"
SUBSCRIPTION_STRATEGY_USE_CASE_PROVIDER = "subscription.strategy"
MOVIEPILOT_CONFIG_SETTING = "mp_config"
WATCHLIST_CONFIG_SETTING = "watchlist_config"
SUBSCRIPTION_STRATEGY_SETTING = "subscription_strategy_config"

_RETIRED_KEYS = frozenset(
    {
        "link_delete_transfer_history",
        "link_delete_download_files",
        "timeout_revive_days",
        "download_timeout_hours",
        "movie_protection_days",
        "series_version_lock_mode",
        "series_version_lock_decay_hours",
    }
)
_STALE_ASSISTANT_KEYS = frozenset(
    {
        "notify",
        "best_version_backfill_enabled",
        "recognition_guard_enabled",
        "recognition_guard_mode",
    }
)
_DEFAULT_STRATEGY = {"subscription_sources": ["mp"]}


class MoviePilotSettingsPort(Protocol):
    def load(self, key: str) -> Any: ...

    def save(self, key: str, value: Any) -> None: ...

    def save_many(self, values: Mapping[str, Any]) -> None: ...


class AssistantDefaultsPort(Protocol):
    def defaults(self) -> Mapping[str, Any]: ...


class SubscriptionAvailabilityPort(Protocol):
    def available_sources(self) -> Sequence[str]: ...


class MoviePilotConfigService:
    def __init__(self, settings: MoviePilotSettingsPort, assistant_defaults: AssistantDefaultsPort) -> None:
        self._settings = settings
        self._assistant_defaults = assistant_defaults

    def get(self) -> dict[str, Any]:
        raw_config = self._settings.load(MOVIEPILOT_CONFIG_SETTING)
        config = dict(raw_config) if isinstance(raw_config, Mapping) else {}
        raw_watchlist = self._settings.load(WATCHLIST_CONFIG_SETTING)
        watchlist = dict(raw_watchlist) if isinstance(raw_watchlist, Mapping) else {}
        assistant = config.get("subscribe_assistant")
        if not isinstance(assistant, Mapping):
            fallback = watchlist.get("subscribe_assistant")
            assistant = fallback if isinstance(fallback, Mapping) else {}
        result: dict[str, Any] = {
            "moviepilot_url": "",
            "moviepilot_username": "",
            "moviepilot_password": "",
            "moviepilot_recognition": False,
            "resubscribe_daily_cap": 10,
            "resubscribe_delay_seconds": 2.0,
            "movie_search_window_days": 1,
            "movie_pause_days": 7,
            "delay_subscription_days": 0,
            "subscribe_assistant": dict(self._assistant_defaults.defaults()),
        }
        result.update(deepcopy(config))
        for key in _RETIRED_KEYS:
            result.pop(key, None)
        result["subscribe_assistant"] = {
            **deepcopy(dict(self._assistant_defaults.defaults())),
            **deepcopy(dict(assistant)),
        }
        return result

    def save(self, value: Mapping[str, Any]) -> dict[str, Any]:
        config = deepcopy(dict(value))
        for key in _RETIRED_KEYS:
            config.pop(key, None)
        raw_assistant = config.get("subscribe_assistant")
        assistant = dict(raw_assistant) if isinstance(raw_assistant, Mapping) else {}
        for key in _STALE_ASSISTANT_KEYS:
            assistant.pop(key, None)
        config["subscribe_assistant"] = {
            **deepcopy(dict(self._assistant_defaults.defaults())),
            **deepcopy(assistant),
        }
        self._settings.save(MOVIEPILOT_CONFIG_SETTING, config)
        return config


class SubscriptionStrategyService:
    def __init__(self, settings: MoviePilotSettingsPort, availability: SubscriptionAvailabilityPort) -> None:
        self._settings = settings
        self._availability = availability

    def get(self) -> dict[str, Any]:
        value = self._settings.load(SUBSCRIPTION_STRATEGY_SETTING)
        config = deepcopy(dict(value)) if isinstance(value, Mapping) else {}
        for key, default in _DEFAULT_STRATEGY.items():
            config.setdefault(key, deepcopy(default))
        sources = config.get("subscription_sources")
        config["subscription_sources"] = ["mp"] if isinstance(sources, list) and "mp" in sources else []
        return config

    def save(self, value: Mapping[str, Any]) -> dict[str, Any]:
        config = deepcopy(dict(value))
        sources = config.get("subscription_sources")
        values: dict[str, Any] = {}
        available = set(self._availability.available_sources())
        config["subscription_sources"] = (
            ["mp"] if isinstance(sources, list) and "mp" in sources and "mp" in available else []
        )
        if "mp" not in config["subscription_sources"]:
            raw_mp = self._settings.load(MOVIEPILOT_CONFIG_SETTING)
            mp_config = deepcopy(dict(raw_mp)) if isinstance(raw_mp, Mapping) else {}
            raw_assistant = mp_config.get("subscribe_assistant")
            if isinstance(raw_assistant, Mapping) and raw_assistant.get("enabled"):
                assistant = deepcopy(dict(raw_assistant))
                assistant["enabled"] = False
                mp_config["subscribe_assistant"] = assistant
                values[MOVIEPILOT_CONFIG_SETTING] = mp_config
        values[SUBSCRIPTION_STRATEGY_SETTING] = config
        self._settings.save_many(values)
        return config


__all__ = [
    "AssistantDefaultsPort",
    "MOVIEPILOT_CONFIG_SETTING",
    "MOVIEPILOT_CONFIG_USE_CASE_PROVIDER",
    "MoviePilotConfigService",
    "MoviePilotSettingsPort",
    "SUBSCRIPTION_STRATEGY_SETTING",
    "SUBSCRIPTION_STRATEGY_USE_CASE_PROVIDER",
    "SubscriptionAvailabilityPort",
    "SubscriptionStrategyService",
    "WATCHLIST_CONFIG_SETTING",
]
