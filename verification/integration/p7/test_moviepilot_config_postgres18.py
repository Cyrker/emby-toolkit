from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etk.domains.subscription.moviepilot_config import (
    MoviePilotConfigService,
    SubscriptionStrategyService,
)
from etk.domains.system.repository import SqlAlchemyAppSettingsRepository
from etk.platform.db import DatabaseRuntime


class _Defaults:
    @staticmethod
    def defaults() -> Mapping[str, Any]:
        return {"enabled": True, "guard_mode": "balanced"}


class _Availability:
    def __init__(self, sources: Sequence[str]) -> None:
        self.sources = tuple(sources)

    def available_sources(self) -> Sequence[str]:
        return self.sources


def _settings(runtime: DatabaseRuntime) -> SqlAlchemyAppSettingsRepository:
    return SqlAlchemyAppSettingsRepository(runtime.unit_of_work)


def test_moviepilot_config_is_canonicalized_and_persisted_as_one_replacement(
    p7_runtime: DatabaseRuntime,
) -> None:
    service = MoviePilotConfigService(_settings(p7_runtime), _Defaults())

    saved = service.save(
        {
            "moviepilot_url": "https://mp.invalid",
            "link_delete_download_files": True,
            "subscribe_assistant": {"enabled": False, "notify": True},
        }
    )

    assert saved == {
        "moviepilot_url": "https://mp.invalid",
        "subscribe_assistant": {"enabled": False, "guard_mode": "balanced"},
    }
    assert _settings(p7_runtime).load("mp_config") == saved
    assert service.get()["subscribe_assistant"] == {"enabled": False, "guard_mode": "balanced"}


def test_strategy_and_assistant_disable_commit_together(p7_runtime: DatabaseRuntime) -> None:
    settings = _settings(p7_runtime)
    settings.save(
        "mp_config",
        {"moviepilot_url": "https://mp.invalid", "subscribe_assistant": {"enabled": True}},
    )
    service = SubscriptionStrategyService(settings, _Availability(("shared_pool",)))

    saved = service.save({"subscription_sources": ["mp", "shared_pool", "hdhive"]})

    assert saved == {"subscription_sources": ["shared_pool"]}
    assert settings.load("subscription_strategy_config") == {"subscription_sources": ["shared_pool"]}
    assert settings.load("mp_config") == {
        "moviepilot_url": "https://mp.invalid",
        "subscribe_assistant": {"enabled": False},
    }


def test_strategy_without_moviepilot_config_does_not_create_one(p7_runtime: DatabaseRuntime) -> None:
    settings = _settings(p7_runtime)
    service = SubscriptionStrategyService(settings, _Availability(("shared_pool",)))

    service.save({"subscription_sources": ["shared_pool"]})

    assert settings.load("subscription_strategy_config") == {"subscription_sources": ["shared_pool"]}
    assert settings.load("mp_config") is None


def test_strategy_write_failure_rolls_back_assistant_disable(p7_runtime: DatabaseRuntime) -> None:
    settings = _settings(p7_runtime)
    original = {"moviepilot_url": "https://mp.invalid", "subscribe_assistant": {"enabled": True}}
    original_strategy = {"subscription_sources": ["mp"], "revision": "before"}
    settings.save("mp_config", original)
    settings.save("subscription_strategy_config", original_strategy)
    service = SubscriptionStrategyService(settings, _Availability(("shared_pool",)))
    constraint = "verification_p7_strategy_atomic"
    with p7_runtime.engine.begin() as connection:
        connection.execute(text(f"ALTER TABLE app_settings DROP CONSTRAINT IF EXISTS {constraint}"))
        connection.execute(
            text(
                f"""
                ALTER TABLE app_settings ADD CONSTRAINT {constraint}
                CHECK (
                    setting_key <> 'subscription_strategy_config'
                    OR value_json ->> 'reject' IS DISTINCT FROM 'true'
                )
                """
            )
        )
    try:
        with pytest.raises(IntegrityError):
            service.save({"subscription_sources": ["shared_pool"], "reject": True})
        assert settings.load("mp_config") == original
        assert settings.load("subscription_strategy_config") == original_strategy
    finally:
        with p7_runtime.engine.begin() as connection:
            connection.execute(text(f"ALTER TABLE app_settings DROP CONSTRAINT IF EXISTS {constraint}"))
