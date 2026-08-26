from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.subscription.moviepilot_config import (
    MOVIEPILOT_CONFIG_USE_CASE_PROVIDER,
    SUBSCRIPTION_STRATEGY_USE_CASE_PROVIDER,
    MoviePilotConfigService,
    SubscriptionStrategyService,
)
from etk.domains.subscription.moviepilot_config_router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = json.loads((REPO_ROOT / "verification/fixtures/http/p7-moviepilot-config.json").read_text(encoding="utf-8"))
SECRET_KEY = b"p7-moviepilot-config-fixture-key"


class FakePermissions:
    def __init__(self) -> None:
        self.admins: set[str] = set()

    def is_admin(self, user_id: str) -> bool:
        return user_id in self.admins


class FakeSettings:
    def __init__(self) -> None:
        self.values: dict[str, Any] = {}
        self.saves: list[tuple[str, Any]] = []
        self.save_many_calls: list[dict[str, Any]] = []
        self.fail_load_keys: set[str] = set()
        self.fail_save = False
        self.fail_save_many = False

    def load(self, key: str) -> Any:
        if key in self.fail_load_keys:
            raise RuntimeError("fixture load failure")
        return deepcopy(self.values.get(key))

    def save(self, key: str, value: Any) -> None:
        if self.fail_save:
            raise RuntimeError("fixture save failure")
        copied = deepcopy(value)
        self.saves.append((key, copied))
        self.values[key] = copied

    def save_many(self, values: Mapping[str, Any]) -> None:
        if self.fail_save_many:
            raise RuntimeError("fixture save-many failure")
        copied = deepcopy(dict(values))
        self.save_many_calls.append(copied)
        self.values.update(copied)


class FakeDefaults:
    @staticmethod
    def defaults() -> Mapping[str, Any]:
        return {"enabled": True, "guard_mode": "balanced", "verify_enabled": True}


class FakeAvailability:
    def __init__(self, sources: Sequence[str] = ("mp", "shared_pool")) -> None:
        self.sources = tuple(sources)

    def available_sources(self) -> Sequence[str]:
        return self.sources


def _build_app(
    *,
    auth_enabled: bool = False,
) -> tuple[FastAPI, FakePermissions, FakeSettings, FakeAvailability]:
    permissions = FakePermissions()
    settings = FakeSettings()
    availability = FakeAvailability()
    moviepilot = MoviePilotConfigService(settings, FakeDefaults())
    strategy = SubscriptionStrategyService(settings, availability)
    container = AppContainer(
        Settings(auth_enabled=auth_enabled),
        repository_providers={"identity": lambda _: permissions},
        use_case_providers={
            MOVIEPILOT_CONFIG_USE_CASE_PROVIDER: lambda _: moviepilot,
            SUBSCRIPTION_STRATEGY_USE_CASE_PROVIDER: lambda _: strategy,
        },
    )
    app = FastAPI()
    app.state.container = container
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return app, permissions, settings, availability


def _set_user(client: TestClient, user_id: str) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set("session", codec.dumps({"emby_user_id": user_id}))


def test_fixture_freezes_four_routes_without_fastapi_422() -> None:
    app, *_ = _build_app()
    paths = app.openapi()["paths"]
    expected = {(route["method"].lower(), route["path"]) for route in FIXTURE["routes"]}
    actual = {(method, path) for path, operations in paths.items() for method in operations}
    assert actual == expected
    for route in FIXTURE["routes"]:
        operation = paths[route["path"]][route["method"].lower()]
        assert "422" not in operation["responses"]
        assert set(operation["responses"]) == {str(value) for value in route["response_statuses"]}
        assert ("requestBody" in operation) is route["request_body"]


def test_admin_contract_defaults_watchlist_fallback_and_retired_keys() -> None:
    app, permissions, settings, _ = _build_app(auth_enabled=True)
    settings.values["mp_config"] = {
        "moviepilot_url": "https://mp.invalid",
        "moviepilot_password": "fixture-password",
        "link_delete_download_files": True,
    }
    settings.values["watchlist_config"] = {"subscribe_assistant": {"enabled": False, "season_cooldown_days": 21}}
    with TestClient(app) as client:
        denied = client.get("/api/subscription/mp/config")
        denied_post = client.post(
            "/api/subscription/strategy",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
        permissions.admins.add("admin")
        _set_user(client, "admin")
        response = client.get("/api/subscription/mp/config")

    assert denied.status_code == 403
    assert denied.json() == {"status": "error", "message": "需要 Emby 管理员权限才能执行此操作"}
    assert denied_post.status_code == 403
    assert denied_post.json() == {"status": "error", "message": "需要 Emby 管理员权限才能执行此操作"}
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["moviepilot_url"] == "https://mp.invalid"
    assert data["moviepilot_password"] == "fixture-password"
    assert "link_delete_download_files" not in data
    assert data["subscribe_assistant"] == {
        "enabled": False,
        "guard_mode": "balanced",
        "verify_enabled": True,
        "season_cooldown_days": 21,
    }


def test_moviepilot_save_replaces_config_and_removes_retired_and_stale_keys() -> None:
    app, _permissions, settings, _ = _build_app()
    with TestClient(app) as client:
        response = client.post(
            "/api/subscription/mp/config",
            json={
                "moviepilot_url": "https://new.invalid",
                "link_delete_transfer_history": True,
                "subscribe_assistant": {
                    "enabled": False,
                    "notify": True,
                    "recognition_guard_mode": "legacy",
                },
            },
        )

    assert response.json() == {"success": True, "message": "MoviePilot 配置已保存生效"}
    assert settings.saves == [
        (
            "mp_config",
            {
                "moviepilot_url": "https://new.invalid",
                "subscribe_assistant": {
                    "enabled": False,
                    "guard_mode": "balanced",
                    "verify_enabled": True,
                },
            },
        )
    ]


def test_strategy_filters_unavailable_sources_and_disables_assistant_atomically() -> None:
    app, _permissions, settings, availability = _build_app()
    availability.sources = ("shared_pool",)
    settings.values["mp_config"] = {
        "moviepilot_url": "https://mp.invalid",
        "subscribe_assistant": {"enabled": True, "guard_mode": "strict"},
    }
    with TestClient(app) as client:
        default = client.get("/api/subscription/strategy")
        saved = client.post(
            "/api/subscription/strategy",
            json={"subscription_sources": ["mp", "shared_pool", "hdhive"], "keep": "opaque"},
        )

    assert default.json() == {"subscription_sources": ["shared_pool", "hdhive", "tg_channel", "mp"]}
    assert saved.json() == {"message": "策略配置已保存"}
    assert settings.save_many_calls == [
        {
            "mp_config": {
                "moviepilot_url": "https://mp.invalid",
                "subscribe_assistant": {"enabled": False, "guard_mode": "strict"},
            },
            "subscription_strategy_config": {
                "subscription_sources": ["shared_pool"],
                "keep": "opaque",
            },
        }
    ]


def test_body_transport_and_non_object_errors_are_legacy_shaped() -> None:
    app, *_ = _build_app()
    with TestClient(app) as client:
        scalar = client.post("/api/subscription/mp/config", json=[])
        invalid_strategy = client.post("/api/subscription/strategy", json=[])
        malformed = client.post(
            "/api/subscription/mp/config",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
        unsupported_strategy = client.post(
            "/api/subscription/strategy",
            content=b"{}",
            headers={"Content-Type": "text/plain"},
        )
        malformed_strategy = client.post(
            "/api/subscription/strategy",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
        empty_strategy = client.post(
            "/api/subscription/strategy",
            content=b"",
            headers={"Content-Type": "application/json"},
        )

    assert scalar.status_code == 400
    assert scalar.json() == {"success": False, "message": "配置格式错误"}
    assert invalid_strategy.status_code == 400
    assert invalid_strategy.json() == {"error": "无效的配置格式"}
    assert malformed.status_code == 400 and malformed.headers["content-type"].startswith("text/html")
    assert unsupported_strategy.status_code == 500
    assert unsupported_strategy.json() == {"error": "保存配置失败"}
    assert malformed_strategy.status_code == 500
    assert malformed_strategy.json() == {"error": "保存配置失败"}
    assert empty_strategy.status_code == 500
    assert empty_strategy.json() == {"error": "保存配置失败"}


def test_strategy_without_existing_enabled_assistant_does_not_create_moviepilot_config() -> None:
    app, _permissions, settings, availability = _build_app()
    availability.sources = ("shared_pool",)
    with TestClient(app) as client:
        response = client.post(
            "/api/subscription/strategy",
            json={"subscription_sources": ["shared_pool"]},
        )

    assert response.status_code == 200
    assert settings.save_many_calls == [{"subscription_strategy_config": {"subscription_sources": ["shared_pool"]}}]
    assert "mp_config" not in settings.values


def test_unexpected_failure_envelopes_match_each_legacy_route() -> None:
    app, _permissions, settings, _availability = _build_app()
    with TestClient(app) as client:
        settings.fail_load_keys.add("mp_config")
        mp_get = client.get("/api/subscription/mp/config")
        settings.fail_load_keys.clear()
        settings.fail_save = True
        mp_post = client.post("/api/subscription/mp/config", json={})
        settings.fail_save = False
        settings.fail_load_keys.add("subscription_strategy_config")
        strategy_get = client.get("/api/subscription/strategy")
        settings.fail_load_keys.clear()
        settings.fail_save_many = True
        strategy_post = client.post("/api/subscription/strategy", json={})

    assert mp_get.status_code == 500 and mp_get.headers["content-type"].startswith("text/html")
    assert mp_post.status_code == 500 and mp_post.headers["content-type"].startswith("text/html")
    assert strategy_get.status_code == 500
    assert strategy_get.json() == {"error": "获取配置失败"}
    assert strategy_post.status_code == 500
    assert strategy_post.json() == {"error": "保存配置失败"}
