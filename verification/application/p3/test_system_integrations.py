from __future__ import annotations

import json
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient
from pydantic import SecretStr

from etk.bootstrap.app_factory import create_app
from etk.bootstrap.runtime_supervisor import RuntimeSupervisor
from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.system.integrations import ExternalResult, SystemIntegrationService
from etk.platform import AppContainer, Settings

REPO_ROOT = Path(__file__).resolve().parents[3]
ROUTE_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "http" / "system-integration-routes.json"
SECRET_KEY = b"p3-system-integrations-session-key"
ADMIN_ROUTES = (
    ("POST", "/api/ai/test", {}),
    ("POST", "/api/ai/models", {}),
    ("POST", "/api/telegram/test", {}),
)


class Permissions:
    def __init__(self) -> None:
        self.admins: set[str] = set()
        self.raise_if_called = False

    def is_admin(self, user_id: str) -> bool:
        if self.raise_if_called:
            raise AssertionError("auth-disabled route queried the permission repository")
        return user_id in self.admins


class Gateway:
    def __init__(self) -> None:
        self.ai_configs: list[dict[str, Any]] = []
        self.model_configs: list[dict[str, Any]] = []
        self.proxy_calls: list[tuple[str, str]] = []
        self.telegram_calls: list[tuple[str, str]] = []
        self.ai_result = ExternalResult({"success": True, "message": "fixture ai ok"})
        self.models_result = ExternalResult({"success": True, "models": ["fixture-model"], "count": 1})
        self.proxy_result = ExternalResult({"success": True, "message": "fixture proxy ok"})
        self.telegram_result = ExternalResult({"success": True, "message": "测试消息发送成功！请检查您的 Telegram。"})

    def test_ai(self, config: Mapping[str, Any]) -> ExternalResult:
        self.ai_configs.append(deepcopy(dict(config)))
        return self.ai_result

    def list_ai_models(self, config: Mapping[str, Any]) -> ExternalResult:
        self.model_configs.append(deepcopy(dict(config)))
        return self.models_result

    def test_proxy(self, proxy_url: str, tmdb_api_key: str) -> ExternalResult:
        self.proxy_calls.append((proxy_url, tmdb_api_key))
        return self.proxy_result

    def test_telegram(self, token: str, chat_id: str) -> ExternalResult:
        self.telegram_calls.append((token, chat_id))
        return self.telegram_result

    def about_info(self) -> tuple[str, list[dict[str, Any]]]:
        return "10.8.65", [{"version": "fixture-release"}]


def _build_app(tmp_path: Path, *, auth_enabled: bool = True):
    settings = Settings(
        auth_enabled=auth_enabled,
        ai_api_key=SecretStr("fixture-ai-key"),
        telegram_bot_token=SecretStr("fixture-telegram-token"),
        tmdb_api_key=SecretStr("fixture-tmdb-key"),
    )
    permissions = Permissions()
    gateway = Gateway()
    service = SystemIntegrationService(
        settings=lambda: settings,
        gateway=gateway,
    )
    container = AppContainer(
        settings,
        repository_providers={"identity": lambda _: permissions},
        use_case_providers={"system-integrations": lambda _: service},
    )
    app = create_app(
        start_runtime=False,
        data_dir=tmp_path / "config",
        secret_key=SECRET_KEY,
        supervisor=RuntimeSupervisor(),
        container=container,
    )
    return app, permissions, gateway


def _set_user(client: TestClient, user_id: str, *, session_admin: bool = False) -> None:
    client.cookies.set(
        "session",
        LegacySessionCodec.from_secret_key(SECRET_KEY).dumps({"emby_user_id": user_id, "emby_is_admin": session_admin}),
    )


def test_fixture_covers_all_five_integration_routes_and_keeps_live_pending() -> None:
    fixture = json.loads(ROUTE_FIXTURE.read_text(encoding="utf-8"))
    assert {(item["method"], item["path"]) for item in fixture["routes"]} == {
        ("POST", "/api/ai/test"),
        ("POST", "/api/ai/models"),
        ("POST", "/api/proxy/test"),
        ("POST", "/api/telegram/test"),
        ("GET", "/api/system/about_info"),
    }
    assert set(fixture["live_acceptance"].values()) == {"pending"}


def test_pro_endpoints_are_not_registered(tmp_path: Path) -> None:
    app, _, _ = _build_app(tmp_path)
    route_paths = {getattr(route, "path", "") for route in app.routes}
    assert {
        "/api/system/pro_pricing",
        "/api/system/activate_pro",
        "/api/system/transfer_pro",
    }.isdisjoint(route_paths)


def test_public_proxy_and_about_contracts_use_injected_gateway(tmp_path: Path) -> None:
    app, _, gateway = _build_app(tmp_path)
    with TestClient(app) as client:
        proxy = client.post("/api/proxy/test", json={"url": "http://proxy.example.invalid"})
        about = client.get("/api/system/about_info")
    assert proxy.status_code == 200
    assert proxy.json() == {"success": True, "message": "fixture proxy ok"}
    assert gateway.proxy_calls == [("http://proxy.example.invalid", "fixture-tmdb-key")]
    assert about.json() == {
        "current_version": "10.8.65",
        "releases": [{"version": "fixture-release"}],
    }


def test_admin_routes_recheck_database_permission_and_ignore_cookie_flag(tmp_path: Path) -> None:
    app, permissions, _ = _build_app(tmp_path)
    with TestClient(app) as client:
        _set_user(client, "fixture-user", session_admin=True)
        for method, path, payload in ADMIN_ROUTES:
            response = client.request(method, path, json=payload)
            assert response.status_code == 403
            assert response.json() == {
                "status": "error",
                "message": "需要 Emby 管理员权限才能执行此操作",
            }
        permissions.admins.add("fixture-admin")
        _set_user(client, "fixture-admin")
        response = client.post("/api/telegram/test", json={"token": "********", "chat_id": "fixture-chat"})
        assert response.status_code == 200


def test_ai_and_telegram_success_contracts(tmp_path: Path) -> None:
    app, permissions, gateway = _build_app(tmp_path)
    permissions.admins.add("fixture-admin")
    with TestClient(app) as client:
        _set_user(client, "fixture-admin")
        assert client.post(
            "/api/ai/test",
            json={"ai_api_key": "********", "ai_provider": "openai"},
        ).json() == {"success": True, "message": "fixture ai ok"}
        assert client.post(
            "/api/ai/models",
            json={"ai_api_key": "********", "ai_provider": "openai"},
        ).json() == {"success": True, "models": ["fixture-model"], "count": 1}
        assert client.post(
            "/api/telegram/test",
            json={"token": "********", "chat_id": "fixture-chat"},
        ).json() == {"success": True, "message": "测试消息发送成功！请检查您的 Telegram。"}

    assert gateway.ai_configs[-1]["ai_api_key"] == "fixture-ai-key"
    assert gateway.model_configs[-1]["ai_api_key"] == "fixture-ai-key"
    assert gateway.telegram_calls == [("fixture-telegram-token", "fixture-chat")]


def test_validation_fallback_and_gateway_failure_envelopes(tmp_path: Path) -> None:
    app, permissions, _ = _build_app(tmp_path)
    permissions.admins.add("fixture-admin")
    with TestClient(app) as client:
        _set_user(client, "fixture-admin")
        assert client.post("/api/ai/test", json={}).status_code == 400
        assert client.post("/api/ai/models", json={}).status_code == 400
        assert client.post("/api/telegram/test", json={}).status_code == 400


def test_auth_disabled_admin_routes_do_not_query_permission_repository(tmp_path: Path) -> None:
    app, permissions, _ = _build_app(tmp_path, auth_enabled=False)
    permissions.raise_if_called = True
    with TestClient(app) as client:
        response = client.post("/api/telegram/test", json={"token": "********", "chat_id": "fixture-chat"})
    assert response.status_code == 200
