from __future__ import annotations

import ast
import json
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.system.standalone import (
    AI_PROMPTS_SETTING,
    CUSTOM_THEME_SETTING,
    MENU_CONFIG_SETTING,
    STANDALONE_SETTINGS_REPOSITORY_PROVIDER,
    STANDALONE_SETTINGS_USE_CASE_PROVIDER,
    TELEGRAM_TEMPLATES_SETTING,
    StandaloneSettingsService,
)
from etk.domains.system.standalone_router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
ROUTE_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "http" / "system-standalone-routes.json"
CONFIG_OWNERSHIP = REPO_ROOT / "verification" / "contracts" / "config-ownership.yaml"
SOURCE_FILES = (
    REPO_ROOT / "src" / "etk" / "domains" / "system" / "standalone.py",
    REPO_ROOT / "src" / "etk" / "domains" / "system" / "standalone_router.py",
)
SECRET_KEY = b"p3-system-standalone-session-key"
ADMIN_ROUTES = (
    ("GET", "/api/telegram/templates", None),
    ("POST", "/api/telegram/templates", {"templates": {}}),
    ("POST", "/api/config/custom_theme", {}),
    ("DELETE", "/api/config/custom_theme", None),
    ("GET", "/api/ai/prompts", None),
    ("POST", "/api/ai/prompts", {}),
    ("POST", "/api/ai/prompts/reset", None),
    ("POST", "/api/system/menu_config", []),
    ("POST", "/api/system/menu_config/reset", None),
)


class FakePermissions:
    def __init__(self) -> None:
        self.admins: set[str] = set()
        self.calls: list[str] = []
        self.raise_if_called = False

    def is_admin(self, user_id: str) -> bool:
        if self.raise_if_called:
            raise AssertionError("auth-disabled requests must not query permissions")
        self.calls.append(user_id)
        return user_id in self.admins


class FakeStandaloneSettingsRepository:
    def __init__(self, values: Mapping[str, Any] | None = None) -> None:
        self.values = deepcopy(dict(values or {}))
        self.saved: list[tuple[str, Any]] = []
        self.deleted: list[str] = []
        self.fail_next: tuple[str, str] | None = None
        self.delete_results: dict[str, bool] = {}

    def load(self, key: str) -> Any:
        self._raise_if_requested("load", key)
        return deepcopy(self.values.get(key))

    def save(self, key: str, value: Any) -> None:
        self._raise_if_requested("save", key)
        copied = deepcopy(value)
        self.values[key] = copied
        self.saved.append((key, copied))

    def delete(self, key: str) -> bool:
        self._raise_if_requested("delete", key)
        result = self.delete_results.get(key, True)
        if result:
            self.values.pop(key, None)
            self.deleted.append(key)
        return result

    def _raise_if_requested(self, operation: str, key: str) -> None:
        if self.fail_next == (operation, key):
            self.fail_next = None
            raise RuntimeError(f"fixture {operation} failure for {key}")


class FakeDefaults:
    def telegram_templates(self) -> Mapping[str, Any]:
        return {
            "library_new": "default-library-new",
            "playback": "default-playback",
        }

    def ai_prompts(self) -> Mapping[str, Any]:
        return {
            "fast_mode": "default-fast",
            "quality_mode": "default-quality",
        }


def _build_app(
    *,
    auth_enabled: bool = True,
    values: Mapping[str, Any] | None = None,
) -> tuple[FastAPI, FakePermissions, FakeStandaloneSettingsRepository]:
    permissions = FakePermissions()
    repository = FakeStandaloneSettingsRepository(values)
    defaults = FakeDefaults()
    container = AppContainer(
        Settings(auth_enabled=auth_enabled),
        repository_providers={
            "identity": lambda _: permissions,
            STANDALONE_SETTINGS_REPOSITORY_PROVIDER: lambda _: repository,
        },
        use_case_providers={
            STANDALONE_SETTINGS_USE_CASE_PROVIDER: lambda current: StandaloneSettingsService(
                current.repository(STANDALONE_SETTINGS_REPOSITORY_PROVIDER),
                defaults,
            )
        },
    )
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app = FastAPI()
    app.state.container = container
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return app, permissions, repository


def _set_user(client: TestClient, user_id: str, *, session_admin: bool = False) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set(
        "session",
        codec.dumps({"emby_user_id": user_id, "emby_is_admin": session_admin}),
    )


def _request(client: TestClient, method: str, path: str, payload: Any = None):
    kwargs = {"json": payload} if payload is not None else {}
    return client.request(method, path, **kwargs)


def test_static_fixture_covers_all_bindings_and_sources_have_no_legacy_dependencies() -> None:
    fixture = json.loads(ROUTE_FIXTURE.read_text(encoding="utf-8"))
    routes = fixture["routes"]
    assert {(item["method"], item["path"]) for item in routes} == {
        ("GET", "/api/telegram/templates"),
        ("POST", "/api/telegram/templates"),
        ("POST", "/api/config/custom_theme"),
        ("DELETE", "/api/config/custom_theme"),
        ("GET", "/api/ai/prompts"),
        ("POST", "/api/ai/prompts"),
        ("POST", "/api/ai/prompts/reset"),
        ("GET", "/api/system/menu_config"),
        ("POST", "/api/system/menu_config"),
        ("POST", "/api/system/menu_config/reset"),
    }
    assert sum(item["permission"] == "admin" for item in routes) == 9
    assert sum(item["permission"] == "public" for item in routes) == 1
    assert fixture["provenance"]["captured_live"] is False

    forbidden = {"config_manager", "constants", "extensions", "settings_db", "utils"}
    imported: set[str] = set()
    for source_path in SOURCE_FILES:
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
    assert imported.isdisjoint(forbidden)


def test_theme_and_menu_settings_have_native_repository_ownership() -> None:
    manifest = json.loads(CONFIG_OWNERSHIP.read_text(encoding="utf-8"))
    settings = {item["key"]: item for item in manifest["standalone_app_settings"]}

    for key in (CUSTOM_THEME_SETTING, MENU_CONFIG_SETTING):
        contract = settings[key]
        assert contract["status"] == "new"
        assert contract["new_owner"] == "etk.domains.system.repository.SqlAlchemyAppSettingsRepository"
        assert contract["validation_owner"] == "system/standalone-settings"
        assert contract["fixture"] == "verification/fixtures/http/system-standalone-routes.json"


def test_success_contracts_defaults_normalization_delete_reset_and_public_menu() -> None:
    app, permissions, repository = _build_app(
        values={
            TELEGRAM_TEMPLATES_SETTING: {"library_new": "custom-library", "unknown": "ignored"},
            AI_PROMPTS_SETTING: {"fast_mode": "custom-fast"},
            MENU_CONFIG_SETTING: [{"key": "public-menu"}],
        }
    )
    permissions.admins.add("fixture-admin")
    with TestClient(app) as client:
        public_menu = client.get("/api/system/menu_config")
        assert public_menu.status_code == 200
        assert public_menu.json() == [{"key": "public-menu"}]
        assert permissions.calls == []

        _set_user(client, "fixture-admin")
        telegram = client.get("/api/telegram/templates")
        assert telegram.status_code == 200
        assert telegram.json() == {
            "library_new": "custom-library",
            "playback": "default-playback",
        }

        telegram_save = client.post(
            "/api/telegram/templates",
            json={"templates": {"library_new": None, "playback": 7, "unknown": "ignored"}},
        )
        assert telegram_save.json() == {
            "success": True,
            "message": "Telegram 通知模板已保存",
            "templates": {"library_new": "", "playback": "7"},
        }

        assert client.post("/api/config/custom_theme", json={"accent": "blue"}).json() == {
            "message": "你的专属主题已保存！"
        }
        assert client.delete("/api/config/custom_theme").json() == {"message": "自定义主题已删除。"}

        assert client.get("/api/ai/prompts").json() == {
            "fast_mode": "custom-fast",
            "quality_mode": "default-quality",
        }
        assert client.post("/api/ai/prompts", json={"fast_mode": "saved-fast"}).json() == {"message": "提示词已保存"}
        assert client.post("/api/ai/prompts/reset").json() == {
            "message": "已恢复默认提示词",
            "prompts": {"fast_mode": "default-fast", "quality_mode": "default-quality"},
        }

        assert client.post("/api/system/menu_config", json={"layout": "compact"}).json() == {
            "message": "菜单配置已保存"
        }
        assert client.post("/api/system/menu_config", json=[{"key": "saved-menu"}]).json() == {
            "message": "菜单配置已保存"
        }
        assert client.post("/api/system/menu_config/reset").json() == {"message": "已恢复默认菜单"}

    assert repository.values[TELEGRAM_TEMPLATES_SETTING] == {"library_new": "", "playback": "7"}
    assert CUSTOM_THEME_SETTING not in repository.values
    assert AI_PROMPTS_SETTING not in repository.values
    assert MENU_CONFIG_SETTING not in repository.values
    assert repository.deleted == [CUSTOM_THEME_SETTING, AI_PROMPTS_SETTING, MENU_CONFIG_SETTING]


@pytest.mark.parametrize(
    ("path", "payload", "expected"),
    [
        (
            "/api/config/custom_theme",
            [],
            {"error": "无效的主题数据格式，必须是一个JSON对象。"},
        ),
        ("/api/ai/prompts", [], {"error": "无效的数据格式"}),
        ("/api/system/menu_config", "invalid", {"error": "无效的数据格式"}),
    ],
)
def test_invalid_payloads_preserve_400_envelopes(path: str, payload: Any, expected: dict[str, str]) -> None:
    app, permissions, _ = _build_app()
    permissions.admins.add("fixture-admin")
    with TestClient(app) as client:
        _set_user(client, "fixture-admin")
        response = client.post(path, json=payload)
    assert response.status_code == 400
    assert response.json() == expected


def test_custom_theme_delete_false_preserves_legacy_500_envelope() -> None:
    app, permissions, repository = _build_app(values={CUSTOM_THEME_SETTING: {"accent": "blue"}})
    permissions.admins.add("fixture-admin")
    repository.delete_results[CUSTOM_THEME_SETTING] = False
    with TestClient(app) as client:
        _set_user(client, "fixture-admin")
        response = client.delete("/api/config/custom_theme")
    assert response.status_code == 500
    assert response.json() == {"error": "删除自定义主题文件时发生服务器内部错误。"}
    assert repository.values[CUSTOM_THEME_SETTING] == {"accent": "blue"}


def test_admin_policy_uses_live_repository_and_auth_disabled_bypasses_it() -> None:
    app, permissions, _ = _build_app()
    with TestClient(app) as client:
        for method, path, payload in ADMIN_ROUTES:
            denied = _request(client, method, path, payload)
            assert denied.status_code == 403
            assert denied.json() == {
                "status": "error",
                "message": "需要 Emby 管理员权限才能执行此操作",
            }

        _set_user(client, "fixture-user", session_admin=True)
        assert client.get("/api/ai/prompts").status_code == 403
        permissions.admins.add("fixture-user")
        assert client.get("/api/ai/prompts").status_code == 200
        permissions.admins.remove("fixture-user")
        assert client.get("/api/ai/prompts").status_code == 403

    disabled_app, disabled_permissions, _ = _build_app(auth_enabled=False)
    disabled_permissions.raise_if_called = True
    with TestClient(disabled_app) as client:
        response = client.post("/api/config/custom_theme", json={"accent": "green"})
    assert response.status_code == 200


@pytest.mark.parametrize(
    ("method", "path", "operation", "key", "payload", "expected"),
    [
        (
            "GET",
            "/api/telegram/templates",
            "load",
            TELEGRAM_TEMPLATES_SETTING,
            None,
            {"error": "读取 Telegram 通知模板失败"},
        ),
        (
            "POST",
            "/api/telegram/templates",
            "save",
            TELEGRAM_TEMPLATES_SETTING,
            {"templates": {}},
            {"error": "保存 Telegram 通知模板失败"},
        ),
        (
            "POST",
            "/api/config/custom_theme",
            "save",
            CUSTOM_THEME_SETTING,
            {},
            {"error": "保存自定义主题时发生服务器内部错误。"},
        ),
        (
            "DELETE",
            "/api/config/custom_theme",
            "delete",
            CUSTOM_THEME_SETTING,
            None,
            {"error": "删除自定义主题时发生服务器内部错误。"},
        ),
        ("GET", "/api/ai/prompts", "load", AI_PROMPTS_SETTING, None, {"error": "获取提示词失败"}),
        ("POST", "/api/ai/prompts", "save", AI_PROMPTS_SETTING, {}, {"error": "保存失败"}),
        ("POST", "/api/ai/prompts/reset", "delete", AI_PROMPTS_SETTING, None, {"error": "重置失败"}),
        ("GET", "/api/system/menu_config", "load", MENU_CONFIG_SETTING, None, {"error": "获取菜单配置失败"}),
        ("POST", "/api/system/menu_config", "save", MENU_CONFIG_SETTING, [], {"error": "保存失败"}),
        ("POST", "/api/system/menu_config/reset", "delete", MENU_CONFIG_SETTING, None, {"error": "重置失败"}),
    ],
)
def test_repository_failures_preserve_500_envelopes(
    method: str,
    path: str,
    operation: str,
    key: str,
    payload: Any,
    expected: dict[str, str],
) -> None:
    app, permissions, repository = _build_app()
    permissions.admins.add("fixture-admin")
    repository.fail_next = (operation, key)
    with TestClient(app) as client:
        if path != "/api/system/menu_config" or method != "GET":
            _set_user(client, "fixture-admin")
        response = _request(client, method, path, payload)
    assert response.status_code == 500
    assert response.json() == expected
