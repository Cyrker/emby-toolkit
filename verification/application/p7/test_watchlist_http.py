from __future__ import annotations

import ast
import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.subscription.watchlist import (
    SHARED_RESOURCE_CONFIG_SETTING,
    WATCHLIST_CONFIG_SETTING,
    WATCHLIST_USE_CASE_PROVIDER,
    WatchlistDeleteContext,
    WatchlistService,
)
from etk.domains.subscription.watchlist_router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_PATH = REPO_ROOT / "verification/fixtures/http/p7-watchlist.json"
FIXTURE: Mapping[str, Any] = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
CONFIG_OWNERSHIP = REPO_ROOT / "verification/contracts/config-ownership.yaml"
SECRET_KEY = b"p7-watchlist-fixture-session-key"


class FakePermissions:
    def __init__(self) -> None:
        self.admins: set[str] = set()
        self.calls: list[str] = []

    def is_admin(self, user_id: str) -> bool:
        self.calls.append(user_id)
        return user_id in self.admins


class FakeSettingsRepository:
    def __init__(
        self,
        values: Mapping[str, Any] | None = None,
        *,
        events: list[str] | None = None,
    ) -> None:
        self.values = deepcopy(dict(values or {}))
        self.saved: list[tuple[str, Any]] = []
        self.deleted: list[str] = []
        self.fail_on: tuple[str, str] | None = None
        self.events = events if events is not None else []

    def load(self, key: str) -> Any:
        self._fail("load", key)
        return deepcopy(self.values.get(key))

    def save(self, key: str, value: Any) -> None:
        self._fail("save", key)
        copied = deepcopy(value)
        self.values[key] = copied
        self.saved.append((key, copied))
        self.events.append(f"settings-save:{key}")

    def delete(self, key: str) -> bool:
        self._fail("delete", key)
        self.values.pop(key, None)
        self.deleted.append(key)
        return True

    def _fail(self, operation: str, key: str) -> None:
        if self.fail_on == (operation, key):
            raise RuntimeError(f"fixture {operation} failure")


class FakeWatchlistRepository:
    def __init__(self, *, events: list[str] | None = None) -> None:
        self.events = events if events is not None else []
        self.items: list[Mapping[str, Any]] = []
        self.contexts: dict[str, Mapping[str, Any]] = {}
        self.names: dict[str, str] = {}
        self.update_status_result = True
        self.remove_result = True
        self.force_end_count = 0
        self.batch_update_count = 0
        self.batch_remove_count = 0
        self.added: list[tuple[str, str]] = []
        self.status_updates: list[tuple[str, str]] = []
        self.batch_updates: list[tuple[tuple[Any, ...], str]] = []
        self.batch_removed: list[tuple[Any, ...]] = []
        self.total_updates: list[tuple[Any, Any, Any]] = []
        self.fail_on: str | None = None

    def list_watchlist_items(self) -> Sequence[Mapping[str, Any]]:
        self._fail("list")
        self.events.append("list")
        return deepcopy(self.items)

    def add_watchlist_item(self, tmdb_id: str, item_name: str) -> None:
        self._fail("add")
        self.added.append((tmdb_id, item_name))

    def update_watchlist_status(self, tmdb_id: str, new_status: str) -> bool:
        self._fail("status")
        self.status_updates.append((tmdb_id, new_status))
        return self.update_status_result

    def watchlist_delete_context(self, tmdb_id: str) -> Mapping[str, Any] | None:
        self._fail("context")
        self.events.append(f"context:{tmdb_id}")
        return self.contexts.get(tmdb_id)

    def remove_watchlist_item(self, tmdb_id: str) -> bool:
        self._fail("remove")
        self.events.append(f"remove:{tmdb_id}")
        return self.remove_result

    def watchlist_item_name(self, tmdb_id: str) -> str | None:
        self._fail("name")
        self.events.append(f"name:{tmdb_id}")
        return self.names.get(tmdb_id)

    def force_end_watchlist_seasons(self, tmdb_ids: Sequence[Any]) -> int:
        self._fail("force")
        self.events.append(f"force:{tuple(tmdb_ids)}")
        return self.force_end_count

    def batch_update_watchlist_status(self, item_ids: Sequence[Any], new_status: str) -> int:
        self._fail("batch-status")
        self.batch_updates.append((tuple(item_ids), new_status))
        return self.batch_update_count

    def batch_remove_watchlist_items(self, tmdb_ids: Sequence[Any]) -> int:
        self._fail("batch-remove")
        normalized = tuple(tmdb_ids)
        self.batch_removed.append(normalized)
        self.events.append(f"batch-remove:{normalized}")
        return self.batch_remove_count

    def update_watchlist_total_episodes(
        self,
        tmdb_id: Any,
        total_episodes: Any,
        item_type: Any,
    ) -> None:
        self._fail("total")
        self.total_updates.append((tmdb_id, total_episodes, item_type))

    def _fail(self, operation: str) -> None:
        if self.fail_on == operation:
            raise RuntimeError(f"fixture {operation} failure")


class FakeExternalDelete:
    def __init__(self, *, events: list[str] | None = None) -> None:
        self.events = events if events is not None else []
        self.results: dict[str, Mapping[str, Any]] = {}
        self.calls: list[WatchlistDeleteContext] = []

    def delete(self, context: WatchlistDeleteContext) -> Mapping[str, Any]:
        self.calls.append(context)
        self.events.append(f"external:{context.tmdb_id}")
        return deepcopy(
            self.results.get(
                context.tmdb_id,
                {
                    "success": True,
                    "mp_cancelled": True,
                    "mp_deleted": 1,
                    "emby_deleted": len(context.emby_ids),
                },
            )
        )


class FakeRefresh:
    def __init__(self, *, events: list[str] | None = None) -> None:
        self.events = events if events is not None else []
        self.ready_value = True
        self.accepted = True
        self.raise_on_submit = False
        self.calls: list[tuple[str, str]] = []

    def ready(self) -> bool:
        self.events.append("refresh-ready")
        return self.ready_value

    def submit(self, *, tmdb_id: str, item_name: str) -> bool:
        self.events.append(f"refresh-submit:{tmdb_id}")
        self.calls.append((tmdb_id, item_name))
        if self.raise_on_submit:
            raise RuntimeError("fixture submission failure")
        return self.accepted


def _build_app(
    *,
    auth_enabled: bool = False,
    setting_values: Mapping[str, Any] | None = None,
) -> tuple[
    FastAPI,
    FakePermissions,
    FakeWatchlistRepository,
    FakeSettingsRepository,
    FakeExternalDelete,
    FakeRefresh,
    list[str],
]:
    events: list[str] = []
    permissions = FakePermissions()
    repository = FakeWatchlistRepository(events=events)
    settings_repository = FakeSettingsRepository(setting_values, events=events)
    external = FakeExternalDelete(events=events)
    refresh = FakeRefresh(events=events)
    service = WatchlistService(repository, settings_repository, external, refresh)
    container = AppContainer(
        Settings(auth_enabled=auth_enabled),
        repository_providers={"identity": lambda _: permissions},
        use_case_providers={WATCHLIST_USE_CASE_PROVIDER: lambda _: service},
    )
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app = FastAPI()
    app.state.container = container
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return app, permissions, repository, settings_repository, external, refresh, events


def _set_user(client: TestClient, user_id: str) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set("session", codec.dumps({"emby_user_id": user_id}))


def test_fixture_and_openapi_freeze_all_eleven_admin_bindings_without_422() -> None:
    assert FIXTURE["schema_version"] == 1
    assert FIXTURE["provenance"]["captured_live"] is False
    assert FIXTURE["provenance"]["sanitized"] is True
    expected = {(route["method"].lower(), route["path"]) for route in FIXTURE["routes"]}
    assert len(expected) == 11
    assert all(route["auth"] == "admin" for route in FIXTURE["routes"])

    app, *_ = _build_app()
    paths = app.openapi()["paths"]
    actual = {(method, path) for path, operations in paths.items() for method in operations}
    assert actual == expected
    for method, path in expected:
        operation = paths[path][method]
        assert "422" not in operation["responses"]
        fixture_route = next(
            route for route in FIXTURE["routes"] if route["method"].lower() == method and route["path"] == path
        )
        assert set(operation["responses"]) == {str(code) for code in fixture_route["response_statuses"]}
        assert ("requestBody" in operation) is bool(fixture_route["request_body"])


def test_sources_use_only_native_ports_and_do_not_absorb_p5_or_p6_ownership() -> None:
    source_paths = (
        REPO_ROOT / "src/etk/domains/subscription/watchlist.py",
        REPO_ROOT / "src/etk/domains/subscription/watchlist_router.py",
    )
    forbidden_import_roots = {
        "config_manager",
        "constants",
        "database",
        "extensions",
        "handler",
        "routes",
        "sqlalchemy",
        "task_manager",
    }
    for source_path in source_paths:
        source = source_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(source_path))
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        assert imported.isdisjoint(forbidden_import_roots)
        assert "media_metadata" not in source
    watchlist_source = source_paths[0].read_text(encoding="utf-8")
    assert "enqueue_active_backfill" not in watchlist_source
    assert "PersistentIntroDetectionGateway" not in watchlist_source


def test_admin_denial_precedes_body_and_repository_access() -> None:
    app, permissions, repository, *_ = _build_app(auth_enabled=True)
    with TestClient(app) as client:
        response = client.post(
            "/api/watchlist/add",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
    assert response.status_code == 403
    assert response.json() == {
        "status": "error",
        "message": "需要 Emby 管理员权限才能执行此操作",
    }
    assert permissions.calls == []
    assert repository.added == []


def test_verified_repository_admin_is_required_when_auth_is_enabled() -> None:
    app, permissions, repository, *_ = _build_app(auth_enabled=True)
    with TestClient(app) as client:
        _set_user(client, "fixture-user")
        denied = client.get("/api/watchlist")
        permissions.admins.add("fixture-user")
        allowed = client.get("/api/watchlist")

    assert denied.status_code == 403
    assert allowed.status_code == 200
    assert allowed.json() == []
    assert permissions.calls == ["fixture-user", "fixture-user"]
    assert repository.events == ["list"]


def test_list_projects_legacy_names_and_top_level_dates() -> None:
    app, _permissions, repository, *_ = _build_app()
    repository.items = [
        {
            "tmdb_id": "season-100-1",
            "item_name": "示例剧集 第 1 季",
            "next_episode_to_air_json": {"episode_number": 12},
            "missing_info_json": {"seasons_with_gaps": [1]},
            "last_checked_at": datetime(2026, 8, 26, 12, 30, tzinfo=UTC),
            "release_year": date(2026, 1, 2),
        }
    ]
    with TestClient(app) as client:
        response = client.get("/api/watchlist")

    assert response.status_code == 200
    assert response.json() == [
        {
            "tmdb_id": "season-100-1",
            "item_name": "示例剧集 第 1 季",
            "next_episode_to_air": {"episode_number": 12},
            "missing_info": {"seasons_with_gaps": [1]},
            "last_checked_at": "2026-08-26T12:30:00+00:00",
            "release_year": "2026-01-02",
        }
    ]


def test_add_status_and_manual_malformed_body_contracts() -> None:
    app, _permissions, repository, *_ = _build_app()
    with TestClient(app) as client:
        added = client.post(
            "/api/watchlist/add",
            json={
                "item_id": "emby-series-fixture",
                "tmdb_id": "100",
                "item_name": "示例剧集",
                "item_type": "Series",
            },
        )
        wrong_type = client.post(
            "/api/watchlist/add",
            json={"item_id": "movie", "tmdb_id": "101", "item_name": "电影", "item_type": "Movie"},
        )
        malformed = client.post(
            "/api/watchlist/add",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
        unsupported = client.post(
            "/api/watchlist/add",
            content=b"{}",
            headers={"Content-Type": "text/plain"},
        )
        non_object = client.post("/api/watchlist/add", json=[])
        status = client.post(
            "/api/watchlist/update_status",
            json={"item_id": "100", "new_status": "Paused"},
        )
        repository.update_status_result = False
        not_found = client.post(
            "/api/watchlist/update_status",
            json={"item_id": "missing", "new_status": "Completed"},
        )
        invalid_status = client.post(
            "/api/watchlist/update_status",
            json={"item_id": "100", "new_status": "NONE"},
        )

    assert added.status_code == 200
    assert added.json() == {"message": "《示例剧集》已成功添加到追剧列表！"}
    assert repository.added == [("100", "示例剧集")]
    assert wrong_type.status_code == 400
    assert wrong_type.json() == {"error": "只能将'剧集'类型添加到追剧列表"}
    assert malformed.status_code == 400
    assert malformed.headers["content-type"].startswith("text/html")
    assert "400 Bad Request" in malformed.text
    assert unsupported.status_code == 415
    assert unsupported.headers["content-type"].startswith("text/html")
    assert "415 Unsupported Media Type" in unsupported.text
    assert non_object.status_code == 500
    assert non_object.headers["content-type"].startswith("text/html")
    assert "500 Internal Server Error" in non_object.text
    assert status.json() == {"message": "状态更新成功"}
    assert not_found.status_code == 404
    assert not_found.json() == {"error": "未在追剧列表中找到该项目"}
    assert invalid_status.status_code == 400
    assert invalid_status.json() == {"error": "请求参数无效"}
    assert repository.status_updates == [("100", "Paused"), ("missing", "Completed")]


def test_batch_force_end_and_status_preserve_counts_and_validation() -> None:
    app, _permissions, repository, *_ = _build_app()
    repository.force_end_count = 2
    repository.batch_update_count = 3
    with TestClient(app) as client:
        force = client.post("/api/watchlist/batch_force_end", json={"item_ids": ["s1", "s2"]})
        batch_status = client.post(
            "/api/watchlist/batch_update_status",
            json={"item_ids": ["100", "101"], "new_status": "Watching"},
        )
        malformed = client.post(
            "/api/watchlist/batch_force_end",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
        invalid_status = client.post(
            "/api/watchlist/batch_update_status",
            json={"item_ids": ["100"], "new_status": "Pending"},
        )

    assert force.json() == {
        "message": "操作成功！已将 2 个季标记为强制完结。它们不会因集数更新而复活，也不会影响同剧其它季。",
        "updated_count": 2,
    }
    assert batch_status.json() == {
        "message": "操作成功！已将 3 个项目的状态更新为 'Watching'。",
        "updated_count": 3,
    }
    assert repository.batch_updates == [(("100", "101"), "Watching")]
    assert malformed.status_code == 400
    assert malformed.headers["content-type"].startswith("text/html")
    assert "400 Bad Request" in malformed.text
    assert invalid_status.status_code == 400
    assert invalid_status.json() == {"error": "无效的状态值: Pending"}


def test_single_remove_completes_external_delete_before_projection_transaction() -> None:
    app, _permissions, repository, _settings, external, _refresh, events = _build_app()
    repository.contexts["100"] = {
        "tmdb_id": "100",
        "title": "示例剧集",
        "emby_ids": ["emby-1", "emby-2"],
    }
    with TestClient(app) as client:
        response = client.post("/api/watchlist/remove/100")

    assert response.status_code == 200
    assert response.json() == {
        "message": "已从追剧列表移除，并已同步取消 MP 订阅、删除 Emby 媒体项",
        "cleanup": {
            "success": True,
            "mp_cancelled": True,
            "mp_deleted": 1,
            "emby_deleted": 2,
            "item": {"tmdb_id": "100", "title": "示例剧集", "emby_ids": ["emby-1", "emby-2"]},
        },
    }
    assert [context.tmdb_id for context in external.calls] == ["100"]
    assert events == ["context:100", "external:100", "remove:100"]


def test_single_remove_not_found_and_external_failure_do_not_mutate_projection() -> None:
    app, _permissions, repository, _settings, external, _refresh, events = _build_app()
    repository.contexts["failure"] = {
        "tmdb_id": "failure",
        "title": "失败剧集",
        "emby_ids": ["emby-fail"],
    }
    external.results["failure"] = {
        "success": False,
        "status": 500,
        "error": "Emby 媒体项删除失败",
        "failed_emby_ids": ["emby-fail"],
    }
    with TestClient(app) as client:
        missing = client.post("/api/watchlist/remove/missing")
        failed = client.post("/api/watchlist/remove/failure")

    assert missing.status_code == 404
    assert missing.json() == {
        "error": "未在追剧列表中找到该项目",
        "details": {"success": False, "status": 404, "error": "未在追剧列表中找到该项目"},
    }
    assert failed.status_code == 500
    assert failed.json()["error"] == "Emby 媒体项删除失败"
    assert failed.json()["details"]["item"] == {
        "tmdb_id": "failure",
        "title": "失败剧集",
        "emby_ids": ["emby-fail"],
    }
    assert not any(event.startswith("remove:") for event in events)


def test_batch_remove_returns_207_and_batches_only_external_successes_after_cleanup() -> None:
    app, _permissions, repository, _settings, external, _refresh, events = _build_app()
    repository.contexts = {
        "100": {"tmdb_id": "100", "title": "成功剧集", "emby_ids": ["emby-100"]},
        "200": {"tmdb_id": "200", "title": "失败剧集", "emby_ids": ["emby-200"]},
    }
    repository.batch_remove_count = 3
    external.results["200"] = {"success": False, "status": 500, "error": "MoviePilot 取消订阅失败"}
    with TestClient(app) as client:
        response = client.post("/api/watchlist/batch_remove", json={"item_ids": ["100", "200"]})

    assert response.status_code == 207
    assert response.json()["message"] == "已移除 3 个追剧项目；同步删除失败 1 个。"
    assert response.json()["removed_count"] == 3
    assert response.json()["failures"] == [
        {
            "tmdb_id": "200",
            "error": "MoviePilot 取消订阅失败",
            "details": {
                "success": False,
                "status": 500,
                "error": "MoviePilot 取消订阅失败",
                "item": {"tmdb_id": "200", "title": "失败剧集", "emby_ids": ["emby-200"]},
            },
        }
    ]
    assert repository.batch_removed == [("100",)]
    assert events == [
        "context:100",
        "external:100",
        "context:200",
        "external:200",
        "batch-remove:('100',)",
    ]


def test_refresh_preserves_false_as_accepted_not_ready_json_and_exception_html() -> None:
    app, _permissions, repository, _settings, _external, refresh, events = _build_app()
    repository.names["100"] = "示例剧集"
    with TestClient(app) as client:
        accepted = client.post("/api/watchlist/refresh/100")
        refresh.ready_value = False
        unavailable = client.post("/api/watchlist/refresh/200")
        refresh.ready_value = True
        refresh.accepted = False
        rejected = client.post("/api/watchlist/refresh/300")
        refresh.accepted = True
        refresh.raise_on_submit = True
        failed = client.post("/api/watchlist/refresh/400")

    assert accepted.status_code == 202
    assert accepted.json() == {"message": "《示例剧集》的刷新任务已在后台启动！"}
    assert unavailable.status_code == 503
    assert unavailable.json() == {"error": "追剧处理模块未就绪"}
    assert rejected.status_code == 202
    assert rejected.json() == {"message": "《未知剧集》的刷新任务已在后台启动！"}
    assert failed.status_code == 500
    assert failed.headers["content-type"].startswith("text/html")
    assert "500 Internal Server Error" in failed.text
    assert "name:200" not in events
    assert refresh.calls == [("100", "示例剧集"), ("300", "未知剧集"), ("400", "未知剧集")]


def test_update_total_episodes_preserves_default_type_and_manual_validation() -> None:
    app, _permissions, repository, *_ = _build_app()
    with TestClient(app) as client:
        updated = client.post(
            "/api/watchlist/update_total_episodes",
            json={"tmdb_id": "season-100-1", "total_episodes": 12},
        )
        invalid = client.post("/api/watchlist/update_total_episodes", json={"tmdb_id": "season-100-1"})

    assert updated.status_code == 200
    assert updated.json() == {"message": "已修正总集数为 12 并锁定。"}
    assert repository.total_updates == [("season-100-1", 12, "Season")]
    assert invalid.status_code == 400
    assert invalid.json() == {"error": "参数无效"}


def test_settings_defaults_legacy_migration_and_pure_normalized_persistence() -> None:
    app, _permissions, _repository, settings, *_ = _build_app(
        setting_values={
            SHARED_RESOURCE_CONFIG_SETTING: {"p115_etk_intro_detection_enabled": "on"},
        }
    )
    with TestClient(app) as client:
        migrated = client.get("/api/watchlist/settings")
        saved = client.post(
            "/api/watchlist/settings",
            json={
                "douban_count_correction": True,
                "intro_detection_trigger_mode": "INVALID",
                "intro_detection_credits_enabled": True,
                "intro_detection_library_ids": ["library-a", "", "library-a", 42],
            },
        )
        reread = client.get("/api/watchlist/settings")

    assert migrated.json() == {
        "douban_count_correction": False,
        "revival_check_days": 365,
        "tg_channel_tracking": False,
        "intro_detection_trigger_mode": "import",
        "intro_detection_credits_enabled": False,
        "intro_detection_library_ids": [],
    }
    assert saved.status_code == 200
    assert saved.json() == {"message": "追剧策略配置已保存"}
    assert settings.values[WATCHLIST_CONFIG_SETTING] == {
        "intro_detection_trigger_mode": "off",
        "douban_count_correction": True,
        "revival_check_days": 365,
        "tg_channel_tracking": False,
        "intro_detection_credits_enabled": True,
        "intro_detection_library_ids": ["library-a", "42"],
    }
    assert reread.json() == settings.values[WATCHLIST_CONFIG_SETTING]
    assert [key for key, _value in settings.saved] == [WATCHLIST_CONFIG_SETTING, WATCHLIST_CONFIG_SETTING]


def test_watchlist_config_has_canonical_ownership_without_absorbing_legacy_fallback() -> None:
    manifest = json.loads(CONFIG_OWNERSHIP.read_text(encoding="utf-8"))
    settings = {item["key"]: item for item in manifest["standalone_app_settings"]}

    canonical = settings[WATCHLIST_CONFIG_SETTING]
    assert canonical["contract_id"] == "setting-d1dd5a22454a"
    assert canonical["status"] == "new"
    assert canonical["new_owner"] == "etk.domains.system.repository.SqlAlchemyAppSettingsRepository"
    assert canonical["fixture"] == "verification/fixtures/http/p7-watchlist.json"
    assert canonical["validation_owner"] == "subscription/settings"

    fallback = settings[SHARED_RESOURCE_CONFIG_SETTING]
    assert fallback["status"] == "legacy"
    assert fallback["new_owner"] is None


def test_settings_preserve_existing_unknown_keys_but_ignore_new_unknown_keys() -> None:
    app, _permissions, _repository, settings, *_ = _build_app(
        setting_values={
            WATCHLIST_CONFIG_SETTING: {
                "subscribe_assistant": {"enabled": True},
                "revival_check_days": 30,
            }
        }
    )
    with TestClient(app) as client:
        response = client.post(
            "/api/watchlist/settings",
            json={"revival_check_days": 60, "unknown_new_key": "ignored"},
        )
        malformed = client.post("/api/watchlist/settings", json=["not", "an", "object"])
        invalid_json = client.post(
            "/api/watchlist/settings",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
        unsupported = client.post("/api/watchlist/settings")

    assert response.status_code == 200
    assert settings.values[WATCHLIST_CONFIG_SETTING]["subscribe_assistant"] == {"enabled": True}
    assert settings.values[WATCHLIST_CONFIG_SETTING]["revival_check_days"] == 60
    assert "unknown_new_key" not in settings.values[WATCHLIST_CONFIG_SETTING]
    assert malformed.status_code == 400
    assert malformed.json() == {"error": "配置格式错误"}
    assert invalid_json.status_code == 500
    assert invalid_json.json() == {"error": "保存配置失败"}
    assert unsupported.status_code == 500
    assert unsupported.json() == {"error": "保存配置失败"}


def test_repository_and_settings_failures_keep_legacy_json_500_envelopes() -> None:
    app, _permissions, repository, settings, *_ = _build_app()
    repository.fail_on = "list"
    with TestClient(app) as client:
        listing = client.get("/api/watchlist")
        repository.fail_on = "total"
        total = client.post(
            "/api/watchlist/update_total_episodes",
            json={"tmdb_id": "season-100-1", "total_episodes": 12},
        )
        settings.fail_on = ("save", WATCHLIST_CONFIG_SETTING)
        settings_response = client.post("/api/watchlist/settings", json={})

    assert listing.status_code == 500
    assert listing.json() == {"error": "获取追剧列表时发生服务器内部错误"}
    assert total.status_code == 500
    assert total.json() == {"error": "数据库更新失败"}
    assert settings_response.status_code == 500
    assert settings_response.json() == {"error": "保存配置失败"}
