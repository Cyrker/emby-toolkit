from __future__ import annotations

import ast
import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from flask import Flask

import config_manager
from database import connection as legacy_connection
from database import settings_db as legacy_settings
from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.p115.router import router
from etk.domains.p115.washing_priority import (
    P115_WASHING_PRIORITY_CONFIG_SETTING,
    P115WashingPriorityService,
    build_washing_priority_recalculate_scope,
    effective_washing_priority_config,
    merge_washing_priority_recalculate_state,
    normalize_washing_priority_recalculate_scope,
    normalize_washing_version_slots,
)
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware
from routes import p115 as legacy_routes

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_PATH = REPO_ROOT / "verification/fixtures/http/p11-p115-washing-priority.json"
ROUTE_OWNERSHIP = REPO_ROOT / "verification/contracts/route-ownership.yaml"
CONFIG_OWNERSHIP = REPO_ROOT / "verification/contracts/config-ownership.yaml"
TABLE_OWNERSHIP = REPO_ROOT / "verification/contracts/table-ownership.yaml"
OPENAPI = REPO_ROOT / "verification/contracts/http/p4-openapi.json"
SERVICE_SOURCE = REPO_ROOT / "src/etk/domains/p115/washing_priority.py"
REPOSITORY_SOURCE = REPO_ROOT / "src/etk/domains/p115/washing_priority_repository.py"
FIXTURE = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


class FakePermissions:
    def is_admin(self, user_id: str) -> bool:
        return user_id == "fixture-admin"


class FakeSettings:
    def __init__(self, values: Mapping[str, Any] | None = None) -> None:
        self.values = deepcopy(dict(values or {}))
        self.saves: list[list[Any]] = []
        self.fail_load = False
        self.fail_save = False

    def load_many(self, keys: Sequence[str]) -> dict[str, Any]:
        if self.fail_load:
            raise RuntimeError("fixture settings load failure")
        return {key: deepcopy(self.values.get(key)) for key in keys}

    def save(self, key: str, value: Any) -> None:
        if self.fail_save:
            raise RuntimeError("fixture settings save failure")
        copied = deepcopy(value)
        self.values[key] = copied
        self.saves.append([key, copied])


class FakeWashingRepository:
    def __init__(self) -> None:
        self.groups = deepcopy(FIXTURE["groups_get"]["rows"])
        self.current_config = {
            "conflict_mode": "replace",
            "skip_scope": "directory",
            "version_slots_enabled": False,
            "version_slots": [],
        }
        self.saved_configs: list[dict[str, Any]] = []
        self.replacements: list[list[Any]] = []
        self.fail = False

    def list_groups(self) -> list[dict[str, Any]]:
        if self.fail:
            raise RuntimeError("fixture washing repository failure")
        return deepcopy(self.groups)

    def replace_groups(self, groups: Sequence[object]) -> tuple[dict[str, Any], dict[str, Any]]:
        if self.fail:
            raise RuntimeError("fixture washing repository failure")
        copied = deepcopy(list(groups))
        self.replacements.append(copied)
        return (
            deepcopy(FIXTURE["groups_post"]["affected_scope"]),
            deepcopy(FIXTURE["groups_post"]["pending_scope"]),
        )

    def save_config(self, value: Mapping[str, Any]) -> dict[str, Any]:
        if self.fail:
            raise RuntimeError("fixture washing repository failure")
        copied = deepcopy(dict(value))
        self.saved_configs.append(copied)
        return deepcopy(FIXTURE["config_post"]["saved"])

    def load_recalculate_state(self) -> dict[str, Any]:
        return {"revision": 0, "media_types": {}}

    def acknowledge_recalculate_scope(self, expected_revision: int, processed_scope: object) -> bool:
        return bool(expected_revision and processed_scope)


def _native_client(
    *,
    settings_values: Mapping[str, Any] | None = None,
    auth_enabled: bool = False,
) -> tuple[TestClient, FakeSettings, FakeWashingRepository]:
    settings = FakeSettings(settings_values)
    repository = FakeWashingRepository()
    service = P115WashingPriorityService(settings=settings, repository=repository)
    container = AppContainer(
        Settings(auth_enabled=auth_enabled),
        repository_providers={"identity": lambda _: FakePermissions()},
    )
    app = FastAPI()
    app.state.container = container
    app.state.p115_washing_priority_service = service
    app.add_middleware(
        CookieSessionMiddleware,
        codec_provider=lambda: LegacySessionCodec.from_secret_key(b"p11-p115-washing-priority"),
    )
    app.include_router(router)
    return TestClient(app), settings, repository


def _legacy_app() -> Flask:
    app = Flask("p11-p115-washing-priority-legacy")
    app.secret_key = "fixture-only"
    app.register_blueprint(legacy_routes.p115_bp)
    return app


def _snapshot(response: Any) -> dict[str, Any]:
    content_type = str(response.headers.get("content-type") or "")
    if content_type.startswith("application/json"):
        body = response.get_json() if hasattr(response, "get_json") else response.json()
    else:
        body = response.get_data(as_text=True) if hasattr(response, "get_data") else response.text
    return {"status": response.status_code, "content_type": content_type, "body": body}


def _json_snapshot(response: Any) -> dict[str, Any]:
    snapshot = _snapshot(response)
    return {"status": snapshot["status"], "body": snapshot["body"]}


class _LegacyGroupsCursor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def __enter__(self) -> _LegacyGroupsCursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, statement: str, _parameters: object = None) -> None:
        assert "SELECT * FROM washing_priority_groups" in statement

    def fetchall(self) -> list[dict[str, Any]]:
        return deepcopy(self._rows)


class _LegacyGroupsConnection:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def __enter__(self) -> _LegacyGroupsConnection:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def cursor(self) -> _LegacyGroupsCursor:
        return _LegacyGroupsCursor(self._rows)


def test_washing_policy_normalization_matches_legacy(monkeypatch: pytest.MonkeyPatch) -> None:
    config_case = FIXTURE["config_get"]
    values = {
        P115_WASHING_PRIORITY_CONFIG_SETTING: deepcopy(config_case["stored"]),
        "p115_rename_config": deepcopy(config_case["legacy_rename"]),
    }
    legacy_saves: list[list[Any]] = []
    monkeypatch.setattr(legacy_settings, "get_setting", lambda key: deepcopy(values.get(key)))
    monkeypatch.setattr(
        legacy_settings,
        "save_setting",
        lambda key, value: legacy_saves.append([key, deepcopy(value)]),
    )

    legacy_config = legacy_settings.get_washing_priority_config()
    native_config, migrated = effective_washing_priority_config(
        values[P115_WASHING_PRIORITY_CONFIG_SETTING],
        values["p115_rename_config"],
    )

    assert migrated == {"skip_scope": "all", "conflict_mode": "keep_both"}
    assert native_config == legacy_config == config_case["response"]["body"]["data"]
    assert legacy_saves == [[P115_WASHING_PRIORITY_CONFIG_SETTING, migrated]]

    slots = [
        {
            "id": " Main 4K ",
            "name": "主版本",
            "fallback": 1,
            "match": {
                "resolution": "4k",
                "source": ["REMUX", "REMUX"],
                "clean_version": 1,
            },
        }
    ]
    assert normalize_washing_version_slots(slots) == legacy_settings._normalize_washing_version_slots(slots)

    old_groups = [
        {
            "media_type": "Movie",
            "target_cids": ["100"],
            "priorities": [{"resolution": ["4k", "1080p"], "_uid": "old"}],
        }
    ]
    new_groups = [
        {
            "media_type": "Movie",
            "target_cids": ["200"],
            "priorities": [{"resolution": ["1080p", "4k"], "_uid": "new"}],
        }
    ]
    assert build_washing_priority_recalculate_scope(
        old_groups,
        new_groups,
    ) == legacy_settings.build_washing_priority_recalculate_scope(old_groups, new_groups)

    current_state = {
        "revision": 4,
        "media_types": {"Movie": {"all": False, "target_cids": ["100"]}},
    }
    added_scope = {"Series": {"all": True, "target_cids": ["ignored"]}}
    assert merge_washing_priority_recalculate_state(
        current_state,
        added_scope,
    ) == legacy_settings.merge_washing_priority_recalculate_state(current_state, added_scope)
    assert normalize_washing_priority_recalculate_scope(
        current_state,
    ) == legacy_settings.normalize_washing_priority_recalculate_scope(current_state)


def test_native_and_isolated_legacy_match_washing_http_golden(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_case = FIXTURE["config_get"]
    values = {
        P115_WASHING_PRIORITY_CONFIG_SETTING: deepcopy(config_case["stored"]),
        "p115_rename_config": deepcopy(config_case["legacy_rename"]),
    }
    monkeypatch.setitem(config_manager.APP_CONFIG, "auth_enabled", False)
    monkeypatch.setattr(legacy_settings, "get_setting", lambda key: deepcopy(values.get(key)))
    monkeypatch.setattr(legacy_settings, "save_setting", lambda _key, _value: None)
    monkeypatch.setattr(
        legacy_settings,
        "save_washing_priority_config",
        lambda _payload: deepcopy(FIXTURE["config_post"]["saved"]),
    )
    monkeypatch.setattr(
        legacy_connection,
        "get_db_connection",
        lambda: _LegacyGroupsConnection(FIXTURE["groups_get"]["rows"]),
    )
    legacy_replacements: list[list[Any]] = []

    def replace_legacy(groups: list[Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        legacy_replacements.append(deepcopy(groups))
        return (
            deepcopy(FIXTURE["groups_post"]["affected_scope"]),
            deepcopy(FIXTURE["groups_post"]["pending_scope"]),
        )

    monkeypatch.setattr(legacy_routes, "_replace_washing_priority_groups", replace_legacy)
    native, native_settings, native_repository = _native_client(settings_values=values)

    with _legacy_app().test_client() as legacy, native:
        legacy_config_get = legacy.get("/api/p115/washing_priority_config")
        native_config_get = native.get("/api/p115/washing_priority_config")
        legacy_config_post = legacy.post(
            "/api/p115/washing_priority_config",
            json=FIXTURE["config_post"]["request"],
        )
        native_config_post = native.post(
            "/api/p115/washing_priority_config",
            json=FIXTURE["config_post"]["request"],
        )
        legacy_groups_get = legacy.get("/api/p115/washing_priority_groups")
        native_groups_get = native.get("/api/p115/washing_priority_groups")
        legacy_groups_post = legacy.post(
            "/api/p115/washing_priority_groups",
            json=FIXTURE["groups_post"]["request"],
        )
        native_groups_post = native.post(
            "/api/p115/washing_priority_groups",
            json=FIXTURE["groups_post"]["request"],
        )

    assert _json_snapshot(legacy_config_get) == _json_snapshot(native_config_get) == config_case["response"]
    assert (
        _json_snapshot(legacy_config_post) == _json_snapshot(native_config_post) == FIXTURE["config_post"]["response"]
    )
    assert _json_snapshot(legacy_groups_get) == _json_snapshot(native_groups_get) == FIXTURE["groups_get"]["response"]
    assert (
        _json_snapshot(legacy_groups_post) == _json_snapshot(native_groups_post) == FIXTURE["groups_post"]["response"]
    )
    assert native_settings.saves == [
        [
            P115_WASHING_PRIORITY_CONFIG_SETTING,
            {"skip_scope": "all", "conflict_mode": "keep_both"},
        ]
    ]
    assert native_repository.saved_configs == [FIXTURE["config_post"]["request"]]
    assert native_repository.replacements == [FIXTURE["groups_post"]["request"]]
    assert legacy_replacements == [FIXTURE["groups_post"]["request"]]


@pytest.mark.parametrize("path", ["/api/p115/washing_priority_config", "/api/p115/washing_priority_groups"])
def test_washing_json_transport_matches_legacy(path: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(config_manager.APP_CONFIG, "auth_enabled", False)
    native, _settings, repository = _native_client()
    with _legacy_app().test_client() as legacy, native:
        for content, content_type in (("{}", "text/plain"), ("{", "application/json"), ("", "application/json")):
            legacy_response = legacy.post(path, data=content, content_type=content_type)
            native_response = native.post(path, content=content, headers={"content-type": content_type})
            assert _snapshot(native_response) == _snapshot(legacy_response)
    assert repository.saved_configs == []
    assert repository.replacements == []


def test_washing_invalid_groups_failures_and_admin_denial_match_legacy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(config_manager.APP_CONFIG, "auth_enabled", False)
    native, native_settings, repository = _native_client()
    with _legacy_app().test_client() as legacy, native:
        legacy_invalid = legacy.post("/api/p115/washing_priority_groups", json={})
        native_invalid = native.post("/api/p115/washing_priority_groups", json={})
    assert (
        _json_snapshot(native_invalid)
        == _json_snapshot(legacy_invalid)
        == {
            "status": 400,
            "body": {"success": False, "message": "规则组数据格式错误"},
        }
    )

    def fail_settings(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("fixture settings load failure")

    def fail_repository(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("fixture washing repository failure")

    monkeypatch.setattr(legacy_settings, "get_washing_priority_config", fail_settings)
    monkeypatch.setattr(legacy_routes, "_replace_washing_priority_groups", fail_repository)
    native_settings.fail_load = True
    repository.fail = True
    with _legacy_app().test_client() as legacy, native:
        legacy_config_failure = legacy.get("/api/p115/washing_priority_config")
        native_config_failure = native.get("/api/p115/washing_priority_config")
        legacy_group_failure = legacy.post("/api/p115/washing_priority_groups", json=[])
        native_group_failure = native.post("/api/p115/washing_priority_groups", json=[])
    assert _json_snapshot(native_config_failure) == _json_snapshot(legacy_config_failure)
    assert _json_snapshot(native_group_failure) == _json_snapshot(legacy_group_failure)

    denied, denied_settings, denied_repository = _native_client(auth_enabled=True)
    denied_settings.fail_load = True
    denied_repository.fail = True
    with denied:
        denied_config = denied.post(
            "/api/p115/washing_priority_config",
            content="{",
            headers={"content-type": "application/json"},
        )
        denied_groups = denied.post(
            "/api/p115/washing_priority_groups",
            content="{",
            headers={"content-type": "application/json"},
        )
    assert denied_config.status_code == denied_groups.status_code == 403
    assert denied_repository.saved_configs == []
    assert denied_repository.replacements == []


def test_washing_source_ownership_and_openapi_are_native() -> None:
    fixture_routes = {(item["method"], item["path"]) for item in FIXTURE["routes"]}
    assert fixture_routes == {
        ("GET", "/api/p115/washing_priority_config"),
        ("POST", "/api/p115/washing_priority_config"),
        ("GET", "/api/p115/washing_priority_groups"),
        ("POST", "/api/p115/washing_priority_groups"),
        ("POST", "/api/p115/washing_priority_recalculate"),
    }
    assert FIXTURE["provenance"]["captured_live"] is False
    assert FIXTURE["provenance"]["external_systems_used"] is False

    for source in (SERVICE_SOURCE, REPOSITORY_SOURCE):
        tree = ast.parse(source.read_text(encoding="utf-8"))
        imported_roots = {
            alias.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
        }
        imported_roots.update(
            node.module.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module
        )
        assert imported_roots.isdisjoint(
            {"config_manager", "database", "handler", "requests", "routes", "tasks", "utils"}
        )

    routes = json.loads(ROUTE_OWNERSHIP.read_text(encoding="utf-8"))
    bindings = {item["contract_id"]: item for item in routes["method_bindings"]}
    for contract_id in (
        "route-binding-1ef0aaa109e4",
        "route-binding-d1ae38f99702",
        "route-binding-46c84ecff732",
        "route-binding-99be52902613",
    ):
        contract = bindings[contract_id]
        assert contract["status"] == "new"
        assert contract["new_owner"] == "etk.domains.p115.router"
        assert contract["fixture"] == "verification/fixtures/http/p11-p115-washing-priority.json"

    config = json.loads(CONFIG_OWNERSHIP.read_text(encoding="utf-8"))
    settings = {item["key"]: item for item in config["standalone_app_settings"]}
    for key in (
        P115_WASHING_PRIORITY_CONFIG_SETTING,
        "p115_washing_priority_recalculate_state",
    ):
        assert settings[key]["status"] == "new"
        assert settings[key]["validation_owner"] == "p115/washing-priority"

    tables = json.loads(TABLE_OWNERSHIP.read_text(encoding="utf-8"))
    table = next(item for item in tables["tables"] if item["name"] == "washing_priority_groups")
    assert table["status"] == "new"
    assert table["new_owner"] == "etk.domains.p115.washing_priority_repository.SqlAlchemyP115WashingPriorityRepository"

    openapi = json.loads(OPENAPI.read_text(encoding="utf-8"))
    for path in (
        "/api/p115/washing_priority_config",
        "/api/p115/washing_priority_groups",
    ):
        assert {"get", "post"} <= set(openapi["paths"][path])
