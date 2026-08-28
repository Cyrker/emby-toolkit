from __future__ import annotations

import ast
import json
from collections.abc import Sequence
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from flask import Flask

import config_manager
from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.p115.local_config_service import (
    P115_DEFAULT_STREAM_CONFIG_SETTING,
    RELEASE_GROUP_MAPPING_SETTING,
    P115LocalSettingsService,
)
from etk.domains.p115.media_info_adapters import PersistentP115MediaInfoPolicyProvider
from etk.domains.p115.router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware
from routes import p115 as legacy_routes
from tasks import helpers

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_PATH = REPO_ROOT / "verification/fixtures/http/p11-p115-local-settings.json"
ROUTE_OWNERSHIP = REPO_ROOT / "verification/contracts/route-ownership.yaml"
CONFIG_OWNERSHIP = REPO_ROOT / "verification/contracts/config-ownership.yaml"
OPENAPI = REPO_ROOT / "verification/contracts/http/p4-openapi.json"
LOCAL_SETTINGS_SOURCE = REPO_ROOT / "src/etk/domains/p115/local_config_service.py"
WORKER_SOURCE = REPO_ROOT / "src/etk/processes/worker.py"
FIXTURE = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


class FakePermissions:
    def is_admin(self, user_id: str) -> bool:
        return user_id == "fixture-admin"


class FakeRepository:
    def __init__(self, values: dict[str, Any] | None = None) -> None:
        self.values = deepcopy(values or {})
        self.saves: list[list[Any]] = []
        self.fail_load = False
        self.fail_save = False

    def load(self, key: str) -> Any:
        if self.fail_load:
            raise RuntimeError("fixture load failure")
        return deepcopy(self.values.get(key))

    def save(self, key: str, value: Any) -> None:
        if self.fail_save:
            raise RuntimeError("fixture save failure")
        copied = deepcopy(value)
        self.values[key] = copied
        self.saves.append([key, copied])

    def load_many(self, keys: Sequence[str]) -> dict[str, Any]:
        if self.fail_load:
            raise RuntimeError("fixture load failure")
        return {key: deepcopy(self.values.get(key)) for key in keys}

    def delete(self, key: str) -> bool:
        return self.values.pop(key, None) is not None


def _native_client(
    values: dict[str, Any] | None = None,
    *,
    auth_enabled: bool = False,
) -> tuple[TestClient, FakeRepository]:
    repository = FakeRepository(values)
    container = AppContainer(
        Settings(auth_enabled=auth_enabled),
        repository_providers={"identity": lambda _: FakePermissions()},
    )
    app = FastAPI()
    app.state.container = container
    app.state.p115_local_settings_service = P115LocalSettingsService(repository)
    app.add_middleware(
        CookieSessionMiddleware,
        codec_provider=lambda: LegacySessionCodec.from_secret_key(b"p11-p115-local-settings"),
    )
    app.include_router(router)
    return TestClient(app), repository


def _legacy_app() -> Flask:
    app = Flask("p11-p115-local-settings-legacy")
    app.secret_key = "fixture-only"
    app.register_blueprint(legacy_routes.p115_bp)
    return app


def _snapshot(response: Any) -> dict[str, Any]:
    return {
        "status": response.status_code,
        "body": response.get_json() if hasattr(response, "get_json") else response.json(),
    }


@pytest.mark.parametrize(
    "case",
    FIXTURE["stream_cases"],
    ids=[case["case_id"] for case in FIXTURE["stream_cases"]],
)
def test_native_and_isolated_legacy_match_default_stream_config_golden(
    case: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = deepcopy(case["stored"])
    legacy_saves: list[list[Any]] = []
    monkeypatch.setitem(config_manager.APP_CONFIG, "auth_enabled", False)
    monkeypatch.setattr(legacy_routes.settings_db, "get_setting", lambda _key: deepcopy(stored))
    monkeypatch.setattr(
        legacy_routes.settings_db,
        "save_setting",
        lambda key, value: legacy_saves.append([key, deepcopy(value)]),
    )
    native, native_repository = _native_client({P115_DEFAULT_STREAM_CONFIG_SETTING: stored})
    request = case["request"]
    request_kwargs = {"json": request["body"]} if "body" in request else {}

    with _legacy_app().test_client() as legacy, native:
        legacy_response = legacy.open(
            request["path"],
            method=request["method"],
            **request_kwargs,
        )
        native_response = native.request(
            request["method"],
            request["path"],
            **request_kwargs,
        )

    assert _snapshot(legacy_response) == case["response"]
    assert _snapshot(native_response) == case["response"]
    assert legacy_saves == case.get("expected_saves", [])
    assert native_repository.saves == case.get("expected_saves", [])


@pytest.mark.parametrize(
    "case",
    FIXTURE["release_group_cases"],
    ids=[case["case_id"] for case in FIXTURE["release_group_cases"]],
)
def test_native_and_isolated_legacy_match_release_group_golden(
    case: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = deepcopy(case["stored"])
    monkeypatch.setitem(config_manager.APP_CONFIG, "auth_enabled", False)
    monkeypatch.setattr(helpers.settings_db, "get_setting", lambda _key: deepcopy(stored))
    native, native_repository = _native_client({RELEASE_GROUP_MAPPING_SETTING: stored})

    with _legacy_app().test_client() as legacy, native:
        legacy_response = legacy.get(case["request"]["path"])
        native_response = native.get(case["request"]["path"])

    assert _snapshot(legacy_response) == case["response"]
    assert _snapshot(native_response) == case["response"]
    assert native_repository.saves == []


def test_local_settings_failures_and_admin_denial_are_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("fixture settings failure")

    monkeypatch.setitem(config_manager.APP_CONFIG, "auth_enabled", False)
    monkeypatch.setattr(legacy_routes.settings_db, "get_setting", fail)
    monkeypatch.setattr(legacy_routes.settings_db, "save_setting", fail)
    with _legacy_app().test_client() as legacy:
        legacy_stream_failure = legacy.get("/api/p115/default_stream_config")
        legacy_stream_save_failure = legacy.post("/api/p115/default_stream_config", json={})
        legacy_release_fallback = legacy.get("/api/p115/release_groups")

    client, repository = _native_client()
    repository.fail_load = True
    with client:
        stream_failure = client.get("/api/p115/default_stream_config")
        release_fallback = client.get("/api/p115/release_groups")
        repository.fail_load = False
        repository.fail_save = True
        stream_save_failure = client.post("/api/p115/default_stream_config", json={})

    assert stream_failure.status_code == legacy_stream_failure.status_code == 500
    assert stream_failure.headers["content-type"].startswith("text/html")
    assert legacy_stream_failure.headers["content-type"].startswith("text/html")
    assert stream_failure.text == legacy_stream_failure.get_data(as_text=True)
    assert stream_save_failure.status_code == legacy_stream_save_failure.status_code == 500
    assert stream_save_failure.headers["content-type"].startswith("text/html")
    assert legacy_stream_save_failure.headers["content-type"].startswith("text/html")
    assert stream_save_failure.text == legacy_stream_save_failure.get_data(as_text=True)
    assert _snapshot(release_fallback) == _snapshot(legacy_release_fallback)

    denied, denied_repository = _native_client(auth_enabled=True)
    denied_repository.fail_load = True
    denied_repository.fail_save = True
    with denied:
        get_response = denied.get("/api/p115/default_stream_config")
        post_response = denied.post("/api/p115/default_stream_config", json={})

    assert get_response.status_code == 403
    assert post_response.status_code == 403
    assert denied_repository.saves == []


def test_default_stream_transport_preserves_legacy_json_errors() -> None:
    client, repository = _native_client()
    with client:
        unsupported = client.post(
            "/api/p115/default_stream_config",
            content="{}",
            headers={"content-type": "text/plain"},
        )
        malformed = client.post(
            "/api/p115/default_stream_config",
            content="{",
            headers={"content-type": "application/json"},
        )

    assert unsupported.status_code == 415
    assert unsupported.headers["content-type"].startswith("text/html")
    assert malformed.status_code == 400
    assert malformed.headers["content-type"].startswith("text/html")
    assert repository.saves == []


def test_media_info_policy_snapshot_reads_the_native_settings_repository() -> None:
    repository = FakeRepository(
        {
            "language_mapping": [
                {"label": "英语", "value": "en", "aliases": ["eng", "EN"]},
            ],
            P115_DEFAULT_STREAM_CONFIG_SETTING: {
                "audio_lang_priority": ["ENG"],
                "subtitle_lang_priority": ["CHT"],
                "audio_priority_order": ["feature"],
            },
        }
    )

    snapshot = PersistentP115MediaInfoPolicyProvider(repository).snapshot()

    assert snapshot.language_mapping[0].value == "en"
    assert snapshot.language_mapping[0].aliases == ("eng", "en")
    assert snapshot.default_streams.audio_language_priority == ("eng",)
    assert snapshot.default_streams.subtitle_language_priority == ("cht",)
    assert snapshot.default_streams.audio_priority_order == ("feature", "param")
    assert snapshot.language_mapping_revision
    assert snapshot.default_stream_config_revision
    assert repository.saves == []
    worker_source = WORKER_SOURCE.read_text(encoding="utf-8")
    assert "PersistentP115MediaInfoPolicyProvider(app_settings)" in worker_source
    assert "LegacyP115MediaInfoPolicyProvider" not in worker_source


def test_local_settings_source_and_ownership_are_native() -> None:
    fixture_routes = {(item["method"], item["path"]) for item in FIXTURE["routes"]}
    assert fixture_routes == {
        ("GET", "/api/p115/default_stream_config"),
        ("POST", "/api/p115/default_stream_config"),
        ("GET", "/api/p115/release_groups"),
    }
    assert FIXTURE["provenance"]["captured_live"] is False
    assert FIXTURE["provenance"]["external_systems_used"] is False

    tree = ast.parse(LOCAL_SETTINGS_SOURCE.read_text(encoding="utf-8"))
    imported_roots = {
        alias.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
    }
    imported_roots.update(
        node.module.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module
    )
    assert imported_roots.isdisjoint({"config_manager", "database", "handler", "requests", "routes", "tasks", "utils"})

    routes = json.loads(ROUTE_OWNERSHIP.read_text(encoding="utf-8"))
    bindings = {item["contract_id"]: item for item in routes["method_bindings"]}
    for contract_id in (
        "route-binding-aadccd657fc9",
        "route-binding-f70c38c28fa0",
        "route-binding-15bb5175513f",
    ):
        contract = bindings[contract_id]
        assert contract["status"] == "new"
        assert contract["new_owner"] == "etk.domains.p115.router"
        assert contract["fixture"] == "verification/fixtures/http/p11-p115-local-settings.json"

    config = json.loads(CONFIG_OWNERSHIP.read_text(encoding="utf-8"))
    settings = {item["key"]: item for item in config["standalone_app_settings"]}
    contract = settings[P115_DEFAULT_STREAM_CONFIG_SETTING]
    assert contract["status"] == "new"
    assert contract["new_owner"] == "etk.domains.system.repository.SqlAlchemyAppSettingsRepository"
    assert contract["validation_owner"] == "p115/local-settings"

    openapi = json.loads(OPENAPI.read_text(encoding="utf-8"))
    paths = openapi["paths"]
    assert {"get", "post"} <= set(paths["/api/p115/default_stream_config"])
    assert {"get"} <= set(paths["/api/p115/release_groups"])
    post_responses = paths["/api/p115/default_stream_config"]["post"]["responses"]
    for status in ("400", "415", "500"):
        assert "text/html" in post_responses[status]["content"]
