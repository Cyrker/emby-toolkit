from __future__ import annotations

import ast
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from flask import Flask

import config_manager
import etk.domains.p115.local_directories as local_directories_module
from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.p115.local_directories import (
    OsP115LocalDirectoryBrowser,
    P115LocalDirectoryListing,
    P115LocalDirectoryNotFound,
    P115LocalDirectoryService,
)
from etk.domains.p115.router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware
from routes import p115 as legacy_routes

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_PATH = REPO_ROOT / "verification/fixtures/http/p11-p115-local-directories.json"
ROUTE_OWNERSHIP = REPO_ROOT / "verification/contracts/route-ownership.yaml"
OPENAPI = REPO_ROOT / "verification/contracts/http/p4-openapi.json"
SOURCE = REPO_ROOT / "src/etk/domains/p115/local_directories.py"
APP_FACTORY = REPO_ROOT / "src/etk/bootstrap/app_factory.py"
FIXTURE = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


class FakePermissions:
    def is_admin(self, user_id: str) -> bool:
        return user_id == "fixture-admin"


class FakeBrowser:
    def __init__(self, case: dict[str, Any]) -> None:
        self.case = deepcopy(case)
        self.calls: list[str] = []

    def browse(self, path: str) -> P115LocalDirectoryListing:
        self.calls.append(path)
        behavior = self.case["browser"]
        if behavior == "not-found":
            raise P115LocalDirectoryNotFound(path)
        if behavior == "permission-denied":
            raise PermissionError(path)
        if behavior == "filesystem-failure":
            raise RuntimeError("fixture filesystem failure")
        body = self.case["response"]["body"]
        return P115LocalDirectoryListing(
            current_path=body["current_path"],
            entries=tuple(deepcopy(body["data"])),
        )


def _native_client(
    case: dict[str, Any],
    *,
    auth_enabled: bool = False,
) -> tuple[TestClient, FakeBrowser]:
    browser = FakeBrowser(case)
    container = AppContainer(
        Settings(auth_enabled=auth_enabled),
        repository_providers={"identity": lambda _: FakePermissions()},
    )
    app = FastAPI()
    app.state.container = container
    app.state.p115_local_directory_service = P115LocalDirectoryService(browser)
    app.add_middleware(
        CookieSessionMiddleware,
        codec_provider=lambda: LegacySessionCodec.from_secret_key(b"p11-p115-local-directories"),
    )
    app.include_router(router)
    return TestClient(app), browser


def _legacy_app() -> Flask:
    app = Flask("p11-p115-local-directories-legacy")
    app.secret_key = "fixture-only"
    app.register_blueprint(legacy_routes.p115_bp)
    return app


def _snapshot(response: Any) -> dict[str, Any]:
    return {
        "status": response.status_code,
        "body": response.get_json() if hasattr(response, "get_json") else response.json(),
    }


def _stub_legacy_filesystem(case: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    behavior = case["browser"]

    def exists(path: str) -> bool:
        if behavior == "not-found":
            return False
        if behavior == "permission-denied":
            raise PermissionError(path)
        if behavior == "filesystem-failure":
            raise RuntimeError("fixture filesystem failure")
        return path in {
            "/",
            "/fixture/root",
            "/fixture/root/Beta",
            "/fixture/root/alpha",
            "/media",
        }

    def is_directory(path: str) -> bool:
        return path in {
            "/",
            "/fixture/root",
            "/fixture/root/Beta",
            "/fixture/root/alpha",
            "/media",
        }

    def list_directory(path: str) -> list[str]:
        if path == "/fixture/root":
            return ["alpha", "file.txt", ".hidden", "Beta"]
        if path == "/":
            return ["media", ".hidden", "file.txt"]
        return []

    monkeypatch.setattr(legacy_routes.os.path, "exists", exists)
    monkeypatch.setattr(legacy_routes.os.path, "isdir", is_directory)
    monkeypatch.setattr(legacy_routes.os, "listdir", list_directory)


@pytest.mark.parametrize(
    "case",
    FIXTURE["cases"],
    ids=[case["case_id"] for case in FIXTURE["cases"]],
)
def test_native_and_isolated_legacy_match_local_directory_golden(
    case: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(config_manager.APP_CONFIG, "auth_enabled", False)
    _stub_legacy_filesystem(case, monkeypatch)
    native, browser = _native_client(case)
    request_path = case["request"]["path"]
    params = {"path": request_path} if request_path else {}

    with _legacy_app().test_client() as legacy, native:
        legacy_response = legacy.get("/api/p115/system/directories", query_string=params)
        native_response = native.get("/api/p115/system/directories", params=params)

    assert _snapshot(legacy_response) == case["response"]
    assert _snapshot(native_response) == case["response"]
    assert browser.calls == [request_path]


def test_empty_path_preserves_posix_root_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    case = {
        "browser": "default-root",
        "response": {
            "status": 200,
            "body": {
                "code": 200,
                "data": [{"name": "media", "path": "/media", "is_parent": False}],
                "current_path": "/",
            },
        },
    }
    monkeypatch.setitem(config_manager.APP_CONFIG, "auth_enabled", False)
    _stub_legacy_filesystem(case, monkeypatch)
    native, browser = _native_client(case)

    with _legacy_app().test_client() as legacy, native:
        legacy_response = legacy.get("/api/p115/system/directories")
        native_response = native.get("/api/p115/system/directories")

    assert _snapshot(native_response) == _snapshot(legacy_response) == case["response"]
    assert browser.calls == [""]


def test_empty_path_preserves_windows_drive_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeKernel32:
        @staticmethod
        def GetLogicalDrives() -> int:
            return (1 << 2) | (1 << 25)

    class FakeWindll:
        kernel32 = FakeKernel32()

    monkeypatch.setattr(local_directories_module.os, "name", "nt")
    monkeypatch.setattr(
        local_directories_module.ctypes,
        "windll",
        FakeWindll(),
        raising=False,
    )

    listing = OsP115LocalDirectoryBrowser().browse("")

    assert listing == P115LocalDirectoryListing(
        current_path="",
        entries=(
            {"name": "C:\\", "path": "C:\\", "is_parent": False},
            {"name": "Z:\\", "path": "Z:\\", "is_parent": False},
        ),
    )


def test_local_directory_admin_denial_precedes_filesystem_access() -> None:
    case = FIXTURE["cases"][0]
    client, browser = _native_client(case, auth_enabled=True)

    with client:
        response = client.get("/api/p115/system/directories", params={"path": "/fixture/root"})

    assert response.status_code == 403
    assert browser.calls == []


def test_os_local_directory_browser_lists_only_visible_directories(tmp_path: Path) -> None:
    (tmp_path / "Beta").mkdir()
    (tmp_path / "alpha").mkdir()
    (tmp_path / ".hidden").mkdir()
    (tmp_path / "file.txt").write_text("fixture", encoding="utf-8")

    listing = OsP115LocalDirectoryBrowser().browse(str(tmp_path))

    assert listing.current_path == str(tmp_path)
    assert listing.entries == (
        {
            "name": ".. [返回上一级]",
            "path": str(tmp_path.parent),
            "is_parent": True,
        },
        {
            "name": "Beta",
            "path": str(tmp_path / "Beta"),
            "is_parent": False,
        },
        {
            "name": "alpha",
            "path": str(tmp_path / "alpha"),
            "is_parent": False,
        },
    )
    with pytest.raises(P115LocalDirectoryNotFound):
        OsP115LocalDirectoryBrowser().browse(str(tmp_path / "missing"))


def test_local_directory_source_ownership_and_openapi_are_native() -> None:
    assert FIXTURE["routes"] == [{"method": "GET", "path": "/api/p115/system/directories"}]
    assert FIXTURE["provenance"]["captured_live"] is False
    assert FIXTURE["provenance"]["external_systems_used"] is False

    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    imported_roots = {
        alias.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
    }
    imported_roots.update(
        node.module.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module
    )
    assert imported_roots.isdisjoint(
        {"config_manager", "database", "handler", "requests", "routes", "tasks", "web_app"}
    )

    routes = json.loads(ROUTE_OWNERSHIP.read_text(encoding="utf-8"))
    binding = next(item for item in routes["method_bindings"] if item["contract_id"] == "route-binding-67ff0905738c")
    assert binding["status"] == "new"
    assert binding["new_owner"] == "etk.domains.p115.router"
    assert binding["fixture"] == "verification/fixtures/http/p11-p115-local-directories.json"

    factory_source = APP_FACTORY.read_text(encoding="utf-8")
    assert '"p115.local-directories"' in factory_source
    assert "P115LocalDirectoryService(OsP115LocalDirectoryBrowser())" in factory_source

    openapi = json.loads(OPENAPI.read_text(encoding="utf-8"))
    operation = openapi["paths"]["/api/p115/system/directories"]["get"]
    assert {parameter["name"] for parameter in operation["parameters"]} == {"path"}
    assert {"403", "404", "500"} <= set(operation["responses"])
