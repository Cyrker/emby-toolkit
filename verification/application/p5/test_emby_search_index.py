from __future__ import annotations

import ast
import json
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from etk.bootstrap.app_factory import create_app
from etk.domains.media.router import router
from etk.domains.media.search_index import EmbySearchIndexService, EmbySearchRepository
from etk.platform import AppContainer, Settings

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = REPO_ROOT / "verification" / "fixtures" / "http" / "p11-emby-search.json"
ROUTE_MANIFEST = REPO_ROOT / "verification" / "contracts" / "route-ownership.yaml"
TABLE_MANIFEST = REPO_ROOT / "verification" / "contracts" / "table-ownership.yaml"
SOURCE = REPO_ROOT / "src" / "etk" / "domains" / "media" / "search_index.py"
SEARCH_ROUTES = {
    ("GET", "/api/emby/search"),
    ("POST", "/api/emby/search/index"),
}


class FakeSearchRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.rows: list[dict[str, Any]] = []
        self.search_result: dict[str, Any] = {"ready": True, "items": []}
        self.complete_deleted = 0
        self.fail = False

    def _check(self) -> None:
        if self.fail:
            raise RuntimeError("fixture search repository failure")

    def start_rebuild(self, token: str) -> None:
        self._check()
        self.calls.append(("start", token))

    def upsert_items(self, rows: Sequence[Mapping[str, Any]]) -> int:
        self._check()
        self.rows = [dict(row) for row in rows]
        self.calls.append(("upsert", len(self.rows)))
        return len(self.rows)

    def complete_rebuild(self, token: str) -> int:
        self._check()
        self.calls.append(("complete", token))
        return self.complete_deleted

    def abort_rebuild(self, token: str) -> None:
        self._check()
        self.calls.append(("abort", token))

    def delete_items(self, item_ids: Iterable[object]) -> int:
        self._check()
        values = list(item_ids)
        self.calls.append(("delete", values))
        return len(values)

    def search(self, query: str, item_types: Sequence[str], limit: int) -> dict[str, Any]:
        self._check()
        self.calls.append(("search", (query, list(item_types), limit)))
        return self.search_result


def _app(repository: FakeSearchRepository) -> FastAPI:
    service = EmbySearchIndexService(cast(EmbySearchRepository, repository))
    container = AppContainer(
        Settings(),
        use_case_providers={"media.search-index": lambda _: service},
    )
    app = FastAPI()
    app.state.container = container
    app.include_router(router)
    return app


def test_fixture_native_scope_and_dependencies() -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert fixture["provenance"]["captured_live"] is False
    assert {(item["method"], item["path"]) for item in fixture["routes"]} == SEARCH_ROUTES

    observed = {
        (method, route.path)
        for route in router.routes
        if isinstance(route, APIRoute) and route.path in {path for _method, path in SEARCH_ROUTES}
        for method in route.methods or set()
    }
    assert observed == SEARCH_ROUTES

    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert imported.isdisjoint({"config_manager", "constants", "extensions", "task_manager", "routes"})


def test_fixture_cases() -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    for case in fixture["cases"]:
        repository = FakeSearchRepository()
        repository.fail = bool(case.get("repository", {}).get("fail", False))
        repository.complete_deleted = int(case.get("repository", {}).get("complete_deleted", 0))
        request = case["request"]
        body = request.get("body")
        if request.get("token_repeat"):
            body = dict(body or {}, token="x" * int(request["token_repeat"]))
        with TestClient(_app(repository)) as client:
            response = client.request(
                request["method"],
                request["path"],
                params=request.get("query"),
                json=body,
            )
        assert response.status_code == case["response"]["status"], case["case_id"]
        assert response.json() == case["response"]["body"], case["case_id"]


def test_http_search_contract() -> None:
    repository = FakeSearchRepository()
    repository.search_result = {
        "ready": True,
        "items": [{"id": 7, "type": "Series", "title": "甄嬛传", "rank": 997}],
    }
    app = _app(repository)

    with TestClient(app) as client:
        empty = client.get("/api/emby/search")
        assert empty.status_code == 200
        assert empty.json() == {"ready": True, "items": []}
        assert "no-store" not in empty.headers.get("cache-control", "")

        result = client.get(
            "/api/emby/search",
            params={"query": "甄嬛传", "types": "Series, Episode", "limit": "invalid"},
        )
        assert result.status_code == 200
        assert result.json() == repository.search_result
        assert result.headers["cache-control"] == "no-store"

    assert repository.calls == [("search", ("甄嬛传", ["Series", "Episode"], 300))]


def test_index_lifecycle_validation_normalization_and_fixture_cases() -> None:
    repository = FakeSearchRepository()
    repository.complete_deleted = 4
    app = _app(repository)

    with TestClient(app) as client:
        assert client.post("/api/emby/search/index", json={"mode": "start", "token": "build-1"}).json() == {"ok": True}
        batch = client.post(
            "/api/emby/search/index",
            json={
                "mode": "batch",
                "items": [
                    {
                        "id": 7,
                        "type": "Series",
                        "title": "甄嬛传",
                        "original_title": "Empresses in the Palace",
                    },
                    {"id": "invalid", "type": "Movie", "title": "Ignored"},
                ],
            },
        )
        assert batch.json() == {"ok": True, "indexed": 1}
        assert repository.rows[0]["emby_item_id"] == 7
        assert repository.rows[0]["search_compact"] == "甄嬛传empressesinthepalace"
        assert "zhz" in repository.rows[0]["pinyin_initials"].split("|")
        assert repository.rows[0]["search_ngrams"]
        assert repository.rows[0]["pinyin_ngrams"]

        incremental = client.post(
            "/api/emby/search/index",
            json={"mode": "incremental", "items": [{"id": 8, "type": "Movie", "title": "Fixture"}]},
        )
        assert incremental.json() == {"ok": True, "indexed": 1}

        complete = client.post("/api/emby/search/index", json={"mode": "complete", "token": "build-1"})
        assert complete.json() == {"ok": True, "deleted": 4}
        assert client.post("/api/emby/search/index", json={"mode": "abort", "token": "build-2"}).json() == {"ok": True}
        deleted = client.post(
            "/api/emby/search/index",
            json={"mode": "delete", "items": [{"id": 7}, {"id": 8}, "ignored"]},
        )
        assert deleted.json() == {"ok": True, "deleted": 2}
        long_token = client.post(
            "/api/emby/search/index",
            json={"mode": "start", "token": "x" * 101},
        )
        assert long_token.status_code == 400
        assert long_token.json() == {"ok": False, "error": "invalid token"}
        assert client.post("/api/emby/search/index", json={"mode": "start"}).json() == {
            "ok": False,
            "error": "token is required",
        }
        assert client.post("/api/emby/search/index", json={"mode": "complete"}).json() == {
            "ok": False,
            "error": "token is required",
        }
        assert client.post("/api/emby/search/index", json={"mode": "unknown"}).json() == {
            "ok": False,
            "error": "invalid mode",
        }
        invalid_items = client.post("/api/emby/search/index", json={"mode": "batch", "items": "invalid"})
        assert invalid_items.status_code == 400
        assert invalid_items.json() == {"ok": False, "error": "invalid items"}

    assert repository.calls == [
        ("start", "build-1"),
        ("upsert", 1),
        ("upsert", 1),
        ("complete", "build-1"),
        ("abort", "build-2"),
        ("delete", [7, 8]),
    ]


def test_repository_failures_preserve_503_envelopes() -> None:
    repository = FakeSearchRepository()
    repository.fail = True
    app = _app(repository)

    with TestClient(app) as client:
        searched = client.get("/api/emby/search", params={"query": "fixture"})
        updated = client.post("/api/emby/search/index", json={"mode": "batch", "items": []})

    assert searched.status_code == 503
    assert searched.json() == {"ready": False, "items": []}
    assert updated.status_code == 503
    assert updated.json() == {"ok": False, "error": "search index update failed"}


def test_search_ownership_is_fixture_backed_and_precedes_compatibility_mount(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    routes = json.loads(ROUTE_MANIFEST.read_text(encoding="utf-8"))
    entries = [
        entry
        for group in (routes["method_bindings"], routes["routes"])
        for entry in group
        if (entry.get("method"), entry["path"]) in SEARCH_ROUTES
        or ("method" not in entry and entry["path"] in {path for _method, path in SEARCH_ROUTES})
    ]
    assert len(entries) == 4
    for entry in entries:
        assert entry["status"] == "new"
        assert entry["new_owner"] == "etk.domains.media.router"
        assert entry["fixture"] == "verification/fixtures/http/p11-emby-search.json"
        assert entry["fixture_exception"] is None
        assert entry["validation_owner"] == "media/search-index"
        assert entry["validation_method"] == "pytest-http-fixture-and-postgres18-search-index-contract"

    tables = json.loads(TABLE_MANIFEST.read_text(encoding="utf-8"))
    table_entries = {entry["name"]: entry for entry in tables["tables"]}
    for name in ("emby_search_index", "emby_search_rebuild_state"):
        entry = table_entries[name]
        assert entry["status"] == "new"
        assert entry["new_owner"] == "etk.domains.media.search_index.SqlAlchemyEmbySearchRepository"
        assert entry["fixture"] == "verification/integration/p5/test_emby_search_index_postgres18.py"

    monkeypatch.setenv("ETK_ENABLE_LEGACY_COMPATIBILITY", "1")
    app = create_app(start_runtime=False)
    try:
        compatibility_index = next(
            index for index, route in enumerate(app.routes) if getattr(route, "name", None) == "legacy-compatibility"
        )
        native_routes = [
            (index, nested)
            for index, included in enumerate(app.routes)
            for nested in getattr(getattr(included, "original_router", None), "routes", ())
            if isinstance(nested, APIRoute) and nested.path in {path for _method, path in SEARCH_ROUTES}
        ]
        assert len(native_routes) == 2
        assert all(index < compatibility_index for index, _route in native_routes)
    finally:
        app.state.container.close()
