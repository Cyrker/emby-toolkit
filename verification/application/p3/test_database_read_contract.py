from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from flask import Flask
from starlette.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec, configure_legacy_session
from etk.domains.operations.database import DatabaseReadRepository, DatabaseReadService
from etk.domains.operations.database_router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
DATABASE_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "http" / "database-read-routes.json"
SECRET_KEY = b"p3-database-read-session-key"


class FakePermissionReader:
    def __init__(self) -> None:
        self.admins = {"fixture-admin"}

    def is_admin(self, user_id: str) -> bool:
        return user_id in self.admins


class FakeDatabaseReadRepository:
    def __init__(self) -> None:
        self.core: dict[str, Any] = {
            "media_cached_total": 12,
            "mediainfo_backed_up_total": 9,
            "mediainfo_hits_total": 34,
        }
        self.library: dict[str, Any] = {
            "media_movies_in_library": 3,
            "media_series_in_library": 2,
            "media_episodes_in_library": 8,
            "resolution_stats": [{"resolution": "4K", "count": 5}],
            "weekly_added_stats": [],
        }
        self.system: dict[str, Any] = {}
        self.release_groups: list[dict[str, Any]] = [{"release_group": "FixtureGroup", "count": 4}]
        self.historical_release_groups: list[dict[str, Any]] = []
        self.tables = ["failed_log", "media_metadata"]
        self.items: list[dict[str, Any]] = [
            {
                "item_id": "fixture-item-001",
                "item_name": "100%_Fixture'; SELECT",
                "failed_at": datetime(2026, 8, 23, 12, 0, 0),
                "reason": "缺失媒体信息: S01E01",
                "item_type": "Episode",
                "score": Decimal("8.50"),
            }
        ]
        self.total = 21
        self.reasons = ["处理评分", "缺失媒体信息", "自定义原因"]
        self.review_calls: list[dict[str, Any]] = []
        self.ranking_limits: list[int] = []
        self.error_method: str | None = None

    def _fail_if_requested(self, method: str) -> None:
        if self.error_method == method:
            raise RuntimeError(f"fixture {method} failure")

    def core_stats(self) -> Mapping[str, Any]:
        self._fail_if_requested("core")
        return self.core

    def library_stats(self) -> Mapping[str, Any]:
        self._fail_if_requested("library")
        return self.library

    def system_stats(self) -> Mapping[str, Any]:
        self._fail_if_requested("system")
        return self.system

    def release_group_rankings(self, limit: int) -> list[dict[str, Any]]:
        self._fail_if_requested("rankings")
        self.ranking_limits.append(limit)
        return self.release_groups

    def historical_release_group_rankings(self, limit: int) -> list[dict[str, Any]]:
        self._fail_if_requested("rankings")
        self.ranking_limits.append(limit)
        return self.historical_release_groups

    def table_names(self) -> list[str]:
        self._fail_if_requested("tables")
        return self.tables

    def review_items(
        self,
        *,
        page: int,
        per_page: int,
        query: str,
        reason: str,
    ) -> tuple[list[dict[str, Any]], int]:
        self._fail_if_requested("review_items")
        self.review_calls.append({"page": page, "per_page": per_page, "query": query, "reason": reason})
        return self.items, self.total

    def review_reasons(self) -> list[str]:
        self._fail_if_requested("review_reasons")
        return self.reasons


def _codec() -> LegacySessionCodec:
    flask_app = Flask("p3-database-read-cookie")
    configure_legacy_session(flask_app, Path("."), secret_key=SECRET_KEY)
    return LegacySessionCodec(flask_app)


def _build_app(
    repository: DatabaseReadRepository,
    *,
    auth_enabled: bool = True,
) -> tuple[FastAPI, LegacySessionCodec]:
    codec = _codec()
    service = DatabaseReadService(repository)
    permissions = FakePermissionReader()
    container = AppContainer(
        Settings(auth_enabled=auth_enabled),
        repository_providers={"identity": lambda _: permissions},
        use_case_providers={"operations.database.read": lambda _: service},
    )
    app = FastAPI()
    app.state.container = container
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return app, codec


def _fixture_cases() -> dict[str, dict[str, Any]]:
    payload = json.loads(DATABASE_FIXTURE.read_text(encoding="utf-8"))
    return {case["case_id"]: case for case in payload["cases"]}


def _assert_fixture_response(response, case_id: str) -> None:
    expected = _fixture_cases()[case_id]["response"]
    assert response.status_code == expected["status"]
    assert response.headers["content-type"] == "application/json"
    assert response.json() == expected["body"]


def _set_session(client: TestClient, codec: LegacySessionCodec, user_id: str) -> None:
    client.cookies.set(
        "session",
        codec.dumps({"emby_user_id": user_id, "emby_is_admin": user_id != "fixture-admin"}),
    )


def test_fixture_reconciles_seven_source_routes_and_excludes_subscription() -> None:
    payload = json.loads(DATABASE_FIXTURE.read_text(encoding="utf-8"))
    assert {(item["method"], item["path"]) for item in payload["routes"]} == {
        ("GET", "/api/database/stats/core"),
        ("GET", "/api/database/stats/library"),
        ("GET", "/api/database/stats/system"),
        ("GET", "/api/database/stats/rankings"),
        ("GET", "/api/database/tables"),
        ("GET", "/api/review_items"),
        ("GET", "/api/review_reasons"),
    }
    reconciliation = payload["scope_reconciliation"]
    assert reconciliation["requested_endpoint_count"] == 9
    assert reconciliation["actual_source_endpoint_count"] == 7
    assert reconciliation["excluded_routes"] == [
        {
            "method": "GET",
            "path": "/api/database/stats/subscription",
            "reason": (
                "excluded-by-task: legacy GET may write subscription_quota_state during daily reset "
                "or cap adjustment, and its SQL/result aliases do not match the route formatter"
            ),
        }
    ]
    assert payload["provenance"]["captured_live"] is False


def test_login_and_admin_permissions_remain_distinct(tmp_path: Path) -> None:
    del tmp_path
    repository = FakeDatabaseReadRepository()
    app, codec = _build_app(repository)

    with TestClient(app) as client:
        _assert_fixture_response(client.get("/api/database/stats/core"), "login-required")
        _assert_fixture_response(client.get("/api/database/tables"), "admin-required")

        _set_session(client, codec, "fixture-user")
        _assert_fixture_response(client.get("/api/database/stats/core"), "core-success")
        _assert_fixture_response(client.get("/api/database/tables"), "admin-required")

        _set_session(client, codec, "fixture-admin")
        _assert_fixture_response(client.get("/api/database/tables"), "tables-success")

    bypass_app, _ = _build_app(repository, auth_enabled=False)
    with TestClient(bypass_app) as client:
        _assert_fixture_response(client.get("/api/database/stats/core"), "login-required")
        _assert_fixture_response(client.get("/api/database/tables"), "tables-success")


def test_stats_tables_and_rankings_success_and_ordering() -> None:
    repository = FakeDatabaseReadRepository()
    app, codec = _build_app(repository)

    with TestClient(app) as client:
        _set_session(client, codec, "fixture-admin")
        _assert_fixture_response(client.get("/api/database/stats/core"), "core-success")
        _assert_fixture_response(client.get("/api/database/stats/library"), "library-success")
        _assert_fixture_response(client.get("/api/database/stats/system"), "system-empty-defaults")
        _assert_fixture_response(client.get("/api/database/stats/rankings"), "rankings-success")
        _assert_fixture_response(client.get("/api/database/tables"), "tables-success")
        assert client.get("/api/database/stats/subscription").status_code == 404

    assert repository.ranking_limits == [5, 5]


def test_review_pagination_filters_special_characters_and_legacy_json() -> None:
    repository = FakeDatabaseReadRepository()
    app, codec = _build_app(repository)
    special_query = "100%_Fixture'; SELECT"

    with TestClient(app) as client:
        _set_session(client, codec, "fixture-admin")
        response = client.get(
            "/api/review_items",
            params={
                "page": "2",
                "per_page": "10",
                "query": f"  {special_query}  ",
                "reason": "  缺失媒体信息  ",
            },
        )
        _assert_fixture_response(response, "review-items-success")
        _assert_fixture_response(client.get("/api/review_reasons"), "review-reasons-success")

        client.get(
            "/api/review_items",
            params={"page": "not-an-int", "per_page": "also-invalid"},
        )
        _assert_fixture_response(
            client.get("/api/review_items", params={"page": "0", "per_page": "0"}),
            "review-items-error",
        )

    assert repository.review_calls == [
        {
            "page": 2,
            "per_page": 10,
            "query": special_query,
            "reason": "缺失媒体信息",
        },
        {"page": 1, "per_page": 20, "query": "", "reason": ""},
        {"page": 0, "per_page": 0, "query": "", "reason": ""},
    ]


def test_review_empty_data_contract() -> None:
    repository = FakeDatabaseReadRepository()
    repository.items = []
    repository.total = 0
    app, codec = _build_app(repository)

    with TestClient(app) as client:
        _set_session(client, codec, "fixture-admin")
        _assert_fixture_response(client.get("/api/review_items"), "review-items-empty")


def test_route_specific_500_envelopes() -> None:
    repository = FakeDatabaseReadRepository()
    app, codec = _build_app(repository)

    with TestClient(app) as client:
        _set_session(client, codec, "fixture-admin")
        for method, path in (
            ("core", "/api/database/stats/core"),
            ("library", "/api/database/stats/library"),
            ("system", "/api/database/stats/system"),
            ("rankings", "/api/database/stats/rankings"),
        ):
            repository.error_method = method
            _assert_fixture_response(client.get(path), "stats-error")

        repository.error_method = "tables"
        _assert_fixture_response(client.get("/api/database/tables"), "tables-error")
        repository.error_method = "review_items"
        _assert_fixture_response(client.get("/api/review_items"), "review-items-error")
        repository.error_method = "review_reasons"
        _assert_fixture_response(client.get("/api/review_reasons"), "review-reasons-error")
