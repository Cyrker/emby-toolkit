from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.subscription.core import (
    SUBSCRIPTION_CORE_USE_CASE_PROVIDER,
    SubscriptionBatchDeleteResult,
    SubscriptionCoreService,
    SubscriptionStateConflictError,
    SubscriptionTransition,
)
from etk.domains.subscription.core_router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = json.loads((REPO_ROOT / "verification/fixtures/http/p7-subscription-core.json").read_text(encoding="utf-8"))
SECRET_KEY = b"p7-subscription-core-fixture-key"


class FakePermissions:
    def __init__(self) -> None:
        self.admins: set[str] = set()

    def is_admin(self, user_id: str) -> bool:
        return user_id in self.admins

    @staticmethod
    def display_names(user_ids: Sequence[str]) -> Mapping[str, str]:
        return {user_id: "Fixture User" for user_id in user_ids}


class FakeAvailability:
    fail = False

    @classmethod
    def status(cls) -> Mapping[str, bool]:
        if cls.fail:
            raise RuntimeError("fixture availability failure")
        return {
            "mp_configured": True,
            "hdhive_configured": False,
            "tg_userbot_configured": True,
            "shared_pool_configured": False,
            "cloud_search_configured": True,
        }


class FakeRepository:
    def __init__(self) -> None:
        self.transitions: list[tuple[dict[str, Any], str]] = []
        self.items: list[dict[str, Any]] = []
        self.batch_values: list[dict[str, Any]] = []
        self.fail_tmdb_id: str | None = None
        self.batch_conflict = False

    def transition(self, request: Mapping[str, Any], target_status: str) -> SubscriptionTransition:
        if str(request.get("tmdb_id")) == self.fail_tmdb_id:
            raise RuntimeError("fixture transition failure")
        self.transitions.append((dict(request), target_status))
        return SubscriptionTransition(mutated=True)

    def list_subscriptions(self) -> list[dict[str, Any]]:
        return list(self.items)

    def batch_delete(self, values: Sequence[Mapping[str, Any]]) -> SubscriptionBatchDeleteResult:
        if self.batch_conflict:
            raise SubscriptionStateConflictError("媒体项正在执行清理任务，不能批量删除")
        self.batch_values = [dict(value) for value in values]
        return SubscriptionBatchDeleteResult(deleted_count=1, reset_count=2, operation_count=3)


def _build_app(
    *,
    auth_enabled: bool = False,
) -> tuple[FastAPI, FakePermissions, FakeRepository]:
    permissions = FakePermissions()
    repository = FakeRepository()
    service = SubscriptionCoreService(repository, FakeAvailability(), permissions)
    container = AppContainer(
        Settings(auth_enabled=auth_enabled),
        repository_providers={"identity": lambda _: permissions},
        use_case_providers={SUBSCRIPTION_CORE_USE_CASE_PROVIDER: lambda _: service},
    )
    app = FastAPI()
    app.state.container = container
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return app, permissions, repository


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


def test_public_status_and_admin_denial_contracts() -> None:
    app, permissions, _repository = _build_app(auth_enabled=True)
    with TestClient(app) as client:
        public = client.get("/api/subscription/status")
        denied_status = client.post("/api/subscription/status", json={"requests": []})
        denied_all = client.get("/api/subscriptions/all")
        permissions.admins.add("admin")
        _set_user(client, "admin")
        allowed_all = client.get("/api/subscriptions/all")

    assert public.json() == {
        "success": True,
        "mp_configured": True,
        "hdhive_configured": False,
        "tg_userbot_configured": True,
        "shared_pool_configured": False,
        "cloud_search_configured": True,
    }
    denial = {"status": "error", "message": "需要 Emby 管理员权限才能执行此操作"}
    assert denied_status.status_code == 403 and denied_status.json() == denial
    assert denied_all.status_code == 403 and denied_all.json() == denial
    assert allowed_all.json() == []


def test_public_status_configuration_failure_remains_html_500() -> None:
    app, *_ = _build_app()
    FakeAvailability.fail = True
    try:
        with TestClient(app) as client:
            response = client.get("/api/subscription/status")
    finally:
        FakeAvailability.fail = False

    assert response.status_code == 500
    assert response.headers["content-type"].startswith("text/html")


def test_status_partial_success_preserves_opaque_source_and_pending_release_transition() -> None:
    app, _permissions, repository = _build_app()
    repository.fail_tmdb_id = "bad"
    source = {"type": "user_request", "user_id": "fixture-user", "opaque": {"channel": "manual"}}
    with TestClient(app) as client:
        response = client.post(
            "/api/subscription/status",
            json={
                "requests": [
                    {
                        "tmdb_id": "100",
                        "item_type": "Movie",
                        "new_status": "PENDING_RELEASE",
                        "source": source,
                    },
                    {"tmdb_id": "bad", "item_type": "Movie", "new_status": "NONE"},
                    {"tmdb_id": "101", "item_type": "Movie", "new_status": "invalid"},
                ]
            },
        )

    assert response.status_code == 200
    assert response.json()["message"] == "已成功提交 1 个媒体项的状态变更请求。 但有 2 个请求处理失败。"
    assert len(response.json()["errors"]) == 2
    assert repository.transitions == [
        (
            {
                "tmdb_id": "100",
                "item_type": "Movie",
                "new_status": "PENDING_RELEASE",
                "source": source,
            },
            "PENDING_RELEASE",
        )
    ]


def test_management_and_batch_delete_keep_legacy_envelope_with_durable_counts() -> None:
    app, _permissions, repository = _build_app()
    repository.items = [
        {
            "tmdb_id": "100",
            "subscription_sources_json": [
                {"type": "manual_add"},
                {"type": "user_request", "user_id": "fixture-user", "opaque": {"kept": True}},
            ],
        }
    ]
    with TestClient(app) as client:
        listed = client.get("/api/subscriptions/all")
        deleted = client.post(
            "/api/media/batch_delete",
            json={"items": [{"tmdb_id": "100", "item_type": "Movie"}]},
        )
        repository.batch_conflict = True
        conflict = client.post(
            "/api/media/batch_delete",
            json={"items": [{"tmdb_id": "100", "item_type": "Movie"}]},
        )

    assert listed.json() == [
        {
            "tmdb_id": "100",
            "subscription_sources_json": [
                {"type": "manual_add"},
                {
                    "type": "user_request",
                    "user_id": "fixture-user",
                    "opaque": {"kept": True},
                    "user": "Fixture User",
                },
            ],
        }
    ]
    assert "user" not in repository.items[0]["subscription_sources_json"][1]
    assert deleted.json() == {
        "message": "成功删除了 1 条记录。",
        "deleted_count": 1,
    }
    assert repository.batch_values == [{"tmdb_id": "100", "item_type": "Movie"}]
    assert conflict.status_code == 409


def test_request_validation_and_transport_errors_are_deterministic() -> None:
    app, *_ = _build_app()
    with TestClient(app) as client:
        empty_status = client.post("/api/subscription/status", json={"requests": []})
        empty_delete = client.post("/api/media/batch_delete", json={"items": []})
        malformed = client.post(
            "/api/subscription/status",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
        unsupported = client.post(
            "/api/media/batch_delete",
            content=b"{}",
            headers={"Content-Type": "text/plain"},
        )
        non_object = client.post("/api/subscription/status", json=[])

    assert empty_status.status_code == 400
    assert empty_status.json() == {"error": "'requests' 必须是一个非空列表"}
    assert empty_delete.status_code == 400
    assert empty_delete.json() == {"error": "请求体必须包含非空的 'items' 列表"}
    assert malformed.status_code == 400 and malformed.headers["content-type"].startswith("text/html")
    assert unsupported.status_code == 415 and unsupported.headers["content-type"].startswith("text/html")
    assert non_object.status_code == 500 and non_object.headers["content-type"].startswith("text/html")
