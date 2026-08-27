from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.media.secondary_effect_adapters import HttpxSecondaryEmbyAdapter
from etk.domains.media.secondary_effects import (
    SecondaryEffectsService,
    SecondaryEmbyPort,
    SecondaryJobPort,
)
from etk.domains.media.secondary_router import effects_router
from etk.domains.operations.jobs import TaskBusyError
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

SECRET_KEY = b"p8-secondary-effects-fixture-key"


class FakePermissions:
    def __init__(self) -> None:
        self.active: set[str] = set()
        self.admins: set[str] = set()

    def is_active(self, user_id: str) -> bool:
        return user_id in self.active

    def is_admin(self, user_id: str) -> bool:
        return user_id in self.active and user_id in self.admins


class FakeEmby:
    def __init__(self) -> None:
        self.fail_libraries = False
        self.fail_views = False
        self.view_calls: list[tuple[str, str, dict[str, str]]] = []

    def configured_libraries(self) -> Sequence[Mapping[str, Any]]:
        if self.fail_libraries:
            raise RuntimeError("fixture library failure")
        return (
            {"Name": "Movies", "Id": "movie-lib", "CollectionType": "movies"},
            {"Name": "Music", "Id": "music-lib", "CollectionType": "music"},
            {"Name": "No type", "Id": "mixed-lib"},
            {"Name": "Missing ID", "CollectionType": "movies"},
        )

    def user_views(
        self,
        user_id: str,
        token: str,
        query: Mapping[str, str],
    ) -> Mapping[str, Any]:
        self.view_calls.append((user_id, token, dict(query)))
        if self.fail_views:
            raise RuntimeError("fixture view failure")
        return {"Items": [{"Id": "view-1"}], "TotalRecordCount": 1}


class FakeJobs:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
        self.busy = False
        self.fail = False

    def submit_task(
        self,
        task_key: str,
        parameters: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> object:
        if self.busy:
            raise TaskBusyError("fixture busy")
        if self.fail:
            raise RuntimeError("fixture submission failure")
        self.calls.append((task_key, dict(parameters or {}), dict(kwargs)))
        return object()


def _build_app() -> tuple[FastAPI, FakePermissions, FakeEmby, FakeJobs]:
    permissions = FakePermissions()
    emby = FakeEmby()
    jobs = FakeJobs()
    container = AppContainer(
        Settings(auth_enabled=True),
        repository_providers={"identity": lambda _: permissions},
    )
    app = FastAPI()
    app.state.container = container
    app.state.secondary_effects_service = SecondaryEffectsService(
        cast(SecondaryEmbyPort, emby),
        cast(SecondaryJobPort, jobs),
    )
    app.state.processor_ready = True
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(effects_router)
    return app, permissions, emby, jobs


def _set_user(client: TestClient, user_id: str) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set("session", codec.dumps({"emby_user_id": user_id}))


def test_effect_router_owns_four_routes_and_libraries_require_active_login() -> None:
    app, permissions, emby, _jobs = _build_app()
    assert {(method, path) for path, operations in app.openapi()["paths"].items() for method in operations} == {
        ("get", "/api/emby_libraries"),
        ("get", "/api/emby/user/{user_id}/views"),
        ("post", "/api/auto_tagging/run_now"),
        ("post", "/api/auto_tagging/clear_now"),
    }

    with TestClient(app) as client:
        anonymous = client.get("/api/emby_libraries")
        _set_user(client, "inactive-user")
        inactive = client.get("/api/emby_libraries")
        permissions.active.add("active-user")
        _set_user(client, "active-user")
        allowed = client.get("/api/emby_libraries")
        emby.fail_libraries = True
        failed = client.get("/api/emby_libraries")

    assert anonymous.status_code == 401
    assert inactive.status_code == 401
    assert allowed.status_code == 200
    assert allowed.json() == [
        {"Name": "Movies", "Id": "movie-lib"},
        {"Name": "No type", "Id": "mixed-lib"},
    ]
    assert failed.status_code == 500


def test_user_views_require_self_or_current_admin_and_filter_transport_inputs() -> None:
    app, permissions, emby, _jobs = _build_app()
    permissions.active.update({"fixture-user", "fixture-admin", "other-user"})
    permissions.admins.add("fixture-admin")

    with TestClient(app) as client:
        _set_user(client, "fixture-user")
        missing_token = client.get("/api/emby/user/fixture-user/views")
        placeholder = client.get(
            "/api/emby/user/fixture-user/views",
            headers={"X-Emby-Token": "********"},
        )
        other_denied = client.get(
            "/api/emby/user/other-user/views",
            headers={"X-Emby-Token": "user-token"},
        )
        own = client.get(
            "/api/emby/user/fixture-user/views",
            params={"api_key": "query-token", "PresetViews": "movies", "unknown": "drop-me"},
        )
        _set_user(client, "fixture-admin")
        admin = client.get(
            "/api/emby/user/other-user/views",
            params={"api_key": "query-token", "IncludeExternalContent": "true", "unknown": "drop-me"},
            headers={"X-Emby-Token": "header-token", "Authorization": "must-not-forward"},
        )
        emby.fail_views = True
        upstream_failure = client.get(
            "/api/emby/user/other-user/views",
            headers={"X-Emby-Token": "header-token"},
        )

    assert missing_token.status_code == 400
    assert placeholder.status_code == 400
    assert other_denied.status_code == 403
    assert own.status_code == 200
    assert admin.status_code == 200
    assert upstream_failure.status_code == 502
    assert emby.view_calls == [
        ("fixture-user", "query-token", {"PresetViews": "movies"}),
        ("other-user", "header-token", {"IncludeExternalContent": "true"}),
        ("other-user", "header-token", {}),
    ]


def test_auto_tagging_submits_fixed_native_task_and_preserves_legacy_clear_semantics() -> None:
    app, permissions, _emby, jobs = _build_app()
    permissions.active.add("fixture-admin")
    permissions.admins.add("fixture-admin")

    with TestClient(app) as client:
        denied = client.post("/api/auto_tagging/run_now", json={"tags": ["A"]})
        _set_user(client, "fixture-admin")
        app.state.processor_ready = False
        not_ready = client.post("/api/auto_tagging/run_now", json={"tags": ["A"]})
        app.state.processor_ready = True
        wrong_media = client.post(
            "/api/auto_tagging/run_now",
            content="{}",
            headers={"content-type": "text/plain"},
        )
        missing_tags = client.post("/api/auto_tagging/run_now", json={"library_ids": ["lib-1"]})
        submitted = client.post(
            "/api/auto_tagging/run_now",
            json={
                "library_ids": [" lib-1 ", "lib-1"],
                "library_name": "Fixture",
                "tags": [" A ", "A"],
                "rating_filters": ["PG-13"],
            },
        )
        cleared = client.post("/api/auto_tagging/clear_now", json={"library_ids": ["lib-1"]})
        jobs.busy = True
        busy = client.post("/api/auto_tagging/clear_now", json={"tags": ["A"]})

    assert denied.status_code == 403
    assert not_ready.status_code == 503
    assert wrong_media.status_code == 415
    assert missing_tags.status_code == 400
    assert submitted.json() == {"message": "批量打标任务已启动"}
    assert cleared.json() == {"message": "批量移除任务已启动"}
    assert busy.status_code == 409
    assert jobs.calls == [
        (
            "execute-auto-tagging-rules",
            {
                "operation": "add",
                "library_ids": ["lib-1", "lib-1"],
                "tags": ["A", "A"],
                "rating_filters": ["PG-13"],
            },
            {"trigger_type": "manual", "max_attempts": 2, "reject_when_busy": True},
        ),
        (
            "execute-auto-tagging-rules",
            {"operation": "remove", "library_ids": ["lib-1"], "tags": None, "rating_filters": None},
            {"trigger_type": "manual", "max_attempts": 2, "reject_when_busy": True},
        ),
    ]


def test_httpx_adapter_uses_only_selected_token_and_query_values() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "Items": [
                    {"Name": "Movies", "Id": "lib-1", "CollectionType": "movies"},
                ],
                "TotalRecordCount": 1,
            },
            request=request,
        )

    settings = Settings(
        emby_server_url="https://emby.example.test/emby",
        emby_user_id="configured-user",
        emby_api_key=SecretStr("configured-token"),
    )
    adapter = HttpxSecondaryEmbyAdapter(
        lambda: settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )

    libraries = adapter.configured_libraries()
    views = adapter.user_views("caller user", "caller-token", {"PresetViews": "movies"})

    assert libraries[0]["Id"] == "lib-1"
    assert views["TotalRecordCount"] == 1
    assert [request.url.raw_path for request in requests] == [
        b"/emby/Users/configured-user/Views",
        b"/emby/Users/caller%20user/Views?PresetViews=movies",
    ]
    assert requests[0].headers["x-emby-token"] == "configured-token"
    assert requests[1].headers["x-emby-token"] == "caller-token"
    assert dict(requests[1].url.params) == {"PresetViews": "movies"}
    assert all("authorization" not in request.headers for request in requests)
    assert all("cookie" not in request.headers for request in requests)
