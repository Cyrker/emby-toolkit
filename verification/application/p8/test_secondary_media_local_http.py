from __future__ import annotations

from copy import deepcopy
from typing import Any, cast

from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.media.secondary import (
    SecondaryMediaProjection,
    SecondaryMediaService,
    SecondaryMediaSettings,
)
from etk.domains.media.secondary_router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

SECRET_KEY = b"p8-secondary-media-fixture-key"


class FakePermissions:
    def __init__(self) -> None:
        self.admins: set[str] = set()

    def is_admin(self, user_id: str) -> bool:
        return user_id in self.admins


class FakeMediaProjection:
    def search_library(self, query: str) -> dict[str, Any]:
        return {
            "items": [{"item_id": "emby-1", "item_name": query}],
            "total_items": 1,
        }

    def search_studios(self, query: str, *, limit: int = 20) -> list[str]:
        assert limit == 20
        return [f"{query} Studio"]


class FakeSettingsRepository:
    def __init__(self) -> None:
        self.values: dict[str, Any] = {"auto_tagging_rules": [{"name": "Fixture Rule"}]}
        self.fail = False

    def load(self, key: str) -> Any:
        if self.fail:
            raise RuntimeError("fixture settings failure")
        return deepcopy(self.values.get(key))

    def save(self, key: str, value: Any) -> None:
        if self.fail:
            raise RuntimeError("fixture settings failure")
        self.values[key] = deepcopy(value)


def _build_app() -> tuple[FastAPI, FakePermissions, FakeSettingsRepository]:
    permissions = FakePermissions()
    settings_repository = FakeSettingsRepository()
    service = SecondaryMediaService(
        media=cast(SecondaryMediaProjection, FakeMediaProjection()),
        settings=cast(SecondaryMediaSettings, settings_repository),
    )
    container = AppContainer(
        Settings(auth_enabled=True),
        repository_providers={"identity": lambda _: permissions},
    )
    app = FastAPI()
    app.state.container = container
    app.state.secondary_media_service = service
    app.state.processor_ready = True
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return app, permissions, settings_repository


def _set_user(client: TestClient, user_id: str) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set("session", codec.dumps({"emby_user_id": user_id}))


def test_four_p8_local_secondary_media_routes_and_boundaries() -> None:
    app, permissions, _settings = _build_app()
    assert {(method, path) for path, operations in app.openapi()["paths"].items() for method in operations} == {
        ("get", "/api/search_emby_library"),
        ("get", "/api/search_studios"),
        ("get", "/api/auto_tagging/rules"),
        ("post", "/api/auto_tagging/rules"),
    }

    with TestClient(app) as client:
        public_search = client.get("/api/search_emby_library", params={"query": "Fixture"})
        admin_denied = client.get("/api/auto_tagging/rules")
        _set_user(client, "fixture-admin")
        still_denied = client.get("/api/search_studios", params={"q": "A"})
        permissions.admins.add("fixture-admin")
        admin_allowed = client.get("/api/search_studios", params={"q": "A"})

    assert public_search.status_code == 200
    assert public_search.json()["total_items"] == 1
    assert admin_denied.status_code == 403
    assert still_denied.status_code == 403
    assert admin_allowed.json() == ["A Studio"]


def test_search_validation_readiness_and_auto_tagging_round_trip() -> None:
    app, permissions, settings = _build_app()
    permissions.admins.add("fixture-admin")
    with TestClient(app) as client:
        empty = client.get("/api/search_emby_library", params={"query": "  "})
        app.state.processor_ready = False
        not_ready = client.get("/api/search_emby_library", params={"query": "Fixture"})
        app.state.processor_ready = True
        _set_user(client, "fixture-admin")
        before = client.get("/api/auto_tagging/rules")
        saved = client.post("/api/auto_tagging/rules", json=[{"name": "Updated"}])
        after = client.get("/api/auto_tagging/rules")
        wrong_media = client.post(
            "/api/auto_tagging/rules",
            content="[]",
            headers={"content-type": "text/plain"},
        )
        settings.fail = True
        storage_error = client.get("/api/auto_tagging/rules")

    assert empty.status_code == 400
    assert not_ready.status_code == 503
    assert before.json() == [{"name": "Fixture Rule"}]
    assert saved.json() == {"message": "配置已保存"}
    assert after.json() == [{"name": "Updated"}]
    assert wrong_media.status_code == 415
    assert storage_error.status_code == 500
