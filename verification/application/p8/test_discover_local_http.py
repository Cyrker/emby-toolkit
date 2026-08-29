from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import Any, cast

from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.media.discover import DiscoverDefaults, DiscoverService, DiscoverSettings
from etk.domains.media.discover_router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

SECRET_KEY = b"p8-discover-local-fixture-key"


class FakeDiscoverSettings:
    def __init__(self) -> None:
        self.values: dict[str, Any] = {
            "language_mapping": [
                {"label": "日语", "value": "ja", "extra": "ignored"},
                {"label": "invalid"},
            ],
            "keyword_mapping": [
                {"label": "科幻", "ids": [878]},
                {"label": "恐怖", "ids": [27]},
            ],
            "studio_mapping": {
                "Movie Studio": {"company_ids": [1]},
                "TV Network": {"network_ids": [2]},
                "Legacy Studio": {"ids": [3]},
            },
            "recommendation_pool": [{"id": 101, "title": "Fixture"}],
            "recommendation_pool_page": 4,
            "recommendation_theme_index": 3,
        }
        self.calls: list[tuple[str, ...]] = []
        self.fail = False

    def load_many(self, keys: Sequence[str]) -> Mapping[str, Any]:
        if self.fail:
            raise RuntimeError("fixture settings failure")
        normalized = tuple(keys)
        self.calls.append(normalized)
        return {key: deepcopy(self.values.get(key)) for key in normalized}


class FakeDiscoverDefaults:
    @staticmethod
    def language_mapping() -> Sequence[Mapping[str, Any]]:
        return [{"label": "默认语言", "value": "default"}]

    @staticmethod
    def keyword_mapping() -> Sequence[Mapping[str, Any]]:
        return [{"label": "默认主题", "ids": [999]}]

    @staticmethod
    def studio_mapping() -> Sequence[Mapping[str, Any]]:
        return [{"label": "默认工作室", "ids": [999]}]


class FakePermissions:
    @staticmethod
    def is_active(user_id: str) -> bool:
        return user_id == "fixture-user"

    @staticmethod
    def is_admin(user_id: str) -> bool:
        return False


def _build_app() -> tuple[FastAPI, FakeDiscoverSettings]:
    repository = FakeDiscoverSettings()
    service = DiscoverService(
        settings=cast(DiscoverSettings, repository),
        defaults=cast(DiscoverDefaults, FakeDiscoverDefaults()),
    )
    app = FastAPI()
    app.state.container = AppContainer(
        Settings(auth_enabled=True),
        repository_providers={"identity": lambda _: FakePermissions()},
    )
    app.state.discover_service = service
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return app, repository


def _set_user(client: TestClient) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set("session", codec.dumps({"emby_user_id": "fixture-user"}))


def test_four_local_discover_routes_require_any_login() -> None:
    app, _repository = _build_app()
    assert {(method, path) for path, operations in app.openapi()["paths"].items() for method in operations} == {
        ("get", "/api/discover/config/languages"),
        ("get", "/api/discover/config/keywords"),
        ("get", "/api/discover/config/studios"),
        ("get", "/api/discover/daily_recommendation"),
    }
    with TestClient(app) as client:
        for path in app.openapi()["paths"]:
            assert client.get(path).status_code == 401


def test_discover_config_and_daily_pool_use_consistent_settings_snapshots() -> None:
    app, repository = _build_app()
    with TestClient(app) as client:
        _set_user(client)
        languages = client.get("/api/discover/config/languages")
        keywords = client.get("/api/discover/config/keywords")
        studios = client.get("/api/discover/config/studios")
        daily = client.get("/api/discover/daily_recommendation")

    assert languages.json() == [{"label": "日语", "value": "ja"}]
    assert keywords.json() == [
        {"label": "科幻", "value": "科幻"},
        {"label": "恐怖", "value": "恐怖"},
    ]
    assert studios.json() == [
        {"label": "Legacy Studio", "value": "Legacy Studio", "types": ["movie", "tv"]},
        {"label": "Movie Studio", "value": "Movie Studio", "types": ["movie"]},
        {"label": "TV Network", "value": "TV Network", "types": ["tv"]},
    ]
    assert daily.json() == {
        "theme_name": "恐怖",
        "pool": [{"id": 101, "title": "Fixture"}],
    }
    assert len(repository.calls) == 4
    assert len(set(repository.calls)) == 1
    assert set(repository.calls[0]) >= {
        "recommendation_pool",
        "recommendation_pool_page",
        "recommendation_theme_index",
    }


def test_discover_fallback_empty_and_storage_failure_envelopes() -> None:
    app, repository = _build_app()
    repository.values["language_mapping"] = None
    repository.values["keyword_mapping"] = None
    repository.values["studio_mapping"] = None
    repository.values["recommendation_pool"] = []
    with TestClient(app) as client:
        _set_user(client)
        assert client.get("/api/discover/config/languages").json() == [{"label": "默认语言", "value": "default"}]
        assert client.get("/api/discover/config/keywords").json() == [{"label": "默认主题", "value": "默认主题"}]
        assert client.get("/api/discover/config/studios").json() == [
            {"label": "默认工作室", "value": "默认工作室", "types": ["movie", "tv"]}
        ]
        assert client.get("/api/discover/daily_recommendation").status_code == 404
        repository.fail = True
        failed = client.get("/api/discover/daily_recommendation")

    assert failed.status_code == 500
    assert failed.json() == {"error": "获取推荐池失败"}


def test_published_empty_daily_pool_is_a_successful_snapshot() -> None:
    app, repository = _build_app()
    repository.values["recommendation_pool"] = []
    repository.values["recommendation_publication_state"] = {"revision": 1}

    with TestClient(app) as client:
        _set_user(client)
        response = client.get("/api/discover/daily_recommendation")

    assert response.status_code == 200
    assert response.json() == {"theme_name": "恐怖", "pool": []}
