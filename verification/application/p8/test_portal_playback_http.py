from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.identity.playback import (
    DashboardMediaProjection,
    PlaybackReportError,
    PlaybackReportErrorKind,
    PlaybackReportingPort,
    PlaybackReportingService,
)
from etk.domains.identity.playback_adapter import HttpxPlaybackReportingAdapter
from etk.domains.identity.users_router import effects_router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

SECRET_KEY = b"p8-playback-fixture-key"
API_KEY = "fixture-emby-api-key-never-expose"


class FakePermissions:
    @staticmethod
    def is_admin(_user_id: str) -> bool:
        return False


class FakePlayback:
    def __init__(self) -> None:
        self.plugin_missing = False

    def report(self, *, user_id: str | None, days: int) -> Sequence[Mapping[str, Any]]:
        if self.plugin_missing:
            raise PlaybackReportError(PlaybackReportErrorKind.PLUGIN_NOT_INSTALLED)
        assert days == 30
        if user_id is None:
            return (
                {
                    "item_id": "episode-1",
                    "item_type": "Episode",
                    "duration": "3600",
                    "date": "2026-08-27",
                    "user_name": "Fixture User",
                },
            )
        assert user_id == "fixture-user"
        return (
            {
                "item_id": "episode-1",
                "item_type": "Episode",
                "item_name": "Fixture Show S01E02",
                "duration": "1800",
                "date": "2026-08-27",
                "time": "12:00:00",
            },
        )

    def item_details(
        self,
        *,
        user_id: str,
        item_ids: Sequence[str],
    ) -> Sequence[Mapping[str, Any]]:
        assert user_id == "fixture-user"
        assert tuple(item_ids) == ("episode-1",)
        return (
            {
                "Id": "episode-1",
                "SeriesName": "Fixture Show",
                "ParentIndexNumber": 1,
                "IndexNumber": 2,
            },
        )


class FakeProjection:
    @staticmethod
    def dashboard_aggregation(emby_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
        assert tuple(emby_ids) == ("episode-1",)
        return {
            "episode-1": {
                "id": "101",
                "name": "Fixture Show",
                "poster_path": "/fixture.jpg",
                "type": "Series",
                "emby_id": "series-1",
            }
        }


def _build_app(provider: FakePlayback) -> FastAPI:
    settings = Settings.model_validate(
        {
            "auth_enabled": True,
            "emby_server_url": "https://emby.fixture.invalid",
            "emby_public_url": "https://watch.fixture.invalid",
            "emby_server_id": "fixture-server",
        }
    )
    service = PlaybackReportingService(
        cast(PlaybackReportingPort, provider),
        cast(DashboardMediaProjection, FakeProjection()),
        settings=lambda: settings,
    )
    container = AppContainer(
        settings,
        repository_providers={"identity": lambda _: FakePermissions()},
    )
    app = FastAPI()
    app.state.container = container
    app.state.playback_reporting_service = service
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(effects_router)
    return app


def _set_user(client: TestClient) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set("session", codec.dumps({"emby_user_id": "fixture-user"}))


def test_personal_and_dashboard_reports_keep_any_login_contract() -> None:
    app = _build_app(FakePlayback())
    with TestClient(app) as client:
        denied = client.get("/api/portal/playback-report")
        _set_user(client)
        personal = client.get("/api/portal/playback-report")
        dashboard = client.get("/api/portal/dashboard-stats")

    assert denied.status_code == 401
    assert personal.status_code == 200
    assert personal.json() == {
        "personal": {
            "total_count": 1,
            "total_minutes": 30,
            "history_list": [
                {
                    "title": "Fixture Show - 第 1 季 - 第 2 集",
                    "date": "2026-08-27 12:00:00",
                    "duration": 30,
                    "item_type": "Episode",
                    "item_id": "episode-1",
                }
            ],
        }
    }
    assert dashboard.status_code == 200
    assert dashboard.json()["chart_users"] == {"names": ["Fixture User"], "hours": [1.0]}
    assert dashboard.json()["media_rank"][0]["id"] == "series-1"


def test_plugin_missing_maps_to_sanitized_404() -> None:
    provider = FakePlayback()
    provider.plugin_missing = True
    app = _build_app(provider)
    with TestClient(app) as client:
        _set_user(client)
        response = client.get("/api/portal/playback-report")

    assert response.status_code == 404
    assert response.json() == {
        "status": "error",
        "message": "服务端未安装 Playback Reporting 插件",
    }


def test_http_adapter_never_places_api_key_in_url_or_observable_error() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(503, request=request)

    settings = Settings.model_validate(
        {
            "emby_server_url": "https://emby.fixture.invalid",
            "emby_api_key": SecretStr(API_KEY),
        }
    )
    adapter = HttpxPlaybackReportingAdapter(
        lambda: settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )

    try:
        adapter.report(user_id=None, days=30)
    except PlaybackReportError as exc:
        observable = str(exc)
    else:
        raise AssertionError("fixture provider failure was not mapped")

    assert len(requests) == 1
    assert "api_key" not in str(requests[0].url)
    assert requests[0].headers["X-Emby-Token"] == API_KEY
    assert API_KEY not in observable
