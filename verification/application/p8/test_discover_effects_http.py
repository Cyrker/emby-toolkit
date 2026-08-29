from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.media.discover_adapters import HttpxDiscoverProviderAdapter
from etk.domains.media.discover_effects import (
    DiscoverEffectsDefaults,
    DiscoverEffectsService,
    DiscoverEffectsSettings,
    DiscoverJobPort,
    DiscoverProjection,
    DiscoverProviderPort,
)
from etk.domains.media.discover_router import effects_router
from etk.domains.operations.jobs import TaskBusyError
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

SECRET_KEY = b"p8-discover-effects-fixture-key"


class FakePermissions:
    def __init__(self) -> None:
        self.active: set[str] = set()

    def is_active(self, user_id: str) -> bool:
        return user_id in self.active

    def is_admin(self, user_id: str) -> bool:
        return False


class FakeProvider:
    def __init__(self) -> None:
        self.movie_pages: dict[int, Mapping[str, Any] | None] = {}
        self.tv_pages: dict[int, Mapping[str, Any] | None] = {}
        self.movie_calls: list[dict[str, Any]] = []
        self.tv_calls: list[dict[str, Any]] = []
        self.credit_calls: list[int] = []
        self.credits: dict[int, Mapping[str, Any] | None] = {}
        self.genre_values: dict[str, Sequence[Mapping[str, Any]] | None] = {}
        self.search_payload: Mapping[str, Any] | None = {"results": [], "total_pages": 0}
        self.search_calls: list[tuple[str, str, int]] = []
        self.details: Mapping[str, Any] | None = None
        self.raise_discover = False

    def discover_movie(self, payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
        self.movie_calls.append(dict(payload))
        if self.raise_discover:
            raise RuntimeError("fixture provider failure")
        return deepcopy(self.movie_pages.get(int(payload["page"])))

    def discover_tv(self, payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
        self.tv_calls.append(dict(payload))
        if self.raise_discover:
            raise RuntimeError("fixture provider failure")
        return deepcopy(self.tv_pages.get(int(payload["page"])))

    def person_tv_credits(self, person_id: int) -> Mapping[str, Any] | None:
        self.credit_calls.append(person_id)
        return deepcopy(self.credits.get(person_id))

    def genres(self, media_type: str) -> Sequence[Mapping[str, Any]] | None:
        return deepcopy(self.genre_values.get(media_type))

    def search(
        self,
        *,
        query: str,
        media_type: str,
        page: int,
    ) -> Mapping[str, Any] | None:
        self.search_calls.append((query, media_type, page))
        return deepcopy(self.search_payload)

    def tv_details(self, tv_id: int) -> Mapping[str, Any] | None:
        assert tv_id > 0
        return deepcopy(self.details)


class FakeProjection:
    def __init__(self) -> None:
        self.in_library: set[str] = set()

    def enrich(
        self,
        payload: Mapping[str, Any],
        *,
        item_type: str,
        hide_in_library: bool,
    ) -> dict[str, Any]:
        result = deepcopy(dict(payload))
        media_type = "movie" if item_type == "Movie" else "tv"
        enriched: list[dict[str, Any]] = []
        for value in result.get("results", []):
            item = dict(value)
            identifier = str(item.get("id"))
            item["in_library"] = identifier in self.in_library
            item["emby_item_id"] = [f"emby-{identifier}"] if item["in_library"] else []
            item["subscription_status"] = "WANTED" if identifier == "20" else None
            item["media_type"] = media_type
            if not hide_in_library or not item["in_library"]:
                enriched.append(item)
        result["results"] = enriched
        result["total_results"] = len(enriched)
        return result

    def enrich_tv_seasons(self, tv_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        result = deepcopy(dict(payload))
        assert tv_id == str(result["id"])
        for season in result.get("seasons", []):
            season["in_library"] = season.get("season_number") == 1
            season["subscription_status"] = "WANTED" if season["in_library"] else "NONE"
        return result


class FakeSettings:
    def __init__(self) -> None:
        self.values: dict[str, Any] = {
            "keyword_mapping": [{"label": "theme", "ids": [10, 11]}],
            "studio_mapping": [
                {
                    "label": "studio",
                    "company_ids": [20, 21],
                    "network_ids": [30],
                }
            ],
            "rating_mapping": {
                "US": [
                    {"code": "R", "label": "限制级", "emby_value": 9},
                    {"code": "XXX", "label": "成人", "emby_value": 15},
                ]
            },
        }

    def load_many(self, keys: Sequence[str]) -> Mapping[str, Any]:
        return {key: deepcopy(self.values.get(key)) for key in keys}


class FakeDefaults:
    def keyword_mapping(self) -> Sequence[Mapping[str, Any]]:
        return ()

    def studio_mapping(self) -> Sequence[Mapping[str, Any]]:
        return ()

    def rating_mapping(self) -> Mapping[str, Sequence[Mapping[str, Any]]]:
        return {}


class FakeJobs:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.busy = False
        self.missing_projection = False

    def submit_task(
        self,
        task_key: str,
        parameters: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> object:
        if self.busy:
            raise TaskBusyError("fixture busy")
        del parameters
        self.calls.append((task_key, dict(kwargs)))
        if self.missing_projection:
            return object()
        return SimpleNamespace(
            projection=SimpleNamespace(job=SimpleNamespace(id=UUID("00000000-0000-0000-0000-000000000288")))
        )


class StepClock:
    def __init__(self, values: Sequence[float]) -> None:
        self.values = list(values)
        self.last = self.values[-1]

    def __call__(self) -> float:
        if self.values:
            self.last = self.values.pop(0)
        return self.last


def _build_app(
    *,
    include_adult: bool = True,
    clock: StepClock | None = None,
) -> tuple[FastAPI, FakePermissions, FakeProvider, FakeProjection, FakeJobs]:
    permissions = FakePermissions()
    provider = FakeProvider()
    projection = FakeProjection()
    jobs = FakeJobs()
    service = DiscoverEffectsService(
        cast(DiscoverProviderPort, provider),
        cast(DiscoverProjection, projection),
        cast(DiscoverEffectsSettings, FakeSettings()),
        cast(DiscoverEffectsDefaults, FakeDefaults()),
        cast(DiscoverJobPort, jobs),
        include_adult=lambda: include_adult,
        clock=clock or StepClock([0]),
    )
    container = AppContainer(
        Settings(auth_enabled=True),
        repository_providers={"identity": lambda _: permissions},
    )
    app = FastAPI()
    app.state.container = container
    app.state.discover_effects_service = service
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(effects_router)
    return app, permissions, provider, projection, jobs


def _set_user(client: TestClient, user_id: str) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set("session", codec.dumps({"emby_user_id": user_id}))


def _item(identifier: int, **values: Any) -> dict[str, Any]:
    return {
        "id": identifier,
        "poster_path": f"/{identifier}.jpg",
        "popularity": float(identifier),
        **values,
    }


def test_six_discover_effect_routes_require_active_login_and_json() -> None:
    app, permissions, provider, _projection, _jobs = _build_app()
    provider.movie_pages[1] = {"results": [], "total_pages": 1}
    assert {(method, path) for path, operations in app.openapi()["paths"].items() for method in operations} == {
        ("post", "/api/discover/movie"),
        ("post", "/api/discover/tv"),
        ("get", "/api/discover/genres/{media_type}"),
        ("post", "/api/discover/search"),
        ("post", "/api/discover/trigger_recommendation_update"),
        ("get", "/api/discover/tmdb/tv/{tv_id}"),
    }

    with TestClient(app) as client:
        anonymous = client.post("/api/discover/movie", json={})
        _set_user(client, "inactive-user")
        inactive = client.post("/api/discover/movie", json={})
        permissions.active.add("fixture-user")
        _set_user(client, "fixture-user")
        wrong_media = client.post(
            "/api/discover/movie",
            content="{}",
            headers={"content-type": "text/plain"},
        )
        allowed = client.post("/api/discover/movie", json={"unknown_legacy_field": "ignored"})

    assert anonymous.status_code == 401
    assert inactive.status_code == 401
    assert wrong_media.status_code == 415
    assert allowed.status_code == 200


def test_movie_label_rating_expansion_three_page_replenishment_and_hide_limit() -> None:
    app, permissions, provider, projection, _jobs = _build_app()
    permissions.active.add("fixture-user")
    for page, start in ((4, 1), (5, 21), (6, 41)):
        provider.movie_pages[page] = {
            "results": [_item(identifier) for identifier in range(start, start + 20)],
            "total_pages": 30,
        }
    projection.in_library.update(str(identifier) for identifier in range(1, 41))

    with TestClient(app) as client:
        _set_user(client, "fixture-user")
        response = client.post(
            "/api/discover/movie",
            json={
                "page": 2,
                "with_keywords": ["theme", "99"],
                "with_companies": ["studio"],
                "with_rating_label": "成人",
                "hide_in_library": True,
            },
        )

    assert response.status_code == 200
    assert [item["id"] for item in response.json()["results"]] == list(range(41, 61))
    assert response.json()["total_results"] == 20
    assert response.json()["total_pages"] == 10
    assert [call["page"] for call in provider.movie_calls] == [4, 5, 6]
    for call in provider.movie_calls:
        assert call["with_keywords"] == "10|11,99"
        assert call["with_companies"] == "20|21"
        assert call["include_adult"] == "true"
        assert call["certification_country"] == "US"
        assert call["certification"] == "NC-17"


def test_tv_person_credit_intersection_person_only_and_discover_intersection() -> None:
    app, permissions, provider, _projection, _jobs = _build_app(include_adult=False)
    permissions.active.add("fixture-user")
    provider.credits = {
        1: {
            "cast": [
                _item(
                    20,
                    genre_ids=[18],
                    origin_country=["US"],
                    original_language="en",
                    first_air_date="2020-01-01",
                    vote_average=8,
                ),
                _item(10, genre_ids=[18], vote_average=4),
            ]
        },
        2: {"crew": [_item(20), _item(30)]},
    }
    provider.tv_pages[1] = {
        "results": [_item(20), _item(999)],
        "total_pages": 1,
    }

    with TestClient(app) as client:
        _set_user(client, "fixture-user")
        person_only = client.post(
            "/api/discover/tv",
            json={
                "with_cast": "1",
                "with_crew": "2",
                "with_genres": "18",
                "with_origin_country": "US",
                "with_original_language": "en",
                "vote_average.gte": 5,
            },
        )
        assert provider.tv_calls == []
        provider.credit_calls.clear()
        intersection = client.post(
            "/api/discover/tv",
            json={"with_cast": "1", "with_crew": "2", "with_keywords": ["theme"]},
        )
        provider.credits[3] = None
        missing_credits = client.post("/api/discover/tv", json={"with_cast": "3"})

    assert [item["id"] for item in person_only.json()["results"]] == [20]
    assert [item["id"] for item in intersection.json()["results"]] == [20]
    assert provider.tv_calls[0]["with_keywords"] == "10|11"
    assert missing_credits.status_code == 502


def test_timeout_search_genres_details_errors_and_durable_recommendation_job() -> None:
    app, permissions, provider, _projection, jobs = _build_app(clock=StepClock([0, 0, 21]))
    permissions.active.add("fixture-user")
    provider.movie_pages[1] = {"results": [_item(1)], "total_pages": 9}
    provider.genre_values["movie"] = [{"id": 18, "name": "剧情"}]
    provider.search_payload = {"results": [_item(7)], "total_pages": 1}
    provider.details = {"id": 8, "seasons": [{"season_number": 1}, {"season_number": 2}]}

    with TestClient(app) as client:
        _set_user(client, "fixture-user")
        timed = client.post("/api/discover/movie", json={"hide_in_library": True})
        genres = client.get("/api/discover/genres/movie")
        invalid_genres = client.get("/api/discover/genres/person")
        empty_search = client.post("/api/discover/search", json={"query": ""})
        search = client.post(
            "/api/discover/search",
            json={"query": "Fixture", "media_type": "movie", "page": 2},
        )
        details = client.get("/api/discover/tmdb/tv/8")
        submitted = client.post("/api/discover/trigger_recommendation_update")
        jobs.busy = True
        busy = client.post("/api/discover/trigger_recommendation_update")
        provider.movie_pages[1] = None
        unavailable = client.post("/api/discover/movie", json={})
        provider.raise_discover = True
        failed = client.post("/api/discover/movie", json={})

    assert [call["page"] for call in provider.movie_calls[:1]] == [1]
    assert len(timed.json()["results"]) == 1
    assert genres.json() == [{"id": 18, "name": "剧情"}]
    assert invalid_genres.status_code == 400
    assert empty_search.status_code == 400
    assert search.json()["results"][0]["id"] == 7
    assert provider.search_calls == [("Fixture", "movie", 2)]
    assert details.json()["seasons"][0]["subscription_status"] == "WANTED"
    assert submitted.status_code == 202
    assert submitted.json() == {
        "status": "ok",
        "task_key": "update-daily-theme",
        "job_id": "00000000-0000-0000-0000-000000000288",
        "message": "更新任务已在后台启动。",
    }
    assert busy.status_code == 409
    assert jobs.calls == [
        (
            "update-daily-theme",
            {
                "trigger_type": "manual",
                "requested_by_user_id": "fixture-user",
                "trace_id": None,
                "max_attempts": 2,
                "reject_when_busy": True,
            },
        )
    ]
    assert unavailable.status_code == 502
    assert failed.status_code == 500


def test_recommendation_trigger_rejects_a_submission_without_a_durable_job_identity() -> None:
    app, permissions, _provider, _projection, jobs = _build_app()
    permissions.active.add("fixture-user")
    jobs.missing_projection = True

    with TestClient(app) as client:
        _set_user(client, "fixture-user")
        response = client.post("/api/discover/trigger_recommendation_update")

    assert response.status_code == 500
    assert response.json() == {"error": "启动任务失败"}


def test_httpx_discover_adapter_uses_fixed_tmdb_paths_and_english_search_fallback() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        path = request.url.path
        if path == "/3/genre/movie/list":
            payload: Any = {"genres": [{"id": 18, "name": "Drama"}]}
        elif path == "/3/search/movie" and request.url.params["language"] == "zh-CN":
            payload = {"results": [], "total_pages": 1}
        elif path == "/3/search/movie":
            payload = {"results": [_item(7)], "total_pages": 1}
        elif path == "/3/person/1/tv_credits":
            payload = {"cast": [_item(20)], "crew": []}
        elif path == "/3/tv/8":
            payload = {"id": 8, "seasons": []}
        else:
            payload = {"results": [_item(1)], "total_pages": 1}
        return httpx.Response(200, json=payload, request=request)

    settings = Settings.model_validate(
        {
            "tmdb_api_base_url": "https://tmdb.example.test/3",
            "tmdb_api_key": "fixture-key",
        }
    )
    adapter = HttpxDiscoverProviderAdapter(
        lambda: settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )

    assert adapter.discover_movie({"page": 1}) is not None
    assert adapter.daily_theme_configured() is True
    assert adapter.movie_details(9) is not None
    assert adapter.person_tv_credits(1) is not None
    assert adapter.genres("movie") == ({"id": 18, "name": "Drama"},)
    search = adapter.search(query="Fixture", media_type="movie", page=2)
    assert search is not None
    assert search["results"][0]["id"] == 7
    assert adapter.tv_details(8) == {"id": 8, "seasons": []}
    assert [request.url.path for request in requests] == [
        "/3/discover/movie",
        "/3/movie/9",
        "/3/person/1/tv_credits",
        "/3/genre/movie/list",
        "/3/search/movie",
        "/3/search/movie",
        "/3/tv/8",
    ]
    assert all(request.url.params["api_key"] == "fixture-key" for request in requests)
    assert [request.url.params["language"] for request in requests if request.url.path == "/3/search/movie"] == [
        "zh-CN",
        "en-US",
    ]
