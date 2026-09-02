from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from typing import Any, cast

import httpx
import pytest
from pydantic import SecretStr

from etk.domains.collection.custom_recommendation import SqlAlchemyCustomCollectionRecommendationService
from etk.domains.collection.custom_task_adapters import HttpxCustomCollectionSourceAdapter
from etk.domains.collection.custom_tasks import (
    CustomCollectionCandidate,
    CustomCollectionSourceIndeterminate,
    CustomCollectionSourceRejected,
)
from etk.platform import Settings


def _settings() -> Settings:
    return Settings(
        tmdb_api_base_url="https://tmdb.example/3",
        tmdb_api_key=SecretStr("tmdb-fixture"),
        emby_server_url="https://emby.example",
        emby_user_id="emby-user",
        emby_api_key=SecretStr("emby-fixture"),
    )


def _adapter(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    clock: Callable[[], datetime] = lambda: datetime(2026, 9, 1, tzinfo=UTC),
) -> HttpxCustomCollectionSourceAdapter:
    transport = httpx.MockTransport(handler)
    return HttpxCustomCollectionSourceAdapter(
        _settings,
        client_factory=lambda: httpx.Client(transport=transport),
        clock=clock,
    )


def _rss(prefix: str, count: int) -> bytes:
    items = "".join(
        f"<item><title>{prefix}{index}</title><guid>tmdb://{prefix == 'B' and 2000 or 1000}{index}</guid></item>"
        for index in range(1, count + 1)
    )
    return f"<rss><channel>{items}</channel></rss>".encode()


def test_multi_url_rss_is_round_robin_and_missing_limit_does_not_truncate_at_fifty() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "feed-a.example":
            return httpx.Response(200, content=_rss("A", 30))
        if request.url.host == "feed-b.example":
            return httpx.Response(200, content=_rss("B", 30))
        raise AssertionError(request.url)

    adapter = _adapter(handler)
    definition = {
        "url": ["https://feed-a.example/list", "https://feed-b.example/list"],
        "item_type": ["Movie"],
    }

    values = adapter.fetch_list(definition)
    limited = adapter.fetch_list({**definition, "limit": 10})

    assert len(values) == 60
    assert [item.title for item in values[:4]] == ["A1", "B1", "A2", "B2"]
    assert len(limited) == 10
    assert [item.title for item in limited[:4]] == ["A1", "B1", "A2", "B2"]


def test_rss_imdb_and_title_matching_preserve_explicit_series_seasons() -> None:
    rss = """<rss><channel>
      <item><title>Fixture Show S2</title><guid>tt1234567</guid></item>
      <item><title>Other Show Season 3</title><link>https://feed.example/other</link></item>
      <item><title>Chinese Show 第二季</title><link>https://feed.example/chinese</link></item>
    </channel></rss>""".encode()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "feed.example":
            return httpx.Response(200, content=rss)
        if request.url.path == "/3/find/tt1234567":
            return httpx.Response(200, json={"tv_results": [{"id": 101, "name": "Fixture Show"}]})
        if request.url.path == "/3/search/tv":
            query = request.url.params["query"]
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "id": 102 if query == "Other Show" else 103,
                            "name": query,
                            "first_air_date": "2024-01-01",
                        }
                    ]
                },
            )
        raise AssertionError(request.url)

    values = _adapter(handler).fetch_list({"url": "https://feed.example/list", "item_type": ["Series"]})

    assert [(item.tmdb_id, item.season) for item in values] == [("101", 2), ("102", 3), ("103", 2)]


def test_tmdb_list_discover_dates_and_tv_person_intersection() -> None:
    observed_dates: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/3/list/42":
            page = int(request.url.params["page"])
            return httpx.Response(
                200,
                json={
                    "items": [{"id": page, "media_type": "movie", "title": f"List {page}"}],
                    "total_pages": 2,
                },
            )
        if request.url.path == "/3/discover/movie":
            observed_dates.append(request.url.params["primary_release_date.gte"])
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "id": 10,
                            "title": "Dated",
                            "release_date": "2026-09-01",
                            "poster_path": "/poster.jpg",
                            "overview": "overview",
                        }
                    ]
                },
            )
        if request.url.path in {"/3/person/1/tv_credits", "/3/person/2/tv_credits"}:
            person = request.url.path.split("/")[3]
            values = [
                {
                    "id": 20,
                    "name": "Shared",
                    "first_air_date": "2025-01-01",
                    "poster_path": "/shared.jpg",
                    "overview": "overview",
                    "vote_average": 8.5,
                    "vote_count": 100,
                    "popularity": 9,
                },
                {
                    "id": 21 if person == "1" else 22,
                    "name": "Unshared",
                    "poster_path": "/other.jpg",
                    "overview": "overview",
                },
            ]
            return httpx.Response(200, json={"cast": values, "crew": values})
        raise AssertionError(request.url)

    adapter = _adapter(handler)
    tmdb_list = adapter.fetch_list({"url": "https://www.themoviedb.org/list/42", "item_type": ["Movie"]})
    dated = adapter.fetch_list(
        {
            "url": "https://www.themoviedb.org/discover/movie?primary_release_date.gte={today+2}",
            "item_type": ["Movie"],
        }
    )
    person = adapter.fetch_list(
        {
            "url": (
                "https://www.themoviedb.org/discover/tv?with_cast=1&with_crew=2"
                "&vote_average.gte=8&sort_by=popularity.desc"
            ),
            "item_type": ["Series"],
        }
    )

    assert [item.tmdb_id for item in tmdb_list] == ["1", "2"]
    assert [item.tmdb_id for item in dated] == ["10"]
    assert observed_dates == ["2026-09-03"]
    assert [item.tmdb_id for item in person] == ["20"]


def test_douban_subject_imdb_is_resolved_before_title_fallback() -> None:
    page = """
    <div class="doulist-item">
      <div class="title"><a href="https://movie.douban.com/subject/1/">Fixture Movie</a></div>
      <div class="abstract">2024 / China</div>
    </div>
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "www.douban.com":
            return httpx.Response(200, text=page if request.url.params.get("start") == "0" else "")
        if request.url.host == "movie.douban.com":
            return httpx.Response(200, text='<a href="https://www.imdb.com/title/tt7654321/">IMDb</a>')
        if request.url.path == "/3/find/tt7654321":
            return httpx.Response(
                200,
                json={"movie_results": [{"id": 88, "title": "Fixture Movie", "release_date": "2024-01-01"}]},
            )
        raise AssertionError(request.url)

    values = _adapter(handler).fetch_list({"url": "https://www.douban.com/doulist/1/", "item_type": ["Movie"]})

    assert values == (CustomCollectionCandidate("88", "Movie", "Fixture Movie", year="2024"),)


@pytest.mark.parametrize(
    ("response", "error_type"),
    [
        (httpx.Response(404), CustomCollectionSourceRejected),
        (httpx.Response(503), CustomCollectionSourceIndeterminate),
    ],
)
def test_source_http_status_classification(
    response: httpx.Response,
    error_type: type[Exception],
) -> None:
    with pytest.raises(error_type):
        _adapter(lambda request: httpx.Response(response.status_code, request=request)).fetch_list(
            {"url": "https://feed.example/list", "item_type": ["Movie"]}
        )


def test_source_timeout_is_indeterminate() -> None:
    def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("fixture timeout", request=request)

    with pytest.raises(CustomCollectionSourceIndeterminate):
        _adapter(timeout).fetch_list({"url": "https://feed.example/list", "item_type": ["Movie"]})


class _Rows:
    def __init__(self, rows: Sequence[Mapping[str, object]]) -> None:
        self._rows = rows

    def mappings(self) -> _Rows:
        return self

    def all(self) -> list[Mapping[str, object]]:
        return list(self._rows)


class _Session:
    def __init__(
        self,
        history: Sequence[Mapping[str, object]],
        vectors: Sequence[Mapping[str, object]],
    ) -> None:
        self.history = history
        self.vectors = vectors
        self.queries: list[str] = []

    def execute(self, statement: object, _params: object = None) -> _Rows:
        sql = str(statement)
        self.queries.append(sql)
        return _Rows(self.history if "user_media_data" in sql else self.vectors)


class _Uow(AbstractContextManager[Any]):
    def __init__(self, session: _Session) -> None:
        self.session = session

    def __exit__(self, *_args: object) -> None:
        return None


class _DiscoveryFailure:
    def recommend(self, *_args: object, **_kwargs: object) -> Sequence[Mapping[str, object] | str]:
        raise RuntimeError("fixture discovery failure")


def test_recommendation_uses_popular_seed_and_deterministic_vector_fallback() -> None:
    session = _Session(
        [{"tmdb_id": "1", "item_type": "Movie", "title": "Seed", "release_year": 2020}],
        [
            {"tmdb_id": "1", "item_type": "Movie", "title": "Seed", "overview_embedding": [1.0, 0.0]},
            {"tmdb_id": "2", "item_type": "Movie", "title": "Second", "overview_embedding": [0.8, 0.6]},
            {"tmdb_id": "3", "item_type": "Movie", "title": "Third", "overview_embedding": [0.7, 0.714]},
            {"tmdb_id": "4", "item_type": "Movie", "title": "Wrong width", "overview_embedding": [1, 0, 0]},
            {"tmdb_id": "5", "item_type": "Movie", "title": "Invalid", "overview_embedding": [float("nan"), 0]},
        ],
    )
    service = SqlAlchemyCustomCollectionRecommendationService(cast(Any, lambda: _Uow(session)), _DiscoveryFailure())

    values = service.generate(
        {"limit": 5, "item_type": ["Movie"], "ai_discovery_ratio": 0.5},
        resolve_title=lambda *_args, **_kwargs: None,
    )

    assert [item.tmdb_id for item in values] == ["2", "3"]
    assert any("user_media_data" in query for query in session.queries)


def test_recommendation_rejects_mixed_seed_dimensions() -> None:
    session = _Session(
        [
            {"tmdb_id": "1", "item_type": "Movie", "title": "Seed 1"},
            {"tmdb_id": "2", "item_type": "Movie", "title": "Seed 2"},
        ],
        [
            {"tmdb_id": "1", "item_type": "Movie", "title": "Seed 1", "overview_embedding": [1, 0]},
            {"tmdb_id": "2", "item_type": "Movie", "title": "Seed 2", "overview_embedding": [1, 0, 0]},
            {"tmdb_id": "3", "item_type": "Movie", "title": "Candidate", "overview_embedding": [0.8, 0.6]},
        ],
    )

    values = SqlAlchemyCustomCollectionRecommendationService(cast(Any, lambda: _Uow(session))).generate(
        {"limit": 3, "item_type": ["Movie"], "ai_discovery_ratio": 0},
        resolve_title=lambda *_args, **_kwargs: None,
    )

    assert values == []
