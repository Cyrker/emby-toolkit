from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from importlib import import_module
from typing import Any, cast

import psycopg
import pytest
from psycopg.rows import dict_row
from sqlalchemy import text
from sqlalchemy.engine import make_url

from etk.domains.media.models import MediaItemType
from etk.domains.media.policies import MediaPresentationPolicy, format_metadata_payload
from etk.domains.media.repository import SqlAlchemyMediaRepository
from etk.platform.db import DatabaseRuntime

legacy_metadata = import_module("database.metadata_provider_db")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _insert_media(runtime: DatabaseRuntime, **values: Any) -> None:
    defaults = {
        "parent_series_tmdb_id": None,
        "season_number": None,
        "episode_number": None,
        "title": "Fixture",
        "original_title": "Fixture Original",
        "overview": "Fixture overview",
        "tagline": "Fixture tagline",
        "release_date": "2026-01-01",
        "last_air_date": None,
        "release_year": 2026,
        "rating": 8.5,
        "official_rating_json": {"US": "PG-13"},
        "custom_rating": None,
        "runtime_minutes": 100,
        "genres_json": [{"name": "Drama"}],
        "keywords_json": [{"name": "Fixture Keyword"}],
        "production_companies_json": [{"name": "Fixture Studio"}],
        "networks_json": [],
        "poster_path": None,
        "backdrop_path": None,
        "logo_path": None,
        "thumb_path": None,
        "metadata_ready": True,
        "actors_ready": False,
        "metadata_schema_version": 1,
        "in_library": True,
        "emby_item_ids_json": [],
        "asset_details_json": [],
        "file_sha1_json": [],
        "file_pickcode_json": [],
    }
    defaults.update(values)
    json_fields = (
        "official_rating_json",
        "genres_json",
        "keywords_json",
        "production_companies_json",
        "networks_json",
        "emby_item_ids_json",
        "asset_details_json",
        "file_sha1_json",
        "file_pickcode_json",
    )
    parameters = {**defaults, **{field: _json(defaults[field]) for field in json_fields}}
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, parent_series_tmdb_id, season_number, episode_number,
                    title, original_title, overview, tagline, release_date, last_air_date,
                    release_year, rating, official_rating_json, custom_rating, runtime_minutes,
                    genres_json, keywords_json, production_companies_json, networks_json,
                    poster_path, backdrop_path, logo_path, thumb_path,
                    metadata_ready, actors_ready, metadata_schema_version, in_library,
                    emby_item_ids_json, asset_details_json, file_sha1_json, file_pickcode_json
                ) VALUES (
                    :tmdb_id, :item_type, :parent_series_tmdb_id, :season_number, :episode_number,
                    :title, :original_title, :overview, :tagline, :release_date, :last_air_date,
                    :release_year, :rating, CAST(:official_rating_json AS jsonb), :custom_rating,
                    :runtime_minutes, CAST(:genres_json AS jsonb), CAST(:keywords_json AS jsonb),
                    CAST(:production_companies_json AS jsonb), CAST(:networks_json AS jsonb),
                    :poster_path, :backdrop_path, :logo_path, :thumb_path,
                    :metadata_ready, :actors_ready, :metadata_schema_version, :in_library,
                    CAST(:emby_item_ids_json AS jsonb), CAST(:asset_details_json AS jsonb),
                    CAST(:file_sha1_json AS jsonb), CAST(:file_pickcode_json AS jsonb)
                )
                """
            ),
            parameters,
        )


def _seed_four_types(runtime: DatabaseRuntime) -> None:
    _insert_media(
        runtime,
        tmdb_id="1001",
        item_type="Movie",
        title="Fixture Movie",
        poster_path="/movie.jpg",
        emby_item_ids_json=["movie-1"],
        asset_details_json=[{"path": "/fixture/movie.mkv"}],
        file_sha1_json=["A" * 40],
        file_pickcode_json=["pc-movie"],
    )
    _insert_media(
        runtime,
        tmdb_id="2001",
        item_type="Series",
        title="Fixture Series",
        runtime_minutes=45,
        emby_item_ids_json=["series-1"],
        asset_details_json=[{"path": "/fixture/series"}],
        file_sha1_json=[""],
        file_pickcode_json=[""],
    )
    _insert_media(
        runtime,
        tmdb_id="2001-S01",
        item_type="Season",
        parent_series_tmdb_id="2001",
        season_number=1,
        title="Season 1",
        runtime_minutes=None,
        emby_item_ids_json=["season-1"],
        asset_details_json=[{"path": "/fixture/series/Season 01"}],
        file_sha1_json=[""],
        file_pickcode_json=[""],
    )
    _insert_media(
        runtime,
        tmdb_id="2001-S01E01",
        item_type="Episode",
        parent_series_tmdb_id="2001",
        season_number=1,
        episode_number=1,
        title="Episode 1",
        runtime_minutes=45,
        emby_item_ids_json=["episode-1"],
        asset_details_json=[{"path": "/fixture/series/Season 01/Episode 01.strm"}],
        file_sha1_json=["B" * 40],
        file_pickcode_json=["pc-episode"],
    )


@pytest.mark.parametrize(
    ("item_type", "path", "root_tmdb_id", "media_type", "season_number", "episode_number"),
    [
        (MediaItemType.MOVIE, "/fixture/movie.mkv", "1001", "movie", None, None),
        (MediaItemType.SERIES, "/fixture/series", "2001", "tv", None, None),
        (MediaItemType.SEASON, "/fixture/series/Season 01", "2001", "tv", 1, None),
        (MediaItemType.EPISODE, "/fixture/series/Season 01/Episode 01.strm", "2001", "tv", 1, 1),
    ],
)
def test_pg18_legacy_and_native_bridge_metadata_are_identical(
    p5_runtime: DatabaseRuntime,
    migrated_p5_postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    item_type: MediaItemType,
    path: str,
    root_tmdb_id: str,
    media_type: str,
    season_number: int | None,
    episode_number: int | None,
) -> None:
    _seed_four_types(p5_runtime)

    @contextmanager
    def legacy_connection() -> Iterator[Any]:
        connection_url = (
            make_url(migrated_p5_postgres_url).set(drivername="postgresql").render_as_string(hide_password=False)
        )
        with psycopg.connect(connection_url, row_factory=cast(Any, dict_row)) as connection:
            yield connection

    monkeypatch.setattr(legacy_metadata, "get_db_connection", legacy_connection)
    monkeypatch.setattr(legacy_metadata.config_manager, "APP_CONFIG", {})
    legacy = legacy_metadata.load_emby_metadata(
        root_tmdb_id,
        media_type,
        item_type.value,
        season_number=season_number,
        episode_number=episode_number,
    )

    repository = SqlAlchemyMediaRepository(p5_runtime.unit_of_work)
    identity = repository.resolve_by_path(
        path,
        item_type,
        season_number=season_number,
        episode_number=episode_number,
    )
    assert identity is not None
    aggregate = repository.load_metadata(identity, item_type)
    assert aggregate is not None
    native = format_metadata_payload(aggregate, MediaPresentationPolicy())

    assert native == legacy
