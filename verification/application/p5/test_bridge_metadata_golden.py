from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from etk.domains.media.models import MediaIdentity, MediaItemType, MetadataAggregate
from etk.domains.media.policies import MediaPresentationPolicy, format_metadata_payload

_FIXTURE = json.loads(
    (Path(__file__).resolve().parents[3] / "verification/fixtures/http/bridge-metadata.json").read_text(
        encoding="utf-8"
    )
)


def _aggregate(body: dict[str, Any]) -> MetadataAggregate:
    item_type = MediaItemType.parse(body["item_type"])
    assert item_type is not None
    series_tmdb_id = str(body.get("series_tmdb_id") or "")
    identity = MediaIdentity(
        series_tmdb_id or str(body["tmdb_id"]),
        "tv" if series_tmdb_id else "movie",
        item_type,
        body.get("season_number"),
        body.get("episode_number"),
    )
    images = body["images"]
    row = {
        "tmdb_id": body["tmdb_id"],
        "item_type": body["item_type"],
        "imdb_id": body["imdb_id"],
        "title": body["name"],
        "original_title": body["original_title"],
        "overview": body["overview"],
        "tagline": body["tagline"],
        "release_date": body["premiere_date"],
        "last_air_date": body["end_date"],
        "release_year": body["production_year"],
        "rating": body["community_rating"],
        "official_rating_json": {"US": body["official_rating"]} if body["official_rating"] else {},
        "custom_rating": body["custom_rating"],
        "runtime_minutes": body["runtime_minutes"],
        "genres_json": [{"name": value} for value in body["genres"]],
        "keywords_json": [],
        "production_companies_json": [],
        "networks_json": [],
        "season_number": body["season_number"],
        "episode_number": body["episode_number"],
        "actors_ready": body["actors_ready"],
        "emby_item_ids_json": [],
        "poster_path": images["primary"],
        "backdrop_path": images["backdrop"],
        "logo_path": images["logo"],
        "thumb_path": images["thumb"],
    }
    return MetadataAggregate(identity, item_type, row)


def test_movie_series_season_episode_bridge_payloads_match_the_frozen_fixture() -> None:
    seen: list[str] = []
    for case in _FIXTURE["cases"]:
        expected = case["response"]
        actual = format_metadata_payload(_aggregate(expected["body"]), MediaPresentationPolicy())
        actual.pop("_screenshot_hash")
        actual.pop("_emby_item_ids")

        assert expected["status"] == 200
        assert expected["headers"] == {"Cache-Control": "no-store"}
        assert actual == expected["body"]
        seen.append(actual["item_type"])

    assert seen == ["Movie", "Series", "Season", "Episode"]
