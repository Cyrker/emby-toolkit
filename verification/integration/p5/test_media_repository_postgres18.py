from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from etk.bootstrap.app_factory import create_app
from etk.bootstrap.runtime_supervisor import RuntimeSupervisor
from etk.domains.media.models import ImagePolicyKey, MediaItemType
from etk.domains.media.policies import MediaPresentationPolicy, format_metadata_payload
from etk.domains.media.repository import MediaAssetAlignmentError, SqlAlchemyMediaRepository
from etk.platform.db import DatabaseRuntime

SHA_A = "A" * 40
SHA_B = "B" * 40


def _repository(runtime: DatabaseRuntime) -> SqlAlchemyMediaRepository:
    return SqlAlchemyMediaRepository(runtime.unit_of_work)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _insert_media(runtime: DatabaseRuntime, **values: Any) -> None:
    defaults: dict[str, Any] = {
        "parent_series_tmdb_id": None,
        "season_number": None,
        "episode_number": None,
        "title": "Fixture",
        "overview": "Fixture overview",
        "release_year": 2026,
        "official_rating_json": {"US": "PG"},
        "genres_json": [{"name": "Drama"}],
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
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id,
                    item_type,
                    parent_series_tmdb_id,
                    season_number,
                    episode_number,
                    title,
                    original_title,
                    overview,
                    release_year,
                    official_rating_json,
                    genres_json,
                    metadata_ready,
                    actors_ready,
                    metadata_schema_version,
                    in_library,
                    emby_item_ids_json,
                    asset_details_json,
                    file_sha1_json,
                    file_pickcode_json
                ) VALUES (
                    :tmdb_id,
                    :item_type,
                    :parent_series_tmdb_id,
                    :season_number,
                    :episode_number,
                    :title,
                    :title,
                    :overview,
                    :release_year,
                    CAST(:official_rating_json AS jsonb),
                    CAST(:genres_json AS jsonb),
                    :metadata_ready,
                    :actors_ready,
                    :metadata_schema_version,
                    :in_library,
                    CAST(:emby_item_ids_json AS jsonb),
                    CAST(:asset_details_json AS jsonb),
                    CAST(:file_sha1_json AS jsonb),
                    CAST(:file_pickcode_json AS jsonb)
                )
                """
            ),
            {
                **defaults,
                "official_rating_json": _json(defaults["official_rating_json"]),
                "genres_json": _json(defaults["genres_json"]),
                "emby_item_ids_json": _json(defaults["emby_item_ids_json"]),
                "asset_details_json": _json(defaults["asset_details_json"]),
                "file_sha1_json": _json(defaults["file_sha1_json"]),
                "file_pickcode_json": _json(defaults["file_pickcode_json"]),
            },
        )


def _seed_four_types(runtime: DatabaseRuntime) -> None:
    _insert_media(
        runtime,
        tmdb_id="100",
        item_type="Movie",
        title="Fixture Movie",
        emby_item_ids_json=["movie-1"],
        asset_details_json=[{"path": "/library/movie.mkv", "source_library_id": "6"}],
        file_sha1_json=[SHA_A],
        file_pickcode_json=["pc-movie"],
    )
    _insert_media(
        runtime,
        tmdb_id="200",
        item_type="Series",
        title="Fixture Series",
        overview="",
        emby_item_ids_json=["series-old"],
        asset_details_json=[{"path": "/library/series", "source_library_id": "7"}],
        file_sha1_json=[""],
        file_pickcode_json=[""],
    )
    _insert_media(
        runtime,
        tmdb_id="201",
        item_type="Season",
        parent_series_tmdb_id="200",
        season_number=1,
        episode_number=99,
        title="Season 1",
        emby_item_ids_json=["season-old"],
        asset_details_json=[{"path": "/library/series/Season 01", "source_library_id": "7"}],
        file_sha1_json=[""],
        file_pickcode_json=[""],
    )
    _insert_media(
        runtime,
        tmdb_id="202",
        item_type="Episode",
        parent_series_tmdb_id="200",
        season_number=1,
        episode_number=1,
        title="Episode 1",
        emby_item_ids_json=["episode-old-1", "episode-old-2"],
        asset_details_json=[
            {"path": "/library/series/Season 01/Episode 01.strm", "source_library_id": "7"},
            {"path": "/library/series/Season 01/Episode 01-direct.mkv", "source_library_id": "7"},
        ],
        file_sha1_json=[SHA_A, SHA_B],
        file_pickcode_json=["pc-episode-a", "pc-episode-b"],
    )


def test_pg18_four_type_identity_metadata_and_backfill_queries(p5_runtime: DatabaseRuntime) -> None:
    _seed_four_types(p5_runtime)
    repository = _repository(p5_runtime)

    movie = repository.resolve_by_path("/library/movie.mkv", MediaItemType.MOVIE)
    series = repository.resolve_by_path("/library/series", MediaItemType.SERIES)
    season = repository.resolve_by_path(
        "/library/series/Season 01",
        MediaItemType.SEASON,
        season_number=1,
    )
    episode = repository.resolve_by_path(
        "/library/series/Season 01/Episode 01.strm",
        MediaItemType.EPISODE,
        season_number=1,
        episode_number=1,
    )

    assert movie is not None and movie.tmdb_id == "100"
    assert series is not None and series.tmdb_id == "200"
    assert season is not None and season.tmdb_id == "200"
    assert episode is not None and episode.tmdb_id == "200"
    assert repository.resolve_by_fingerprint(MediaItemType.MOVIE, sha1=SHA_A) == movie
    assert repository.resolve_by_fingerprint(MediaItemType.EPISODE, pick_code="PC-EPISODE-B") == episode
    assert repository.resolve_by_emby_id("episode-old-1") == episode

    season_metadata = repository.load_metadata(season, MediaItemType.SEASON)
    episode_metadata = repository.load_metadata(episode, MediaItemType.EPISODE)
    assert season_metadata is not None
    assert episode_metadata is not None
    season_payload = format_metadata_payload(season_metadata, MediaPresentationPolicy())
    episode_payload = format_metadata_payload(episode_metadata, MediaPresentationPolicy())
    assert season_payload["season_number"] == 1
    assert season_payload["episode_number"] is None
    assert episode_payload["series_tmdb_id"] == "200"
    assert episode_payload["season_number"] == 1
    assert episode_payload["episode_number"] == 1

    by_item = repository.find_backfill_targets_by_emby_ids(["episode-old-1"])
    by_library = repository.find_backfill_targets_by_library("7")
    assert [(value.tmdb_id, value.media_type) for value in by_item] == [("200", "tv")]
    assert [(value.tmdb_id, value.media_type) for value in by_library] == [("200", "tv")]


def test_pg18_image_policy_replace_and_reference_are_transactional(p5_runtime: DatabaseRuntime) -> None:
    repository = _repository(p5_runtime)
    key = ImagePolicyKey.create(MediaItemType.EPISODE, "200", season_number=1, episode_number=1)
    first = repository.replace_image_policy(
        key,
        [{"type": "Primary", "limit": 1, "min_width": 0}],
        [{"type": "Primary", "source_url": "http://fixture.invalid/one", "index": 0}],
    )
    second = repository.replace_image_policy(
        key,
        [{"type": "Backdrop", "limit": 1, "min_width": 0}],
        [{"type": "Backdrop", "source_url": "http://fixture.invalid/two", "index": 0}],
    )

    assert first == ()
    assert second == ({"type": "Primary", "source_url": "http://fixture.invalid/one", "index": 0},)
    record = repository.get_image_policy(key)
    assert record is not None
    assert record.rules == ({"type": "Backdrop", "limit": 1, "min_width": 0},)
    assert repository.source_is_referenced("http://fixture.invalid/two") is True
    assert repository.source_is_referenced("http://fixture.invalid/one") is False


def test_pg18_item_rebind_updates_one_slot_and_parent_hierarchy(p5_runtime: DatabaseRuntime) -> None:
    _seed_four_types(p5_runtime)
    repository = _repository(p5_runtime)

    result = repository.bind_emby_item(
        item_id="episode-new-2",
        item_type=MediaItemType.EPISODE,
        item_path="/library/series/Season 01/Episode 01-direct.mkv",
        sha1=SHA_B,
        pick_code="pc-episode-b",
        tmdb_id="202",
        series_id="series-new",
        season_id="season-new",
    )

    assert result["updated"] is True
    assert result["parent_updates"] == 2
    with p5_runtime.engine.connect() as connection:
        rows = {
            (row["tmdb_id"], row["item_type"]): row
            for row in connection.execute(
                text(
                    """
                    SELECT tmdb_id, item_type, emby_item_ids_json, file_sha1_json, file_pickcode_json
                    FROM media_metadata
                    WHERE tmdb_id IN ('200', '201', '202')
                    """
                )
            ).mappings()
        }
    assert rows[("202", "Episode")]["emby_item_ids_json"] == ["episode-old-1", "episode-new-2"]
    assert rows[("202", "Episode")]["file_sha1_json"] == [SHA_A, SHA_B]
    assert rows[("202", "Episode")]["file_pickcode_json"] == ["pc-episode-a", "pc-episode-b"]
    assert rows[("200", "Series")]["emby_item_ids_json"] == ["series-new"]
    assert rows[("201", "Season")]["emby_item_ids_json"] == ["season-new"]


def test_pg18_media_info_context_and_episode_query_use_aligned_item_slots(
    p5_runtime: DatabaseRuntime,
) -> None:
    _seed_four_types(p5_runtime)
    mediainfo = [
        {
            "MediaSourceInfo": {
                "RunTimeTicks": 600_000_000,
                "MediaStreams": [{"Type": "Video", "Codec": "h264"}],
            },
            "Chapters": [],
        }
    ]
    with p5_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO p115_mediainfo_cache (sha1, mediainfo_json)
                VALUES (:sha1, CAST(:mediainfo AS jsonb))
                """
            ),
            {"sha1": SHA_B, "mediainfo": _json(mediainfo)},
        )

    repository = _repository(p5_runtime)
    context = repository.resolve_media_info_context("episode-old-2")
    episodes = repository.list_series_media_info_episodes("series-old")

    assert context.item_id == "episode-old-2"
    assert context.sha1 == SHA_B
    assert context.mediainfo == tuple(mediainfo)
    assert context.parent_series_tmdb_id == "200"
    assert context.season_number == 1
    assert context.episode_number == 1
    assert [(value.emby_id, value.season_number, value.episode_number) for value in episodes] == [
        ("episode-old-1", 1, 1)
    ]


def test_pg18_media_info_context_rejects_legacy_unaligned_slots(
    p5_runtime: DatabaseRuntime,
) -> None:
    _insert_media(
        p5_runtime,
        tmdb_id="303",
        item_type="Movie",
        emby_item_ids_json=["movie-303", "movie-303-other"],
        asset_details_json=[{"path": "/library/movie-303.mkv"}],
        file_sha1_json=[SHA_A],
        file_pickcode_json=["pc-303", "pc-303-other"],
    )

    with pytest.raises(MediaAssetAlignmentError):
        _repository(p5_runtime).resolve_media_info_context("movie-303")


def test_pg18_ambiguous_or_unaligned_identity_fails_closed(p5_runtime: DatabaseRuntime) -> None:
    _insert_media(
        p5_runtime,
        tmdb_id="300",
        item_type="Movie",
        emby_item_ids_json=["movie-a"],
        asset_details_json=[{"path": "/library/ambiguous.mkv"}],
        file_sha1_json=[SHA_A],
        file_pickcode_json=["pc-a"],
    )
    _insert_media(
        p5_runtime,
        tmdb_id="301",
        item_type="Movie",
        emby_item_ids_json=["movie-b"],
        asset_details_json=[{"path": "/library/ambiguous.mkv"}],
        file_sha1_json=[SHA_A],
        file_pickcode_json=["pc-b"],
    )
    repository = _repository(p5_runtime)
    assert repository.resolve_by_path("/library/ambiguous.mkv", MediaItemType.MOVIE) is None
    assert repository.resolve_by_fingerprint(MediaItemType.MOVIE, sha1=SHA_A) is None

    _insert_media(
        p5_runtime,
        tmdb_id="302",
        item_type="Movie",
        emby_item_ids_json=["one", "two"],
        asset_details_json=[{"path": "/library/one.mkv"}, {"path": "/library/two.mkv"}],
        file_sha1_json=[SHA_B],
        file_pickcode_json=["pc-one", "pc-two"],
    )
    result: Mapping[str, Any] = repository.bind_emby_item(
        item_id="new",
        item_type=MediaItemType.MOVIE,
        item_path="/library/one.mkv",
        sha1=SHA_B,
        tmdb_id="302",
    )
    assert result == {"updated": False, "reason": "unaligned_asset_versions"}


def test_pg18_emby_event_reservation_deduplicates_and_allows_failed_retry(
    p5_runtime: DatabaseRuntime,
) -> None:
    first_repository = _repository(p5_runtime)
    second_repository = _repository(p5_runtime)

    token = first_repository.reserve_emby_event("event-digest", 30)

    assert token is not None
    assert second_repository.reserve_emby_event("event-digest", 30) is None
    assert second_repository.release_emby_event("event-digest", "wrong-token") is False
    assert first_repository.release_emby_event("event-digest", token) is True
    assert second_repository.reserve_emby_event("event-digest", 30) is not None


def test_pg18_emby_event_reservation_prunes_only_stale_p5_rows(
    p5_runtime: DatabaseRuntime,
) -> None:
    with p5_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                VALUES
                    ('p5:emby-event:stale', '{}'::jsonb, CURRENT_TIMESTAMP - INTERVAL '2 hours'),
                    ('p5:emby-event:recent', '{}'::jsonb, CURRENT_TIMESTAMP),
                    ('unrelated:stale', '{}'::jsonb, CURRENT_TIMESTAMP - INTERVAL '2 hours')
                """
            )
        )

    assert _repository(p5_runtime).reserve_emby_event("current", 30) is not None

    with p5_runtime.engine.connect() as connection:
        keys = set(connection.execute(text("SELECT setting_key FROM app_settings")).scalars())
    assert "p5:emby-event:stale" not in keys
    assert {"p5:emby-event:recent", "p5:emby-event:current", "unrelated:stale"} <= keys


def test_pg18_default_app_serves_metadata_without_materializing_flask(
    p5_runtime: DatabaseRuntime,
    migrated_p5_postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _insert_media(
        p5_runtime,
        tmdb_id="500",
        item_type="Movie",
        title="Native Movie",
        emby_item_ids_json=["movie-500"],
        asset_details_json=[{"path": "/library/native-movie.mkv", "source_library_id": "9"}],
        file_sha1_json=[SHA_A],
        file_pickcode_json=["pc-native"],
    )
    monkeypatch.setenv("ETK_DATABASE_URL", migrated_p5_postgres_url)
    app = create_app(
        start_runtime=False,
        data_dir=tmp_path / "config",
        secret_key=b"p5-native-media-session-key",
        supervisor=RuntimeSupervisor(),
    )

    assert app.state.compatibility_app.flask_app is None
    with TestClient(app) as client:
        response = client.get(
            "/api/emby/metadata",
            params={"path": "/library/native-movie.mkv", "item_type": "Movie"},
        )

    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store"
    assert response.json()["tmdb_id"] == "500"
    assert response.json()["name"] == "Native Movie"
    assert "_emby_item_ids" not in response.json()
    assert "_screenshot_hash" not in response.json()
    assert app.state.compatibility_app.flask_app is None
