from __future__ import annotations

import json
from collections.abc import Iterator

import pytest
from sqlalchemy import text

from etk.domains.sharing import SqlAlchemySharingProjectionRepository

SHA_A = "A" * 40
SHA_B = "B" * 40
SHA_C = "C" * 40
SHA_D = "D" * 40
SHA_E = "E" * 40


@pytest.fixture(autouse=True)
def _clean_projection_tables(p9_runtime) -> Iterator[None]:
    tables = (
        "shared_completed_season_share_channels",
        "shared_rapid_source_files",
        "shared_rapid_sources",
        "shared_virtual_imports",
        "shared_credit_snapshot",
        "p115_mediainfo_cache",
        "p115_filesystem_cache",
        "media_metadata",
    )
    with p9_runtime.engine.begin() as connection:
        for table in tables:
            connection.execute(text(f"DELETE FROM {table}"))
    yield
    with p9_runtime.engine.begin() as connection:
        for table in tables:
            connection.execute(text(f"DELETE FROM {table}"))


def test_postgres_local_source_summary_aggregation_and_channel_projection(p9_runtime) -> None:
    with p9_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO shared_credit_snapshot(id, credit, remote_devices)
                VALUES (1, 9, 2)
                """
            )
        )
        movie_id = connection.scalar(
            text(
                """
                INSERT INTO shared_rapid_sources(
                    source_key, source_kind, center_source_id, tmdb_id, item_type,
                    title, sha1, file_name, status, center_status, file_count,
                    raw_json, updated_at
                ) VALUES (
                    'movie-101', 'movie', 'movie-center', '101', 'Movie',
                    'Fixture Movie', :sha1, 'Fixture Movie.mkv', 'active', 'reported', 1,
                    '{"marker":"movie"}'::jsonb, NOW() - INTERVAL '1 hour'
                )
                RETURNING id
                """
            ),
            {"sha1": SHA_A},
        )
        first_episode_id = connection.scalar(
            text(
                """
                INSERT INTO shared_rapid_sources(
                    source_key, source_kind, center_source_id, tmdb_id, item_type,
                    parent_series_tmdb_id, season_number, episode_number, title,
                    status, center_status, file_count, raw_json, updated_at
                ) VALUES (
                    'episode-201-1', 'episode', 'svg_episode_1', '201', 'Episode',
                    '200', 1, 1, 'Fixture Series', 'active', 'reported', 1,
                    '{"marker":"episode-1"}'::jsonb, NOW() - INTERVAL '2 minutes'
                )
                RETURNING id
                """
            )
        )
        second_episode_id = connection.scalar(
            text(
                """
                INSERT INTO shared_rapid_sources(
                    source_key, source_kind, center_source_id, tmdb_id, item_type,
                    parent_series_tmdb_id, season_number, episode_number, title,
                    status, center_status, file_count, raw_json, updated_at
                ) VALUES (
                    'episode-201-2', 'episode', 'svg_episode_2', '201', 'Episode',
                    '200', 1, 2, 'Fixture Series', 'active', 'reported', 1,
                    '{"marker":"episode-2","hub_id":"hub-201"}'::jsonb, NOW()
                )
                RETURNING id
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO shared_completed_season_share_channels(
                    channel_id, center_source_id, local_source_id, hub_id,
                    share_code, status, review_status, status_message, raw_json
                ) VALUES (
                    'channel-201', 'svg_episode_2', :local_source_id, 'hub-201',
                    'share-201', 'valid', 'approved', 'ready',
                    CAST(:channel_raw AS jsonb)
                )
                """
            ),
            {
                "local_source_id": second_episode_id,
                "channel_raw": json.dumps({"share_kind": "logical_season", "tmdb_id": "201", "season_number": 1}),
            },
        )

    repository = SqlAlchemySharingProjectionRepository(p9_runtime.unit_of_work, account_id=lambda: "1")

    summary = repository.summary()
    listed = repository.list_local_sources(status="usable", page=1, page_size=30)
    by_sha1 = repository.list_local_sources(status="all", keyword=SHA_A, page=1, page_size=30)

    assert summary["shares"]["total"] == 3
    assert summary["shares"]["alive"] == 3
    assert summary["shares"]["alive_videos"] == 3
    assert summary["shares"]["alive_series_count"] == 1
    assert summary["shares"]["alive_season_count"] == 1
    assert summary["credit"]["credit"] == 9
    assert listed["total"] == 2
    season = next(item for item in listed["items"] if item["item_type"] == "Season")
    assert season["source_ids"] == [second_episode_id, first_episode_id]
    assert season["episode_numbers"] == [1, 2]
    assert season["raw_json"]["marker"] == "episode-2"
    assert season["has_share_channel"] is True
    assert season["share_channel_status"] == "valid"
    assert season["share_transfer_available"] is True
    assert season["completed_share_channel"]["share_code"] == "share-201"
    assert by_sha1["total"] == 1
    assert by_sha1["items"][0]["id"] == movie_id


def test_postgres_local_source_channel_status_filters(p9_runtime) -> None:
    with p9_runtime.engine.begin() as connection:
        source_ids = (
            connection.execute(
                text(
                    """
                INSERT INTO shared_rapid_sources(
                    source_key, source_kind, tmdb_id, item_type, season_number,
                    title, status, center_status, file_count
                ) VALUES
                    ('filter-valid', 'logical_season', '301', 'Season', 1,
                     'Valid channel', 'active', 'reported', 1),
                    ('filter-pending', 'logical_season', '302', 'Season', 1,
                     'Pending channel', 'active', 'reported', 1),
                    ('filter-none', 'logical_season', '303', 'Season', 1,
                     'No channel', 'active', 'partial', 1)
                RETURNING id
                """
                )
            )
            .scalars()
            .all()
        )
        connection.execute(
            text(
                """
                INSERT INTO shared_completed_season_share_channels(
                    channel_id, center_source_id, local_source_id, share_code, status, review_status
                ) VALUES
                    ('filter-channel-valid', 'svg_filter_valid', :valid_id,
                     'valid-code', 'valid', 'approved'),
                    ('filter-channel-pending', 'svg_filter_pending', :pending_id,
                     NULL, 'creating', 'pending_review')
                """
            ),
            {"valid_id": source_ids[0], "pending_id": source_ids[1]},
        )

    repository = SqlAlchemySharingProjectionRepository(p9_runtime.unit_of_work, account_id=lambda: "1")

    with_share = repository.list_local_sources(status="with_share")
    share_valid = repository.list_local_sources(status="share_valid")
    share_pending = repository.list_local_sources(status="share_pending")
    without_share = repository.list_local_sources(status="without_share")
    partial = repository.list_local_sources(status="partial")

    assert with_share["total"] == 2
    assert share_valid["total"] == 1
    assert share_valid["items"][0]["tmdb_id"] == "301"
    assert share_pending["total"] == 1
    assert share_pending["items"][0]["tmdb_id"] == "302"
    assert without_share["total"] == 1
    assert without_share["items"][0]["tmdb_id"] == "303"
    assert partial["total"] == 1
    assert partial["items"][0]["tmdb_id"] == "303"


def test_postgres_media_search_and_manual_validation_use_local_raw_contract(p9_runtime) -> None:
    valid_raw = {
        "format": {"bit_rate": "12000000"},
        "streams": [
            {"codec_type": "video", "codec_name": "hevc", "width": 3840, "height": 2160},
            {"codec_type": "audio", "codec_name": "aac", "tags": {"title": "国语"}},
        ],
    }
    invalid_raw = {
        "format": {"bit_rate": "8000000"},
        "streams": [
            {"codec_type": "video", "codec_name": "h264", "width": 1920, "height": 1080},
            {"codec_type": "audio", "codec_name": "aac", "tags": {}},
        ],
    }
    with p9_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata(
                    tmdb_id, item_type, in_library, title, file_sha1_json,
                    file_pickcode_json, release_year
                ) VALUES
                    ('101', 'Movie', TRUE, 'Fixture Movie', CAST(:sha_a AS jsonb), '["pc-a"]'::jsonb, 2026),
                    ('102', 'Movie', TRUE, 'Broken Movie', CAST(:sha_b AS jsonb), '["pc-b"]'::jsonb, 2025)
                """
            ),
            {"sha_a": json.dumps([SHA_A]), "sha_b": json.dumps([SHA_B])},
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache(
                    id, parent_id, name, local_path, sha1, pick_code, size, account_id, entry_kind
                )
                VALUES
                    ('file-a', 'dir-a', 'Fixture Movie.mkv', '/Movies/Fixture Movie.mkv',
                     NULL, 'pc-a', 1000, '1', 'file'),
                    ('file-b', 'dir-b', 'Broken Movie.mkv', '/Movies/Broken Movie.mkv',
                     :sha_b, 'pc-b', 900, '1', 'file'),
                    ('other-account', 'other-dir', 'Other Account.mkv', '/Other/Other Account.mkv',
                     :sha_c, 'pc-a', 500, '2', 'file')
                """
            ),
            {"sha_b": SHA_B, "sha_c": SHA_C},
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_mediainfo_cache(sha1, raw_ffprobe_json, mediainfo_json)
                VALUES
                    (:sha_a, CAST(:valid_raw AS jsonb), '{}'::jsonb),
                    (:sha_b, CAST(:invalid_raw AS jsonb), '{}'::jsonb)
                """
            ),
            {
                "sha_a": SHA_A,
                "sha_b": SHA_B,
                "valid_raw": json.dumps(valid_raw),
                "invalid_raw": json.dumps(invalid_raw),
            },
        )

    repository = SqlAlchemySharingProjectionRepository(p9_runtime.unit_of_work, account_id=lambda: "1")

    searched = repository.search_shareable_media("Fixture", limit=10)
    valid = repository.validate_manual_source({"item_type": "film", "tmdb_id": "101"})
    invalid = repository.validate_manual_source({"item_type": "Movie", "tmdb_id": "102"})

    assert len(searched) == 1
    assert searched[0]["resolvable"] is True
    assert searched[0]["root_fid"] == "file-a"
    assert searched[0]["file_count"] == 1
    assert valid["valid"] is True
    assert valid["files"][0]["sha1"] == SHA_A
    assert valid["files"][0]["preid"] == ""
    assert invalid["valid"] is False
    assert invalid["reason"] == "raw_missing"
    assert invalid["missing_raw"] == [
        {"sha1": SHA_B, "file_name": "Broken Movie.mkv", "reason": "RAW 或 summary_json 缺失"}
    ]


def test_postgres_manual_validation_preserves_file_identity_and_emby_raw_shapes(p9_runtime) -> None:
    ffprobe_raw = {"streams": [{"codec_type": "video", "codec_name": "hevc", "width": 3840, "height": 2160}]}
    media_source_raw = {
        "MediaSourceInfo": {
            "MediaStreams": [
                {"Type": "Video", "Codec": "HEVC", "Width": 1920, "Height": 1080},
                {"Type": "Audio", "Title": "国语"},
            ]
        }
    }
    top_level_raw = {
        "MediaStreams": [
            {"Type": "Video", "Codec": "H264", "Width": 1280, "Height": 720},
            {"Type": "Subtitle", "DisplayTitle": "简体"},
        ]
    }
    audio_only_raw = {"MediaStreams": [{"Type": "Audio", "Title": "国语"}]}
    with p9_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata(
                    tmdb_id, item_type, in_library, title, file_sha1_json, file_pickcode_json
                ) VALUES
                    ('401', 'Movie', TRUE, 'Two Files', CAST(:two_sha AS jsonb), '["pc-a","pc-b"]'::jsonb),
                    ('402', 'Movie', TRUE, 'Media Source Shape', CAST(:sha_c AS jsonb), '["pc-c"]'::jsonb),
                    ('403', 'Movie', TRUE, 'Top Level Shape', CAST(:sha_d AS jsonb), '["pc-d"]'::jsonb),
                    ('404', 'Movie', TRUE, 'Audio Only', CAST(:sha_e AS jsonb), '["pc-e"]'::jsonb),
                    ('405', 'Movie', TRUE, 'Sparse Slots', CAST(:sparse_sha AS jsonb),
                     '["pc-empty","pc-b2"]'::jsonb)
                """
            ),
            {
                "two_sha": json.dumps([SHA_A, SHA_B]),
                "sha_c": json.dumps([SHA_C]),
                "sha_d": json.dumps([SHA_D]),
                "sha_e": json.dumps([SHA_E]),
                "sparse_sha": json.dumps(["", SHA_B]),
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache(
                    id, parent_id, name, local_path, sha1, pick_code, size, account_id, entry_kind
                ) VALUES
                    ('two-a', 'dir-two', 'Two Files A.mkv', '/Movies/Two Files A.mkv', NULL, 'pc-a', 1000, '1', 'file'),
                    ('two-b', 'dir-two', 'Two Files B.mkv', '/Movies/Two Files B.mkv', NULL, 'pc-b', 900, '1', 'file'),
                    ('shape-c', 'dir-c', 'Media Source.mkv',
                     '/Movies/Media Source.mkv', NULL, 'pc-c', 800, '1', 'file'),
                    ('shape-d', 'dir-d', 'Top Level.mkv', '/Movies/Top Level.mkv', NULL, 'pc-d', 700, '1', 'file'),
                    ('audio-e', 'dir-e', 'Audio Only.mkv', '/Movies/Audio Only.mkv', NULL, 'pc-e', 600, '1', 'file'),
                    ('sparse-empty', 'dir-sparse', 'Sparse Empty.mkv',
                     '/Movies/Sparse Empty.mkv', NULL, 'pc-empty', 500, '1', 'file'),
                    ('sparse-b', 'dir-sparse', 'Sparse B.mkv',
                     '/Movies/Sparse B.mkv', NULL, 'pc-b2', 400, '1', 'file'),
                    ('dir-two', '0', 'Other Account Directory', '/Other/Directory',
                     NULL, NULL, 0, '2', 'directory')
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_mediainfo_cache(sha1, raw_ffprobe_json, mediainfo_json)
                VALUES
                    (:sha_a, CAST(:ffprobe_raw AS jsonb), '{}'::jsonb),
                    (:sha_b, CAST(:ffprobe_raw AS jsonb), '{}'::jsonb),
                    (:sha_c, CAST(:media_source_raw AS jsonb), '{}'::jsonb),
                    (:sha_d, CAST(:top_level_raw AS jsonb), '{}'::jsonb),
                    (:sha_e, CAST(:audio_only_raw AS jsonb), '{}'::jsonb)
                """
            ),
            {
                "sha_a": SHA_A,
                "sha_b": SHA_B,
                "sha_c": SHA_C,
                "sha_d": SHA_D,
                "sha_e": SHA_E,
                "ffprobe_raw": json.dumps(ffprobe_raw),
                "media_source_raw": json.dumps(media_source_raw),
                "top_level_raw": json.dumps(top_level_raw),
                "audio_only_raw": json.dumps(audio_only_raw),
            },
        )

    repository = SqlAlchemySharingProjectionRepository(p9_runtime.unit_of_work, account_id=lambda: "1")

    two_files = repository.validate_manual_source({"item_type": "Movie", "tmdb_id": "401"})
    media_source = repository.validate_manual_source({"item_type": "Movie", "tmdb_id": "402"})
    top_level = repository.validate_manual_source({"item_type": "Movie", "tmdb_id": "403"})
    audio_only = repository.validate_manual_source({"item_type": "Movie", "tmdb_id": "404"})
    sparse_slots = repository.validate_manual_source({"item_type": "Movie", "tmdb_id": "405"})

    assert two_files["valid"] is True
    assert two_files["file_count"] == 2
    assert {file["sha1"] for file in two_files["files"]} == {SHA_A, SHA_B}
    assert two_files["root_fid"] == ""
    assert media_source["valid"] is True
    assert top_level["valid"] is True
    assert audio_only["valid"] is False
    assert audio_only["reason"] == "raw_missing"
    assert sparse_slots["valid"] is True
    assert sparse_slots["file_count"] == 1
    assert sparse_slots["files"][0]["fid"] == "sparse-b"
    assert sparse_slots["files"][0]["pick_code"] == "pc-b2"
    assert sparse_slots["files"][0]["sha1"] == SHA_B


def test_postgres_virtual_import_alias_filters_and_pagination(p9_runtime) -> None:
    with p9_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO shared_virtual_imports(source_kind, source_id, tmdb_id, item_type, title, status)
                VALUES
                    ('center', 'movie-1', '101', 'Movie', 'Movie', 'virtual'),
                    ('center', 'series-1', '201', 'Series', 'Series', 'virtual'),
                    ('center', 'episode-1', '202', 'Episode', 'Episode', 'virtual')
                """
            )
        )

    repository = SqlAlchemySharingProjectionRepository(p9_runtime.unit_of_work, account_id=lambda: "1")

    movies = repository.list_virtual_imports(item_type="film", status="virtual")
    television = repository.list_virtual_imports(item_type="tv", status="virtual")
    second_page = repository.list_virtual_imports(item_type="all", status="virtual", page=2, page_size=2)

    assert movies["total"] == 1
    assert movies["items"][0]["item_type"] == "Movie"
    assert television["total"] == 2
    assert {item["item_type"] for item in television["items"]} == {"Series", "Episode"}
    assert second_page["total"] == 3
    assert second_page["page"] == 2
    assert len(second_page["items"]) == 1
