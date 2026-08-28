from __future__ import annotations

from sqlalchemy import text

from etk.domains.p115 import SqlAlchemyP115OrganizeRecordsRepository
from etk.platform.db import DatabaseRuntime


def _seed_records(runtime: DatabaseRuntime) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO p115_organize_records (
                    file_id, pick_code, original_name, renamed_name, status,
                    fail_reason, tmdb_id, media_type, target_cid,
                    category_name, processed_at, season_number
                ) VALUES
                    ('file-1', 'pc-1', 'Original.mkv', 'Movie.mkv', 'success',
                     NULL, '1', 'movie', 'target-1', '电影', NOW(), NULL),
                    ('file-2', 'pc-2', 'Unknown.mkv', NULL, 'unrecognized',
                     '未识别', NULL, NULL, 'target-2', NULL, NOW() - INTERVAL '8 days', NULL)
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache (
                    id, parent_id, name, local_path, account_id, entry_kind
                ) VALUES
                    ('file-1', 'source', 'Original.mkv', '/source/Original.mkv', '115001', 'file'),
                    ('target-1', 'root', 'Movie', '/电影/Movie', '115001', 'directory'),
                    ('target-child', 'target-1', 'Season 01', '/电影/Movie/Season 01', '115001', 'directory'),
                    ('target-grandchild', 'target-child', 'Episode.mkv', '/电影/Movie/Season 01/Episode.mkv',
                     '115001', 'file'),
                    ('sibling', 'root', 'Other', '/电影/Other', '115001', 'directory')
                """
            )
        )


def test_records_query_preserves_filters_statistics_and_legacy_envelope_data(
    p6_runtime: DatabaseRuntime,
) -> None:
    _seed_records(p6_runtime)
    repository = SqlAlchemyP115OrganizeRecordsRepository(p6_runtime.unit_of_work)

    page = repository.list_records(
        page=1,
        per_page=15,
        search="Movie",
        status="success",
        cid="target-1",
        fail_reason="",
    )

    assert page.total == 1
    assert [item["file_id"] for item in page.items] == ["file-1"]
    assert page.stats == {
        "total": 2,
        "success": 1,
        "unrecognized": 1,
        "unqualified": 0,
        "thisWeek": 1,
    }
    assert page.fail_reasons == ({"fail_reason": "未识别", "count": 1},)


def test_delete_record_removes_only_local_record_source_target_and_direct_child_cache(
    p6_runtime: DatabaseRuntime,
) -> None:
    _seed_records(p6_runtime)
    repository = SqlAlchemyP115OrganizeRecordsRepository(p6_runtime.unit_of_work)
    with p6_runtime.engine.connect() as connection:
        record_id = int(connection.scalar(text("SELECT id FROM p115_organize_records WHERE file_id = 'file-1'")))

    repository.delete_record(record_id)
    repository.delete_record(record_id)

    with p6_runtime.engine.connect() as connection:
        remaining_records = set(connection.scalars(text("SELECT file_id FROM p115_organize_records")))
        remaining_cache = set(connection.scalars(text("SELECT id FROM p115_filesystem_cache")))
    assert remaining_records == {"file-2"}
    assert remaining_cache == {"target-grandchild", "sibling"}


def test_postgres18_backfill_resolves_pickcode_and_sha1_without_overwriting_identity(
    p6_runtime: DatabaseRuntime,
) -> None:
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO p115_organize_records (
                    file_id, pick_code, original_name, status, tmdb_id
                ) VALUES
                    ('backfill-movie', 'pc-movie', 'Movie.mkv', 'success', NULL),
                    ('backfill-episode', NULL, 'Episode.mkv', 'success', NULL),
                    ('backfill-existing', 'pc-existing', 'Existing.mkv', 'success', 'keep-me'),
                    ('backfill-unmatched', 'pc-unmatched', 'Unknown.mkv', 'success', NULL)
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache (
                    id, parent_id, name, sha1, account_id, entry_kind
                ) VALUES
                    ('backfill-movie', 'root', 'Movie.mkv', NULL, '115001', 'file'),
                    ('backfill-episode', 'root', 'Episode.mkv', 'ABCDEF', '115001', 'file'),
                    ('backfill-existing', 'root', 'Existing.mkv', NULL, '115001', 'file'),
                    ('backfill-unmatched', 'root', 'Unknown.mkv', NULL, '115001', 'file')
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, file_pickcode_json, file_sha1_json,
                    parent_series_tmdb_id, in_library, last_updated_at
                ) VALUES
                    ('movie-100', 'Movie', '["pc-movie"]'::jsonb, '[]'::jsonb,
                     NULL, TRUE, NOW()),
                    ('episode-1', 'Episode', '[]'::jsonb, '["abcdef"]'::jsonb,
                     'series-200', TRUE, NOW()),
                    ('existing-300', 'Movie', '["pc-existing"]'::jsonb, '[]'::jsonb,
                     NULL, TRUE, NOW())
                """
            )
        )

    repository = SqlAlchemyP115OrganizeRecordsRepository(p6_runtime.unit_of_work)

    assert repository.backfill_missing_tmdb_ids() == 2

    with p6_runtime.engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT file_id, tmdb_id
                FROM p115_organize_records
                WHERE file_id LIKE 'backfill-%'
                ORDER BY file_id
                """
            )
        ).all()
    assert rows == [
        ("backfill-episode", "series-200"),
        ("backfill-existing", "keep-me"),
        ("backfill-movie", "movie-100"),
        ("backfill-unmatched", None),
    ]
