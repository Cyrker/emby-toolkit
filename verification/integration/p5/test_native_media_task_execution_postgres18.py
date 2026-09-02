from __future__ import annotations

from sqlalchemy import text

from etk.domains.media.task_execution import (
    SqlAlchemyMediaMetadataTaskRepository,
    SqlAlchemyScreenshotRepository,
)


def test_pg18_metadata_task_repository_preserves_file_projection_and_updates_roles(p5_runtime) -> None:
    with p5_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, overview, in_library,
                    emby_item_ids_json, file_sha1_json, file_pickcode_json, actors_json,
                    metadata_schema_version
                ) VALUES (
                    '100', 'Movie', 'Old title', 'Old overview', TRUE,
                    '[\"emby-100\"]'::jsonb, '[\"sha1\"]'::jsonb, '[\"pick\"]'::jsonb,
                    '[{\"name\":\"Existing\",\"character\":\"Lead\"}]'::jsonb, 0
                )
                """
            )
        )
    repository = SqlAlchemyMediaMetadataTaskRepository(p5_runtime.unit_of_work)

    assert repository.list_backfill_candidates()[0]["tmdb_id"] == "100"
    assert repository.upsert_metadata(
        {"Id": "emby-100", "Type": "Movie", "ProviderIds": {"Tmdb": "100"}},
        {
            "id": 100,
            "title": "New title",
            "overview": "New overview",
            "release_date": "2024-01-01",
            "genres": [],
        },
        preserve_people=True,
    )
    rows = repository.list_role_candidates()
    assert rows and rows[0]["actors_json"] == [{"name": "Existing", "character": "Lead"}]
    assert repository.update_roles(
        "100",
        "Movie",
        [{"name": "Existing", "character": "主角"}],
    )
    with p5_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                SELECT file_sha1_json, file_pickcode_json, actors_json, title
                FROM media_metadata
                WHERE tmdb_id = '100' AND item_type = 'Movie'
                """
                )
            )
            .mappings()
            .one()
        )
    assert row["file_sha1_json"] == ["sha1"]
    assert row["file_pickcode_json"] == ["pick"]
    assert row["actors_json"] == [{"name": "Existing", "character": "主角"}]
    assert row["title"] == "New title"


def test_pg18_screenshot_repository_reads_asset_paths_and_records_hash(p5_runtime) -> None:
    with p5_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, in_library, emby_item_ids_json, asset_details_json
                ) VALUES (
                    '200', 'Episode', TRUE, '[\"emby-200\"]'::jsonb,
                    '[{\"path\":\"/media/episode.strm\"}]'::jsonb
                )
                """
            )
        )
    repository = SqlAlchemyScreenshotRepository(p5_runtime.unit_of_work)

    candidates = repository.list_candidates()
    assert candidates == (
        {
            "tmdb_id": "200",
            "item_type": "Episode",
            "emby_item_id": "emby-200",
            "path": "/media/episode.strm",
        },
    )
    assert repository.record_screenshot(candidates[0], content_hash="a" * 64)
    with p5_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT screenshot_hash FROM media_metadata WHERE tmdb_id = '200'")) == "a" * 64


def test_pg18_metadata_offline_reconcile_is_scoped_to_selected_libraries(p5_runtime) -> None:
    with p5_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, in_library, emby_item_ids_json, asset_details_json
                ) VALUES
                    ('301', 'Movie', 'Library One', TRUE, '["emby-301"]'::jsonb,
                     '[{"emby_item_id":"emby-301","source_library_id":"library-1"}]'::jsonb),
                    ('302', 'Movie', 'Library Two', TRUE, '["emby-302"]'::jsonb,
                     '[{"emby_item_id":"emby-302","source_library_id":"library-2"}]'::jsonb)
                """
            )
        )

    repository = SqlAlchemyMediaMetadataTaskRepository(p5_runtime.unit_of_work)
    assert repository.mark_offline([], library_ids=["library-1"]) == 0
    assert repository.mark_offline(["emby-seen"], library_ids=["library-1"]) == 1
    with p5_runtime.engine.connect() as connection:
        states = connection.execute(
            text(
                """
                SELECT tmdb_id, in_library
                FROM media_metadata
                WHERE tmdb_id IN ('301', '302')
                ORDER BY tmdb_id
                """
            )
        ).all()
    assert states == [("301", False), ("302", True)]
