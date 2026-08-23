from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from importlib import import_module

import psycopg2
import pytest
from psycopg2.extras import RealDictCursor
from sqlalchemy import text
from sqlalchemy.engine import make_url

from etk.platform.db import DatabaseRuntime

import_module("config_manager")
maintenance_db = import_module("database.maintenance_db")


def _connection_url(value: str) -> str:
    return make_url(value).set(drivername="postgresql").render_as_string(hide_password=False)


def test_pg18_deep_delete_retry_resumes_after_local_commit_without_repeating_local_mutation(
    p5_runtime: DatabaseRuntime,
    migrated_p5_postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sha1 = "D" * 40
    with p5_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, original_title, metadata_ready,
                    in_library, emby_item_ids_json, asset_details_json,
                    file_sha1_json, file_pickcode_json
                ) VALUES (
                    '5001', 'Movie', 'Delete Fixture', 'Delete Fixture', TRUE,
                    TRUE, '["movie-delete"]'::jsonb,
                    '[{"path":"/fixture/delete.mkv","emby_item_id":"movie-delete"}]'::jsonb,
                    CAST(:sha1s AS jsonb), '["pc-delete"]'::jsonb
                )
                """
            ),
            {"sha1s": json.dumps([sha1])},
        )

    @contextmanager
    def legacy_connection() -> Iterator[object]:
        connection = psycopg2.connect(
            _connection_url(migrated_p5_postgres_url),
            cursor_factory=RealDictCursor,
        )
        try:
            yield connection
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    extensions = import_module("extensions")
    config_manager = import_module("config_manager")
    monkeypatch.setattr(extensions, "media_processor_instance", None)
    monkeypatch.setattr(config_manager, "APP_CONFIG", {})
    monkeypatch.setattr(maintenance_db, "get_db_connection", legacy_connection)
    shared_calls: list[list[dict[str, object]]] = []

    def shared_cleanup(contexts: list[dict[str, object]]) -> dict[str, object]:
        shared_calls.append(contexts)
        return {"ok": len(shared_calls) > 1, "attempt": len(shared_calls)}

    monkeypatch.setattr(maintenance_db, "_cleanup_shared_sources_after_media_delete", shared_cleanup)
    options = {
        "item_id": "movie-delete",
        "item_name": "Delete Fixture",
        "item_type": "Movie",
        "operation_id": "fixture-delete-token",
    }

    first = maintenance_db.cleanup_deleted_media_item(**options)
    second = maintenance_db.cleanup_deleted_media_item(**options)
    replay = maintenance_db.cleanup_deleted_media_item(**options)

    assert first is not None and first["ok"] is False
    assert second is not None and second["ok"] is True
    assert replay == second
    assert len(shared_calls) == 2
    with p5_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                SELECT in_library, emby_item_ids_json, asset_details_json,
                       file_sha1_json, file_pickcode_json
                FROM media_metadata
                WHERE tmdb_id = '5001' AND item_type = 'Movie'
                """
                )
            )
            .mappings()
            .one()
        )
        receipt = connection.scalar(
            text("SELECT value_json FROM app_settings WHERE setting_key = :key"),
            {"key": maintenance_db._deep_delete_receipt_key("fixture-delete-token")},
        )
    assert row["in_library"] is False
    assert row["emby_item_ids_json"] == []
    assert row["asset_details_json"] is None
    assert row["file_sha1_json"] == []
    assert row["file_pickcode_json"] == []
    assert receipt["state"] == "confirmed"
    assert "fixture-delete-token" not in json.dumps(receipt)


def test_pg18_deep_delete_unknown_shared_outcome_is_not_blindly_replayed(
    p5_runtime: DatabaseRuntime,
    migrated_p5_postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with p5_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, original_title, metadata_ready,
                    in_library, emby_item_ids_json, asset_details_json,
                    file_sha1_json, file_pickcode_json
                ) VALUES (
                    '5002', 'Movie', 'Unknown Fixture', 'Unknown Fixture', TRUE,
                    TRUE, '["movie-unknown"]'::jsonb,
                    '[{"path":"/fixture/unknown.mkv","emby_item_id":"movie-unknown"}]'::jsonb,
                    '[""]'::jsonb, '[""]'::jsonb
                )
                """
            )
        )

    @contextmanager
    def legacy_connection() -> Iterator[object]:
        connection = psycopg2.connect(
            _connection_url(migrated_p5_postgres_url),
            cursor_factory=RealDictCursor,
        )
        try:
            yield connection
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    extensions = import_module("extensions")
    config_manager = import_module("config_manager")
    monkeypatch.setattr(extensions, "media_processor_instance", None)
    monkeypatch.setattr(config_manager, "APP_CONFIG", {})
    monkeypatch.setattr(maintenance_db, "get_db_connection", legacy_connection)
    calls = 0

    def unknown(_contexts: object) -> dict[str, object]:
        nonlocal calls
        calls += 1
        raise RuntimeError("fixture shared response lost")

    monkeypatch.setattr(maintenance_db, "_cleanup_shared_sources_after_media_delete", unknown)
    options = {
        "item_id": "movie-unknown",
        "item_name": "Unknown Fixture",
        "item_type": "Movie",
        "operation_id": "fixture-unknown-token",
    }

    first = maintenance_db.cleanup_deleted_media_item(**options)
    replay = maintenance_db.cleanup_deleted_media_item(**options)

    assert first is not None and first["ok"] is False
    assert replay == {
        "ok": False,
        "item_id": "movie-unknown",
        "item_type": "Movie",
        "indeterminate": True,
        "error": "shared cleanup outcome is unknown; automatic replay is forbidden",
    }
    assert calls == 1
