from __future__ import annotations

import json

import pytest
from sqlalchemy import text

from etk.domains.p115.upload_projection import SqlAlchemyP115UploadProjectionRepository
from etk.platform.db import DatabaseRuntime

ACCOUNT_ID = "115001"
SHA1 = "A" * 40


def _repository(runtime: DatabaseRuntime) -> SqlAlchemyP115UploadProjectionRepository:
    return SqlAlchemyP115UploadProjectionRepository(runtime.unit_of_work)


def test_upload_projection_repository_runs_on_postgres18_and_reads_account_bound_target(
    p6_runtime: DatabaseRuntime,
) -> None:
    with p6_runtime.engine.begin() as connection:
        assert str(connection.scalar(text("SHOW server_version_num"))).startswith("18")
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache (
                    id, parent_id, name, local_path, account_id, entry_kind
                ) VALUES (
                    '9001', '0', 'Movies', '/媒体库/Movies', :account_id, 'directory'
                )
                """
            ),
            {"account_id": ACCOUNT_ID},
        )

    repository = _repository(p6_runtime)

    assert repository.target_relative_path("9001", account_id=ACCOUNT_ID) == "媒体库/Movies"
    with pytest.raises(RuntimeError, match="belongs to another account"):
        repository.target_relative_path("9001", account_id="115002")


def test_upload_projection_filesystem_upsert_is_replay_safe_and_conflicts_roll_back(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    values = {
        "account_id": ACCOUNT_ID,
        "file_id": "501",
        "parent_id": "9001",
        "name": "Movie.mkv",
        "sha1": SHA1,
        "pick_code": "pc-upload",
        "size": 7,
        "local_path": "Movies/Movie.mkv",
    }

    repository.persist_remote_file(**values)
    repository.persist_remote_file(**dict(values, size=8))

    with p6_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                    SELECT id, parent_id, name, local_path, sha1, pick_code, size,
                           account_id, entry_kind
                    FROM p115_filesystem_cache
                    WHERE id = '501'
                    """
                )
            )
            .mappings()
            .one()
        )
        assert connection.scalar(text("SELECT count(*) FROM p115_filesystem_cache")) == 1
    assert dict(row) == {
        "id": "501",
        "parent_id": "9001",
        "name": "Movie.mkv",
        "local_path": "Movies/Movie.mkv",
        "sha1": SHA1,
        "pick_code": "pc-upload",
        "size": 8,
        "account_id": ACCOUNT_ID,
        "entry_kind": "file",
    }

    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache (
                    id, parent_id, name, local_path, sha1, pick_code,
                    size, account_id, entry_kind
                ) VALUES (
                    'existing', '9001', 'Existing.mkv', 'Movies/Existing.mkv',
                    :sha1, 'PC-CONFLICT', 9, :account_id, 'file'
                )
                """
            ),
            {"sha1": "B" * 40, "account_id": ACCOUNT_ID},
        )

    with pytest.raises(RuntimeError, match="pick-code identity conflicts"):
        repository.persist_remote_file(**dict(values, file_id="502", pick_code="pc-conflict"))

    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM p115_filesystem_cache WHERE id = '502'")) == 0


def test_upload_projection_media_info_upsert_and_account_drift_are_atomic(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    first = [{"Id": "fixture-source", "Container": "mkv"}]
    second = [{"Id": "fixture-source", "Container": "matroska"}]
    raw = {"format": {"format_name": "matroska"}, "streams": []}

    repository.persist_media_info(
        account_id=ACCOUNT_ID,
        sha1=SHA1,
        formatted=first,
        raw=raw,
        formatter_revision="formatter-v1",
    )
    repository.persist_media_info(
        account_id=ACCOUNT_ID,
        sha1=SHA1,
        formatted=second,
        raw={"replacement": True},
        formatter_revision="formatter-v2",
    )

    with p6_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                    SELECT mediainfo_json, raw_ffprobe_json, cache_revision,
                           raw_schema_version, formatter_revision, probe_revision
                    FROM p115_mediainfo_cache
                    WHERE sha1 = :sha1
                    """
                ),
                {"sha1": SHA1},
            )
            .mappings()
            .one()
        )
        assert connection.scalar(text("SELECT count(*) FROM p115_mediainfo_cache")) == 1
    assert row["mediainfo_json"] == second
    assert row["raw_ffprobe_json"] == raw
    assert row["cache_revision"] == 2
    assert row["raw_schema_version"] == 1
    assert row["formatter_revision"] == "formatter-v2"
    assert row["probe_revision"] == "p6-local-upload-ffprobe-v1"

    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE app_settings
                SET value_json = CAST(:value AS jsonb), last_updated_at = NOW()
                WHERE setting_key = 'p115_auth_tokens'
                """
            ),
            {"value": json.dumps({"account_id": "115002"})},
        )

    with pytest.raises(RuntimeError, match="account changed"):
        repository.persist_media_info(
            account_id=ACCOUNT_ID,
            sha1="C" * 40,
            formatted=first,
            raw=raw,
            formatter_revision="formatter-v1",
        )

    with p6_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT count(*) FROM p115_mediainfo_cache WHERE sha1 = :sha1"),
                {"sha1": "C" * 40},
            )
            == 0
        )
