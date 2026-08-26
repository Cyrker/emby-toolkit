from __future__ import annotations

import json
from dataclasses import replace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etk.domains.p115.fingerprints import (
    P115FileFingerprint,
    P115FingerprintAccountChanged,
    P115FingerprintConflict,
)
from etk.domains.p115.media_info import P115MediaProbeItemStatus
from etk.domains.p115.media_info_repository import SqlAlchemyP115MediaInfoRepository
from etk.domains.p115.media_probe import P115MediaProbeRequest
from etk.platform.db import DatabaseRuntime

SHA_A = "A" * 40
SHA_B = "B" * 40
PROVENANCE_A = "a" * 64
PROVENANCE_B = "b" * 64


def _repository(runtime: DatabaseRuntime) -> SqlAlchemyP115MediaInfoRepository:
    return SqlAlchemyP115MediaInfoRepository(runtime.unit_of_work)


def _insert_media_row(runtime: DatabaseRuntime) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, in_library,
                    emby_item_ids_json, asset_details_json,
                    file_sha1_json, file_pickcode_json
                )
                VALUES (
                    '101', 'Movie', 'Fixture Movie', TRUE,
                    '["emby-a", "emby-b"]'::jsonb,
                    '[{"path":"/library/A.strm"},{"path":"/library/B.strm"}]'::jsonb,
                    '["", ""]'::jsonb,
                    '["", ""]'::jsonb
                )
                """
            )
        )


def _fingerprint(
    *,
    file_id: str,
    parent_id: str,
    name: str,
    sha1: str,
    pick_code: str,
    local_path: str,
) -> P115FileFingerprint:
    return P115FileFingerprint(
        file_id=file_id,
        parent_id=parent_id,
        name=name,
        sha1=sha1,
        pick_code=pick_code,
        size=1024,
        account_id="115001",
        local_path=local_path,
    )


def _insert_restore_row(runtime: DatabaseRuntime) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, original_language, in_library,
                    emby_item_ids_json, asset_details_json,
                    file_sha1_json, file_pickcode_json
                )
                VALUES (
                    '202', 'Movie', 'Restore Fixture', 'en', TRUE,
                    '["emby-restore"]'::jsonb,
                    CAST(:assets AS jsonb),
                    CAST(:sha1s AS jsonb), '["pc-restore"]'::jsonb
                )
                """
            ),
            {
                "assets": json.dumps([{"path": "/library/Restore.Fixture.mkv", "size_bytes": 2048}]),
                "sha1s": json.dumps([SHA_A]),
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache (
                    id, parent_id, name, sha1, pick_code,
                    size, account_id, entry_kind
                )
                VALUES (
                    '9202', '8202', 'Provider.Restore.Fixture.mkv', :sha1,
                    'pc-restore', 4096, '115001', 'file'
                )
                """
            ),
            {"sha1": SHA_A},
        )


def test_p6_6_migration_is_head_and_declares_revisioned_cache(
    p6_runtime: DatabaseRuntime,
) -> None:
    with p6_runtime.engine.connect() as connection:
        head = connection.scalar(text("SELECT version_num FROM etk_migrations.alembic_version"))
        columns = {
            row["column_name"]
            for row in connection.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'p115_mediainfo_cache'
                    """
                )
            ).mappings()
        }
        probe_table = connection.scalar(text("SELECT to_regclass('public.p115_media_probe_items')"))
    assert head == "0012_p6_intro_detection"
    assert columns >= {
        "updated_at",
        "cache_revision",
        "raw_schema_version",
        "formatter_revision",
        "probe_revision",
    }
    assert probe_table == "p115_media_probe_items"


def test_cache_query_is_canonical_and_hit_count_is_explicit(
    p6_runtime: DatabaseRuntime,
) -> None:
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO p115_mediainfo_cache (
                    sha1, mediainfo_json, raw_ffprobe_json,
                    formatter_revision, probe_revision
                )
                VALUES (
                    :sha1,
                    '[{"MediaSourceInfo":{"MediaStreams":[]},"Chapters":[]}]'::jsonb,
                    '{"_etk":{"tmdb_id":"101","type":"movie"}}'::jsonb,
                    'p6-v1', 'ffprobe-v1'
                )
                """
            ),
            {"sha1": SHA_A},
        )
    repository = _repository(p6_runtime)

    first = repository.find_by_sha1(SHA_A)
    second = repository.find_by_sha1(SHA_A, record_hit=True)

    assert first is not None and first.hit_count == 0
    assert second is not None and second.hit_count == 1
    assert second.raw == {"_etk": {"tmdb_id": "101", "type": "movie"}}
    assert second.cache_revision == 0


def test_cache_content_updates_advance_revision_and_touch_updated_at(
    p6_runtime: DatabaseRuntime,
) -> None:
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO p115_mediainfo_cache (
                    sha1, mediainfo_json, raw_ffprobe_json, updated_at
                ) VALUES (
                    :sha1, jsonb_build_object('version', 0), jsonb_build_object('version', 0),
                    '2000-01-01T00:00:00Z'::timestamptz
                )
                """
            ),
            {"sha1": SHA_A},
        )
        connection.execute(
            text(
                "UPDATE p115_mediainfo_cache SET mediainfo_json = jsonb_build_object('version', 1) WHERE sha1 = :sha1"
            ),
            {"sha1": SHA_A},
        )
        after_formatted = (
            connection.execute(
                text("SELECT cache_revision, updated_at FROM p115_mediainfo_cache WHERE sha1 = :sha1"),
                {"sha1": SHA_A},
            )
            .mappings()
            .one()
        )
        connection.execute(
            text(
                "UPDATE p115_mediainfo_cache SET raw_ffprobe_json = jsonb_build_object('version', 1) WHERE sha1 = :sha1"
            ),
            {"sha1": SHA_A},
        )
        after_raw_revision = connection.scalar(
            text("SELECT cache_revision FROM p115_mediainfo_cache WHERE sha1 = :sha1"),
            {"sha1": SHA_A},
        )
        connection.execute(
            text(
                """
                UPDATE p115_mediainfo_cache
                SET mediainfo_json = jsonb_build_object('version', 2),
                    cache_revision = 3
                WHERE sha1 = :sha1
                """
            ),
            {"sha1": SHA_A},
        )
        final = (
            connection.execute(
                text("SELECT cache_revision, updated_at FROM p115_mediainfo_cache WHERE sha1 = :sha1"),
                {"sha1": SHA_A},
            )
            .mappings()
            .one()
        )

    assert after_formatted["cache_revision"] == 1
    assert str(after_formatted["updated_at"]) != "2000-01-01 00:00:00+00:00"
    assert after_raw_revision == 2
    assert final["cache_revision"] == 3
    assert final["updated_at"] >= after_formatted["updated_at"]


def test_new_cache_writes_reject_noncanonical_sha1(
    p6_runtime: DatabaseRuntime,
) -> None:
    with pytest.raises(IntegrityError):
        with p6_runtime.engine.begin() as connection:
            connection.execute(
                text("INSERT INTO p115_mediainfo_cache (sha1) VALUES (:sha1)"),
                {"sha1": SHA_A.lower()},
            )


def test_pick_code_resolution_is_account_bound_and_ambiguous_rows_fail_closed(
    p6_runtime: DatabaseRuntime,
) -> None:
    with p6_runtime.engine.begin() as connection:
        for file_id, sha1 in (("9001", SHA_A), ("9002", SHA_B)):
            connection.execute(
                text(
                    """
                    INSERT INTO p115_filesystem_cache (
                        id, parent_id, name, sha1, pick_code,
                        size, account_id, entry_kind
                    )
                    VALUES (
                        :file_id, '8001', :name, :sha1, 'pc-shared',
                        1024, '115001', 'file'
                    )
                    """
                ),
                {"file_id": file_id, "name": f"{file_id}.mkv", "sha1": sha1},
            )
    repository = _repository(p6_runtime)

    assert repository.resolve_sha1_by_pick_code("pc-shared", account_id="115001") is None
    assert repository.resolve_sha1_by_pick_code("pc-shared", account_id="115002") is None
    with pytest.raises(P115FingerprintConflict, match="ambiguous"):
        repository.find_fingerprint(account_id="115001", pick_code="pc-shared")


def test_duplicate_sha1_does_not_override_strong_fingerprint_identity_or_block_commit(
    p6_runtime: DatabaseRuntime,
) -> None:
    _insert_media_row(p6_runtime)
    with p6_runtime.engine.begin() as connection:
        for file_id, parent_id, name, pick_code, local_path in (
            ("9001", "8001", "A.mkv", "pc-a", "Movies/A.mkv"),
            ("9002", "8002", "B.mkv", "pc-b", "Movies/B.mkv"),
        ):
            connection.execute(
                text(
                    """
                    INSERT INTO p115_filesystem_cache (
                        id, parent_id, name, local_path, sha1, pick_code,
                        size, account_id, entry_kind
                    ) VALUES (
                        :file_id, :parent_id, :name, :local_path, :sha1, :pick_code,
                        1024, '115001', 'file'
                    )
                    """
                ),
                {
                    "file_id": file_id,
                    "parent_id": parent_id,
                    "name": name,
                    "local_path": local_path,
                    "sha1": SHA_A,
                    "pick_code": pick_code,
                },
            )
    repository = _repository(p6_runtime)

    by_pick_code = repository.find_fingerprint(account_id="115001", pick_code="pc-a", sha1=SHA_A)
    by_file_id = repository.find_fingerprint(account_id="115001", file_id="9002", sha1=SHA_A)

    assert by_pick_code is not None and by_pick_code.file_id == "9001"
    assert by_file_id is not None and by_file_id.file_id == "9002"
    with pytest.raises(P115FingerprintConflict, match="ambiguous"):
        repository.find_fingerprint(account_id="115001", sha1=SHA_A)

    row = repository.list_repair_rows()[0]
    candidate = _fingerprint(
        file_id="9003",
        parent_id="8003",
        name="C.mkv",
        sha1=SHA_A,
        pick_code="pc-c",
        local_path="Movies/C.mkv",
    )
    assert repository.commit_repair(row, 0, candidate, account_id="115001") is True

    with p6_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT count(*) FROM p115_filesystem_cache WHERE sha1 = :sha1"),
                {"sha1": SHA_A},
            )
            == 3
        )


def test_fingerprint_repairs_preserve_positions_across_multiple_atomic_commits(
    p6_runtime: DatabaseRuntime,
) -> None:
    _insert_media_row(p6_runtime)
    repository = _repository(p6_runtime)
    row = repository.list_repair_rows()[0]
    first = _fingerprint(
        file_id="9001",
        parent_id="8001",
        name="A.mkv",
        sha1=SHA_A,
        pick_code="pc-a",
        local_path="Movies/A.mkv",
    )
    second = _fingerprint(
        file_id="9002",
        parent_id="8001",
        name="B.mkv",
        sha1=SHA_B,
        pick_code="pc-b",
        local_path="Movies/B.mkv",
    )

    assert repository.commit_repair(row, 0, first, account_id="115001") is True
    assert repository.commit_repair(row, 1, second, account_id="115001") is True

    with p6_runtime.engine.connect() as connection:
        media = (
            connection.execute(
                text(
                    """
                SELECT file_sha1_json, file_pickcode_json
                FROM media_metadata
                WHERE tmdb_id = '101' AND item_type = 'Movie'
                """
                )
            )
            .mappings()
            .one()
        )
        cache = (
            connection.execute(
                text(
                    """
                SELECT id, account_id, entry_kind, sha1, pick_code, local_path
                FROM p115_filesystem_cache
                ORDER BY id
                """
                )
            )
            .mappings()
            .all()
        )
    assert media["file_sha1_json"] == [SHA_A, SHA_B]
    assert media["file_pickcode_json"] == ["pc-a", "pc-b"]
    assert [dict(value) for value in cache] == [
        {
            "id": "9001",
            "account_id": "115001",
            "entry_kind": "file",
            "sha1": SHA_A,
            "pick_code": "pc-a",
            "local_path": "Movies/A.mkv",
        },
        {
            "id": "9002",
            "account_id": "115001",
            "entry_kind": "file",
            "sha1": SHA_B,
            "pick_code": "pc-b",
            "local_path": "Movies/B.mkv",
        },
    ]


def test_fingerprint_account_drift_rolls_back_cache_and_media_write(
    p6_runtime: DatabaseRuntime,
) -> None:
    _insert_media_row(p6_runtime)
    repository = _repository(p6_runtime)
    row = repository.list_repair_rows()[0]
    value = _fingerprint(
        file_id="9001",
        parent_id="8001",
        name="A.mkv",
        sha1=SHA_A,
        pick_code="pc-a",
        local_path="Movies/A.mkv",
    )
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE app_settings
                SET value_json = '{"account_id":"115002"}'::jsonb
                WHERE setting_key = 'p115_auth_tokens'
                """
            )
        )

    with pytest.raises(P115FingerprintAccountChanged):
        repository.commit_repair(row, 0, value, account_id="115001")

    with p6_runtime.engine.connect() as connection:
        cache_count = connection.scalar(text("SELECT count(*) FROM p115_filesystem_cache"))
        arrays = (
            connection.execute(
                text(
                    """
                SELECT file_sha1_json, file_pickcode_json
                FROM media_metadata
                WHERE tmdb_id = '101' AND item_type = 'Movie'
                """
                )
            )
            .mappings()
            .one()
        )
    assert cache_count == 0
    assert arrays["file_sha1_json"] == ["", ""]
    assert arrays["file_pickcode_json"] == ["", ""]


def test_fingerprint_slot_conflict_does_not_partially_write_cache(
    p6_runtime: DatabaseRuntime,
) -> None:
    _insert_media_row(p6_runtime)
    repository = _repository(p6_runtime)
    row = repository.list_repair_rows()[0]
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE media_metadata
                SET file_sha1_json = CAST(:sha1s AS jsonb)
                WHERE tmdb_id = '101' AND item_type = 'Movie'
                """
            ),
            {"sha1s": json.dumps([SHA_B, ""])},
        )
    value = _fingerprint(
        file_id="9001",
        parent_id="8001",
        name="A.mkv",
        sha1=SHA_A,
        pick_code="pc-a",
        local_path="Movies/A.mkv",
    )

    with pytest.raises(P115FingerprintConflict, match="SHA1 slot"):
        repository.commit_repair(row, 0, value, account_id="115001")

    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM p115_filesystem_cache")) == 0


def test_restore_targets_preserve_source_path_and_provider_file_name(
    p6_runtime: DatabaseRuntime,
) -> None:
    _insert_restore_row(p6_runtime)

    targets = _repository(p6_runtime).list_restore_targets(account_id="115001")

    assert len(targets) == 1
    target = targets[0]
    assert target.path == "/library/Restore.Fixture.mkv"
    assert target.file_name == "Provider.Restore.Fixture.mkv"
    assert target.file_size == 4096
    assert target.sha1 == SHA_A
    assert target.pick_code == "pc-restore"


def test_probe_worklist_claim_reclaim_finish_and_terminal_replay(
    p6_runtime: DatabaseRuntime,
) -> None:
    _insert_restore_row(p6_runtime)
    repository = _repository(p6_runtime)
    target = repository.list_restore_targets(account_id="115001")[0]
    operation_key = "restore-job-fixture"

    first = repository.reserve_probe(
        operation_key=operation_key,
        provenance_key=PROVENANCE_A,
        source="restore_mediainfo",
        account_id="115001",
        target=target,
    )
    active_replay = repository.reserve_probe(
        operation_key=operation_key,
        provenance_key=PROVENANCE_A,
        source="restore_mediainfo",
        account_id="115001",
        target=target,
    )

    assert first.claimed is True
    assert first.attempts == 1
    assert active_replay.status is P115MediaProbeItemStatus.PROBING
    assert active_replay.claimed is False
    assert active_replay.attempts == 1

    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO p115_mediainfo_cache (
                    sha1, mediainfo_json, raw_ffprobe_json, cache_revision
                ) VALUES (
                    :sha1, jsonb_build_object('version', 4), jsonb_build_object('version', 4), 4
                )
                """
            ),
            {"sha1": SHA_A},
        )
        connection.execute(
            text(
                """
                UPDATE p115_media_probe_items
                SET lease_expires_at = NOW() - INTERVAL '1 second'
                WHERE id = :id
                """
            ),
            {"id": first.id},
        )

    reclaimed = repository.reserve_probe(
        operation_key=operation_key,
        provenance_key=PROVENANCE_A,
        source="restore_mediainfo",
        account_id="115001",
        target=target,
    )
    assert reclaimed.claimed is True
    assert reclaimed.claim_token != first.claim_token
    assert reclaimed.attempts == 2
    assert reclaimed.cache_revision == 4

    with pytest.raises(RuntimeError, match="claim changed"):
        repository.finish_probe(first, status=P115MediaProbeItemStatus.RETRYABLE)

    repository.finish_probe(
        reclaimed,
        status=P115MediaProbeItemStatus.RETRYABLE,
        provider_backend="openapi",
        error={"code": "fixture_retry"},
    )
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE p115_mediainfo_cache
                SET mediainfo_json = jsonb_build_object('version', 5)
                WHERE sha1 = :sha1
                """
            ),
            {"sha1": SHA_A},
        )
    retry = repository.reserve_probe(
        operation_key=operation_key,
        provenance_key=PROVENANCE_A,
        source="restore_mediainfo",
        account_id="115001",
        target=target,
    )
    assert retry.claimed is True
    assert retry.attempts == 3
    assert retry.cache_revision == 5

    repository.finish_probe(
        retry,
        status=P115MediaProbeItemStatus.REJECTED,
        provider_backend="openapi",
        result={"probe_method": "ffprobe"},
    )
    terminal_replay = repository.reserve_probe(
        operation_key=operation_key,
        provenance_key=PROVENANCE_A,
        source="restore_mediainfo",
        account_id="115001",
        target=target,
    )
    assert terminal_replay.status is P115MediaProbeItemStatus.REJECTED
    assert terminal_replay.claimed is False
    assert terminal_replay.attempts == 3

    with pytest.raises(P115FingerprintConflict, match="operation identity"):
        repository.reserve_probe(
            operation_key=operation_key,
            provenance_key=PROVENANCE_A,
            source="restore_mediainfo",
            account_id="115001",
            target=replace(target, pick_code="pc-other"),
        )

    with p6_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                    SELECT status, attempts, claim_token, lease_expires_at,
                           provider_backend, result_json, last_error_json, finished_at
                    FROM p115_media_probe_items
                    WHERE operation_key = :operation_key
                      AND sha1 = :sha1
                      AND provenance_key = :provenance_key
                    """
                ),
                {
                    "operation_key": operation_key,
                    "sha1": SHA_A,
                    "provenance_key": PROVENANCE_A,
                },
            )
            .mappings()
            .one()
        )
    assert row["status"] == "rejected"
    assert row["attempts"] == 3
    assert row["claim_token"] is None
    assert row["lease_expires_at"] is None
    assert row["provider_backend"] == "openapi"
    assert row["result_json"] == {"probe_method": "ffprobe"}
    assert row["last_error_json"] is None
    assert row["finished_at"] is not None


def test_strm_probe_confirmation_atomically_inserts_then_reuses_cache(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    target = P115MediaProbeRequest(
        account_id="115001",
        sha1=SHA_A,
        pick_code="pc-strm",
        file_name="Fixture.mkv",
        file_size=1024,
    )
    generated = repository.reserve_probe(
        operation_key="strm-generated",
        provenance_key=PROVENANCE_A,
        source="strm_sync",
        account_id="115001",
        target=target,
    )

    revision = repository.confirm_probe(
        generated,
        account_id="115001",
        sha1=SHA_A,
        pick_code="pc-strm",
        provider_backend="openapi",
        result={"probe_method": "ffprobe", "cache_reused": False},
        formatted=[{"MediaSourceInfo": {"MediaStreams": []}, "Chapters": []}],
        raw={"streams": [], "_etk": {"sha1": SHA_A}},
        formatter_revision="fixture-formatter-v1",
        probe_revision="fixture-probe-v1",
    )

    assert revision == 1
    replay = repository.reserve_probe(
        operation_key="strm-generated",
        provenance_key=PROVENANCE_A,
        source="strm_sync",
        account_id="115001",
        target=target,
    )
    assert replay.status is P115MediaProbeItemStatus.CONFIRMED
    assert replay.result == {
        "probe_method": "ffprobe",
        "cache_reused": False,
        "cache_revision": 1,
        "source": "strm_sync",
    }

    reused = repository.reserve_probe(
        operation_key="strm-reuse",
        provenance_key=PROVENANCE_A,
        source="strm_sync",
        account_id="115001",
        target=target,
    )
    assert reused.cache_revision == 1
    reused_revision = repository.confirm_probe(
        reused,
        account_id="115001",
        sha1=SHA_A,
        pick_code="pc-strm",
        provider_backend="cache",
        result={"probe_method": "cache", "cache_reused": True},
    )
    assert reused_revision == 1

    with p6_runtime.engine.connect() as connection:
        cache = (
            connection.execute(
                text(
                    """
                    SELECT cache_revision, formatter_revision, probe_revision,
                           mediainfo_json, raw_ffprobe_json
                    FROM p115_mediainfo_cache
                    WHERE sha1 = :sha1
                    """
                ),
                {"sha1": SHA_A},
            )
            .mappings()
            .one()
        )
        work = (
            connection.execute(
                text(
                    """
                    SELECT operation_key, status, provider_backend, result_json
                    FROM p115_media_probe_items
                    WHERE sha1 = :sha1
                    ORDER BY operation_key
                    """
                ),
                {"sha1": SHA_A},
            )
            .mappings()
            .all()
        )
    assert cache["cache_revision"] == 1
    assert cache["formatter_revision"] == "fixture-formatter-v1"
    assert cache["probe_revision"] == "fixture-probe-v1"
    assert cache["raw_ffprobe_json"]["_etk"]["sha1"] == SHA_A
    assert [(row["operation_key"], row["status"], row["provider_backend"]) for row in work] == [
        ("strm-generated", "confirmed", "openapi"),
        ("strm-reuse", "confirmed", "cache"),
    ]
    assert work[1]["result_json"]["cache_reused"] is True


def test_confirmed_probe_replay_is_fenced_by_cache_evidence_and_provenance(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    target = P115MediaProbeRequest(
        account_id="115001",
        sha1=SHA_A,
        pick_code="pc-strm",
        file_name="Fixture.mkv",
    )
    operation_key = "strm-provenance-fence"
    first = repository.reserve_probe(
        operation_key=operation_key,
        provenance_key=PROVENANCE_A,
        source="strm_sync",
        account_id="115001",
        target=target,
    )
    repository.confirm_probe(
        first,
        account_id="115001",
        sha1=SHA_A,
        pick_code="pc-strm",
        provider_backend="openapi",
        formatted=[{"MediaSourceInfo": {"MediaStreams": []}, "Chapters": []}],
        raw={"streams": [], "_etk": {"sha1": SHA_A}},
        formatter_revision="fixture-formatter-v1",
        probe_revision="fixture-probe-v1",
    )

    unchanged = repository.reserve_probe(
        operation_key=operation_key,
        provenance_key=PROVENANCE_A,
        source="strm_sync",
        account_id="115001",
        target=target,
    )
    assert unchanged.status is P115MediaProbeItemStatus.CONFIRMED
    assert unchanged.claimed is False

    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE p115_mediainfo_cache
                SET mediainfo_json = '[{"MediaSourceInfo":{"MediaStreams":[{"Type":"Video"}]} }]'::jsonb
                WHERE sha1 = :sha1
                """
            ),
            {"sha1": SHA_A},
        )
    changed = repository.reserve_probe(
        operation_key=operation_key,
        provenance_key=PROVENANCE_A,
        source="strm_sync",
        account_id="115001",
        target=target,
    )
    assert changed.claimed is True
    assert changed.attempts == 2
    assert changed.cache_revision == 2
    repository.finish_probe(changed, status=P115MediaProbeItemStatus.RETRYABLE)

    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE p115_mediainfo_cache SET mediainfo_json = NULL WHERE sha1 = :sha1"),
            {"sha1": SHA_A},
        )
    unformatted = repository.reserve_probe(
        operation_key=operation_key,
        provenance_key=PROVENANCE_A,
        source="strm_sync",
        account_id="115001",
        target=target,
    )
    assert unformatted.claimed is True
    assert unformatted.attempts == 3
    assert unformatted.cache_revision == 3

    new_generation = repository.reserve_probe(
        operation_key=operation_key,
        provenance_key=PROVENANCE_B,
        source="strm_sync",
        account_id="115001",
        target=target,
    )
    assert new_generation.claimed is True
    assert new_generation.attempts == 1
    assert new_generation.id != unformatted.id

    with p6_runtime.engine.connect() as connection:
        generations = connection.scalar(
            text(
                """
                SELECT count(*)
                FROM p115_media_probe_items
                WHERE operation_key = :operation_key AND sha1 = :sha1
                """
            ),
            {"operation_key": operation_key, "sha1": SHA_A},
        )
    assert generations == 2


def test_strm_probe_confirmation_account_drift_rolls_back_cache_and_worklist(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    target = P115MediaProbeRequest(
        account_id="115001",
        sha1=SHA_A,
        pick_code="pc-strm",
        file_name="Fixture.mkv",
    )
    reservation = repository.reserve_probe(
        operation_key="strm-account-drift",
        provenance_key=PROVENANCE_A,
        source="strm_sync",
        account_id="115001",
        target=target,
    )
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE app_settings
                SET value_json = '{"account_id":"115002"}'::jsonb
                WHERE setting_key = 'p115_auth_tokens'
                """
            )
        )

    with pytest.raises(P115FingerprintAccountChanged):
        repository.confirm_probe(
            reservation,
            account_id="115001",
            sha1=SHA_A,
            pick_code="pc-strm",
            provider_backend="openapi",
            formatted=[{"MediaSourceInfo": {"MediaStreams": []}, "Chapters": []}],
            raw={"streams": []},
            formatter_revision="fixture-formatter-v1",
            probe_revision="fixture-probe-v1",
        )

    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM p115_mediainfo_cache")) == 0
        work = (
            connection.execute(
                text(
                    """
                    SELECT status, claim_token IS NOT NULL AS has_claim
                    FROM p115_media_probe_items
                    WHERE operation_key = 'strm-account-drift'
                    """
                )
            )
            .mappings()
            .one()
        )
    assert work == {"status": "probing", "has_claim": True}
