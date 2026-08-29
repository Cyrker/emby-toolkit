from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text

from etk.domains.p115.washing_evaluator import (
    WashingOrganizeBackfill,
    build_washing_policy_snapshot,
    evaluate_washing_item,
)
from etk.domains.p115.washing_priority import P115_WASHING_PRIORITY_RECALCULATE_SETTING
from etk.domains.p115.washing_priority_repository import SqlAlchemyP115WashingPriorityRepository
from etk.domains.p115.washing_recalculation import P115WashingPriorityRecalculationService
from etk.domains.p115.washing_recalculation_repository import (
    SqlAlchemyP115WashingRecalculationRepository,
    WashingRecalculationDrift,
)
from etk.platform import Settings
from etk.platform.db import DatabaseRuntime, EngineConfig, SqlAlchemyUnitOfWork

TMDB_ID = "9911711"
CACHE_ID = "p11-washing-recalculation-file"
PICK_CODE = "p11-washing-recalculation-pc"
SHA1 = "A11711A11711A11711A11711A11711A11711A117"
NOW = datetime(2026, 8, 29, 9, 0, tzinfo=UTC)


class FakeRuntimeGateway:
    def episode_runtime_minutes(
        self,
        series_tmdb_id: str,
        season_number: int,
        episode_number: int,
    ) -> float | None:
        raise AssertionError(f"Movie fixture must not query TMDb: {series_tmdb_id} S{season_number}E{episode_number}")


class RecordingProgress:
    def __init__(self) -> None:
        self.updates: list[tuple[int, str]] = []
        self.checkpoints = 0

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        self.checkpoints += 1


def test_washing_recalculation_updates_snapshots_and_acks_revision_on_postgres18(
    migrated_p6_postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = DatabaseRuntime.create(
        EngineConfig(
            migrated_p6_postgres_url,
            application_name="etk-p11-washing-recalculation",
        )
    )
    setting_keys = (
        "p115_washing_priority_config",
        "p115_rename_config",
        "language_mapping",
        "release_group_mapping",
        "p115_sorting_rules",
        P115_WASHING_PRIORITY_RECALCULATE_SETTING,
    )
    setting_snapshots: list[dict[str, Any]] = []
    group_snapshots: list[dict[str, Any]] = []
    sequence_snapshot: dict[str, Any] | None = None
    mutated = False
    try:
        runtime.start()
        with runtime.engine.begin() as connection:
            assert int(connection.scalar(text("SHOW server_version_num")) or 0) // 10000 == 18
            setting_snapshots = [
                dict(row)
                for row in connection.execute(
                    text(
                        """
                        SELECT setting_key, value_json, last_updated_at
                        FROM app_settings
                        WHERE setting_key = ANY(CAST(:keys AS text[]))
                        """
                    ),
                    {"keys": list(setting_keys)},
                ).mappings()
            ]
            group_snapshots = [
                dict(row)
                for row in connection.execute(
                    text(
                        """
                        SELECT id, name, media_type, target_cids, priorities, sort_order
                        FROM washing_priority_groups
                        ORDER BY id
                        """
                    )
                ).mappings()
            ]
            sequence_snapshot = dict(
                connection.execute(text("SELECT last_value, is_called FROM washing_priority_groups_id_seq"))
                .mappings()
                .one()
            )
            mutated = True
            _delete_fixture_rows(connection)
            connection.execute(text("DELETE FROM washing_priority_groups"))
            connection.execute(
                text("DELETE FROM app_settings WHERE setting_key = ANY(CAST(:keys AS text[]))"),
                {"keys": list(setting_keys)},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json)
                    VALUES
                        (
                            'p115_washing_priority_config',
                            CAST(:config AS jsonb)
                        ),
                        (
                            :state_key,
                            CAST(:state AS jsonb)
                        )
                    """
                ),
                {
                    "state_key": P115_WASHING_PRIORITY_RECALCULATE_SETTING,
                    "config": json.dumps({"version_slots_enabled": False, "version_slots": []}),
                    "state": json.dumps(
                        {
                            "revision": 7,
                            "media_types": {"Movie": {"all": False, "target_cids": ["100"]}},
                        }
                    ),
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO washing_priority_groups (
                        name,
                        media_type,
                        target_cids,
                        priorities,
                        sort_order
                    )
                    VALUES (
                        'P11 fixture',
                        'Movie',
                        CAST(:target_cids AS jsonb),
                        CAST(:priorities AS jsonb),
                        0
                    )
                    """
                ),
                {
                    "target_cids": json.dumps(["100"]),
                    "priorities": json.dumps([{"resolution": ["1080p"], "audio": ["chi", "eng"]}]),
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO media_metadata (
                        tmdb_id,
                        item_type,
                        title,
                        original_language,
                        in_library,
                        file_sha1_json,
                        file_pickcode_json,
                        asset_details_json,
                        washing_snapshot_json
                    )
                    VALUES (
                        :tmdb_id,
                        'Movie',
                        'P11 Washing Fixture',
                        'eng',
                        TRUE,
                        CAST(:sha1s AS jsonb),
                        CAST(:pick_codes AS jsonb),
                        '[]'::jsonb,
                        CAST(:snapshot AS jsonb)
                    )
                    """
                ),
                {
                    "tmdb_id": TMDB_ID,
                    "sha1s": json.dumps([SHA1]),
                    "pick_codes": json.dumps([PICK_CODE]),
                    "snapshot": json.dumps({"target_cid": "100"}),
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO p115_filesystem_cache (
                        id,
                        parent_id,
                        name,
                        local_path,
                        sha1,
                        pick_code,
                        size,
                        account_id,
                        entry_kind,
                        washing_snapshot_json
                    )
                    VALUES (
                        :cache_id,
                        'p11-parent',
                        'P11.Washing.1080p.WEB-DL-GROUP.mkv',
                        '/mnt/media/Movies/P11.Washing.1080p.WEB-DL-GROUP.mkv',
                        :sha1,
                        :pick_code,
                        :size,
                        '115001',
                        'file',
                        CAST(:snapshot AS jsonb)
                    )
                    """
                ),
                {
                    "cache_id": CACHE_ID,
                    "sha1": SHA1,
                    "pick_code": PICK_CODE,
                    "size": 10 * 1024**3,
                    "snapshot": json.dumps({"target_cid": "100"}),
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO p115_organize_records (
                        file_id,
                        pick_code,
                        original_name,
                        status,
                        target_cid
                    )
                    VALUES (:file_id, :pick_code, 'P11 Washing Fixture', 'success', '100')
                    """
                ),
                {"file_id": CACHE_ID, "pick_code": PICK_CODE},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO p115_mediainfo_cache (sha1, mediainfo_json)
                    VALUES (:sha1, CAST(:mediainfo AS jsonb))
                    """
                ),
                {"sha1": SHA1, "mediainfo": json.dumps(_mediainfo())},
            )
        recalculation_repository = SqlAlchemyP115WashingRecalculationRepository(runtime.unit_of_work)
        state_repository = SqlAlchemyP115WashingPriorityRepository(runtime.unit_of_work)
        service = P115WashingPriorityRecalculationService(
            repository=recalculation_repository,
            state=state_repository,
            settings=lambda: Settings(
                local_strm_root="/mnt/media",
                p115_media_root_name="媒体库",
            ),
            episode_runtime=FakeRuntimeGateway(),
            clock=lambda: NOW,
        )
        progress = RecordingProgress()
        result = service.execute(
            progress,
            item_type="movie",
            affected_scope={"Movie": {"all": False, "target_cids": ["100"]}},
            scope_revision=7,
        )

        assert result.partial is False
        assert result.stats["scanned_items"] == 1
        assert result.stats["updated_items"] == 1
        assert result.stats["evaluated_versions"] == 1
        assert result.stats["scope_acknowledged"] is True
        assert progress.checkpoints == 1
        with runtime.engine.connect() as connection:
            media = (
                connection.execute(
                    text(
                        """
                    SELECT washing_level, washing_snapshot_json
                    FROM media_metadata
                    WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'
                    """
                    ),
                    {"tmdb_id": TMDB_ID},
                )
                .mappings()
                .one()
            )
            cache = (
                connection.execute(
                    text(
                        """
                    SELECT washing_level, washing_snapshot_json
                    FROM p115_filesystem_cache
                    WHERE id = :cache_id
                    """
                    ),
                    {"cache_id": CACHE_ID},
                )
                .mappings()
                .one()
            )
        assert media["washing_level"] == cache["washing_level"] == 1
        assert media["washing_snapshot_json"]["versions"][0]["target_cid"] == "100"
        assert cache["washing_snapshot_json"]["target_cid"] == "100"
        assert state_repository.load_recalculate_state() == {
            "revision": 7,
            "media_types": {},
        }

        with runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE app_settings
                    SET value_json = CAST(:value AS jsonb)
                    WHERE setting_key = :key
                    """
                ),
                {
                    "key": P115_WASHING_PRIORITY_RECALCULATE_SETTING,
                    "value": json.dumps(
                        {
                            "revision": 8,
                            "media_types": {"Movie": {"all": True, "target_cids": []}},
                        }
                    ),
                },
            )
        stale = service.execute(
            RecordingProgress(),
            item_type="movie",
            affected_scope={"Movie": {"all": True, "target_cids": []}},
            scope_revision=7,
        )
        assert stale.stats["scope_acknowledged"] is False
        assert state_repository.load_recalculate_state()["revision"] == 8

        loaded = recalculation_repository.load_item(TMDB_ID, "Movie")
        assert loaded is not None
        policy_settings, groups = recalculation_repository.load_policy_source()
        mutation = evaluate_washing_item(
            loaded,
            build_washing_policy_snapshot(
                settings=policy_settings,
                groups=groups,
                local_root="/mnt/media",
                media_root_name="媒体库",
            ),
            evaluated_at=NOW + timedelta(minutes=1),
        )
        mutation_with_failed_backfill = replace(
            mutation,
            organize_backfills=(
                WashingOrganizeBackfill(
                    cache_id=CACHE_ID,
                    pick_code=PICK_CODE,
                    original_name="P11 Washing Fixture",
                    target_cid="200",
                    category_name="Movies",
                ),
            ),
        )

        def fail_backfill_with_database_error(
            uow: SqlAlchemyUnitOfWork,
            _item: object,
            _backfill: object,
        ) -> bool:
            uow.session.execute(text("SELECT 1 / 0"))
            return False

        with monkeypatch.context() as patch:
            patch.setattr(
                SqlAlchemyP115WashingRecalculationRepository,
                "_backfill_target_cid",
                staticmethod(fail_backfill_with_database_error),
            )
            assert recalculation_repository.persist_item(loaded, mutation_with_failed_backfill) == 0

        with runtime.engine.connect() as connection:
            timestamps = dict(
                connection.execute(
                    text(
                        """
                        SELECT
                            (
                                SELECT washing_snapshot_json->>'evaluated_at'
                                FROM media_metadata
                                WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'
                            ) AS media,
                            (
                                SELECT washing_snapshot_json->>'evaluated_at'
                                FROM p115_filesystem_cache
                                WHERE id = :cache_id
                            ) AS cache
                    """
                    ),
                    {"tmdb_id": TMDB_ID, "cache_id": CACHE_ID},
                )
                .mappings()
                .one()
            )
        assert timestamps == {"media": "2026-08-29T09:01:00Z", "cache": "2026-08-29T09:01:00Z"}

        with runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET in_library = FALSE
                    WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'
                    """
                ),
                {"tmdb_id": TMDB_ID},
            )
        with pytest.raises(WashingRecalculationDrift, match="disappeared"):
            recalculation_repository.persist_item(loaded, mutation)
        with runtime.engine.connect() as connection:
            persisted_timestamp = connection.scalar(
                text(
                    """
                    SELECT washing_snapshot_json->>'evaluated_at'
                    FROM media_metadata
                    WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'
                    """
                ),
                {"tmdb_id": TMDB_ID},
            )
        assert persisted_timestamp == "2026-08-29T09:01:00Z"
    finally:
        try:
            if mutated:
                with runtime.engine.begin() as connection:
                    _delete_fixture_rows(connection)
                    connection.execute(text("DELETE FROM washing_priority_groups"))
                    for snapshot in group_snapshots:
                        connection.execute(
                            text(
                                """
                                INSERT INTO washing_priority_groups (
                                    id,
                                    name,
                                    media_type,
                                    target_cids,
                                    priorities,
                                    sort_order
                                )
                                VALUES (
                                    :id,
                                    :name,
                                    :media_type,
                                    CAST(:target_cids AS jsonb),
                                    CAST(:priorities AS jsonb),
                                    :sort_order
                                )
                                """
                            ),
                            {
                                **snapshot,
                                "target_cids": json.dumps(snapshot["target_cids"]),
                                "priorities": json.dumps(snapshot["priorities"]),
                            },
                        )
                    connection.execute(
                        text("DELETE FROM app_settings WHERE setting_key = ANY(CAST(:keys AS text[]))"),
                        {"keys": list(setting_keys)},
                    )
                    for snapshot in setting_snapshots:
                        connection.execute(
                            text(
                                """
                                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                                VALUES (:setting_key, CAST(:value_json AS jsonb), :last_updated_at)
                                """
                            ),
                            {
                                **snapshot,
                                "value_json": json.dumps(snapshot["value_json"]),
                            },
                        )
                    if sequence_snapshot is not None:
                        connection.execute(
                            text(
                                """
                                SELECT setval(
                                    'washing_priority_groups_id_seq',
                                    :last_value,
                                    :is_called
                                )
                                """
                            ),
                            sequence_snapshot,
                        )
        finally:
            runtime.shutdown()


def _delete_fixture_rows(connection: Any) -> None:
    connection.execute(
        text("DELETE FROM p115_organize_records WHERE file_id = :file_id OR pick_code = :pick_code"),
        {"file_id": CACHE_ID, "pick_code": PICK_CODE},
    )
    connection.execute(
        text("DELETE FROM p115_filesystem_cache WHERE id = :cache_id"),
        {"cache_id": CACHE_ID},
    )
    connection.execute(
        text("DELETE FROM p115_mediainfo_cache WHERE sha1 = :sha1"),
        {"sha1": SHA1},
    )
    connection.execute(
        text("DELETE FROM media_metadata WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'"),
        {"tmdb_id": TMDB_ID},
    )


def _mediainfo() -> list[dict[str, Any]]:
    return [
        {
            "MediaSourceInfo": {
                "Size": 10 * 1024**3,
                "MediaStreams": [
                    {"Type": "Video", "Width": 1920, "Height": 1080, "Codec": "hevc"},
                    {"Type": "Audio", "Language": "chi"},
                ],
            }
        }
    ]
