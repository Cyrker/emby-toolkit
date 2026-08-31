from __future__ import annotations

import json

import pytest
from sqlalchemy import text

from etk.domains.p115 import (
    P115QuickDeployLock,
    SqlAlchemyP115MaintenanceRepository,
    SqlAlchemyP115QuickDeployConfigurationRepository,
)
from etk.platform.db import DatabaseRuntime


def test_maintenance_repository_scopes_unrecognized_deletion_to_account_parent_and_file_kind(
    p6_runtime: DatabaseRuntime,
) -> None:
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache (
                    id, parent_id, name, local_path, account_id, entry_kind
                ) VALUES
                    ('delete-me', '900', 'delete.mkv', '/900/delete.mkv', '115001', 'file'),
                    ('wrong-account', '900', 'account.mkv', '/900/account.mkv', '115002', 'file'),
                    ('wrong-parent', '901', 'parent.mkv', '/901/parent.mkv', '115001', 'file'),
                    ('keep-directory', '900', 'directory', '/900/directory', '115001', 'directory')
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_organize_records (file_id, original_name, status)
                VALUES
                    ('delete-me', 'delete.mkv', 'unrecognized'),
                    ('wrong-account', 'account.mkv', 'unrecognized'),
                    ('wrong-parent', 'parent.mkv', 'unrecognized'),
                    ('keep-directory', 'directory', 'unrecognized')
                """
            )
        )

    repository = SqlAlchemyP115MaintenanceRepository(p6_runtime.unit_of_work)

    assert repository.cached_unrecognized_file_ids(account_id="115001", parent_id="900") == frozenset({"delete-me"})
    assert (
        repository.delete_confirmed_unrecognized_facts(
            ["delete-me", "wrong-account", "wrong-parent", "keep-directory"],
            account_id="115001",
            parent_id="900",
        )
        == 1
    )

    with p6_runtime.engine.connect() as connection:
        cache_ids = set(
            connection.scalars(
                text(
                    """
                    SELECT id FROM p115_filesystem_cache
                    WHERE id IN ('delete-me', 'wrong-account', 'wrong-parent', 'keep-directory')
                    """
                )
            )
        )
        record_ids = set(
            connection.scalars(
                text(
                    """
                    SELECT file_id FROM p115_organize_records
                    WHERE file_id IN ('delete-me', 'wrong-account', 'wrong-parent', 'keep-directory')
                    """
                )
            )
        )
    assert cache_ids == {"wrong-account", "wrong-parent", "keep-directory"}
    assert record_ids == {"wrong-account", "wrong-parent", "keep-directory"}


def test_maintenance_speedtest_sample_uses_account_bound_cache_facts(p6_runtime: DatabaseRuntime) -> None:
    sha1 = "A" * 40
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, in_library,
                    file_pickcode_json, file_sha1_json, asset_details_json
                ) VALUES (
                    'maintenance-speedtest', 'Movie', 'Fixture Movie', TRUE,
                    CAST(:pick_codes AS jsonb), CAST(:sha1s AS jsonb), CAST(:assets AS jsonb)
                )
                """
            ),
            {
                "pick_codes": json.dumps(["pick-code-1"]),
                "sha1s": json.dumps([sha1]),
                "assets": json.dumps([{"file_name": "Fixture.Movie.mkv"}]),
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache (
                    id, parent_id, name, local_path, sha1, pick_code, size, account_id, entry_kind
                ) VALUES (
                    'speedtest-file', 'movies', 'Fixture.Movie.mkv', '/movies/Fixture.Movie.mkv',
                    :sha1, 'pick-code-1', 1048576, '115001', 'file'
                )
                """
            ),
            {"sha1": sha1},
        )

    repository = SqlAlchemyP115MaintenanceRepository(
        p6_runtime.unit_of_work,
        choice=lambda indexes: indexes[0],
    )

    sample = repository.speedtest_sample(account_id="115001")

    assert sample is not None
    assert sample["pick_code"] == "pick-code-1"
    assert sample["sha1"] == sha1
    assert sample["size"] == 1048576
    assert sample["file_name"] == "Fixture.Movie.mkv"


def test_quick_deploy_local_configuration_bundle_rolls_back_as_one_transaction(
    p6_runtime: DatabaseRuntime,
) -> None:
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json)
                VALUES
                    ('p115_auth_tokens', CAST(:auth AS jsonb)),
                    ('p115_rename_config', CAST(:rename AS jsonb))
                ON CONFLICT (setting_key) DO UPDATE SET
                    value_json = EXCLUDED.value_json,
                    last_updated_at = NOW()
                """
            ),
            {
                "auth": json.dumps({"account_id": "115001"}),
                "rename": json.dumps({"baseline": True}),
            },
        )
        connection.execute(text("DELETE FROM washing_priority_groups"))
        connection.execute(
            text(
                """
                INSERT INTO washing_priority_groups (
                    name, media_type, target_cids, priorities, sort_order
                ) VALUES (
                    'baseline', 'Movie', '[]'::jsonb, '[]'::jsonb, 0
                )
                """
            )
        )

    def snapshot() -> tuple[dict[str, object], list[dict[str, object]]]:
        with p6_runtime.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT setting_key, value_json
                    FROM app_settings
                    WHERE setting_key IN (
                        'p115_rename_config',
                        'p115_sorting_rules',
                        'p115_sorting_rules_account_id',
                        'p115_washing_priority_config',
                        'p115_washing_priority_recalculate_state'
                    )
                    """
                )
            ).mappings()
            settings = {str(row["setting_key"]): row["value_json"] for row in rows}
            groups = [
                dict(row)
                for row in connection.execute(
                    text(
                        """
                        SELECT name, media_type, target_cids, priorities, sort_order
                        FROM washing_priority_groups
                        ORDER BY sort_order
                        """
                    )
                ).mappings()
            ]
        return settings, groups

    before = snapshot()

    repository = SqlAlchemyP115QuickDeployConfigurationRepository(p6_runtime.unit_of_work)
    assert p6_runtime.raw_pool is not None
    lock = P115QuickDeployLock(p6_runtime.raw_pool.connection)

    with pytest.raises(KeyError, match="media_type"):
        with lock.hold():
            repository.save_bundle(
                rename_config={"replacement": True},
                sorting_rules=[{"name": "replacement"}],
                account_id="115001",
                washing_config={"conflict_mode": "replace"},
                washing_groups=[{"name": "invalid-last-step"}],
            )

    after = snapshot()

    assert after == before
    assert after[0]["p115_rename_config"] == {"baseline": True}
    assert after[1] == [
        {
            "name": "baseline",
            "media_type": "Movie",
            "target_cids": [],
            "priorities": [],
            "sort_order": 0,
        }
    ]
