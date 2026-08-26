from __future__ import annotations

import json

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etk.domains.p115 import (
    P115DirectoryCacheEntry,
    P115DirectoryRoot,
    SqlAlchemyP115DirectoryCacheRepository,
    SqlAlchemyP115EpisodeRegexRepository,
    SqlAlchemyP115RenameConfigRepository,
    SqlAlchemyP115SortingRulesRepository,
)
from etk.platform.db import DatabaseRuntime


def _entry(entry_id: str, root: str, name: str) -> P115DirectoryCacheEntry:
    return P115DirectoryCacheEntry(entry_id, root, name, f"Movies/{name}")


def test_postgres18_repository_reconciles_one_root_idempotently(
    p6_runtime: DatabaseRuntime,
) -> None:
    with p6_runtime.engine.begin() as connection:
        version = int(connection.scalar(text("SHOW server_version_num")) or 0)
        assert 180000 <= version < 190000
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache (
                    id,
                    parent_id,
                    name,
                    local_path,
                    sha1,
                    account_id,
                    entry_kind
                )
                VALUES
                    ('dir-existing', 'root-a', 'Old Name', 'old/Old Name', NULL, '115001', 'directory'),
                    ('stale', 'root-a', 'Stale', 'old/Stale', NULL, '115001', 'directory'),
                    ('same-root-file', 'root-a', 'movie.mkv', 'old/movie.mkv', 'ABC', '115001', 'file'),
                    ('retired-account', 'root-z', 'Retired', 'old/Retired', NULL, '115000', 'directory'),
                    ('other-root', 'root-b', 'Other', 'other/Other', 'DEF', '115001', 'file')
                """
            )
        )

    repository = SqlAlchemyP115DirectoryCacheRepository(p6_runtime.unit_of_work)
    root = P115DirectoryRoot("root-a", "Movies")
    entries = (_entry("dir-existing", "root-a", "Existing"), _entry("new", "root-a", "New"))

    first = repository.reconcile_children(root, entries, account_id="115001")
    second = repository.reconcile_children(root, entries, account_id="115001")

    assert first.cached == 2
    assert first.cleaned == 2
    assert second.cached == 2
    assert second.cleaned == 0
    with p6_runtime.engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT id, parent_id, name, local_path, account_id, entry_kind
                FROM p115_filesystem_cache
                ORDER BY parent_id, name
                """
            )
        ).mappings()
        assert [dict(row) for row in rows] == [
            {
                "id": "dir-existing",
                "parent_id": "root-a",
                "name": "Existing",
                "local_path": "Movies/Existing",
                "account_id": "115001",
                "entry_kind": "directory",
            },
            {
                "id": "new",
                "parent_id": "root-a",
                "name": "New",
                "local_path": "Movies/New",
                "account_id": "115001",
                "entry_kind": "directory",
            },
            {
                "id": "same-root-file",
                "parent_id": "root-a",
                "name": "movie.mkv",
                "local_path": "old/movie.mkv",
                "account_id": "115001",
                "entry_kind": "file",
            },
            {
                "id": "other-root",
                "parent_id": "root-b",
                "name": "Other",
                "local_path": "other/Other",
                "account_id": "115001",
                "entry_kind": "file",
            },
        ]


def test_postgres18_repository_rolls_back_the_whole_root_on_conflict(
    p6_runtime: DatabaseRuntime,
) -> None:
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache (
                    id,
                    parent_id,
                    name,
                    local_path,
                    account_id,
                    entry_kind
                )
                VALUES ('file-name-conflict', 'root-a', 'Second', 'other/Second', '115001', 'file')
                """
            )
        )

    repository = SqlAlchemyP115DirectoryCacheRepository(p6_runtime.unit_of_work)
    with pytest.raises(IntegrityError):
        repository.reconcile_children(
            P115DirectoryRoot("root-a", "Movies"),
            (
                _entry("would-have-been-written", "root-a", "First"),
                _entry("directory-name-conflict", "root-a", "Second"),
            ),
            account_id="115001",
        )

    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM p115_filesystem_cache WHERE parent_id = 'root-a'")) == 1
        assert (
            connection.scalar(
                text(
                    """
                SELECT COUNT(*)
                FROM p115_filesystem_cache
                WHERE id = 'file-name-conflict'
                  AND parent_id = 'root-a'
                  AND entry_kind = 'file'
                """
                )
            )
            == 1
        )


def test_postgres18_repository_rechecks_account_under_the_auth_lock(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = SqlAlchemyP115DirectoryCacheRepository(p6_runtime.unit_of_work)

    with pytest.raises(RuntimeError, match="账号在目录缓存提交前发生变化"):
        repository.reconcile_children(
            P115DirectoryRoot("root-a", "Movies"),
            (_entry("dir-a", "root-a", "A"),),
            account_id="115002",
        )

    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM p115_filesystem_cache")) == 0


def test_postgres18_sorting_rules_repository_atomically_binds_rules_and_account(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = SqlAlchemyP115SortingRulesRepository(p6_runtime.unit_of_work)
    rules = [
        {
            "id": "movies",
            "cid": "dir-movies",
            "category_path": "电影",
            "unknown": {"preserved": True},
        }
    ]

    saved = repository.save(rules, account_id="115001")
    loaded = repository.load()

    assert saved.account_id == "115001"
    assert loaded == saved
    assert list(loaded.rules) == rules
    with p6_runtime.engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT setting_key, value_json
                FROM app_settings
                WHERE setting_key IN (
                    'p115_sorting_rules',
                    'p115_sorting_rules_account_id'
                )
                ORDER BY setting_key
                """
            )
        ).mappings()
        assert [dict(row) for row in rows] == [
            {"setting_key": "p115_sorting_rules", "value_json": rules},
            {"setting_key": "p115_sorting_rules_account_id", "value_json": "115001"},
        ]


def test_postgres18_sorting_rules_repository_rolls_back_both_settings_on_auth_cas_failure(
    p6_runtime: DatabaseRuntime,
) -> None:
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                VALUES
                    ('p115_sorting_rules', '[{"id":"original"}]'::jsonb, NOW()),
                    ('p115_sorting_rules_account_id', '"115001"'::jsonb, NOW())
                """
            )
        )
    repository = SqlAlchemyP115SortingRulesRepository(p6_runtime.unit_of_work)

    with pytest.raises(RuntimeError, match="账号在分类规则提交前发生变化"):
        repository.save([{"id": "replacement"}], account_id="115002")

    with p6_runtime.engine.connect() as connection:
        values = dict(
            list(
                connection.execute(
                    text(
                        """
                        SELECT setting_key, value_json
                        FROM app_settings
                        WHERE setting_key IN (
                            'p115_sorting_rules',
                            'p115_sorting_rules_account_id'
                        )
                        """
                    )
                ).tuples()
            )
        )
    assert values == {
        "p115_sorting_rules": [{"id": "original"}],
        "p115_sorting_rules_account_id": "115001",
    }


def test_postgres18_sorting_rules_repository_loads_legacy_json_strings_and_invalid_values(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = SqlAlchemyP115SortingRulesRepository(p6_runtime.unit_of_work)
    legacy_rules = [{"id": "legacy", "cid": "dir-legacy", "extra": "kept"}]
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                VALUES
                    ('p115_sorting_rules', CAST(:rules AS jsonb), NOW()),
                    ('p115_sorting_rules_account_id', '"115001"'::jsonb, NOW())
                """
            ),
            {"rules": json.dumps(json.dumps(legacy_rules, ensure_ascii=False))},
        )

    loaded = repository.load()
    assert list(loaded.rules) == legacy_rules
    assert loaded.account_id == "115001"

    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE app_settings
                SET value_json = '"not-json"'::jsonb,
                    last_updated_at = NOW()
                WHERE setting_key = 'p115_sorting_rules'
                """
            )
        )
    assert repository.load().rules == ()


def test_postgres18_rename_config_repository_preserves_unknown_fields_and_revision(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = SqlAlchemyP115RenameConfigRepository(p6_runtime.unit_of_work)
    config = {
        "main_dir_template": "{{title}} {tmdb={{tmdbid}}}",
        "keep_original_name": False,
        "unknown": {"preserved": True},
    }

    saved = repository.save(config)
    loaded = repository.load()

    assert loaded == saved
    assert loaded.config == config
    assert loaded.revision == saved.revision
    with p6_runtime.engine.connect() as connection:
        assert (
            connection.scalar(text("SELECT value_json FROM app_settings WHERE setting_key = 'p115_rename_config'"))
            == config
        )


def test_postgres18_episode_regex_repository_preserves_ordered_validated_rules(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = SqlAlchemyP115EpisodeRegexRepository(p6_runtime.unit_of_work)
    rules = [
        {
            "id": "first",
            "enabled": True,
            "name": "First",
            "pattern": "EP(\\d+)",
            "mode": "episode_only",
            "season_group": 1,
            "episode_group": 1,
            "default_season": 1,
        },
        {
            "id": "second",
            "enabled": True,
            "name": "Second",
            "pattern": "S(\\d+)E(\\d+)",
            "mode": "season_episode",
            "season_group": 1,
            "episode_group": 2,
            "default_season": 1,
        },
    ]

    saved = repository.save(rules)
    loaded = repository.load()

    assert loaded == saved
    assert list(loaded.rules) == rules
    assert loaded.revision == saved.revision

    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE app_settings
                SET value_json = '{"not":"an array"}'::jsonb,
                    last_updated_at = NOW()
                WHERE setting_key = 'p115_episode_regex_rules'
                """
            )
        )
    assert repository.load().rules == ()
