from __future__ import annotations

import json
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
from pathlib import Path
from queue import Queue
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etk.platform.db import DatabaseRuntime, EngineConfig

REPO_ROOT = Path(__file__).resolve().parents[3]
MIGRATION_HEAD = "0024_p11_sharing_cancellation"
PREVIOUS_HEAD = "0020_p11_cleanup_config_cutover"
CONSTRAINT = "verification_p11_license_alias_cutover"
SETTING_KEYS = ("license_key", "pro_license_key")
LICENSE_ALIAS_CUTOVER_SQL = str(
    import_module("migrations.versions.0021_p11_license_alias_cutover").LICENSE_ALIAS_CUTOVER_SQL
)


@pytest.fixture
def license_runtime(migrated_postgres18_url: str) -> Iterator[DatabaseRuntime]:
    runtime = DatabaseRuntime.create(
        EngineConfig(migrated_postgres18_url, application_name="etk-p11-license-alias-cutover"),
    )
    runtime.start()
    try:
        yield runtime
    finally:
        runtime.shutdown()


def test_license_alias_cutover_accepts_only_strings_and_never_overwrites_canonical(
    license_runtime: DatabaseRuntime,
) -> None:
    prior = _snapshot_settings(license_runtime)
    try:
        _clear_settings(license_runtime)
        _execute_cutover(license_runtime)
        assert _setting_value(license_runtime, "pro_license_key") is None

        for ignored in ({"unexpected": True}, ["unexpected"], "   "):
            _put_setting(license_runtime, "license_key", ignored)
            _execute_cutover(license_runtime)
            assert _setting_value(license_runtime, "pro_license_key") is None

        _put_setting(license_runtime, "license_key", "ETK-Y-ALIAS")
        _execute_cutover(license_runtime)
        assert _setting_value(license_runtime, "pro_license_key") == "ETK-Y-ALIAS"
        assert _setting_value(license_runtime, "license_key") == "ETK-Y-ALIAS"

        _put_setting(license_runtime, "license_key", "ETK-L-NEW-ALIAS")
        _put_setting(license_runtime, "pro_license_key", "ETK-M-CANONICAL")
        _execute_cutover(license_runtime)
        assert _setting_value(license_runtime, "pro_license_key") == "ETK-M-CANONICAL"

        for empty in ("", "   ", None):
            _put_setting(license_runtime, "pro_license_key", empty)
            _execute_cutover(license_runtime)
            assert _setting_value(license_runtime, "pro_license_key") == "ETK-L-NEW-ALIAS"

        with license_runtime.engine.begin() as connection:
            connection.execute(text("UPDATE app_settings SET value_json = NULL WHERE setting_key = 'pro_license_key'"))
            connection.execute(text(LICENSE_ALIAS_CUTOVER_SQL))
        assert _setting_value(license_runtime, "pro_license_key") == "ETK-L-NEW-ALIAS"

        _put_setting(license_runtime, "pro_license_key", {"malformed": True})
        _execute_cutover(license_runtime)
        assert _setting_value(license_runtime, "pro_license_key") == {"malformed": True}

        connection = license_runtime.engine.connect()
        transaction = connection.begin()
        try:
            connection.execute(
                text(
                    """
                    UPDATE app_settings
                    SET value_json = '""'::jsonb, last_updated_at = NOW()
                    WHERE setting_key = 'pro_license_key'
                    """
                )
            )
            connection.execute(text(LICENSE_ALIAS_CUTOVER_SQL))
            assert (
                connection.scalar(text("SELECT value_json FROM app_settings WHERE setting_key = 'pro_license_key'"))
                == "ETK-L-NEW-ALIAS"
            )
            transaction.rollback()
        finally:
            if transaction.is_active:
                transaction.rollback()
            connection.close()
        assert _setting_value(license_runtime, "pro_license_key") == {"malformed": True}
    finally:
        _restore_settings(license_runtime, prior)


def test_license_alias_cutover_rolls_back_retries_and_is_one_way_through_alembic(
    license_runtime: DatabaseRuntime,
    migrated_postgres18_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prior = _snapshot_settings(license_runtime)
    config = Config(str(REPO_ROOT / "alembic.ini"))
    monkeypatch.setenv("ETK_DATABASE_URL", migrated_postgres18_url)
    try:
        command.downgrade(config, PREVIOUS_HEAD)
        _clear_settings(license_runtime)
        _put_setting(license_runtime, "license_key", "ETK-Y-ALEMBIC")
        with license_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    f"""
                    ALTER TABLE app_settings ADD CONSTRAINT {CONSTRAINT}
                    CHECK (setting_key <> 'pro_license_key')
                    """
                )
            )

        with pytest.raises(IntegrityError):
            command.upgrade(config, "head")
        assert _alembic_head(license_runtime) == PREVIOUS_HEAD
        assert _setting_value(license_runtime, "pro_license_key") is None

        with license_runtime.engine.begin() as connection:
            connection.execute(text(f"ALTER TABLE app_settings DROP CONSTRAINT {CONSTRAINT}"))
        command.upgrade(config, "head")
        assert _alembic_head(license_runtime) == MIGRATION_HEAD
        assert _setting_value(license_runtime, "pro_license_key") == "ETK-Y-ALEMBIC"

        command.downgrade(config, PREVIOUS_HEAD)
        _put_setting(license_runtime, "license_key", "ETK-L-CHANGED-AFTER-DOWNGRADE")
        command.upgrade(config, "head")
        assert _setting_value(license_runtime, "pro_license_key") == "ETK-Y-ALEMBIC"
    finally:
        with license_runtime.engine.begin() as connection:
            connection.execute(text(f"ALTER TABLE app_settings DROP CONSTRAINT IF EXISTS {CONSTRAINT}"))
        command.upgrade(config, "head")
        _restore_settings(license_runtime, prior)


def test_license_alias_cutover_waits_for_and_preserves_a_concurrent_canonical_writer(
    license_runtime: DatabaseRuntime,
) -> None:
    prior = _snapshot_settings(license_runtime)
    _clear_settings(license_runtime)
    _put_setting(license_runtime, "license_key", "ETK-L-ALIAS")
    writer = license_runtime.engine.connect()
    transaction = writer.begin()
    try:
        writer.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                VALUES ('pro_license_key', '"ETK-Y-CONCURRENT"'::jsonb, NOW())
                """
            )
        )
        pids: Queue[int] = Queue()
        with ThreadPoolExecutor(max_workers=1) as executor:
            cutover = executor.submit(_execute_observed_cutover, license_runtime, pids)
            cutover_pid = pids.get(timeout=10)
            _wait_until_blocked(license_runtime, cutover_pid)
            transaction.commit()
            cutover.result(timeout=10)
        assert _setting_value(license_runtime, "pro_license_key") == "ETK-Y-CONCURRENT"
    finally:
        if transaction.is_active:
            transaction.rollback()
        writer.close()
        _restore_settings(license_runtime, prior)


def _execute_cutover(runtime: DatabaseRuntime) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(text(LICENSE_ALIAS_CUTOVER_SQL))


def _execute_observed_cutover(runtime: DatabaseRuntime, pids: Queue[int]) -> None:
    with runtime.engine.begin() as connection:
        pids.put(int(connection.scalar(text("SELECT pg_backend_pid()")) or 0))
        connection.execute(text(LICENSE_ALIAS_CUTOVER_SQL))


def _wait_until_blocked(runtime: DatabaseRuntime, pid: int) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        with runtime.engine.connect() as connection:
            blockers = connection.scalar(
                text("SELECT cardinality(pg_blocking_pids(:pid))"),
                {"pid": pid},
            )
        if int(blockers or 0) > 0:
            return
        time.sleep(0.01)
    raise AssertionError(f"PostgreSQL backend {pid} did not enter the expected app_settings lock wait")


def _clear_settings(runtime: DatabaseRuntime) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM app_settings WHERE setting_key = ANY(CAST(:keys AS text[]))"),
            {"keys": list(SETTING_KEYS)},
        )


def _put_setting(runtime: DatabaseRuntime, key: str, value: Any) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                VALUES (:key, CAST(:value AS jsonb), NOW())
                ON CONFLICT (setting_key) DO UPDATE
                SET value_json = EXCLUDED.value_json, last_updated_at = EXCLUDED.last_updated_at
                """
            ),
            {"key": key, "value": json.dumps(value)},
        )


def _setting_value(runtime: DatabaseRuntime, key: str) -> Any:
    with runtime.engine.connect() as connection:
        return connection.scalar(
            text("SELECT value_json FROM app_settings WHERE setting_key = :key"),
            {"key": key},
        )


def _alembic_head(runtime: DatabaseRuntime) -> str:
    with runtime.engine.connect() as connection:
        return str(connection.scalar(text("SELECT version_num FROM etk_migrations.alembic_version")) or "")


def _snapshot_settings(runtime: DatabaseRuntime) -> dict[str, tuple[Any, Any]]:
    with runtime.engine.connect() as connection:
        rows = (
            connection.execute(
                text(
                    """
                    SELECT setting_key, value_json, last_updated_at
                    FROM app_settings
                    WHERE setting_key = ANY(CAST(:keys AS text[]))
                    """
                ),
                {"keys": list(SETTING_KEYS)},
            )
            .mappings()
            .all()
        )
    return {str(row["setting_key"]): (row["value_json"], row["last_updated_at"]) for row in rows}


def _restore_settings(runtime: DatabaseRuntime, snapshot: dict[str, tuple[Any, Any]]) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM app_settings WHERE setting_key = ANY(CAST(:keys AS text[]))"),
            {"keys": list(SETTING_KEYS)},
        )
        for key, (value, updated_at) in snapshot.items():
            connection.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES (:key, CAST(:value AS jsonb), :updated_at)
                    """
                ),
                {"key": key, "value": json.dumps(value), "updated_at": updated_at},
            )
