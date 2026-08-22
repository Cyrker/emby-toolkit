from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text

from etk.platform.config import LegacyConfigAdapter, PostgresLegacyConfigStore
from etk.platform.db import DatabaseRuntime, EngineConfig, PsycopgPoolConfig
from scripts.refactor.compare_schema import collect_catalog, compare_catalogs, load_catalog

REPO_ROOT = Path(__file__).resolve().parents[3]
CATALOG_PATH = REPO_ROOT / "verification" / "contracts" / "db" / "legacy-pg-catalog.json"
POSTGRES_18_IMAGE = "postgres:18-alpine@sha256:d3e1620b530c944afa6e887d22eb899824da68e19c52024bf98f5220c88a65b2"


@pytest.fixture(scope="session")
def postgres18_url() -> Iterator[str]:
    configured = os.environ.get("ETK_TEST_DATABASE_URL")
    if configured:
        yield configured
        return
    if os.environ.get("ETK_RUN_POSTGRES_INTEGRATION") != "1":
        pytest.skip("set ETK_RUN_POSTGRES_INTEGRATION=1 to start the disposable PostgreSQL 18 container")

    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(
        image=POSTGRES_18_IMAGE,
        username="etk",
        password="etk-test-only",
        dbname="etk",
        driver="psycopg",
    ) as container:
        yield container.get_connection_url(driver="psycopg")


@pytest.fixture(scope="session")
def migrated_postgres18_url(postgres18_url: str) -> str:
    previous = os.environ.get("ETK_DATABASE_URL")
    os.environ["ETK_DATABASE_URL"] = postgres18_url
    try:
        config = Config(str(REPO_ROOT / "alembic.ini"))
        command.upgrade(config, "head")
    finally:
        if previous is None:
            os.environ.pop("ETK_DATABASE_URL", None)
        else:
            os.environ["ETK_DATABASE_URL"] = previous
    return postgres18_url


def test_pg18_alembic_schema_matches_legacy_catalog(migrated_postgres18_url: str) -> None:
    expected = load_catalog(CATALOG_PATH)
    actual = collect_catalog(migrated_postgres18_url)
    result = compare_catalogs(expected, actual)

    assert actual["server_version_num"].startswith("18")
    assert result["counts"] == {
        "tables": {"expected": 39, "actual": 39},
        "columns": {"expected": 478, "actual": 478},
        "constraints": {"expected": 60, "actual": 60},
        "indexes": {"expected": 120, "actual": 120},
    }
    assert result["differences"] == {
        key: {"missing": [], "unexpected": [], "changed": []} for key in ("tables", "columns", "constraints", "indexes")
    }
    assert result["match"] is True


def test_pg18_uow_rollback_raw_pool_and_shutdown(migrated_postgres18_url: str) -> None:
    runtime = DatabaseRuntime.create(
        EngineConfig(migrated_postgres18_url, application_name="etk-integration"),
        raw_pool_config=PsycopgPoolConfig(
            migrated_postgres18_url,
            min_size=1,
            max_size=2,
            application_name="etk-integration-raw",
        ),
    )
    runtime.start()
    assert runtime.raw_pool is not None and runtime.raw_pool.closed is False
    try:
        with runtime.unit_of_work() as uow:
            uow.session.execute(
                text("INSERT INTO app_settings (setting_key, value_json) VALUES (:key, CAST(:value AS jsonb))"),
                {"key": "p2-rollback", "value": '{"state":"temporary"}'},
            )

        with runtime.unit_of_work() as uow:
            assert (
                uow.session.scalar(
                    text("SELECT count(*) FROM app_settings WHERE setting_key = :key"), {"key": "p2-rollback"}
                )
                == 0
            )

        with runtime.raw_pool.connection() as connection:
            row = connection.execute("SHOW server_version_num").fetchone()
            assert row is not None and row[0].startswith("18")
    finally:
        runtime.shutdown()

    assert runtime.raw_pool.closed is True


def test_pg18_schema_gate_rejects_character_width_drift(migrated_postgres18_url: str) -> None:
    expected = load_catalog(CATALOG_PATH)
    runtime = DatabaseRuntime.create(EngineConfig(migrated_postgres18_url, application_name="etk-schema-mutation"))
    try:
        with runtime.engine.begin() as connection:
            connection.execute(text("ALTER TABLE media_image_cache ALTER COLUMN content_hash TYPE character(32)"))
        result = compare_catalogs(expected, collect_catalog(migrated_postgres18_url))

        assert result["match"] is False
        changed = result["differences"]["columns"]["changed"]
        assert [item["key"] for item in changed] == [{"table": "media_image_cache", "name": "content_hash"}]
        assert changed[0]["actual"]["character_maximum_length"] == 32
    finally:
        with runtime.engine.begin() as connection:
            connection.execute(text("ALTER TABLE media_image_cache ALTER COLUMN content_hash TYPE character(64)"))
        runtime.shutdown()


def test_pg18_runtime_shutdown_times_out_with_active_raw_lease_then_retries(
    migrated_postgres18_url: str,
) -> None:
    runtime = DatabaseRuntime.create(
        EngineConfig(migrated_postgres18_url, application_name="etk-shutdown-retry"),
        raw_pool_config=PsycopgPoolConfig(
            migrated_postgres18_url,
            min_size=1,
            max_size=1,
            close_timeout_seconds=0.05,
            application_name="etk-shutdown-retry-raw",
        ),
    )
    runtime.start()
    assert runtime.raw_pool is not None
    raw_pool = runtime.raw_pool
    try:
        with raw_pool.connection() as connection:
            with pytest.raises(TimeoutError, match="1 active lease"):
                runtime.shutdown()
            assert raw_pool.closed is False
            row = connection.execute("SELECT 42").fetchone()
            assert row == (42,)

        runtime.shutdown()
        assert raw_pool.closed is True
        with pytest.raises(RuntimeError, match="not open"):
            with raw_pool.connection():
                pass
    finally:
        if not raw_pool.closed:
            runtime.shutdown()


def test_pg18_legacy_config_store_loads_and_atomically_merges_unknown_keys(
    migrated_postgres18_url: str,
    tmp_path: Path,
) -> None:
    store = PostgresLegacyConfigStore(migrated_postgres18_url)
    store.save_dynamic_config({"proxy_enabled": False, "future_key": {"keep": True}})
    adapter = LegacyConfigAdapter(tmp_path / "config.ini", environ={}, dynamic_store=store)
    first = adapter.load()
    second = adapter.load()

    adapter.merge_and_save(first, {"proxy_enabled": True, "first_unknown": "preserved"})
    merged = adapter.merge_and_save(second, {"monitor_enabled": True})
    persisted = store.load_dynamic_config()
    store.save_setting("p2_future_standalone", {"enabled": True})

    assert persisted is not None
    assert persisted["proxy_enabled"] is True
    assert persisted["monitor_enabled"] is True
    assert persisted["future_key"] == {"keep": True}
    assert persisted["first_unknown"] == "preserved"
    assert merged.proxy_enabled is True
    assert merged.monitor_enabled is True
    assert store.load_setting("p2_future_standalone") == {"enabled": True}
    assert "etk-test-only" not in repr(store)
