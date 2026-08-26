from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

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
LEGACY_TABLE_COUNT = 39
GREENFIELD_TABLES = {
    "idempotency_records",
    "job_attempts",
    "job_runs",
    "media_effects",
    "outbox_events",
    "p115_organize_effect_events",
    "p115_organize_effects",
    "p115_organize_execution_actions",
    "p115_organize_plan_actions",
    "p115_organize_plan_executions",
    "p115_organize_plans",
    "p115_organize_reconciliation_intents",
    "p115_media_probe_items",
    "p115_strm_sync_plans",
    "ingestion_monitor_items",
    "ingestion_pending_bindings",
    "p115_upload_monitor_controls",
    "p115_upload_operations",
    "p115_playback_operations",
    "p115_playback_artifacts",
    "p115_playback_leases",
    "p115_playback_qr_sessions",
    "p115_intro_detection_operations",
    "subscription_operations",
    "subscription_quota_reservations",
    "subscription_runtime_states",
}
EXPECTED_TABLE_COUNT = LEGACY_TABLE_COUNT + len(GREENFIELD_TABLES)
P6_P115_EXTENSION_NAMES = {
    "columns": {"account_id", "entry_kind", "strm_content_sha256", "sync_root_id"},
    "constraints": {
        "ck_p115_filesystem_cache_account_id",
        "ck_p115_filesystem_cache_account_id_present",
        "ck_p115_filesystem_cache_entry_kind",
        "ck_p115_filesystem_cache_entry_kind_present",
        "ck_p115_filesystem_cache_strm_content_sha256",
        "ck_p115_filesystem_cache_strm_hash_file_only",
    },
    "indexes": {"idx_p115_account_parent_kind", "idx_p115_account_sync_root_kind"},
}
P6_P115_MEDIAINFO_EXTENSION_NAMES = {
    "columns": {"updated_at", "cache_revision", "raw_schema_version", "formatter_revision", "probe_revision"},
    "constraints": {
        "ck_p115_mediainfo_cache_sha1_canonical",
        "ck_p115_mediainfo_cache_revision",
        "ck_p115_mediainfo_cache_raw_schema_version",
        "ck_p115_mediainfo_cache_formatter_revision",
        "ck_p115_mediainfo_cache_probe_revision",
    },
    "indexes": {"uq_p115_mediainfo_cache_upper_sha1"},
}


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
    complete_catalog = collect_catalog(migrated_postgres18_url)
    actual = _legacy_catalog(complete_catalog)
    result = compare_catalogs(expected, actual)

    assert complete_catalog["server_version_num"].startswith("18")
    assert len(complete_catalog["tables"]) == EXPECTED_TABLE_COUNT
    assert {table["name"] for table in complete_catalog["tables"]} - {
        table["name"] for table in actual["tables"]
    } == GREENFIELD_TABLES
    assert result["counts"] == {
        "tables": {"expected": LEGACY_TABLE_COUNT, "actual": LEGACY_TABLE_COUNT},
        "columns": {"expected": 478, "actual": 478},
        "constraints": {"expected": 60, "actual": 60},
        "indexes": {"expected": 120, "actual": 120},
    }
    assert result["differences"] == {
        key: {"missing": [], "unexpected": [], "changed": []} for key in ("tables", "columns", "constraints", "indexes")
    }
    assert result["match"] is True
    for family, names in P6_P115_EXTENSION_NAMES.items():
        assert {
            item["name"]
            for item in complete_catalog[family]
            if item["table"] == "p115_filesystem_cache" and item["name"] in names
        } == names
    for family, names in P6_P115_MEDIAINFO_EXTENSION_NAMES.items():
        assert {
            item["name"]
            for item in complete_catalog[family]
            if item["table"] == "p115_mediainfo_cache" and item["name"] in names
        } == names


def _legacy_catalog(catalog: dict[str, Any]) -> dict[str, Any]:
    filtered = dict(catalog)
    filtered["tables"] = [item for item in catalog["tables"] if item["name"] not in GREENFIELD_TABLES]
    for family in ("columns", "constraints", "indexes"):
        filtered[family] = [
            item
            for item in catalog[family]
            if item["table"] not in GREENFIELD_TABLES
            and not (item["table"] == "p115_filesystem_cache" and item["name"] in P6_P115_EXTENSION_NAMES[family])
            and not (
                item["table"] == "p115_mediainfo_cache" and item["name"] in P6_P115_MEDIAINFO_EXTENSION_NAMES[family]
            )
        ]
    return filtered


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
