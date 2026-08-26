from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text

from etk.platform.db import DatabaseRuntime, EngineConfig, PsycopgPoolConfig

REPO_ROOT = Path(__file__).resolve().parents[3]
POSTGRES_18_IMAGE = "postgres:18-alpine@sha256:d3e1620b530c944afa6e887d22eb899824da68e19c52024bf98f5220c88a65b2"


@pytest.fixture(scope="session")
def p6_postgres_url() -> Iterator[str]:
    configured = os.environ.get("ETK_TEST_DATABASE_URL")
    if configured:
        yield configured
        return
    if os.environ.get("ETK_RUN_POSTGRES_INTEGRATION") != "1":
        pytest.skip("set ETK_RUN_POSTGRES_INTEGRATION=1 to start disposable PostgreSQL 18")

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
def migrated_p6_postgres_url(p6_postgres_url: str) -> Iterator[str]:
    previous = os.environ.get("ETK_DATABASE_URL")
    os.environ["ETK_DATABASE_URL"] = p6_postgres_url
    try:
        command.upgrade(Config(str(REPO_ROOT / "alembic.ini")), "head")
        yield p6_postgres_url
    finally:
        if previous is None:
            os.environ.pop("ETK_DATABASE_URL", None)
        else:
            os.environ["ETK_DATABASE_URL"] = previous


@pytest.fixture()
def p6_runtime(migrated_p6_postgres_url: str) -> Iterator[DatabaseRuntime]:
    runtime = DatabaseRuntime.create(
        EngineConfig(migrated_p6_postgres_url, application_name="etk-p6-directory-cache"),
        raw_pool_config=PsycopgPoolConfig(
            migrated_p6_postgres_url,
            application_name="etk-p6-raw",
        ),
    )
    runtime.start()
    with runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM p115_organize_effect_events"))
        connection.execute(text("DELETE FROM p115_organize_reconciliation_intents"))
        connection.execute(
            text(
                """
                UPDATE p115_organize_effects
                SET status = 'retryable',
                    sponsor_execution_id = NULL,
                    sponsor_action_index = NULL,
                    sponsor_account_id = NULL,
                    claimed_by = NULL,
                    claim_token = NULL,
                    claimed_at = NULL,
                    lease_expires_at = NULL,
                    provider_call_started_at = NULL,
                    receipt_json = NULL,
                    receipt_persisted_at = NULL,
                    finished_at = NULL
                WHERE status IN ('claimed', 'requesting', 'awaiting_readback')
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE p115_organize_effects
                SET sponsor_execution_id = NULL,
                    sponsor_action_index = NULL,
                    sponsor_account_id = NULL
                """
            )
        )
        connection.execute(text("DELETE FROM p115_organize_execution_actions"))
        connection.execute(text("DELETE FROM p115_organize_effects"))
        connection.execute(text("DELETE FROM p115_organize_plan_executions"))
        connection.execute(text("DELETE FROM p115_intro_detection_operations"))
        connection.execute(text("DELETE FROM p115_intro_detection_failures"))
        connection.execute(text("DELETE FROM p115_intro_fingerprint_failures"))
        connection.execute(text("DELETE FROM p115_intro_fingerprint_cache"))
        connection.execute(text("DELETE FROM outbox_events"))
        connection.execute(text("DELETE FROM media_effects"))
        connection.execute(text("DELETE FROM p115_media_probe_items"))
        connection.execute(text("DELETE FROM p115_strm_sync_plans"))
        connection.execute(text("DELETE FROM job_attempts"))
        connection.execute(text("DELETE FROM job_runs"))
        connection.execute(text("DELETE FROM p115_organize_plan_actions"))
        connection.execute(text("DELETE FROM p115_organize_plans"))
        connection.execute(text("DELETE FROM p115_organize_records"))
        connection.execute(text("DELETE FROM p115_filesystem_cache"))
        connection.execute(text("DELETE FROM p115_mediainfo_cache"))
        connection.execute(text("DELETE FROM media_metadata"))
        connection.execute(
            text(
                """
                DELETE FROM app_settings
                WHERE setting_key IN (
                    'dynamic_app_config',
                    'p115_episode_regex_rules',
                    'p115_rename_config',
                    'p115_sorting_rules',
                    'p115_sorting_rules_account_id'
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                VALUES ('p115_auth_tokens', '{"account_id":"115001"}'::jsonb, NOW())
                ON CONFLICT (setting_key) DO UPDATE SET
                    value_json = EXCLUDED.value_json,
                    last_updated_at = NOW()
                """
            )
        )
    try:
        yield runtime
    finally:
        runtime.shutdown()
