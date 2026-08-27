from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text

from etk.platform.db import DatabaseRuntime, EngineConfig

REPO_ROOT = Path(__file__).resolve().parents[3]
POSTGRES_18_IMAGE = "postgres:18-alpine@sha256:d3e1620b530c944afa6e887d22eb899824da68e19c52024bf98f5220c88a65b2"


@pytest.fixture(scope="session")
def p8_postgres_url() -> Iterator[str]:
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
def migrated_p8_postgres_url(p8_postgres_url: str) -> Iterator[str]:
    previous = os.environ.get("ETK_DATABASE_URL")
    os.environ["ETK_DATABASE_URL"] = p8_postgres_url
    try:
        probe = DatabaseRuntime.create(
            EngineConfig(p8_postgres_url, application_name="etk-p8-postgres-version-probe"),
        )
        probe.start()
        try:
            with probe.engine.connect() as connection:
                server_version_num = str(connection.scalar(text("SHOW server_version_num")) or "")
        finally:
            probe.shutdown()
        if not server_version_num.startswith("18"):
            pytest.fail(f"P8 integration tests require PostgreSQL 18, got {server_version_num or 'unknown'}")
        command.upgrade(Config(str(REPO_ROOT / "alembic.ini")), "head")
        yield p8_postgres_url
    finally:
        if previous is None:
            os.environ.pop("ETK_DATABASE_URL", None)
        else:
            os.environ["ETK_DATABASE_URL"] = previous


@pytest.fixture()
def p8_runtime(migrated_p8_postgres_url: str) -> Iterator[DatabaseRuntime]:
    runtime = DatabaseRuntime.create(
        EngineConfig(migrated_p8_postgres_url, application_name="etk-p8-local-domains"),
    )
    runtime.start()
    _truncate_fixture_rows(runtime)
    try:
        yield runtime
    finally:
        _truncate_fixture_rows(runtime)
        runtime.shutdown()


def _truncate_fixture_rows(runtime: DatabaseRuntime) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                TRUNCATE TABLE
                    idempotency_records,
                    job_attempts,
                    job_runs,
                    portal_subscription_requests,
                    database_maintenance_operations,
                    database_backup_artifacts,
                    identity_avatar_artifacts,
                    identity_operations,
                    emby_favorite_items,
                    user_media_data,
                    invitations,
                    emby_users_extended,
                    user_templates,
                    emby_users,
                    collection_operations,
                    media_effects,
                    outbox_events,
                    subscription_quota_reservations,
                    subscription_operations,
                    resubscribe_index,
                    actor_subscriptions,
                    failed_log,
                    processed_log,
                    custom_collections,
                    collections_info,
                    media_image_policy_cache,
                    media_image_cache,
                    media_metadata,
                    app_settings
                RESTART IDENTITY CASCADE
                """
            )
        )
