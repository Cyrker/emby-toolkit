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
def p7_postgres_url() -> Iterator[str]:
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
def migrated_p7_postgres_url(p7_postgres_url: str) -> Iterator[str]:
    previous = os.environ.get("ETK_DATABASE_URL")
    os.environ["ETK_DATABASE_URL"] = p7_postgres_url
    try:
        command.upgrade(Config(str(REPO_ROOT / "alembic.ini")), "head")
        yield p7_postgres_url
    finally:
        if previous is None:
            os.environ.pop("ETK_DATABASE_URL", None)
        else:
            os.environ["ETK_DATABASE_URL"] = previous


@pytest.fixture()
def p7_runtime(migrated_p7_postgres_url: str) -> Iterator[DatabaseRuntime]:
    runtime = DatabaseRuntime.create(
        EngineConfig(migrated_p7_postgres_url, application_name="etk-p7-subscription-runtime"),
    )
    runtime.start()
    with runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM outbox_events WHERE topic = 'subscription.operation.requested'"))
        connection.execute(text("DELETE FROM subscription_quota_reservations"))
        connection.execute(text("DELETE FROM subscription_operations"))
        connection.execute(text("DELETE FROM subscription_runtime_states"))
        connection.execute(text("DELETE FROM app_settings WHERE setting_key = 'subscription_quota_state'"))
    try:
        yield runtime
    finally:
        runtime.shutdown()
