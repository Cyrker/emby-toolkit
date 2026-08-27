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
def p9_postgres_url() -> Iterator[str]:
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
def migrated_p9_runtime(p9_postgres_url: str) -> Iterator[DatabaseRuntime]:
    previous = os.environ.get("ETK_DATABASE_URL")
    os.environ["ETK_DATABASE_URL"] = p9_postgres_url
    runtime = DatabaseRuntime.create(EngineConfig(p9_postgres_url, application_name="etk-p9-ledger"))
    runtime.start()
    try:
        server_version = str(runtime.engine.connect().scalar(text("SHOW server_version_num")) or "")
        if not server_version.startswith("18"):
            pytest.fail(f"P9 requires PostgreSQL 18, got {server_version or 'unknown'}")
        command.upgrade(Config(str(REPO_ROOT / "alembic.ini")), "head")
        yield runtime
    finally:
        runtime.shutdown()
        if previous is None:
            os.environ.pop("ETK_DATABASE_URL", None)
        else:
            os.environ["ETK_DATABASE_URL"] = previous


@pytest.fixture()
def p9_runtime(migrated_p9_runtime: DatabaseRuntime) -> Iterator[DatabaseRuntime]:
    with migrated_p9_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                TRUNCATE TABLE
                    proxy_playback_leases,
                    sharing_credit_ledger,
                    sharing_credit_accounts,
                    sharing_operations,
                    outbox_events
                RESTART IDENTITY CASCADE
                """
            )
        )
    yield migrated_p9_runtime
