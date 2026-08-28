from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text

from etk.domains.media.effects import MEDIA_EFFECT_TOPIC
from etk.platform.db import DatabaseRuntime, EngineConfig

REPO_ROOT = Path(__file__).resolve().parents[3]
POSTGRES_18_IMAGE = "postgres:18-alpine@sha256:d3e1620b530c944afa6e887d22eb899824da68e19c52024bf98f5220c88a65b2"


@pytest.fixture(scope="session")
def p5_postgres_url() -> Iterator[str]:
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
def migrated_p5_postgres_url(p5_postgres_url: str) -> Iterator[str]:
    previous = os.environ.get("ETK_DATABASE_URL")
    os.environ["ETK_DATABASE_URL"] = p5_postgres_url
    try:
        command.upgrade(Config(str(REPO_ROOT / "alembic.ini")), "head")
        yield p5_postgres_url
    finally:
        if previous is None:
            os.environ.pop("ETK_DATABASE_URL", None)
        else:
            os.environ["ETK_DATABASE_URL"] = previous


@pytest.fixture()
def p5_runtime(migrated_p5_postgres_url: str) -> Iterator[DatabaseRuntime]:
    runtime = DatabaseRuntime.create(
        EngineConfig(migrated_p5_postgres_url, application_name="etk-p5-integration"),
    )
    runtime.start()
    try:
        _clear_p5_state(runtime)
        yield runtime
    finally:
        try:
            _clear_p5_state(runtime)
        finally:
            runtime.shutdown()


def _clear_p5_state(runtime: DatabaseRuntime) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM outbox_events WHERE topic = :topic"),
            {"topic": MEDIA_EFFECT_TOPIC},
        )
        connection.execute(
            text(
                """
                TRUNCATE TABLE
                    emby_search_rebuild_state,
                    emby_search_index,
                    media_effects,
                    p115_media_probe_items,
                    p115_mediainfo_cache,
                    media_image_policy_cache,
                    media_image_cache,
                    media_metadata,
                    person_metadata,
                    collections_info,
                    app_settings
                RESTART IDENTITY CASCADE
                """
            )
        )
