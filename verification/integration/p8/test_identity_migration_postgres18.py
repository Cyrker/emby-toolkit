from __future__ import annotations

import os
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import DBAPIError

REPO_ROOT = Path(__file__).resolve().parents[3]
PREVIOUS_REVISION = "0015_p7_assistant_claims"


def test_identity_revision_downgrades_to_predecessor_and_upgrades_to_head(
    migrated_p8_postgres_url: str,
) -> None:
    config = Config(str(REPO_ROOT / "alembic.ini"))
    previous = os.environ.get("ETK_DATABASE_URL")
    os.environ["ETK_DATABASE_URL"] = migrated_p8_postgres_url
    engine = create_engine(migrated_p8_postgres_url)
    try:
        command.downgrade(config, PREVIOUS_REVISION)
        downgraded_tables = set(inspect(engine).get_table_names(schema="public"))
        invitation_columns = {column["name"] for column in inspect(engine).get_columns("invitations", schema="public")}
        assert "identity_operations" not in downgraded_tables
        assert "identity_avatar_artifacts" not in downgraded_tables
        assert {"reservation_id", "reserved_at", "reservation_expires_at"}.isdisjoint(invitation_columns)
        with engine.connect() as connection:
            assert (
                connection.scalar(text("SELECT version_num FROM etk_migrations.alembic_version")) == PREVIOUS_REVISION
            )
    finally:
        command.upgrade(config, "head")
        if previous is None:
            os.environ.pop("ETK_DATABASE_URL", None)
        else:
            os.environ["ETK_DATABASE_URL"] = previous
        engine.dispose()

    verification_engine = create_engine(migrated_p8_postgres_url)
    try:
        upgraded_tables = set(inspect(verification_engine).get_table_names(schema="public"))
        assert {
            "identity_operations",
            "identity_avatar_artifacts",
            "portal_subscription_requests",
        }.issubset(upgraded_tables)
        with verification_engine.connect() as connection:
            assert (
                connection.scalar(text("SELECT version_num FROM etk_migrations.alembic_version"))
                == ScriptDirectory.from_config(config).get_current_head()
            )
    finally:
        verification_engine.dispose()


def test_identity_revision_refuses_downgrade_with_indeterminate_provider_state(
    migrated_p8_postgres_url: str,
) -> None:
    config = Config(str(REPO_ROOT / "alembic.ini"))
    previous = os.environ.get("ETK_DATABASE_URL")
    os.environ["ETK_DATABASE_URL"] = migrated_p8_postgres_url
    engine = create_engine(migrated_p8_postgres_url)
    operation_id = "00000000-0000-0000-0000-000000000816"
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO identity_operations (
                        id, operation_key, request_hash, kind, scope_key,
                        request_json, status, current_step, state_json, finished_at
                    ) VALUES (
                        CAST(:id AS uuid), :operation_key, :request_hash,
                        'set_status', 'user:migration-indeterminate',
                        CAST(:request_json AS jsonb),
                        'indeterminate', 'write',
                        CAST(:state_json AS jsonb),
                        NOW()
                    )
                    """
                ),
                {
                    "id": operation_id,
                    "operation_key": "8" * 64,
                    "request_hash": b"8" * 32,
                    "request_json": '{"user_id":"migration-indeterminate"}',
                    "state_json": '{"receipts":{"status_policy":{"confirmed":true}}}',
                },
            )

        try:
            command.downgrade(config, PREVIOUS_REVISION)
        except DBAPIError as exc:
            assert "cannot downgrade P8 identity operations" in str(exc)
        else:
            raise AssertionError("unsafe identity downgrade unexpectedly succeeded")

        with engine.connect() as connection:
            assert (
                connection.scalar(text("SELECT version_num FROM etk_migrations.alembic_version"))
                == ScriptDirectory.from_config(config).get_current_head()
            )
    finally:
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM identity_operations WHERE id = CAST(:id AS uuid)"),
                {"id": operation_id},
            )
        if previous is None:
            os.environ.pop("ETK_DATABASE_URL", None)
        else:
            os.environ["ETK_DATABASE_URL"] = previous
        engine.dispose()


def test_identity_revision_installs_username_fence_and_state_machine_constraints(
    migrated_p8_postgres_url: str,
) -> None:
    engine = create_engine(migrated_p8_postgres_url)
    try:
        with engine.connect() as connection:
            registration_index = connection.scalar(
                text("SELECT to_regclass('public.uq_identity_operation_active_registration_username')")
            )
            constraints = set(
                connection.scalars(
                    text(
                        """
                        SELECT constraint_name
                        FROM information_schema.table_constraints
                        WHERE table_schema = 'public'
                          AND table_name IN ('invitations', 'identity_avatar_artifacts')
                        """
                    )
                )
            )
    finally:
        engine.dispose()

    assert registration_index == "uq_identity_operation_active_registration_username"
    assert {
        "ck_invitations_reservation_fields",
        "ck_identity_avatar_expiry_window",
    }.issubset(constraints)


def test_identity_revision_refuses_downgrade_with_portal_request_receipts(
    migrated_p8_postgres_url: str,
) -> None:
    config = Config(str(REPO_ROOT / "alembic.ini"))
    previous = os.environ.get("ETK_DATABASE_URL")
    os.environ["ETK_DATABASE_URL"] = migrated_p8_postgres_url
    engine = create_engine(migrated_p8_postgres_url)
    receipt_id = "00000000-0000-0000-0000-000000000817"
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO portal_subscription_requests (
                        id, request_key_hash, request_fingerprint,
                        user_id, response_json, operation_ids
                    ) VALUES (
                        CAST(:id AS uuid), :request_key_hash, :request_fingerprint,
                        'fixture-user', '{"status":"WANTED"}'::jsonb, '{}'::uuid[]
                    )
                    """
                ),
                {
                    "id": receipt_id,
                    "request_key_hash": "a" * 64,
                    "request_fingerprint": "b" * 64,
                },
            )

        try:
            command.downgrade(config, PREVIOUS_REVISION)
        except DBAPIError as exc:
            assert "cannot downgrade P8 portal request receipts" in str(exc)
        else:
            raise AssertionError("unsafe portal receipt downgrade unexpectedly succeeded")
    finally:
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM portal_subscription_requests WHERE id = CAST(:id AS uuid)"),
                {"id": receipt_id},
            )
        if previous is None:
            os.environ.pop("ETK_DATABASE_URL", None)
        else:
            os.environ["ETK_DATABASE_URL"] = previous
        engine.dispose()
