from __future__ import annotations

import json
from threading import Event, Thread
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etk.domains.p115 import (
    P115_AUTH_TOKENS_SETTING,
    SqlAlchemyP115CredentialRepository,
)
from etk.platform.config.locking import DYNAMIC_CONFIG_LOCK_NAMESPACE, P115_AUTH_LOCK_KEY
from etk.platform.db import DatabaseRuntime, EngineConfig

CONSTRAINT = "verification_p11_p115_credentials"


def test_p115_credentials_preserve_account_binding_cas_and_rollback(
    migrated_p6_postgres_url: str,
) -> None:
    runtime = DatabaseRuntime.create(
        EngineConfig(
            migrated_p6_postgres_url,
            application_name="etk-p11-p115-credentials",
        )
    )
    snapshot: dict[str, Any] | None = None
    mutated = False
    try:
        runtime.start()
        with runtime.engine.connect() as connection:
            assert int(connection.scalar(text("SHOW server_version_num")) or 0) // 10000 == 18
            row = (
                connection.execute(
                    text(
                        """
                        SELECT value_json, last_updated_at
                        FROM app_settings
                        WHERE setting_key = :key
                        """
                    ),
                    {"key": P115_AUTH_TOKENS_SETTING},
                )
                .mappings()
                .first()
            )
            snapshot = dict(row) if row is not None else None

        mutated = True
        with runtime.engine.begin() as connection:
            connection.execute(
                text("DELETE FROM app_settings WHERE setting_key = :key"),
                {"key": P115_AUTH_TOKENS_SETTING},
            )

        invalidations: list[str] = []
        repository = SqlAlchemyP115CredentialRepository(
            runtime.unit_of_work,
            on_change=lambda: invalidations.append("changed"),
        )
        assert repository.load().account_id == ""

        with runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES (:key, CAST(:value AS jsonb), NOW())
                    """
                ),
                {"key": P115_AUTH_TOKENS_SETTING, "value": json.dumps("corrupt-record")},
            )
        malformed = _raw(runtime)
        with pytest.raises(TypeError, match="JSON object"):
            repository.load()
        with pytest.raises(TypeError, match="JSON object"):
            repository.save_openapi("fixture-access", "fixture-refresh", account_id="115001")
        assert _raw(runtime) == malformed
        with runtime.engine.begin() as connection:
            connection.execute(
                text("DELETE FROM app_settings WHERE setting_key = :key"),
                {"key": P115_AUTH_TOKENS_SETTING},
            )

        repository.save_openapi("fixture-access-a", "fixture-refresh-a", account_id="115001")
        repository.save_cookie("UID=115001_fixture; CID=fixture", "tv")
        loaded = repository.load()
        assert loaded.account_id == "115001"
        assert loaded.has_openapi is True
        assert loaded.has_cookie is True
        assert loaded.app_type == "tv"
        assert repository.app_label("tv") == "安卓电视端"
        assert "fixture-access-a" not in repr(loaded)

        invalidations.clear()
        repository.save_cookie("UID=115001_fixture; CID=fixture", "alipaymini")
        assert invalidations == ["changed"]
        repository.save_cookie("UID=115001_fixture; CID=fixture", "alipaymini")
        assert invalidations == ["changed"]

        before_noop = _raw(runtime)
        repository.save_openapi(
            None,
            None,
            account_id="115001",
            expected_access_token="fixture-access-a",
        )
        assert _raw(runtime) == before_noop

        repository.save_openapi(
            "fixture-access-b",
            "fixture-refresh-b",
            account_id="115001",
            expected_access_token="fixture-access-a",
        )
        before_cas_failure = _raw(runtime)
        with pytest.raises(RuntimeError, match="拒绝覆盖"):
            repository.save_openapi(
                "stale-access",
                "stale-refresh",
                account_id="115001",
                expected_access_token="fixture-access-a",
            )
        assert _raw(runtime) == before_cas_failure
        with pytest.raises(RuntimeError, match="刷新凭据"):
            repository.save_bundle(
                "stale-access",
                "stale-refresh",
                account_id="115001",
                credential_type="openapi",
                expected_access_token="fixture-access-b",
                expected_refresh_token="fixture-refresh-a",
            )
        assert _raw(runtime) == before_cas_failure

        repository.save_cookie("UID=115002_fixture", "web")
        cookie_replacement = _raw(runtime)
        assert cookie_replacement["value_json"] == {
            "access_token": None,
            "refresh_token": None,
            "cookie": "UID=115002_fixture",
            "app_type": "web",
            "account_id": "115002",
            "openapi_account_id": None,
            "cookie_account_id": "115002",
        }
        assert repository.load().account_id == "115002"

        repository.save_openapi("fixture-access-c", "fixture-refresh-c", account_id="115003")
        openapi_replacement = _raw(runtime)
        assert openapi_replacement["value_json"] == {
            "access_token": "fixture-access-c",
            "refresh_token": "fixture-refresh-c",
            "cookie": None,
            "app_type": "web",
            "account_id": "115003",
            "openapi_account_id": "115003",
            "cookie_account_id": None,
        }
        assert repository.load().account_id == "115003"

        before_invalid_cookie = _raw(runtime)
        with pytest.raises(ValueError, match="稳定账号身份"):
            repository.save_cookie("not-an-account-cookie", "web")
        assert _raw(runtime) == before_invalid_cookie

        invalidations.clear()
        race_started = Event()
        race_finished = Event()
        race_errors: list[BaseException] = []
        legacy_payload: dict[str, Any] = {}

        def native_race_write() -> None:
            race_started.set()
            try:
                repository.save_openapi(
                    "native-race-access",
                    "native-race-refresh",
                    account_id="115003",
                    expected_access_token="fixture-access-c",
                )
            except BaseException as exc:
                race_errors.append(exc)
            finally:
                race_finished.set()

        with runtime.engine.connect() as legacy_connection:
            legacy_connection.execute(
                text("SELECT pg_advisory_lock(:namespace, :lock_key)"),
                {"namespace": DYNAMIC_CONFIG_LOCK_NAMESPACE, "lock_key": P115_AUTH_LOCK_KEY},
            )
            legacy_connection.commit()
            thread = Thread(target=native_race_write, daemon=True)
            thread.start()
            try:
                assert race_started.wait(timeout=1)
                assert race_finished.wait(timeout=0.2) is False
                legacy_payload = dict(before_invalid_cookie["value_json"])
                legacy_payload.update(
                    access_token="legacy-race-access",
                    refresh_token="legacy-race-refresh",
                )
                legacy_connection.execute(
                    text(
                        """
                        UPDATE app_settings
                        SET value_json = CAST(:value AS jsonb), last_updated_at = NOW()
                        WHERE setting_key = :key
                        """
                    ),
                    {
                        "key": P115_AUTH_TOKENS_SETTING,
                        "value": json.dumps(legacy_payload, ensure_ascii=False),
                    },
                )
                legacy_connection.commit()
            finally:
                legacy_connection.execute(
                    text("SELECT pg_advisory_unlock(:namespace, :lock_key)"),
                    {"namespace": DYNAMIC_CONFIG_LOCK_NAMESPACE, "lock_key": P115_AUTH_LOCK_KEY},
                )
                legacy_connection.commit()
            thread.join(timeout=5)
        assert race_finished.is_set()
        assert len(race_errors) == 1
        assert isinstance(race_errors[0], RuntimeError)
        assert _raw(runtime)["value_json"] == legacy_payload
        assert invalidations == []

        with runtime.engine.begin() as connection:
            connection.execute(
                text(
                    f"""
                    ALTER TABLE app_settings
                    ADD CONSTRAINT {CONSTRAINT}
                    CHECK (setting_key <> '{P115_AUTH_TOKENS_SETTING}'
                           OR value_json ->> 'access_token' <> 'reject-by-constraint')
                    """
                )
            )
        before_database_failure = _raw(runtime)
        with pytest.raises(IntegrityError):
            repository.save_openapi(
                "reject-by-constraint",
                "fixture-refresh-rejected",
                account_id="115003",
                expected_access_token="legacy-race-access",
            )
        assert _raw(runtime) == before_database_failure
    finally:
        try:
            if mutated:
                with runtime.engine.begin() as connection:
                    connection.execute(text(f"ALTER TABLE app_settings DROP CONSTRAINT IF EXISTS {CONSTRAINT}"))
                    connection.execute(
                        text("DELETE FROM app_settings WHERE setting_key = :key"),
                        {"key": P115_AUTH_TOKENS_SETTING},
                    )
                    if snapshot is not None:
                        connection.execute(
                            text(
                                """
                                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                                VALUES (:key, CAST(:value AS jsonb), :last_updated_at)
                                """
                            ),
                            {
                                "key": P115_AUTH_TOKENS_SETTING,
                                "value": json.dumps(snapshot["value_json"], ensure_ascii=False),
                                "last_updated_at": snapshot["last_updated_at"],
                            },
                        )
        finally:
            runtime.shutdown()


def _raw(runtime: DatabaseRuntime) -> dict[str, Any]:
    with runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                    SELECT value_json, last_updated_at
                    FROM app_settings
                    WHERE setting_key = :key
                    """
                ),
                {"key": P115_AUTH_TOKENS_SETTING},
            )
            .mappings()
            .one()
        )
    return dict(row)
