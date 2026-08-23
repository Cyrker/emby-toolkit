from __future__ import annotations

from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier, Event
from typing import Any, cast

import psycopg
from psycopg.rows import dict_row
from sqlalchemy import text

import config_manager
from database import settings_db
from etk.bootstrap.config_sync import ApiRuntimeConfigSynchronizer, SqlAlchemyDynamicConfigReader
from etk.platform import Settings
from etk.platform.db import DatabaseRuntime
from handler import p115_service
from routes import p115 as p115_routes


def _legacy_connection_factory(database_url: str):
    @contextmanager
    def connect():
        with psycopg.connect(database_url, row_factory=cast(Any, dict_row)) as database:
            yield database

    return connect


def test_deep_delete_snapshot_survives_separate_api_database_sessions(
    p4_runtime: DatabaseRuntime,
    migrated_p4_postgres_url: str,
    monkeypatch,
) -> None:
    from database import connection as legacy_connection

    database_url = migrated_p4_postgres_url.replace("postgresql+psycopg://", "postgresql://", 1)

    @contextmanager
    def connect():
        with psycopg.connect(database_url, row_factory=cast(Any, dict_row)) as database:
            yield database

    monkeypatch.setattr(legacy_connection, "get_db_connection", connect)
    token = "fixture-restart-token"
    snapshot = {
        "created_at": p115_routes.time.time(),
        "item_id": "123",
        "item_name": "Fixture",
        "item_type": "Movie",
        "pickcodes": ["pc-fixture"],
    }
    with p4_runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM app_settings WHERE setting_key LIKE :prefix"),
            {"prefix": f"{p115_routes._DEEP_DELETE_SETTING_PREFIX}%"},
        )

    p115_routes._store_deep_delete_snapshot(token, snapshot)
    claimed = p115_routes._claim_deep_delete_snapshot(token)

    assert claimed == snapshot
    assert p115_routes._claim_deep_delete_snapshot(token) is None


class RecordingRuntime:
    def __init__(self, values: Mapping[str, Any]) -> None:
        self._config = dict(values)
        self.applied = 0

    @property
    def config(self) -> Mapping[str, Any]:
        return self._config

    def apply_persisted_config_snapshot(self, new_config: Mapping[str, Any]) -> None:
        self.applied += 1
        self._config.update(dict(new_config))


def test_api_request_boundary_reads_worker_config_from_a_separate_database_session(
    p4_runtime: DatabaseRuntime,
    migrated_p4_postgres_url: str,
) -> None:
    with p4_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM app_settings WHERE setting_key = 'dynamic_app_config'"))

    current = [Settings.model_validate({"monitor_enabled": False})]
    runtime = RecordingRuntime(current[0].to_runtime_dict())
    reader = SqlAlchemyDynamicConfigReader(p4_runtime.unit_of_work)
    synchronizer = ApiRuntimeConfigSynchronizer(
        snapshot_reader=reader.read,
        settings_provider=lambda: current[0],
        settings_consumer=lambda value: current.__setitem__(0, value),
        runtime=runtime,
    )
    database_url = migrated_p4_postgres_url.replace("postgresql+psycopg://", "postgresql://", 1)
    with psycopg.connect(database_url) as worker_connection:
        worker_connection.execute(
            """
            INSERT INTO app_settings (setting_key, value_json, last_updated_at)
            VALUES ('dynamic_app_config', %s::jsonb, NOW())
            ON CONFLICT (setting_key) DO UPDATE
            SET value_json = EXCLUDED.value_json, last_updated_at = NOW()
            """,
            ('{"monitor_enabled": true, "p115_media_root_cid": "worker-cid"}',),
        )

    assert synchronizer.synchronize() is True
    assert synchronizer.synchronize() is False
    assert current[0].monitor_enabled is True
    assert current[0].p115_media_root_cid == "worker-cid"
    assert runtime.applied == 1


def test_api_and_worker_dynamic_config_merges_preserve_distinct_keys(
    p4_runtime: DatabaseRuntime,
    migrated_p4_postgres_url: str,
    monkeypatch,
) -> None:
    with p4_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM app_settings WHERE setting_key = 'dynamic_app_config'"))

    database_url = migrated_p4_postgres_url.replace("postgresql+psycopg://", "postgresql://", 1)
    monkeypatch.setattr(settings_db, "get_db_connection", _legacy_connection_factory(database_url))
    barrier = Barrier(2)

    def merge(update: dict[str, Any]) -> dict[str, Any]:
        barrier.wait()
        return settings_db.merge_setting_mapping("dynamic_app_config", update)

    with ThreadPoolExecutor(max_workers=2) as executor:
        api = executor.submit(merge, {"monitor_enabled": True})
        worker = executor.submit(merge, {"p115_media_root_cid": "worker-cid"})
        api.result()
        worker.result()

    persisted = settings_db.get_setting("dynamic_app_config")
    assert persisted is not None
    assert persisted["monitor_enabled"] is True
    assert persisted["p115_media_root_cid"] == "worker-cid"


def test_quick_deploy_merges_monitor_path_against_the_latest_api_snapshot(
    p4_runtime: DatabaseRuntime,
    migrated_p4_postgres_url: str,
    monkeypatch,
) -> None:
    with p4_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM app_settings WHERE setting_key = 'dynamic_app_config'"))

    database_url = migrated_p4_postgres_url.replace("postgresql+psycopg://", "postgresql://", 1)
    monkeypatch.setattr(settings_db, "get_db_connection", _legacy_connection_factory(database_url))
    initial = {
        "monitor_paths": ["/api-added-during-job"],
        "p115_api_priority": "proapi",
        "p115_min_video_size": 42,
        "p115_extensions": ["fixture"],
    }
    settings_db.save_setting("dynamic_app_config", initial, log_success=False)
    monkeypatch.setattr(config_manager, "APP_CONFIG", dict(initial))

    published = config_manager.save_config_with_monitor_path(
        {"monitor_enabled": True, "p115_media_root_cid": "worker-cid"},
        "/fixture/media",
    )
    persisted = settings_db.get_setting("dynamic_app_config")
    assert persisted is not None

    assert persisted["monitor_paths"] == ["/api-added-during-job", "/fixture/media"]
    assert persisted["p115_api_priority"] == "proapi"
    assert persisted["p115_min_video_size"] == 42
    assert persisted["p115_extensions"] == ["fixture"]
    assert published == {
        "monitor_enabled": True,
        "monitor_paths": ["/api-added-during-job", "/fixture/media"],
        "p115_media_root_cid": "worker-cid",
    }


def test_reload_rollback_does_not_erase_a_worker_commit(
    p4_runtime: DatabaseRuntime,
    migrated_p4_postgres_url: str,
    monkeypatch,
) -> None:
    with p4_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM app_settings WHERE setting_key = 'dynamic_app_config'"))

    database_url = migrated_p4_postgres_url.replace("postgresql+psycopg://", "postgresql://", 1)
    monkeypatch.setattr(settings_db, "get_db_connection", _legacy_connection_factory(database_url))
    initial = {"monitor_enabled": False, "p115_media_root_cid": "before"}
    settings_db.save_setting("dynamic_app_config", initial, log_success=False)
    monkeypatch.setattr(config_manager, "APP_CONFIG", dict(initial))
    previous_persisted, previous_memory = config_manager.capture_dynamic_config_state()

    config_manager.save_config({"monitor_enabled": True})
    settings_db.merge_setting_mapping("dynamic_app_config", {"p115_media_root_cid": "worker-cid"})
    config_manager.restore_dynamic_config_state(
        previous_persisted,
        previous_memory,
        expected_updates={"monitor_enabled": True},
    )

    persisted = settings_db.get_setting("dynamic_app_config")
    assert persisted == {"monitor_enabled": False, "p115_media_root_cid": "worker-cid"}
    assert config_manager.APP_CONFIG == persisted


def test_reload_rollback_lock_serializes_a_same_key_same_value_worker_commit(
    p4_runtime: DatabaseRuntime,
    migrated_p4_postgres_url: str,
    monkeypatch,
) -> None:
    with p4_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM app_settings WHERE setting_key = 'dynamic_app_config'"))

    database_url = migrated_p4_postgres_url.replace("postgresql+psycopg://", "postgresql://", 1)
    monkeypatch.setattr(settings_db, "get_db_connection", _legacy_connection_factory(database_url))
    initial = {"monitor_enabled": False}
    settings_db.save_setting("dynamic_app_config", initial, log_success=False)
    monkeypatch.setattr(config_manager, "APP_CONFIG", dict(initial))
    worker_started = Event()
    worker_finished = Event()

    def worker_commit() -> None:
        worker_started.set()
        config_manager.save_config({"monitor_enabled": True})
        worker_finished.set()

    with ThreadPoolExecutor(max_workers=1) as executor:
        with config_manager.dynamic_config_write_lock():
            previous_persisted, previous_memory = config_manager.capture_dynamic_config_state()
            config_manager.save_config({"monitor_enabled": True})
            future = executor.submit(worker_commit)
            assert worker_started.wait(1)
            assert not worker_finished.wait(0.1)
            config_manager.restore_dynamic_config_state(
                previous_persisted,
                previous_memory,
                expected_updates={"monitor_enabled": True},
            )
        future.result()

    persisted = settings_db.get_setting("dynamic_app_config")
    assert persisted == {"monitor_enabled": True}


def test_quick_deploy_auth_lock_serializes_an_account_switch(
    p4_runtime: DatabaseRuntime,
    migrated_p4_postgres_url: str,
    monkeypatch,
) -> None:
    with p4_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM app_settings WHERE setting_key = 'p115_auth_tokens'"))

    database_url = migrated_p4_postgres_url.replace("postgresql+psycopg://", "postgresql://", 1)
    monkeypatch.setattr(settings_db, "get_db_connection", _legacy_connection_factory(database_url))
    monkeypatch.setattr(settings_db.logger, "trace", lambda *_args, **_kwargs: None, raising=False)
    p115_service.save_115_tokens(
        "account-a-token",
        "account-a-refresh",
        account_id="115001",
        credential_type="openapi",
    )
    switch_started = Event()
    switch_finished = Event()

    def switch_account() -> None:
        switch_started.set()
        p115_service.save_115_tokens(
            None,
            None,
            "UID=115002_fixture; CID=fixture",
            "web",
            credential_type="cookie",
        )
        switch_finished.set()

    with ThreadPoolExecutor(max_workers=1) as executor:
        with settings_db.p115_auth_write_lock():
            future = executor.submit(switch_account)
            assert switch_started.wait(1)
            assert not switch_finished.wait(0.1)
            assert p115_service.get_115_account_id() == "115001"
        future.result()

    persisted = cast(dict[str, Any], settings_db.get_setting("p115_auth_tokens"))
    assert persisted["account_id"] == "115002"
    assert persisted["cookie_account_id"] == "115002"
    assert persisted["access_token"] is None
    assert persisted["openapi_account_id"] is None
