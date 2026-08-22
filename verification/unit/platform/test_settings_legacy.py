from __future__ import annotations

import configparser
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from etk.platform.config import LegacyConfigAdapter


class MemoryDynamicStore:
    def __init__(self, values: Mapping[str, Any] | None = None) -> None:
        self.values = dict(values or {})
        self.save_calls = 0

    def load_dynamic_config(self) -> Mapping[str, Any]:
        return self.values

    def save_dynamic_config(self, values: Mapping[str, Any]) -> None:
        self.values = dict(values)
        self.save_calls += 1

    def merge_dynamic_config(self, updates: Mapping[str, Any]) -> Mapping[str, Any]:
        self.values.update(dict(updates))
        self.save_calls += 1
        return self.values


def _read_ini(path: Path) -> configparser.ConfigParser:
    parser = configparser.ConfigParser(interpolation=None)
    parser.read(path, encoding="utf-8")
    return parser


def test_legacy_adapter_preserves_source_precedence(tmp_path: Path) -> None:
    config_path = tmp_path / "config.ini"
    config_path.write_text(
        """
[Database]
db_host = ini-host
db_port = 5433
db_password = ini-password

[Authentication]
auth_enabled = false
username = ini-admin
""".strip(),
        encoding="utf-8",
    )
    adapter = LegacyConfigAdapter(
        config_path,
        environ={
            "DB_HOST": "env-host",
            "DB_PORT": "5544",
            "DB_PASSWORD": "env-password",
            "TMDB_API_BASE_URL": "https://env.example/3",
        },
    )

    settings = adapter.load(
        {
            "tmdb_api_base_url": "https://dynamic.example/3",
            "proxy_enabled": True,
        }
    )

    assert settings.db_host == "env-host"
    assert settings.db_port == 5544
    assert settings.db_password.get_secret_value() == "env-password"
    assert settings.auth_enabled is False
    assert settings.username == "ini-admin"
    assert settings.tmdb_api_base_url == "https://dynamic.example/3"
    assert settings.proxy_enabled is True
    assert settings.is_first_run is False


def test_first_run_forces_auth_and_invalid_port_environment_is_ignored(tmp_path: Path) -> None:
    adapter = LegacyConfigAdapter(tmp_path / "missing.ini", environ={"DB_PORT": "invalid"})

    settings = adapter.load()

    assert settings.auth_enabled is True
    assert settings.db_port == 5432
    assert settings.is_first_run is True


def test_environment_only_adapter_does_not_require_a_config_path() -> None:
    adapter = LegacyConfigAdapter(None, environ={"DB_HOST": "greenfield-db", "DB_PORT": "6432"})

    settings = adapter.load()

    assert settings.db_host == "greenfield-db"
    assert settings.db_port == 6432
    assert settings.is_first_run is True
    with pytest.raises(RuntimeError, match="config path is not configured"):
        adapter.write_config_ini(settings)


def test_dynamic_save_does_not_persist_environment_bootstrap_values(tmp_path: Path) -> None:
    config_path = tmp_path / "config.ini"
    config_path.write_text("[Database]\ndb_host = ini-host\n", encoding="utf-8")
    store = MemoryDynamicStore()
    adapter = LegacyConfigAdapter(
        config_path,
        environ={"DB_HOST": "environment-host"},
        dynamic_store=store,
    )

    loaded = adapter.load()
    adapter.merge_and_save(loaded, {"proxy_enabled": True})

    assert loaded.db_host == "environment-host"
    assert _read_ini(config_path).get("Database", "db_host") == "ini-host"
    assert store.values["proxy_enabled"] is True


def test_unknown_ini_and_dynamic_keys_survive_round_trip(tmp_path: Path) -> None:
    config_path = tmp_path / "config.ini"
    config_path.write_text(
        """
[Database]
db_host = initial-host
future_pool_mode = elastic

[FutureSection]
FutureOption = keep-me
""".strip(),
        encoding="utf-8",
    )
    store = MemoryDynamicStore(
        {
            "emby_api_key": "emby-secret",
            "future_feature": {
                "enabled": True,
                "access_token": "future-secret",
            },
        }
    )
    adapter = LegacyConfigAdapter(config_path, environ={}, dynamic_store=store)

    loaded = adapter.load()
    dynamic_updated = adapter.merge_and_save(
        loaded,
        {
            "db_host": "updated-host",
            "proxy_enabled": True,
            "future_added_later": {"mode": "preserve"},
        },
    )
    assert dynamic_updated.db_host == "initial-host"
    updated = dynamic_updated.with_updates({"db_host": "updated-host"})
    adapter.write_config_ini(updated)
    reloaded = adapter.load()
    parser = _read_ini(config_path)

    assert updated.get("future_feature")["enabled"] is True
    assert reloaded.get("future_feature") == store.values["future_feature"]
    assert reloaded.get("future_added_later") == {"mode": "preserve"}
    assert reloaded.db_host == "updated-host"
    assert reloaded.proxy_enabled is True
    assert parser.get("Database", "future_pool_mode") == "elastic"
    assert parser.get("FutureSection", "FutureOption") == "keep-me"
    assert store.save_calls == 1
    assert store.values["emby_api_key"] == "emby-secret"
    assert "future-secret" not in repr(reloaded)


def test_concurrent_snapshots_merge_without_reverting_prior_updates(tmp_path: Path) -> None:
    store = MemoryDynamicStore({"future_key": {"keep": True}})
    adapter = LegacyConfigAdapter(tmp_path / "config.ini", environ={}, dynamic_store=store)
    first = adapter.load()
    second = adapter.load()

    adapter.merge_and_save(first, {"proxy_enabled": True, "first_unknown": "preserved"})
    merged = adapter.merge_and_save(second, {"monitor_enabled": True})

    assert store.values["proxy_enabled"] is True
    assert store.values["monitor_enabled"] is True
    assert store.values["first_unknown"] == "preserved"
    assert store.values["future_key"] == {"keep": True}
    assert merged.proxy_enabled is True
    assert merged.monitor_enabled is True


def test_malformed_ini_falls_back_to_defaults_but_is_not_overwritten(tmp_path: Path) -> None:
    config_path = tmp_path / "config.ini"
    config_path.write_text("db_password = visible-only-in-file", encoding="utf-8")
    adapter = LegacyConfigAdapter(config_path, environ={})

    loaded = adapter.load()

    assert loaded.db_host == "localhost"
    assert loaded.is_first_run is False
    with pytest.raises(RuntimeError, match="Unable to parse existing legacy config"):
        adapter.write_config_ini(loaded)
    assert config_path.read_text(encoding="utf-8") == "db_password = visible-only-in-file"
