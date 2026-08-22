from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

from etk.bootstrap import app_factory
from etk.bootstrap.app_factory import create_app


def test_app_factory_wires_settings_container_trace_and_metrics(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class EmptyDynamicStore:
        def load_dynamic_config(self) -> Mapping[str, Any]:
            return {}

        def save_dynamic_config(self, values: Mapping[str, Any]) -> None:
            raise AssertionError(f"unexpected save: {values}")

        def merge_dynamic_config(self, updates: Mapping[str, Any]) -> Mapping[str, Any]:
            raise AssertionError(f"unexpected merge: {updates}")

    data_dir = tmp_path / "greenfield-config"
    database_urls: list[str] = []
    monkeypatch.setenv("APP_DATA_DIR", str(data_dir))
    monkeypatch.setenv("DB_HOST", "greenfield-db")
    monkeypatch.setenv("DB_PORT", "6543")
    monkeypatch.setenv("DB_PASSWORD", "local-test-password")
    monkeypatch.setenv("ETK_DATABASE_URL", "")
    monkeypatch.setattr(
        app_factory,
        "PostgresLegacyConfigStore",
        lambda url: (database_urls.append(url), EmptyDynamicStore())[1],
    )

    app = create_app(start_runtime=False)

    assert app.state.settings.db_host == "greenfield-db"
    assert app.state.settings.db_port == 6543
    assert app.state.settings.db_password.get_secret_value() == "local-test-password"
    assert database_urls == ["postgresql+psycopg://postgres:local-test-password@greenfield-db:6543/emby_toolkit"]
    assert app.state.container.closed is False
    assert not data_dir.exists()

    with TestClient(app) as client:
        health = client.get("/api/health", headers={"x-trace-id": "p2-wiring-trace"})
        metrics = client.get("/metrics")

    assert health.status_code == 200
    assert health.headers["x-trace-id"] == "p2-wiring-trace"
    assert metrics.status_code == 200
    assert metrics.headers["content-type"].startswith("text/plain")
    assert "etk_api_requests_total" in metrics.text
    assert app.state.container.closed is True
    assert not data_dir.exists()


def test_default_container_loads_dynamic_settings_when_greenfield_database_is_configured(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class DynamicStore:
        def load_dynamic_config(self) -> Mapping[str, Any]:
            return {"proxy_enabled": True, "future_dynamic_key": "preserved"}

        def save_dynamic_config(self, values: Mapping[str, Any]) -> None:
            raise AssertionError(f"unexpected save: {values}")

        def merge_dynamic_config(self, updates: Mapping[str, Any]) -> Mapping[str, Any]:
            raise AssertionError(f"unexpected merge: {updates}")

    monkeypatch.setenv("APP_DATA_DIR", str(tmp_path / "config"))
    monkeypatch.setenv("ETK_DATABASE_URL", "postgresql+psycopg://etk:local-only@db:5432/etk")
    monkeypatch.setattr(app_factory, "PostgresLegacyConfigStore", lambda _url: DynamicStore())

    app = create_app(start_runtime=False)

    assert app.state.settings.proxy_enabled is True
    assert app.state.settings.get("future_dynamic_key") == "preserved"
    assert app.state.config_adapter is not None


def test_default_container_loads_dynamic_settings_from_legacy_database_ini(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class DynamicStore:
        def load_dynamic_config(self) -> Mapping[str, Any]:
            return {"proxy_enabled": True, "future_ini_dynamic_key": "preserved"}

        def save_dynamic_config(self, values: Mapping[str, Any]) -> None:
            raise AssertionError(f"unexpected save: {values}")

        def merge_dynamic_config(self, updates: Mapping[str, Any]) -> Mapping[str, Any]:
            raise AssertionError(f"unexpected merge: {updates}")

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "config.ini").write_text(
        """
[Database]
db_host = ini-db
db_port = 6432
db_user = ini-user
db_password = ini-password
db_name = ini-name
""".strip(),
        encoding="utf-8",
    )
    for key in (*app_factory._LEGACY_DATABASE_ENVIRONMENT, "ETK_DATABASE_URL"):
        monkeypatch.delenv(key, raising=False)
    database_urls: list[str] = []
    monkeypatch.setattr(
        app_factory,
        "PostgresLegacyConfigStore",
        lambda url: (database_urls.append(url), DynamicStore())[1],
    )

    app = create_app(start_runtime=False, data_dir=config_dir)

    assert database_urls == ["postgresql+psycopg://ini-user:ini-password@ini-db:6432/ini-name"]
    assert app.state.settings.proxy_enabled is True
    assert app.state.settings.get("future_ini_dynamic_key") == "preserved"
