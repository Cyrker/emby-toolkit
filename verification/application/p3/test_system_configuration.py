from __future__ import annotations

import json
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient
from pydantic import SecretStr

from etk.bootstrap.app_factory import create_app
from etk.bootstrap.runtime_supervisor import RuntimeSupervisor
from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.system import configuration_router
from etk.domains.system.configuration import ConfigurationService
from etk.platform import AppContainer, Settings

REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "http" / "system-config-routes.json"
SECRET_KEY = b"p3-system-configuration-session-key"
EMBY_USER_ID = "a" * 32
EMBY_URL = "http://example.invalid"


class Permissions:
    def __init__(self, admins: set[str] | None = None) -> None:
        self.admins = admins or set()

    def is_admin(self, user_id: str) -> bool:
        return user_id in self.admins


class SettingsRepository:
    def __init__(self) -> None:
        self.values: dict[str, Any] = {
            "custom_theme": {"primary": "fixture-blue"},
        }

    def load(self, key: str) -> Any:
        return deepcopy(self.values.get(key))


class SettingsHolder:
    def __init__(self, settings: Settings) -> None:
        self.value = settings


class Writer:
    def __init__(self, holder: SettingsHolder) -> None:
        self.holder = holder
        self.updates: list[dict[str, Any]] = []
        self.error: Exception | None = None

    def save(self, updates: Mapping[str, Any]) -> Settings:
        if self.error is not None:
            raise self.error
        copied = deepcopy(dict(updates))
        self.updates.append(copied)
        self.holder.value = self.holder.value.with_updates(copied)
        return self.holder.value


class Gateway:
    def __init__(self) -> None:
        self.library_reads = 0
        self.origin_syncs: list[str] = []
        self.discovery_syncs: list[str] = []

    def preview_etk_url(self, config: Mapping[str, Any]) -> str:
        del config
        return "http://example.invalid"

    def preview_proxy_url(self, config: Mapping[str, Any]) -> str:
        del config
        return "http://proxy.example.invalid"

    def validate_internal_url(self, value: str) -> str:
        if not value.startswith("http://"):
            raise ValueError("fixture URL must use http")
        return value.rstrip("/")

    def valid_library_ids(self, config: Mapping[str, Any]) -> set[str] | None:
        assert config["emby_api_key"] == "fixture-token"
        self.library_reads += 1
        return {"fixture-library-valid", "fixture-native-valid"}

    def server_id(self) -> str | None:
        return "fixture-server"

    def sync_plugin_origin(self, config: Mapping[str, Any], etk_url: str) -> bool:
        assert config["emby_api_key"] == "fixture-token"
        self.origin_syncs.append(etk_url)
        return True

    def sync_plugin_discovery(self, config: Mapping[str, Any], discovery_url: str) -> bool:
        assert config["emby_api_key"] == "fixture-token"
        self.discovery_syncs.append(discovery_url)
        return True


def _settings(*, auth_enabled: bool = True, configured: bool = True) -> Settings:
    values: dict[str, Any] = {
        "auth_enabled": auth_enabled,
        "future_dynamic_key": "preserved",
        "db_password": SecretStr("fixture-db-password"),
        "emby_api_key": SecretStr("fixture-token") if configured else SecretStr(""),
        "emby_auth_mode": "user_token" if configured else "",
        "emby_server_url": EMBY_URL if configured else "",
        "emby_user_id": EMBY_USER_ID if configured else "",
    }
    return Settings(**values)


def _build_app(
    tmp_path: Path,
    *,
    auth_enabled: bool = True,
    configured: bool = True,
    admins: set[str] | None = None,
):
    holder = SettingsHolder(_settings(auth_enabled=auth_enabled, configured=configured))
    repository = SettingsRepository()
    writer = Writer(holder)
    gateway = Gateway()
    service = ConfigurationService(
        settings=lambda: holder.value,
        repository=repository,
        writer=writer,
        gateway=gateway,
    )
    container = AppContainer(
        holder.value,
        repository_providers={"identity": lambda _: Permissions(admins)},
        use_case_providers={"system-configuration": lambda _: service},
    )
    app = create_app(
        start_runtime=False,
        data_dir=tmp_path / "config",
        secret_key=SECRET_KEY,
        supervisor=RuntimeSupervisor(),
        container=container,
    )
    return app, holder, writer, gateway


def _cookie(values: Mapping[str, Any]) -> str:
    return LegacySessionCodec.from_secret_key(SECRET_KEY).dumps(values)


def _case(case_id: str) -> dict[str, Any]:
    payload = json.loads(CONFIG_FIXTURE.read_text(encoding="utf-8"))
    return next(case for case in payload["cases"] if case["case_id"] == case_id)


def _assert_case(response, case_id: str) -> None:
    expected = _case(case_id)["response"]
    assert response.status_code == expected["status"]
    assert response.json() == expected["body"]


def test_config_fixture_covers_read_and_write() -> None:
    fixture = json.loads(CONFIG_FIXTURE.read_text(encoding="utf-8"))
    assert {(route["method"], route["path"]) for route in fixture["routes"]} == {
        ("GET", "/api/config"),
        ("POST", "/api/config"),
    }
    assert fixture["provenance"]["external_validation"] == "pending"


def test_config_policy_does_not_treat_auth_disabled_as_setup_access(tmp_path: Path) -> None:
    app, _, _, _ = _build_app(tmp_path, auth_enabled=False, configured=True)
    with TestClient(app) as client:
        _assert_case(client.get("/api/config"), "read-denied")
        _assert_case(client.post("/api/config", json={"proxy_enabled": True}), "save-denied")
        _assert_case(client.post("/api/config"), "save-denied")
        _assert_case(client.post("/api/config", json=[]), "save-denied")


def test_unconfigured_setup_mode_and_live_db_admin_policy(tmp_path: Path) -> None:
    unconfigured, _, _, _ = _build_app(tmp_path / "unconfigured", configured=False)
    with TestClient(unconfigured) as client:
        assert client.get("/api/config").status_code == 200

    setup_app, _, _, _ = _build_app(tmp_path / "setup")
    with TestClient(setup_app) as client:
        client.cookies.set("session", _cookie({"is_setup_mode": True}))
        assert client.get("/api/config").status_code == 200

    admin_app, _, _, _ = _build_app(tmp_path / "admin", admins={"fixture-admin"})
    with TestClient(admin_app) as client:
        client.cookies.set(
            "session",
            _cookie({"emby_user_id": "fixture-user", "emby_is_admin": True}),
        )
        _assert_case(client.get("/api/config"), "read-denied")
        client.cookies.set("session", _cookie({"emby_user_id": "fixture-admin"}))
        assert client.get("/api/config").status_code == 200


def test_read_masks_secrets_and_adds_legacy_derived_fields(tmp_path: Path) -> None:
    app, _, _, _ = _build_app(tmp_path, admins={"fixture-admin"})
    with TestClient(app) as client:
        client.cookies.set("session", _cookie({"emby_user_id": "fixture-admin"}))
        response = client.get("/api/config")
    assert response.status_code == 200
    payload = response.json()
    assert payload["db_password"] == "********"
    assert payload["emby_api_key"] == "********"
    assert payload["future_dynamic_key"] == "preserved"
    assert payload["emby_server_id"] == "fixture-server"
    assert payload["custom_theme"] == {"primary": "fixture-blue"}
    assert {"pro_license_key", "pro_expire_time", "is_pro_active"}.isdisjoint(payload)
    assert payload["etk_server_url_auto_detected"] == EMBY_URL
    assert payload["proxy_discovery_url_auto_detected"] == "http://proxy.example.invalid"


def test_save_restores_masks_filters_libraries_and_uses_partial_update(tmp_path: Path) -> None:
    app, holder, writer, gateway = _build_app(tmp_path, admins={"fixture-admin"})
    request_payload = {
        "emby_api_key": "********",
        "emby_server_url": "http://attacker.example.invalid",
        "emby_user_id": "b" * 32,
        "emby_auth_mode": "api_key",
        "etk_server_url_manual": EMBY_URL + "/",
        "libraries_to_process": ["fixture-library-valid", "fixture-library-stale"],
        "proxy_enabled": True,
        "proxy_lock_discovery_address": True,
        "proxy_discovery_url_manual": "http://proxy.example.invalid",
        "proxy_native_view_selection": ["fixture-native-valid", "fixture-native-stale"],
        "etk_server_url_auto_detected": "response-only",
        "custom_theme": {"ignored": True},
        "new_unknown_key": "ignored",
    }
    with TestClient(app) as client:
        client.cookies.set("session", _cookie({"emby_user_id": "fixture-admin"}))
        response = client.post("/api/config", json=request_payload)
    _assert_case(response, "save-success")

    assert len(writer.updates) == 1
    updates = writer.updates[0]
    assert updates["emby_server_url"] == EMBY_URL
    assert updates["emby_api_key"] == "fixture-token"
    assert updates["emby_user_id"] == EMBY_USER_ID
    assert updates["emby_auth_mode"] == "user_token"
    assert updates["libraries_to_process"] == ["fixture-library-valid"]
    assert updates["proxy_native_view_selection"] == ["fixture-native-valid"]
    assert "etk_server_url_auto_detected" not in updates
    assert "custom_theme" not in updates
    assert "new_unknown_key" not in updates
    assert "future_dynamic_key" not in updates
    assert holder.value.get("future_dynamic_key") == "preserved"
    assert gateway.library_reads == 1
    assert gateway.origin_syncs == [EMBY_URL]
    assert gateway.discovery_syncs == ["http://proxy.example.invalid"]


def test_tmdb_and_network_proxy_updates_do_not_call_emby(tmp_path: Path) -> None:
    app, _, writer, gateway = _build_app(tmp_path, admins={"fixture-admin"})

    with TestClient(app) as client:
        client.cookies.set("session", _cookie({"emby_user_id": "fixture-admin"}))
        response = client.post(
            "/api/config",
            json={
                "tmdb_api_key": "fixture-tmdb-key",
                "tmdb_api_base_url": "https://tmdb.fixture.invalid/3",
                "network_proxy_enabled": True,
                "network_http_proxy_url": "http://proxy.fixture.invalid:8080",
            },
        )

    assert response.status_code == 200
    assert len(writer.updates) == 1
    assert gateway.library_reads == 0
    assert gateway.origin_syncs == []
    assert gateway.discovery_syncs == []


def test_plugin_configuration_sync_is_scoped_to_related_fields(tmp_path: Path) -> None:
    app, _, _, gateway = _build_app(tmp_path, admins={"fixture-admin"})

    with TestClient(app) as client:
        client.cookies.set("session", _cookie({"emby_user_id": "fixture-admin"}))
        origin_response = client.post("/api/config", json={"etk_server_url_manual": EMBY_URL + "/"})
        discovery_response = client.post("/api/config", json={"proxy_port": 8197})

    assert origin_response.status_code == 200
    assert discovery_response.status_code == 200
    assert gateway.library_reads == 0
    assert gateway.origin_syncs == [EMBY_URL]
    assert gateway.discovery_syncs == [""]


def test_save_route_offloads_synchronous_configuration_work(tmp_path: Path, monkeypatch) -> None:
    app, _, _, _ = _build_app(tmp_path, admins={"fixture-admin"})
    offloaded: list[str] = []

    async def record_offload(function, *args, **kwargs):
        offloaded.append(function.__name__)
        return function(*args, **kwargs)

    monkeypatch.setattr(configuration_router, "run_in_threadpool", record_offload)
    with TestClient(app) as client:
        client.cookies.set("session", _cookie({"emby_user_id": "fixture-admin"}))
        response = client.post("/api/config", json={"tmdb_api_key": "fixture-tmdb-key"})

    assert response.status_code == 200
    assert offloaded == ["_config_denial", "save"]


def test_save_validation_and_compensated_writer_failure_return_legacy_errors(tmp_path: Path) -> None:
    app, _, writer, _ = _build_app(tmp_path, admins={"fixture-admin"})
    with TestClient(app) as client:
        client.cookies.set("session", _cookie({"emby_user_id": "fixture-admin"}))
        _assert_case(client.post("/api/config", json={}), "save-empty")
        invalid = client.post("/api/config", json={"proxy_enabled": True})
        assert invalid.status_code == 200

        writer.error = RuntimeError("fixture reload was rolled back")
        failed = client.post("/api/config", json={"proxy_enabled": False})
    assert failed.status_code == 500
    assert failed.json() == {"error": "保存配置时发生服务器内部错误: fixture reload was rolled back"}
