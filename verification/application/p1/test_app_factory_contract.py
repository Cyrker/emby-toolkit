from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient
from starlette.routing import Mount

from etk.bootstrap.app_factory import create_app
from etk.bootstrap.runtime_supervisor import RuntimeAdapter, RuntimeRole, RuntimeSupervisor
from etk.compatibility.p115_credentials import (
    LazyPersistentP115CredentialGateway,
    clear_persistent_p115_credential_gateway,
    persistent_p115_credential_gateway,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
API_BASELINE = REPO_ROOT / "verification" / "contracts" / "http" / "legacy-runtime-url-map.json"

EXPECTED_BLUEPRINT_NAMES = (
    "watchlist",
    "collections",
    "custom_collections",
    "actor_subscriptions",
    "logs",
    "database_admin",
    "system",
    "media_api",
    "media_proxy",
    "actions",
    "cover_generator_config",
    "tasks",
    "resubscribe",
    "media_cleanup_bp",
    "user_management_bp",
    "webhook_bp",
    "unified_auth_bp",
    "user_portal_bp",
    "discover_bp",
    "115_bp",
    "subscription_bp",
    "shared_resource_bp",
)


class NoopAdapter(RuntimeAdapter):
    @property
    def config(self) -> Mapping[str, Any]:
        return {}

    def start(self, *, role: RuntimeRole, background: bool) -> None:
        raise AssertionError("runtime must not start in this test")

    def save_config_and_reload(self, _: Mapping[str, Any]) -> None:
        raise AssertionError("reload must not run in this test")

    def stop(self) -> None:
        raise AssertionError("runtime must not stop in this test")


class RecordingAdapter(RuntimeAdapter):
    def __init__(self, events: list[Any]):
        self.events = events

    @property
    def config(self) -> Mapping[str, Any]:
        return {}

    def start(self, *, role: RuntimeRole, background: bool) -> None:
        self.events.append(("start", role, background))

    def save_config_and_reload(self, _: Mapping[str, Any]) -> None:
        raise AssertionError("reload must not run in this test")

    def stop(self) -> None:
        self.events.append(("stop",))


def _contract_bindings(flask_app) -> set[tuple[str, str]]:
    return {
        (method, rule.rule)
        for rule in flask_app.url_map.iter_rules()
        for method in sorted(rule.methods - {"HEAD", "OPTIONS"})
    }


def _baseline_bindings() -> set[tuple[str, str]]:
    payload = json.loads(API_BASELINE.read_text(encoding="utf-8"))
    return {(method, rule["path"]) for rule in payload["rules"] for method in rule["methods_contract"]}


def test_fastapi_health_uses_native_static_fallback_without_legacy_runtime(tmp_path: Path) -> None:
    static_dir = tmp_path / "static"
    static_dir.mkdir()
    (static_dir / "index.html").write_text("<html>fixture SPA</html>", encoding="utf-8")
    runtime = RuntimeSupervisor(lambda: NoopAdapter())
    app = create_app(
        start_runtime=False,
        static_dir=static_dir,
        data_dir=tmp_path / "data",
        secret_key=b"fixture-session-signing-key",
        supervisor=runtime,
    )
    compatibility = app.state.compatibility_app

    assert not any(isinstance(route, Mount) and route.name == "legacy-compatibility" for route in app.routes)
    assert getattr(app.routes[-1], "name", None) == "spa_fallback"
    assert (
        next(index for index, route in enumerate(app.routes) if getattr(route, "path", None) == "/api/health")
        < len(app.routes) - 1
    )

    with TestClient(app) as client:
        response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert response.headers["content-type"] == "application/json"
    assert compatibility.flask_app is None
    assert not (tmp_path / "data").exists()


def test_flask_compatibility_routes_spa_static_and_setup_contract(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    static_dir = tmp_path / "static"
    static_dir.mkdir()
    (static_dir / "index.html").write_text("<html>fixture SPA</html>", encoding="utf-8")
    (static_dir / "fixture.txt").write_text("fixture static", encoding="utf-8")
    monkeypatch.setenv("APP_DATA_DIR", str(data_dir))
    monkeypatch.setenv("ETK_ENABLE_LEGACY_COMPATIBILITY", "1")

    runtime = RuntimeSupervisor(lambda: NoopAdapter())
    app = create_app(
        start_runtime=False,
        static_dir=static_dir,
        data_dir=data_dir,
        secret_key=b"fixture-session-signing-key",
        supervisor=runtime,
    )
    compatibility = app.state.compatibility_app

    with TestClient(app) as client:
        assert client.get("/login").text == "<html>fixture SPA</html>"
        assert client.get("/unknown-spa-route").text == "<html>fixture SPA</html>"
        assert client.post("/unknown-spa-route").status_code == 405
        static_response = client.get("/static/fixture.txt")
        assert static_response.status_code == 200
        assert static_response.text == "fixture static"
        setup_response = client.get("/api/auth/check_status")
        assert setup_response.status_code == 200
        assert setup_response.json() == {
            "status": "setup_required",
            "message": "系统未配置",
        }

    flask_app = compatibility.materialize()
    assert tuple(flask_app.blueprints) == EXPECTED_BLUEPRINT_NAMES
    assert len(flask_app.blueprints) == 22

    compatibility_bindings = _contract_bindings(flask_app)
    assert len(list(flask_app.url_map.iter_rules())) == 344
    assert len(compatibility_bindings) == 361
    assert compatibility_bindings | {("GET", "/api/health")} == _baseline_bindings()

    from handler.tg_userbot import is_tg_task_consumer_running

    assert not is_tg_task_consumer_running()


def test_p4_api_startup_keeps_compatibility_app_lazy_and_owns_no_background_services(
    tmp_path: Path,
    monkeypatch,
) -> None:
    clear_persistent_p115_credential_gateway()
    data_dir = tmp_path / "data"
    static_dir = tmp_path / "static"
    static_dir.mkdir()
    (static_dir / "index.html").write_text("<html>fixture SPA</html>", encoding="utf-8")
    monkeypatch.setenv("APP_DATA_DIR", str(data_dir))
    monkeypatch.setenv("ETK_ENABLE_LEGACY_COMPATIBILITY", "1")
    events: list[Any] = []
    runtime = RuntimeSupervisor(lambda: RecordingAdapter(events))
    app = create_app(
        start_runtime=True,
        static_dir=static_dir,
        data_dir=data_dir,
        secret_key=b"fixture-session-signing-key",
        supervisor=runtime,
    )

    with TestClient(app) as client:
        assert app.state.compatibility_app.flask_app is None
        assert client.get("/api/health").json() == {"status": "ok"}
        assert app.state.compatibility_app.flask_app is None
        assert events == [("start", RuntimeRole.API, False)]
        assert isinstance(persistent_p115_credential_gateway(), LazyPersistentP115CredentialGateway)

    assert events == [("start", RuntimeRole.API, False), ("stop",)]
    assert persistent_p115_credential_gateway() is None
