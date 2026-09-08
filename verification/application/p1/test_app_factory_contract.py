from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient
from starlette.routing import Mount

from etk.bootstrap.app_factory import create_app
from etk.bootstrap.runtime_supervisor import RuntimeAdapter, RuntimeRole, RuntimeSupervisor


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
    assert not any(isinstance(route, Mount) and route.name == "legacy-compatibility" for route in app.routes)
    assert not hasattr(app.state, "compatibility_app")
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
    assert not (tmp_path / "data").exists()


def test_legacy_compatibility_environment_cannot_restore_wsgi_fallback(tmp_path: Path, monkeypatch) -> None:
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
    assert not any(isinstance(route, Mount) and route.name == "legacy-compatibility" for route in app.routes)
    assert not hasattr(app.state, "compatibility_app")

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


def test_p4_api_startup_ignores_compatibility_flag_and_owns_no_background_services(
    tmp_path: Path,
    monkeypatch,
) -> None:
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
        assert not hasattr(app.state, "compatibility_app")
        assert client.get("/api/health").json() == {"status": "ok"}
        assert not hasattr(app.state, "compatibility_app")
        assert events == [("start", RuntimeRole.API, False)]

    assert events == [("start", RuntimeRole.API, False), ("stop",)]
