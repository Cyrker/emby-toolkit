from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient
from pydantic import SecretStr

import task_manager
from etk.domains.identity.compatibility import LegacyEmbyIdentityGateway
from etk.platform import Settings
from etk.platform.control_plane import ControlPlaneClient, ControlPlaneError
from etk.processes.control_plane import (
    CommandLedger,
    ControlPlaneCommand,
    ControlPlaneController,
    ControlPlaneUpdateDisabledError,
    create_control_plane_app,
    ensure_control_plane_token,
)
from tasks import system_update

REPO_ROOT = Path(__file__).resolve().parents[3]


class FakeContainer:
    def __init__(self, *, container_id: str, name: str, service: str, project: str = "fixture") -> None:
        self.id = container_id
        self.name = name
        self.restart_calls = 0
        self.restart_error: Exception | None = None
        self.image = SimpleNamespace(tags=["fixture/image:latest"])
        self.attrs = {
            "Config": {
                "Image": "fixture/image:latest",
                "Labels": {
                    "com.docker.compose.project": project,
                    "com.docker.compose.service": service,
                },
            },
            "State": {
                "Status": "running",
                "StartedAt": "before",
                "Health": {"Status": "healthy"},
            },
        }

    def restart(self, **_kwargs) -> None:
        self.restart_calls += 1
        self.attrs["State"]["StartedAt"] = "after"
        if self.restart_error is not None:
            raise self.restart_error

    def reload(self) -> None:
        return None


class FakeDockerClient:
    def __init__(self, own: FakeContainer, target: FakeContainer) -> None:
        self.closed = False
        self._all = [own, target]
        self.containers = SimpleNamespace(
            get=lambda identity: next(item for item in self._all if identity in {item.id, item.name}),
            list=lambda **_kwargs: list(self._all),
        )

    def close(self) -> None:
        self.closed = True


class FakeEmbyContainer:
    def __init__(
        self,
        *,
        name: str,
        service: str,
        ip_address: str,
        plugin_install_opt_in: bool = False,
    ) -> None:
        labels = {"com.docker.compose.service": service}
        if plugin_install_opt_in:
            labels["com.embytoolkit.control-plane.plugin-install"] = "true"
        self.name = name
        self.restart_calls = 0
        self.attrs = {
            "Config": {
                "Image": "emby/embyserver:latest",
                "Labels": labels,
                "ExposedPorts": {"8096/tcp": {}},
            },
            "NetworkSettings": {
                "Networks": {
                    "fixture": {
                        "IPAddress": ip_address,
                        "Aliases": [name],
                    }
                },
                "Ports": {"8096/tcp": [{"HostPort": "8096"}]},
            },
        }

    def restart(self, **_kwargs) -> None:
        self.restart_calls += 1


def test_emby_detection_ignores_unrelated_compose_metadata_paths() -> None:
    from handler.etk_plugin_update import _is_emby_container

    container = SimpleNamespace(
        name="fixture-embyserver-1",
        attrs={
            "Config": {
                "Image": "python:3.12-slim",
                "Labels": {
                    "com.docker.compose.service": "embyserver",
                    "com.docker.compose.project.config_files": "/workspace/emby-toolkit/docker/compose.yml",
                },
            }
        },
    )

    assert _is_emby_container(container) is True


def test_restart_is_explicit_observed_and_idempotent(tmp_path: Path, monkeypatch) -> None:
    own = FakeContainer(container_id="control-id", name="fixture-control", service="control-plane")
    target = FakeContainer(container_id="api-id", name="fixture-api", service="api")
    clients = []

    def factory():
        client = FakeDockerClient(own, target)
        clients.append(client)
        return client

    monkeypatch.setenv("HOSTNAME", "control-id")
    controller = ControlPlaneController(
        CommandLedger(tmp_path / "commands.json"),
        docker_client_factory=factory,
        allowed_services={"api"},
        own_service="control-plane",
        sleep=lambda _seconds: None,
    )
    command = ControlPlaneCommand(
        request_id="fixture-restart-job",
        action="restart",
        payload={"target_service": "api"},
    )

    first = controller.execute(command)
    second = controller.execute(command)

    assert target.restart_calls == 1
    assert first["target_service"] == "api"
    assert first["observed"] == {
        "status": "running",
        "health": "healthy",
        "started_at": "after",
    }
    assert [item["stage"] for item in first["stages"]] == [
        "requested",
        "prepared",
        "dispatched",
        "observed-complete",
    ]
    assert second == first
    assert all(client.closed for client in clients)


def test_update_is_fail_closed_and_never_uses_caller_selected_helpers(tmp_path: Path) -> None:
    docker_calls = []

    def docker_factory():
        docker_calls.append(True)
        raise AssertionError("disabled update must not access Docker")

    ledger = CommandLedger(tmp_path / "commands.json")
    controller = ControlPlaneController(
        ledger,
        docker_client_factory=docker_factory,
        allowed_services={"api"},
    )

    with pytest.raises(PermissionError, match="caller-provided"):
        controller.execute(
            ControlPlaneCommand(
                request_id="caller-helper",
                action="update",
                payload={
                    "target_service": "api",
                    "strategy": "docker_helper",
                    "helper_image": "attacker.invalid/helper:latest",
                },
            )
        )
    with pytest.raises(ControlPlaneUpdateDisabledError, match="release workflow"):
        controller.execute(
            ControlPlaneCommand(
                request_id="server-default",
                action="update",
                payload={"target_service": "api"},
            )
        )

    assert docker_calls == []
    caller_record = ledger.get("caller-helper")
    server_record = ledger.get("server-default")
    assert caller_record is not None and caller_record["stage"] == "failed-no-effect"
    assert server_record is not None and server_record["stage"] == "failed-no-effect"
    assert "attacker.invalid" not in ledger.path.read_text(encoding="utf-8")


def test_plugin_failure_certainty_tracks_whether_dispatch_started(tmp_path: Path) -> None:
    command = ControlPlaneCommand(
        request_id="fixture-plugin-certainty",
        action="plugin-install",
        payload={
            "emby_url": "http://emby.invalid",
            "emby_token": "fixture-token",
            "etk_origin": "http://etk.invalid",
        },
    )
    calls = []

    def fail_before_effect(_command, _effect_started):
        calls.append("before")
        raise RuntimeError("fixture pre-effect failure: http://emby.invalid?api_key=fixture-token")

    ledger = CommandLedger(tmp_path / "commands.json")
    controller = ControlPlaneController(
        ledger,
        plugin_preflight=lambda _command: {"container_name": "fixture-emby"},
        plugin_installer=fail_before_effect,
    )

    with pytest.raises(RuntimeError, match="pre-effect"):
        controller.execute(command)

    assert calls == ["before"]
    status = controller.status(command.request_id)
    record = ledger.get(command.request_id)
    assert status is not None and status["certainty"] == "confirmed-no-effect"
    assert record is not None and record["stage"] == "failed-no-effect"
    assert "fixture-token" not in ledger.path.read_text(encoding="utf-8")

    def fail_after_effect(_command, effect_started):
        calls.append("after")
        effect_started()
        raise RuntimeError("fixture post-dispatch failure")

    controller = ControlPlaneController(
        ledger,
        plugin_preflight=lambda _command: {"container_name": "fixture-emby"},
        plugin_installer=fail_after_effect,
    )
    with pytest.raises(RuntimeError, match="post-dispatch"):
        controller.execute(command)

    assert calls == ["before", "after"]
    status = controller.status(command.request_id)
    record = ledger.get(command.request_id)
    assert status is not None and status["certainty"] == "unknown"
    assert record is not None and record["stage"] == "failed-unknown"

    with pytest.raises(RuntimeError, match="cannot be proven complete"):
        controller.execute(command)
    assert calls == ["before", "after"]


def test_restart_reconciles_a_disconnected_dispatch_without_restarting_twice(tmp_path: Path, monkeypatch) -> None:
    own = FakeContainer(container_id="control-id", name="fixture-control", service="control-plane")
    target = FakeContainer(container_id="api-id", name="fixture-api", service="api")

    target.restart_error = RuntimeError("fixture Docker connection lost")
    monkeypatch.setenv("HOSTNAME", "control-id")
    ledger = CommandLedger(tmp_path / "commands.json")
    controller = ControlPlaneController(
        ledger,
        docker_client_factory=lambda: FakeDockerClient(own, target),
        allowed_services={"api"},
        own_service="control-plane",
        sleep=lambda _seconds: None,
    )
    command = ControlPlaneCommand(
        request_id="fixture-restart-reconcile",
        action="restart",
        payload={"target_service": "api"},
    )

    with pytest.raises(RuntimeError, match="connection lost"):
        controller.execute(command)
    record = ledger.get(command.request_id)
    assert record is not None and record["stage"] == "failed-unknown"

    target.restart_error = AssertionError("reconciliation must not restart twice")
    result = controller.execute(command)

    assert result["certainty"] == "observed-complete"
    assert target.restart_calls == 1


def test_control_plane_http_requires_the_shared_token(tmp_path: Path) -> None:
    token_file = tmp_path / "control-plane.token"
    token = ensure_control_plane_token(token_file)

    class Controller:
        def execute(self, command):
            return {"ok": True, "request_id": command.request_id}

    app = create_control_plane_app(
        controller=Controller(),  # type: ignore[arg-type]
        token_file=token_file,
        ledger_file=tmp_path / "commands.json",
    )
    with TestClient(app) as client:
        assert client.get("/health").json() == {"status": "ok", "role": "control-plane"}
        unauthorized = client.post(
            "/v1/commands",
            json={"request_id": "fixture", "action": "restart", "payload": {"target_service": "api"}},
        )
        authorized = client.post(
            "/v1/commands",
            headers={"authorization": f"Bearer {token}"},
            json={"request_id": "fixture", "action": "restart", "payload": {"target_service": "api"}},
        )

    assert unauthorized.status_code == 401
    assert authorized.json() == {"ok": True, "request_id": "fixture"}
    assert os.stat(token_file).st_mode & 0o777 == 0o600


def test_control_plane_client_uses_job_identity_and_never_exposes_the_token(tmp_path: Path) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("fixture-control-plane-token-value-123456", encoding="utf-8")
    captured: dict[str, Any] = {"requests": []}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["authorization"] = request.headers["authorization"]
        captured["requests"].append((request.url.path, json.loads(request.content)))
        if request.url.path.endswith("/prepare"):
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "request_id": "fixture-job",
                    "stage": "prepared",
                    "certainty": "confirmed-no-effect",
                    "stages": [],
                },
            )
        return httpx.Response(
            200,
            json={
                "ok": True,
                "request_id": "fixture-job",
                "stage": "observed-complete",
                "certainty": "observed-complete",
                "stages": [],
            },
        )

    client = ControlPlaneClient(
        base_url="http://control-plane:5260",
        token_file=token_file,
        transport=httpx.MockTransport(handler),
    )

    result = client.restart_service("api", request_id="fixture-job")

    assert result["ok"] is True
    expected_payload = {
        "request_id": "fixture-job",
        "action": "restart",
        "payload": {"target_service": "api"},
    }
    assert captured["requests"] == [
        ("/v1/commands/prepare", expected_payload),
        ("/v1/commands/execute", expected_payload),
    ]
    assert captured["authorization"].startswith("Bearer ")
    assert "fixture-control-plane-token-value-123456" not in repr(result)


def test_control_plane_client_clears_effect_marker_after_confirmed_pre_effect_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("fixture-control-plane-token-value-123456", encoding="utf-8")
    effects = []
    monkeypatch.setattr(
        task_manager,
        "record_persistent_effect_started",
        lambda label: effects.append(("start", label)),
    )
    monkeypatch.setattr(
        task_manager,
        "record_persistent_effect_abandoned",
        lambda label: effects.append(("abandon", label)),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/prepare"):
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "request_id": "fixture-no-effect",
                    "stage": "prepared",
                    "certainty": "confirmed-no-effect",
                },
            )
        return httpx.Response(
            500,
            json={
                "ok": False,
                "request_id": "fixture-no-effect",
                "stage": "failed-no-effect",
                "certainty": "confirmed-no-effect",
                "message": "fixture rejected before dispatch",
            },
        )

    client = ControlPlaneClient(
        base_url="http://control-plane:5260",
        token_file=token_file,
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(ControlPlaneError, match="before dispatch") as raised:
        client.restart_service("api", request_id="fixture-no-effect")

    assert raised.value.certainty == "confirmed-no-effect"
    assert effects == [
        ("start", "control-plane:restart:api"),
        ("abandon", "control-plane:restart:api"),
    ]


@pytest.mark.parametrize(
    ("status_payload", "expected_certainty", "expected_effect"),
    [
        (
            {
                "ok": True,
                "request_id": "fixture-disconnect",
                "stage": "observed-complete",
                "certainty": "observed-complete",
                "target_service": "api",
            },
            "observed-complete",
            "commit",
        ),
        (
            {
                "ok": False,
                "request_id": "fixture-disconnect",
                "stage": "failed-unknown",
                "certainty": "unknown",
            },
            "unknown",
            None,
        ),
    ],
)
def test_control_plane_client_reconciles_a_lost_execute_response(
    tmp_path: Path,
    monkeypatch,
    status_payload,
    expected_certainty,
    expected_effect,
) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("fixture-control-plane-token-value-123456", encoding="utf-8")
    effects = []
    monkeypatch.setattr(task_manager, "record_persistent_effect_started", lambda _label: effects.append("start"))
    monkeypatch.setattr(
        task_manager,
        "record_persistent_effect_committed",
        lambda _label: effects.append("commit"),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/prepare"):
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "request_id": "fixture-disconnect",
                    "stage": "prepared",
                    "certainty": "confirmed-no-effect",
                },
            )
        if request.method == "POST":
            raise httpx.ReadTimeout("fixture execute response lost", request=request)
        return httpx.Response(200, json=status_payload)

    client = ControlPlaneClient(
        base_url="http://control-plane:5260",
        token_file=token_file,
        transport=httpx.MockTransport(handler),
    )

    if expected_certainty == "observed-complete":
        result = client.restart_service("api", request_id="fixture-disconnect")
        assert result["target_service"] == "api"
    else:
        with pytest.raises(ControlPlaneError) as raised:
            client.restart_service("api", request_id="fixture-disconnect")
        assert raised.value.certainty == expected_certainty

    expected = ["start"] + ([expected_effect] if expected_effect else [])
    assert effects == expected


def test_control_plane_client_update_payload_cannot_include_strategy_or_helper_image(tmp_path: Path) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("fixture-control-plane-token-value-123456", encoding="utf-8")
    captured = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append((request.url.path, json.loads(request.content)))
        stage = "prepared" if request.url.path.endswith("/prepare") else "observed-complete"
        certainty = "confirmed-no-effect" if stage == "prepared" else "observed-complete"
        return httpx.Response(
            200,
            json={
                "ok": True,
                "request_id": "fixture-update",
                "stage": stage,
                "certainty": certainty,
                "stages": [],
            },
        )

    client = ControlPlaneClient(
        base_url="http://control-plane:5260",
        token_file=token_file,
        transport=httpx.MockTransport(handler),
    )

    result = client.update_service("api", request_id="fixture-update")

    assert result["ok"] is True
    expected_payload = {
        "request_id": "fixture-update",
        "action": "update",
        "payload": {"target_service": "api"},
    }
    assert captured == [
        ("/v1/commands/prepare", expected_payload),
        ("/v1/commands/execute", expected_payload),
    ]
    with pytest.raises(ControlPlaneError, match="caller-provided"):
        client.update_service(
            "api",
            request_id="rejected-update",
            strategy="docker_helper",
            helper_image="attacker.invalid/helper:latest",
        )


def test_control_plane_client_preserves_explicit_fail_closed_error(tmp_path: Path) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("fixture-control-plane-token-value-123456", encoding="utf-8")
    client = ControlPlaneClient(
        base_url="http://control-plane:5260",
        token_file=token_file,
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                500,
                json={
                    "ok": False,
                    "message": "Greenfield split-role update requires a complete release workflow",
                },
            )
        ),
    )

    with pytest.raises(ControlPlaneError, match="complete release workflow"):
        client.update_service("api", request_id="fixture-disabled-update")


def test_invalid_control_plane_url_is_confirmed_before_any_effect(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("ETK_CONTROL_PLANE_URL", raising=False)
    client = ControlPlaneClient(base_url="", token_file=tmp_path / "unused-token")

    with pytest.raises(ControlPlaneError, match="not configured") as raised:
        client.restart_service("api", request_id="fixture-invalid-url")

    assert raised.value.certainty == "confirmed-no-effect"


def test_plugin_command_does_not_persist_emby_credentials(tmp_path: Path, monkeypatch) -> None:
    from handler import etk_plugin_update

    captured = {}

    def install(*_args, **kwargs):
        kwargs.pop("effect_started")()
        captured.update(kwargs)
        return {"ok": True, "status": "installed", "message": "fixture plugin ready"}

    monkeypatch.setattr(etk_plugin_update, "ensure_etk_plugin_installed", install)
    ledger_path = tmp_path / "commands.json"
    controller = ControlPlaneController(
        CommandLedger(ledger_path),
        docker_client_factory=lambda: (_ for _ in ()).throw(AssertionError("plugin action owns Docker internally")),
        allowed_emby_services={"emby"},
        plugin_preflight=lambda _command: {"container_name": "fixture-emby"},
    )

    result = controller.execute(
        ControlPlaneCommand(
            request_id="fixture-plugin-job",
            action="plugin-install",
            payload={
                "emby_url": "http://emby.invalid",
                "emby_token": "fixture-emby-secret-token",
                "etk_origin": "http://etk.invalid",
            },
        )
    )

    assert result["ok"] is True
    assert captured == {"allowed_services": {"emby"}}
    assert "fixture-emby-secret-token" not in ledger_path.read_text(encoding="utf-8")


def test_plugin_container_resolution_rejects_port_only_and_single_candidate_fallback(monkeypatch) -> None:
    from handler import etk_plugin_update

    local_emby = FakeEmbyContainer(
        name="unrelated-local-emby",
        service="emby",
        ip_address="172.18.0.20",
    )
    client = SimpleNamespace(containers=SimpleNamespace(list=lambda: [local_emby]))
    monkeypatch.setattr(
        etk_plugin_update,
        "_emby_get_json",
        lambda *_args: {"Id": "authorized-remote-server"},
    )
    monkeypatch.setattr(etk_plugin_update, "_public_server_id", lambda _url: "unrelated-local-server")

    with pytest.raises(RuntimeError, match="Server ID"):
        etk_plugin_update._resolve_emby_container(
            client,
            "http://remote-emby.invalid:8096",
            "fixture-token",
            allowed_services={"emby"},
        )

    monkeypatch.setattr(etk_plugin_update, "_public_server_id", lambda _url: "authorized-remote-server")
    assert (
        etk_plugin_update._resolve_emby_container(
            client,
            "http://remote-emby.invalid:8096",
            "fixture-token",
            allowed_services={"emby"},
        )
        is local_emby
    )


def test_plugin_install_never_writes_or_restarts_a_server_id_mismatch(monkeypatch) -> None:
    from handler import etk_plugin_update

    local_emby = FakeEmbyContainer(
        name="unrelated-local-emby",
        service="emby",
        ip_address="172.18.0.24",
    )

    class Client:
        def __init__(self) -> None:
            self.closed = False
            self.containers = SimpleNamespace(list=lambda: [local_emby])

        def close(self) -> None:
            self.closed = True

    client = Client()
    monkeypatch.setattr(etk_plugin_update, "is_etk_plugin_installed", lambda *_args: False)
    monkeypatch.setattr(etk_plugin_update.docker, "from_env", lambda: client)
    monkeypatch.setattr(
        etk_plugin_update,
        "_emby_get_json",
        lambda *_args: {"Id": "authorized-remote-server"},
    )
    monkeypatch.setattr(etk_plugin_update, "_public_server_id", lambda _url: "unrelated-local-server")
    monkeypatch.setattr(
        etk_plugin_update,
        "_resolve_plugins_path",
        lambda _container: (_ for _ in ()).throw(
            AssertionError("identity mismatch must stop before filesystem access")
        ),
    )
    monkeypatch.setattr(
        etk_plugin_update,
        "_download_latest_plugin_dll",
        lambda: (_ for _ in ()).throw(AssertionError("identity mismatch must stop before download")),
    )

    result = etk_plugin_update.ensure_etk_plugin_installed(
        "http://remote-emby.invalid:8096",
        "fixture-token",
        allowed_services={"emby"},
    )

    assert result["ok"] is False
    assert "Server ID" in result["message"]
    assert local_emby.restart_calls == 0
    assert client.closed is True


def test_plugin_container_resolution_requires_server_side_service_or_label_allowlist(monkeypatch) -> None:
    from handler import etk_plugin_update

    not_allowed = FakeEmbyContainer(
        name="unlisted-emby",
        service="media",
        ip_address="172.18.0.21",
    )
    opted_in = FakeEmbyContainer(
        name="opted-in-emby",
        service="media",
        ip_address="172.18.0.22",
        plugin_install_opt_in=True,
    )
    client = SimpleNamespace(containers=SimpleNamespace(list=lambda: [not_allowed, opted_in]))
    monkeypatch.setattr(
        etk_plugin_update,
        "_emby_get_json",
        lambda *_args: {"ServerId": "authorized-server"},
    )
    monkeypatch.setattr(etk_plugin_update, "_public_server_id", lambda _url: "authorized-server")

    resolved = etk_plugin_update._resolve_emby_container(
        client,
        "http://authorized-emby.invalid",
        "fixture-token",
        allowed_services=set(),
    )

    assert resolved is opted_in


def test_plugin_container_resolution_requires_authorized_stable_server_id(monkeypatch) -> None:
    from handler import etk_plugin_update

    local_emby = FakeEmbyContainer(
        name="allowlisted-emby",
        service="emby",
        ip_address="172.18.0.23",
    )
    client = SimpleNamespace(containers=SimpleNamespace(list=lambda: [local_emby]))
    monkeypatch.setattr(etk_plugin_update, "_emby_get_json", lambda *_args: {})
    monkeypatch.setattr(
        etk_plugin_update,
        "_public_server_id",
        lambda _url: (_ for _ in ()).throw(AssertionError("must not probe candidates without authorized ID")),
    )

    with pytest.raises(RuntimeError, match="稳定 Server ID"):
        etk_plugin_update._resolve_emby_container(
            client,
            "http://authorized-emby.invalid",
            "fixture-token",
            allowed_services={"emby"},
        )


def test_identity_plugin_gateway_dispatches_to_control_plane(monkeypatch) -> None:
    captured = {}

    class Client:
        def install_emby_plugin(self, **values):
            captured.update(values)
            return {"ok": True, "plugin_install": {"ok": True, "message": "fixture ready"}}

    monkeypatch.setattr("etk.platform.control_plane.ControlPlaneClient", Client)
    settings = Settings(
        emby_server_url="http://emby.invalid",
        emby_api_key=SecretStr("fixture-token"),
        emby_user_id="fixture-admin",
        emby_auth_mode="user_token",
        etk_server_url_manual="http://192.168.1.10:5257",
    )

    result = LegacyEmbyIdentityGateway().install_plugin(
        {"_normalized_url": "http://emby.invalid", "AccessToken": "fixture-token"},
        settings,
    )

    assert result == {"ok": True, "message": "fixture ready"}
    assert captured == {
        "emby_url": "http://emby.invalid",
        "emby_token": "fixture-token",
        "etk_origin": "http://192.168.1.10:5257",
    }


def test_system_update_worker_fails_closed_without_control_plane_or_docker_access(monkeypatch) -> None:
    progress = []

    class Client:
        def __init__(self):
            raise AssertionError("disabled split-role update must not contact control-plane")

    monkeypatch.setattr("etk.platform.control_plane.ControlPlaneClient", Client)
    monkeypatch.setenv("ETK_PROCESS_ROLE", "worker")
    monkeypatch.setattr(
        system_update.docker,
        "from_env",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("Worker must not access Docker")),
    )
    monkeypatch.setattr(
        task_manager,
        "update_status_from_thread",
        lambda percent, message: progress.append((percent, message)),
    )
    processor = SimpleNamespace(
        config={
            "system_update_strategy": "docker_helper",
            "system_update_helper_image": "fixture/helper:latest",
        }
    )

    result = system_update.task_check_and_update_container(processor)

    assert result["ok"] is False
    assert result["updated"] is False
    assert result["status"] == "disabled"
    assert "release workflow" in result["message"]
    assert "fixture/helper:latest" not in repr(result)
    assert progress[-1] == (-1, system_update.SPLIT_ROLE_UPDATE_DISABLED_MESSAGE)


def test_split_role_legacy_update_generator_refuses_before_docker_access(monkeypatch) -> None:
    monkeypatch.setenv("ETK_PROCESS_ROLE", "api")
    monkeypatch.setattr(
        system_update.docker,
        "from_env",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("split-role API must not access Docker")),
    )

    events = list(
        system_update._update_process_generator(
            "api",
            "etk-refactor:local",
            strategy="docker_helper",
            helper_image="attacker.invalid/helper:latest",
        )
    )

    assert events == [
        {
            "status": system_update.SPLIT_ROLE_UPDATE_DISABLED_MESSAGE,
            "event": "ERROR",
            "current_version": None,
            "target_version": None,
        }
    ]


def test_compose_mounts_the_docker_socket_only_into_the_restricted_control_plane() -> None:
    compose = yaml.safe_load((REPO_ROOT / "docker" / "compose.refactor.yml").read_text(encoding="utf-8"))
    services = compose["services"]
    socket_mount = "/var/run/docker.sock:/var/run/docker.sock:ro"
    token_path = "/run/etk-control-plane/control-plane.token"

    assert socket_mount in services["control-plane"]["volumes"]
    assert socket_mount not in services["worker"]["volumes"]
    assert socket_mount not in services["api"]["volumes"]
    assert services["control-plane"]["read_only"] is True
    assert services["control-plane"]["cap_drop"] == ["ALL"]
    assert services["worker"]["depends_on"]["control-plane"]["condition"] == "service_healthy"
    assert services["control-plane"]["environment"]["ETK_CONTROL_PLANE_EMBY_ALLOWED_SERVICES"] == (
        "${ETK_CONTROL_PLANE_EMBY_ALLOWED_SERVICES:-}"
    )
    assert services["control-plane"]["environment"]["ETK_CONTROL_PLANE_TOKEN_FILE"] == token_path
    assert services["worker"]["environment"]["ETK_CONTROL_PLANE_TOKEN_FILE"] == token_path
    assert "control_plane_runtime:/run/etk-control-plane" in services["control-plane"]["volumes"]
    assert "control_plane_runtime:/run/etk-control-plane:ro" in services["worker"]["volumes"]
    assert "control_plane_runtime" in compose["volumes"]
    for service_name in ("api", "coordinator", "migrate"):
        assert "ETK_CONTROL_PLANE_TOKEN_FILE" not in services[service_name]["environment"]
        assert all(
            not str(volume).startswith("control_plane_runtime:") for volume in services[service_name].get("volumes", [])
        )


def test_ci_explicitly_enables_the_celery_redis_reliability_gate() -> None:
    workflow = yaml.safe_load((REPO_ROOT / ".github" / "workflows" / "refactor-platform.yml").read_text())
    steps = workflow["jobs"]["postgres-platform"]["steps"]
    integration = next(step for step in steps if step.get("name") == "Run platform integration tests")

    assert integration["env"]["ETK_RUN_CELERY_REDIS_INTEGRATION"] == "1"
