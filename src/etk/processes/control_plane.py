from __future__ import annotations

import hashlib
import json
import os
import secrets
import threading
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, Request
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import JSONResponse

from etk.platform.observability.redaction import redact_sensitive


class ControlPlaneConflictError(RuntimeError):
    pass


class ControlPlaneUpdateDisabledError(RuntimeError):
    pass


class ControlPlaneIndeterminateError(RuntimeError):
    pass


ControlPlaneCertainty = Literal["confirmed-no-effect", "unknown", "observed-complete"]


class ControlPlaneCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str = Field(min_length=1, max_length=255)
    action: Literal["restart", "update", "plugin-install"]
    payload: dict[str, Any] = Field(default_factory=dict)


class CommandLedger:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()

    def reserve(self, command: ControlPlaneCommand) -> dict[str, Any]:
        request_hash = _command_hash(command)
        with self._lock:
            data = self._load_locked()
            commands = data.setdefault("commands", {})
            current = commands.get(command.request_id)
            if current is not None:
                if current.get("action") != command.action or current.get("request_hash") != request_hash:
                    raise ControlPlaneConflictError("control-plane request_id was reused with different input")
                return dict(current)
            now = _utc_timestamp()
            current = {
                "request_id": command.request_id,
                "action": command.action,
                "request_hash": request_hash,
                "stage": "requested",
                "transitions": [{"stage": "requested", "at": now}],
                "details": {},
                "result": None,
                "error": None,
            }
            commands[command.request_id] = current
            self._save_locked(data)
            return dict(current)

    def transition(
        self,
        request_id: str,
        stage: str,
        *,
        details: Mapping[str, Any] | None = None,
        result: Mapping[str, Any] | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            data = self._load_locked()
            current = data.setdefault("commands", {}).get(request_id)
            if current is None:
                raise LookupError(request_id)
            current["stage"] = stage
            current.setdefault("transitions", []).append({"stage": stage, "at": _utc_timestamp()})
            if details is not None:
                current["details"] = dict(details)
            if result is not None:
                current["result"] = dict(result)
            current["error"] = str(error) if error else None
            self._save_locked(data)
            return dict(current)

    def get(self, request_id: str) -> dict[str, Any] | None:
        with self._lock:
            current = self._load_locked().get("commands", {}).get(request_id)
            return dict(current) if isinstance(current, dict) else None

    def _load_locked(self) -> dict[str, Any]:
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {"schema_version": 1, "commands": {}}
        except (OSError, ValueError) as exc:
            raise RuntimeError("control-plane command ledger is unreadable") from exc
        if not isinstance(value, dict) or not isinstance(value.get("commands", {}), dict):
            raise RuntimeError("control-plane command ledger has an invalid shape")
        value.setdefault("schema_version", 1)
        value.setdefault("commands", {})
        return value

    def _save_locked(self, data: Mapping[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.tmp")
        temporary.write_text(
            json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
            encoding="utf-8",
        )
        os.chmod(temporary, 0o600)
        os.replace(temporary, self.path)


class ControlPlaneController:
    def __init__(
        self,
        ledger: CommandLedger,
        *,
        docker_client_factory: Callable[[], Any] | None = None,
        allowed_services: set[str] | None = None,
        allowed_emby_services: set[str] | None = None,
        own_service: str | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        plugin_preflight: Callable[[ControlPlaneCommand], Mapping[str, Any]] | None = None,
        plugin_installer: (Callable[[ControlPlaneCommand, Callable[[], None]], Mapping[str, Any]] | None) = None,
    ) -> None:
        self.ledger = ledger
        self._docker_client_factory = docker_client_factory or _docker_client
        configured_allowed = {
            value.strip()
            for value in os.environ.get("ETK_CONTROL_PLANE_ALLOWED_SERVICES", "api").split(",")
            if value.strip()
        }
        self._allowed_services = allowed_services if allowed_services is not None else configured_allowed
        configured_emby_services = {
            value.strip()
            for value in os.environ.get("ETK_CONTROL_PLANE_EMBY_ALLOWED_SERVICES", "").split(",")
            if value.strip()
        }
        self._allowed_emby_services = (
            allowed_emby_services if allowed_emby_services is not None else configured_emby_services
        )
        self._own_service = own_service or os.environ.get("ETK_CONTROL_PLANE_SERVICE", "control-plane")
        self._sleep = sleep
        self._monotonic = monotonic
        self._plugin_preflight = plugin_preflight or self._preflight_plugin
        self._plugin_installer = plugin_installer or self._run_plugin_installer
        self._execution_lock = threading.RLock()

    def execute(self, command: ControlPlaneCommand) -> dict[str, Any]:
        self.prepare(command)
        return self.execute_prepared(command)

    def prepare(self, command: ControlPlaneCommand) -> dict[str, Any]:
        """Reserve and preflight a command without entering an external write."""
        with self._execution_lock:
            record = self.ledger.reserve(command)
            stage = str(record.get("stage") or "requested")
            if stage in {"observed-complete", "dispatched", "failed-unknown"}:
                return self._record_response(record)
            try:
                details = self._prepare_action(command)
            except Exception as exc:
                self.ledger.transition(
                    command.request_id,
                    "failed-no-effect",
                    error=_safe_error(exc),
                )
                raise
            prepared = self.ledger.transition(
                command.request_id,
                "prepared",
                details=details,
            )
            return self._record_response(prepared)

    def execute_prepared(self, command: ControlPlaneCommand) -> dict[str, Any]:
        """Dispatch only a prepared command and preserve outcome certainty in the ledger."""
        with self._execution_lock:
            record = self.ledger.reserve(command)
            stage = str(record.get("stage") or "requested")
            if stage == "observed-complete" and isinstance(record.get("result"), dict):
                return self._result_with_transitions(command.request_id, record["result"])
            if stage in {"requested", "failed-no-effect"}:
                self.prepare(command)
                record = self.ledger.get(command.request_id) or record
                stage = str(record.get("stage") or "requested")
            try:
                if command.action == "restart":
                    result = self._restart(command, record)
                elif command.action == "update":
                    result = self._update(command)
                else:
                    result = self._install_plugin(command, record)
            except Exception as exc:
                latest = self.ledger.get(command.request_id) or record
                failure_stage = (
                    "failed-unknown"
                    if str(latest.get("stage") or "") in {"dispatched", "failed-unknown"}
                    else "failed-no-effect"
                )
                self.ledger.transition(
                    command.request_id,
                    failure_stage,
                    error=_safe_error(exc),
                )
                raise
            completed = self.ledger.transition(
                command.request_id,
                "observed-complete",
                result=result,
            )
            return self._result_with_transitions(command.request_id, completed.get("result") or result)

    def status(self, request_id: str) -> dict[str, Any] | None:
        record = self.ledger.get(str(request_id or "").strip())
        return self._record_response(record) if record is not None else None

    def _prepare_action(self, command: ControlPlaneCommand) -> dict[str, Any]:
        if command.action == "restart":
            target_service = self._target_service(command.payload)
            client, container = self._resolve_service_container(target_service)
            try:
                return {
                    "target_service": target_service,
                    "container_id": str(getattr(container, "id", "") or ""),
                    "container_name": str(getattr(container, "name", "") or ""),
                    "previous_started_at": _container_started_at(container),
                }
            finally:
                _close_docker_client(client)
        if command.action == "update":
            self._update(command)
        details = self._plugin_preflight(command)
        return _plugin_preflight_details(details)

    def _restart(self, command: ControlPlaneCommand, record: Mapping[str, Any]) -> dict[str, Any]:
        target_service = self._target_service(command.payload)
        client, container = self._resolve_service_container(target_service)
        try:
            details = dict(record.get("details") or {})
            previous_started_at = str(details.get("previous_started_at") or _container_started_at(container) or "")
            stage = str(record.get("stage") or "")
            if stage not in {"dispatched", "failed-unknown"}:
                details = {
                    "target_service": target_service,
                    "container_id": str(getattr(container, "id", "") or ""),
                    "container_name": str(getattr(container, "name", "") or ""),
                    "previous_started_at": _container_started_at(container) or previous_started_at,
                }
                self.ledger.transition(command.request_id, "dispatched", details=details)
                container.restart(timeout=10)
                previous_started_at = str(details["previous_started_at"])
            observed = self._observe_restarted(container, previous_started_at, timeout_seconds=120)
            return {
                "ok": True,
                "action": "restart",
                "target_service": target_service,
                "container_name": str(getattr(container, "name", "") or ""),
                "observed": observed,
                "message": f"服务 '{target_service}' 已重启并恢复运行。",
            }
        finally:
            _close_docker_client(client)

    def _update(self, command: ControlPlaneCommand) -> dict[str, Any]:
        target_service = self._target_service(command.payload)
        caller_controlled = {"strategy", "helper_image"}.intersection(command.payload)
        if caller_controlled:
            raise PermissionError("control-plane rejects caller-provided update strategy or helper image")
        raise ControlPlaneUpdateDisabledError(
            "Greenfield split-role 自动更新已禁用：尚未实现覆盖迁移、Worker drain 和全部进程角色的 release workflow；"
            f"未更新服务 '{target_service}'。"
        )

    def _install_plugin(
        self,
        command: ControlPlaneCommand,
        record: Mapping[str, Any],
    ) -> dict[str, Any]:
        if str(record.get("stage") or "") in {"dispatched", "failed-unknown"}:
            raise ControlPlaneIndeterminateError(
                "plugin installation was dispatched but cannot be proven complete without replay"
            )

        effect_started = False

        def mark_dispatched() -> None:
            nonlocal effect_started
            if effect_started:
                return
            self.ledger.transition(
                command.request_id,
                "dispatched",
                details={"operation": "plugin-install"},
            )
            effect_started = True

        result = dict(self._plugin_installer(command, mark_dispatched))
        if not result.get("ok"):
            raise RuntimeError(str(result.get("message") or "embytool Bridge 插件安装失败"))
        return {
            "ok": True,
            "action": "plugin-install",
            "plugin_install": result,
            "message": str(result.get("message") or "embytool Bridge 插件已就绪"),
        }

    def _preflight_plugin(self, command: ControlPlaneCommand) -> Mapping[str, Any]:
        emby_url = str(command.payload.get("emby_url") or "").strip().rstrip("/")
        emby_token = str(command.payload.get("emby_token") or "").strip()
        if not emby_url or not emby_token:
            raise ValueError("插件安装命令缺少 Emby 授权")
        from handler.etk_plugin_update import preflight_etk_plugin_install

        return dict(
            preflight_etk_plugin_install(
                emby_url,
                emby_token,
                allowed_services=self._allowed_emby_services,
            )
        )

    def _run_plugin_installer(
        self,
        command: ControlPlaneCommand,
        effect_started: Callable[[], None],
    ) -> Mapping[str, Any]:
        emby_url = str(command.payload.get("emby_url") or "").strip().rstrip("/")
        emby_token = str(command.payload.get("emby_token") or "").strip()
        etk_origin = str(command.payload.get("etk_origin") or "").strip().rstrip("/")
        from handler.etk_plugin_update import ensure_etk_plugin_installed

        return dict(
            ensure_etk_plugin_installed(
                emby_url,
                emby_token,
                etk_origin,
                allowed_services=self._allowed_emby_services,
                effect_started=effect_started,
            )
        )

    def _target_service(self, payload: Mapping[str, Any]) -> str:
        target_service = str(payload.get("target_service") or "").strip()
        if not target_service:
            raise ValueError("control-plane command requires an explicit target_service")
        if target_service not in self._allowed_services:
            raise PermissionError(f"control-plane target service is not allowed: {target_service}")
        if target_service == self._own_service:
            raise PermissionError("control-plane refuses to restart or update itself")
        return target_service

    def _resolve_service_container(self, target_service: str) -> tuple[Any, Any]:
        client = self._docker_client_factory()
        try:
            hostname = str(os.environ.get("HOSTNAME") or "").strip()
            if not hostname:
                raise RuntimeError("control-plane HOSTNAME is unavailable")
            own_container = client.containers.get(hostname)
            own_labels = _container_labels(own_container)
            project = str(own_labels.get("com.docker.compose.project") or "").strip()
            if not project:
                raise RuntimeError("control-plane is not running in a Compose project")
            candidates = []
            for container in client.containers.list(all=True):
                labels = _container_labels(container)
                if (
                    labels.get("com.docker.compose.project") == project
                    and labels.get("com.docker.compose.service") == target_service
                ):
                    candidates.append(container)
            if len(candidates) != 1:
                raise RuntimeError(f"无法唯一定位 Compose 服务 '{target_service}'，候选数量: {len(candidates)}")
            target = candidates[0]
            if str(getattr(target, "id", "") or "") == str(getattr(own_container, "id", "") or ""):
                raise PermissionError("control-plane refuses to target its own container")
            return client, target
        except Exception:
            _close_docker_client(client)
            raise

    def _observe_restarted(self, container: Any, previous_started_at: str, *, timeout_seconds: float) -> dict[str, Any]:
        deadline = self._monotonic() + timeout_seconds
        while self._monotonic() < deadline:
            container.reload()
            attrs = getattr(container, "attrs", {}) or {}
            state = attrs.get("State") or {}
            started_at = str(state.get("StartedAt") or "")
            status = str(state.get("Status") or "").lower()
            health = str((state.get("Health") or {}).get("Status") or "").lower()
            restarted = bool(started_at and started_at != previous_started_at)
            healthy = status == "running" and health in {"", "healthy"}
            if restarted and healthy:
                return {"status": status, "health": health or "not-configured", "started_at": started_at}
            self._sleep(0.5)
        raise TimeoutError("target service restart was dispatched but not observed complete")

    def _record_response(self, record: Mapping[str, Any]) -> dict[str, Any]:
        request_id = str(record.get("request_id") or "")
        stage = str(record.get("stage") or "requested")
        result = record.get("result")
        if stage == "observed-complete" and isinstance(result, Mapping):
            return self._result_with_transitions(request_id, result)
        response: dict[str, Any] = {
            "ok": stage == "prepared",
            "request_id": request_id,
            "action": str(record.get("action") or ""),
            "stage": stage,
            "certainty": _certainty_for_stage(stage),
            "stages": list(record.get("transitions") or []),
        }
        error = str(record.get("error") or "").strip()
        if error:
            response["message"] = error
        return response

    def _result_with_transitions(self, request_id: str, result: Mapping[str, Any]) -> dict[str, Any]:
        record = self.ledger.get(request_id) or {}
        output = dict(result)
        output["ok"] = bool(output.get("ok", True))
        output["request_id"] = request_id
        output["stage"] = "observed-complete"
        output["certainty"] = "observed-complete"
        output["stages"] = list(record.get("transitions") or [])
        return output


def create_control_plane_app(
    *,
    controller: ControlPlaneController | None = None,
    token_file: Path | None = None,
    ledger_file: Path | None = None,
) -> FastAPI:
    token_path = token_file or Path(
        os.environ.get(
            "ETK_CONTROL_PLANE_TOKEN_FILE",
            "/run/etk-control-plane/control-plane.token",
        )
    )
    token = ensure_control_plane_token(token_path)
    command_ledger = ledger_file or Path(
        os.environ.get("ETK_CONTROL_PLANE_LEDGER_FILE", "/config/control-plane-commands.json")
    )
    command_controller = controller or ControlPlaneController(CommandLedger(command_ledger))
    app = FastAPI(title="embytool Docker Control Plane")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "role": "control-plane"}

    def authorized(request: Request) -> bool:
        supplied = str(request.headers.get("authorization") or "")
        return secrets.compare_digest(supplied, f"Bearer {token}")

    def invoke(
        command: ControlPlaneCommand,
        request: Request,
        operation: Callable[[ControlPlaneCommand], dict[str, Any]],
    ) -> JSONResponse:
        if not authorized(request):
            return JSONResponse({"ok": False, "message": "unauthorized"}, status_code=401)
        try:
            return JSONResponse(operation(command))
        except ControlPlaneConflictError as exc:
            return JSONResponse({"ok": False, "message": str(exc)}, status_code=409)
        except Exception as exc:
            status = command_controller.status(command.request_id)
            payload = dict(status or {})
            payload.update({"ok": False, "message": _safe_error(exc), "request_id": command.request_id})
            return JSONResponse(payload, status_code=500)

    @app.post("/v1/commands")
    def command_endpoint(command: ControlPlaneCommand, request: Request) -> JSONResponse:
        return invoke(command, request, command_controller.execute)

    @app.post("/v1/commands/prepare")
    def prepare_endpoint(command: ControlPlaneCommand, request: Request) -> JSONResponse:
        return invoke(command, request, command_controller.prepare)

    @app.post("/v1/commands/execute")
    def execute_endpoint(command: ControlPlaneCommand, request: Request) -> JSONResponse:
        return invoke(command, request, command_controller.execute_prepared)

    @app.get("/v1/commands/{request_id}")
    def status_endpoint(request_id: str, request: Request) -> JSONResponse:
        if not authorized(request):
            return JSONResponse({"ok": False, "message": "unauthorized"}, status_code=401)
        status = command_controller.status(request_id)
        if status is None:
            return JSONResponse(
                {
                    "ok": False,
                    "message": "control-plane command was not found",
                    "request_id": request_id,
                    "certainty": "confirmed-no-effect",
                },
                status_code=404,
            )
        return JSONResponse(status)

    return app


def ensure_control_plane_token(path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        existing = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        existing = ""
    if existing:
        if len(existing) < 32:
            raise RuntimeError("control-plane token file contains an invalid token")
        os.chmod(path, 0o600)
        return existing
    token = secrets.token_urlsafe(48)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return ensure_control_plane_token(path)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(token)
    return token


def _command_hash(command: ControlPlaneCommand) -> str:
    encoded = json.dumps(
        {"action": command.action, "payload": command.payload},
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _docker_client() -> Any:
    import docker

    return docker.from_env()


def _container_labels(container: Any) -> dict[str, str]:
    attrs = getattr(container, "attrs", {}) or {}
    labels = (attrs.get("Config") or {}).get("Labels") or {}
    return {str(key): str(value) for key, value in labels.items()}


def _container_started_at(container: Any) -> str:
    attrs = getattr(container, "attrs", {}) or {}
    return str((attrs.get("State") or {}).get("StartedAt") or "")


def _close_docker_client(client: Any) -> None:
    close = getattr(client, "close", None)
    if callable(close):
        close()


def _certainty_for_stage(stage: str) -> ControlPlaneCertainty:
    if stage == "observed-complete":
        return "observed-complete"
    if stage in {"requested", "prepared", "failed-no-effect"}:
        return "confirmed-no-effect"
    return "unknown"


def _plugin_preflight_details(details: Mapping[str, Any]) -> dict[str, Any]:
    allowed = {"already_installed", "container_name", "plugins_path"}
    return {str(key): value for key, value in details.items() if str(key) in allowed}


def _safe_error(error: BaseException) -> str:
    return str(redact_sensitive(str(error)))[:2000]


def _utc_timestamp() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat()


__all__ = [
    "CommandLedger",
    "ControlPlaneCertainty",
    "ControlPlaneCommand",
    "ControlPlaneConflictError",
    "ControlPlaneController",
    "ControlPlaneIndeterminateError",
    "ControlPlaneUpdateDisabledError",
    "create_control_plane_app",
    "ensure_control_plane_token",
]
