from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from etk.platform.control_plane import ControlPlaneClient


class SystemContainerNotFoundError(LookupError):
    def __init__(self, container_name: str) -> None:
        super().__init__(container_name)
        self.container_name = container_name


class SystemRestartGateway(Protocol):
    def restart(self, request_id: str) -> Mapping[str, Any]: ...


class SystemRestartProgress(Protocol):
    job_id: Any

    def checkpoint(self) -> None: ...

    def update(self, progress: int, message: str) -> None: ...

    def record_started_effect(self, label: str) -> None: ...

    def record_committed_effect(self, label: str) -> None: ...


class ControlPlaneSystemRestartGateway:
    """Dispatch restart to the pre-running observer instead of the Worker Docker host."""

    def restart(self, request_id: str) -> Mapping[str, Any]:
        return ControlPlaneClient(track_legacy_effects=False).restart_service(
            "api",
            request_id=request_id,
        )


class SystemRestartService:
    def __init__(self, gateway: SystemRestartGateway) -> None:
        self._gateway = gateway

    def restart(self, request_id: str) -> Mapping[str, Any]:
        return self._gateway.restart(request_id)


class NativeSystemRestartOperation:
    def __init__(self, service: SystemRestartService | None = None) -> None:
        self._service = service or SystemRestartService(ControlPlaneSystemRestartGateway())

    def execute(
        self,
        parameters: Mapping[str, Any],
        progress: SystemRestartProgress,
    ) -> Mapping[str, Any]:
        if parameters:
            raise ValueError("system restart operation does not accept parameters")
        progress.checkpoint()
        request_id = str(progress.job_id)
        if not request_id:
            raise ValueError("system restart operation requires a persistent Job identity")
        label = "system-restart:api"
        progress.record_started_effect(label)
        result = dict(self._service.restart(request_id))
        progress.record_committed_effect(label)
        progress.update(100, "API 服务重启请求已提交")
        return result


__all__ = [
    "ControlPlaneSystemRestartGateway",
    "NativeSystemRestartOperation",
    "SystemContainerNotFoundError",
    "SystemRestartGateway",
    "SystemRestartService",
]
