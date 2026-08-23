from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from etk.platform.control_plane import ControlPlaneClient


class SystemContainerNotFoundError(LookupError):
    def __init__(self, container_name: str) -> None:
        super().__init__(container_name)
        self.container_name = container_name


class SystemRestartGateway(Protocol):
    def restart(self) -> Mapping[str, Any]: ...


class ControlPlaneSystemRestartGateway:
    """Dispatch restart to the pre-running observer instead of the Worker Docker host."""

    def restart(self) -> Mapping[str, Any]:
        return ControlPlaneClient().restart_service("api")


class SystemRestartService:
    def __init__(self, gateway: SystemRestartGateway) -> None:
        self._gateway = gateway

    def restart(self) -> Mapping[str, Any]:
        return self._gateway.restart()


def run_system_restart_persistent() -> dict[str, Any]:
    """Execute Docker control only after a durable Worker has claimed the Job."""
    import task_manager

    task_manager.persistent_execution_checkpoint()
    service = SystemRestartService(ControlPlaneSystemRestartGateway())
    result = dict(service.restart())
    return result


__all__ = [
    "ControlPlaneSystemRestartGateway",
    "SystemContainerNotFoundError",
    "SystemRestartGateway",
    "SystemRestartService",
    "run_system_restart_persistent",
]
