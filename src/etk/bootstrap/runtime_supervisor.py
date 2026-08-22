from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from enum import StrEnum
from typing import Any, Protocol

logger = logging.getLogger(__name__)


class RuntimeRole(StrEnum):
    API = "api"
    PROXY = "proxy"


class RuntimeState(StrEnum):
    NEW = "new"
    STARTING = "starting"
    RUNNING = "running"
    RELOADING = "reloading"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


class RuntimeAdapter(Protocol):
    @property
    def config(self) -> Mapping[str, Any]: ...

    def start(self, *, role: RuntimeRole, background: bool) -> None: ...

    def save_config_and_reload(self, new_config: Mapping[str, Any]) -> None: ...

    def stop(self) -> None: ...


def _default_adapter_factory() -> RuntimeAdapter:
    from .legacy_runtime import LegacyRuntimeAdapter

    return LegacyRuntimeAdapter()


class RuntimeSupervisor:
    """Own the compatibility runtime lifecycle without importing it at module load."""

    def __init__(self, adapter_factory: Callable[[], RuntimeAdapter] | None = None):
        self._adapter_factory = adapter_factory or _default_adapter_factory
        self._lock = threading.RLock()
        self._operation_lock = threading.Lock()
        self._adapter: RuntimeAdapter | None = None
        self._role: RuntimeRole | None = None
        self._background = False
        self._state = RuntimeState.NEW

    @property
    def state(self) -> RuntimeState:
        with self._lock:
            return self._state

    @property
    def role(self) -> RuntimeRole | None:
        with self._lock:
            return self._role

    @property
    def config(self) -> dict[str, Any]:
        with self._lock:
            adapter = self._adapter
            if adapter is None:
                return {}
            return dict(adapter.config)

    def start(
        self,
        *,
        role: RuntimeRole | str = RuntimeRole.API,
        background: bool | None = None,
    ) -> None:
        with self._operation_lock:
            self._start(role=role, background=background)

    def _start(
        self,
        *,
        role: RuntimeRole | str = RuntimeRole.API,
        background: bool | None = None,
    ) -> None:
        resolved_role = RuntimeRole(role)
        resolved_background = resolved_role is RuntimeRole.API if background is None else bool(background)

        with self._lock:
            if self._state is RuntimeState.RUNNING:
                if self._role is resolved_role and self._background == resolved_background:
                    return
                raise RuntimeError(f"runtime already started as {self._role}; cannot switch to {resolved_role}")
            if self._state is RuntimeState.FAILED and self._adapter is not None:
                raise RuntimeError("failed runtime must be stopped before it can be started again")
            if self._state in {
                RuntimeState.STARTING,
                RuntimeState.RELOADING,
                RuntimeState.STOPPING,
            }:
                raise RuntimeError(f"runtime lifecycle is busy: {self._state}")
            adapter = self._adapter_factory()
            self._adapter = adapter
            self._role = resolved_role
            self._background = resolved_background
            self._state = RuntimeState.STARTING

        try:
            adapter.start(role=resolved_role, background=resolved_background)
        except BaseException:
            try:
                adapter.stop()
            except BaseException:
                logger.exception("Runtime cleanup after failed start also failed")
            finally:
                with self._lock:
                    self._adapter = None
                    self._role = None
                    self._background = False
                    self._state = RuntimeState.FAILED
            raise

        with self._lock:
            self._state = RuntimeState.RUNNING

    def save_config_and_reload(self, new_config: Mapping[str, Any]) -> None:
        with self._operation_lock:
            self._save_config_and_reload(new_config)

    def _save_config_and_reload(self, new_config: Mapping[str, Any]) -> None:
        if not isinstance(new_config, Mapping):
            raise TypeError("new_config must be a mapping")

        with self._lock:
            if self._state is not RuntimeState.RUNNING or self._adapter is None:
                raise RuntimeError("runtime is not running")
            adapter = self._adapter
            self._state = RuntimeState.RELOADING
            try:
                adapter.save_config_and_reload(dict(new_config))
            except BaseException:
                self._state = RuntimeState.FAILED
                raise
            else:
                self._state = RuntimeState.RUNNING

    def stop(self) -> None:
        with self._operation_lock:
            self._stop()

    def _stop(self) -> None:
        with self._lock:
            if self._state in {RuntimeState.NEW, RuntimeState.STOPPED}:
                self._state = RuntimeState.STOPPED
                return
            if self._state is RuntimeState.STOPPING:
                return
            adapter = self._adapter
            self._state = RuntimeState.STOPPING

        try:
            if adapter is not None:
                adapter.stop()
        except BaseException:
            with self._lock:
                self._state = RuntimeState.FAILED
            raise
        else:
            with self._lock:
                self._adapter = None
                self._role = None
                self._background = False
                self._state = RuntimeState.STOPPED


_SUPERVISOR_LOCK = threading.Lock()
_SUPERVISOR: RuntimeSupervisor | None = None


def get_runtime_supervisor() -> RuntimeSupervisor:
    """Return the Flask-injected supervisor when available, otherwise the process singleton."""
    try:
        from flask import current_app

        injected = current_app.extensions.get("etk_runtime_supervisor")
        if isinstance(injected, RuntimeSupervisor):
            return injected
        if injected is not None:
            return injected
    except RuntimeError:
        pass

    global _SUPERVISOR
    with _SUPERVISOR_LOCK:
        if _SUPERVISOR is None:
            _SUPERVISOR = RuntimeSupervisor()
        return _SUPERVISOR
