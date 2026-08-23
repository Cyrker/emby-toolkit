from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from sqlalchemy import text

from etk.platform import Settings
from etk.platform.jobs import UnitOfWorkFactory


class RuntimeSnapshotApplier(Protocol):
    @property
    def config(self) -> Mapping[str, Any]: ...

    def apply_persisted_config_snapshot(self, new_config: Mapping[str, Any]) -> None: ...


class SqlAlchemyDynamicConfigReader:
    def __init__(self, uow_factory: UnitOfWorkFactory) -> None:
        self._uow_factory = uow_factory

    def read(self) -> dict[str, Any]:
        with self._uow_factory() as uow:
            value = uow.session.scalar(
                text("SELECT value_json FROM app_settings WHERE setting_key = 'dynamic_app_config'")
            )
        if value is None:
            return {}
        if not isinstance(value, Mapping):
            raise TypeError("dynamic_app_config must contain a JSON object")
        return {str(key): item for key, item in value.items()}


class ApiRuntimeConfigSynchronizer:
    """Converge one API process to the persisted config at request boundaries."""

    def __init__(
        self,
        *,
        snapshot_reader: Callable[[], Mapping[str, Any]],
        settings_provider: Callable[[], Settings],
        settings_consumer: Callable[[Settings], None],
        runtime: RuntimeSnapshotApplier,
        max_snapshot_retries: int = 3,
    ) -> None:
        if max_snapshot_retries < 1:
            raise ValueError("max_snapshot_retries must be positive")
        self._snapshot_reader = snapshot_reader
        self._settings_provider = settings_provider
        self._settings_consumer = settings_consumer
        self._runtime = runtime
        self._max_snapshot_retries = max_snapshot_retries
        self._lock = threading.Lock()

    def synchronize(self) -> bool:
        with self._lock:
            changed = False
            for _ in range(self._max_snapshot_retries):
                snapshot = self._read_snapshot()
                changed = self._apply(snapshot) or changed
                if self._read_snapshot() == snapshot:
                    return changed
            raise RuntimeError("persisted configuration changed continuously during API synchronization")

    def _read_snapshot(self) -> dict[str, Any]:
        value = self._snapshot_reader()
        if not isinstance(value, Mapping):
            raise TypeError("persisted configuration snapshot must be a mapping")
        return {str(key): item for key, item in value.items()}

    def _apply(self, snapshot: Mapping[str, Any]) -> bool:
        if not snapshot:
            return False
        current = self._settings_provider()
        current_values = current.to_runtime_dict()
        runtime_values = dict(self._runtime.config)
        settings_current = _contains_snapshot(current_values, snapshot)
        runtime_current = _contains_snapshot(runtime_values, snapshot)
        if settings_current and runtime_current:
            return False

        desired = current.with_updates(snapshot)
        if not runtime_current:
            self._runtime.apply_persisted_config_snapshot(desired.to_runtime_dict())
        runtime_values = dict(self._runtime.config)
        refreshed = desired.with_updates(runtime_values) if runtime_values else desired
        self._settings_consumer(refreshed)
        return True


def _contains_snapshot(current: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    return all(current.get(key) == value for key, value in expected.items())


__all__ = [
    "ApiRuntimeConfigSynchronizer",
    "SqlAlchemyDynamicConfigReader",
]
