from __future__ import annotations

import sys
import types
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from copy import deepcopy
from typing import Any

import pytest

from etk.bootstrap.legacy_runtime import LegacyRuntimeAdapter
from etk.bootstrap.runtime_supervisor import RuntimeReloadError, RuntimeRole, RuntimeSupervisor
from etk.domains.identity.compatibility import RuntimeConfigurationWriter
from etk.platform import AppContainer, Settings


class FakeConfigManager(types.ModuleType):
    def __init__(self) -> None:
        super().__init__("config_manager")
        self.persisted: dict[str, Any] = {"revision": 1, "removed_on_failure": "preserved"}
        self.APP_CONFIG: dict[str, Any] = deepcopy(self.persisted)
        self.fail_save_after_write = False
        self.restore_calls = 0
        self.concurrent_updates_on_restore: dict[str, Any] = {}
        self.restore_expected_updates: dict[str, Any] | None = None

    def capture_dynamic_config_state(self) -> tuple[dict[str, Any], dict[str, Any]]:
        return deepcopy(self.persisted), deepcopy(self.APP_CONFIG)

    @contextmanager
    def dynamic_config_write_lock(self):
        yield

    def save_config(self, updates: dict[str, Any]) -> None:
        self.persisted.update(deepcopy(updates))
        self.APP_CONFIG.update(deepcopy(updates))
        if self.fail_save_after_write:
            raise RuntimeError("fixture persistence failure after write")

    def restore_dynamic_config_state(
        self,
        persisted: dict[str, Any],
        in_memory: dict[str, Any],
        *,
        expected_updates: dict[str, Any] | None = None,
    ) -> None:
        self.restore_calls += 1
        self.restore_expected_updates = deepcopy(expected_updates)
        self.persisted.update(deepcopy(self.concurrent_updates_on_restore))
        if expected_updates is None:
            self.persisted = deepcopy(persisted)
        else:
            for key, expected in expected_updates.items():
                if self.persisted.get(key) != expected:
                    continue
                if key in persisted:
                    self.persisted[key] = deepcopy(persisted[key])
                else:
                    self.persisted.pop(key, None)
        self.APP_CONFIG.clear()
        self.APP_CONFIG.update(deepcopy(in_memory))
        self.APP_CONFIG.update(deepcopy(self.persisted))


def _install_runtime_modules(monkeypatch, events: list[str]) -> FakeConfigManager:
    config_manager = FakeConfigManager()
    monkeypatch.setitem(sys.modules, "config_manager", config_manager)

    class TaskManagerModule(types.ModuleType):
        @staticmethod
        def is_task_running() -> bool:
            return False

    task_manager = TaskManagerModule("task_manager")
    monkeypatch.setitem(sys.modules, "task_manager", task_manager)

    class Scheduler:
        def update_all_scheduled_jobs(self) -> None:
            events.append("scheduler")

    class SchedulerModule(types.ModuleType):
        scheduler_manager: Scheduler

    scheduler_module = SchedulerModule("scheduler_manager")
    scheduler_module.scheduler_manager = Scheduler()
    monkeypatch.setitem(sys.modules, "scheduler_manager", scheduler_module)

    return config_manager


def _configured_adapter(
    monkeypatch,
    config_manager: FakeConfigManager,
    events: list[str],
    *,
    fail_once: str | None,
) -> LegacyRuntimeAdapter:
    adapter = LegacyRuntimeAdapter()
    adapter._started = True
    adapter._background = True
    adapter._config = config_manager.APP_CONFIG
    remaining_failure = [fail_once]

    def action(name: str) -> Callable[..., None]:
        def run(*args: Any, **kwargs: Any) -> None:
            del args, kwargs
            events.append(name)
            if remaining_failure[0] == name:
                remaining_failure[0] = None
                raise RuntimeError(f"fixture {name} failure")

        return run

    monkeypatch.setattr(adapter, "_replace_processors", action("processors"))
    monkeypatch.setattr(adapter, "_restart_monitor", action("monitor"))
    monkeypatch.setattr(adapter, "_refresh_shared_device_event_listener", action("shared-listener"))
    monkeypatch.setattr(adapter, "_refresh_telegram_services", action("telegram"))
    monkeypatch.setattr(adapter, "_refresh_vector_cache_service", action("vector-cache"))
    return adapter


def test_runtime_failure_restores_exact_config_and_rebuilds_previous_services(monkeypatch) -> None:
    events: list[str] = []
    config_manager = _install_runtime_modules(monkeypatch, events)
    adapter = _configured_adapter(
        monkeypatch,
        config_manager,
        events,
        fail_once="monitor",
    )

    with pytest.raises(RuntimeReloadError, match="previous state was restored") as captured:
        adapter.save_config_and_reload({"revision": 2, "new_key": "temporary"})

    assert captured.value.compensated is True
    assert config_manager.persisted == {"revision": 1, "removed_on_failure": "preserved"}
    assert config_manager.APP_CONFIG == {"revision": 1, "removed_on_failure": "preserved"}
    assert adapter.config == config_manager.APP_CONFIG
    assert config_manager.restore_calls == 1
    assert events == [
        "processors",
        "monitor",
        "processors",
        "monitor",
        "shared-listener",
        "telegram",
        "vector-cache",
    ]

    events.clear()
    adapter.save_config_and_reload({"revision": 3})
    assert config_manager.persisted["revision"] == 3
    assert events == [
        "processors",
        "monitor",
        "shared-listener",
        "telegram",
        "vector-cache",
    ]


def test_partial_persistence_failure_rolls_back_without_restarting_services(monkeypatch) -> None:
    events: list[str] = []
    config_manager = _install_runtime_modules(monkeypatch, events)
    config_manager.fail_save_after_write = True
    adapter = _configured_adapter(
        monkeypatch,
        config_manager,
        events,
        fail_once=None,
    )

    with pytest.raises(RuntimeReloadError, match="previous state was restored") as captured:
        adapter.save_config_and_reload({"revision": 2, "new_key": "temporary"})

    assert captured.value.compensated is True
    assert config_manager.persisted == {"revision": 1, "removed_on_failure": "preserved"}
    assert config_manager.APP_CONFIG == {"revision": 1, "removed_on_failure": "preserved"}
    assert config_manager.restore_calls == 1
    assert events == []


def test_reload_rollback_preserves_a_concurrent_worker_commit(monkeypatch) -> None:
    events: list[str] = []
    config_manager = _install_runtime_modules(monkeypatch, events)
    config_manager.concurrent_updates_on_restore = {"worker_key": "committed"}
    adapter = _configured_adapter(
        monkeypatch,
        config_manager,
        events,
        fail_once="monitor",
    )

    with pytest.raises(RuntimeReloadError, match="previous state was restored"):
        adapter.save_config_and_reload({"revision": 2, "new_key": "temporary"})

    assert config_manager.persisted == {
        "revision": 1,
        "removed_on_failure": "preserved",
        "worker_key": "committed",
    }
    assert config_manager.APP_CONFIG == config_manager.persisted
    assert config_manager.restore_expected_updates == {"revision": 2, "new_key": "temporary"}


def test_persisted_snapshot_reload_does_not_write_the_database_again(monkeypatch) -> None:
    events: list[str] = []
    config_manager = _install_runtime_modules(monkeypatch, events)
    adapter = _configured_adapter(
        monkeypatch,
        config_manager,
        events,
        fail_once=None,
    )

    adapter.apply_persisted_config_snapshot({"revision": 2, "monitor_enabled": True})

    assert config_manager.persisted == {"revision": 1, "removed_on_failure": "preserved"}
    assert config_manager.APP_CONFIG["revision"] == 2
    assert config_manager.APP_CONFIG["monitor_enabled"] is True
    assert events == [
        "processors",
        "monitor",
        "shared-listener",
        "telegram",
        "vector-cache",
    ]


class SnapshotRuntimeAdapter:
    def __init__(self) -> None:
        self._config: dict[str, Any] = {"monitor_enabled": False}

    @property
    def config(self) -> Mapping[str, Any]:
        return self._config

    def start(self, *, role: RuntimeRole, background: bool) -> None:
        del role, background

    def save_config_and_reload(self, new_config: Mapping[str, Any]) -> None:
        self._config.update(dict(new_config))
        self._config["runtime_concurrent_key"] = "preserved"

    def stop(self) -> None:
        pass


def test_configuration_writer_uses_post_reload_runtime_snapshot_without_second_database_read() -> None:
    adapter = SnapshotRuntimeAdapter()
    supervisor = RuntimeSupervisor(lambda: adapter)
    supervisor.start(role=RuntimeRole.API, background=False)
    container = AppContainer(Settings.model_validate({"monitor_enabled": False, "native_extra": "preserved"}))
    writer = RuntimeConfigurationWriter(container, supervisor)

    refreshed = writer.save({"monitor_enabled": True})

    assert refreshed.monitor_enabled is True
    assert refreshed.get("native_extra") == "preserved"
    assert refreshed.get("runtime_concurrent_key") == "preserved"
    assert container.settings is refreshed
    assert supervisor.state.value == "running"
    supervisor.stop()
