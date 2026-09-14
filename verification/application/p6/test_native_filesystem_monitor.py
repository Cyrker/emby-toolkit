from __future__ import annotations

import time
from types import SimpleNamespace

from watchdog.events import FileCreatedEvent, FileModifiedEvent
from watchdog.observers.polling import PollingObserver

from etk.platform import Settings
from etk.processes.coordinator import CoordinatorRuntime
from etk.processes.filesystem_monitor import NativeFilesystemMonitor


class Ingress:
    def __init__(self):
        self.events = []

    def record_event(self, kind, path, **kwargs):
        self.events.append((kind, path, kwargs))
        return ("persisted",)


class Observer:
    def __init__(self):
        self.alive = False
        self.paths = []

    def schedule(self, handler, path, recursive):
        self.paths.append(path)

    def start(self):
        self.alive = True

    def stop(self):
        self.alive = False

    def join(self, timeout):
        pass

    def is_alive(self):
        return self.alive


def test_monitor_lifecycle_excludes_duplicate_roots_and_ignores_modified(tmp_path):
    child = tmp_path / "nested"
    child.mkdir()
    ingress = Ingress()
    observers = []

    def factory():
        observer = Observer()
        observers.append(observer)
        return observer

    monitor = NativeFilesystemMonitor(ingress, observer_factory=factory)
    settings = Settings(monitor_enabled=True, monitor_paths=[str(tmp_path), str(child)], monitor_extensions=["mkv"])
    monitor.sync(settings)
    monitor.sync(settings)
    assert len(observers) == 1 and observers[0].paths == [str(tmp_path)]
    monitor.on_created(FileCreatedEvent(str(child / "episode.mkv")))
    monitor.on_created(FileCreatedEvent(str(child / "partial.mkv.!qB")))
    monitor.on_modified(FileModifiedEvent(str(child / "episode.mkv")))
    assert len(ingress.events) == 1
    monitor.sync(settings.with_updates({"monitor_enabled": False}))
    monitor.on_created(FileCreatedEvent(str(child / "ignored.mkv")))
    assert len(ingress.events) == 1 and not observers[0].alive


def test_actual_new_file_event_reaches_ingress_without_initial_full_scan(tmp_path):
    (tmp_path / "existing.mkv").write_bytes(b"old")
    ingress = Ingress()
    monitor = NativeFilesystemMonitor(ingress, observer_factory=lambda: PollingObserver(timeout=0.02))
    try:
        monitor.sync(Settings(monitor_enabled=True, monitor_paths=[str(tmp_path)]))
        (tmp_path / "new.mkv").write_bytes(b"new")
        deadline = time.monotonic() + 3
        while not ingress.events and time.monotonic() < deadline:
            time.sleep(0.01)
        assert any(kind == "created" and path.endswith("new.mkv") for kind, path, _ in ingress.events)
        assert not any(path.endswith("existing.mkv") for _, path, _ in ingress.events)
    finally:
        monitor.stop()


def test_coordinator_syncs_monitor_even_when_scheduled_tasks_are_disabled():
    settings = Settings(monitor_enabled=True, monitor_paths=["/fixture/library"])
    synced = []
    stopped = []
    runtime = CoordinatorRuntime.__new__(CoordinatorRuntime)
    runtime._config_adapter = SimpleNamespace(load=lambda: settings)
    runtime._load_runtime_context = lambda: {}
    runtime._filesystem_monitor = SimpleNamespace(sync=synced.append, stop=lambda: stopped.append(True))
    runtime._schedules = None
    runtime._refresh_settings(force=True)
    assert synced == [settings]
    runtime._shutdown_native_leader()
    assert stopped == [True]
