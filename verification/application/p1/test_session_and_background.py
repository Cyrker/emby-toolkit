from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import types
from pathlib import Path

import pytest
from flask import Flask
from flask.sessions import SecureCookieSessionInterface

from etk.compatibility.background import (
    BackgroundCapacityError,
    active_background_task_count,
    resume_background_tasks,
    spawn,
    spawn_later,
    stop_background_tasks,
)
from etk.compatibility.session_codec import LegacySessionCodec

REPO_ROOT = Path(__file__).resolve().parents[3]
SESSION_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "http" / "flask-session.json"


def test_session_codec_is_bidirectional_with_flask_fixture() -> None:
    fixture = json.loads(SESSION_FIXTURE.read_text(encoding="utf-8"))
    app = Flask("legacy-session-fixture")
    app.secret_key = bytes.fromhex(fixture["signing_key_hex"])
    codec = LegacySessionCodec(app)
    serializer = SecureCookieSessionInterface().get_signing_serializer(app)
    assert serializer is not None

    assert app.config["SESSION_COOKIE_NAME"] == fixture["cookie_name"]
    assert codec.loads(fixture["signed_value"]) == fixture["payload"]
    assert serializer.loads(fixture["signed_value"]) == fixture["payload"]
    assert serializer.loads(codec.dumps(fixture["payload"])) == fixture["payload"]


def test_compatibility_background_spawn_and_cancellation() -> None:
    resume_background_tasks()
    immediate = threading.Event()
    delayed = threading.Event()
    try:
        handle = spawn(immediate.set)
        handle.join(timeout=2)
        assert immediate.is_set()
        assert handle.ready()

        cancelled = spawn_later(0.05, delayed.set)
        cancelled.kill()
        cancelled.join(timeout=1)
        assert not delayed.is_set()

        completed = spawn_later(0.01, delayed.set)
        completed.join(timeout=2)
        assert delayed.is_set()
    finally:
        assert stop_background_tasks(timeout=2)


def test_background_dispatcher_is_bounded_and_shutdown_is_retryable() -> None:
    resume_background_tasks()
    release = threading.Event()
    started = threading.Event()

    def blocking_task() -> None:
        started.set()
        release.wait()

    handle = spawn(blocking_task)
    assert started.wait(2)
    delayed = [spawn_later(60, lambda: None) for _ in range(40)]
    dispatcher_threads = [thread for thread in threading.enumerate() if thread.name.startswith("etk-compat-")]
    assert len([thread for thread in dispatcher_threads if "worker" in thread.name]) == 8
    assert len([thread for thread in dispatcher_threads if "scheduler" in thread.name]) == 1
    assert active_background_task_count() == 41

    assert not stop_background_tasks(timeout=0.01)
    assert not handle.ready()
    assert all(item.ready() for item in delayed)

    release.set()
    handle.join(2)
    assert stop_background_tasks(timeout=2)
    assert active_background_task_count() == 0
    resume_background_tasks()


def test_delayed_tasks_share_the_dispatcher_capacity_limit() -> None:
    resume_background_tasks()
    handles = [spawn_later(3600, lambda: None) for _ in range(256)]
    assert active_background_task_count() == 256
    with pytest.raises(BackgroundCapacityError, match="capacity"):
        spawn_later(3600, lambda: None)
    for handle in handles:
        handle.kill()
    assert active_background_task_count() == 0
    assert stop_background_tasks(timeout=2)
    resume_background_tasks()


def test_monitor_stop_retains_a_live_observer_for_retry(monkeypatch) -> None:
    from monitor_service import MonitorService

    class Observer:
        alive = True

        def stop(self) -> None:
            pass

        def join(self, timeout: float) -> None:
            assert timeout == 0.01

        def is_alive(self) -> bool:
            return self.alive

    observer = Observer()
    monitor = MonitorService.__new__(MonitorService)
    monitor.observer = observer
    monitor._starter_handle = None
    monitor._observer_lock = threading.RLock()
    monitor._start_generation = 0
    upload_module = types.SimpleNamespace(UploadMonitorRuntime=types.SimpleNamespace(_instance=None))
    monkeypatch.setitem(sys.modules, "handler.p115_upload_monitor", upload_module)

    assert monitor.stop(timeout=0.01) is False
    assert monitor.observer is observer
    assert monitor.stop(timeout=0.01) is False
    assert monitor.observer is observer

    observer.alive = False
    assert monitor.stop(timeout=0.01) is True
    assert monitor.observer is None


def test_tg_queue_consumer_is_explicit_and_stoppable(tmp_path: Path) -> None:
    script = """
import json
import threading
import handler.tg_userbot as module

before = module.is_tg_task_consumer_running()
imported_threads = [thread.name for thread in threading.enumerate() if thread.name == 'TG_Task_Consumer']
module.start_tg_task_consumer()
started = module.is_tg_task_consumer_running()
stopped_cleanly = module.stop_tg_task_consumer(timeout=2)
after = module.is_tg_task_consumer_running()
print(json.dumps({
    'before': before,
    'imported_threads': imported_threads,
    'started': started,
    'stopped_cleanly': stopped_cleanly,
    'after': after,
}))
"""
    environment = os.environ.copy()
    environment.update(APP_DATA_DIR=str(tmp_path / "data"), PYTHONDONTWRITEBYTECODE="1")
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=REPO_ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    assert payload == {
        "before": False,
        "imported_threads": [],
        "started": True,
        "stopped_cleanly": True,
        "after": False,
    }
