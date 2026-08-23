from __future__ import annotations

import threading
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

from etk.bootstrap.app_factory import create_app
from etk.bootstrap.config_sync import ApiRuntimeConfigSynchronizer
from etk.platform import Settings


class RecordingRuntime:
    def __init__(self, values: Mapping[str, Any]) -> None:
        self._config = dict(values)
        self.applied: list[dict[str, Any]] = []

    @property
    def config(self) -> Mapping[str, Any]:
        return self._config

    def apply_persisted_config_snapshot(self, new_config: Mapping[str, Any]) -> None:
        applied = dict(new_config)
        self.applied.append(applied)
        self._config.update(applied)


def test_synchronizer_updates_runtime_and_container_once_without_a_background_thread() -> None:
    current = [Settings.model_validate({"monitor_enabled": False})]
    runtime = RecordingRuntime(current[0].to_runtime_dict())
    snapshot = {"monitor_enabled": True, "p115_media_root_cid": "fixture-cid"}
    baseline_threads = {thread.ident for thread in threading.enumerate()}
    synchronizer = ApiRuntimeConfigSynchronizer(
        snapshot_reader=lambda: snapshot,
        settings_provider=lambda: current[0],
        settings_consumer=lambda value: current.__setitem__(0, value),
        runtime=runtime,
    )

    assert synchronizer.synchronize() is True
    assert synchronizer.synchronize() is False

    assert current[0].monitor_enabled is True
    assert current[0].p115_media_root_cid == "fixture-cid"
    assert len(runtime.applied) == 1
    assert {thread.ident for thread in threading.enumerate()} == baseline_threads


def test_concurrent_request_boundaries_apply_one_process_snapshot_once() -> None:
    current = [Settings.model_validate({"monitor_enabled": False})]
    runtime = RecordingRuntime(current[0].to_runtime_dict())
    synchronizer = ApiRuntimeConfigSynchronizer(
        snapshot_reader=lambda: {"monitor_enabled": True},
        settings_provider=lambda: current[0],
        settings_consumer=lambda value: current.__setitem__(0, value),
        runtime=runtime,
    )
    barrier = threading.Barrier(3)
    results: list[bool] = []

    def synchronize() -> None:
        barrier.wait(timeout=2)
        results.append(synchronizer.synchronize())

    workers = [threading.Thread(target=synchronize) for _ in range(2)]
    for worker in workers:
        worker.start()
    barrier.wait(timeout=2)
    for worker in workers:
        worker.join(timeout=2)

    assert sorted(results) == [False, True]
    assert len(runtime.applied) == 1


class RequestSynchronizer:
    def __init__(self, *, failure: Exception | None = None) -> None:
        self.failure = failure
        self.calls = 0

    def synchronize(self) -> bool:
        self.calls += 1
        if self.failure is not None:
            raise self.failure
        return False


def _static_dir(tmp_path: Path) -> Path:
    static_dir = tmp_path / "static"
    static_dir.mkdir()
    (static_dir / "index.html").write_text("<html>fixture</html>", encoding="utf-8")
    return static_dir


def test_api_middleware_synchronizes_business_requests_but_not_health(tmp_path: Path) -> None:
    synchronizer = RequestSynchronizer()
    app = create_app(
        start_runtime=False,
        static_dir=_static_dir(tmp_path),
        data_dir=tmp_path / "data",
        secret_key=b"fixture-session-signing-key",
        runtime_config_synchronizer=synchronizer,
    )

    with TestClient(app) as client:
        assert client.get("/api/health").status_code == 200
        assert synchronizer.calls == 0
        assert client.get("/api/auth/check_status").status_code == 200

    assert synchronizer.calls == 1


def test_api_middleware_returns_503_instead_of_serving_a_stale_snapshot(tmp_path: Path) -> None:
    synchronizer = RequestSynchronizer(failure=RuntimeError("fixture reload failure"))
    app = create_app(
        start_runtime=False,
        static_dir=_static_dir(tmp_path),
        data_dir=tmp_path / "data",
        secret_key=b"fixture-session-signing-key",
        runtime_config_synchronizer=synchronizer,
    )

    with TestClient(app) as client:
        response = client.get("/api/auth/check_status")

    assert response.status_code == 503
    assert response.json() == {"status": "error", "message": "运行配置同步失败，请稍后重试"}
