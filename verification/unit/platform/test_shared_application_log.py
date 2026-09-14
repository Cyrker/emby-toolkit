from __future__ import annotations

import logging
import multiprocessing
from pathlib import Path

from etk.platform.observability.file_log import SharedApplicationLog


def _write_logs(directory, worker):
    handler = SharedApplicationLog(Path(directory), max_bytes=1000000)
    for index in range(20):
        handler.emit(logging.LogRecord("fixture", logging.INFO, "", 0, f"worker-{worker}-line-{index}", (), None))


def test_multiple_processes_share_one_file(tmp_path):
    context = multiprocessing.get_context("spawn")
    processes = [context.Process(target=_write_logs, args=(str(tmp_path), worker)) for worker in range(2)]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0
    lines = (tmp_path / "app.log").read_text().splitlines()
    assert len(lines) == 40
    assert len(list(tmp_path.glob("app.log*"))) == 1


def test_rotation_keeps_only_one_previous_file_and_redacts_secrets(tmp_path):
    handler = SharedApplicationLog(tmp_path, max_bytes=180)
    for _ in range(10):
        handler.emit(
            logging.LogRecord(
                "fixture", logging.INFO, "", 0, "download https://fixture.invalid/image?api_key=private-value", (), None
            )
        )
    assert sorted(p.name for p in tmp_path.glob("app.log*")) == ["app.log", "app.log.1"]
    assert all("private-value" not in path.read_text() for path in tmp_path.glob("app.log*"))
