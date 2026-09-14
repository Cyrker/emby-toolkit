from datetime import UTC, datetime, timedelta

from etk.domains.operations.job_logs import PersistentJobLogStorage
from etk.domains.operations.logs import LocalLogStorage, LogService
from etk.platform.jobs import PostgresJobStore


def test_one_history_entry_merges_parent_child_and_runtime_logs(p4_runtime, tmp_path):
    store = PostgresJobStore(p4_runtime.unit_of_work)
    parent = store.create_job(task_key="fixture-parent", payload={"display_name": "获取演职人员"})
    child = store.create_job(
        task_key="fixture-child",
        payload={"display_name": "下载分集主图"},
        parent_job_id=parent.projection.job.id,
        workflow_step=1,
    )
    now = datetime.now(UTC)
    store.append_log(
        parent.projection.job.id, {"timestamp": now.isoformat(), "level": "info", "message": "演员资料已获取"}
    )
    store.append_log(
        child.projection.job.id,
        {"timestamp": (now + timedelta(seconds=1)).isoformat(), "level": "info", "message": "分集主图已下载"},
    )
    (tmp_path / "app.log").write_text("2000-01-01 00:00:00,000 - fixture - INFO - 收到 MoviePilot 通知\n")
    storage = PersistentJobLogStorage(p4_runtime.unit_of_work, LocalLogStorage(tmp_path))
    assert storage.list_names() == ["app.log"]
    lines = storage.read_lines("app.log")
    assert "MoviePilot" in lines[1]
    assert next(i for i, line in enumerate(lines) if "演员资料已获取" in line) < next(
        i for i, line in enumerate(lines) if "分集主图已下载" in line
    )
    assert len(LogService(storage).search_all_logs("分集主图已下载")) == 1
    assert not list(tmp_path.glob("app.log.task-*"))
