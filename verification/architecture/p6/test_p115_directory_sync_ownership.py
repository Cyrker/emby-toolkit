from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from etk.platform.tasks import P6_NATIVE_TASK_KEYS

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_p6_native_tasks_have_native_catalog_and_execution_ownership() -> None:
    assert P6_NATIVE_TASK_KEYS == {
        "full-sync-strm",
        "intro-fingerprint-backfill",
        "monitor-115-life-events",
        "play-pool-daily-speedtest",
        "repair-p115-fingerprints",
        "scan-monitor-folders",
        "scan-organize-115",
        "sync-115-directory-tree",
        "manual-correct-organize-records",
    }

    fixture = json.loads(
        (REPO_ROOT / "verification/fixtures/tasks/ordered-task-catalog.json").read_text(encoding="utf-8")
    )
    definitions = {item["key"]: item for item in fixture["tasks"]}
    assert all(definitions[task_key]["handler_kind"] == "native" for task_key in P6_NATIVE_TASK_KEYS)
    assert definitions["manual-correct-organize-records"]["target_stage"] == "P6"
    assert fixture["metrics"]["native_handler_count"] == sum(
        item["handler_kind"] == "native" for item in fixture["tasks"]
    )
    assert fixture["metrics"]["legacy_adapter_count"] == sum(
        item["handler_kind"] == "legacy" for item in fixture["tasks"]
    )

    tasks = json.loads((REPO_ROOT / "verification/contracts/task-ownership.yaml").read_text(encoding="utf-8"))
    tasks_by_key = {item["task_key"]: item for item in tasks["tasks"]}
    expected_execution_owners = {
        "full-sync-strm": "etk.domains.p115.strm_sync.P115StrmSyncService",
        "intro-fingerprint-backfill": ("etk.domains.p115.intro_detection_compatibility.P115IntroBackfillTaskService"),
        "monitor-115-life-events": "etk.domains.p115.strm_sync.P115StrmSyncService",
        "play-pool-daily-speedtest": ("etk.domains.p115.playback_service.P115PlaybackService.daily_speedtest"),
        "repair-p115-fingerprints": "etk.domains.p115.fingerprints.P115FingerprintRepairService",
        "sync-115-directory-tree": "etk.domains.p115.directory_sync.P115DirectorySyncService",
        "scan-organize-115": "etk.domains.p115.organize_execution.P115OrganizeExecutionService.execute",
        "manual-correct-organize-records": (
            "etk.domains.p115.organize_execution.P115OrganizeExecutionService.reconcile"
        ),
        "scan-monitor-folders": "etk.processes.p6_ingestion_tasks.P6IngestionScanService",
    }
    for task_key, execution_owner in expected_execution_owners.items():
        task = tasks_by_key[task_key]
        assert task["status"] == "new"
        envelope_owner = (
            "etk.processes.p6_ingestion_tasks.P6IngestionTaskRouter"
            if task_key == "scan-monitor-folders"
            else "etk.processes.p6_p115_tasks.P6P115TaskRouter"
        )
        assert task["new_owner"] == envelope_owner
        assert task["target_stage"] == "P6"
        assert task["target_domains"] == ["p115", "ingestion"]
        assert task["ownership_layers"] == {
            "envelope_owner": envelope_owner,
            "execution_kind": "native-use-case",
            "execution_owner": execution_owner,
        }

    tables = json.loads((REPO_ROOT / "verification/contracts/table-ownership.yaml").read_text(encoding="utf-8"))
    table = next(item for item in tables["tables"] if item["name"] == "p115_filesystem_cache")
    assert table["status"] == "new"
    assert table["new_owner"] == "etk.domains.p115.repository.SqlAlchemyP115DirectoryCacheRepository"


def test_native_directory_sync_import_does_not_load_legacy_task_or_provider_modules() -> None:
    program = """
import json
import sys
from etk.processes.p6_p115_tasks import P6P115TaskRouter
forbidden = {'tasks.p115', 'handler.p115_service', 'database.connection', 'extensions'}
print(json.dumps({
    'loaded': sorted(forbidden.intersection(sys.modules)),
    'router': P6P115TaskRouter.__name__,
}))
"""
    environment = os.environ.copy()
    python_path = [str(REPO_ROOT / "src"), str(REPO_ROOT)]
    if environment.get("PYTHONPATH"):
        python_path.append(environment["PYTHONPATH"])
    environment["PYTHONPATH"] = os.pathsep.join(python_path)

    completed = subprocess.run(
        [sys.executable, "-c", program],
        cwd=REPO_ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(completed.stdout) == {
        "loaded": [],
        "router": "P6P115TaskRouter",
    }
