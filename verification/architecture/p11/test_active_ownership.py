from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]

RETIRED_TABLES = {
    "shared_credit_ledger_local",
    "shared_credit_snapshot",
}
RETIRED_SETTINGS = {
    "license_key",
    "media_cleanup_delete_delay",
    "media_cleanup_keep_one_per_res",
    "media_cleanup_library_ids",
    "media_cleanup_rules",
    "p115_copy_play_active_sources",
    "p115_copy_play_clones",
    "p115_play_pool_sessions",
    "p115_play_pool_user_cookie_rewards",
    "p115_virtual_play_sessions",
    "stream_feature_mapping",
}


def _manifest(name: str) -> dict[str, Any]:
    return json.loads((REPO_ROOT / "verification" / "contracts" / name).read_text(encoding="utf-8"))


def _assert_closed(entries: list[dict[str, Any]]) -> None:
    assert all(entry["status"] != "legacy" for entry in entries)
    for entry in entries:
        if entry["status"] == "new":
            assert entry["new_owner"]
        elif entry["status"] == "retired-approved":
            assert entry["retirement_approval"]
        else:
            raise AssertionError(f"unexpected ownership status: {entry['status']!r}")


def test_route_task_table_config_and_frontend_active_ownership_is_closed() -> None:
    routes = _manifest("route-ownership.yaml")
    tasks = _manifest("task-ownership.yaml")
    tables = _manifest("table-ownership.yaml")
    config = _manifest("config-ownership.yaml")
    frontend = _manifest("frontend-route-ownership.yaml")

    for entries in (
        routes["routes"],
        routes["method_bindings"],
        tasks["tasks"],
        tables["tables"],
        config["bootstrap"],
        config["dynamic"],
        config["standalone_app_settings"],
        frontend["routes"],
    ):
        _assert_closed(entries)
    assert {entry["name"] for entry in tables["tables"] if entry["status"] == "retired-approved"} == (RETIRED_TABLES)
    assert {
        entry["key"] for entry in config["standalone_app_settings"] if entry["status"] == "retired-approved"
    } == RETIRED_SETTINGS
    assert config["unresolved_dynamic_setting_calls"] == []


def test_native_role_import_graph_does_not_load_legacy_runtime_modules() -> None:
    program = """
import json
import sys
import etk.bootstrap.app_factory
import etk.processes.coordinator
import etk.processes.proxy
import etk.processes.worker
forbidden = (
    'config_manager', 'database', 'extensions', 'handler', 'routes',
    'scheduler_manager', 'task_manager', 'tasks',
)
loaded = sorted(
    name for name in sys.modules
    if any(name == prefix or name.startswith(prefix + '.') for prefix in forbidden)
)
print(json.dumps(loaded))
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

    assert json.loads(completed.stdout) == []


def test_runtime_context_and_translation_cache_have_native_wiring_sentinels() -> None:
    coordinator = (REPO_ROOT / "src/etk/processes/coordinator.py").read_text(encoding="utf-8")
    worker = (REPO_ROOT / "src/etk/processes/worker.py").read_text(encoding="utf-8")

    assert "import config_manager" not in coordinator
    assert "from database import settings_db" not in coordinator
    assert "import extensions" not in coordinator
    assert "SqlAlchemyAppSettingsRepository(container.database.unit_of_work)" in coordinator
    assert "SqlAlchemyTranslationCache(database.unit_of_work)" in worker
    assert "ConfiguredBatchTranslation(settings_provider, translation_cache)" in worker
