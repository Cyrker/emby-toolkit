from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from pathlib import Path

from fastapi.routing import APIRoute

from etk.domains.p115.router import router

REPO_ROOT = Path(__file__).resolve().parents[3]


def _source(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def test_p6_5_strm_domain_has_no_later_slice_runtime_ownership() -> None:
    source = _source("src/etk/domains/p115/strm_sync.py")
    tree = ast.parse(source)
    imports = {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names} | {
        str(node.module or "") for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    }
    assert not any(
        forbidden in imported
        for imported in imports
        for forbidden in (
            "ffprobe",
            "monitor_service",
            "p115_upload_monitor",
            "tasks.p115",
            "watchdog",
        )
    )
    assert all(
        marker not in source
        for marker in (
            "subprocess.run",
            "fs_move",
            "virtual-play",
            "intro_fingerprint",
            "play_pool",
        )
    )
    assert "mediainfo_deferred_to_p6_6" in source
    assert "watchdog_deferred_to_p6_7" in source
    assert "life_event_daemon_deferred_to_p6_8" not in source


def test_p6_8_life_event_daemon_is_retired_from_active_runtime() -> None:
    runtime = _source("src/etk/bootstrap/legacy_runtime.py")
    legacy_task = _source("tasks/p115.py")

    assert "LifeEventMonitorDaemon" not in runtime
    assert "class LifeEventMonitorDaemon" in legacy_task
    assert "生活事件监控已停用" in legacy_task


def test_p6_5_worker_uses_confirmable_httpx_emby_delivery_not_legacy_notify() -> None:
    worker = _source("src/etk/processes/worker.py")
    tree = ast.parse(worker)
    gateway = _source("src/etk/domains/p115/emby_path_gateway.py")
    p6_router_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "P6P115TaskRouter"
    ]
    full_router_calls = [call for call in p6_router_calls if len(call.args) == 6]
    isolated_router_calls = [
        call
        for call in p6_router_calls
        if not call.args
        and {
            "directory_sync",
            "organize_execution",
            "organize_record_tmdb_backfill",
            "strm_sync",
            "fingerprint_repair",
            "intro_backfill",
            "organize_reconciliation",
        }
        <= {keyword.arg for keyword in call.keywords}
    ]
    assert len(full_router_calls) == 1
    assert len(isolated_router_calls) == 1
    p6_router_call = full_router_calls[0]
    assert isinstance(p6_router_call.args[2], ast.Call)
    assert isinstance(p6_router_call.args[2].func, ast.Name)
    assert p6_router_call.args[2].func.id == "P115StrmSyncService"
    assert isinstance(p6_router_call.args[4], ast.Name)
    assert p6_router_call.args[4].id == "playback_service"
    assert isinstance(p6_router_call.args[5], ast.Call)
    assert isinstance(p6_router_call.args[5].func, ast.Name)
    assert p6_router_call.args[5].func.id == "P115IntroBackfillTaskService"
    strm_mediainfo_assignments = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == "P115StrmMediaInfoEnrichmentService"
    ]
    assert len(strm_mediainfo_assignments) == 2
    strm_mediainfo_targets = {
        target.id
        for assignment in strm_mediainfo_assignments
        for target in assignment.targets
        if isinstance(target, ast.Name)
    }
    strm_mediainfo_keyword = next(keyword for keyword in p6_router_call.args[2].keywords if keyword.arg == "mediainfo")
    assert isinstance(strm_mediainfo_keyword.value, ast.Name)
    assert strm_mediainfo_keyword.value.id in strm_mediainfo_targets
    fingerprint_assignments = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == "P115FingerprintRepairService"
    ]
    assert len(fingerprint_assignments) == 2
    fingerprint_variables = {
        target.id
        for assignment in fingerprint_assignments
        for target in assignment.targets
        if isinstance(target, ast.Name)
    }
    assert isinstance(p6_router_call.args[3], ast.Name)
    assert p6_router_call.args[3].id in fingerprint_variables
    restore_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "P115MediaInfoRestoreService"
    ]
    assert len(restore_calls) == 2
    for restore_call in restore_calls:
        fingerprint_keyword = next(keyword for keyword in restore_call.keywords if keyword.arg == "fingerprints")
        assert isinstance(fingerprint_keyword.value, ast.Name)
        assert fingerprint_keyword.value.id in fingerprint_variables

    delivery_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "EmbyPathChangeEffectDelivery"
    ]
    assert len(delivery_calls) == 1
    assert len(delivery_calls[0].args) == 2
    assert isinstance(delivery_calls[0].args[1], ast.Call)
    assert isinstance(delivery_calls[0].args[1].func, ast.Name)
    assert delivery_calls[0].args[1].func.id == "HttpxEmbyPathChangeGateway"
    assert not any(isinstance(node, ast.Name) and node.id == "LegacyEmbyPathChangeGateway" for node in ast.walk(tree))
    assert "/Library/Media/Updated" in gateway
    assert "/Refresh" in gateway
    assert "_CONFIRMED_STATUS_CODES = frozenset({200, 204})" in gateway


def test_p6_5_persistent_plan_and_cache_columns_are_declared_by_single_head() -> None:
    migration = _source("migrations/versions/0007_p6_strm_sync.py")
    tree = ast.parse(migration)
    create_table = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "op"
        and node.func.attr == "create_table"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and node.args[0].value == "p115_strm_sync_plans"
    )
    columns = {
        str(argument.args[0].value)
        for argument in create_table.args[1:]
        if isinstance(argument, ast.Call)
        and isinstance(argument.func, ast.Attribute)
        and argument.func.attr == "Column"
        and argument.args
        and isinstance(argument.args[0], ast.Constant)
    }
    assert 'down_revision: str | Sequence[str] | None = "0006_p6_organize_execution"' in migration
    assert columns >= {
        "plan_id",
        "account_id",
        "expected_cache_revision",
        "operation_revision",
        "plan_json",
        "status",
        "local_effects_started",
    }
    assert '"sync_root_id"' in migration
    assert '"strm_content_sha256"' in migration
    assert "roots_array" in migration
    assert "writes_array" in migration
    assert "deletes_array" in migration
    assert "notifications_object" in migration


def test_p6_5_route_and_task_ownership_are_fixture_backed() -> None:
    bindings = [
        route
        for route in router.routes
        if isinstance(route, APIRoute) and route.path == "/api/p115/replace_strm" and route.methods == {"POST"}
    ]
    assert len(bindings) == 1
    assert bindings[0].endpoint.__module__ == "etk.domains.p115.router"

    routes = json.loads(_source("verification/contracts/route-ownership.yaml"))
    replace_route = next(
        route
        for route in routes["method_bindings"]
        if route["method"] == "POST" and route["path"] == "/api/p115/replace_strm"
    )
    assert replace_route["status"] == "new"
    assert replace_route["new_owner"] == "etk.domains.p115.router"
    assert replace_route["fixture"] == "verification/fixtures/http/p6-p115-replace-strm.json"

    tasks = json.loads(_source("verification/contracts/task-ownership.yaml"))
    tasks_by_key = {task["task_key"]: task for task in tasks["tasks"]}
    for task_key in ("full-sync-strm", "monitor-115-life-events"):
        task = tasks_by_key[task_key]
        assert task["status"] == "new"
        assert task["new_owner"] == "etk.processes.p6_p115_tasks.P6P115TaskRouter"
        assert task["ownership_layers"]["execution_owner"] == ("etk.domains.p115.strm_sync.P115StrmSyncService")


def test_native_strm_execution_imports_do_not_load_legacy_modules() -> None:
    program = """
import json
import sys
from etk.domains.p115 import (
    AtomicP115LocalStrmLibraryGateway,
    HttpxP115StrmSyncGateway,
    P115StrmSyncService,
)
from etk.processes.p6_p115_tasks import P6P115TaskRouter
forbidden_prefixes = ('database', 'extensions', 'handler', 'routes', 'task_manager', 'tasks')
loaded = sorted(
    name for name in sys.modules
    if any(name == prefix or name.startswith(prefix + '.') for prefix in forbidden_prefixes)
)
print(json.dumps({
    'gateway': HttpxP115StrmSyncGateway.__name__,
    'library': AtomicP115LocalStrmLibraryGateway.__name__,
    'loaded': loaded,
    'router': P6P115TaskRouter.__name__,
    'service': P115StrmSyncService.__name__,
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
        "gateway": "HttpxP115StrmSyncGateway",
        "library": "AtomicP115LocalStrmLibraryGateway",
        "loaded": [],
        "router": "P6P115TaskRouter",
        "service": "P115StrmSyncService",
    }
