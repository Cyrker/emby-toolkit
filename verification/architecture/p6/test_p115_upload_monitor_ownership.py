from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any

from fastapi.routing import APIRoute

from etk.domains import p115
from etk.domains.p115 import (
    LegacyP115UploadProjection,
    LegacyP115UploadProvider,
    NativeP115UploadSourceCleanupGateway,
    P115UploadMonitorConfigService,
    P115UploadMonitorIngressService,
    PostgresP115UploadMonitorRepository,
    UploadedSource,
    UploadMonitorControl,
    UploadOperation,
)
from etk.domains.p115.router import router

REPO_ROOT = Path(__file__).resolve().parents[3]
UPLOAD_FIXTURE = "verification/fixtures/http/p6-p115-upload-monitor.json"


def _source(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def _manifest(relative_path: str) -> dict[str, Any]:
    return json.loads(_source(relative_path))


def _imports(relative_path: str) -> set[str]:
    tree = ast.parse(_source(relative_path))
    direct = {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    imported_from = {str(node.module or "") for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    return direct | imported_from


def _contracts(entries: list[Any]) -> dict[str, dict[str, Any]]:
    return {str(entry["contract_id"]): entry for entry in entries if isinstance(entry, dict)}


def test_upload_monitor_route_and_public_domain_exports_are_native() -> None:
    bindings = [
        route
        for route in router.routes
        if isinstance(route, APIRoute) and route.path == "/api/p115/upload_monitor/config"
    ]
    assert {(frozenset(route.methods or ()), route.endpoint.__module__) for route in bindings} == {
        (frozenset({"GET"}), "etk.domains.p115.router"),
        (frozenset({"POST"}), "etk.domains.p115.router"),
    }
    expected_exports = {
        "LegacyP115UploadProjection",
        "LegacyP115UploadProvider",
        "NativeP115UploadSourceCleanupGateway",
        "P115UploadMonitorConfigService",
        "P115UploadMonitorIngressService",
        "PostgresP115UploadMonitorRepository",
        "UploadedSource",
        "UploadMonitorControl",
        "UploadOperation",
    }
    assert expected_exports <= set(p115.__all__)
    exported_classes = {
        LegacyP115UploadProjection,
        LegacyP115UploadProvider,
        NativeP115UploadSourceCleanupGateway,
        P115UploadMonitorConfigService,
        P115UploadMonitorIngressService,
        PostgresP115UploadMonitorRepository,
        UploadedSource,
        UploadMonitorControl,
        UploadOperation,
    }
    assert {value.__name__ for value in exported_classes} == expected_exports


def test_upload_monitor_persistence_and_domain_boundaries_are_native() -> None:
    migration = _source("migrations/versions/0010_p6_upload_monitor.py")
    assert 'down_revision: str | Sequence[str] | None = "0009_p6_ingestion_monitor"' in migration
    assert '"p115_upload_monitor_controls"' in migration
    assert '"p115_upload_operations"' in migration
    assert "p115_upload_records" in _source("src/etk/domains/p115/upload_repository.py")

    for relative_path in (
        "src/etk/domains/p115/upload_monitor.py",
        "src/etk/domains/p115/upload_repository.py",
    ):
        imports = _imports(relative_path)
        assert not any(value == "handler" or value.startswith("handler.") for value in imports)
        assert "threading" not in imports
        assert "queue" not in imports


def test_coordinator_worker_and_deep_delete_own_the_active_upload_lifecycle() -> None:
    coordinator = _source("src/etk/processes/coordinator.py")
    worker = _source("src/etk/processes/worker.py")
    celery = _source("src/etk/processes/celery_app.py")
    deep_delete = _source("src/etk/processes/p6_deep_delete.py")

    ensure = coordinator.index(").ensure_config()")
    coordinator_setup = coordinator.index("self._p115_upload_coordinator = P115UploadMonitorCoordinator(")
    assert ensure < coordinator_setup
    assert "persistent_upload_monitor_ingress" not in coordinator
    assert "UploadMonitorRuntime" not in coordinator
    assert "handler.p115_upload_monitor" not in coordinator

    assert "P115UploadOperationExecutor(" in worker
    assert "persistent_upload_source_cleanup" not in worker
    assert "execute_p115_upload_operation" in celery
    assert 'EXECUTE_P115_UPLOAD_OPERATION_TASK = "etk.p115.upload.execute"' in celery

    assert "persistent_upload_source_cleanup" not in deep_delete
    assert "upload_sources=NativeP115UploadSourceCleanupGateway(" in worker
    assert "delete_local_sources_by_pickcodes" not in deep_delete
    assert "LegacyP115UploadSourceCleanupGateway" not in deep_delete


def test_upload_monitor_legacy_contracts_have_fixture_backed_native_owners() -> None:
    routes = _manifest("verification/contracts/route-ownership.yaml")
    route_entries = _contracts(routes["method_bindings"])
    decorator_entries = _contracts(routes["routes"])
    for contract_id in ("route-binding-db961218ae8c", "route-binding-52dc313869ae"):
        entry = route_entries[contract_id]
        assert entry["status"] == "new"
        assert entry["new_owner"] == "etk.domains.p115.router"
        assert entry["fixture"] == UPLOAD_FIXTURE
    decorator = decorator_entries["route-decorator-931f8e775a88"]
    assert decorator["status"] == "new"
    assert decorator["new_owner"] == "etk.domains.p115.router"
    assert decorator["fixture"] == UPLOAD_FIXTURE

    settings = _manifest("verification/contracts/config-ownership.yaml")
    setting = next(
        entry
        for entry in settings["standalone_app_settings"]
        if isinstance(entry, dict) and entry.get("contract_id") == "setting-9c5e65b39ae5"
    )
    assert setting["status"] == "new"
    assert setting["new_owner"] == "etk.domains.p115.upload_monitor.P115UploadMonitorConfigService"

    tables = _manifest("verification/contracts/table-ownership.yaml")
    table = next(
        entry
        for entry in tables["tables"]
        if isinstance(entry, dict) and entry.get("contract_id") == "table-0661d1e5c85d"
    )
    assert table["status"] == "new"
    assert table["new_owner"] == "etk.domains.p115.upload_repository.PostgresP115UploadMonitorRepository"
