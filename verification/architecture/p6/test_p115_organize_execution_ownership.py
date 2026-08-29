from __future__ import annotations

import ast
import json
from pathlib import Path

from fastapi.routing import APIRoute

from etk.domains.p115.router import router

REPO_ROOT = Path(__file__).resolve().parents[3]
EXECUTION_MODULE = REPO_ROOT / "src/etk/domains/p115/organize_execution.py"
ADAPTER_MODULE = REPO_ROOT / "src/etk/domains/p115/adapters.py"
READBACK_MODULE = REPO_ROOT / "src/etk/domains/p115/organize_readback.py"
LEGACY_CLIENT_MODULE = REPO_ROOT / "handler/p115_service.py"
OPENAPI_PATH = REPO_ROOT / "verification/contracts/http/p4-openapi.json"
ROUTE_OWNERSHIP_PATH = REPO_ROOT / "verification/contracts/route-ownership.yaml"
TABLE_OWNERSHIP_PATH = REPO_ROOT / "verification/contracts/table-ownership.yaml"
ROUTER_MODULE = REPO_ROOT / "src/etk/domains/p115/router.py"
RECORDS_REPOSITORY_MODULE = REPO_ROOT / "src/etk/domains/p115/organize_records_repository.py"
HTTP_FIXTURE = "verification/fixtures/http/p6-p115-organize-execution-records.json"

P6_EXECUTION_RECORD_OPERATIONS = {
    ("POST", "/api/p115/organize/plans/{plan_id}/execute"),
    ("GET", "/api/p115/organize/executions/{execution_id}"),
    ("POST", "/api/p115/organize/executions/{execution_id}/reconcile"),
    ("GET", "/api/p115/records"),
    ("DELETE", "/api/p115/records/{record_id}"),
}
NATIVE_ONLY_EXECUTION_PATHS = {
    "/api/p115/organize/plans/{plan_id}/execute",
    "/api/p115/organize/executions/{execution_id}",
    "/api/p115/organize/executions/{execution_id}/reconcile",
}
LEGACY_RECORD_BINDINGS = {
    ("GET", "/api/p115/records"),
    ("DELETE", "/api/p115/records/<int:record_id>"),
}


def _class(tree: ast.Module, name: str) -> ast.ClassDef:
    return next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == name)


def _function(node: ast.AST, name: str) -> ast.FunctionDef:
    return next(value for value in ast.walk(node) if isinstance(value, ast.FunctionDef) and value.name == name)


def _called_attributes(node: ast.AST) -> set[str]:
    return {
        call.func.attr for call in ast.walk(node) if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
    }


def test_execution_scope_contains_only_ensure_move_rename_provider_effects() -> None:
    tree = ast.parse(EXECUTION_MODULE.read_text(encoding="utf-8"))
    mutate = _function(tree, "_mutate")
    called = _called_attributes(mutate)
    identifiers = {
        value.casefold()
        for node in ast.walk(tree)
        for value in (
            node.id if isinstance(node, ast.Name) else None,
            node.attr if isinstance(node, ast.Attribute) else None,
        )
        if value is not None
    }

    assert called & {"ensure_directory", "move", "rename"} == {"ensure_directory", "move", "rename"}
    assert identifiers.isdisjoint(
        {
            "smartorganizer",
            "fs_copy",
            "fs_delete",
            "strm",
            "subtitle",
            "watchdog",
            "ffmpeg",
            "ffprobe",
            "recycle",
            "trash",
        }
    )


def test_reconciliation_and_authoritative_readback_have_no_provider_mutation_call() -> None:
    tree = ast.parse(EXECUTION_MODULE.read_text(encoding="utf-8"))
    service = _class(tree, "P115OrganizeExecutionService")
    reconcile = _function(service, "reconcile")
    readback = _function(tree, "_readback_action")

    assert _called_attributes(reconcile).isdisjoint({"ensure_directory", "move", "rename"})
    assert (
        _called_attributes(readback)
        & {
            "ensure_directory",
            "move",
            "rename",
            "copy",
            "delete",
        }
        == set()
    )
    assert _called_attributes(readback) & {"list_children"} == {"list_children"}


def test_native_adapter_uses_only_fixed_backend_transport_methods() -> None:
    tree = ast.parse(ADAPTER_MODULE.read_text(encoding="utf-8"))
    client = _class(tree, "_LegacyP115OrganizeMutationClient")
    method_names = {
        str(call.args[0].value)
        for call in ast.walk(client)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
        and call.func.attr == "_call"
        and call.args
        and isinstance(call.args[0], ast.Constant)
        and isinstance(call.args[0].value, str)
    }

    assert method_names == {
        "fs_files_backend",
        "fs_mkdir_backend",
        "fs_move_backend",
        "fs_rename_backend",
    }
    assert "fs_mkdir" not in method_names
    assert "fs_move" not in method_names
    assert "fs_rename" not in method_names


def test_native_reconciliation_adapter_is_read_only_and_has_no_legacy_imports() -> None:
    tree = ast.parse(READBACK_MODULE.read_text(encoding="utf-8"))
    imported_roots = {
        alias.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
    }
    imported_roots.update(
        str(node.module).split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module and not node.level
    )
    assert imported_roots.isdisjoint({"config_manager", "database", "extensions", "handler", "routes", "tasks"})

    client = _class(tree, "_HttpxP115OrganizeReadbackClient")
    list_children = _function(client, "list_children")
    assert _called_attributes(list_children) & {"list_page"} == {"list_page"}
    for method_name in ("ensure_directory", "move", "rename"):
        method = _function(client, method_name)
        assert _called_attributes(method) == set()


def test_legacy_fixed_backend_methods_never_fallback_and_surface_transport_exceptions() -> None:
    tree = ast.parse(LEGACY_CLIENT_MODULE.read_text(encoding="utf-8"))
    required = {
        "fs_files_backend",
        "fs_mkdir_backend",
        "fs_move_backend",
        "fs_rename_backend",
    }
    methods = {
        node.name: node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name in required
    }

    assert set(methods) == required
    for name, method in methods.items():
        calls = [
            call
            for call in ast.walk(method)
            if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and call.func.attr == "_call_api"
        ]
        assert len(calls) == 1, name
        keywords = {keyword.arg: keyword.value for keyword in calls[0].keywords}
        assert {"force_openapi", "force_cookie", "raise_fixed_errors"} <= set(keywords), name
        assert isinstance(keywords["raise_fixed_errors"], ast.Constant), name
        assert keywords["raise_fixed_errors"].value is True, name
        assert _called_attributes(method).isdisjoint({"fs_delete", "delete", "recycle", "trash"}), name


def test_p64_routes_have_exact_native_fastapi_ownership_and_openapi_contract() -> None:
    p64_paths = {path for _method, path in P6_EXECUTION_RECORD_OPERATIONS}
    observed: set[tuple[str, str]] = set()
    endpoint_modules: set[str] = set()
    for route_value in router.routes:
        if not isinstance(route_value, APIRoute) or route_value.path not in p64_paths:
            continue
        observed.update((method, route_value.path) for method in (route_value.methods or set()))
        endpoint_modules.add(route_value.endpoint.__module__)

    assert observed == P6_EXECUTION_RECORD_OPERATIONS
    assert endpoint_modules == {"etk.domains.p115.router"}

    schema = json.loads(OPENAPI_PATH.read_text(encoding="utf-8"))
    for method, path in P6_EXECUTION_RECORD_OPERATIONS:
        operation = schema["paths"][path][method.lower()]
        assert operation["responses"]["200"]["content"]
    for path in {
        "/api/p115/organize/plans/{plan_id}/execute",
        "/api/p115/organize/executions/{execution_id}/reconcile",
    }:
        operation = schema["paths"][path]["post"]
        assert "422" not in operation["responses"]
        assert operation["requestBody"]["content"]["application/json"]["schema"]


def test_native_only_execution_routes_are_not_fabricated_as_legacy_manifest_entries() -> None:
    ownership = json.loads(ROUTE_OWNERSHIP_PATH.read_text(encoding="utf-8"))
    catalog_paths = {str(entry.get("path")) for family in ("routes", "method_bindings") for entry in ownership[family]}

    assert catalog_paths.isdisjoint(NATIVE_ONLY_EXECUTION_PATHS)


def test_legacy_record_bindings_and_table_have_explicit_native_owners() -> None:
    route_ownership = json.loads(ROUTE_OWNERSHIP_PATH.read_text(encoding="utf-8"))
    bindings = {
        (entry["method"], entry["path"]): entry
        for entry in route_ownership["method_bindings"]
        if (entry["method"], entry["path"]) in LEGACY_RECORD_BINDINGS
    }
    assert set(bindings) == LEGACY_RECORD_BINDINGS
    for entry in bindings.values():
        assert entry["status"] == "new"
        assert entry["new_owner"] == "etk.domains.p115.router"
        assert entry["fixture"] == HTTP_FIXTURE
        assert entry["fixture_exception"] is None
        assert entry["validation_owner"] == "p115/organize-records"

    table_ownership = json.loads(TABLE_OWNERSHIP_PATH.read_text(encoding="utf-8"))
    record_table = next(entry for entry in table_ownership["tables"] if entry["name"] == "p115_organize_records")
    assert record_table["status"] == "new"
    assert record_table["new_owner"] == (
        "etk.domains.p115.organize_records_repository.SqlAlchemyP115OrganizeRecordsRepository"
    )
    assert record_table["validation_owner"] == "p115/organize-records"


def test_record_delete_transport_and_repository_have_no_remote_provider_delete_path() -> None:
    router_tree = ast.parse(ROUTER_MODULE.read_text(encoding="utf-8"))
    delete_handler = next(
        node
        for node in router_tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "delete_organize_record"
    )
    handler_nodes = [child for statement in delete_handler.body for child in ast.walk(statement)]
    handler_calls = {
        node.func.id if isinstance(node.func, ast.Name) else node.func.attr
        for node in handler_nodes
        if isinstance(node, ast.Call) and isinstance(node.func, (ast.Name, ast.Attribute))
    }
    handler_attributes = {node.attr for node in handler_nodes if isinstance(node, ast.Attribute)}
    assert "_organize_records_service" in handler_calls
    assert "delete_record" in handler_attributes
    assert handler_calls.union(handler_attributes).isdisjoint(
        {
            "_organize_execution_service",
            "client",
            "delete",
            "fs_delete",
            "open",
        }
    )

    repository_tree = ast.parse(RECORDS_REPOSITORY_MODULE.read_text(encoding="utf-8"))
    imported_modules = {
        str(node.module)
        for node in ast.walk(repository_tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    identifiers = {
        node.id if isinstance(node, ast.Name) else node.attr
        for node in ast.walk(repository_tree)
        if isinstance(node, (ast.Name, ast.Attribute))
    }
    assert imported_modules.isdisjoint({"handler.p115_service", "etk.domains.p115.adapters"})
    assert identifiers.isdisjoint({"P115Service", "fs_delete", "p115_delete"})
