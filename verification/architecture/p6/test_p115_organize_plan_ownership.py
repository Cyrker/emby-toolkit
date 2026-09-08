from __future__ import annotations

import ast
import json
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from fastapi.routing import APIRoute

from etk.domains.p115.router import router

REPO_ROOT = Path(__file__).resolve().parents[3]
OPENAPI_PATH = REPO_ROOT / "verification/contracts/http/p4-openapi.json"
ROUTE_OWNERSHIP_PATH = REPO_ROOT / "verification/contracts/route-ownership.yaml"
ORGANIZE_PLAN_MODULE = REPO_ROOT / "src/etk/domains/p115/organize_plan.py"
ADAPTER_MODULE = REPO_ROOT / "src/etk/domains/p115/adapters.py"

P6_PLANNING_OPERATIONS = {
    ("GET", "/api/p115/sorting_rules"),
    ("POST", "/api/p115/sorting_rules"),
    ("GET", "/api/p115/rename_config"),
    ("POST", "/api/p115/rename_config"),
    ("POST", "/api/p115/rename_config/preview"),
    ("GET", "/api/p115/episode_regex_rules"),
    ("POST", "/api/p115/episode_regex_rules"),
    ("POST", "/api/p115/organize/plans/dry-run"),
    ("GET", "/api/p115/organize/plans/{plan_id}"),
}
NATIVE_ONLY_PLAN_PATHS = {
    "/api/p115/organize/plans/dry-run",
    "/api/p115/organize/plans/{plan_id}",
}
FORBIDDEN_IDENTIFIERS = {
    "smartorganizer",
    "subprocess",
    "ffmpeg",
    "ffprobe",
    "strm",
    "emby",
    "telegram",
}
FORBIDDEN_PROVIDER_WRITES = {
    "copy",
    "delete",
    "fs_copy",
    "fs_delete",
    "fs_mkdir",
    "fs_move",
    "fs_rename",
    "mkdir",
    "move",
    "rename",
    "transfer",
}


def test_p6_planning_routes_have_exact_native_fastapi_ownership_and_openapi_contract() -> None:
    planning_paths = {path for _method, path in P6_PLANNING_OPERATIONS}
    observed: set[tuple[str, str]] = set()
    endpoint_modules: set[str] = set()
    for route in router.routes:
        if not isinstance(route, APIRoute) or route.path not in planning_paths:
            continue
        observed.update((method, route.path) for method in (route.methods or set()))
        endpoint_modules.add(route.endpoint.__module__)

    assert observed == P6_PLANNING_OPERATIONS
    assert endpoint_modules == {"etk.domains.p115.router"}

    schema = json.loads(OPENAPI_PATH.read_text(encoding="utf-8"))
    for method, path in P6_PLANNING_OPERATIONS:
        operation = schema["paths"][path][method.lower()]
        if method == "POST":
            assert "422" not in operation["responses"], (method, path)
            assert operation.get("requestBody"), (method, path)
    request_schema = schema["paths"]["/api/p115/organize/plans/dry-run"]["post"]["requestBody"]["content"][
        "application/json"
    ]["schema"]
    assert set(request_schema["properties"]) == {
        "recognition_hints",
        "request_intent_id",
        "source_cid",
        "source_file_ids",
        "source_name",
    }
    response_schema = schema["components"]["schemas"]["OrganizePlanDocumentResponse"]
    assert {"actions", "errors", "revisions", "source_snapshot", "status", "warnings"} <= set(
        response_schema["properties"]
    )


def test_native_only_plan_routes_are_not_fabricated_as_legacy_manifest_entries() -> None:
    ownership = json.loads(ROUTE_OWNERSHIP_PATH.read_text(encoding="utf-8"))
    catalog_paths = {str(entry.get("path")) for family in ("routes", "method_bindings") for entry in ownership[family]}

    assert catalog_paths.isdisjoint(NATIVE_ONLY_PLAN_PATHS)


def test_dry_run_service_has_only_read_provider_calls_and_no_effect_runtime_dependencies() -> None:
    tree = ast.parse(ORGANIZE_PLAN_MODULE.read_text(encoding="utf-8"))
    identifiers = {
        value.casefold()
        for node in ast.walk(tree)
        for value in (
            node.id if isinstance(node, ast.Name) else None,
            node.attr if isinstance(node, ast.Attribute) else None,
        )
        if value is not None
    }
    imports = {
        module.casefold().split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for module in ([alias.name for alias in node.names] if isinstance(node, ast.Import) else [str(node.module)])
    }
    client_calls = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "client"
    }
    called_names = {
        node.func.id.casefold() if isinstance(node.func, ast.Name) else node.func.attr.casefold()
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, (ast.Name, ast.Attribute))
    }

    assert identifiers.isdisjoint(FORBIDDEN_IDENTIFIERS)
    assert imports.isdisjoint(FORBIDDEN_IDENTIFIERS)
    assert called_names.isdisjoint(FORBIDDEN_PROVIDER_WRITES)
    assert client_calls == {"list_page"}


def test_dry_run_compatibility_gateways_import_only_pure_legacy_helpers_and_plaintext_defaults() -> None:
    tree = ast.parse(ADAPTER_MODULE.read_text(encoding="utf-8"))
    module_imports = {
        (str(node.module), alias.name) for node in tree.body if isinstance(node, ast.ImportFrom) for alias in node.names
    }
    assert {
        ("etk.policy_defaults", "DEFAULT_COUNTRY_MAPPING"),
        ("etk.policy_defaults", "DEFAULT_KEYWORD_MAPPING"),
        ("etk.policy_defaults", "DEFAULT_LANGUAGE_MAPPING"),
        ("etk.policy_defaults", "DEFAULT_STUDIO_MAPPING"),
    } <= module_imports
    classes = {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.ClassDef)
        and node.name in {"LegacyP115RuleRecognitionGateway", "LegacyP115PlanNamingGateway"}
    }
    assert set(classes) == {"LegacyP115RuleRecognitionGateway", "LegacyP115PlanNamingGateway"}

    observed_imports: dict[str, set[tuple[str, str]]] = {}
    for name, class_node in classes.items():
        observed_imports[name] = {
            (str(node.module), alias.name)
            for node in ast.walk(class_node)
            if isinstance(node, ast.ImportFrom)
            for alias in node.names
        }
        observed_imports[name].update(
            ("", alias.name) for node in ast.walk(class_node) if isinstance(node, ast.Import) for alias in node.names
        )
        identifiers = {
            value.casefold()
            for node in ast.walk(class_node)
            for value in (
                node.id if isinstance(node, ast.Name) else None,
                node.attr if isinstance(node, ast.Attribute) else None,
            )
            if value is not None
        }
        called_names = {
            node.func.id.casefold() if isinstance(node.func, ast.Name) else node.func.attr.casefold()
            for node in ast.walk(class_node)
            if isinstance(node, ast.Call) and isinstance(node.func, (ast.Name, ast.Attribute))
        }
        assert identifiers.isdisjoint(FORBIDDEN_IDENTIFIERS)
        assert called_names.isdisjoint(FORBIDDEN_PROVIDER_WRITES)

    assert observed_imports == {
        "LegacyP115RuleRecognitionGateway": {
            ("handler.p115_service", "_build_rule_parse_result"),
        },
        "LegacyP115PlanNamingGateway": {("handler.p115_rename", "P115RenameRenderer")},
    }


def test_p6_upload_monitor_migration_is_the_single_alembic_head() -> None:
    scripts = ScriptDirectory.from_config(Config(str(REPO_ROOT / "alembic.ini")))

    assert scripts.get_heads() == ["0027_p11_watchlist_display"]
