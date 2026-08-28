from __future__ import annotations

import argparse
import ast
from collections import defaultdict
from pathlib import Path
from typing import Any

from manifest_common import (
    REPO_ROOT,
    ManifestError,
    apply_contract_override,
    dump_yaml_compatible_json,
    evaluate_static,
    iter_project_python_files,
    load_constant_symbols,
    parse_python,
    relative_path,
    source_ref,
    stable_id,
)

SECRET_FRAGMENTS = ("password", "secret", "token", "cookie", "api_key", "license_key")
EXPLICIT_SECRET_KEYS = frozenset({"mp_config", "p115_play_pool_config"})


def _is_secret_key(key: str) -> bool:
    normalized = key.casefold()
    return normalized in EXPLICIT_SECRET_KEYS or any(fragment in normalized for fragment in SECRET_FRAGMENTS)


def _find_top_level_assignment(tree: ast.Module, name: str) -> ast.Assign:
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name for target in node.targets
        ):
            return node
    raise ManifestError(f"assignment {name} not found")


def _find_assignment(tree: ast.Module, name: str) -> ast.Assign:
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name for target in node.targets
        ):
            return node
    raise ManifestError(f"assignment {name} not found")


def _extract_definition(
    path: Path,
    tree: ast.Module,
    name: str,
    symbols: dict[str, Any],
    *,
    storage: str,
    mutability: str,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    assignment = _find_top_level_assignment(tree, name)
    if not isinstance(assignment.value, ast.Dict):
        raise ManifestError(f"{name} is not a dictionary")
    history: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for key_node, value_node in zip(assignment.value.keys, assignment.value.values):  # noqa: B905
        if key_node is None:
            continue
        key = str(evaluate_static(key_node, symbols))
        definition = evaluate_static(value_node, symbols)
        if not isinstance(definition, tuple) or len(definition) != 3:
            raise ManifestError(f"invalid config definition for {key}")
        section, declared_type, default = definition
        history[key].append(
            {
                "section": str(section),
                "declared_type": str(declared_type),
                "default": default,
                "source": source_ref(path, key_node.lineno),
            }
        )
    entries: list[dict[str, Any]] = []
    for key, definitions in sorted(history.items()):
        effective = definitions[-1]
        entries.append(
            apply_contract_override(
                {
                    "contract_id": stable_id("config", key),
                    "key": key,
                    **effective,
                    "definition_history": definitions,
                    "storage": storage,
                    "mutability": mutability,
                    "secret": _is_secret_key(key),
                    "target_stage": "P2",
                    "status": "legacy",
                    "new_owner": None,
                    "validation_owner": "system/configuration",
                    "validation_method": "config-contract",
                },
                "configs",
            )
        )
    return entries, history


def _static_setting_keys(
    call: ast.Call,
    local_symbols: dict[str, set[str]],
    constant_symbols: dict[str, Any],
    *,
    argument_index: int = 0,
) -> list[str]:
    if len(call.args) <= argument_index:
        return []
    argument = call.args[argument_index]
    if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
        return [argument.value]
    if isinstance(argument, ast.Name):
        return sorted(local_symbols.get(argument.id, set()))
    if isinstance(argument, ast.Attribute) and isinstance(argument.value, ast.Name):
        value = constant_symbols.get(f"{argument.value.id}.{argument.attr}")
        return [value] if isinstance(value, str) else []
    if (
        isinstance(argument, ast.Call)
        and isinstance(argument.func, ast.Name)
        and argument.func.id == "getattr"
        and len(argument.args) >= 3
        and isinstance(argument.args[2], ast.Constant)
        and isinstance(argument.args[2].value, str)
    ):
        return [argument.args[2].value]
    return []


def _static_string_values(node: ast.AST, constant_symbols: dict[str, Any]) -> set[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return {node.value}
    if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
        values = {item.value for item in node.elts if isinstance(item, ast.Constant) and isinstance(item.value, str)}
        return values if len(values) == len(node.elts) else set()
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "getattr"
        and len(node.args) >= 3
        and isinstance(node.args[0], ast.Name)
        and isinstance(node.args[1], ast.Constant)
        and isinstance(node.args[1].value, str)
        and isinstance(node.args[2], ast.Constant)
        and isinstance(node.args[2].value, str)
    ):
        value = constant_symbols.get(
            f"{node.args[0].id}.{node.args[1].value}",
            node.args[2].value,
        )
        return {value} if isinstance(value, str) else set()
    return set()


def _scan_setting_calls() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    resolved: dict[str, list[dict[str, Any]]] = defaultdict(list)
    unresolved: list[dict[str, Any]] = []
    call_names = {"get_setting", "save_setting", "delete_setting", "_save_setting_with_cursor"}
    constant_symbols = load_constant_symbols(REPO_ROOT / "constants.py", module_alias="constants")
    constant_symbols.update(
        load_constant_symbols(
            REPO_ROOT / "database/settings_db.py",
            module_alias="settings_db",
        )
    )
    for path in iter_project_python_files():
        rel = relative_path(path)
        if rel == "main.py" or rel.startswith(("scripts/refactor/", "verification/", "src/etk/")):
            continue
        try:
            _, tree = parse_python(path)
        except (SyntaxError, UnicodeDecodeError):
            continue
        local_symbols: dict[str, set[str]] = defaultdict(set)
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                values = _static_string_values(node.value, constant_symbols)
                for target in node.targets:
                    if isinstance(target, ast.Name) and values:
                        local_symbols[target.id].update(values)
            if isinstance(node, ast.For) and isinstance(node.target, ast.Name):
                values = _static_string_values(node.iter, constant_symbols)
                if values:
                    local_symbols[node.target.id].update(values)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            operation = (
                node.func.attr
                if isinstance(node.func, ast.Attribute)
                else node.func.id
                if isinstance(node.func, ast.Name)
                else None
            )
            if operation in {"get_shared_resource_config", "save_shared_resource_config"}:
                resolved["shared_resource_config"].append(
                    {"operation": operation, "source": source_ref(path, node.lineno)}
                )
                continue
            if operation not in call_names:
                continue
            keys = _static_setting_keys(
                node,
                local_symbols,
                constant_symbols,
                argument_index=1 if operation == "_save_setting_with_cursor" else 0,
            )
            item = {"operation": operation, "source": source_ref(path, node.lineno)}
            if keys:
                for key in keys:
                    resolved[key].append(item)
            elif (
                operation == "_save_setting_with_cursor"
                and rel == "database/settings_db.py"
                and len(node.args) > 1
                and isinstance(node.args[1], ast.Name)
                and node.args[1].id == "setting_key"
            ):
                # The public save_setting wrapper owns this deliberately dynamic forwarding call.
                continue
            else:
                unresolved.append(item)
    entries = [
        apply_contract_override(
            {
                "contract_id": stable_id("setting", key),
                "key": key,
                "storage": "app_settings",
                "storage_default": None,
                "default_source": "no database default; read fallbacks are call-site-specific",
                "accesses": sorted(accesses, key=lambda item: (item["source"], item["operation"])),
                "secret": _is_secret_key(key),
                "mutability": "runtime_dynamic",
                "restart_level": "call-site-specific",
                "target_stage": "P2",
                "status": "legacy",
                "new_owner": None,
                "validation_owner": "system/configuration",
                "validation_method": "app-settings-contract",
            },
            "settings",
        )
        for key, accesses in sorted(resolved.items())
    ]
    return entries, sorted(unresolved, key=lambda item: item["source"])


def _environment_aliases(tree: ast.Module, symbols: dict[str, Any]) -> list[dict[str, str]]:
    assignment = _find_assignment(tree, "env_to_config_map")
    mapping = evaluate_static(assignment.value, symbols)
    return [
        {"environment": str(environment), "config_key": str(config_key)}
        for environment, config_key in sorted(mapping.items())
    ]


def export_config() -> dict[str, Any]:
    path = REPO_ROOT / "config_manager.py"
    _, tree = parse_python(path)
    symbols = load_constant_symbols(REPO_ROOT / "constants.py", module_alias="constants")
    bootstrap, bootstrap_history = _extract_definition(
        path,
        tree,
        "BOOTSTRAP_CONFIG_DEF",
        symbols,
        storage="config.ini",
        mutability="process_bootstrap",
    )
    dynamic, dynamic_history = _extract_definition(
        path,
        tree,
        "DYNAMIC_CONFIG_DEF",
        symbols,
        storage="app_settings.dynamic_app_config",
        mutability="dynamic_reload",
    )
    standalone, unresolved = _scan_setting_calls()
    aliases = _environment_aliases(tree, symbols)
    alias_map: dict[str, list[str]] = defaultdict(list)
    for item in aliases:
        alias_map[item["config_key"]].append(item["environment"])
    for item in [*bootstrap, *dynamic]:
        item["environment_aliases"] = sorted(alias_map.get(item["key"], []))

    duplicate_definitions = {
        key: definitions
        for key, definitions in {**bootstrap_history, **dynamic_history}.items()
        if len(definitions) > 1
    }
    static_keys = {item["key"] for item in [*bootstrap, *dynamic]}
    standalone_keys = {item["key"] for item in standalone}
    return {
        "schema_version": 1,
        "generator": "scripts/refactor/export_config.py",
        "source_scope": ["config_manager.py", "constants.py", "**/*.py app_settings calls"],
        "metrics": {
            "bootstrap_key_count": len(bootstrap),
            "dynamic_key_count": len(dynamic),
            "app_config_key_count": len(static_keys),
            "standalone_setting_key_count": len(standalone_keys),
            "duplicate_definition_key_count": len(duplicate_definitions),
            "unresolved_setting_call_count": len(unresolved),
        },
        "baseline_variances": {
            "duplicate_config_definitions": duplicate_definitions,
            "environment_precedence_review": [
                {
                    "key": "tmdb_api_base_url",
                    "note": (
                        "The environment value is loaded into bootstrap memory "
                        "and then overwritten by the dynamic merge."
                    ),
                    "source": "config_manager.py:318",
                }
            ],
            "standalone_default_semantics": (
                "app_settings has no database-level value default; callers apply key-specific fallbacks."
            ),
        },
        "runtime_derived_config": [
            {
                "key": "is_pro_active",
                "storage": "process_memory",
                "sources": ["scheduler_manager.py:62", "routes/system.py:933"],
            },
            {
                "key": "pro_expire_time",
                "storage": "app_settings_and_process_memory",
                "sources": ["scheduler_manager.py:597", "routes/system.py:933"],
            },
            {
                "key": "app_base_url",
                "storage": "request_fallback_only",
                "sources": ["routes/user_management.py:124"],
            },
        ],
        "config_response_only_fields": [
            "emby_server_id",
            "custom_theme",
            "masked.pro_license_key",
            "*_auto_detected",
            "*_auto_detected_error",
            "*_effective",
        ],
        "bootstrap": bootstrap,
        "dynamic": dynamic,
        "standalone_app_settings": standalone,
        "environment_aliases": aliases,
        "unresolved_dynamic_setting_calls": unresolved,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Export legacy configuration contracts.")
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "verification/contracts/config-ownership.yaml",
    )
    args = parser.parse_args()
    payload = export_config()
    dump_yaml_compatible_json(args.output, payload)
    print(
        f"app_config={payload['metrics']['app_config_key_count']} "
        f"standalone={payload['metrics']['standalone_setting_key_count']} output={args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
