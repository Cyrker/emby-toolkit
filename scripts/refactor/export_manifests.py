from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
from typing import Any

from export_bridge_contracts import export_bridge_contracts
from export_config import export_config
from export_frontend_routes import export_frontend_routes
from export_routes import export_routes
from export_tables import export_tables
from export_tasks import export_tasks
from manifest_common import (
    REPO_ROOT,
    apply_contract_override,
    dump_json,
    dump_yaml_compatible_json,
    relative_path,
    stable_id,
)

EXPECTED = {
    "legacy_python_files": 126,
    "vue_files": 55,
    "blueprints": 22,
    "route_decorators": 343,
    "method_bindings": 367,
    "task_keys": 50,
    "tables": 39,
}


def _legacy_python_files() -> list[str]:
    excluded = {
        ".git",
        ".venv",
        "frontend",
        "local_data",
        "migrations",
        "node_modules",
        "scripts",
        "src",
        "verification",
    }
    return sorted(
        relative_path(path)
        for path in REPO_ROOT.rglob("*.py")
        if path.relative_to(REPO_ROOT) != Path("main.py")
        and not any(part in excluded for part in path.relative_to(REPO_ROOT).parts)
    )


def _source_digest(paths: list[str]) -> str:
    digest = hashlib.sha256()
    for relative in sorted(paths):
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update((REPO_ROOT / relative).read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _method_bindings(route_payload: dict[str, Any]) -> list[dict[str, Any]]:
    bindings: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for route in route_payload["routes"]:
        for method in route["methods"]:
            identity = (route["application"], method, route["path"])
            if identity in seen:
                raise ValueError(
                    f"duplicate active application/method/path binding: "
                    f"{route['application']} {method} {route['path']}"
                )
            seen.add(identity)
            bindings.append(
                apply_contract_override(
                {
                    "contract_id": stable_id(
                        "route-binding", route["application"], method, route["path"]
                    ),
                    "method": method,
                    "path": route["path"],
                    "application": route["application"],
                    "decorator_contract_id": route["contract_id"],
                    "source": route["source"],
                    "endpoint": route["endpoint"],
                    "target_domains": route["target_domains"],
                    "target_stage": route["target_stage"],
                    "status": route["status"],
                    "new_owner": route["new_owner"],
                    "retirement_approval": route["retirement_approval"],
                    "fixture": route["fixture"],
                    "fixture_exception": route["fixture_exception"],
                    "validation_owner": route["validation_owner"],
                    "validation_method": route["validation_method"],
                },
                "route_bindings",
                )
            )
    return sorted(
        bindings,
        key=lambda item: (item["application"], item["path"], item["method"]),
    )


def export_all(output_dir: Path, verify_counts: bool = True) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    routes = export_routes()
    routes["method_bindings"] = _method_bindings(routes)
    tasks = export_tasks()
    tables = export_tables()
    config = export_config()
    frontend = export_frontend_routes()
    bridge = export_bridge_contracts()
    python_files = _legacy_python_files()
    actual = {
        "legacy_python_files": len(python_files),
        "vue_files": frontend["metrics"]["vue_file_count"],
        "blueprints": routes["metrics"]["blueprint_count"],
        "route_decorators": routes["metrics"]["route_decorator_count"],
        "method_bindings": len(routes["method_bindings"]),
        "task_keys": tasks["metrics"]["task_key_count"],
        "tables": tables["metrics"]["table_count"],
    }
    mismatches = {
        key: {"expected": EXPECTED[key], "actual": actual[key]}
        for key in EXPECTED
        if actual[key] != EXPECTED[key]
    }
    if verify_counts and mismatches:
        raise ValueError(f"baseline count mismatch: {mismatches}")

    dump_yaml_compatible_json(output_dir / "route-ownership.yaml", routes)
    dump_yaml_compatible_json(output_dir / "task-ownership.yaml", tasks)
    dump_yaml_compatible_json(output_dir / "table-ownership.yaml", tables)
    dump_yaml_compatible_json(output_dir / "config-ownership.yaml", config)
    dump_yaml_compatible_json(output_dir / "frontend-route-ownership.yaml", frontend)
    dump_yaml_compatible_json(output_dir / "bridge-contract-ownership.yaml", bridge)
    inventory = {
        "schema_version": 1,
        "generator": "scripts/refactor/export_manifests.py",
        "metrics": actual,
        "expected": EXPECTED,
        "mismatches": mismatches,
        "legacy_source_sha256": _source_digest(
            [*python_files, *frontend["vue_files"], "frontend/src/router/index.js"]
        ),
        "legacy_python_files": python_files,
        "vue_files": frontend["vue_files"],
    }
    dump_json(output_dir / "source-inventory.json", inventory)
    summary = {
        "schema_version": 1,
        "baseline_revision": "main@00786dcd632c08a25016276d2531d650ab9ee00c",
        "legacy_source_sha256": inventory["legacy_source_sha256"],
        "generated_by": "scripts/refactor/export_manifests.py",
        "counts": actual,
        "expected_counts": EXPECTED,
        "count_mismatches": mismatches,
        "baseline_variances": {
            "frontend_page_routes": frontend["baseline_variances"],
            "task_help": tasks["baseline_variances"],
            "config": config["baseline_variances"],
        },
    }
    dump_json(output_dir / "baseline-summary.json", summary)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Export all P0 static manifests.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=REPO_ROOT / "verification/contracts",
    )
    parser.add_argument("--allow-count-drift", action="store_true")
    args = parser.parse_args()
    summary = export_all(args.output_dir, verify_counts=not args.allow_count_drift)
    counts = " ".join(f"{key}={value}" for key, value in summary["counts"].items())
    print(f"{counts} output={args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
