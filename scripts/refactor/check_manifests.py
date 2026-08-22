from __future__ import annotations

import argparse
import filecmp
import tempfile
from typing import Any
from pathlib import Path

from compare_runtime_routes import compare_runtime_routes
from export_manifests import export_all
from manifest_common import REPO_ROOT, load_json, load_ownership_overrides, stable_id


MANIFEST_FILES = [
    "baseline-summary.json",
    "source-inventory.json",
    "route-ownership.yaml",
    "task-ownership.yaml",
    "table-ownership.yaml",
    "config-ownership.yaml",
    "frontend-route-ownership.yaml",
    "bridge-contract-ownership.yaml",
]

CONTRACT_COLLECTIONS = {
    "route_decorators": ("route-ownership.yaml", "routes"),
    "route_bindings": ("route-ownership.yaml", "method_bindings"),
    "tasks": ("task-ownership.yaml", "tasks"),
    "tables": ("table-ownership.yaml", "tables"),
    "frontend_routes": ("frontend-route-ownership.yaml", "routes"),
    "bridge_contracts": ("bridge-contract-ownership.yaml", "contracts"),
}


def _config_contracts(directory: Path) -> dict[str, list[dict[str, Any]]]:
    payload = load_json(directory / "config-ownership.yaml")
    return {
        "configs": [*payload["bootstrap"], *payload["dynamic"]],
        "settings": payload["standalone_app_settings"],
    }


def _fixture_paths(entry: dict[str, Any]) -> list[str]:
    result: list[str] = []
    fixture = entry.get("fixture")
    if isinstance(fixture, str) and fixture:
        result.append(fixture)
    fixtures = entry.get("fixtures")
    if isinstance(fixtures, list):
        result.extend(value for value in fixtures if isinstance(value, str) and value)
    schema = entry.get("authoritative_schema_fixture")
    if isinstance(schema, str) and schema:
        result.append(schema)
    return result


def _validate_contracts(directory: Path, override_path: Path) -> list[str]:
    failures: list[str] = []
    collections: dict[str, list[dict[str, Any]]] = {}
    for kind, (manifest_name, collection_name) in CONTRACT_COLLECTIONS.items():
        payload = load_json(directory / manifest_name)
        collections[kind] = payload[collection_name]
    collections.update(_config_contracts(directory))

    known_ids = {
        kind: {entry["contract_id"] for entry in entries}
        for kind, entries in collections.items()
    }
    overrides = load_ownership_overrides(override_path)
    override_collections = overrides["contracts"]
    unknown_kinds = sorted(set(override_collections) - set(collections))
    if unknown_kinds:
        failures.append(f"unknown ownership override collections: {unknown_kinds}")
    for kind, values in override_collections.items():
        if kind not in known_ids or not isinstance(values, dict):
            continue
        unknown_ids = sorted(set(values) - known_ids[kind])
        if unknown_ids:
            failures.append(f"unknown {kind} ownership override ids: {unknown_ids}")

    allowed_statuses = {"legacy", "new", "retired-approved"}
    for kind, entries in collections.items():
        ids = [entry["contract_id"] for entry in entries]
        if len(ids) != len(set(ids)):
            failures.append(f"duplicate contract ids in {kind}")
        for entry in entries:
            contract_id = entry["contract_id"]
            status = entry.get("status")
            if status not in allowed_statuses:
                failures.append(f"{kind}:{contract_id}: invalid status {status!r}")
            if status == "new" and not entry.get("new_owner"):
                failures.append(f"{kind}:{contract_id}: new contract has no new_owner")
            if status == "retired-approved" and not entry.get("retirement_approval"):
                failures.append(
                    f"{kind}:{contract_id}: retired contract has no retirement_approval"
                )
            if not entry.get("validation_owner") or not entry.get("validation_method"):
                failures.append(f"{kind}:{contract_id}: validation ownership is incomplete")
            for fixture in _fixture_paths(entry):
                if not (REPO_ROOT / fixture).is_file():
                    failures.append(f"{kind}:{contract_id}: missing fixture {fixture}")

    route_bindings = collections["route_bindings"]
    identities = [
        (entry["application"], entry["method"], entry["path"])
        for entry in route_bindings
    ]
    if len(identities) != len(set(identities)):
        failures.append("duplicate active Method/Path route binding")
    for entry in route_bindings:
        expected_id = stable_id(
            "route-binding", entry["application"], entry["method"], entry["path"]
        )
        if entry["contract_id"] != expected_id:
            failures.append(f"unstable route binding id: {entry['contract_id']}")
        if not entry.get("fixture") and not entry.get("fixture_exception"):
            failures.append(
                f"route_bindings:{entry['contract_id']}: missing fixture or explicit exception"
            )
    return failures


def _validate_reference_artifacts(generated: Path, contract_dir: Path) -> list[str]:
    failures: list[str] = []
    required = [
        contract_dir / "db/legacy-schema.sql",
        contract_dir / "db/legacy-pg-catalog.json",
        contract_dir / "http/legacy-runtime-url-map.json",
        contract_dir / "http/legacy-runtime-routes.json",
        contract_dir / "http/legacy-proxy-runtime-url-map.json",
        contract_dir / "http/legacy-proxy-runtime-routes.json",
        contract_dir / "http/legacy-local-health.json",
        contract_dir / "http/static-vs-runtime-route-diff.json",
        contract_dir / "legacy-reference-summary.json",
        contract_dir / "manual-live-acceptance.md",
    ]
    for path in required:
        if not path.is_file():
            failures.append(f"missing disposable-reference artifact: {path}")
    if failures:
        return failures

    catalog = load_json(contract_dir / "db/legacy-pg-catalog.json")
    table_manifest = load_json(generated / "table-ownership.yaml")
    static_tables = {entry["name"] for entry in table_manifest["tables"]}
    runtime_tables = {entry["name"] for entry in catalog["tables"]}
    if runtime_tables != static_tables:
        failures.append(
            f"static/runtime table mismatch: static_only={sorted(static_tables - runtime_tables)} "
            f"runtime_only={sorted(runtime_tables - static_tables)}"
        )
    if catalog.get("server_version_num") != "160014":
        failures.append("legacy reference PostgreSQL version drifted from recorded 16.14 baseline")
    if len(catalog.get("columns", [])) != 478:
        failures.append("legacy reference column count drifted from 478")
    if len(catalog.get("indexes", [])) != 120:
        failures.append("legacy reference index count drifted from 120")
    if len(catalog.get("constraints", [])) != 60:
        failures.append("legacy reference constraint count drifted from 60")

    actual_diff = compare_runtime_routes(
        generated / "route-ownership.yaml",
        contract_dir / "http/legacy-runtime-routes.json",
        contract_dir / "http/legacy-proxy-runtime-routes.json",
    )
    expected_diff = load_json(contract_dir / "http/static-vs-runtime-route-diff.json")
    if actual_diff != expected_diff:
        failures.append("stale static/runtime route difference artifact")
    return failures


def check_manifests(contract_dir: Path) -> list[str]:
    failures: list[str] = []
    with tempfile.TemporaryDirectory(prefix="etk-p0-manifests-") as temporary:
        generated = Path(temporary)
        export_all(generated)
        for name in MANIFEST_FILES:
            expected = contract_dir / name
            actual = generated / name
            if not expected.exists():
                failures.append(f"missing committed manifest: {expected}")
            elif not filecmp.cmp(expected, actual, shallow=False):
                failures.append(f"stale committed manifest: {expected}")
        override_path = contract_dir / "ownership-overrides.json"
        if not override_path.is_file():
            failures.append(f"missing ownership overlay: {override_path}")
        else:
            failures.extend(_validate_contracts(generated, override_path))
        failures.extend(_validate_reference_artifacts(generated, contract_dir))
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description="Regenerate and compare P0 manifests.")
    parser.add_argument(
        "--contract-dir",
        type=Path,
        default=REPO_ROOT / "verification/contracts",
    )
    args = parser.parse_args()
    failures = check_manifests(args.contract_dir)
    if failures:
        print("\n".join(failures))
        return 1
    print(f"manifest_check=clean path={args.contract_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
