from __future__ import annotations

import ast
import hashlib
import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
OWNERSHIP_OVERRIDES_PATH = REPO_ROOT / "verification/contracts/ownership-overrides.json"

CONTRACT_OVERRIDE_FIELDS = {
    "status",
    "new_owner",
    "retirement_approval",
    "fixture",
    "fixtures",
    "fixture_exception",
    "validation_owner",
    "validation_method",
    "notes",
}


class ManifestError(RuntimeError):
    """Raised when a source construct cannot be represented deterministically."""


def parse_python(path: Path) -> tuple[str, ast.Module]:
    source = path.read_text(encoding="utf-8-sig")
    return source, ast.parse(source, filename=str(path))


def relative_path(path: Path) -> str:
    return path.resolve().relative_to(REPO_ROOT).as_posix()


def stable_id(prefix: str, *parts: object) -> str:
    normalized = "\x1f".join(str(part) for part in parts)
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}-{digest}"


def dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def dump_yaml_compatible_json(path: Path, payload: Any) -> None:
    """JSON is a YAML 1.2 subset and avoids a bootstrap-time YAML dependency."""
    dump_json(path, payload)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_ownership_overrides(path: Path = OWNERSHIP_OVERRIDES_PATH) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": 1, "contracts": {}}
    payload = load_json(path)
    if payload.get("schema_version") != 1 or not isinstance(payload.get("contracts"), dict):
        raise ManifestError(f"invalid ownership override file: {path}")
    return payload


def apply_contract_override(
    entry: dict[str, Any],
    contract_kind: str,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = overrides or load_ownership_overrides()
    kind_overrides = payload.get("contracts", {}).get(contract_kind, {})
    if not isinstance(kind_overrides, dict):
        raise ManifestError(f"ownership overrides for {contract_kind} must be an object")
    override = kind_overrides.get(entry["contract_id"], {})
    if not isinstance(override, dict):
        raise ManifestError(f"ownership override for {entry['contract_id']} must be an object")
    unsupported = sorted(set(override) - CONTRACT_OVERRIDE_FIELDS)
    if unsupported:
        raise ManifestError(f"unsupported ownership override fields for {entry['contract_id']}: {unsupported}")
    return {**entry, **override}


def source_ref(path: Path, line: int) -> str:
    return f"{relative_path(path)}:{line}"


def evaluate_static(node: ast.AST, symbols: dict[str, Any] | None = None) -> Any:
    symbols = symbols or {}
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        if node.id in symbols:
            return symbols[node.id]
        raise ManifestError(f"unresolved name: {node.id}")
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
        key = f"{node.value.id}.{node.attr}"
        if key in symbols:
            return symbols[key]
        if node.attr in symbols:
            return symbols[node.attr]
        raise ManifestError(f"unresolved attribute: {key}")
    if isinstance(node, ast.List):
        return [evaluate_static(item, symbols) for item in node.elts]
    if isinstance(node, ast.Tuple):
        return tuple(evaluate_static(item, symbols) for item in node.elts)
    if isinstance(node, ast.Set):
        return {evaluate_static(item, symbols) for item in node.elts}
    if isinstance(node, ast.Dict):
        return {
            evaluate_static(key, symbols): evaluate_static(value, symbols)
            # ``zip(strict=...)`` is unavailable on the system Python 3.9
            # used by the local contract tooling; AST dictionaries always
            # expose parallel key/value arrays.
            for key, value in zip(node.keys, node.values)  # noqa: B905
            if key is not None
        }
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        return -evaluate_static(node.operand, symbols)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return evaluate_static(node.left, symbols) + evaluate_static(node.right, symbols)
    raise ManifestError(f"unsupported static expression: {ast.dump(node, include_attributes=False)}")


def load_constant_symbols(path: Path, module_alias: str | None = None) -> dict[str, Any]:
    _, tree = parse_python(path)
    symbols: dict[str, Any] = {}
    changed = True
    assignments = [node for node in tree.body if isinstance(node, (ast.Assign, ast.AnnAssign))]
    while changed:
        changed = False
        for node in assignments:
            target = node.targets[0] if isinstance(node, ast.Assign) else node.target
            value = node.value
            if not isinstance(target, ast.Name) or value is None or target.id in symbols:
                continue
            try:
                resolved = evaluate_static(value, symbols)
            except ManifestError:
                continue
            symbols[target.id] = resolved
            if module_alias:
                symbols[f"{module_alias}.{target.id}"] = resolved
            changed = True
    return symbols


def iter_project_python_files() -> Iterable[Path]:
    excluded = {".git", ".venv", "node_modules", "frontend", "local_data"}
    for path in sorted(REPO_ROOT.rglob("*.py")):
        relative = path.relative_to(REPO_ROOT)
        if relative == Path("web_app.py") or any(part in excluded for part in relative.parts):
            continue
        yield path
