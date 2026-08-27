from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

from manifest_common import (
    REPO_ROOT,
    apply_contract_override,
    dump_yaml_compatible_json,
    relative_path,
    stable_id,
)


def _scan_balanced(text: str, start: int, opening: str, closing: str) -> int:
    depth = 0
    quote: str | None = None
    escaped = False
    line_comment = False
    block_comment = False
    index = start
    while index < len(text):
        character = text[index]
        following = text[index + 1] if index + 1 < len(text) else ""
        if line_comment:
            if character == "\n":
                line_comment = False
            index += 1
            continue
        if block_comment:
            if character == "*" and following == "/":
                block_comment = False
                index += 2
            else:
                index += 1
            continue
        if quote:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                quote = None
            index += 1
            continue
        if character == "/" and following == "/":
            line_comment = True
            index += 2
            continue
        if character == "/" and following == "*":
            block_comment = True
            index += 2
            continue
        if character in {"'", '"', "`"}:
            quote = character
        elif character == opening:
            depth += 1
        elif character == closing:
            depth -= 1
            if depth == 0:
                return index
        index += 1
    raise ValueError(f"unbalanced {opening}{closing} block")


def _route_objects(text: str) -> list[tuple[int, str]]:
    declaration = re.search(r"\bconst\s+routes\s*=\s*\[", text)
    if not declaration:
        raise ValueError("const routes array not found")
    array_start = text.find("[", declaration.start())
    array_end = _scan_balanced(text, array_start, "[", "]")
    result: list[tuple[int, str]] = []
    index = array_start + 1
    while index < array_end:
        if text[index] == "{":
            end = _scan_balanced(text, index, "{", "}")
            result.append((index, text[index : end + 1]))
            index = end + 1
        else:
            index += 1
    return result


def _string_property(block: str, name: str) -> str | None:
    match = re.search(rf"\b{name}\s*:\s*(['\"])(.*?)\1", block, re.DOTALL)
    return match.group(2) if match else None


def _boolean_property(block: str, name: str) -> bool | None:
    match = re.search(rf"\b{name}\s*:\s*(true|false)\b", block)
    return match.group(1) == "true" if match else None


def _component(block: str) -> tuple[str | None, str | None]:
    lazy = re.search(r"\bcomponent\s*:\s*\(\s*\)\s*=>\s*import\(\s*(['\"])(.*?)\1\s*\)", block)
    if lazy:
        return None, lazy.group(2)
    direct = re.search(r"\bcomponent\s*:\s*([A-Za-z_$][A-Za-z0-9_$]*)", block)
    return (direct.group(1), None) if direct else (None, None)


def export_frontend_routes() -> dict[str, Any]:
    router_candidates = (
        REPO_ROOT / "frontend/src/router/index.ts",
        REPO_ROOT / "frontend/src/router/index.js",
    )
    path = next((candidate for candidate in router_candidates if candidate.exists()), router_candidates[0])
    text = path.read_text(encoding="utf-8-sig")
    routes: list[dict[str, Any]] = []
    for offset, block in _route_objects(text):
        route_path = _string_property(block, "path")
        if route_path is None:
            raise ValueError(f"non-static frontend route at {relative_path(path)}:{text.count(chr(10), 0, offset) + 1}")
        name = _string_property(block, "name")
        redirect = _string_property(block, "redirect")
        component, lazy_component = _component(block)
        if not redirect and not component and not lazy_component:
            raise ValueError(f"route {route_path} has no static component or redirect")
        line = text.count("\n", 0, offset) + 1
        routes.append(
            apply_contract_override(
                {
                    "contract_id": stable_id("frontend-route", route_path),
                    "path": route_path,
                    "name": name,
                    "redirect": redirect,
                    "component": component,
                    "lazy_component": lazy_component,
                    "props": _boolean_property(block, "props"),
                    "meta": {
                        "public": _boolean_property(block, "public"),
                        "requires_auth": _boolean_property(block, "requiresAuth"),
                        "requires_admin": _boolean_property(block, "requiresAdmin"),
                    },
                    "guard_enforcement": {
                        "public": "enforced",
                        "requires_auth": "implicit_for_non_public_routes",
                        "requires_admin": "not_enforced_by_router_guard",
                    },
                    "source": f"{relative_path(path)}:{line}",
                    "target_domain": "frontend",
                    "target_stage": "P10",
                    "status": "legacy",
                    "new_owner": None,
                    "validation_owner": "frontend",
                    "validation_method": "frontend-route-contract",
                },
                "frontend_routes",
            )
        )
    routes.sort(key=lambda item: item["path"])
    vue_files = sorted(relative_path(item) for item in (REPO_ROOT / "frontend/src").rglob("*.vue"))
    page_routes = [item for item in routes if not item["redirect"]]
    return {
        "schema_version": 1,
        "generator": "scripts/refactor/export_frontend_routes.py",
        "source_scope": [relative_path(path), "frontend/src/**/*.vue"],
        "router_source": relative_path(path),
        "metrics": {
            "vue_file_count": len(vue_files),
            "route_entry_count": len(routes),
            "page_route_count": len(page_routes),
            "redirect_route_count": sum(1 for item in routes if item["redirect"]),
            "public_route_count": sum(1 for item in page_routes if item["meta"]["public"] is True),
            "requires_auth_route_count": sum(1 for item in page_routes if item["meta"]["requires_auth"] is True),
            "declared_admin_route_count": sum(1 for item in page_routes if item["meta"]["requires_admin"] is True),
        },
        "baseline_variances": {
            "plan_page_route_count": 23,
            "source_page_route_count": len(page_routes),
            "resolution": "Preserve every source route; no route is retired without explicit approval.",
            "router_guard_requires_admin": "declared but not enforced",
        },
        "vue_files": vue_files,
        "routes": routes,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Export frontend route ownership data.")
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "verification/contracts/frontend-route-ownership.yaml",
    )
    args = parser.parse_args()
    payload = export_frontend_routes()
    dump_yaml_compatible_json(args.output, payload)
    print(
        f"vue={payload['metrics']['vue_file_count']} "
        f"pages={payload['metrics']['page_route_count']} output={args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
