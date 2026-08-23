from __future__ import annotations

import argparse
import ast
from pathlib import Path
from typing import Any

from manifest_common import (
    REPO_ROOT,
    ManifestError,
    apply_contract_override,
    dump_yaml_compatible_json,
    evaluate_static,
    parse_python,
    relative_path,
    source_ref,
    stable_id,
)

ROUTE_OWNERS: dict[str, tuple[list[str], str]] = {
    "routes/unified_auth.py": (["identity"], "P3"),
    "routes/system.py": (["system", "operations"], "P3/P4"),
    "routes/logs.py": (["operations"], "P3"),
    "routes/database_admin.py": (["operations"], "P3/P11"),
    "routes/tasks.py": (["operations"], "P3/P4"),
    "routes/actions.py": (["media", "operations"], "P5/P8"),
    "routes/webhook.py": (["ingestion", "subscription", "sharing"], "P5/P7/P9"),
    "routes/p115.py": (["p115"], "P6"),
    "routes/watchlist.py": (["subscription"], "P7"),
    "routes/actor_subscriptions.py": (["people", "subscription"], "P7"),
    "routes/subscription.py": (["subscription", "sharing"], "P7/P9"),
    "routes/resubscribe.py": (["subscription"], "P7"),
    "routes/media_cleanup.py": (["subscription", "operations"], "P7"),
    "routes/tmdb_collections.py": (["collection"], "P8"),
    "routes/custom_collections.py": (["collection", "sharing"], "P8/P9"),
    "routes/cover_generator_config.py": (["collection"], "P8"),
    "routes/user_management.py": (["users", "identity"], "P8"),
    "routes/user_portal.py": (["users"], "P8"),
    "routes/discover.py": (["media", "users"], "P8"),
    "routes/shared_resource.py": (["sharing"], "P9"),
    "reverse_proxy.py": (["sharing", "users"], "P9"),
}


MEDIA_ROUTE_OWNERS: dict[tuple[str, str], tuple[list[str], str, str]] = {
    ("GET", "/api/search_emby_library"): (["operations"], "P8", "operations/review"),
    ("GET", "/api/media_for_editing/<item_id>"): (["media"], "P5", "media/cast-editor"),
    ("POST", "/api/update_media_cast_sa/<item_id>"): (["media"], "P5", "media/cast-editor"),
    # P5 owns the public binding because `cache=` is the media image-store contract.
    # The legacy `url=` mode remains a separate egress-policy migration concern.
    ("GET", "/api/image_proxy"): (["media"], "P5", "media/image-delivery"),
    ("GET", "/image_proxy/<path:image_path>"): (["media"], "P5", "media/emby-image-delivery"),
    ("POST", "/api/actions/translate_cast_sa"): (["media"], "P5", "media/cast-editor"),
    ("POST", "/api/preview_processed_cast/<item_id>"): (["media"], "P5", "media/cast-editor"),
    ("GET", "/api/emby_libraries"): (["media"], "P8", "media/library-query"),
    ("GET", "/api/emby/user/<user_id>/views"): (["users"], "P8", "users/integration-settings"),
    ("GET", "/api/search_studios"): (["collection"], "P8", "collection/custom-collection"),
    ("POST", "/api/subscription/status"): (["subscription"], "P7", "subscription/state"),
    ("GET", "/api/subscriptions/all"): (["subscription"], "P7", "subscription/management"),
    ("POST", "/api/media/batch_delete"): (["subscription"], "P7", "subscription/management"),
    ("GET", "/api/subscription/strategy"): (["subscription"], "P7", "subscription/strategy"),
    ("POST", "/api/subscription/strategy"): (["subscription"], "P7", "subscription/strategy"),
    ("GET", "/api/auto_tagging/rules"): (["media"], "P8", "media/auto-tagging"),
    ("POST", "/api/auto_tagging/rules"): (["media"], "P8", "media/auto-tagging"),
    ("POST", "/api/auto_tagging/run_now"): (["media"], "P8", "media/auto-tagging"),
    ("POST", "/api/auto_tagging/clear_now"): (["media"], "P8", "media/auto-tagging"),
    ("POST", "/api/update_media_image/<item_id>"): (["media"], "P5", "media/image-editor"),
    ("GET", "/api/tmdb_images/<item_id>"): (["media"], "P5", "media/image-editor"),
    ("GET", "/api/media_info/edit/<item_id>"): (["media"], "P5", "media/mediainfo-editor"),
    ("GET", "/api/media_info/chapters/<item_id>"): (["media"], "P5", "media/mediainfo-chapters"),
    ("POST", "/api/media_info/chapters/<item_id>"): (["media"], "P5", "media/mediainfo-chapters"),
    ("POST", "/api/media_info/edit/<item_id>"): (["media"], "P5", "media/mediainfo-editor"),
    ("GET", "/api/media_info/series/<series_id>/episodes"): (["media"], "P5", "media/mediainfo-editor"),
}

P5_EMBY_ROUTE_PATHS = frozenset(
    {
        "/api/emby/plugin-update",
        "/api/emby/metadata",
        "/api/emby/metadata/backfill",
        "/api/emby/metadata/backfill/item",
        "/api/emby/metadata/backfill/items",
        "/api/emby/metadata/backfill/library",
        "/api/emby/metadata/images/search",
        "/api/emby/metadata/images/refresh",
        "/api/emby/metadata/images/sync",
        "/api/emby/events",
    }
)


HTTP_DECORATORS = {"route", "get", "post", "put", "delete", "patch", "websocket"}


def _join_url(prefix: str, path: str) -> str:
    if not prefix:
        return path or "/"
    if not path:
        return prefix or "/"
    return f"{prefix.rstrip('/')}/{path.lstrip('/')}"


def _call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _fixture_for_route(source_path: str, route_path: str) -> tuple[str | None, str | None]:
    if source_path == "reverse_proxy.py":
        return "verification/fixtures/http/reverse-proxy-scenarios.json", None
    if route_path == "/api/emby/metadata":
        return "verification/fixtures/http/bridge-metadata.json", None
    if route_path.startswith("/api/emby/"):
        return "verification/fixtures/http/bridge-validation-errors.json", None
    if route_path.startswith("/api/p115/mediainfo"):
        return "verification/fixtures/http/bridge-mediainfo.json", None
    return (
        None,
        "P0 freezes route identity statically; behavioral fixtures are assigned during the owning migration stage.",
    )


def _route_ownership(
    source_path: str,
    route_path: str,
    methods: list[str],
) -> tuple[list[str], str, str]:
    if source_path == "routes/webhook.py" and route_path in P5_EMBY_ROUTE_PATHS:
        if route_path == "/api/emby/events":
            return ["media", "subscription", "sharing"], "P5/P7/P9", "media/bridge-transport"
        return ["media"], "P5", "media/bridge"
    if source_path != "routes/media.py":
        domains, stage = ROUTE_OWNERS.get(source_path, (["unassigned"], "UNASSIGNED"))
        return domains, stage, "/".join(domains)

    ownerships: list[tuple[list[str], str, str]] = []
    for method in methods:
        ownership = MEDIA_ROUTE_OWNERS.get((method, route_path))
        if ownership is None:
            raise ManifestError(f"media route owner mapping missing for: {method} {route_path}")
        ownerships.append(ownership)

    first = ownerships[0]
    if any(ownership != first for ownership in ownerships[1:]):
        identities = ", ".join(f"{method} {route_path}" for method in methods)
        raise ManifestError(f"route decorator spans multiple method-level owners: {identities}")
    return first


def _blueprints(path: Path, tree: ast.Module) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        call = node.value
        if not isinstance(target, ast.Name) or not isinstance(call, ast.Call):
            continue
        if _call_name(call.func) != "Blueprint":
            continue
        if not call.args:
            raise ManifestError(f"Blueprint without name at {source_ref(path, node.lineno)}")
        blueprint_name = evaluate_static(call.args[0])
        prefix = ""
        for keyword in call.keywords:
            if keyword.arg == "url_prefix":
                prefix = str(evaluate_static(keyword.value))
        result.append(
            {
                "variable": target.id,
                "name": str(blueprint_name),
                "url_prefix": prefix,
                "source": source_ref(path, node.lineno),
            }
        )
    return sorted(result, key=lambda item: (item["source"], item["variable"]))


def _decorated_functions(tree: ast.Module) -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    return sorted(
        (
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.decorator_list
        ),
        key=lambda node: node.lineno,
    )


def export_routes() -> dict[str, Any]:
    paths = sorted((REPO_ROOT / "routes").glob("*.py")) + [REPO_ROOT / "reverse_proxy.py"]
    blueprints: list[dict[str, Any]] = []
    routes: list[dict[str, Any]] = []
    for path in paths:
        if path.name == "__init__.py":
            continue
        _, tree = parse_python(path)
        file_blueprints = _blueprints(path, tree)
        blueprints.extend(file_blueprints)
        prefix_by_variable = {item["variable"]: item["url_prefix"] for item in file_blueprints}
        rel = relative_path(path)
        application = "proxy" if rel == "reverse_proxy.py" else "api"
        for function in _decorated_functions(tree):
            for decorator in function.decorator_list:
                if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Attribute):
                    continue
                kind = decorator.func.attr
                if kind not in HTTP_DECORATORS:
                    continue
                if not isinstance(decorator.func.value, ast.Name):
                    raise ManifestError(f"unsupported route receiver at {source_ref(path, decorator.lineno)}")
                receiver = decorator.func.value.id
                if not decorator.args:
                    raise ManifestError(f"route without path at {source_ref(path, decorator.lineno)}")
                route_path = str(evaluate_static(decorator.args[0]))
                if kind == "route":
                    methods = ["GET"]
                    for keyword in decorator.keywords:
                        if keyword.arg == "methods":
                            methods = [str(value).upper() for value in evaluate_static(keyword.value)]
                else:
                    methods = ["WEBSOCKET" if kind == "websocket" else kind.upper()]
                full_path = _join_url(prefix_by_variable.get(receiver, ""), route_path)
                domains, stage, validation_owner = _route_ownership(rel, full_path, methods)
                fixture, fixture_exception = _fixture_for_route(rel, full_path)
                routes.append(
                    apply_contract_override(
                        {
                            "contract_id": stable_id(
                                "route-decorator",
                                rel,
                                receiver,
                                route_path,
                                ",".join(sorted(methods)),
                            ),
                            "source": source_ref(path, decorator.lineno),
                            "application": application,
                            "blueprint": receiver,
                            "endpoint": function.name,
                            "declared_path": route_path,
                            "path": full_path,
                            "methods": methods,
                            "target_domains": domains,
                            "target_stage": stage,
                            "status": "legacy",
                            "new_owner": None,
                            "retirement_approval": None,
                            "fixture": fixture,
                            "fixture_exception": fixture_exception,
                            "validation_owner": validation_owner,
                            "validation_method": "static-route-contract",
                        },
                        "route_decorators",
                    )
                )
    routes.sort(key=lambda item: (item["path"], item["methods"], item["source"]))
    unassigned = sorted({item["source"].split(":", 1)[0] for item in routes if item["target_stage"] == "UNASSIGNED"})
    if unassigned:
        raise ManifestError(f"route owner mapping missing for: {', '.join(unassigned)}")
    return {
        "schema_version": 1,
        "generator": "scripts/refactor/export_routes.py",
        "source_scope": ["routes/**/*.py", "reverse_proxy.py"],
        "metrics": {
            "blueprint_count": len(blueprints),
            "route_decorator_count": len(routes),
            "http_method_binding_count": sum(len(item["methods"]) for item in routes),
        },
        "blueprints": blueprints,
        "routes": routes,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Export the legacy Flask route ownership manifest.")
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "verification/contracts/route-ownership.yaml",
    )
    args = parser.parse_args()
    payload = export_routes()
    dump_yaml_compatible_json(args.output, payload)
    print(
        f"blueprints={payload['metrics']['blueprint_count']} "
        f"routes={payload['metrics']['route_decorator_count']} output={args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
