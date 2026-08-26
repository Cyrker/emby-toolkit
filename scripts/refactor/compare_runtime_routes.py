from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from manifest_common import REPO_ROOT, dump_json, load_json


def _items(identities: set[tuple[str, str, str]]) -> list[dict[str, str]]:
    return [
        {"application": application, "method": method, "path": path}
        for application, method, path in sorted(identities, key=lambda item: (item[0], item[2], item[1]))
    ]


def compare_runtime_routes(
    static_manifest: Path,
    api_runtime_manifest: Path,
    proxy_runtime_manifest: Path,
) -> dict[str, Any]:
    static = load_json(static_manifest)
    api_runtime = load_json(api_runtime_manifest)
    proxy_runtime = load_json(proxy_runtime_manifest)
    static_unfiltered = {(entry["application"], entry["method"], entry["path"]) for entry in static["method_bindings"]}
    static_normalized = {identity for identity in static_unfiltered if identity[1] not in {"HEAD", "OPTIONS"}}
    runtime_normalized: set[tuple[str, str, str]] = set()
    for application, runtime in (("api", api_runtime), ("proxy", proxy_runtime)):
        runtime_normalized.update(
            (application, method, entry["path"])
            for entry in runtime["rules"]
            for method in entry["methods"]
            if method not in {"HEAD", "OPTIONS"}
        )
    runtime_only = runtime_normalized - static_normalized
    static_only = static_normalized - runtime_normalized
    static_only_unfiltered = static_unfiltered - runtime_normalized
    method_noise = static_only_unfiltered - static_only
    return {
        "comparison_key": "application+method+path",
        "normalization": "HEAD and OPTIONS removed from both sets to avoid Flask implicit-method noise",
        "counts": {
            "static_decorators": static["metrics"]["route_decorator_count"],
            "static_method_bindings_unfiltered": len(static_unfiltered),
            "static_method_bindings_normalized": len(static_normalized),
            "runtime_api_rules": api_runtime["rule_count"],
            "runtime_api_method_bindings_contract": api_runtime["method_binding_count"],
            "runtime_proxy_rules": proxy_runtime["rule_count"],
            "runtime_proxy_method_bindings_contract": proxy_runtime["method_binding_count"],
            "runtime_method_bindings_normalized": len(runtime_normalized),
        },
        "runtime_only_normalized": _items(runtime_only),
        "static_only_normalized": _items(static_only),
        "static_only_unfiltered": _items(static_only_unfiltered),
        "classification": {
            "runtime_only": {
                "items": _items(runtime_only),
                "likely_reason": (
                    "Created by the PyArmor/bootstrap Flask app rather than decorators in routes/*.py "
                    "or by Flask's implicit static routing. The API /api/health and SPA fallbacks are "
                    "bootstrap-defined; both Flask applications add their own /static route."
                ),
            },
            "static_only": {
                "items": _items(static_only),
                "likely_reason": "No normalized static-only contract remains after both Flask apps are exported.",
            },
            "method_normalization_artifacts": {
                "items": _items(method_noise),
                "likely_reason": (
                    "The runtime contract export removes Flask implicit HEAD/OPTIONS; these do not "
                    "establish missing HTTP behavior."
                ),
            },
            "application_identity_note": (
                "The API SPA and Proxy applications both own GET / and GET /<path:path> on distinct "
                "process roles. Contract identity includes application so this intentional overlap is "
                "not mistaken for one implementation."
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare static and disposable-runtime route contracts.")
    parser.add_argument(
        "--static",
        type=Path,
        default=REPO_ROOT / "verification/contracts/route-ownership.yaml",
    )
    parser.add_argument(
        "--runtime-api",
        type=Path,
        default=REPO_ROOT / "verification/contracts/http/legacy-runtime-routes.json",
    )
    parser.add_argument(
        "--runtime-proxy",
        type=Path,
        default=REPO_ROOT / "verification/contracts/http/legacy-proxy-runtime-routes.json",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "verification/contracts/http/static-vs-runtime-route-diff.json",
    )
    args = parser.parse_args()
    payload = compare_runtime_routes(args.static, args.runtime_api, args.runtime_proxy)
    dump_json(args.output, payload)
    print(
        f"runtime_only={len(payload['runtime_only_normalized'])} "
        f"static_only={len(payload['static_only_normalized'])} output={args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
