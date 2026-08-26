from __future__ import annotations

import argparse
import importlib
import os
from pathlib import Path
from typing import Any

from manifest_common import REPO_ROOT, dump_json


def export_runtime_routes(module_name: str, attribute: str) -> dict[str, Any]:
    if os.environ.get("ETK_ALLOW_ISOLATED_LEGACY_REFERENCE") != "1":
        raise RuntimeError("set ETK_ALLOW_ISOLATED_LEGACY_REFERENCE=1 only inside a disposable isolated container")
    module = importlib.import_module(module_name)
    application = getattr(module, attribute)
    rules: list[dict[str, Any]] = []
    for rule in application.url_map.iter_rules():
        methods_raw = sorted(rule.methods or [])
        methods_contract = [method for method in methods_raw if method not in {"HEAD", "OPTIONS"}]
        rules.append(
            {
                "endpoint": rule.endpoint,
                "path": rule.rule,
                "methods_raw": methods_raw,
                "methods_contract": methods_contract,
            }
        )
    rules.sort(key=lambda item: (item["path"], item["endpoint"], item["methods_raw"]))
    return {
        "rule_count": len(rules),
        "raw_method_binding_count": sum(len(item["methods_raw"]) for item in rules),
        "contract_method_binding_count": sum(len(item["methods_contract"]) for item in rules),
        "rules": rules,
    }


def normalized_routes(payload: dict[str, Any]) -> dict[str, Any]:
    rules = [
        {
            "endpoint": item["endpoint"],
            "path": item["path"],
            "methods": item["methods_contract"],
        }
        for item in payload["rules"]
    ]
    return {
        "rule_count": len(rules),
        "method_binding_count": sum(len(item["methods"]) for item in rules),
        "rules": rules,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export Flask runtime routes from a disposable isolated reference container."
    )
    parser.add_argument("--module", default="web_app")
    parser.add_argument("--attribute", default="app")
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "verification/contracts/http/legacy-runtime-url-map.json",
    )
    parser.add_argument(
        "--normalized-output",
        type=Path,
        default=REPO_ROOT / "verification/contracts/http/legacy-runtime-routes.json",
    )
    args = parser.parse_args()
    payload = export_runtime_routes(args.module, args.attribute)
    dump_json(args.output, payload)
    dump_json(args.normalized_output, normalized_routes(payload))
    print(
        f"runtime_rules={payload['rule_count']} bindings={payload['contract_method_binding_count']} "
        f"output={args.output} normalized={args.normalized_output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
