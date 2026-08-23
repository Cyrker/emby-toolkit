from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from etk.bootstrap.app_factory import create_app  # noqa: E402

DEFAULT_OUTPUT = REPO_ROOT / "verification" / "contracts" / "http" / "p4-openapi.json"
EXPECTED_PATHS = 42
EXPECTED_OPERATIONS = 47
BODY_METHODS = {
    ("post", "/api/ai/models"),
    ("post", "/api/ai/prompts"),
    ("post", "/api/ai/test"),
    ("post", "/api/auth/login"),
    ("post", "/api/auth/reauthorize"),
    ("post", "/api/auth/setup"),
    ("post", "/api/auth/verify_recovery"),
    ("post", "/api/config"),
    ("post", "/api/config/custom_theme"),
    ("post", "/api/proxy/test"),
    ("post", "/api/system/activate_pro"),
    ("post", "/api/system/menu_config"),
    ("post", "/api/system/transfer_pro"),
    ("post", "/api/telegram/templates"),
    ("post", "/api/telegram/test"),
    ("post", "/api/tasks/run"),
}


def build_openapi() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="etk-p4-openapi-") as temporary:
        app = create_app(
            start_runtime=False,
            data_dir=Path(temporary) / "config",
            secret_key=b"p4-openapi-contract-key",
        )
        schema = app.openapi()
    _validate_schema(schema)
    return schema


def _validate_schema(schema: dict[str, Any]) -> None:
    paths = schema.get("paths", {})
    operations = sum(len(methods) for methods in paths.values())
    if len(paths) != EXPECTED_PATHS or operations != EXPECTED_OPERATIONS:
        raise RuntimeError(f"unexpected P4 OpenAPI surface: paths={len(paths)} operations={operations}")
    for method, path in BODY_METHODS:
        operation = paths.get(path, {}).get(method, {})
        if not operation.get("requestBody"):
            raise RuntimeError(f"P4 OpenAPI operation has no request body schema: {method} {path}")
        if "422" in operation.get("responses", {}):
            raise RuntimeError(f"P4 compatibility operation exposes FastAPI 422 validation: {method} {path}")
    for path, methods in paths.items():
        for method, operation in methods.items():
            success = operation.get("responses", {}).get("200", {})
            content = success.get("content", {})
            if not content:
                raise RuntimeError(f"P4 OpenAPI operation has no 200 content schema: {method} {path}")
            if "application/json" in content and not content["application/json"].get("schema"):
                raise RuntimeError(f"P4 OpenAPI operation has an empty JSON schema: {method} {path}")


def _render(schema: dict[str, Any]) -> str:
    return json.dumps(schema, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Freeze the local P4 FastAPI OpenAPI contract.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    rendered = _render(build_openapi())
    if args.check:
        if not args.output.is_file() or args.output.read_text(encoding="utf-8") != rendered:
            print(f"stale P4 OpenAPI contract: {args.output}")
            return 1
        print(f"p4_openapi_check=clean path={args.output}")
        return 0
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    print(f"p4_openapi_written={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
