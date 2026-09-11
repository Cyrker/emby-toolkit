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
# The compatibility OpenAPI snapshot is regenerated at each backend stage.
# Keep the historical filename for existing consumers while validating the
# current P11 surface.
EXPECTED_PATHS = 260
EXPECTED_OPERATIONS = 309
RETIRED_EXTERNAL_RESOURCE_PATH_PREFIXES = (
    "/api/shared/resources",
    "/api/subscription/hdhive",
    "/api/subscription/tg_userbot",
    "/api/subscription/cloud",
)
BODY_METHODS = {
    ("post", "/api/ai/models"),
    ("post", "/api/ai/prompts"),
    ("post", "/api/ai/test"),
    ("post", "/api/emby/collections/activate"),
    ("post", "/api/actions/translate_cast_sa"),
    ("post", "/api/auth/login"),
    ("post", "/api/auth/reauthorize"),
    ("post", "/api/auth/setup"),
    ("post", "/api/auth/verify_recovery"),
    ("post", "/api/config"),
    ("post", "/api/emby/events"),
    ("post", "/api/emby/search/index"),
    ("post", "/api/emby/metadata/backfill/item"),
    ("post", "/api/emby/metadata/backfill/items"),
    ("post", "/api/emby/metadata/backfill/library"),
    ("post", "/api/emby/metadata/images/sync"),
    ("post", "/api/media_info/chapters/{item_id}"),
    ("post", "/api/media_info/edit/{item_id}"),
    ("post", "/api/p115/cookie"),
    ("post", "/api/p115/episode_regex_rules"),
    ("post", "/api/p115/mediainfo/{pick_code}/deep-delete/commit"),
    ("post", "/api/p115/mediainfo/{pick_code}/deep-delete/prepare"),
    ("post", "/api/p115/mediainfo/sha1/{sha1}/deep-delete/commit"),
    ("post", "/api/p115/mediainfo/sha1/{sha1}/deep-delete/prepare"),
    ("post", "/api/p115/organize/executions/{execution_id}/reconcile"),
    ("post", "/api/p115/organize/plans/{plan_id}/execute"),
    ("post", "/api/p115/organize/plans/dry-run"),
    ("post", "/api/p115/play_pool"),
    ("post", "/api/p115/play_pool/accounts"),
    ("put", "/api/p115/play_pool/accounts/{account_id}"),
    ("post", "/api/p115/play_pool/user-account"),
    ("post", "/api/p115/rename_config"),
    ("post", "/api/p115/rename_config/preview"),
    ("post", "/api/p115/replace_strm"),
    ("post", "/api/p115/sorting_rules"),
    ("post", "/api/p115/temp_dir_config"),
    ("post", "/api/p115/upload_monitor/config"),
    ("post", "/api/p115/washing_priority_recalculate"),
    ("post", "/api/update_media_image/{item_id}"),
    ("post", "/api/update_media_cast_sa/{item_id}"),
    ("post", "/api/config/custom_theme"),
    ("post", "/api/proxy/test"),
    ("post", "/api/system/menu_config"),
    ("post", "/api/telegram/templates"),
    ("post", "/api/telegram/test"),
    ("post", "/api/tasks/run"),
}
ORGANIZE_PLAN_REQUEST_FIELDS = {
    "recognition_hints",
    "request_intent_id",
    "source_cid",
    "source_file_ids",
    "source_name",
}
ORGANIZE_PLAN_DOCUMENT_FIELDS = {
    "account_id",
    "actions",
    "config_snapshot",
    "created_at",
    "errors",
    "expires_at",
    "input_fingerprint",
    "plan_id",
    "planner_version",
    "provider_evidence",
    "recognition_evidence",
    "request_hash",
    "request_intent_id",
    "revisions",
    "risk_summary",
    "schema_version",
    "source_root",
    "source_snapshot",
    "status",
    "warnings",
}
ORGANIZE_PLAN_ACTION_FIELDS = {
    "action_id",
    "action_index",
    "decision_basis",
    "deferred_slice",
    "dependencies",
    "destination",
    "destructive",
    "effect_key",
    "expected_postcondition",
    "kind",
    "preconditions",
    "risk",
    "subject",
}
ORGANIZE_EXECUTION_REQUEST_FIELDS = {"approved_execution_intent_id"}
ORGANIZE_RECONCILIATION_REQUEST_FIELDS = {"approved_reconciliation_intent_id"}
ORGANIZE_RECORD_QUERY_FIELDS = {"cid", "fail_reason", "page", "per_page", "search", "status"}
REPLACE_STRM_REQUEST_FIELDS = {"mode", "replace", "search"}
P6_ORGANIZE_EXECUTION_RESPONSES = {
    ("get", "/api/p115/organize/executions/{execution_id}"): {
        "200": {"application/json"},
        "403": {"application/json"},
        "404": {"application/json"},
        "500": {"application/json"},
    },
    ("post", "/api/p115/organize/executions/{execution_id}/reconcile"): {
        "200": {"application/json"},
        "400": {"application/json"},
        "403": {"application/json"},
        "404": {"application/json"},
        "409": {"application/json"},
        "415": {"text/html"},
        "500": {"application/json"},
    },
    ("post", "/api/p115/organize/plans/{plan_id}/execute"): {
        "200": {"application/json"},
        "202": {"application/json"},
        "400": {"application/json"},
        "403": {"application/json"},
        "404": {"application/json"},
        "409": {"application/json"},
        "415": {"text/html"},
        "500": {"application/json"},
    },
    ("get", "/api/p115/records"): {
        "200": {"application/json"},
        "403": {"application/json"},
        "500": {"application/json"},
    },
    ("delete", "/api/p115/records/{record_id}"): {
        "200": {"application/json"},
        "403": {"application/json"},
        "404": {"application/json"},
        "500": {"application/json"},
    },
}
P6_MEDIAINFO_RESPONSES = {
    ("get", "/api/p115/mediainfo/{pick_code}"): {"200", "400", "404", "422"},
    ("get", "/api/p115/mediainfo/sha1/{sha1}"): {"200", "400", "404", "422"},
    ("get", "/api/p115/mediainfo/{pick_code}/metadata"): {"200", "400", "404"},
    ("get", "/api/p115/mediainfo/sha1/{sha1}/metadata"): {"200", "400", "404"},
    ("post", "/api/p115/mediainfo/{pick_code}/metadata/backfill"): {"200", "202", "404", "409"},
    ("post", "/api/p115/mediainfo/sha1/{sha1}/metadata/backfill"): {"200", "202", "404", "409"},
}
P6_7_BRIDGE_RESPONSES = {
    ("post", "/api/p115/mediainfo/{pick_code}/item-ready"): {"200", "404", "409", "503"},
    ("post", "/api/p115/mediainfo/sha1/{sha1}/item-ready"): {"200", "404", "409", "503"},
    ("post", "/api/p115/mediainfo/{pick_code}/deep-delete/prepare"): {"200", "400", "404", "409"},
    ("post", "/api/p115/mediainfo/sha1/{sha1}/deep-delete/prepare"): {"200", "400", "404", "409"},
    ("post", "/api/p115/mediainfo/{pick_code}/deep-delete/commit"): {"202", "400", "404", "410", "500"},
    ("post", "/api/p115/mediainfo/sha1/{sha1}/deep-delete/commit"): {"202", "400", "404", "410", "500"},
}
P6_8_UPLOAD_MONITOR_RESPONSES = {
    ("get", "/api/p115/upload_monitor/config"): {"200", "403", "500"},
    ("post", "/api/p115/upload_monitor/config"): {"200", "400", "403", "500"},
}
P6_10_INTRO_SYNC_RESPONSES = {
    ("post", "/api/p115/mediainfo/{pick_code}/intro-sync"): {"200", "404"},
    ("post", "/api/p115/mediainfo/sha1/{sha1}/intro-sync"): {"200"},
}
P11_INTRO_BACKFILL_RESPONSES = {
    ("post", "/api/emby/intro-detection/backfill"): {"202", "503"},
}
P6_9_PLAYBACK_OPERATIONS = {
    ("get", "/api/p115/play/{pick_code}"),
    ("head", "/api/p115/play/{pick_code}"),
    ("get", "/api/p115/play/{pick_code}/{filename}"),
    ("head", "/api/p115/play/{pick_code}/{filename}"),
    ("get", "/api/p115/play_pool"),
    ("post", "/api/p115/play_pool"),
    ("post", "/api/p115/play_pool/accounts"),
    ("put", "/api/p115/play_pool/accounts/{account_id}"),
    ("delete", "/api/p115/play_pool/accounts/{account_id}"),
    ("post", "/api/p115/play_pool/accounts/{account_id}/speedtest"),
    ("post", "/api/p115/play_pool/cleanup"),
    ("get", "/api/p115/play_pool/cookie_qrcode"),
    ("get", "/api/p115/play_pool/cookie_qrcode/status"),
    ("get", "/api/p115/play_pool/user-account"),
    ("post", "/api/p115/play_pool/user-account"),
    ("delete", "/api/p115/play_pool/user-account"),
    ("get", "/api/p115/play_pool/user-account/rewards"),
    ("get", "/api/p115/temp_dir_config"),
    ("post", "/api/p115/temp_dir_config"),
}
P6_9_BODY_OPERATIONS = {
    ("post", "/api/p115/play_pool"),
    ("post", "/api/p115/play_pool/accounts"),
    ("put", "/api/p115/play_pool/accounts/{account_id}"),
    ("post", "/api/p115/play_pool/user-account"),
    ("post", "/api/p115/temp_dir_config"),
}

# A handful of compatibility endpoints intentionally return a raw Starlette
# ``Response`` (gzip downloads and legacy JSON envelopes assembled at runtime).
# FastAPI represents those dynamic payloads as ``application/json: {}``; that
# is an explicit contract choice rather than an accidental missing schema.
EMPTY_RESPONSE_SCHEMA_ALLOWED = {
    ("post", "/api/actions/mark_item_processed/{item_id}"),
    ("post", "/api/actions/clear_review_items"),
    ("post", "/api/actions/reprocess_item/{item_id}"),
    ("post", "/api/actions/reprocess_all_review_items"),
    ("post", "/api/database/export"),
    ("post", "/api/database/preview-backup"),
    ("post", "/api/database/import"),
    ("post", "/api/actions/clear_tables"),
    ("post", "/api/database/correct-sequences"),
    ("post", "/api/actions/prepare-for-library-rebuild"),
    ("post", "/api/actions/cleanup-offline-media"),
    ("post", "/api/actions/clear-vectors"),
    ("get", "/api/database/maintenance/operations/{operation_id}"),
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
        raise RuntimeError(f"unexpected P11 OpenAPI surface: paths={len(paths)} operations={operations}")
    retired_paths = sorted(
        path for path in paths if any(path.startswith(prefix) for prefix in RETIRED_EXTERNAL_RESOURCE_PATH_PREFIXES)
    )
    if retired_paths:
        raise RuntimeError(f"retired external-resource paths remain in OpenAPI: {retired_paths}")
    operation_ids: dict[str, tuple[str, str]] = {}
    for path, methods in paths.items():
        for method, operation in methods.items():
            operation_id = operation.get("operationId")
            if operation_id:
                previous = operation_ids.get(operation_id)
                if previous is not None:
                    duplicate = f"{previous[0]} {previous[1]} and {method} {path}"
                    raise RuntimeError(f"duplicate OpenAPI operationId {operation_id!r}: {duplicate}")
                operation_ids[operation_id] = (method, path)
    for method, path in BODY_METHODS:
        operation = paths.get(path, {}).get(method, {})
        if not operation.get("requestBody"):
            raise RuntimeError(f"P4 OpenAPI operation has no request body schema: {method} {path}")
        if "422" in operation.get("responses", {}):
            raise RuntimeError(f"P4 compatibility operation exposes FastAPI 422 validation: {method} {path}")
    organize_operation = paths["/api/p115/organize/plans/dry-run"]["post"]
    organize_request = organize_operation["requestBody"]["content"]["application/json"]["schema"]
    if set(organize_request.get("properties", {})) != ORGANIZE_PLAN_REQUEST_FIELDS:
        raise RuntimeError("P6 organize-plan request DTO fields changed")
    schemas = schema.get("components", {}).get("schemas", {})
    if set(schemas.get("OrganizePlanDocumentResponse", {}).get("properties", {})) != ORGANIZE_PLAN_DOCUMENT_FIELDS:
        raise RuntimeError("P6 organize-plan response DTO fields changed")
    if set(schemas.get("OrganizePlanActionResponse", {}).get("properties", {})) != ORGANIZE_PLAN_ACTION_FIELDS:
        raise RuntimeError("P6 organize-plan action DTO fields changed")
    execute = paths["/api/p115/organize/plans/{plan_id}/execute"]["post"]
    execute_request = execute["requestBody"]["content"]["application/json"]["schema"]
    if set(execute_request.get("properties", {})) != ORGANIZE_EXECUTION_REQUEST_FIELDS:
        raise RuntimeError("P6 organize-execution request DTO fields changed")
    reconcile = paths["/api/p115/organize/executions/{execution_id}/reconcile"]["post"]
    reconcile_request = reconcile["requestBody"]["content"]["application/json"]["schema"]
    if set(reconcile_request.get("properties", {})) != ORGANIZE_RECONCILIATION_REQUEST_FIELDS:
        raise RuntimeError("P6 organize-reconciliation request DTO fields changed")
    records = paths["/api/p115/records"]["get"]
    if {parameter["name"] for parameter in records.get("parameters", [])} != ORGANIZE_RECORD_QUERY_FIELDS:
        raise RuntimeError("P6 organize-record query fields changed")
    record_delete = paths["/api/p115/records/{record_id}"]["delete"]
    if record_delete.get("parameters") != [
        {
            "name": "record_id",
            "in": "path",
            "required": True,
            "schema": {"type": "integer"},
        }
    ]:
        raise RuntimeError("P6 organize-record delete path contract changed")
    for (method, path), expected_responses in P6_ORGANIZE_EXECUTION_RESPONSES.items():
        operation = paths.get(path, {}).get(method)
        if operation is None:
            raise RuntimeError(f"P6 organize execution operation missing: {method} {path}")
        observed_responses = {
            status: set(response.get("content", {})) for status, response in operation.get("responses", {}).items()
        }
        if observed_responses != expected_responses:
            raise RuntimeError(f"P6 organize execution response contract changed: {method} {path}")
    replace_strm = paths["/api/p115/replace_strm"]["post"]
    replace_request = replace_strm["requestBody"]["content"]["application/json"]["schema"]
    if set(replace_request.get("properties", {})) != REPLACE_STRM_REQUEST_FIELDS:
        raise RuntimeError("P6 replace-STRM request DTO fields changed")
    replace_responses = {
        status: set(response.get("content", {})) for status, response in replace_strm.get("responses", {}).items()
    }
    if replace_responses != {
        "200": {"application/json"},
        "400": {"application/json", "text/html"},
        "403": {"application/json"},
        "415": {"text/html"},
        "500": {"application/json", "text/html"},
    }:
        raise RuntimeError("P6 replace-STRM response contract changed")
    for (method, path), expected_statuses in P6_MEDIAINFO_RESPONSES.items():
        operation = paths.get(path, {}).get(method)
        if operation is None:
            raise RuntimeError(f"P6 MediaInfo operation missing: {method} {path}")
        if operation.get("requestBody"):
            raise RuntimeError(f"P6 MediaInfo query/backfill unexpectedly declares a request body: {method} {path}")
        if set(operation.get("responses", {})) != expected_statuses:
            raise RuntimeError(f"P6 MediaInfo response statuses changed: {method} {path}")
        parameters = operation.get("parameters", [])
        path_parameters = [parameter for parameter in parameters if parameter.get("in") == "path"]
        if len(path_parameters) != 1 or path_parameters[0].get("required") is not True:
            raise RuntimeError(f"P6 MediaInfo path identity contract changed: {method} {path}")
    for (method, path), expected_statuses in P6_7_BRIDGE_RESPONSES.items():
        operation = paths.get(path, {}).get(method)
        if operation is None:
            raise RuntimeError(f"P6.7 Bridge operation missing: {method} {path}")
        if set(operation.get("responses", {})) != expected_statuses:
            raise RuntimeError(f"P6.7 Bridge response statuses changed: {method} {path}")
        if "422" in operation.get("responses", {}):
            raise RuntimeError(f"P6.7 Bridge operation exposes automatic 422: {method} {path}")
        parameters = operation.get("parameters", [])
        path_parameters = [parameter for parameter in parameters if parameter.get("in") == "path"]
        if len(path_parameters) != 1 or path_parameters[0].get("required") is not True:
            raise RuntimeError(f"P6.7 Bridge path identity contract changed: {method} {path}")
    for (method, path), expected_statuses in P6_8_UPLOAD_MONITOR_RESPONSES.items():
        operation = paths.get(path, {}).get(method)
        if operation is None:
            raise RuntimeError(f"P6.8 upload-monitor operation missing: {method} {path}")
        if set(operation.get("responses", {})) != expected_statuses:
            raise RuntimeError(f"P6.8 upload-monitor response statuses changed: {method} {path}")
        if "422" in operation.get("responses", {}):
            raise RuntimeError(f"P6.8 upload-monitor operation exposes automatic 422: {method} {path}")
    for (method, path), expected_statuses in P6_10_INTRO_SYNC_RESPONSES.items():
        operation = paths.get(path, {}).get(method)
        if operation is None:
            raise RuntimeError(f"P6.10 intro-sync operation missing: {method} {path}")
        if operation.get("requestBody"):
            raise RuntimeError(f"P6.10 intro-sync unexpectedly declares a request body: {method} {path}")
        if set(operation.get("responses", {})) != expected_statuses:
            raise RuntimeError(f"P6.10 intro-sync response statuses changed: {method} {path}")
        if "422" in operation.get("responses", {}):
            raise RuntimeError(f"P6.10 intro-sync exposes automatic 422: {method} {path}")
        parameters = operation.get("parameters", [])
        path_parameters = [parameter for parameter in parameters if parameter.get("in") == "path"]
        query_parameters = [parameter for parameter in parameters if parameter.get("in") == "query"]
        if len(path_parameters) != 1 or path_parameters[0].get("required") is not True:
            raise RuntimeError(f"P6.10 intro-sync path identity changed: {method} {path}")
        if [parameter.get("name") for parameter in query_parameters] != ["emby_item_id"]:
            raise RuntimeError(f"P6.10 intro-sync query field changed: {method} {path}")
        if query_parameters[0].get("required") is not False:
            raise RuntimeError(f"P6.10 intro-sync query unexpectedly became required: {method} {path}")
    for (method, path), expected_statuses in P11_INTRO_BACKFILL_RESPONSES.items():
        operation = paths.get(path, {}).get(method)
        if operation is None:
            raise RuntimeError(f"P11 intro-backfill operation missing: {method} {path}")
        if operation.get("requestBody"):
            raise RuntimeError(f"P11 intro-backfill unexpectedly declares a request body: {method} {path}")
        if set(operation.get("responses", {})) != expected_statuses:
            raise RuntimeError(f"P11 intro-backfill response statuses changed: {method} {path}")
        if "422" in operation.get("responses", {}):
            raise RuntimeError(f"P11 intro-backfill exposes automatic 422: {method} {path}")
    observed_playback = {
        (method, path)
        for path, methods in paths.items()
        if path.startswith(("/api/p115/play/", "/api/p115/play_pool", "/api/p115/temp_dir_config"))
        for method in methods
    }
    if observed_playback != P6_9_PLAYBACK_OPERATIONS:
        raise RuntimeError("P6.9 playback OpenAPI operation set changed")
    for method, path in P6_9_PLAYBACK_OPERATIONS:
        operation = paths[path][method]
        responses = operation.get("responses", {})
        if "422" in responses:
            raise RuntimeError(f"P6.9 playback operation exposes automatic 422: {method} {path}")
        if method == "get" and path.startswith("/api/p115/play/"):
            if set(responses) != {"200", "206", "302", "404", "500", "default"}:
                raise RuntimeError(f"P6.9 direct-play passthrough responses changed: {method} {path}")
        elif method == "head":
            if set(responses) != {"200"} or responses["200"].get("content"):
                raise RuntimeError(f"P6.9 direct-play HEAD response changed: {method} {path}")
        else:
            expected = {"200", "400", "401", "403", "500"}
            if (method, path) in P6_9_BODY_OPERATIONS:
                expected.add("415")
                request_body = operation.get("requestBody", {})
                if request_body.get("required") is not True or set(request_body.get("content", {})) != {
                    "application/json"
                }:
                    raise RuntimeError(f"P6.9 JSON request-body contract changed: {method} {path}")
                if set(responses["400"].get("content", {})) != {"application/json", "text/html"}:
                    raise RuntimeError(f"P6.9 malformed JSON response contract changed: {method} {path}")
                if set(responses["415"].get("content", {})) != {"text/html"}:
                    raise RuntimeError(f"P6.9 unsupported media response contract changed: {method} {path}")
            if set(responses) != expected:
                raise RuntimeError(f"P6.9 playback response statuses changed: {method} {path}")
    for path, methods in paths.items():
        for method, operation in methods.items():
            if method.casefold() == "head":
                continue
            responses = operation.get("responses", {})
            success = next(
                (
                    response
                    for status, response in sorted(responses.items())
                    if str(status).isdigit() and 200 <= int(status) < 300
                ),
                {},
            )
            content = success.get("content", {})
            if not content:
                raise RuntimeError(f"P4 OpenAPI operation has no 2xx content schema: {method} {path}")
            if (
                "application/json" in content
                and not content["application/json"].get("schema")
                and (method.casefold(), path) not in EMPTY_RESPONSE_SCHEMA_ALLOWED
            ):
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
