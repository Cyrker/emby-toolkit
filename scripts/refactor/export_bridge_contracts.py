from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from export_routes import export_routes
from manifest_common import REPO_ROOT, apply_contract_override, dump_yaml_compatible_json, stable_id


EMBY_INBOUND: dict[tuple[str, str], dict[str, Any]] = {
    ("GET", "/api/emby/metadata"): {
        "request_fields": ["query.path", "query.item_type", "query.season_number?", "query.episode_number?"],
        "response_statuses": [200, 400, 404],
    },
    ("POST", "/api/emby/metadata/backfill"): {
        "request_fields": [],
        "response_statuses": [202, 200, 409],
    },
    ("POST", "/api/emby/metadata/backfill/item"): {
        "request_fields": ["body.emby_item_id"],
        "response_statuses": [202, 200, 400, 409],
    },
    ("POST", "/api/emby/metadata/backfill/items"): {
        "request_fields": ["body.emby_item_ids[]"],
        "response_statuses": [202, 200, 400, 409],
    },
    ("POST", "/api/emby/metadata/backfill/library"): {
        "request_fields": ["body.emby_library_id", "body.refresh_mode"],
        "response_statuses": [202, 200, 400, 409],
    },
    ("POST", "/api/emby/metadata/images/refresh"): {
        "request_fields": ["query.path?", "query.tmdb_id?", "query.item_type", "query.season_number?", "query.episode_number?"],
        "response_statuses": [200, 400, 404, 500, 502, 503],
    },
    ("GET", "/api/emby/metadata/images/search"): {
        "request_fields": ["query.path?", "query.tmdb_id?", "query.item_type", "query.include_all_languages?"],
        "response_statuses": [200, 400, 404, 502, 503],
    },
    ("POST", "/api/emby/metadata/images/sync"): {
        "request_fields": ["query.path?", "query.tmdb_id?", "query.item_type", "body.rules[]", "body.force?"],
        "response_statuses": [200, 400, 404, 410, 502],
    },
    ("POST", "/api/emby/events"): {
        "request_fields": ["body._etk_source", "body.Event"],
        "response_statuses": [200, 202, 400, 500, 503],
        "auth": "source-marker-only",
    },
    ("GET", "/api/emby/plugin-update"): {
        "request_fields": [],
        "response_statuses": [200, 502],
        "response_content_types": ["application/octet-stream", "application/json"],
    },
}


P115_OPERATIONS = {
    "": ("GET", [200, 400, 404, 422], ["path.identity"]),
    "/metadata": (
        "GET",
        [200, 400, 404],
        ["path.identity", "query.item_type?", "query.season_number?", "query.episode_number?"],
    ),
    "/metadata/backfill": ("POST", [202, 200, 404, 409], ["path.identity"]),
    "/intro-sync": ("POST", [200, 404], ["path.identity", "query.emby_item_id"]),
    "/item-ready": ("POST", [200, 404, 409], ["path.identity", "query.emby_item_id"]),
    "/deep-delete/prepare": (
        "POST",
        [200, 400, 404, 409],
        ["path.identity", "body.root_item_id", "body.anchor_item_id", "body.item_type"],
    ),
    "/deep-delete/commit": (
        "POST",
        [202, 400, 404, 410],
        ["path.identity", "body.token"],
    ),
}


def _p115_inbound() -> dict[tuple[str, str], dict[str, Any]]:
    contracts: dict[tuple[str, str], dict[str, Any]] = {}
    bases = ["/api/p115/mediainfo/<pick_code>", "/api/p115/mediainfo/sha1/<sha1>"]
    for base in bases:
        for suffix, (method, statuses, fields) in P115_OPERATIONS.items():
            contracts[(method, base + suffix)] = {
                "request_fields": fields,
                "response_statuses": statuses,
            }
    return contracts


OUTBOUND = [
    {
        "method": "POST",
        "path": "/ETKMediaInfo/Origin",
        "source": "handler/emby.py:106",
        "request_fields": ["query.api_key", "body.Url"],
        "response_fields": ["Configured|configured"],
    },
    {
        "method": "POST",
        "path": "/ETKMediaInfo/DiscoveryUrl",
        "source": "handler/emby.py:128",
        "request_fields": ["query.api_key", "body.Url"],
        "response_fields": ["Configured|configured"],
    },
    {
        "method": "POST",
        "path": "/Items/{item_id}/ETKMediaInfo",
        "source": "handler/emby.py:180",
        "request_fields": ["header.X-Emby-Token", "body.MediaSourceInfo", "body.Chapters"],
        "response_fields": ["StreamCount?", "PreservedExternalStreamCount?"],
    },
    {
        "method": "POST",
        "path": "/Items/{item_id}/ETKMediaInfo/Intro",
        "source": "handler/emby.py:240",
        "request_fields": ["body.IntroStartTicks", "body.IntroEndTicks"],
        "response_fields": [],
    },
    {
        "method": "POST",
        "path": "/Items/{item_id}/ETKMediaInfo/Credits",
        "source": "handler/emby.py:286",
        "request_fields": ["body.CreditsStartTicks"],
        "response_fields": [],
    },
    {
        "method": "POST",
        "path": "/Items/{item_id}/ETKMediaInfo/Chapters",
        "source": "handler/emby.py:329",
        "request_fields": ["body.Clear*", "body.*Ticks?"],
        "response_fields": [],
    },
]


def _inbound_fixtures(path: str) -> list[str]:
    fixtures = ["verification/fixtures/http/bridge-validation-errors.json"]
    if path == "/api/emby/metadata":
        fixtures.insert(0, "verification/fixtures/http/bridge-metadata.json")
    if path in {
        "/api/p115/mediainfo/<pick_code>",
        "/api/p115/mediainfo/sha1/<sha1>",
    }:
        fixtures.insert(0, "verification/fixtures/http/bridge-mediainfo.json")
    return fixtures


def export_bridge_contracts() -> dict[str, Any]:
    required = {**EMBY_INBOUND, **_p115_inbound()}
    route_payload = export_routes()
    route_lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for route in route_payload["routes"]:
        for method in route["methods"]:
            route_lookup[(method, route["path"])] = route
    missing = sorted(f"{method} {path}" for method, path in required if (method, path) not in route_lookup)
    if missing:
        raise ValueError(f"required Bridge contracts missing from source: {', '.join(missing)}")

    inbound: list[dict[str, Any]] = []
    for (method, path), details in sorted(required.items()):
        source = route_lookup[(method, path)]
        inbound.append(
            apply_contract_override(
            {
                "contract_id": stable_id("bridge-inbound", method, path),
                "direction": "inbound",
                "transport": "http",
                "method": method,
                "path": path,
                "source": source["source"],
                "endpoint": source["endpoint"],
                "auth": details.get("auth", "none"),
                "request_fields": details["request_fields"],
                "response_statuses": details["response_statuses"],
                "response_content_types": details.get("response_content_types", ["application/json"]),
                "redactions": [
                    "Authorization",
                    "Cookie",
                    "X-Emby-Token",
                    "path.identity",
                    "filesystem paths",
                    "external URLs",
                    "user identifiers",
                ],
                "target_stage": "P5" if path.startswith("/api/emby/") else "P6",
                "status": "legacy",
                "new_owner": None,
                "fixtures": _inbound_fixtures(path),
                "validation_owner": "media/bridge" if path.startswith("/api/emby/") else "p115/bridge",
                "validation_method": "golden-master-and-stub-contract",
            },
            "bridge_contracts",
            )
        )

    outbound = [
        apply_contract_override({
            "contract_id": stable_id("bridge-outbound", item["method"], item["path"]),
            "direction": "outbound",
            "transport": "http",
            "auth": "emby-token",
            "response_statuses": [200],
            "response_content_types": ["application/json"],
            "redactions": ["query.api_key", "header.X-Emby-Token", "body.Url"],
            "target_stage": "P5",
            "status": "legacy",
            "new_owner": None,
            "fixtures": ["verification/fixtures/stubs/external-services.json"],
            "validation_owner": "media/bridge",
            "validation_method": "stub-contract",
            **item,
        }, "bridge_contracts")
        for item in OUTBOUND
    ]
    return {
        "schema_version": 1,
        "generator": "scripts/refactor/export_bridge_contracts.py",
        "source_scope": ["routes/webhook.py", "routes/p115.py", "handler/emby.py"],
        "metrics": {
            "required_inbound_contract_count": len(inbound),
            "outbound_bridge_contract_count": len(outbound),
            "total_contract_count": len(inbound) + len(outbound),
        },
        "limitations": [
            "The Bridge DLL source is not part of this repository; outbound response schemas cover only fields consumed by ETK.",
            "Success payloads that depend on database or external adapters require sanitized fixtures and local stubs.",
        ],
        "contracts": inbound + outbound,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Export Bridge, Emby, and 115 contract ownership.")
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "verification/contracts/bridge-contract-ownership.yaml",
    )
    args = parser.parse_args()
    payload = export_bridge_contracts()
    dump_yaml_compatible_json(args.output, payload)
    print(f"bridge_contracts={payload['metrics']['total_contract_count']} output={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
