from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
TOOLS = REPO_ROOT / "scripts/refactor"
sys.path.insert(0, str(TOOLS))

from etk.processes import p5_media_tasks  # noqa: E402
from scripts.refactor.export_bridge_contracts import export_bridge_contracts  # noqa: E402
from scripts.refactor.export_manifests import _method_bindings  # noqa: E402
from scripts.refactor.export_routes import P5_EMBY_ROUTE_PATHS, ROUTE_OWNERS, export_routes  # noqa: E402
from scripts.refactor.export_tasks import P5_TASK_EXECUTION_OWNERS, export_tasks  # noqa: E402

EXPECTED_MEDIA_OWNERSHIP = {
    ("GET", "/api/search_emby_library"): (("operations",), "P8", "operations/review"),
    ("GET", "/api/media_for_editing/<item_id>"): (("media",), "P5", "media/cast-editor"),
    ("POST", "/api/update_media_cast_sa/<item_id>"): (("media",), "P5", "media/cast-editor"),
    ("GET", "/api/image_proxy"): (("media",), "P5", "media/image-delivery"),
    ("GET", "/image_proxy/<path:image_path>"): (("media",), "P5", "media/emby-image-delivery"),
    ("POST", "/api/actions/translate_cast_sa"): (("media",), "P5", "media/cast-editor"),
    ("POST", "/api/preview_processed_cast/<item_id>"): (("media",), "P5", "media/cast-editor"),
    ("GET", "/api/emby_libraries"): (("media",), "P8", "media/library-query"),
    ("GET", "/api/emby/user/<user_id>/views"): (("users",), "P8", "users/integration-settings"),
    ("GET", "/api/search_studios"): (("collection",), "P8", "collection/custom-collection"),
    ("POST", "/api/subscription/status"): (("subscription",), "P7", "subscription/state"),
    ("GET", "/api/subscriptions/all"): (("subscription",), "P7", "subscription/management"),
    ("POST", "/api/media/batch_delete"): (("subscription",), "P7", "subscription/management"),
    ("GET", "/api/subscription/strategy"): (("subscription",), "P7", "subscription/strategy"),
    ("POST", "/api/subscription/strategy"): (("subscription",), "P7", "subscription/strategy"),
    ("GET", "/api/auto_tagging/rules"): (("media",), "P8", "media/auto-tagging"),
    ("POST", "/api/auto_tagging/rules"): (("media",), "P8", "media/auto-tagging"),
    ("POST", "/api/auto_tagging/run_now"): (("media",), "P8", "media/auto-tagging"),
    ("POST", "/api/auto_tagging/clear_now"): (("media",), "P8", "media/auto-tagging"),
    ("POST", "/api/update_media_image/<item_id>"): (("media",), "P5", "media/image-editor"),
    ("GET", "/api/tmdb_images/<item_id>"): (("media",), "P5", "media/image-editor"),
    ("GET", "/api/media_info/edit/<item_id>"): (("media",), "P5", "media/mediainfo-editor"),
    ("GET", "/api/media_info/chapters/<item_id>"): (("media",), "P5", "media/mediainfo-chapters"),
    ("POST", "/api/media_info/chapters/<item_id>"): (("media",), "P5", "media/mediainfo-chapters"),
    ("POST", "/api/media_info/edit/<item_id>"): (("media",), "P5", "media/mediainfo-editor"),
    ("GET", "/api/media_info/series/<series_id>/episodes"): (
        ("media",),
        "P5",
        "media/mediainfo-editor",
    ),
}

EXPECTED_BRIDGE_OWNERSHIP = {
    (method, path): (
        ("media", "subscription", "sharing") if path == "/api/emby/events" else ("media",),
        "P5/P7/P9" if path == "/api/emby/events" else "P5",
        "media/bridge-transport" if path == "/api/emby/events" else "media/bridge",
    )
    for method, path in (
        ("GET", "/api/emby/plugin-update"),
        ("GET", "/api/emby/metadata"),
        ("POST", "/api/emby/metadata/backfill"),
        ("POST", "/api/emby/metadata/backfill/item"),
        ("POST", "/api/emby/metadata/backfill/items"),
        ("POST", "/api/emby/metadata/backfill/library"),
        ("GET", "/api/emby/metadata/images/search"),
        ("POST", "/api/emby/metadata/images/refresh"),
        ("POST", "/api/emby/metadata/images/sync"),
        ("POST", "/api/emby/events"),
    )
}


def _observed_ownership(entries: list[dict[str, object]]) -> dict[tuple[str, str], tuple[tuple[str, ...], str, str]]:
    observed: dict[tuple[str, str], tuple[tuple[str, ...], str, str]] = {}
    for entry in entries:
        methods = entry.get("methods")
        if methods is None:
            methods = [entry["method"]]
        assert isinstance(methods, list)
        assert len(methods) == 1
        method = methods[0]
        path = entry["path"]
        domains = entry["target_domains"]
        stage = entry["target_stage"]
        validation_owner = entry["validation_owner"]
        assert isinstance(method, str)
        assert isinstance(path, str)
        assert isinstance(domains, list)
        assert all(isinstance(domain, str) for domain in domains)
        assert isinstance(stage, str)
        assert isinstance(validation_owner, str)
        observed[(method, path)] = (tuple(domains), stage, validation_owner)
    return observed


def test_media_route_decorators_and_method_bindings_have_exact_method_level_ownership() -> None:
    routes = export_routes()
    media_decorators = [entry for entry in routes["routes"] if str(entry["source"]).startswith("routes/media.py:")]
    media_bindings = [
        entry for entry in _method_bindings(routes) if str(entry["source"]).startswith("routes/media.py:")
    ]

    assert len(media_decorators) == 26
    assert len(media_bindings) == 26
    assert _observed_ownership(media_decorators) == EXPECTED_MEDIA_OWNERSHIP
    assert _observed_ownership(media_bindings) == EXPECTED_MEDIA_OWNERSHIP
    assert Counter(value[1] for value in EXPECTED_MEDIA_OWNERSHIP.values()) == {
        "P5": 13,
        "P7": 5,
        "P8": 8,
    }


def test_bridge_route_ownership_separates_p5_contracts_from_future_event_handlers() -> None:
    routes = export_routes()
    bridge_decorators = [
        entry
        for entry in routes["routes"]
        if str(entry["source"]).startswith("routes/webhook.py:") and entry["path"] in P5_EMBY_ROUTE_PATHS
    ]
    bridge_bindings = [
        entry
        for entry in _method_bindings(routes)
        if str(entry["source"]).startswith("routes/webhook.py:") and entry["path"] in P5_EMBY_ROUTE_PATHS
    ]

    assert len(bridge_decorators) == 10
    assert len(bridge_bindings) == 10
    assert _observed_ownership(bridge_decorators) == EXPECTED_BRIDGE_OWNERSHIP
    assert _observed_ownership(bridge_bindings) == EXPECTED_BRIDGE_OWNERSHIP


def test_method_level_media_ownership_does_not_change_other_route_file_defaults() -> None:
    routes = export_routes()
    for entry in routes["routes"]:
        source_path = str(entry["source"]).rsplit(":", 1)[0]
        if source_path == "routes/media.py":
            continue
        if source_path == "routes/webhook.py" and entry["path"] in P5_EMBY_ROUTE_PATHS:
            continue
        expected_domains, expected_stage = ROUTE_OWNERS[source_path]
        assert entry["target_domains"] == expected_domains
        assert entry["target_stage"] == expected_stage


def test_bridge_stage_boundary_keeps_p115_transport_as_an_explicit_p6_gate() -> None:
    payload = export_bridge_contracts()
    contracts = payload["contracts"]
    p5_inbound = [
        value for value in contracts if value["direction"] == "inbound" and str(value["path"]).startswith("/api/emby/")
    ]
    p6_inbound = [
        value
        for value in contracts
        if value["direction"] == "inbound" and str(value["path"]).startswith("/api/p115/mediainfo/")
    ]
    outbound = [value for value in contracts if value["direction"] == "outbound"]

    assert len(p5_inbound) == 10
    assert len(p6_inbound) == 14
    assert len(outbound) == 6
    assert all(value["status"] == "new" and value["target_stage"] == "P5" for value in p5_inbound)
    assert all(value["status"] == "legacy" and value["target_stage"] == "P6" for value in p6_inbound)
    assert all(value["response_statuses"] == ["2XX"] for value in outbound)
    assert payload["stage_boundary"] == {
        "decision": "split-central-media-and-p115-transport",
        "p5_owned": {
            "inbound_contracts": "the ten /api/emby/* operations",
            "outbound_contracts": "the six ETKMediaInfo Bridge write operations",
            "invariants": "media identity, metadata, image, event transport, durable refresh, and deep-delete receipts",
        },
        "p6_deferred": {
            "inbound_contracts": "the fourteen PickCode/SHA1 /api/p115/mediainfo/* operations",
            "reason": "the approved route ownership table assigns routes/p115.py and P115 business execution to P6",
            "acceptance": "P6 must provide native HTTP ownership and legacy-vs-native differential evidence",
        },
        "compatibility_rule": "P5 must not mark the fourteen P115 operations new or claim them locally accepted",
    }


def test_p5_task_manifest_separates_native_envelope_from_legacy_execution() -> None:
    tasks = {
        value["task_key"]: value for value in export_tasks()["tasks"] if value["task_key"] in P5_TASK_EXECUTION_OWNERS
    }

    assert set(tasks) == set(P5_TASK_EXECUTION_OWNERS)
    expected_runtime_owners: dict[str, str | dict[str, str]] = {
        task_key: ".".join(owner) for task_key, owner in p5_media_tasks._LEGACY_HANDLERS.items()
    }
    default_backfill_owner = expected_runtime_owners["backfill-media-metadata"]
    assert isinstance(default_backfill_owner, str)
    expected_runtime_owners["backfill-media-metadata"] = {
        "default": default_backfill_owner,
        "when_targets_present": ".".join(p5_media_tasks._TARGETED_BACKFILL_HANDLER),
    }
    assert P5_TASK_EXECUTION_OWNERS == expected_runtime_owners

    for task_key, execution_owner in expected_runtime_owners.items():
        task = tasks[task_key]
        assert task["status"] == "new"
        assert task["new_owner"] == "etk.processes.p5_media_tasks.P5MediaTaskRouter"
        expected_layers: dict[str, object] = {
            "envelope_owner": "etk.processes.p5_media_tasks.P5MediaTaskRouter",
            "execution_kind": "legacy-compatibility-callable",
            "retirement_stage": "P11",
        }
        if isinstance(execution_owner, str):
            expected_layers["execution_owner"] = execution_owner
        else:
            expected_layers["execution_owners"] = execution_owner
        assert task["ownership_layers"] == expected_layers
