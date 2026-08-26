from __future__ import annotations

import json
import re
from pathlib import Path

from etk.bootstrap.app_factory import create_app
from etk.domains.subscription.actor_router import router as actor_router
from etk.domains.subscription.watchlist_router import router as watchlist_router

REPO_ROOT = Path(__file__).resolve().parents[3]
ROUTE_MANIFEST = REPO_ROOT / "verification/contracts/route-ownership.yaml"
OWNERS = {
    "etk.domains.subscription.actor_router": (
        "/api/actor-subscriptions",
        "verification/fixtures/http/p7-actor-subscriptions.json",
        9,
    ),
    "etk.domains.subscription.watchlist_router": (
        "/api/watchlist",
        "verification/fixtures/http/p7-watchlist.json",
        11,
    ),
}


def _fastapi_path(path: str) -> str:
    return re.sub(r"<(?:(?:int|string):)?([^>]+)>", r"{\1}", path)


def test_actor_and_watchlist_bindings_have_native_manifest_owners() -> None:
    bindings = json.loads(ROUTE_MANIFEST.read_text(encoding="utf-8"))["method_bindings"]

    for owner, (prefix, fixture, expected_count) in OWNERS.items():
        owned = [binding for binding in bindings if binding.get("new_owner") == owner]
        assert len(owned) == expected_count
        assert all(binding["path"] == prefix or binding["path"].startswith(prefix + "/") for binding in owned)
        assert all(binding["status"] == "new" for binding in owned)
        assert all(binding["fixture"] == fixture for binding in owned)
        assert all(binding["fixture_exception"] is None for binding in owned)


def test_actor_and_watchlist_native_routes_precede_compatibility_mount() -> None:
    manifest = json.loads(ROUTE_MANIFEST.read_text(encoding="utf-8"))["method_bindings"]
    owned = [binding for binding in manifest if binding.get("new_owner") in OWNERS]
    app = create_app(start_runtime=False)
    try:
        schema_paths = app.openapi()["paths"]
        for binding in owned:
            path = _fastapi_path(binding["path"])
            assert path in schema_paths
            assert binding["method"].lower() in schema_paths[path]

        compatibility_index = next(
            index for index, route in enumerate(app.routes) if getattr(route, "name", None) == "legacy-compatibility"
        )
        native_router_indexes = {
            index
            for index, route in enumerate(app.routes)
            if getattr(route, "original_router", None) is actor_router
            or getattr(route, "original_router", None) is watchlist_router
        }
        assert len(native_router_indexes) == 2
        assert all(index < compatibility_index for index in native_router_indexes)
    finally:
        app.state.container.close()
