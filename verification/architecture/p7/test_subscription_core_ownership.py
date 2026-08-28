from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from etk.bootstrap.app_factory import create_app
from etk.domains.subscription.cleanup_router import router as cleanup_router
from etk.domains.subscription.core_router import router as core_router
from etk.domains.subscription.moviepilot_config_router import router as moviepilot_config_router
from etk.domains.subscription.resubscribe_router import router as resubscribe_router

REPO_ROOT = Path(__file__).resolve().parents[3]
CORE = REPO_ROOT / "src/etk/domains/subscription/core.py"
ROUTER = REPO_ROOT / "src/etk/domains/subscription/core_router.py"
MEDIA_PROJECTION = REPO_ROOT / "src/etk/domains/media/subscription_projection.py"
FIXTURES = (
    REPO_ROOT / "verification/fixtures/http/p7-cleanup.json",
    REPO_ROOT / "verification/fixtures/http/p7-moviepilot-config.json",
    REPO_ROOT / "verification/fixtures/http/p7-resubscribe.json",
    REPO_ROOT / "verification/fixtures/http/p7-subscription-core.json",
)


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    result: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            result.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            result.add(node.module)
    return result


def test_subscription_core_keeps_media_metadata_sql_behind_p5_projection() -> None:
    core_source = CORE.read_text(encoding="utf-8")
    router_source = ROUTER.read_text(encoding="utf-8")
    projection_source = MEDIA_PROJECTION.read_text(encoding="utf-8")

    assert "media_metadata" not in core_source
    assert "media_metadata" not in router_source
    assert "media_metadata" in projection_source
    assert "subscription_operations" in core_source
    assert "sqlalchemy" not in _imports(ROUTER)
    assert not any(module.startswith("etk.domains.subscription") for module in _imports(MEDIA_PROJECTION))
    assert "emby_users" not in projection_source
    assert "cleanup_index" not in projection_source
    assert "resubscribe_index" not in projection_source
    assert "subscription_operations" not in projection_source
    assert not any(module.split(".")[0] in {"database", "handler", "routes", "tasks"} for module in _imports(CORE))
    assert not any(module.split(".")[0] in {"database", "handler", "routes", "tasks"} for module in _imports(ROUTER))


def test_p7_http_fixtures_are_all_native_and_precede_compatibility_mount(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ETK_ENABLE_LEGACY_COMPATIBILITY", "1")
    expected = {
        (route["method"], route["path"])
        for fixture in FIXTURES
        for route in json.loads(fixture.read_text(encoding="utf-8"))["routes"]
    }
    app = create_app(start_runtime=False)
    try:
        schema = app.openapi()["paths"]
        for method, path in expected:
            assert path in schema
            assert method.casefold() in schema[path]
        compatibility_index = next(
            index for index, route in enumerate(app.routes) if getattr(route, "name", None) == "legacy-compatibility"
        )
        native_routers = (
            cleanup_router,
            core_router,
            moviepilot_config_router,
            resubscribe_router,
        )
        indexes = {
            index
            for index, route in enumerate(app.routes)
            if any(getattr(route, "original_router", None) is router for router in native_routers)
        }
        assert len(indexes) == len(native_routers)
        assert all(index < compatibility_index for index in indexes)
    finally:
        app.state.container.close()


def test_subscription_projection_describes_opaque_sources_and_atomic_delete_guards() -> None:
    source = MEDIA_PROJECTION.read_text(encoding="utf-8")
    core_source = CORE.read_text(encoding="utf-8")
    assert "subscription_sources_json" in source
    assert "PENDING_RELEASE" in source
    assert "FOR UPDATE" in source
    assert "cleanup_execution_items" in core_source
    assert "subscription_operations" in core_source
    assert "resubscribe_index" in core_source
