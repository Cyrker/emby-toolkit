from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from etk.bootstrap.app_factory import create_app
from etk.domains.media.discover_router import router as discover_router
from etk.domains.media.secondary_router import router as secondary_router
from etk.domains.operations.database_router import router as database_router

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = REPO_ROOT / "verification/fixtures/http/p8-secondary-local.json"
APP_FACTORY = REPO_ROOT / "src/etk/bootstrap/app_factory.py"
LOCAL_SOURCES = (
    REPO_ROOT / "src/etk/domains/identity/users.py",
    REPO_ROOT / "src/etk/domains/identity/users_repository.py",
    REPO_ROOT / "src/etk/domains/identity/users_router.py",
    REPO_ROOT / "src/etk/domains/identity/user_activity.py",
    REPO_ROOT / "src/etk/domains/media/discover.py",
    REPO_ROOT / "src/etk/domains/media/discover_router.py",
    REPO_ROOT / "src/etk/domains/media/secondary.py",
    REPO_ROOT / "src/etk/domains/media/secondary_router.py",
    REPO_ROOT / "src/etk/domains/media/user_projection.py",
    REPO_ROOT / "src/etk/domains/operations/database.py",
    REPO_ROOT / "src/etk/domains/operations/database_router.py",
)
LEGACY_IMPORT_ROOTS = {
    "config_manager",
    "database",
    "extensions",
    "handler",
    "routes",
    "services",
    "task_manager",
    "tasks",
}
P8_EFFECT_PATHS = {
    ("POST", "/api/admin/user_templates"),
    ("GET", "/api/admin/users"),
    ("POST", "/api/admin/users/{user_id}/template"),
    ("POST", "/api/admin/users/{user_id}/status"),
    ("DELETE", "/api/admin/users/{user_id}"),
    ("POST", "/api/register/invite"),
    ("POST", "/api/portal/subscribe"),
    ("GET", "/api/portal/telegram-bot-info"),
    ("POST", "/api/portal/upload-avatar"),
    ("GET", "/api/portal/playback-report"),
    ("GET", "/api/portal/dashboard-stats"),
    ("POST", "/api/discover/movie"),
    ("POST", "/api/discover/tv"),
    ("GET", "/api/discover/genres/{media_type}"),
    ("POST", "/api/discover/search"),
    ("POST", "/api/discover/trigger_recommendation_update"),
    ("GET", "/api/discover/tmdb/tv/{tv_id}"),
    ("GET", "/api/emby_libraries"),
    ("GET", "/api/emby/user/{user_id}/views"),
    ("POST", "/api/auto_tagging/run_now"),
    ("POST", "/api/auto_tagging/clear_now"),
    ("POST", "/api/actions/reprocess_item/{item_id}"),
    ("POST", "/api/actions/reprocess_all_review_items"),
}


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    result: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            result.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            result.add(node.module)
    return result


def test_p8_local_sources_do_not_reverse_import_legacy_runtime() -> None:
    for path in LOCAL_SOURCES:
        imports = _imports(path)
        assert not any(module.split(".")[0] in LEGACY_IMPORT_ROOTS for module in imports), path


def test_secondary_local_and_effect_routes_precede_compatibility(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ETK_ENABLE_LEGACY_COMPATIBILITY", "1")
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    expected = {(route["method"], route["path"]) for route in fixture["routes"]}
    assert len(expected) == 11
    app = create_app(start_runtime=False)
    try:
        schema = app.openapi()["paths"]
        for method, path in expected:
            assert method.casefold() in schema[path]
        for method, path in P8_EFFECT_PATHS:
            assert method.casefold() in schema[path]

        compatibility_index = next(
            index for index, route in enumerate(app.routes) if getattr(route, "name", None) == "legacy-compatibility"
        )
        for expected_router in (database_router, discover_router, secondary_router):
            indexes = [
                index
                for index, route in enumerate(app.routes)
                if getattr(route, "original_router", None) is expected_router
            ]
            assert len(indexes) == 1 and indexes[0] < compatibility_index
    finally:
        app.state.container.close()


def test_user_activity_and_collection_events_use_the_native_future_domain_gateway() -> None:
    source = APP_FACTORY.read_text(encoding="utf-8")
    assert "LegacyFutureDomainEventGateway" not in source
    assert "P8UserEventGateway" in source
    assert "P8CollectionRemovalEventGateway" in source
    assert "P8FutureDomainEventGateway" in source
