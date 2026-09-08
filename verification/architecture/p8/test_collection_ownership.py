from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from etk.bootstrap.app_factory import create_app
from etk.domains.collection.router import router as collection_router

REPO_ROOT = Path(__file__).resolve().parents[3]
COLLECTION_ROOT = REPO_ROOT / "src/etk/domains/collection"
COLLECTION_REPOSITORY = COLLECTION_ROOT / "repository.py"
MEDIA_PROJECTION = REPO_ROOT / "src/etk/domains/media/collection_projection.py"
USER_PROJECTION = REPO_ROOT / "src/etk/domains/identity/collection_projection.py"
APP_FACTORY = REPO_ROOT / "src/etk/bootstrap/app_factory.py"
FIXTURE = REPO_ROOT / "verification/fixtures/http/p8-collection-cover.json"
LOCAL_SOURCES = (
    COLLECTION_ROOT / "local_gateway.py",
    COLLECTION_ROOT / "renderer.py",
    COLLECTION_REPOSITORY,
    COLLECTION_ROOT / "router.py",
    COLLECTION_ROOT / "service.py",
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
EFFECT_OPERATIONS = {
    ("GET", "/api/collections/provider/metadata/{tmdb_collection_id}"),
    ("GET", "/api/collections/provider/search"),
    ("DELETE", "/api/collections/{emby_collection_id}"),
    ("DELETE", "/api/custom_collections/{collection_id}"),
    ("POST", "/api/custom_collections/{collection_id}/fix_match"),
    ("GET", "/api/custom_collections/config/emby_libraries"),
    ("GET", "/api/custom_collections/config/tmdb_movie_genres"),
    ("GET", "/api/custom_collections/config/tmdb_tv_genres"),
    ("GET", "/api/custom_collections/config/tmdb_search_companies"),
    ("GET", "/api/custom_collections/config/tmdb_search_persons"),
    ("GET", "/api/config/cover_generator/libraries"),
}


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    result: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            result.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            result.add(node.module)
    return result


def test_collection_local_sources_do_not_reverse_import_legacy_runtime() -> None:
    for path in LOCAL_SOURCES:
        imports = _imports(path)
        assert not any(module.split(".")[0] in LEGACY_IMPORT_ROOTS for module in imports), path

    assert "services.cover_generator" not in (COLLECTION_ROOT / "renderer.py").read_text(encoding="utf-8")
    assert "LegacyCollectionGateway" not in APP_FACTORY.read_text(encoding="utf-8")


def test_collection_repository_keeps_cross_domain_tables_behind_owner_projections() -> None:
    repository_source = COLLECTION_REPOSITORY.read_text(encoding="utf-8")
    media_source = MEDIA_PROJECTION.read_text(encoding="utf-8")
    user_source = USER_PROJECTION.read_text(encoding="utf-8")

    assert "collections_info" in repository_source
    assert "custom_collections" in repository_source
    assert "media_metadata" not in repository_source
    assert "emby_users" not in repository_source
    assert "user_templates" not in repository_source
    assert "media_metadata" in media_source
    assert "collections_info" not in media_source
    assert "custom_collections" not in media_source
    assert "emby_users" in user_source
    assert "emby_users_extended" in user_source
    assert "user_templates" in user_source
    assert "media_metadata" not in user_source
    assert "collections_info" not in user_source


def test_collection_routes_and_effects_ignore_the_retired_compatibility_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ETK_ENABLE_LEGACY_COMPATIBILITY", "1")
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    expected = {(route["method"], route["path"]) for route in fixture["routes"]}
    app = create_app(start_runtime=False)
    try:
        schema = app.openapi()["paths"]
        for method, path in expected:
            assert method.casefold() in schema[path]
        for method, path in EFFECT_OPERATIONS:
            assert method.casefold() in schema[path]

        assert not any(getattr(route, "name", None) == "legacy-compatibility" for route in app.routes)
        router_indexes = {
            index
            for index, route in enumerate(app.routes)
            if getattr(route, "original_router", None) is collection_router
        }
        assert len(router_indexes) == 1
    finally:
        app.state.container.close()
