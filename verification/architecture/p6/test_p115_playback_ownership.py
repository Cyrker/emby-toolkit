from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any

from alembic.config import Config
from alembic.script import ScriptDirectory
from fastapi.routing import APIRoute

from etk.domains.p115.playback_router import router

REPO_ROOT = Path(__file__).resolve().parents[3]
PLAYBACK_FIXTURE = "verification/fixtures/http/p6-p115-playback.json"
PLAYBACK_OWNER = "etk.domains.p115.playback_router"
PLAYBACK_TABLES = {
    "p115_playback_operations",
    "p115_playback_artifacts",
    "p115_playback_leases",
    "p115_playback_qr_sessions",
}
NATIVE_CONFIG_KEYS = {
    "p115_play_pool_config",
    "p115_temp_dir_config",
}
DEFERRED_COMPATIBILITY_KEYS = {
    "p115_copy_play_active_sources",
    "p115_copy_play_clones",
    "p115_play_pool_sessions",
    "p115_play_pool_user_cookie_rewards",
    "p115_virtual_play_sessions",
}


def _source(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8-sig")


def _json(relative_path: str) -> dict[str, Any]:
    return json.loads(_source(relative_path))


def _legacy_path(path: str) -> str:
    return path.replace("{filename}", "<path:filename>").replace("{", "<").replace("}", ">")


def _function(relative_path: str, name: str) -> ast.FunctionDef | ast.AsyncFunctionDef:
    tree = ast.parse(_source(relative_path))
    return next(
        node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name
    )


def _called_attributes(function: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    return {
        node.func.attr
        for node in ast.walk(function)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }


def _called_names(function: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    return {
        node.func.id for node in ast.walk(function) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }


def test_playback_router_and_committed_legacy_manifest_have_exactly_nineteen_native_bindings() -> None:
    fixture = _json(PLAYBACK_FIXTURE)
    expected = {(str(entry["method"]), str(entry["path"])) for entry in fixture["routes"]}
    observed = {
        (method, route.path.replace("{filename:path}", "{filename}"))
        for route in router.routes
        if isinstance(route, APIRoute)
        for method in route.methods or set()
    }

    assert len(expected) == 19
    assert observed == expected
    assert {route.endpoint.__module__ for route in router.routes if isinstance(route, APIRoute)} == {PLAYBACK_OWNER}

    manifest = _json("verification/contracts/route-ownership.yaml")
    assert len(manifest["routes"]) == 343
    assert len(manifest["method_bindings"]) == 367
    bindings = {
        (str(entry["method"]), str(entry["path"])): entry
        for entry in manifest["method_bindings"]
        if str(entry["path"]).startswith(("/api/p115/play/", "/api/p115/play_pool", "/api/p115/temp_dir_config"))
    }
    expected_legacy = {(method, _legacy_path(path)) for method, path in expected}
    assert set(bindings) == expected_legacy
    for identity in expected_legacy:
        entry = bindings[identity]
        assert entry["status"] == "new"
        assert entry["new_owner"] == PLAYBACK_OWNER
        assert entry["fixture"] == PLAYBACK_FIXTURE


def test_playback_openapi_snapshot_has_exact_operations_and_transport_responses() -> None:
    fixture = _json(PLAYBACK_FIXTURE)
    expected = {(str(entry["method"]).lower(), str(entry["path"])) for entry in fixture["routes"]}
    schema = _json("verification/contracts/http/p4-openapi.json")
    observed = {
        (method, path)
        for path, methods in schema["paths"].items()
        if path.startswith(("/api/p115/play/", "/api/p115/play_pool", "/api/p115/temp_dir_config"))
        for method in methods
    }

    assert observed == expected
    for method, path in expected:
        operation = schema["paths"][path][method]
        assert "422" not in operation["responses"]
    for path in ("/api/p115/play/{pick_code}", "/api/p115/play/{pick_code}/{filename}"):
        assert set(schema["paths"][path]["get"]["responses"]) == {
            "200",
            "206",
            "302",
            "404",
            "500",
            "default",
        }
        assert schema["paths"][path]["head"]["responses"] == {"200": {"description": "Legacy probe response"}}


def test_playback_migration_is_single_head_and_owns_exactly_four_native_tables() -> None:
    migration = _source("migrations/versions/0011_p6_playback.py")
    tree = ast.parse(migration)
    created_tables = {
        str(node.args[0].value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "create_table"
        and node.args
        and isinstance(node.args[0], ast.Constant)
    }

    assert 'revision: str = "0011_p6_playback"' in migration
    assert 'down_revision: str | Sequence[str] | None = "0010_p6_upload_monitor"' in migration
    assert created_tables == PLAYBACK_TABLES
    repository = _source("src/etk/domains/p115/playback_repository.py")
    assert all(table in repository for table in PLAYBACK_TABLES)
    scripts = ScriptDirectory.from_config(Config(str(REPO_ROOT / "alembic.ini")))
    assert scripts.get_heads() == ["0024_p11_sharing_cancellation"]


def test_only_actively_native_playback_config_keys_are_claimed() -> None:
    manifest = _json("verification/contracts/config-ownership.yaml")
    settings = {
        str(entry["key"]): entry
        for entry in manifest["standalone_app_settings"]
        if str(entry["key"]).startswith(
            (
                "p115_copy_play_",
                "p115_play_pool_",
                "p115_temp_dir_",
                "p115_virtual_play_",
            )
        )
    }

    for key in NATIVE_CONFIG_KEYS:
        entry = settings[key]
        assert entry["status"] == "new"
        assert entry["new_owner"] == "etk.domains.p115.playback.P115PlaybackConfigService"
        assert entry["fixture"] == PLAYBACK_FIXTURE
    for key in DEFERRED_COMPATIBILITY_KEYS:
        entry = settings[key]
        assert entry["status"] == "legacy"
        assert entry["new_owner"] is None


def test_active_legacy_playback_wrappers_check_the_native_gateway_before_fallback() -> None:
    expected_calls = {
        "handler/p115_play_pool.py": {
            "is_pool_enabled": "pool_enabled",
            "has_usable_pool": "pool_available",
            "has_usable_pool_for_user": "pool_available",
            "prepare_play_pool_pick_code": "prepare_pool",
            "get_direct_url": "pool_direct_url",
            "cleanup_expired_sessions": "cleanup",
            "cleanup_temp_dir_old_videos": "cleanup",
            "cleanup_for_playback_stop": "release_for_stop",
        },
        "handler/p115_copy_play.py": {
            "prepare_copy_play_pick_code": "prepare_copy",
            "cleanup_expired_clones": "cleanup",
            "cleanup_for_playback_stop": "release_for_stop",
        },
        "handler/p115_temp_dir.py": {
            "cleanup_all_temp_videos": "cleanup",
        },
        "reverse_proxy.py": {
            "_resolve_virtual_play_url": "resolve_virtual",
        },
    }
    for relative_path, functions in expected_calls.items():
        for function_name, gateway_method in functions.items():
            function = _function(relative_path, function_name)
            assert "persistent_playback_gateway" in _called_names(function)
            assert gateway_method in _called_attributes(function)

    for function_name in (
        "discard_copy_play_clone",
        "should_use_copy_play_for_source",
        "record_source_play",
        "recycle_clone_after_direct_url",
    ):
        function = _function("handler/p115_copy_play.py", function_name)
        assert "persistent_playback_gateway" in _called_names(function)

    role_sources = {
        "src/etk/bootstrap/app_factory.py": (
            "install_persistent_playback_gateway",
            "clear_persistent_playback_gateway",
        ),
        "src/etk/processes/worker.py": ("install_persistent_playback_gateway", "clear_persistent_playback_gateway"),
    }
    for relative_path, calls in role_sources.items():
        source = _source(relative_path)
        assert all(call in source for call in calls)


def test_p9_effects_and_proxy_transport_stay_outside_native_playback_core() -> None:
    forbidden_fragments = {
        "reverse_proxy",
        "send_telegram",
        "shared_center",
        "shared_credit_ledger",
        "shared_virtual_db",
    }
    for relative_path in (
        "src/etk/domains/p115/playback.py",
        "src/etk/domains/p115/playback_repository.py",
        "src/etk/domains/p115/playback_service.py",
        "src/etk/domains/p115/playback_router.py",
        "src/etk/processes/p6_playback.py",
    ):
        source = _source(relative_path).casefold()
        assert forbidden_fragments.isdisjoint(source.split())
        assert all(fragment not in source for fragment in forbidden_fragments)

    adapters = _source("src/etk/domains/p115/playback_adapters.py")
    assert "class LegacyP115PlaybackUserGateway" in adapters
    assert "send_telegram_message" in adapters
    assert "class LegacyP115PlaybackSourceGateway" in adapters
    assert "shared_virtual_db" in adapters
    assert "reverse_proxy" not in adapters
