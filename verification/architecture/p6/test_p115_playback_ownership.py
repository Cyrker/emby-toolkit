from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
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
RETIRED_CONFIG_REPLACEMENTS = {
    "p115_copy_play_active_sources": "etk.domains.p115.playback_repository.PostgresP115PlaybackRepository",
    "p115_copy_play_clones": "etk.domains.p115.playback_repository.PostgresP115PlaybackRepository",
    "p115_play_pool_sessions": "etk.domains.p115.playback_repository.PostgresP115PlaybackRepository",
    "p115_play_pool_user_cookie_rewards": None,
    "p115_virtual_play_sessions": "etk.domains.p115.playback_repository.PostgresP115PlaybackRepository",
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


def test_playback_router_preserves_fifteen_bindings_and_retires_user_contributions() -> None:
    fixture = _json(PLAYBACK_FIXTURE)
    expected = {(str(entry["method"]), str(entry["path"])) for entry in fixture["routes"]}
    observed = {
        (method, route.path.replace("{filename:path}", "{filename}"))
        for route in router.routes
        if isinstance(route, APIRoute)
        for method in route.methods or set()
    }

    assert len(expected) == 15
    assert observed == expected
    assert {route.endpoint.__module__ for route in router.routes if isinstance(route, APIRoute)} == {PLAYBACK_OWNER}

    manifest = _json("verification/contracts/route-ownership.yaml")
    assert len(manifest["routes"]) == 343
    assert len(manifest["method_bindings"]) == 367
    bindings = {
        (str(entry["method"]), str(entry["path"])): entry
        for entry in manifest["method_bindings"]
        if entry["status"] == "new"
        and str(entry["path"]).startswith(("/api/p115/play/", "/api/p115/play_pool", "/api/p115/temp_dir_config"))
    }
    expected_legacy = {(method, _legacy_path(path)) for method, path in expected}
    assert set(bindings) == expected_legacy
    for identity in expected_legacy:
        entry = bindings[identity]
        assert entry["status"] == "new"
        assert entry["new_owner"] == PLAYBACK_OWNER
        assert entry["fixture"] == PLAYBACK_FIXTURE

    retired = [entry for entry in manifest["method_bindings"] if "/play_pool/user-account" in entry["path"]]
    assert len(retired) == 4
    assert all(entry["status"] == "retired-approved" and entry["retirement_approval"] for entry in retired)


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
    assert scripts.get_heads() == ["0028_media_notice_batches"]


def test_native_playback_config_and_approved_state_replacements_are_claimed() -> None:
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
    for key, owner in RETIRED_CONFIG_REPLACEMENTS.items():
        entry = settings[key]
        assert entry["status"] == "retired-approved"
        assert entry["new_owner"] == owner
        assert entry["retirement_approval"]


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

    for relative_path in ("src/etk/bootstrap/app_factory.py", "src/etk/processes/worker.py"):
        source = _source(relative_path)
        assert "persistent_playback_gateway" not in source


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


def test_native_daily_speedtest_wiring_is_processor_free_and_import_clean() -> None:
    worker = _source("src/etk/processes/worker.py")
    app_factory = _source("src/etk/bootstrap/app_factory.py")
    provider = _source("src/etk/domains/p115/playback_native.py")
    maintenance_transport = _source("src/etk/domains/p115/maintenance_transport.py")
    assert '"play-pool-daily-speedtest"' in worker
    assert "native_playback_recovery_unsupported" not in provider
    for legacy_symbol in (
        "LegacyP115DirectLinkGateway",
        "LegacyP115PlaybackProvider",
        "LegacyP115PlaybackSourceGateway",
        "LegacyP115PlaybackUserGateway",
    ):
        assert legacy_symbol not in worker
        assert legacy_symbol not in app_factory
    for operation_kind in ("PREPARE", "ENSURE_TEMP_DIR", "SPEEDTEST", "RECLAIM"):
        assert f"PlaybackOperationKind.{operation_kind}" in provider
    assert 'refresh_token=""' in maintenance_transport
    assert "refresh_token=snapshot.refresh_token" not in maintenance_transport
    for symbol in (
        "NativeP115PlaybackProvider",
        "NativeP115PlaybackRapidSigner",
        "NativeP115PoolPlaybackClientFactory",
        "SqlAlchemyP115PlaybackUserRepository",
        "SqlAlchemyP115PlaybackSourceGateway",
        "SqlAlchemyP115PlaybackUserGateway",
        "playback=playback_service",
    ):
        assert symbol in worker
    for symbol in (
        "NativeP115DirectLinkGateway",
        "SqlAlchemyP115PlaybackConfigBackend",
        "SqlAlchemyP115PlaybackUserRepository",
        "SqlAlchemyP115PlaybackSourceGateway",
        "SqlAlchemyP115PlaybackUserGateway",
    ):
        assert symbol in app_factory

    program = """
import json
import sys
from etk.domains.p115.playback_native import (
    NativeP115DirectLinkGateway,
    NativeP115PlaybackProvider,
    NativeP115PlaybackRapidSigner,
    NativeP115PoolPlaybackClientFactory,
    SqlAlchemyP115PlaybackSourceGateway,
)
from etk.domains.p115.playback_user_repository import (
    SqlAlchemyP115PlaybackUserRepository,
    SqlAlchemyP115PlaybackUserGateway,
)
forbidden = ('database', 'handler', 'routes', 'task_manager', 'tasks')
loaded = sorted(
    name for name in sys.modules
    if any(name == prefix or name.startswith(prefix + '.') for prefix in forbidden)
)
print(json.dumps({
    'loaded': loaded,
    'direct_links': NativeP115DirectLinkGateway.__name__,
    'provider': NativeP115PlaybackProvider.__name__,
    'signer': NativeP115PlaybackRapidSigner.__name__,
    'clients': NativeP115PoolPlaybackClientFactory.__name__,
    'sources': SqlAlchemyP115PlaybackSourceGateway.__name__,
    'identities': SqlAlchemyP115PlaybackUserRepository.__name__,
    'users': SqlAlchemyP115PlaybackUserGateway.__name__,
}))
"""
    environment = os.environ.copy()
    python_path = [str(REPO_ROOT / "src"), str(REPO_ROOT)]
    if environment.get("PYTHONPATH"):
        python_path.append(environment["PYTHONPATH"])
    environment["PYTHONPATH"] = os.pathsep.join(python_path)
    completed = subprocess.run(
        [sys.executable, "-c", program],
        cwd=REPO_ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(completed.stdout) == {
        "loaded": [],
        "direct_links": "NativeP115DirectLinkGateway",
        "provider": "NativeP115PlaybackProvider",
        "signer": "NativeP115PlaybackRapidSigner",
        "clients": "NativeP115PoolPlaybackClientFactory",
        "sources": "SqlAlchemyP115PlaybackSourceGateway",
        "identities": "SqlAlchemyP115PlaybackUserRepository",
        "users": "SqlAlchemyP115PlaybackUserGateway",
    }
