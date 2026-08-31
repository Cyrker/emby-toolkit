from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any

from fastapi.routing import APIRoute

from etk.domains.p115.router import router

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = "verification/fixtures/http/p6-p115-mediainfo.json"
INGESTION_FIXTURE = "verification/fixtures/http/p6-p115-ingestion-delete.json"
INTRO_FIXTURE = "verification/fixtures/http/p6-p115-intro-sync.json"

P6_6_NATIVE_OPERATIONS = {
    ("GET", "/api/p115/mediainfo/{pick_code}"),
    ("GET", "/api/p115/mediainfo/sha1/{sha1}"),
    ("GET", "/api/p115/mediainfo/{pick_code}/metadata"),
    ("GET", "/api/p115/mediainfo/sha1/{sha1}/metadata"),
    ("POST", "/api/p115/mediainfo/{pick_code}/metadata/backfill"),
    ("POST", "/api/p115/mediainfo/sha1/{sha1}/metadata/backfill"),
}
P6_7_NATIVE_OPERATIONS = {
    ("POST", "/api/p115/mediainfo/{pick_code}/item-ready"),
    ("POST", "/api/p115/mediainfo/sha1/{sha1}/item-ready"),
    ("POST", "/api/p115/mediainfo/{pick_code}/deep-delete/prepare"),
    ("POST", "/api/p115/mediainfo/sha1/{sha1}/deep-delete/prepare"),
    ("POST", "/api/p115/mediainfo/{pick_code}/deep-delete/commit"),
    ("POST", "/api/p115/mediainfo/sha1/{sha1}/deep-delete/commit"),
}
P6_10_NATIVE_OPERATIONS = {
    ("POST", "/api/p115/mediainfo/{pick_code}/intro-sync"),
    ("POST", "/api/p115/mediainfo/sha1/{sha1}/intro-sync"),
}
NATIVE_OPERATIONS = P6_6_NATIVE_OPERATIONS | P6_7_NATIVE_OPERATIONS | P6_10_NATIVE_OPERATIONS
DEFERRED_PATHS: set[str] = set()


def _source(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def _contract(relative_path: str) -> dict[str, Any]:
    return json.loads(_source(relative_path))


def test_p6_10_has_exactly_fourteen_native_mediainfo_http_operations() -> None:
    observed = {
        (method, route.path)
        for route in router.routes
        if isinstance(route, APIRoute) and route.path.startswith("/api/p115/mediainfo/")
        for method in route.methods or set()
    }

    assert observed == NATIVE_OPERATIONS
    deferred_native_paths = {
        path.replace("<pick_code>", "{pick_code}").replace("<sha1>", "{sha1}") for path in DEFERRED_PATHS
    }
    assert deferred_native_paths.isdisjoint({path for _method, path in observed})


def test_p6_10_route_and_bridge_ownership_are_fully_native() -> None:
    routes = _contract("verification/contracts/route-ownership.yaml")
    bindings = {
        (str(entry["method"]), str(entry["path"])): entry
        for entry in routes["method_bindings"]  # type: ignore[index]
        if str(entry["path"]).startswith("/api/p115/mediainfo/")
    }
    for method, path in {
        (method, path.replace("{pick_code}", "<pick_code>").replace("{sha1}", "<sha1>"))
        for method, path in NATIVE_OPERATIONS
    }:
        entry = bindings[(method, path)]
        assert entry["status"] == "new"
        assert entry["new_owner"] == "etk.domains.p115.router"
        native_path = path.replace("<pick_code>", "{pick_code}").replace("<sha1>", "{sha1}")
        expected_fixture = (
            FIXTURE
            if (method, native_path) in P6_6_NATIVE_OPERATIONS
            else INTRO_FIXTURE
            if (method, native_path) in P6_10_NATIVE_OPERATIONS
            else INGESTION_FIXTURE
        )
        assert entry["fixture"] == expected_fixture
    for path in DEFERRED_PATHS:
        entry = bindings[("POST", path)]
        assert entry["status"] == "legacy"
        assert entry["new_owner"] is None
        assert entry["fixture"] is None
        assert entry["validation_method"] == "static-source-contract-deferred"

    bridge = _contract("verification/contracts/bridge-contract-ownership.yaml")
    contracts = {
        (str(entry["method"]), str(entry["path"])): entry
        for entry in bridge["contracts"]  # type: ignore[index]
        if str(entry["path"]).startswith("/api/p115/mediainfo/")
    }
    for method, path in {
        (method, path.replace("{pick_code}", "<pick_code>").replace("{sha1}", "<sha1>"))
        for method, path in NATIVE_OPERATIONS
    }:
        entry = contracts[(method, path)]
        assert entry["status"] == "new"
        assert entry["new_owner"] == "etk.domains.p115.router"
        native_path = path.replace("<pick_code>", "{pick_code}").replace("<sha1>", "{sha1}")
        expected_fixture = (
            FIXTURE
            if (method, native_path) in P6_6_NATIVE_OPERATIONS
            else INTRO_FIXTURE
            if (method, native_path) in P6_10_NATIVE_OPERATIONS
            else INGESTION_FIXTURE
        )
        assert entry["fixtures"] == [expected_fixture]
    for path in DEFERRED_PATHS:
        entry = contracts[("POST", path)]
        assert entry["status"] == "legacy"
        assert entry["new_owner"] is None
        assert entry["fixtures"] == []
        assert entry["validation_method"] == "static-source-contract-deferred"


def test_p6_6_task_and_table_ownership_are_native_and_fixture_backed() -> None:
    tasks = _contract("verification/contracts/task-ownership.yaml")
    tasks_by_key = {str(entry["task_key"]): entry for entry in tasks["tasks"]}  # type: ignore[index]
    repair = tasks_by_key["repair-p115-fingerprints"]
    assert repair["status"] == "new"
    assert repair["new_owner"] == "etk.processes.p6_p115_tasks.P6P115TaskRouter"
    assert repair["ownership_layers"]["execution_owner"] == (
        "etk.domains.p115.fingerprints.P115FingerprintRepairService"
    )
    assert repair["validation_method"] == (
        "ordered-task-registry-native-http-router-fingerprint-postgres18-redis-celery-and-import-sentinel"
    )
    assert "account- and credential-fenced HTTP" in repair["notes"]
    restore = tasks_by_key["restore_mediainfo"]
    assert restore["status"] == "new"
    assert restore["new_owner"] == "etk.processes.p5_media_tasks.P5MediaTaskRouter"
    assert restore["ownership_layers"] == {
        "envelope_owner": "etk.processes.p5_media_tasks.P5MediaTaskRouter",
        "execution_kind": "native-use-case",
        "execution_owner": "etk.domains.p115.media_info_restore.P115MediaInfoRestoreService",
    }
    assert restore["validation_method"] == (
        "ordered-task-registry-native-sdk-direct-url-native-only-worker-postgres18-effect-redis-celery-and-"
        "import-sentinel"
    )
    assert "low-level p115client direct-URL reads" in restore["notes"]

    tables = _contract("verification/contracts/table-ownership.yaml")
    cache = next(entry for entry in tables["tables"] if entry["name"] == "p115_mediainfo_cache")  # type: ignore[index]
    assert cache["status"] == "new"
    assert cache["new_owner"] == ("etk.domains.p115.media_info_repository.SqlAlchemyP115MediaInfoRepository")
    assert cache["fixture"] == "verification/integration/p6/test_p115_media_info_postgres.py"


def test_p6_6_migration_owns_revisioned_cache_and_internal_probe_worklist() -> None:
    migration = _source("migrations/versions/0008_p6_mediainfo_cache.py")
    tree = ast.parse(migration)
    create_probe = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "op"
        and node.func.attr == "create_table"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and node.args[0].value == "p115_media_probe_items"
    )
    probe_columns = {
        str(argument.args[0].value)
        for argument in create_probe.args[1:]
        if isinstance(argument, ast.Call)
        and isinstance(argument.func, ast.Attribute)
        and argument.func.attr == "Column"
        and argument.args
        and isinstance(argument.args[0], ast.Constant)
    }
    cache_columns = {
        str(node.args[1].args[0].value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "add_column"
        and len(node.args) >= 2
        and isinstance(node.args[0], ast.Constant)
        and node.args[0].value == "p115_mediainfo_cache"
        and isinstance(node.args[1], ast.Call)
        and node.args[1].args
        and isinstance(node.args[1].args[0], ast.Constant)
    }

    assert 'down_revision: str | Sequence[str] | None = "0007_p6_strm_sync"' in migration
    assert cache_columns == {
        "updated_at",
        "cache_revision",
        "raw_schema_version",
        "formatter_revision",
        "probe_revision",
    }
    assert probe_columns >= {
        "id",
        "operation_key",
        "provenance_key",
        "source",
        "account_id",
        "sha1",
        "pick_code",
        "file_name",
        "status",
        "attempts",
        "claim_token",
        "lease_expires_at",
        "expected_cache_revision",
        "provider_backend",
        "result_json",
        "last_error_json",
        "finished_at",
    }
    assert "p115_mediainfo_cache_revision_guard" in migration
    assert "trg_p115_mediainfo_cache_revision_guard" in migration
    assert "uq_p115_media_probe_operation_sha1_provenance" in migration


def test_p6_6_native_business_services_do_not_import_legacy_task_or_cache_owners() -> None:
    for relative_path in (
        "src/etk/domains/p115/direct_url_transport.py",
        "src/etk/domains/p115/fingerprints.py",
        "src/etk/domains/p115/fingerprint_transport.py",
        "src/etk/domains/p115/media_info.py",
        "src/etk/domains/p115/media_info_repository.py",
        "src/etk/domains/p115/media_info_restore.py",
        "src/etk/domains/p115/media_probe.py",
    ):
        source = _source(relative_path)
        assert "tasks.media" not in source
        assert "P115CacheManager" not in source
        assert "_extract_115_fingerprints" not in source


def test_fingerprint_worker_uses_native_http_provider_without_legacy_runtime_fallback() -> None:
    transport = _source("src/etk/domains/p115/fingerprint_transport.py")
    worker = _source("src/etk/processes/worker.py")

    assert "from p115pickcode import to_id" in transport
    assert "p115client" not in transport
    assert "import_module" not in transport
    assert "handler.p115_service" not in transport
    assert "LegacyP115FingerprintRemoteGateway" not in worker
    assert worker.count("HttpxP115FingerprintRemoteGateway(") == 2


def test_mediainfo_and_intro_use_native_sdk_direct_urls_in_both_worker_profiles() -> None:
    transport = _source("src/etk/domains/p115/direct_url_transport.py")
    worker = _source("src/etk/processes/worker.py")

    tree = ast.parse(transport)
    imported_roots = {
        alias.name.split(".", 1)[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
    } | {
        str(node.module or "").split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.level == 0
    }
    assert imported_roots.isdisjoint({"database", "extensions", "handler", "routes", "task_manager", "tasks"})
    assert "\nfrom p115client import" not in transport
    assert "    from p115client import P115OpenClient as Client" in transport
    assert "    from p115client import P115Client as Client" in transport
    assert 'refresh_token=""' in transport
    assert 'app="os_windows"' in transport
    assert ".download_url_info_open(" in transport
    assert ".download_url_app(" in transport
    assert ".download_url_open(" not in transport
    assert ".download_url(" not in transport
    assert worker.count("P115SdkDirectUrlGateway(") == 6


def test_legacy_webhook_and_consistency_seams_only_submit_the_p6_native_repair_task() -> None:
    expected = {
        "routes/webhook.py": "_repair_webhook_p115_fingerprints_for_emby_ids",
        "tasks/helpers.py": "_submit_native_p115_fingerprint_repair",
    }
    for relative_path, function_name in expected.items():
        tree = ast.parse(_source(relative_path))
        function = next(
            node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function_name
        )
        imported_names = {
            (str(node.module or ""), alias.name)
            for node in ast.walk(function)
            if isinstance(node, ast.ImportFrom)
            for alias in node.names
        }
        called_attributes = {
            node.func.attr
            for node in ast.walk(function)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        identifiers = {node.id for node in ast.walk(function) if isinstance(node, ast.Name)}

        media_module = "tasks.media" if relative_path.startswith("routes/") else "media"
        assert (media_module, "task_repair_p115_fingerprints") in imported_names
        assert "submit_persistent_task" in called_attributes
        assert "submit_task" not in called_attributes
        assert "persistent_background_gateway_installed" not in identifiers
        assert "repair_p115_fingerprints_for_rows" not in identifiers
        assert "_extract_115_fingerprints" not in identifiers
