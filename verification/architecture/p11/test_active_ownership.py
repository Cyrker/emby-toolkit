from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]

RETIRED_TABLES = {
    "shared_credit_ledger_local",
    "shared_credit_snapshot",
}
RETIRED_SETTINGS = {
    "license_key",
    "media_cleanup_delete_delay",
    "media_cleanup_keep_one_per_res",
    "media_cleanup_library_ids",
    "media_cleanup_rules",
    "p115_copy_play_active_sources",
    "p115_copy_play_clones",
    "p115_play_pool_sessions",
    "p115_play_pool_user_cookie_rewards",
    "p115_virtual_play_sessions",
    "stream_feature_mapping",
}


def _manifest(name: str) -> dict[str, Any]:
    return json.loads((REPO_ROOT / "verification" / "contracts" / name).read_text(encoding="utf-8"))


def _assert_closed(entries: list[dict[str, Any]]) -> None:
    assert all(entry["status"] != "legacy" for entry in entries)
    for entry in entries:
        if entry["status"] == "new":
            assert entry["new_owner"]
        elif entry["status"] == "retired-approved":
            assert entry["retirement_approval"]
        else:
            raise AssertionError(f"unexpected ownership status: {entry['status']!r}")


def test_route_task_table_config_and_frontend_active_ownership_is_closed() -> None:
    routes = _manifest("route-ownership.yaml")
    tasks = _manifest("task-ownership.yaml")
    tables = _manifest("table-ownership.yaml")
    config = _manifest("config-ownership.yaml")
    frontend = _manifest("frontend-route-ownership.yaml")

    for entries in (
        routes["routes"],
        routes["method_bindings"],
        tasks["tasks"],
        tables["tables"],
        config["bootstrap"],
        config["dynamic"],
        config["standalone_app_settings"],
        frontend["routes"],
    ):
        _assert_closed(entries)
    assert {entry["name"] for entry in tables["tables"] if entry["status"] == "retired-approved"} == (RETIRED_TABLES)
    assert {
        entry["key"] for entry in config["standalone_app_settings"] if entry["status"] == "retired-approved"
    } == RETIRED_SETTINGS
    assert config["unresolved_dynamic_setting_calls"] == []


def test_native_role_import_graph_does_not_load_legacy_runtime_modules() -> None:
    program = """
import json
import sys
import etk.bootstrap.app_factory
import etk.processes.coordinator
import etk.processes.proxy
import etk.processes.worker
forbidden = (
    'config_manager', 'database', 'extensions', 'handler', 'routes',
    'scheduler_manager', 'task_manager', 'tasks',
)
loaded = sorted(
    name for name in sys.modules
    if any(name == prefix or name.startswith(prefix + '.') for prefix in forbidden)
)
print(json.dumps(loaded))
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

    assert json.loads(completed.stdout) == []


def test_runtime_context_and_translation_cache_have_native_wiring_sentinels() -> None:
    coordinator = (REPO_ROOT / "src/etk/processes/coordinator.py").read_text(encoding="utf-8")
    worker = (REPO_ROOT / "src/etk/processes/worker.py").read_text(encoding="utf-8")

    assert "import config_manager" not in coordinator
    assert "from database import settings_db" not in coordinator
    assert "import extensions" not in coordinator
    assert "SqlAlchemyAppSettingsRepository(container.database.unit_of_work)" in coordinator
    assert "SqlAlchemyTranslationCache(database.unit_of_work)" in worker
    assert "ConfiguredBatchTranslation(settings_provider, translation_cache)" in worker


def test_active_worker_submission_and_p7_effect_paths_have_no_dynamic_legacy_fallback() -> None:
    jobs = (REPO_ROOT / "src/etk/domains/operations/jobs.py").read_text(encoding="utf-8")
    worker = (REPO_ROOT / "src/etk/processes/worker.py").read_text(encoding="utf-8")
    subscription_effects = (REPO_ROOT / "src/etk/processes/p7_subscription_effects.py").read_text(encoding="utf-8")
    resubscribe_cleanup = (REPO_ROOT / "src/etk/processes/p7_resubscribe_cleanup.py").read_text(encoding="utf-8")

    assert "importlib" not in jobs
    assert '"legacy_operation"' not in jobs
    assert "dynamic callable submission is disabled" in jobs
    assert "dynamic background callable submission is disabled" in jobs
    assert "from .p7_cleanup import" not in worker
    assert "import etk.processes.p7_cleanup" not in worker
    assert "task_manager" not in subscription_effects
    assert "task_manager" not in resubscribe_cleanup


def test_api_and_worker_active_adapters_do_not_reach_removed_compatibility_owners() -> None:
    app_factory = (REPO_ROOT / "src/etk/bootstrap/app_factory.py").read_text(encoding="utf-8")
    worker = (REPO_ROOT / "src/etk/processes/worker.py").read_text(encoding="utf-8")
    native_adapter_sources = (
        REPO_ROOT / "src/etk/domains/p115/directory_transport.py",
        REPO_ROOT / "src/etk/domains/p115/intro_sync_adapters.py",
        REPO_ROOT / "src/etk/domains/p115/rename_renderer.py",
        REPO_ROOT / "src/etk/domains/media/discover_defaults.py",
    )
    retired_symbols = (
        "DisabledCompatibilityApplication",
        "LegacyAssistantDefaultsAdapter",
        "LegacyDiscoverDefaults",
        "LegacyP115DirectoryGateway",
        "LegacyP115DirectoryTreeGateway",
        "LegacyP115IntroSnapshotExporter",
        "LegacyP115IntroSyncFeature",
        "LegacyP115PlanNamingGateway",
        "LegacyP115RenamePreviewGateway",
        "LegacyP115RuleRecognitionGateway",
        "LegacyP115SortingRuleDirectoryGateway",
        "clear_persistent_intro_detection_gateway",
        "clear_persistent_media_effect_gateway",
        "clear_persistent_p115_credential_gateway",
        "clear_persistent_playback_gateway",
        "install_persistent_intro_detection_gateway",
        "install_persistent_p115_credential_gateway",
        "install_persistent_playback_gateway",
        "invalidate_legacy_p115_credential_clients",
    )

    assert "compatibility_app" not in app_factory
    for symbol in retired_symbols:
        assert symbol not in app_factory
        assert symbol not in worker
    for source in native_adapter_sources:
        text = source.read_text(encoding="utf-8")
        assert "from handler" not in text
        assert "from database" not in text
        assert "from services" not in text


def test_refactor_image_context_excludes_retired_runtime_sources() -> None:
    ignored = {
        line.strip()
        for line in (REPO_ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }

    assert {
        "web_app.py",
        "pyarmor_runtime_000000/",
        "src/etk/bootstrap/legacy_runtime.py",
        "src/etk/compatibility/flask_app.py",
        "src/etk/compatibility/legacy_jobs.py",
    } <= ignored
