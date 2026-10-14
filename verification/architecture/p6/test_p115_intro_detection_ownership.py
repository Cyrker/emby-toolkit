from __future__ import annotations

import ast
import json
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

from etk.platform.tasks.catalog import P6_P115_NATIVE_TASK_KEYS
from etk.processes.p6_intro_detection import EXECUTE_P115_INTRO_DETECTION_OPERATION_TASK

REPO_ROOT = Path(__file__).resolve().parents[3]
INTRO_FIXTURE = "verification/fixtures/http/p6-p115-intro-sync.json"
INTRO_BACKFILL_FIXTURE = "verification/fixtures/http/p11-p115-intro-backfill.json"
LEGACY_INTRO_TABLES = {
    "p115_intro_detection_failures",
    "p115_intro_fingerprint_cache",
    "p115_intro_fingerprint_failures",
}


def _source(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def _contract(relative_path: str) -> dict[str, object]:
    return json.loads(_source(relative_path))


def test_intro_detection_migration_declares_durable_state_and_revision_chain_has_one_head() -> None:
    migration = _source("migrations/versions/0012_p6_intro_detection.py")
    tree = ast.parse(migration)
    operation_table = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "create_table"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and node.args[0].value == "p115_intro_detection_operations"
    )
    columns = {
        str(argument.args[0].value)
        for argument in operation_table.args[1:]
        if isinstance(argument, ast.Call)
        and isinstance(argument.func, ast.Attribute)
        and argument.func.attr == "Column"
        and argument.args
        and isinstance(argument.args[0], ast.Constant)
    }

    assert 'revision: str = "0012_p6_intro_detection"' in migration
    assert 'down_revision: str | Sequence[str] | None = "0011_p6_playback"' in migration
    assert columns >= {
        "id",
        "event_sequence",
        "operation_key",
        "request_hash",
        "source_set_revision",
        "policy_revision",
        "request_json",
        "status",
        "phase",
        "state_revision",
        "progress_json",
        "attempts",
        "available_at",
        "claimed_by",
        "claim_token",
        "lease_expires_at",
        "cancel_requested_at",
        "result_json",
        "error_json",
        "finished_at",
    }
    assert "uq_p115_intro_active_season" in migration
    assert "p115.intro-detection.requested" in migration
    scripts = ScriptDirectory.from_config(Config(str(REPO_ROOT / "alembic.ini")))
    assert scripts.get_heads() == ["0028_media_notice_batches"]


def test_intro_detection_runtime_is_wired_through_worker_coordinator_celery_and_gateway() -> None:
    worker = _source("src/etk/processes/worker.py")
    coordinator = _source("src/etk/processes/coordinator.py")
    celery = _source("src/etk/processes/celery_app.py")
    app_factory = _source("src/etk/bootstrap/app_factory.py")

    assert "P115IntroDetectionOperationExecutor" in worker
    assert "PostgresP115IntroRuntimeSource" in worker
    assert "NativeP115IntroDirectUrlGateway" in worker
    assert "SharedIntroExportEffectDelivery" in worker
    assert "LegacyP7IntroRuntimeSource" not in worker
    assert "LegacyP9IntroChaptersChangedHook" not in worker
    assert "execute_p115_intro_detection_operation" in worker
    assert "persistent_intro_detection_gateway" not in worker
    assert "P115IntroDetectionCoordinator" in coordinator
    assert "intro_result = intro.tick()" in coordinator
    assert EXECUTE_P115_INTRO_DETECTION_OPERATION_TASK in celery
    assert "execute_p115_intro_detection_operation" in celery
    assert '"p115.intro-detection-gateway"' in app_factory
    assert '"p115.intro-runtime"' in app_factory
    assert "PostgresP115IntroRuntimeSource" in app_factory
    assert "LegacyP7IntroRuntimeSource" not in app_factory
    assert "persistent_intro_detection_gateway" not in app_factory


def test_intro_task_and_legacy_tables_have_native_p6_10_ownership_without_contract_drift() -> None:
    assert "intro-fingerprint-backfill" in P6_P115_NATIVE_TASK_KEYS
    tasks = _contract("verification/contracts/task-ownership.yaml")
    task = next(
        entry
        for entry in tasks["tasks"]  # type: ignore[index]
        if entry["task_key"] == "intro-fingerprint-backfill"
    )
    assert task["status"] == "new"
    assert task["new_owner"] == "etk.processes.p6_p115_tasks.P6P115TaskRouter"
    assert task["processor_type"] == "watchlist"
    assert task["ownership_layers"] == {
        "envelope_owner": "etk.processes.p6_p115_tasks.P6P115TaskRouter",
        "execution_kind": "native-use-case",
        "execution_owner": ("etk.domains.p115.intro_backfill.P115IntroBackfillTaskService"),
    }

    tables = _contract("verification/contracts/table-ownership.yaml")
    owned = {
        entry["name"]: entry
        for entry in tables["tables"]  # type: ignore[index]
        if entry["name"] in LEGACY_INTRO_TABLES
    }
    assert set(owned) == LEGACY_INTRO_TABLES
    assert all(entry["status"] == "new" for entry in owned.values())
    assert all(
        entry["new_owner"] == "etk.domains.p115.intro_detection_repository.PostgresP115IntroDetectionRepository"
        for entry in owned.values()
    )
    assert all(
        entry["fixture"] == "verification/integration/p6/test_p115_intro_detection_postgres18.py"
        for entry in owned.values()
    )


def test_intro_backfill_http_binding_is_native_and_only_submits_the_durable_task() -> None:
    routes = _contract("verification/contracts/route-ownership.yaml")
    bindings = [
        entry
        for entry in routes["method_bindings"]  # type: ignore[index]
        if entry["method"] == "POST" and entry["path"] == "/api/emby/intro-detection/backfill"
    ]
    assert len(bindings) == 1
    assert bindings[0]["status"] == "new"
    assert bindings[0]["new_owner"] == "etk.domains.p115.intro_backfill_router"
    assert bindings[0]["fixture"] == INTRO_BACKFILL_FIXTURE

    route = _source("src/etk/domains/p115/intro_backfill_router.py")
    assert '"intro-fingerprint-backfill"' in route
    assert '{"force": False}' in route
    assert "enqueue_active_backfill" not in route
    assert "handler" not in route
    assert "tasks." not in route


def test_intro_sync_owns_two_native_auth_none_contracts_with_fixture_and_optional_item_id() -> None:
    bridge = _contract("verification/contracts/bridge-contract-ownership.yaml")
    contracts = [
        entry
        for entry in bridge["contracts"]  # type: ignore[index]
        if str(entry["path"]).endswith("/intro-sync")
    ]
    assert len(contracts) == 2
    assert all(entry["status"] == "new" for entry in contracts)
    assert all(entry["new_owner"] == "etk.domains.p115.router" for entry in contracts)
    assert all(entry["auth"] == "none" for entry in contracts)
    assert all(entry["request_fields"] == ["path.identity", "query.emby_item_id?"] for entry in contracts)
    assert all(entry["fixtures"] == [INTRO_FIXTURE] for entry in contracts)


def test_intro_native_core_has_typed_p7_reads_and_durable_p9_effect_without_legacy_imports() -> None:
    core = _source("src/etk/domains/p115/intro_detection.py")
    repository = _source("src/etk/domains/p115/intro_detection_repository.py")
    adapters = _source("src/etk/domains/p115/intro_detection_adapters.py")
    runtime = _source("src/etk/domains/p115/intro_runtime.py")
    gateway = _source("src/etk/domains/p115/intro_gateway.py")
    effect = _source("src/etk/domains/sharing/intro_effects.py")
    combined = "\n".join((core, repository, adapters, runtime, gateway, effect))

    assert "watchlist_db" not in combined
    assert "settings_db" not in combined
    assert "SharedCenterClient" not in combined
    assert "Telegram" not in combined
    assert "handler.shared_intro_service" not in combined
    assert "from handler" not in combined
    assert "from database" not in combined
    assert "https://" not in repr(json.loads(json.dumps({"core": core}))).lower()
    assert "IntroDetectionPolicyPort" in core
    assert "PostgresP115IntroRuntimeSource" in runtime
    assert 'SHARED_INTRO_EXPORT_EFFECT = "sharing.intro.export"' in effect
