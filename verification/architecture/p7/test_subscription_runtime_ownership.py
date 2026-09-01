from __future__ import annotations

import ast
import json
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

REPO_ROOT = Path(__file__).resolve().parents[3]


def _source(path: str) -> str:
    return (REPO_ROOT / path).read_text(encoding="utf-8")


def test_subscription_runtime_migration_is_current_single_head_and_adds_only_native_tables() -> None:
    migration = _source("migrations/versions/0013_p7_subscription_runtime.py")
    tree = ast.parse(migration)
    created_tables = {
        call.args[0].value
        for call in ast.walk(tree)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
        and call.func.attr == "create_table"
        and call.args
        and isinstance(call.args[0], ast.Constant)
        and isinstance(call.args[0].value, str)
    }

    assert 'revision: str = "0013_p7_subscription_runtime"' in migration
    assert 'down_revision: str | Sequence[str] | None = "0012_p6_intro_detection"' in migration
    assert created_tables == {
        "subscription_operations",
        "subscription_quota_reservations",
        "subscription_runtime_states",
    }
    scripts = ScriptDirectory.from_config(Config(str(REPO_ROOT / "alembic.ini")))
    assert scripts.get_heads() == ["0025_p11_collection_activation"]


def test_subscription_runtime_repository_owns_quota_lock_and_state_cas_without_legacy_business_imports() -> None:
    source = _source("src/etk/domains/subscription/runtime_repository.py")

    assert "pg_advisory_xact_lock" in source
    assert "expected_revision" in source
    assert "subscription_quota_state" in source
    assert "handler.moviepilot" not in source
    assert "database.settings_db" not in source
    assert "web_app" not in source


def test_subscription_quota_projection_is_derived_and_direct_fallback_writes_fail_closed() -> None:
    runtime_source = _source("src/etk/domains/subscription/runtime_repository.py")
    runtime_tree = ast.parse(runtime_source)
    canonical_cap = next(
        node
        for node in ast.walk(runtime_tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "_canonical_cap"
    )
    canonical_source = ast.get_source_segment(runtime_source, canonical_cap) or ""

    assert "_projected_cap_for_date" not in runtime_source
    assert "subscription_quota_state" not in canonical_source
    assert "setting_key = 'mp_config'" in canonical_source

    settings_source = _source("database/settings_db.py")
    settings_tree = ast.parse(settings_source)
    getter = next(
        node
        for node in ast.walk(settings_tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "get_subscription_quota"
    )
    decrement = next(
        node
        for node in ast.walk(settings_tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "decrement_subscription_quota"
    )
    getter_source = ast.get_source_segment(settings_source, getter) or ""
    decrement_source = ast.get_source_segment(settings_source, decrement) or ""

    assert "quota_availability_dbapi" in getter_source
    assert "subscription_quota_state" not in getter_source
    assert "get_db_connection" not in decrement_source
    assert "_save_setting_with_cursor" not in decrement_source
    assert "return False" in decrement_source

    moviepilot_source = _source("handler/moviepilot.py")
    moviepilot_tree = ast.parse(moviepilot_source)
    subscribe = next(
        node
        for node in ast.walk(moviepilot_tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "subscribe_with_custom_payload"
    )
    subscribe_source = ast.get_source_segment(moviepilot_source, subscribe) or ""

    assert subscribe_source.index("if consume_quota:") < subscribe_source.index("_get_access_token")
    assert "settings_db.decrement_subscription_quota" not in subscribe_source
    assert "settings_db.decrement_subscription_quota" not in _source("tasks/resubscribe.py")


def test_subscribe_assistant_recovery_keeps_non_consuming_quota_contract() -> None:
    source = _source("services/subscribe_assistant/manager.py")
    tree = ast.parse(source)
    functions = {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name
        in {
            "_create_subscription",
            "_keep_full_washing_subscription_active",
            "_repair_snapshot_subscription",
        }
    }
    create = functions["_create_subscription"]
    assert isinstance(create.args.defaults[-1], ast.Constant)
    assert create.args.defaults[-1].value is False

    create_source = ast.get_source_segment(source, create) or ""
    repair_source = ast.get_source_segment(source, functions["_repair_snapshot_subscription"]) or ""
    washing_source = ast.get_source_segment(source, functions["_keep_full_washing_subscription_active"]) or ""

    assert "consume_quota=consume_quota" in create_source
    assert "consume_quota=" not in repair_source
    assert "consume_quota=" not in washing_source


def test_subscribe_assistant_processed_execution_remains_an_explicit_compatibility_boundary() -> None:
    manifest = json.loads(_source("verification/contracts/task-ownership.yaml"))
    task = next(entry for entry in manifest["tasks"] if entry["task_key"] == "subscribe-assistant-maintenance")

    assert task["status"] == "new"
    assert task["new_owner"] == "etk.processes.p7_subscription_tasks.P7SubscriptionTaskRouter"
    assert task["ownership_layers"] == {
        "envelope_owner": "etk.processes.p7_subscription_tasks.P7SubscriptionTaskRouter",
        "execution_kind": "legacy-compatibility-callable",
        "execution_owner": "etk.processes.p7_subscription_tasks.LegacySubscribeAssistantMaintenanceGateway",
        "retirement_stage": "P11",
    }
    assert task["validation_method"] == (
        "ordered-task-registry-native-envelope-compatibility-worker-outcome-postgres18-and-redis-celery"
    )
    assert "legacy Subscribe Assistant manager/store" in task["notes"]


def test_manual_subscription_batch_has_processor_free_native_execution_ownership() -> None:
    manifest = json.loads(_source("verification/contracts/task-ownership.yaml"))
    task = next(entry for entry in manifest["tasks"] if entry["task_key"] == "manual_subscribe_batch")

    assert task["status"] == "new"
    assert task["new_owner"] == "etk.processes.p7_subscription_tasks.P7SubscriptionTaskRouter"
    assert task["ownership_layers"] == {
        "envelope_owner": "etk.processes.p7_subscription_tasks.P7SubscriptionTaskRouter",
        "execution_kind": "native-use-case",
        "execution_owner": "etk.domains.subscription.manual_batch.ManualSubscriptionBatchService",
    }
    assert task["validation_owner"] == "subscription/manual-batch"
    assert "no legacy task callable" in task["notes"]

    service = _source("src/etk/domains/subscription/manual_batch.py")
    adapter = _source("src/etk/domains/subscription/manual_batch_adapters.py")
    for forbidden in ("task_manager", "tasks.", "handler.", "Processor", "config_manager", "extensions"):
        assert forbidden not in service
        assert forbidden not in adapter


def test_native_telegram_listener_has_no_legacy_runtime_imports_or_local_queue() -> None:
    manager = _source("src/etk/integrations/telegram_userbot.py")
    gateway = _source("src/etk/domains/subscription/telegram_listener.py")

    for forbidden_import in (
        "from handler",
        "import handler",
        "from tasks",
        "import tasks",
        "from database",
        "import database",
        "import config_manager",
        "import task_manager",
        "tg_task_queue",
        "DurableTelegramTaskQueue",
    ):
        assert forbidden_import not in manager
        assert forbidden_import not in gateway
