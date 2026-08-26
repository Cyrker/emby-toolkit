from __future__ import annotations

import ast
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
    assert scripts.get_heads() == ["0015_p7_assistant_claims"]


def test_subscription_runtime_repository_owns_quota_lock_and_state_cas_without_legacy_business_imports() -> None:
    source = _source("src/etk/domains/subscription/runtime_repository.py")

    assert "pg_advisory_xact_lock" in source
    assert "expected_revision" in source
    assert "subscription_quota_state" in source
    assert "handler.moviepilot" not in source
    assert "database.settings_db" not in source
    assert "web_app" not in source
