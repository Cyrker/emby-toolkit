from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
MIGRATION = REPO_ROOT / "migrations" / "versions" / "0002_p4_persistent_jobs.py"


def test_p4_migration_is_a_new_revision_with_all_four_ledgers() -> None:
    source = MIGRATION.read_text(encoding="utf-8")
    tree = ast.parse(source)
    assignments = {
        node.target.id: ast.literal_eval(node.value)
        for node in tree.body
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value is not None
    }

    assert assignments["revision"] == "0002_p4_persistent_jobs"
    assert assignments["down_revision"] == "0001_p2_legacy_baseline"
    for table in ("job_runs", "job_attempts", "outbox_events", "idempotency_records"):
        assert f'"{table}"' in source
    assert "uq_idempotency_records_scope_key_hash" in source
    assert "uq_job_attempts_job_attempt_no" in source
    assert "uq_job_runs_single_active_root_serial" in source
    assert "ck_job_runs_workflow_parent_pair" in source
    assert "ix_outbox_events_claim_recovery" in source
    assert "jsonb_typeof(logs_json) = 'array'" in source
    assert 'op.execute("SET LOCAL search_path TO public")' in source
