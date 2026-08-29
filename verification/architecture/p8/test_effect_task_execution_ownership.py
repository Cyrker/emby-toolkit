from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "scripts/refactor"))

from scripts.refactor.export_tasks import P8_NATIVE_EXECUTION_OWNERS  # noqa: E402

TASK_MANIFEST = REPO_ROOT / "verification/contracts/task-ownership.yaml"


def _task(task_key: str) -> dict[str, object]:
    manifest = json.loads(TASK_MANIFEST.read_text(encoding="utf-8"))
    return next(entry for entry in manifest["tasks"] if entry["task_key"] == task_key)


def test_user_sync_has_native_processed_execution_ownership() -> None:
    assert P8_NATIVE_EXECUTION_OWNERS["sync-all-user-data"] == ("etk.domains.identity.user_sync.UserSyncService")
    task = _task("sync-all-user-data")

    assert task["ownership_layers"] == {
        "envelope_owner": "etk.processes.p8_effect_tasks.P8EffectTaskRouter",
        "execution_kind": "native-use-case",
        "execution_owner": "etk.domains.identity.user_sync.UserSyncService",
    }
    assert "processor-free normal path" in str(task["notes"])


def test_system_auto_update_has_native_skipped_execution_ownership() -> None:
    assert P8_NATIVE_EXECUTION_OWNERS["system-auto-update"] == ("etk.processes.p8_effect_tasks.P8EffectTaskRouter")
    task = _task("system-auto-update")

    assert task["ownership_layers"] == {
        "envelope_owner": "etk.processes.p8_effect_tasks.P8EffectTaskRouter",
        "execution_kind": "native-use-case",
        "execution_owner": "etk.processes.p8_effect_tasks.P8EffectTaskRouter",
    }
    assert "native SKIPPED" in str(task["notes"])
