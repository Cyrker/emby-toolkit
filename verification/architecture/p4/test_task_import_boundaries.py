from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_catalog_registry_and_adapter_import_without_legacy_executable_modules() -> None:
    source = f"""
import json
import sys
import threading
from pathlib import Path

from etk.domains.operations.runtime import ManifestTaskCatalog
from etk.platform.tasks import LegacyTaskAdapter, TaskRegistry

root = Path({str(REPO_ROOT)!r})
ManifestTaskCatalog(root / 'verification/contracts/task-ownership.yaml').available('all')
TaskRegistry.from_fixture(root / 'verification/fixtures/tasks/ordered-task-catalog.json')
forbidden = {{
    'tasks.core',
    'core_processor',
    'watchlist_processor',
    'actor_subscription_processor',
    'extensions',
    'web_app',
}}
print(json.dumps({{
    'loaded': sorted(forbidden.intersection(sys.modules)),
    'threads': [thread.name for thread in threading.enumerate()],
    'adapter': LegacyTaskAdapter.__name__,
}}))
"""
    environment = os.environ.copy()
    python_path = [str(REPO_ROOT / "src"), str(REPO_ROOT)]
    if environment.get("PYTHONPATH"):
        python_path.append(environment["PYTHONPATH"])
    environment["PYTHONPATH"] = os.pathsep.join(python_path)

    completed = subprocess.run(
        [sys.executable, "-c", source],
        cwd=REPO_ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    observed = json.loads(completed.stdout)

    assert observed == {
        "loaded": [],
        "threads": ["MainThread"],
        "adapter": "LegacyTaskAdapter",
    }
