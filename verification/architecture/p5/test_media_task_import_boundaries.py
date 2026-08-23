from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_p5_native_task_router_import_does_not_load_legacy_task_modules() -> None:
    source = f"""
import json
import sys
from pathlib import Path

from etk.platform.tasks import TaskRegistry
from etk.processes.p5_media_tasks import P5MediaTaskRouter

root = Path({str(REPO_ROOT)!r})
TaskRegistry.from_fixture(root / 'verification/fixtures/tasks/ordered-task-catalog.json')
forbidden = {{'tasks.media', 'tasks.actors', 'core_processor', 'extensions'}}
print(json.dumps({{
    'loaded': sorted(forbidden.intersection(sys.modules)),
    'router': P5MediaTaskRouter.__name__,
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

    assert json.loads(completed.stdout) == {
        "loaded": [],
        "router": "P5MediaTaskRouter",
    }
