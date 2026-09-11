from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_shared_virtual_import_history_is_preserved_without_active_api_wiring() -> None:
    manifest = json.loads((REPO_ROOT / "verification/contracts/route-ownership.yaml").read_text(encoding="utf-8"))
    expected = {
        ("POST", "/api/shared/resources/center/import"),
        ("DELETE", "/api/shared/resources/virtual-imports/<int:virtual_id>"),
        ("POST", "/api/shared/resources/virtual-imports/<int:virtual_id>/promote"),
    }
    observed = {
        (binding["method"], binding["path"])
        for binding in manifest["method_bindings"]
        if (binding["method"], binding["path"]) in expected
        and binding["status"] == "new"
        and binding["new_owner"] == "etk.domains.sharing.router"
    }
    assert observed == expected

    app_factory = (REPO_ROOT / "src/etk/bootstrap/app_factory.py").read_text(encoding="utf-8")
    assert 'register_use_case(\n        "sharing.virtual-imports"' not in app_factory
    assert 'virtual_imports=current.use_case("sharing.virtual-imports")' not in app_factory


def test_shared_virtual_import_native_imports_do_not_load_legacy_modules() -> None:
    program = """
import json
import sys
from etk.domains.sharing import (
    AtomicVirtualImportProjector,
    NativeP115RapidTransferGateway,
    RapidTransferOperationDelivery,
    SqlAlchemyVirtualImportRepository,
    VirtualImportOperationDelivery,
)
forbidden_prefixes = ('database', 'extensions', 'handler', 'routes', 'task_manager', 'tasks')
loaded = sorted(
    name for name in sys.modules
    if any(name == prefix or name.startswith(prefix + '.') for prefix in forbidden_prefixes)
)
print(json.dumps({
    'loaded': loaded,
    'projector': AtomicVirtualImportProjector.__name__,
    'rapid': RapidTransferOperationDelivery.__name__,
    'rapid_gateway': NativeP115RapidTransferGateway.__name__,
    'repository': SqlAlchemyVirtualImportRepository.__name__,
    'virtual': VirtualImportOperationDelivery.__name__,
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
        "projector": "AtomicVirtualImportProjector",
        "rapid": "RapidTransferOperationDelivery",
        "rapid_gateway": "NativeP115RapidTransferGateway",
        "repository": "SqlAlchemyVirtualImportRepository",
        "virtual": "VirtualImportOperationDelivery",
    }
