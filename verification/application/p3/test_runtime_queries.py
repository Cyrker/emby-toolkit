from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

from etk.bootstrap.app_factory import create_app
from etk.bootstrap.runtime_supervisor import RuntimeSupervisor
from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.operations.runtime import ManifestTaskCatalog
from etk.platform import AppContainer, Settings
from etk.platform.tasks import RETIRED_EXTERNAL_RESOURCE_TASK_KEYS

REPO_ROOT = Path(__file__).resolve().parents[3]
TASK_MANIFEST = REPO_ROOT / "verification" / "contracts" / "task-ownership.yaml"
ORDERED_TASK_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "tasks" / "ordered-task-catalog.json"
RUNTIME_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "http" / "runtime-query-routes.json"
SECRET_KEY = b"p3-runtime-query-session-key"


class Permissions:
    def __init__(self, admins: set[str] | None = None) -> None:
        self.admins = admins or set()

    def is_admin(self, user_id: str) -> bool:
        return user_id in self.admins


class StatusReader:
    def read_status(self) -> Mapping[str, Any]:
        return {
            "is_running": False,
            "current_action": "",
            "progress": 0,
            "message": "idle",
            "last_action": "",
            "logs": ["license_key=<redacted>"],
        }


def _app(tmp_path: Path, *, auth_enabled: bool, admins: set[str] | None = None):
    settings = Settings(auth_enabled=auth_enabled)
    container = AppContainer(
        settings,
        repository_providers={"identity": lambda _: Permissions(admins)},
        use_case_providers={
            "runtime-status": lambda _: StatusReader(),
            "task-catalog": lambda _: ManifestTaskCatalog(TASK_MANIFEST),
        },
    )
    return create_app(
        start_runtime=False,
        data_dir=tmp_path / "config",
        secret_key=SECRET_KEY,
        supervisor=RuntimeSupervisor(),
        container=container,
    )


def _session_cookie(values: Mapping[str, Any]) -> str:
    return LegacySessionCodec.from_secret_key(SECRET_KEY).dumps(values)


def test_runtime_fixture_and_manifest_catalog_cover_query_contracts() -> None:
    fixture = json.loads(RUNTIME_FIXTURE.read_text(encoding="utf-8"))
    assert {(route["method"], route["path"]) for route in fixture["routes"]} == {
        ("GET", "/api/status"),
        ("GET", "/api/tasks/available"),
    }

    catalog = ManifestTaskCatalog(TASK_MANIFEST)
    all_tasks = catalog.available("all")
    chain_tasks = catalog.available("chain")
    manual_tasks = catalog.available("manual")
    ordered_fixture = json.loads(ORDERED_TASK_FIXTURE.read_text(encoding="utf-8"))
    assert len(ordered_fixture["contexts"]["all"]) == 50
    assert len(all_tasks) == 46
    assert len(chain_tasks) == 25
    assert len(manual_tasks) == 25
    for context, tasks in (("all", all_tasks), ("chain", chain_tasks), ("manual", manual_tasks)):
        expected = [
            key for key in ordered_fixture["contexts"][context] if key not in RETIRED_EXTERNAL_RESOURCE_TASK_KEYS
        ]
        assert [task["key"] for task in tasks] == expected
        assert {task["key"] for task in tasks}.isdisjoint(RETIRED_EXTERNAL_RESOURCE_TASK_KEYS)
    assert catalog.available("invalid") == chain_tasks
    assert all("processor_type" in task for task in all_tasks)
    assert all("processor_type" not in task for task in chain_tasks + manual_tasks)
    assert {task["key"] for task in manual_tasks}.isdisjoint({"task-chain-high-freq", "task-chain-low-freq"})
    assert {task["key"] for task in chain_tasks}.issubset({task["key"] for task in all_tasks})

    dockerfile = (REPO_ROOT / "Dockerfile.refactor").read_text(encoding="utf-8")
    assert "COPY verification/contracts/task-ownership.yaml ./verification/contracts/task-ownership.yaml" in dockerfile


def test_status_is_public_and_task_catalog_preserves_admin_matrix(tmp_path: Path) -> None:
    app = _app(tmp_path, auth_enabled=True, admins={"fixture-admin"})
    with TestClient(app) as client:
        status = client.get("/api/status")
        assert status.status_code == 200
        assert status.json() == {
            "is_running": False,
            "current_action": "",
            "progress": 0,
            "message": "idle",
            "last_action": "",
            "logs": ["license_key=<redacted>"],
        }

        denied = client.get("/api/tasks/available")
        assert denied.status_code == 403
        assert denied.json() == {
            "status": "error",
            "message": "需要 Emby 管理员权限才能执行此操作",
        }

        client.cookies.set(
            "session",
            _session_cookie({"emby_user_id": "fixture-user", "emby_is_admin": True}),
        )
        assert client.get("/api/tasks/available").status_code == 403
        client.cookies.set("session", _session_cookie({"emby_user_id": "fixture-admin"}))
        allowed = client.get("/api/tasks/available?context=manual")
        assert allowed.status_code == 200
        assert allowed.json()


def test_auth_disabled_task_catalog_does_not_materialize_database(tmp_path: Path) -> None:
    app = _app(tmp_path, auth_enabled=False)
    with TestClient(app) as client:
        response = client.get("/api/tasks/available?context=invalid")
    assert response.status_code == 200
    assert response.json() == ManifestTaskCatalog(TASK_MANIFEST).available("chain")
