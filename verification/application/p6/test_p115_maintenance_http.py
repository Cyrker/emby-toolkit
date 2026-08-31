from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.operations.jobs import JobSubmissionService
from etk.domains.p115 import (
    P115CredentialSnapshot,
    P115DirectUrl,
    P115MaintenanceIntentService,
    P115QuickDeployIntentService,
    P115RenameMoviePilotService,
    P115SpeedtestService,
)
from etk.domains.p115.router import router
from etk.platform import AppContainer, Settings
from etk.platform.jobs import JobStatus
from etk.platform.tasks import TaskRegistry
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
TASK_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "tasks" / "ordered-task-catalog.json"


class FakePermissions:
    def __init__(self, *, admin: bool = True) -> None:
        self.admin = admin

    def is_admin(self, _user_id: str) -> bool:
        return self.admin


class FakeCredentials:
    def load(self) -> P115CredentialSnapshot:
        return P115CredentialSnapshot(
            account_id="115001",
            access_token="fixture-token",
            cookie="UID=115001; CID=fixture",
            app_type="web",
        )

    def app_label(self, app_type: str) -> str:
        return app_type


class FakeAppSettings:
    def revision(self, key: str) -> str:
        return {"dynamic_app_config": "dynamic-revision", "mp_config": "mp-revision"}[key]

    def merge_mapping_if_revision(
        self,
        key: str,
        updates: Mapping[str, Any],
        *,
        expected_revision: str,
        remove_keys: Sequence[str] = (),
    ) -> Mapping[str, Any]:
        del key, expected_revision, remove_keys
        return dict(updates)


class FakeSpeedFacts:
    def speedtest_sample(self, *, account_id: str) -> dict[str, Any]:
        assert account_id == "115001"
        return {"pick_code": "pick-code-123456", "sha1": "A" * 40, "file_name": "fixture.mkv"}


class FakeDirectUrls:
    def resolve(
        self,
        pick_code: str,
        *,
        expected_account_id: str,
        user_agent: str,
    ) -> P115DirectUrl:
        assert (pick_code, expected_account_id) == ("pick-code-123456", "115001")
        return P115DirectUrl(
            url="https://download.example.test/file",
            user_agent=user_agent,
            backend="openapi",
            method="fixture",
        )


class FakeDownloads:
    def measure(self, _url: str, **_options: object) -> dict[str, Any]:
        return {"speed_text": "2.00 MB/s", "downloaded_bytes": 1024 * 1024}


class FakeMoviePilot:
    def __init__(self) -> None:
        self.values = {
            "MOVIE_RENAME_FORMAT": "{{title}}/{{title}}{{fileExt}}",
            "TV_RENAME_FORMAT": "{{title}}/Season {{ season|string }}.zfill(2)/{{title}} {{season_episode}}{{fileExt}}",
        }

    def get_system_setting(self, key: str) -> Any:
        return self.values[key]


class ImmediateJobStore:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.projections: dict[UUID, SimpleNamespace] = {}

    def create_job(self, **values: object) -> SimpleNamespace:
        self.calls.append(dict(values))
        payload = values["payload"]
        assert isinstance(payload, dict)
        operation = str(payload["native_operation"])
        job_id = uuid4()
        result = {
            "p115.mkdir": {"provider_response": {"state": True, "cid": "directory-1"}},
            "p115.recycle-bin-empty": {"ok": True},
            "p115.empty-unrecognized": {"ok": True, "deleted_count": 0},
            "p115.moviepilot-rename-export": {
                "mp_movie_template": "movie-template",
                "mp_tv_template": "tv-template",
            },
            "p115.quick-deploy": {
                "config": {"monitor_enabled": True},
                "tree": {},
                "local_dirs": [],
                "emby_libraries": [],
            },
        }[operation]
        job = SimpleNamespace(
            id=job_id,
            status=JobStatus.SUCCEEDED,
            progress=100,
            progress_message="done",
            result=result,
            error_summary={},
        )
        projection = SimpleNamespace(job=job, current_attempt=SimpleNamespace(id=uuid4()))
        self.projections[job_id] = projection
        return SimpleNamespace(projection=projection, reused=False)

    def get_job(self, job_id: UUID) -> SimpleNamespace | None:
        return self.projections.get(job_id)

    def has_active_job(self) -> bool:
        return False


def _settings(tmp_path: Path, *, auth_enabled: bool = False) -> Settings:
    return Settings.model_validate(
        {
            "auth_enabled": auth_enabled,
            "emby_server_url": "http://emby.example.test",
            "emby_api_key": "emby-token",
            "emby_auth_mode": "user_token",
            "emby_user_id": "admin-1",
            "etk_server_url": "http://etk.example.test",
            "local_strm_root": str(tmp_path / "media"),
            "p115_unrecognized_cid": "900",
        }
    )


def _client(tmp_path: Path, *, auth_enabled: bool = False, admin: bool = True) -> tuple[TestClient, ImmediateJobStore]:
    settings = _settings(tmp_path, auth_enabled=auth_enabled)
    permissions = FakePermissions(admin=admin)
    credentials = FakeCredentials()
    app_settings = FakeAppSettings()
    store = ImmediateJobStore()
    jobs = JobSubmissionService(store, TaskRegistry.from_fixture(TASK_FIXTURE))  # type: ignore[arg-type]
    container = AppContainer(
        settings,
        repository_providers={"identity": lambda _container: permissions},
        use_case_providers={"operations.jobs": lambda _container: jobs},
    )
    speedtest = P115SpeedtestService(
        credentials=credentials,  # type: ignore[arg-type]
        facts=FakeSpeedFacts(),  # type: ignore[arg-type]
        direct_urls=FakeDirectUrls(),  # type: ignore[arg-type]
        downloads=FakeDownloads(),  # type: ignore[arg-type]
        clock=lambda: datetime(2026, 8, 31, 10, 0, 0),
    )
    rename_moviepilot = P115RenameMoviePilotService(
        gateway=FakeMoviePilot(),  # type: ignore[arg-type]
        settings=app_settings,
    )
    app = FastAPI()
    app.state.container = container
    app.state.p115_maintenance_intent_service = P115MaintenanceIntentService(
        credentials=credentials,  # type: ignore[arg-type]
        priority=lambda: "openapi",
    )
    app.state.p115_speedtest_service = speedtest
    app.state.p115_quick_deploy_intent_service = P115QuickDeployIntentService(
        settings=lambda: settings,
        credentials=credentials,  # type: ignore[arg-type]
        app_settings=app_settings,
        priority=lambda: "openapi",
    )
    app.state.p115_rename_moviepilot_service = rename_moviepilot
    app.state.configuration_synchronizer = None
    codec = LegacySessionCodec.from_secret_key(b"p6-p115-maintenance-http-session")
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return TestClient(app), store


def test_p115_maintenance_http_routes_submit_bounded_native_operations(tmp_path: Path) -> None:
    client, store = _client(tmp_path)

    speedtest = client.get("/api/p115/speedtest", params={"max_mb": 1, "seconds": 3})
    mkdir = client.post(
        "/api/p115/mkdir",
        headers={"Idempotency-Key": "mkdir-key"},
        json={"pid": "100", "name": "电影"},
    )
    recycle = client.post(
        "/api/p115/recycle_bin/empty",
        headers={"Idempotency-Key": "recycle-key"},
    )
    unrecognized = client.post(
        "/api/p115/unrecognized/empty",
        headers={"Idempotency-Key": "unrecognized-key"},
    )

    assert speedtest.status_code == 200
    assert speedtest.json()["status"] == "success"
    assert mkdir.status_code == 200
    assert mkdir.json() == {"status": "success", "data": {"state": True, "cid": "directory-1"}}
    assert recycle.status_code == 200
    assert recycle.json()["success"] is True
    assert unrecognized.status_code == 200
    assert unrecognized.json()["data"]["accepted"] is True
    assert [call["task_key"] for call in store.calls] == [
        "native-operation:p115.mkdir",
        "native-operation:p115.recycle-bin-empty",
        "native-operation:p115.empty-unrecognized",
    ]
    assert [call["max_attempts"] for call in store.calls] == [1, 1, 1]


def test_p115_moviepilot_and_quick_deploy_http_contracts(tmp_path: Path) -> None:
    client, store = _client(tmp_path)

    imported = client.get("/api/p115/rename_config/mp/import")
    exported = client.post(
        "/api/p115/rename_config/mp/export",
        headers={"Idempotency-Key": "mp-export-key"},
        json={
            "main_dir_template": "{{title_zh}}",
            "season_dir_template": "Season {{season_no}}",
            "movie_file_template": "{{title_zh}}",
            "tv_file_template": "{{title_zh}} {{s_e}}",
        },
    )
    deployed = client.post(
        "/api/p115/quick_deploy",
        params={"stream": 1},
        headers={"Idempotency-Key": "quick-deploy-key"},
    )

    assert imported.status_code == 200
    assert imported.json()["success"] is True
    assert exported.status_code == 200
    assert exported.json()["data"] == {
        "mp_movie_template": "movie-template",
        "mp_tv_template": "tv-template",
    }
    events = [json.loads(line) for line in deployed.text.splitlines()]
    assert [event["type"] for event in events] == ["accepted", "progress", "done"]
    assert events[-1]["status"] == "confirmed"
    assert [call["task_key"] for call in store.calls] == [
        "native-operation:p115.moviepilot-rename-export",
        "native-operation:p115.quick-deploy",
    ]


def test_p115_mutation_permission_denial_precedes_body_and_job_access(tmp_path: Path) -> None:
    client, store = _client(tmp_path, auth_enabled=True, admin=False)

    response = client.post(
        "/api/p115/mkdir",
        content="not-json",
        headers={"content-type": "application/json"},
    )

    assert response.status_code == 403
    assert store.calls == []


def test_p115_synchronous_operation_timeouts_return_durable_job_handles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def timeout(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr("etk.domains.p115.router._await_p115_operation", timeout)
    client, _store = _client(tmp_path)
    responses = [
        client.post(
            "/api/p115/mkdir",
            headers={"Idempotency-Key": "timeout-mkdir"},
            json={"pid": "100", "name": "电影"},
        ),
        client.post(
            "/api/p115/recycle_bin/empty",
            headers={"Idempotency-Key": "timeout-recycle"},
        ),
        client.post(
            "/api/p115/rename_config/mp/export",
            headers={"Idempotency-Key": "timeout-export"},
            json={
                "main_dir_template": "{{title_zh}}",
                "season_dir_template": "Season {{season_no}}",
                "movie_file_template": "{{title_zh}}",
                "tv_file_template": "{{title_zh}} {{s_e}}",
            },
        ),
        client.post(
            "/api/p115/quick_deploy",
            headers={"Idempotency-Key": "timeout-quick-deploy"},
        ),
    ]

    for response in responses:
        assert response.status_code == 503
        payload = response.json()
        assert payload["operation_status"] == "indeterminate"
        UUID(payload["job_id"])
        assert "job_id" in payload["message"]


def test_p115_maintenance_fixture_matches_owned_routes_and_openapi() -> None:
    fixture_path = REPO_ROOT / "verification/fixtures/http/p6-p115-maintenance.json"
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    expected = {(route["method"], route["path"]) for route in fixture["routes"]}
    assert len(expected) == 7

    ownership = json.loads((REPO_ROOT / "verification/contracts/route-ownership.yaml").read_text(encoding="utf-8"))
    bindings = [row for row in ownership["method_bindings"] if row.get("validation_owner") == "p115/maintenance"]
    assert {(row["method"], row["path"]) for row in bindings} == expected
    assert all(
        row["status"] == "new"
        and row["new_owner"] == "etk.domains.p115.router"
        and row["fixture"] == "verification/fixtures/http/p6-p115-maintenance.json"
        for row in bindings
    )

    decorators = [row for row in ownership["routes"] if row.get("validation_owner") == "p115/maintenance"]
    assert {(method, row["path"]) for row in decorators for method in row["methods"]} == expected
    assert all(
        row["status"] == "new"
        and row["new_owner"] == "etk.domains.p115.router"
        and row["fixture"] == "verification/fixtures/http/p6-p115-maintenance.json"
        for row in decorators
    )

    schema = json.loads((REPO_ROOT / "verification/contracts/http/p4-openapi.json").read_text(encoding="utf-8"))
    for method, path in expected:
        operation = schema["paths"][path][method.lower()]
        assert "422" not in operation["responses"], (method, path)
