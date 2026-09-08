from __future__ import annotations

import json
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from flask import Flask

import config_manager
from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.p115 import (
    NativeP115RenamePreviewGateway,
    P115RenameConfigService,
    P115RenameConfigSnapshot,
)
from etk.domains.p115.router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware
from routes import p115 as legacy_routes

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = json.loads((REPO_ROOT / "verification/fixtures/http/p6-p115-rename-config.json").read_text())
GOLDEN_CASES = tuple(FIXTURE["golden_cases"])


class FakePermissions:
    def is_admin(self, user_id: str) -> bool:
        return user_id == "fixture-admin"


class FakeRepository:
    def __init__(self, config: Mapping[str, Any]) -> None:
        self.snapshot = P115RenameConfigSnapshot(deepcopy(dict(config)))
        self.saves: list[dict[str, Any]] = []

    def load(self) -> P115RenameConfigSnapshot:
        return self.snapshot

    def save(self, config: Mapping[str, Any]) -> P115RenameConfigSnapshot:
        saved = deepcopy(dict(config))
        self.saves.append(saved)
        self.snapshot = P115RenameConfigSnapshot(saved)
        return self.snapshot


class FakeWashingPriority:
    def __init__(self) -> None:
        self.conflict_modes: list[object] = []

    def save_conflict_mode(self, conflict_mode: object) -> None:
        self.conflict_modes.append(conflict_mode)


def _native_client(
    stored: Mapping[str, Any],
    *,
    auth_enabled: bool = False,
) -> tuple[TestClient, FakeRepository, FakeWashingPriority]:
    repository = FakeRepository(stored)
    washing = FakeWashingPriority()
    service = P115RenameConfigService(
        repository=repository,
        preview=NativeP115RenamePreviewGateway(),
        washing_priority=washing,
    )
    container = AppContainer(
        Settings.model_validate({"auth_enabled": auth_enabled}),
        repository_providers={"identity": lambda _: FakePermissions()},
    )
    app = FastAPI()
    app.state.container = container
    app.state.p115_rename_config_service = service
    codec = LegacySessionCodec.from_secret_key(b"p6-rename-config-session")
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return TestClient(app), repository, washing


def _snapshot(response: Any) -> dict[str, Any]:
    return {
        "status": response.status_code,
        "body": response.get_json() if hasattr(response, "get_json") else response.json(),
    }


@pytest.mark.parametrize("case", GOLDEN_CASES, ids=[case["case_id"] for case in GOLDEN_CASES])
def test_native_and_isolated_legacy_match_frozen_rename_config_golden(
    case: Mapping[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = deepcopy(case.get("stored") or {})
    legacy_saves: list[dict[str, Any]] = []
    legacy_conflict_modes: list[object] = []
    monkeypatch.setitem(config_manager.APP_CONFIG, "auth_enabled", False)
    monkeypatch.setattr(legacy_routes.settings_db, "get_setting", lambda _key: deepcopy(stored))
    monkeypatch.setattr(
        legacy_routes.settings_db,
        "save_setting",
        lambda _key, value: legacy_saves.append(deepcopy(dict(value))),
    )
    monkeypatch.setattr(
        legacy_routes.settings_db,
        "save_washing_priority_config",
        lambda value: legacy_conflict_modes.append(value.get("conflict_mode")),
    )
    legacy_app = Flask("p6-p115-rename-config-legacy")
    legacy_app.secret_key = "fixture"
    legacy_app.register_blueprint(legacy_routes.p115_bp)
    native, native_repository, native_washing = _native_client(stored)

    request = case["request"]
    body = request.get("body")
    with legacy_app.test_client() as legacy, native:
        legacy_response = legacy.open(
            str(request["path"]),
            method=str(request["method"]),
            json=body if "body" in request else None,
        )
        native_response = native.request(
            str(request["method"]),
            str(request["path"]),
            json=body if "body" in request else None,
        )

    assert _snapshot(legacy_response) == case["response"]
    assert _snapshot(native_response) == case["response"]
    if "expected_saved" in case:
        assert legacy_saves == [case["expected_saved"]]
        assert native_repository.saves == [case["expected_saved"]]
    else:
        assert legacy_saves == []
        assert native_repository.saves == []
    expected_mode = [case["expected_conflict_mode"]] if "expected_conflict_mode" in case else []
    assert legacy_conflict_modes == expected_mode
    assert native_washing.conflict_modes == expected_mode


def test_preview_keep_original_name_has_no_persistence_or_provider_effect() -> None:
    client, repository, washing = _native_client({})
    with client:
        response = client.post(
            "/api/p115/rename_config/preview",
            json={
                "config": {"keep_original_name": True},
                "templates": {
                    "main_dir_template": "{{title}}",
                    "season_dir_template": "Season {{season_no}}",
                    "movie_file_template": "ignored",
                    "tv_file_template": "ignored",
                },
            },
        )

    assert response.status_code == 200
    assert response.json()["data"]["movie_file"] == ("Parasite.2019.60fps.1080p.BluRay.HDR.AVC.DDP5.1-CMCT.mkv")
    assert response.json()["data"]["tv_file"] == ("Breaking.Bad.S01E01.2160p.NF.WEB-DL.HDR.60fps.HEVC.Atmos-HHWEB.mp4")
    assert repository.saves == []
    assert washing.conflict_modes == []


def test_rename_config_native_transport_preserves_legacy_json_errors() -> None:
    client, repository, _washing = _native_client({})
    with client:
        unsupported = client.post(
            "/api/p115/rename_config",
            content="{}",
            headers={"content-type": "text/plain"},
        )
        malformed = client.post(
            "/api/p115/rename_config/preview",
            content="{",
            headers={"content-type": "application/json"},
        )

    assert unsupported.status_code == 415
    assert unsupported.headers["content-type"].startswith("text/html")
    assert malformed.status_code == 400
    assert malformed.headers["content-type"].startswith("text/html")
    assert repository.saves == []


def test_rename_config_admin_denial_precedes_body_parsing_and_persistence() -> None:
    client, repository, washing = _native_client({}, auth_enabled=True)
    with client:
        response = client.post(
            "/api/p115/rename_config",
            content="{",
            headers={"content-type": "application/json"},
        )

    assert response.status_code == 403
    assert response.json()["status"] == "error"
    assert repository.saves == []
    assert washing.conflict_modes == []
