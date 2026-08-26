from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
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
    P115EpisodeMatch,
    P115EpisodeRegexService,
    P115EpisodeRegexSnapshot,
    match_episode_regex,
    normalize_episode_regex_rules,
)
from etk.domains.p115.router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware
from routes import p115 as legacy_routes

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = json.loads((REPO_ROOT / "verification/fixtures/http/p6-p115-episode-regex.json").read_text())
GOLDEN_CASES = tuple(FIXTURE["golden_cases"])


class FakePermissions:
    def is_admin(self, user_id: str) -> bool:
        return user_id == "fixture-admin"


class FakeRepository:
    def __init__(self, raw_rules: object) -> None:
        rules = (
            tuple(deepcopy(dict(rule)) for rule in raw_rules if isinstance(rule, Mapping))
            if isinstance(raw_rules, list)
            else ()
        )
        self.snapshot = P115EpisodeRegexSnapshot(rules)
        self.saves: list[list[dict[str, Any]]] = []

    def load(self) -> P115EpisodeRegexSnapshot:
        return self.snapshot

    def save(self, rules: Sequence[Mapping[str, Any]]) -> P115EpisodeRegexSnapshot:
        saved = [deepcopy(dict(rule)) for rule in rules]
        self.saves.append(saved)
        self.snapshot = P115EpisodeRegexSnapshot(tuple(saved))
        return self.snapshot


def _native_client(stored: object, *, auth_enabled: bool = False) -> tuple[TestClient, FakeRepository]:
    repository = FakeRepository(stored)
    service = P115EpisodeRegexService(repository)
    container = AppContainer(
        Settings.model_validate({"auth_enabled": auth_enabled}),
        repository_providers={"identity": lambda _: FakePermissions()},
    )
    app = FastAPI()
    app.state.container = container
    app.state.p115_episode_regex_service = service
    codec = LegacySessionCodec.from_secret_key(b"p6-episode-regex-session")
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return TestClient(app), repository


def _snapshot(response: Any) -> dict[str, Any]:
    return {
        "status": response.status_code,
        "body": response.get_json() if hasattr(response, "get_json") else response.json(),
    }


@pytest.mark.parametrize("case", GOLDEN_CASES, ids=[case["case_id"] for case in GOLDEN_CASES])
def test_native_and_isolated_legacy_match_frozen_episode_regex_golden(
    case: Mapping[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = deepcopy(case.get("stored"))
    legacy_saves: list[list[dict[str, Any]]] = []
    monkeypatch.setitem(config_manager.APP_CONFIG, "auth_enabled", False)
    monkeypatch.setattr(legacy_routes.settings_db, "get_setting", lambda _key: deepcopy(stored))
    monkeypatch.setattr(
        legacy_routes.settings_db,
        "save_setting",
        lambda _key, value: legacy_saves.append(deepcopy(value)),
    )
    legacy_app = Flask("p6-p115-episode-regex-legacy")
    legacy_app.secret_key = "fixture"
    legacy_app.register_blueprint(legacy_routes.p115_bp)
    native, native_repository = _native_client(stored)

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
    expected_saves = [case["expected_saved"]] if "expected_saved" in case else []
    assert legacy_saves == expected_saves
    assert native_repository.saves == expected_saves


@pytest.mark.parametrize(
    "rules",
    [
        [{"pattern": "["}],
        [{"pattern": "(\\d+)", "season_group": -1}],
        [{"pattern": "(\\d+)", "default_season": -1}],
        [{"pattern": "(\\d+)", "episode_group": "bad"}],
        {"not": "an array"},
    ],
)
def test_normalization_matches_the_legacy_error_contract(rules: object) -> None:
    assert normalize_episode_regex_rules(rules) == legacy_routes._normalize_episode_regex_rules(rules)


def test_runtime_matching_uses_first_valid_rule_and_relative_path_fallback() -> None:
    rules = [
        {"enabled": False, "name": "Disabled", "pattern": r"EP(\d+)", "mode": "episode_only"},
        {
            "enabled": True,
            "name": "Filename",
            "pattern": r"EP(\d+)",
            "mode": "episode_only",
            "episode_group": 1,
            "default_season": 2,
        },
        {
            "enabled": True,
            "name": "Path",
            "pattern": r"Season (\d+)/Episode (\d+)",
            "mode": "season_episode",
            "season_group": 1,
            "episode_group": 2,
        },
    ]

    assert match_episode_regex(rules, "Show.EP07.mkv") == P115EpisodeMatch(2, 7, "Filename")
    assert match_episode_regex(
        rules,
        "Show.mkv",
        relative_path="Show/Season 3/Episode 8/Show.mkv",
    ) == P115EpisodeMatch(3, 8, "Path")


def test_episode_regex_native_transport_preserves_legacy_json_errors() -> None:
    client, repository = _native_client([])
    with client:
        unsupported = client.post(
            "/api/p115/episode_regex_rules",
            content="[]",
            headers={"content-type": "text/plain"},
        )
        malformed = client.post(
            "/api/p115/episode_regex_rules",
            content="[",
            headers={"content-type": "application/json"},
        )

    assert unsupported.status_code == 415
    assert unsupported.headers["content-type"].startswith("text/html")
    assert malformed.status_code == 400
    assert malformed.headers["content-type"].startswith("text/html")
    assert repository.saves == []


def test_episode_regex_admin_denial_precedes_body_parsing_and_persistence() -> None:
    client, repository = _native_client([], auth_enabled=True)
    with client:
        response = client.post(
            "/api/p115/episode_regex_rules",
            content="[",
            headers={"content-type": "application/json"},
        )

    assert response.status_code == 403
    assert response.json()["status"] == "error"
    assert repository.saves == []


def test_truthy_json_scalar_preserves_the_legacy_500_without_saving(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy_saves: list[object] = []
    monkeypatch.setitem(config_manager.APP_CONFIG, "auth_enabled", False)
    monkeypatch.setattr(legacy_routes.settings_db, "save_setting", lambda *_args: legacy_saves.append(_args))
    legacy_app = Flask("p6-p115-episode-regex-scalar")
    legacy_app.secret_key = "fixture"
    legacy_app.register_blueprint(legacy_routes.p115_bp)
    native, repository = _native_client([])

    with legacy_app.test_client() as legacy, native:
        legacy_response = legacy.post("/api/p115/episode_regex_rules", json=1)
        native_response = native.post("/api/p115/episode_regex_rules", json=1)

    assert legacy_response.status_code == native_response.status_code == 500
    assert legacy_response.headers["content-type"].startswith("text/html")
    assert native_response.headers["content-type"].startswith("text/html")
    assert legacy_saves == []
    assert repository.saves == []
