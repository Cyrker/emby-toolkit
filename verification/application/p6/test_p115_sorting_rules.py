from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from flask import Flask

import config_manager
from database import settings_db
from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.p115 import (
    P115CredentialSnapshot,
    P115SortingRulesService,
    P115SortingRulesSnapshot,
)
from etk.domains.p115.router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware
from routes import p115 as legacy_routes

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = json.loads((REPO_ROOT / "verification/fixtures/http/p6-p115-sorting-rules.json").read_text(encoding="utf-8"))
GOLDEN_CASES = tuple(FIXTURE["golden_cases"])
NATIVE_VARIANCE_CASES = tuple(FIXTURE["native_variance_cases"])


class FakeCredentials:
    def __init__(self, account_id: str = "115001") -> None:
        self.account_id = account_id

    def load(self) -> P115CredentialSnapshot:
        return P115CredentialSnapshot(account_id=self.account_id)

    def save_openapi(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("sorting rules must not save credentials")

    def save_cookie(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("sorting rules must not save credentials")

    def app_label(self, app_type: str) -> str:
        return app_type


class FakeRepository:
    def __init__(self, snapshot: P115SortingRulesSnapshot | None = None) -> None:
        self.snapshot = snapshot or P115SortingRulesSnapshot(())
        self.saves: list[tuple[list[dict[str, Any]], str]] = []

    def load(self) -> P115SortingRulesSnapshot:
        return self.snapshot

    def save(
        self,
        rules: Sequence[Mapping[str, Any]],
        *,
        account_id: str,
    ) -> P115SortingRulesSnapshot:
        normalized = [deepcopy(dict(rule)) for rule in rules]
        self.saves.append((normalized, account_id))
        self.snapshot = P115SortingRulesSnapshot(tuple(normalized), account_id)
        return self.snapshot


class FakeDirectories:
    def __init__(
        self,
        paths: Mapping[str, Sequence[Mapping[str, Any]] | Exception | None] | None = None,
        *,
        available: bool = True,
    ) -> None:
        self.paths = dict(paths or {})
        self.available = available
        self.calls: list[tuple[str, str]] = []

    def is_available(self, *, expected_account_id: str) -> bool:
        del expected_account_id
        return self.available

    def resolve_path(
        self,
        *,
        cid: str,
        expected_account_id: str,
    ) -> Sequence[Mapping[str, Any]] | None:
        self.calls.append((cid, expected_account_id))
        value = self.paths.get(cid)
        if isinstance(value, Exception):
            raise value
        return value


class FakePermissions:
    def is_admin(self, user_id: str) -> bool:
        return user_id == "fixture-admin"


def _service(
    *,
    account_id: str = "115001",
    snapshot: P115SortingRulesSnapshot | None = None,
    paths: Mapping[str, Sequence[Mapping[str, Any]] | Exception | None] | None = None,
    auth_enabled: bool = False,
    directory_available: bool = True,
) -> tuple[P115SortingRulesService, FakeRepository, FakeDirectories, AppContainer]:
    repository = FakeRepository(snapshot)
    directories = FakeDirectories(paths, available=directory_available)
    container = AppContainer(
        Settings.model_validate(
            {
                "auth_enabled": auth_enabled,
                "p115_media_root_cid": "media-root",
                "p115_media_root_name": "媒体库",
            }
        ),
        repository_providers={"identity": lambda _: FakePermissions()},
    )
    service = P115SortingRulesService(
        settings=lambda: container.settings,
        credentials=FakeCredentials(account_id),
        repository=repository,
        directories=directories,
        clock=lambda: 123.456,
    )
    return service, repository, directories, container


def _snapshot_from_stored(value: object, account_id: str) -> P115SortingRulesSnapshot:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            value = []
    rules = (
        tuple(deepcopy(dict(rule)) for rule in value if isinstance(rule, Mapping)) if isinstance(value, list) else ()
    )
    return P115SortingRulesSnapshot(rules, account_id)


def _native_client(
    *,
    stored: object,
    account_id: str,
    paths: Mapping[str, Sequence[Mapping[str, Any]] | Exception | None] | None = None,
    directory_available: bool = True,
) -> tuple[TestClient, FakeRepository, FakeDirectories]:
    service, repository, directories, container = _service(
        account_id=account_id,
        snapshot=_snapshot_from_stored(stored, account_id),
        paths=paths,
        directory_available=directory_available,
    )
    app = FastAPI()
    app.state.container = container
    app.state.p115_sorting_rules_service = service
    codec = LegacySessionCodec.from_secret_key(b"p6-sorting-rules-golden-session")
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return TestClient(app), repository, directories


def _response_snapshot(response: Any) -> dict[str, Any]:
    return {
        "status": response.status_code,
        "body": response.get_json() if hasattr(response, "get_json") else response.json(),
    }


@pytest.mark.parametrize("case", GOLDEN_CASES, ids=[case["case_id"] for case in GOLDEN_CASES])
def test_native_and_isolated_legacy_match_frozen_sorting_rules_golden(
    case: Mapping[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = deepcopy(case.get("stored"))
    account_id = str(case.get("account_id") or "")
    provider_mode = str(case.get("provider") or "")
    legacy_saves: list[object] = []
    legacy_provider_calls: list[str] = []

    class LegacyProvider:
        def fs_files(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
            cid = str(payload.get("cid") or "")
            legacy_provider_calls.append(cid)
            if provider_mode == "must-not-call":
                raise AssertionError("same-CID path reuse must not call the provider")
            if provider_mode == "error":
                raise RuntimeError("fixture provider failure")
            return {"state": True, "path": []}

    monkeypatch.setitem(config_manager.APP_CONFIG, "auth_enabled", False)
    monkeypatch.setattr(legacy_routes.time, "time", lambda: 123.456)
    monkeypatch.setattr(
        legacy_routes,
        "get_config",
        lambda: {
            legacy_routes.constants.CONFIG_OPTION_115_MEDIA_ROOT_CID: "media-root",
            legacy_routes.constants.CONFIG_OPTION_115_MEDIA_ROOT_NAME: "媒体库",
        },
    )
    monkeypatch.setattr(legacy_routes.settings_db, "get_setting", lambda _key: deepcopy(stored))
    monkeypatch.setattr(
        legacy_routes.settings_db,
        "save_setting",
        lambda _key, value: legacy_saves.append(deepcopy(value)),
    )
    monkeypatch.setattr(
        legacy_routes.P115Service,
        "get_client",
        lambda: None if provider_mode == "unavailable" else LegacyProvider(),
    )
    legacy_app = Flask("p6-p115-sorting-rules-legacy")
    legacy_app.secret_key = "fixture"
    legacy_app.register_blueprint(legacy_routes.p115_bp)

    native_paths: dict[str, Sequence[Mapping[str, Any]] | Exception | None] = {}
    request_body = case["request"].get("body")
    if isinstance(request_body, list):
        for rule in request_body:
            if not isinstance(rule, Mapping) or not rule.get("cid"):
                continue
            cid = str(rule["cid"])
            if provider_mode == "error":
                native_paths[cid] = RuntimeError("fixture provider failure")
            elif provider_mode == "must-not-call":
                native_paths[cid] = AssertionError("same-CID path reuse must not call the provider")
    native, native_repository, native_directories = _native_client(
        stored=stored,
        account_id=account_id,
        paths=native_paths,
        directory_available=provider_mode != "unavailable",
    )

    request = case["request"]
    with legacy_app.test_client() as legacy, native:
        legacy_response = legacy.open(
            str(request["path"]),
            method=str(request["method"]),
            json=request_body if "body" in request else None,
        )
        native_response = native.request(
            str(request["method"]),
            str(request["path"]),
            json=request_body if "body" in request else None,
        )

    assert _response_snapshot(legacy_response) == case["response"]
    assert _response_snapshot(native_response) == case["response"]
    if "expected_saved" in case:
        assert legacy_saves == [case["expected_saved"]]
        assert native_repository.saves == [(case["expected_saved"], account_id)]
    else:
        assert legacy_saves == []
        assert native_repository.saves == []
    if provider_mode == "error":
        assert legacy_provider_calls == ["new-dir"]
        assert native_directories.calls == [("new-dir", account_id)]
    elif provider_mode == "must-not-call":
        assert legacy_provider_calls == []
        assert native_directories.calls == []


@pytest.mark.parametrize(
    "case",
    NATIVE_VARIANCE_CASES,
    ids=[case["case_id"] for case in NATIVE_VARIANCE_CASES],
)
def test_malformed_mixed_element_array_is_an_explicit_fail_closed_variance(
    case: Mapping[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = case["request"]
    request_body = deepcopy(request["body"])
    legacy_saves: list[object] = []
    monkeypatch.setitem(config_manager.APP_CONFIG, "auth_enabled", False)
    monkeypatch.setattr(legacy_routes.settings_db, "get_setting", lambda _key: [])
    monkeypatch.setattr(
        legacy_routes.settings_db,
        "save_setting",
        lambda _key, value: legacy_saves.append(deepcopy(value)),
    )
    monkeypatch.setattr(legacy_routes.P115Service, "get_client", lambda: None)
    legacy_app = Flask("p6-p115-sorting-rules-mixed-variance")
    legacy_app.secret_key = "fixture"
    legacy_app.register_blueprint(legacy_routes.p115_bp)
    native, native_repository, native_directories = _native_client(
        stored=[],
        account_id=str(case["account_id"]),
        directory_available=True,
    )

    with legacy_app.test_client() as legacy, native:
        legacy_response = legacy.open(
            str(request["path"]),
            method=str(request["method"]),
            json=request_body,
        )
        native_response = native.request(
            str(request["method"]),
            str(request["path"]),
            json=request_body,
        )

    legacy_expected = case["legacy_unavailable_provider_observation"]
    assert _response_snapshot(legacy_response) == {
        "status": legacy_expected["status"],
        "body": legacy_expected["body"],
    }
    assert legacy_saves == [legacy_expected["expected_saved"]]
    assert _response_snapshot(native_response) == case["native_response"]
    assert native_repository.saves == case["native_expected_saved"]
    assert native_directories.calls == case["native_expected_provider_calls"]


def test_list_preserves_legacy_array_shape_and_supplies_missing_ids() -> None:
    service, _repository, _directories, _container = _service(
        snapshot=P115SortingRulesSnapshot(
            (
                {"name": "Movies", "cid": "movies"},
                {"id": "existing", "name": "Series", "cid": "series"},
            ),
            "115001",
        )
    )

    result = service.list_rules()

    assert result.status_code == 200
    assert result.payload == [
        {"id": "123456", "name": "Movies", "cid": "movies"},
        {"id": "existing", "name": "Series", "cid": "series"},
    ]


def test_save_reuses_same_account_paths_and_resolves_changed_cids_before_atomic_binding() -> None:
    snapshot = P115SortingRulesSnapshot(
        (
            {"id": "same", "cid": "movies", "category_path": "媒体库/电影", "dir_name": "电影"},
            {"id": "changed", "cid": "old", "category_path": "旧目录", "dir_name": "旧目录"},
        ),
        "115001",
    )
    service, repository, directories, _container = _service(
        snapshot=snapshot,
        paths={
            "series": (
                {"cid": "0", "name": "根目录"},
                {"cid": "media-root", "name": "媒体库"},
                {"cid": "tv", "name": "剧集"},
                {"cid": "series", "name": "欧美剧"},
            )
        },
    )

    result = service.save_rules(
        [
            {"id": "same", "cid": "movies", "dir_name": "电影"},
            {"id": "changed", "cid": "series", "dir_name": "欧美剧"},
        ]
    )

    assert result.payload == {"status": "success", "message": "115 分类规则已保存"}
    assert directories.calls == [("series", "115001")]
    assert repository.saves == [
        (
            [
                {"id": "same", "cid": "movies", "dir_name": "电影", "category_path": "电影"},
                {
                    "id": "changed",
                    "cid": "series",
                    "dir_name": "欧美剧",
                    "category_path": "剧集/欧美剧",
                },
            ],
            "115001",
        )
    ]


def test_rules_from_another_account_are_never_reused_and_missing_identity_saves_unbound() -> None:
    previous = P115SortingRulesSnapshot(
        ({"id": "same", "cid": "movies", "category_path": "电影", "dir_name": "电影"},),
        "115000",
    )
    service, repository, directories, _container = _service(
        snapshot=previous,
        paths={"movies": None},
    )

    result = service.save_rules([{"id": "same", "cid": "movies", "dir_name": "电影"}])

    assert result.status_code == 200
    assert directories.calls == [("movies", "115001")]
    assert repository.saves[0][1] == "115001"

    unbound, unbound_repository, _directories, _container = _service(account_id="")
    saved = unbound.save_rules([{"cid": "movies", "dir_name": "电影"}])
    assert saved.status_code == 200
    assert saved.payload == {"status": "success", "message": "115 分类规则已保存"}
    assert unbound_repository.saves == [([{"cid": "movies", "dir_name": "电影"}], "")]


def test_native_http_contract_keeps_array_get_and_legacy_json_transport_errors() -> None:
    service, repository, _directories, container = _service(
        snapshot=P115SortingRulesSnapshot(({"id": "movies", "cid": "movies"},), "115001")
    )
    app = FastAPI()
    app.state.container = container
    app.state.p115_sorting_rules_service = service
    codec = LegacySessionCodec.from_secret_key(b"p6-sorting-rules-session")
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)

    with TestClient(app) as client:
        listed = client.get("/api/p115/sorting_rules")
        saved = client.post("/api/p115/sorting_rules", json={"not": "a list"})
        unsupported = client.post(
            "/api/p115/sorting_rules",
            content="[]",
            headers={"content-type": "text/plain"},
        )
        malformed = client.post(
            "/api/p115/sorting_rules",
            content="[",
            headers={"content-type": "application/json"},
        )

    assert listed.status_code == 200
    assert listed.json() == [{"id": "movies", "cid": "movies"}]
    assert saved.status_code == 200
    assert saved.json() == {"status": "success", "message": "115 分类规则已保存"}
    assert repository.saves[-1] == ([], "115001")
    assert unsupported.status_code == 415
    assert unsupported.headers["content-type"].startswith("text/html")
    assert malformed.status_code == 400
    assert malformed.headers["content-type"].startswith("text/html")


def test_admin_denial_happens_before_json_parsing_and_provider_access() -> None:
    service, repository, directories, container = _service(auth_enabled=True)
    app = FastAPI()
    app.state.container = container
    app.state.p115_sorting_rules_service = service
    codec = LegacySessionCodec.from_secret_key(b"p6-sorting-rules-denial-session")
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)

    with TestClient(app) as client:
        response = client.post(
            "/api/p115/sorting_rules",
            content="[",
            headers={"content-type": "application/json"},
        )

    assert response.status_code == 403
    assert response.json()["status"] == "error"
    assert repository.saves == []
    assert directories.calls == []


def test_legacy_quick_deploy_rule_save_reuses_the_outer_auth_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executed: list[tuple[str, object]] = []
    connections: list[object] = []

    class Cursor:
        def execute(self, statement: str, parameters: object = None) -> None:
            executed.append((" ".join(statement.split()), parameters))

        def fetchone(self) -> dict[str, str]:
            return {"account_id": "115001"}

    class Connection:
        def __init__(self) -> None:
            self.cursor_value = Cursor()

        def cursor(self) -> Cursor:
            return self.cursor_value

        def commit(self) -> None:
            executed.append(("COMMIT", None))

    @contextmanager
    def connection_factory():
        connection = Connection()
        connections.append(connection)
        yield connection

    monkeypatch.setattr(settings_db, "get_db_connection", connection_factory)

    with settings_db.p115_auth_write_lock():
        settings_db.save_p115_sorting_rules([{"id": "movies"}], account_id="115001")

    statements = [statement for statement, _parameters in executed]
    assert len(connections) == 2
    assert sum("pg_advisory_lock" in statement for statement in statements) == 1
    assert sum("pg_advisory_unlock" in statement for statement in statements) == 1
    setting_parameters = [
        parameters for statement, parameters in executed if statement.startswith("INSERT INTO app_settings")
    ]
    assert setting_parameters == [
        ("p115_sorting_rules", '[{"id": "movies"}]'),
        ("p115_sorting_rules_account_id", '"115001"'),
    ]
