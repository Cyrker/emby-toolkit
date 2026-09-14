from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from flask import Flask

import config_manager
from etk.bootstrap.app_factory import create_app
from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.p115 import (
    P115AuthDirectoryService,
    P115AuthorizationPoll,
    P115AuthorizationSession,
    P115AuthorizationSessionStore,
    P115CredentialSnapshot,
    SqlAlchemyP115CredentialRepository,
)
from etk.domains.p115.router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware
from routes import p115 as legacy_routes

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = json.loads((REPO_ROOT / "verification/fixtures/http/p6-p115-auth-directory.json").read_text(encoding="utf-8"))
GOLDEN_CASES = tuple(FIXTURE["golden_cases"])


def test_expired_status_refreshes_once_and_returns_capacity(monkeypatch):
    client, auth, credentials, *_ = _build_native(
        credentials=P115CredentialSnapshot(access_token="old", refresh_token="refresh-old", account_id="115001")
    )
    refreshed = []

    def refresh(token):
        refreshed.append(token)
        return {"state": True, "data": {"access_token": "new", "refresh_token": "refresh-new"}}

    def check(token):
        return (
            {"state": False, "code": 40140125}
            if token == "old"
            else {
                "state": True,
                "data": {"user_id": "115001", "rt_space_info": {"all_remain": {"size": 1024, "size_format": "1 KB"}}},
            }
        )

    monkeypatch.setattr(auth, "refresh_openapi", refresh)
    monkeypatch.setattr(auth, "check_openapi", check)
    response = client.get("/api/p115/status")
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["valid"] and data["user_info"]["rt_space_info"]["all_remain"]["size_format"] == "1 KB"
    assert credentials.value.access_token == "new" and credentials.value.refresh_token == "refresh-new"
    assert refreshed == ["refresh-old"]
    client.get("/api/p115/status")
    assert refreshed == ["refresh-old"]


@pytest.mark.parametrize("wrong_account", [False, True])
def test_failed_or_wrong_account_refresh_preserves_tokens_and_reports_cookie_separately(monkeypatch, wrong_account):
    original = P115CredentialSnapshot(
        access_token="old", refresh_token="refresh-old", account_id="115001", cookie="cookie"
    )
    client, auth, credentials, *_ = _build_native(credentials=original)
    auth.cookie_check = {"state": True}
    monkeypatch.setattr(
        auth,
        "check_openapi",
        lambda token: (
            {"state": False, "code": 40140125} if token == "old" else {"state": True, "data": {"user_id": "999999"}}
        ),
    )
    monkeypatch.setattr(
        auth,
        "refresh_openapi",
        lambda _: {"state": wrong_account, "data": {"access_token": "new", "refresh_token": "refresh-new"}},
    )
    data = client.get("/api/p115/status").json()["data"]
    assert not data["valid"] and data["cookie_valid"] is True
    assert "容量" in data["msg"] and "Cookie" in data["msg"]
    assert credentials.value == original and not credentials.openapi_saves


def test_status_never_refreshes_on_transient_or_unrelated_failure(monkeypatch):
    client, auth, *_ = _build_native(
        credentials=P115CredentialSnapshot(access_token="old", refresh_token="refresh-old", account_id="115001")
    )
    auth.openapi_check = {"state": False, "code": 500, "message": "provider error"}

    def unexpected(_):
        raise AssertionError("must not refresh on unrelated failures")

    monkeypatch.setattr(auth, "refresh_openapi", unexpected)
    assert not client.get("/api/p115/status").json()["data"]["valid"]


def test_concurrent_authorization_is_not_overwritten_during_refresh(monkeypatch):
    client, auth, credentials, *_ = _build_native(
        credentials=P115CredentialSnapshot(access_token="old", refresh_token="refresh-old", account_id="115001")
    )
    monkeypatch.setattr(
        auth,
        "check_openapi",
        lambda token: (
            {"state": False, "code": 40140125} if token == "old" else {"state": True, "data": {"user_id": "115001"}}
        ),
    )
    newer = P115CredentialSnapshot(access_token="newer-login", refresh_token="newer-refresh", account_id="115001")

    def refresh(_):
        credentials.value = newer
        return {"state": True, "data": {"access_token": "stale-result", "refresh_token": "stale-refresh"}}

    monkeypatch.setattr(auth, "refresh_openapi", refresh)
    client.get("/api/p115/status")
    assert credentials.value == newer


class FakePermissions:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def is_admin(self, user_id: str) -> bool:
        self.calls.append(user_id)
        return user_id == "fixture-admin"


class FakeAuthorization:
    def __init__(self) -> None:
        self.openapi_create_error: Exception | None = None
        self.openapi_poll = P115AuthorizationPoll("waiting")
        self.cookie_poll = P115AuthorizationPoll("waiting")
        self.openapi_check: Mapping[str, Any] = {"state": False, "message": "invalid"}
        self.cookie_check: Mapping[str, Any] = {"state": False}
        self.calls: list[tuple[str, Any]] = []

    def create_openapi_qrcode(self, client_id: str):
        self.calls.append(("create_openapi", client_id))
        if self.openapi_create_error is not None:
            raise self.openapi_create_error
        return (
            {"qrcode": "fixture-openapi-qr", "uid": "openapi-uid"},
            P115AuthorizationSession(
                uid="openapi-uid",
                time_value="1",
                sign="sign",
                code_verifier="verifier",
                created_at=100,
            ),
        )

    def poll_openapi_qrcode(self, session: P115AuthorizationSession) -> P115AuthorizationPoll:
        self.calls.append(("poll_openapi", session.uid))
        return self.openapi_poll

    def create_cookie_qrcode(self, app_type: str):
        self.calls.append(("create_cookie", app_type))
        return (
            {"qrcode": "fixture-cookie-qr", "uid": "cookie-uid"},
            P115AuthorizationSession(
                uid="cookie-uid",
                time_value="2",
                sign="sign",
                app_type=app_type,
                created_at=100,
            ),
        )

    def poll_cookie_qrcode(self, session: P115AuthorizationSession) -> P115AuthorizationPoll:
        self.calls.append(("poll_cookie", session.uid))
        return self.cookie_poll

    def check_openapi(self, access_token: str) -> Mapping[str, Any]:
        self.calls.append(("check_openapi", access_token))
        return self.openapi_check

    def check_cookie(self, cookie: str, app_type: str) -> Mapping[str, Any]:
        self.calls.append(("check_cookie", (cookie, app_type)))
        return self.cookie_check

    def refresh_openapi(self, refresh_token: str) -> Mapping[str, Any]:
        self.calls.append(("refresh_openapi", refresh_token))
        return {"state": False}


class FakeCredentials:
    def __init__(self, value: P115CredentialSnapshot | None = None) -> None:
        self.value = value or P115CredentialSnapshot()
        self.openapi_saves: list[tuple[str | None, str | None, str, str | None]] = []
        self.cookie_saves: list[tuple[str, str]] = []

    def load(self) -> P115CredentialSnapshot:
        return self.value

    def save_openapi(
        self,
        access_token: str | None,
        refresh_token: str | None,
        *,
        account_id: str,
        expected_access_token: str | None = None,
    ) -> None:
        if not account_id:
            raise ValueError("无法确认 115 OpenAPI Token 的稳定账号身份")
        if expected_access_token is not None and expected_access_token != self.value.access_token:
            raise RuntimeError("凭据已发生变化")
        self.openapi_saves.append((access_token, refresh_token, account_id, expected_access_token))
        self.value = P115CredentialSnapshot(
            access_token=access_token or self.value.access_token,
            refresh_token=refresh_token or self.value.refresh_token,
            cookie=self.value.cookie,
            app_type=self.value.app_type,
            account_id=account_id,
        )

    def save_cookie(self, cookie: str, app_type: str) -> None:
        self.cookie_saves.append((cookie, app_type))
        self.value = P115CredentialSnapshot(
            cookie=cookie,
            app_type=app_type,
            account_id="115001" if "UID=115001" in cookie else self.value.account_id,
        )

    def app_label(self, app_type: str) -> str:
        return {"web": "网页版", "alipaymini": "支付宝小程序"}.get(app_type, app_type)


class FakeDirectories:
    def __init__(self, response: Mapping[str, Any] | None = None) -> None:
        self.response = response
        self.calls: list[tuple[int, str, str]] = []

    def list_entries(self, *, cid: int, search: str, expected_account_id: str):
        self.calls.append((cid, search, expected_account_id))
        return self.response


def _build_native(
    *,
    settings: Mapping[str, Any] | None = None,
    credentials: P115CredentialSnapshot | None = None,
    directories: Mapping[str, Any] | None = None,
    auth_enabled: bool = False,
) -> tuple[
    TestClient,
    FakeAuthorization,
    FakeCredentials,
    FakeDirectories,
    P115AuthorizationSessionStore,
    FakePermissions,
]:
    authorization = FakeAuthorization()
    credential_store = FakeCredentials(credentials)
    directory_gateway = FakeDirectories(directories)
    sessions = P115AuthorizationSessionStore(clock=lambda: 100)
    permissions = FakePermissions()
    values = {"auth_enabled": auth_enabled, **dict(settings or {})}
    container = AppContainer(
        Settings.model_validate(values),
        repository_providers={"identity": lambda _: permissions},
    )
    service = P115AuthDirectoryService(
        settings=lambda: container.settings,
        authorization=authorization,
        credentials=credential_store,
        directories=directory_gateway,
        sessions=sessions,
    )
    app = FastAPI()
    app.state.container = container
    app.state.p115_auth_directory_service = service
    codec = LegacySessionCodec.from_secret_key(b"p6-p115-auth-directory-session")
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return TestClient(app), authorization, credential_store, directory_gateway, sessions, permissions


def _request(client: Any, request: Mapping[str, Any]):
    if hasattr(client, "open"):
        return client.open(
            str(request["path"]),
            method=str(request["method"]),
            query_string=request.get("query"),
            json=request.get("body") if "body" in request else None,
        )
    kwargs: dict[str, Any] = {"params": request.get("query")}
    if "body" in request:
        kwargs["json"] = request["body"]
    return client.request(str(request["method"]), str(request["path"]), **kwargs)


def _snapshot(response: Any, expected: Mapping[str, Any]) -> dict[str, Any]:
    value: dict[str, Any] = {"status": response.status_code}
    if "text" in expected:
        value["text"] = response.get_data(as_text=True) if hasattr(response, "get_data") else response.text
    else:
        value["body"] = response.get_json() if hasattr(response, "get_json") else response.json()
    return value


@pytest.fixture()
def legacy_client(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setitem(config_manager.APP_CONFIG, "auth_enabled", False)
    monkeypatch.setattr(legacy_routes, "get_config", lambda: {})
    monkeypatch.setattr(legacy_routes.P115Service, "get_client", lambda: None)
    monkeypatch.setattr(legacy_routes.P115Service, "reset_cookie_client", lambda: None)
    monkeypatch.setattr("handler.p115_service.get_115_tokens", lambda: (None, None, None, "web"))
    monkeypatch.setattr("handler.p115_service.get_115_app_label", lambda value: "网页版" if value == "web" else value)
    monkeypatch.setattr("handler.p115_service.save_115_tokens", lambda *_args, **_kwargs: None)
    legacy_routes._qrcode_data.update(
        {
            "qrcode": None,
            "uid": None,
            "time": None,
            "sign": None,
            "code_verifier": None,
            "access_token": None,
            "refresh_token": None,
        }
    )
    legacy_routes._cookie_qrcode_data.update({"uid": None, "time": None, "sign": None})
    app = Flask("p6-p115-auth-directory-legacy")
    app.secret_key = "fixture"
    app.register_blueprint(legacy_routes.p115_bp)
    return app.test_client()


@pytest.mark.parametrize("case", GOLDEN_CASES, ids=[case["case_id"] for case in GOLDEN_CASES])
def test_native_and_isolated_legacy_match_frozen_auth_directory_golden(
    case: Mapping[str, Any],
    legacy_client: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    directory_response = case.get("gateway", {}).get("directories", {"state": True, "data": []})
    native, authorization, *_rest = _build_native(
        settings=case.get("settings"),
        directories=directory_response,
    )
    if case.get("gateway", {}).get("openapi_create_error"):
        monkeypatch.setattr(legacy_routes, "_generate_qrcode", lambda: None)
        authorization.openapi_create_error = RuntimeError("fixture provider failure")
    with native:
        legacy_response = _request(legacy_client, case["request"])
        native_response = _request(native, case["request"])
    assert _snapshot(legacy_response, case["response"]) == case["response"]
    assert _snapshot(native_response, case["response"]) == case["response"]


def test_openapi_qrcode_success_then_poll_persists_stable_account_identity() -> None:
    client, authorization, credentials, _directories, _sessions, _permissions = _build_native(
        settings={"p115_app_id": "fixture-app"}
    )
    authorization.openapi_poll = P115AuthorizationPoll(
        "success",
        user_info={"user_id": "115001", "user_name": "Fixture"},
        access_token="fixture-access",
        refresh_token="fixture-refresh",
    )
    with client:
        created = client.post("/api/p115/qrcode")
        polled = client.get("/api/p115/qrcode/status")

    assert created.status_code == 200
    assert created.json() == {"success": True, "data": {"qrcode": "fixture-openapi-qr", "uid": "openapi-uid"}}
    assert polled.json() == {"success": True, "status": "success", "message": "授权成功！"}
    assert credentials.openapi_saves == [("fixture-access", "fixture-refresh", "115001", None)]
    assert "fixture-access" not in str(polled.json())


def test_openapi_poll_success_without_refresh_token_preserves_legacy_no_write_contract() -> None:
    client, authorization, credentials, _directories, sessions, _permissions = _build_native()
    sessions.put(
        "openapi",
        P115AuthorizationSession(uid="uid", time_value="1", sign="s", code_verifier="v", created_at=100),
    )
    authorization.openapi_poll = P115AuthorizationPoll(
        "success",
        user_info={"user_id": "115001"},
        access_token="fixture-access-only",
        refresh_token="",
    )

    with client:
        response = client.get("/api/p115/qrcode/status")

    assert response.status_code == 200
    assert response.json() == {"success": True, "status": "success", "message": "授权成功！"}
    assert credentials.openapi_saves == []
    assert sessions.get("openapi") is None


def test_openapi_poll_rejects_unbound_token_without_persisting_secrets() -> None:
    client, authorization, credentials, _directories, sessions, _permissions = _build_native()
    sessions.put(
        "openapi",
        P115AuthorizationSession(uid="uid", time_value="1", sign="s", code_verifier="v", created_at=100),
    )
    authorization.openapi_poll = P115AuthorizationPoll(
        "success",
        user_info={},
        access_token="fixture-secret-access",
        refresh_token="fixture-secret-refresh",
    )
    with client:
        response = client.get("/api/p115/qrcode/status")
    assert response.status_code == 500
    assert response.json()["status"] == "error"
    assert credentials.openapi_saves == []
    assert "fixture-secret" not in response.text


def test_cookie_qrcode_success_and_app_mismatch_fail_closed() -> None:
    client, authorization, credentials, _directories, _sessions, _permissions = _build_native()
    authorization.cookie_poll = P115AuthorizationPoll(
        "success",
        cookie="UID=115001_fixture; CID=fixture",
        app_type="alipaymini",
    )
    with client:
        created = client.get("/api/p115/cookie_qrcode?app=alipaymini")
        mismatch = client.get("/api/p115/cookie_qrcode/status?app=web")
        accepted = client.get("/api/p115/cookie_qrcode/status?app=alipaymini")
    assert created.json() == {"success": True, "data": {"qrcode": "fixture-cookie-qr"}}
    assert mismatch.json() == {"success": False, "status": "expired", "message": "请先获取二维码"}
    assert accepted.json() == {"success": True, "status": "success", "message": "Cookie 获取成功！"}
    assert credentials.cookie_saves == [("UID=115001_fixture; CID=fixture", "alipaymini")]


def test_callback_resolves_account_identity_from_token_and_never_echoes_tokens() -> None:
    client, authorization, credentials, _directories, _sessions, _permissions = _build_native()
    authorization.openapi_check = {"state": True, "data": {"user_id": "115001", "user_name": "Fixture"}}
    encoded = base64.b64encode(
        json.dumps({"access_token": "fixture-access", "refresh_token": "fixture-refresh"}).encode()
    ).decode()
    with client:
        response = client.get("/api/p115/auto_save_auth", params={"data": encoded})
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/html; charset=utf-8"
    assert "授权成功" in response.text
    assert "fixture-access" not in response.text
    assert authorization.calls == [("check_openapi", "fixture-access")]
    assert credentials.openapi_saves == [("fixture-access", "fixture-refresh", "115001", None)]


def test_callback_rejects_claimed_identity_that_disagrees_with_token() -> None:
    client, authorization, credentials, _directories, _sessions, _permissions = _build_native()
    authorization.openapi_check = {"state": True, "data": {"user_id": "115001", "user_name": "Fixture"}}
    encoded = base64.b64encode(
        json.dumps(
            {
                "access_token": "fixture-secret-access",
                "refresh_token": "fixture-secret-refresh",
                "user_id": "115999",
            }
        ).encode()
    ).decode()
    with client:
        response = client.get("/api/p115/auto_save_auth", params={"data": encoded})

    assert response.status_code == 400
    assert response.text == "解析授权码失败: 授权数据中的账号身份与 Token 不一致"
    assert "fixture-secret" not in response.text
    assert authorization.calls == [("check_openapi", "fixture-secret-access")]
    assert credentials.openapi_saves == []


def test_status_checks_one_immutable_credential_snapshot_and_binds_identity_conditionally() -> None:
    snapshot = P115CredentialSnapshot(
        access_token="fixture-access",
        refresh_token="fixture-refresh",
        cookie="UID=115001_fixture; CID=fixture",
        app_type="web",
        account_id="115001",
    )
    client, authorization, credentials, _directories, _sessions, _permissions = _build_native(credentials=snapshot)
    authorization.openapi_check = {"state": True, "data": {"user_id": "115001", "user_name": "Fixture"}}
    authorization.cookie_check = {"state": True, "data": []}
    with client:
        response = client.get("/api/p115/status")
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["valid"] is True
    assert data["cookie_valid"] is True
    assert data["msg"] == "Token + Cookie 均有效（网页版）"
    assert credentials.openapi_saves == [(None, None, "115001", "fixture-access")]


def test_directory_listing_normalizes_openapi_and_cookie_folder_shapes() -> None:
    response = {
        "state": True,
        "data": [
            {"fid": "10", "fn": "OpenAPI", "fc": 0, "pid": "7"},
            {"cid": "11", "n": "Cookie", "ico": "folder", "pid": "7"},
            {"fid": "12", "fn": "video.mkv", "fc": 1, "pid": "7"},
        ],
        "path": [{"file_name": "Fixture Root"}],
    }
    client, _authorization, _credentials, directories, _sessions, _permissions = _build_native(
        credentials=P115CredentialSnapshot(account_id="115001"),
        directories=response,
    )
    with client:
        result = client.get("/api/p115/dirs", params={"cid": "7", "search": "fixture"})
    assert result.status_code == 200
    assert result.json() == {
        "success": True,
        "data": [
            {"id": "10", "name": "OpenAPI", "parent_id": "7"},
            {"id": "11", "name": "Cookie", "parent_id": "7"},
        ],
        "current": {"id": "7", "name": "Fixture Root"},
    }
    assert directories.calls == [(7, "fixture", "115001")]


def test_admin_denial_precedes_provider_and_body_processing() -> None:
    client, authorization, credentials, directories, _sessions, permissions = _build_native(auth_enabled=True)
    with client:
        response = client.post("/api/p115/cookie", content="not-json")
    assert response.status_code == 403
    assert response.json() == {"status": "error", "message": "需要 Emby 管理员权限才能执行此操作"}
    assert authorization.calls == []
    assert credentials.cookie_saves == []
    assert directories.calls == []
    assert permissions.calls == []


def test_openapi_declares_exact_native_slice_without_automatic_422() -> None:
    client, *_rest = _build_native()
    schema = cast(FastAPI, client.app).openapi()
    expected = {(route["method"].lower(), route["path"]) for route in FIXTURE["routes"]}
    expected_paths = {path for _method, path in expected}
    actual = {
        (method, path)
        for path, operations in schema["paths"].items()
        if path in expected_paths
        for method in operations
    }
    assert actual == expected
    for method, path in expected:
        assert "422" not in schema["paths"][path][method]["responses"], (method, path)


def test_app_factory_wires_native_p115_credential_repository(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = SimpleNamespace(unit_of_work=lambda: None)
    monkeypatch.setattr("etk.bootstrap.app_factory._create_database_runtime", lambda _container: database)
    app = create_app(
        start_runtime=False,
        data_dir=tmp_path,
        secret_key=b"fixture-p115-credential-wiring-key",
    )

    credentials = app.state.container.repository("p115.credentials")
    service = app.state.container.use_case("p115.auth-directory")

    assert isinstance(credentials, SqlAlchemyP115CredentialRepository)
    assert service._credentials is credentials


def test_fixture_routes_are_exact_legacy_bindings() -> None:
    snapshot = json.loads(
        (REPO_ROOT / "verification/contracts/http/legacy-runtime-routes.json").read_text(encoding="utf-8")
    )
    expected = {(route["method"], route["path"]) for route in FIXTURE["routes"]}
    actual = {
        (method, route["path"])
        for route in snapshot["rules"]
        for method in route["methods"]
        if (method, route["path"]) in expected
    }
    assert actual == expected


def test_committed_ownership_and_openapi_cover_only_auth_directory_slice() -> None:
    expected = {(route["method"], route["path"]) for route in FIXTURE["routes"]}
    assert len(expected) == 8

    ownership = json.loads((REPO_ROOT / "verification/contracts/route-ownership.yaml").read_text(encoding="utf-8"))
    owned_bindings = [
        row for row in ownership["method_bindings"] if row.get("validation_owner") == "p115/auth-directory"
    ]
    assert {(row["method"], row["path"]) for row in owned_bindings} == expected
    assert all(
        row["status"] == "new"
        and row["new_owner"] == "etk.domains.p115.router"
        and row["fixture"] == "verification/fixtures/http/p6-p115-auth-directory.json"
        for row in owned_bindings
    )

    owned_decorators = [row for row in ownership["routes"] if row.get("validation_owner") == "p115/auth-directory"]
    assert len(owned_decorators) == 8
    assert {(method, row["path"]) for row in owned_decorators for method in row["methods"]} == expected
    assert all(
        row["status"] == "new"
        and row["new_owner"] == "etk.domains.p115.router"
        and row["fixture"] == "verification/fixtures/http/p6-p115-auth-directory.json"
        for row in owned_decorators
    )

    schema = json.loads((REPO_ROOT / "verification/contracts/http/p4-openapi.json").read_text(encoding="utf-8"))
    for method, path in expected:
        operation = schema["paths"][path][method.lower()]
        assert "422" not in operation["responses"], (method, path)

    maintenance_owned_later = {
        ("POST", "/api/p115/mkdir"),
        ("GET", "/api/p115/speedtest"),
    }
    ownership_by_operation = {(row["method"], row["path"]): row for row in ownership["method_bindings"]}
    assert all(
        ownership_by_operation[operation]["status"] == "new"
        and ownership_by_operation[operation]["new_owner"] == "etk.domains.p115.router"
        and ownership_by_operation[operation]["fixture"] == "verification/fixtures/http/p6-p115-maintenance.json"
        for operation in maintenance_owned_later
    )
    playback_owned_later = {
        ("GET", "/api/p115/play_pool"),
        ("POST", "/api/p115/play_pool"),
    }
    assert all(
        ownership_by_operation[operation]["status"] == "new"
        and ownership_by_operation[operation]["new_owner"] == "etk.domains.p115.playback_router"
        for operation in playback_owned_later
    )
