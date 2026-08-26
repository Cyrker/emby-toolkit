from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from flask import Flask, jsonify, session
from pydantic import SecretStr

from etk.bootstrap.app_factory import create_app
from etk.bootstrap.runtime_supervisor import RuntimeSupervisor
from etk.compatibility.session_codec import LegacySessionCodec, configure_legacy_session
from etk.domains.identity import IdentityService, RecoveryTokenStore
from etk.domains.identity.ports import PostAuthorizationSubmission
from etk.platform import AppContainer, Settings

REPO_ROOT = Path(__file__).resolve().parents[3]
IDENTITY_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "http" / "identity-routes.json"
SECRET_KEY = b"p3-identity-session-contract-key"
EMBY_URL = "http://example.invalid"


class SettingsHolder:
    def __init__(self, settings: Settings) -> None:
        self.value = settings


class FakeIdentityRepository:
    def __init__(self) -> None:
        self.users: list[dict[str, Any]] = []
        self.admins = {"fixture-admin"}

    def upsert_user(self, user: Mapping[str, Any]) -> None:
        self.users.append(deepcopy(dict(user)))

    def is_admin(self, user_id: str) -> bool:
        return user_id in self.admins

    def subscription_permission(self, user_id: str) -> bool:
        return user_id == "fixture-user"

    def display_names(self, user_ids: Sequence[str]) -> Mapping[str, str]:
        return {user_id: user_id for user_id in user_ids}


class FakeEmbyIdentityGateway:
    def __init__(self) -> None:
        self.authenticate_calls: list[tuple[str, str, str, str]] = []
        self.test_success = True
        self.plugin_result: dict[str, Any] = {"ok": True, "message": "ETK 插件已就绪"}
        self.library_result: dict[str, Any] = {"ok": True, "message": "媒体库加速配置已应用"}

    def authenticate(
        self,
        base_url: str,
        username: str,
        password: str,
        *,
        device_id: str,
    ) -> Mapping[str, Any] | None:
        self.authenticate_calls.append((base_url, username, password, device_id))
        if username == "invalid":
            return None
        is_admin = username == "fixture-admin"
        return {
            "AccessToken": "fixture-token",
            "User": {
                "Id": "fixture-admin" if is_admin else "fixture-user",
                "Name": "Fixture Admin" if is_admin else "Fixture User",
                "Policy": {"IsAdministrator": is_admin},
            },
        }

    def get_user(self, base_url: str, access_token: str, user_id: str) -> Mapping[str, Any] | None:
        del base_url, access_token, user_id
        return None

    def test_connection(self, base_url: str, access_token: str) -> Mapping[str, Any]:
        assert base_url == EMBY_URL
        assert access_token == "fixture-token"
        return {"success": self.test_success, "error": "fixture failure"}

    def install_plugin(self, authorization: Mapping[str, Any], settings: Settings) -> Mapping[str, Any]:
        assert authorization["AccessToken"] == "fixture-token"
        assert settings.emby_api_key.get_secret_value() == "fixture-token"
        return deepcopy(self.plugin_result)

    def configure_libraries(
        self,
        authorization: Mapping[str, Any],
        plugin_install: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        assert authorization["AccessToken"] == "fixture-token"
        assert plugin_install == self.plugin_result
        return deepcopy(self.library_result)


class FakePostAuthorizationSubmitter:
    def __init__(self) -> None:
        self.settings: list[Settings] = []

    def submit(self, settings: Settings) -> PostAuthorizationSubmission:
        self.settings.append(settings)
        return PostAuthorizationSubmission(
            job_id="00000000-0000-0000-0000-0000000001aa",
            status="queued",
            reused=False,
            resumed=False,
        )


class RecordingConfigurationWriter:
    def __init__(self, holder: SettingsHolder) -> None:
        self.holder = holder
        self.updates: list[dict[str, Any]] = []
        self.error: Exception | None = None

    def save(self, updates: Mapping[str, Any]) -> Settings:
        if self.error is not None:
            raise self.error
        copied = deepcopy(dict(updates))
        self.updates.append(copied)
        self.holder.value = self.holder.value.with_updates(copied)
        return self.holder.value


def _configured_settings() -> Settings:
    return Settings(
        emby_server_url=EMBY_URL,
        emby_api_key=SecretStr("fixture-token"),
        emby_user_id="fixture-admin",
        emby_auth_mode="user_token",
    )


def _legacy_codec() -> LegacySessionCodec:
    app = Flask("p3-identity-codec")
    configure_legacy_session(app, Path("."), secret_key=SECRET_KEY)
    return LegacySessionCodec(app)


def _fixture_cases() -> dict[str, dict[str, Any]]:
    payload = json.loads(IDENTITY_FIXTURE.read_text(encoding="utf-8"))
    return {case["case_id"]: case for case in payload["cases"]}


def _assert_case(response, case_id: str) -> None:
    expected = _fixture_cases()[case_id]["response"]
    assert response.status_code == expected["status"]
    assert response.json() == expected["body"]
    assert response.headers["content-type"] == "application/json"


def _build_app(
    tmp_path: Path,
    *,
    configured: bool,
) -> tuple[Any, SettingsHolder, FakeIdentityRepository, FakeEmbyIdentityGateway, RecordingConfigurationWriter]:
    holder = SettingsHolder(_configured_settings() if configured else Settings())
    repository = FakeIdentityRepository()
    gateway = FakeEmbyIdentityGateway()
    post_authorization = FakePostAuthorizationSubmitter()
    configuration = RecordingConfigurationWriter(holder)
    service = IdentityService(
        settings=lambda: holder.value,
        repository=repository,
        gateway=gateway,
        configuration=configuration,
        post_authorization=post_authorization,
        recovery_tokens=RecoveryTokenStore(token_factory=lambda: "fixture-token"),
    )
    container = AppContainer(
        holder.value,
        use_case_providers={"identity": lambda _: service},
    )
    app = create_app(
        start_runtime=False,
        data_dir=tmp_path / "config",
        secret_key=SECRET_KEY,
        supervisor=RuntimeSupervisor(),
        container=container,
    )
    return app, holder, repository, gateway, configuration


def test_fixture_covers_all_eight_identity_routes() -> None:
    payload = json.loads(IDENTITY_FIXTURE.read_text(encoding="utf-8"))
    assert {(item["method"], item["path"]) for item in payload["routes"]} == {
        ("GET", "/api/auth/check_status"),
        ("POST", "/api/auth/login"),
        ("POST", "/api/auth/request_recovery"),
        ("POST", "/api/auth/verify_recovery"),
        ("POST", "/api/auth/setup"),
        ("GET", "/api/auth/service_status"),
        ("POST", "/api/auth/reauthorize"),
        ("POST", "/api/auth/logout"),
    }
    assert payload["provenance"]["captured_live"] is False
    assert payload["provenance"]["external_validation"] == "pending"


def test_unconfigured_setup_recovery_and_password_non_persistence(tmp_path: Path) -> None:
    app, holder, repository, gateway, configuration = _build_app(tmp_path, configured=False)

    with TestClient(app) as client:
        _assert_case(client.get("/api/auth/check_status"), "check-setup-required")
        _assert_case(
            client.post(
                "/api/auth/login",
                json={"username": "fixture-user", "password": "fixture-password"},
            ),
            "login-setup-required",
        )
        _assert_case(client.post("/api/auth/request_recovery"), "recovery-request")
        _assert_case(
            client.post("/api/auth/verify_recovery", json={"token": "fixture-token"}),
            "recovery-verified",
        )
        _assert_case(
            client.post("/api/auth/verify_recovery", json={"token": "fixture-token"}),
            "recovery-rejected",
        )
        setup = client.post(
            "/api/auth/setup",
            json={
                "url": EMBY_URL,
                "username": "fixture-admin",
                "password": "fixture-password",
            },
        )
        _assert_case(setup, "setup-success")

        assert client.get("/api/auth/check_status").json() == {
            "status": "logged_in",
            "user": {"id": "fixture-admin", "name": "Fixture Admin", "is_admin": True},
        }

    assert holder.value.emby_auth_mode == "user_token"
    assert configuration.updates == [
        {
            "emby_server_url": EMBY_URL,
            "emby_api_key": "fixture-token",
            "emby_user_id": "fixture-admin",
            "emby_auth_mode": "user_token",
        }
    ]
    assert "fixture-password" not in repr(configuration.updates)
    assert repository.users[-1]["Id"] == "fixture-admin"
    assert gateway.authenticate_calls[-1] == (
        EMBY_URL,
        "fixture-admin",
        "fixture-password",
        "emby-toolkit-service",
    )


def test_configured_login_status_reauthorize_logout_and_cookie_bridge(tmp_path: Path) -> None:
    app, _, repository, gateway, configuration = _build_app(tmp_path, configured=True)
    codec = _legacy_codec()

    with TestClient(app) as client:
        _assert_case(client.get("/api/auth/check_status"), "check-login-required")
        _assert_case(client.get("/api/auth/service_status"), "service-status-login-required")
        _assert_case(client.post("/api/auth/setup"), "setup-forbidden")
        _assert_case(client.post("/api/auth/reauthorize"), "service-status-login-required")
        _assert_case(
            client.post(
                "/api/auth/reauthorize",
                content="{",
                headers={"content-type": "application/json"},
            ),
            "service-status-login-required",
        )
        _assert_case(
            client.post(
                "/api/auth/setup",
                json={
                    "url": EMBY_URL,
                    "username": "fixture-admin",
                    "password": "fixture-password",
                },
            ),
            "setup-forbidden",
        )
        client.cookies.set(
            "session",
            codec.dumps(
                {
                    "_permanent": True,
                    "emby_user_id": "fixture-user",
                    "emby_username": "Fixture User",
                    "emby_is_admin": False,
                }
            ),
        )
        _assert_case(client.get("/api/auth/check_status"), "check-logged-in")
        _assert_case(client.get("/api/auth/service_status"), "service-status-authorized")
        _assert_case(
            client.post(
                "/api/auth/reauthorize",
                json={
                    "url": EMBY_URL,
                    "username": "fixture-admin",
                    "password": "fixture-password",
                },
            ),
            "reauthorize-success",
        )
        _assert_case(client.post("/api/auth/logout"), "logout-success")
        assert "Max-Age=0" in client.post("/api/auth/logout").headers["set-cookie"]
        _assert_case(
            client.post(
                "/api/auth/login",
                json={"username": "invalid", "password": "fixture-password"},
            ),
            "login-invalid",
        )
        login = client.post(
            "/api/auth/login",
            json={"username": "fixture-user", "password": "fixture-password"},
        )
        _assert_case(login, "login-success")
        new_cookie = login.cookies.get("session")

    assert new_cookie is not None
    assert codec.loads(new_cookie) == {
        "_permanent": True,
        "emby_is_admin": False,
        "emby_user_id": "fixture-user",
        "emby_username": "Fixture User",
    }
    assert repository.users[-1]["Id"] == "fixture-user"
    assert configuration.updates[-1]["emby_user_id"] == "fixture-admin"
    assert gateway.authenticate_calls[-1][3] == "my-emby-toolkit-auth-v4"

    legacy_app = Flask("p3-legacy-cookie-canary")
    configure_legacy_session(legacy_app, Path("."), secret_key=SECRET_KEY)

    @legacy_app.get("/portal-canary")
    def portal_canary():
        return jsonify(
            {
                "emby_user_id": session.get("emby_user_id"),
                "emby_username": session.get("emby_username"),
                "emby_is_admin": session.get("emby_is_admin"),
            }
        )

    with legacy_app.test_client() as client:
        client.set_cookie("session", new_cookie)
        assert client.get("/portal-canary").get_json() == {
            "emby_user_id": "fixture-user",
            "emby_username": "Fixture User",
            "emby_is_admin": False,
        }


def test_setup_failure_does_not_persist_account_password_or_start_session(tmp_path: Path) -> None:
    app, _, repository, _, configuration = _build_app(tmp_path, configured=False)
    configuration.error = RuntimeError("fixture persistence failure")

    with TestClient(app) as client:
        response = client.post(
            "/api/auth/setup",
            json={
                "url": EMBY_URL,
                "username": "fixture-admin",
                "password": "fixture-password",
            },
        )
        assert response.status_code == 500
        assert response.json() == {"status": "error", "message": "保存配置失败，请检查日志"}
        assert response.cookies.get("session") is None

    assert configuration.updates == []
    assert repository.users == []


@pytest.mark.parametrize("path", ["/api/auth/login", "/api/auth/verify_recovery"])
def test_identity_body_validation_preserves_legacy_http_errors(tmp_path: Path, path: str) -> None:
    app, *_ = _build_app(tmp_path, configured=True)

    with TestClient(app) as client:
        unsupported = client.post(path)
        malformed = client.post(
            path,
            content="{",
            headers={"content-type": "application/json"},
        )
        wrong_shape = client.post(path, json=[])

    assert unsupported.status_code == 415
    assert malformed.status_code == 400
    assert wrong_shape.status_code == 500
    for response in (unsupported, malformed, wrong_shape):
        assert response.headers["content-type"] == "text/html; charset=utf-8"
        assert response.text.startswith("<!doctype html>")


def test_recovery_token_is_single_use_thread_safe_and_expires() -> None:
    now = [100.0]
    store = RecoveryTokenStore(
        token_factory=lambda: "fixture-token",
        clock=lambda: now[0],
        lifetime_seconds=300,
    )
    assert store.issue() == "FIXTURE-TOKEN"
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(store.consume, ["fixture-token"] * 16))
    assert results.count(True) == 1
    assert results.count(False) == 15

    assert store.issue() == "FIXTURE-TOKEN"
    now[0] = 401.0
    assert store.consume("fixture-token") is False
