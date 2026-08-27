from __future__ import annotations

from pathlib import Path

from flask import Flask
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from etk.compatibility.session_codec import LazyLegacySessionCodec, LegacySessionCodec, configure_legacy_session
from etk.platform.web import CookieSessionMiddleware, PermissionPolicy, SessionState, get_request_session


def _legacy_codec(secret_key: bytes) -> LegacySessionCodec:
    app = Flask("session-contract")
    configure_legacy_session(app, Path("."), secret_key=secret_key)
    return LegacySessionCodec(app)


def test_cookie_middleware_reads_legacy_cookie_and_writes_flask_compatible_cookie() -> None:
    secret_key = b"p3-session-contract-key"
    codec = _legacy_codec(secret_key)

    async def endpoint(request: Request) -> JSONResponse:
        session = get_request_session(request)
        previous = session.get("emby_user_id")
        session.update(
            {
                "_permanent": True,
                "emby_user_id": "new-user",
                "emby_username": "New User",
                "emby_is_admin": True,
            }
        )
        return JSONResponse({"previous": previous})

    app = Starlette(routes=[Route("/session", endpoint)])
    app = CookieSessionMiddleware(app, codec_provider=lambda: codec)
    legacy_cookie = codec.dumps({"emby_user_id": "legacy-user", "_permanent": True})

    with TestClient(app) as client:
        client.cookies.set("session", legacy_cookie)
        response = client.get("/session")

    assert response.status_code == 200
    assert response.json() == {"previous": "legacy-user"}
    written = response.cookies.get("session")
    assert written is not None
    decoded = codec.loads(written)
    assert decoded == {
        "_permanent": True,
        "emby_is_admin": True,
        "emby_user_id": "new-user",
        "emby_username": "New User",
    }
    assert "HttpOnly" in response.headers["set-cookie"]
    assert "expires=" in response.headers["set-cookie"].lower()


def test_cookie_middleware_rejects_tampering_and_logout_deletes_cookie() -> None:
    codec = _legacy_codec(b"p3-session-contract-key")

    async def endpoint(request: Request) -> JSONResponse:
        session = get_request_session(request)
        observed = dict(session)
        session.clear()
        return JSONResponse(observed)

    app = CookieSessionMiddleware(
        Starlette(routes=[Route("/logout", endpoint)]),
        codec_provider=lambda: codec,
    )
    with TestClient(app) as client:
        client.cookies.set("session", "tampered-cookie")
        response = client.get("/logout")

    assert response.json() == {}
    assert "Max-Age=0" in response.headers["set-cookie"]


def test_lazy_session_codec_does_not_create_secret_until_used(tmp_path: Path) -> None:
    data_dir = tmp_path / "config"
    codec = LazyLegacySessionCodec(data_dir)

    assert not data_dir.exists()
    encoded = codec.dumps({"emby_user_id": "user"})

    assert (data_dir / ".flask_secret").is_file()
    assert codec.loads(encoded) == {"emby_user_id": "user"}


def test_permission_policy_matches_legacy_auth_matrix_and_rechecks_admin() -> None:
    class Permissions:
        def __init__(self) -> None:
            self.admins = {"admin"}
            self.active = {"admin", "user"}

        def is_admin(self, user_id: str) -> bool:
            return user_id in self.admins

        def is_active(self, user_id: str) -> bool:
            return user_id in self.active

    permissions = Permissions()
    enabled = PermissionPolicy(permissions, auth_enabled=True)

    assert enabled.require_admin(SessionState()).status_code == 403
    assert enabled.require_admin(SessionState({"emby_user_id": "user", "emby_is_admin": True})).allowed is False
    assert enabled.require_admin(SessionState({"emby_user_id": "admin"})).allowed is True
    assert enabled.require_emby_login(SessionState()).status_code == 401
    assert enabled.require_any_login(SessionState({"emby_user_id": "user"})).allowed is True
    permissions.active.remove("user")
    assert enabled.require_emby_login(SessionState({"emby_user_id": "user"})).status_code == 401
    assert enabled.require_any_login(SessionState({"emby_user_id": "user"})).status_code == 401
    assert PermissionPolicy(Permissions(), auth_enabled=False).require_admin(SessionState()).allowed is True
    assert enabled.require_config_access(SessionState(), configured=False).allowed is True
    assert enabled.require_config_access(SessionState({"is_setup_mode": True}), configured=True).allowed is True
    assert enabled.require_config_access(SessionState({"emby_user_id": "admin"}), configured=True).allowed is True
    assert enabled.require_config_access(SessionState({"emby_user_id": "user"}), configured=True).status_code == 403
    assert (
        PermissionPolicy(Permissions(), auth_enabled=False)
        .require_config_access(SessionState(), configured=True)
        .allowed
        is False
    )
