from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict


class IdentityRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")


class LoginRequest(IdentityRequest):
    username: str = ""
    password: str = ""


class RecoveryVerifyRequest(IdentityRequest):
    token: str = ""


class SetupRequest(IdentityRequest):
    url: str = ""
    username: str = ""
    password: str = ""


class ReauthorizeRequest(IdentityRequest):
    url: str = ""
    username: str = ""
    password: str = ""


class AuthUser(BaseModel):
    id: str
    name: str | None = None
    is_admin: bool = False
    allow_unrestricted_subscriptions: bool | None = None
    user_type: str | None = None


class AuthCheckResponse(BaseModel):
    status: Literal["setup_required", "logged_in", "login_required"]
    message: str | None = None
    user: AuthUser | None = None


class StatusResponse(BaseModel):
    status: str
    message: str | None = None
    code: str | None = None


class LoginResponse(BaseModel):
    status: Literal["ok"] = "ok"
    user: AuthUser


class SetupResponse(BaseModel):
    status: Literal["ok"] = "ok"
    message: str
    plugin_install: dict[str, Any]
    library_config: dict[str, Any]
    user: AuthUser


class ServiceStatusResponse(BaseModel):
    authorized: bool
    url: str = ""


class AuthorizationDetails(BaseModel):
    url: str
    user_id: str
    user_name: str = ""
    auth_mode: Literal["user_token"] = "user_token"


class ReauthorizeResponse(BaseModel):
    status: Literal["ok"] = "ok"
    message: str
    plugin_install: dict[str, Any]
    library_config: dict[str, Any]
    authorization: AuthorizationDetails


@dataclass(frozen=True, slots=True)
class IdentityResult:
    payload: BaseModel
    status_code: int = 200


def error_result(message: str, *, status_code: int, code: str | None = None) -> IdentityResult:
    return IdentityResult(StatusResponse(status="error", message=message, code=code), status_code)
