from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class P115Result:
    payload: dict[str, Any] | list[Any] | str
    status_code: int = 200
    media_type: str = "application/json"


@dataclass(frozen=True, slots=True)
class P115CredentialSnapshot:
    access_token: str = field(default="", repr=False)
    refresh_token: str = field(default="", repr=False)
    cookie: str = field(default="", repr=False)
    app_type: str = "web"
    account_id: str = ""

    @property
    def has_openapi(self) -> bool:
        return bool(self.access_token)

    @property
    def has_cookie(self) -> bool:
        return bool(self.cookie)


@dataclass(frozen=True, slots=True)
class P115AuthorizationSession:
    uid: str
    created_at: float
    time_value: str = ""
    sign: str = ""
    app_type: str = "web"
    code_verifier: str = field(default="", repr=False)


@dataclass(frozen=True, slots=True)
class P115AuthorizationPoll:
    status: str
    message: str = ""
    user_info: dict[str, Any] = field(default_factory=dict)
    access_token: str = field(default="", repr=False)
    refresh_token: str = field(default="", repr=False)
    cookie: str = field(default="", repr=False)
    app_type: str = "web"


__all__ = [
    "P115AuthorizationPoll",
    "P115AuthorizationSession",
    "P115CredentialSnapshot",
    "P115Result",
]
