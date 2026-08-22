from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from typing import Any

from pydantic import BaseModel, SecretBytes, SecretStr

REDACTED = "<redacted>"
RECURSIVE = "<recursive>"

_SENSITIVE_PARTS = frozenset(
    {
        "authorization",
        "credential",
        "credentials",
        "cookie",
        "passwd",
        "password",
        "secret",
        "token",
    }
)
_SENSITIVE_COMPOUNDS = frozenset(
    {
        "access_key",
        "api_key",
        "api_secret",
        "auth_key",
        "auth_tokens",
        "license_key",
        "private_key",
        "refresh_token",
        "session_key",
    }
)
_URL_USERINFO = re.compile(
    r"(?P<prefix>\b[a-z][a-z0-9+.-]*://[^/@\s:]+:)(?P<secret>[^@/\s]+)(?=@)",
    flags=re.IGNORECASE,
)
_QUERY_SECRET = re.compile(
    r"(?P<prefix>[?&](?:access_token|api_key|api_secret|password|refresh_token|secret|token)=)[^&#\s]+",
    flags=re.IGNORECASE,
)
_ASSIGNED_SECRET = re.compile(
    r"(?P<prefix>\b(?:access_token|api_key|api_secret|authorization|password|refresh_token|secret|token)\s*[=:]\s*)"
    r"(?:(?P<quote>['\"])[^'\"\r\n]*(?P=quote)|[^\s,;&#'\"]+)",
    flags=re.IGNORECASE,
)
_BEARER_TOKEN = re.compile(r"(?P<prefix>\bBearer\s+)[A-Za-z0-9._~+/=-]+", flags=re.IGNORECASE)


def is_sensitive_key(key: object) -> bool:
    """Return whether a structured field name denotes secret material."""

    raw = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(key))
    normalized = re.sub(r"[^a-z0-9]+", "_", raw.casefold()).strip("_")
    if not normalized:
        return False
    if normalized in _SENSITIVE_COMPOUNDS:
        return True
    parts = set(normalized.split("_"))
    if parts & _SENSITIVE_PARTS:
        return True
    return any(normalized.endswith(f"_{suffix}") for suffix in _SENSITIVE_COMPOUNDS)


def redact_sensitive(value: Any) -> Any:
    """Recursively copy structured data while replacing sensitive fields."""

    return _redact(value, active=set())


def _redact(value: Any, *, active: set[int]) -> Any:
    if isinstance(value, (SecretStr, SecretBytes)):
        return REDACTED
    if isinstance(value, str):
        return _redact_text(value)
    if value is None or isinstance(value, (bool, int, float)):
        return value

    identity = id(value)
    if identity in active:
        return RECURSIVE

    active.add(identity)
    try:
        if isinstance(value, Mapping):
            return {
                str(key): REDACTED if is_sensitive_key(key) else _redact(item, active=active)
                for key, item in value.items()
            }
        if isinstance(value, BaseModel):
            redacted = getattr(value, "redacted_dict", None)
            if callable(redacted):
                return redacted()
            return _redact(value.model_dump(), active=active)
        if is_dataclass(value) and not isinstance(value, type):
            return _redact(asdict(value), active=active)
        if isinstance(value, (list, tuple)):
            return [_redact(item, active=active) for item in value]
        if isinstance(value, (set, frozenset)):
            return [_redact(item, active=active) for item in sorted(value, key=repr)]
        return repr(value)
    finally:
        active.remove(identity)


def _redact_text(value: str) -> str:
    redacted = _URL_USERINFO.sub(rf"\g<prefix>{REDACTED}", value)
    redacted = _QUERY_SECRET.sub(rf"\g<prefix>{REDACTED}", redacted)
    redacted = _BEARER_TOKEN.sub(rf"\g<prefix>{REDACTED}", redacted)
    redacted = _ASSIGNED_SECRET.sub(rf"\g<prefix>{REDACTED}", redacted)
    return redacted
