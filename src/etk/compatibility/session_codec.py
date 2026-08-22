from __future__ import annotations

import os
import secrets
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from flask import Flask
from flask.sessions import SecureCookieSessionInterface

SECRET_FILE_NAME = ".flask_secret"


def load_or_create_secret_key(data_dir: Path) -> bytes:
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / SECRET_FILE_NAME
    try:
        value = path.read_bytes()
    except FileNotFoundError:
        value = secrets.token_bytes(24)
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            value = path.read_bytes()
        else:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(value)
    if not value:
        raise RuntimeError(f"legacy Flask session key is empty: {path}")
    return value


def configure_legacy_session(
    app: Flask,
    data_dir: Path,
    *,
    secret_key: bytes | None = None,
) -> None:
    app.secret_key = secret_key if secret_key is not None else load_or_create_secret_key(data_dir)
    app.config.setdefault("SESSION_COOKIE_NAME", "session")


class LegacySessionCodec:
    """Read and write the same signed cookie payload as Flask's session interface."""

    def __init__(self, app: Flask):
        serializer = SecureCookieSessionInterface().get_signing_serializer(app)
        if serializer is None:
            raise RuntimeError("Flask session serializer is unavailable")
        self._serializer = serializer

    def dumps(self, values: Mapping[str, Any]) -> str:
        return self._serializer.dumps(dict(values))

    def loads(self, value: str, *, max_age: int | None = None) -> dict[str, Any]:
        payload = self._serializer.loads(value, max_age=max_age)
        if not isinstance(payload, dict):
            raise TypeError("legacy session payload is not an object")
        return payload
