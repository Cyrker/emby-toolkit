from __future__ import annotations

import json
import os
import secrets
import threading
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from itsdangerous import BadData, URLSafeTimedSerializer

SECRET_FILE_NAME = ".flask_secret"


class _CompactJsonSerializer:
    """Keep the compact JSON representation used by the legacy cookie contract."""

    def dumps(self, value: Any) -> str:
        return json.dumps(value, separators=(",", ":"), ensure_ascii=True)

    def loads(self, value: str | bytes) -> Any:
        return json.loads(value)


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
        raise RuntimeError(f"session key is empty: {path}")
    return value


class NativeSessionCodec:
    """Signed, timed session codec with no Flask runtime dependency."""

    def __init__(self, secret_key: bytes) -> None:
        if not secret_key:
            raise ValueError("session secret key must not be empty")
        self._serializer = URLSafeTimedSerializer(
            secret_key,
            salt="cookie-session",
            serializer=_CompactJsonSerializer(),
            signer_kwargs={"key_derivation": "hmac", "digest_method": "sha1"},
        )

    def dumps(self, values: Mapping[str, Any]) -> str:
        return self._serializer.dumps(dict(values))

    def loads(self, value: str, *, max_age: int | None = None) -> dict[str, Any]:
        payload = self._serializer.loads(value, max_age=max_age)
        if not isinstance(payload, dict):
            raise TypeError("session payload is not an object")
        return payload


class LazyNativeSessionCodec:
    """Load the native serializer and key only when a session is read or written."""

    def __init__(self, data_dir: Path, *, secret_key: bytes | None = None) -> None:
        self._data_dir = data_dir
        self._secret_key = secret_key
        self._lock = threading.Lock()
        self._codec: NativeSessionCodec | None = None

    def dumps(self, values: Mapping[str, Any]) -> str:
        return self._resolve().dumps(values)

    def loads(self, value: str, *, max_age: int | None = None) -> dict[str, Any]:
        return self._resolve().loads(value, max_age=max_age)

    def _resolve(self) -> NativeSessionCodec:
        if self._codec is None:
            with self._lock:
                if self._codec is None:
                    self._codec = NativeSessionCodec(self._secret_key or load_or_create_secret_key(self._data_dir))
        return self._codec


__all__ = ["BadData", "LazyNativeSessionCodec", "NativeSessionCodec", "SECRET_FILE_NAME", "load_or_create_secret_key"]
