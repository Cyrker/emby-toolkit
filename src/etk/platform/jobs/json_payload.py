from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from typing import Any

from .types import InvalidJsonPayloadError, JsonObject

_FORBIDDEN_DURABLE_KEYS = frozenset(
    {
        "access_token",
        "api_key",
        "apikey",
        "authorization",
        "cookie",
        "cookies",
        "password",
        "secret",
        "secret_ref",
        "token",
    }
)
_FORBIDDEN_DURABLE_SUFFIXES = (
    "_api_key",
    "_cookie",
    "_password",
    "_secret",
    "_secret_ref",
    "_token",
)
_FORBIDDEN_DURABLE_COMPACT_KEYS = frozenset(key.replace("_", "") for key in _FORBIDDEN_DURABLE_KEYS)
_FORBIDDEN_DURABLE_COMPACT_SUFFIXES = tuple(suffix.replace("_", "") for suffix in _FORBIDDEN_DURABLE_SUFFIXES)


def normalize_json_object(value: Mapping[str, Any], *, field: str = "payload") -> JsonObject:
    """Return a detached JSON object after strict, lossless round-trip validation."""

    if type(value) is not dict:
        raise InvalidJsonPayloadError(f"{field} must be a plain JSON object")
    _validate_json_value(value, path=field)
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        decoded = json.loads(encoded, parse_constant=_reject_constant)
    except (TypeError, ValueError, OverflowError) as exc:
        raise InvalidJsonPayloadError(f"{field} is not strict JSON: {exc}") from exc
    if type(decoded) is not dict:
        raise InvalidJsonPayloadError(f"{field} must round-trip to a JSON object")
    return decoded


def normalize_durable_json_object(value: Mapping[str, Any], *, field: str = "payload") -> JsonObject:
    normalized = normalize_json_object(value, field=field)
    _reject_durable_secrets(normalized, path=field)
    return normalized


def canonical_json(value: Mapping[str, Any], *, field: str = "payload") -> str:
    normalized = normalize_json_object(value, field=field)
    return json.dumps(
        normalized,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def canonical_json_hash(value: Mapping[str, Any], *, field: str = "payload") -> bytes:
    return hashlib.sha256(canonical_json(value, field=field).encode("utf-8")).digest()


def _validate_json_value(value: Any, *, path: str) -> None:
    if value is None or type(value) in {bool, int, str}:
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise InvalidJsonPayloadError(f"{path} contains a non-finite number")
        return
    if type(value) is list:
        for index, item in enumerate(value):
            _validate_json_value(item, path=f"{path}[{index}]")
        return
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise InvalidJsonPayloadError(f"{path} contains a non-string object key")
            _validate_json_value(item, path=f"{path}.{key}")
        return
    if callable(value):
        raise InvalidJsonPayloadError(f"{path} contains a callable")
    raise InvalidJsonPayloadError(f"{path} contains unsupported type {type(value).__name__}")


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant {value!r} is forbidden")


def _reject_durable_secrets(value: Any, *, path: str) -> None:
    if type(value) is dict:
        for key, item in value.items():
            normalized_key = key.strip().casefold().replace("-", "_")
            compact_key = "".join(character for character in normalized_key if character.isalnum())
            if (
                normalized_key in _FORBIDDEN_DURABLE_KEYS
                or normalized_key.endswith(_FORBIDDEN_DURABLE_SUFFIXES)
                or compact_key in _FORBIDDEN_DURABLE_COMPACT_KEYS
                or compact_key.endswith(_FORBIDDEN_DURABLE_COMPACT_SUFFIXES)
            ):
                raise InvalidJsonPayloadError(f"{path} cannot persist secret field {key!r}")
            _reject_durable_secrets(item, path=f"{path}.{key}")
    elif type(value) is list:
        for index, item in enumerate(value):
            _reject_durable_secrets(item, path=f"{path}[{index}]")
