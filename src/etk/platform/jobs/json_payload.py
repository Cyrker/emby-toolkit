from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from typing import Any

from .types import InvalidJsonPayloadError, JsonObject


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
