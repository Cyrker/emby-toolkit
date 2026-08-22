from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from starlette.responses import JSONResponse


def fastapi_json_response(
    payload: Any,
    *,
    status_code: int = 200,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    """Return an explicit legacy status/envelope without FastAPI response coercion."""
    return JSONResponse(content=payload, status_code=status_code, headers=dict(headers or {}))


def flask_json_response(payload: Any, *, status_code: int = 200):
    from flask import jsonify

    return jsonify(payload), status_code
