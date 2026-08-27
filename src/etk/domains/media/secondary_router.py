from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Request
from starlette.responses import Response
from werkzeug.exceptions import BadRequest, UnsupportedMediaType

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .secondary import SecondaryMediaHttpResult, SecondaryMediaService

router = APIRouter(tags=["media-secondary"])
effects_router = APIRouter(tags=["media-secondary"])
JsonValue = dict[str, Any] | list[Any] | str | int | float | bool | None


def _responses(*codes: int) -> dict[int | str, dict[str, Any]]:
    return {code: {"description": "Legacy-compatible P8 secondary media response"} for code in codes}


_JSON_BODY = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {},
            }
        },
    }
}


def _service(request: Request) -> SecondaryMediaService:
    direct = getattr(request.app.state, "secondary_media_service", None)
    if isinstance(direct, SecondaryMediaService):
        return direct
    value = request.app.state.container.use_case("media.secondary")
    if not isinstance(value, SecondaryMediaService):
        raise TypeError("media.secondary provider returned an invalid service")
    return value


def _admin_denial(request: Request) -> Response | None:
    container = request.app.state.container
    decision = PermissionPolicy(
        container.repository("identity"),
        auth_enabled=bool(container.settings.auth_enabled),
    ).require_admin(get_request_session(request))
    if decision.allowed:
        return None
    return fastapi_json_response(
        {"status": "error", "message": "需要 Emby 管理员权限才能执行此操作"},
        status_code=decision.status_code,
    )


def _processor_ready(request: Request) -> bool:
    direct = getattr(request.app.state, "processor_ready", None)
    if isinstance(direct, bool):
        return direct
    if callable(direct):
        return bool(direct())
    return bool(request.app.state.container.client("operations.job-readiness").media_ready())


def _response(result: SecondaryMediaHttpResult) -> Response:
    return fastapi_json_response(result.payload, status_code=result.status_code)


async def _json_value(request: Request) -> tuple[Any, Response | None]:
    content_type = request.headers.get("content-type", "").partition(";")[0].strip().casefold()
    if content_type != "application/json" and not (
        content_type.startswith("application/") and content_type.endswith("+json")
    ):
        return None, _html_error(UnsupportedMediaType())
    try:
        return await request.json(), None
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, _html_error(BadRequest())


def _html_error(error: BadRequest | UnsupportedMediaType) -> Response:
    headers = dict(error.get_headers())
    return Response(
        content=error.get_body(),
        status_code=int(error.code or 500),
        headers={"content-type": headers["Content-Type"]},
    )


@router.get(
    "/api/search_emby_library",
    response_model=JsonValue,
    responses=_responses(400, 500, 503),
)
def search_emby_library(request: Request) -> Response:
    if not _processor_ready(request):
        return fastapi_json_response({"error": "核心处理器未就绪。"}, status_code=503)
    return _response(_service(request).search_library(request.query_params.get("query", "")))


@router.get(
    "/api/search_studios",
    response_model=JsonValue,
    responses=_responses(403, 500),
)
def search_studios(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(_service(request).search_studios(request.query_params.get("q", "")))


@router.get(
    "/api/auto_tagging/rules",
    response_model=JsonValue,
    responses=_responses(403, 500),
)
def get_auto_tagging_rules(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).auto_tagging_rules())


@router.post(
    "/api/auto_tagging/rules",
    response_model=JsonValue,
    responses=_responses(403, 415, 500),
    openapi_extra=_JSON_BODY,
)
async def save_auto_tagging_rules(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_value(request)
    if error is not None:
        return error
    return _response(_service(request).save_auto_tagging_rules(payload))


__all__ = ["effects_router", "router"]
