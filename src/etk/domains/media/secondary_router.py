from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Request
from starlette.responses import Response
from werkzeug.exceptions import BadRequest, UnsupportedMediaType

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .secondary import SecondaryMediaHttpResult, SecondaryMediaService
from .secondary_effects import SecondaryEffectsHttpResult, SecondaryEffectsService

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

_USER_VIEWS_QUERY_ALLOWLIST = frozenset(
    {
        "EnableImages",
        "EnableImageTypes",
        "GroupItems",
        "ImageTypeLimit",
        "IncludeExternalContent",
        "PresetViews",
    }
)


def _service(request: Request) -> SecondaryMediaService:
    direct = getattr(request.app.state, "secondary_media_service", None)
    if isinstance(direct, SecondaryMediaService):
        return direct
    value = request.app.state.container.use_case("media.secondary")
    if not isinstance(value, SecondaryMediaService):
        raise TypeError("media.secondary provider returned an invalid service")
    return value


def _effects_service(request: Request) -> SecondaryEffectsService:
    direct = getattr(request.app.state, "secondary_effects_service", None)
    if isinstance(direct, SecondaryEffectsService):
        return direct
    value = request.app.state.container.use_case("media.secondary.effects")
    if not isinstance(value, SecondaryEffectsService):
        raise TypeError("media.secondary.effects provider returned an invalid service")
    return value


def _permission_policy(request: Request) -> PermissionPolicy:
    container = request.app.state.container
    return PermissionPolicy(
        container.repository("identity"),
        auth_enabled=bool(container.settings.auth_enabled),
    )


def _admin_denial(request: Request) -> Response | None:
    decision = _permission_policy(request).require_admin(get_request_session(request))
    if decision.allowed:
        return None
    return fastapi_json_response(
        {"status": "error", "message": "需要 Emby 管理员权限才能执行此操作"},
        status_code=decision.status_code,
    )


def _login_denial(request: Request) -> Response | None:
    decision = _permission_policy(request).require_any_login(get_request_session(request))
    if decision.allowed:
        return None
    return fastapi_json_response(
        {"status": "error", "message": decision.message},
        status_code=decision.status_code,
    )


def _self_or_admin_denial(request: Request, user_id: str) -> Response | None:
    session = get_request_session(request)
    login = _permission_policy(request).require_any_login(session)
    if not login.allowed:
        return fastapi_json_response(
            {"status": "error", "message": login.message},
            status_code=login.status_code,
        )
    if login.user_id == str(user_id or "").strip():
        return None
    admin = _permission_policy(request).require_admin(session)
    if admin.allowed:
        return None
    return fastapi_json_response(
        {"status": "error", "message": "只能访问自己的 Emby 媒体库视图"},
        status_code=403,
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


def _effects_response(result: SecondaryEffectsHttpResult) -> Response:
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


@effects_router.get(
    "/api/emby_libraries",
    response_model=JsonValue,
    responses=_responses(401, 500),
)
def get_emby_libraries(request: Request) -> Response:
    denied = _login_denial(request)
    return denied or _effects_response(_effects_service(request).libraries())


@effects_router.get(
    "/api/emby/user/{user_id}/views",
    response_model=JsonValue,
    responses=_responses(400, 401, 403, 500, 502),
)
def get_emby_user_views(request: Request, user_id: str) -> Response:
    denied = _self_or_admin_denial(request, user_id)
    if denied is not None:
        return denied
    token = str(request.headers.get("x-emby-token") or request.query_params.get("api_key") or "").strip()
    # 配置页只能拿到脱敏值，此处必须回退到服务端保存的 Token。
    if not token or set(token) == {"*"}:
        token = request.app.state.container.settings.emby_api_key.get_secret_value().strip()
    query = {name: request.query_params[name] for name in _USER_VIEWS_QUERY_ALLOWLIST if name in request.query_params}
    return _effects_response(_effects_service(request).user_views(user_id, token, query))


@effects_router.post(
    "/api/auto_tagging/run_now",
    response_model=JsonValue,
    responses=_responses(400, 403, 409, 415, 500, 503),
    openapi_extra=_JSON_BODY,
)
async def run_auto_tagging(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    if not _processor_ready(request):
        return fastapi_json_response({"error": "核心处理器未就绪。"}, status_code=503)
    payload, error = await _json_value(request)
    if error is not None:
        return error
    return _effects_response(_effects_service(request).submit_auto_tagging(payload, operation="add"))


@effects_router.post(
    "/api/auto_tagging/clear_now",
    response_model=JsonValue,
    responses=_responses(400, 403, 409, 415, 500, 503),
    openapi_extra=_JSON_BODY,
)
async def clear_auto_tagging(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    if not _processor_ready(request):
        return fastapi_json_response({"error": "核心处理器未就绪。"}, status_code=503)
    payload, error = await _json_value(request)
    if error is not None:
        return error
    return _effects_response(_effects_service(request).submit_auto_tagging(payload, operation="remove"))


__all__ = ["effects_router", "router"]
