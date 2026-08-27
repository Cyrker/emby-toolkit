from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Request
from starlette.concurrency import run_in_threadpool
from starlette.responses import Response
from werkzeug.exceptions import BadRequest, NotFound, UnsupportedMediaType
from werkzeug.http import http_date

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .users import UsersHttpResult, UsersService

router = APIRouter(tags=["users"])
effects_router = APIRouter(tags=["users"])
JsonModel = dict[str, Any] | list[Any]


def _responses(*codes: int) -> dict[int | str, dict[str, Any]]:
    return {code: {"description": "Legacy-compatible P8 users response"} for code in codes}


_OBJECT_BODY = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {"type": "object", "additionalProperties": True},
            }
        },
    }
}


def _service(request: Request) -> UsersService:
    direct = getattr(request.app.state, "users_service", None)
    if isinstance(direct, UsersService):
        return direct
    value = request.app.state.container.use_case("users")
    if not isinstance(value, UsersService):
        raise TypeError("users provider returned an invalid service")
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


def _login_denial(request: Request) -> Response | None:
    decision = PermissionPolicy.require_any_login(get_request_session(request))
    if decision.allowed:
        return None
    return fastapi_json_response(
        {"status": "error", "message": decision.message},
        status_code=decision.status_code,
    )


def _response(result: UsersHttpResult) -> Response:
    return fastapi_json_response(_jsonable(result.payload), status_code=result.status_code)


def _jsonable(value: Any) -> Any:
    if isinstance(value, date | datetime):
        return http_date(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_jsonable(item) for item in value]
    return value


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


def _html_error(error: BadRequest | NotFound | UnsupportedMediaType) -> Response:
    headers = dict(error.get_headers())
    return Response(
        content=error.get_body(),
        status_code=int(error.code or 500),
        headers={"content-type": headers["Content-Type"]},
    )


def _integer(value: str) -> int | Response:
    try:
        return int(value)
    except (TypeError, ValueError):
        return _html_error(NotFound())


def _query_int(request: Request, key: str, default: int) -> int:
    try:
        return int(request.query_params.get(key, default))
    except (TypeError, ValueError):
        return default


@router.get(
    "/api/admin/user_templates",
    response_model=JsonModel,
    responses=_responses(403, 500),
)
def get_all_templates(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).list_templates())


@router.put(
    "/api/admin/user_templates/{template_id}",
    response_model=dict[str, Any],
    responses=_responses(400, 403, 404, 409, 415, 500),
    openapi_extra=_OBJECT_BODY,
)
async def update_template(request: Request, template_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    parsed_id = _integer(template_id)
    if isinstance(parsed_id, Response):
        return parsed_id
    payload, error = await _json_value(request)
    if error is not None:
        return error
    return _response(await run_in_threadpool(_service(request).update_template, parsed_id, payload))


@router.delete(
    "/api/admin/user_templates/{template_id}",
    response_model=dict[str, Any],
    responses=_responses(403, 404, 500),
)
def delete_template(request: Request, template_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    parsed_id = _integer(template_id)
    if isinstance(parsed_id, Response):
        return parsed_id
    return _response(_service(request).delete_template(parsed_id))


@router.get(
    "/api/admin/invitations",
    response_model=JsonModel,
    responses=_responses(403, 500),
)
def get_all_invitations(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).list_invitations())


@router.post(
    "/api/admin/invitations",
    response_model=dict[str, Any],
    status_code=201,
    responses=_responses(400, 403, 415, 500),
    openapi_extra=_OBJECT_BODY,
)
async def create_invitation(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_value(request)
    if error is not None:
        return error
    return _response(
        await run_in_threadpool(
            _service(request).create_invitations,
            payload,
            str(request.base_url).rstrip("/"),
        )
    )


@router.delete(
    "/api/admin/invitations/{invitation_id}",
    response_model=dict[str, Any],
    responses=_responses(403, 404, 500),
)
def delete_invitation(request: Request, invitation_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    parsed_id = _integer(invitation_id)
    if isinstance(parsed_id, Response):
        return parsed_id
    return _response(_service(request).delete_invitation(parsed_id))


@router.post(
    "/api/admin/users/{user_id}/expiration",
    response_model=dict[str, Any],
    responses=_responses(400, 403, 415, 500),
    openapi_extra=_OBJECT_BODY,
)
async def set_user_expiration(request: Request, user_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_value(request)
    if error is not None:
        return error
    return _response(await run_in_threadpool(_service(request).set_expiration, user_id, payload))


@router.get(
    "/api/register/invite/validate/{token}",
    response_model=dict[str, Any],
    responses=_responses(404, 410, 500),
)
def validate_invite_token(request: Request, token: str) -> Response:
    return _response(_service(request).validate_invitation(token))


@router.get(
    "/api/portal/account-info",
    response_model=dict[str, Any],
    responses=_responses(401, 500),
)
def get_account_info(request: Request) -> Response:
    denied = _login_denial(request)
    if denied is not None:
        return denied
    session = get_request_session(request)
    return _response(_service(request).account_info(str(session["emby_user_id"])))


@router.get(
    "/api/portal/subscription-history",
    response_model=dict[str, Any],
    responses=_responses(401, 500),
)
def get_subscription_history(request: Request) -> Response:
    denied = _login_denial(request)
    if denied is not None:
        return denied
    session = get_request_session(request)
    return _response(
        _service(request).subscription_history(
            str(session["emby_user_id"]),
            page=_query_int(request, "page", 1),
            page_size=_query_int(request, "page_size", 10),
            status=request.query_params.get("status", "all"),
        )
    )


@router.post(
    "/api/portal/telegram-chat-id",
    response_model=dict[str, Any],
    responses=_responses(400, 401, 415, 500),
    openapi_extra=_OBJECT_BODY,
)
async def save_telegram_chat_id(request: Request) -> Response:
    denied = _login_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_value(request)
    if error is not None:
        return error
    session = get_request_session(request)
    return _response(
        await run_in_threadpool(
            _service(request).save_telegram_chat_id,
            str(session["emby_user_id"]),
            payload,
        )
    )


@router.get(
    "/api/portal/subscription-stats",
    response_model=dict[str, Any],
    responses=_responses(401),
)
def get_subscription_stats(request: Request) -> Response:
    denied = _login_denial(request)
    if denied is not None:
        return denied
    session = get_request_session(request)
    return _response(_service(request).subscription_stats(str(session["emby_user_id"])))


__all__ = ["effects_router", "router"]
