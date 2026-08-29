from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from email import policy
from email.parser import BytesParser
from typing import Any

from fastapi import APIRouter, Request
from pydantic import SecretStr
from starlette.concurrency import run_in_threadpool
from starlette.responses import Response
from werkzeug.exceptions import BadRequest, NotFound, UnsupportedMediaType
from werkzeug.http import http_date

from etk.compatibility.response_contract import fastapi_json_response
from etk.domains.subscription.portal import (
    PORTAL_SUBSCRIPTION_USE_CASE_PROVIDER,
    PortalSubscriptionHttpResult,
    PortalSubscriptionService,
)
from etk.platform.web import PermissionPolicy, get_request_session

from .playback import PlaybackHttpResult, PlaybackReportingService
from .users import UsersHttpResult, UsersService
from .users_effects import IdentityEffectsHttpResult, IdentityEffectsService

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


def _effects_service(request: Request) -> IdentityEffectsService:
    direct = getattr(request.app.state, "identity_effects_service", None)
    if isinstance(direct, IdentityEffectsService):
        return direct
    value = request.app.state.container.use_case("users.effects")
    if not isinstance(value, IdentityEffectsService):
        raise TypeError("users effects provider returned an invalid service")
    return value


def _playback_service(request: Request) -> PlaybackReportingService:
    direct = getattr(request.app.state, "playback_reporting_service", None)
    if isinstance(direct, PlaybackReportingService):
        return direct
    value = request.app.state.container.use_case("users.playback")
    if not isinstance(value, PlaybackReportingService):
        raise TypeError("users playback provider returned an invalid service")
    return value


def _portal_subscription_service(request: Request) -> PortalSubscriptionService:
    direct = getattr(request.app.state, "portal_subscription_service", None)
    if isinstance(direct, PortalSubscriptionService):
        return direct
    value = request.app.state.container.use_case(PORTAL_SUBSCRIPTION_USE_CASE_PROVIDER)
    if not isinstance(value, PortalSubscriptionService):
        raise TypeError("portal subscription provider returned an invalid service")
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
    container = request.app.state.container
    decision = PermissionPolicy(
        container.repository("identity"),
        auth_enabled=bool(container.settings.auth_enabled),
    ).require_any_login(get_request_session(request))
    if decision.allowed:
        return None
    return fastapi_json_response(
        {"status": "error", "message": decision.message},
        status_code=decision.status_code,
    )


def _response(result: UsersHttpResult) -> Response:
    return fastapi_json_response(_jsonable(result.payload), status_code=result.status_code)


def _effects_response(result: IdentityEffectsHttpResult) -> Response:
    response = fastapi_json_response(_jsonable(result.payload), status_code=result.status_code)
    if result.status_code == 503 and isinstance(result.payload, Mapping):
        operation = result.payload.get("operation")
        if isinstance(operation, Mapping):
            status_url = str(operation.get("status_url") or "").strip()
            if status_url:
                response.headers["Location"] = status_url
                response.headers["Retry-After"] = "2"
    return response


def _playback_response(result: PlaybackHttpResult) -> Response:
    return fastapi_json_response(_jsonable(result.payload), status_code=result.status_code)


def _portal_subscription_response(result: PortalSubscriptionHttpResult) -> Response:
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


async def _avatar_upload(request: Request) -> tuple[bytes | None, str | None, Response | None]:
    content_type = request.headers.get("content-type", "")
    if content_type.partition(";")[0].strip().casefold() != "multipart/form-data":
        return None, None, _html_error(UnsupportedMediaType())
    body = await request.body()
    if len(body) > 6 * 1024 * 1024:
        return (
            None,
            None,
            fastapi_json_response(
                {"status": "error", "message": "图片大小不能超过 5MB"},
                status_code=400,
            ),
        )
    try:
        message = BytesParser(policy=policy.default).parsebytes(
            b"Content-Type: " + content_type.encode("latin-1") + b"\r\nMIME-Version: 1.0\r\n\r\n" + body
        )
    except (UnicodeEncodeError, ValueError):
        message = None
    if message is None or not message.is_multipart():
        return (
            None,
            None,
            fastapi_json_response(
                {"status": "error", "message": "上传表单格式无效"},
                status_code=400,
            ),
        )
    for part in message.iter_parts():
        name = str(part.get_param("name", header="content-disposition") or "")
        if name != "avatar" or not part.get_filename():
            continue
        payload = part.get_payload(decode=True)
        if not isinstance(payload, bytes | bytearray):
            break
        return bytes(payload), part.get_content_type(), None
    return (
        None,
        None,
        fastapi_json_response(
            {"status": "error", "message": "未找到文件"},
            status_code=400,
        ),
    )


@router.get(
    "/api/admin/user_templates",
    response_model=JsonModel,
    responses=_responses(403, 500),
)
def get_all_templates(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).list_templates())


@effects_router.post(
    "/api/admin/user_templates",
    response_model=dict[str, Any],
    status_code=201,
    responses=_responses(400, 403, 404, 409, 415, 500),
    openapi_extra=_OBJECT_BODY,
)
async def create_template(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_value(request)
    if error is not None:
        return error
    return _effects_response(await run_in_threadpool(_effects_service(request).create_template, payload))


@effects_router.get(
    "/api/admin/users",
    response_model=JsonModel,
    responses=_responses(403, 500),
)
def get_all_managed_users(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _effects_response(_effects_service(request).list_managed_users())


@effects_router.post(
    "/api/admin/users/{user_id}/template",
    response_model=dict[str, Any],
    responses=_responses(400, 403, 404, 409, 415, 500, 503),
    openapi_extra=_OBJECT_BODY,
)
async def change_user_template(request: Request, user_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_value(request)
    if error is not None:
        return error
    return _effects_response(
        await run_in_threadpool(
            _effects_service(request).assign_template,
            user_id,
            payload,
            idempotency_key=request.headers.get("idempotency-key"),
        )
    )


@effects_router.post(
    "/api/admin/users/{user_id}/status",
    response_model=dict[str, Any],
    responses=_responses(400, 403, 404, 409, 415, 500, 503),
    openapi_extra=_OBJECT_BODY,
)
async def set_user_status(request: Request, user_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_value(request)
    if error is not None:
        return error
    return _effects_response(
        await run_in_threadpool(
            _effects_service(request).set_status,
            user_id,
            payload,
            idempotency_key=request.headers.get("idempotency-key"),
        )
    )


@effects_router.delete(
    "/api/admin/users/{user_id}",
    response_model=dict[str, Any],
    responses=_responses(403, 404, 409, 500, 503),
)
async def delete_user(request: Request, user_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _effects_response(
        await run_in_threadpool(
            _effects_service(request).delete_user,
            user_id,
            idempotency_key=request.headers.get("idempotency-key"),
        )
    )


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
    responses=_responses(403, 404, 409, 500),
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
    responses=_responses(403, 404, 409, 500),
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
    responses=_responses(400, 403, 409, 415, 500),
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


@effects_router.get(
    "/api/portal/telegram-bot-info",
    response_model=dict[str, Any],
    responses=_responses(401),
)
def get_telegram_bot_info(request: Request) -> Response:
    denied = _login_denial(request)
    if denied is not None:
        return denied
    return _response(_service(request).telegram_bot_info())


@effects_router.post(
    "/api/portal/subscribe",
    response_model=dict[str, Any],
    responses=_responses(400, 401, 409, 415, 500),
    openapi_extra=_OBJECT_BODY,
)
async def request_portal_subscription(request: Request) -> Response:
    denied = _login_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_value(request)
    if error is not None:
        return error
    session = get_request_session(request)
    user_id = str(session["emby_user_id"])
    is_admin = bool(request.app.state.container.repository("identity").is_admin(user_id))
    return _portal_subscription_response(
        await run_in_threadpool(
            _portal_subscription_service(request).submit,
            user_id=user_id,
            username=str(session.get("emby_username") or user_id),
            is_admin=is_admin,
            payload=payload,
            idempotency_key=request.headers.get("idempotency-key"),
        )
    )


@effects_router.post(
    "/api/portal/upload-avatar",
    response_model=dict[str, Any],
    responses=_responses(400, 401, 409, 415, 500, 503),
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "multipart/form-data": {
                    "schema": {
                        "type": "object",
                        "required": ["avatar"],
                        "properties": {
                            "avatar": {"type": "string", "format": "binary"},
                        },
                    }
                }
            },
        }
    },
)
async def upload_avatar(request: Request) -> Response:
    denied = _login_denial(request)
    if denied is not None:
        return denied
    content, content_type, error = await _avatar_upload(request)
    if error is not None:
        return error
    if content is None or content_type is None:
        return fastapi_json_response(
            {"status": "error", "message": "未找到文件"},
            status_code=400,
        )
    session = get_request_session(request)
    return _effects_response(
        await run_in_threadpool(
            _effects_service(request).upload_avatar,
            str(session["emby_user_id"]),
            content=content,
            content_type=content_type,
            idempotency_key=request.headers.get("idempotency-key"),
        )
    )


@effects_router.get(
    "/api/identity/operations/{operation_id}/status",
    response_model=dict[str, Any],
    responses=_responses(400, 401, 403, 404, 500),
)
async def get_identity_operation_status(request: Request, operation_id: str) -> Response:
    session = get_request_session(request)
    actor_user_id = str(session.get("emby_user_id") or "").strip() or None
    invitation_token = str(request.headers.get("x-invitation-token") or "").strip() or None
    container = request.app.state.container
    is_admin = (
        PermissionPolicy(
            container.repository("identity"),
            auth_enabled=bool(container.settings.auth_enabled),
        )
        .require_admin(session)
        .allowed
    )
    if actor_user_id is None and invitation_token is None and not is_admin:
        return fastapi_json_response(
            {"status": "error", "message": "需要登录或邀请凭据才能读取操作状态"},
            status_code=401,
        )
    return _effects_response(
        await run_in_threadpool(
            _effects_service(request).operation_status,
            operation_id,
            actor_user_id=actor_user_id,
            is_admin=is_admin,
            invitation_token=invitation_token,
        )
    )


@effects_router.get(
    "/api/portal/playback-report",
    response_model=dict[str, Any],
    responses=_responses(400, 401, 404, 500),
)
def get_playback_report(request: Request) -> Response:
    denied = _login_denial(request)
    if denied is not None:
        return denied
    session = get_request_session(request)
    return _playback_response(
        _playback_service(request).personal_report(
            str(session["emby_user_id"]),
            days=_query_int(request, "days", 30),
            media_type=request.query_params.get("media_type", "all"),
        )
    )


@effects_router.get(
    "/api/portal/dashboard-stats",
    response_model=dict[str, Any],
    responses=_responses(401, 404, 500),
)
def get_dashboard_stats(request: Request) -> Response:
    denied = _login_denial(request)
    if denied is not None:
        return denied
    return _playback_response(
        _playback_service(request).dashboard(
            days=_query_int(request, "days", 30),
        )
    )


@effects_router.post(
    "/api/register/invite",
    response_model=dict[str, Any],
    status_code=201,
    responses=_responses(400, 409, 415, 500, 503),
    openapi_extra=_OBJECT_BODY,
)
async def register_with_invite(request: Request) -> Response:
    payload, error = await _json_value(request)
    if error is not None:
        return error
    if not isinstance(payload, dict):
        return _effects_response(
            IdentityEffectsHttpResult(
                {"status": "error", "message": "用户名、密码和邀请码不能为空"},
                400,
            )
        )
    username = str(payload.pop("username", "") or "")
    raw_password = str(payload.pop("password", "") or "")
    invitation_token = str(payload.pop("token", "") or "")
    payload.clear()
    password = SecretStr(raw_password)
    raw_password = ""
    return _effects_response(
        await run_in_threadpool(
            _effects_service(request).register_with_invitation,
            username=username,
            password=password,
            invitation_token=invitation_token,
        )
    )


__all__ = ["effects_router", "router"]
