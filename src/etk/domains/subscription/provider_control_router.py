from __future__ import annotations

import json
import secrets
from collections.abc import Mapping
from typing import Any

from fastapi import APIRouter, Request
from starlette.concurrency import run_in_threadpool
from starlette.responses import JSONResponse, Response
from werkzeug.exceptions import BadRequest, InternalServerError, UnsupportedMediaType

from etk.compatibility.response_contract import fastapi_json_response
from etk.domains.sharing.service import SharingHttpResult
from etk.platform.web import PermissionPolicy, get_request_session

from .provider_control import PROVIDER_CONTROL_USE_CASE, ProviderControlService

router = APIRouter(prefix="/api/subscription", tags=["subscription", "sharing"])


def _responses(*status_codes: int) -> dict[int | str, dict[str, Any]]:
    return {status: {"description": "Provider-control response"} for status in status_codes}


_OBJECT_BODY = {
    "requestBody": {
        "required": True,
        "content": {"application/json": {"schema": {"type": "object", "additionalProperties": True}}},
    }
}
_OPTIONAL_OBJECT_BODY = {
    "requestBody": {
        "required": False,
        "content": {"application/json": {"schema": {"type": "object", "additionalProperties": True}}},
    }
}


def _service(request: Request) -> ProviderControlService:
    value = request.app.state.container.use_case(PROVIDER_CONTROL_USE_CASE)
    if not isinstance(value, ProviderControlService):
        raise TypeError("subscription.provider-control provider returned an invalid service")
    return value


def _admin_denial(request: Request) -> JSONResponse | None:
    container = request.app.state.container
    decision = PermissionPolicy(
        container.repository("identity"),
        auth_enabled=bool(container.settings.auth_enabled),
    ).require_admin(get_request_session(request))
    if decision.allowed:
        return None
    return fastapi_json_response(
        {"status": "error", "message": "需要 Emby 管理员权限才能执行此操作"},
        status_code=403,
    )


def _principal_id(request: Request) -> str:
    return str(get_request_session(request).get("emby_user_id") or "local").strip() or "local"


def _idempotency_key(request: Request) -> str:
    value = str(request.headers.get("idempotency-key") or "").strip()
    return value or secrets.token_urlsafe(24)


def _response(result: SharingHttpResult) -> JSONResponse:
    return fastapi_json_response(
        result.payload,
        status_code=result.status_code,
        headers=dict(result.headers),
    )


def _html_error(error: BadRequest | InternalServerError | UnsupportedMediaType) -> Response:
    headers = dict(error.get_headers())
    return Response(
        content=error.get_body(),
        status_code=int(error.code or 500),
        headers={"content-type": headers["Content-Type"]},
    )


async def _json_object(
    request: Request,
    *,
    allow_empty: bool = False,
) -> tuple[dict[str, Any] | None, Response | None]:
    raw = await request.body()
    if allow_empty and not raw:
        return {}, None
    content_type = request.headers.get("content-type", "").partition(";")[0].strip().casefold()
    if content_type != "application/json" and not (
        content_type.startswith("application/") and content_type.endswith("+json")
    ):
        return None, _html_error(UnsupportedMediaType())
    if not raw:
        return None, _html_error(BadRequest())
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, _html_error(BadRequest())
    if not isinstance(value, Mapping):
        return None, _html_error(InternalServerError())
    return dict(value), None


def _query_int(request: Request, key: str, *, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(str(request.query_params.get(key, default)).strip())
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(value, maximum))


@router.get(
    "/cloud/resources",
    response_model=dict[str, Any],
    responses=_responses(200, 400, 403, 500),
)
async def cloud_resources(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        result = await run_in_threadpool(
            _service(request).cloud_resources,
            query=dict(request.query_params),
            idempotency_key=str(request.headers.get("idempotency-key") or "").strip() or None,
        )
    except Exception:
        return _html_error(InternalServerError())
    return _response(result)


@router.post(
    "/cloud/download",
    response_model=dict[str, Any],
    responses=_responses(200, 202, 400, 401, 403, 409, 415, 500, 503),
    openapi_extra=_OBJECT_BODY,
)
async def cloud_download(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    try:
        result = await run_in_threadpool(
            _service(request).cloud_download,
            payload,
            principal_id=_principal_id(request),
            idempotency_key=_idempotency_key(request),
        )
    except Exception:
        return _html_error(InternalServerError())
    return _response(result)


@router.get(
    "/hdhive/config",
    response_model=dict[str, Any],
    responses=_responses(200, 403, 500),
)
async def hdhive_configuration(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        return _response(await run_in_threadpool(_service(request).hdhive_configuration))
    except Exception:
        return _html_error(InternalServerError())


@router.post(
    "/hdhive/config",
    response_model=dict[str, Any],
    responses=_responses(200, 400, 403, 415, 500),
    openapi_extra=_OBJECT_BODY,
)
async def save_hdhive_configuration(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    try:
        return _response(await run_in_threadpool(_service(request).save_hdhive_configuration, payload))
    except Exception:
        return _html_error(InternalServerError())


@router.get(
    "/hdhive/authorize_url",
    response_model=dict[str, Any],
    responses=_responses(200, 202, 403, 409, 500, 503),
)
async def hdhive_authorize_url(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        return _response(
            await run_in_threadpool(
                _service(request).hdhive_authorize_url,
                principal_id=_principal_id(request),
                idempotency_key=_idempotency_key(request),
            )
        )
    except Exception:
        return _html_error(InternalServerError())


@router.post(
    "/hdhive/clear_authorization",
    response_model=dict[str, Any],
    responses=_responses(200, 202, 403, 409, 500, 503),
)
async def clear_hdhive_authorization(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        result = await run_in_threadpool(
            _service(request).clear_hdhive_authorization,
            principal_id=_principal_id(request),
            idempotency_key=_idempotency_key(request),
        )
    except Exception:
        return _html_error(InternalServerError())
    return _response(result)


@router.get(
    "/hdhive/subscriptions",
    response_model=dict[str, Any],
    responses=_responses(200, 401, 403, 500, 503),
)
async def hdhive_subscriptions(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        result = await run_in_threadpool(
            _service(request).hdhive_subscriptions,
            target_type=str(request.query_params.get("target_type") or "").strip() or None,
            status=str(request.query_params.get("status") or "active").strip() or "active",
            query=str(request.query_params.get("q") or "").strip() or None,
            page=_query_int(request, "page", default=1, minimum=1, maximum=10_000),
            page_size=_query_int(request, "page_size", default=100, minimum=1, maximum=200),
        )
    except Exception:
        return _html_error(InternalServerError())
    return _response(result)


@router.post(
    "/hdhive/subscriptions",
    response_model=dict[str, Any],
    responses=_responses(200, 202, 400, 401, 403, 409, 415, 500, 503),
    openapi_extra=_OBJECT_BODY,
)
async def create_hdhive_subscription(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    try:
        result = await run_in_threadpool(
            _service(request).create_hdhive_subscription,
            payload,
            principal_id=_principal_id(request),
            idempotency_key=_idempotency_key(request),
        )
    except Exception:
        return _html_error(InternalServerError())
    return _response(result)


@router.get(
    "/hdhive/subscriptions/check",
    response_model=dict[str, Any],
    responses=_responses(200, 400, 401, 403, 500, 503),
)
async def check_hdhive_subscription(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        result = await run_in_threadpool(
            _service(request).hdhive_subscription_check,
            target_type=str(request.query_params.get("target_type") or "").strip(),
            target_key=str(request.query_params.get("target_key") or "").strip(),
        )
    except Exception:
        return _html_error(InternalServerError())
    return _response(result)


@router.delete(
    "/hdhive/subscriptions/{subscription_id}",
    response_model=dict[str, Any],
    responses=_responses(200, 202, 400, 401, 403, 409, 500, 503),
)
async def delete_hdhive_subscription(request: Request, subscription_id: int) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        result = await run_in_threadpool(
            _service(request).delete_hdhive_subscription,
            subscription_id,
            principal_id=_principal_id(request),
            idempotency_key=_idempotency_key(request),
        )
    except Exception:
        return _html_error(InternalServerError())
    return _response(result)


@router.get(
    "/hdhive/resources",
    response_model=dict[str, Any],
    responses=_responses(200, 400, 401, 403, 500, 503),
)
async def hdhive_resources(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        result = await run_in_threadpool(
            _service(request).hdhive_resources,
            tmdb_id=request.query_params.get("tmdb_id"),
            media_type=request.query_params.get("media_type"),
            season=request.query_params.get("season"),
        )
    except Exception:
        return _html_error(InternalServerError())
    return _response(result)


@router.post(
    "/hdhive/download",
    response_model=dict[str, Any],
    responses=_responses(200, 202, 400, 401, 403, 409, 415, 500, 503),
    openapi_extra=_OBJECT_BODY,
)
async def hdhive_download(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    try:
        result = await run_in_threadpool(
            _service(request).hdhive_download,
            payload,
            principal_id=_principal_id(request),
            idempotency_key=_idempotency_key(request),
        )
    except Exception:
        return _html_error(InternalServerError())
    return _response(result)


@router.post(
    "/hdhive/checkin",
    response_model=dict[str, Any],
    responses=_responses(200, 202, 401, 403, 409, 415, 500, 503),
    openapi_extra=_OPTIONAL_OBJECT_BODY,
)
async def hdhive_checkin(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request, allow_empty=True)
    if error is not None:
        return error
    assert payload is not None
    try:
        result = await run_in_threadpool(
            _service(request).hdhive_checkin,
            payload,
            principal_id=_principal_id(request),
            idempotency_key=_idempotency_key(request),
        )
    except Exception:
        return _html_error(InternalServerError())
    return _response(result)


@router.get(
    "/tg_userbot/config",
    response_model=dict[str, Any],
    responses=_responses(200, 403, 500),
)
async def telegram_configuration(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        return _response(await run_in_threadpool(_service(request).telegram_configuration))
    except Exception:
        return _html_error(InternalServerError())


@router.post(
    "/tg_userbot/config",
    response_model=dict[str, Any],
    responses=_responses(200, 400, 403, 415, 500),
    openapi_extra=_OBJECT_BODY,
)
async def save_telegram_configuration(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    try:
        return _response(await run_in_threadpool(_service(request).save_telegram_configuration, payload))
    except Exception:
        return _html_error(InternalServerError())


@router.get(
    "/tg_userbot/status",
    response_model=dict[str, Any],
    responses=_responses(200, 403, 503),
)
async def telegram_status(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(
        await run_in_threadpool(
            _service(request).telegram_status,
            str(request.query_params.get("command_id") or ""),
        )
    )


@router.post(
    "/tg_userbot/send_code",
    response_model=dict[str, Any],
    responses=_responses(200, 403, 409, 503),
)
async def telegram_send_code(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(
        await run_in_threadpool(
            _service(request).telegram_send_code,
            idempotency_key=_idempotency_key(request),
        )
    )


@router.post(
    "/tg_userbot/login",
    response_model=dict[str, Any],
    responses=_responses(200, 400, 403, 409, 415, 503),
    openapi_extra=_OBJECT_BODY,
)
async def telegram_login(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    return _response(
        await run_in_threadpool(
            _service(request).telegram_login,
            payload.get("code"),
            idempotency_key=_idempotency_key(request),
        )
    )


@router.post(
    "/tg_userbot/logout",
    response_model=dict[str, Any],
    responses=_responses(200, 403, 409, 503),
)
async def telegram_logout(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(
        await run_in_threadpool(
            _service(request).telegram_logout,
            idempotency_key=_idempotency_key(request),
        )
    )


@router.post(
    "/tg_userbot/clear_session",
    response_model=dict[str, Any],
    responses=_responses(200, 403, 409, 500, 503),
)
async def telegram_clear_session(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(
        await run_in_threadpool(
            _service(request).telegram_clear_session,
            idempotency_key=_idempotency_key(request),
        )
    )


__all__ = ["router"]
