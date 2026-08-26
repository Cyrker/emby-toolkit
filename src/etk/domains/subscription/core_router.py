from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from fastapi import APIRouter, Request
from starlette.concurrency import run_in_threadpool
from starlette.responses import JSONResponse, Response
from werkzeug.exceptions import BadRequest, InternalServerError, UnsupportedMediaType

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .core import SUBSCRIPTION_CORE_USE_CASE_PROVIDER, SubscriptionCoreService, SubscriptionHttpResult

router = APIRouter(prefix="/api", tags=["subscription"])


def _responses(*status_codes: int) -> dict[int | str, dict[str, Any]]:
    return {status_code: {"description": "Legacy-compatible subscription response"} for status_code in status_codes}


def _request_body(schema: Mapping[str, Any]) -> dict[str, Any]:
    return {"requestBody": {"required": True, "content": {"application/json": {"schema": dict(schema)}}}}


_STATUS_BODY = _request_body(
    {
        "type": "object",
        "properties": {"requests": {"type": "array", "items": {"type": "object"}}},
    }
)
_DELETE_BODY = _request_body(
    {
        "type": "object",
        "properties": {"items": {"type": "array", "items": {"type": "object"}}},
    }
)


def _service(request: Request) -> SubscriptionCoreService:
    service = request.app.state.container.use_case(SUBSCRIPTION_CORE_USE_CASE_PROVIDER)
    if not isinstance(service, SubscriptionCoreService):
        raise TypeError("subscription.core provider returned an invalid service")
    return service


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


def _response(result: SubscriptionHttpResult) -> JSONResponse:
    return fastapi_json_response(result.payload, status_code=result.status_code)


def _html_error(error: BadRequest | InternalServerError | UnsupportedMediaType) -> Response:
    headers = dict(error.get_headers())
    return Response(
        content=error.get_body(),
        status_code=int(error.code or 500),
        headers={"content-type": headers["Content-Type"]},
    )


def _is_json_content_type(value: str) -> bool:
    mimetype = value.partition(";")[0].strip().casefold()
    return mimetype == "application/json" or (mimetype.startswith("application/") and mimetype.endswith("+json"))


async def _legacy_json_object(request: Request) -> tuple[dict[str, Any] | None, Response | None]:
    if not _is_json_content_type(request.headers.get("content-type", "")):
        return None, _html_error(UnsupportedMediaType())
    raw = await request.body()
    if not raw:
        return None, _html_error(BadRequest())
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, _html_error(BadRequest())
    if not isinstance(value, Mapping):
        return None, _html_error(InternalServerError())
    return dict(value), None


@router.get(
    "/subscription/status",
    response_model=dict[str, Any],
    responses=_responses(200, 500),
)
async def subscription_status(request: Request) -> Response:
    try:
        result = await run_in_threadpool(_service(request).status)
    except Exception:
        return _html_error(InternalServerError())
    return _response(result)


@router.post(
    "/subscription/status",
    response_model=dict[str, Any],
    responses=_responses(200, 400, 403, 415, 500),
    openapi_extra=_STATUS_BODY,
)
async def update_subscription_status(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    data, error = await _legacy_json_object(request)
    if error is not None:
        return error
    assert data is not None
    return _response(await run_in_threadpool(_service(request).update_status, data.get("requests")))


@router.get(
    "/subscriptions/all",
    response_model=list[dict[str, Any]],
    responses=_responses(200, 403, 500),
)
async def subscriptions(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(await run_in_threadpool(_service(request).list_subscriptions))


@router.post(
    "/media/batch_delete",
    response_model=dict[str, Any],
    responses=_responses(200, 400, 403, 409, 415, 500),
    openapi_extra=_DELETE_BODY,
)
async def batch_delete(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    data, error = await _legacy_json_object(request)
    if error is not None:
        return error
    assert data is not None
    return _response(await run_in_threadpool(_service(request).batch_delete, data.get("items")))


__all__ = ["router"]
