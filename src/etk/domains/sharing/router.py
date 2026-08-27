from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Request
from starlette.concurrency import run_in_threadpool
from starlette.responses import Response
from werkzeug.exceptions import BadRequest, UnsupportedMediaType

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .service import SharingHttpResult, SharingService

router = APIRouter(prefix="/api/shared/resources", tags=["sharing"])

_JSON_OBJECT_REQUEST_BODY = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {"type": "object", "additionalProperties": True},
            }
        },
    }
}
_DEVICE_REQUEST_BODY = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "required": ["server_id_hash"],
                    "properties": {
                        "name": {"type": "string"},
                        "server_id_hash": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
            }
        },
    }
}


def _service(request: Request) -> SharingService:
    direct = getattr(request.app.state, "sharing_service", None)
    if isinstance(direct, SharingService):
        return direct
    value = request.app.state.container.use_case("sharing")
    if not isinstance(value, SharingService):
        raise TypeError("sharing provider returned an invalid service")
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


def _response(result: SharingHttpResult) -> Response:
    return fastapi_json_response(result.payload, status_code=result.status_code, headers=dict(result.headers))


def _account_id(request: Request) -> str:
    query_value = str(request.query_params.get("account_id") or "").strip()
    if query_value:
        return query_value
    return str(get_request_session(request).get("emby_user_id") or "local").strip() or "local"


def _int_query(request: Request, key: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(request.query_params.get(key, default))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(value, maximum))


async def _json_object(request: Request) -> tuple[dict[str, Any] | None, Response | None]:
    content_type = request.headers.get("content-type", "").partition(";")[0].strip().casefold()
    if content_type != "application/json" and not (
        content_type.startswith("application/") and content_type.endswith("+json")
    ):
        return None, _html_error(UnsupportedMediaType())
    try:
        value = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, _html_error(BadRequest())
    if not isinstance(value, Mapping):
        return None, _html_error(BadRequest())
    return dict(value), None


def _html_error(error: BadRequest | UnsupportedMediaType) -> Response:
    headers = dict(error.get_headers())
    return Response(
        content=error.get_body(),
        status_code=int(error.code or 500),
        headers={"content-type": headers["Content-Type"]},
    )


@router.get(
    "/credit/balance",
    response_model=dict[str, Any],
    responses={403: {"description": "admin required"}, 503: {"description": "provider unavailable"}},
)
def credit_balance(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).credit_balance(_account_id(request)))


@router.get(
    "/credit/ledger",
    response_model=dict[str, Any],
    responses={403: {"description": "admin required"}, 503: {"description": "provider unavailable"}},
)
def credit_ledger(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    limit = _int_query(request, "limit", 100, 1, 1000)
    return _response(_service(request).credit_ledger(_account_id(request), limit=limit))


@router.get(
    "/center/sources",
    response_model=dict[str, Any],
    responses={403: {"description": "admin required"}, 503: {"description": "provider unavailable"}},
)
def list_center_sources(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    service = _service(request)
    query: dict[str, Any] = {
        key: request.query_params.get(key, default)
        for key, default in (
            ("q", ""),
            ("status", "alive,available,updating,inconsistent,incomplete"),
            ("source_kind", ""),
            ("item_type", ""),
            ("tmdb_id", ""),
            ("source_id", ""),
        )
    }
    query["limit"] = _int_query(request, "limit", 200, 1, 1000)
    query["offset"] = _int_query(request, "offset", 0, 0, 10_000_000)
    return _response(service.list_sources(**query))


@router.post(
    "/center/device/register",
    response_model=dict[str, Any],
    responses={
        400: {"description": "invalid request"},
        403: {"description": "admin required"},
        415: {"description": "JSON required"},
        503: {"description": "provider unavailable"},
    },
    openapi_extra=_DEVICE_REQUEST_BODY,
)
async def register_center_device(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    name = str(payload.get("name") or "ETK Device").strip() or "ETK Device"
    server_id_hash = str(payload.get("server_id_hash") or "").strip()
    if not server_id_hash:
        return fastapi_json_response({"success": False, "message": "server_id_hash is required"}, status_code=400)
    return _response(
        await run_in_threadpool(
            _service(request).register_device,
            name=name,
            server_id_hash=server_id_hash,
        )
    )


@router.get(
    "/center/device/status",
    response_model=dict[str, Any],
    responses={403: {"description": "admin required"}, 503: {"description": "provider unavailable"}},
)
def center_device_status(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).device_status())


@router.post(
    "/shares/manual-create",
    response_model=dict[str, Any],
    responses={
        201: {"description": "source registered"},
        400: {"description": "invalid request"},
        403: {"description": "admin required"},
        415: {"description": "JSON required"},
        503: {"description": "provider unavailable"},
    },
    openapi_extra=_JSON_OBJECT_REQUEST_BODY,
)
async def register_source(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    kind = str(payload.pop("source_kind", payload.pop("kind", "movie")) or "movie").strip().lower()
    try:
        result = await run_in_threadpool(
            _service(request).register_source,
            kind,
            payload,
            idempotency_key=request.headers.get("idempotency-key"),
        )
    except (TypeError, ValueError):
        return fastapi_json_response({"success": False, "message": "共享源请求无效"}, status_code=400)
    return _response(result)


@router.post(
    "/transfers/lease",
    response_model=dict[str, Any],
    responses={
        202: {"description": "transfer queued"},
        400: {"description": "invalid request"},
        403: {"description": "admin required"},
        415: {"description": "JSON required"},
        503: {"description": "provider unavailable"},
    },
    openapi_extra=_JSON_OBJECT_REQUEST_BODY,
)
async def acquire_transfer_lease(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    try:
        result = await run_in_threadpool(_service(request).acquire_transfer_lease, payload)
    except (TypeError, ValueError):
        return fastapi_json_response({"success": False, "message": "转存租约请求无效"}, status_code=400)
    return _response(result)


@router.post(
    "/transfers/report",
    response_model=dict[str, Any],
    responses={
        400: {"description": "invalid request"},
        403: {"description": "admin required"},
        415: {"description": "JSON required"},
        503: {"description": "provider unavailable"},
    },
    openapi_extra=_JSON_OBJECT_REQUEST_BODY,
)
async def report_transfer(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    try:
        result = await run_in_threadpool(_service(request).report_transfer, payload)
    except (TypeError, ValueError):
        return fastapi_json_response({"success": False, "message": "转存报告请求无效"}, status_code=400)
    return _response(result)


@router.post(
    "/share-requests/quote",
    response_model=dict[str, Any],
    responses={
        400: {"description": "invalid request"},
        403: {"description": "admin required"},
        415: {"description": "JSON required"},
        503: {"description": "provider unavailable"},
    },
    openapi_extra=_JSON_OBJECT_REQUEST_BODY,
)
async def quote_share_request(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    try:
        result = await run_in_threadpool(_service(request).quote_share_request, payload)
    except (TypeError, ValueError):
        return fastapi_json_response({"success": False, "message": "求共享报价请求无效"}, status_code=400)
    return _response(result)


async def _share_requests(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    service = _service(request)
    if request.method == "GET":
        query: dict[str, Any] = {
            key: request.query_params.get(key, default)
            for key, default in (
                ("keyword", ""),
                ("status", "open"),
                ("media_type", ""),
                ("target_type", ""),
            )
        }
        query["limit"] = _int_query(request, "limit", 100, 1, 500)
        query["offset"] = _int_query(request, "offset", 0, 0, 10_000_000)
        return _response(await run_in_threadpool(service.list_share_requests, **query))
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    try:
        result = await run_in_threadpool(service.create_share_request, payload)
    except (TypeError, ValueError):
        return fastapi_json_response({"success": False, "message": "求共享请求无效"}, status_code=400)
    return _response(result)


@router.get(
    "/share-requests",
    response_model=dict[str, Any],
    name="list_share_requests",
    responses={403: {"description": "admin required"}, 503: {"description": "provider unavailable"}},
)
async def list_share_requests(request: Request) -> Response:
    return await _share_requests(request)


@router.post(
    "/share-requests",
    response_model=dict[str, Any],
    name="create_share_request",
    responses={
        202: {"description": "request queued"},
        400: {"description": "invalid request"},
        403: {"description": "admin required"},
        415: {"description": "JSON required"},
        503: {"description": "provider unavailable"},
    },
    openapi_extra=_JSON_OBJECT_REQUEST_BODY,
)
async def create_share_request(request: Request) -> Response:
    return await _share_requests(request)


@router.get(
    "/operations/{operation_id}/status",
    response_model=dict[str, Any],
    responses={
        403: {"description": "admin required"},
        404: {"description": "not found"},
        503: {"description": "unavailable"},
    },
)
def sharing_operation_status(request: Request, operation_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        UUID(operation_id)
    except (TypeError, ValueError, AttributeError):
        return fastapi_json_response({"success": False, "message": "共享操作不存在"}, status_code=404)
    return _response(_service(request).operation_status(operation_id))


__all__ = ["router"]
