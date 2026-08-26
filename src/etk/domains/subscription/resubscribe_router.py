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

from .resubscribe import RESUBSCRIBE_USE_CASE_PROVIDER, ResubscribeResult, ResubscribeService

router = APIRouter(prefix="/api/resubscribe", tags=["subscription"])


def _responses(*status_codes: int) -> dict[int | str, dict[str, Any]]:
    return {status_code: {"description": "Legacy-compatible resubscribe response"} for status_code in status_codes}


def _request_body(schema: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": dict(schema)}},
        }
    }


_RULE_BODY = _request_body({"type": "object", "additionalProperties": True})
_ORDER_BODY = _request_body({"type": "array", "items": {}})
_BATCH_BODY = _request_body(
    {
        "type": "object",
        "properties": {
            "item_ids": {"oneOf": [{"type": "array", "items": {}}, {"type": "string"}]},
            "action": {"type": "string"},
        },
    }
)


def _service(request: Request) -> ResubscribeService:
    service = request.app.state.container.use_case(RESUBSCRIBE_USE_CASE_PROVIDER)
    if not isinstance(service, ResubscribeService):
        raise TypeError("subscription.resubscribe provider returned an invalid service")
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


def _response(result: ResubscribeResult) -> JSONResponse:
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


async def _legacy_json_value(request: Request) -> tuple[object | None, Response | None]:
    if not _is_json_content_type(request.headers.get("content-type", "")):
        return None, _html_error(UnsupportedMediaType())
    raw = await request.body()
    if not raw:
        return None, _html_error(BadRequest())
    try:
        return json.loads(raw), None
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, _html_error(BadRequest())


async def _legacy_json_object(request: Request) -> tuple[dict[str, Any] | None, Response | None]:
    value, error = await _legacy_json_value(request)
    if error is not None:
        return None, error
    if not isinstance(value, Mapping):
        return None, _html_error(InternalServerError())
    return dict(value), None


@router.get("/rules", responses=_responses(200, 403, 500))
async def list_rules(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(await run_in_threadpool(_service(request).list_rules))


@router.post(
    "/rules",
    status_code=201,
    responses=_responses(201, 400, 403, 409, 415, 500),
    openapi_extra=_RULE_BODY,
)
async def create_rule(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    data, error = await _legacy_json_object(request)
    if error is not None:
        return error
    assert data is not None
    return _response(await run_in_threadpool(_service(request).create_rule, data))


@router.put(
    "/rules/{rule_id:int}",
    responses=_responses(200, 400, 403, 404, 415, 500),
    openapi_extra=_RULE_BODY,
)
async def update_rule(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    data, error = await _legacy_json_object(request)
    if error is not None:
        return error
    assert data is not None
    rule_id = int(request.path_params["rule_id"])
    return _response(await run_in_threadpool(_service(request).update_rule, rule_id, data))


@router.delete("/rules/{rule_id:int}", responses=_responses(200, 403, 404, 500))
async def delete_rule(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    rule_id = int(request.path_params["rule_id"])
    return _response(await run_in_threadpool(_service(request).delete_rule, rule_id))


@router.post(
    "/rules/order",
    responses=_responses(200, 400, 403, 415, 500),
    openapi_extra=_ORDER_BODY,
)
async def order_rules(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    data, error = await _legacy_json_value(request)
    if error is not None:
        return error
    if not isinstance(data, list):
        return fastapi_json_response({"error": "请求体必须是一个ID数组"}, status_code=400)
    return _response(await run_in_threadpool(_service(request).order_rules, data))


@router.get("/library_status", responses=_responses(200, 403, 500))
async def list_library_status(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(await run_in_threadpool(_service(request).list_library_status))


@router.post("/refresh_status", status_code=202, responses=_responses(202, 403, 409, 500))
async def refresh_status(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(await run_in_threadpool(_service(request).refresh_status))


@router.post("/resubscribe_all", status_code=202, responses=_responses(202, 403, 409, 500))
async def resubscribe_all(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(await run_in_threadpool(_service(request).resubscribe_all))


@router.get("/libraries", responses=_responses(200, 403, 500, 503))
async def libraries(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(await run_in_threadpool(_service(request).libraries))


@router.post(
    "/batch_action",
    responses=_responses(200, 202, 400, 403, 409, 415, 500),
    openapi_extra=_BATCH_BODY,
)
async def batch_action(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    data, error = await _legacy_json_object(request)
    if error is not None:
        return error
    assert data is not None
    return _response(
        await run_in_threadpool(
            _service(request).batch_action,
            data.get("item_ids"),
            data.get("action"),
        )
    )


__all__ = ["router"]
