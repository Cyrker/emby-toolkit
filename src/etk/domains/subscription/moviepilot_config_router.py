from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any

from fastapi import APIRouter, Request
from starlette.concurrency import run_in_threadpool
from starlette.responses import JSONResponse, Response
from werkzeug.exceptions import BadRequest, InternalServerError, UnsupportedMediaType

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .moviepilot_config import (
    MOVIEPILOT_CONFIG_USE_CASE_PROVIDER,
    SUBSCRIPTION_STRATEGY_USE_CASE_PROVIDER,
    MoviePilotConfigService,
    SubscriptionStrategyService,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/subscription", tags=["subscription"])


def _responses(*status_codes: int) -> dict[int | str, dict[str, Any]]:
    return {
        status_code: {"description": "Legacy-compatible subscription configuration response"}
        for status_code in status_codes
    }


_OBJECT_BODY = {
    "requestBody": {
        "required": True,
        "content": {"application/json": {"schema": {"type": "object", "additionalProperties": True}}},
    }
}


def _moviepilot_service(request: Request) -> MoviePilotConfigService:
    service = request.app.state.container.use_case(MOVIEPILOT_CONFIG_USE_CASE_PROVIDER)
    if not isinstance(service, MoviePilotConfigService):
        raise TypeError("subscription.moviepilot-config provider returned an invalid service")
    return service


def _strategy_service(request: Request) -> SubscriptionStrategyService:
    service = request.app.state.container.use_case(SUBSCRIPTION_STRATEGY_USE_CASE_PROVIDER)
    if not isinstance(service, SubscriptionStrategyService):
        raise TypeError("subscription.strategy provider returned an invalid service")
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


@router.get("/mp/config", responses=_responses(200, 403, 500))
async def get_moviepilot_config(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        config = await run_in_threadpool(_moviepilot_service(request).get)
    except Exception:
        logger.exception("failed to load MoviePilot configuration")
        return _html_error(InternalServerError())
    return fastapi_json_response({"success": True, "data": config})


@router.post(
    "/mp/config",
    responses=_responses(200, 400, 403, 415, 500),
    openapi_extra=_OBJECT_BODY,
)
async def save_moviepilot_config(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    value, error = await _legacy_json_value(request)
    if error is not None:
        return error
    if not isinstance(value, Mapping):
        return fastapi_json_response({"success": False, "message": "配置格式错误"}, status_code=400)
    try:
        await run_in_threadpool(_moviepilot_service(request).save, value)
    except Exception:
        logger.exception("failed to save MoviePilot configuration")
        return _html_error(InternalServerError())
    return fastapi_json_response({"success": True, "message": "MoviePilot 配置已保存生效"})


@router.get("/strategy", responses=_responses(200, 403, 500))
async def get_subscription_strategy(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        config = await run_in_threadpool(_strategy_service(request).get)
    except Exception:
        logger.exception("failed to load subscription strategy")
        return fastapi_json_response({"error": "获取配置失败"}, status_code=500)
    return fastapi_json_response(config)


@router.post(
    "/strategy",
    responses=_responses(200, 400, 403, 500),
    openapi_extra=_OBJECT_BODY,
)
async def save_subscription_strategy(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    value, error = await _legacy_json_value(request)
    if error is not None:
        return fastapi_json_response({"error": "保存配置失败"}, status_code=500)
    if not isinstance(value, Mapping):
        return fastapi_json_response({"error": "无效的配置格式"}, status_code=400)
    try:
        await run_in_threadpool(_strategy_service(request).save, value)
    except Exception:
        logger.exception("failed to save subscription strategy")
        return fastapi_json_response({"error": "保存配置失败"}, status_code=500)
    return fastapi_json_response({"message": "策略配置已保存"})


__all__ = ["router"]
