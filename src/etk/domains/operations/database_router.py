from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Request
from starlette.responses import Response
from werkzeug.http import http_date

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.container import ProviderNotRegisteredError
from etk.platform.web import PermissionPolicy, get_request_session

from .database import (
    DatabaseReadService,
    DatabaseStatusResponse,
    DatabaseTableListResponse,
    ReviewItemsResponse,
)

router = APIRouter(prefix="/api", tags=["operations"])
logger = logging.getLogger(__name__)

_DATABASE_READ_PROVIDER = "operations.database.read"
_PERMISSION_POLICY_PROVIDER = "permission_policy"
_PERMISSION_READER_PROVIDERS = ("permissions", "identity")


def _container(request: Request) -> Any:
    return request.app.state.container


def _service(request: Request) -> DatabaseReadService:
    direct = getattr(request.app.state, "database_read_service", None)
    if isinstance(direct, DatabaseReadService):
        return direct
    service = _container(request).use_case(_DATABASE_READ_PROVIDER)
    if not isinstance(service, DatabaseReadService):
        raise TypeError("operations.database.read provider returned an invalid service")
    return service


def _permission_policy(request: Request) -> PermissionPolicy:
    direct = getattr(request.app.state, "permission_policy", None)
    if isinstance(direct, PermissionPolicy):
        return direct

    container = _container(request)
    try:
        policy = container.use_case(_PERMISSION_POLICY_PROVIDER)
    except ProviderNotRegisteredError:
        policy = None
    if policy is not None:
        if not isinstance(policy, PermissionPolicy):
            raise TypeError("permission_policy provider returned an invalid policy")
        return policy

    for provider_name in _PERMISSION_READER_PROVIDERS:
        try:
            reader = container.repository(provider_name)
        except ProviderNotRegisteredError:
            continue
        if not callable(getattr(reader, "is_admin", None)):
            raise TypeError(f"permission reader {provider_name!r} does not implement is_admin")
        return PermissionPolicy(reader, auth_enabled=bool(container.settings.auth_enabled))
    raise ProviderNotRegisteredError("no permission policy or permission reader is registered")


def _login_denial(request: Request) -> Response | None:
    decision = PermissionPolicy.require_any_login(get_request_session(request))
    if decision.allowed:
        return None
    return _json_response(
        {"status": "error", "message": decision.message},
        status_code=decision.status_code,
    )


def _admin_denial(request: Request) -> Response | None:
    decision = _permission_policy(request).require_admin(get_request_session(request))
    if decision.allowed:
        return None
    return _json_response(
        {"status": "error", "message": decision.message},
        status_code=decision.status_code,
    )


def _json_response(payload: Any, *, status_code: int = 200) -> Response:
    return fastapi_json_response(_legacy_jsonable(payload), status_code=status_code)


def _legacy_jsonable(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return http_date(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _legacy_jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_legacy_jsonable(item) for item in value]
    return value


def _legacy_int_query(request: Request, name: str, default: int) -> int:
    raw = request.query_params.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


@router.get("/database/stats/core", response_model=DatabaseStatusResponse)
def database_stats_core(request: Request) -> Response:
    denied = _login_denial(request)
    if denied is not None:
        return denied
    try:
        return _json_response({"status": "success", "data": _service(request).core_stats()})
    except Exception as exc:
        logger.error("获取核心统计失败: %s", exc)
        return _json_response({"status": "error", "data": {}}, status_code=500)


@router.get("/database/stats/library", response_model=DatabaseStatusResponse)
def database_stats_library(request: Request) -> Response:
    denied = _login_denial(request)
    if denied is not None:
        return denied
    try:
        return _json_response({"status": "success", "data": _service(request).library_stats()})
    except Exception as exc:
        logger.error("获取媒体库统计失败: %s", exc)
        return _json_response({"status": "error", "data": {}}, status_code=500)


@router.get("/database/stats/system", response_model=DatabaseStatusResponse)
def database_stats_system(request: Request) -> Response:
    denied = _login_denial(request)
    if denied is not None:
        return denied
    try:
        return _json_response({"status": "success", "data": _service(request).system_stats()})
    except Exception as exc:
        logger.error("获取系统统计失败: %s", exc)
        return _json_response({"status": "error", "data": {}}, status_code=500)


@router.get("/database/stats/rankings", response_model=DatabaseStatusResponse)
def database_stats_rankings(request: Request) -> Response:
    denied = _login_denial(request)
    if denied is not None:
        return denied
    try:
        return _json_response({"status": "success", "data": _service(request).rankings()})
    except Exception as exc:
        logger.error("获取排行统计失败: %s", exc)
        return _json_response({"status": "error", "data": {}}, status_code=500)


@router.get("/database/tables", response_model=DatabaseTableListResponse)
def database_tables(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        return _json_response(_service(request).table_names())
    except Exception as exc:
        logger.error("获取 PostgreSQL 表列表时出错: %s", exc, exc_info=True)
        return _json_response({"error": "无法获取数据库表列表"}, status_code=500)


@router.get("/review_items", response_model=ReviewItemsResponse)
def review_items(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied

    page = _legacy_int_query(request, "page", 1)
    per_page = _legacy_int_query(request, "per_page", 20)
    query = request.query_params.get("query", "").strip()
    reason = request.query_params.get("reason", "").strip()
    try:
        return _json_response(
            _service(request).review_items(
                page=page,
                per_page=per_page,
                query=query,
                reason=reason,
            )
        )
    except Exception:
        return _json_response(
            {"error": "获取待复核列表时发生服务器内部错误"},
            status_code=500,
        )


@router.get("/review_reasons", response_model=DatabaseStatusResponse)
def review_reasons(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        return _json_response({"status": "success", "data": _service(request).review_reasons()})
    except Exception:
        return _json_response({"error": "获取原因列表失败"}, status_code=500)
