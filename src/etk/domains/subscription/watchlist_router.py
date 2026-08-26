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

from .watchlist import (
    BATCH_WATCHLIST_STATUSES,
    WATCHLIST_STATUSES,
    WATCHLIST_USE_CASE_PROVIDER,
    WatchlistRefreshUnavailable,
    WatchlistService,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/watchlist", tags=["subscription"])


def _request_body(properties: Mapping[str, Any], required: tuple[str, ...] = ()) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "object", "properties": dict(properties)}
    if required:
        schema["required"] = list(required)
    return {
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": schema}},
        }
    }


def _path_parameter(name: str) -> dict[str, Any]:
    return {
        "parameters": [
            {
                "name": name,
                "in": "path",
                "required": True,
                "schema": {"type": "string"},
            }
        ]
    }


def _responses(*status_codes: int) -> dict[int | str, dict[str, Any]]:
    return {status_code: {"description": "Legacy-compatible JSON response"} for status_code in status_codes}


_ADD_BODY = _request_body(
    {
        "item_id": {"type": ["string", "integer"]},
        "tmdb_id": {"type": ["string", "integer"]},
        "item_name": {"type": "string"},
        "item_type": {"type": "string"},
    },
    ("item_id", "tmdb_id", "item_name", "item_type"),
)
_STATUS_BODY = _request_body(
    {
        "item_id": {"type": ["string", "integer"]},
        "new_status": {"type": "string", "enum": sorted(WATCHLIST_STATUSES)},
    },
    ("item_id", "new_status"),
)
_ITEM_IDS_BODY = _request_body(
    {"item_ids": {"type": "array", "minItems": 1, "items": {}}},
    ("item_ids",),
)
_BATCH_STATUS_BODY = _request_body(
    {
        "item_ids": {"type": "array", "minItems": 1, "items": {}},
        "new_status": {"type": "string", "enum": sorted(BATCH_WATCHLIST_STATUSES)},
    },
    ("item_ids", "new_status"),
)
_TOTAL_EPISODES_BODY = _request_body(
    {
        "tmdb_id": {"type": ["string", "integer"]},
        "total_episodes": {},
        "item_type": {"type": "string", "default": "Season"},
    },
    ("tmdb_id", "total_episodes"),
)
_SETTINGS_BODY = _request_body(
    {
        "douban_count_correction": {},
        "revival_check_days": {},
        "tg_channel_tracking": {},
        "intro_detection_trigger_mode": {
            "type": "string",
            "enum": ["off", "import", "favorite", "playback"],
        },
        "intro_detection_credits_enabled": {},
        "intro_detection_library_ids": {"type": "array", "items": {}},
    }
)


def _service(request: Request) -> WatchlistService:
    service = request.app.state.container.use_case(WATCHLIST_USE_CASE_PROVIDER)
    if not isinstance(service, WatchlistService):
        raise TypeError("subscription.watchlist provider returned an invalid service")
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


async def _legacy_json_object(request: Request) -> tuple[dict[str, Any] | None, Response | None]:
    payload, error = await _legacy_json_value(request)
    if error is not None:
        return None, error
    if not isinstance(payload, Mapping):
        return None, _html_error(InternalServerError())
    return dict(payload), None


@router.get("", response_model=list[dict[str, Any]], responses=_responses(403, 500))
def get_watchlist(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        return fastapi_json_response(_service(request).list_items())
    except Exception:
        logger.exception("Unable to read watchlist")
        return fastapi_json_response({"error": "获取追剧列表时发生服务器内部错误"}, status_code=500)


@router.post(
    "/add",
    response_model=dict[str, Any],
    responses=_responses(400, 403, 415, 500),
    openapi_extra=_ADD_BODY,
)
async def add_to_watchlist(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    data, body_error = await _legacy_json_object(request)
    if body_error is not None:
        return body_error
    assert data is not None
    item_id = data.get("item_id")
    tmdb_id = data.get("tmdb_id")
    item_name = data.get("item_name")
    item_type = data.get("item_type")
    if not all([item_id, tmdb_id, item_name, item_type]):
        return fastapi_json_response({"error": "缺少必要的项目信息"}, status_code=400)
    if item_type != "Series":
        return fastapi_json_response({"error": "只能将'剧集'类型添加到追剧列表"}, status_code=400)
    try:
        await run_in_threadpool(_service(request).add, tmdb_id, item_name)
        return fastapi_json_response({"message": f"《{item_name}》已成功添加到追剧列表！"})
    except Exception:
        logger.exception("Unable to add watchlist item")
        return fastapi_json_response({"error": "服务器在添加时发生内部错误"}, status_code=500)


@router.post(
    "/update_status",
    response_model=dict[str, Any],
    responses=_responses(400, 403, 404, 415, 500),
    openapi_extra=_STATUS_BODY,
)
async def update_watchlist_status(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    data, body_error = await _legacy_json_object(request)
    if body_error is not None:
        return body_error
    assert data is not None
    item_id = data.get("item_id")
    new_status = data.get("new_status")
    if not item_id or new_status not in WATCHLIST_STATUSES:
        return fastapi_json_response({"error": "请求参数无效"}, status_code=400)
    try:
        updated = await run_in_threadpool(_service(request).update_status, item_id, str(new_status))
        if updated:
            return fastapi_json_response({"message": "状态更新成功"})
        return fastapi_json_response({"error": "未在追剧列表中找到该项目"}, status_code=404)
    except Exception:
        logger.exception("Unable to update watchlist status")
        return fastapi_json_response({"error": "服务器在更新状态时发生内部错误"}, status_code=500)


@router.post(
    "/remove/{item_id}",
    response_model=dict[str, Any],
    responses=_responses(403, 404, 500),
    openapi_extra=_path_parameter("item_id"),
)
async def remove_from_watchlist(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    item_id = str(request.path_params.get("item_id") or "")
    try:
        result = await run_in_threadpool(_service(request).remove, item_id)
        if not result.cleanup.get("success"):
            return fastapi_json_response(
                {
                    "error": result.cleanup.get("error") or "删除外部资源失败",
                    "details": dict(result.cleanup),
                },
                status_code=_cleanup_status(result.cleanup),
            )
        if result.removed:
            return fastapi_json_response(
                {
                    "message": "已从追剧列表移除，并已同步取消 MP 订阅、删除 Emby 媒体项",
                    "cleanup": dict(result.cleanup),
                }
            )
        return fastapi_json_response({"error": "未在追剧列表中找到该项目"}, status_code=404)
    except Exception:
        logger.exception("Unable to remove watchlist item")
        return fastapi_json_response({"error": "移除项目时发生未知的服务器内部错误"}, status_code=500)


@router.post(
    "/refresh/{item_id}",
    response_model=dict[str, Any],
    status_code=202,
    responses=_responses(403, 500, 503),
    openapi_extra=_path_parameter("item_id"),
)
async def refresh_watchlist_item(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    item_id = str(request.path_params.get("item_id") or "")
    try:
        item_name = await run_in_threadpool(_service(request).refresh, item_id)
        return fastapi_json_response(
            {"message": f"《{item_name}》的刷新任务已在后台启动！"},
            status_code=202,
        )
    except WatchlistRefreshUnavailable:
        return fastapi_json_response({"error": "追剧处理模块未就绪"}, status_code=503)
    except Exception:
        logger.exception("Unable to submit watchlist refresh")
        return _html_error(InternalServerError())


@router.post(
    "/batch_force_end",
    response_model=dict[str, Any],
    responses=_responses(400, 403, 415, 500),
    openapi_extra=_ITEM_IDS_BODY,
)
async def batch_force_end(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    data, body_error = await _legacy_json_object(request)
    if body_error is not None:
        return body_error
    assert data is not None
    item_ids = data.get("item_ids")
    if not isinstance(item_ids, list) or not item_ids:
        return fastapi_json_response(
            {"error": "请求参数无效：必须提供一个包含项目ID的列表 (item_ids)。"},
            status_code=400,
        )
    try:
        updated_count = await run_in_threadpool(_service(request).force_end, item_ids)
        return fastapi_json_response(
            {
                "message": (
                    f"操作成功！已将 {updated_count} 个季标记为强制完结。"
                    "它们不会因集数更新而复活，也不会影响同剧其它季。"
                ),
                "updated_count": updated_count,
            }
        )
    except Exception:
        logger.exception("Unable to force-end watchlist seasons")
        return fastapi_json_response(
            {"error": "批量强制完结项目时发生未知的服务器内部错误"},
            status_code=500,
        )


@router.post(
    "/batch_update_status",
    response_model=dict[str, Any],
    responses=_responses(400, 403, 415, 500),
    openapi_extra=_BATCH_STATUS_BODY,
)
async def batch_update_status(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    data, body_error = await _legacy_json_object(request)
    if body_error is not None:
        return body_error
    assert data is not None
    item_ids = data.get("item_ids")
    new_status = data.get("new_status")
    if not isinstance(item_ids, list) or not item_ids:
        return fastapi_json_response(
            {"error": "请求参数无效：必须提供一个包含项目ID的列表 (item_ids)。"},
            status_code=400,
        )
    if new_status not in BATCH_WATCHLIST_STATUSES:
        return fastapi_json_response({"error": f"无效的状态值: {new_status}"}, status_code=400)
    try:
        updated_count = await run_in_threadpool(
            _service(request).batch_update_status,
            item_ids,
            str(new_status),
        )
        return fastapi_json_response(
            {
                "message": f"操作成功！已将 {updated_count} 个项目的状态更新为 '{new_status}'。",
                "updated_count": updated_count,
            }
        )
    except Exception:
        logger.exception("Unable to batch-update watchlist status")
        return fastapi_json_response(
            {"error": "批量更新项目状态时发生未知的服务器内部错误"},
            status_code=500,
        )


@router.post(
    "/batch_remove",
    response_model=dict[str, Any],
    responses=_responses(207, 400, 403, 415, 500),
    openapi_extra=_ITEM_IDS_BODY,
)
async def batch_remove(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    data, body_error = await _legacy_json_object(request)
    if body_error is not None:
        return body_error
    assert data is not None
    item_ids = data.get("item_ids")
    if not isinstance(item_ids, list) or not item_ids:
        return fastapi_json_response(
            {"error": "请求参数无效：必须提供一个包含项目ID的列表 (item_ids)。"},
            status_code=400,
        )
    try:
        result = await run_in_threadpool(_service(request).batch_remove, item_ids)
        return fastapi_json_response(
            {
                "message": f"已移除 {result.removed_count} 个追剧项目；同步删除失败 {len(result.failures)} 个。",
                "removed_count": result.removed_count,
                "cleanup": [dict(value) for value in result.cleanups],
                "failures": [dict(value) for value in result.failures],
            },
            status_code=result.status_code,
        )
    except Exception:
        logger.exception("Unable to batch-remove watchlist items")
        return fastapi_json_response(
            {"error": "批量移除项目时发生未知的服务器内部错误"},
            status_code=500,
        )


@router.post(
    "/update_total_episodes",
    response_model=dict[str, Any],
    responses=_responses(400, 403, 415, 500),
    openapi_extra=_TOTAL_EPISODES_BODY,
)
async def update_total_episodes(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    data, body_error = await _legacy_json_object(request)
    if body_error is not None:
        return body_error
    assert data is not None
    tmdb_id = data.get("tmdb_id")
    total_episodes = data.get("total_episodes")
    item_type = data.get("item_type", "Season")
    if not tmdb_id or total_episodes is None:
        return fastapi_json_response({"error": "参数无效"}, status_code=400)
    try:
        await run_in_threadpool(
            _service(request).update_total_episodes,
            tmdb_id,
            total_episodes,
            item_type,
        )
        return fastapi_json_response({"message": f"已修正总集数为 {total_episodes} 并锁定。"})
    except Exception:
        logger.exception("Unable to update watchlist total episodes")
        return fastapi_json_response({"error": "数据库更新失败"}, status_code=500)


@router.get("/settings", response_model=dict[str, Any], responses=_responses(403, 500))
def get_watchlist_settings(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        return fastapi_json_response(_service(request).get_settings())
    except Exception:
        logger.exception("Unable to read watchlist settings")
        return fastapi_json_response({"error": "获取配置失败"}, status_code=500)


@router.post(
    "/settings",
    response_model=dict[str, Any],
    responses=_responses(400, 403, 500),
    openapi_extra=_SETTINGS_BODY,
)
async def save_watchlist_settings(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, body_error = await _legacy_json_value(request)
    if body_error is not None:
        return fastapi_json_response({"error": "保存配置失败"}, status_code=500)
    if not isinstance(payload, Mapping):
        return fastapi_json_response({"error": "配置格式错误"}, status_code=400)
    try:
        await run_in_threadpool(_service(request).save_settings, dict(payload))
        return fastapi_json_response({"message": "追剧策略配置已保存"})
    except Exception:
        logger.exception("Unable to save watchlist settings")
        return fastapi_json_response({"error": "保存配置失败"}, status_code=500)


def _cleanup_status(cleanup: Mapping[str, Any]) -> int:
    try:
        status_code = int(cleanup.get("status") or 500)
    except (TypeError, ValueError):
        return 500
    return status_code if 400 <= status_code <= 599 else 500


__all__ = ["router"]
