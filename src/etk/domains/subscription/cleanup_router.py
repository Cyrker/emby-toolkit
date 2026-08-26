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

from .cleanup import (
    CLEANUP_USE_CASE_PROVIDER,
    CleanupService,
    CleanupVersionNotFoundError,
)
from .cleanup_repository import CleanupExecutionConflictError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/cleanup", tags=["subscription"])


def _responses(*status_codes: int) -> dict[int | str, dict[str, Any]]:
    return {status_code: {"description": "Legacy-compatible cleanup response"} for status_code in status_codes}


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


_TASK_IDS_BODY = _request_body(
    {"task_ids": {"type": "array", "minItems": 1, "items": {}}},
    ("task_ids",),
)
_SETTINGS_BODY = _request_body(
    {
        "rules": {"type": "array", "items": {"type": "object"}},
        "library_ids": {"type": "array", "items": {}},
        "keep_one_per_res": {},
        "delete_delay": {},
        "manual_delete_without_confirm": {},
    },
    ("rules", "library_ids"),
)
_DELETE_VERSION_BODY = _request_body(
    {"emby_id": {"type": ["string", "integer"]}},
    ("emby_id",),
)


def _service(request: Request) -> CleanupService:
    service = request.app.state.container.use_case(CLEANUP_USE_CASE_PROVIDER)
    if not isinstance(service, CleanupService):
        raise TypeError("subscription.cleanup provider returned an invalid service")
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


def _busy(service: CleanupService) -> JSONResponse | None:
    if not service.busy():
        return None
    return fastapi_json_response({"error": "后台有任务正在运行，请稍后再试。"}, status_code=409)


def _unavailable(service: CleanupService) -> JSONResponse | None:
    if service.processor_ready():
        return None
    return fastapi_json_response({"error": "核心处理器未就绪。"}, status_code=503)


def _submission_context(request: Request) -> tuple[str | None, str | None]:
    session = get_request_session(request)
    user_id = str(session.get("emby_user_id") or "").strip() or None
    trace_id = str(getattr(request.state, "trace_id", "") or "").strip() or None
    return user_id, trace_id


def _claim_conflict(error: CleanupExecutionConflictError) -> JSONResponse:
    return fastapi_json_response(
        {"error": "清理任务已提交执行，不能同时忽略或删除。", "conflict_ids": list(error.conflict_ids)},
        status_code=409,
    )


@router.get("/tasks", responses=_responses(200, 403, 500))
def get_cleanup_tasks(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        return fastapi_json_response(_service(request).list_tasks())
    except Exception as exc:
        logger.exception("Unable to list cleanup tasks")
        return fastapi_json_response({"error": f"获取清理任务失败: {exc}"}, status_code=500)


@router.post(
    "/execute",
    status_code=202,
    responses=_responses(202, 400, 403, 409, 415, 500, 503),
    openapi_extra=_TASK_IDS_BODY,
)
async def execute_cleanup_tasks(request: Request) -> Response:
    service = _service(request)
    busy = _busy(service)
    if busy is not None:
        return busy
    unavailable = _unavailable(service)
    if unavailable is not None:
        return unavailable
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    data, body_error = await _legacy_json_object(request)
    if body_error is not None:
        return body_error
    assert data is not None
    task_ids = data.get("task_ids")
    if not task_ids or not isinstance(task_ids, list):
        return fastapi_json_response({"error": "缺少或无效的 task_ids 参数"}, status_code=400)
    try:
        user_id, trace_id = _submission_context(request)
        await run_in_threadpool(
            service.execute,
            task_ids,
            requested_by_user_id=user_id,
            trace_id=trace_id,
        )
    except CleanupExecutionConflictError as exc:
        return _claim_conflict(exc)
    except Exception:
        logger.exception("Unable to submit cleanup execution")
        return _html_error(InternalServerError())
    return fastapi_json_response({"message": "清理任务已提交到后台执行。"}, status_code=202)


@router.post(
    "/ignore",
    responses=_responses(200, 400, 403, 409, 415, 500),
    openapi_extra=_TASK_IDS_BODY,
)
async def ignore_cleanup_tasks(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    data, body_error = await _legacy_json_object(request)
    if body_error is not None:
        return body_error
    assert data is not None
    task_ids = data.get("task_ids")
    if not task_ids or not isinstance(task_ids, list):
        return fastapi_json_response({"error": "缺少或无效的 task_ids 参数"}, status_code=400)
    try:
        updated_count = await run_in_threadpool(_service(request).ignore, task_ids)
        return fastapi_json_response({"message": f"成功忽略 {updated_count} 个任务。"})
    except CleanupExecutionConflictError as exc:
        return _claim_conflict(exc)
    except Exception as exc:
        return fastapi_json_response({"error": f"忽略任务时失败: {exc}"}, status_code=500)


@router.post(
    "/delete",
    responses=_responses(200, 400, 403, 409, 415, 500),
    openapi_extra=_TASK_IDS_BODY,
)
async def delete_cleanup_tasks(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    data, body_error = await _legacy_json_object(request)
    if body_error is not None:
        return body_error
    assert data is not None
    task_ids = data.get("task_ids")
    if not task_ids or not isinstance(task_ids, list):
        return fastapi_json_response({"error": "缺少或无效的 task_ids 参数"}, status_code=400)
    try:
        deleted_count = await run_in_threadpool(_service(request).delete, task_ids)
        return fastapi_json_response({"message": f"成功删除 {deleted_count} 个任务。"})
    except CleanupExecutionConflictError as exc:
        return _claim_conflict(exc)
    except Exception as exc:
        return fastapi_json_response({"error": f"删除任务时失败: {exc}"}, status_code=500)


@router.post("/clear_all", responses=_responses(200, 202, 403, 409, 500, 503))
async def clear_all_cleanup_tasks(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    service = _service(request)
    busy = _busy(service)
    if busy is not None:
        return busy
    unavailable = _unavailable(service)
    if unavailable is not None:
        return unavailable
    try:
        user_id, trace_id = _submission_context(request)
        result = await run_in_threadpool(
            service.clear_all,
            requested_by_user_id=user_id,
            trace_id=trace_id,
        )
        if not result.submitted:
            return fastapi_json_response({"message": "没有发现待处理的清理任务。"})
        return fastapi_json_response({"message": "一键清理任务已提交到后台。"}, status_code=202)
    except CleanupExecutionConflictError as exc:
        return _claim_conflict(exc)
    except Exception as exc:
        logger.exception("Unable to submit all pending cleanup tasks")
        return fastapi_json_response({"error": f"一键清理失败: {exc}"}, status_code=500)


@router.get("/settings", responses=_responses(200, 403, 500))
def get_cleanup_settings(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        return fastapi_json_response(_service(request).get_settings())
    except Exception as exc:
        logger.exception("Unable to read cleanup settings")
        return fastapi_json_response({"error": f"获取清理设置失败: {exc}"}, status_code=500)


@router.post(
    "/settings",
    responses=_responses(200, 400, 403, 415, 500),
    openapi_extra=_SETTINGS_BODY,
)
async def save_cleanup_settings(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, body_error = await _legacy_json_value(request)
    if body_error is not None:
        return body_error
    if not isinstance(payload, Mapping):
        return fastapi_json_response({"error": "无效的数据格式，必须是一个对象。"}, status_code=400)
    data = dict(payload)
    rules = data.get("rules")
    library_ids = data.get("library_ids")
    delete_delay = data.get("delete_delay")
    if not isinstance(rules, list):
        return fastapi_json_response({"error": "无效的规则格式，'rules' 必须是一个列表。"}, status_code=400)
    if not isinstance(library_ids, list):
        return fastapi_json_response(
            {"error": "无效的媒体库格式，'library_ids' 必须是一个列表。"},
            status_code=400,
        )
    if delete_delay is not None and (not isinstance(delete_delay, int) or delete_delay < 0):
        return fastapi_json_response({"error": "删除延迟必须是非负整数。"}, status_code=400)
    try:
        await run_in_threadpool(_service(request).save_settings, data)
        return fastapi_json_response({"message": "清理设置已成功保存！"})
    except Exception as exc:
        return fastapi_json_response({"error": f"保存清理设置时失败: {exc}"}, status_code=500)


@router.post("/scan", status_code=202, responses=_responses(202, 403, 409, 500, 503))
async def trigger_cleanup_scan(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    service = _service(request)
    busy = _busy(service)
    if busy is not None:
        return busy
    unavailable = _unavailable(service)
    if unavailable is not None:
        return unavailable
    session = get_request_session(request)
    user_id = str(session.get("emby_user_id") or "").strip() or None
    trace_id = str(getattr(request.state, "trace_id", "") or "").strip() or None
    try:
        await run_in_threadpool(
            service.scan,
            requested_by_user_id=user_id,
            trace_id=trace_id,
        )
        return fastapi_json_response({"message": "扫描任务已提交到后台执行。"}, status_code=202)
    except Exception as exc:
        logger.exception("Unable to submit cleanup scan")
        return fastapi_json_response({"error": f"提交扫描任务失败: {exc}"}, status_code=500)


@router.post(
    "/delete_version",
    status_code=202,
    responses=_responses(202, 400, 403, 404, 409, 415, 500),
    openapi_extra=_DELETE_VERSION_BODY,
)
async def delete_single_version(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    data, body_error = await _legacy_json_object(request)
    if body_error is not None:
        return body_error
    assert data is not None
    emby_id = data.get("emby_id")
    if not emby_id:
        return fastapi_json_response({"error": "缺少 emby_id"}, status_code=400)
    try:
        user_id, trace_id = _submission_context(request)
        await run_in_threadpool(
            _service(request).delete_version,
            emby_id,
            requested_by_user_id=user_id,
            trace_id=trace_id,
        )
        return fastapi_json_response({"message": "删除任务已提交到后台。"}, status_code=202)
    except CleanupVersionNotFoundError:
        return fastapi_json_response({"error": "未找到对应的物理文件路径"}, status_code=404)
    except CleanupExecutionConflictError as exc:
        return _claim_conflict(exc)
    except Exception as exc:
        logger.exception("Unable to delete cleanup media version")
        return fastapi_json_response({"error": f"删除失败: {exc}"}, status_code=500)


__all__ = ["router"]
