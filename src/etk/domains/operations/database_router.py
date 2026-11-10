from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from email import policy
from email.parser import BytesParser
from typing import Any, cast
from uuid import UUID

from fastapi import APIRouter, Query, Request
from starlette.concurrency import run_in_threadpool
from starlette.responses import Response
from werkzeug.exceptions import BadRequest, UnsupportedMediaType
from werkzeug.http import http_date

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.container import ProviderNotRegisteredError
from etk.platform.web import PermissionPolicy, get_request_session

from .database import (
    DatabaseReadService,
    DatabaseStatusResponse,
    DatabaseTableListResponse,
    ReviewItemsResponse,
    ReviewTaskBusyError,
)
from .database_maintenance import (
    DatabaseMaintenanceBusyError,
    DatabaseMaintenanceConflictError,
    DatabaseMaintenanceHttpResult,
    DatabaseMaintenanceOperationKind,
    DatabaseMaintenanceService,
    DatabaseMaintenanceSizeLimitError,
    DatabaseMaintenanceValidationError,
)
from .review_reprocess import ReviewReprocessHttpResult, ReviewReprocessService

router = APIRouter(prefix="/api", tags=["operations"])
effects_router = APIRouter(prefix="/api", tags=["operations"])
maintenance_router = APIRouter(prefix="/api", tags=["operations"])
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


def _review_reprocess_service(request: Request) -> ReviewReprocessService:
    direct = getattr(request.app.state, "review_reprocess_service", None)
    if isinstance(direct, ReviewReprocessService):
        return direct
    service = _container(request).use_case("operations.review-reprocess")
    if not isinstance(service, ReviewReprocessService):
        raise TypeError("operations.review-reprocess provider returned an invalid service")
    return service


def _maintenance_service(request: Request) -> DatabaseMaintenanceService:
    direct = getattr(request.app.state, "database_maintenance_service", None)
    if isinstance(direct, DatabaseMaintenanceService):
        return direct
    service = _container(request).use_case("operations.database-maintenance")
    if not isinstance(service, DatabaseMaintenanceService):
        raise TypeError("operations.database-maintenance provider returned an invalid service")
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
    container = request.app.state.container
    decision = PermissionPolicy(
        container.repository("identity"),
        auth_enabled=bool(container.settings.auth_enabled),
    ).require_any_login(get_request_session(request))
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


def _review_reprocess_response(result: ReviewReprocessHttpResult) -> Response:
    response = _json_response(result.payload, status_code=result.status_code)
    if result.location:
        response.headers["Location"] = result.location
    return response


def _processor_ready(request: Request) -> bool:
    direct = getattr(request.app.state, "processor_ready", None)
    if isinstance(direct, bool):
        return direct
    if callable(direct):
        return bool(direct())
    return bool(_container(request).client("operations.job-readiness").media_ready())


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


async def _backup_upload(request: Request) -> tuple[bytes | None, str, Response | None]:
    content_type = request.headers.get("content-type", "")
    if content_type.partition(";")[0].strip().casefold() != "multipart/form-data":
        return None, "backup.json.gz", _html_error(UnsupportedMediaType())
    body = await request.body()
    # A small allowance covers multipart headers/boundaries while keeping the
    # compressed artifact limit enforced by inspect_backup_bytes.
    if len(body) > 65 * 1024 * 1024:
        return (
            None,
            "backup.json.gz",
            _json_response({"status": "error", "message": "压缩备份不能超过 64 MiB"}, status_code=400),
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
            "backup.json.gz",
            _json_response({"status": "error", "message": "上传表单格式无效"}, status_code=400),
        )
    for part in message.iter_parts():
        name = str(part.get_param("name", header="content-disposition") or "")
        filename = str(part.get_filename() or "").strip()
        if name not in {"file", "backup"} or not filename:
            continue
        payload = part.get_payload(decode=True)
        if isinstance(payload, bytes | bytearray):
            return bytes(payload), filename, None
    return None, "backup.json.gz", _json_response({"status": "error", "message": "未找到文件"}, status_code=400)


def _maintenance_response(result: DatabaseMaintenanceHttpResult) -> Response:
    response = _json_response(result.payload, status_code=result.status_code)
    if result.status_code == 202 and isinstance(result.payload, Mapping):
        status_url = str(result.payload.get("status_url") or "").strip()
        if status_url:
            response.headers["Location"] = status_url
            response.headers["Retry-After"] = "2"
    return response


def _maintenance_error(exc: Exception) -> Response:
    if isinstance(exc, DatabaseMaintenanceBusyError | DatabaseMaintenanceConflictError):
        return _json_response({"status": "error", "message": str(exc)}, status_code=409)
    if isinstance(exc, DatabaseMaintenanceSizeLimitError | DatabaseMaintenanceValidationError):
        return _json_response({"status": "error", "message": str(exc)}, status_code=400)
    logger.exception("database maintenance request failed")
    return _json_response({"status": "error", "message": "服务器在处理数据库维护请求时发生内部错误"}, status_code=500)


def _html_error(error: BadRequest | UnsupportedMediaType) -> Response:
    headers = dict(error.get_headers())
    return Response(
        content=error.get_body(),
        status_code=int(error.code or 500),
        headers={"content-type": headers["Content-Type"]},
    )


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


@router.get("/database/stats/subscription", response_model=DatabaseStatusResponse)
def database_stats_subscription(request: Request) -> Response:
    denied = _login_denial(request)
    if denied is not None:
        return denied
    try:
        return _json_response({"status": "success", "data": _service(request).subscription_stats()})
    except Exception as exc:
        logger.error("获取订阅统计失败: %s", exc)
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


@router.post("/actions/mark_item_processed/{item_id}")
def mark_review_item_processed(item_id: str, request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        deleted = _service(request).mark_review_item_processed(item_id)
    except ReviewTaskBusyError:
        return _json_response({"error": "后台有任务正在运行，请稍后再试。"}, status_code=409)
    except Exception:
        return _json_response({"error": "服务器内部错误"}, status_code=500)
    if not deleted:
        return _json_response({"error": f"未在待复核列表中找到项目 {item_id}。"}, status_code=404)
    return _json_response({"message": f"项目 {item_id} 已成功从待复核列表移除。"})


@router.post("/actions/clear_review_items")
def clear_review_items(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        count = _service(request).clear_review_items()
    except Exception:
        logger.exception("API调用 clear_review_items 时发生错误")
        return _json_response({"error": "服务器在处理时发生内部错误"}, status_code=500)
    if count > 0:
        message = f"操作成功！已从待复核列表移除 {count} 个项目。"
    else:
        message = "操作完成，待复核列表本就是空的。"
    return _json_response({"message": message})


@effects_router.get("/actions/reprocess_item/{item_id}/identification", response_model=dict[str, Any])
def reprocess_identification(
    item_id: str, request: Request, job_id: str | None = Query(None, max_length=36),
    query: str | None = Query(None, max_length=200), year: int | None = Query(None, ge=1800, le=2200),
    page: int = Query(1, ge=1, le=500), manual_id: str | None = Query(None, max_length=12),
    context_only: bool = False,
) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _review_reprocess_response(_review_reprocess_service(request).identification_preview(
        item_id, job_id=job_id, query=query, year=year, page=page, manual_id=manual_id, context_only=context_only,
    ))


@effects_router.post("/actions/reprocess_item/{item_id}")
async def reprocess_review_item(item_id: str, request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    if not _processor_ready(request):
        return _json_response({"error": "核心处理器未就绪。"}, status_code=503)
    payload, error = await _json_value(request)
    if error is not None:
        return error
    session = get_request_session(request)
    return _review_reprocess_response(
        await run_in_threadpool(
            _review_reprocess_service(request).submit_one,
            item_id,
            payload,
            requested_by_user_id=str(session.get("emby_user_id") or "").strip() or None,
            trace_id=str(getattr(request.state, "trace_id", "") or "").strip() or None,
            idempotency_key=str(request.headers.get("idempotency-key") or "").strip() or None,
        )
    )


@effects_router.post("/actions/reprocess_all_review_items")
async def reprocess_all_review_items(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    if not _processor_ready(request):
        return _json_response({"error": "核心处理器未就绪。"}, status_code=503)
    payload, error = await _json_value(request)
    if error is not None:
        return error
    session = get_request_session(request)
    return _review_reprocess_response(
        _review_reprocess_service(request).submit_all(
            payload,
            requested_by_user_id=str(session.get("emby_user_id") or "").strip() or None,
            trace_id=str(getattr(request.state, "trace_id", "") or "").strip() or None,
            idempotency_key=str(request.headers.get("idempotency-key") or "").strip() or None,
        )
    )


@maintenance_router.post("/database/export", include_in_schema=True)
async def database_export(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_value(request)
    if error is not None:
        return error
    if not isinstance(payload, Mapping):
        return _json_response({"status": "error", "message": "请求体必须是 JSON 对象"}, status_code=400)
    try:
        exported = await run_in_threadpool(
            _maintenance_service(request).export,
            cast(Any, payload.get("tables")),
        )
        response = Response(exported.content, media_type="application/gzip", status_code=200)
        response.headers["Content-Disposition"] = f'attachment; filename="{exported.filename}"'
        response.headers["X-ETK-Expanded-Size"] = str(exported.expanded_size)
        response.headers["X-ETK-Compressed-Size"] = str(exported.compressed_size)
        response.headers["X-ETK-Compression-Ratio"] = str(exported.compression_ratio)
        return response
    except Exception as exc:
        return _maintenance_error(exc)


@maintenance_router.post("/database/preview-backup", include_in_schema=True)
async def database_preview_backup(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    content, filename, error = await _backup_upload(request)
    if error is not None:
        return error
    if content is None:
        return _json_response({"status": "error", "message": "未找到文件"}, status_code=400)
    try:
        result = await run_in_threadpool(
            _maintenance_service(request).preview,
            content,
            filename=filename,
        )
        return _maintenance_response(result)
    except Exception as exc:
        return _maintenance_error(exc)


@maintenance_router.post("/database/import", include_in_schema=True)
async def database_import(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_value(request)
    if error is not None:
        return error
    if not isinstance(payload, Mapping):
        return _json_response(
            {"status": "error", "message": "导入请求必须引用 artifact_id，且请求体为 JSON 对象"}, status_code=400
        )
    if "file" in payload or "backup" in payload or "backup_bytes" in payload:
        return _json_response(
            {"status": "error", "message": "导入只接受已持久化 artifact_id，不接受 backup bytes"}, status_code=400
        )
    session = get_request_session(request)
    try:
        result = await run_in_threadpool(
            _maintenance_service(request).submit,
            DatabaseMaintenanceOperationKind.IMPORT,
            payload,
            principal_id=str(session.get("emby_user_id") or "admin"),
            idempotency_key=str(request.headers.get("idempotency-key") or "").strip() or None,
        )
        return _maintenance_response(result)
    except Exception as exc:
        return _maintenance_error(exc)


async def _submit_maintenance_action(
    request: Request,
    kind: DatabaseMaintenanceOperationKind,
) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_value(request)
    if error is not None:
        return error
    payload = payload if isinstance(payload, Mapping) else {}
    session = get_request_session(request)
    try:
        result = await run_in_threadpool(
            _maintenance_service(request).submit,
            kind,
            payload,
            principal_id=str(session.get("emby_user_id") or "admin"),
            idempotency_key=str(request.headers.get("idempotency-key") or "").strip() or None,
        )
        return _maintenance_response(result)
    except Exception as exc:
        return _maintenance_error(exc)


@maintenance_router.post("/actions/clear_tables", include_in_schema=True)
async def database_clear_tables(request: Request) -> Response:
    return await _submit_maintenance_action(request, DatabaseMaintenanceOperationKind.CLEAR_TABLES)


@maintenance_router.post("/database/correct-sequences", include_in_schema=True)
async def database_correct_sequences(request: Request) -> Response:
    return await _submit_maintenance_action(request, DatabaseMaintenanceOperationKind.CORRECT_SEQUENCES)


@maintenance_router.post("/actions/prepare-for-library-rebuild", include_in_schema=True)
async def database_prepare_for_library_rebuild(request: Request) -> Response:
    return await _submit_maintenance_action(request, DatabaseMaintenanceOperationKind.PREPARE_FOR_LIBRARY_REBUILD)


@maintenance_router.post("/actions/cleanup-offline-media", include_in_schema=True)
async def database_cleanup_offline_media(request: Request) -> Response:
    return await _submit_maintenance_action(request, DatabaseMaintenanceOperationKind.CLEANUP_OFFLINE_MEDIA)


@maintenance_router.post("/actions/clear-vectors", include_in_schema=True)
async def database_clear_vectors(request: Request) -> Response:
    return await _submit_maintenance_action(request, DatabaseMaintenanceOperationKind.CLEAR_VECTORS)


@maintenance_router.get("/database/maintenance/operations/{operation_id}", include_in_schema=True)
def database_maintenance_status(operation_id: str, request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        parsed = UUID(operation_id)
    except ValueError:
        return _json_response({"status": "error", "message": "operation_id must be a UUID"}, status_code=400)
    try:
        return _maintenance_response(_maintenance_service(request).status(parsed))
    except Exception as exc:
        return _maintenance_error(exc)


# A short alias is retained for clients that model operation status as a
# top-level operations resource.
@maintenance_router.get("/database/maintenance/{operation_id}", include_in_schema=False)
def database_maintenance_status_alias(operation_id: str, request: Request) -> Response:
    return database_maintenance_status(operation_id, request)


__all__ = ["effects_router", "maintenance_router", "router"]
