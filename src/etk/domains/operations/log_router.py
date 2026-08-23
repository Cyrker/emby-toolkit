from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Request
from starlette.responses import Response
from werkzeug.exceptions import Forbidden, InternalServerError, NotFound
from werkzeug.utils import secure_filename

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.container import ProviderNotRegisteredError
from etk.platform.web import PermissionPolicy, get_request_session

from .logs import (
    LocalLogStorage,
    LogAccessDenied,
    LogContextBlock,
    LogFileMissing,
    LogReadFailure,
    LogSearchMatch,
    LogService,
    render_log_html,
)

router = APIRouter(prefix="/api/logs", tags=["operations"])
logger = logging.getLogger(__name__)

_LOG_SERVICE_PROVIDER = "operations.logs"
_PERMISSION_POLICY_PROVIDER = "permission_policy"
_PERMISSION_READER_PROVIDERS = ("permissions", "identity")


def _container(request: Request) -> Any:
    return request.app.state.container


def _service(request: Request) -> LogService:
    container = _container(request)
    try:
        service = container.use_case(_LOG_SERVICE_PROVIDER)
    except ProviderNotRegisteredError:
        service = _deferred_local_service(request, container)
    if not isinstance(service, LogService):
        raise TypeError("operations.logs provider returned an invalid service")
    return service


def _deferred_local_service(request: Request, container: Any) -> LogService:
    cached = getattr(request.app.state, "operations_log_service", None)
    if isinstance(cached, LogService):
        return cached

    configured = getattr(request.app.state, "log_directory", None)
    if configured is None:
        configured = getattr(container.settings, "log_directory", None)
    if configured is None:
        raise ProviderNotRegisteredError(
            "operations.logs provider is not registered and no log directory is configured"
        )

    service = LogService(LocalLogStorage(Path(configured)))
    request.app.state.operations_log_service = service
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


def _admin_denial(request: Request) -> Response | None:
    decision = _permission_policy(request).require_admin(get_request_session(request))
    if decision.allowed:
        return None
    return fastapi_json_response(
        {"status": "error", "message": decision.message},
        status_code=decision.status_code,
    )


def _html_error(error: Forbidden | NotFound | InternalServerError) -> Response:
    headers = dict(error.get_headers())
    return Response(
        content=error.get_body(),
        status_code=int(error.code or 500),
        headers={"content-type": headers["Content-Type"]},
    )


@router.get("/list", response_model=list[str])
def list_log_files(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        return fastapi_json_response(_service(request).list_log_files())
    except Exception as exc:
        logger.error("API: 无法列出日志文件: %s", exc, exc_info=True)
        return fastapi_json_response({"error": "无法读取日志文件列表"}, status_code=500)


@router.get(
    "/view",
    response_class=Response,
    responses={
        200: {
            "content": {
                "text/plain": {"schema": {"type": "string"}},
                "text/html": {"schema": {"type": "string"}},
            }
        }
    },
)
def view_log_file(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied

    filename = request.query_params.get("filename", "")
    output_format = request.query_params.get("format", "json").lower()
    try:
        safe_name, lines = _service(request).view_log_file(filename)
    except LogAccessDenied:
        return _html_error(Forbidden("禁止访问非日志文件或无效的文件名。"))
    except LogFileMissing:
        return _html_error(NotFound("文件未找到。"))
    except LogReadFailure as exc:
        safe_name = secure_filename(filename)
        logger.error("API: 读取日志文件 %r 时出错: %s", safe_name, exc, exc_info=True)
        return _html_error(InternalServerError(f"读取文件 '{safe_name}' 时发生内部错误。"))
    except Exception as exc:
        safe_name = secure_filename(filename)
        logger.error("API: 读取日志文件 %r 时出错: %s", safe_name, exc, exc_info=True)
        return _html_error(InternalServerError(f"读取文件 '{safe_name}' 时发生内部错误。"))

    if output_format == "html":
        block = LogContextBlock(file=safe_name, date="", lines=lines)
        return Response(render_log_html([block], query=""), media_type="text/html")
    return Response("".join(lines), media_type="text/plain")


@router.get("/search", response_model=list[LogSearchMatch])
def search_all_logs(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied

    query = request.query_params.get("q", "").strip()
    if not query:
        return fastapi_json_response({"error": "搜索关键词不能为空"}, status_code=400)
    try:
        matches = _service(request).search_all_logs(query)
        return fastapi_json_response([item.to_payload() for item in matches])
    except Exception as exc:
        logger.error("API: 全局日志搜索时发生严重错误: %s", exc, exc_info=True)
        return fastapi_json_response({"error": "搜索过程中发生服务器内部错误"}, status_code=500)


@router.get("/search_context", response_model=list[LogContextBlock])
def search_logs_with_context(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied

    query = request.query_params.get("q", "").strip()
    output_format = request.query_params.get("format", "json").lower()
    if not query:
        return fastapi_json_response({"error": "搜索关键词不能为空"}, status_code=400)
    try:
        blocks = _service(request).search_logs_with_context(query)
        if output_format == "html":
            return Response(render_log_html(blocks, query), media_type="text/html")
        return fastapi_json_response([item.to_payload() for item in blocks])
    except Exception as exc:
        logger.error("API: 上下文日志搜索错误: %s", exc, exc_info=True)
        return fastapi_json_response({"error": "服务器内部错误"}, status_code=500)
