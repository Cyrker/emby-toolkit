from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Query, Request

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .runtime import (
    RuntimeStatusReader,
    RuntimeStatusResponse,
    TaskCatalog,
    TaskDefinitionResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["operations"])


def _status_reader(request: Request) -> RuntimeStatusReader:
    return request.app.state.container.use_case("runtime-status")


def _task_catalog(request: Request) -> TaskCatalog:
    return request.app.state.container.use_case("task-catalog")


def _admin_denial(request: Request):
    container = request.app.state.container
    policy = PermissionPolicy(
        container.repository("identity"),
        auth_enabled=container.settings.auth_enabled,
    )
    decision = policy.require_admin(get_request_session(request))
    if decision.allowed:
        return None
    return fastapi_json_response(
        {"status": "error", "message": "需要 Emby 管理员权限才能执行此操作"},
        status_code=403,
    )


@router.get("/status", response_model=RuntimeStatusResponse)
def get_runtime_status(request: Request) -> Any:
    return fastapi_json_response(dict(_status_reader(request).read_status()))


@router.get("/tasks/available", response_model=list[TaskDefinitionResponse])
def get_available_tasks(
    request: Request,
    context: str = Query(default="chain"),
) -> Any:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        return fastapi_json_response(_task_catalog(request).available(context))
    except Exception:
        logger.exception("Unable to read the task catalog")
        return fastapi_json_response({"error": "无法获取可用任务列表"}, status_code=500)
