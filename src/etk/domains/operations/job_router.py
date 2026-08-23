from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any, Protocol
from uuid import UUID

from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict
from starlette.responses import Response
from werkzeug.exceptions import BadRequest, InternalServerError, UnsupportedMediaType

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.jobs import IdempotencyConflictError, InvalidJsonPayloadError, JobNotFoundError
from etk.platform.web import PermissionPolicy, get_request_session

from .jobs import (
    JobSubmissionService,
    TaskBusyError,
    UnknownTaskError,
    serialize_projection,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["operations"])


class TaskRunRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    task_name: str


class ProcessorReadiness(Protocol):
    def media_ready(self) -> bool: ...

    def any_ready(self) -> bool: ...

    def signal_stop_all(self) -> None: ...


def _service(request: Request) -> JobSubmissionService:
    service = request.app.state.container.use_case("operations.jobs")
    if not isinstance(service, JobSubmissionService):
        raise TypeError("operations.jobs provider returned an invalid service")
    return service


def _readiness(request: Request) -> ProcessorReadiness:
    return request.app.state.container.client("operations.job-readiness")


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


@router.post(
    "/tasks/run",
    response_model=dict[str, Any],
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": TaskRunRequest.model_json_schema()}},
        }
    },
)
async def run_task(request: Request) -> Any:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    service = _service(request)
    if service.is_busy():
        return fastapi_json_response({"error": "后台有任务正在运行，请稍后再试。"}, status_code=409)
    if not _readiness(request).media_ready():
        return fastapi_json_response({"error": "核心处理器未就绪。"}, status_code=503)

    payload, invalid = await _json_object(request)
    if invalid is not None:
        return invalid
    assert payload is not None
    task_name = payload.pop("task_name", None)
    if not task_name:
        return fastapi_json_response({"error": "请求体中缺少 'task_name' 参数"}, status_code=400)
    user_id = str(get_request_session(request).get("emby_user_id") or "").strip() or None
    try:
        submitted = service.submit_task(
            str(task_name),
            payload,
            trigger_type="manual",
            requested_by_user_id=user_id,
            idempotency_key=_idempotency_key(request),
            idempotency_scope="api.tasks.run" if _idempotency_key(request) else None,
            trace_id=str(getattr(request.state, "trace_id", "") or "").strip() or None,
            reject_when_busy=True,
        )
    except UnknownTaskError:
        return fastapi_json_response({"error": f"未知的任务名称: {task_name}"}, status_code=404)
    except IdempotencyConflictError:
        return fastapi_json_response({"error": "幂等键已用于不同的任务请求。"}, status_code=409)
    except TaskBusyError:
        # Preserve the legacy submit-race behavior: the decorator-style busy
        # check already passed, but the final submit lost the race.
        return fastapi_json_response({"error": "任务提交失败，未知错误。"}, status_code=500)
    except (InvalidJsonPayloadError, ValueError):
        logger.exception("Task request payload could not be persisted")
        return fastapi_json_response({"error": "任务提交失败，未知错误。"}, status_code=500)
    except Exception as exc:
        logger.exception("Task submission failed")
        return fastapi_json_response({"error": f"服务器内部错误: {exc}"}, status_code=500)
    display_name = submitted.projection.job.request_payload.get("display_name") or str(task_name)
    return fastapi_json_response(
        {"message": f"任务 '{display_name}' 已成功提交。"},
        status_code=202,
        headers={"x-etk-job-id": str(submitted.projection.job.id)},
    )


@router.post("/trigger_stop_task", response_model=dict[str, Any])
def stop_current_task(request: Request) -> Any:
    readiness = _readiness(request)
    if not readiness.any_ready():
        return fastapi_json_response({"error": "核心处理器未就绪"}, status_code=503)
    try:
        _service(request).cancel_current()
        readiness.signal_stop_all()
    except Exception as exc:
        logger.exception("Unable to request task cancellation")
        return fastapi_json_response({"error": f"发送停止信号时出错: {exc}"}, status_code=500)
    return fastapi_json_response({"message": "已发送停止任务请求。"})


@router.get("/jobs/{job_id}", response_model=dict[str, Any])
def get_job(request: Request, job_id: str) -> Any:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    parsed = _uuid(job_id)
    if parsed is None:
        return fastapi_json_response({"error": "任务不存在"}, status_code=404)
    projection = _service(request).get(parsed)
    if projection is None:
        return fastapi_json_response({"error": "任务不存在"}, status_code=404)
    return fastapi_json_response(serialize_projection(projection))


@router.post("/jobs/{job_id}/cancel", response_model=dict[str, Any])
def cancel_job(request: Request, job_id: str) -> Any:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    parsed = _uuid(job_id)
    if parsed is None:
        return fastapi_json_response({"error": "任务不存在"}, status_code=404)
    try:
        projection = _service(request).cancel(parsed)
    except JobNotFoundError:
        return fastapi_json_response({"error": "任务不存在"}, status_code=404)
    return fastapi_json_response(serialize_projection(projection), status_code=202)


async def _json_object(request: Request) -> tuple[dict[str, Any] | None, Response | None]:
    content_type = request.headers.get("content-type", "").partition(";")[0].strip().casefold()
    if content_type != "application/json" and not (
        content_type.startswith("application/") and content_type.endswith("+json")
    ):
        return None, _html_error(UnsupportedMediaType())
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, _html_error(BadRequest())
    if not isinstance(payload, Mapping):
        return None, _html_error(InternalServerError())
    return dict(payload), None


def _html_error(error: BadRequest | InternalServerError | UnsupportedMediaType) -> Response:
    headers = dict(error.get_headers())
    return Response(
        content=error.get_body(),
        status_code=int(error.code or 500),
        headers={"content-type": headers["Content-Type"]},
    )


def _uuid(value: str) -> UUID | None:
    try:
        return UUID(value)
    except (TypeError, ValueError, AttributeError):
        return None


def _idempotency_key(request: Request) -> str | None:
    value = str(request.headers.get("idempotency-key") or "").strip()
    return value or None
