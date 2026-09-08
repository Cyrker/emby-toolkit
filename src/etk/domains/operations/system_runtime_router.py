from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, Request
from starlette.responses import StreamingResponse

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.jobs import JobStatus
from etk.platform.web import PermissionPolicy, get_request_session

from .jobs import JobSubmissionService, TaskBusyError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/system", tags=["operations"])


def _jobs(request: Request) -> JobSubmissionService:
    service = request.app.state.container.use_case("operations.jobs")
    if not isinstance(service, JobSubmissionService):
        raise TypeError("operations.jobs provider returned an invalid service")
    return service


def _admin_denial(request: Request):
    container = request.app.state.container
    decision = PermissionPolicy(
        container.repository("identity"),
        auth_enabled=container.settings.auth_enabled,
    ).require_admin(get_request_session(request))
    if decision.allowed:
        return None
    return fastapi_json_response(
        {"status": "error", "message": "需要 Emby 管理员权限才能执行此操作"},
        status_code=403,
    )


@router.get("/update/stream")
def stream_update(request: Request) -> Any:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    jobs = _jobs(request)
    if jobs.is_busy():
        return fastapi_json_response({"error": "后台有任务正在运行，请稍后再试。"}, status_code=409)
    session = get_request_session(request)
    try:
        submitted = jobs.submit_task(
            "system-auto-update",
            {},
            trigger_type="system",
            requested_by_user_id=str(session.get("emby_user_id") or "").strip() or None,
            idempotency_key=str(request.headers.get("idempotency-key") or "").strip() or None,
            idempotency_scope=(
                "api.system.update" if str(request.headers.get("idempotency-key") or "").strip() else None
            ),
            trace_id=str(getattr(request.state, "trace_id", "") or "").strip() or None,
            reject_when_busy=True,
        )
    except TaskBusyError:
        return fastapi_json_response({"error": "后台有任务正在运行，请稍后再试。"}, status_code=409)
    except Exception as exc:
        logger.exception("Unable to create system update Job")
        return fastapi_json_response({"error": f"服务器内部错误: {exc}"}, status_code=500)
    return StreamingResponse(
        _project_update_events(jobs, submitted.projection.job.id),
        media_type="text/event-stream",
        headers={
            "cache-control": "no-cache",
            "x-accel-buffering": "no",
            "x-etk-job-id": str(submitted.projection.job.id),
        },
    )


@router.post("/restart", response_model=dict[str, Any])
def restart_system(request: Request) -> Any:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        idempotency_key = str(request.headers.get("idempotency-key") or "").strip() or None
        submitted = _jobs(request).submit_native_operation(
            "system.restart",
            {},
            display_name="重启 ETK API 服务",
            trigger_type="system",
            timeout_seconds=180,
            idempotency_key=idempotency_key,
            idempotency_scope="api.system.restart" if idempotency_key else None,
        )
        return fastapi_json_response(
            {
                "message": "重启任务已提交。",
                "job_id": str(submitted.projection.job.id),
            }
        )
    except Exception as exc:
        logger.exception("System restart Job submission failed")
        return fastapi_json_response({"error": f"发生意外错误: {exc}"}, status_code=500)


async def _project_update_events(
    jobs: JobSubmissionService,
    job_id,
) -> AsyncIterator[str]:
    last_message: str | None = None
    while True:
        projection = jobs.get(job_id)
        if projection is None:
            yield _sse({"status": "更新任务状态已丢失。", "event": "ERROR"})
            return
        job = projection.job
        if job.progress_message and job.progress_message != last_message:
            last_message = job.progress_message
            yield _sse({"status": job.progress_message})
        if job.status.terminal:
            if job.status is JobStatus.SUCCEEDED:
                result = dict(job.result or {})
                if result.get("ok") is False:
                    yield _sse(
                        {
                            "status": result.get("message") or job.progress_message or "更新任务失败。",
                            "event": "ERROR",
                        }
                    )
                    return
                event = "NO_UPDATE" if result.get("status") == "up_to_date" else "DONE"
                yield _sse(
                    {
                        "status": result.get("message") or "更新检查完成。",
                        "event": event,
                        "updated": bool(result.get("updated", False)),
                        "current_version": result.get("current_version"),
                        "target_version": result.get("target_version"),
                    }
                )
            else:
                error = dict(job.error_summary or {})
                yield _sse(
                    {
                        "status": error.get("message") or job.progress_message or "更新任务失败。",
                        "event": "ERROR",
                    }
                )
            return
        await asyncio.sleep(0.5)


def _sse(payload: dict[str, Any]) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}\n\n"


__all__ = ["router"]
