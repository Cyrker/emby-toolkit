from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Request
from starlette.responses import JSONResponse

from etk.compatibility.response_contract import fastapi_json_response
from etk.domains.operations.jobs import JobSubmissionService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/emby", tags=["p115", "intro-detection"])


def _jobs(request: Request) -> JobSubmissionService:
    value = request.app.state.container.use_case("operations.jobs")
    if not isinstance(value, JobSubmissionService):
        raise TypeError("operations.jobs provider returned an invalid service")
    return value


@router.post(
    "/intro-detection/backfill",
    status_code=202,
    response_model=dict[str, Any],
    responses={503: {"model": dict[str, Any]}},
)
def trigger_emby_intro_detection_backfill(request: Request) -> JSONResponse:
    """Submit the opt-in intro fallback without scanning in the request process."""

    try:
        submitted = _jobs(request).submit_task(
            "intro-fingerprint-backfill",
            {"force": False},
            trigger_type="system",
            max_attempts=1,
            trace_id=str(getattr(request.state, "trace_id", "") or "").strip() or None,
        )
    except Exception:
        logger.exception("Intro-detection backfill Job submission failed")
        return fastapi_json_response(
            {
                "ok": False,
                "submitted": False,
                "reason": "persistent_submit_failed",
            },
            status_code=503,
        )
    return fastapi_json_response(
        {
            "ok": True,
            "submitted": True,
            "job_id": str(submitted.projection.job.id),
        },
        status_code=202,
    )


__all__ = ["router"]
