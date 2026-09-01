from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, RootModel
from starlette.concurrency import run_in_threadpool
from starlette.responses import JSONResponse

from etk.compatibility.response_contract import fastapi_json_response

from .moviepilot_webhook import MoviePilotWebhookService

router = APIRouter(tags=["subscription", "moviepilot"])


class MoviePilotWebhookRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: str | None = None
    Event: str | None = None
    data: dict[str, Any] | None = None


class MoviePilotWebhookResponse(RootModel[dict[str, Any]]):
    pass


def _service(request: Request) -> MoviePilotWebhookService:
    value = request.app.state.container.use_case("subscription.moviepilot-webhook")
    if not isinstance(value, MoviePilotWebhookService):
        raise TypeError("subscription.moviepilot-webhook provider returned an invalid service")
    return value


@router.post(
    "/webhook/emby",
    response_model=MoviePilotWebhookResponse,
    responses={
        409: {"model": MoviePilotWebhookResponse},
        500: {"model": MoviePilotWebhookResponse},
        503: {"model": MoviePilotWebhookResponse},
    },
    openapi_extra={
        "requestBody": {
            "required": False,
            "content": {"application/json": {"schema": MoviePilotWebhookRequest.model_json_schema()}},
        }
    },
)
async def moviepilot_webhook(request: Request) -> JSONResponse:
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        payload = {}
    normalized = dict(payload) if isinstance(payload, dict) else {}
    try:
        result = await run_in_threadpool(_service(request).handle, normalized)
    except Exception as exc:
        return fastapi_json_response(
            {"status": "error", "message": str(exc)},
            status_code=500,
        )
    return fastapi_json_response(result.payload, status_code=result.status_code, headers=result.headers)


__all__ = ["router"]
