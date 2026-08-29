from __future__ import annotations

import json
from typing import Any, Literal

from fastapi import APIRouter, Request
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool
from starlette.responses import Response
from werkzeug.exceptions import BadRequest, UnsupportedMediaType

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .discover import DiscoverHttpResult, DiscoverService
from .discover_effects import DiscoverEffectsHttpResult, DiscoverEffectsService

router = APIRouter(prefix="/api/discover", tags=["discover"])
effects_router = APIRouter(prefix="/api/discover", tags=["discover"])
JsonValue = dict[str, Any] | list[Any]


class RecommendationUpdateAccepted(BaseModel):
    status: Literal["ok"]
    task_key: Literal["update-daily-theme"]
    job_id: str
    message: str


_JSON_BODY = {
    "requestBody": {
        "required": True,
        "content": {"application/json": {"schema": {"type": "object"}}},
    }
}


def _responses(*codes: int) -> dict[int | str, dict[str, Any]]:
    return {code: {"description": "Legacy-compatible P8 discover response"} for code in codes}


def _service(request: Request) -> DiscoverService:
    direct = getattr(request.app.state, "discover_service", None)
    if isinstance(direct, DiscoverService):
        return direct
    value = request.app.state.container.use_case("media.discover")
    if not isinstance(value, DiscoverService):
        raise TypeError("media.discover provider returned an invalid service")
    return value


def _effects_service(request: Request) -> DiscoverEffectsService:
    direct = getattr(request.app.state, "discover_effects_service", None)
    if isinstance(direct, DiscoverEffectsService):
        return direct
    value = request.app.state.container.use_case("media.discover.effects")
    if not isinstance(value, DiscoverEffectsService):
        raise TypeError("media.discover.effects provider returned an invalid service")
    return value


def _login_denial(request: Request) -> Response | None:
    container = request.app.state.container
    decision = PermissionPolicy(
        container.repository("identity"),
        auth_enabled=bool(container.settings.auth_enabled),
    ).require_any_login(get_request_session(request))
    if decision.allowed:
        return None
    return fastapi_json_response(
        {"status": "error", "message": decision.message},
        status_code=decision.status_code,
    )


def _response(result: DiscoverHttpResult) -> Response:
    return fastapi_json_response(result.payload, status_code=result.status_code)


def _effects_response(result: DiscoverEffectsHttpResult) -> Response:
    return fastapi_json_response(result.payload, status_code=result.status_code)


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


def _html_error(error: BadRequest | UnsupportedMediaType) -> Response:
    headers = dict(error.get_headers())
    return Response(
        content=error.get_body(),
        status_code=int(error.code or 500),
        headers={"content-type": headers["Content-Type"]},
    )


@router.get(
    "/config/languages",
    response_model=JsonValue,
    responses=_responses(401, 500),
)
def discover_languages(request: Request) -> Response:
    denied = _login_denial(request)
    return denied or _response(_service(request).languages())


@router.get(
    "/config/keywords",
    response_model=JsonValue,
    responses=_responses(401, 500),
)
def discover_keywords(request: Request) -> Response:
    denied = _login_denial(request)
    return denied or _response(_service(request).keywords())


@router.get(
    "/config/studios",
    response_model=JsonValue,
    responses=_responses(401, 500),
)
def discover_studios(request: Request) -> Response:
    denied = _login_denial(request)
    return denied or _response(_service(request).studios())


@router.get(
    "/daily_recommendation",
    response_model=JsonValue,
    responses=_responses(401, 404, 500),
)
def daily_recommendation(request: Request) -> Response:
    denied = _login_denial(request)
    return denied or _response(_service(request).daily_recommendation())


@effects_router.post(
    "/movie",
    response_model=JsonValue,
    responses=_responses(400, 401, 415, 500, 502),
    openapi_extra=_JSON_BODY,
)
async def discover_movies(request: Request) -> Response:
    denied = _login_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_value(request)
    if error is not None:
        return error
    return _effects_response(await run_in_threadpool(_effects_service(request).discover, "movie", payload))


@effects_router.post(
    "/tv",
    response_model=JsonValue,
    responses=_responses(400, 401, 415, 500, 502),
    openapi_extra=_JSON_BODY,
)
async def discover_tv(request: Request) -> Response:
    denied = _login_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_value(request)
    if error is not None:
        return error
    return _effects_response(await run_in_threadpool(_effects_service(request).discover, "tv", payload))


@effects_router.get(
    "/genres/{media_type}",
    response_model=JsonValue,
    responses=_responses(400, 401, 500),
)
def discover_genres(request: Request, media_type: str) -> Response:
    denied = _login_denial(request)
    return denied or _effects_response(_effects_service(request).genres(media_type))


@effects_router.post(
    "/search",
    response_model=JsonValue,
    responses=_responses(400, 401, 415, 500),
    openapi_extra=_JSON_BODY,
)
async def search_discover_media(request: Request) -> Response:
    denied = _login_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_value(request)
    if error is not None:
        return error
    return _effects_response(await run_in_threadpool(_effects_service(request).search, payload))


@effects_router.post(
    "/trigger_recommendation_update",
    status_code=202,
    response_model=RecommendationUpdateAccepted,
    responses=_responses(401, 409, 500),
)
def trigger_recommendation_update(request: Request) -> Response:
    denied = _login_denial(request)
    if denied is not None:
        return denied
    user_id = str(get_request_session(request).get("emby_user_id") or "").strip() or None
    trace_id = str(getattr(request.state, "trace_id", "") or "").strip() or None
    return _effects_response(
        _effects_service(request).trigger_recommendation_update(
            requested_by_user_id=user_id,
            trace_id=trace_id,
        )
    )


@effects_router.get(
    "/tmdb/tv/{tv_id}",
    response_model=JsonValue,
    responses=_responses(400, 401, 500),
)
def get_tmdb_tv_details(request: Request, tv_id: int) -> Response:
    denied = _login_denial(request)
    return denied or _effects_response(_effects_service(request).tv_details(tv_id))


__all__ = ["effects_router", "router"]
