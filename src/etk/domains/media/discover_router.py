from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from starlette.responses import Response

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .discover import DiscoverHttpResult, DiscoverService

router = APIRouter(prefix="/api/discover", tags=["discover"])
effects_router = APIRouter(prefix="/api/discover", tags=["discover"])
JsonValue = dict[str, Any] | list[Any]


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


def _login_denial(request: Request) -> Response | None:
    decision = PermissionPolicy.require_any_login(get_request_session(request))
    if decision.allowed:
        return None
    return fastapi_json_response(
        {"status": "error", "message": decision.message},
        status_code=decision.status_code,
    )


def _response(result: DiscoverHttpResult) -> Response:
    return fastapi_json_response(result.payload, status_code=result.status_code)


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


__all__ = ["effects_router", "router"]
