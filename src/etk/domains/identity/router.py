from __future__ import annotations

import json

from fastapi import APIRouter, Request
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, ValidationError
from starlette.concurrency import run_in_threadpool
from starlette.responses import Response
from werkzeug.exceptions import BadRequest, InternalServerError, UnsupportedMediaType

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .models import (
    AuthCheckResponse,
    LoginRequest,
    LoginResponse,
    ReauthorizeRequest,
    ReauthorizeResponse,
    RecoveryVerifyRequest,
    ServiceStatusResponse,
    SetupRequest,
    SetupResponse,
    StatusResponse,
)
from .service import IdentityService

router = APIRouter(prefix="/api/auth", tags=["identity"])


def _request_body(model: type[BaseModel]) -> dict[str, object]:
    return {
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": model.model_json_schema()}},
        }
    }


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


async def _validated_body(
    request: Request,
    model: type[BaseModel],
) -> tuple[BaseModel | None, Response | None]:
    if not _is_json_content_type(request.headers.get("content-type", "")):
        return None, _html_error(UnsupportedMediaType())
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, _html_error(BadRequest())
    if not isinstance(payload, dict):
        return None, _html_error(InternalServerError())
    try:
        return model.model_validate(payload), None
    except ValidationError:
        return None, _html_error(InternalServerError())


def _service(request: Request) -> IdentityService:
    service = request.app.state.container.use_case("identity")
    if not isinstance(service, IdentityService):
        raise TypeError("identity provider returned an invalid service")
    return service


def _response(result):
    return fastapi_json_response(
        jsonable_encoder(result.payload, exclude_none=True),
        status_code=result.status_code,
    )


def _require_login(request: Request):
    container = request.app.state.container
    decision = PermissionPolicy(
        _service(request).repository,
        auth_enabled=bool(container.settings.auth_enabled),
    ).require_emby_login(get_request_session(request))
    if decision.allowed:
        return None
    return fastapi_json_response(
        {"status": "error", "message": "需要先登录"},
        status_code=decision.status_code,
    )


@router.get("/check_status", response_model=AuthCheckResponse)
def check_status(request: Request):
    return _response(_service(request).check_status(get_request_session(request)))


@router.post(
    "/login",
    response_model=LoginResponse,
    openapi_extra=_request_body(LoginRequest),
)
async def login(request: Request):
    payload, invalid = await _validated_body(request, LoginRequest)
    if invalid is not None:
        return invalid
    assert isinstance(payload, LoginRequest)
    result = await run_in_threadpool(
        _service(request).login,
        payload,
        get_request_session(request),
    )
    return _response(result)


@router.post("/request_recovery", response_model=StatusResponse)
def request_recovery(request: Request):
    return _response(_service(request).request_recovery())


@router.post(
    "/verify_recovery",
    response_model=StatusResponse,
    openapi_extra=_request_body(RecoveryVerifyRequest),
)
async def verify_recovery(request: Request):
    payload, invalid = await _validated_body(request, RecoveryVerifyRequest)
    if invalid is not None:
        return invalid
    assert isinstance(payload, RecoveryVerifyRequest)
    return _response(_service(request).verify_recovery(payload, get_request_session(request)))


@router.post(
    "/setup",
    response_model=SetupResponse,
    openapi_extra=_request_body(SetupRequest),
)
async def setup(request: Request):
    service = _service(request)
    session = get_request_session(request)
    if service.is_configured() and not session.get("is_setup_mode"):
        return fastapi_json_response(
            {"status": "error", "message": "系统已配置，且无重置权限"},
            status_code=403,
        )
    payload, invalid = await _validated_body(request, SetupRequest)
    if invalid is not None:
        return invalid
    assert isinstance(payload, SetupRequest)
    return _response(await run_in_threadpool(service.setup, payload, session))


@router.get("/service_status", response_model=ServiceStatusResponse)
def service_status(request: Request):
    denied = _require_login(request)
    if denied is not None:
        return denied
    return _response(_service(request).service_status())


@router.post(
    "/reauthorize",
    response_model=ReauthorizeResponse,
    openapi_extra=_request_body(ReauthorizeRequest),
)
async def reauthorize(request: Request):
    denied = _require_login(request)
    if denied is not None:
        return denied
    payload, invalid = await _validated_body(request, ReauthorizeRequest)
    if invalid is not None:
        return invalid
    assert isinstance(payload, ReauthorizeRequest)
    return _response(await run_in_threadpool(_service(request).reauthorize, payload))


@router.post("/logout", response_model=StatusResponse)
def logout(request: Request):
    return _response(_service(request).logout(get_request_session(request)))
