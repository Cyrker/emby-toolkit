from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, RootModel
from starlette.responses import JSONResponse

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .integrations import SystemIntegrationService

router = APIRouter(prefix="/api", tags=["system"])


class AIConfigurationRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    ai_provider: str | None = None
    ai_api_key: str | None = None
    ai_base_url: str | None = None
    ai_model_name: str | None = None


class ProxyTestRequest(BaseModel):
    url: str | None = None


class TelegramTestRequest(BaseModel):
    token: str | None = None
    chat_id: str | int | None = None


class LicenseRequest(BaseModel):
    license_key: str | None = None


class ExternalObjectResponse(RootModel[dict[str, Any]]):
    pass


class AboutInfoResponse(BaseModel):
    current_version: str
    releases: list[dict[str, Any]]


def _request_body(model: type[BaseModel]) -> dict[str, Any]:
    return {
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": model.model_json_schema()}},
        }
    }


def _service(request: Request) -> SystemIntegrationService:
    value = request.app.state.container.use_case("system-integrations")
    if not isinstance(value, SystemIntegrationService):
        raise TypeError("system integrations provider returned an invalid service")
    return value


def _admin_denial(request: Request) -> JSONResponse | None:
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


async def _json_object(request: Request) -> dict[str, Any]:
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _response(result) -> JSONResponse:
    return fastapi_json_response(result.payload, status_code=result.status_code)


@router.post(
    "/ai/test",
    response_model=ExternalObjectResponse,
    openapi_extra=_request_body(AIConfigurationRequest),
)
async def test_ai(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload = AIConfigurationRequest.model_validate(await _json_object(request)).model_dump(exclude_none=True)
    return _response(_service(request).test_ai(payload))


@router.post(
    "/ai/models",
    response_model=ExternalObjectResponse,
    openapi_extra=_request_body(AIConfigurationRequest),
)
async def list_ai_models(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload = AIConfigurationRequest.model_validate(await _json_object(request)).model_dump(exclude_none=True)
    return _response(_service(request).list_ai_models(payload))


@router.post(
    "/proxy/test",
    response_model=ExternalObjectResponse,
    openapi_extra=_request_body(ProxyTestRequest),
)
async def test_proxy(request: Request) -> JSONResponse:
    payload = ProxyTestRequest.model_validate(await _json_object(request)).model_dump(exclude_none=True)
    return _response(_service(request).test_proxy(payload))


@router.post(
    "/telegram/test",
    response_model=ExternalObjectResponse,
    openapi_extra=_request_body(TelegramTestRequest),
)
async def test_telegram(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload = TelegramTestRequest.model_validate(await _json_object(request)).model_dump(exclude_none=True)
    return _response(_service(request).test_telegram(payload))


@router.get("/system/about_info", response_model=AboutInfoResponse)
def about_info(request: Request) -> JSONResponse:
    return _response(_service(request).about_info())


@router.get("/system/pro_pricing", response_model=ExternalObjectResponse)
def pro_pricing(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(_service(request).pricing())


@router.post(
    "/system/activate_pro",
    response_model=ExternalObjectResponse,
    openapi_extra=_request_body(LicenseRequest),
)
async def activate_pro(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload = LicenseRequest.model_validate(await _json_object(request)).model_dump(exclude_none=True)
    return _response(_service(request).activate(payload))


@router.post(
    "/system/transfer_pro",
    response_model=ExternalObjectResponse,
    openapi_extra=_request_body(LicenseRequest),
)
async def transfer_pro(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload = LicenseRequest.model_validate(await _json_object(request)).model_dump(exclude_none=True)
    return _response(_service(request).transfer(payload))
