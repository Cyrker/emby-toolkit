from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Request
from pydantic import ValidationError

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .configuration import (
    ConfigurationResponse,
    ConfigurationSavedResponse,
    ConfigurationService,
    ConfigurationUpdateRequest,
)

router = APIRouter(prefix="/api", tags=["system"])


def _request_body() -> dict[str, object]:
    return {
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    "schema": ConfigurationUpdateRequest.model_json_schema(),
                }
            },
        }
    }


async def _configuration_body(request: Request) -> dict[str, Any]:
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}
    try:
        return ConfigurationUpdateRequest.model_validate(payload).root
    except ValidationError:
        return {}


def _service(request: Request) -> ConfigurationService:
    value = request.app.state.container.use_case("system-configuration")
    if not isinstance(value, ConfigurationService):
        raise TypeError("system configuration provider returned an invalid service")
    return value


def _config_denial(request: Request, *, operation: str):
    container = request.app.state.container
    decision = PermissionPolicy(
        container.repository("identity"),
        auth_enabled=container.settings.auth_enabled,
    ).require_config_access(
        get_request_session(request),
        configured=_service(request).is_configured(),
    )
    if decision.allowed:
        return None
    return fastapi_json_response(
        {"status": "error", "message": f"需要管理员权限才能{operation}配置"},
        status_code=403,
    )


@router.get("/config", response_model=ConfigurationResponse)
def read_config(request: Request) -> Any:
    denied = _config_denial(request, operation="读取")
    if denied is not None:
        return denied
    result = _service(request).read()
    return fastapi_json_response(result.payload, status_code=result.status_code)


@router.post(
    "/config",
    response_model=ConfigurationSavedResponse,
    openapi_extra=_request_body(),
)
async def save_config(request: Request) -> Any:
    denied = _config_denial(request, operation="保存")
    if denied is not None:
        return denied
    result = _service(request).save(await _configuration_body(request))
    return fastapi_json_response(result.payload, status_code=result.status_code)
