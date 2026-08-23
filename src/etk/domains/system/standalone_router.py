from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, RootModel, ValidationError
from starlette.responses import JSONResponse

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .standalone import STANDALONE_SETTINGS_USE_CASE_PROVIDER, StandaloneSettingsService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["system"])


class TelegramTemplatesRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    templates: dict[str, Any] | None = None


class ObjectRequest(RootModel[dict[str, Any]]):
    pass


class MenuConfigRequest(RootModel[dict[str, Any] | list[Any]]):
    pass


class StringMapResponse(RootModel[dict[str, str]]):
    pass


class ObjectMapResponse(RootModel[dict[str, Any]]):
    pass


class MenuConfigResponse(RootModel[dict[str, Any] | list[Any]]):
    pass


class MessageResponse(BaseModel):
    message: str


class TelegramTemplatesSavedResponse(BaseModel):
    success: bool
    message: str
    templates: dict[str, str]


class PromptsResetResponse(BaseModel):
    message: str
    prompts: dict[str, Any]


def _request_body(model: type[BaseModel]) -> dict[str, Any]:
    return {
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": model.model_json_schema()}},
        }
    }


def _settings(request: Request) -> StandaloneSettingsService:
    service = request.app.state.container.use_case(STANDALONE_SETTINGS_USE_CASE_PROVIDER)
    if not isinstance(service, StandaloneSettingsService):
        raise TypeError("standalone settings provider returned an invalid service")
    return service


def _admin_denial(request: Request) -> JSONResponse | None:
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


async def _json_body(request: Request) -> Any:
    try:
        return await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


@router.get("/telegram/templates", response_model=StringMapResponse)
def get_telegram_notification_templates(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        return fastapi_json_response(_settings(request).get_telegram_templates())
    except Exception:
        logger.exception("Unable to read Telegram notification templates")
        return fastapi_json_response({"error": "读取 Telegram 通知模板失败"}, status_code=500)


@router.post(
    "/telegram/templates",
    response_model=TelegramTemplatesSavedResponse,
    openapi_extra=_request_body(TelegramTemplatesRequest),
)
async def save_telegram_notification_templates(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload = await _json_body(request)
    if isinstance(payload, dict):
        payload = TelegramTemplatesRequest.model_validate(payload).model_dump()
    try:
        templates = _settings(request).save_telegram_templates(payload or {})
        return fastapi_json_response({"success": True, "message": "Telegram 通知模板已保存", "templates": templates})
    except Exception:
        logger.exception("Unable to save Telegram notification templates")
        return fastapi_json_response({"error": "保存 Telegram 通知模板失败"}, status_code=500)


@router.post(
    "/config/custom_theme",
    response_model=MessageResponse,
    openapi_extra=_request_body(ObjectRequest),
)
async def save_custom_theme(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    theme = await _json_body(request)
    try:
        theme = ObjectRequest.model_validate(theme).root
    except ValidationError:
        return fastapi_json_response({"error": "无效的主题数据格式，必须是一个JSON对象。"}, status_code=400)
    try:
        _settings(request).save_custom_theme(theme)
        return fastapi_json_response({"message": "你的专属主题已保存！"})
    except Exception:
        logger.exception("Unable to save custom theme")
        return fastapi_json_response({"error": "保存自定义主题时发生服务器内部错误。"}, status_code=500)


@router.delete("/config/custom_theme", response_model=MessageResponse)
def delete_custom_theme(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        if _settings(request).delete_custom_theme():
            return fastapi_json_response({"message": "自定义主题已删除。"})
        return fastapi_json_response({"error": "删除自定义主题文件时发生服务器内部错误。"}, status_code=500)
    except Exception:
        logger.exception("Unable to delete custom theme")
        return fastapi_json_response({"error": "删除自定义主题时发生服务器内部错误。"}, status_code=500)


@router.get("/ai/prompts", response_model=ObjectMapResponse)
def get_ai_prompts(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        return fastapi_json_response(_settings(request).get_ai_prompts())
    except Exception:
        logger.exception("Unable to read AI prompts")
        return fastapi_json_response({"error": "获取提示词失败"}, status_code=500)


@router.post(
    "/ai/prompts",
    response_model=MessageResponse,
    openapi_extra=_request_body(ObjectRequest),
)
async def save_ai_prompts(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    prompts = await _json_body(request)
    try:
        prompts = ObjectRequest.model_validate(prompts).root
    except ValidationError:
        return fastapi_json_response({"error": "无效的数据格式"}, status_code=400)
    try:
        _settings(request).save_ai_prompts(prompts)
        return fastapi_json_response({"message": "提示词已保存"})
    except Exception:
        logger.exception("Unable to save AI prompts")
        return fastapi_json_response({"error": "保存失败"}, status_code=500)


@router.post("/ai/prompts/reset", response_model=PromptsResetResponse)
def reset_ai_prompts(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        prompts = _settings(request).reset_ai_prompts()
        return fastapi_json_response({"message": "已恢复默认提示词", "prompts": prompts})
    except Exception:
        logger.exception("Unable to reset AI prompts")
        return fastapi_json_response({"error": "重置失败"}, status_code=500)


@router.get("/system/menu_config", response_model=MenuConfigResponse)
def get_menu_config(request: Request) -> JSONResponse:
    try:
        return fastapi_json_response(_settings(request).get_menu_config())
    except Exception:
        logger.exception("Unable to read menu configuration")
        return fastapi_json_response({"error": "获取菜单配置失败"}, status_code=500)


@router.post(
    "/system/menu_config",
    response_model=MessageResponse,
    openapi_extra=_request_body(MenuConfigRequest),
)
async def save_menu_config(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    menu_config = await _json_body(request)
    try:
        menu_config = MenuConfigRequest.model_validate(menu_config).root
    except ValidationError:
        return fastapi_json_response({"error": "无效的数据格式"}, status_code=400)
    try:
        _settings(request).save_menu_config(menu_config)
        return fastapi_json_response({"message": "菜单配置已保存"})
    except Exception:
        logger.exception("Unable to save menu configuration")
        return fastapi_json_response({"error": "保存失败"}, status_code=500)


@router.post("/system/menu_config/reset", response_model=MessageResponse)
def reset_menu_config(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        _settings(request).reset_menu_config()
        return fastapi_json_response({"message": "已恢复默认菜单"})
    except Exception:
        logger.exception("Unable to reset menu configuration")
        return fastapi_json_response({"error": "重置失败"}, status_code=500)
