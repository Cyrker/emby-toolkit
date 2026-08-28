from __future__ import annotations

import json
import os
from collections.abc import Mapping
from functools import partial
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, RootModel, ValidationError
from starlette.concurrency import run_in_threadpool
from starlette.responses import HTMLResponse, JSONResponse, Response
from werkzeug.exceptions import BadRequest, InternalServerError, UnsupportedMediaType

from etk.compatibility.response_contract import fastapi_json_response
from etk.domains.media.models import MediaIdentity, MediaItemType, ServiceResult
from etk.platform.web import PermissionPolicy, get_request_session

from .auth_directory import P115AuthDirectoryService
from .deep_delete import P115DeepDeleteService
from .episode_regex import P115EpisodeRegexService
from .intro_sync import P115IntroSyncNotFound, P115IntroSyncService
from .item_ready import P115ItemReadyService
from .local_config_service import P115LocalSettingsService
from .media_info import (
    P115MediaInfoCacheService,
    P115MediaInfoIdentity,
    P115MediaInfoInvalidCache,
    P115MediaInfoInvalidIdentity,
    P115MediaInfoNotFound,
)
from .models import P115Result
from .organize_execution import (
    P115ExecutionBusy,
    P115ExecutionConflict,
    P115ExecutionNotFound,
    P115ExecutionPreflightError,
    P115ExecutionStateError,
    P115OrganizeExecutionService,
)
from .organize_plan import P115OrganizePlanService
from .organize_records import P115OrganizeRecordsService
from .rename_config import P115RenameConfigService
from .sorting_rules import P115SortingRulesService
from .strm_sync import P115StrmReplaceService
from .upload_monitor import P115UploadMonitorConfigService

router = APIRouter(prefix="/api/p115", tags=["p115"])


class ExternalObjectResponse(RootModel[dict[str, Any]]):
    pass


class ExternalArrayResponse(RootModel[list[Any]]):
    pass


class ManualCookieRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    cookie: str | None = None
    app_type: str | None = None


class OrganizePlanRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    source_cid: str | None = None
    source_name: str | None = None
    request_intent_id: str | None = None
    recognition_hints: dict[str, dict[str, Any]] | None = None


class OrganizePlanActionResponse(BaseModel):
    action_index: int
    action_id: str
    kind: Literal["ENSURE_DIRECTORY", "MOVE", "RENAME"]
    subject: dict[str, Any]
    destination: dict[str, Any]
    dependencies: list[str]
    preconditions: dict[str, Any]
    effect_key: str
    decision_basis: dict[str, Any]
    risk: dict[str, Any]
    destructive: bool
    expected_postcondition: dict[str, Any]
    deferred_slice: str


class OrganizePlanDocumentResponse(BaseModel):
    plan_id: str
    schema_version: int
    planner_version: str
    request_intent_id: str
    request_hash: str
    input_fingerprint: str
    status: Literal["ready", "blocked"]
    account_id: str
    source_root: dict[str, str]
    revisions: dict[str, str]
    config_snapshot: dict[str, Any]
    source_snapshot: list[dict[str, Any]]
    provider_evidence: dict[str, Any]
    recognition_evidence: dict[str, Any]
    warnings: list[dict[str, Any]]
    errors: list[dict[str, Any]]
    risk_summary: dict[str, Any]
    actions: list[OrganizePlanActionResponse]
    created_at: str
    expires_at: str


class OrganizePlanEnvelopeResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    success: bool
    data: OrganizePlanDocumentResponse | None = None
    message: str | None = None


class OrganizeExecutionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    approved_execution_intent_id: str


class OrganizeReconciliationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    approved_reconciliation_intent_id: str


class OrganizeExecutionEnvelopeResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    success: bool
    data: dict[str, Any] | None = None
    message: str | None = None


class OrganizeRecordsResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    success: bool
    items: list[dict[str, Any]] | None = None
    total: int | None = None
    fail_reasons: list[dict[str, Any]] | None = None
    stats: dict[str, int] | None = None
    message: str | None = None


class ReplaceStrmRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    mode: str | None = "plain"
    search: str | None = ""
    replace: str | None = ""


def _auth_service(request: Request) -> P115AuthDirectoryService:
    direct = getattr(request.app.state, "p115_auth_directory_service", None)
    if isinstance(direct, P115AuthDirectoryService):
        return direct
    value = request.app.state.container.use_case("p115.auth-directory")
    if not isinstance(value, P115AuthDirectoryService):
        raise TypeError("p115.auth-directory provider returned an invalid service")
    return value


def _sorting_rules_service(request: Request) -> P115SortingRulesService:
    direct = getattr(request.app.state, "p115_sorting_rules_service", None)
    if isinstance(direct, P115SortingRulesService):
        return direct
    value = request.app.state.container.use_case("p115.sorting-rules")
    if not isinstance(value, P115SortingRulesService):
        raise TypeError("p115.sorting-rules provider returned an invalid service")
    return value


def _rename_config_service(request: Request) -> P115RenameConfigService:
    direct = getattr(request.app.state, "p115_rename_config_service", None)
    if isinstance(direct, P115RenameConfigService):
        return direct
    value = request.app.state.container.use_case("p115.rename-config")
    if not isinstance(value, P115RenameConfigService):
        raise TypeError("p115.rename-config provider returned an invalid service")
    return value


def _episode_regex_service(request: Request) -> P115EpisodeRegexService:
    direct = getattr(request.app.state, "p115_episode_regex_service", None)
    if isinstance(direct, P115EpisodeRegexService):
        return direct
    value = request.app.state.container.use_case("p115.episode-regex")
    if not isinstance(value, P115EpisodeRegexService):
        raise TypeError("p115.episode-regex provider returned an invalid service")
    return value


def _organize_plan_service(request: Request) -> P115OrganizePlanService:
    direct = getattr(request.app.state, "p115_organize_plan_service", None)
    if isinstance(direct, P115OrganizePlanService):
        return direct
    value = request.app.state.container.use_case("p115.organize-plans")
    if not isinstance(value, P115OrganizePlanService):
        raise TypeError("p115.organize-plans provider returned an invalid service")
    return value


def _organize_execution_service(request: Request) -> P115OrganizeExecutionService:
    direct = getattr(request.app.state, "p115_organize_execution_service", None)
    if isinstance(direct, P115OrganizeExecutionService):
        return direct
    value = request.app.state.container.use_case("p115.organize-executions")
    if not isinstance(value, P115OrganizeExecutionService):
        raise TypeError("p115.organize-executions provider returned an invalid service")
    return value


def _organize_records_service(request: Request) -> P115OrganizeRecordsService:
    direct = getattr(request.app.state, "p115_organize_records_service", None)
    if isinstance(direct, P115OrganizeRecordsService):
        return direct
    value = request.app.state.container.use_case("p115.organize-records")
    if not isinstance(value, P115OrganizeRecordsService):
        raise TypeError("p115.organize-records provider returned an invalid service")
    return value


def _strm_replace_service(request: Request) -> P115StrmReplaceService:
    direct = getattr(request.app.state, "p115_strm_replace_service", None)
    if isinstance(direct, P115StrmReplaceService):
        return direct
    value = request.app.state.container.use_case("p115.strm-replace")
    if not isinstance(value, P115StrmReplaceService):
        raise TypeError("p115.strm-replace provider returned an invalid service")
    return value


def _media_info_service(request: Request) -> P115MediaInfoCacheService:
    direct = getattr(request.app.state, "p115_media_info_service", None)
    if isinstance(direct, P115MediaInfoCacheService):
        return direct
    value = request.app.state.container.use_case("p115.media-info")
    if not isinstance(value, P115MediaInfoCacheService):
        raise TypeError("p115.media-info provider returned an invalid service")
    return value


def _media_service(request: Request) -> Any:
    direct = getattr(request.app.state, "media_service", None)
    if direct is not None:
        return direct
    return request.app.state.container.use_case("media")


def _item_ready_service(request: Request) -> P115ItemReadyService:
    direct = getattr(request.app.state, "p115_item_ready_service", None)
    if isinstance(direct, P115ItemReadyService):
        return direct
    value = request.app.state.container.use_case("p115.item-ready")
    if not isinstance(value, P115ItemReadyService):
        raise TypeError("p115.item-ready provider returned an invalid service")
    return value


def _intro_sync_service(request: Request) -> P115IntroSyncService:
    direct = getattr(request.app.state, "p115_intro_sync_service", None)
    if isinstance(direct, P115IntroSyncService):
        return direct
    value = request.app.state.container.use_case("p115.intro-sync")
    if not isinstance(value, P115IntroSyncService):
        raise TypeError("p115.intro-sync provider returned an invalid service")
    return value


def _deep_delete_service(request: Request) -> P115DeepDeleteService:
    direct = getattr(request.app.state, "p115_deep_delete_service", None)
    if isinstance(direct, P115DeepDeleteService):
        return direct
    value = request.app.state.container.use_case("p115.deep-delete")
    if not isinstance(value, P115DeepDeleteService):
        raise TypeError("p115.deep-delete provider returned an invalid service")
    return value


def _upload_monitor_service(request: Request) -> P115UploadMonitorConfigService:
    direct = getattr(request.app.state, "p115_upload_monitor_service", None)
    if isinstance(direct, P115UploadMonitorConfigService):
        return direct
    value = request.app.state.container.use_case("p115.upload-monitor-config")
    if not isinstance(value, P115UploadMonitorConfigService):
        raise TypeError("p115.upload-monitor-config provider returned an invalid service")
    return value


def _local_settings_service(request: Request) -> P115LocalSettingsService:
    direct = getattr(request.app.state, "p115_local_settings_service", None)
    if isinstance(direct, P115LocalSettingsService):
        return direct
    value = request.app.state.container.use_case("p115.local-settings")
    if not isinstance(value, P115LocalSettingsService):
        raise TypeError("p115.local-settings provider returned an invalid service")
    return value


def _admin_denial(request: Request) -> JSONResponse | None:
    container = request.app.state.container
    decision = PermissionPolicy(
        container.repository("identity"),
        auth_enabled=bool(container.settings.auth_enabled),
    ).require_admin(get_request_session(request))
    if decision.allowed:
        return None
    return fastapi_json_response(
        {"status": "error", "message": "需要 Emby 管理员权限才能执行此操作"},
        status_code=decision.status_code,
    )


def _response(result: P115Result) -> Response:
    if isinstance(result.payload, str):
        return HTMLResponse(result.payload, status_code=result.status_code)
    return fastapi_json_response(result.payload, status_code=result.status_code)


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


async def _legacy_json_object(request: Request) -> tuple[dict[str, Any] | None, Response | None]:
    if not _is_json_content_type(request.headers.get("content-type", "")):
        return None, _html_error(UnsupportedMediaType())
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, _html_error(BadRequest())
    if not isinstance(payload, Mapping):
        return None, _html_error(InternalServerError())
    return dict(payload), None


async def _legacy_json_value(request: Request) -> tuple[object | None, Response | None]:
    if not _is_json_content_type(request.headers.get("content-type", "")):
        return None, _html_error(UnsupportedMediaType())
    try:
        return await request.json(), None
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, _html_error(BadRequest())


async def _silent_json_object(request: Request) -> dict[str, Any]:
    """Match the legacy Bridge handlers: absent or invalid JSON behaves like an empty object."""

    raw = await request.body()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


_COOKIE_BODY = {
    "requestBody": {
        "required": True,
        "content": {"application/json": {"schema": ManualCookieRequest.model_json_schema()}},
    }
}

_JSON_VALUE_BODY = {
    "requestBody": {
        "required": True,
        "content": {"application/json": {"schema": {}}},
    }
}

_ORGANIZE_PLAN_BODY = {
    "requestBody": {
        "required": True,
        "content": {"application/json": {"schema": OrganizePlanRequest.model_json_schema()}},
    }
}

_ORGANIZE_EXECUTION_BODY = {
    "requestBody": {
        "required": True,
        "content": {"application/json": {"schema": OrganizeExecutionRequest.model_json_schema()}},
    }
}

_ORGANIZE_RECONCILIATION_BODY = {
    "requestBody": {
        "required": True,
        "content": {"application/json": {"schema": OrganizeReconciliationRequest.model_json_schema()}},
    }
}

_REPLACE_STRM_BODY = {
    "requestBody": {
        "required": True,
        "content": {"application/json": {"schema": ReplaceStrmRequest.model_json_schema()}},
    }
}

_UPLOAD_MONITOR_BODY = {
    "requestBody": {
        "required": False,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "enabled": {"type": "boolean"},
                        "mappings": {"type": "array", "maxItems": 50, "items": {"type": "object"}},
                    },
                }
            }
        },
    }
}

_DEEP_DELETE_PREPARE_BODY = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "required": ["root_item_id", "anchor_item_id", "item_type"],
                    "properties": {
                        "root_item_id": {"type": "string"},
                        "anchor_item_id": {"type": "string"},
                        "item_type": {"type": "string"},
                        "item_name": {"type": "string"},
                        "series_id": {"type": "string"},
                    },
                }
            }
        },
    }
}

_DEEP_DELETE_COMMIT_BODY = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "required": ["token"],
                    "properties": {"token": {"type": "string"}},
                }
            }
        },
    }
}


@router.get(
    "/default_stream_config",
    response_model=ExternalObjectResponse,
    responses={
        403: {"model": ExternalObjectResponse},
        500: {"content": {"text/html": {"schema": {"type": "string"}}}},
    },
)
async def get_default_stream_config(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    result = await run_in_threadpool(_local_settings_service(request).get_default_stream_config)
    return _html_error(InternalServerError()) if result.status_code == 500 else _response(result)


@router.post(
    "/default_stream_config",
    response_model=ExternalObjectResponse,
    responses={
        400: {"content": {"text/html": {"schema": {"type": "string"}}}},
        403: {"model": ExternalObjectResponse},
        415: {"content": {"text/html": {"schema": {"type": "string"}}}},
        500: {"content": {"text/html": {"schema": {"type": "string"}}}},
    },
    openapi_extra=_JSON_VALUE_BODY,
)
async def save_default_stream_config(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    raw_payload, error = await _legacy_json_value(request)
    if error is not None:
        return error
    payload = dict(raw_payload) if isinstance(raw_payload, Mapping) else {}
    result = await run_in_threadpool(_local_settings_service(request).save_default_stream_config, payload)
    return _html_error(InternalServerError()) if result.status_code == 500 else _response(result)


@router.get(
    "/release_groups",
    response_model=ExternalObjectResponse,
    responses={403: {"model": ExternalObjectResponse}, 500: {"model": ExternalObjectResponse}},
)
async def get_release_groups(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(await run_in_threadpool(_local_settings_service(request).get_release_groups))


def _query_parameters(*names: str) -> dict[str, Any]:
    return {
        "parameters": [
            {
                "name": name,
                "in": "query",
                "required": False,
                "schema": {"type": "string"},
            }
            for name in names
        ]
    }


def _path_parameter(name: str, *, schema_type: str = "string") -> dict[str, Any]:
    return {
        "parameters": [
            {
                "name": name,
                "in": "path",
                "required": True,
                "schema": {"type": schema_type},
            }
        ]
    }


def _path_and_query_parameters(path_name: str, *query_names: str) -> dict[str, Any]:
    return {
        "parameters": [
            *_path_parameter(path_name)["parameters"],
            *_query_parameters(*query_names)["parameters"],
        ]
    }


@router.post(
    "/qrcode",
    response_model=ExternalObjectResponse,
    responses={
        400: {"model": ExternalObjectResponse},
        403: {"model": ExternalObjectResponse},
        500: {"model": ExternalObjectResponse},
    },
)
async def create_openapi_qrcode(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(await run_in_threadpool(_auth_service(request).create_openapi_qrcode))


@router.get(
    "/qrcode/status",
    response_model=ExternalObjectResponse,
    responses={403: {"model": ExternalObjectResponse}, 500: {"model": ExternalObjectResponse}},
)
async def poll_openapi_qrcode(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(await run_in_threadpool(_auth_service(request).poll_openapi_qrcode))


@router.get(
    "/cookie_qrcode",
    response_model=ExternalObjectResponse,
    responses={403: {"model": ExternalObjectResponse}, 500: {"model": ExternalObjectResponse}},
    openapi_extra=_query_parameters("app"),
)
async def create_cookie_qrcode(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    app_type = str(request.query_params.get("app") or "alipaymini")
    return _response(await run_in_threadpool(_auth_service(request).create_cookie_qrcode, app_type))


@router.get(
    "/cookie_qrcode/status",
    response_model=ExternalObjectResponse,
    responses={403: {"model": ExternalObjectResponse}, 500: {"model": ExternalObjectResponse}},
    openapi_extra=_query_parameters("app"),
)
async def poll_cookie_qrcode(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    app_type = str(request.query_params.get("app") or "")
    return _response(await run_in_threadpool(_auth_service(request).poll_cookie_qrcode, app_type))


@router.post(
    "/cookie",
    response_model=ExternalObjectResponse,
    responses={
        400: {"content": {"text/html": {"schema": {"type": "string"}}}},
        403: {"model": ExternalObjectResponse},
        415: {"content": {"text/html": {"schema": {"type": "string"}}}},
        500: {
            "content": {
                "application/json": {"schema": {"type": "object", "additionalProperties": True}},
                "text/html": {"schema": {"type": "string"}},
            }
        },
    },
    openapi_extra=_COOKIE_BODY,
)
async def save_manual_cookie(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, invalid = await _legacy_json_object(request)
    if invalid is not None:
        return invalid
    assert payload is not None
    cookie = str(payload.get("cookie") or "").strip()
    app_type = str(payload.get("app_type") or "web").strip()
    return _response(await run_in_threadpool(_auth_service(request).save_manual_cookie, cookie, app_type))


@router.get(
    "/auto_save_auth",
    response_model=None,
    response_class=HTMLResponse,
    responses={
        200: {"content": {"text/html": {"schema": {"type": "string"}}}},
        400: {"content": {"text/html": {"schema": {"type": "string"}}}},
        403: {"model": ExternalObjectResponse},
    },
    openapi_extra=_query_parameters("data"),
)
async def save_callback_authorization(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    encoded = str(request.query_params.get("data") or "").strip()
    return _response(await run_in_threadpool(_auth_service(request).save_callback_authorization, encoded))


@router.get(
    "/status",
    response_model=ExternalObjectResponse,
    responses={403: {"model": ExternalObjectResponse}, 500: {"model": ExternalObjectResponse}},
)
async def status(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(await run_in_threadpool(_auth_service(request).status))


@router.get(
    "/upload_monitor/config",
    response_model=ExternalObjectResponse,
    responses={403: {"model": ExternalObjectResponse}, 500: {"model": ExternalObjectResponse}},
)
async def get_upload_monitor_config(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    service = _upload_monitor_service(request)
    config, status_payload = await run_in_threadpool(lambda: (service.get(), service.status()))
    return fastapi_json_response({"success": True, "data": config, "status": status_payload})


@router.post(
    "/upload_monitor/config",
    response_model=ExternalObjectResponse,
    responses={
        400: {"model": ExternalObjectResponse},
        403: {"model": ExternalObjectResponse},
        500: {"model": ExternalObjectResponse},
    },
    openapi_extra=_UPLOAD_MONITOR_BODY,
)
async def save_upload_monitor_config(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload = await _silent_json_object(request)
    raw_mapping_value = payload.get("mappings")
    raw_mappings: list[Any] = raw_mapping_value if isinstance(raw_mapping_value, list) else []
    if len(raw_mappings) > 50:
        return fastapi_json_response({"success": False, "message": "上传监控目录最多配置 50 组"}, status_code=400)

    seen_paths: set[str] = set()
    for index, item in enumerate(raw_mappings, start=1):
        if not isinstance(item, Mapping):
            return fastapi_json_response(
                {"success": False, "message": f"第 {index} 组目录映射格式无效"},
                status_code=400,
            )
        local_dir = os.path.normpath(str(item.get("local_dir") or "").strip())
        target_cid = str(item.get("target_cid") or "").strip()
        if not local_dir or not os.path.isdir(local_dir):
            return fastapi_json_response(
                {"success": False, "message": f"第 {index} 组本地目录不存在或不可访问"},
                status_code=400,
            )
        if not target_cid or target_cid == "0":
            return fastapi_json_response(
                {"success": False, "message": f"第 {index} 组未选择有效的 115 目录"},
                status_code=400,
            )
        normalized_path = os.path.normcase(os.path.abspath(local_dir))
        if normalized_path in seen_paths:
            return fastapi_json_response(
                {"success": False, "message": f"本地目录重复配置: {local_dir}"},
                status_code=400,
            )
        seen_paths.add(normalized_path)

    if payload.get("enabled") and not raw_mappings:
        return fastapi_json_response(
            {"success": False, "message": "启用上传监控前至少配置一组目录映射"},
            status_code=400,
        )
    service = _upload_monitor_service(request)
    try:
        config, control = await run_in_threadpool(service.save, payload)
        status_payload = await run_in_threadpool(service.status)
    except Exception as exc:
        return fastapi_json_response({"success": False, "message": str(exc)}, status_code=500)
    return fastapi_json_response(
        {
            "success": True,
            "message": "上传监控配置已保存并生效",
            "data": config,
            "status": status_payload,
            "monitor_restarted": control.applied,
        }
    )


@router.get(
    "/dirs",
    response_model=ExternalObjectResponse,
    responses={403: {"model": ExternalObjectResponse}, 500: {"model": ExternalObjectResponse}},
    openapi_extra=_query_parameters("cid", "search"),
)
async def list_directories(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(
        await run_in_threadpool(
            _auth_service(request).list_directories,
            cid_value=request.query_params.get("cid", "0"),
            search=str(request.query_params.get("search") or ""),
        )
    )


@router.get(
    "/sorting_rules",
    response_model=ExternalArrayResponse,
    responses={403: {"model": ExternalObjectResponse}, 500: {"model": ExternalObjectResponse}},
)
async def list_sorting_rules(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(await run_in_threadpool(_sorting_rules_service(request).list_rules))


@router.post(
    "/sorting_rules",
    response_model=ExternalObjectResponse,
    responses={
        400: {"content": {"text/html": {"schema": {"type": "string"}}}},
        403: {"model": ExternalObjectResponse},
        415: {"content": {"text/html": {"schema": {"type": "string"}}}},
        500: {"model": ExternalObjectResponse},
    },
    openapi_extra=_JSON_VALUE_BODY,
)
async def save_sorting_rules(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, invalid = await _legacy_json_value(request)
    if invalid is not None:
        return invalid
    return _response(await run_in_threadpool(_sorting_rules_service(request).save_rules, payload))


@router.post(
    "/replace_strm",
    response_model=ExternalObjectResponse,
    responses={
        400: {
            "content": {
                "application/json": {"schema": ExternalObjectResponse.model_json_schema()},
                "text/html": {"schema": {"type": "string"}},
            }
        },
        403: {"model": ExternalObjectResponse},
        415: {"content": {"text/html": {"schema": {"type": "string"}}}},
        500: {
            "content": {
                "application/json": {"schema": ExternalObjectResponse.model_json_schema()},
                "text/html": {"schema": {"type": "string"}},
            }
        },
    },
    openapi_extra=_REPLACE_STRM_BODY,
)
async def replace_strm_files(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, invalid = await _legacy_json_object(request)
    if invalid is not None:
        return invalid
    assert payload is not None
    return _response(await run_in_threadpool(_strm_replace_service(request).replace, payload))


@router.get(
    "/rename_config",
    response_model=ExternalObjectResponse,
    responses={403: {"model": ExternalObjectResponse}, 500: {"model": ExternalObjectResponse}},
)
async def get_rename_config(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(await run_in_threadpool(_rename_config_service(request).get_config))


@router.post(
    "/rename_config",
    response_model=ExternalObjectResponse,
    responses={
        400: {"content": {"text/html": {"schema": {"type": "string"}}}},
        403: {"model": ExternalObjectResponse},
        415: {"content": {"text/html": {"schema": {"type": "string"}}}},
        500: {"model": ExternalObjectResponse},
    },
    openapi_extra=_JSON_VALUE_BODY,
)
async def save_rename_config(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, invalid = await _legacy_json_value(request)
    if invalid is not None:
        return invalid
    return _response(await run_in_threadpool(_rename_config_service(request).save_config, payload))


@router.post(
    "/rename_config/preview",
    response_model=ExternalObjectResponse,
    responses={
        400: {
            "content": {
                "application/json": {"schema": ExternalObjectResponse.model_json_schema()},
                "text/html": {"schema": {"type": "string"}},
            }
        },
        403: {"model": ExternalObjectResponse},
        415: {"content": {"text/html": {"schema": {"type": "string"}}}},
        500: {"model": ExternalObjectResponse},
    },
    openapi_extra=_JSON_VALUE_BODY,
)
async def preview_rename_config(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, invalid = await _legacy_json_value(request)
    if invalid is not None:
        return invalid
    return _response(await run_in_threadpool(_rename_config_service(request).preview_config, payload))


@router.get(
    "/episode_regex_rules",
    response_model=ExternalObjectResponse,
    responses={403: {"model": ExternalObjectResponse}, 500: {"model": ExternalObjectResponse}},
)
async def get_episode_regex_rules(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(await run_in_threadpool(_episode_regex_service(request).list_rules))


@router.post(
    "/episode_regex_rules",
    response_model=ExternalObjectResponse,
    responses={
        400: {
            "content": {
                "application/json": {"schema": ExternalObjectResponse.model_json_schema()},
                "text/html": {"schema": {"type": "string"}},
            }
        },
        403: {"model": ExternalObjectResponse},
        415: {"content": {"text/html": {"schema": {"type": "string"}}}},
        500: {
            "content": {
                "application/json": {"schema": ExternalObjectResponse.model_json_schema()},
                "text/html": {"schema": {"type": "string"}},
            }
        },
    },
    openapi_extra=_JSON_VALUE_BODY,
)
async def save_episode_regex_rules(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, invalid = await _legacy_json_value(request)
    if invalid is not None:
        return invalid
    if payload and not isinstance(payload, (list, Mapping)):
        return _html_error(InternalServerError())
    return _response(await run_in_threadpool(_episode_regex_service(request).save_rules, payload))


@router.post(
    "/organize/plans/dry-run",
    response_model=OrganizePlanEnvelopeResponse,
    responses={
        400: {
            "content": {
                "application/json": {"schema": OrganizePlanEnvelopeResponse.model_json_schema()},
                "text/html": {"schema": {"type": "string"}},
            }
        },
        403: {"model": ExternalObjectResponse},
        409: {"model": OrganizePlanEnvelopeResponse},
        415: {"content": {"text/html": {"schema": {"type": "string"}}}},
        500: {"model": ExternalObjectResponse},
    },
    openapi_extra=_ORGANIZE_PLAN_BODY,
)
async def create_organize_dry_run_plan(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, invalid = await _legacy_json_value(request)
    if invalid is not None:
        return invalid
    try:
        validated = OrganizePlanRequest.model_validate(payload)
    except ValidationError:
        return _response(P115Result({"success": False, "message": "整理计划请求格式错误"}, 400))
    normalized = validated.model_dump(exclude_none=True, exclude_unset=True)
    return _response(await run_in_threadpool(_organize_plan_service(request).create, normalized))


@router.get(
    "/organize/plans/{plan_id}",
    response_model=OrganizePlanEnvelopeResponse,
    responses={
        403: {"model": ExternalObjectResponse},
        404: {"model": OrganizePlanEnvelopeResponse},
        500: {"model": ExternalObjectResponse},
    },
)
async def get_organize_dry_run_plan(request: Request, plan_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(await run_in_threadpool(_organize_plan_service(request).get, plan_id))


@router.post(
    "/organize/plans/{plan_id}/execute",
    response_model=OrganizeExecutionEnvelopeResponse,
    responses={
        400: {"model": OrganizeExecutionEnvelopeResponse},
        202: {
            "model": OrganizeExecutionEnvelopeResponse,
            "headers": {"x-etk-job-id": {"schema": {"type": "string"}}},
        },
        403: {"model": ExternalObjectResponse},
        404: {"model": OrganizeExecutionEnvelopeResponse},
        409: {"model": OrganizeExecutionEnvelopeResponse},
        415: {"content": {"text/html": {"schema": {"type": "string"}}}},
        500: {"model": ExternalObjectResponse},
    },
    openapi_extra={**_path_parameter("plan_id"), **_ORGANIZE_EXECUTION_BODY},
)
async def execute_organize_plan(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    plan_id = str(request.path_params.get("plan_id") or "")
    payload, invalid = await _legacy_json_value(request)
    if invalid is not None:
        return invalid
    try:
        validated = OrganizeExecutionRequest.model_validate(payload)
    except ValidationError:
        return fastapi_json_response(
            {"success": False, "message": "整理执行请求格式错误"},
            status_code=400,
        )
    session = get_request_session(request)
    user_id = str(session.get("emby_user_id") or "").strip() or None
    trace_id = str(getattr(request.state, "trace_id", "") or "").strip() or None
    try:
        reservation = await run_in_threadpool(
            partial(
                _organize_execution_service(request).prepare,
                plan_id,
                validated.approved_execution_intent_id,
                requested_by_user_id=user_id,
                trace_id=trace_id,
            )
        )
    except (ValueError, P115ExecutionPreflightError) as exc:
        return fastapi_json_response({"success": False, "message": str(exc)}, status_code=400)
    except P115ExecutionNotFound as exc:
        return fastapi_json_response({"success": False, "message": str(exc)}, status_code=404)
    except (P115ExecutionBusy, P115ExecutionConflict, P115ExecutionStateError) as exc:
        return fastapi_json_response({"success": False, "message": str(exc)}, status_code=409)
    except Exception as exc:
        return fastapi_json_response({"success": False, "message": str(exc)}, status_code=500)
    return fastapi_json_response(
        {"success": True, "data": reservation.execution.to_dict()},
        status_code=202 if reservation.created else 200,
        headers={"x-etk-job-id": str(reservation.execution.job_id)},
    )


@router.get(
    "/organize/executions/{execution_id}",
    response_model=OrganizeExecutionEnvelopeResponse,
    responses={
        403: {"model": ExternalObjectResponse},
        404: {"model": OrganizeExecutionEnvelopeResponse},
        500: {"model": ExternalObjectResponse},
    },
    openapi_extra=_path_parameter("execution_id"),
)
async def get_organize_execution(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    parsed = _uuid(str(request.path_params.get("execution_id") or ""))
    if parsed is None:
        return fastapi_json_response({"success": False, "message": "整理执行不存在"}, status_code=404)
    try:
        execution = await run_in_threadpool(_organize_execution_service(request).get, parsed)
    except Exception as exc:
        return fastapi_json_response({"success": False, "message": str(exc)}, status_code=500)
    if execution is None:
        return fastapi_json_response({"success": False, "message": "整理执行不存在"}, status_code=404)
    return fastapi_json_response({"success": True, "data": execution.to_dict()})


@router.post(
    "/organize/executions/{execution_id}/reconcile",
    response_model=OrganizeExecutionEnvelopeResponse,
    responses={
        400: {"model": OrganizeExecutionEnvelopeResponse},
        403: {"model": ExternalObjectResponse},
        404: {"model": OrganizeExecutionEnvelopeResponse},
        409: {"model": OrganizeExecutionEnvelopeResponse},
        415: {"content": {"text/html": {"schema": {"type": "string"}}}},
        500: {"model": ExternalObjectResponse},
    },
    openapi_extra={**_path_parameter("execution_id"), **_ORGANIZE_RECONCILIATION_BODY},
)
async def reconcile_organize_execution(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    parsed = _uuid(str(request.path_params.get("execution_id") or ""))
    if parsed is None:
        return fastapi_json_response({"success": False, "message": "整理执行不存在"}, status_code=404)
    payload, invalid = await _legacy_json_value(request)
    if invalid is not None:
        return invalid
    try:
        validated = OrganizeReconciliationRequest.model_validate(payload)
    except ValidationError:
        return fastapi_json_response(
            {"success": False, "message": "整理对账请求格式错误"},
            status_code=400,
        )
    actor = str(get_request_session(request).get("emby_user_id") or "").strip() or None
    try:
        result = await run_in_threadpool(
            partial(
                _organize_execution_service(request).reconcile,
                parsed,
                validated.approved_reconciliation_intent_id,
                actor=actor,
            )
        )
    except ValueError as exc:
        return fastapi_json_response({"success": False, "message": str(exc)}, status_code=400)
    except P115ExecutionNotFound as exc:
        return fastapi_json_response({"success": False, "message": str(exc)}, status_code=404)
    except (
        P115ExecutionBusy,
        P115ExecutionConflict,
        P115ExecutionPreflightError,
        P115ExecutionStateError,
    ) as exc:
        return fastapi_json_response({"success": False, "message": str(exc)}, status_code=409)
    except Exception as exc:
        return fastapi_json_response({"success": False, "message": str(exc)}, status_code=500)
    return fastapi_json_response({"success": True, "data": result.to_dict()})


@router.get(
    "/records",
    response_model=OrganizeRecordsResponse,
    responses={403: {"model": ExternalObjectResponse}, 500: {"model": OrganizeRecordsResponse}},
    openapi_extra=_query_parameters("page", "per_page", "search", "status", "cid", "fail_reason"),
)
async def get_organize_records(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    result = await run_in_threadpool(
        _organize_records_service(request).list_records,
        dict(request.query_params),
    )
    return _response(result)


@router.delete(
    "/records/{record_id}",
    response_model=OrganizeRecordsResponse,
    responses={
        403: {"model": ExternalObjectResponse},
        404: {"model": OrganizeRecordsResponse},
        500: {"model": OrganizeRecordsResponse},
    },
    openapi_extra=_path_parameter("record_id", schema_type="integer"),
)
async def delete_organize_record(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    raw_record_id = str(request.path_params.get("record_id") or "")
    if not raw_record_id.isdigit():
        return fastapi_json_response({"success": False, "message": "整理记录不存在"}, status_code=404)
    return _response(
        await run_in_threadpool(
            _organize_records_service(request).delete_record,
            int(raw_record_id),
        )
    )


@router.get(
    "/mediainfo/{pick_code}",
    response_model=ExternalObjectResponse,
    responses={
        400: {"model": ExternalObjectResponse},
        404: {"model": ExternalObjectResponse},
        422: {"model": ExternalObjectResponse},
    },
)
async def get_cached_mediainfo_by_pick_code(request: Request, pick_code: str) -> Response:
    try:
        payload = await run_in_threadpool(_media_info_service(request).formatted_by_pick_code, pick_code)
    except P115MediaInfoInvalidIdentity:
        return fastapi_json_response({"error": "invalid sha1"}, status_code=400)
    except P115MediaInfoNotFound:
        return fastapi_json_response({"error": "media info cache not found"}, status_code=404)
    except P115MediaInfoInvalidCache:
        return fastapi_json_response({"error": "invalid media info cache"}, status_code=422)
    return fastapi_json_response(payload, headers={"Cache-Control": "no-store"})


@router.get(
    "/mediainfo/sha1/{sha1}",
    response_model=ExternalObjectResponse,
    responses={
        400: {"model": ExternalObjectResponse},
        404: {"model": ExternalObjectResponse},
        422: {"model": ExternalObjectResponse},
    },
)
async def get_cached_mediainfo_by_sha1(request: Request, sha1: str) -> Response:
    try:
        payload = await run_in_threadpool(_media_info_service(request).formatted_by_sha1, sha1)
    except P115MediaInfoInvalidIdentity:
        return fastapi_json_response({"error": "invalid sha1"}, status_code=400)
    except P115MediaInfoNotFound:
        return fastapi_json_response({"error": "media info cache not found"}, status_code=404)
    except P115MediaInfoInvalidCache:
        return fastapi_json_response({"error": "invalid media info cache"}, status_code=422)
    return fastapi_json_response(payload, headers={"Cache-Control": "no-store"})


@router.get(
    "/mediainfo/{pick_code}/metadata",
    response_model=ExternalObjectResponse,
    responses={400: {"model": ExternalObjectResponse}, 404: {"model": ExternalObjectResponse}},
    openapi_extra=_path_and_query_parameters("pick_code", "item_type", "season_number", "episode_number"),
)
async def get_cached_metadata_by_pick_code(request: Request) -> Response:
    pick_code = str(request.path_params.get("pick_code") or "")
    try:
        identity = await run_in_threadpool(_media_info_service(request).metadata_identity_by_pick_code, pick_code)
    except P115MediaInfoInvalidIdentity:
        return fastapi_json_response({"error": "invalid sha1"}, status_code=400)
    except P115MediaInfoNotFound:
        return fastapi_json_response({"error": "metadata identity not found"}, status_code=404)
    return await _cached_metadata_response(request, identity)


@router.get(
    "/mediainfo/sha1/{sha1}/metadata",
    response_model=ExternalObjectResponse,
    responses={400: {"model": ExternalObjectResponse}, 404: {"model": ExternalObjectResponse}},
    openapi_extra=_path_and_query_parameters("sha1", "item_type", "season_number", "episode_number"),
)
async def get_cached_metadata_by_sha1(request: Request) -> Response:
    sha1 = str(request.path_params.get("sha1") or "")
    try:
        identity = await run_in_threadpool(_media_info_service(request).metadata_identity_by_sha1, sha1)
    except P115MediaInfoInvalidIdentity:
        return fastapi_json_response({"error": "invalid sha1"}, status_code=400)
    except P115MediaInfoNotFound:
        return fastapi_json_response({"error": "metadata identity not found"}, status_code=404)
    return await _cached_metadata_response(request, identity)


@router.post(
    "/mediainfo/{pick_code}/metadata/backfill",
    response_model=ExternalObjectResponse,
    responses={
        202: {"model": ExternalObjectResponse},
        404: {"model": ExternalObjectResponse},
        409: {"model": ExternalObjectResponse},
    },
    openapi_extra=_path_parameter("pick_code"),
)
async def trigger_metadata_backfill_by_pick_code(request: Request) -> Response:
    pick_code = str(request.path_params.get("pick_code") or "")
    try:
        await run_in_threadpool(_media_info_service(request).sha1_by_pick_code, pick_code)
    except (P115MediaInfoInvalidIdentity, P115MediaInfoNotFound):
        return fastapi_json_response({"error": "metadata identity not found"}, status_code=404)
    result = await run_in_threadpool(_media_service(request).backfill_all)
    return _media_service_response(result)


@router.post(
    "/mediainfo/sha1/{sha1}/metadata/backfill",
    response_model=ExternalObjectResponse,
    responses={
        202: {"model": ExternalObjectResponse},
        404: {"model": ExternalObjectResponse},
        409: {"model": ExternalObjectResponse},
    },
    openapi_extra=_path_parameter("sha1"),
)
async def trigger_metadata_backfill_by_sha1(request: Request) -> Response:
    sha1 = str(request.path_params.get("sha1") or "")
    try:
        exists = await run_in_threadpool(_media_info_service(request).has_formatted_by_sha1, sha1)
    except P115MediaInfoInvalidIdentity:
        exists = False
    if not exists:
        return fastapi_json_response({"error": "metadata identity not found"}, status_code=404)
    result = await run_in_threadpool(_media_service(request).backfill_all)
    return _media_service_response(result)


@router.post(
    "/mediainfo/{pick_code}/item-ready",
    response_model=ExternalObjectResponse,
    responses={
        404: {"model": ExternalObjectResponse},
        409: {"model": ExternalObjectResponse},
        503: {"model": ExternalObjectResponse},
    },
    openapi_extra=_path_and_query_parameters("pick_code", "emby_item_id"),
)
async def accept_emby_item_by_pick_code(request: Request) -> Response:
    pick_code = str(request.path_params.get("pick_code") or "")
    result = await run_in_threadpool(
        _item_ready_service(request).accept_by_pick_code,
        pick_code,
        request.query_params.get("emby_item_id"),
    )
    return _media_service_response(result)


@router.post(
    "/mediainfo/sha1/{sha1}/item-ready",
    response_model=ExternalObjectResponse,
    responses={
        404: {"model": ExternalObjectResponse},
        409: {"model": ExternalObjectResponse},
        503: {"model": ExternalObjectResponse},
    },
    openapi_extra=_path_and_query_parameters("sha1", "emby_item_id"),
)
async def accept_emby_item_by_sha1(request: Request) -> Response:
    sha1 = str(request.path_params.get("sha1") or "")
    result = await run_in_threadpool(
        _item_ready_service(request).accept_by_sha1,
        sha1,
        request.query_params.get("emby_item_id"),
    )
    return _media_service_response(result)


@router.post(
    "/mediainfo/{pick_code}/intro-sync",
    response_model=ExternalObjectResponse,
    responses={404: {"model": ExternalObjectResponse}},
    openapi_extra=_path_and_query_parameters("pick_code", "emby_item_id"),
)
async def sync_intro_by_pick_code(request: Request) -> Response:
    pick_code = str(request.path_params.get("pick_code") or "")
    try:
        payload = await run_in_threadpool(
            _intro_sync_service(request).sync_by_pick_code,
            pick_code,
            request.query_params.get("emby_item_id"),
        )
    except P115IntroSyncNotFound:
        return fastapi_json_response({"error": "media info cache not found"}, status_code=404)
    return fastapi_json_response(payload)


@router.post(
    "/mediainfo/sha1/{sha1}/intro-sync",
    response_model=ExternalObjectResponse,
    openapi_extra=_path_and_query_parameters("sha1", "emby_item_id"),
)
async def sync_intro_by_sha1(request: Request) -> Response:
    sha1 = str(request.path_params.get("sha1") or "")
    payload = await run_in_threadpool(
        _intro_sync_service(request).sync_by_sha1,
        sha1,
        request.query_params.get("emby_item_id"),
    )
    return fastapi_json_response(payload)


@router.post(
    "/mediainfo/{pick_code}/deep-delete/prepare",
    response_model=ExternalObjectResponse,
    responses={
        400: {"model": ExternalObjectResponse},
        404: {"model": ExternalObjectResponse},
        409: {"model": ExternalObjectResponse},
    },
    openapi_extra={**_path_parameter("pick_code"), **_DEEP_DELETE_PREPARE_BODY},
)
async def prepare_deep_delete_by_pick_code(request: Request) -> Response:
    pick_code = str(request.path_params.get("pick_code") or "")
    payload = await _silent_json_object(request)
    result = await run_in_threadpool(
        _deep_delete_service(request).prepare_by_pick_code,
        pick_code,
        payload,
    )
    return _media_service_response(result)


@router.post(
    "/mediainfo/sha1/{sha1}/deep-delete/prepare",
    response_model=ExternalObjectResponse,
    responses={
        400: {"model": ExternalObjectResponse},
        404: {"model": ExternalObjectResponse},
        409: {"model": ExternalObjectResponse},
    },
    openapi_extra={**_path_parameter("sha1"), **_DEEP_DELETE_PREPARE_BODY},
)
async def prepare_deep_delete_by_sha1(request: Request) -> Response:
    sha1 = str(request.path_params.get("sha1") or "")
    payload = await _silent_json_object(request)
    result = await run_in_threadpool(
        _deep_delete_service(request).prepare_by_sha1,
        sha1,
        payload,
    )
    return _media_service_response(result)


@router.post(
    "/mediainfo/{pick_code}/deep-delete/commit",
    status_code=202,
    response_model=ExternalObjectResponse,
    responses={
        202: {"model": ExternalObjectResponse},
        400: {"model": ExternalObjectResponse},
        404: {"model": ExternalObjectResponse},
        410: {"model": ExternalObjectResponse},
        500: {"model": ExternalObjectResponse},
    },
    openapi_extra={**_path_parameter("pick_code"), **_DEEP_DELETE_COMMIT_BODY},
)
async def commit_deep_delete_by_pick_code(request: Request) -> Response:
    pick_code = str(request.path_params.get("pick_code") or "")
    payload = await _silent_json_object(request)
    result = await run_in_threadpool(
        _deep_delete_service(request).commit_by_pick_code,
        pick_code,
        payload,
    )
    return _media_service_response(result)


@router.post(
    "/mediainfo/sha1/{sha1}/deep-delete/commit",
    status_code=202,
    response_model=ExternalObjectResponse,
    responses={
        202: {"model": ExternalObjectResponse},
        400: {"model": ExternalObjectResponse},
        404: {"model": ExternalObjectResponse},
        410: {"model": ExternalObjectResponse},
        500: {"model": ExternalObjectResponse},
    },
    openapi_extra={**_path_parameter("sha1"), **_DEEP_DELETE_COMMIT_BODY},
)
async def commit_deep_delete_by_sha1(request: Request) -> Response:
    sha1 = str(request.path_params.get("sha1") or "")
    payload = await _silent_json_object(request)
    result = await run_in_threadpool(
        _deep_delete_service(request).commit_by_sha1,
        sha1,
        payload,
    )
    return _media_service_response(result)


async def _cached_metadata_response(request: Request, identity: P115MediaInfoIdentity) -> Response:
    default_type = MediaItemType.MOVIE if identity.media_type == "movie" else MediaItemType.SERIES
    requested_type = MediaItemType.parse(request.query_params.get("item_type") or default_type.value)
    if requested_type is None:
        return fastapi_json_response({"error": "invalid item_type"}, status_code=400)
    season_number = (
        _query_integer(request, "season_number", identity.season_number)
        if requested_type in {MediaItemType.SEASON, MediaItemType.EPISODE}
        else None
    )
    episode_number = (
        _query_integer(request, "episode_number", identity.episode_number)
        if requested_type is MediaItemType.EPISODE
        else None
    )
    try:
        media_identity = MediaIdentity(
            tmdb_id=identity.tmdb_id,
            media_type=identity.media_type,
            item_type=requested_type,
            season_number=season_number,
            episode_number=episode_number,
        )
    except ValueError:
        return fastapi_json_response({"error": "metadata cache not found"}, status_code=404)
    result = await run_in_threadpool(
        partial(
            _media_service(request).provide_bridge_metadata_identity,
            media_identity,
            requested_type,
            base_url=str(request.base_url),
        )
    )
    return _media_service_response(result)


def _query_integer(request: Request, name: str, fallback: int | None) -> int | None:
    value: object = request.query_params.get(name)
    if value in (None, ""):
        value = fallback
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _media_service_response(result: ServiceResult) -> Response:
    if not isinstance(result, ServiceResult):
        raise TypeError("media service returned an invalid result")
    return fastapi_json_response(
        dict(result.payload),
        status_code=result.status_code,
        headers=dict(result.headers or {}),
    )


def _uuid(value: str) -> UUID | None:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


__all__ = ["router"]
