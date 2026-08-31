from __future__ import annotations

import asyncio
import json
import os
import secrets
import time
from collections.abc import AsyncIterator, Mapping
from functools import partial
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, RootModel, ValidationError
from starlette.concurrency import run_in_threadpool
from starlette.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from werkzeug.exceptions import BadRequest, InternalServerError, UnsupportedMediaType

from etk.compatibility.response_contract import fastapi_json_response
from etk.domains.media.models import MediaIdentity, MediaItemType, ServiceResult
from etk.domains.operations.jobs import JobSubmissionService, TaskBusyError
from etk.platform.jobs import IdempotencyConflictError, InvalidJsonPayloadError, JobProjection, JobStatus
from etk.platform.web import PermissionPolicy, get_request_session

from .auth_directory import P115AuthDirectoryService
from .deep_delete import P115DeepDeleteService
from .episode_regex import P115EpisodeRegexService
from .intro_sync import P115IntroSyncNotFound, P115IntroSyncService
from .item_ready import P115ItemReadyService
from .local_config_service import P115LocalSettingsService
from .local_directories import P115LocalDirectoryService
from .maintenance import P115MaintenanceIntentService, P115SpeedtestService
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
from .quick_deploy import P115QuickDeployIntentService
from .rename_config import P115RenameConfigService
from .rename_moviepilot import P115RenameMoviePilotService
from .sorting_rules import P115SortingRulesService
from .strm_sync import P115StrmReplaceService
from .upload_monitor import P115UploadMonitorConfigService
from .washing_priority import P115WashingPriorityService

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


def _rename_moviepilot_service(request: Request) -> P115RenameMoviePilotService:
    direct = getattr(request.app.state, "p115_rename_moviepilot_service", None)
    if isinstance(direct, P115RenameMoviePilotService):
        return direct
    value = request.app.state.container.use_case("p115.rename-moviepilot")
    if not isinstance(value, P115RenameMoviePilotService):
        raise TypeError("p115.rename-moviepilot provider returned an invalid service")
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


def _local_directory_service(request: Request) -> P115LocalDirectoryService:
    direct = getattr(request.app.state, "p115_local_directory_service", None)
    if isinstance(direct, P115LocalDirectoryService):
        return direct
    value = request.app.state.container.use_case("p115.local-directories")
    if not isinstance(value, P115LocalDirectoryService):
        raise TypeError("p115.local-directories provider returned an invalid service")
    return value


def _washing_priority_service(request: Request) -> P115WashingPriorityService:
    direct = getattr(request.app.state, "p115_washing_priority_service", None)
    if isinstance(direct, P115WashingPriorityService):
        return direct
    value = request.app.state.container.use_case("p115.washing-priority")
    if not isinstance(value, P115WashingPriorityService):
        raise TypeError("p115.washing-priority provider returned an invalid service")
    return value


def _job_submission_service(request: Request) -> JobSubmissionService:
    value = request.app.state.container.use_case("operations.jobs")
    if not isinstance(value, JobSubmissionService):
        raise TypeError("operations.jobs provider returned an invalid service")
    return value


def _maintenance_intents(request: Request) -> P115MaintenanceIntentService:
    direct = getattr(request.app.state, "p115_maintenance_intent_service", None)
    if isinstance(direct, P115MaintenanceIntentService):
        return direct
    value = request.app.state.container.use_case("p115.maintenance-intents")
    if not isinstance(value, P115MaintenanceIntentService):
        raise TypeError("p115.maintenance-intents provider returned an invalid service")
    return value


def _speedtest_service(request: Request) -> P115SpeedtestService:
    direct = getattr(request.app.state, "p115_speedtest_service", None)
    if isinstance(direct, P115SpeedtestService):
        return direct
    value = request.app.state.container.use_case("p115.speedtest")
    if not isinstance(value, P115SpeedtestService):
        raise TypeError("p115.speedtest provider returned an invalid service")
    return value


def _quick_deploy_intent(request: Request) -> P115QuickDeployIntentService:
    direct = getattr(request.app.state, "p115_quick_deploy_intent_service", None)
    if isinstance(direct, P115QuickDeployIntentService):
        return direct
    value = request.app.state.container.use_case("p115.quick-deploy-intent")
    if not isinstance(value, P115QuickDeployIntentService):
        raise TypeError("p115.quick-deploy-intent provider returned an invalid service")
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


def _legacy_unsupported_media_type() -> UnsupportedMediaType:
    return UnsupportedMediaType(
        description=("Did not attempt to load JSON data because the request Content-Type was not 'application/json'.")
    )


def _idempotency_key(request: Request) -> str | None:
    value = str(request.headers.get("idempotency-key") or "").strip()
    return value or None


async def _legacy_json_object(request: Request) -> tuple[dict[str, Any] | None, Response | None]:
    if not _is_json_content_type(request.headers.get("content-type", "")):
        return None, _html_error(_legacy_unsupported_media_type())
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, _html_error(BadRequest())
    if not isinstance(payload, Mapping):
        return None, _html_error(InternalServerError())
    return dict(payload), None


async def _legacy_json_value(request: Request) -> tuple[object | None, Response | None]:
    if not _is_json_content_type(request.headers.get("content-type", "")):
        return None, _html_error(_legacy_unsupported_media_type())
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


@router.get(
    "/washing_priority_config",
    response_model=ExternalObjectResponse,
    responses={403: {"model": ExternalObjectResponse}, 500: {"model": ExternalObjectResponse}},
)
async def get_washing_priority_config(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(await run_in_threadpool(_washing_priority_service(request).get_config))


@router.post(
    "/washing_priority_config",
    response_model=ExternalObjectResponse,
    responses={403: {"model": ExternalObjectResponse}, 500: {"model": ExternalObjectResponse}},
    openapi_extra=_JSON_VALUE_BODY,
)
async def save_washing_priority_config(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        if not _is_json_content_type(request.headers.get("content-type", "")):
            raise _legacy_unsupported_media_type()
        try:
            raw_payload = await request.json()
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise BadRequest() from exc
    except (BadRequest, UnsupportedMediaType) as exc:
        return fastapi_json_response(
            {"success": False, "message": str(exc)},
            status_code=500,
        )
    payload = dict(raw_payload) if isinstance(raw_payload, Mapping) else {}
    return _response(
        await run_in_threadpool(
            _washing_priority_service(request).save_config,
            payload,
        )
    )


@router.get(
    "/washing_priority_groups",
    response_model=ExternalObjectResponse,
    responses={403: {"model": ExternalObjectResponse}, 500: {"model": ExternalObjectResponse}},
)
async def get_washing_priority_groups(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(await run_in_threadpool(_washing_priority_service(request).list_groups))


@router.post(
    "/washing_priority_groups",
    response_model=ExternalObjectResponse,
    responses={
        400: {
            "content": {
                "application/json": {"schema": {"$ref": "#/components/schemas/ExternalObjectResponse"}},
                "text/html": {"schema": {"type": "string"}},
            }
        },
        403: {"model": ExternalObjectResponse},
        415: {"content": {"text/html": {"schema": {"type": "string"}}}},
        500: {"model": ExternalObjectResponse},
    },
    openapi_extra=_JSON_VALUE_BODY,
)
async def save_washing_priority_groups(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    raw_payload, error = await _legacy_json_value(request)
    if error is not None:
        return error
    return _response(
        await run_in_threadpool(
            _washing_priority_service(request).replace_groups,
            raw_payload,
        )
    )


@router.post(
    "/washing_priority_recalculate",
    response_model=ExternalObjectResponse,
    responses={
        200: {"model": ExternalObjectResponse},
        202: {"model": ExternalObjectResponse},
        400: {"content": {"text/html": {"schema": {"type": "string"}}}},
        403: {"model": ExternalObjectResponse},
        409: {"model": ExternalObjectResponse},
        415: {"content": {"text/html": {"schema": {"type": "string"}}}},
        500: {
            "content": {
                "application/json": {"schema": {"$ref": "#/components/schemas/ExternalObjectResponse"}},
                "text/html": {"schema": {"type": "string"}},
            }
        },
    },
    openapi_extra=_JSON_VALUE_BODY,
)
async def recalculate_washing_priorities(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    raw_payload, error = await _legacy_json_value(request)
    if error is not None:
        return error
    if raw_payload is None:
        payload: dict[str, Any] = {}
    elif isinstance(raw_payload, Mapping):
        payload = dict(raw_payload)
    else:
        return _html_error(InternalServerError())

    item_type = payload.get("item_type") or "all"
    limit = payload.get("limit")
    force_full = payload.get("full") is True
    idempotency_key = _idempotency_key(request)
    idempotency_scope = "api.p115.washing_priority_recalculate" if idempotency_key else None
    affected_scope: object = None
    scope_revision: object = None
    if not force_full:
        try:
            pending_state = await run_in_threadpool(_washing_priority_service(request).load_recalculate_state)
        except Exception as exc:
            return fastapi_json_response(
                {"success": False, "message": str(exc)},
                status_code=500,
            )
        affected_scope = pending_state.get("media_types") or {}
        scope_revision = pending_state.get("revision")
        if not affected_scope:
            if idempotency_key is not None and idempotency_scope is not None:
                try:
                    existing = await run_in_threadpool(
                        partial(
                            _job_submission_service(request).get_idempotent,
                            scope=idempotency_scope,
                            key=idempotency_key,
                        )
                    )
                except Exception as exc:
                    return fastapi_json_response(
                        {"success": False, "message": str(exc)},
                        status_code=500,
                    )
                if existing is not None:
                    if not _same_washing_recalculation_intent(existing, item_type=item_type, limit=limit):
                        return fastapi_json_response(
                            {
                                "success": False,
                                "message": "幂等键已用于不同的洗版优先级重算请求。",
                            },
                            status_code=409,
                        )
                    job_id = str(existing.job.id)
                    return _washing_recalculation_submission_response(job_id, reused=True)
            return fastapi_json_response(
                {
                    "success": True,
                    "message": "当前没有受规则变更影响的媒体项",
                    "data": {"scanned_items": 0, "updated_items": 0},
                }
            )

    user_id = str(get_request_session(request).get("emby_user_id") or "").strip() or None
    try:
        submitted = await run_in_threadpool(
            partial(
                _job_submission_service(request).submit_task,
                "recalculate_library_washing_priorities",
                {
                    "item_type": item_type,
                    "limit": limit,
                    "affected_scope": affected_scope,
                    "scope_revision": scope_revision,
                },
                trigger_type="manual",
                requested_by_user_id=user_id,
                idempotency_key=idempotency_key,
                idempotency_scope=idempotency_scope,
                trace_id=str(getattr(request.state, "trace_id", "") or "").strip() or None,
                # The store resolves replay before its atomic serial-queue busy constraint.
                reject_when_busy=False,
            )
        )
    except (TaskBusyError, IdempotencyConflictError) as exc:
        message = (
            "幂等键已用于不同的洗版优先级重算请求。"
            if isinstance(exc, IdempotencyConflictError)
            else "后台有任务正在运行，请稍后再试。"
        )
        return fastapi_json_response({"success": False, "message": message}, status_code=409)
    except (InvalidJsonPayloadError, TypeError, ValueError) as exc:
        return fastapi_json_response({"success": False, "message": str(exc)}, status_code=500)
    except Exception as exc:
        return fastapi_json_response({"success": False, "message": str(exc)}, status_code=500)

    job_id = str(submitted.projection.job.id)
    return _washing_recalculation_submission_response(
        job_id,
        reused=submitted.reused,
    )


def _washing_recalculation_submission_response(job_id: str, *, reused: bool) -> Response:
    return fastapi_json_response(
        {
            "success": True,
            "message": "受影响媒体项的优先级重算任务已在后台启动",
            "data": {"job_id": job_id, "reused": reused},
        },
        status_code=200 if reused else 202,
        headers={"x-etk-job-id": job_id},
    )


def _same_washing_recalculation_intent(
    projection: object,
    *,
    item_type: object,
    limit: object,
) -> bool:
    job = getattr(projection, "job", None)
    if str(getattr(job, "task_key", "") or "") != "recalculate_library_washing_priorities":
        return False
    request_payload = getattr(job, "request_payload", None)
    if not isinstance(request_payload, Mapping):
        return False
    parameters = request_payload.get("parameters")
    if not isinstance(parameters, Mapping):
        return False
    # affected_scope and scope_revision are derived from mutable pending state;
    # compare only the caller-controlled intent when replaying after ack.
    return (
        parameters.get("affected_scope") is not None
        and parameters.get("item_type") == item_type
        and parameters.get("limit") == limit
    )


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


@router.get(
    "/system/directories",
    response_model=ExternalObjectResponse,
    responses={
        403: {"model": ExternalObjectResponse},
        404: {"model": ExternalObjectResponse},
        500: {"model": ExternalObjectResponse},
    },
    openapi_extra=_query_parameters("path"),
)
async def get_local_directories(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    path = str(request.query_params.get("path") or "").strip()
    return _response(
        await run_in_threadpool(
            _local_directory_service(request).browse,
            path,
        )
    )


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
    "/speedtest",
    response_model=ExternalObjectResponse,
    responses={
        400: {"model": ExternalObjectResponse},
        403: {"model": ExternalObjectResponse},
        500: {"model": ExternalObjectResponse},
    },
    openapi_extra=_query_parameters("max_mb", "seconds"),
)
async def speedtest_115_download(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        result = await run_in_threadpool(
            partial(
                _speedtest_service(request).run,
                max_mb=request.query_params.get("max_mb"),
                seconds=request.query_params.get("seconds"),
                user_agent=request.headers.get("user-agent"),
            )
        )
    except Exception as exc:
        return fastapi_json_response({"status": "error", "message": str(exc)}, status_code=500)
    return _response(result)


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


@router.post(
    "/mkdir",
    response_model=ExternalObjectResponse,
    responses={
        400: {
            "content": {
                "application/json": {"schema": ExternalObjectResponse.model_json_schema()},
                "text/html": {"schema": {"type": "string"}},
            }
        },
        403: {"model": ExternalObjectResponse},
        409: {"model": ExternalObjectResponse},
        415: {"content": {"text/html": {"schema": {"type": "string"}}}},
        500: {"model": ExternalObjectResponse},
        503: {"model": ExternalObjectResponse},
    },
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "pid": {"type": "string"},
                            "cid": {"type": "string"},
                            "name": {"type": "string"},
                        },
                    }
                }
            },
        }
    },
)
async def create_115_directory(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _legacy_json_object(request)
    if error is not None:
        return error
    assert payload is not None
    try:
        parameters = await run_in_threadpool(
            _maintenance_intents(request).mkdir_parameters,
            payload.get("pid") or payload.get("cid"),
            payload.get("name"),
        )
    except ValueError as exc:
        return fastapi_json_response({"status": "error", "message": str(exc)}, status_code=400)
    submitted = await _submit_p115_native_operation(
        request,
        "p115.mkdir",
        parameters,
        display_name="创建 115 目录",
        timeout_seconds=60,
        idempotency_scope="api.p115.mkdir",
    )
    if isinstance(submitted, Response):
        return submitted
    projection = await _await_p115_operation(_job_submission_service(request), submitted.job.id, timeout_seconds=65)
    if projection is None:
        return fastapi_json_response(
            {
                "status": "error",
                "operation_status": "indeterminate",
                "message": "创建 115 目录任务未在限定时间内完成，请通过 job_id 查询最终状态",
                "job_id": str(submitted.job.id),
            },
            status_code=503,
        )
    if projection.job.status is not JobStatus.SUCCEEDED:
        return _native_operation_failure(projection, legacy_status_key=True)
    result = dict(projection.job.result or {})
    provider_response = result.get("provider_response")
    if not isinstance(provider_response, Mapping):
        return fastapi_json_response(
            {"status": "error", "message": "创建 115 目录任务返回了无效结果"},
            status_code=500,
        )
    return fastapi_json_response({"status": "success", "data": dict(provider_response)})


@router.post(
    "/recycle_bin/empty",
    response_model=ExternalObjectResponse,
    responses={
        400: {"model": ExternalObjectResponse},
        403: {"model": ExternalObjectResponse},
        409: {"model": ExternalObjectResponse},
        500: {"model": ExternalObjectResponse},
        503: {"model": ExternalObjectResponse},
    },
)
async def empty_recycle_bin(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        parameters = await run_in_threadpool(_maintenance_intents(request).recycle_bin_parameters)
    except ValueError as exc:
        return fastapi_json_response({"success": False, "message": str(exc)}, status_code=500)
    submitted = await _submit_p115_native_operation(
        request,
        "p115.recycle-bin-empty",
        parameters,
        display_name="清空 115 回收站",
        timeout_seconds=90,
        idempotency_scope="api.p115.recycle-bin-empty",
    )
    if isinstance(submitted, Response):
        return submitted
    projection = await _await_p115_operation(_job_submission_service(request), submitted.job.id, timeout_seconds=95)
    if projection is None:
        return fastapi_json_response(
            {
                "success": False,
                "operation_status": "indeterminate",
                "message": "清空 115 回收站任务未在限定时间内完成，请通过 job_id 查询最终状态",
                "job_id": str(submitted.job.id),
            },
            status_code=503,
        )
    if projection.job.status is not JobStatus.SUCCEEDED:
        return _native_operation_failure(projection)
    return fastapi_json_response({"success": True, "message": "回收站已彻底清空！"})


@router.post(
    "/unrecognized/empty",
    response_model=ExternalObjectResponse,
    responses={
        400: {"model": ExternalObjectResponse},
        403: {"model": ExternalObjectResponse},
        409: {"model": ExternalObjectResponse},
        503: {"model": ExternalObjectResponse},
    },
)
async def empty_unrecognized_files(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        parameters = await run_in_threadpool(
            _maintenance_intents(request).unrecognized_parameters,
            request.app.state.container.settings.p115_unrecognized_cid,
        )
    except ValueError as exc:
        status_code = 200 if "未配置未识别目录" in str(exc) else 400
        return fastapi_json_response({"success": False, "message": str(exc)}, status_code=status_code)
    submitted = await _submit_p115_native_operation(
        request,
        "p115.empty-unrecognized",
        parameters,
        display_name="清空 115 未识别目录",
        timeout_seconds=1800,
        idempotency_scope="api.p115.empty-unrecognized",
    )
    if isinstance(submitted, Response):
        return submitted
    job_id = str(submitted.job.id)
    return fastapi_json_response(
        {
            "success": True,
            "message": "未识别目录清空任务已提交",
            "job_id": job_id,
            "data": {"accepted": True, "job_id": job_id},
        }
    )


@router.post(
    "/quick_deploy",
    response_model=ExternalObjectResponse,
    responses={
        200: {
            "content": {
                "application/json": {"schema": ExternalObjectResponse.model_json_schema()},
                "application/x-ndjson": {"schema": {"type": "string"}},
            }
        },
        400: {"model": ExternalObjectResponse},
        403: {"model": ExternalObjectResponse},
        409: {"model": ExternalObjectResponse},
        500: {"model": ExternalObjectResponse},
        503: {"model": ExternalObjectResponse},
    },
    openapi_extra=_query_parameters("stream"),
)
async def quick_deploy_115(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        parameters = await run_in_threadpool(_quick_deploy_intent(request).parameters)
    except (RuntimeError, ValueError) as exc:
        return fastapi_json_response({"success": False, "message": str(exc)}, status_code=400)
    submitted = await _submit_p115_native_operation(
        request,
        "p115.quick-deploy",
        parameters,
        display_name="一键部署 115 基础配置",
        timeout_seconds=1800,
        idempotency_scope="api.p115.quick-deploy",
    )
    if isinstance(submitted, Response):
        return submitted
    jobs = _job_submission_service(request)
    if request.query_params.get("stream") == "1":
        return StreamingResponse(
            _quick_deploy_events(request, jobs, submitted),
            media_type="application/x-ndjson",
            headers={
                "cache-control": "no-cache",
                "x-accel-buffering": "no",
                "x-etk-job-id": str(submitted.job.id),
            },
        )

    completed = await _await_p115_operation(jobs, submitted.job.id, timeout_seconds=1805)
    if completed is None:
        return fastapi_json_response(
            {
                "success": False,
                "operation_status": "indeterminate",
                "message": "一键部署任务未在限定时间内完成，请通过 job_id 查询最终状态",
                "job_id": str(submitted.job.id),
            },
            status_code=503,
        )
    event = await _quick_deploy_terminal_event(request, completed)
    if event.get("success") is not True:
        return fastapi_json_response(event, status_code=500)
    return fastapi_json_response(
        {
            "success": True,
            "message": "115 网盘基础配置已部署完成",
            "data": event.get("data") or {},
        }
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
    "/rename_config/mp/import",
    response_model=ExternalObjectResponse,
    responses={
        400: {"model": ExternalObjectResponse},
        403: {"model": ExternalObjectResponse},
        500: {"model": ExternalObjectResponse},
    },
)
async def import_rename_config_from_mp(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(await run_in_threadpool(_rename_moviepilot_service(request).import_templates))


@router.post(
    "/rename_config/mp/export",
    response_model=ExternalObjectResponse,
    responses={
        400: {
            "content": {
                "application/json": {"schema": ExternalObjectResponse.model_json_schema()},
                "text/html": {"schema": {"type": "string"}},
            }
        },
        403: {"model": ExternalObjectResponse},
        409: {"model": ExternalObjectResponse},
        415: {"content": {"text/html": {"schema": {"type": "string"}}}},
        500: {"model": ExternalObjectResponse},
        503: {"model": ExternalObjectResponse},
    },
    openapi_extra=_JSON_VALUE_BODY,
)
async def export_rename_config_to_mp(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, invalid = await _legacy_json_value(request)
    if invalid is not None:
        return invalid
    prepared = await run_in_threadpool(_rename_moviepilot_service(request).export_parameters, payload)
    if prepared.status_code != 200 or not isinstance(prepared.payload, Mapping):
        return _response(prepared)
    parameters = prepared.payload.get("data")
    if not isinstance(parameters, Mapping):
        return fastapi_json_response(
            {"success": False, "message": "MoviePilot 模板导出参数无效"},
            status_code=500,
        )
    submitted = await _submit_p115_native_operation(
        request,
        "p115.moviepilot-rename-export",
        dict(parameters),
        display_name="导出重命名模板到 MoviePilot",
        timeout_seconds=90,
        idempotency_scope="api.p115.rename-config-moviepilot-export",
    )
    if isinstance(submitted, Response):
        return submitted
    projection = await _await_p115_operation(_job_submission_service(request), submitted.job.id, timeout_seconds=95)
    if projection is None:
        return fastapi_json_response(
            {
                "success": False,
                "operation_status": "indeterminate",
                "message": "MoviePilot 模板导出任务未在限定时间内完成，请通过 job_id 查询最终状态",
                "job_id": str(submitted.job.id),
            },
            status_code=503,
        )
    if projection.job.status is not JobStatus.SUCCEEDED:
        return _native_operation_failure(projection)
    result = dict(projection.job.result or {})
    return fastapi_json_response(
        {
            "success": True,
            "message": "已把当前模板写入 MoviePilot",
            "data": {
                "mp_movie_template": result.get("mp_movie_template"),
                "mp_tv_template": result.get("mp_tv_template"),
            },
        }
    )


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


async def _quick_deploy_events(
    request: Request,
    jobs: JobSubmissionService,
    initial: JobProjection,
) -> AsyncIterator[str]:
    yield _ndjson({"type": "accepted", "job_id": str(initial.job.id)})
    submitted_at = time.monotonic()
    stream_timeout_seconds = max(
        35.0,
        float(getattr(initial.job, "timeout_seconds", None) or 1800) + 5.0,
    )
    last_progress: tuple[int, str | None] | None = None
    cancellation_requested = False
    cancellation_requested_at: float | None = None
    while True:
        projection = await run_in_threadpool(jobs.get, initial.job.id)
        if projection is None:
            yield _ndjson(
                {
                    "type": "done",
                    "success": False,
                    "status": "indeterminate",
                    "message": "持久任务状态已丢失",
                    "job_id": str(initial.job.id),
                }
            )
            return
        job = projection.job
        if (
            not cancellation_requested
            and job.status in {JobStatus.QUEUED, JobStatus.RETRY_WAIT}
            and time.monotonic() - submitted_at >= 30
        ):
            cancellation_requested = True
            try:
                projection = await run_in_threadpool(jobs.cancel, job.id)
            except Exception as exc:
                yield _ndjson(
                    {
                        "type": "done",
                        "success": False,
                        "status": "indeterminate",
                        "message": f"一键部署任务取消结果无法确认: {exc}",
                        "job_id": str(job.id),
                    }
                )
                return
            job = projection.job
            if job.status is JobStatus.CANCELLED:
                yield _ndjson(
                    {
                        "type": "done",
                        "success": False,
                        "status": "cancelled",
                        "message": "一键部署任务未在限定时间内开始，请检查 Coordinator 和 Worker 状态",
                        "job_id": str(job.id),
                    }
                )
                return
            if job.status.terminal:
                yield _ndjson(await _quick_deploy_terminal_event(request, projection))
                return
            if job.status not in {JobStatus.RUNNING, JobStatus.CANCEL_REQUESTED}:
                yield _ndjson(
                    {
                        "type": "done",
                        "success": False,
                        "status": "indeterminate",
                        "message": "一键部署任务取消后状态无法确认",
                        "job_id": str(job.id),
                    }
                )
                return
            cancellation_requested_at = time.monotonic()
        current_progress = (int(job.progress), job.progress_message)
        if job.progress_message and current_progress != last_progress:
            last_progress = current_progress
            yield _ndjson(
                {
                    "type": "progress",
                    "percent": int(job.progress),
                    "message": job.progress_message,
                }
            )
        if job.status.terminal:
            yield _ndjson(await _quick_deploy_terminal_event(request, projection))
            return
        if cancellation_requested_at is not None:
            if time.monotonic() - cancellation_requested_at >= 30:
                yield _ndjson(
                    {
                        "type": "done",
                        "success": False,
                        "status": "indeterminate",
                        "message": "一键部署任务取消后仍未到达终态，请通过 job_id 查询最终状态",
                        "job_id": str(job.id),
                    }
                )
                return
        elif time.monotonic() - submitted_at >= stream_timeout_seconds:
            yield _ndjson(
                {
                    "type": "done",
                    "success": False,
                    "status": "indeterminate",
                    "message": "一键部署任务仍在运行或状态无法推进，请通过 job_id 查询最终状态",
                    "job_id": str(job.id),
                }
            )
            return
        await asyncio.sleep(0.25)


async def _quick_deploy_terminal_event(request: Request, projection: JobProjection) -> dict[str, Any]:
    job = projection.job
    if job.status is JobStatus.SUCCEEDED:
        result = dict(job.result or {})
        if not isinstance(result.get("config"), Mapping):
            return {
                "type": "done",
                "success": False,
                "status": "indeterminate",
                "message": "一键部署任务缺少可重载的配置结果",
                "job_id": str(job.id),
            }
        synchronizer = getattr(request.app.state, "configuration_synchronizer", None)
        if synchronizer is not None:
            try:
                await run_in_threadpool(synchronizer.synchronize)
            except Exception as exc:
                return {
                    "type": "done",
                    "success": False,
                    "status": "confirmed",
                    "message": f"一键部署已执行，但 API 配置重载失败: {exc}",
                    "job_id": str(job.id),
                    "data": result,
                }
        return {
            "type": "done",
            "success": True,
            "status": "confirmed",
            "message": "115 网盘基础配置已部署完成",
            "job_id": str(job.id),
            "data": result,
        }
    error = dict(job.error_summary or {})
    error_type = str(error.get("type") or "")
    status = (
        "indeterminate"
        if error_type in {"PersistentEffectIndeterminate", "PersistentExecutionInterruptedAfterEffect"}
        else ("cancelled" if job.status is JobStatus.CANCELLED else "rejected")
    )
    return {
        "type": "done",
        "success": False,
        "status": status,
        "message": str(error.get("message") or job.progress_message or "一键部署任务失败"),
        "job_id": str(job.id),
    }


def _ndjson(payload: Mapping[str, Any]) -> str:
    return json.dumps(dict(payload), ensure_ascii=False, separators=(",", ":")) + "\n"


async def _submit_p115_native_operation(
    request: Request,
    operation_key: str,
    parameters: Mapping[str, Any],
    *,
    display_name: str,
    timeout_seconds: int,
    idempotency_scope: str,
) -> JobProjection | Response:
    idempotency_key = _idempotency_key(request) or secrets.token_urlsafe(24)
    if idempotency_key is not None and (
        len(idempotency_key) > 255 or not idempotency_key.isascii() or not idempotency_key.isprintable()
    ):
        return fastapi_json_response(
            {"success": False, "message": "Idempotency-Key 必须是 1-255 个可打印 ASCII 字符"},
            status_code=400,
        )
    session = get_request_session(request)
    try:
        submitted = await run_in_threadpool(
            partial(
                _job_submission_service(request).submit_native_operation,
                operation_key,
                parameters,
                display_name=display_name,
                trigger_type="manual",
                requested_by_user_id=str(session.get("emby_user_id") or "").strip() or None,
                idempotency_key=idempotency_key,
                idempotency_scope=idempotency_scope if idempotency_key is not None else None,
                max_attempts=1,
                timeout_seconds=timeout_seconds,
                trace_id=str(getattr(request.state, "trace_id", "") or "").strip() or None,
                reject_when_busy=False,
            )
        )
    except IdempotencyConflictError as exc:
        return fastapi_json_response({"success": False, "message": str(exc)}, status_code=409)
    except TaskBusyError:
        return fastapi_json_response(
            {"success": False, "message": "后台有任务正在运行，请稍后再试。"},
            status_code=503,
        )
    except (InvalidJsonPayloadError, TypeError, ValueError) as exc:
        return fastapi_json_response({"success": False, "message": str(exc)}, status_code=400)
    except Exception as exc:
        return fastapi_json_response(
            {"success": False, "message": f"持久任务提交失败: {exc}"},
            status_code=503,
        )
    return submitted.projection


async def _await_p115_operation(
    jobs: JobSubmissionService,
    job_id: UUID,
    *,
    timeout_seconds: float,
) -> JobProjection | None:
    deadline = time.monotonic() + max(0.1, float(timeout_seconds))
    while True:
        projection = await run_in_threadpool(jobs.get, job_id)
        if projection is None or projection.job.status.terminal:
            return projection
        if time.monotonic() >= deadline:
            return None
        await asyncio.sleep(0.25)


def _native_operation_failure(
    projection: JobProjection,
    *,
    legacy_status_key: bool = False,
) -> Response:
    error = dict(projection.job.error_summary or {})
    error_type = str(error.get("type") or "")
    indeterminate = error_type in {
        "PersistentEffectIndeterminate",
        "PersistentExecutionInterruptedAfterEffect",
    }
    payload: dict[str, Any] = {
        "message": str(error.get("message") or projection.job.progress_message or "持久任务失败"),
        "job_id": str(projection.job.id),
    }
    if legacy_status_key:
        payload["status"] = "error"
    else:
        payload["success"] = False
    if indeterminate:
        payload["operation_status"] = "indeterminate"
    return fastapi_json_response(payload, status_code=500)


def _uuid(value: str) -> UUID | None:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


__all__ = ["router"]
