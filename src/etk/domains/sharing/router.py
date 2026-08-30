from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Request
from starlette.concurrency import run_in_threadpool
from starlette.responses import Response
from werkzeug.exceptions import BadRequest, UnsupportedMediaType

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .service import SharingHttpResult, SharingService

router = APIRouter(prefix="/api/shared/resources", tags=["sharing"])

_JSON_OBJECT_REQUEST_BODY = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {"type": "object", "additionalProperties": True},
            }
        },
    }
}
_DEVICE_REQUEST_BODY = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
            }
        },
    }
}


def _service(request: Request) -> SharingService:
    direct = getattr(request.app.state, "sharing_service", None)
    if isinstance(direct, SharingService):
        return direct
    value = request.app.state.container.use_case("sharing")
    if not isinstance(value, SharingService):
        raise TypeError("sharing provider returned an invalid service")
    return value


def _admin_denial(request: Request) -> Response | None:
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


def _response(result: SharingHttpResult) -> Response:
    return fastapi_json_response(result.payload, status_code=result.status_code, headers=dict(result.headers))


def _account_id(request: Request) -> str:
    query_value = str(request.query_params.get("account_id") or "").strip()
    if query_value:
        return query_value
    return str(get_request_session(request).get("emby_user_id") or "local").strip() or "local"


def _principal_id(request: Request) -> str:
    return str(get_request_session(request).get("emby_user_id") or "local").strip() or "local"


def _int_query(request: Request, key: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(request.query_params.get(key, default))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(value, maximum))


def _positive_path_int(value: object) -> int | None:
    try:
        normalized = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return normalized if normalized > 0 else None


def _source_ids(payload: Mapping[str, Any]) -> list[int]:
    raw = payload.get("ids") or payload.get("source_ids") or []
    if not isinstance(raw, list):
        return []
    result: list[int] = []
    for value in raw:
        if isinstance(value, bool):
            continue
        normalized = _positive_path_int(value)
        if normalized is not None and normalized not in result:
            result.append(normalized)
    return result


def _bool_query(request: Request, *keys: str, default: bool = False) -> bool:
    value: object = None
    for key in keys:
        if key in request.query_params:
            value = request.query_params.get(key)
            break
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().casefold() in {"1", "true", "yes", "y", "on", "启用", "开启", "是"}


async def _json_object(
    request: Request,
    *,
    allow_empty: bool = False,
) -> tuple[dict[str, Any] | None, Response | None]:
    body = await request.body()
    content_type = request.headers.get("content-type", "").partition(";")[0].strip().casefold()
    if (
        allow_empty
        and not body
        and (
            not content_type
            or content_type == "application/json"
            or (content_type.startswith("application/") and content_type.endswith("+json"))
        )
    ):
        return {}, None
    if content_type != "application/json" and not (
        content_type.startswith("application/") and content_type.endswith("+json")
    ):
        return None, _html_error(UnsupportedMediaType())
    try:
        value = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, _html_error(BadRequest())
    if not isinstance(value, Mapping):
        return None, _html_error(BadRequest())
    return dict(value), None


def _html_error(error: BadRequest | UnsupportedMediaType) -> Response:
    headers = dict(error.get_headers())
    return Response(
        content=error.get_body(),
        status_code=int(error.code or 500),
        headers={"content-type": headers["Content-Type"]},
    )


@router.get(
    "/config",
    response_model=dict[str, Any],
    responses={403: {"description": "admin required"}, 503: {"description": "configuration unavailable"}},
)
def shared_resource_config(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).configuration())


@router.post(
    "/config",
    response_model=dict[str, Any],
    responses={
        400: {"description": "invalid request"},
        403: {"description": "admin required"},
        415: {"description": "JSON required"},
        503: {"description": "configuration unavailable"},
    },
    openapi_extra=_JSON_OBJECT_REQUEST_BODY,
)
async def save_shared_resource_config(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    try:
        result = await run_in_threadpool(_service(request).save_configuration, payload)
    except (TypeError, ValueError):
        return fastapi_json_response({"success": False, "message": "共享资源配置请求无效"}, status_code=400)
    return _response(result)


@router.get(
    "/summary",
    response_model=dict[str, Any],
    responses={403: {"description": "admin required"}, 503: {"description": "projection unavailable"}},
)
def shared_resource_summary(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).summary())


@router.get(
    "/shares",
    response_model=dict[str, Any],
    responses={403: {"description": "admin required"}, 503: {"description": "projection unavailable"}},
)
def list_local_sources(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(
        _service(request).local_sources(
            status=request.query_params.get("status") or "usable",
            keyword=request.query_params.get("keyword") or request.query_params.get("q") or "",
            page=_int_query(request, "page", 1, 1, 10_000_000),
            page_size=_int_query(request, "page_size", 30, 1, 200),
            raw_limit=_int_query(request, "raw_limit", 200_000, 1_000, 200_000),
        )
    )


@router.post(
    "/shares/{source_id}/check",
    response_model=dict[str, Any],
    responses={
        403: {"description": "admin required"},
        404: {"description": "source not found"},
        503: {"description": "projection unavailable"},
    },
)
def check_local_source(request: Request, source_id: str) -> Response:
    denied = _admin_denial(request)
    normalized_id = _positive_path_int(source_id)
    if normalized_id is None:
        return denied or fastapi_json_response({"success": False, "message": "本地共享源不存在"}, status_code=404)
    return denied or _response(_service(request).local_source_check(normalized_id))


_SOURCE_LIFECYCLE_RESPONSES: dict[int | str, dict[str, Any]] = {
    202: {"description": "provider operation queued", "model": dict[str, Any]},
    400: {"description": "invalid request"},
    403: {"description": "admin required"},
    404: {"description": "source not found"},
    409: {"description": "operation conflict", "model": dict[str, Any]},
    503: {"description": "sharing runtime unavailable"},
}


@router.post(
    "/shares/{source_id}/cancel",
    response_model=dict[str, Any],
    responses=_SOURCE_LIFECYCLE_RESPONSES,
)
def disable_local_source(request: Request, source_id: str) -> Response:
    denied = _admin_denial(request)
    normalized_id = _positive_path_int(source_id)
    if normalized_id is None:
        return denied or fastapi_json_response({"success": False, "message": "本地共享源不存在"}, status_code=404)
    return denied or _response(
        _service(request).disable_local_source(
            normalized_id,
            principal_id=_principal_id(request),
            idempotency_key=request.headers.get("idempotency-key"),
        )
    )


def _delete_local_source(request: Request, source_id: str) -> Response:
    denied = _admin_denial(request)
    normalized_id = _positive_path_int(source_id)
    if normalized_id is None:
        return denied or fastapi_json_response({"success": False, "message": "本地共享源不存在"}, status_code=404)
    return denied or _response(
        _service(request).delete_local_source(
            normalized_id,
            principal_id=_principal_id(request),
            idempotency_key=request.headers.get("idempotency-key"),
        )
    )


@router.post(
    "/shares/{source_id}/delete",
    response_model=dict[str, Any],
    responses=_SOURCE_LIFECYCLE_RESPONSES,
)
def delete_local_source_post(request: Request, source_id: str) -> Response:
    return _delete_local_source(request, source_id)


@router.delete(
    "/shares/{source_id}/delete",
    response_model=dict[str, Any],
    responses=_SOURCE_LIFECYCLE_RESPONSES,
)
def delete_local_source_delete(request: Request, source_id: str) -> Response:
    return _delete_local_source(request, source_id)


async def _local_source_lifecycle_batch(request: Request, *, delete: bool) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    source_ids = _source_ids(payload)
    if not source_ids:
        message = "缺少要删除的本地源 ID" if delete else "缺少要停用的本地源 ID"
        return fastapi_json_response({"success": False, "message": message}, status_code=400)
    action = _service(request).delete_local_sources if delete else _service(request).disable_local_sources
    try:
        result = await run_in_threadpool(
            action,
            source_ids,
            principal_id=_principal_id(request),
            idempotency_key=request.headers.get("idempotency-key"),
        )
    except (TypeError, ValueError):
        return fastapi_json_response({"success": False, "message": "共享源批量请求无效"}, status_code=400)
    return _response(result)


@router.post(
    "/shares/cancel-batch",
    response_model=dict[str, Any],
    responses={**_SOURCE_LIFECYCLE_RESPONSES, 415: {"description": "JSON required"}},
    openapi_extra=_JSON_OBJECT_REQUEST_BODY,
)
async def disable_local_sources_batch(request: Request) -> Response:
    return await _local_source_lifecycle_batch(request, delete=False)


@router.post(
    "/shares/delete-batch",
    response_model=dict[str, Any],
    responses={**_SOURCE_LIFECYCLE_RESPONSES, 415: {"description": "JSON required"}},
    openapi_extra=_JSON_OBJECT_REQUEST_BODY,
)
async def delete_local_sources_batch(request: Request) -> Response:
    return await _local_source_lifecycle_batch(request, delete=True)


@router.post(
    "/shares/{source_id}/share/cancel",
    response_model=dict[str, Any],
    responses={
        403: {"description": "admin required"},
        404: {"description": "source not found"},
        410: {"description": "retired route"},
    },
)
def cancel_retired_completed_season_share(request: Request, source_id: str) -> Response:
    denied = _admin_denial(request)
    if _positive_path_int(source_id) is None:
        return denied or fastapi_json_response({"success": False, "message": "本地共享源不存在"}, status_code=404)
    return denied or fastapi_json_response(
        {
            "success": False,
            "message": "旧 completed_season 分享取消接口已停用；逻辑季分享由中心 logical_season_share_channels 管理。",
        },
        status_code=410,
    )


@router.get(
    "/media/search",
    response_model=dict[str, Any],
    responses={403: {"description": "admin required"}, 503: {"description": "projection unavailable"}},
)
def search_shareable_media(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(
        _service(request).search_shareable_media(
            request.query_params.get("keyword") or request.query_params.get("q") or "",
            limit=_int_query(request, "limit", 100, 1, 500),
        )
    )


@router.post(
    "/shares/manual-validate",
    response_model=dict[str, Any],
    responses={
        400: {"description": "invalid request"},
        403: {"description": "admin required"},
        415: {"description": "JSON required"},
        503: {"description": "projection unavailable"},
    },
    openapi_extra=_JSON_OBJECT_REQUEST_BODY,
)
async def validate_manual_source(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    try:
        result = await run_in_threadpool(_service(request).validate_manual_source, payload)
    except (TypeError, ValueError):
        return fastapi_json_response({"success": False, "message": "共享源预校验请求无效"}, status_code=400)
    return _response(result)


@router.get(
    "/virtual-imports",
    response_model=dict[str, Any],
    responses={403: {"description": "admin required"}, 503: {"description": "projection unavailable"}},
)
def list_virtual_imports(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(
        _service(request).virtual_imports(
            status=request.query_params.get("status") or "virtual",
            keyword=request.query_params.get("keyword") or request.query_params.get("q") or "",
            item_type=request.query_params.get("item_type") or request.query_params.get("type") or "all",
            page=_int_query(request, "page", 1, 1, 10_000_000),
            page_size=_int_query(request, "page_size", 30, 1, 200),
        )
    )


@router.get(
    "/credit/balance",
    response_model=dict[str, Any],
    responses={403: {"description": "admin required"}, 503: {"description": "provider unavailable"}},
)
def credit_balance(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).credit_balance(_account_id(request)))


@router.get(
    "/credit/ledger",
    response_model=dict[str, Any],
    responses={403: {"description": "admin required"}, 503: {"description": "provider unavailable"}},
)
def credit_ledger(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    limit = _int_query(request, "limit", 100, 1, 1000)
    return _response(_service(request).credit_ledger(_account_id(request), limit=limit))


@router.get(
    "/center/sources",
    response_model=dict[str, Any],
    responses={403: {"description": "admin required"}, 503: {"description": "provider unavailable"}},
)
def list_center_sources(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    service = _service(request)
    query: dict[str, Any] = {
        "q": request.query_params.get("q") or request.query_params.get("keyword") or "",
        "status": request.query_params.get("status") or "alive,available",
        "item_type": request.query_params.get("item_type") or "",
        "tmdb_id": request.query_params.get("tmdb_id") or "",
        "order_by": request.query_params.get("order_by") or "latest",
        "genre_id": request.query_params.get("genre_id") or "",
        "release_year": request.query_params.get("release_year") or None,
        "force_refresh": False,
    }
    default_limit = _int_query(request, "page_size", 200, 1, 1000)
    query["limit"] = _int_query(request, "limit", default_limit, 1, 1000)
    query["offset"] = _int_query(request, "offset", 0, 0, 10_000_000)
    return _response(service.list_sources(**query))


@router.get(
    "/center/sources/tags",
    response_model=dict[str, Any],
    responses={403: {"description": "admin required"}},
)
def center_source_tags(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).display_tags())


@router.get(
    "/center/sources/home",
    response_model=dict[str, Any],
    responses={403: {"description": "admin required"}, 503: {"description": "provider unavailable"}},
)
def center_sources_home(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    service = _service(request)
    configuration = service.configuration().payload
    data = configuration.get("data", {}) if isinstance(configuration, Mapping) else {}
    sections = data.get("p115_shared_center_home_sections", []) if isinstance(data, Mapping) else []
    return _response(
        service.display_home(
            limit_per_section=_int_query(request, "limit_per_section", 10, 1, 20),
            force_refresh=_bool_query(request, "force_refresh", "refresh", "no_cache"),
            sections=sections if isinstance(sections, list) else [],
        )
    )


@router.get(
    "/center/sources/children",
    response_model=dict[str, Any],
    responses={403: {"description": "admin required"}, 503: {"description": "provider unavailable"}},
)
def center_source_children(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    source_ids = list(
        dict.fromkeys(
            value.strip()
            for value in str(request.query_params.get("source_ids") or "").replace(";", ",").split(",")
            if value.strip()
        )
    )
    source_id = str(request.query_params.get("source_id") or "").strip()
    if source_id and source_id not in source_ids:
        source_ids.insert(0, source_id)
    return _response(
        _service(request).display_children(
            source_kind=request.query_params.get("source_kind") or "",
            source_id=source_id,
            source_ids=source_ids,
            hub_id=request.query_params.get("hub_id") or "",
            limit=_int_query(request, "limit", 5000, 1, 20_000),
            offset=_int_query(request, "offset", 0, 0, 10_000_000),
        )
    )


@router.get(
    "/center/sources/detail",
    response_model=dict[str, Any],
    responses={403: {"description": "admin required"}, 503: {"description": "provider unavailable"}},
)
def center_source_detail(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(
        _service(request).display_detail(
            source_kind=request.query_params.get("source_kind") or "",
            source_id=request.query_params.get("source_id") or "",
            hub_id=request.query_params.get("hub_id") or "",
            tmdb_id=request.query_params.get("tmdb_id") or "",
            item_type=request.query_params.get("item_type") or "",
            season_number=request.query_params.get("season_number") or None,
            limit=_int_query(request, "limit", 200, 1, 1000),
            include_people=_bool_query(request, "include_people"),
        )
    )


@router.post(
    "/center/device/register",
    response_model=dict[str, Any],
    responses={
        202: {"description": "registration queued", "model": dict[str, Any]},
        400: {"description": "invalid request"},
        403: {"description": "admin required"},
        409: {"description": "operation conflict", "model": dict[str, Any]},
        415: {"description": "JSON required"},
        503: {"description": "provider unavailable"},
    },
    openapi_extra=_DEVICE_REQUEST_BODY,
)
async def register_center_device(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request, allow_empty=True)
    if error is not None:
        return error
    assert payload is not None
    name = str(payload.get("name") or "ETK Device").strip() or "ETK Device"
    try:
        result = await run_in_threadpool(
            _service(request).register_device,
            name=name,
            principal_id=_principal_id(request),
            idempotency_key=request.headers.get("idempotency-key"),
        )
    except (TypeError, ValueError):
        return fastapi_json_response({"success": False, "message": "中心设备登记请求无效"}, status_code=400)
    return _response(result)


@router.get(
    "/center/device/status",
    response_model=dict[str, Any],
    responses={403: {"description": "admin required"}, 503: {"description": "provider unavailable"}},
)
def center_device_status(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).device_status())


@router.post(
    "/shares/manual-create",
    response_model=dict[str, Any],
    responses={
        202: {"description": "registration queued", "model": dict[str, Any]},
        400: {"description": "invalid request"},
        403: {"description": "admin required"},
        409: {"description": "operation conflict", "model": dict[str, Any]},
        415: {"description": "JSON required"},
        503: {"description": "provider unavailable"},
    },
    openapi_extra=_JSON_OBJECT_REQUEST_BODY,
)
async def register_source(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    kind = str(payload.pop("source_kind", payload.pop("kind", "movie")) or "movie").strip().lower()
    try:
        result = await run_in_threadpool(
            _service(request).register_source,
            kind,
            payload,
            principal_id=_principal_id(request),
            idempotency_key=request.headers.get("idempotency-key"),
        )
    except (TypeError, ValueError):
        return fastapi_json_response({"success": False, "message": "共享源请求无效"}, status_code=400)
    return _response(result)


@router.post(
    "/transfers/lease",
    response_model=dict[str, Any],
    responses={
        202: {"description": "transfer queued", "model": dict[str, Any]},
        400: {"description": "invalid request"},
        403: {"description": "admin required"},
        409: {"description": "operation conflict", "model": dict[str, Any]},
        415: {"description": "JSON required"},
        503: {"description": "provider unavailable"},
    },
    openapi_extra=_JSON_OBJECT_REQUEST_BODY,
)
async def acquire_transfer_lease(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    try:
        result = await run_in_threadpool(
            _service(request).acquire_transfer_lease,
            payload,
            principal_id=_principal_id(request),
            idempotency_key=request.headers.get("idempotency-key"),
        )
    except (TypeError, ValueError):
        return fastapi_json_response({"success": False, "message": "转存租约请求无效"}, status_code=400)
    return _response(result)


@router.post(
    "/transfers/report",
    response_model=dict[str, Any],
    responses={
        202: {"description": "report queued", "model": dict[str, Any]},
        400: {"description": "invalid request"},
        403: {"description": "admin required"},
        409: {"description": "operation conflict", "model": dict[str, Any]},
        415: {"description": "JSON required"},
        503: {"description": "provider unavailable"},
    },
    openapi_extra=_JSON_OBJECT_REQUEST_BODY,
)
async def report_transfer(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    try:
        result = await run_in_threadpool(
            _service(request).report_transfer,
            payload,
            principal_id=_principal_id(request),
            idempotency_key=request.headers.get("idempotency-key"),
        )
    except (TypeError, ValueError):
        return fastapi_json_response({"success": False, "message": "转存报告请求无效"}, status_code=400)
    return _response(result)


@router.post(
    "/share-requests/quote",
    response_model=dict[str, Any],
    responses={
        400: {"description": "invalid request"},
        403: {"description": "admin required"},
        415: {"description": "JSON required"},
        503: {"description": "provider unavailable"},
    },
    openapi_extra=_JSON_OBJECT_REQUEST_BODY,
)
async def quote_share_request(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    try:
        result = await run_in_threadpool(_service(request).quote_share_request, payload)
    except (TypeError, ValueError):
        return fastapi_json_response({"success": False, "message": "求共享报价请求无效"}, status_code=400)
    return _response(result)


@router.get(
    "/share-requests/param-options",
    response_model=dict[str, Any],
    responses={403: {"description": "admin required"}},
)
def share_request_param_options(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).share_request_param_options())


@router.get(
    "/share-requests/tmdb/search",
    response_model=dict[str, Any],
    responses={
        400: {"description": "TMDb is not configured"},
        403: {"description": "admin required"},
        503: {"description": "TMDb unavailable"},
    },
)
def search_share_request_tmdb(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(
        _service(request).search_share_request_tmdb(
            request.query_params.get("keyword") or request.query_params.get("q") or "",
            media_type=request.query_params.get("media_type") or "movie",
        )
    )


async def _share_request_participant_mutation(request: Request, group_id: str, *, cancel: bool) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    service = _service(request)
    action = service.cancel_share_request if cancel else service.co_request_share_request
    try:
        result = await run_in_threadpool(
            action,
            group_id,
            payload,
            principal_id=_principal_id(request),
            idempotency_key=request.headers.get("idempotency-key"),
        )
    except (TypeError, ValueError):
        message = "取消求共享请求无效" if cancel else "同求请求无效"
        return fastapi_json_response({"success": False, "message": message}, status_code=400)
    return _response(result)


_SHARE_REQUEST_PARTICIPANT_RESPONSES: dict[int | str, dict[str, Any]] = {
    202: {"description": "operation queued", "model": dict[str, Any]},
    400: {"description": "invalid request"},
    403: {"description": "admin required"},
    409: {"description": "operation conflict", "model": dict[str, Any]},
    415: {"description": "JSON required"},
    503: {"description": "provider unavailable"},
}


@router.post(
    "/share-requests/{group_id}/co-request",
    response_model=dict[str, Any],
    responses=_SHARE_REQUEST_PARTICIPANT_RESPONSES,
    openapi_extra=_JSON_OBJECT_REQUEST_BODY,
)
async def co_request_share_request(request: Request, group_id: str) -> Response:
    return await _share_request_participant_mutation(request, group_id, cancel=False)


@router.post(
    "/share-requests/{group_id}/cancel",
    response_model=dict[str, Any],
    responses=_SHARE_REQUEST_PARTICIPANT_RESPONSES,
    openapi_extra=_JSON_OBJECT_REQUEST_BODY,
)
async def cancel_share_request(request: Request, group_id: str) -> Response:
    return await _share_request_participant_mutation(request, group_id, cancel=True)


async def _share_requests(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    service = _service(request)
    if request.method == "GET":
        query: dict[str, Any] = {
            key: request.query_params.get(key, default)
            for key, default in (
                ("keyword", ""),
                ("status", "open"),
                ("media_type", ""),
                ("target_type", ""),
            )
        }
        query["limit"] = _int_query(request, "limit", 100, 1, 500)
        query["offset"] = _int_query(request, "offset", 0, 0, 10_000_000)
        return _response(await run_in_threadpool(service.list_share_requests, **query))
    payload, error = await _json_object(request)
    if error is not None:
        return error
    assert payload is not None
    try:
        result = await run_in_threadpool(
            service.create_share_request,
            payload,
            principal_id=_principal_id(request),
            idempotency_key=request.headers.get("idempotency-key"),
        )
    except (TypeError, ValueError):
        return fastapi_json_response({"success": False, "message": "求共享请求无效"}, status_code=400)
    return _response(result)


@router.get(
    "/share-requests",
    response_model=dict[str, Any],
    name="list_share_requests",
    responses={403: {"description": "admin required"}, 503: {"description": "provider unavailable"}},
)
async def list_share_requests(request: Request) -> Response:
    return await _share_requests(request)


@router.post(
    "/share-requests",
    response_model=dict[str, Any],
    name="create_share_request",
    responses={
        202: {"description": "request queued", "model": dict[str, Any]},
        400: {"description": "invalid request"},
        403: {"description": "admin required"},
        409: {"description": "operation conflict", "model": dict[str, Any]},
        415: {"description": "JSON required"},
        503: {"description": "provider unavailable"},
    },
    openapi_extra=_JSON_OBJECT_REQUEST_BODY,
)
async def create_share_request(request: Request) -> Response:
    return await _share_requests(request)


@router.get(
    "/operations/{operation_id}/status",
    response_model=dict[str, Any],
    responses={
        403: {"description": "admin required"},
        404: {"description": "not found"},
        503: {"description": "unavailable"},
    },
)
def sharing_operation_status(request: Request, operation_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        UUID(operation_id)
    except (TypeError, ValueError, AttributeError):
        return fastapi_json_response({"success": False, "message": "共享操作不存在"}, status_code=404)
    return _response(_service(request).operation_status(operation_id))


@router.post(
    "/operations/{operation_id}/cancel",
    response_model=dict[str, Any],
    responses={
        202: {"description": "cancellation recorded after provider checkpoint", "model": dict[str, Any]},
        403: {"description": "admin required"},
        404: {"description": "not found"},
        409: {"description": "operation already terminal", "model": dict[str, Any]},
        503: {"description": "unavailable"},
    },
)
def cancel_sharing_operation(request: Request, operation_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        UUID(operation_id)
    except (TypeError, ValueError, AttributeError):
        return fastapi_json_response({"success": False, "message": "共享操作不存在"}, status_code=404)
    return _response(_service(request).cancel_operation(operation_id))


__all__ = ["router"]
