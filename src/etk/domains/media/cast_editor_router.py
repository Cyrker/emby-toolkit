from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict
from starlette.responses import HTMLResponse, JSONResponse, Response

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .cast_editor import CastEditorResult, MediaCastEditorService, legacy_http_error_result

router = APIRouter(prefix="/api", tags=["media", "cast-editor"])


class CastEditingResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    item_id: str
    current_emby_cast: list[Any]


class CastActorResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: Any = None
    role: Any = None


class CastUpdateResponse(BaseModel):
    message: str | None = None
    error: str | None = None


def _service(request: Request) -> MediaCastEditorService:
    direct = getattr(request.app.state, "media_cast_editor_service", None)
    if isinstance(direct, MediaCastEditorService):
        return direct
    value = request.app.state.container.use_case("media.cast-editor")
    if not isinstance(value, MediaCastEditorService):
        raise TypeError("media.cast-editor provider returned an invalid service")
    return value


def _permission_policy(request: Request) -> PermissionPolicy:
    direct = getattr(request.app.state, "permission_policy", None)
    if isinstance(direct, PermissionPolicy):
        return direct
    container = request.app.state.container
    return PermissionPolicy(
        container.repository("identity"),
        auth_enabled=bool(container.settings.auth_enabled),
    )


def _admin_denial(request: Request) -> JSONResponse | None:
    decision = _permission_policy(request).require_admin(get_request_session(request))
    if decision.allowed:
        return None
    return fastapi_json_response(
        {"status": "error", "message": decision.message},
        status_code=decision.status_code,
    )


def _response(result: CastEditorResult) -> Response:
    if result.media_type == "text/html":
        return HTMLResponse(content=str(result.payload), status_code=result.status_code)
    return fastapi_json_response(result.payload, status_code=result.status_code)


def _path_parameter(name: str) -> dict[str, Any]:
    return {
        "parameters": [
            {
                "name": name,
                "in": "path",
                "required": True,
                "schema": {"type": "string"},
            }
        ]
    }


def _error_responses(
    *,
    json_statuses: tuple[int, ...] = (),
    html_statuses: tuple[int, ...] = (),
) -> dict[int | str, dict[str, Any]]:
    responses: dict[int | str, dict[str, Any]] = {}
    for status in set(json_statuses) | set(html_statuses):
        content: dict[str, Any] = {}
        if status in json_statuses:
            content["application/json"] = {"schema": {"type": "object", "additionalProperties": True}}
        if status in html_statuses:
            content["text/html"] = {"schema": {"type": "string"}}
        responses[status] = {"content": content}
    return responses


_UPDATE_BODY = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "cast": {"type": "array", "items": {}},
                        "item_name": {},
                    },
                    "required": ["cast"],
                }
            }
        },
    }
}

_TRANSLATE_BODY = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "cast": {"type": "array", "items": {}},
                        "title": {},
                        "year": {},
                    },
                    "required": ["cast"],
                }
            }
        },
    }
}


@router.get(
    "/media_for_editing/{item_id}",
    response_model=CastEditingResponse,
    responses=_error_responses(json_statuses=(403, 404, 503), html_statuses=(500,)),
    openapi_extra=_path_parameter("item_id"),
)
def media_for_editing(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    service = _service(request)
    unavailable = service.unavailable()
    if unavailable is not None:
        return _response(unavailable)
    return _response(service.media_for_editing(str(request.path_params.get("item_id") or "")))


@router.post(
    "/update_media_cast_sa/{item_id}",
    response_model=CastUpdateResponse,
    status_code=202,
    responses=_error_responses(
        json_statuses=(400, 403, 503),
        html_statuses=(400, 415, 500),
    ),
    openapi_extra={**_path_parameter("item_id"), **_UPDATE_BODY},
)
async def update_media_cast(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    service = _service(request)
    unavailable = service.unavailable()
    if unavailable is not None:
        return _response(unavailable)
    payload, invalid = await _legacy_json(request)
    if invalid is not None:
        return _response(invalid)
    try:
        valid = bool(payload) and "cast" in payload and isinstance(payload["cast"], list)
    except (KeyError, TypeError):
        return _response(legacy_http_error_result(500))
    if not valid:
        return fastapi_json_response(
            {"error": "请求体中缺少有效的 'cast' 列表"},
            status_code=400,
        )
    cast = payload["cast"]
    item_id = str(request.path_params.get("item_id") or "")
    item_name = payload.get("item_name", f"未知项目(ID:{item_id})")
    return _response(service.update_cast(item_id, cast, item_name=item_name))


@router.post(
    "/actions/translate_cast_sa",
    response_model=list[CastActorResponse],
    responses=_error_responses(
        json_statuses=(400, 403, 500, 503),
        html_statuses=(400, 415, 500),
    ),
    openapi_extra=_TRANSLATE_BODY,
)
async def translate_cast(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    service = _service(request)
    unavailable = service.unavailable()
    if unavailable is not None:
        return _response(unavailable)
    payload, invalid = await _legacy_json(request)
    if invalid is not None:
        return _response(invalid)
    if not isinstance(payload, Mapping):
        return _response(legacy_http_error_result(500))
    current_cast = payload.get("cast")
    if not isinstance(current_cast, list):
        return fastapi_json_response(
            {"error": "请求体必须包含 'cast' 列表。"},
            status_code=400,
        )
    return _response(
        service.translate_cast(
            current_cast,
            title=payload.get("title"),
            year=payload.get("year"),
        )
    )


@router.post(
    "/preview_processed_cast/{item_id}",
    response_model=list[CastActorResponse],
    responses=_error_responses(json_statuses=(404, 500, 503)),
    openapi_extra=_path_parameter("item_id"),
)
def preview_processed_cast(request: Request) -> Response:
    service = _service(request)
    unavailable = service.unavailable()
    if unavailable is not None:
        return _response(unavailable)
    return _response(service.preview_processed_cast(str(request.path_params.get("item_id") or "")))


async def _legacy_json(
    request: Request,
) -> tuple[Any, CastEditorResult | None]:
    content_type = request.headers.get("content-type", "").partition(";")[0].strip().casefold()
    if content_type != "application/json" and not (
        content_type.startswith("application/") and content_type.endswith("+json")
    ):
        return None, legacy_http_error_result(415)
    try:
        return await request.json(), None
    except (UnicodeDecodeError, ValueError):
        return None, legacy_http_error_result(400)


__all__ = ["router"]
