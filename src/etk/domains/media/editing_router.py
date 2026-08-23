from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict
from starlette.responses import JSONResponse, Response
from werkzeug.exceptions import BadRequest, InternalServerError, UnsupportedMediaType

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .editing import MediaEditingResult, MediaEditingService

router = APIRouter(prefix="/api", tags=["media", "editing"])


class MediaInfoResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    sha1: str
    mediainfo: Any


class MediaInfoChaptersResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    sha1: str
    intro_start_seconds: float | None = None
    intro_end_seconds: float | None = None
    credits_start_seconds: float | None = None
    runtime_seconds: float | None = None
    intro_enabled: bool
    credits_enabled: bool


class MediaInfoEpisodeResponse(BaseModel):
    emby_id: str
    season_number: int | None = None
    episode_number: int | None = None
    title: str


class MediaInfoWriteResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    message: str | None = None
    error: str | None = None
    effect_id: str | None = None
    effect_status: str | None = None


def _service(request: Request) -> MediaEditingService:
    direct = getattr(request.app.state, "media_editing_service", None)
    if isinstance(direct, MediaEditingService):
        return direct
    value = request.app.state.container.use_case("media.editing")
    if not isinstance(value, MediaEditingService):
        raise TypeError("media.editing provider returned an invalid service")
    return value


def _response(result: MediaEditingResult) -> JSONResponse:
    return fastapi_json_response(result.payload, status_code=result.status_code)


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


async def _json_body(request: Request) -> tuple[Mapping[str, Any] | None, Response | None]:
    if not _is_json_content_type(request.headers.get("content-type", "")):
        return None, _html_error(UnsupportedMediaType())
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, _html_error(BadRequest())
    if not isinstance(payload, Mapping):
        return None, _html_error(InternalServerError())
    return dict(payload), None


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


_READ_ERROR_RESPONSES = _error_responses(json_statuses=(404, 409, 500, 503))
_WRITE_ERROR_RESPONSES = _error_responses(
    json_statuses=(202, 400, 403, 404, 409, 500, 502, 503),
    html_statuses=(400, 415, 500),
)


_CHAPTER_UPDATE_REQUEST_BODY = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "sha1": {"type": "string"},
                        "intro_enabled": {"type": "boolean"},
                        "intro_start_seconds": {"type": "number"},
                        "intro_end_seconds": {"type": "number"},
                        "credits_enabled": {"type": "boolean"},
                        "credits_start_seconds": {"type": "number"},
                    },
                    "required": ["sha1"],
                }
            }
        },
    }
}


_MEDIA_INFO_UPDATE_REQUEST_BODY = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "sha1": {"type": "string"},
                        "mediainfo": {
                            "oneOf": [
                                {"type": "object"},
                                {"type": "array", "items": {"type": "object"}, "minItems": 1},
                            ]
                        },
                    },
                    "required": ["sha1", "mediainfo"],
                }
            }
        },
    }
}


@router.get(
    "/media_info/edit/{item_id}",
    response_model=MediaInfoResponse,
    responses=_READ_ERROR_RESPONSES,
    openapi_extra=_path_parameter("item_id"),
)
def media_info(request: Request) -> JSONResponse:
    return _response(_service(request).media_info(str(request.path_params.get("item_id") or "")))


@router.post(
    "/media_info/edit/{item_id}",
    response_model=MediaInfoWriteResponse,
    responses=_WRITE_ERROR_RESPONSES,
    openapi_extra={**_path_parameter("item_id"), **_MEDIA_INFO_UPDATE_REQUEST_BODY},
)
async def update_media_info(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    service = _service(request)
    unavailable = service.unavailable()
    if unavailable is not None:
        return _response(unavailable)
    payload, invalid = await _json_body(request)
    if invalid is not None:
        return invalid
    assert payload is not None
    return _response(
        service.update_media_info(
            str(request.path_params.get("item_id") or ""),
            payload,
        )
    )


@router.get(
    "/media_info/chapters/{item_id}",
    response_model=MediaInfoChaptersResponse,
    responses=_READ_ERROR_RESPONSES,
    openapi_extra=_path_parameter("item_id"),
)
def media_info_chapters(request: Request) -> JSONResponse:
    return _response(_service(request).chapters(str(request.path_params.get("item_id") or "")))


@router.post(
    "/media_info/chapters/{item_id}",
    response_model=MediaInfoWriteResponse,
    responses=_WRITE_ERROR_RESPONSES,
    openapi_extra={**_path_parameter("item_id"), **_CHAPTER_UPDATE_REQUEST_BODY},
)
async def update_media_info_chapters(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    service = _service(request)
    unavailable = service.unavailable()
    if unavailable is not None:
        return _response(unavailable)
    payload, invalid = await _json_body(request)
    if invalid is not None:
        return invalid
    assert payload is not None
    return _response(
        service.update_chapters(
            str(request.path_params.get("item_id") or ""),
            payload,
        )
    )


@router.get(
    "/media_info/series/{series_id}/episodes",
    response_model=list[MediaInfoEpisodeResponse],
    responses=_READ_ERROR_RESPONSES,
    openapi_extra=_path_parameter("series_id"),
)
def media_info_series_episodes(request: Request) -> JSONResponse:
    return _response(_service(request).series_episodes(str(request.path_params.get("series_id") or "")))


__all__ = ["router"]
