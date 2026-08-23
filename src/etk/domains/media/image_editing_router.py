from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from email import policy
from email.parser import BytesParser
from typing import Any
from urllib.parse import parse_qs

from fastapi import APIRouter, Request
from pydantic import BaseModel
from starlette.responses import JSONResponse

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .image_editing import ImageEditingResult, MediaImageEditingService

router = APIRouter(prefix="/api", tags=["media", "image-editing"])


class TmdbImageCandidateResponse(BaseModel):
    preview: str
    original: str
    aspect_ratio: Any
    width: int | None = None
    height: int | None = None


class TmdbImageCatalogResponse(BaseModel):
    posters: list[TmdbImageCandidateResponse]
    backdrops: list[TmdbImageCandidateResponse]
    logos: list[TmdbImageCandidateResponse]


class ImageUpdateResponse(BaseModel):
    status: str | None = None
    message: str | None = None
    error: str | None = None
    effect_id: str | None = None
    effect_status: str | None = None


@dataclass(frozen=True, slots=True)
class _ParsedImageUpdate:
    image_type: object
    image_url: object
    image_bytes: bytes | None = None
    content_type: str | None = None


def _service(request: Request) -> MediaImageEditingService:
    direct = getattr(request.app.state, "media_image_editing_service", None)
    if isinstance(direct, MediaImageEditingService):
        return direct
    value = request.app.state.container.use_case("media.image-editing")
    if not isinstance(value, MediaImageEditingService):
        raise TypeError("media.image-editing provider returned an invalid service")
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


def _response(result: ImageEditingResult) -> JSONResponse:
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


def _json_responses(*statuses: int) -> dict[int | str, dict[str, Any]]:
    return {status: {"model": ImageUpdateResponse} for status in statuses}


_IMAGE_UPDATE_REQUEST_BODY = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "image_type": {"type": "string"},
                        "image_url": {"type": "string"},
                    },
                    "required": ["image_type"],
                    "oneOf": [{"required": ["image_url"]}],
                }
            },
            "multipart/form-data": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "image_type": {"type": "string"},
                        "image_url": {"type": "string"},
                        "file": {"type": "string", "format": "binary"},
                    },
                    "required": ["image_type"],
                    "oneOf": [
                        {"required": ["image_url"]},
                        {"required": ["file"]},
                    ],
                }
            },
        },
    }
}


@router.get(
    "/tmdb_images/{item_id}",
    response_model=TmdbImageCatalogResponse,
    responses=_json_responses(400, 404, 500, 503),
    openapi_extra=_path_parameter("item_id"),
)
def tmdb_images(request: Request) -> JSONResponse:
    return _response(_service(request).tmdb_images(str(request.path_params.get("item_id") or "")))


@router.post(
    "/update_media_image/{item_id}",
    response_model=ImageUpdateResponse,
    responses=_json_responses(202, 400, 403, 500, 503),
    openapi_extra={**_path_parameter("item_id"), **_IMAGE_UPDATE_REQUEST_BODY},
)
async def update_media_image(request: Request) -> JSONResponse:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    service = _service(request)
    unavailable = service.unavailable()
    if unavailable is not None:
        return _response(unavailable)
    parsed, invalid = await _parse_image_update(request)
    if invalid is not None:
        return invalid
    assert parsed is not None
    return _response(
        service.update_image(
            str(request.path_params.get("item_id") or ""),
            image_type=parsed.image_type,
            image_url=parsed.image_url,
            image_bytes=parsed.image_bytes,
            content_type=parsed.content_type,
        )
    )


async def _parse_image_update(
    request: Request,
) -> tuple[_ParsedImageUpdate | None, JSONResponse | None]:
    content_type = request.headers.get("content-type", "").partition(";")[0].strip().casefold()
    if content_type == "application/json" or (
        content_type.startswith("application/") and content_type.endswith("+json")
    ):
        try:
            payload = await request.json()
        except (UnicodeDecodeError, ValueError):
            return None, fastapi_json_response({"error": "服务器内部错误"}, status_code=500)
        if not isinstance(payload, Mapping):
            return None, fastapi_json_response({"error": "服务器内部错误"}, status_code=500)
        return (
            _ParsedImageUpdate(
                image_type=payload.get("image_type"),
                image_url=payload.get("image_url"),
            ),
            None,
        )

    try:
        if content_type == "multipart/form-data":
            return _parse_multipart(request.headers.get("content-type", ""), await request.body()), None
        if content_type == "application/x-www-form-urlencoded":
            values = parse_qs((await request.body()).decode("utf-8"), keep_blank_values=True)
            return (
                _ParsedImageUpdate(
                    image_type=_first_form_value(values, "image_type"),
                    image_url=_first_form_value(values, "image_url"),
                ),
                None,
            )
    except (LookupError, UnicodeDecodeError, ValueError):
        return None, fastapi_json_response({"error": "请求体格式无效"}, status_code=400)
    return _ParsedImageUpdate(image_type="", image_url=""), None


def _parse_multipart(content_type: str, body: bytes) -> _ParsedImageUpdate:
    message = BytesParser(policy=policy.default).parsebytes(
        b"Content-Type: " + content_type.encode("latin-1") + b"\r\nMIME-Version: 1.0\r\n\r\n" + body
    )
    if not message.is_multipart():
        raise ValueError("multipart body is invalid")
    fields: dict[str, str] = {}
    image_bytes: bytes | None = None
    upload_content_type: str | None = None
    for part in message.iter_parts():
        name = str(part.get_param("name", header="content-disposition") or "")
        if not name:
            continue
        filename = part.get_filename()
        raw_payload = part.get_payload(decode=True)
        if raw_payload is None:
            payload = b""
        elif isinstance(raw_payload, bytes | bytearray):
            payload = bytes(raw_payload)
        else:
            raise ValueError("multipart part payload is invalid")
        if name == "file" and filename:
            image_bytes = payload
            upload_content_type = part.get_content_type()
            continue
        if name not in fields:
            charset = part.get_content_charset() or "utf-8"
            fields[name] = payload.decode(charset)
    return _ParsedImageUpdate(
        image_type=fields.get("image_type"),
        image_url=fields.get("image_url"),
        image_bytes=image_bytes,
        content_type=upload_content_type,
    )


def _first_form_value(values: Mapping[str, list[str]], name: str) -> str:
    items = values.get(name) or []
    return items[0] if items else ""


__all__ = ["router"]
