from __future__ import annotations

import os
from email.utils import formatdate, parsedate
from pathlib import Path
from time import time
from typing import Any
from zlib import adler32

from fastapi import APIRouter, Request
from starlette.datastructures import Headers
from starlette.responses import FileResponse, Response, StreamingResponse

from etk.compatibility.response_contract import fastapi_json_response

from .image_proxy import (
    DEFAULT_UPSTREAM_CONTENT_TYPE,
    IMMUTABLE_IMAGE_CACHE_CONTROL,
    CachedImage,
    ImageProxyBytesResponse,
    ImageProxyJsonResponse,
    ImageProxyResult,
    MediaImageProxyService,
    UpstreamImage,
)

router = APIRouter(tags=["media", "image-proxy"])


def _service(request: Request) -> MediaImageProxyService:
    direct = getattr(request.app.state, "media_image_proxy_service", None)
    if isinstance(direct, MediaImageProxyService):
        return direct
    value = request.app.state.container.use_case("media.image-proxy")
    if not isinstance(value, MediaImageProxyService):
        raise TypeError("media.image-proxy provider returned an invalid service")
    return value


def _response(request: Request, result: ImageProxyResult) -> Response:
    if isinstance(result, ImageProxyJsonResponse):
        return fastapi_json_response(result.payload, status_code=result.status_code)
    if isinstance(result, ImageProxyBytesResponse):
        return Response(
            result.content,
            status_code=result.status_code,
            headers={"Content-Type": result.content_type},
        )
    if isinstance(result, CachedImage):
        return _cached_response(request, result)
    if isinstance(result, UpstreamImage):
        return StreamingResponse(
            result.chunks,
            status_code=result.status_code,
            headers={"Content-Type": result.content_type or DEFAULT_UPSTREAM_CONTENT_TYPE},
        )
    raise TypeError("unsupported image proxy result")


def _cached_response(request: Request, result: CachedImage) -> Response:
    path = Path(result.path)
    stat_result = path.stat()
    path_check = adler32(os.fsencode(path)) & 0xFFFFFFFF
    response = FileResponse(
        path,
        media_type=result.content_type,
        headers={
            "Cache-Control": IMMUTABLE_IMAGE_CACHE_CONTROL,
            "Date": formatdate(time(), usegmt=True),
            "ETag": f'"{stat_result.st_mtime}-{stat_result.st_size}-{path_check}"',
        },
        filename=path.name,
        content_disposition_type="inline",
        stat_result=stat_result,
    )
    if _is_not_modified(response.headers, request.headers):
        preserved = {
            name: value
            for name, value in response.headers.items()
            if name in {"accept-ranges", "cache-control", "content-disposition", "date", "etag"}
        }
        return Response(status_code=304, headers=preserved)
    return response


def _is_not_modified(response_headers: Headers, request_headers: Headers) -> bool:
    if if_none_match := request_headers.get("if-none-match"):
        etag = response_headers["etag"]
        return etag in [tag.strip().removeprefix("W/") for tag in if_none_match.split(",")]
    try:
        if_modified_since = parsedate(request_headers["if-modified-since"])
        last_modified = parsedate(response_headers["last-modified"])
    except KeyError:
        return False
    return if_modified_since is not None and last_modified is not None and if_modified_since >= last_modified


def _external_query_parameters() -> dict[str, Any]:
    return {
        "parameters": [
            {
                "name": name,
                "in": "query",
                "required": False,
                "schema": {"type": "string"},
            }
            for name in ("cache", "url")
        ]
    }


def _image_path_parameter() -> dict[str, Any]:
    return {
        "parameters": [
            {
                "name": "image_path",
                "in": "path",
                "required": True,
                "schema": {"type": "string"},
            }
        ]
    }


_BINARY_CONTENT = {
    "image/png": {"schema": {"type": "string", "format": "binary"}},
    "application/octet-stream": {"schema": {"type": "string", "format": "binary"}},
}


_BINARY_RESPONSES: dict[int | str, dict[str, Any]] = {
    200: {
        "content": _BINARY_CONTENT,
    },
    "2XX": {"description": "Other successful upstream status"},
    "3XX": {"description": "Upstream redirect or conditional response"},
}


@router.get(
    "/api/image_proxy",
    response_model=None,
    response_class=Response,
    responses={
        **_BINARY_RESPONSES,
        206: {"content": _BINARY_CONTENT},
        304: {"description": "Not Modified"},
        400: {"content": {"application/json": {}}},
        404: {"content": {"application/json": {}, "image/png": {}}},
        416: {"content": {"text/plain": {"schema": {"type": "string"}}}},
    },
    openapi_extra=_external_query_parameters(),
)
def proxy_external_image(request: Request) -> Response:
    return _response(
        request,
        _service(request).external_image(
            cache_key=request.query_params.get("cache"),
            external_url=request.query_params.get("url"),
        ),
    )


@router.get(
    "/image_proxy/{image_path:path}",
    response_model=None,
    response_class=Response,
    responses={
        **_BINARY_RESPONSES,
        404: {"content": {"application/json": {}}},
        503: {"content": {"application/json": {}}},
    },
    openapi_extra=_image_path_parameter(),
)
def proxy_emby_image(request: Request) -> Response:
    image_path = str(request.path_params.get("image_path") or "")
    if not image_path:
        return fastapi_json_response({"detail": "Not Found"}, status_code=404)
    return _response(
        request,
        _service(request).emby_image(
            image_path,
            raw_query=request.scope["query_string"],
        ),
    )


__all__ = ["router"]
