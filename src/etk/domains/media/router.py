from __future__ import annotations

import json
from typing import Any, Literal

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel, ConfigDict, RootModel
from starlette.responses import JSONResponse, StreamingResponse

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .bridge_client import HttpxPluginUpdateGateway, PluginUpdateError
from .models import MediaItemType, ServiceResult
from .search_index import EmbySearchIndexService
from .service import MediaService

router = APIRouter(prefix="/api/emby", tags=["media", "bridge"])


class ExternalObjectResponse(RootModel[dict[str, Any]]):
    pass


class BackfillItemRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    emby_item_id: str | None = None


class BackfillItemsRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    emby_item_ids: list[str] | None = None


class BackfillLibraryRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    emby_library_id: str | int | None = None
    refresh_mode: str | None = None


class ImageSyncRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    rules: list[dict[str, Any]] | None = None
    force: bool | None = None


class EmbyEventRequest(RootModel[dict[str, Any]]):
    pass


class SearchIndexRequest(RootModel[dict[str, Any]]):
    pass


def _json_responses(*statuses: int) -> dict[int | str, dict[str, Any]]:
    return {status: {"model": ExternalObjectResponse} for status in statuses}


def _request_body(model: type[BaseModel]) -> dict[str, Any]:
    return {
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": model.model_json_schema()}},
        }
    }


def _query_parameters(*names: str) -> dict[str, Any]:
    integer_names = {"season_number", "episode_number"}
    boolean_names = {"include_all_languages"}
    return {
        "parameters": [
            {
                "name": name,
                "in": "query",
                "required": False,
                "schema": {
                    "type": ("integer" if name in integer_names else "boolean" if name in boolean_names else "string")
                },
            }
            for name in names
        ]
    }


def _service(request: Request) -> MediaService:
    value = request.app.state.container.use_case("media")
    if not isinstance(value, MediaService):
        raise TypeError("media provider returned an invalid service")
    return value


def _plugin_updates(request: Request) -> HttpxPluginUpdateGateway:
    value = request.app.state.container.client("media-plugin-updates")
    if not isinstance(value, HttpxPluginUpdateGateway):
        raise TypeError("plugin update provider returned an invalid gateway")
    return value


def _search_index(request: Request) -> EmbySearchIndexService:
    value = request.app.state.container.use_case("media.search-index")
    if not isinstance(value, EmbySearchIndexService):
        raise TypeError("media search-index provider returned an invalid service")
    return value


def _response(result: ServiceResult) -> JSONResponse:
    return fastapi_json_response(
        result.payload,
        status_code=result.status_code,
        headers=result.headers,
    )


@router.get("/scrape-records", response_model=dict[str, Any])
def scrape_records(
    request: Request,
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=100),
    search: str = Query("", max_length=200),
    status: Literal["", "running", "cancelled", "failed", "incomplete", "skipped", "completed"] = "",
) -> JSONResponse:
    container = request.app.state.container
    policy = PermissionPolicy(container.repository("identity"), auth_enabled=container.settings.auth_enabled)
    decision = policy.require_admin(get_request_session(request))
    if not decision.allowed:
        return fastapi_json_response({"error": decision.message}, status_code=decision.status_code)
    return _response(_service(request).scrape_records(page=page, per_page=per_page, search=search, status=status))


async def _json_object(request: Request) -> dict[str, Any]:
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _number(request: Request, name: str) -> int | None:
    value = request.query_params.get(name)
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _truthy(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _image_request(request: Request, action: str) -> tuple[MediaItemType | None, str, str] | JSONResponse:
    raw_type = str(request.query_params.get("item_type") or "").strip()
    item_type = MediaItemType.parse(raw_type, allow_boxset=True)
    path = str(request.query_params.get("path") or "").strip()
    tmdb_id = str(request.query_params.get("tmdb_id") or "").strip()

    def invalid(message: str) -> JSONResponse:
        payload = {"ok": False, "error": message} if action == "refresh" else {"error": message}
        return fastapi_json_response(payload, status_code=400)

    if item_type is None:
        return invalid(f"invalid image {action} request")
    if item_type is MediaItemType.BOXSET and not tmdb_id.isdigit():
        return invalid("invalid collection tmdb id")
    if item_type is not MediaItemType.BOXSET and not path:
        return invalid(f"invalid image {action} request")
    return item_type, path, tmdb_id


@router.get(
    "/plugin-update",
    response_model=None,
    response_class=StreamingResponse,
    responses={
        200: {"content": {"application/octet-stream": {"schema": {"type": "string", "format": "binary"}}}},
        502: {"model": ExternalObjectResponse},
    },
)
def plugin_update(request: Request) -> StreamingResponse | JSONResponse:
    try:
        artifact = _plugin_updates(request).download()
    except PluginUpdateError:
        return fastapi_json_response({"error": "plugin_update_download_failed"}, status_code=502)
    headers = {
        "Content-Disposition": 'attachment; filename="ETKMediaInfoBridge.dll"',
        "Cache-Control": "no-store",
    }
    if artifact.content_length:
        headers["Content-Length"] = artifact.content_length
    return StreamingResponse(
        artifact.chunks,
        media_type="application/octet-stream",
        headers=headers,
    )


@router.get(
    "/metadata",
    response_model=ExternalObjectResponse,
    responses=_json_responses(400, 404),
    openapi_extra=_query_parameters("path", "item_type", "season_number", "episode_number"),
)
def metadata(request: Request) -> JSONResponse:
    path = str(request.query_params.get("path") or "").strip()
    item_type = MediaItemType.parse(request.query_params.get("item_type"))
    if not path or item_type is None:
        return fastapi_json_response({"error": "invalid metadata path request"}, status_code=400)
    return _response(
        _service(request).provide_bridge_metadata(
            path,
            item_type,
            season_number=_number(request, "season_number"),
            episode_number=_number(request, "episode_number"),
            base_url=str(request.base_url),
        )
    )


@router.post(
    "/metadata/backfill",
    status_code=202,
    response_model=ExternalObjectResponse,
    responses=_json_responses(200, 409),
)
def backfill_all(request: Request) -> JSONResponse:
    return _response(_service(request).backfill_all())


@router.post(
    "/metadata/backfill/item",
    status_code=202,
    response_model=ExternalObjectResponse,
    responses=_json_responses(200, 400, 409),
    openapi_extra=_request_body(BackfillItemRequest),
)
async def backfill_item(request: Request) -> JSONResponse:
    payload = await _json_object(request)
    item_id = str(payload.get("emby_item_id") or "").strip()
    if not item_id:
        return fastapi_json_response({"ok": False, "error": "emby_item_id is required"}, status_code=400)
    return _response(_service(request).backfill_items([item_id]))


@router.post(
    "/metadata/backfill/items",
    status_code=202,
    response_model=ExternalObjectResponse,
    responses=_json_responses(200, 400, 409),
    openapi_extra=_request_body(BackfillItemsRequest),
)
async def backfill_items(request: Request) -> JSONResponse:
    payload = await _json_object(request)
    values = payload.get("emby_item_ids")
    item_ids = [str(value).strip() for value in values if str(value or "").strip()] if isinstance(values, list) else []
    if not item_ids:
        return fastapi_json_response({"ok": False, "error": "emby_item_ids is required"}, status_code=400)
    return _response(_service(request).backfill_items(item_ids))


@router.post(
    "/metadata/backfill/library",
    status_code=202,
    response_model=ExternalObjectResponse,
    responses=_json_responses(200, 400, 409),
    openapi_extra=_request_body(BackfillLibraryRequest),
)
async def backfill_library(request: Request) -> JSONResponse:
    payload = await _json_object(request)
    return _response(
        _service(request).backfill_library(
            str(payload.get("emby_library_id") or "").strip(),
            str(payload.get("refresh_mode") or "").strip(),
        )
    )


@router.get(
    "/metadata/images/search",
    response_model=ExternalObjectResponse,
    responses=_json_responses(400, 404, 502, 503),
    openapi_extra=_query_parameters(
        "item_type",
        "path",
        "tmdb_id",
        "season_number",
        "episode_number",
        "include_all_languages",
    ),
)
def search_images(request: Request) -> JSONResponse:
    parsed = _image_request(request, "search")
    if isinstance(parsed, JSONResponse):
        return parsed
    item_type, path, tmdb_id = parsed
    assert item_type is not None
    return _response(
        _service(request).search_bridge_images(
            item_type,
            path=path,
            tmdb_id=tmdb_id,
            season_number=_number(request, "season_number"),
            episode_number=_number(request, "episode_number"),
            include_all_languages=_truthy(request.query_params.get("include_all_languages")),
        )
    )


@router.post(
    "/metadata/images/refresh",
    response_model=ExternalObjectResponse,
    responses=_json_responses(400, 404, 500, 502, 503),
    openapi_extra=_query_parameters("item_type", "path", "tmdb_id", "season_number", "episode_number"),
)
def refresh_images(request: Request) -> JSONResponse:
    parsed = _image_request(request, "refresh")
    if isinstance(parsed, JSONResponse):
        return parsed
    item_type, path, tmdb_id = parsed
    assert item_type is not None
    return _response(
        _service(request).refresh_bridge_images(
            item_type,
            path=path,
            tmdb_id=tmdb_id,
            season_number=_number(request, "season_number"),
            episode_number=_number(request, "episode_number"),
        )
    )


@router.post(
    "/metadata/images/sync",
    response_model=ExternalObjectResponse,
    responses=_json_responses(400, 404, 410, 502),
    openapi_extra={
        **_query_parameters("item_type", "path", "tmdb_id", "season_number", "episode_number"),
        **_request_body(ImageSyncRequest),
    },
)
async def sync_images(request: Request) -> JSONResponse:
    parsed = _image_request(request, "sync")
    if isinstance(parsed, JSONResponse):
        return parsed
    item_type, path, tmdb_id = parsed
    assert item_type is not None
    payload = await _json_object(request)
    raw_rules = payload.get("rules")
    rules = [dict(value) for value in raw_rules if isinstance(value, dict)] if isinstance(raw_rules, list) else []
    return _response(
        _service(request).sync_image_policy(
            item_type,
            path=path,
            tmdb_id=tmdb_id,
            season_number=_number(request, "season_number"),
            episode_number=_number(request, "episode_number"),
            rules=rules,
            force=bool(payload.get("force")),
            base_url=str(request.base_url),
        )
    )


@router.get(
    "/search",
    response_model=ExternalObjectResponse,
    responses=_json_responses(503),
    openapi_extra=_query_parameters("query", "types", "limit"),
)
def search_index(request: Request) -> JSONResponse:
    raw_types = str(request.query_params.get("types") or "")
    item_types = [value.strip() for value in raw_types.split(",") if value.strip()]
    try:
        limit = int(request.query_params.get("limit") or 300)
    except (TypeError, ValueError):
        limit = 300
    return _response(
        _search_index(request).search(
            str(request.query_params.get("query") or "").strip(),
            item_types,
            limit,
        )
    )


@router.post(
    "/search/index",
    response_model=ExternalObjectResponse,
    responses=_json_responses(400, 503),
    openapi_extra=_request_body(SearchIndexRequest),
)
async def update_search_index(request: Request) -> JSONResponse:
    return _response(_search_index(request).update(await _json_object(request)))


@router.post(
    "/events",
    status_code=202,
    response_model=ExternalObjectResponse,
    responses=_json_responses(200, 400, 500, 503),
    openapi_extra=_request_body(EmbyEventRequest),
)
async def events(request: Request) -> JSONResponse:
    payload = await _json_object(request)
    return _response(
        _service(request).handle_emby_event(
            payload,
            host_url=str(request.base_url),
            remote_addr=request.client.host if request.client is not None else "",
        )
    )


__all__ = ["router"]
