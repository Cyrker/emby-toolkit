from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Request
from starlette.concurrency import run_in_threadpool
from starlette.responses import Response
from werkzeug.exceptions import BadRequest, NotFound, UnsupportedMediaType
from werkzeug.http import http_date

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .activation import CollectionActivationService
from .service import CollectionHttpResult, CollectionService

router = APIRouter(tags=["collections"])
effects_router = APIRouter(tags=["collections"])
JsonModel = dict[str, Any] | list[Any]


def _request_body(schema: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": dict(schema)}},
        }
    }


_ANY_JSON_BODY = _request_body({})
_OBJECT_BODY = _request_body({"type": "object", "additionalProperties": True})
_ARRAY_BODY = _request_body({"type": "array", "items": {}})
_COLLECTION_SEARCH_QUERY = {
    "parameters": [
        {
            "name": "query",
            "in": "query",
            "required": False,
            "schema": {"type": "string", "default": ""},
        }
    ]
}
_COMPANY_SEARCH_QUERY = {
    "parameters": [
        {"name": "q", "in": "query", "required": False, "schema": {"type": "string", "default": ""}},
        {
            "name": "type",
            "in": "query",
            "required": False,
            "schema": {"type": "string", "default": "company"},
        },
    ]
}
_PERSON_SEARCH_QUERY = {
    "parameters": [{"name": "q", "in": "query", "required": False, "schema": {"type": "string", "default": ""}}]
}


def _responses(*codes: int) -> dict[int | str, dict[str, Any]]:
    return {code: {"description": "Legacy-compatible P8 collection response"} for code in codes}


def _service(request: Request) -> CollectionService:
    direct = getattr(request.app.state, "collection_service", None)
    if isinstance(direct, CollectionService):
        return direct
    value = request.app.state.container.use_case("collection")
    if not isinstance(value, CollectionService):
        raise TypeError("collection provider returned an invalid service")
    return value


def _activation_service(request: Request) -> CollectionActivationService:
    direct = getattr(request.app.state, "collection_activation_service", None)
    if isinstance(direct, CollectionActivationService):
        return direct
    value = request.app.state.container.use_case("collection.activation")
    if not isinstance(value, CollectionActivationService):
        raise TypeError("collection.activation provider returned an invalid service")
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


def _login_denial(request: Request) -> Response | None:
    container = request.app.state.container
    decision = PermissionPolicy(
        container.repository("identity"),
        auth_enabled=bool(container.settings.auth_enabled),
    ).require_any_login(get_request_session(request))
    if decision.allowed:
        return None
    return fastapi_json_response(
        {"status": "error", "message": decision.message},
        status_code=decision.status_code,
    )


def _response(result: CollectionHttpResult) -> Response:
    return fastapi_json_response(
        _legacy_json_value(result.payload),
        status_code=result.status_code,
        headers=dict(result.headers or {}),
    )


def _legacy_json_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return http_date(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _legacy_json_value(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_legacy_json_value(item) for item in value]
    return value


async def _json_value(request: Request) -> tuple[Any, Response | None]:
    content_type = request.headers.get("content-type", "").partition(";")[0].strip().casefold()
    if content_type != "application/json" and not (
        content_type.startswith("application/") and content_type.endswith("+json")
    ):
        return None, _html_error(UnsupportedMediaType())
    try:
        return await request.json(), None
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, _html_error(BadRequest())


async def _silent_json_value(request: Request) -> Any:
    """Match Flask's silent JSON parsing used by the plugin callback."""

    content_type = request.headers.get("content-type", "").partition(";")[0].strip().casefold()
    if content_type != "application/json" and not (
        content_type.startswith("application/") and content_type.endswith("+json")
    ):
        return {}
    try:
        return await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}


def _html_error(error: BadRequest | NotFound | UnsupportedMediaType) -> Response:
    headers = dict(error.get_headers())
    return Response(
        content=error.get_body(),
        status_code=int(error.code or 500),
        headers={"content-type": headers["Content-Type"]},
    )


def _integer(value: str) -> int | None:
    return int(value) if value.isdigit() else None


def _principal_id(request: Request) -> str:
    user_id = str(get_request_session(request).get("emby_user_id") or "").strip()
    return f"collection-admin:{user_id or 'local-setup'}"


async def _body_call(
    request: Request,
    method: Callable[..., CollectionHttpResult],
    *args: Any,
) -> Response:
    payload, error = await _json_value(request)
    if error is not None:
        return error
    return _response(await run_in_threadpool(method, *args, payload))


@router.post(
    "/api/emby/collections/activate",
    response_model=dict[str, Any],
    responses=_responses(400, 404, 409, 500),
    openapi_extra=_OBJECT_BODY,
)
async def api_activate_emby_collection(request: Request) -> Response:
    payload = await _silent_json_value(request)
    body, status_code = await run_in_threadpool(_activation_service(request).activate, payload)
    return fastapi_json_response(body, status_code=status_code)


@effects_router.get(
    "/api/collections/provider/metadata/{tmdb_collection_id}",
    response_model=dict[str, Any],
    responses=_responses(400, 404, 410, 500),
)
def api_get_collection_provider_metadata(request: Request, tmdb_collection_id: str) -> Response:
    return _response(_service(request).provider_metadata(tmdb_collection_id, str(request.base_url)))


@effects_router.get(
    "/api/collections/provider/search",
    response_model=list[dict[str, Any]],
    responses=_responses(500),
    openapi_extra=_COLLECTION_SEARCH_QUERY,
)
def api_search_collection_provider(request: Request) -> Response:
    return _response(_service(request).provider_search(request.query_params.get("query", "")))


@router.get("/api/collections/status", response_model=JsonModel, responses=_responses(403, 500))
def api_get_collections_status(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).native_status())


@effects_router.delete(
    "/api/collections/{emby_collection_id}",
    response_model=dict[str, Any],
    responses=_responses(202, 403, 409, 500),
)
def api_delete_collection(request: Request, emby_collection_id: str) -> Response:
    denied = _admin_denial(request)
    return denied or _response(
        _service(request).delete_native(
            emby_collection_id,
            principal_id=_principal_id(request),
            idempotency_key=request.headers.get("idempotency-key"),
        )
    )


@effects_router.get(
    "/api/collection-operations/{operation_id}/status",
    response_model=dict[str, Any],
    responses=_responses(400, 403, 404, 500),
)
def api_get_collection_operation_status(request: Request, operation_id: str) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).operation_status(operation_id))


@effects_router.post(
    "/api/collection-operations/{operation_id}/reconcile",
    response_model=dict[str, Any],
    responses=_responses(400, 403, 404, 409, 415, 500, 503),
    openapi_extra=_OBJECT_BODY,
)
async def api_reconcile_collection_operation(request: Request, operation_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_value(request)
    if error is not None:
        return error
    result = await run_in_threadpool(
        _service(request).reconcile_operation,
        operation_id,
        payload,
        resolved_by=_principal_id(request),
    )
    return _response(result)


@router.get("/api/collections/settings", response_model=dict[str, Any], responses=_responses(403, 500))
def api_get_collection_settings(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).native_settings())


@router.post(
    "/api/collections/settings",
    response_model=dict[str, Any],
    responses=_responses(400, 403, 415, 500),
    openapi_extra=_OBJECT_BODY,
)
async def api_save_collection_settings(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return await _body_call(request, _service(request).save_native_settings)


@router.get("/api/custom_collections/config/emby_users", response_model=JsonModel, responses=_responses(403, 500))
def api_get_emby_users(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).custom_users())


@router.get("/api/custom_collections", response_model=JsonModel, responses=_responses(403, 500))
def api_get_all_custom_collections(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).list_custom())


@router.post(
    "/api/custom_collections",
    response_model=JsonModel,
    status_code=201,
    responses=_responses(400, 403, 409, 415, 500),
    openapi_extra=_OBJECT_BODY,
)
async def api_create_custom_collection(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return await _body_call(request, _service(request).create_custom)


@router.put(
    "/api/custom_collections/{collection_id}",
    response_model=JsonModel,
    responses=_responses(400, 403, 404, 415, 500),
    openapi_extra=_OBJECT_BODY,
)
async def api_update_custom_collection(request: Request, collection_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    value = _integer(collection_id)
    if value is None:
        return _html_error(NotFound())
    return await _body_call(request, _service(request).update_custom, value)


@router.post(
    "/api/custom_collections/update_order",
    response_model=dict[str, Any],
    responses=_responses(400, 403, 415, 500),
    openapi_extra=_OBJECT_BODY,
)
async def api_update_custom_collections_order(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return await _body_call(request, _service(request).update_custom_order)


@effects_router.delete(
    "/api/custom_collections/{collection_id}",
    response_model=dict[str, Any],
    responses=_responses(202, 403, 404, 409, 500),
)
def api_delete_custom_collection(request: Request, collection_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    value = _integer(collection_id)
    return (
        _html_error(NotFound())
        if value is None
        else _response(
            _service(request).delete_custom(
                value,
                principal_id=_principal_id(request),
                idempotency_key=request.headers.get("idempotency-key"),
            )
        )
    )


@router.get(
    "/api/custom_collections/{collection_id}/status",
    response_model=JsonModel,
    responses=_responses(403, 404, 500),
)
def api_get_custom_collection_status(request: Request, collection_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    value = _integer(collection_id)
    return _html_error(NotFound()) if value is None else _response(_service(request).custom_status(value))


@effects_router.post(
    "/api/custom_collections/{collection_id}/fix_match",
    response_model=dict[str, Any],
    responses=_responses(400, 403, 404, 409, 415, 500),
    openapi_extra=_OBJECT_BODY,
)
async def api_fix_media_match_in_custom_collection(request: Request, collection_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    value = _integer(collection_id)
    if value is None:
        return _html_error(NotFound())
    return await _body_call(request, _service(request).fix_custom_match, value)


@router.get("/api/custom_collections/config/tags", response_model=JsonModel, responses=_responses(403, 500))
def api_get_tags_for_filter(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).filter_options("tags"))


@effects_router.get(
    "/api/custom_collections/config/emby_libraries",
    response_model=JsonModel,
    responses=_responses(403, 500),
)
def api_get_emby_libraries_for_filter(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).custom_libraries())


@effects_router.get(
    "/api/custom_collections/config/tmdb_movie_genres",
    response_model=JsonModel,
    responses=_responses(401, 500),
)
def api_get_tmdb_movie_genres(request: Request) -> Response:
    denied = _login_denial(request)
    return denied or _response(_service(request).tmdb_genres("movie"))


@effects_router.get(
    "/api/custom_collections/config/tmdb_tv_genres",
    response_model=JsonModel,
    responses=_responses(401, 500),
)
def api_get_tmdb_tv_genres(request: Request) -> Response:
    denied = _login_denial(request)
    return denied or _response(_service(request).tmdb_genres("tv"))


@effects_router.get(
    "/api/custom_collections/config/tmdb_search_companies",
    response_model=JsonModel,
    responses=_responses(401, 500),
    openapi_extra=_COMPANY_SEARCH_QUERY,
)
def api_search_tmdb_companies(request: Request) -> Response:
    denied = _login_denial(request)
    if denied is not None:
        return denied
    return _response(
        _service(request).search_companies(
            request.query_params.get("q", ""),
            request.query_params.get("type", "company"),
        )
    )


@effects_router.get(
    "/api/custom_collections/config/tmdb_search_persons",
    response_model=JsonModel,
    responses=_responses(401, 500),
    openapi_extra=_PERSON_SEARCH_QUERY,
)
def api_search_tmdb_persons(request: Request) -> Response:
    denied = _login_denial(request)
    return denied or _response(_service(request).search_people(request.query_params.get("q", "")))


@router.get("/api/custom_collections/config/movie_genres", response_model=JsonModel, responses=_responses(403, 500))
def api_get_movie_genres_config(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).filter_options("movie_genres"))


@router.get("/api/custom_collections/config/tv_genres", response_model=JsonModel, responses=_responses(403, 500))
def api_get_tv_genres_config(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).filter_options("tv_genres"))


def _public_mapping(request: Request, key: str, default: str) -> Response:
    return _response(_service(request).mapping(key, default))


async def _save_mapping(request: Request, key: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_value(request)
    if error is not None:
        return error
    return _response(await run_in_threadpool(_service(request).save_mapping, key, payload))


def _mapping_defaults(request: Request, key: str) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).mapping_defaults(key))


@router.get("/api/custom_collections/config/keyword_mapping", response_model=JsonModel)
def api_get_keyword_mapping(request: Request) -> Response:
    return _public_mapping(request, "keyword_mapping", "keyword_mapping")


@router.post(
    "/api/custom_collections/config/keyword_mapping",
    response_model=dict[str, Any],
    responses=_responses(403, 415, 500),
    openapi_extra=_ANY_JSON_BODY,
)
async def api_save_keyword_mapping(request: Request) -> Response:
    return await _save_mapping(request, "keyword_mapping")


@router.get(
    "/api/custom_collections/config/keyword_mapping/defaults", response_model=JsonModel, responses=_responses(403)
)
def api_get_keyword_defaults(request: Request) -> Response:
    return _mapping_defaults(request, "keyword_mapping")


@router.get("/api/custom_collections/config/release_group_mapping", response_model=JsonModel)
def api_get_release_group_mapping(request: Request) -> Response:
    return _public_mapping(request, "release_group_mapping", "release_group_mapping")


@router.post(
    "/api/custom_collections/config/release_group_mapping",
    response_model=JsonModel,
    responses=_responses(400, 403, 415, 500),
    openapi_extra=_ARRAY_BODY,
)
async def api_save_release_group_mapping(request: Request) -> Response:
    return await _save_mapping(request, "release_group_mapping")


@router.get(
    "/api/custom_collections/config/release_group_mapping/defaults", response_model=JsonModel, responses=_responses(403)
)
def api_get_release_group_defaults(request: Request) -> Response:
    return _mapping_defaults(request, "release_group_mapping")


@router.get("/api/custom_collections/config/junk_file_patterns", response_model=JsonModel)
def api_get_junk_file_patterns(request: Request) -> Response:
    return _public_mapping(request, "junk_file_patterns", "junk_file_patterns")


@router.post(
    "/api/custom_collections/config/junk_file_patterns",
    response_model=JsonModel,
    responses=_responses(400, 403, 415, 500),
    openapi_extra=_ARRAY_BODY,
)
async def api_save_junk_file_patterns(request: Request) -> Response:
    return await _save_mapping(request, "junk_file_patterns")


@router.get(
    "/api/custom_collections/config/junk_file_patterns/defaults", response_model=JsonModel, responses=_responses(403)
)
def api_get_junk_file_pattern_defaults(request: Request) -> Response:
    return _mapping_defaults(request, "junk_file_patterns")


@router.get("/api/custom_collections/config/keywords", response_model=JsonModel, responses=_responses(401, 500))
def api_get_keywords_for_filter(request: Request) -> Response:
    denied = _login_denial(request)
    return denied or _response(_service(request).filter_options("keywords"))


@router.get("/api/custom_collections/config/studio_mapping", response_model=JsonModel)
def api_get_studio_mapping(request: Request) -> Response:
    return _public_mapping(request, "studio_mapping", "studio_mapping")


@router.post(
    "/api/custom_collections/config/studio_mapping",
    response_model=dict[str, Any],
    responses=_responses(403, 415, 500),
    openapi_extra=_ANY_JSON_BODY,
)
async def api_save_studio_mapping(request: Request) -> Response:
    return await _save_mapping(request, "studio_mapping")


@router.get(
    "/api/custom_collections/config/studio_mapping/defaults", response_model=JsonModel, responses=_responses(403)
)
def api_get_studio_defaults(request: Request) -> Response:
    return _mapping_defaults(request, "studio_mapping")


@router.get("/api/custom_collections/config/studios", response_model=JsonModel, responses=_responses(401, 500))
def api_get_studios_for_filter(request: Request) -> Response:
    denied = _login_denial(request)
    return denied or _response(_service(request).filter_options("studios"))


@router.get("/api/custom_collections/config/country_mapping", response_model=JsonModel)
def api_get_country_mapping(request: Request) -> Response:
    return _public_mapping(request, "country_mapping", "country_mapping")


@router.post(
    "/api/custom_collections/config/country_mapping",
    response_model=dict[str, Any],
    responses=_responses(403, 415, 500),
    openapi_extra=_ANY_JSON_BODY,
)
async def api_save_country_mapping(request: Request) -> Response:
    return await _save_mapping(request, "country_mapping")


@router.get(
    "/api/custom_collections/config/country_mapping/defaults", response_model=JsonModel, responses=_responses(403)
)
def api_get_country_defaults(request: Request) -> Response:
    return _mapping_defaults(request, "country_mapping")


@router.get("/api/custom_collections/config/tmdb_countries", response_model=JsonModel, responses=_responses(401, 500))
def api_get_tmdb_countries(request: Request) -> Response:
    denied = _login_denial(request)
    return denied or _response(_service(request).filter_options("countries"))


@router.get("/api/custom_collections/config/language_mapping", response_model=JsonModel)
def api_get_language_mapping(request: Request) -> Response:
    return _public_mapping(request, "language_mapping", "language_mapping")


@router.post(
    "/api/custom_collections/config/language_mapping",
    response_model=dict[str, Any],
    responses=_responses(403, 415, 500),
    openapi_extra=_ANY_JSON_BODY,
)
async def api_save_language_mapping(request: Request) -> Response:
    return await _save_mapping(request, "language_mapping")


@router.get(
    "/api/custom_collections/config/language_mapping/defaults", response_model=JsonModel, responses=_responses(403)
)
def api_get_language_defaults(request: Request) -> Response:
    return _mapping_defaults(request, "language_mapping")


@router.get("/api/custom_collections/config/languages", response_model=JsonModel, responses=_responses(401, 500))
def api_get_languages_for_filter(request: Request) -> Response:
    denied = _login_denial(request)
    return denied or _response(_service(request).filter_options("languages"))


@router.get("/api/custom_collections/config/unified_ratings", response_model=JsonModel)
def api_get_unified_ratings(request: Request) -> Response:
    return _public_mapping(request, "unified_ratings", "unified_ratings")


@router.post(
    "/api/custom_collections/config/unified_ratings",
    response_model=dict[str, Any],
    responses=_responses(403, 415, 500),
    openapi_extra=_ANY_JSON_BODY,
)
async def api_save_unified_ratings(request: Request) -> Response:
    return await _save_mapping(request, "unified_ratings")


@router.get(
    "/api/custom_collections/config/unified_ratings/defaults", response_model=JsonModel, responses=_responses(403)
)
def api_get_unified_ratings_defaults(request: Request) -> Response:
    return _mapping_defaults(request, "unified_ratings")


@router.get("/api/custom_collections/config/rating_mapping", response_model=JsonModel)
def api_get_rating_mapping(request: Request) -> Response:
    return _public_mapping(request, "rating_mapping", "rating_mapping")


@router.post(
    "/api/custom_collections/config/rating_mapping",
    response_model=JsonModel,
    responses=_responses(400, 403, 415, 500),
    openapi_extra=_OBJECT_BODY,
)
async def api_save_rating_mapping(request: Request) -> Response:
    return await _save_mapping(request, "rating_mapping")


@router.get(
    "/api/custom_collections/config/rating_mapping/defaults", response_model=JsonModel, responses=_responses(403)
)
def api_get_rating_mapping_defaults(request: Request) -> Response:
    return _mapping_defaults(request, "rating_mapping")


@router.get("/api/custom_collections/config/rating_priority", response_model=JsonModel)
def api_get_rating_priority(request: Request) -> Response:
    return _public_mapping(request, "rating_priority", "rating_priority")


@router.post(
    "/api/custom_collections/config/rating_priority",
    response_model=JsonModel,
    responses=_responses(400, 403, 415, 500),
    openapi_extra=_ARRAY_BODY,
)
async def api_save_rating_priority(request: Request) -> Response:
    return await _save_mapping(request, "rating_priority")


@router.get(
    "/api/custom_collections/config/rating_priority/defaults", response_model=JsonModel, responses=_responses(403)
)
def api_get_rating_priority_defaults(request: Request) -> Response:
    return _mapping_defaults(request, "rating_priority")


@router.get(
    "/api/custom_collections/config/unified_ratings_options", response_model=JsonModel, responses=_responses(403, 500)
)
def api_get_unified_ratings_for_filter(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).filter_options("unified_ratings"))


@router.get("/api/config/cover_generator", response_model=JsonModel, responses=_responses(403, 500))
def get_cover_generator_config(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).cover_config())


@router.post(
    "/api/config/cover_generator",
    response_model=dict[str, Any],
    responses=_responses(403, 415, 500),
    openapi_extra=_ANY_JSON_BODY,
)
async def save_cover_generator_config(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_value(request)
    if error is not None:
        if error.status_code == 400:
            return _response(CollectionHttpResult({"error": "保存配置失败"}, 500))
        return error
    return _response(await run_in_threadpool(_service(request).save_cover_config, payload))


@effects_router.get(
    "/api/config/cover_generator/libraries",
    response_model=JsonModel,
    responses=_responses(403, 500),
)
def get_all_libraries(request: Request) -> Response:
    denied = _admin_denial(request)
    return denied or _response(_service(request).cover_libraries())


@router.post(
    "/api/config/cover_generator/preview",
    response_model=dict[str, Any],
    responses=_responses(400, 403, 415, 500),
    openapi_extra=_OBJECT_BODY,
)
async def api_generate_cover_preview(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return await _body_call(request, _service(request).cover_preview)


__all__ = ["effects_router", "router"]
