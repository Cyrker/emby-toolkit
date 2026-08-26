from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Protocol

from fastapi import APIRouter, Request
from starlette.concurrency import run_in_threadpool
from starlette.responses import JSONResponse, PlainTextResponse, RedirectResponse, Response, StreamingResponse
from werkzeug.exceptions import BadRequest, InternalServerError, UnsupportedMediaType

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .playback import PlaybackDirectResponse

router = APIRouter(prefix="/api/p115", tags=["p115-playback"])


class PlaybackHttpService(Protocol):
    def get_public_config(self) -> dict[str, Any]: ...

    def save_pool_settings(self, payload: Mapping[str, Any]) -> dict[str, Any]: ...

    def add_account(self, payload: Mapping[str, Any]) -> dict[str, Any]: ...

    def update_account(self, account_id: str, payload: Mapping[str, Any]) -> dict[str, Any]: ...

    def delete_account(self, account_id: str) -> bool: ...

    def speedtest_account(self, account_id: str, intent_key: str = "") -> dict[str, Any]: ...

    def cleanup(self) -> int: ...

    def save_user_account(self, user_id: str, username: str, payload: Mapping[str, Any]) -> dict[str, Any]: ...

    def get_user_account(self, user_id: str) -> dict[str, Any]: ...

    def delete_user_account(self, user_id: str) -> bool: ...

    def reward_summary(self, user_id: str) -> dict[str, Any]: ...

    def get_temp_config(self) -> dict[str, Any]: ...

    def save_temp_config(self, payload: Mapping[str, Any]) -> dict[str, Any]: ...

    def direct_play(self, pick_code: str, user_agent: str, range_header: str) -> PlaybackDirectResponse: ...


class PlaybackQrHttpService(Protocol):
    def create(self, owner_user_id: str, *, mode: str, app_type: str) -> dict[str, Any]: ...

    def poll(self, owner_user_id: str, *, provider_uid: str = "") -> dict[str, Any]: ...


_OBJECT_SCHEMA: dict[str, Any] = {"type": "object", "additionalProperties": True}
_JSON_RESPONSES: dict[int | str, dict[str, Any]] = {
    200: {"content": {"application/json": {"schema": _OBJECT_SCHEMA}}},
    400: {"content": {"application/json": {"schema": _OBJECT_SCHEMA}}},
    401: {"content": {"application/json": {"schema": _OBJECT_SCHEMA}}},
    403: {"content": {"application/json": {"schema": _OBJECT_SCHEMA}}},
    500: {"content": {"application/json": {"schema": _OBJECT_SCHEMA}}},
}
_JSON_BODY_RESPONSES: dict[int | str, dict[str, Any]] = {
    **_JSON_RESPONSES,
    400: {
        "content": {
            "application/json": {"schema": _OBJECT_SCHEMA},
            "text/html": {"schema": {"type": "string"}},
        }
    },
    415: {"content": {"text/html": {"schema": {"type": "string"}}}},
}
_BODY: dict[str, Any] = {
    "requestBody": {
        "required": True,
        "content": {"application/json": {"schema": _OBJECT_SCHEMA}},
    }
}


def _path_parameters(*names: str) -> dict[str, Any]:
    return {
        "parameters": [{"name": name, "in": "path", "required": True, "schema": {"type": "string"}} for name in names]
    }


def _query_parameters(*names: str) -> dict[str, Any]:
    return {
        "parameters": [{"name": name, "in": "query", "required": False, "schema": {"type": "string"}} for name in names]
    }


def _speedtest_parameters() -> dict[str, Any]:
    return {
        "parameters": [
            *_path_parameters("account_id")["parameters"],
            {
                "name": "Idempotency-Key",
                "in": "header",
                "required": False,
                "schema": {"type": "string"},
            },
        ]
    }


def _service(request: Request) -> PlaybackHttpService:
    direct = getattr(request.app.state, "p115_playback_service", None)
    if direct is not None:
        return direct
    return request.app.state.container.use_case("p115.playback")


def _qr_service(request: Request) -> PlaybackQrHttpService:
    direct = getattr(request.app.state, "p115_playback_qr_service", None)
    if direct is not None:
        return direct
    return request.app.state.container.use_case("p115.playback-qr")


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


def _emby_user(request: Request) -> tuple[str, JSONResponse | None]:
    session = get_request_session(request)
    if "emby_user_id" in session:
        return str(session.get("emby_user_id") or "").strip(), None
    return "", fastapi_json_response(
        {"status": "error", "message": "需要 Emby 用户登录才能访问此资源"},
        status_code=401,
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


async def _legacy_json_object(request: Request) -> tuple[dict[str, Any] | None, Response | None]:
    if not _is_json_content_type(request.headers.get("content-type", "")):
        return None, _html_error(UnsupportedMediaType())
    raw = await request.body()
    if not raw:
        return None, _html_error(BadRequest())
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, _html_error(BadRequest())
    payload = payload or {}
    if not isinstance(payload, Mapping):
        return None, _html_error(InternalServerError())
    return dict(payload), None


def _json(payload: Mapping[str, Any], status_code: int = 200) -> JSONResponse:
    return fastapi_json_response(dict(payload), status_code=status_code)


def _direct_response(result: PlaybackDirectResponse) -> Response:
    if result.kind == "redirect":
        response = RedirectResponse(result.location, status_code=result.status_code)
        response.headers["Access-Control-Allow-Origin"] = "*"
        return response
    stream = result.stream
    if result.kind == "stream" and stream is not None:

        def closing_chunks():
            try:
                yield from stream.chunks
            finally:
                stream.close()

        return StreamingResponse(
            closing_chunks(),
            status_code=stream.status_code,
            headers=dict(stream.headers),
        )
    return PlainTextResponse(result.body, status_code=result.status_code)


_PLAY_RESPONSES: dict[int | str, dict[str, Any]] = {
    200: {"content": {"text/plain": {"schema": {"type": "string"}}}},
    206: {"description": "Upstream partial-content response"},
    302: {"description": "115 direct-link redirect"},
    404: {"content": {"text/plain": {"schema": {"type": "string"}}}},
    500: {"content": {"text/plain": {"schema": {"type": "string"}}}},
    "default": {"description": "Upstream status, headers, and body are passed through"},
}


@router.get(
    "/play/{pick_code}",
    response_class=Response,
    response_model=None,
    responses=_PLAY_RESPONSES,
    openapi_extra=_path_parameters("pick_code"),
)
async def direct_play(request: Request) -> Response:
    result = await run_in_threadpool(
        _service(request).direct_play,
        str(request.path_params.get("pick_code") or ""),
        str(request.headers.get("user-agent") or ""),
        str(request.headers.get("range") or ""),
    )
    return _direct_response(result)


@router.head(
    "/play/{pick_code}",
    response_class=Response,
    response_model=None,
    responses={200: {"description": "Legacy probe response"}},
)
def direct_play_head(request: Request) -> Response:
    return Response(status_code=200)


@router.get(
    "/play/{pick_code}/{filename:path}",
    response_class=Response,
    response_model=None,
    responses=_PLAY_RESPONSES,
    openapi_extra=_path_parameters("pick_code", "filename"),
)
async def direct_play_with_filename(request: Request) -> Response:
    result = await run_in_threadpool(
        _service(request).direct_play,
        str(request.path_params.get("pick_code") or ""),
        str(request.headers.get("user-agent") or ""),
        str(request.headers.get("range") or ""),
    )
    return _direct_response(result)


@router.head(
    "/play/{pick_code}/{filename:path}",
    response_class=Response,
    response_model=None,
    responses={200: {"description": "Legacy probe response"}},
)
def direct_play_with_filename_head(request: Request) -> Response:
    return Response(status_code=200)


@router.get("/play_pool", response_model=None, responses=_JSON_RESPONSES)
async def get_play_pool(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _json({"success": True, "data": await run_in_threadpool(_service(request).get_public_config)})


@router.post("/play_pool", response_model=None, responses=_JSON_BODY_RESPONSES, openapi_extra=_BODY)
async def save_play_pool(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, invalid = await _legacy_json_object(request)
    if invalid is not None:
        return invalid
    assert payload is not None
    data = await run_in_threadpool(_service(request).save_pool_settings, payload)
    return _json({"success": True, "data": data})


@router.post("/play_pool/accounts", response_model=None, responses=_JSON_BODY_RESPONSES, openapi_extra=_BODY)
async def add_play_pool_account(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, invalid = await _legacy_json_object(request)
    if invalid is not None:
        return invalid
    assert payload is not None
    if not str(payload.get("cookie") or "").strip() and not str(payload.get("access_token") or "").strip():
        return _json({"success": False, "message": "Cookie/OpenAPI 不能为空"}, 400)
    data = await run_in_threadpool(_service(request).add_account, payload)
    return _json({"success": True, "data": data})


@router.put(
    "/play_pool/accounts/{account_id}",
    response_model=None,
    responses=_JSON_BODY_RESPONSES,
    openapi_extra={**_path_parameters("account_id"), **_BODY},
)
async def update_play_pool_account(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, invalid = await _legacy_json_object(request)
    if invalid is not None:
        return invalid
    assert payload is not None
    data = await run_in_threadpool(
        _service(request).update_account,
        str(request.path_params.get("account_id") or ""),
        payload,
    )
    return _json({"success": True, "data": data})


@router.delete(
    "/play_pool/accounts/{account_id}",
    response_model=None,
    responses=_JSON_RESPONSES,
    openapi_extra=_path_parameters("account_id"),
)
async def delete_play_pool_account(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    deleted = await run_in_threadpool(
        _service(request).delete_account,
        str(request.path_params.get("account_id") or ""),
    )
    return _json({"success": bool(deleted)})


@router.post(
    "/play_pool/accounts/{account_id}/speedtest",
    response_model=None,
    responses=_JSON_RESPONSES,
    openapi_extra=_speedtest_parameters(),
)
async def speedtest_play_pool_account(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    try:
        data = await run_in_threadpool(
            _service(request).speedtest_account,
            str(request.path_params.get("account_id") or ""),
            str(request.headers.get("idempotency-key") or "").strip(),
        )
    except Exception as exc:
        return _json({"success": False, "message": str(exc)}, 500)
    return _json({"success": True, "data": data})


@router.post("/play_pool/cleanup", response_model=None, responses=_JSON_RESPONSES)
async def cleanup_play_pool(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    removed = await run_in_threadpool(_service(request).cleanup)
    return _json({"success": True, "removed": int(removed)})


@router.get(
    "/play_pool/cookie_qrcode",
    response_model=None,
    responses=_JSON_RESPONSES,
    openapi_extra=_query_parameters("mode", "app"),
)
async def create_play_pool_qrcode(request: Request) -> Response:
    user_id, denied = _emby_user(request)
    if denied is not None:
        return denied
    mode = str(request.query_params.get("mode") or "cookie").strip().casefold()
    app_type = str(request.query_params.get("app") or "alipaymini")
    try:
        result = await run_in_threadpool(
            _qr_service(request).create,
            user_id,
            mode=mode,
            app_type=app_type,
        )
    except Exception as exc:
        return _json({"success": False, "message": str(exc)}, 500)
    return _json(result)


@router.get(
    "/play_pool/cookie_qrcode/status",
    response_model=None,
    responses=_JSON_RESPONSES,
    openapi_extra=_query_parameters("mode", "app", "uid"),
)
async def poll_play_pool_qrcode(request: Request) -> Response:
    user_id, denied = _emby_user(request)
    if denied is not None:
        return denied
    try:
        result = await run_in_threadpool(
            _qr_service(request).poll,
            user_id,
            provider_uid=str(request.query_params.get("uid") or "").strip(),
        )
    except Exception as exc:
        return _json({"success": False, "status": "error", "message": str(exc)}, 500)
    return _json(result)


@router.post("/play_pool/user-account", response_model=None, responses=_JSON_BODY_RESPONSES, openapi_extra=_BODY)
async def save_user_play_pool_account(request: Request) -> Response:
    user_id, denied = _emby_user(request)
    if denied is not None:
        return denied
    if not user_id:
        return _json({"success": False, "message": "未登录"}, 401)
    payload, invalid = await _legacy_json_object(request)
    if invalid is not None:
        return invalid
    assert payload is not None
    username = str(get_request_session(request).get("emby_username") or "").strip()
    try:
        data = await run_in_threadpool(_service(request).save_user_account, user_id, username, payload)
    except ValueError as exc:
        return _json({"success": False, "message": str(exc)}, 400)
    return _json({"success": True, "data": data})


@router.get("/play_pool/user-account", response_model=None, responses=_JSON_RESPONSES)
async def get_user_play_pool_account(request: Request) -> Response:
    user_id, denied = _emby_user(request)
    if denied is not None:
        return denied
    data = await run_in_threadpool(_service(request).get_user_account, user_id)
    return _json({"success": True, "data": data})


@router.delete("/play_pool/user-account", response_model=None, responses=_JSON_RESPONSES)
async def delete_user_play_pool_account(request: Request) -> Response:
    user_id, denied = _emby_user(request)
    if denied is not None:
        return denied
    deleted = await run_in_threadpool(_service(request).delete_user_account, user_id)
    return _json({"success": True, "deleted": bool(deleted)})


@router.get("/play_pool/user-account/rewards", response_model=None, responses=_JSON_RESPONSES)
async def get_user_play_pool_rewards(request: Request) -> Response:
    user_id, denied = _emby_user(request)
    if denied is not None:
        return denied
    data = await run_in_threadpool(_service(request).reward_summary, user_id)
    return _json({"success": True, "data": data})


@router.get("/temp_dir_config", response_model=None, responses=_JSON_RESPONSES)
async def get_temp_dir_config(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    data = await run_in_threadpool(_service(request).get_temp_config)
    return _json({"success": True, "data": data})


@router.post("/temp_dir_config", response_model=None, responses=_JSON_BODY_RESPONSES, openapi_extra=_BODY)
async def save_temp_dir_config(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, invalid = await _legacy_json_object(request)
    if invalid is not None:
        return invalid
    assert payload is not None
    try:
        data = await run_in_threadpool(_service(request).save_temp_config, payload)
    except Exception as exc:
        return _json({"success": False, "message": str(exc)}, 500)
    return _json({"success": True, "data": data})


__all__ = ["PlaybackHttpService", "PlaybackQrHttpService", "router"]
