from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Request
from starlette.responses import JSONResponse, Response
from werkzeug.exceptions import BadRequest, InternalServerError, NotFound, UnsupportedMediaType
from werkzeug.http import http_date

from etk.compatibility.response_contract import fastapi_json_response
from etk.platform.web import PermissionPolicy, get_request_session

from .actor import ActorSubscriptionResult, ActorSubscriptionService

router = APIRouter(prefix="/api", tags=["subscription", "actors"])


def _service(request: Request) -> ActorSubscriptionService:
    direct = getattr(request.app.state, "actor_subscription_service", None)
    if isinstance(direct, ActorSubscriptionService):
        return direct
    value = request.app.state.container.use_case("subscription.actors")
    if not isinstance(value, ActorSubscriptionService):
        raise TypeError("subscription.actors provider returned an invalid service")
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
        {"status": "error", "message": "需要 Emby 管理员权限才能执行此操作"},
        status_code=403,
    )


def _response(result: ActorSubscriptionResult) -> Response:
    if result.media_type == "text/html":
        return Response(content=str(result.payload), status_code=result.status_code, media_type="text/html")
    return fastapi_json_response(_legacy_json_value(result.payload), status_code=result.status_code)


@router.get("/actor-subscriptions/search", response_model=list[dict[str, Any]])
def search_actors(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(_service(request).search(request.query_params.get("name", "")))


@router.get("/actor-subscriptions/default-config", response_model=dict[str, Any])
def get_default_actor_config(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(_service(request).get_default_config())


@router.post("/actor-subscriptions/default-config", response_model=dict[str, Any])
async def save_default_actor_config(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_value(request)
    if error is not None:
        return _response(ActorSubscriptionResult({"error": "保存默认配置时发生服务器内部错误"}, status_code=500))
    return _response(_service(request).save_default_config(payload))


@router.get("/actor-subscriptions", response_model=list[dict[str, Any]])
def list_actor_subscriptions(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    return _response(_service(request).list_subscriptions())


@router.post("/actor-subscriptions", response_model=dict[str, Any])
async def create_actor_subscription(request: Request) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    payload, error = await _json_value(request)
    if error is not None:
        return _html_error(error)
    if not isinstance(payload, Mapping):
        return _html_error(InternalServerError())
    return _response(_service(request).create_subscription(dict(payload)))


@router.post("/actor-subscriptions/{sub_id}/refresh", response_model=dict[str, Any])
def refresh_actor_subscription(request: Request, sub_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    subscription_id = _subscription_id(sub_id)
    if subscription_id is None:
        return _html_error(NotFound())
    return _response(_service(request).refresh_subscription(subscription_id))


@router.get("/actor-subscriptions/{sub_id}", response_model=dict[str, Any])
def get_actor_subscription(request: Request, sub_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    subscription_id = _subscription_id(sub_id)
    if subscription_id is None:
        return _html_error(NotFound())
    return _response(_service(request).get_subscription(subscription_id))


@router.put("/actor-subscriptions/{sub_id}", response_model=dict[str, Any])
async def update_actor_subscription(request: Request, sub_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    subscription_id = _subscription_id(sub_id)
    if subscription_id is None:
        return _html_error(NotFound())
    payload, error = await _json_value(request)
    if error is not None or not isinstance(payload, Mapping):
        return _response(ActorSubscriptionResult({"error": "更新订阅时发生服务器内部错误"}, 500))
    return _response(_service(request).update_subscription(subscription_id, dict(payload)))


@router.delete("/actor-subscriptions/{sub_id}", response_model=dict[str, Any])
def delete_actor_subscription(request: Request, sub_id: str) -> Response:
    denied = _admin_denial(request)
    if denied is not None:
        return denied
    subscription_id = _subscription_id(sub_id)
    if subscription_id is None:
        return _html_error(NotFound())
    return _response(_service(request).delete_subscription(subscription_id))


async def _json_value(request: Request) -> tuple[Any, BadRequest | UnsupportedMediaType | None]:
    content_type = request.headers.get("content-type", "").partition(";")[0].strip().casefold()
    if content_type != "application/json" and not (
        content_type.startswith("application/") and content_type.endswith("+json")
    ):
        return None, UnsupportedMediaType()
    try:
        return await request.json(), None
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, BadRequest()


def _subscription_id(value: str) -> int | None:
    return int(value) if value.isdigit() else None


def _html_error(error: BadRequest | InternalServerError | NotFound | UnsupportedMediaType) -> Response:
    headers = dict(error.get_headers())
    return Response(
        content=error.get_body(),
        status_code=int(error.code or 500),
        headers={"content-type": headers["Content-Type"]},
    )


def _legacy_json_value(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return http_date(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _legacy_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_legacy_json_value(item) for item in value]
    return value


__all__ = ["router"]
