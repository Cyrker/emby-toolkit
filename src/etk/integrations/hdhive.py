"""Typed HDHive relay adapter.

The relay's unlock endpoint can debit an account without exposing an
idempotency key.  ``unlock`` therefore never retries a POST and maps a
transport/408/429/5xx result to ``INDETERMINATE`` for durable reconciliation.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from typing import Any, Protocol
from urllib.parse import quote

import httpx

from etk.platform import Settings

from .http_policy import (
    ExternalAdapterError,
    HttpTransportPolicy,
    ProviderDisposition,
    ProviderResponse,
    classify_status,
    decode_json_object,
)


class HDHivePort(Protocol):
    def ping(self) -> ProviderResponse[dict[str, Any]]: ...

    def get_resources(
        self,
        tmdb_id: str | int,
        media_type: str,
        *,
        season: int | None = None,
    ) -> ProviderResponse[dict[str, Any]]: ...

    def unlock(self, slug: str) -> ProviderResponse[dict[str, Any]]: ...

    def checkin(self, *, is_gambler: bool = False) -> ProviderResponse[dict[str, Any]]: ...


class HDHiveAdapterError(ExternalAdapterError):
    pass


class HDHiveUnlockIndeterminateError(HDHiveAdapterError):
    """The unlock request may have reached the relay/provider."""


class HttpxHDHiveAdapter:
    system = "hdhive"

    def __init__(
        self,
        settings: Callable[[], Settings | Mapping[str, Any]] | Settings | Mapping[str, Any],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        policy: HttpTransportPolicy | None = None,
    ) -> None:
        self._settings = settings if callable(settings) else lambda: settings
        self._client_factory = client_factory or self._default_client
        self._policy = policy or HttpTransportPolicy(system=self.system)

    def register(self) -> ProviderResponse[dict[str, Any]]:
        register_token = _secret_setting(self._settings(), "hdhive_register_token", "HDHIVE_RELAY_REGISTER_TOKEN")
        headers: dict[str, str] = {}
        if register_token:
            headers["X-ETK-Register-Token"] = register_token
        return self._request(
            "POST",
            "/api/etk/register",
            operation="register",
            headers=headers,
        )

    def ping(self) -> ProviderResponse[dict[str, Any]]:
        return self._request("GET", "/api/hdhive/status", operation="ping", retryable=True)

    def authorize_url(self) -> ProviderResponse[dict[str, Any]]:
        return self._request("POST", "/api/etk/authorize-url", operation="authorize_url")

    def clear_authorization(self) -> ProviderResponse[dict[str, Any]]:
        return self._request("DELETE", "/api/hdhive/authorization", operation="clear_authorization")

    def quota(self) -> ProviderResponse[dict[str, Any]]:
        return self._request("GET", "/api/hdhive/quota", operation="quota", retryable=True)

    def vip_entitlements(self) -> ProviderResponse[dict[str, Any]]:
        return self._request("GET", "/api/hdhive/vip/entitlements", operation="vip_entitlements", retryable=True)

    def usage_today(self) -> ProviderResponse[dict[str, Any]]:
        return self._request("GET", "/api/hdhive/usage/today", operation="usage_today", retryable=True)

    def user_info(self) -> ProviderResponse[dict[str, Any]]:
        return self._request("GET", "/api/hdhive/me", operation="user_info", retryable=True)

    def get_resources(
        self,
        tmdb_id: str | int,
        media_type: str,
        *,
        season: int | None = None,
    ) -> ProviderResponse[dict[str, Any]]:
        identifier = _text(tmdb_id, "tmdb_id", 64)
        if not identifier:
            raise ValueError("tmdb_id must not be empty")
        kind = _media_type(media_type)
        params = {"season": season} if season is not None else None
        return self._request(
            "GET",
            f"/api/hdhive/resources/{quote(kind, safe='')}/{quote(identifier, safe='')}",
            operation="get_resources",
            params=params,
            retryable=True,
        )

    def unlock(self, slug: str) -> ProviderResponse[dict[str, Any]]:
        normalized = _text(slug, "slug", 512)
        if not normalized:
            raise ValueError("slug must not be empty")
        try:
            result = self._request(
                "POST",
                "/api/hdhive/unlock",
                operation="unlock",
                json={"slug": normalized},
            )
        except HDHiveUnlockIndeterminateError:
            raise
        except HDHiveAdapterError as exc:
            if exc.disposition is ProviderDisposition.INDETERMINATE:
                raise HDHiveUnlockIndeterminateError(
                    str(exc),
                    disposition=ProviderDisposition.INDETERMINATE,
                    system=self.system,
                    operation="unlock",
                    status_code=exc.status_code,
                ) from exc
            raise
        if result.disposition is ProviderDisposition.INDETERMINATE:
            raise HDHiveUnlockIndeterminateError(
                "HDHive unlock outcome is indeterminate",
                disposition=ProviderDisposition.INDETERMINATE,
                system=self.system,
                operation="unlock",
                status_code=result.status_code,
            )
        return result

    def checkin(self, *, is_gambler: bool = False) -> ProviderResponse[dict[str, Any]]:
        return self._request(
            "POST",
            "/api/hdhive/checkin",
            operation="checkin",
            json={"is_gambler": bool(is_gambler)},
        )

    def list_subscriptions(
        self,
        *,
        target_type: str | None = None,
        status: str = "active",
        query: str | None = None,
        page: int = 1,
        page_size: int = 100,
    ) -> ProviderResponse[dict[str, Any]]:
        params: dict[str, Any] = {
            "status": _text(status, "status", 80),
            "page": _bounded(page, 1, 10_000, "page"),
            "page_size": _bounded(page_size, 1, 500, "page_size"),
        }
        if target_type:
            params["target_type"] = _text(target_type, "target_type", 80)
        if query:
            params["q"] = _text(query, "query", 200)
        return self._request(
            "GET",
            "/api/hdhive/subscriptions",
            operation="list_subscriptions",
            params=params,
            retryable=True,
        )

    def create_subscription(
        self,
        target_type: str,
        target_id: int,
        target_key: str,
        *,
        media_filters: Mapping[str, Any] | None = None,
    ) -> ProviderResponse[dict[str, Any]]:
        if isinstance(target_id, bool) or not isinstance(target_id, int) or target_id < 1:
            raise ValueError("target_id must be a positive integer")
        body: dict[str, Any] = {
            "target_type": _text(target_type, "target_type", 80),
            "target_id": target_id,
            "target_key": _text(target_key, "target_key", 255),
        }
        if not body["target_type"] or not body["target_key"]:
            raise ValueError("target_type and target_key must not be empty")
        if media_filters is not None:
            body["media_filters"] = dict(media_filters)
        return self._request("POST", "/api/hdhive/subscriptions", operation="create_subscription", json=body)

    def check_subscription(self, target_type: str, target_key: str) -> ProviderResponse[dict[str, Any]]:
        return self._request(
            "GET",
            "/api/hdhive/subscriptions/check",
            operation="check_subscription",
            params={
                "target_type": _text(target_type, "target_type", 80),
                "target_key": _text(target_key, "target_key", 255),
            },
            retryable=True,
        )

    def delete_subscription(self, subscription_id: int) -> ProviderResponse[dict[str, Any]]:
        if isinstance(subscription_id, bool) or not isinstance(subscription_id, int) or subscription_id < 1:
            raise ValueError("subscription_id must be a positive integer")
        return self._request(
            "DELETE",
            f"/api/hdhive/subscriptions/{subscription_id}",
            operation="delete_subscription",
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        operation: str,
        params: Mapping[str, Any] | None = None,
        json: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        retryable: bool = False,
    ) -> ProviderResponse[dict[str, Any]]:
        base_url = self._base_url()
        normalized_method = method.upper()
        url = f"{base_url}/{path.lstrip('/')}"
        request_headers = self._headers()
        request_headers.update(dict(headers or {}))
        try:
            response = self._policy.request(
                lambda: self._client_request(
                    normalized_method,
                    url,
                    params=params,
                    json=json,
                    headers=request_headers,
                ),
                operation=operation,
                retryable=retryable,
            )
        except ExternalAdapterError as exc:
            error_type = (
                HDHiveUnlockIndeterminateError
                if operation == "unlock" and exc.disposition is ProviderDisposition.INDETERMINATE
                else HDHiveAdapterError
            )
            raise error_type(
                str(exc),
                disposition=exc.disposition,
                system=self.system,
                operation=operation,
                status_code=exc.status_code,
            ) from exc
        disposition = classify_status(response.status_code, mutation=normalized_method not in {"GET", "HEAD"})
        if disposition is not ProviderDisposition.CONFIRMED:
            return ProviderResponse(disposition, response.status_code, None, _retry_after(response))
        try:
            payload = decode_json_object(response, system=self.system, operation=operation)
        except ExternalAdapterError as exc:
            raise HDHiveAdapterError(
                str(exc),
                disposition=(
                    ProviderDisposition.INDETERMINATE
                    if normalized_method not in {"GET", "HEAD"}
                    else ProviderDisposition.RETRYABLE
                ),
                system=self.system,
                operation=operation,
                status_code=response.status_code,
            ) from exc
        if payload.get("success") is False or payload.get("ok") is False:
            return ProviderResponse(ProviderDisposition.REJECTED, response.status_code, payload, _retry_after(response))
        return ProviderResponse(ProviderDisposition.CONFIRMED, response.status_code, payload, _retry_after(response))

    def _client_request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        with self._client_factory() as client:
            return client.request(method, url, **kwargs)

    def _base_url(self) -> str:
        cfg = _setting(self._settings(), "hdhive_config", default={})
        cfg = cfg if isinstance(cfg, Mapping) else {}
        value = _setting(cfg, "relay_base_url", default="") or _setting(
            self._settings(), "hdhive_relay_base_url", "HDHIVE_RELAY_BASE_URL", default=""
        )
        base = str(value or "").strip().rstrip("/")
        if not base.startswith(("http://", "https://")):
            raise HDHiveAdapterError(
                "HDHive relay URL is not configured",
                disposition=ProviderDisposition.REJECTED,
                system=self.system,
                operation="configuration",
            )
        return base

    def _headers(self) -> dict[str, str]:
        cfg = _setting(self._settings(), "hdhive_config", default={})
        cfg = cfg if isinstance(cfg, Mapping) else {}
        instance_id = str(_setting(cfg, "relay_instance_id", "instance_id", default="") or "").strip()
        instance_secret = str(_setting(cfg, "relay_instance_secret", "instance_secret", default="") or "").strip()
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if instance_id:
            headers["X-ETK-Instance-ID"] = instance_id
        if instance_secret:
            headers["X-ETK-Instance-Secret"] = instance_secret
        return headers

    def _default_client(self) -> httpx.Client:
        value = _setting(self._settings(), "hdhive_timeout", "external_http_timeout", default=30)
        try:
            timeout = max(1.0, min(float(value), 120.0))
        except (TypeError, ValueError):
            timeout = 30.0
        return httpx.Client(timeout=timeout, follow_redirects=False)


def _setting(source: Any, *keys: str, default: Any = None) -> Any:
    for key in keys:
        if isinstance(source, Mapping) and key in source:
            return source[key]
        getter = getattr(source, "get", None)
        if callable(getter):
            value = getter(key, None)
            if value is not None:
                return value
        if hasattr(source, key):
            value = getattr(source, key)
            if value is not None:
                return value
    return default


def _secret_setting(source: Any, *keys: str) -> str:
    value = _setting(source, *keys, default="")
    if hasattr(value, "get_secret_value"):
        value = value.get_secret_value()
    return str(value or "").strip()


def _text(value: Any, field: str, limit: int) -> str:
    normalized = str(value or "").strip()
    if len(normalized) > limit:
        raise ValueError(f"{field} is too long")
    return normalized


def _bounded(value: Any, minimum: int, maximum: int, field: str) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if not minimum <= result <= maximum:
        raise ValueError(f"{field} must be between {minimum} and {maximum}")
    return result


def _media_type(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"movie", "movies", "film", "films"}:
        return "movie"
    if normalized in {"tv", "series", "season", "episode", "show", "shows", "tvshow", "tvshows"}:
        return "tv"
    raise ValueError("media_type must be movie or tv")


def _retry_after(response: httpx.Response) -> float | None:
    raw = response.headers.get("Retry-After")
    try:
        value = float(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None
    return value if value is not None and math.isfinite(value) and value >= 0 else None


HDHiveAdapter = HttpxHDHiveAdapter


__all__ = [
    "HDHiveAdapter",
    "HDHiveAdapterError",
    "HDHivePort",
    "HDHiveUnlockIndeterminateError",
    "HttpxHDHiveAdapter",
]
