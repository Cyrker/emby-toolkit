"""Typed Shared Center HTTP port and adapter.

The compatibility client in ``handler/shared_center_client.py`` is still
available during the migration, but new application code talks to this small
port.  It makes status mapping and retry behaviour explicit and keeps provider
payloads out of durable Job records.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Callable, Mapping, Sequence
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


class SharedCenterPort(Protocol):
    def health(self) -> ProviderResponse[dict[str, Any]]: ...

    def register_device(self, *, name: str, server_id_hash: str) -> ProviderResponse[dict[str, Any]]: ...

    def device_status(self) -> ProviderResponse[dict[str, Any]]: ...

    def list_sources(
        self,
        *,
        q: str = "",
        status: str = "alive,available,updating,inconsistent,incomplete",
        source_kind: str = "",
        item_type: str = "",
        tmdb_id: str = "",
        source_id: str = "",
        limit: int = 200,
        offset: int = 0,
    ) -> ProviderResponse[dict[str, Any]]: ...

    def register_source(self, source_kind: str, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]: ...

    def acquire_transfer_lease(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]: ...

    def report_transfer(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]: ...

    def share_sync_heartbeat(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]: ...

    def run_maintenance(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]: ...

    def quote_share_request(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]: ...

    def create_share_request(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]: ...

    def list_share_requests(self, **query: Any) -> ProviderResponse[dict[str, Any]]: ...

    def upload_intro_chapters(
        self,
        sha1: str,
        chapters: Sequence[Mapping[str, Any]],
        *,
        file_name: str = "",
        reason: str = "",
    ) -> ProviderResponse[dict[str, Any]]: ...


class SharedCenterError(ExternalAdapterError):
    """Provider-specific alias useful to application exception mapping."""


class HttpxSharedCenterAdapter:
    """Shared Center adapter with bounded read retries and no POST replay."""

    system = "shared_center"

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

    def health(self) -> ProviderResponse[dict[str, Any]]:
        return self.request_json("GET", "/api/v1/health", operation="health", retryable=True)

    def register_device(self, *, name: str = "ETK Device", server_id_hash: str) -> ProviderResponse[dict[str, Any]]:
        normalized_hash = _hash(server_id_hash, "server_id_hash")
        normalized_name = _text(name, "name", limit=120) or "ETK Device"
        return self.request_json(
            "POST",
            "/api/v1/devices/register",
            operation="register_device",
            json={"name": normalized_name, "server_id_hash": normalized_hash},
        )

    def me(self) -> ProviderResponse[dict[str, Any]]:
        return self.request_json("GET", "/api/v1/me", operation="me", retryable=True)

    def device_status(self) -> ProviderResponse[dict[str, Any]]:
        return self.request_json("GET", "/api/v1/devices/status", operation="device_status", retryable=True)

    def credit_ledger(self, *, limit: int = 200) -> ProviderResponse[dict[str, Any]]:
        bounded = _bounded_int(limit, minimum=1, maximum=1000, field="limit")
        return self.request_json(
            "GET",
            "/api/v1/credit/ledger",
            operation="credit_ledger",
            params={"limit": bounded},
            retryable=True,
        )

    def list_sources(
        self,
        *,
        q: str = "",
        status: str = "alive,available,updating,inconsistent,incomplete",
        source_kind: str = "",
        item_type: str = "",
        tmdb_id: str = "",
        source_id: str = "",
        limit: int = 200,
        offset: int = 0,
    ) -> ProviderResponse[dict[str, Any]]:
        params = {
            "q": _text(q, "q", limit=200),
            "status": _text(status, "status", limit=300),
            "source_kind": _text(source_kind, "source_kind", limit=80),
            "item_type": _text(item_type, "item_type", limit=80),
            "tmdb_id": _text(tmdb_id, "tmdb_id", limit=80),
            "source_id": _text(source_id, "source_id", limit=160),
            "limit": _bounded_int(limit, minimum=1, maximum=1000, field="limit"),
            "offset": _bounded_int(offset, minimum=0, maximum=10_000_000, field="offset"),
        }
        return self.request_json("GET", "/api/v1/sources/list", operation="list_sources", params=params, retryable=True)

    def list_display_sources(self, **query: Any) -> ProviderResponse[dict[str, Any]]:
        return self._display_query("/api/v1/sources/display-list", "list_display_sources", query)

    def list_cloud_search_sources(self, **query: Any) -> ProviderResponse[dict[str, Any]]:
        return self._display_query("/api/v1/sources/cloud-search", "list_cloud_search_sources", query)

    def list_display_children(self, **query: Any) -> ProviderResponse[dict[str, Any]]:
        return self._display_query("/api/v1/sources/display-children", "list_display_children", query)

    def display_detail(self, **query: Any) -> ProviderResponse[dict[str, Any]]:
        return self._display_query("/api/v1/sources/display-detail", "display_detail", query)

    def register_source(self, source_kind: str, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]:
        kind = _segment(source_kind, "source_kind")
        body = _object(payload, "source payload")
        if kind not in {"movie", "episode"}:
            raise ValueError("source_kind must be movie or episode")
        return self.request_json(
            "POST",
            f"/api/v1/sources/{kind}/register",
            operation="register_source",
            json=body,
        )

    def disable_source(
        self, source_kind: str, source_id: str, *, message: str = ""
    ) -> ProviderResponse[dict[str, Any]]:
        kind = _segment(source_kind, "source_kind")
        identifier = _segment(source_id, "source_id")
        return self.request_json(
            "POST",
            f"/api/v1/sources/{quote(kind, safe='')}/{quote(identifier, safe='')}/disable",
            operation="disable_source",
            json={"message": _text(message, "message", limit=500)},
        )

    def acquire_transfer_lease(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]:
        return self.request_json(
            "POST",
            "/api/v1/transfers/lease",
            operation="acquire_transfer_lease",
            json=_object(payload, "lease payload"),
        )

    def report_transfer(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]:
        return self.request_json(
            "POST", "/api/v1/transfers/report", operation="report_transfer", json=_object(payload, "transfer report")
        )

    def share_sync_heartbeat(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]:
        return self.request_json(
            "POST",
            "/api/v1/devices/share-sync/heartbeat",
            operation="share_sync_heartbeat",
            json=_object(payload, "share sync heartbeat"),
        )

    def upload_intro_chapters(
        self,
        sha1: str,
        chapters: Sequence[Mapping[str, Any]],
        *,
        file_name: str = "",
        reason: str = "",
    ) -> ProviderResponse[dict[str, Any]]:
        normalized_sha1 = _hash40(sha1, "sha1").upper()
        if not isinstance(chapters, Sequence) or isinstance(chapters, (str, bytes, bytearray)) or not chapters:
            raise ValueError("intro chapters must be a non-empty list")
        normalized_chapters = [_object(chapter, "intro chapter") for chapter in chapters]
        return self.request_json(
            "POST",
            "/api/v1/intro-chapters/upload",
            operation="upload_intro_chapters",
            json={
                "sha1": normalized_sha1,
                "chapters": normalized_chapters,
                "file_name": _text(file_name, "file_name", limit=1024),
                "reason": _text(reason, "reason", limit=200),
            },
        )

    def run_maintenance(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]:
        """Run the provider-side maintenance heartbeat without replaying it."""

        return self.share_sync_heartbeat(payload)

    def quote_share_request(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]:
        return self.request_json(
            "POST",
            "/api/v1/share-requests/quote",
            operation="quote_share_request",
            json=_object(payload, "quote payload"),
        )

    def create_share_request(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]:
        return self.request_json(
            "POST", "/api/v1/share-requests", operation="create_share_request", json=_object(payload, "share request")
        )

    def list_share_requests(self, **query: Any) -> ProviderResponse[dict[str, Any]]:
        return self._display_query("/api/v1/share-requests", "list_share_requests", query)

    def request_json(
        self,
        method: str,
        path: str,
        *,
        operation: str,
        params: Mapping[str, Any] | None = None,
        json: Mapping[str, Any] | None = None,
        retryable: bool = False,
    ) -> ProviderResponse[dict[str, Any]]:
        base_url = self._base_url()
        normalized_method = str(method or "GET").upper()
        url = f"{base_url}/{str(path).lstrip('/')}"
        try:
            response = self._policy.request(
                lambda: self._client_request(
                    normalized_method,
                    url,
                    params=dict(params or {}),
                    json=dict(json) if json is not None else None,
                ),
                operation=operation,
                retryable=retryable,
            )
        except ExternalAdapterError as exc:
            raise SharedCenterError(
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
            raise SharedCenterError(
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
        # The Shared Center envelope uses ``ok``/``success``.  A syntactically
        # valid explicit failure is a provider rejection, not a transport error.
        if payload.get("ok") is False or payload.get("success") is False:
            return ProviderResponse(ProviderDisposition.REJECTED, response.status_code, payload, _retry_after(response))
        return ProviderResponse(ProviderDisposition.CONFIRMED, response.status_code, payload, _retry_after(response))

    def _display_query(
        self,
        path: str,
        operation: str,
        query: Mapping[str, Any],
    ) -> ProviderResponse[dict[str, Any]]:
        params: dict[str, Any] = {}
        for key, value in query.items():
            if value is None:
                continue
            if key in {"limit", "offset", "limit_per_section"}:
                params[key] = _bounded_int(
                    value,
                    minimum=0 if key == "offset" else 1,
                    maximum=20_000 if key == "limit" else (20 if key == "limit_per_section" else 10_000_000),
                    field=key,
                )
            elif isinstance(value, bool):
                params[key] = 1 if value else 0
            else:
                params[key] = _text(value, key, limit=500)
        return self.request_json("GET", path, operation=operation, params=params, retryable=True)

    def _client_request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        with self._client_factory() as client:
            return client.request(method, url, headers=self._headers(), **kwargs)

    def _base_url(self) -> str:
        value = _setting(self._settings(), "p115_shared_center_url", "shared_center_url", default="")
        normalized = str(value or "").strip().rstrip("/")
        if not normalized:
            raise SharedCenterError(
                "Shared Center URL is not configured",
                disposition=ProviderDisposition.REJECTED,
                system=self.system,
                operation="configuration",
            )
        if not normalized.startswith(("http://", "https://")):
            raise SharedCenterError(
                "Shared Center URL must use HTTP or HTTPS",
                disposition=ProviderDisposition.REJECTED,
                system=self.system,
                operation="configuration",
            )
        return normalized

    def _headers(self) -> dict[str, str]:
        version = str(_setting(self._settings(), "app_version", "version", default="0.0.0") or "0.0.0").strip()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": f"ETK/{version or '0.0.0'} SharingAdapter",
            "X-Client-Version": version or "0.0.0",
            "X-ETK-Version": version or "0.0.0",
        }
        server_hash = (
            str(_setting(self._settings(), "emby_server_id_hash", "server_id_hash", default="") or "").strip().lower()
        )
        if server_hash:
            headers["X-Server-ID-Hash"] = _hash(server_hash, "server_id_hash")
        return headers

    def _default_client(self) -> httpx.Client:
        timeout_value = _setting(self._settings(), "shared_center_timeout", "external_http_timeout", default=20)
        try:
            timeout = max(1.0, min(float(timeout_value), 120.0))
        except (TypeError, ValueError):
            timeout = 20.0
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


def _text(value: Any, field: str, *, limit: int) -> str:
    if value is None:
        return ""
    normalized = str(value).strip()
    if len(normalized) > limit:
        raise ValueError(f"{field} is too long")
    return normalized


def _segment(value: Any, field: str) -> str:
    normalized = _text(value, field, limit=200)
    if not normalized or "/" in normalized or "\\" in normalized:
        raise ValueError(f"{field} must be a single path segment")
    return normalized


def _object(value: Mapping[str, Any], field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field} must be an object")
    return dict(value)


def _bounded_int(value: Any, *, minimum: int, maximum: int, field: str) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if result < minimum or result > maximum:
        raise ValueError(f"{field} must be between {minimum} and {maximum}")
    return result


def _hash(value: Any, field: str) -> str:
    normalized = _text(value, field, limit=128).lower()
    if len(normalized) == 64:
        try:
            bytes.fromhex(normalized)
        except ValueError as exc:
            raise ValueError(f"{field} must be a SHA-256 hex digest") from exc
        return normalized
    # Accept a raw server identity only at the adapter boundary and expose
    # its digest in headers; operation payloads never carry the raw value.
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _hash40(value: Any, field: str) -> str:
    normalized = _text(value, field, limit=40).upper()
    if len(normalized) != 40:
        raise ValueError(f"{field} must be a SHA-1 hex digest")
    try:
        bytes.fromhex(normalized)
    except ValueError as exc:
        raise ValueError(f"{field} must be a SHA-1 hex digest") from exc
    return normalized


def _retry_after(response: httpx.Response) -> float | None:
    raw = response.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) and value >= 0 else None


# Names used by the migration handoff and by callers that prefer the shorter
# ``Httpx...`` spelling.
SharedCenterAdapter = HttpxSharedCenterAdapter


__all__ = [
    "HttpxSharedCenterAdapter",
    "SharedCenterAdapter",
    "SharedCenterError",
    "SharedCenterPort",
]
