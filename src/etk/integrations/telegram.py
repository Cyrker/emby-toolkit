"""Typed Telegram Bot and UserBot ports for P9.

Bot HTTP calls are intentionally short-lived and never retried for mutations:
Telegram does not offer an idempotency key for message delivery.  UserBot
commands are queued through the durable control bridge and are therefore safe
to claim/retry independently of the API process.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Collection, Mapping, Sequence
from typing import Any, Protocol, cast

import httpx

from etk.compatibility.tg_control_bridge import (
    TelegramCommandFailedError,
    TelegramCommandIdempotencyConflictError,
    TelegramCommandIndeterminateError,
    TelegramCommandTimeoutError,
    TelegramControlBridge,
)
from etk.platform import Settings
from etk.platform.jobs import normalize_json_object

from .http_policy import (
    ExternalAdapterError,
    HttpTransportPolicy,
    ProviderDisposition,
    ProviderResponse,
    classify_status,
    decode_json_object,
)


class TelegramBotPort(Protocol):
    def get_me(self) -> ProviderResponse[dict[str, Any]]: ...

    def send_message(
        self,
        chat_id: str,
        text: str,
        *,
        disable_notification: bool = False,
        reply_markup: Mapping[str, Any] | None = None,
        parse_mode: str | None = None,
    ) -> ProviderResponse[dict[str, Any]]: ...

    def send_photo(
        self,
        chat_id: str,
        photo: str,
        caption: str,
        *,
        disable_notification: bool = False,
        parse_mode: str | None = None,
    ) -> ProviderResponse[dict[str, Any]]: ...

    def get_updates(
        self, *, offset: int | None = None, timeout: int = 20, limit: int = 100
    ) -> ProviderResponse[dict[str, Any]]: ...


class TelegramUserBotPort(Protocol):
    def enqueue(self, command_type: str, payload: Mapping[str, Any] | None = None) -> str: ...

    def enqueue_once(
        self,
        command_type: str,
        payload: Mapping[str, Any] | None = None,
        *,
        idempotency_key: str,
        result_owner_key: str | None = None,
    ) -> str: ...

    def claim_next(
        self,
        owner_id: str,
        *,
        lease_seconds: int = 60,
        command_types: Collection[str] | None = None,
    ) -> Mapping[str, Any] | None: ...

    def complete(
        self, command_id: str, owner_id: str, result: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]: ...

    def fail(self, command_id: str, owner_id: str, error: str) -> Mapping[str, Any]: ...

    def get_command(self, command_id: str) -> Mapping[str, Any] | None: ...

    def get_runtime_status(self) -> Mapping[str, Any]: ...

    def create_resource_secret(
        self,
        payload: Mapping[str, Any],
        *,
        reference: str,
        owner_key: str,
        ttl_seconds: float,
    ) -> tuple[str, bool]: ...

    def destroy_resource_secret(self, reference: str, *, owner_key: str | None = None) -> bool: ...

    def consume_resource_secret(self, reference: str, owner_key: str) -> Mapping[str, Any]: ...

    def wait(
        self,
        command_id: str,
        *,
        timeout_seconds: float = 45,
        poll_seconds: float = 0.1,
    ) -> Mapping[str, Any]: ...


class TelegramAdapterError(ExternalAdapterError):
    """Safe alias for API-layer exception mapping."""


class TelegramUserBotIdempotencyConflictError(RuntimeError):
    pass


class TelegramUserBotCommandFailedError(RuntimeError):
    pass


class TelegramUserBotCommandIndeterminateError(RuntimeError):
    pass


class TelegramUserBotCommandTimeoutError(TimeoutError):
    pass


class HttpxTelegramBotAdapter:
    system = "telegram"

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

    def get_me(self) -> ProviderResponse[dict[str, Any]]:
        return self._request("GET", "getMe", operation="get_me", retryable=True)

    def send_message(
        self,
        chat_id: str,
        text: str,
        *,
        disable_notification: bool = False,
        reply_markup: Mapping[str, Any] | None = None,
        parse_mode: str | None = None,
    ) -> ProviderResponse[dict[str, Any]]:
        normalized_chat = _text(chat_id, "chat_id", 128)
        normalized_text = _text(text, "text", 4096)
        if not normalized_chat or not normalized_text:
            raise ValueError("chat_id and text must not be empty")
        body: dict[str, Any] = {
            "chat_id": normalized_chat,
            "text": normalized_text,
            "disable_notification": bool(disable_notification),
        }
        if reply_markup is not None:
            body["reply_markup"] = _object(reply_markup, "reply_markup")
        if parse_mode is not None:
            body["parse_mode"] = _parse_mode(parse_mode)
        return self._request("POST", "sendMessage", operation="send_message", json=body)

    def send_photo(
        self,
        chat_id: str,
        photo: str,
        caption: str,
        *,
        disable_notification: bool = False,
        parse_mode: str | None = None,
    ) -> ProviderResponse[dict[str, Any]]:
        body = {
            "chat_id": _text(chat_id, "chat_id", 128),
            "photo": _text(photo, "photo", 2048),
            "caption": _text(caption, "caption", 1024),
            "disable_notification": bool(disable_notification),
        }
        if not body["chat_id"] or not body["photo"]:
            raise ValueError("chat_id and photo must not be empty")
        if parse_mode is not None:
            body["parse_mode"] = _parse_mode(parse_mode)
        return self._request("POST", "sendPhoto", operation="send_photo", json=body)

    def answer_callback_query(
        self,
        callback_query_id: str,
        *,
        text: str | None = None,
        show_alert: bool = False,
    ) -> ProviderResponse[dict[str, Any]]:
        body: dict[str, Any] = {
            "callback_query_id": _text(callback_query_id, "callback_query_id", 256),
            "show_alert": bool(show_alert),
        }
        if text:
            body["text"] = _text(text, "text", 200)
        if not body["callback_query_id"]:
            raise ValueError("callback_query_id must not be empty")
        return self._request("POST", "answerCallbackQuery", operation="answer_callback_query", json=body)

    def set_my_commands(self, commands: Sequence[Mapping[str, Any]]) -> ProviderResponse[dict[str, Any]]:
        normalized = [_object(item, "command") for item in commands]
        if len(normalized) > 100:
            raise ValueError("commands cannot contain more than 100 items")
        return self._request("POST", "setMyCommands", operation="set_my_commands", json={"commands": normalized})

    def get_updates(
        self,
        *,
        offset: int | None = None,
        timeout: int = 20,
        limit: int = 100,
    ) -> ProviderResponse[dict[str, Any]]:
        if offset is not None and (isinstance(offset, bool) or not isinstance(offset, int)):
            raise TypeError("offset must be an integer")
        params = {
            "timeout": _bounded(timeout, minimum=0, maximum=50, field="timeout"),
            "limit": _bounded(limit, minimum=1, maximum=100, field="limit"),
        }
        if offset is not None:
            params["offset"] = offset
        return self._request("GET", "getUpdates", operation="get_updates", params=params, retryable=True)

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        operation: str,
        params: Mapping[str, Any] | None = None,
        json: Mapping[str, Any] | None = None,
        retryable: bool = False,
    ) -> ProviderResponse[dict[str, Any]]:
        token = self._token()
        base_url = self._base_url()
        url = f"{base_url}/bot{token}/{endpoint}"
        try:
            response = self._policy.request(
                lambda: self._client_request(method, url, params=params, json=json),
                operation=operation,
                retryable=retryable,
            )
        except ExternalAdapterError as exc:
            raise TelegramAdapterError(
                str(exc),
                disposition=exc.disposition,
                system=self.system,
                operation=operation,
                status_code=exc.status_code,
            ) from exc
        disposition = classify_status(response.status_code, mutation=method.upper() not in {"GET", "HEAD"})
        if disposition is not ProviderDisposition.CONFIRMED:
            return ProviderResponse(disposition, response.status_code, None, _retry_after(response))
        try:
            payload = decode_json_object(response, system=self.system, operation=operation)
        except ExternalAdapterError as exc:
            raise TelegramAdapterError(
                str(exc),
                disposition=(
                    ProviderDisposition.INDETERMINATE
                    if method.upper() not in {"GET", "HEAD"}
                    else ProviderDisposition.RETRYABLE
                ),
                system=self.system,
                operation=operation,
                status_code=response.status_code,
            ) from exc
        if payload.get("ok") is not True:
            return ProviderResponse(ProviderDisposition.REJECTED, response.status_code, payload, _retry_after(response))
        result = payload.get("result")
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            response.status_code,
            {"ok": True, "result": result} if result is not None else payload,
            _retry_after(response),
        )

    def _client_request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        with self._client_factory() as client:
            return client.request(
                method,
                url,
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                **kwargs,
            )

    def _token(self) -> str:
        source = self._settings()
        value = _setting(source, "telegram_bot_token", default="")
        if hasattr(value, "get_secret_value"):
            value = value.get_secret_value()
        token = str(value or "").strip()
        if not token:
            raise TelegramAdapterError(
                "Telegram bot token is not configured",
                disposition=ProviderDisposition.REJECTED,
                system=self.system,
                operation="configuration",
            )
        return token

    def _base_url(self) -> str:
        value = _setting(self._settings(), "telegram_api_base_url", default="https://api.telegram.org")
        base = str(value or "").strip().rstrip("/")
        if not base.startswith(("http://", "https://")):
            raise TelegramAdapterError(
                "Telegram API base URL must use HTTP or HTTPS",
                disposition=ProviderDisposition.REJECTED,
                system=self.system,
                operation="configuration",
            )
        return base

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        value = _setting(settings, "telegram_timeout", "external_http_timeout", default=20)
        try:
            timeout = max(1.0, min(float(value), 120.0))
        except (TypeError, ValueError):
            timeout = 20.0
        proxy = (
            str(_setting(settings, "network_http_proxy_url", default="") or "").strip()
            if bool(_setting(settings, "network_proxy_enabled", default=False))
            else ""
        )
        return httpx.Client(proxy=proxy or None, timeout=timeout, follow_redirects=False, trust_env=False)


class DurableTelegramUserBotAdapter:
    """Typed facade over the PostgreSQL-backed UserBot command bridge."""

    def __init__(self, bridge: TelegramControlBridge | TelegramUserBotPort | None = None) -> None:
        self._bridge = bridge or TelegramControlBridge()

    def enqueue(self, command_type: str, payload: Mapping[str, Any] | None = None) -> str:
        normalized = normalize_json_object(dict(payload or {}), field="telegram command payload")
        return self._bridge.enqueue(command_type, normalized)

    def enqueue_once(
        self,
        command_type: str,
        payload: Mapping[str, Any] | None = None,
        *,
        idempotency_key: str,
        result_owner_key: str | None = None,
    ) -> str:
        normalized = normalize_json_object(dict(payload or {}), field="telegram command payload")
        try:
            return self._bridge.enqueue_once(
                command_type,
                normalized,
                idempotency_key=idempotency_key,
                result_owner_key=result_owner_key,
            )
        except TelegramCommandIdempotencyConflictError as exc:
            raise TelegramUserBotIdempotencyConflictError(str(exc)) from exc

    def claim_next(
        self,
        owner_id: str,
        *,
        lease_seconds: int = 60,
        command_types: Collection[str] | None = None,
    ) -> Mapping[str, Any] | None:
        if command_types is None:
            return self._bridge.claim_next(owner_id, lease_seconds=lease_seconds)
        return self._bridge.claim_next(
            owner_id,
            lease_seconds=lease_seconds,
            command_types=command_types,
        )

    def complete(self, command_id: str, owner_id: str, result: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        return self._bridge.complete(
            command_id,
            owner_id,
            normalize_json_object(dict(result or {}), field="telegram result"),
        )

    def fail(self, command_id: str, owner_id: str, error: str) -> Mapping[str, Any]:
        return self._bridge.fail(command_id, owner_id, str(error or "Telegram command failed")[:500])

    def get_command(self, command_id: str) -> Mapping[str, Any] | None:
        return self._bridge.get_command(command_id)

    def get_runtime_status(self) -> Mapping[str, Any]:
        return self._bridge.get_runtime_status()

    def create_resource_secret(
        self,
        payload: Mapping[str, Any],
        *,
        reference: str,
        owner_key: str,
        ttl_seconds: float,
    ) -> tuple[str, bool]:
        create = getattr(self._bridge, "create_resource_secret", None)
        if not callable(create):
            raise RuntimeError("Telegram resource secret creation is unavailable")
        return cast(
            tuple[str, bool],
            create(
                normalize_json_object(dict(payload), field="telegram resource secret"),
                reference=reference,
                owner_key=owner_key,
                ttl_seconds=ttl_seconds,
            ),
        )

    def destroy_resource_secret(self, reference: str, *, owner_key: str | None = None) -> bool:
        destroy = getattr(self._bridge, "destroy_resource_secret", None)
        if not callable(destroy):
            raise RuntimeError("Telegram resource secret deletion is unavailable")
        return bool(destroy(reference, owner_key=owner_key))

    def consume_resource_secret(self, reference: str, owner_key: str) -> Mapping[str, Any]:
        return self._bridge.consume_resource_secret(reference, owner_key)

    def wait(
        self,
        command_id: str,
        *,
        timeout_seconds: float = 45,
        poll_seconds: float = 0.1,
    ) -> Mapping[str, Any]:
        try:
            return self._bridge.wait(
                command_id,
                timeout_seconds=timeout_seconds,
                poll_seconds=poll_seconds,
            )
        except TelegramCommandTimeoutError as exc:
            raise TelegramUserBotCommandTimeoutError(str(exc)) from exc
        except TelegramCommandIndeterminateError as exc:
            raise TelegramUserBotCommandIndeterminateError(str(exc)) from exc
        except TelegramCommandFailedError as exc:
            raise TelegramUserBotCommandFailedError(str(exc)) from exc


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


def _text(value: Any, field: str, limit: int) -> str:
    normalized = str(value or "").strip()
    if len(normalized) > limit:
        raise ValueError(f"{field} is too long")
    return normalized


def _parse_mode(value: str) -> str:
    if value not in {"HTML", "MarkdownV2"}:
        raise ValueError("unsupported Telegram parse mode")
    return value


def _object(value: Mapping[str, Any], field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field} must be an object")
    return dict(value)


def _bounded(value: Any, *, minimum: int, maximum: int, field: str) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if not minimum <= result <= maximum:
        raise ValueError(f"{field} must be between {minimum} and {maximum}")
    return result


def _retry_after(response: httpx.Response) -> float | None:
    raw = response.headers.get("Retry-After")
    try:
        value = float(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None
    return value if value is not None and math.isfinite(value) and value >= 0 else None


TelegramBotAdapter = HttpxTelegramBotAdapter
TelegramUserBotAdapter = DurableTelegramUserBotAdapter


__all__ = [
    "DurableTelegramUserBotAdapter",
    "HttpxTelegramBotAdapter",
    "TelegramAdapterError",
    "TelegramBotAdapter",
    "TelegramBotPort",
    "TelegramUserBotAdapter",
    "TelegramUserBotIdempotencyConflictError",
    "TelegramUserBotPort",
]
