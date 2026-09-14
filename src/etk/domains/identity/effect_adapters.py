from __future__ import annotations

import base64
import hashlib
import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any
from urllib.parse import quote

import httpx
from pydantic import SecretStr

from etk.platform import Settings

from .effect_ports import (
    ProviderWriteOutcome,
    ProviderWriteResult,
    RegistrationSecretLease,
)

logger = logging.getLogger(__name__)


class InProcessRegistrationSecretPort:
    @staticmethod
    def lease(secret: SecretStr) -> RegistrationSecretLease:
        return RegistrationSecretLease(secret)


class HttpxEmbyIdentityEffectsAdapter:
    """Typed Emby user adapter; provider bodies and credentials are never returned or logged."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def list_users(self) -> Sequence[Mapping[str, object]]:
        base_url, headers = self._authorization()
        try:
            with self._client_factory() as client:
                response = client.get(f"{base_url}/Users", headers=headers)
                if response.status_code != 200:
                    raise RuntimeError("Emby user query was rejected")
                payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("Emby user query is unavailable") from exc
        if not isinstance(payload, list):
            raise RuntimeError("Emby user query returned an invalid payload")
        return tuple(dict(item) for item in payload if isinstance(item, Mapping))

    def find_user_by_name(self, username: str) -> Mapping[str, object] | None:
        normalized = str(username or "").strip().casefold()
        if not normalized:
            return None
        matches = [user for user in self.list_users() if str(user.get("Name") or "").strip().casefold() == normalized]
        if len(matches) > 1:
            raise RuntimeError("Emby username readback is ambiguous")
        return matches[0] if matches else None

    def get_user(self, user_id: str) -> Mapping[str, object] | None:
        normalized_id = str(user_id or "").strip()
        if not normalized_id:
            return None
        base_url, headers = self._authorization()
        try:
            with self._client_factory() as client:
                response = client.get(f"{base_url}/Users/{quote(normalized_id, safe='')}", headers=headers)
                if response.status_code == 404:
                    return None
                if response.status_code != 200:
                    raise RuntimeError("Emby user readback was rejected")
                payload: Any = response.json()
                if isinstance(payload, Mapping) and "Configuration" not in payload:
                    configuration_response = client.get(
                        f"{base_url}/Users/{quote(normalized_id, safe='')}/Configuration",
                        headers=headers,
                    )
                    if configuration_response.status_code == 200:
                        configuration: Any = configuration_response.json()
                        if not isinstance(configuration, Mapping):
                            raise RuntimeError("Emby user configuration readback returned an invalid payload")
                        payload = {**dict(payload), "Configuration": dict(configuration)}
                    elif configuration_response.status_code != 404:
                        raise RuntimeError("Emby user configuration readback was rejected")
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("Emby user readback is unavailable") from exc
        if not isinstance(payload, Mapping):
            raise RuntimeError("Emby user readback returned an invalid payload")
        return dict(payload)

    def avatar_sha256(self, user_id: str) -> bytes | None:
        normalized_id = str(user_id or "").strip()
        if not normalized_id:
            return None
        base_url, headers = self._authorization()
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}/Users/{quote(normalized_id, safe='')}/Images/Primary",
                    headers=headers,
                )
                if response.status_code == 404:
                    return None
                if response.status_code != 200 or len(response.content) > 10 * 1024 * 1024:
                    raise RuntimeError("Emby avatar readback was rejected")
                return hashlib.sha256(response.content).digest()
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("Emby avatar readback is unavailable") from exc

    def create_user(self, username: str) -> tuple[ProviderWriteResult, str | None]:
        base_url, headers = self._authorization(json_content=True)
        try:
            with self._client_factory() as client:
                response = client.post(f"{base_url}/Users/New", headers=headers, json={"Name": username})
                if response.status_code == 200:
                    payload: Any = response.json()
                    user_id = str(payload.get("Id") or "").strip() if isinstance(payload, Mapping) else ""
                    if user_id:
                        return ProviderWriteResult(ProviderWriteOutcome.CONFIRMED, 200), user_id
                    return ProviderWriteResult(ProviderWriteOutcome.UNKNOWN, 200), None
                if response.status_code in {400, 401, 403, 409, 422}:
                    return ProviderWriteResult(ProviderWriteOutcome.REJECTED, response.status_code), None
                return ProviderWriteResult(ProviderWriteOutcome.UNKNOWN, response.status_code), None
        except (httpx.TimeoutException, httpx.TransportError):
            return ProviderWriteResult(ProviderWriteOutcome.UNKNOWN), None

    def set_password(self, user_id: str, password: str) -> ProviderWriteResult:
        base_url, headers = self._authorization(json_content=True)
        payload = {"Id": user_id, "CurrentPw": "", "NewPw": password}
        return self._write(
            "POST",
            f"{base_url}/Users/{quote(user_id, safe='')}/Password",
            headers=headers,
            payload=payload,
            confirmed_statuses={200, 204},
        )

    def verify_password(self, username: str, password: str) -> bool | None:
        base_url = self._base_url()
        headers = {
            "Content-Type": "application/json",
            "X-Emby-Authorization": (
                'Emby Client="Emby Web", Device="ETK", DeviceId="etk-registration-readback", Version="1"'
            ),
        }
        try:
            with self._client_factory() as client:
                response = client.post(
                    f"{base_url}/Users/AuthenticateByName",
                    headers=headers,
                    json={"Username": username, "LoginType": "Manual", "Pw": password},
                )
                if response.status_code == 200:
                    payload: Any = response.json()
                    return bool(isinstance(payload, Mapping) and payload.get("AccessToken") and payload.get("User"))
                if response.status_code in {400, 401, 403}:
                    return False
                return None
        except (httpx.HTTPError, TypeError, ValueError):
            return None

    def set_policy(self, user_id: str, policy: Mapping[str, object]) -> ProviderWriteResult:
        base_url, headers = self._authorization(json_content=True)
        return self._write(
            "POST",
            f"{base_url}/Users/{quote(user_id, safe='')}/Policy",
            headers=headers,
            payload=dict(policy),
            confirmed_statuses={200, 204},
        )

    def set_configuration(
        self,
        user_id: str,
        configuration: Mapping[str, object],
    ) -> ProviderWriteResult:
        base_url, headers = self._authorization(json_content=True)
        result = self._write(
            "POST",
            f"{base_url}/Users/{quote(user_id, safe='')}/Configuration",
            headers=headers,
            payload=dict(configuration),
            confirmed_statuses={200, 204},
        )
        if result.status_code != 404:
            return result
        current = self.get_user(user_id)
        if current is None:
            return result
        fallback = dict(current)
        fallback["Configuration"] = dict(configuration)
        return self._write(
            "POST",
            f"{base_url}/Users/{quote(user_id, safe='')}",
            headers=headers,
            payload=fallback,
            confirmed_statuses={200, 204},
        )

    def delete_user(self, user_id: str) -> ProviderWriteResult:
        base_url, headers = self._authorization()
        return self._write(
            "DELETE",
            f"{base_url}/Users/{quote(user_id, safe='')}",
            headers=headers,
            payload=None,
            confirmed_statuses={200, 204, 404},
        )

    def upload_avatar(
        self,
        user_id: str,
        content: bytes,
        content_type: str,
    ) -> ProviderWriteResult:
        base_url, headers = self._authorization()
        headers["Content-Type"] = content_type
        try:
            encoded = base64.b64encode(content)
            with self._client_factory() as client:
                response = client.post(
                    f"{base_url}/Users/{quote(user_id, safe='')}/Images/Primary",
                    headers=headers,
                    content=encoded,
                )
        except (httpx.TimeoutException, httpx.TransportError):
            return ProviderWriteResult(ProviderWriteOutcome.UNKNOWN)
        if response.status_code in {200, 204}:
            return ProviderWriteResult(ProviderWriteOutcome.CONFIRMED, response.status_code)
        if response.status_code in {400, 401, 403, 404, 409, 413, 415, 422}:
            return ProviderWriteResult(ProviderWriteOutcome.REJECTED, response.status_code)
        return ProviderWriteResult(ProviderWriteOutcome.UNKNOWN, response.status_code)

    def _write(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        payload: Mapping[str, object] | None,
        confirmed_statuses: set[int],
    ) -> ProviderWriteResult:
        try:
            with self._client_factory() as client:
                response = client.request(method, url, headers=headers, json=payload)
        except (httpx.TimeoutException, httpx.TransportError):
            return ProviderWriteResult(ProviderWriteOutcome.UNKNOWN)
        if response.status_code in confirmed_statuses:
            return ProviderWriteResult(ProviderWriteOutcome.CONFIRMED, response.status_code)
        if response.status_code in {400, 401, 403, 409, 422}:
            return ProviderWriteResult(ProviderWriteOutcome.REJECTED, response.status_code)
        return ProviderWriteResult(ProviderWriteOutcome.UNKNOWN, response.status_code)

    def _authorization(self, *, json_content: bool = False) -> tuple[str, dict[str, str]]:
        headers = {"X-Emby-Token": self._api_key(), "Accept": "application/json"}
        if json_content:
            headers["Content-Type"] = "application/json"
        return self._base_url(), headers

    def _base_url(self) -> str:
        value = self._settings().emby_server_url.strip().rstrip("/")
        if not value:
            raise RuntimeError("Emby server URL is not configured")
        return value

    def _api_key(self) -> str:
        value = self._settings().emby_api_key.get_secret_value().strip()
        if not value:
            raise RuntimeError("Emby API key is not configured")
        return value

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        return httpx.Client(
            timeout=max(1.0, float(settings.emby_api_timeout)),
            trust_env=False,
        )


class HttpxTelegramBotInfoGateway:
    """Telegram getMe adapter whose observable failures never contain the bot token."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def bot_username(self) -> str | None:
        token = self._settings().telegram_bot_token.get_secret_value().strip()
        if not token:
            return None
        try:
            with self._client_factory() as client:
                response = client.get(f"https://api.telegram.org/bot{token}/getMe")
                if response.status_code != 200:
                    logger.warning("Telegram getMe returned a non-success status")
                    return None
                payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError):
            logger.warning("Telegram getMe request failed")
            return None
        if not isinstance(payload, Mapping) or payload.get("ok") is not True:
            return None
        result = payload.get("result")
        if not isinstance(result, Mapping):
            return None
        username = str(result.get("username") or "").strip()
        return username or None

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(proxy=proxy or None, timeout=20)


__all__ = [
    "HttpxEmbyIdentityEffectsAdapter",
    "HttpxTelegramBotInfoGateway",
    "InProcessRegistrationSecretPort",
]
