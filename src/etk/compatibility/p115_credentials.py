from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from typing import Any, Protocol


class PersistentP115CredentialGateway(Protocol):
    def load_record(self) -> Mapping[str, Any]: ...

    def save_bundle(
        self,
        access_token: str | None,
        refresh_token: str | None,
        *,
        cookie: str | None = None,
        app_type: str | None = None,
        account_id: str | None = None,
        credential_type: str | None = None,
        expected_access_token: str | None = None,
        expected_refresh_token: str | None = None,
    ) -> bool: ...


class LazyPersistentP115CredentialGateway:
    """Install the compatibility seam without opening PostgreSQL at API startup."""

    def __init__(self, provider: Callable[[], PersistentP115CredentialGateway]) -> None:
        self._provider = provider

    def load_record(self) -> Mapping[str, Any]:
        return self._provider().load_record()

    def save_bundle(
        self,
        access_token: str | None,
        refresh_token: str | None,
        *,
        cookie: str | None = None,
        app_type: str | None = None,
        account_id: str | None = None,
        credential_type: str | None = None,
        expected_access_token: str | None = None,
        expected_refresh_token: str | None = None,
    ) -> bool:
        return self._provider().save_bundle(
            access_token,
            refresh_token,
            cookie=cookie,
            app_type=app_type,
            account_id=account_id,
            credential_type=credential_type,
            expected_access_token=expected_access_token,
            expected_refresh_token=expected_refresh_token,
        )


_LOCK = threading.RLock()
_GATEWAY: PersistentP115CredentialGateway | None = None


def install_persistent_p115_credential_gateway(gateway: PersistentP115CredentialGateway) -> None:
    if gateway is None:
        raise TypeError("persistent P115 credential gateway is required")
    global _GATEWAY
    with _LOCK:
        if _GATEWAY is not None and _GATEWAY is not gateway:
            raise RuntimeError("a different persistent P115 credential gateway is already installed")
        _GATEWAY = gateway


def clear_persistent_p115_credential_gateway(
    gateway: PersistentP115CredentialGateway | None = None,
) -> None:
    global _GATEWAY
    with _LOCK:
        if gateway is None or _GATEWAY is gateway:
            _GATEWAY = None


def persistent_p115_credential_gateway() -> PersistentP115CredentialGateway | None:
    with _LOCK:
        return _GATEWAY


def invalidate_legacy_p115_credential_clients() -> None:
    from handler.p115_service import P115Service

    P115Service._openapi_client = None
    P115Service._token_cache = None
    P115Service.reset_cookie_client()


__all__ = [
    "LazyPersistentP115CredentialGateway",
    "PersistentP115CredentialGateway",
    "clear_persistent_p115_credential_gateway",
    "install_persistent_p115_credential_gateway",
    "invalidate_legacy_p115_credential_clients",
    "persistent_p115_credential_gateway",
]
