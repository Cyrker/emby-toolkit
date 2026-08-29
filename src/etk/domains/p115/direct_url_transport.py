from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol, cast

from p115client import P115Client, P115OpenClient

from .auth_directory import P115CredentialStore
from .media_info_adapters import P115DirectUrl
from .models import P115CredentialSnapshot

_OPENAPI_DOWNURL_BASE_URL = "https://proapi.115.com"
_COOKIE_DOWNURL_BASE_URL = "https://proapi.115.com"


class _OpenApiDownUrlClient(Protocol):
    def download_url_info_open(self, pick_code: str, **kwargs: Any) -> object: ...


class _CookieDownUrlClient(Protocol):
    def download_url_app(self, pick_code: str, **kwargs: Any) -> object: ...


OpenApiClientFactory = Callable[[str], _OpenApiDownUrlClient]
CookieClientFactory = Callable[[str, str], _CookieDownUrlClient]


class P115SdkDirectUrlGateway:
    """Resolve one short-lived URL through a credential-fenced p115client call."""

    def __init__(
        self,
        *,
        credentials: P115CredentialStore,
        priority: Callable[[], str],
        openapi_client_factory: OpenApiClientFactory | None = None,
        cookie_client_factory: CookieClientFactory | None = None,
        timeout_seconds: float = 15.0,
    ) -> None:
        self._credentials = credentials
        self._priority = priority
        self._openapi_client_factory = openapi_client_factory or _openapi_client
        self._cookie_client_factory = cookie_client_factory or _cookie_client
        self._timeout_seconds = max(1.0, float(timeout_seconds))

    def resolve(
        self,
        pick_code: str,
        *,
        expected_account_id: str,
        user_agent: str,
    ) -> P115DirectUrl | None:
        normalized_pick_code = str(pick_code or "").strip()
        account_id = str(expected_account_id or "").strip()
        normalized_user_agent = str(user_agent or "Mozilla/5.0").strip() or "Mozilla/5.0"
        if not normalized_pick_code or not account_id.isdigit():
            return None

        snapshot = self._credentials.load()
        if str(snapshot.account_id or "").strip() != account_id:
            return None
        priority = str(self._priority() or "openapi").strip().lower()
        backends = ("cookie", "openapi") if priority == "cookie" else ("openapi", "cookie")
        return self._resolve_backends(
            snapshot=snapshot,
            backends=backends,
            pick_code=normalized_pick_code,
            user_agent=normalized_user_agent,
        )

    def resolve_backend(
        self,
        pick_code: str,
        *,
        expected_account_id: str,
        expected_backend: str,
        user_agent: str,
    ) -> P115DirectUrl | None:
        normalized_pick_code = str(pick_code or "").strip()
        account_id = str(expected_account_id or "").strip()
        backend = str(expected_backend or "").strip().lower()
        normalized_user_agent = str(user_agent or "Mozilla/5.0").strip() or "Mozilla/5.0"
        if not normalized_pick_code or not account_id.isdigit() or backend not in {"openapi", "cookie"}:
            return None
        snapshot = self._credentials.load()
        if str(snapshot.account_id or "").strip() != account_id:
            return None
        return self._resolve_backends(
            snapshot=snapshot,
            backends=(backend,),
            pick_code=normalized_pick_code,
            user_agent=normalized_user_agent,
        )

    def _resolve_backends(
        self,
        *,
        snapshot: P115CredentialSnapshot,
        backends: Sequence[str],
        pick_code: str,
        user_agent: str,
    ) -> P115DirectUrl | None:
        for backend in backends:
            credential = snapshot.cookie if backend == "cookie" else snapshot.access_token
            if not credential:
                continue
            resolved = self._resolve_backend(
                backend=backend,
                snapshot=snapshot,
                pick_code=pick_code,
                user_agent=user_agent,
            )
            if resolved is _CREDENTIAL_DRIFT:
                return None
            if isinstance(resolved, P115DirectUrl):
                return resolved
        return None

    def _resolve_backend(
        self,
        *,
        backend: str,
        snapshot: P115CredentialSnapshot,
        pick_code: str,
        user_agent: str,
    ) -> P115DirectUrl | object | None:
        if not self._credentials_match(snapshot, backend=backend):
            return _CREDENTIAL_DRIFT
        try:
            # High-level SDK helpers resolve a stable PickCode and may upload into an empty root.
            if backend == "openapi":
                client = self._openapi_client_factory(snapshot.access_token)
                raw = client.download_url_info_open(
                    pick_code,
                    user_agent=user_agent,
                    base_url=_OPENAPI_DOWNURL_BASE_URL,
                    timeout=self._timeout_seconds,
                )
                method = "p115client.download_url_info_open"
            else:
                client = self._cookie_client_factory(snapshot.cookie, snapshot.app_type)
                raw = client.download_url_app(
                    pick_code,
                    user_agent=user_agent,
                    app="os_windows",
                    base_url=_COOKIE_DOWNURL_BASE_URL,
                    timeout=self._timeout_seconds,
                )
                method = "p115client.download_url_app"
        except Exception:
            if not self._credentials_match(snapshot, backend=backend):
                return _CREDENTIAL_DRIFT
            return None
        if not self._credentials_match(snapshot, backend=backend):
            return _CREDENTIAL_DRIFT
        url = _extract_direct_url(raw)
        if not url:
            return None
        try:
            return P115DirectUrl(
                url=url,
                user_agent=user_agent,
                backend=backend,
                method=method,
            )
        except ValueError:
            return None

    def _credentials_match(self, expected: P115CredentialSnapshot, *, backend: str) -> bool:
        current = self._credentials.load()
        if str(current.account_id or "").strip() != str(expected.account_id or "").strip():
            return False
        if backend == "openapi":
            return current.access_token == expected.access_token
        return current.cookie == expected.cookie and current.app_type == expected.app_type


_CREDENTIAL_DRIFT = object()


def _openapi_client(access_token: str) -> _OpenApiDownUrlClient:
    return cast(
        _OpenApiDownUrlClient,
        P115OpenClient(
            access_token=access_token,
            refresh_token="",
            console_qrcode=False,
        ),
    )


def _cookie_client(cookie: str, app_type: str) -> _CookieDownUrlClient:
    return cast(
        _CookieDownUrlClient,
        P115Client(
            cookies=cookie,
            app=app_type,
            console_qrcode=False,
        ),
    )


def _extract_direct_url(value: object, *, depth: int = 0) -> str:
    if depth > 5:
        return ""
    if isinstance(value, str):
        normalized = value.strip()
        return normalized if normalized.startswith(("http://", "https://")) else ""
    if isinstance(value, Mapping):
        for key in ("url", "download_url", "downurl", "direct_url"):
            if key in value:
                resolved = _extract_direct_url(value[key], depth=depth + 1)
                if resolved:
                    return resolved
        data = value.get("data")
        if isinstance(data, Mapping):
            for nested in data.values():
                resolved = _extract_direct_url(nested, depth=depth + 1)
                if resolved:
                    return resolved
        elif isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
            for nested in data:
                resolved = _extract_direct_url(nested, depth=depth + 1)
                if resolved:
                    return resolved
    return ""


__all__ = ["P115SdkDirectUrlGateway"]
