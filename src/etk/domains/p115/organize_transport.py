from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

import httpx

from .auth_directory import P115CredentialStore
from .directory_transport import p115_cookie_user_agent
from .organize_execution import (
    P115OrganizeMutationClient,
    P115ProviderNoEffectError,
    P115RemoteEntry,
)
from .organize_readback import HttpxP115OrganizeReadbackGateway

_OPENAPI_BASE_URL = "https://proapi.115.com"
_COOKIE_BASE_URL = "https://webapi.115.com"
_SUPPORTED_BACKENDS = frozenset({"openapi", "cookie"})


class P115OrganizeTransportError(RuntimeError):
    pass


class HttpxP115OrganizeMutationGateway:
    """Open one account-bound, backend-pinned organize mutation client."""

    def __init__(
        self,
        *,
        credentials: P115CredentialStore | Callable[[], P115CredentialStore],
        priority: Callable[[], str],
        client_factory: Callable[..., httpx.Client] = httpx.Client,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._credential_source = credentials
        self._priority = priority
        self._client_factory = client_factory
        self._timeout_seconds = max(1.0, float(timeout_seconds))

    def open(
        self,
        *,
        expected_account_id: str,
        expected_backend: str | None = None,
    ) -> P115OrganizeMutationClient | None:
        account_id = str(expected_account_id or "").strip()
        if not account_id:
            return None
        credentials = self._credential_source() if callable(self._credential_source) else self._credential_source
        snapshot = credentials.load()
        if str(snapshot.account_id or "").strip() != account_id:
            raise P115OrganizeTransportError("115 账号在整理客户端创建前发生变化")

        requested_backend = str(expected_backend or "").strip().lower()
        if requested_backend and requested_backend not in _SUPPORTED_BACKENDS:
            raise P115OrganizeTransportError("115 整理请求了不支持的固定 API 后端")
        if requested_backend:
            candidates = (requested_backend,)
        else:
            priority = str(self._priority() or "openapi").strip().lower()
            candidates = ("cookie", "openapi") if priority == "cookie" else ("openapi", "cookie")

        backend = next(
            (
                candidate
                for candidate in candidates
                if (candidate == "openapi" and snapshot.access_token) or (candidate == "cookie" and snapshot.cookie)
            ),
            None,
        )
        if backend is None:
            return None

        credential_value = snapshot.access_token if backend == "openapi" else snapshot.cookie
        credential_app_type = "" if backend == "openapi" else snapshot.app_type
        headers = (
            {
                "Authorization": f"Bearer {snapshot.access_token}",
                "User-Agent": "Emby-toolkit/1.0 (OpenAPI)",
            }
            if backend == "openapi"
            else {
                "Cookie": snapshot.cookie,
                "User-Agent": p115_cookie_user_agent(snapshot.app_type),
            }
        )
        readback = HttpxP115OrganizeReadbackGateway(
            credentials=credentials,
            priority=self._priority,
            client_factory=self._client_factory,
            timeout_seconds=self._timeout_seconds,
        ).open(expected_account_id=account_id, expected_backend=backend)
        if readback is None:
            return None
        return _HttpxP115OrganizeMutationClient(
            credentials=credentials,
            account_id=account_id,
            backend=backend,
            credential_value=credential_value,
            credential_app_type=credential_app_type,
            headers=headers,
            readback=readback,
            client_factory=self._client_factory,
            timeout_seconds=self._timeout_seconds,
        )


class _HttpxP115OrganizeMutationClient:
    def __init__(
        self,
        *,
        credentials: P115CredentialStore,
        account_id: str,
        backend: str,
        credential_value: str,
        credential_app_type: str,
        headers: Mapping[str, str],
        readback: P115OrganizeMutationClient,
        client_factory: Callable[..., httpx.Client],
        timeout_seconds: float,
    ) -> None:
        self._credentials = credentials
        self._account_id = account_id
        self._backend = backend
        self._credential_value = credential_value
        self._credential_app_type = credential_app_type
        self._headers = dict(headers)
        self._readback = readback
        self._client_factory = client_factory
        self._timeout_seconds = timeout_seconds

    @property
    def backend(self) -> str:
        return self._backend

    def list_children(self, parent_id: str) -> Sequence[P115RemoteEntry]:
        return self._readback.list_children(parent_id)

    def ensure_directory(self, parent_id: str, name: str) -> Mapping[str, Any]:
        normalized_parent = _required_text(parent_id, "parent_id")
        normalized_name = _required_text(name, "name")
        if self._backend == "openapi":
            path = "/open/folder/add"
            form = {"pid": normalized_parent, "file_name": normalized_name}
        else:
            path = "/files/add"
            form = {"cname": normalized_name, "pid": normalized_parent}
        return self._post(path, form)

    def move(self, file_id: str, parent_id: str) -> Mapping[str, Any]:
        normalized_file_id = _required_text(file_id, "file_id")
        normalized_parent = _required_text(parent_id, "parent_id")
        if normalized_parent == "0":
            raise P115ProviderNoEffectError("115 整理禁止将文件移动到根目录 CID 0")
        if self._backend == "openapi":
            path = "/open/ufile/move"
            form = {"file_ids": normalized_file_id, "to_cid": normalized_parent}
        else:
            path = "/files/move"
            form = {"pid": normalized_parent, "fid": normalized_file_id}
        return self._post(path, form)

    def rename(self, file_id: str, name: str) -> Mapping[str, Any]:
        normalized_file_id = _required_text(file_id, "file_id")
        normalized_name = _required_text(name, "name")
        if self._backend == "openapi":
            path = "/open/ufile/update"
            form = {"file_id": normalized_file_id, "file_name": normalized_name}
        else:
            path = "/files/batch_rename"
            form = {f"files_new_name[{normalized_file_id}]": normalized_name}
        return self._post(path, form)

    def _post(self, path: str, form: Mapping[str, str]) -> Mapping[str, Any]:
        self._assert_identity(before_request=True)
        try:
            context = self._client_factory(timeout=self._timeout_seconds, follow_redirects=False)
        except Exception as exc:
            raise P115ProviderNoEffectError("115 整理 HTTP client 未创建，请求未发出") from exc

        request_started = False
        try:
            with context as client:
                self._assert_identity(before_request=True)
                request_started = True
                response = client.post(
                    f"{_OPENAPI_BASE_URL if self._backend == 'openapi' else _COOKIE_BASE_URL}{path}",
                    data=dict(form),
                    headers=self._headers,
                )
        except P115ProviderNoEffectError:
            raise
        except Exception as exc:
            if not request_started:
                raise P115ProviderNoEffectError("115 整理请求未发出") from exc
            raise P115OrganizeTransportError("115 整理请求已开始但未取得有效响应") from exc
        finally:
            if request_started:
                self._assert_identity(before_request=False)

        if response.status_code >= 500:
            raise P115OrganizeTransportError(f"115 整理请求返回 HTTP {response.status_code}")
        if 400 <= response.status_code < 500:
            return {
                "state": False,
                "http_status": response.status_code,
                "message": "115 提供方拒绝整理请求",
            }
        try:
            payload = response.json()
        except ValueError as exc:
            raise P115OrganizeTransportError("115 整理请求返回了无效 JSON") from exc
        if not isinstance(payload, Mapping):
            raise P115OrganizeTransportError("115 整理请求返回了无效响应结构")
        return _safe_receipt(payload, response.status_code)

    def _assert_identity(self, *, before_request: bool) -> None:
        snapshot = self._credentials.load()
        current_account_id = str(snapshot.account_id or "").strip()
        current_credential = snapshot.access_token if self._backend == "openapi" else snapshot.cookie
        current_app_type = "" if self._backend == "openapi" else snapshot.app_type
        if (
            current_account_id == self._account_id
            and current_credential == self._credential_value
            and current_app_type == self._credential_app_type
        ):
            return
        message = "115 账号、凭据或 Cookie app type 在整理请求期间发生变化"
        if before_request:
            raise P115ProviderNoEffectError(message)
        raise P115OrganizeTransportError(message)


def _safe_receipt(payload: Mapping[str, Any], http_status: int) -> dict[str, Any]:
    state = payload.get("state")
    succeeded = state is True or state in (1, "1") or str(state).strip().lower() == "true"
    if "state" not in payload:
        succeeded = payload.get("errno") in (0, "0") or payload.get("code") in (0, 200, "0", "200")
    code = payload.get("errno") if payload.get("errno") is not None else payload.get("code")
    message = str(payload.get("message") or payload.get("error_msg") or payload.get("error") or "").strip()
    return {
        "state": succeeded,
        "http_status": int(http_status),
        "provider_code": code if isinstance(code, (str, int)) and not isinstance(code, bool) else None,
        "message": message[:512],
    }


def _required_text(value: object, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise P115ProviderNoEffectError(f"115 整理请求缺少 {field}")
    return normalized


__all__ = [
    "HttpxP115OrganizeMutationGateway",
    "P115OrganizeTransportError",
]
