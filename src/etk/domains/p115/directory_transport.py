from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from .auth_directory import P115CredentialStore
from .directory_sync import P115DirectoryTreeClient

_OPENAPI_FILES_URL = "https://proapi.115.com/open/ufile/files"
_COOKIE_FILES_URL = "https://webapi.115.com/files"

_COOKIE_USER_AGENTS = {
    "web": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "mac": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) 115Browser/25.0.3.2"
    ),
    "linux": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) 115Browser/25.0.3.2"),
    "tv": (
        "Mozilla/5.0 (Linux; Android 7.1.2; 115disk Build/NHG47K; wv) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Version/4.0 Chrome/89.0.4389.114 Safari/537.36"
    ),
    "alipaymini": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Mobile/15E148 AlipayDefined(nt:WIFI,ws:390|844|3.0) "
        "AliApp(AP/10.5.33.8143) AlipayClient/10.5.33.8143 Language/zh-Hans Region/CN"
    ),
    "wechatmini": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.42(0x18002a2c) "
        "NetType/WIFI Language/zh_CN"
    ),
}


class P115DirectoryTransportError(RuntimeError):
    pass


class P115DirectoryCredentialChanged(RuntimeError):
    pass


class HttpxP115DirectoryTreeGateway:
    """Open one account-bound, backend-pinned directory-list client."""

    def __init__(
        self,
        *,
        credentials: P115CredentialStore,
        priority: Callable[[], str],
        client_factory: Callable[..., httpx.Client] = httpx.Client,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._credentials = credentials
        self._priority = priority
        self._client_factory = client_factory
        self._timeout_seconds = max(1.0, float(timeout_seconds))

    def open(
        self,
        *,
        expected_account_id: str,
        expected_backend: str | None = None,
    ) -> P115DirectoryTreeClient | None:
        account_id = str(expected_account_id or "").strip()
        if not account_id:
            return None
        snapshot = self._credentials.load()
        if str(snapshot.account_id or "").strip() != account_id:
            raise P115DirectoryCredentialChanged("115 账号在目录同步客户端创建前发生变化")

        requested_backend = str(expected_backend or "").strip().lower()
        if requested_backend and requested_backend not in {"openapi", "cookie"}:
            raise P115DirectoryTransportError("115 目录树请求了不支持的固定 API 后端")
        if requested_backend:
            backends = (requested_backend,)
        else:
            priority = str(self._priority() or "openapi").strip().lower()
            backends = ("cookie", "openapi") if priority == "cookie" else ("openapi", "cookie")
        clients: list[_HttpxP115DirectoryTreeClient] = []
        for backend in backends:
            if backend == "openapi" and snapshot.access_token:
                clients.append(
                    _HttpxP115DirectoryTreeClient(
                        credentials=self._credentials,
                        account_id=account_id,
                        backend=backend,
                        headers={
                            "Authorization": f"Bearer {snapshot.access_token}",
                            "User-Agent": "Emby-toolkit/1.0 (OpenAPI)",
                        },
                        credential_value=snapshot.access_token,
                        credential_app_type="",
                        client_factory=self._client_factory,
                        timeout_seconds=self._timeout_seconds,
                    )
                )
            if backend == "cookie" and snapshot.cookie:
                clients.append(
                    _HttpxP115DirectoryTreeClient(
                        credentials=self._credentials,
                        account_id=account_id,
                        backend=backend,
                        headers={
                            "Cookie": snapshot.cookie,
                            "User-Agent": _cookie_user_agent(snapshot.app_type),
                        },
                        credential_value=snapshot.cookie,
                        credential_app_type=snapshot.app_type,
                        client_factory=self._client_factory,
                        timeout_seconds=self._timeout_seconds,
                    )
                )
        if not clients:
            return None
        return clients[0] if requested_backend else _FallbackP115DirectoryTreeClient(clients)


class _FallbackP115DirectoryTreeClient:
    def __init__(self, clients: list[_HttpxP115DirectoryTreeClient]) -> None:
        self._clients = tuple(clients)
        self._pinned: _HttpxP115DirectoryTreeClient | None = None

    def list_page(self, *, cid: str, limit: int, offset: int) -> Mapping[str, Any]:
        if self._pinned is not None:
            return self._pinned.list_page(cid=cid, limit=limit, offset=offset)

        last_response: Mapping[str, Any] | None = None
        last_error: Exception | None = None
        for client in self._clients:
            try:
                response = client.list_page(cid=cid, limit=limit, offset=offset)
            except P115DirectoryCredentialChanged:
                raise
            except Exception as exc:
                last_error = exc
                continue
            if response.get("state") is True:
                self._pinned = client
                return response
            last_response = response
        if last_response is not None:
            return last_response
        if last_error is not None:
            raise last_error
        raise P115DirectoryTransportError("115 目录树没有可用的稳定 API 后端")


class _HttpxP115DirectoryTreeClient:
    def __init__(
        self,
        *,
        credentials: P115CredentialStore,
        account_id: str,
        backend: str,
        headers: Mapping[str, str],
        credential_value: str,
        credential_app_type: str,
        client_factory: Callable[..., httpx.Client],
        timeout_seconds: float,
    ) -> None:
        self._credentials = credentials
        self._account_id = account_id
        self._backend = backend
        self._headers = dict(headers)
        self._credential_value = credential_value
        self._credential_app_type = credential_app_type
        self._client_factory = client_factory
        self._timeout_seconds = timeout_seconds

    @property
    def backend(self) -> str:
        return self._backend

    def list_page(self, *, cid: str, limit: int, offset: int) -> Mapping[str, Any]:
        url = _OPENAPI_FILES_URL if self._backend == "openapi" else _COOKIE_FILES_URL
        params = {
            "cid": str(cid),
            "limit": int(limit),
            "offset": int(offset),
            "record_open_time": 0,
            "count_folders": 0,
            "show_dir": 1,
        }
        if self._backend == "cookie":
            params["aid"] = 1

        def invoke() -> dict[str, Any]:
            self._assert_account()
            try:
                with self._client_factory(timeout=self._timeout_seconds, follow_redirects=True) as client:
                    response = client.get(url, params=params, headers=self._headers)
                    response.raise_for_status()
                    try:
                        payload = response.json()
                    except ValueError as exc:
                        raise P115DirectoryTransportError("115 目录树分页返回了无效 JSON") from exc
            finally:
                self._assert_account()
            if not isinstance(payload, Mapping):
                raise P115DirectoryTransportError("115 目录树分页返回了无效响应结构")
            normalized = _normalize_list_response(payload)
            if normalized["state"] is True and normalized["_etk_authoritative_list_payload"] is not True:
                raise P115DirectoryTransportError("115 目录树分页缺少权威列表载荷")
            normalized["_etk_api_label"] = "OpenAPI" if self._backend == "openapi" else "Cookie"
            normalized["_etk_api_label_lower"] = self._backend
            return normalized

        retrying = Retrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=0.1, max=1.0),
            retry=retry_if_exception_type(httpx.TransportError),
            reraise=True,
        )
        return retrying(invoke)

    def _assert_account(self) -> None:
        snapshot = self._credentials.load()
        current = str(snapshot.account_id or "").strip()
        if current != self._account_id:
            raise P115DirectoryCredentialChanged("115 账号在目录同步期间发生变化")
        current_credential = snapshot.access_token if self._backend == "openapi" else snapshot.cookie
        current_app_type = "" if self._backend == "openapi" else snapshot.app_type
        if current_credential != self._credential_value or current_app_type != self._credential_app_type:
            raise P115DirectoryCredentialChanged("115 凭据在目录同步期间发生变化")


def _normalize_list_response(payload: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    data = normalized.get("data")
    authoritative = isinstance(data, list)
    if isinstance(data, Mapping):
        for key in ("list", "items", "files", "data"):
            nested = data.get(key)
            if isinstance(nested, list):
                data = nested
                authoritative = True
                break
    elif data is None:
        for key in ("list", "items", "files"):
            nested = normalized.get(key)
            if isinstance(nested, list):
                data = nested
                authoritative = True
                break
    if data is None:
        data = []
    if isinstance(data, list):
        data = [_normalize_item(item) for item in data]
    if normalized.get("_etk_authoritative_list_payload") is False:
        authoritative = False
    normalized["state"] = _provider_succeeded(normalized)
    normalized["data"] = data
    normalized["_etk_authoritative_list_payload"] = authoritative
    return normalized


def _normalize_item(value: object) -> object:
    if not isinstance(value, Mapping):
        return value
    item = dict(value)
    raw_fid = item.get("fid") or item.get("file_id") or item.get("id")
    raw_cid = item.get("cid")
    raw_pid = item.get("pid") or item.get("parent_id") or item.get("parentId")
    name = item.get("fn") or item.get("n") or item.get("file_name") or item.get("name") or item.get("title")
    pick_code = item.get("pc") or item.get("pick_code") or item.get("pickcode")
    sha1 = item.get("sha1") or item.get("sha") or item.get("file_sha1")
    size = item.get("fs") or item.get("size") or item.get("file_size") or item.get("s")

    category = item.get("fc")
    if category is None:
        category = item.get("file_category")
    if category is None:
        category = item.get("type")
    icon = item.get("ico") or item.get("icon") or item.get("class")
    folder_flag = any(
        _truthy_directory_flag(candidate)
        for candidate in (item.get("is_dir"), item.get("is_directory"), item.get("is_folder"), icon)
    )
    cid_looks_like_folder = raw_cid is not None and raw_fid is None and not any((pick_code, sha1, size))
    if category is None:
        if folder_flag or cid_looks_like_folder:
            category = "0"
        elif pick_code or sha1 or size or raw_fid is not None:
            category = "1"

    is_folder = str(category) == "0"
    file_id = raw_cid if raw_fid is None and is_folder else raw_fid
    parent_id = raw_cid if raw_pid is None and not is_folder else raw_pid
    if file_id is not None:
        item.setdefault("fid", str(file_id))
        item.setdefault("file_id", str(file_id))
    if name is not None:
        item.setdefault("fn", name)
        item.setdefault("n", name)
        item.setdefault("file_name", name)
        item.setdefault("name", name)
    if parent_id is not None:
        item.setdefault("pid", str(parent_id))
        item.setdefault("parent_id", str(parent_id))
    if category is not None:
        item["fc"] = str(category)
        item.setdefault("file_category", str(category))
    return item


def _provider_succeeded(payload: Mapping[str, Any]) -> bool:
    state = payload.get("state")
    if state is True or state in (1, "1") or str(state).lower() == "true":
        return True
    if "state" in payload and state not in (None, ""):
        return False
    if payload.get("errno") in (0, "0") and not payload.get("error"):
        return True
    return payload.get("code") in (0, 200, "0", "200") and not (
        payload.get("error") or payload.get("error_msg") or payload.get("message")
    )


def _truthy_directory_flag(value: object) -> bool:
    if value is True:
        return True
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "folder", "dir", "directory"}


def _cookie_user_agent(app_type: str) -> str:
    normalized = str(app_type or "web").strip().lower() or "web"
    return _COOKIE_USER_AGENTS.get(normalized, _COOKIE_USER_AGENTS["web"])


__all__ = [
    "HttpxP115DirectoryTreeGateway",
    "P115DirectoryCredentialChanged",
    "P115DirectoryTransportError",
]
