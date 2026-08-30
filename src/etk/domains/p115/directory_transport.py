from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from .auth_directory import P115CredentialStore
from .direct_url_transport import P115SdkDirectUrlGateway
from .directory_sync import P115DirectoryTreeClient
from .strm_sync import P115StrmSyncClient

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
        settings: Callable[[], Any] | None = None,
        direct_urls: P115SdkDirectUrlGateway | None = None,
        client_factory: Callable[..., httpx.Client] = httpx.Client,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._credentials = credentials
        self._priority = priority
        self._settings = settings
        self._client_factory = client_factory
        self._timeout_seconds = max(1.0, float(timeout_seconds))
        self._direct_urls = direct_urls or P115SdkDirectUrlGateway(
            credentials=credentials,
            priority=priority,
            timeout_seconds=self._timeout_seconds,
        )

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
                        direct_urls=self._direct_urls,
                        settings=self._settings,
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
                            "User-Agent": p115_cookie_user_agent(snapshot.app_type),
                        },
                        credential_value=snapshot.cookie,
                        credential_app_type=snapshot.app_type,
                        direct_urls=self._direct_urls,
                        settings=self._settings,
                        client_factory=self._client_factory,
                        timeout_seconds=self._timeout_seconds,
                    )
                )
        if not clients:
            return None
        return clients[0] if requested_backend else _FallbackP115DirectoryTreeClient(clients)


class HttpxP115StrmSyncGateway:
    """Expose the account-bound transport through the complete STRM contract."""

    def __init__(
        self,
        *,
        credentials: P115CredentialStore,
        priority: Callable[[], str],
        settings: Callable[[], Any] | None = None,
        direct_urls: P115SdkDirectUrlGateway | None = None,
        client_factory: Callable[..., httpx.Client] = httpx.Client,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._directory = HttpxP115DirectoryTreeGateway(
            credentials=credentials,
            priority=priority,
            settings=settings,
            direct_urls=direct_urls,
            client_factory=client_factory,
            timeout_seconds=timeout_seconds,
        )

    def open(self, *, expected_account_id: str) -> P115StrmSyncClient | None:
        return cast(
            P115StrmSyncClient | None,
            self._directory.open(expected_account_id=expected_account_id),
        )


class _FallbackP115DirectoryTreeClient:
    def __init__(self, clients: list[_HttpxP115DirectoryTreeClient]) -> None:
        self._clients = tuple(clients)
        self._pinned: _HttpxP115DirectoryTreeClient | None = None

    @property
    def backend(self) -> str:
        return self._pinned.backend if self._pinned is not None else ""

    def list_page(
        self,
        *,
        cid: str,
        limit: int,
        offset: int,
        file_type: int | None = None,
    ) -> Mapping[str, Any]:
        if self._pinned is not None:
            return self._pinned.list_page(cid=cid, limit=limit, offset=offset, file_type=file_type)

        last_response: Mapping[str, Any] | None = None
        last_error: Exception | None = None
        for client in self._clients:
            try:
                response = client.list_page(cid=cid, limit=limit, offset=offset, file_type=file_type)
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

    def directory_path(self, directory_id: str) -> Sequence[Mapping[str, Any]]:
        if self._pinned is None:
            self.list_page(cid=directory_id, limit=1, offset=0)
        assert self._pinned is not None
        return self._pinned.directory_path(directory_id)

    def download_subtitle(self, pick_code: str, *, max_bytes: int) -> bytes:
        if self._pinned is None:
            self.list_page(cid="0", limit=1, offset=0)
        assert self._pinned is not None
        return self._pinned.download_subtitle(pick_code, max_bytes=max_bytes)


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
        direct_urls: P115SdkDirectUrlGateway,
        settings: Callable[[], Any] | None,
        client_factory: Callable[..., httpx.Client],
        timeout_seconds: float,
    ) -> None:
        self._credentials = credentials
        self._account_id = account_id
        self._backend = backend
        self._headers = dict(headers)
        self._credential_value = credential_value
        self._credential_app_type = credential_app_type
        self._direct_urls = direct_urls
        self._settings = settings
        self._client_factory = client_factory
        self._timeout_seconds = timeout_seconds

    @property
    def backend(self) -> str:
        return self._backend

    def list_page(
        self,
        *,
        cid: str,
        limit: int,
        offset: int,
        file_type: int | None = None,
    ) -> Mapping[str, Any]:
        url = _OPENAPI_FILES_URL if self._backend == "openapi" else _COOKIE_FILES_URL
        params = {
            "cid": str(cid),
            "limit": int(limit),
            "offset": int(offset),
            "record_open_time": 0,
            "count_folders": 0,
            "show_dir": 1,
        }
        if file_type is not None:
            params["type"] = int(file_type)
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

    def directory_path(self, directory_id: str) -> Sequence[Mapping[str, Any]]:
        response = self.list_page(cid=str(directory_id), limit=1, offset=0)
        path = response.get("path")
        if not isinstance(path, list) or any(not isinstance(node, Mapping) for node in path):
            raise P115DirectoryTransportError("115 目录路径回读缺少权威 path")
        return tuple(node for node in path if isinstance(node, Mapping))

    def download_subtitle(self, pick_code: str, *, max_bytes: int) -> bytes:
        if max_bytes <= 0:
            raise ValueError("subtitle download limit must be positive")
        self._assert_account()
        resolved = self._direct_urls.resolve_backend(
            str(pick_code),
            expected_account_id=self._account_id,
            expected_backend=self._backend,
            user_agent="Mozilla/5.0",
        )
        if resolved is None:
            raise P115DirectoryTransportError("115 字幕下载直链解析失败")
        headers = {"User-Agent": resolved.user_agent}
        if self._backend == "cookie":
            headers["Cookie"] = self._credential_value
        proxy = None
        timeout = self._timeout_seconds
        if self._settings is not None:
            settings = self._settings()
            if bool(getattr(settings, "network_proxy_enabled", False)):
                proxy = str(getattr(settings, "network_http_proxy_url", "") or "").strip() or None
            timeout = max(timeout, float(getattr(settings, "p115_request_interval", 0) or 0) + 15.0)
        content = bytearray()
        self._assert_account()
        try:
            with self._client_factory(
                proxy=proxy,
                timeout=timeout,
                follow_redirects=True,
            ) as client:
                with client.stream("GET", resolved.url, headers=headers) as response:
                    response.raise_for_status()
                    content_length = response.headers.get("content-length")
                    if content_length and int(content_length) > max_bytes:
                        raise P115DirectoryTransportError("115 字幕文件超过本地下载上限")
                    for chunk in response.iter_bytes(64 * 1024):
                        content.extend(chunk)
                        if len(content) > max_bytes:
                            raise P115DirectoryTransportError("115 字幕文件超过本地下载上限")
        finally:
            self._assert_account()
        return bytes(content)

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
        if "path" not in normalized and isinstance(data.get("path"), list):
            normalized["path"] = list(data["path"])
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


def p115_cookie_user_agent(app_type: str) -> str:
    normalized = str(app_type or "web").strip().lower() or "web"
    return _COOKIE_USER_AGENTS.get(normalized, _COOKIE_USER_AGENTS["web"])


__all__ = [
    "HttpxP115DirectoryTreeGateway",
    "P115DirectoryCredentialChanged",
    "P115DirectoryTransportError",
    "p115_cookie_user_agent",
]
