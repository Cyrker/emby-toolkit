from __future__ import annotations

import hashlib
import os
import time
from collections.abc import Callable, Mapping
from typing import Any, Protocol, cast

import httpx

from .auth_directory import P115CredentialStore
from .directory_sync import P115DirectoryTreeClient
from .directory_transport import HttpxP115DirectoryTreeGateway
from .models import P115CredentialSnapshot


class _P115SdkClient(Protocol):
    user_id: int | str
    user_key: str

    def request(
        self,
        url: str,
        method: str = "GET",
        payload: object = None,
        **kwargs: Any,
    ) -> object: ...

    def fs_mkdir(self, name: str, pid: str, **kwargs: Any) -> object: ...

    def fs_copy(self, file_ids: list[str], pid: str, **kwargs: Any) -> object: ...

    def fs_delete(self, file_ids: list[str], **kwargs: Any) -> object: ...

    def fs_info(self, file_id: str, **kwargs: Any) -> object: ...

    def fs_file_skim(self, file_id: str, **kwargs: Any) -> object: ...

    def recyclebin_clean(self, payload: object = ..., **kwargs: Any) -> object: ...

    def recyclebin_list(self, payload: object = ..., **kwargs: Any) -> object: ...


SdkClientFactory = Callable[[P115CredentialSnapshot, str], _P115SdkClient]
HttpClientFactory = Callable[[], httpx.Client]

_UPLOAD_PART_SIZE = 64 * 1024 * 1024
_UPLOAD_ENDPOINT = "https://oss-cn-shenzhen.aliyuncs.com"


class P115MaintenanceTransportError(RuntimeError):
    pass


class HttpxP115BoundedDownloadGateway:
    """Read a bounded byte range from one already-resolved provider URL."""

    def __init__(
        self,
        *,
        client_factory: HttpClientFactory | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client_factory = client_factory or self._default_client
        self._clock = clock

    def measure(
        self,
        url: str,
        *,
        user_agent: str,
        max_bytes: int,
        max_seconds: int,
        checkpoint: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        normalized_url = str(url or "").strip()
        if not normalized_url.startswith(("http://", "https://")):
            raise ValueError("115 download URL must use HTTP or HTTPS")
        byte_limit = max(1024 * 1024, min(int(max_bytes), 128 * 1024 * 1024))
        second_limit = max(3, min(int(max_seconds), 30))
        headers = {
            "User-Agent": str(user_agent or "Mozilla/5.0"),
            "Accept": "*/*",
            "Connection": "close",
            "Range": f"bytes=0-{byte_limit - 1}",
        }
        downloaded = 0
        first_byte_ms: int | None = None
        started_at = self._clock()
        if checkpoint is not None:
            checkpoint()
        with self._client_factory() as client:
            if checkpoint is not None:
                checkpoint()
            with client.stream("GET", normalized_url, headers=headers) as response:
                if checkpoint is not None:
                    checkpoint()
                if response.status_code >= 400:
                    raise RuntimeError(f"下载测速请求失败，HTTP {response.status_code}")
                content_length = response.headers.get("Content-Length")
                for chunk in response.iter_bytes(chunk_size=256 * 1024):
                    if checkpoint is not None:
                        checkpoint()
                    if not chunk:
                        continue
                    now = self._clock()
                    if first_byte_ms is None:
                        first_byte_ms = int((now - started_at) * 1000)
                    remaining = byte_limit - downloaded
                    downloaded += min(len(chunk), remaining)
                    if downloaded >= byte_limit or now - started_at >= second_limit:
                        break
                status_code = response.status_code
        if checkpoint is not None:
            checkpoint()

        elapsed = max(self._clock() - started_at, 0.001)
        mb_per_second = downloaded / 1024 / 1024 / elapsed
        return {
            "downloaded_bytes": downloaded,
            "downloaded_human": _human_bytes(downloaded),
            "elapsed_seconds": round(elapsed, 2),
            "first_byte_ms": first_byte_ms,
            "mb_per_second": round(mb_per_second, 2),
            "mbps": round(mb_per_second * 8, 2),
            "speed_text": f"{mb_per_second:.2f} MB/s",
            "status_code": status_code,
            "content_length": content_length,
            "range_bytes": byte_limit,
        }

    @staticmethod
    def _default_client() -> httpx.Client:
        return httpx.Client(
            follow_redirects=True,
            timeout=httpx.Timeout(connect=8.0, read=35.0, write=8.0, pool=8.0),
        )


class P115PinnedOperationsClient:
    """One credential-fenced SDK client plus an authoritative listing client."""

    def __init__(
        self,
        *,
        credentials: P115CredentialStore,
        snapshot: P115CredentialSnapshot,
        backend: str,
        sdk_factory: SdkClientFactory,
        directory: P115DirectoryTreeClient,
        timeout_seconds: float,
    ) -> None:
        self._credentials = credentials
        self._snapshot = snapshot
        self._backend = backend
        self._sdk_factory = sdk_factory
        self._directory = directory
        self._timeout_seconds = max(1.0, float(timeout_seconds))

    @property
    def account_id(self) -> str:
        return str(self._snapshot.account_id or "").strip()

    @property
    def backend(self) -> str:
        return self._backend

    @property
    def credential_fingerprint(self) -> str:
        return _credential_fingerprint(self._snapshot, self._backend)

    def list_children(self, parent_id: str) -> tuple[dict[str, Any], ...]:
        parent = str(parent_id or "").strip()
        if not parent:
            raise ValueError("115 parent directory ID is required")
        offset = 0
        values: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for _page in range(1000):
            response = self._directory.list_page(cid=parent, limit=1000, offset=offset)
            if response.get("state") is not True or response.get("_etk_authoritative_list_payload") is not True:
                raise P115MaintenanceTransportError("115 directory listing was not authoritative")
            raw_items = response.get("data")
            if not isinstance(raw_items, list) or any(not isinstance(item, Mapping) for item in raw_items):
                raise P115MaintenanceTransportError("115 directory listing returned an invalid payload")
            items = [dict(item) for item in raw_items if isinstance(item, Mapping)]
            for item in items:
                item_id = _item_id(item)
                if item_id and item_id in seen_ids:
                    raise P115MaintenanceTransportError("115 directory listing returned a duplicate item ID")
                if item_id:
                    seen_ids.add(item_id)
                values.append(item)
            offset += len(items)
            total = _optional_count(response.get("count") or response.get("total"))
            if not items or len(items) < 1000 or (total is not None and offset >= total):
                self._assert_credentials()
                return tuple(values)
        raise P115MaintenanceTransportError("115 directory listing exceeded the page limit")

    def create_directory(self, parent_id: str, name: str) -> Mapping[str, Any]:
        self._assert_credentials()
        try:
            value = self._sdk_factory(self._snapshot, self._backend).fs_mkdir(
                str(name),
                str(parent_id),
                timeout=self._timeout_seconds,
            )
        finally:
            self._assert_credentials()
        return _mapping(value, "115 mkdir")

    def import_share(self, share_code: str, receive_code: str, parent_id: str) -> Mapping[str, Any]:
        normalized_share_code = str(share_code or "").strip()
        normalized_receive_code = str(receive_code or "").strip()
        target = str(parent_id or "").strip()
        if not normalized_share_code or not target:
            raise ValueError("115 share import requires a share code and target directory")
        if self._backend != "cookie":
            raise P115MaintenanceTransportError("115 share import requires the Cookie backend")
        self._assert_credentials()
        try:
            value = self._sdk_factory(self._snapshot, self._backend).request(
                "https://webapi.115.com/share/receive",
                "POST",
                {
                    "share_code": normalized_share_code,
                    "receive_code": normalized_receive_code,
                    "cid": target,
                },
                timeout=self._timeout_seconds,
            )
        finally:
            self._assert_credentials()
        return _mapping(value, "115 share import")

    def copy_files(self, file_ids: list[str], parent_id: str) -> Mapping[str, Any]:
        normalized = list(dict.fromkeys(str(value or "").strip() for value in file_ids if str(value or "").strip()))
        target = str(parent_id or "").strip()
        if not normalized or not target:
            raise ValueError("115 copy requires source file IDs and a target directory")
        self._assert_credentials()
        try:
            value = self._sdk_factory(self._snapshot, self._backend).fs_copy(
                normalized,
                target,
                timeout=self._timeout_seconds,
            )
        finally:
            self._assert_credentials()
        return _mapping(value, "115 copy")

    def delete_files(self, file_ids: list[str]) -> Mapping[str, Any]:
        normalized = list(dict.fromkeys(str(value or "").strip() for value in file_ids if str(value or "").strip()))
        if not normalized:
            return {"state": True}
        self._assert_credentials()
        try:
            value = self._sdk_factory(self._snapshot, self._backend).fs_delete(
                normalized,
                timeout=self._timeout_seconds,
            )
        finally:
            self._assert_credentials()
        return _mapping(value, "115 delete")

    def file_info(self, file_id: str) -> Mapping[str, Any]:
        normalized = str(file_id or "").strip()
        if not normalized:
            raise ValueError("115 file ID is required")
        self._assert_credentials()
        try:
            sdk = self._sdk_factory(self._snapshot, self._backend)
            if self._backend == "openapi":
                value = sdk.fs_info(normalized, timeout=self._timeout_seconds)
            else:
                value = sdk.fs_file_skim(normalized, timeout=self._timeout_seconds)
        finally:
            self._assert_credentials()
        return _mapping(value, "115 file info")

    def upload_local_file(
        self,
        path: str,
        parent_id: str,
        *,
        session: Mapping[str, Any],
        checkpoint: Callable[[Mapping[str, Any]], None],
        progress: Callable[[int], None] | None = None,
    ) -> Mapping[str, Any]:
        """Upload one stable local file while persisting every resumable request checkpoint."""

        from p115oss import oss_multipart_upload_init, oss_upload, upload_file, upload_file_init

        oss_upload_sync = cast(Callable[..., object], oss_upload)
        multipart_init_sync = cast(Callable[..., object], oss_multipart_upload_init)

        source = os.path.abspath(str(path or ""))
        target = str(parent_id or "").strip()
        if not os.path.isfile(source) or not target:
            raise ValueError("115 upload source and target directory are required")
        size = os.path.getsize(source)
        filename = os.path.basename(source)
        resume = dict(session)
        sdk = self._sdk_factory(self._snapshot, self._backend)
        api_kwargs: dict[str, Any] = {}
        if self._backend == "openapi":
            api_kwargs["headers"] = {"authorization": f"Bearer {self._snapshot.access_token}"}
        else:
            api_kwargs["user_id"] = str(sdk.user_id)
            api_kwargs["user_key"] = str(sdk.user_key)

        self._assert_credentials()
        try:
            checkpoint(
                {
                    **resume,
                    "phase": "upload_init" if not resume.get("upload_id") else "multipart_upload",
                    "target_cid": target,
                    "filename": filename,
                    "filesize": size,
                }
            )
            if not resume.get("callback") or not resume.get("upload_id"):
                initialized = upload_file_init(
                    source,
                    pid=target,
                    filename=filename,
                    filesize=size,
                    **api_kwargs,
                )
                if not isinstance(initialized, Mapping) or initialized.get("state") is not True:
                    raise P115MaintenanceTransportError("115 upload initialization was rejected")
                if initialized.get("reuse"):
                    return dict(initialized)
                data = initialized.get("data")
                if not isinstance(data, Mapping):
                    raise P115MaintenanceTransportError("115 upload initialization payload is invalid")
                callback_value = data.get("callback")
                object_key = str(data.get("object") or "").strip()
                bucket = str(data.get("bucket") or "").strip()
                if not callback_value or not object_key or not bucket:
                    raise P115MaintenanceTransportError("115 upload initialization has no resumable OSS identity")
                if size <= _UPLOAD_PART_SIZE:
                    direct = {
                        **resume,
                        "phase": "direct_upload",
                        "target_cid": target,
                        "filename": filename,
                        "filesize": size,
                        "callback": callback_value,
                        "object": object_key,
                        "bucket": bucket,
                    }
                    checkpoint(direct)
                    with open(source, "rb") as stream:
                        result = oss_upload_sync(
                            object_key,
                            stream,
                            callback=callback_value,
                            bucket=bucket,
                            endpoint=_UPLOAD_ENDPOINT,
                            reporthook=progress,
                            **({"headers": api_kwargs["headers"]} if "headers" in api_kwargs else {}),
                        )
                    return _mapping(result, "115 direct upload")
                upload_id = multipart_init_sync(
                    object_key,
                    None,
                    bucket=bucket,
                    endpoint=_UPLOAD_ENDPOINT,
                    **({"headers": api_kwargs["headers"]} if "headers" in api_kwargs else {}),
                )
                resume = {
                    "filename": filename,
                    "filesha1": str(data.get("filesha1") or data.get("fileid") or ""),
                    "filesize": size,
                    "partsize": _UPLOAD_PART_SIZE,
                    "callback": callback_value,
                    "upload_id": str(upload_id),
                    "bucket": bucket,
                }
                checkpoint(resume)

            result = upload_file(
                source,
                pid=target,
                filename=str(resume.get("filename") or filename),
                filesha1=str(resume.get("filesha1") or ""),
                filesize=int(resume.get("filesize") or size),
                partsize=int(resume.get("partsize") or _UPLOAD_PART_SIZE),
                callback=resume["callback"],
                upload_id=str(resume["upload_id"]),
                bucket=str(resume.get("bucket") or "fhnfile"),
                endpoint=_UPLOAD_ENDPOINT,
                reporthook=progress,
                **api_kwargs,
            )
            return _mapping(result, "115 multipart upload")
        finally:
            self._assert_credentials()

    def clear_recycle_bin(self) -> Mapping[str, Any]:
        self._assert_credentials()
        try:
            value = self._sdk_factory(self._snapshot, self._backend).recyclebin_clean(
                {},
                timeout=self._timeout_seconds,
            )
        finally:
            self._assert_credentials()
        return _mapping(value, "115 recycle-bin clean")

    def recycle_bin_empty(self) -> bool | None:
        self._assert_credentials()
        try:
            value = self._sdk_factory(self._snapshot, self._backend).recyclebin_list(
                {"offset": 0, "limit": 1},
                timeout=self._timeout_seconds,
            )
        except Exception:
            self._assert_credentials()
            return None
        self._assert_credentials()
        payload = _mapping(value, "115 recycle-bin list")
        if not _provider_succeeded(payload):
            return None
        items = _items(payload)
        if items is None:
            count = _optional_count(payload.get("count") or payload.get("total"))
            return count == 0 if count is not None else None
        return not items

    def _assert_credentials(self) -> None:
        current = self._credentials.load()
        if str(current.account_id or "").strip() != self.account_id:
            raise P115MaintenanceTransportError("115 account changed during the provider request")
        if self._backend == "openapi":
            if current.access_token != self._snapshot.access_token:
                raise P115MaintenanceTransportError("115 OpenAPI credential changed during the provider request")
            return
        if current.cookie != self._snapshot.cookie or current.app_type != self._snapshot.app_type:
            raise P115MaintenanceTransportError("115 Cookie credential changed during the provider request")


class P115PrimaryOperationsGateway:
    def __init__(
        self,
        *,
        credentials: P115CredentialStore,
        priority: Callable[[], str],
        directory: HttpxP115DirectoryTreeGateway | None = None,
        sdk_factory: SdkClientFactory | None = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._credentials = credentials
        self._priority = priority
        self._timeout_seconds = max(1.0, float(timeout_seconds))
        self._directory = directory or HttpxP115DirectoryTreeGateway(
            credentials=credentials,
            priority=priority,
            timeout_seconds=self._timeout_seconds,
        )
        self._sdk_factory = sdk_factory or _default_sdk_client

    def open(
        self,
        *,
        expected_account_id: str | None = None,
        expected_backend: str | None = None,
        expected_credential_fingerprint: str | None = None,
    ) -> P115PinnedOperationsClient | None:
        snapshot = self._credentials.load()
        account_id = str(snapshot.account_id or "").strip()
        expected = str(expected_account_id or "").strip()
        if not account_id:
            return None
        if expected and expected != account_id:
            raise P115MaintenanceTransportError("115 account changed before the provider client was opened")
        requested_backend = str(expected_backend or "").strip().lower()
        backend = requested_backend or _select_backend(snapshot, self._priority())
        if backend not in {"openapi", "cookie"} or not _credential(snapshot, backend):
            if requested_backend or expected_credential_fingerprint:
                raise P115MaintenanceTransportError("115 pinned provider backend is no longer available")
            return None
        expected_fingerprint = str(expected_credential_fingerprint or "").strip().lower()
        if expected_fingerprint and _credential_fingerprint(snapshot, backend) != expected_fingerprint:
            raise P115MaintenanceTransportError("115 provider credential changed before the provider client was opened")
        directory = self._directory.open(expected_account_id=account_id, expected_backend=backend)
        if directory is None:
            if requested_backend or expected_fingerprint:
                raise P115MaintenanceTransportError("115 pinned provider directory is unavailable")
            return None
        return P115PinnedOperationsClient(
            credentials=self._credentials,
            snapshot=snapshot,
            backend=backend,
            sdk_factory=self._sdk_factory,
            directory=directory,
            timeout_seconds=self._timeout_seconds,
        )


def _default_sdk_client(snapshot: P115CredentialSnapshot, backend: str) -> _P115SdkClient:
    if backend == "openapi":
        from p115client import P115OpenClient

        return cast(
            _P115SdkClient,
            P115OpenClient(
                access_token=snapshot.access_token,
                refresh_token="",
                console_qrcode=False,
            ),
        )
    from p115client import P115Client

    return cast(
        _P115SdkClient,
        P115Client(
            cookies=snapshot.cookie,
            app=snapshot.app_type,
            console_qrcode=False,
        ),
    )


def _select_backend(snapshot: P115CredentialSnapshot, priority: str) -> str:
    preferred = str(priority or "openapi").strip().lower()
    order = ("cookie", "openapi") if preferred == "cookie" else ("openapi", "cookie")
    return next((backend for backend in order if _credential(snapshot, backend)), "")


def _credential(snapshot: P115CredentialSnapshot, backend: str) -> str:
    return str(snapshot.access_token if backend == "openapi" else snapshot.cookie or "").strip()


def _credential_fingerprint(snapshot: P115CredentialSnapshot, backend: str) -> str:
    credential = _credential(snapshot, backend)
    app_type = str(snapshot.app_type or "").strip() if backend == "cookie" else ""
    return hashlib.sha256(f"{backend}\0{app_type}\0{credential}".encode()).hexdigest()


def _mapping(value: object, operation: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise P115MaintenanceTransportError(f"{operation} returned an invalid response")
    return dict(value)


def _items(value: Mapping[str, Any]) -> list[Mapping[str, Any]] | None:
    data: object = value.get("data")
    if isinstance(data, Mapping):
        for key in ("list", "items", "data"):
            if isinstance(data.get(key), list):
                data = data[key]
                break
    if data is None:
        for key in ("list", "items"):
            if isinstance(value.get(key), list):
                data = value[key]
                break
    if data is None:
        return None
    if not isinstance(data, list) or any(not isinstance(item, Mapping) for item in data):
        raise P115MaintenanceTransportError("115 recycle-bin listing returned an invalid item list")
    return [item for item in data if isinstance(item, Mapping)]


def _provider_succeeded(value: Mapping[str, Any]) -> bool:
    if value.get("state") is True or value.get("success") is True:
        return True
    code = value.get("code")
    return str(code or "").strip() in {"0", "200"}


def _optional_count(value: object) -> int | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _item_id(value: Mapping[str, Any]) -> str:
    return str(value.get("fid") or value.get("file_id") or value.get("id") or value.get("cid") or "").strip()


def item_name(value: Mapping[str, Any]) -> str:
    return str(value.get("fn") or value.get("file_name") or value.get("name") or value.get("n") or "").strip()


def item_is_directory(value: Mapping[str, Any]) -> bool:
    category = value.get("fc") if value.get("fc") is not None else value.get("file_category")
    if category is None:
        category = value.get("type")
    if str(category) == "0":
        return True
    return any(
        str(value.get(key) or "").strip().lower() in {"1", "true", "folder", "dir", "directory"}
        for key in ("is_dir", "is_directory", "is_folder", "ico", "icon")
    )


def provider_succeeded(value: Mapping[str, Any]) -> bool:
    return _provider_succeeded(value)


def response_id(value: Mapping[str, Any]) -> str:
    direct = _item_id(value)
    if direct:
        return direct
    data = value.get("data")
    return _item_id(data) if isinstance(data, Mapping) else ""


def response_ids(value: Mapping[str, Any]) -> frozenset[str]:
    values = {_item_id(value), response_id(value)}
    data: object = value.get("data")
    items: list[object]
    if isinstance(data, Mapping):
        nested = next(
            (data.get(key) for key in ("list", "items", "data") if isinstance(data.get(key), list)),
            None,
        )
        items = list(nested) if isinstance(nested, list) else [data]
    elif isinstance(data, list):
        items = list(data)
    else:
        nested = next(
            (value.get(key) for key in ("list", "items") if isinstance(value.get(key), list)),
            None,
        )
        items = list(nested) if isinstance(nested, list) else []
    values.update(_item_id(item) for item in items if isinstance(item, Mapping))
    return frozenset(item for item in values if item)


def _human_bytes(value: int) -> str:
    size = float(max(0, value))
    units = ("B", "KB", "MB", "GB", "TB")
    index = 0
    while size >= 1024 and index < len(units) - 1:
        size /= 1024
        index += 1
    return f"{int(size)} {units[index]}" if index == 0 else f"{size:.2f} {units[index]}"


__all__ = [
    "HttpxP115BoundedDownloadGateway",
    "P115MaintenanceTransportError",
    "P115PinnedOperationsClient",
    "P115PrimaryOperationsGateway",
    "item_is_directory",
    "item_name",
    "provider_succeeded",
    "response_id",
    "response_ids",
]
