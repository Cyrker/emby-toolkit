from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol, cast

import httpx

from .auth_directory import P115CredentialStore
from .directory_transport import HttpxP115DirectoryTreeGateway
from .organize_execution import P115OrganizeMutationClient, P115RemoteEntry

_PAGE_LIMIT = 1000
_MAX_PAGES = 1000


class P115OrganizeReadbackMutationForbidden(RuntimeError):
    pass


class _DirectoryPageClient(Protocol):
    @property
    def backend(self) -> str: ...

    def list_page(self, *, cid: str, limit: int, offset: int) -> Mapping[str, Any]: ...


class HttpxP115OrganizeReadbackGateway:
    """Open a fixed-backend, account-bound client that can only read organize state."""

    def __init__(
        self,
        *,
        credentials: P115CredentialStore,
        priority: Callable[[], str],
        client_factory: Callable[..., httpx.Client] = httpx.Client,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._directories = HttpxP115DirectoryTreeGateway(
            credentials=credentials,
            priority=priority,
            client_factory=client_factory,
            timeout_seconds=timeout_seconds,
        )

    def open(
        self,
        *,
        expected_account_id: str,
        expected_backend: str | None = None,
    ) -> P115OrganizeMutationClient | None:
        backend = str(expected_backend or "").strip().lower()
        if backend not in {"openapi", "cookie"}:
            return None
        client = self._directories.open(
            expected_account_id=expected_account_id,
            expected_backend=backend,
        )
        if client is None or str(getattr(client, "backend", "") or "").strip().lower() != backend:
            return None
        return _HttpxP115OrganizeReadbackClient(cast(_DirectoryPageClient, client), backend)


class _HttpxP115OrganizeReadbackClient:
    def __init__(self, client: _DirectoryPageClient, backend: str) -> None:
        self._client = client
        self._backend = backend

    @property
    def backend(self) -> str:
        return self._backend

    def list_children(self, parent_id: str) -> Sequence[P115RemoteEntry]:
        parent = str(parent_id or "").strip()
        if not parent:
            raise RuntimeError("115 权威目录回读缺少 parent_id")
        offset = 0
        received = 0
        reported_total: int | None = None
        pages = 0
        entries: list[P115RemoteEntry] = []
        seen_ids: set[str] = set()
        seen_pages: set[str] = set()
        while True:
            response = self._client.list_page(cid=parent, limit=_PAGE_LIMIT, offset=offset)
            if response.get("state") is not True:
                raise RuntimeError(str(response.get("message") or response.get("error_msg") or "115 目录回读失败"))
            label = str(response.get("_etk_api_label_lower") or "").strip().lower()
            if label != self._backend:
                raise RuntimeError("115 整理回读后端与固定执行后端不一致")
            if response.get("_etk_authoritative_list_payload") is not True:
                raise RuntimeError("115 整理回读缺少权威列表载荷")
            data = response.get("data")
            if not isinstance(data, list) or any(not isinstance(item, Mapping) for item in data):
                raise RuntimeError("115 整理回读返回了无效目录列表")
            if len(data) > _PAGE_LIMIT:
                raise RuntimeError("115 整理回读页面超过请求上限")
            total = _optional_nonnegative_count(response.get("count"))
            if total is not None:
                if reported_total is None:
                    reported_total = total
                elif reported_total != total:
                    raise RuntimeError("115 整理回读分页总数发生变化")
            if not data:
                if reported_total is not None and received != reported_total:
                    raise RuntimeError("115 整理回读分页不完整")
                break
            if pages >= _MAX_PAGES:
                raise RuntimeError("115 整理回读分页超过安全上限")
            pages += 1
            page_hash = _stable_json_hash([dict(item) for item in data if isinstance(item, Mapping)])
            if page_hash in seen_pages:
                raise RuntimeError("115 整理回读重复返回相同页面")
            seen_pages.add(page_hash)
            for item in data:
                entry = _remote_entry(parent, item)
                if entry.id in seen_ids:
                    raise RuntimeError(f"115 整理回读重复返回项目 ID: {entry.id}")
                seen_ids.add(entry.id)
                entries.append(entry)
            received += len(data)
            if reported_total is not None and received > reported_total:
                raise RuntimeError("115 整理回读数量超过 provider count")
            offset += len(data)
        return tuple(entries)

    def ensure_directory(self, parent_id: str, name: str) -> Mapping[str, Any]:
        del parent_id, name
        raise P115OrganizeReadbackMutationForbidden("read-only organize reconciliation cannot create directories")

    def move(self, file_id: str, parent_id: str) -> Mapping[str, Any]:
        del file_id, parent_id
        raise P115OrganizeReadbackMutationForbidden("read-only organize reconciliation cannot move files")

    def rename(self, file_id: str, name: str) -> Mapping[str, Any]:
        del file_id, name
        raise P115OrganizeReadbackMutationForbidden("read-only organize reconciliation cannot rename files")


def _remote_entry(parent_id: str, item: Mapping[str, Any]) -> P115RemoteEntry:
    item_id = str(item.get("fid") or item.get("file_id") or item.get("id") or item.get("cid") or "").strip()
    name = str(item.get("fn") or item.get("file_name") or item.get("n") or item.get("name") or "").strip()
    if not item_id or not name:
        raise RuntimeError("115 整理回读项目缺少 ID 或名称")
    category = item.get("fc") if item.get("fc") is not None else item.get("type")
    if str(category) == "0":
        entry_kind = "directory"
    elif str(category) == "1" or item.get("sha") or item.get("sha1") or item.get("pc"):
        entry_kind = "file"
    else:
        raise RuntimeError("115 整理回读项目类型未知")
    size = _optional_nonnegative_count(item.get("fs") if item.get("fs") is not None else item.get("size")) or 0
    normalized = {
        "id": item_id,
        "parent_id": parent_id,
        "name": name,
        "entry_kind": entry_kind,
        "sha1": str(item.get("sha") or item.get("sha1") or "").strip().upper(),
        "pick_code": str(item.get("pc") or item.get("pick_code") or "").strip(),
        "size": size,
        "updated_at": str(item.get("te") or item.get("upt") or item.get("updated_at") or "").strip(),
    }
    return P115RemoteEntry(**normalized, raw_hash=_stable_json_hash(normalized))


def _optional_nonnegative_count(value: object) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        raise RuntimeError("115 provider 返回了无效 count")
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("115 provider 返回了无效 count") from exc
    if parsed < 0:
        raise RuntimeError("115 provider 返回了负数 count")
    return parsed


def _stable_json_hash(value: object) -> str:
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    except (TypeError, ValueError) as exc:
        raise RuntimeError("115 整理回读项目无法规范化") from exc
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "HttpxP115OrganizeReadbackGateway",
    "P115OrganizeReadbackMutationForbidden",
]
