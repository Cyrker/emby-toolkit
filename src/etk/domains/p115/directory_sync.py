from __future__ import annotations

import hashlib
import json
import posixpath
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from etk.platform.jobs import (
    PersistentEffectIndeterminate,
    PersistentExecutionCancelled,
    PersistentExecutionInterruptedAfterEffect,
    PersistentExecutionTimedOut,
)
from etk.platform.tasks import TaskOutcome

from .auth_directory import P115CredentialStore
from .sorting_rules import P115_SORTING_RULES_ACCOUNT_KEY, P115_SORTING_RULES_KEY

_PAGE_LIMIT = 1000
_MAX_PAGES = 1000


@dataclass(frozen=True, slots=True)
class P115DirectoryRoot:
    cid: str
    name: str


@dataclass(frozen=True, slots=True)
class P115DirectoryCacheEntry:
    id: str
    parent_id: str
    name: str
    local_path: str


@dataclass(frozen=True, slots=True)
class P115DirectoryReconcileResult:
    cached: int
    cleaned: int


class P115SettingsReader(Protocol):
    def load(self, key: str) -> Any: ...


class P115DirectoryTreeClient(Protocol):
    def list_page(self, *, cid: str, limit: int, offset: int) -> Mapping[str, Any]: ...


class P115DirectoryTreeGateway(Protocol):
    def open(self, *, expected_account_id: str) -> P115DirectoryTreeClient | None: ...


class P115DirectoryCacheRepository(Protocol):
    def reconcile_children(
        self,
        root: P115DirectoryRoot,
        entries: Sequence[P115DirectoryCacheEntry],
        *,
        account_id: str,
    ) -> P115DirectoryReconcileResult: ...


class P115TaskProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


class P115DirectorySyncService:
    """Converge direct child-directory cache rows from authoritative 115 pages."""

    def __init__(
        self,
        *,
        settings: P115SettingsReader,
        credentials: P115CredentialStore,
        directories: P115DirectoryTreeGateway,
        repository: P115DirectoryCacheRepository,
    ) -> None:
        self._settings = settings
        self._credentials = credentials
        self._directories = directories
        self._repository = repository

    def execute(self, progress: P115TaskProgress) -> TaskOutcome:
        progress.update(0, "=== 开始全量同步 115 目录树到本地数据库 ===")
        roots = _sorting_roots(self._settings.load(P115_SORTING_RULES_KEY))
        if not roots:
            message = "未配置有效的分类目标目录 CID，无需同步。"
            progress.update(100, message)
            return TaskOutcome.skipped(message, _details(len(roots)))

        credentials = self._credentials.load()
        account_id = str(credentials.account_id or "").strip()
        if not account_id:
            message = "无法确认稳定的 115 账号身份，目录缓存未修改。"
            progress.update(100, message)
            return TaskOutcome.skipped(message, _details(len(roots)))

        rules_account_id = str(self._settings.load(P115_SORTING_RULES_ACCOUNT_KEY) or "").strip()
        if rules_account_id != account_id:
            message = "115 分类规则未绑定当前账号，目录缓存未修改。请重新保存分类规则。"
            progress.update(-1, message)
            return TaskOutcome.failed(
                message,
                _details(
                    len(roots),
                    errors=(
                        {
                            "cid": "",
                            "name": P115_SORTING_RULES_KEY,
                            "error": message,
                        },
                    ),
                ),
            )

        client = self._directories.open(expected_account_id=account_id)
        if client is None:
            message = "115 客户端未初始化，目录缓存未修改。"
            progress.update(100, message)
            return TaskOutcome.skipped(message, _details(len(roots)))

        cached = 0
        cleaned = 0
        completed = 0
        errors: list[dict[str, str]] = []
        for index, root in enumerate(roots):
            progress.checkpoint()
            progress.update(
                int(index / len(roots) * 100),
                f"正在扫描第 {index + 1}/{len(roots)} 个分类目录: [{root.name}] ...",
            )
            try:
                entries = self._read_authoritative_children(
                    client=client,
                    root=root,
                    account_id=account_id,
                    progress=progress,
                )
                self._assert_account(account_id)
                progress.checkpoint()
                result = self._repository.reconcile_children(
                    root,
                    entries,
                    account_id=account_id,
                )
            except (
                PersistentEffectIndeterminate,
                PersistentExecutionCancelled,
                PersistentExecutionInterruptedAfterEffect,
                PersistentExecutionTimedOut,
            ):
                raise
            except Exception as exc:
                errors.append({"cid": root.cid, "name": root.name, "error": str(exc)})
                continue
            cached += result.cached
            cleaned += result.cleaned
            completed += 1

        details = _details(
            len(roots),
            completed=completed,
            cached=cached,
            cleaned=cleaned,
            errors=errors,
        )
        message = f"同步结束：共更新 {cached} 个目录，清理 {cleaned} 个失效缓存"
        if errors:
            failed_message = f"{message}；{len(errors)} 个根目录未完成权威分页，已跳过清理"
            progress.update(-1, failed_message)
            if completed:
                return TaskOutcome.partial(failed_message, details)
            return TaskOutcome.failed(failed_message, details)
        progress.update(100, f"=== {message} ===")
        return TaskOutcome.succeeded(message, details)

    def _read_authoritative_children(
        self,
        *,
        client: P115DirectoryTreeClient,
        root: P115DirectoryRoot,
        account_id: str,
        progress: P115TaskProgress,
    ) -> tuple[P115DirectoryCacheEntry, ...]:
        offset = 0
        page = 0
        received_count = 0
        reported_total: int | None = None
        entries: dict[str, P115DirectoryCacheEntry] = {}
        seen_item_ids: set[str] = set()
        seen_page_signatures: set[bytes] = set()
        while True:
            progress.checkpoint()
            self._assert_account(account_id)
            response = client.list_page(cid=root.cid, limit=_PAGE_LIMIT, offset=offset)
            if not isinstance(response, Mapping) or response.get("state") is not True:
                message = _provider_error(response)
                raise RuntimeError(message or "115 目录树分页返回失败状态")
            data = response.get("data")
            if not isinstance(data, list):
                raise RuntimeError("115 目录树分页返回了无效 data 结构")
            if len(data) > _PAGE_LIMIT:
                raise RuntimeError("115 目录树分页返回数量超过请求上限")

            response_total = _optional_nonnegative_int(response, ("count",), label="count")
            if response_total is not None:
                if reported_total is None:
                    reported_total = response_total
                elif reported_total != response_total:
                    raise RuntimeError(f"115 目录树分页总数发生变化：{reported_total} -> {response_total}")

            if not data:
                _assert_complete_count(reported_total, received_count)
                break

            if page >= _MAX_PAGES:
                raise RuntimeError(f"115 目录树分页超过安全上限 {_MAX_PAGES} 个非空页")
            page += 1
            signature = _page_signature(data)
            if signature in seen_page_signatures:
                raise RuntimeError("115 目录树分页重复返回了相同页面")
            seen_page_signatures.add(signature)
            page_count = 0
            for item in data:
                if not isinstance(item, Mapping):
                    raise RuntimeError("115 目录树分页包含无效目录项")
                item_id = _provider_item_id(item)
                if item_id:
                    if item_id in seen_item_ids:
                        raise RuntimeError(f"115 目录树分页重复返回了项目 ID: {item_id}")
                    seen_item_ids.add(item_id)
                entry = _cache_entry(root, item)
                if entry is None:
                    continue
                entries[entry.id] = entry
                page_count += 1
            received_count += len(data)
            if reported_total is not None and received_count > reported_total:
                raise RuntimeError(
                    f"115 目录树分页数量超过报告总数：报告 {reported_total} 条，实际至少 {received_count} 条"
                )
            progress.update(
                min(99, page),
                f"[{root.name}] 翻阅第 {page} 页，发现 {page_count} 个目录。",
            )
            next_offset = offset + len(data)
            if next_offset <= offset:
                raise RuntimeError("115 目录树分页 offset 未前进")
            offset = next_offset
        return tuple(entries.values())

    def _assert_account(self, expected_account_id: str) -> None:
        current = str(self._credentials.load().account_id or "").strip()
        if current != expected_account_id:
            raise RuntimeError("115 账号在目录同步期间发生变化，拒绝写入缓存")


def _sorting_roots(raw: object) -> tuple[P115DirectoryRoot, ...]:
    value = json.loads(raw) if isinstance(raw, str) else raw
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError("p115_sorting_rules 必须是数组")
    roots: dict[str, P115DirectoryRoot] = {}
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            raise ValueError(f"p115_sorting_rules[{index}] 必须是对象")
        if not item.get("enabled", True) or not item.get("cid"):
            continue
        cid = str(item["cid"]).strip()
        if not cid or cid == "0":
            continue
        name = str(item.get("category_path") or item.get("dir_name") or item.get("name") or f"CID:{cid}")
        roots[cid] = P115DirectoryRoot(cid, name)
    return tuple(roots.values())


def _cache_entry(root: P115DirectoryRoot, item: Mapping[str, Any]) -> P115DirectoryCacheEntry | None:
    category = item.get("fc") if item.get("fc") is not None else item.get("type")
    if str(category) != "0":
        return None
    entry_id = str(item.get("fid") or item.get("file_id") or "").strip()
    name = str(item.get("fn") or item.get("n") or item.get("file_name") or "").strip()
    if not entry_id or not name:
        return None
    return P115DirectoryCacheEntry(
        id=entry_id,
        parent_id=root.cid,
        name=name,
        local_path=posixpath.join(root.name, name),
    )


def _provider_item_id(item: Mapping[str, Any]) -> str:
    return str(item.get("fid") or item.get("file_id") or item.get("id") or item.get("cid") or "").strip()


def _page_signature(data: Sequence[object]) -> bytes:
    try:
        encoded = json.dumps(
            [dict(item) if isinstance(item, Mapping) else item for item in data],
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    except (TypeError, ValueError) as exc:
        raise RuntimeError("115 目录树分页包含无法规范化的项目") from exc
    return hashlib.sha256(encoded).digest()


def _optional_nonnegative_int(
    response: Mapping[str, Any],
    keys: Sequence[str],
    *,
    label: str,
) -> int | None:
    values: list[int] = []
    for key in keys:
        raw = response.get(key)
        if raw in (None, ""):
            continue
        if isinstance(raw, bool):
            raise RuntimeError(f"115 目录树分页返回了无效 {label}")
        try:
            value = int(str(raw))
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"115 目录树分页返回了无效 {label}") from exc
        if value < 0:
            raise RuntimeError(f"115 目录树分页返回了负数 {label}")
        values.append(value)
    if not values:
        return None
    if any(value != values[0] for value in values[1:]):
        raise RuntimeError(f"115 目录树分页返回了冲突 {label}")
    return values[0]


def _assert_complete_count(reported_total: int | None, received_count: int) -> None:
    if reported_total is not None and received_count != reported_total:
        raise RuntimeError(f"115 目录树分页数据不完整：应有 {reported_total} 条，实际收到 {received_count} 条")


def _provider_error(response: object) -> str:
    if not isinstance(response, Mapping):
        return ""
    return str(response.get("error_msg") or response.get("error") or response.get("message") or "").strip()


def _details(
    roots_total: int,
    *,
    completed: int = 0,
    cached: int = 0,
    cleaned: int = 0,
    errors: Sequence[Mapping[str, str]] = (),
) -> dict[str, object]:
    return {
        "roots_total": roots_total,
        "roots_completed": completed,
        "roots_failed": len(errors),
        "cached": cached,
        "cleaned": cleaned,
        "errors": [dict(error) for error in errors],
        "account_bound": True,
        "authoritative_cleanup_only": True,
    }


__all__ = [
    "P115DirectoryCacheEntry",
    "P115DirectoryCacheRepository",
    "P115DirectoryReconcileResult",
    "P115DirectoryRoot",
    "P115DirectorySyncService",
    "P115DirectoryTreeClient",
    "P115DirectoryTreeGateway",
    "P115SettingsReader",
    "P115TaskProgress",
]
