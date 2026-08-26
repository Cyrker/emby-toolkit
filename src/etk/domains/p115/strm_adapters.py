from __future__ import annotations

import hashlib
import os
import re
import secrets
import stat
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast

import httpx
from sqlalchemy import Engine, text

from etk.platform import Settings

from .strm_sync import (
    P115LocalDeleteAction,
    P115LocalPathState,
    P115LocalReconcileResult,
    P115LocalStrmLibrary,
    P115LocalWriteResult,
    P115StrmSyncClient,
)

_USER_AGENT = "Mozilla/5.0"
_METADATA_NAMES = frozenset(
    {
        "tvshow.nfo",
        "season.nfo",
        "movie.nfo",
        "collection.nfo",
        "poster.jpg",
        "folder.jpg",
        "fanart.jpg",
        "landscape.jpg",
        "logo.png",
        "clearlogo.png",
        "banner.jpg",
        "backdrop.jpg",
        "theme.mp3",
    }
)
_SEASON_IMAGE = re.compile(
    r"^season.*(?:poster|fanart|banner|landscape|logo|clearlogo|thumb)\.(?:jpg|png)$",
    re.IGNORECASE,
)
_SEASON_NFO = re.compile(r"^season.*\.nfo$", re.IGNORECASE)


class LegacyP115StrmSyncGateway:
    """Pin one account/backend while reusing the compatibility provider SDKs."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[..., httpx.Client] = httpx.Client,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory

    def open(self, *, expected_account_id: str) -> P115StrmSyncClient | None:
        from handler.p115_service import P115Service, get_115_account_id

        account_id = str(expected_account_id or "").strip()
        if not account_id or get_115_account_id() != account_id:
            return None
        client = P115Service.get_client(expected_account_id=account_id)
        if client is None or not hasattr(client, "p115_strm_backends"):
            return None
        available = tuple(str(value or "").strip().lower() for value in client.p115_strm_backends())
        backend = next((value for value in available if value in {"openapi", "cookie"}), "")
        if not backend:
            return None
        return _LegacyP115StrmSyncClient(
            client,
            account_id,
            backend,
            settings=self._settings,
            client_factory=self._client_factory,
        )


class _LegacyP115StrmSyncClient:
    def __init__(
        self,
        client: Any,
        account_id: str,
        backend: str,
        *,
        settings: Callable[[], Settings],
        client_factory: Callable[..., httpx.Client],
    ) -> None:
        self._client = client
        self._account_id = account_id
        self._backend = backend
        self._settings = settings
        self._client_factory = client_factory

    @property
    def backend(self) -> str:
        return self._backend

    def list_page(
        self,
        *,
        cid: str,
        file_type: int,
        limit: int,
        offset: int,
    ) -> Mapping[str, Any]:
        return self._files(
            {
                "cid": cid,
                "type": file_type,
                "limit": limit,
                "offset": offset,
                "record_open_time": 0,
            }
        )

    def directory_path(self, directory_id: str) -> Sequence[Mapping[str, Any]]:
        response = self._files(
            {
                "cid": str(directory_id),
                "limit": 1,
                "offset": 0,
                "record_open_time": 0,
                "count_folders": 0,
            }
        )
        path = response.get("path")
        if not isinstance(path, list) or any(not isinstance(node, Mapping) for node in path):
            raise RuntimeError("115 目录路径回读缺少权威 path")
        return tuple(cast(Mapping[str, Any], node) for node in path)

    def download_subtitle(self, pick_code: str, *, max_bytes: int) -> bytes:
        from handler.p115_service import P115Service, get_115_account_id, p115_expected_account

        if max_bytes <= 0:
            raise ValueError("subtitle download limit must be positive")
        if get_115_account_id() != self._account_id:
            raise RuntimeError("115 账号在字幕下载前发生变化")
        with p115_expected_account(self._account_id):
            direct_url = self._client.resolve_download_url_backend(
                str(pick_code),
                self._backend,
                user_agent=_USER_AGENT,
            )
        if not direct_url:
            raise RuntimeError("115 字幕下载直链解析失败")
        headers = {"User-Agent": _USER_AGENT}
        if self._backend == "cookie":
            cookie = str(P115Service.get_cookies() or "").strip()
            if cookie:
                headers["Cookie"] = cookie
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        timeout = httpx.Timeout(max(1.0, float(settings.p115_request_interval) + 15.0))
        content = bytearray()
        with self._client_factory(proxy=proxy or None, timeout=timeout, follow_redirects=True) as http:
            with http.stream("GET", str(direct_url), headers=headers) as response:
                response.raise_for_status()
                content_length = response.headers.get("content-length")
                if content_length and int(content_length) > max_bytes:
                    raise RuntimeError("115 字幕文件超过本地下载上限")
                for chunk in response.iter_bytes(64 * 1024):
                    content.extend(chunk)
                    if len(content) > max_bytes:
                        raise RuntimeError("115 字幕文件超过本地下载上限")
        if get_115_account_id() != self._account_id:
            raise RuntimeError("115 账号在字幕下载期间发生变化")
        return bytes(content)

    def _files(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        from handler.p115_service import get_115_account_id, p115_expected_account

        if get_115_account_id() != self._account_id:
            raise RuntimeError("115 账号在 STRM 全量扫描期间发生变化")
        with p115_expected_account(self._account_id):
            response = self._client.fs_files_backend(dict(payload), self._backend)
        if not isinstance(response, Mapping):
            raise RuntimeError("115 STRM 全量扫描返回了无效响应")
        if response.get("state") is True:
            label = str(response.get("_etk_api_label_lower") or "").strip().lower()
            if label != self._backend:
                raise RuntimeError("115 STRM 全量扫描后端发生变化")
            if response.get("_etk_authoritative_list_payload") is not True:
                raise RuntimeError("115 STRM 全量扫描缺少权威列表载荷")
        return cast(Mapping[str, Any], response)


class AtomicP115LocalStrmLibraryGateway:
    def open(self, root: str) -> P115LocalStrmLibrary | None:
        value = str(root or "").strip()
        if not value:
            return None
        path = Path(value).expanduser()
        if not path.is_absolute():
            return None
        try:
            initial = path.lstat()
            resolved = path.resolve(strict=True)
            resolved_state = resolved.lstat()
            confirmed = path.lstat()
        except OSError:
            return None
        if (
            stat.S_ISLNK(initial.st_mode)
            or stat.S_ISLNK(confirmed.st_mode)
            or not stat.S_ISDIR(initial.st_mode)
            or not stat.S_ISDIR(confirmed.st_mode)
            or not stat.S_ISDIR(resolved_state.st_mode)
            or (initial.st_dev, initial.st_ino) != (confirmed.st_dev, confirmed.st_ino)
            or (confirmed.st_dev, confirmed.st_ino)
            != (
                resolved_state.st_dev,
                resolved_state.st_ino,
            )
        ):
            return None
        return _AtomicP115LocalStrmLibrary(
            resolved,
            root_device=resolved_state.st_dev,
            root_inode=resolved_state.st_ino,
        )


class _AtomicP115LocalStrmLibrary:
    def __init__(self, root: Path, *, root_device: int, root_inode: int) -> None:
        self._root = root
        self._root_device = root_device
        self._root_inode = root_inode

    def root_path(self) -> str:
        return str(self._root)

    def absolute_path(self, relative_path: str) -> str:
        return str(self._root.joinpath(*self._relative_parts(relative_path)))

    def path_state(self, relative_path: str) -> P115LocalPathState:
        with self._parent_descriptor(relative_path, create=False) as (parent, name):
            if parent is None:
                return P115LocalPathState(kind="absent")
            return self._path_state_at(parent, name, display_path=self.absolute_path(relative_path))

    def plan_delete(
        self,
        relative_path: str,
        *,
        kind: str,
        source: str,
        expected_sha256: str | None = None,
    ) -> P115LocalDeleteAction | None:
        normalized_kind = str(kind or "").strip().lower()
        if normalized_kind not in {"file", "directory"}:
            raise ValueError("本地 STRM 删除动作类型无效")
        normalized_source = str(source or "").strip().lower()
        if normalized_source not in {"reconcile", "cache_projection"}:
            raise ValueError("本地 STRM 删除动作来源无效")
        normalized_path = "/".join(self._relative_parts(relative_path))
        target = self.absolute_path(normalized_path)
        state = self.path_state(normalized_path)
        if state.kind == "absent":
            return None
        if state.kind != normalized_kind:
            raise RuntimeError(f"本地 STRM 删除目标类型发生变化: {target}")
        if normalized_source == "cache_projection":
            expected_digest = str(expected_sha256 or "").strip().lower()
            if len(expected_digest) != 64:
                raise RuntimeError(f"本地 STRM 删除目标缺少缓存 ownership 摘要: {target}")
            try:
                bytes.fromhex(expected_digest)
            except ValueError as exc:
                raise RuntimeError(f"本地 STRM 删除目标缺少缓存 ownership 摘要: {target}") from exc
            if state.kind != "file" or state.sha256 != expected_digest:
                raise RuntimeError(f"本地 STRM 删除目标与缓存 ownership 摘要不匹配: {target}")
        elif expected_sha256 is not None:
            raise ValueError("fresh reconcile 删除动作不能声明缓存 ownership 摘要")
        return P115LocalDeleteAction(
            kind=normalized_kind,
            path=normalized_path,
            absolute_path=target,
            source=normalized_source,
            expected=state,
        )

    def exists(self, relative_path: str) -> bool:
        return self.path_state(relative_path).kind == "file"

    def strm_matches(self, relative_path: str, content: str) -> bool:
        expected = content.encode("utf-8")
        state = self.path_state(relative_path)
        return (
            state.kind == "file"
            and state.size == len(expected)
            and state.sha256 == hashlib.sha256(expected).hexdigest()
        )

    def write_strm(self, relative_path: str, content: str) -> P115LocalWriteResult:
        target = self.absolute_path(relative_path)
        desired = content.encode("utf-8")
        with self._parent_descriptor(relative_path, create=True) as (parent, name):
            assert parent is not None
            state = self._path_state_at(parent, name, display_path=target)
            if state.kind not in {"absent", "file"}:
                raise RuntimeError(f"本地 STRM 目标不是普通文件: {target}")
            if (
                state.kind == "file"
                and state.size == len(desired)
                and state.sha256 == hashlib.sha256(desired).hexdigest()
            ):
                return P115LocalWriteResult(False, target)
            self._atomic_write_at(parent, name, desired, previous=state)
        self._assert_root()
        return P115LocalWriteResult(True, target)

    def write_subtitle(self, relative_path: str, content: bytes) -> P115LocalWriteResult:
        target = self.absolute_path(relative_path)
        with self._parent_descriptor(relative_path, create=True) as (parent, name):
            assert parent is not None
            state = self._path_state_at(parent, name, display_path=target)
            if state.kind != "absent":
                if state.kind != "file":
                    raise RuntimeError(f"本地字幕目标不是普通文件: {target}")
                return P115LocalWriteResult(False, target)
            self._atomic_write_at(parent, name, bytes(content), previous=state)
        self._assert_root()
        return P115LocalWriteResult(True, target)

    def reconcile(
        self,
        *,
        roots: Sequence[str],
        desired_paths: Sequence[str],
        cleanup: bool,
        allowed_deletions: Sequence[P115LocalDeleteAction],
    ) -> P115LocalReconcileResult:
        preview = self._preview_reconcile(
            roots=roots,
            desired_paths=desired_paths,
            cleanup=cleanup,
        )
        allowed = self._allowed_deletions(allowed_deletions)
        preview_by_path = {action.path.casefold(): action for action in preview.planned_deletions}
        deferred: set[str] = {
            action.path for key, action in preview_by_path.items() if key not in allowed or allowed[key] != action
        }

        selected: list[P115LocalDeleteAction] = []
        for action in allowed.values():
            if action.source == "reconcile" and preview_by_path.get(action.path.casefold()) != action:
                deferred.add(action.path)
                continue
            try:
                current = self.plan_delete(
                    action.path,
                    kind=action.kind,
                    source=action.source,
                    expected_sha256=action.expected.sha256 if action.source == "cache_projection" else None,
                )
            except (OSError, RuntimeError, ValueError):
                self._assert_root()
                deferred.add(action.path)
                continue
            if current is None:
                continue
            if current != action:
                deferred.add(action.path)
                continue
            selected.append(action)

        # Re-read every persisted identity before the first unlink/rmdir. This keeps a
        # same-path replacement out of the effect set even if it races with preview.
        confirmed: list[P115LocalDeleteAction] = []
        for action in selected:
            try:
                current = self.plan_delete(
                    action.path,
                    kind=action.kind,
                    source=action.source,
                    expected_sha256=action.expected.sha256 if action.source == "cache_projection" else None,
                )
            except (OSError, RuntimeError, ValueError):
                self._assert_root()
                deferred.add(action.path)
                continue
            if current != action:
                deferred.add(action.path)
                continue
            confirmed.append(action)

        deleted: list[str] = []
        for action in (value for value in confirmed if value.kind == "file"):
            with self._parent_descriptor(action.path, create=False) as (parent, name):
                if parent is None:
                    continue
                result = self._delete_file_if_matches(parent, name, action)
                if result == "deleted":
                    deleted.append(action.path)
                elif result == "deferred":
                    deferred.add(action.path)
            self._assert_root()

        cleaned: list[str] = []
        failed_directories: list[str] = []
        for action in (value for value in confirmed if value.kind == "directory"):
            with self._parent_descriptor(action.path, create=False) as (parent, name):
                if parent is None:
                    continue
                result = self._delete_directory_if_matches(parent, name, action)
                if result == "deleted":
                    cleaned.append(action.path)
                elif result == "deferred":
                    deferred.add(action.path)
                elif result == "not_empty":
                    failed_directories.append(action.path)
            self._assert_root()
        return P115LocalReconcileResult(
            tuple(deleted),
            tuple(cleaned),
            tuple(sorted(deferred | set(failed_directories), key=str.casefold)),
        )

    def preview_reconcile(
        self,
        *,
        roots: Sequence[str],
        desired_paths: Sequence[str],
        cleanup: bool,
    ) -> P115LocalReconcileResult:
        return self._preview_reconcile(
            roots=roots,
            desired_paths=desired_paths,
            cleanup=cleanup,
        )

    def _preview_reconcile(
        self,
        *,
        roots: Sequence[str],
        desired_paths: Sequence[str],
        cleanup: bool,
    ) -> P115LocalReconcileResult:
        desired = {self._target(path, create_parent=False) for path in desired_paths}
        desired_strm_bases: dict[Path, set[str]] = {}
        for path in desired:
            if path.suffix.casefold() == ".strm":
                desired_strm_bases.setdefault(path.parent, set()).add(str(path.with_suffix("")))
        delete_candidates: set[Path] = set()
        all_files: set[Path] = set()
        all_directories: set[Path] = set()
        remaining_files: set[Path] = set(desired)
        root_paths = tuple(dict.fromkeys(self._target(root, create_parent=False) for root in roots))
        if not root_paths:
            raise ValueError("STRM 同步至少需要一个本地根目录")
        if any(not any(path.is_relative_to(root) for root in root_paths) for path in desired):
            raise ValueError("STRM 目标文件超出本次同步根目录")
        for root in root_paths:
            if not root.exists():
                continue
            if root.is_symlink() or not root.is_dir():
                raise RuntimeError(f"STRM 同步根目录不是安全目录: {root}")
            for current, directories, files in os.walk(root, topdown=True, followlinks=False):
                current_path = Path(current)
                symlink_directories = [
                    current_path / name for name in directories if (current_path / name).is_symlink()
                ]
                remaining_files.update(symlink_directories)
                directories[:] = [name for name in directories if not (current_path / name).is_symlink()]
                all_directories.add(current_path)
                all_directories.update(current_path / name for name in directories)
                virtual_bases: set[str] = set()
                for name in files:
                    path = current_path / name
                    if path.is_symlink():
                        remaining_files.add(path)
                        continue
                    all_files.add(path)
                    if path.suffix.casefold() == ".strm" and self._is_virtual_strm(path):
                        virtual_bases.add(str(path.with_suffix("")))
                valid_bases = desired_strm_bases.get(current_path, set()) | virtual_bases
                for name in files:
                    path = current_path / name
                    if path.is_symlink():
                        continue
                    if path in desired:
                        remaining_files.add(path)
                        continue
                    lower = name.casefold()
                    if lower.endswith(".strm"):
                        if self._is_virtual_strm(path):
                            remaining_files.add(path)
                            continue
                        delete_candidates.add(path)
                        continue
                    if not cleanup:
                        remaining_files.add(path)
                        continue
                    if lower in _METADATA_NAMES or _SEASON_IMAGE.match(lower) or _SEASON_NFO.match(lower):
                        remaining_files.add(path)
                        continue
                    path_text = str(path)
                    if any(
                        path_text.startswith(f"{base}.") or path_text.startswith(f"{base}-") for base in valid_bases
                    ):
                        remaining_files.add(path)
                        continue
                    delete_candidates.add(path)

        directory_candidates: set[Path] = set()
        if cleanup:
            protected_directories = set(root_paths)
            protected_assets = set(desired)
            protected_assets.update(path for path in remaining_files if path.suffix.casefold() == ".strm")
            for path in protected_assets:
                current = path.parent
                while current.is_relative_to(self._root):
                    protected_directories.add(current)
                    if current in root_paths or current == self._root:
                        break
                    current = current.parent
            directory_candidates = all_directories - protected_directories
            for path in all_files | desired:
                current = path.parent
                while current.is_relative_to(self._root):
                    if current in directory_candidates:
                        delete_candidates.add(path)
                        break
                    if current == self._root:
                        break
                    current = current.parent

        deleted_paths = tuple(sorted((self._relative(path) for path in delete_candidates), key=str.casefold))
        directory_paths = tuple(
            sorted(
                (self._relative(path) for path in directory_candidates),
                key=lambda value: (-value.count("/"), value.casefold()),
            )
        )
        planned_deletions = tuple(
            action
            for kind, paths in (("file", deleted_paths), ("directory", directory_paths))
            for path in paths
            if (
                action := self.plan_delete(
                    path,
                    kind=kind,
                    source="reconcile",
                )
            )
            is not None
        )
        return P115LocalReconcileResult(
            deleted_paths,
            directory_paths,
            planned_deletions=planned_deletions,
        )

    def replace_strm(self, *, mode: str, search: str, replacement: str) -> tuple[int, int]:
        self._assert_root()
        pattern = re.compile(search) if mode == "regex" else None
        fixed = 0
        skipped = 0
        for current, directories, files in os.walk(self._root, topdown=True, followlinks=False):
            current_path = Path(current)
            directories[:] = [name for name in directories if not (current_path / name).is_symlink()]
            for name in files:
                if not name.endswith(".strm"):
                    continue
                path = current_path / name
                if path.is_symlink():
                    skipped += 1
                    continue
                path = self._target(self._relative(path), create_parent=False)
                content = path.read_text(encoding="utf-8").strip()
                if mode not in {"plain", "regex"}:
                    skipped += 1
                    continue
                new_content = (
                    content.replace(search, replacement) if pattern is None else pattern.sub(replacement, content)
                )
                if new_content == content:
                    skipped += 1
                    continue
                relative_path = self._relative(path)
                with self._parent_descriptor(relative_path, create=False) as (parent, target_name):
                    if parent is None:
                        skipped += 1
                        continue
                    previous = self._path_state_at(parent, target_name, display_path=str(path))
                    if previous.kind != "file":
                        skipped += 1
                        continue
                    self._atomic_write_at(parent, target_name, new_content.encode("utf-8"), previous=previous)
                self._assert_root()
                fixed += 1
        return fixed, skipped

    def _allowed_deletions(
        self,
        values: Sequence[P115LocalDeleteAction],
    ) -> dict[str, P115LocalDeleteAction]:
        normalized: dict[str, P115LocalDeleteAction] = {}
        for action in values:
            if (
                action.kind not in {"file", "directory"}
                or action.source not in {"reconcile", "cache_projection"}
                or action.expected.kind != action.kind
            ):
                raise ValueError("持久 STRM action plan 包含无效删除身份")
            relative = "/".join(self._relative_parts(action.path))
            target = self.absolute_path(relative)
            if relative != action.path or target != action.absolute_path:
                raise ValueError("持久 STRM action plan 包含非规范化本地路径")
            key = relative.casefold()
            if key in normalized:
                raise ValueError("持久 STRM action plan 包含重复本地路径")
            normalized[key] = action
        return normalized

    def _relative(self, path: Path) -> str:
        try:
            return path.relative_to(self._root).as_posix()
        except ValueError as exc:
            raise ValueError("本地 STRM 路径超出配置根目录") from exc

    @staticmethod
    def _relative_parts(relative_path: str) -> tuple[str, ...]:
        raw = str(relative_path or "").strip().replace("\\", "/")
        parts = tuple(raw.split("/"))
        if not parts or raw.startswith("/") or any(part in {"", ".", ".."} or "\x00" in part for part in parts):
            raise ValueError("本地 STRM 路径超出配置根目录")
        return parts

    @contextmanager
    def _root_descriptor(self) -> Iterator[int]:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(self._root, flags)
        except OSError as exc:
            try:
                self._assert_root()
            except RuntimeError as root_exc:
                raise root_exc from exc
            raise RuntimeError("本地 STRM 根目录在 gateway 打开后不可用") from exc
        try:
            state = os.fstat(descriptor)
            if not stat.S_ISDIR(state.st_mode) or state.st_dev != self._root_device or state.st_ino != self._root_inode:
                raise RuntimeError("本地 STRM 根目录在 gateway 打开后发生变化")
            self._assert_root()
            yield descriptor
        finally:
            os.close(descriptor)

    @contextmanager
    def _parent_descriptor(
        self,
        relative_path: str,
        *,
        create: bool,
    ) -> Iterator[tuple[int | None, str]]:
        parts = self._relative_parts(relative_path)
        with self._root_descriptor() as root_descriptor:
            descriptors: list[int] = []
            current = root_descriptor
            missing = False
            try:
                flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
                for part in parts[:-1]:
                    try:
                        child = os.open(part, flags, dir_fd=current)
                    except FileNotFoundError:
                        if not create:
                            missing = True
                            break
                        os.mkdir(part, dir_fd=current)
                        child = os.open(part, flags, dir_fd=current)
                    except OSError as exc:
                        raise RuntimeError(
                            f"本地 STRM 路径包含符号链接或父级不是安全目录: {self._root.joinpath(*parts[:-1])}"
                        ) from exc
                    child_state = os.fstat(child)
                    if not stat.S_ISDIR(child_state.st_mode):
                        os.close(child)
                        raise RuntimeError(f"本地 STRM 路径父级不是目录: {self._root.joinpath(*parts[:-1])}")
                    descriptors.append(child)
                    current = child
                yield (None if missing else current, parts[-1])
            finally:
                for descriptor in reversed(descriptors):
                    os.close(descriptor)

    def _target(self, relative_path: str, *, create_parent: bool) -> Path:
        self._assert_root()
        raw = str(relative_path or "").strip().replace("\\", "/")
        parts = tuple(part for part in raw.split("/") if part not in {"", "."})
        if not parts or any(part == ".." or "\x00" in part for part in parts) or raw.startswith("/"):
            raise ValueError("本地 STRM 路径超出配置根目录")
        target = self._root.joinpath(*parts)
        current = self._root
        for part in parts[:-1]:
            candidate = current / part
            try:
                state = candidate.lstat()
            except FileNotFoundError:
                if not create_parent:
                    current = candidate
                    continue
                candidate.mkdir()
                state = candidate.lstat()
            if stat.S_ISLNK(state.st_mode):
                raise RuntimeError(f"本地 STRM 路径包含符号链接: {candidate}")
            if not stat.S_ISDIR(state.st_mode):
                raise RuntimeError(f"本地 STRM 路径父级不是目录: {candidate}")
            resolved = candidate.resolve(strict=True)
            if not resolved.is_relative_to(self._root):
                raise ValueError("本地 STRM 路径超出配置根目录")
            current = resolved
        self._assert_root()
        resolved_parent = target.parent.resolve(strict=False)
        if not resolved_parent.is_relative_to(self._root):
            raise ValueError("本地 STRM 路径超出配置根目录")
        try:
            target_state = target.lstat()
        except FileNotFoundError:
            target_state = None
        if target_state is not None and stat.S_ISLNK(target_state.st_mode):
            raise RuntimeError(f"本地 STRM 文件不能是符号链接: {target}")
        self._assert_root()
        return resolved_parent / target.name

    def _assert_root(self) -> None:
        try:
            state = self._root.lstat()
            resolved = self._root.resolve(strict=True)
        except OSError as exc:
            raise RuntimeError("本地 STRM 根目录在 gateway 打开后不可用") from exc
        if (
            not stat.S_ISDIR(state.st_mode)
            or stat.S_ISLNK(state.st_mode)
            or resolved != self._root
            or state.st_dev != self._root_device
            or state.st_ino != self._root_inode
        ):
            raise RuntimeError("本地 STRM 根目录在 gateway 打开后发生变化")

    def _path_state_at(self, parent: int, name: str, *, display_path: str) -> P115LocalPathState:
        try:
            state = os.stat(name, dir_fd=parent, follow_symlinks=False)
        except FileNotFoundError:
            return P115LocalPathState(kind="absent")
        if stat.S_ISLNK(state.st_mode):
            raise RuntimeError(f"本地 STRM 路径不能是符号链接: {display_path}")
        if stat.S_ISDIR(state.st_mode):
            return P115LocalPathState(
                kind="directory",
                size=max(int(state.st_size), 0),
                device=state.st_dev,
                inode=state.st_ino,
                mtime_ns=state.st_mtime_ns,
            )
        if not stat.S_ISREG(state.st_mode):
            raise RuntimeError(f"本地 STRM 路径不是普通文件或目录: {display_path}")

        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(name, flags, dir_fd=parent)
        try:
            before = os.fstat(descriptor)
            digest = hashlib.sha256()
            total = 0
            while True:
                chunk = os.read(descriptor, 1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
                total += len(chunk)
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        if (
            before.st_dev != state.st_dev
            or before.st_ino != state.st_ino
            or after.st_dev != before.st_dev
            or after.st_ino != before.st_ino
            or before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns
            or total != after.st_size
        ):
            raise RuntimeError(f"本地 STRM 文件在身份读取期间发生变化: {display_path}")
        return P115LocalPathState(
            kind="file",
            size=max(int(after.st_size), 0),
            sha256=digest.hexdigest(),
            device=after.st_dev,
            inode=after.st_ino,
            mtime_ns=after.st_mtime_ns,
        )

    def _atomic_write_at(
        self,
        parent: int,
        name: str,
        content: bytes,
        *,
        previous: P115LocalPathState,
    ) -> None:
        mode = 0o644
        if previous.kind == "file":
            current_mode = os.stat(name, dir_fd=parent, follow_symlinks=False).st_mode
            mode = current_mode & 0o777
        temporary_name = f".{name}.{secrets.token_hex(12)}"
        descriptor = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            mode,
            dir_fd=parent,
        )
        try:
            written = 0
            while written < len(content):
                written += os.write(descriptor, content[written:])
            os.fsync(descriptor)
            os.fchmod(descriptor, mode)
        finally:
            os.close(descriptor)
        try:
            os.replace(
                temporary_name,
                name,
                src_dir_fd=parent,
                dst_dir_fd=parent,
            )
            os.fsync(parent)
        finally:
            try:
                os.unlink(temporary_name, dir_fd=parent)
            except FileNotFoundError:
                pass

    def _delete_file_if_matches(
        self,
        parent: int,
        name: str,
        action: P115LocalDeleteAction,
    ) -> str:
        quarantine = f".etk-delete-file-{secrets.token_hex(12)}"
        with self._root_descriptor() as root:
            try:
                os.rename(
                    name,
                    quarantine,
                    src_dir_fd=parent,
                    dst_dir_fd=root,
                )
            except FileNotFoundError:
                return "absent"
            try:
                state = self._path_state_at(root, quarantine, display_path=action.absolute_path)
            except (OSError, RuntimeError):
                state = None
            attached = self._root_is_attached(root) and self._parent_is_attached(root, parent, action.path)
            if state == action.expected and attached:
                os.unlink(quarantine, dir_fd=root)
                os.fsync(root)
                return "deleted"
            self._restore_quarantined_file(root, parent, name, quarantine)
            return "deferred"

    def _delete_directory_if_matches(
        self,
        parent: int,
        name: str,
        action: P115LocalDeleteAction,
    ) -> str:
        quarantine = f".etk-delete-directory-{secrets.token_hex(12)}"
        with self._root_descriptor() as root:
            try:
                os.rename(
                    name,
                    quarantine,
                    src_dir_fd=parent,
                    dst_dir_fd=root,
                )
            except FileNotFoundError:
                return "absent"
            try:
                state = self._path_state_at(root, quarantine, display_path=action.absolute_path)
            except (OSError, RuntimeError):
                state = None
            attached = self._root_is_attached(root) and self._parent_is_attached(root, parent, action.path)
            if (
                state is not None
                and state.kind == "directory"
                and (state.device, state.inode) == (action.expected.device, action.expected.inode)
                and attached
            ):
                try:
                    os.rmdir(quarantine, dir_fd=root)
                except OSError:
                    self._restore_quarantined_directory(root, parent, name, quarantine)
                    return "not_empty"
                os.fsync(root)
                return "deleted"
            self._restore_quarantined_directory(root, parent, name, quarantine)
            return "deferred"

    @staticmethod
    def _restore_quarantined_file(root: int, parent: int, name: str, quarantine: str) -> None:
        try:
            os.link(
                quarantine,
                name,
                src_dir_fd=root,
                dst_dir_fd=parent,
                follow_symlinks=False,
            )
        except FileExistsError as exc:
            raise RuntimeError(f"本地 STRM 删除目标在原子隔离期间发生变化，保留隔离文件: {quarantine}") from exc
        except OSError as exc:
            raise RuntimeError(f"本地 STRM 删除目标身份变化且无法安全还原，保留隔离文件: {quarantine}") from exc
        os.unlink(quarantine, dir_fd=root)
        os.fsync(root)
        os.fsync(parent)

    @staticmethod
    def _restore_quarantined_directory(root: int, parent: int, name: str, quarantine: str) -> None:
        try:
            os.stat(name, dir_fd=parent, follow_symlinks=False)
        except FileNotFoundError:
            try:
                os.rename(
                    quarantine,
                    name,
                    src_dir_fd=root,
                    dst_dir_fd=parent,
                )
            except OSError as exc:
                raise RuntimeError(f"本地 STRM 目录身份变化且无法安全还原，保留隔离目录: {quarantine}") from exc
            os.fsync(root)
            os.fsync(parent)
            return
        raise RuntimeError(f"本地 STRM 目录在原子隔离期间发生变化，保留隔离目录: {quarantine}")

    @staticmethod
    def _parent_is_attached(root: int, parent: int, relative_path: str) -> bool:
        parts = _AtomicP115LocalStrmLibrary._relative_parts(relative_path)
        current = os.dup(root)
        try:
            flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
            for part in parts[:-1]:
                child = os.open(part, flags, dir_fd=current)
                os.close(current)
                current = child
            expected = os.fstat(current)
            observed = os.fstat(parent)
            return (expected.st_dev, expected.st_ino) == (observed.st_dev, observed.st_ino)
        except OSError:
            return False
        finally:
            os.close(current)

    def _root_is_attached(self, root: int) -> bool:
        try:
            self._assert_root()
        except RuntimeError:
            return False
        state = os.fstat(root)
        return (state.st_dev, state.st_ino) == (self._root_device, self._root_inode)

    @staticmethod
    def _is_virtual_strm(path: Path) -> bool:
        try:
            return "/api/p115/virtual-play/" in path.read_text(encoding="utf-8")[:2048]
        except (OSError, UnicodeError):
            return False


class PostgresP115StrmSyncCoordinator:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    @contextmanager
    def hold(self, scope: str) -> Iterator[None]:
        key = str(scope or "").strip()
        if not key:
            raise ValueError("STRM sync lock scope must not be empty")
        with self._engine.connect() as connection:
            connection.execute(text("SELECT pg_advisory_lock(hashtextextended(:scope, 0))"), {"scope": key})
            try:
                yield
            finally:
                connection.execute(text("SELECT pg_advisory_unlock(hashtextextended(:scope, 0))"), {"scope": key})


__all__ = [
    "AtomicP115LocalStrmLibraryGateway",
    "LegacyP115StrmSyncGateway",
    "PostgresP115StrmSyncCoordinator",
]
