from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import PurePosixPath
from typing import Any, Protocol
from urllib.parse import unquote, urlsplit

from etk.platform.tasks import TaskOutcome

from .auth_directory import P115CredentialStore
from .directory_sync import P115TaskProgress

_SHA1_PATTERN = re.compile(r"^[A-F0-9]{40}$")
_REVISION_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_VIRTUAL_STRM_PATTERN = re.compile(r"/api/p115/virtual-play/\d+/[A-Fa-f0-9]{40}(?:/|$)")
_VIDEO_EXTENSIONS = frozenset(
    {
        ".avi",
        ".flv",
        ".iso",
        ".m2ts",
        ".m4v",
        ".mkv",
        ".mov",
        ".mp4",
        ".mpg",
        ".mpeg",
        ".rmvb",
        ".strm",
        ".ts",
        ".webm",
        ".wmv",
    }
)


class P115FingerprintConflict(RuntimeError):
    pass


class P115FingerprintAccountChanged(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class P115FileFingerprint:
    file_id: str
    parent_id: str
    name: str
    sha1: str
    pick_code: str
    size: int
    account_id: str
    local_path: str = ""

    def __post_init__(self) -> None:
        normalized = {
            "file_id": str(self.file_id or "").strip(),
            "parent_id": str(self.parent_id or "").strip(),
            "name": str(self.name or "").strip(),
            "sha1": canonical_sha1(self.sha1),
            "pick_code": str(self.pick_code or "").strip(),
            "account_id": str(self.account_id or "").strip(),
            "local_path": _relative_local_path(self.local_path),
        }
        if not normalized["file_id"] or not normalized["parent_id"] or not normalized["name"]:
            raise ValueError("115 fingerprint requires stable file, parent, and name identity")
        if not normalized["sha1"] or not normalized["pick_code"]:
            raise ValueError("115 fingerprint requires PickCode and SHA1")
        if not normalized["account_id"].isdigit():
            raise ValueError("115 fingerprint requires a stable numeric account ID")
        if isinstance(self.size, bool) or int(self.size) < 0:
            raise ValueError("115 fingerprint size must be non-negative")
        for field, value in normalized.items():
            object.__setattr__(self, field, value)
        object.__setattr__(self, "size", int(self.size))


@dataclass(frozen=True, slots=True)
class P115FingerprintHints:
    pick_code: str = ""
    sha1: str = ""
    local_paths: tuple[str, ...] = ()
    virtual: bool = False

    def __post_init__(self) -> None:
        pick_code = str(self.pick_code or "").strip()
        sha1 = canonical_sha1(self.sha1, allow_empty=True)
        local_paths = tuple(dict.fromkeys(path for value in self.local_paths if (path := _relative_local_path(value))))
        object.__setattr__(self, "pick_code", pick_code)
        object.__setattr__(self, "sha1", sha1)
        object.__setattr__(self, "local_paths", local_paths)
        object.__setattr__(self, "virtual", bool(self.virtual))


@dataclass(frozen=True, slots=True)
class P115FingerprintRepairRow:
    tmdb_id: str
    item_type: str
    title: str
    assets: tuple[Mapping[str, Any], ...]
    sha1s: tuple[str, ...]
    pick_codes: tuple[str, ...]
    revision: str

    def __post_init__(self) -> None:
        if not str(self.tmdb_id or "").strip():
            raise ValueError("fingerprint repair row requires TMDb ID")
        if self.item_type not in {"Movie", "Episode"}:
            raise ValueError("fingerprint repair only supports Movie and Episode rows")
        if any(not isinstance(asset, Mapping) for asset in self.assets):
            raise ValueError("fingerprint repair assets must be objects")
        if len(self.sha1s) > len(self.assets) or len(self.pick_codes) > len(self.assets):
            raise ValueError("fingerprint slots must remain position-aligned with assets")
        if not _REVISION_PATTERN.fullmatch(str(self.revision or "")):
            raise ValueError("fingerprint repair row revision is invalid")

    @classmethod
    def create(
        cls,
        *,
        tmdb_id: object,
        item_type: object,
        title: object,
        assets: Sequence[Mapping[str, Any]],
        sha1s: Sequence[object],
        pick_codes: Sequence[object],
        revision: object,
    ) -> P115FingerprintRepairRow:
        normalized_assets = tuple(dict(asset) for asset in assets)
        normalized_sha1s = [canonical_sha1(value, allow_empty=True) for value in sha1s]
        normalized_pick_codes = [str(value or "").strip() for value in pick_codes]
        if len(normalized_sha1s) > len(normalized_assets) or len(normalized_pick_codes) > len(normalized_assets):
            raise ValueError("fingerprint slots must remain position-aligned with assets")
        normalized_sha1s.extend("" for _ in range(len(normalized_assets) - len(normalized_sha1s)))
        normalized_pick_codes.extend("" for _ in range(len(normalized_assets) - len(normalized_pick_codes)))
        return cls(
            tmdb_id=str(tmdb_id or "").strip(),
            item_type=str(item_type or "").strip().title(),
            title=str(title or "").strip(),
            assets=normalized_assets,
            sha1s=tuple(normalized_sha1s),
            pick_codes=tuple(normalized_pick_codes),
            revision=str(revision or "").strip().lower(),
        )


class P115FingerprintRepository(Protocol):
    def list_repair_rows(self) -> tuple[P115FingerprintRepairRow, ...]: ...

    def find_fingerprint(
        self,
        *,
        account_id: str,
        pick_code: str = "",
        sha1: str = "",
        file_id: str = "",
        local_paths: Sequence[str] = (),
    ) -> P115FileFingerprint | None: ...

    def commit_repair(
        self,
        row: P115FingerprintRepairRow,
        asset_index: int,
        fingerprint: P115FileFingerprint,
        *,
        account_id: str,
    ) -> bool: ...


class P115FingerprintHintGateway(Protocol):
    def inspect(self, path: str, *, local_root: str) -> P115FingerprintHints: ...


class P115FingerprintRemoteClient(Protocol):
    backend: str

    def lookup(self, pick_code: str) -> P115FileFingerprint | None: ...


class P115FingerprintRemoteGateway(Protocol):
    def open(self, *, expected_account_id: str) -> P115FingerprintRemoteClient | None: ...


class P115FingerprintRepairService:
    """Repair aligned Movie/Episode fingerprints with cache-first, account-bound writes."""

    def __init__(
        self,
        *,
        credentials: P115CredentialStore,
        repository: P115FingerprintRepository,
        hints: P115FingerprintHintGateway,
        remote: P115FingerprintRemoteGateway,
        local_root: Callable[[], str],
    ) -> None:
        self._credentials = credentials
        self._repository = repository
        self._hints = hints
        self._remote = remote
        self._local_root = local_root

    def execute(self, progress: P115TaskProgress) -> TaskOutcome:
        progress.update(0, "正在扫描缺失 PC/SHA1 的在库视频...")
        account_id = str(self._credentials.load().account_id or "").strip()
        if not account_id.isdigit():
            message = "无法确认稳定的 115 账号身份，指纹缓存未修改。"
            progress.update(-1, message)
            return TaskOutcome.failed(message, _stats(total_rows=0, account_drift=True))

        rows = self._repository.list_repair_rows()
        if not rows:
            message = "没有可处理的在库电影/分集资产。"
            progress.update(100, message)
            return TaskOutcome.skipped(message, _stats(total_rows=0))

        stats = _stats(total_rows=len(rows))
        remote_client: P115FingerprintRemoteClient | None = None
        local_root = str(self._local_root() or "").strip()
        for row_index, row in enumerate(rows):
            progress.checkpoint()
            progress.update(
                int(row_index / max(len(rows), 1) * 100),
                f"正在检查 ({row_index + 1}/{len(rows)})，已修复 {stats['fixed_assets']} 个资产...",
            )
            for asset_index, asset in enumerate(row.assets):
                progress.checkpoint()
                path = _asset_path(asset)
                if not path:
                    stats["skipped_invalid"] += 1
                    continue
                if _is_virtual_target(path):
                    stats["skipped_virtual"] += 1
                    continue
                if _path_extension(path) not in _VIDEO_EXTENSIONS:
                    stats["skipped_non_video"] += 1
                    continue

                stats["scanned_assets"] += 1
                current_sha1 = row.sha1s[asset_index]
                current_pick_code = row.pick_codes[asset_index]
                if not current_sha1 or not current_pick_code:
                    stats["missing_assets"] += 1

                try:
                    inspected = self._hints.inspect(path, local_root=local_root)
                    if inspected.virtual:
                        stats["skipped_virtual"] += 1
                        stats["scanned_assets"] -= 1
                        if not current_sha1 or not current_pick_code:
                            stats["missing_assets"] -= 1
                        continue
                    known_sha1 = _compatible_value(
                        current_sha1,
                        inspected.sha1,
                        label="SHA1",
                        normalize=lambda value: canonical_sha1(value, allow_empty=True),
                    )
                    known_pick_code = _compatible_value(
                        current_pick_code,
                        inspected.pick_code,
                        label="PickCode",
                        normalize=lambda value: str(value or "").strip(),
                    )
                    cached = self._repository.find_fingerprint(
                        account_id=account_id,
                        pick_code=known_pick_code,
                        sha1=known_sha1,
                        local_paths=inspected.local_paths,
                    )
                    if cached is not None:
                        _assert_fingerprint_compatible(
                            cached,
                            account_id=account_id,
                            sha1=known_sha1,
                            pick_code=known_pick_code,
                        )
                        candidate = cached
                        source = "cache"
                    else:
                        candidate = None
                        source = ""

                    if candidate is None and known_pick_code:
                        self._assert_account(account_id)
                        if remote_client is None:
                            remote_client = self._remote.open(expected_account_id=account_id)
                        if remote_client is not None:
                            remote_value = remote_client.lookup(known_pick_code)
                            self._assert_account(account_id)
                            if remote_value is not None:
                                _assert_fingerprint_compatible(
                                    remote_value,
                                    account_id=account_id,
                                    sha1=known_sha1,
                                    pick_code=known_pick_code,
                                )
                                candidate = (
                                    replace(
                                        remote_value,
                                        local_path=_preferred_local_path(inspected.local_paths),
                                    )
                                    if not remote_value.local_path and inspected.local_paths
                                    else remote_value
                                )
                                source = "remote"

                    if candidate is None:
                        stats["failed_assets"] += 1
                        continue
                    already_complete = current_sha1 == candidate.sha1 and current_pick_code == candidate.pick_code
                    if already_complete:
                        if source == "remote":
                            self._assert_account(account_id)
                            if not self._repository.commit_repair(
                                row,
                                asset_index,
                                candidate,
                                account_id=account_id,
                            ):
                                stats["failed_assets"] += 1
                                continue
                            stats["cache_updates"] += 1
                            stats["cache_seeded_assets"] += 1
                        stats["verified_assets"] += 1
                        continue
                    self._assert_account(account_id)
                    if not self._repository.commit_repair(
                        row,
                        asset_index,
                        candidate,
                        account_id=account_id,
                    ):
                        stats["failed_assets"] += 1
                        continue
                    stats["fixed_assets"] += 1
                    stats["updated_rows"] += 1
                    stats["cache_updates"] += 1
                    if source == "cache":
                        stats["cache_fixed_assets"] += 1
                    elif source == "remote":
                        stats["remote_fixed_assets"] += 1
                except P115FingerprintAccountChanged:
                    stats["account_drift"] = True
                    stats["failed_assets"] += 1
                except (P115FingerprintConflict, ValueError):
                    stats["failed_assets"] += 1

        message = (
            f"补齐115指纹完成：扫描资产 {stats['scanned_assets']}，缺失 {stats['missing_assets']}，"
            f"成功补齐 {stats['fixed_assets']}，缓存命中 {stats['cache_fixed_assets']}，"
            f"现场查询 {stats['remote_fixed_assets']}，失败 {stats['failed_assets']}。"
        )
        if stats["failed_assets"]:
            progress.update(-1, message)
            if stats["fixed_assets"] or stats["verified_assets"]:
                return TaskOutcome.partial(message, stats)
            return TaskOutcome.failed(message, stats)
        progress.update(100, message)
        return TaskOutcome.succeeded(message, stats)

    def _assert_account(self, expected_account_id: str) -> None:
        current = str(self._credentials.load().account_id or "").strip()
        if current != expected_account_id:
            raise P115FingerprintAccountChanged("115 account changed during fingerprint repair")


def canonical_sha1(value: object, *, allow_empty: bool = False) -> str:
    normalized = str(value or "").strip().upper()
    if not normalized and allow_empty:
        return ""
    if not _SHA1_PATTERN.fullmatch(normalized):
        raise ValueError("SHA1 must be 40 hexadecimal characters")
    return normalized


def _stats(*, total_rows: int, account_drift: bool = False) -> dict[str, int | bool]:
    return {
        "total_rows": total_rows,
        "scanned_assets": 0,
        "missing_assets": 0,
        "fixed_assets": 0,
        "verified_assets": 0,
        "cache_fixed_assets": 0,
        "remote_fixed_assets": 0,
        "failed_assets": 0,
        "updated_rows": 0,
        "cache_updates": 0,
        "cache_seeded_assets": 0,
        "skipped_virtual": 0,
        "skipped_non_video": 0,
        "skipped_invalid": 0,
        "account_drift": account_drift,
    }


def _compatible_value(
    first: object,
    second: object,
    *,
    label: str,
    normalize: Callable[[object], str],
) -> str:
    left = normalize(first)
    right = normalize(second)
    if left and right and left != right:
        raise P115FingerprintConflict(f"conflicting {label} values")
    return left or right


def _assert_fingerprint_compatible(
    fingerprint: P115FileFingerprint,
    *,
    account_id: str,
    sha1: str,
    pick_code: str,
) -> None:
    if fingerprint.account_id != account_id:
        raise P115FingerprintConflict("fingerprint belongs to another 115 account")
    if sha1 and fingerprint.sha1 != sha1:
        raise P115FingerprintConflict("fingerprint SHA1 conflicts with the media slot")
    if pick_code and fingerprint.pick_code != pick_code:
        raise P115FingerprintConflict("fingerprint PickCode conflicts with the media slot")


def _asset_path(asset: Mapping[str, Any]) -> str:
    return str(asset.get("path") or asset.get("Path") or "").strip()


def _is_virtual_target(value: str) -> bool:
    return bool(_VIRTUAL_STRM_PATTERN.search(str(value or "")))


def _path_extension(value: str) -> str:
    parsed = urlsplit(unquote(str(value or "").strip()))
    path = parsed.path if parsed.scheme else str(value or "").split("#", 1)[0].split("?", 1)[0]
    return PurePosixPath(path.replace("\\", "/")).suffix.lower()


def _preferred_local_path(values: Sequence[str]) -> str:
    candidates = tuple(dict.fromkeys(_relative_local_path(value) for value in values if value))
    videos = tuple(value for value in candidates if _path_extension(value) in (_VIDEO_EXTENSIONS - {".strm"}))
    return min(videos or candidates, key=len, default="")


def _relative_local_path(value: object) -> str:
    text = unquote(str(value or "").strip()).replace("\\", "/")
    if not text:
        return ""
    parsed = urlsplit(text)
    if parsed.scheme or parsed.netloc or text.startswith("/") or re.match(r"^[A-Za-z]:/", text):
        raise ValueError("fingerprint local path must be relative")
    path = PurePosixPath(text)
    if any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("fingerprint local path is invalid")
    return path.as_posix()


__all__ = [
    "P115FileFingerprint",
    "P115FingerprintAccountChanged",
    "P115FingerprintConflict",
    "P115FingerprintHintGateway",
    "P115FingerprintHints",
    "P115FingerprintRemoteClient",
    "P115FingerprintRemoteGateway",
    "P115FingerprintRepairRow",
    "P115FingerprintRepairService",
    "P115FingerprintRepository",
    "canonical_sha1",
]
