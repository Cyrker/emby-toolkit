from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import quote, urlsplit

from etk.platform import Settings

from .organize_records import P115OrganizeRecordWriter
from .sorting_rules import P115SortingRulesRepository, P115SortingRulesSnapshot
from .strm_sync import P115LocalStrmLibraryGateway, P115StrmSyncCoordinator, P115StrmSyncGateway
from .upload_projection import P115UploadIngestionIngress, P115UploadProjectionRepository

logger = logging.getLogger(__name__)
_VIDEOS = frozenset({".mp4", ".mkv", ".avi", ".ts", ".iso", ".rmvb", ".wmv", ".mov", ".m2ts", ".flv", ".mpg"})
_SUBTITLES = frozenset({".srt", ".ass", ".ssa", ".sub", ".vtt", ".sup"})


class NativeMoviePilotStrmProjection:
    """复用原子落盘、文件缓存和持久入库入口，不扫描或改变云端目录。"""

    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        repository: P115UploadProjectionRepository,
        records: P115OrganizeRecordWriter,
        sorting_rules: P115SortingRulesRepository,
        libraries: P115LocalStrmLibraryGateway,
        ingestion: P115UploadIngestionIngress,
        remote: P115StrmSyncGateway,
        coordinator: P115StrmSyncCoordinator,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._records = records
        self._sorting_rules = sorting_rules
        self._libraries = libraries
        self._ingestion = ingestion
        self._remote = remote
        self._coordinator = coordinator

    def project(
        self, *, file: Mapping[str, Any], event_key: str, assert_current: Callable[[], None]
    ) -> Mapping[str, Any]:
        settings = self._settings()
        relative = _relative_path(file, settings.p115_media_root_name)
        extension = PurePosixPath(relative).suffix.lower()
        if extension not in _VIDEOS | _SUBTITLES:
            return {"skipped": True, "reason": "unsupported_extension"}
        rules = self._sorting_rules.load()
        if rules.account_id and rules.account_id != str(file["account_id"]):
            raise RuntimeError("MP 直出：分类规则属于另一个 115 账号")
        target_cid, category_name = _record_category(relative, rules, settings.p115_media_root_name)
        library = self._libraries.open(settings.local_strm_root)
        if library is None:
            raise ValueError("MP 直出：本地 STRM 根目录不可用，请检查目录配置与挂载")
        base_url = str(settings.etk_server_url or "").strip().rstrip("/")
        parsed = urlsplit(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.query or parsed.fragment:
            raise ValueError("MP 直出：请配置有效的 embytool 访问地址")
        target = str(PurePosixPath(relative).with_suffix(".strm")) if extension in _VIDEOS else relative
        content = (
            f"{base_url}/api/p115/play/{quote(str(file['pick_code']), safe='')}/"
            f"{quote(str(file['file_name']), safe='')}"
        )
        subtitle = None
        if extension in _SUBTITLES and not library.exists(target):
            client = self._remote.open(expected_account_id=str(file["account_id"]))
            if client is None:
                raise RuntimeError("MP 直出：115 字幕客户端不可用")
            subtitle = client.download_subtitle(str(file["pick_code"]), max_bytes=128 * 1024 * 1024)
        with self._coordinator.hold("p115-strm-sync:local"):
            assert_current()
            current = self._settings()
            if (current.local_strm_root, current.p115_media_root_name, current.etk_server_url) != (
                settings.local_strm_root,
                settings.p115_media_root_name,
                settings.etk_server_url,
            ):
                raise RuntimeError("MP 直出配置在落盘前发生变化")
            if extension in _VIDEOS and library.exists(target) and not library.strm_matches(target, content):
                raise ValueError("MP 直出：目标 STRM 已存在且内容不同，拒绝覆盖")
            # 先持久化播放/媒体流身份，再让文件监控看到新文件。
            self._repository.persist_remote_file(
                account_id=str(file["account_id"]),
                file_id=str(file["file_id"]),
                parent_id=str(file["parent_id"]),
                name=str(file["file_name"]),
                sha1=str(file["sha1"]),
                pick_code=str(file["pick_code"]),
                size=int(file["size"]),
                local_path=relative,
            )
            assert_current()
            changed = False
            ingestion_submitted = False
            if extension in _VIDEOS:
                if not library.strm_matches(target, content):
                    changed = library.create_strm(target, content).changed
                # 重试时即使文件已存在，也补交同一持久事件，覆盖落盘后进程退出的窗口。
                recorded = self._ingestion.record_event(
                    "created",
                    library.absolute_path(target),
                    source="active",
                    source_event_key=f"moviepilot-passthrough:{event_key}",
                )
                ingestion_submitted = bool(recorded)
            elif subtitle is not None:
                changed = library.write_subtitle(target, subtitle).changed
            record_created = False
            if extension in _SUBTITLES or ingestion_submitted:
                assert_current()
                record_created = self._records.add_missing_record(
                    {
                        "file_id": str(file["file_id"]),
                        "pick_code": str(file["pick_code"]),
                        "original_name": str(file["file_name"]),
                        "tmdb_id": str(file["tmdb_id"]),
                        "media_type": str(file["media_type"]),
                        "season_number": file.get("season_number"),
                        "target_cid": target_cid,
                        "category_name": category_name,
                    }
                )
            else:
                logger.info("MP 直出未写成功记录：入库事件被策略跳过；%s", target)
        logger.info("MP 直出已完成单文件映射：%s；已提交入库=%s", target, ingestion_submitted)
        return {
            "local_path": library.absolute_path(target),
            "changed": changed,
            "ingestion_submitted": ingestion_submitted,
            "record_created": record_created,
        }


def _record_category(
    relative: str, rules: P115SortingRulesSnapshot, media_root_name: str
) -> tuple[str | None, str]:
    parent = str(PurePosixPath(relative).parent).casefold()
    matches: set[tuple[str, str]] = set()
    for rule in rules.rules:
        cid = str(rule.get("cid") or "").strip()
        path = str(rule.get("category_path") or rule.get("dir_name") or "").strip().replace("\\", "/").strip("/")
        root = str(media_root_name or "").strip("/")
        if root and (path == root or path.startswith(root + "/")):
            path = path[len(root):].strip("/")
        if rule.get("enabled") is False or not cid or cid == "0" or not path:
            continue
        if parent == path.casefold() or parent.startswith(path.casefold() + "/"):
            matches.add((cid, path))
    if matches:
        longest = max(len(path) for _, path in matches)
        specific = {(cid, path) for cid, path in matches if len(path) == longest}
        if len(specific) == 1:
            cid, path = specific.pop()
            return cid, f"MP直出/{path}"
    return None, "MP直出/未匹配分类"


def _relative_path(file: Mapping[str, Any], media_root_name: str) -> str:
    path = str(file.get("path") or "")
    parts = path.split("/")
    root = str(media_root_name or "").strip("/")
    if not path.startswith("/") or not root or "/" in root or "\\" in path:
        raise ValueError("MP 直出：云端路径或媒体库根目录名称无效")
    if any(part in {".", ".."} or any(ord(c) < 32 for c in part) for part in parts):
        raise ValueError("MP 直出：云端路径包含非法分量")
    if parts.count(root) != 1:
        raise ValueError("MP 直出：无法在云端路径中唯一定位媒体库根目录")
    relative = parts[parts.index(root) + 1 :]
    if not relative or any(not part for part in relative) or relative[-1] != file.get("file_name"):
        raise ValueError("MP 直出：云端路径与文件名不匹配")
    return "/".join(relative)
