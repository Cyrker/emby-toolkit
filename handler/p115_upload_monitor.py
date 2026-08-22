import hashlib
import json
import logging
import os
import queue
import tempfile
import threading
import time
import uuid
from typing import Any, Dict, Iterable, List, Optional

from watchdog.events import FileSystemEventHandler

import config_manager
import constants
from database import p115_upload_db, settings_db
from etk.compatibility.background import BackgroundHandle, spawn, spawn_later


logger = logging.getLogger(__name__)

UPLOAD_MONITOR_CONFIG_KEY = "p115_upload_monitor_config"
DEFAULT_PART_SIZE = 64 * 1024 * 1024
DIRECT_UPLOAD_MAX_SIZE = DEFAULT_PART_SIZE
SCAN_BATCH_SIZE = 100
SCAN_MAX_QUEUED = 500
UPLOAD_IGNORED_EXTENSIONS = {".strm"}
UPLOAD_IGNORED_SUFFIXES = (
    ".!qb", ".part", ".partial", ".filepart", ".crdownload", ".download", ".opdownload",
    ".aria2", ".tmp", ".temp", ".td", ".xltd", ".ut!", ".!ut", ".bc!", ".jc!",
)
MEDIA_OUTPUT_EXTENSIONS = {
    ".mp4", ".mkv", ".avi", ".mov", ".iso", ".ts", ".m2ts", ".wmv", ".rmvb", ".flv", ".mpg", ".mpeg",
    ".flac", ".m4a", ".mp3", ".aac", ".wav", ".ape", ".ogg", ".opus",
}
VIDEO_OUTPUT_EXTENSIONS = {
    ".mp4", ".mkv", ".avi", ".mov", ".iso", ".ts", ".m2ts", ".wmv", ".rmvb", ".flv",
    ".mpg", ".mpeg", ".webm",
}


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "enabled"}
    return bool(value)


def _normalize_extensions(extensions: Iterable[str]) -> set[str]:
    normalized = set()
    for value in extensions or []:
        ext = str(value or "").strip().lower().replace("*", "")
        if not ext:
            continue
        normalized.add(ext if ext.startswith(".") else f".{ext}")
    return normalized


def _mapping_id(local_dir: str, target_cid: str) -> str:
    raw = f"{os.path.normcase(local_dir)}\0{target_cid}".encode("utf-8", errors="ignore")
    return hashlib.sha1(raw).hexdigest()[:16]


def _default_upload_extensions() -> set[str]:
    return _normalize_extensions(
        config_manager.APP_CONFIG.get(
            constants.CONFIG_OPTION_MONITOR_EXTENSIONS,
            constants.DEFAULT_MONITOR_EXTENSIONS,
        )
    ) - UPLOAD_IGNORED_EXTENSIONS


def normalize_upload_monitor_config(value: Any) -> Dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    mappings = []
    seen = set()
    for item in (raw.get("mappings") if isinstance(raw.get("mappings"), list) else []):
        if not isinstance(item, dict):
            continue
        local_dir = os.path.normpath(str(item.get("local_dir") or "").strip())
        target_cid = str(item.get("target_cid") or "").strip()
        if not local_dir or not target_cid or target_cid == "0":
            continue
        dedupe_key = os.path.normcase(os.path.abspath(local_dir))
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        mode = str(item.get("mode") or "keep").strip().lower()
        if mode not in {"keep", "delete"}:
            mode = "keep"
        extension_values = item.get("extensions")
        extensions = (
            _normalize_extensions(extension_values)
            if isinstance(extension_values, list)
            else _default_upload_extensions()
        ) - UPLOAD_IGNORED_EXTENSIONS
        mappings.append({
            "id": str(item.get("id") or _mapping_id(local_dir, target_cid)).strip(),
            "enabled": _as_bool(item.get("enabled"), True),
            "local_dir": local_dir,
            "target_cid": target_cid,
            "target_name": str(item.get("target_name") or target_cid).strip(),
            "mode": mode,
            "extensions": sorted(extensions),
        })
    return {"enabled": _as_bool(raw.get("enabled"), False), "mappings": mappings}


def get_upload_monitor_config() -> Dict[str, Any]:
    return normalize_upload_monitor_config(settings_db.get_setting(UPLOAD_MONITOR_CONFIG_KEY) or {})


def save_upload_monitor_config(value: Any) -> Dict[str, Any]:
    config = normalize_upload_monitor_config(value)
    settings_db.save_setting(UPLOAD_MONITOR_CONFIG_KEY, config)
    UploadMonitorRuntime.instance().prune_jobs(config)
    return config


def upload_monitor_enabled() -> bool:
    config = get_upload_monitor_config()
    UploadMonitorRuntime.instance().prune_jobs(config)
    return bool(config["enabled"] and any(item["enabled"] for item in config["mappings"]))


def get_upload_monitor_status() -> Dict[str, Any]:
    status = p115_upload_db.get_status()
    public_keys = {
        "id", "status", "path", "relative_path", "target_name", "mode", "fingerprint",
        "uploaded_bytes", "total_bytes", "progress", "upload_backend", "error", "attempts", "updated_at",
    }
    recent = [
        {key: item.get(key) for key in public_keys if key in item}
        for item in status.get("recent", [])
    ]
    return {**status, "recent": recent}


def _file_fingerprint(path: str) -> Dict[str, int]:
    stat = os.stat(path)
    return {"size": int(stat.st_size), "mtime_ns": int(stat.st_mtime_ns)}


def _same_fingerprint(left: Any, right: Any) -> bool:
    return bool(
        isinstance(left, dict)
        and isinstance(right, dict)
        and int(left.get("size") or -1) == int(right.get("size") or -2)
        and int(left.get("mtime_ns") or -1) == int(right.get("mtime_ns") or -2)
    )


def _path_in_root(path: str, root: str) -> bool:
    try:
        return os.path.commonpath([os.path.abspath(path), os.path.abspath(root)]) == os.path.abspath(root)
    except (ValueError, OSError):
        return False


def _is_ignored_upload_path(path: str) -> bool:
    lower_path = str(path or "").lower()
    return (
        os.path.splitext(lower_path)[1] in UPLOAD_IGNORED_EXTENSIONS
        or lower_path.endswith(UPLOAD_IGNORED_SUFFIXES)
    )


def _find_mapping(path: str, mappings: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    matches = [item for item in mappings if item.get("enabled") and _path_in_root(path, item["local_dir"])]
    if not matches:
        return None
    return max(matches, key=lambda item: len(os.path.abspath(item["local_dir"])))


class UploadMonitorEventHandler(FileSystemEventHandler):
    def __init__(self, mappings: List[Dict[str, Any]]):
        self.mappings = mappings

    def _accept(self, path: str) -> bool:
        if not path or os.path.basename(path).startswith("."):
            return False
        if _is_ignored_upload_path(path):
            return False
        extension = os.path.splitext(path)[1].lower()
        mapping = _find_mapping(path, self.mappings)
        return (
            extension not in UPLOAD_IGNORED_EXTENSIONS
            and mapping is not None
            and extension in _normalize_extensions(mapping.get("extensions"))
        )

    def _schedule(self, path: str) -> None:
        if self._accept(path):
            UploadMonitorRuntime.instance().schedule_path(path, self.mappings)

    def _schedule_delete(self, path: str) -> None:
        mapping = _find_mapping(path, self.mappings)
        if self._accept(path) and mapping and mapping.get("mode") == "keep":
            UploadMonitorRuntime.instance().schedule_deleted_path(path)

    def on_created(self, event):
        if not event.is_directory:
            self._schedule(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            self._schedule(event.src_path)

    def on_deleted(self, event):
        if not event.is_directory:
            self._schedule_delete(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self._schedule_delete(event.src_path)
            self._schedule(event.dest_path)


class UploadMonitorRuntime:
    _instance = None
    _instance_lock = threading.Lock()

    def __init__(self):
        self._queue: queue.Queue[str | None] = queue.Queue()
        self._queued = set()
        self._timers: Dict[str, BackgroundHandle] = {}
        self._suppressed_deletes: Dict[str, float] = {}
        self._lock = threading.RLock()
        self._state_lock = threading.RLock()
        self._remote_dir_cache: Dict[tuple, str] = {}
        self._worker_started = False
        self._worker_thread: threading.Thread | None = None

    @classmethod
    def instance(cls):
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def start(self) -> None:
        with self._lock:
            if self._worker_thread and self._worker_thread.is_alive():
                return
            self._worker_started = True
            worker = threading.Thread(target=self._worker_loop, name="P115UploadMonitor", daemon=True)
            self._worker_thread = worker
            worker.start()

    def stop(self, timeout: float = 5.0) -> bool:
        with self._lock:
            worker = self._worker_thread
            for timer in self._timers.values():
                timer.cancel()
            self._timers.clear()
            while True:
                try:
                    queued = self._queue.get_nowait()
                except queue.Empty:
                    break
                if queued is not None:
                    self._queued.discard(queued)
                self._queue.task_done()
            if worker and worker.is_alive():
                self._queue.put(None)
        if worker and worker is not threading.current_thread():
            worker.join(timeout=max(0.0, float(timeout)))
        with self._lock:
            if worker and worker.is_alive():
                return False
            if self._worker_thread is worker:
                self._worker_thread = None
            self._worker_started = False
            return True

    def prune_jobs(self, config: Optional[Dict[str, Any]] = None) -> int:
        config = config if isinstance(config, dict) else get_upload_monitor_config()
        active_mappings = {
            item["id"]: item
            for item in config.get("mappings", [])
            if config.get("enabled") and item.get("enabled")
        }
        removed_paths = []
        with self._state_lock:
            jobs = p115_upload_db.list_jobs()
            removed_job_ids = []
            for job_id, job in jobs.items():
                mapping = active_mappings.get(str((job or {}).get("mapping_id") or ""))
                path = str((job or {}).get("path") or "")
                if (
                    not mapping
                    or not _path_in_root(path, mapping["local_dir"])
                    or str((job or {}).get("target_cid") or "") != str(mapping["target_cid"])
                ):
                    removed_job_ids.append(job_id)
                    if path:
                        removed_paths.append(os.path.normpath(path))
            p115_upload_db.delete_jobs(removed_job_ids)

        if removed_paths:
            with self._lock:
                for path in removed_paths:
                    timer = self._timers.pop(path, None)
                    if timer:
                        timer.cancel()
            logger.info("  ➜ [115上传监控] 已清理 %s 个失效目录映射的上传任务。", len(removed_paths))
        return len(removed_paths)

    def schedule_path(self, path: str, mappings: List[Dict[str, Any]], delay: float = 3.0) -> None:
        norm_path = os.path.normpath(path)
        with self._lock:
            old = self._timers.pop(norm_path, None)
            if old:
                old.cancel()
            timer = spawn_later(delay, self.enqueue_path, norm_path, mappings)
            self._timers[norm_path] = timer

    def schedule_deleted_path(self, path: str, delay: float = 2.0) -> None:
        norm_path = os.path.normpath(path)
        with self._lock:
            old = self._timers.pop(norm_path, None)
            if old:
                old.cancel()
            timer = spawn_later(delay, self._handle_deleted_path, norm_path)
            self._timers[norm_path] = timer

    @staticmethod
    def _delete_path_key(path: str) -> str:
        return os.path.normcase(os.path.abspath(os.path.normpath(path)))

    def suppress_deleted_path(self, path: str, ttl: float = 15.0) -> None:
        now = time.monotonic()
        with self._lock:
            self._suppressed_deletes = {
                key: expires_at
                for key, expires_at in self._suppressed_deletes.items()
                if expires_at > now
            }
            self._suppressed_deletes[self._delete_path_key(path)] = now + ttl

    def unsuppress_deleted_path(self, path: str) -> None:
        with self._lock:
            self._suppressed_deletes.pop(self._delete_path_key(path), None)

    def _consume_suppressed_delete(self, path: str) -> bool:
        now = time.monotonic()
        with self._lock:
            expires_at = self._suppressed_deletes.pop(self._delete_path_key(path), 0)
        return expires_at > now

    def _handle_deleted_path(self, path: str) -> None:
        with self._lock:
            self._timers.pop(os.path.normpath(path), None)
        if self._consume_suppressed_delete(path):
            logger.debug("  ➜ [115上传监控] 已忽略 Emby 联动流程产生的本地删除事件: %s", path)
            return
        if os.path.exists(path):
            return

        config = get_upload_monitor_config()
        mapping = _find_mapping(path, config.get("mappings", []))
        if not config.get("enabled") or not mapping or mapping.get("mode") != "keep":
            return

        records = p115_upload_db.find_completed_by_local_paths([os.path.abspath(path)])
        records = [row for row in records if str(row.get("mapping_id") or "") == str(mapping.get("id") or "")]
        for record in records:
            completed = record.get("payload_json") or {}
            if not isinstance(completed, dict):
                continue
            strm_path = self._remove_local_outputs(mapping, completed, path)
            if strm_path:
                emby_url = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
                emby_api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY)
                if emby_url and emby_api_key:
                    try:
                        from handler import emby
                        emby.notify_emby_file_changes([strm_path], emby_url, emby_api_key, update_type="Deleted")
                    except Exception as exc:
                        logger.warning("  ➜ [115上传监控] 通知 Emby 删除本地镜像失败: %s -> %s", strm_path, exc)

            pick_code = str(record.get("pick_code") or completed.get("pick_code") or "").strip()
            if pick_code:
                from handler.p115_service import delete_115_files_by_webhook
                delete_115_files_by_webhook([pick_code])
            else:
                p115_upload_db.delete_completed_and_jobs([record.get("record_key")], [path])
                logger.warning("  ➜ [115上传监控] 本地源文件已删除，但历史记录缺少 pick_code，无法联动删除 115: %s", path)
            logger.info("  ➜ [115上传监控] 本地删除已触发双向同步: %s", path)

    def enqueue_path(self, path: str, mappings: Optional[List[Dict[str, Any]]] = None) -> None:
        with self._lock:
            self._timers.pop(os.path.normpath(path), None)
        self._enqueue_paths([path], mappings)

    def _enqueue_paths(self, paths: Iterable[str], mappings: Optional[List[Dict[str, Any]]] = None) -> int:
        config = get_upload_monitor_config()
        if not config["enabled"]:
            return 0
        active_mappings = mappings or config["mappings"]
        job_ids = []

        with self._state_lock:
            candidates = []
            for path in paths:
                if not os.path.isfile(path):
                    continue
                mapping = _find_mapping(path, active_mappings)
                if not mapping:
                    continue
                try:
                    fingerprint = _file_fingerprint(path)
                except OSError:
                    continue
                rel_path = os.path.relpath(path, mapping["local_dir"])
                completed_key = f"{mapping['id']}:{os.path.normcase(os.path.abspath(path))}"
                job_id = hashlib.sha1(
                    f"{completed_key}\0{fingerprint['size']}\0{fingerprint['mtime_ns']}".encode("utf-8", errors="ignore")
                ).hexdigest()
                candidates.append((path, mapping, fingerprint, rel_path, completed_key, job_id))

            completed_records = p115_upload_db.get_completed(item[4] for item in candidates)
            existing_jobs = p115_upload_db.get_jobs(item[5] for item in candidates)
            jobs_to_save = []
            for path, mapping, fingerprint, rel_path, completed_key, job_id in candidates:
                if _same_fingerprint((completed_records.get(completed_key) or {}).get("fingerprint"), fingerprint):
                    continue
                job = existing_jobs.get(job_id) or {}
                job.update({
                    "id": job_id,
                    "status": "pending",
                    "path": path,
                    "relative_path": rel_path,
                    "mapping_id": mapping["id"],
                    "target_cid": mapping["target_cid"],
                    "target_name": mapping["target_name"],
                    "mode": mapping["mode"],
                    "fingerprint": fingerprint,
                    "updated_at": time.time(),
                })
                jobs_to_save.append(job)
                job_ids.append(job_id)
            p115_upload_db.upsert_jobs(jobs_to_save)

        if not job_ids:
            return 0
        self.start()
        with self._lock:
            for job_id in job_ids:
                if job_id not in self._queued:
                    self._queued.add(job_id)
                    self._queue.put(job_id)
        return len(job_ids)

    def scan_existing(self, mappings: List[Dict[str, Any]]) -> None:
        def _scan():
            batch = []
            queued_count = 0
            for mapping in mappings:
                root = mapping["local_dir"]
                if not mapping.get("enabled") or not os.path.isdir(root):
                    continue
                allowed = _normalize_extensions(mapping.get("extensions")) - UPLOAD_IGNORED_EXTENSIONS
                for current_root, _, files in os.walk(root):
                    for name in files:
                        path = os.path.join(current_root, name)
                        if not _is_ignored_upload_path(path) and os.path.splitext(name)[1].lower() in allowed:
                            batch.append(path)
                            if len(batch) >= SCAN_BATCH_SIZE:
                                while self._queue.qsize() >= SCAN_MAX_QUEUED:
                                    time.sleep(1)
                                queued_count += self._enqueue_paths(batch, mappings)
                                batch.clear()
            if batch:
                while self._queue.qsize() >= SCAN_MAX_QUEUED:
                    time.sleep(1)
                queued_count += self._enqueue_paths(batch, mappings)
            logger.info("  ➜ [115上传监控] 已有文件扫描完成，加入上传队列 %s 个。", queued_count)

        spawn(_scan)

    def _wait_for_stability(self, path: str) -> bool:
        last_size = -1
        stable_count = 0
        for _ in range(60):
            if not os.path.isfile(path):
                return False
            try:
                size = os.path.getsize(path)
            except OSError:
                return False
            if size > 0 and size == last_size:
                stable_count += 1
                if stable_count >= 3:
                    return True
            else:
                stable_count = 0
            last_size = size
            time.sleep(1)
        return False

    def _worker_loop(self) -> None:
        while True:
            job_id = self._queue.get()
            if job_id is None:
                self._queue.task_done()
                return
            try:
                self._process_job(job_id)
            except Exception:
                logger.exception("  ➜ [115上传监控] 处理上传任务异常: %s", job_id)
            finally:
                with self._lock:
                    self._queued.discard(job_id)
                self._queue.task_done()

    def _update_job(self, job_id: str, **values) -> Optional[Dict[str, Any]]:
        with self._state_lock:
            job = p115_upload_db.get_job(job_id)
            if not isinstance(job, dict):
                return None
            job.update(values)
            job["updated_at"] = time.time()
            p115_upload_db.upsert_jobs([job])
            return dict(job)

    def _merge_resume(self, job_id: str, resume: Dict[str, Any]) -> None:
        with self._state_lock:
            job = p115_upload_db.get_job(job_id)
            if not isinstance(job, dict):
                return
            merged = dict(job.get("resume") or {})
            merged.update(resume)
            job["resume"] = merged
            job["updated_at"] = time.time()
            p115_upload_db.upsert_jobs([job])

    def _make_progress_hook(self, job_id: str, total_bytes: int):
        uploaded_bytes = 0
        last_saved_at = 0.0
        last_saved_percent = -1

        def _report(delta: int):
            nonlocal uploaded_bytes, last_saved_at, last_saved_percent
            try:
                uploaded_bytes = min(total_bytes, uploaded_bytes + max(0, int(delta or 0)))
            except (TypeError, ValueError):
                return
            percent = round((uploaded_bytes / total_bytes) * 100, 1) if total_bytes > 0 else 0
            now = time.monotonic()
            if (percent >= 100 and last_saved_percent < 100) or now - last_saved_at >= 1:
                self._update_job(
                    job_id,
                    uploaded_bytes=uploaded_bytes,
                    total_bytes=total_bytes,
                    progress=percent,
                )
                last_saved_at = now
                last_saved_percent = percent

        return _report

    def _process_job(self, job_id: str) -> None:
        with self._state_lock:
            job = (p115_upload_db.get_job(job_id) or {}).copy()
        if _is_ignored_upload_path(job.get("path") or ""):
            self._remove_job(job_id)
            return
        if not job or not os.path.isfile(job.get("path") or ""):
            self._update_job(job_id, status="failed", error="本地文件不存在")
            return
        config = get_upload_monitor_config()
        active_mapping = next(
            (
                item for item in config["mappings"]
                if item.get("enabled") and item.get("id") == job.get("mapping_id")
            ),
            None,
        )
        if not config["enabled"] or not active_mapping:
            self._update_job(job_id, status="pending", error="上传监控或目录映射已停用")
            return
        job.update({
            "target_cid": active_mapping["target_cid"],
            "target_name": active_mapping["target_name"],
            "mode": active_mapping["mode"],
        })
        self._update_job(
            job_id,
            target_cid=job["target_cid"],
            target_name=job["target_name"],
            mode=job["mode"],
        )
        path = job["path"]
        if not self._wait_for_stability(path):
            self._fail_and_retry(job_id, "文件在稳定性检测期间消失或持续写入")
            return
        current_fingerprint = _file_fingerprint(path)
        if not _same_fingerprint(current_fingerprint, job.get("fingerprint")):
            self._remove_job(job_id)
            self.enqueue_path(path)
            return

        self._update_job(
            job_id,
            status="uploading",
            error="",
            uploaded_bytes=0,
            total_bytes=int(job["fingerprint"]["size"]),
            progress=0,
        )
        try:
            target_cid = self._ensure_target_directory(job)
            result, resume, backend = self._upload_file_resumable(path, target_cid, job.get("resume"), job_id=job_id)
            if resume:
                self._merge_resume(job_id, resume)
            self._update_job(job_id, progress=100, uploaded_bytes=int(job["fingerprint"]["size"]), upload_backend=backend)
            if not self._upload_succeeded(result):
                raise RuntimeError(str((result or {}).get("message") or (result or {}).get("error_msg") or result))
            uploaded_file = self._cache_uploaded_file(job, result, target_cid)
            if not self._create_local_outputs(job, result, target_cid, uploaded_file=uploaded_file):
                raise RuntimeError("115 上传成功，但自动生成 STRM 或 mediainfo JSON 失败")
        except Exception as exc:
            resume = self._resume_from_exception(exc)
            if resume:
                self._merge_resume(job_id, resume)
            self._fail_and_retry(job_id, str(exc))
            return

        if job.get("mode") == "delete":
            try:
                if _same_fingerprint(_file_fingerprint(path), job.get("fingerprint")):
                    os.remove(path)
                    logger.info("  ➜ [115上传监控] 上传成功并删除本地文件: %s", path)
            except OSError as exc:
                logger.warning("  ➜ [115上传监控] 上传成功，但删除本地文件失败: %s -> %s", path, exc)

        self._complete_job(job_id, job, uploaded_file or result)
        if os.path.isfile(path):
            latest = _file_fingerprint(path)
            if not _same_fingerprint(latest, job.get("fingerprint")):
                self.enqueue_path(path)

    @staticmethod
    def _upload_result_value(result: Any, *keys: str) -> str:
        if not isinstance(result, dict):
            return ""
        data = result.get("data") if isinstance(result.get("data"), dict) else {}
        for key in keys:
            value = result.get(key)
            if value in (None, ""):
                value = data.get(key)
            if value not in (None, ""):
                return str(value)
        return ""

    @staticmethod
    def _fill_emby_screenshot_after_ingest(strm_path: str) -> None:
        if not _as_bool(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EXTRACT_THUMB), False):
            return

        import extensions
        from handler import emby

        processor = extensions.media_processor_instance
        if not processor:
            logger.warning("  ➜ [115上传监控] 核心处理器未就绪，跳过 Emby 截图补齐: %s", strm_path)
            return

        item = None
        for delay in (1, 2, 4, 8, 15, 30):
            time.sleep(delay)
            item = emby.get_emby_item_by_path(
                strm_path,
                processor.emby_url,
                processor.emby_api_key,
                processor.emby_user_id,
            )
            if item and item.get("Id"):
                break
        if not item or not item.get("Id"):
            logger.warning("  ➜ [115上传监控] STRM 入库后仍未取得 Emby Item，跳过截图: %s", strm_path)
            return

        item_id = str(item["Id"])
        details = emby.get_emby_item_details(
            item_id,
            processor.emby_url,
            processor.emby_api_key,
            processor.emby_user_id,
            fields="Path,ImageTags,SeriesId,ParentIndexNumber,IndexNumber",
        ) or {}
        if (details.get("ImageTags") or {}).get("Primary"):
            logger.debug("  ➜ [115上传监控] Emby Item 已有主图，跳过截图: %s", strm_path)
            return

        with tempfile.TemporaryDirectory(prefix="etk-upload-monitor-thumb-") as temp_dir:
            temp_path = os.path.join(temp_dir, f"{item_id}.jpg")
            if not processor._extract_thumb_via_ffmpeg(strm_path, temp_path):
                logger.warning("  ➜ [115上传监控] 从 115 直链截图失败: %s", strm_path)
                return
            try:
                latest = emby.get_emby_item_details(
                    item_id,
                    processor.emby_url,
                    processor.emby_api_key,
                    processor.emby_user_id,
                    fields="ImageTags",
                ) or {}
                if (latest.get("ImageTags") or {}).get("Primary"):
                    logger.debug("  ➜ [115上传监控] 截图期间 Emby 已补入主图，跳过上传: %s", strm_path)
                    return
                with open(temp_path, "rb") as image_file:
                    image_data = image_file.read()
                if emby.upload_item_image(
                    processor.emby_url,
                    processor.emby_api_key,
                    item_id,
                    image_data,
                    content_type="image/jpeg",
                    image_type="Primary",
                    delete_existing=False,
                ):
                    item_type = str(details.get("Type") or item.get("Type") or "").title()
                    if item_type == "Episode":
                        from database.metadata_provider_db import resolve_metadata_identity_by_path
                        from handler.media_image_cache import archive_episode_screenshot

                        identity = resolve_metadata_identity_by_path(
                            strm_path,
                            "Episode",
                            season_number=details.get("ParentIndexNumber"),
                            episode_number=details.get("IndexNumber"),
                        )
                        if not identity and details.get("SeriesId"):
                            series = emby.get_emby_item_details(
                                str(details["SeriesId"]),
                                processor.emby_url,
                                processor.emby_api_key,
                                processor.emby_user_id,
                                fields="ProviderIds",
                            ) or {}
                            series_tmdb_id = (series.get("ProviderIds") or {}).get("Tmdb")
                            if series_tmdb_id:
                                identity = {
                                    "tmdb_id": str(series_tmdb_id),
                                    "season_number": details.get("ParentIndexNumber"),
                                    "episode_number": details.get("IndexNumber"),
                                }
                        if identity:
                            archive_episode_screenshot(
                                identity.get("tmdb_id"),
                                identity.get("season_number"),
                                identity.get("episode_number"),
                                image_data,
                                "image/jpeg",
                            )
                    logger.info("  ➜ [115上传监控] 已补齐 Emby 缓存截图: %s", strm_path)
                else:
                    logger.warning("  ➜ [115上传监控] 上传 Emby 缓存截图失败: %s", strm_path)
            finally:
                try:
                    os.remove(temp_path)
                except FileNotFoundError:
                    pass

    def _resolve_local_output_directory(self, target_cid: str, fallback_name: str = "") -> str:
        local_root = str(
            config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT) or ""
        ).strip()
        if not local_root:
            raise RuntimeError("未配置本地 STRM 根目录")
        local_root = os.path.abspath(local_root)

        from handler.p115_service import P115CacheManager, P115Service

        media_root_cid = str(
            config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_115_MEDIA_ROOT_CID) or "0"
        )
        relative_path = str(P115CacheManager.get_local_path(target_cid) or "").strip()
        if not relative_path:
            client = P115Service.get_client()
            if not client:
                raise RuntimeError("115 客户端未初始化，无法解析本地 STRM 镜像路径")
            response = client.fs_files({
                "cid": target_cid,
                "limit": 1,
                "record_open_time": 0,
                "count_folders": 0,
            })
            path_nodes = response.get("path") if isinstance(response, dict) else []
            path_nodes = path_nodes if isinstance(path_nodes, list) else []
            start_index = None
            if media_root_cid != "0":
                for index, node in enumerate(path_nodes):
                    node_id = node.get("cid") or node.get("file_id") or node.get("fid") or node.get("id")
                    if str(node_id) == media_root_cid:
                        start_index = index + 1
                        break
            if start_index is None:
                start_index = 0
            names = []
            for node in path_nodes[start_index:]:
                node_id = str(node.get("cid") or node.get("file_id") or node.get("fid") or node.get("id") or "")
                name = str(
                    node.get("file_name") or node.get("fn") or node.get("name") or node.get("n") or ""
                ).strip()
                if name and node_id != "0" and name not in {".", "..", "/", "根目录"}:
                    names.append(name)
            relative_path = "/".join(names)

        parts = [part for part in relative_path.replace("\\", "/").split("/") if part not in {"", ".", ".."}]
        if (
            not parts
            and str(target_cid) != media_root_cid
            and fallback_name not in {"", "/", ".", ".."}
        ):
            parts = [fallback_name]
        output_dir = os.path.abspath(os.path.join(local_root, *parts))
        if os.path.commonpath([local_root, output_dir]) != local_root:
            raise RuntimeError("115 目标目录无法安全映射到本地 STRM 根目录")
        return output_dir

    def _cache_uploaded_file(self, job: Dict[str, Any], result: Dict[str, Any], target_cid: str) -> Dict[str, Any]:
        path = job["path"]
        file_name = os.path.basename(path)
        fid = self._upload_result_value(result, "file_id", "fid", "id")
        pick_code = self._upload_result_value(result, "pick_code", "pickcode", "pc")
        sha1 = self._upload_result_value(result, "sha1", "file_sha1", "filesha1", "fileid").upper()

        from handler.p115_service import P115CacheManager, P115Service

        if not fid or not pick_code or not sha1:
            client = P115Service.get_client()
            if client:
                for attempt in range(3):
                    response = client.fs_files({
                        "cid": target_cid,
                        "search_value": file_name,
                        "limit": 100,
                        "record_open_time": 0,
                        "count_folders": 0,
                    })
                    items = response.get("data") if isinstance(response, dict) else []
                    items = items if isinstance(items, list) else []
                    exact = next((
                        item for item in items
                        if str(item.get("fn") or item.get("file_name") or item.get("n") or item.get("name") or "") == file_name
                        and (
                            not pick_code
                            or str(item.get("pc") or item.get("pick_code") or item.get("pickcode") or "") == pick_code
                        )
                    ), None)
                    if exact:
                        fid = fid or str(exact.get("fid") or exact.get("file_id") or exact.get("id") or "")
                        pick_code = pick_code or str(exact.get("pc") or exact.get("pick_code") or exact.get("pickcode") or "")
                        sha1 = sha1 or str(exact.get("sha1") or exact.get("sha") or exact.get("file_sha1") or "").upper()
                        break
                    if attempt < 2:
                        time.sleep(1)

        if not fid or not pick_code:
            raise RuntimeError(f"115 上传成功，但无法确认远端文件身份: {file_name}")

        with open(path, "rb") as file_obj:
            preid = hashlib.sha1(file_obj.read(131072)).hexdigest().upper()

        local_path = None
        local_root = str(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT) or "").strip()
        if local_root:
            output_dir = self._resolve_local_output_directory(target_cid, job.get("target_name") or "")
            local_path = os.path.relpath(os.path.join(output_dir, file_name), os.path.abspath(local_root)).replace("\\", "/")

        P115CacheManager.save_file_cache(
            fid=fid,
            parent_id=target_cid,
            name=file_name,
            sha1=sha1 or None,
            pick_code=pick_code,
            local_path=local_path,
            size=os.path.getsize(path),
        )
        logger.debug("  ➜ [115上传监控] 已写入 115 文件缓存: %s (fid=%s, pc=%s...)", file_name, fid, pick_code[:8])
        return {
            "fid": fid,
            "parent_id": str(target_cid),
            "file_name": file_name,
            "sha1": sha1,
            "preid": preid,
            "pick_code": pick_code,
            "size": os.path.getsize(path),
        }

    def _create_local_outputs(
        self,
        job: Dict[str, Any],
        result: Dict[str, Any],
        target_cid: str,
        uploaded_file: Optional[Dict[str, Any]] = None,
    ) -> bool:
        path = job["path"]
        if os.path.splitext(path)[1].lower() not in MEDIA_OUTPUT_EXTENSIONS:
            return True
        uploaded_file = uploaded_file if isinstance(uploaded_file, dict) else {}
        pick_code = str(uploaded_file.get("pick_code") or self._upload_result_value(result, "pick_code", "pickcode"))
        if not pick_code:
            logger.error("  ➜ [115上传监控] 上传结果缺少 pick_code，无法生成 STRM: %s", path)
            return False

        output_dir = self._resolve_local_output_directory(target_cid, job.get("target_name") or "")
        from handler.p115_service import resolve_p115_sorting_target_by_local_path
        local_root = str(
            config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT) or ""
        ).strip()
        category = resolve_p115_sorting_target_by_local_path(output_dir, local_root=local_root)
        if not category:
            logger.info(
                "  ➜ [115上传监控] 目标目录未命中分类规则，跳过生成本地派生文件: %s",
                output_dir,
            )
            return True

        os.makedirs(output_dir, exist_ok=True)
        from monitor_service import _generate_local_mediainfo, enqueue_file_actively
        stem = os.path.splitext(os.path.basename(path))[0]
        mediainfo = _generate_local_mediainfo(
            path,
            cache_sha1=uploaded_file.get("sha1") or None,
            cache_file_info=uploaded_file,
        )
        if not mediainfo:
            return False

        etk_url = str(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_ETK_SERVER_URL) or "").strip().rstrip("/")
        if not etk_url.startswith(("http://", "https://")):
            logger.error("  ➜ [115上传监控] etk_server_url 无效，无法生成 STRM: %s", etk_url)
            return False
        strm_content = f"{etk_url}/api/p115/play/{pick_code}"
        rename_config = settings_db.get_setting("p115_rename_config") or {}
        if isinstance(rename_config, dict) and rename_config.get("strm_url_fmt") == "with_name":
            strm_content = f"{strm_content}/{os.path.basename(path)}"
        strm_path = os.path.join(output_dir, stem + ".strm")
        temp_path = strm_path + ".etk-tmp"
        try:
            with open(temp_path, "w", encoding="utf-8") as file_obj:
                file_obj.write(strm_content)
            os.replace(temp_path, strm_path)
        except Exception as exc:
            logger.error("  ➜ [115上传监控] 写入 STRM 失败: %s -> %s", strm_path, exc)
            return False

        job["derived_paths"] = [strm_path]
        enqueue_file_actively(strm_path)
        if (
            os.path.splitext(path)[1].lower() in VIDEO_OUTPUT_EXTENSIONS
            and _as_bool(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EXTRACT_THUMB), False)
        ):
            spawn(self._fill_emby_screenshot_after_ingest, strm_path)
        logger.info("  ➜ [115上传监控] 已生成 STRM 与媒体信息: %s", strm_path)
        return True

    def _remove_local_outputs(self, mapping: Dict[str, Any], completed: Dict[str, Any], source_path: str) -> str:
        if os.path.splitext(source_path)[1].lower() not in MEDIA_OUTPUT_EXTENSIONS:
            return ""
        local_root = str(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT) or "").strip()
        if not local_root:
            return ""
        local_root = os.path.abspath(local_root)
        derived_paths = completed.get("derived_paths") if isinstance(completed.get("derived_paths"), list) else []

        if not derived_paths:
            try:
                category_root = self._resolve_local_output_directory(
                    str(mapping.get("target_cid") or completed.get("target_cid") or ""),
                    str(mapping.get("target_name") or ""),
                )
                from handler.p115_service import resolve_p115_sorting_target_by_local_path
                if not resolve_p115_sorting_target_by_local_path(category_root, local_root=local_root):
                    return ""
                relative_dir = os.path.dirname(str(completed.get("relative_path") or ""))
                output_dir = os.path.abspath(os.path.join(category_root, relative_dir))
                if os.path.commonpath([category_root, output_dir]) != os.path.abspath(category_root):
                    return ""
                stem = os.path.splitext(os.path.basename(source_path))[0]
                derived_paths = [
                    os.path.join(output_dir, stem + ".strm"),
                    os.path.join(output_dir, stem + "-poster.jpg"),
                ]
            except Exception as exc:
                logger.warning("  ➜ [115上传监控] 回算本地镜像路径失败: %s -> %s", source_path, exc)
                return ""

        strm_path = ""
        for value in derived_paths:
            candidate_value = str(value or "").strip()
            if not candidate_value:
                continue
            candidate = os.path.abspath(candidate_value)
            if not _path_in_root(candidate, local_root):
                continue
            if candidate.lower().endswith(".strm"):
                strm_path = candidate
            if os.path.isfile(candidate):
                try:
                    os.remove(candidate)
                    logger.info("  ➜ [115上传监控] 已删除本地镜像文件: %s", candidate)
                except OSError as exc:
                    logger.warning("  ➜ [115上传监控] 删除本地镜像文件失败: %s -> %s", candidate, exc)
        return strm_path

    def _remove_job(self, job_id: str) -> None:
        with self._state_lock:
            p115_upload_db.delete_jobs([job_id])

    def _complete_job(self, job_id: str, job: Dict[str, Any], result: Dict[str, Any]) -> None:
        completed_key = f"{job['mapping_id']}:{os.path.normcase(os.path.abspath(job['path']))}"
        with self._state_lock:
            completed = {
                "mapping_id": job["mapping_id"],
                "local_path": os.path.abspath(job["path"]),
                "fingerprint": job["fingerprint"],
                "target_cid": job["target_cid"],
                "relative_path": job["relative_path"],
                "pick_code": str((result or {}).get("pick_code") or (result or {}).get("pickcode") or ""),
                "derived_paths": list(job.get("derived_paths") or []),
                "completed_at": time.time(),
            }
            p115_upload_db.complete_job(job_id, completed_key, completed)
        logger.info("  ➜ [115上传监控] 上传完成: %s -> %s", job["path"], job.get("target_name") or job["target_cid"])

    def _fail_and_retry(self, job_id: str, error: str) -> None:
        with self._state_lock:
            job = p115_upload_db.get_job(job_id)
            if not isinstance(job, dict):
                return
            attempts = int(job.get("attempts") or 0) + 1
            delay = min(300, 15 * (2 ** min(attempts - 1, 4)))
            job.update(status="failed", error=error[:500], attempts=attempts, updated_at=time.time())
            p115_upload_db.upsert_jobs([job])
        logger.warning("  ➜ [115上传监控] 上传失败，%s 秒后续传重试: %s -> %s", delay, job.get("path"), error)
        spawn_later(delay, self._requeue_job, job_id)

    def _requeue_job(self, job_id: str) -> None:
        with self._lock:
            if job_id in self._queued:
                return
            self._queued.add(job_id)
            self._queue.put(job_id)

    def _ensure_target_directory(self, job: Dict[str, Any]) -> str:
        relative_dir = os.path.dirname(job.get("relative_path") or "")
        if not relative_dir or relative_dir == ".":
            return str(job["target_cid"])
        from handler.p115_service import P115Service

        client = P115Service.get_client()
        if not client:
            raise RuntimeError("115 客户端未初始化")
        current_cid = str(job["target_cid"])
        current_rel = ""
        for name in [part for part in relative_dir.replace("\\", "/").split("/") if part not in {"", ".", ".."}]:
            current_rel = f"{current_rel}/{name}".strip("/")
            cache_key = (str(job["target_cid"]), current_rel.casefold())
            with self._lock:
                cached = self._remote_dir_cache.get(cache_key)
            if cached:
                current_cid = cached
                continue
            found = self._find_child_directory(client, current_cid, name)
            if not found:
                response = client.fs_mkdir(name, current_cid)
                if response.get("state"):
                    data = response.get("data") if isinstance(response.get("data"), dict) else {}
                    found = response.get("cid") or data.get("file_id") or data.get("fid") or data.get("cid")
                if not found:
                    for _ in range(3):
                        time.sleep(1)
                        found = self._find_child_directory(client, current_cid, name)
                        if found:
                            break
            if not found:
                raise RuntimeError(f"无法创建或找到 115 目录: {name}")
            current_cid = str(found)
            with self._lock:
                self._remote_dir_cache[cache_key] = current_cid
        return current_cid

    @staticmethod
    def _find_child_directory(client, parent_cid: str, name: str) -> str:
        response = client.fs_files({"cid": parent_cid, "limit": 1000, "record_open_time": 0})
        for item in response.get("data") if isinstance(response.get("data"), list) else []:
            item_name = str(item.get("fn") or item.get("file_name") or item.get("n") or item.get("name") or "")
            item_type = item.get("fc") if item.get("fc") is not None else item.get("file_category")
            is_dir = str(item_type) == "0" or str(item.get("is_dir")).lower() in {"1", "true"}
            if is_dir and item_name == name:
                return str(item.get("fid") or item.get("file_id") or item.get("id") or item.get("cid") or "")
        return ""

    @staticmethod
    def _upload_succeeded(result: Any) -> bool:
        if not isinstance(result, dict):
            return False
        if result.get("state") is False:
            return False
        return bool(
            result.get("state")
            or result.get("success")
            or result.get("pick_code")
            or result.get("pickcode")
            or result.get("code") in {0, 200}
        )

    @staticmethod
    def _resume_from_exception(exc: Exception) -> Optional[Dict[str, Any]]:
        value = exc.args[0] if getattr(exc, "args", None) else None
        if not isinstance(value, dict) or not value.get("callback") or not value.get("upload_id"):
            return None
        return {
            key: value.get(key)
            for key in ("filename", "filesha1", "filesize", "partsize", "callback", "upload_id", "bucket")
            if value.get(key) not in (None, "")
        }

    def _upload_file_resumable(self, path: str, target_cid: str, resume: Any, job_id: str = ""):
        from handler.p115_service import P115Service, get_115_api_priority, get_115_tokens
        try:
            from p115oss import MultipartUploadAbort, oss_multipart_upload_init, oss_upload, upload_file, upload_file_init
        except ImportError as exc:
            raise RuntimeError("当前 p115client 版本不支持持久化分片续传，请按 requirements.txt 更新依赖") from exc

        access_token, _, cookie, _ = get_115_tokens()
        endpoint = "https://oss-cn-shenzhen.aliyuncs.com"
        size = os.path.getsize(path)
        filename = os.path.basename(path)
        resume = dict(resume) if isinstance(resume, dict) else {}
        progress_hook = self._make_progress_hook(job_id, size) if job_id else None

        raw_client = None
        if cookie:
            unified_client = P115Service.get_client()
            raw_client = getattr(unified_client, "raw_client", None) if unified_client else None

        priority = get_115_api_priority()
        if resume.get("backend") in {"cookie", "openapi"}:
            backends = [resume["backend"]]
        elif priority == "cookie":
            backends = ["cookie", "openapi"]
        else:
            backends = ["openapi", "cookie"]

        last_error = None
        for backend in backends:
            if backend == "openapi" and not access_token:
                continue
            if backend == "cookie" and not raw_client:
                continue
            try:
                return self._upload_file_with_backend(
                    path=path,
                    target_cid=target_cid,
                    resume=resume,
                    backend=backend,
                    endpoint=endpoint,
                    filename=filename,
                    size=size,
                    access_token=access_token,
                    raw_client=raw_client,
                    progress_hook=progress_hook,
                    job_id=job_id,
                    upload_file_init=upload_file_init,
                    oss_multipart_upload_init=oss_multipart_upload_init,
                    oss_upload=oss_upload,
                    upload_file=upload_file,
                )
            except MultipartUploadAbort as exc:
                if self._resume_from_exception(exc):
                    raise
                last_error = exc
                logger.warning("  ➜ [115上传监控] %s 直传失败，尝试备用接口: %s", backend, exc)
            except Exception as exc:
                last_error = exc
                if resume.get("upload_id"):
                    raise
                logger.warning("  ➜ [115上传监控] %s 上传初始化失败，尝试备用接口: %s", backend, exc)
        raise RuntimeError(f"没有可用的 115 上传接口: {last_error or '未配置 Token/Cookie'}")

    def _upload_file_with_backend(
        self,
        *,
        path: str,
        target_cid: str,
        resume: Dict[str, Any],
        backend: str,
        endpoint: str,
        filename: str,
        size: int,
        access_token: str,
        raw_client,
        progress_hook,
        job_id: str,
        upload_file_init,
        oss_multipart_upload_init,
        oss_upload,
        upload_file,
    ):
        if job_id:
            self._update_job(job_id, upload_backend=backend)
        api_kwargs = {}
        if backend == "openapi":
            api_kwargs["headers"] = {"authorization": f"Bearer {access_token}"}
        else:
            api_kwargs["user_id"] = str(raw_client.user_id)
            api_kwargs["user_key"] = str(raw_client.user_key)

        if not resume.get("callback") or not resume.get("upload_id"):
            init_result = upload_file_init(
                path,
                pid=target_cid,
                filename=filename,
                filesize=size,
                **api_kwargs,
            )
            if not isinstance(init_result, dict) or not init_result.get("state"):
                raise RuntimeError(f"115 上传初始化失败: {init_result}")
            if init_result.get("reuse"):
                return init_result, None, backend
            data = init_result.get("data") if isinstance(init_result.get("data"), dict) else {}
            callback = data.get("callback")
            object_key = data.get("object")
            bucket = data.get("bucket")
            if not callback or not object_key or not bucket:
                raise RuntimeError(f"115 上传初始化缺少 OSS 参数: {init_result}")
            if size <= DIRECT_UPLOAD_MAX_SIZE:
                with open(path, "rb") as file_obj:
                    result = oss_upload(
                        object_key,
                        file_obj,
                        callback=callback,
                        bucket=bucket,
                        endpoint=endpoint,
                        reporthook=progress_hook,
                        **({"headers": api_kwargs["headers"]} if "headers" in api_kwargs else {}),
                    )
                return result, None, backend
            upload_id = oss_multipart_upload_init(
                object_key,
                None,
                bucket=bucket,
                endpoint=endpoint,
                **({"headers": api_kwargs["headers"]} if "headers" in api_kwargs else {}),
            )
            resume = {
                "filename": filename,
                "filesha1": data.get("filesha1") or data.get("fileid") or "",
                "filesize": size,
                "partsize": DEFAULT_PART_SIZE,
                "callback": callback,
                "upload_id": upload_id,
                "bucket": bucket,
                "backend": backend,
            }
            if job_id:
                self._merge_resume(job_id, resume)

        try:
            result = upload_file(
                path,
                pid=target_cid,
                filename=resume.get("filename") or filename,
                filesha1=resume.get("filesha1") or "",
                filesize=int(resume.get("filesize") or size),
                partsize=int(resume.get("partsize") or DEFAULT_PART_SIZE),
                callback=resume["callback"],
                upload_id=resume["upload_id"],
                bucket=resume.get("bucket") or "fhnfile",
                endpoint=endpoint,
                reporthook=progress_hook,
                **api_kwargs,
            )
            return result, resume, backend
        except Exception:
            raise


def delete_local_sources_by_pickcodes(pickcodes: Iterable[str]) -> Dict[str, int]:
    """Delete unchanged local sources tracked by active bidirectional mappings."""
    values = [pickcodes] if isinstance(pickcodes, str) else (pickcodes or [])
    normalized_pickcodes = {
        str(value or "").strip()
        for value in values
        if str(value or "").strip()
    }
    result = {"matched": 0, "deleted": 0, "missing": 0, "changed": 0, "failed": 0}
    if not normalized_pickcodes:
        return result

    config = get_upload_monitor_config()
    active_mappings = {
        item["id"]: item
        for item in config.get("mappings", [])
        if config.get("enabled") and item.get("enabled") and item.get("mode") == "keep"
    }
    if not active_mappings:
        return result

    runtime = UploadMonitorRuntime.instance()
    removed_paths = []
    removed_completed_keys = []
    with runtime._state_lock:
        records = p115_upload_db.find_completed_by_pickcodes(normalized_pickcodes)
        for record in records:
            completed_key = str(record.get("record_key") or "")
            completed = record.get("payload_json") or {}
            if not isinstance(completed, dict):
                continue
            mapping_id = str(record.get("mapping_id") or completed.get("mapping_id") or "")
            mapping = active_mappings.get(mapping_id)
            if not mapping:
                continue

            result["matched"] += 1
            local_path = str(record.get("local_path") or completed.get("local_path") or "").strip()
            if not local_path:
                local_path = os.path.join(mapping["local_dir"], os.path.normpath(str(completed.get("relative_path") or "")))
            local_path = os.path.abspath(local_path)
            if not _path_in_root(local_path, mapping["local_dir"]):
                result["failed"] += 1
                logger.warning("  ➜ [115上传监控] 双向删除拒绝越界路径: %s", local_path)
                continue

            if os.path.exists(local_path):
                try:
                    current_fingerprint = _file_fingerprint(local_path)
                except OSError as exc:
                    result["failed"] += 1
                    logger.warning("  ➜ [115上传监控] 双向删除读取源文件失败: %s -> %s", local_path, exc)
                    continue
                if not _same_fingerprint(completed.get("fingerprint"), current_fingerprint):
                    result["changed"] += 1
                    logger.warning("  ➜ [115上传监控] 源文件已变更，跳过 Emby 双向删除: %s", local_path)
                    continue
                try:
                    runtime.suppress_deleted_path(local_path)
                    os.remove(local_path)
                    result["deleted"] += 1
                    logger.info("  ➜ [115上传监控] Emby 联动删除本地源文件: %s", local_path)
                except OSError as exc:
                    runtime.unsuppress_deleted_path(local_path)
                    result["failed"] += 1
                    logger.warning("  ➜ [115上传监控] Emby 联动删除本地源文件失败: %s -> %s", local_path, exc)
                    continue
            else:
                result["missing"] += 1

            removed_completed_keys.append(completed_key)
            removed_paths.append(local_path)
        p115_upload_db.delete_completed_and_jobs(removed_completed_keys, removed_paths)

    if removed_paths:
        with runtime._lock:
            for local_path in removed_paths:
                timer = runtime._timers.pop(os.path.normpath(local_path), None)
                if timer:
                    timer.cancel()
    return result


def schedule_upload_monitor(observer, extensions: Iterable[str]) -> List[str]:
    config = get_upload_monitor_config()
    mappings = [item for item in config["mappings"] if item.get("enabled")]
    if not config["enabled"] or not mappings:
        return []
    handler = UploadMonitorEventHandler(mappings)
    started = []
    for root in sorted({item["local_dir"] for item in mappings}):
        if not os.path.isdir(root):
            logger.warning("  ➜ [115上传监控] 本地目录不存在，已跳过: %s", root)
            continue
        observer.schedule(handler, root, recursive=True)
        started.append(root)
    if started:
        runtime = UploadMonitorRuntime.instance()
        runtime.start()
        runtime.scan_existing(mappings)
    return started
