# monitor_service.py

import os
import time
import json
import logging
import shutil
import subprocess
import threading
from typing import List, Optional, Any, Set, Dict
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from etk.compatibility.background import spawn, spawn_later

import constants
import config_manager
import handler.emby as emby

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from core_processor import MediaProcessor

logger = logging.getLogger(__name__)

# --- 全局队列和锁 ---
FILE_EVENT_QUEUE = set() 
QUEUE_LOCK = threading.Lock()
DEBOUNCE_TIMER = None
DEBOUNCE_DELAY = 3 # 防抖延迟秒数
EMBY_BIND_QUEUE = set()
EMBY_BIND_MEDIAINFO = {}
EMBY_BIND_LOCK = threading.Lock()
EMBY_BIND_TIMER = None
EMBY_BIND_DEBOUNCE_SECONDS = 3
EMBY_BIND_RETRY_COUNTS = {}
EMBY_BIND_MAX_RETRIES = 2
EMBY_BIND_RETRY_DELAY_SECONDS = 10
EMBY_BIND_PLUGIN_WAIT_SECONDS = 8
EMBY_BIND_PENDING_PATHS = set()
EMBY_BIND_PLUGIN_COMPLETED = set()
EMBY_BIND_CONDITION = threading.Condition(EMBY_BIND_LOCK)
AUDIO_EXTENSIONS = {'.flac', '.m4a', '.mp3', '.aac', '.wav', '.ape', '.ogg', '.opus'}
MEDIA_PREP_LOCK = threading.Lock()
MEDIA_PREP_INFLIGHT = set()
MEDIA_PREP_RECENT = {}
MEDIA_PREP_DEDUPE_SECONDS = 30

# --- 全局队列抑制标志 ---
IS_PROCESSING_PAUSED = False

class MediaFileHandler(FileSystemEventHandler):
    """
    文件系统事件处理器 (纯净版：仅监控媒体文件的新增和移动)
    """
    def __init__(self, extensions: List[str], exclude_dirs: List[str] = None):
        self.exclude_dirs = exclude_dirs or []
        self.extensions = []
        for ext in extensions:
            if not ext: continue
            clean_ext = ext.strip().lower().replace('*', '')
            if clean_ext:
                if not clean_ext.startswith('.'):
                    clean_ext = '.' + clean_ext
                self.extensions.append(clean_ext)
        
        logger.trace(f"  [实时监控] 已加载监控后缀: {self.extensions}")

    def _is_valid_media_file(self, file_path: str) -> bool:
        if os.path.exists(file_path) and os.path.isdir(file_path): 
            return False
        
        _, ext = os.path.splitext(file_path)
        if ext.lower() not in self.extensions: 
            return False
        
        filename = os.path.basename(file_path)
        if filename.startswith('.'): return False
        if filename.endswith(('.part', '.!qB', '.crdownload', '.tmp', '.aria2')): return False

        return True

    def on_created(self, event):
        if not event.is_directory and self._is_valid_media_file(event.src_path):
            self._enqueue_file(event.src_path)

    def on_modified(self, event):
        return

    def on_moved(self, event):
        if not event.is_directory and self._is_valid_media_file(event.dest_path):
            self._enqueue_file(event.dest_path)

    def _enqueue_file(self, file_path: str):
        """新增/移动文件入队"""
        enqueue_file_actively(file_path)

def _is_path_excluded(file_path: str, exclude_paths: List[str]) -> bool:
    if not exclude_paths:
        return False
    norm_file = os.path.normpath(file_path).lower()
    for exc in exclude_paths:
        norm_exc = os.path.normpath(exc).lower()
        if norm_file == norm_exc or norm_file.startswith(norm_exc + os.sep):
            return True
    return False

def _is_etk_standard_strm(file_path: str) -> bool:
    if not str(file_path or '').lower().endswith('.strm'):
        return True
    try:
        if not os.path.exists(file_path) or os.path.getsize(file_path) <= 0:
            return False
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read(2048).strip()
        return '/api/p115/play/' in content or '/api/p115/virtual-play/' in content
    except Exception as e:
        logger.warning(f"  ➜ [实时监控] 读取 STRM 失败，已跳过：{os.path.basename(file_path)}，原因：{e}")
        return False

def _filter_etk_standard_files(file_paths: List[str]) -> List[str]:
    valid = []
    for fp in file_paths or []:
        if _is_etk_standard_strm(fp):
            valid.append(fp)
        else:
            logger.warning(f"  ➜ [实时监控] 非 ETK 标准 STRM，已跳过：{os.path.basename(fp)}")
    return valid


def _ensure_metadata_snapshot(file_path: str, tmdb_id: str, media_type: str, title: str, processor) -> bool:
    from database.metadata_provider_db import has_initial_tmdb_metadata, persist_initial_tmdb_metadata
    from handler.p115_service import P115CacheManager
    import handler.tmdb as tmdb

    _pick_code, sha1 = processor._extract_115_fingerprints(file_path)
    season_number, episode_number = processor._extract_season_episode_from_path(file_path)

    def _patch_identity(original_language=None):
        if not sha1:
            return
        P115CacheManager.patch_raw_ffprobe_etk_context(
            sha1,
            tmdb_id=tmdb_id,
            media_type=media_type,
            original_language=original_language,
            season_number=season_number,
            episode_number=episode_number,
        )

    if has_initial_tmdb_metadata(tmdb_id, media_type):
        _patch_identity()
        return True
    api_key = getattr(processor, 'tmdb_api_key', None)
    if not api_key:
        logger.warning("  ➜ [STRM入库] 缺少 TMDb API Key，无法准备首次刮削元数据: %s", title)
        return False
    if media_type == 'tv':
        details = tmdb.get_tv_details(
            int(tmdb_id),
            api_key,
            append_to_response="keywords,content_ratings,networks,credits,alternative_titles,external_ids,images",
        )
    else:
        details = tmdb.get_movie_details(
            int(tmdb_id),
            api_key,
            append_to_response="keywords,release_dates,credits,alternative_titles,external_ids,images",
        )
    if not details:
        return False
    _patch_identity(details.get('original_language'))
    ai_translator = getattr(processor, 'ai_translator', None)
    if ai_translator:
        from tasks import helpers
        translation_config = dict(config_manager.APP_CONFIG)
        translation_config[constants.CONFIG_OPTION_AI_TRANSLATE_ACTOR_ROLE] = False
        helpers.translate_tmdb_metadata_recursively(
            item_type='Series' if media_type == 'tv' else 'Movie',
            tmdb_data=details,
            ai_translator=ai_translator,
            item_name=title,
            tmdb_api_key=api_key,
            config=translation_config,
        )
    snapshot_title = details.get('name') if media_type == 'tv' else details.get('title')
    return persist_initial_tmdb_metadata(
        details,
        media_type,
        title=snapshot_title or title,
        image_language_preference=processor.config.get(
            constants.CONFIG_OPTION_TMDB_IMAGE_LANGUAGE_PREFERENCE, 'zh'
        ),
    )


def _identify_physical_media(file_path: str, processor):
    try:
        filename = os.path.basename(file_path)
        media_dir = os.path.dirname(file_path)
        folder_name = os.path.basename(media_dir)
        parent_name = os.path.basename(os.path.dirname(media_dir))
        season_num, episode_num = processor._extract_season_episode_from_path(file_path)
        is_season_dir = processor._extract_season_from_path_or_text(folder_name) is not None
        main_dir_name = parent_name if is_season_dir else folder_name
        forced_type = 'tv' if is_season_dir or (season_num is not None and episode_num is not None) else None

        from handler.p115_service import _identify_media_enhanced
        _pick_code, sha1 = processor._extract_115_fingerprints(file_path, allow_api_fallback=False)

        tmdb_id, media_type, title = _identify_media_enhanced(
            filename,
            main_dir_name=main_dir_name,
            has_season_subdirs=is_season_dir,
            forced_media_type=forced_type,
            ai_translator=processor.ai_translator,
            use_ai=bool(processor.ai_translator and processor.config.get(constants.CONFIG_OPTION_AI_RECOGNITION, False)),
            is_folder=False,
            sha1=sha1,
        )
        tmdb_id = str(tmdb_id or '').strip()
        if not tmdb_id.isdigit() or int(tmdb_id) <= 0 or media_type not in {'movie', 'tv'}:
            return None
        logger.debug(
            "  ➜ [入库预备] 增强识别命中《%s》，TMDb: %s，类型: %s。",
            title or filename, tmdb_id, media_type,
        )
        return tmdb_id, media_type, title or os.path.splitext(filename)[0]
    except Exception as e:
        logger.warning("  ➜ [入库预备] 增强识别失败: %s -> %s", file_path, e)
        return None


def _generate_local_mediainfo(
    file_path: str,
    *,
    cache_sha1: Optional[str] = None,
    cache_file_info: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    if not shutil.which('ffprobe'):
        logger.error("  ➜ [入库预备] 容器内未找到 ffprobe，无法生成媒体信息。")
        return False

    try:
        command = [
            'ffprobe', '-v', 'error', '-show_format', '-show_streams', '-show_chapters',
            '-print_format', 'json', file_path,
        ]
        result = subprocess.run(command, capture_output=True, text=True, timeout=300, check=False)
        if result.returncode != 0:
            logger.warning(
                "  ➜ [入库预备] ffprobe 失败: %s -> %s",
                os.path.basename(file_path), (result.stderr or '').strip()[:300],
            )
            return False

        raw_probe = json.loads(result.stdout or '{}')
        from handler.p115_media_analyzer import P115MediaAnalyzerMixin
        formatted = P115MediaAnalyzerMixin()._build_emby_mediainfo_from_ffprobe(
            raw_probe,
            {'file_name': os.path.basename(file_path), 'size': os.path.getsize(file_path)},
        )
        if not formatted:
            logger.warning("  ➜ [入库预备] ffprobe 未解析出有效媒体流: %s", file_path)
            return False

        if cache_sha1:
            from handler.p115_service import P115CacheManager
            file_info = dict(cache_file_info or {})
            if not P115CacheManager.save_mediainfo_cache(
                str(cache_sha1).upper(),
                formatted,
                raw_ffprobe_json=raw_probe,
                file_info=file_info,
                fid=file_info.get('fid') or file_info.get('file_id') or file_info.get('id'),
                pick_code=file_info.get('pick_code') or file_info.get('pc') or file_info.get('pickcode'),
                file_name=file_info.get('file_name') or file_info.get('fn') or os.path.basename(file_path),
            ):
                logger.warning("  ➜ [入库预备] 媒体信息写入 p115_mediainfo_cache 失败: %s", file_path)
                return False
        logger.debug("  ➜ [入库预备] ffprobe 已生成内存媒体信息: %s", file_path)
        return formatted
    except subprocess.TimeoutExpired:
        logger.warning("  ➜ [入库预备] ffprobe 超时: %s", file_path)
        return False
    except Exception as e:
        logger.warning("  ➜ [入库预备] 生成媒体信息失败: %s -> %s", file_path, e)
        return False


def prepare_physical_files_for_binding(processor, file_paths: List[str]) -> List[str]:
    etk_url = str(processor.config.get(constants.CONFIG_OPTION_ETK_SERVER_URL) or '').strip()
    if not emby.configure_etk_plugin_origin(processor.emby_url, processor.emby_api_key, etk_url):
        logger.error("  ➜ [入库准备] 无法把 ETK 服务地址注册到 Emby 插件，已停止实体媒体扫描。")
        return []
    ready_paths = []
    for file_path in file_paths or []:
        identity = _identify_physical_media(file_path, processor)
        if not identity:
            logger.warning("  ➜ [入库预备] 无法识别媒体，已跳过 Emby 扫描: %s", file_path)
            continue
        tmdb_id, media_type, title = identity
        if not _ensure_metadata_snapshot(file_path, tmdb_id, media_type, title, processor):
            continue
        season_number, episode_number = processor._extract_season_episode_from_path(file_path)
        from database.metadata_provider_db import register_metadata_asset_path
        if not register_metadata_asset_path(
            tmdb_id,
            media_type,
            file_path,
            season_number=season_number,
            episode_number=episode_number,
        ):
            continue
        mediainfo = _generate_local_mediainfo(file_path)
        if not mediainfo:
            continue
        ready_paths.append(file_path)
        enqueue_emby_binding(file_path, mediainfo=mediainfo)

    return ready_paths


def _prepare_strm_for_binding(processor, file_path: str):
    normalized_path = os.path.normpath(file_path)
    prepared = False
    try:
        identity = _identify_physical_media(file_path, processor)
        if not identity:
            logger.warning("  ➜ [STRM入库] 无法识别媒体，已跳过 Emby 扫描: %s", file_path)
            return
        tmdb_id, media_type, title = identity
        if not _ensure_metadata_snapshot(file_path, tmdb_id, media_type, title, processor):
            logger.warning("  ➜ [STRM入库] 首次刮削元数据未就绪，已跳过 Emby 扫描: %s", file_path)
            return
        enqueue_emby_binding(file_path)
        prepared = True
    finally:
        with MEDIA_PREP_LOCK:
            MEDIA_PREP_INFLIGHT.discard(normalized_path)
            if prepared:
                MEDIA_PREP_RECENT[normalized_path] = time.time()
            else:
                MEDIA_PREP_RECENT.pop(normalized_path, None)


def _enqueue_strm_preparation(processor, file_path: str) -> bool:
    normalized_path = os.path.normpath(file_path)
    now = time.time()
    with MEDIA_PREP_LOCK:
        if normalized_path in MEDIA_PREP_INFLIGHT:
            return False
        if now - MEDIA_PREP_RECENT.get(normalized_path, 0) <= MEDIA_PREP_DEDUPE_SECONDS:
            return False
        MEDIA_PREP_INFLIGHT.add(normalized_path)

    spawn(_prepare_strm_for_binding, processor, file_path)
    return True


def enqueue_emby_binding(file_path: str, mediainfo: Optional[Dict[str, Any]] = None):
    """Batch media paths for active Emby Item ID discovery."""
    global EMBY_BIND_TIMER
    if not file_path or not os.path.isfile(file_path):
        return

    with EMBY_BIND_CONDITION:
        normalized_path = os.path.normpath(file_path)
        EMBY_BIND_QUEUE.add(normalized_path)
        EMBY_BIND_PENDING_PATHS.add(normalized_path)
        if mediainfo:
            EMBY_BIND_MEDIAINFO[normalized_path] = mediainfo
        if EMBY_BIND_TIMER:
            EMBY_BIND_TIMER.cancel()
        EMBY_BIND_TIMER = spawn_later(EMBY_BIND_DEBOUNCE_SECONDS, _process_emby_binding_queue)


def accept_plugin_emby_binding(item: Dict[str, Any], sha1: str = '', expected_pick_code: str = '') -> Dict[str, Any]:
    """Accept bridge binding callbacks for both ETK imports and Emby rescans."""
    import extensions

    processor = extensions.media_processor_instance
    item = item if isinstance(item, dict) else {}
    item_id = str(item.get('Id') or '').strip()
    if not processor or not item_id or item.get('Type') not in {'Movie', 'Episode'}:
        return {'ok': False, 'skipped': True, 'reason': 'invalid_item'}

    with EMBY_BIND_CONDITION:
        pending_paths = list(EMBY_BIND_PENDING_PATHS)

    item_path = emby._normalize_emby_media_path(item.get('Path'))
    matched_path = next(
        (
            path for path in pending_paths
            if emby._normalize_emby_media_path(path).casefold() == item_path.casefold()
        ),
        None,
    )
    if not matched_path:
        wanted_sha1 = str(sha1 or '').strip().upper()
        wanted_pick_code = str(expected_pick_code or '').strip().lower()
        for path in pending_paths:
            if not str(path).lower().endswith('.strm'):
                continue
            pick_code, path_sha1 = processor._extract_115_fingerprints(path)
            if wanted_pick_code and str(pick_code or '').strip().lower() == wanted_pick_code:
                matched_path = path
                break
            if wanted_sha1 and str(path_sha1 or '').strip().upper() == wanted_sha1:
                matched_path = path
                break
    if not matched_path:
        from database import media_db

        rebound = media_db.rebind_emby_item_by_fingerprint(
            item_id=item_id,
            item_type=item.get('Type'),
            item_path=item.get('Path'),
            sha1=sha1,
            pick_code=expected_pick_code,
            tmdb_id=((item.get('ProviderIds') or {}).get('Tmdb')),
            series_id=item.get('SeriesId'),
            season_id=item.get('SeasonId'),
        )
        if rebound.get('updated') or rebound.get('parent_updates'):
            logger.info(
                '  ➜ [自动重绑] %s 已绑定新的 Emby ItemID: %s',
                rebound.get('title') or item.get('Name') or item_id,
                item_id,
            )
            return {'ok': True, 'rebound': True, **rebound}
        reason = str(rebound.get('reason') or '').strip()
        if not reason:
            return {'ok': True, 'skipped': True, 'reason': 'already_bound', **rebound}
        retryable = reason in {'asset_not_found', 'database_error'}
        return {
            'ok': False,
            'skipped': False,
            'retryable': retryable,
            **rebound,
        }

    normalized = emby._normalize_emby_media_path(matched_path)
    with EMBY_BIND_CONDITION:
        if normalized in EMBY_BIND_PLUGIN_COMPLETED:
            return {'ok': True, 'skipped': True, 'reason': 'already_completed'}
        EMBY_BIND_PLUGIN_COMPLETED.add(normalized)
        EMBY_BIND_CONDITION.notify_all()

    try:
        from routes.webhook import dispatch_active_emby_items
        dispatch_active_emby_items([item])
    except Exception:
        with EMBY_BIND_CONDITION:
            EMBY_BIND_PLUGIN_COMPLETED.discard(normalized)
            EMBY_BIND_CONDITION.notify_all()
        raise

    with EMBY_BIND_CONDITION:
        EMBY_BIND_QUEUE.discard(matched_path)
        EMBY_BIND_PENDING_PATHS.discard(matched_path)
        EMBY_BIND_RETRY_COUNTS.pop(os.path.normpath(matched_path), None)
        EMBY_BIND_MEDIAINFO.pop(os.path.normpath(matched_path), None)
    logger.info(
        "  ➜ [主动入库] 插件已上报 Emby Item ID，跳过路径轮询: %s -> %s",
        os.path.basename(matched_path),
        item_id,
    )
    return {'ok': True, 'accepted': True, 'item_id': item_id}


def _process_emby_binding_queue():
    global EMBY_BIND_TIMER
    with EMBY_BIND_LOCK:
        file_paths = list(EMBY_BIND_QUEUE)
        EMBY_BIND_QUEUE.clear()
        EMBY_BIND_TIMER = None

    import extensions
    processor = extensions.media_processor_instance
    if not processor or not file_paths:
        return

    ready_paths = [path for path in file_paths if os.path.exists(path)]
    if not ready_paths:
        return

    base_url = processor.emby_url
    api_key = processor.emby_api_key
    user_id = processor.emby_user_id
    if not base_url or not api_key:
        return

    logger.info("  ➜ [主动入库] 通知 Emby 扫描 %s 个媒体文件，随后主动获取 Item ID。", len(ready_paths))
    emby.notify_emby_file_changes(ready_paths, base_url, api_key)
    plugin_wait_paths = [path for path in ready_paths if str(path).lower().endswith('.strm')]
    if plugin_wait_paths:
        deadline = time.monotonic() + EMBY_BIND_PLUGIN_WAIT_SECONDS
        with EMBY_BIND_CONDITION:
            while True:
                waiting = [
                    path for path in plugin_wait_paths
                    if emby._normalize_emby_media_path(path) not in EMBY_BIND_PLUGIN_COMPLETED
                ]
                remaining = deadline - time.monotonic()
                if not waiting or remaining <= 0:
                    break
                EMBY_BIND_CONDITION.wait(timeout=remaining)
            plugin_completed = set(EMBY_BIND_PLUGIN_COMPLETED)
    else:
        plugin_completed = set()

    poll_paths = [
        path for path in ready_paths
        if emby._normalize_emby_media_path(path) not in plugin_completed
    ]
    fallback_strm_paths = [path for path in poll_paths if str(path).lower().endswith('.strm')]
    if fallback_strm_paths:
        logger.info(
            "  ➜ [主动入库] 插件优先窗口结束，%s 个 STRM 改用路径轮询兜底。",
            len(fallback_strm_paths),
        )
    found = emby.wait_for_emby_items_by_path(
        poll_paths,
        base_url,
        api_key,
        user_id,
        retry_delays=(0, 0.25, 0.5, 1, 2, 4, 8),
    )

    injected = {}
    for path, item in found.items():
        item_id = item.get("Id")
        normalized_path = os.path.normpath(path)
        payload = EMBY_BIND_MEDIAINFO.get(normalized_path)
        if payload:
            result = emby.apply_etk_mediainfo(
                item_id,
                payload,
                base_url,
                api_key,
                drop_conflicting_external_streams=True,
            )
        else:
            _pick_code, sha1 = processor._extract_115_fingerprints(path)
            result = emby.apply_cached_etk_mediainfo(
                item_id,
                sha1,
                base_url,
                api_key,
                drop_conflicting_external_streams=True,
            )
        if result is not None:
            injected[path] = item

    if injected:
        from routes.webhook import dispatch_active_emby_items
        dispatch_active_emby_items(list(injected.values()))
        with EMBY_BIND_LOCK:
            for path in ready_paths:
                normalized = emby._normalize_emby_media_path(path)
                if normalized in injected:
                    EMBY_BIND_PENDING_PATHS.discard(path)
                    EMBY_BIND_RETRY_COUNTS.pop(os.path.normpath(path), None)
                    EMBY_BIND_MEDIAINFO.pop(os.path.normpath(path), None)

    with EMBY_BIND_CONDITION:
        for path in ready_paths:
            normalized = emby._normalize_emby_media_path(path)
            if normalized in plugin_completed:
                EMBY_BIND_PLUGIN_COMPLETED.discard(normalized)

    missing = [
        path for path in ready_paths
        if emby._normalize_emby_media_path(path) not in injected
        and emby._normalize_emby_media_path(path) not in plugin_completed
    ]
    if missing:
        injection_failed = [path for path in missing if path in found]
        item_not_found = [path for path in missing if path not in found]
        if injection_failed:
            logger.warning(
                "  ➜ [主动入库] %s 个媒体文件已取得 Emby Item ID，但媒体信息注入失败。",
                len(injection_failed),
            )
        if item_not_found:
            logger.warning(
                "  ➜ [主动入库] %s 个媒体文件暂未取得 Emby Item ID。",
                len(item_not_found),
            )
        retrying = []
        exhausted = []
        with EMBY_BIND_LOCK:
            for path in missing:
                normalized_path = os.path.normpath(path)
                attempts = EMBY_BIND_RETRY_COUNTS.get(normalized_path, 0) + 1
                if attempts <= EMBY_BIND_MAX_RETRIES:
                    EMBY_BIND_RETRY_COUNTS[normalized_path] = attempts
                    EMBY_BIND_QUEUE.add(normalized_path)
                    retrying.append(path)
                else:
                    EMBY_BIND_PENDING_PATHS.discard(normalized_path)
                    EMBY_BIND_RETRY_COUNTS.pop(normalized_path, None)
                    EMBY_BIND_MEDIAINFO.pop(normalized_path, None)
                    exhausted.append(path)

            if retrying and EMBY_BIND_TIMER is None:
                EMBY_BIND_TIMER = spawn_later(
                    EMBY_BIND_RETRY_DELAY_SECONDS,
                    _process_emby_binding_queue,
                )

        if retrying:
            logger.warning(
                "  ➜ [主动入库] %s 个媒体文件将在 %s 秒后重扫重试。",
                len(retrying), EMBY_BIND_RETRY_DELAY_SECONDS,
            )
        if exhausted:
            logger.error(
                "  ➜ [主动入库] %s 个媒体文件连续 %s 轮未完成入库绑定，已停止自动重试。",
                len(exhausted), EMBY_BIND_MAX_RETRIES + 1,
            )

def enqueue_file_actively(file_path: str):
    """主动将文件推入监控队列"""
    global DEBOUNCE_TIMER
    if not _is_etk_standard_strm(file_path):
        logger.warning(f"  ➜ [实时监控] 非 ETK 标准 STRM，已跳过：{os.path.basename(file_path)}")
        return

    exclude_paths = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_MONITOR_EXCLUDE_DIRS, [])
    if str(file_path or "").lower().endswith(".strm") and not _is_path_excluded(file_path, exclude_paths):
        processor = MonitorService.processor_instance
        if not processor:
            logger.warning("  ➜ [STRM入库] 核心处理器未就绪，已跳过: %s", os.path.basename(file_path))
            return
        if _enqueue_strm_preparation(processor, file_path):
            logger.debug("  ➜ [STRM入库] 加入识别/数据库元数据预备队列: %s", os.path.basename(file_path))
        return

    with QUEUE_LOCK:
        if file_path not in FILE_EVENT_QUEUE:
            logger.info(f"  ➜ [主动推送] 文件加入监控队列: {os.path.basename(file_path)}")
        
        FILE_EVENT_QUEUE.add(file_path)
        
        if DEBOUNCE_TIMER: DEBOUNCE_TIMER.kill()
        DEBOUNCE_TIMER = spawn_later(DEBOUNCE_DELAY, process_batch_queue)

def process_batch_queue():
    """处理新增/修改队列"""
    if not config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_MONITOR_ENABLED, False):
        with QUEUE_LOCK:
            FILE_EVENT_QUEUE.clear()
        return
        
    global DEBOUNCE_TIMER, IS_PROCESSING_PAUSED
    
    if IS_PROCESSING_PAUSED:
        with QUEUE_LOCK:
            if DEBOUNCE_TIMER: DEBOUNCE_TIMER.kill()
            DEBOUNCE_TIMER = spawn_later(5.0, process_batch_queue)
        return

    with QUEUE_LOCK:
        files_to_process = list(FILE_EVENT_QUEUE)
        FILE_EVENT_QUEUE.clear()
        DEBOUNCE_TIMER = None
    
    if not files_to_process: return
    
    processor = MonitorService.processor_instance
    if not processor: return

    exclude_paths = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_MONITOR_EXCLUDE_DIRS, [])

    files_to_scrape = []
    files_to_refresh_only = []

    for file_path in files_to_process:
        if os.path.splitext(file_path)[1].lower() in AUDIO_EXTENSIONS:
            files_to_refresh_only.append(file_path)
        elif _is_path_excluded(file_path, exclude_paths):
            files_to_refresh_only.append(file_path)
        else:
            files_to_scrape.append(file_path)

    if files_to_scrape:
        logger.info(f"  ➜ [实时监控] 准备识别并入库 {len(files_to_scrape)} 个实体媒体文件。")
        spawn(_handle_batch_file_task, processor, files_to_scrape)

    if files_to_refresh_only:
        logger.info(f"  ➜ [实时监控] 发现 {len(files_to_refresh_only)} 个文件命中排除路径，将跳过刮削直接刷新 Emby。")
        spawn(_handle_batch_refresh_only_task, files_to_refresh_only)

def _handle_batch_file_task(processor, file_paths: List[str]):
    valid_files = _wait_for_files_stability(file_paths)
    valid_files = _filter_etk_standard_files(valid_files)
    if not valid_files: return
    prepare_physical_files_for_binding(processor, valid_files)

def _handle_batch_refresh_only_task(file_paths: List[str]):
    valid_files = _wait_for_files_stability(file_paths)
    valid_files = _filter_etk_standard_files(valid_files)
    if not valid_files: return
    
    config = config_manager.APP_CONFIG
    base_url = config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
    api_key = config.get(constants.CONFIG_OPTION_EMBY_API_KEY)
    delay_seconds = config.get(constants.CONFIG_OPTION_MONITOR_EXCLUDE_REFRESH_DELAY, 0)

    if not base_url or not api_key:
        return

    if delay_seconds > 0:
        logger.info(f"  ➜ [实时监控-排除路径] 等待 {delay_seconds} 秒后通知 Emby 刷新...")
        time.sleep(delay_seconds)
        if not config.get(constants.CONFIG_OPTION_MONITOR_ENABLED, False):
            return

    logger.info(f"  ➜ [实时监控-排除路径] 正在向 Emby 发送 {len(valid_files)} 个文件的极速入库通知。")
    emby.notify_emby_file_changes(valid_files, base_url, api_key)

def _wait_for_files_stability(file_paths: List[str]) -> List[str]:
    """
    文件稳定性检测 (仅针对媒体文件)
    """
    valid_files = []
    pending_files = {fp: {'last_size': -1, 'stable_count': 0} for fp in file_paths if os.path.exists(fp)}
            
    for _ in range(60):
        if not pending_files: break
            
        for fp in list(pending_files.keys()):
            if not os.path.exists(fp):
                del pending_files[fp]
                continue
                
            try:
                size = os.path.getsize(fp)
                if fp.lower().endswith('.strm') and size > 0:
                    valid_files.append(fp)
                    del pending_files[fp]
                    continue
                
                if size > 0 and size == pending_files[fp]['last_size']:
                    pending_files[fp]['stable_count'] += 1
                else:
                    pending_files[fp]['stable_count'] = 0
                    
                pending_files[fp]['last_size'] = size
                if pending_files[fp]['stable_count'] >= 3:
                    valid_files.append(fp)
                    del pending_files[fp]
            except Exception:
                pass
                
        if pending_files:
            time.sleep(1)
            
    return valid_files

class MonitorService:
    processor_instance = None
    active_instance = None

    def __init__(self, config: dict, processor: 'MediaProcessor'):
        self.config = config
        self.processor = processor
        MonitorService.processor_instance = processor 
        MonitorService.active_instance = self
        
        self.observer: Optional[Any] = None
        self._starter_handle = None
        self._observer_lock = threading.RLock()
        self._start_generation = 0
        self.enabled = self.config.get(constants.CONFIG_OPTION_MONITOR_ENABLED, False)
        self.paths = self.config.get(constants.CONFIG_OPTION_MONITOR_PATHS, [])
        self.extensions = self.config.get(constants.CONFIG_OPTION_MONITOR_EXTENSIONS, constants.DEFAULT_MONITOR_EXTENSIONS)
        self.exclude_dirs = self.config.get(constants.CONFIG_OPTION_MONITOR_EXCLUDE_DIRS, constants.DEFAULT_MONITOR_EXCLUDE_DIRS)

    def start(self):
        try:
            from handler.p115_upload_monitor import upload_monitor_enabled
            has_upload_monitor = upload_monitor_enabled()
        except Exception as e:
            logger.warning(f"  ➜ [115上传监控] 读取配置失败，已跳过启动: {e}")
            has_upload_monitor = False

        if (not self.enabled or not self.paths) and not has_upload_monitor:
            return

        with self._observer_lock:
            self._start_generation += 1
            generation = self._start_generation

        def _async_start():
            observer = Observer()
            started_paths = []
            if self.enabled and self.paths:
                event_handler = MediaFileHandler(self.extensions, self.exclude_dirs)
                for path in self.paths:
                    if os.path.exists(path) and os.path.isdir(path):
                        try:
                            observer.schedule(event_handler, path, recursive=True)
                            started_paths.append(path)
                        except Exception as e:
                            logger.error(f"  ➜ 无法监控目录 '{path}': {e}")

            upload_paths = []
            if has_upload_monitor:
                try:
                    from handler.p115_upload_monitor import schedule_upload_monitor
                    upload_paths = schedule_upload_monitor(observer, self.extensions)
                except Exception as e:
                    logger.error(f"  ➜ [115上传监控] 启动失败: {e}", exc_info=True)

            if started_paths or upload_paths:
                with self._observer_lock:
                    if generation != self._start_generation:
                        return
                    self.observer = observer
                    observer.start()
                logger.info(
                    f"  ➜ [实时监控] 服务已启动，媒体目录 {len(started_paths)} 个，"
                    f"115 上传目录 {len(upload_paths)} 个。"
                )

        self._starter_handle = spawn(_async_start)

    def stop(self, timeout: float = 5.0):
        with self._observer_lock:
            self._start_generation += 1
            observer = self.observer
            self.observer = None
            starter = self._starter_handle
            self._starter_handle = None
        stopped = True
        if starter is not None:
            starter.kill()
            starter.join(timeout=max(0.0, float(timeout)))
            stopped = starter.ready()
            if not starter.ready():
                with self._observer_lock:
                    self._starter_handle = starter
        if observer:
            try:
                observer.stop()
                if observer.is_alive():
                    observer.join(timeout=max(0.0, float(timeout)))
                stopped = stopped and not observer.is_alive()
            except RuntimeError:
                stopped = stopped and not observer.is_alive()
            if observer.is_alive():
                with self._observer_lock:
                    self.observer = observer
        try:
            from handler.p115_upload_monitor import UploadMonitorRuntime

            upload_runtime = UploadMonitorRuntime._instance
            if upload_runtime is not None:
                stopped = upload_runtime.stop(timeout=timeout) and stopped
        except ImportError:
            pass
        return stopped

    @classmethod
    def restart_active(cls):
        instance = cls.active_instance
        if not instance:
            return False
        if instance.stop() is False:
            return False
        instance.config = config_manager.APP_CONFIG
        instance.enabled = instance.config.get(constants.CONFIG_OPTION_MONITOR_ENABLED, False)
        instance.paths = instance.config.get(constants.CONFIG_OPTION_MONITOR_PATHS, [])
        instance.extensions = instance.config.get(
            constants.CONFIG_OPTION_MONITOR_EXTENSIONS,
            constants.DEFAULT_MONITOR_EXTENSIONS,
        )
        instance.exclude_dirs = instance.config.get(
            constants.CONFIG_OPTION_MONITOR_EXCLUDE_DIRS,
            constants.DEFAULT_MONITOR_EXCLUDE_DIRS,
        )
        instance.start()
        return True

def pause_queue_processing():
    global IS_PROCESSING_PAUSED
    IS_PROCESSING_PAUSED = True

def resume_queue_processing():
    global IS_PROCESSING_PAUSED, DEBOUNCE_TIMER
    IS_PROCESSING_PAUSED = False
    with QUEUE_LOCK:
        if FILE_EVENT_QUEUE:
            if DEBOUNCE_TIMER: DEBOUNCE_TIMER.kill()
            DEBOUNCE_TIMER = spawn_later(1, process_batch_queue)
