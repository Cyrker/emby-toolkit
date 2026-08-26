import logging
import re
import time
from datetime import datetime

from database import settings_db

logger = logging.getLogger(__name__)
TEMP_DIR_CONFIG_KEY = "p115_temp_dir_config"
DEFAULT_TEMP_CLEANUP_CRON = "0 * * * *"

DEFAULT_TEMP_DIR_NAME = "ETK临时目录"
TEMP_VIDEO_EXTENSIONS = {
    "mkv", "mp4", "avi", "mov", "ts", "m2ts", "wmv", "flv", "webm", "iso", "rmvb"
}


def _safe_int(value, default=0):
    try:
        if value in (None, "", [], {}):
            return default
        return int(float(value))
    except Exception:
        return default


def get_temp_dir_name():
    return DEFAULT_TEMP_DIR_NAME


def get_temp_dir_config():
    data = settings_db.get_setting(TEMP_DIR_CONFIG_KEY) or {}
    if not isinstance(data, dict):
        data = {}
    cid = str(data.get("cid") or "").strip()
    if "cleanup_cron" in data:
        cleanup_cron = str(data.get("cleanup_cron") or "").strip()
    else:
        cleanup_cron = DEFAULT_TEMP_CLEANUP_CRON if cid else ""
    return {
        "name": get_temp_dir_name(),
        "cid": cid,
        "cleanup_cron": cleanup_cron,
        "updated_at": str(data.get("updated_at") or "").strip(),
    }


def save_temp_dir_config(client, cleanup_cron=DEFAULT_TEMP_CLEANUP_CRON):
    cid = ensure_temp_dir(client, create_if_missing=True)
    config = {
        "name": get_temp_dir_name(),
        "cid": cid,
        "cleanup_cron": str(cleanup_cron or "").strip(),
        "updated_at": datetime.now().isoformat(),
    }
    settings_db.save_setting(TEMP_DIR_CONFIG_KEY, config)
    return config


def _item_id(item):
    return str(
        (item or {}).get("fid")
        or (item or {}).get("file_id")
        or (item or {}).get("id")
        or (item or {}).get("cid")
        or ""
    ).strip()


def _item_name(item):
    return str(
        (item or {}).get("name")
        or (item or {}).get("file_name")
        or (item or {}).get("fn")
        or (item or {}).get("n")
        or ""
    ).strip()


def _item_pick_code(item):
    return str((item or {}).get("pick_code") or (item or {}).get("pickcode") or (item or {}).get("pc") or "").strip()


def _item_size(item):
    try:
        return int(float((item or {}).get("size") or (item or {}).get("fs") or (item or {}).get("s") or 0))
    except Exception:
        return 0


def _item_sha1(item):
    return str((item or {}).get("sha1") or (item or {}).get("sha") or (item or {}).get("file_sha1") or "").strip().upper()


def _item_is_dir(item):
    if not isinstance(item, dict):
        return False
    value = item.get("fc")
    if value is None:
        value = item.get("file_category")
    if value is not None:
        return str(value) == "0"
    icon = str(item.get("ico") or item.get("icon") or "").lower()
    return icon in ("folder", "dir", "directory") or str(item.get("is_dir")).lower() in ("1", "true")


def _item_ts(item):
    for key in (
        "utime", "update_time", "updated_at", "upt", "uet", "uppt", "user_utime", "file_utime", "u_time",
        "mtime", "modify_time", "modified_at", "t", "te",
        "ptime", "create_time", "created_at", "user_ptime", "file_ptime", "p_time",
        "ctime", "time", "tp", "open_time",
    ):
        value = (item or {}).get(key)
        if value in (None, "", [], {}):
            continue
        try:
            if isinstance(value, (int, float)):
                ts = float(value)
                ts = ts / 1000.0 if ts > 100000000000 else ts
                if ts > 946684800:
                    return ts
                continue
            text = str(value).strip()
            if text.isdigit():
                ts = float(text)
                ts = ts / 1000.0 if ts > 100000000000 else ts
                if ts > 946684800:
                    return ts
                continue
            ts = datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
            if ts > 946684800:
                return ts
        except Exception:
            continue
    return 0.0


def _list_items(client, cid, **extra):
    resp = client.fs_files({
        "cid": str(cid),
        "limit": 1000,
        "offset": 0,
        "record_open_time": 0,
        "count_folders": 0,
        **extra,
    })
    if not isinstance(resp, dict) or not resp.get("state"):
        raise RuntimeError((resp or {}).get("error_msg") or (resp or {}).get("message") or "读取 115 临时目录失败")
    data = resp.get("data") or resp.get("items") or resp.get("list") or []
    if isinstance(data, dict):
        data = data.get("list") or data.get("items") or []
    return data if isinstance(data, list) else []


def _duplicate_name_match(actual_name, expected_name):
    actual = str(actual_name or "").strip()
    expected = str(expected_name or "").strip()
    if not expected:
        return False
    if actual == expected:
        return True
    if "." in expected:
        stem, ext = expected.rsplit(".", 1)
        return bool(re.fullmatch(re.escape(stem) + r"\(\d+\)\." + re.escape(ext), actual, flags=re.IGNORECASE))
    return bool(re.fullmatch(re.escape(expected) + r"\(\d+\)", actual, flags=re.IGNORECASE))


def find_temp_video(client, cid, *, sha1="", size=0, file_name=""):
    sha1 = str(sha1 or "").strip().upper()
    size = _safe_int(size)
    file_name = str(file_name or "").strip()
    best = {}
    best_score = -1
    for item in _list_items(client, cid, show_dir=0):
        if _item_is_dir(item):
            continue
        item_name = _item_name(item)
        item_sha1 = _item_sha1(item)
        item_size = _item_size(item)
        name_match = _duplicate_name_match(item_name, file_name)
        sha1_match = bool(sha1 and item_sha1 and item_sha1 == sha1)
        size_match = bool(size and item_size and item_size == size)
        if sha1 and item_sha1 and item_sha1 != sha1:
            continue
        if size and item_size and item_size != size and not sha1_match:
            continue
        if not (sha1_match or name_match or (size_match and file_name and item_name)):
            continue
        score = (4 if sha1_match else 0) + (2 if name_match else 0) + (1 if size_match else 0)
        if score > best_score:
            best = item
            best_score = score

    if not best:
        return {}
    fid = _item_id(best)
    pc = _item_pick_code(best)
    if fid and not pc and hasattr(client, "fs_get_info"):
        try:
            detail_resp = client.fs_get_info(fid)
            detail = detail_resp.get("data") if isinstance(detail_resp, dict) else {}
            if isinstance(detail, list):
                detail = detail[0] if detail else {}
            if isinstance(detail, dict):
                merged = dict(best)
                merged.update({k: v for k, v in detail.items() if v not in (None, "", [], {})})
                best = merged
        except Exception as e:
            logger.debug("  ➜ [115临时目录] 查询临时文件详情失败：fid=%s, err=%s", fid, e)
    pc = _item_pick_code(best)
    fid = _item_id(best)
    if not fid or not pc:
        return {}
    return {
        "fid": fid,
        "pick_code": pc,
        "name": _item_name(best) or file_name,
        "sha1": _item_sha1(best),
        "size": _item_size(best),
        "raw": best,
    }


def ensure_temp_dir(client, create_if_missing=False):
    name = get_temp_dir_name()
    if not create_if_missing:
        cid = get_temp_dir_config().get("cid")
        if cid:
            return cid
        raise RuntimeError("115 临时目录未保存，请在临时目录设置中保存配置后再使用。若目录被误删，请重新保存该配置。")

    cid = _find_temp_dir_cid(client, name)
    if cid:
        logger.debug("  ➜ [115临时目录] 已确认临时目录：%s (%s)", name, cid)
        return cid

    resp = _mkdir_temp_dir_remote(client, name)
    if not isinstance(resp, dict) or not resp.get("state"):
        raise RuntimeError((resp or {}).get("error_msg") or (resp or {}).get("message") or f"创建 115 临时目录失败: {resp}")

    cid = _wait_temp_dir_cid(client, name)
    if cid:
        logger.info("  ➜ [115临时目录] 已自动创建临时目录：%s (%s)", name, cid)
        return cid

    raise RuntimeError(f"115 临时目录创建接口返回成功但远端未刷出目录：{name}, resp={resp}")


def _mkdir_temp_dir_remote(client, name):
    if hasattr(client, "_iter_management_clients"):
        last_resp = None
        for _, api_client in client._iter_management_clients("fs_mkdir"):
            try:
                if hasattr(client, "_rate_limit"):
                    client._rate_limit()
                resp = api_client.fs_mkdir(name, "0")
                last_resp = resp
                if isinstance(resp, dict) and (resp.get("state") or _is_exists_response(resp)):
                    return {"state": True, "raw": resp}
            except Exception as e:
                if _is_exists_response({"message": str(e)}):
                    return {"state": True, "raw_error": str(e)}
                last_resp = {"state": False, "message": str(e)}
        return last_resp or {"state": False, "message": "创建 115 临时目录失败"}
    return client.fs_mkdir(name, "0")


def _is_exists_response(resp):
    text = str(resp or "").lower()
    return any(x in text for x in ("已存在", "目录名称已存在", "already", "exist", "exists", "same_name", "重复"))


def _wait_temp_dir_cid(client, name):
    for _ in range(10):
        cid = _find_temp_dir_cid(client, name)
        if cid:
            return cid
        time.sleep(0.5)
    return ""


def _find_temp_dir_cid(client, name):
    for item in _list_items(client, "0", show_dir=1):
        if _item_is_dir(item) and _item_name(item) == name:
            cid = _item_id(item)
            if cid:
                return cid
    return ""


def cleanup_old_temp_videos_for_client(client, older_than_hours=3, label="", cid=""):
    cid = str(cid or "").strip() or ensure_temp_dir(client)
    cutoff = time.time() - max(float(older_than_hours or 3), 0.1) * 3600
    delete_ids = []
    for item in _list_items(client, cid, show_dir=0):
        if _item_is_dir(item):
            continue
        name = _item_name(item)
        ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
        if ext not in TEMP_VIDEO_EXTENSIONS:
            continue
        ts = _item_ts(item)
        if ts and ts < cutoff:
            item_id = _item_id(item)
            if item_id:
                delete_ids.append(item_id)

    if not delete_ids:
        return {"deleted": 0, "cid": cid, "label": label}

    resp = client.fs_delete(delete_ids)
    if not isinstance(resp, dict) or not resp.get("state"):
        raise RuntimeError((resp or {}).get("error_msg") or (resp or {}).get("message") or f"删除 115 临时文件失败: {resp}")
    logger.info("  ➜ [115临时目录] 已清理 %s 个过期视频：%s", len(delete_ids), label or cid)
    return {"deleted": len(delete_ids), "cid": cid, "label": label}


def _is_virtual_play_enabled():
    try:
        cfg = settings_db.get_shared_resource_config() or {}
        return bool(cfg.get("p115_shared_virtual_import_enabled"))
    except Exception as e:
        logger.debug("  ➜ [115临时目录] 读取虚拟播放开关失败：%s", e)
        return False


def cleanup_all_temp_videos(older_than_hours=3):
    from etk.compatibility.playback import persistent_playback_gateway

    gateway = persistent_playback_gateway()
    if gateway is not None:
        removed = gateway.cleanup()
        return {
            "deleted": removed,
            "details": [{"deleted": removed, "label": "native-playback", "lease_guarded": True}],
            "main_cleanup_enabled": True,
            "play_pool_cleanup_enabled": gateway.pool_enabled(),
        }
    total = 0
    details = []
    from handler import p115_play_pool
    from handler.p115_copy_play import is_copy_play_enabled
    from handler.p115_service import P115Service

    copy_play_enabled = is_copy_play_enabled()
    virtual_play_enabled = _is_virtual_play_enabled()
    play_pool_enabled = p115_play_pool.is_pool_enabled()

    if copy_play_enabled or virtual_play_enabled:
        client = P115Service.get_client()
        if client:
            result = cleanup_old_temp_videos_for_client(client, older_than_hours, "主号")
            total += int(result.get("deleted") or 0)
            details.append(result)
    else:
        logger.info("  ➜ [115临时目录] 跳过主号临时目录清理：复制播放和虚拟播放均未开启。")
        details.append({"deleted": 0, "label": "主号", "skipped": True, "reason": "copy_play_and_virtual_play_disabled"})

    if play_pool_enabled:
        for result in p115_play_pool.cleanup_temp_dir_old_videos(older_than_hours):
            total += int(result.get("deleted") or 0)
            details.append(result)
    else:
        logger.info("  ➜ [115临时目录] 跳过小号临时目录清理：小号播放未开启。")
        details.append({"deleted": 0, "label": "小号", "skipped": True, "reason": "play_pool_disabled"})

    return {
        "deleted": total,
        "details": details,
        "main_cleanup_enabled": bool(copy_play_enabled or virtual_play_enabled),
        "play_pool_cleanup_enabled": bool(play_pool_enabled),
    }
