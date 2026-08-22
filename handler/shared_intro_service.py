# handler/shared_intro_service.py
import json
import logging
import re
from typing import Any, Dict, List
from urllib.parse import unquote

from database import settings_db
from database.connection import get_db_connection
from handler.p115_service import P115CacheManager
from handler.shared_center_client import SharedCenterClient, shared_center_enabled
from etk.compatibility.background import spawn

logger = logging.getLogger(__name__)


INTRO_MARKER_TYPES = {"IntroStart", "IntroEnd"}


def shared_intro_enabled() -> bool:
    try:
        cfg = settings_db.get_shared_resource_config() or {}
        return bool(cfg.get("p115_shared_resource_enabled")) and bool(cfg.get("p115_shared_intro_enabled"))
    except Exception as e:
        logger.debug(f"  ➜ [共享片头] 读取配置失败，按未启用处理: {e}")
        return False


def _norm_sha1(value: Any) -> str:
    text = str(value or "").strip().upper()
    return text if re.fullmatch(r"[A-F0-9]{40}", text) else ""


def extract_intro_chapters(mediainfo: Any) -> List[Dict[str, Any]]:
    """从 ETK/Emby 媒体信息中提取片头章节。

    片头结束使用 MarkerType=IntroEnd，名称可能显示“片尾”，这里按 MarkerType
    判断，不按 Name 判断。
    """
    root = mediainfo
    if isinstance(root, list) and root:
        root = root[0]
    if not isinstance(root, dict):
        return []
    raw_chapters = root.get("Chapters")
    if not isinstance(raw_chapters, list):
        return []
    chapters = []
    for index, item in enumerate(raw_chapters):
        if not isinstance(item, dict):
            continue
        marker = str(item.get("MarkerType") or "").strip()
        if marker not in INTRO_MARKER_TYPES:
            continue
        if "#SA" in str(item.get("Name") or ""):
            return []
        try:
            ticks = int(float(item.get("StartPositionTicks")))
        except Exception:
            continue
        if ticks < 0:
            continue
        chapter = {
            "StartPositionTicks": ticks,
            "Name": str(item.get("Name") or ("片头" if marker == "IntroStart" else "片头结束")),
            "MarkerType": marker,
            "ChapterIndex": int(item.get("ChapterIndex") if item.get("ChapterIndex") is not None else index),
        }
        chapters.append(chapter)
    marker_set = {x.get("MarkerType") for x in chapters}
    if not {"IntroStart", "IntroEnd"}.issubset(marker_set):
        return []
    chapters.sort(key=lambda x: (int(x.get("StartPositionTicks") or 0), str(x.get("MarkerType") or "")))
    return chapters


def has_intro_chapters(mediainfo: Any) -> bool:
    return bool(extract_intro_chapters(mediainfo))


def merge_intro_chapters(mediainfo: Any, chapters: List[Dict[str, Any]]) -> Any:
    if not isinstance(chapters, list) or not chapters:
        return mediainfo
    if isinstance(mediainfo, list):
        if not mediainfo or not isinstance(mediainfo[0], dict):
            return mediainfo
        target = mediainfo[0]
    elif isinstance(mediainfo, dict):
        target = mediainfo
    else:
        return mediainfo
    existing = target.get("Chapters")
    if not isinstance(existing, list):
        existing = []
    kept = [x for x in existing if not (isinstance(x, dict) and str(x.get("MarkerType") or "") in INTRO_MARKER_TYPES)]
    target["Chapters"] = kept + [dict(x) for x in chapters if isinstance(x, dict)]
    return mediainfo


def _emby_item_matches_cache(item: Any, sha1: str, expected_pick_code: str = "") -> bool:
    if not isinstance(item, dict):
        return False
    values = [item.get("Path")]
    values.extend(
        source.get("Path")
        for source in (item.get("MediaSources") or [])
        if isinstance(source, dict)
    )
    sha1 = _norm_sha1(sha1)
    expected_pick_code = str(expected_pick_code or "").strip().lower()
    for value in values:
        text = unquote(str(value or ""))
        play_match = re.search(r"/api/p115/play/([^/?#]+)", text, flags=re.IGNORECASE)
        if play_match and expected_pick_code and play_match.group(1).strip().lower() == expected_pick_code:
            return True
        virtual_match = re.search(
            r"/api/p115/virtual-play/[^/?#]+/([A-Fa-f0-9]{40})(?:[/?#]|$)",
            text,
            flags=re.IGNORECASE,
        )
        if virtual_match and sha1 and virtual_match.group(1).upper() == sha1:
            return True
    return False


def get_verified_emby_item_for_cache(
    sha1: str,
    item_id: str,
    *,
    expected_pick_code: str = "",
) -> Dict[str, Any]:
    sha1 = _norm_sha1(sha1)
    item_id = str(item_id or "").strip()
    if not sha1 or not re.fullmatch(r"\d{1,20}", item_id):
        return {}

    from handler import emby
    import config_manager
    import constants

    item = emby.get_emby_item_details(
        item_id,
        config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL),
        config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY),
        config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_USER_ID),
        fields="Path,MediaSources,Chapters,SeriesId,SeasonId,SeriesName,ParentIndexNumber,IndexNumber,Type,Name",
        silent_404=True,
    )
    return item if _emby_item_matches_cache(item, sha1, expected_pick_code) else {}


def sync_intro_from_emby_item(sha1: str, item_id: str, *, expected_pick_code: str = "") -> Dict[str, Any]:
    """Persist Emby's detected intro chapters before the bridge restores cached media info."""
    if not shared_intro_enabled():
        return {"ok": False, "skipped": True, "reason": "shared_intro_disabled"}
    sha1 = _norm_sha1(sha1)
    item_id = str(item_id or "").strip()
    item = get_verified_emby_item_for_cache(
        sha1,
        item_id,
        expected_pick_code=expected_pick_code,
    )
    if not item:
        return {"ok": False, "skipped": True, "reason": "item_identity_mismatch"}

    chapters = extract_intro_chapters({"Chapters": item.get("Chapters") or []})
    if not chapters:
        return {"ok": False, "skipped": True, "reason": "intro_not_detected"}

    from psycopg2.extras import Json

    changed = False
    cache_missing = False
    mediainfo = None
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT mediainfo_json FROM p115_mediainfo_cache WHERE sha1=%s FOR UPDATE",
                (sha1,),
            )
            row = cur.fetchone()
            mediainfo = row.get("mediainfo_json") if row else None
            if isinstance(mediainfo, str):
                mediainfo = json.loads(mediainfo)
            if not mediainfo:
                cache_missing = True
            elif extract_intro_chapters(mediainfo) != chapters:
                merge_intro_chapters(mediainfo, chapters)
                cur.execute(
                    "UPDATE p115_mediainfo_cache SET mediainfo_json=%s WHERE sha1=%s",
                    (Json(mediainfo, dumps=lambda obj: json.dumps(obj, ensure_ascii=False)), sha1),
                )
                changed = True
        conn.commit()

    if cache_missing:
        return {"ok": False, "skipped": True, "reason": "media_info_cache_not_found"}

    if changed:
        file_name = str(item.get("Name") or "").strip()

        def _upload():
            try:
                upload_intro_for_cache_sha1(
                    sha1,
                    mediainfo,
                    file_name=file_name,
                    reason="emby_item_updated",
                )
            except Exception as e:
                logger.debug(f"  ➜ [共享片头] Emby 片头后台上传失败: {sha1[:12]}... -> {e}")

        spawn(_upload)
        series_name = str(item.get("SeriesName") or "").strip()
        season_number = item.get("ParentIndexNumber")
        episode_number = item.get("IndexNumber")
        if series_name and season_number is not None and episode_number is not None:
            item_label = f"《{series_name}》S{int(season_number):02d}E{int(episode_number):02d}"
        else:
            item_label = str(item.get("Name") or f"Emby Item {item_id}").strip()
        logger.info(
            f"  ➜ [共享片头] 已从{item_label}回写片头到媒体信息缓存: "
            f"{sha1[:12]}...（{len(chapters)} 个章节）"
        )
    return {"ok": True, "updated": changed, "chapter_count": len(chapters)}


def upload_intro_for_cache_sha1(sha1: str, mediainfo: Any, *, file_name: str = "", reason: str = "") -> Dict[str, Any]:
    if not shared_intro_enabled():
        return {"ok": False, "skipped": True, "reason": "shared_intro_disabled"}
    chapters = extract_intro_chapters(mediainfo)
    sha1 = _norm_sha1(sha1)
    if not sha1 or not chapters or not shared_center_enabled():
        return {"ok": False, "skipped": True}
    resp = SharedCenterClient().upload_intro_chapters(sha1, chapters, file_name=file_name, reason=reason)
    return {"ok": bool(resp.get("ok", True)), "sha1": sha1, "center": resp}


def fetch_intro_map(sha1_list: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    if not shared_intro_enabled():
        return {}
    sha1s = []
    for value in sha1_list or []:
        sha1 = _norm_sha1(value)
        if sha1 and sha1 not in sha1s:
            sha1s.append(sha1)
    if not sha1s:
        return {}
    try:
        resp = SharedCenterClient().intro_chapters_batch(sha1s)
    except Exception as e:
        logger.debug(f"  ➜ [共享片头] 拉取中心片头失败: {e}")
        return {}
    out = {}
    for item in resp.get("items") or []:
        sha1 = _norm_sha1((item or {}).get("sha1"))
        chapters = (item or {}).get("chapters")
        if sha1 and isinstance(chapters, list) and chapters:
            out[sha1] = chapters
    return out


def merge_intro_into_local_cache(sha1: str, chapters: List[Dict[str, Any]]) -> bool:
    if not shared_intro_enabled():
        return False
    sha1 = _norm_sha1(sha1)
    if not sha1 or not chapters:
        return False
    try:
        text = P115CacheManager.get_mediainfo_cache_text(sha1)
        if not text:
            return False
        data = json.loads(text)
        if extract_intro_chapters(data):
            return False
        merge_intro_chapters(data, chapters)
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE p115_mediainfo_cache SET mediainfo_json=%s WHERE sha1=%s",
                    (json.dumps(data, ensure_ascii=False), sha1),
                )
            conn.commit()
        return True
    except Exception as e:
        logger.debug(f"  ➜ [共享片头] 合并片头到本地缓存失败: {sha1[:12]} -> {e}")
        return False


def scan_and_upload_local_intro(limit: int = 500) -> Dict[str, Any]:
    if not shared_intro_enabled():
        return {"ok": False, "skipped": True, "reason": "shared_intro_disabled"}
    scanned = uploaded = failed = skipped = 0
    errors = []
    try:
        resp = SharedCenterClient().intro_chapters_missing(limit=limit)
    except Exception as e:
        return {"ok": False, "scanned": 0, "uploaded": 0, "skipped": 0, "failed": 1, "errors": [{"message": str(e)}]}

    items = [x for x in (resp.get("items") or []) if isinstance(x, dict)]
    for item in items:
        scanned += 1
        sha1 = _norm_sha1(item.get("sha1"))
        if not sha1:
            skipped += 1
            continue
        try:
            text = P115CacheManager.get_mediainfo_cache_text(sha1)
            data = json.loads(text) if text else None
            chapters = extract_intro_chapters(data)
            if not chapters:
                skipped += 1
                continue
            resp = SharedCenterClient().upload_intro_chapters(
                sha1,
                chapters,
                file_name=str(item.get("file_name") or "").strip(),
                reason="maintenance_backfill",
            )
            if resp.get("duplicate"):
                skipped += 1
            elif resp.get("ok", True):
                uploaded += 1
            else:
                failed += 1
                errors.append({"sha1": sha1, "message": resp.get("message") or resp.get("detail") or "upload_failed"})
        except Exception as e:
            failed += 1
            errors.append({"sha1": sha1, "message": str(e)})
    return {"ok": failed == 0, "scanned": scanned, "uploaded": uploaded, "skipped": skipped, "failed": failed, "errors": errors[:20]}
