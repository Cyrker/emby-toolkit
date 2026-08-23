from __future__ import annotations

from typing import Any


def cleanup_p115_temp_videos(_processor: Any, *, older_than_hours: int = 3) -> dict[str, Any]:
    from handler.p115_temp_dir import cleanup_all_temp_videos

    return dict(cleanup_all_temp_videos(older_than_hours=older_than_hours))


def check_etk_plugin_update(_processor: Any) -> dict[str, Any]:
    from handler.etk_plugin_update import check_and_update_etk_plugin

    updated = bool(check_and_update_etk_plugin())
    return {"ok": True, "updated": updated}


def verify_pro_status(_processor: Any) -> dict[str, Any]:
    import requests

    import config_manager
    import extensions
    from scheduler_manager import _keep_saved_pro_if_valid

    server_id = str(getattr(extensions, "EMBY_SERVER_ID", "") or "").strip()
    if not server_id:
        return {"ok": True, "checked": False, "reason": "server_id_unavailable"}
    try:
        response = requests.post(
            "https://auth.55565576.xyz",
            json={"action": "check", "server_id": server_id},
            timeout=10,
        ).json()
    except Exception as exc:
        retained = _keep_saved_pro_if_valid()
        if not retained:
            config_manager.APP_CONFIG["is_pro_active"] = False
        return {"ok": retained, "checked": False, "reason": "request_failed", "message": str(exc)}

    if response.get("success") and response.get("is_pro"):
        config_manager.APP_CONFIG["is_pro_active"] = True
        config_manager.APP_CONFIG["pro_expire_time"] = response.get("expire_time", "")
        return {"ok": True, "checked": True, "active": True}
    if response.get("success"):
        config_manager.APP_CONFIG["is_pro_active"] = False
        return {"ok": True, "checked": True, "active": False, "message": str(response.get("msg") or "")}

    retained = _keep_saved_pro_if_valid()
    if not retained:
        config_manager.APP_CONFIG["is_pro_active"] = False
    return {
        "ok": retained,
        "checked": True,
        "active": retained,
        "message": str(response.get("msg") or "authorization check failed"),
    }


__all__ = ["check_etk_plugin_update", "cleanup_p115_temp_videos", "verify_pro_status"]
