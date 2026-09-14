from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from etk.domains.media.effect_execution import MediaEffectDeliveryIndeterminate, MediaEffectDeliveryRetryable
from etk.integrations.telegram import TelegramBotPort
from etk.platform import Settings

logger = logging.getLogger(__name__)


class NativeUserEventEffectsAdapter:
    """将播放事件交给现有原生服务，不加载旧 Flask/进程内播放状态。"""

    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        favorite: Callable[[Mapping[str, Any]], Mapping[str, Any]],
        halfway: Callable[[Mapping[str, Any]], Mapping[str, Any]],
        cleanup: Callable[[Mapping[str, Any]], int],
        release_proxy: Callable[..., int],
        telegram: TelegramBotPort,
        admin_chat_ids: Callable[[], Sequence[str]],
    ) -> None:
        self._settings = settings
        self._favorite = favorite
        self._halfway = halfway
        self._cleanup = cleanup
        self._release_proxy = release_proxy
        self._telegram = telegram
        self._admin_chat_ids = admin_chat_ids

    def execute(self, kind: str, snapshot: Mapping[str, Any]) -> Mapping[str, Any]:
        item = _mapping(snapshot.get("Item"))
        user = _mapping(snapshot.get("User"))
        playback = _mapping(snapshot.get("PlaybackInfo"))
        session = _mapping(snapshot.get("Session"))
        item_id = str(item.get("Id") or snapshot.get("ItemId") or "").strip()
        user_id = str(user.get("Id") or snapshot.get("UserId") or "").strip()
        session_id = str(playback.get("PlaySessionId") or snapshot.get("PlaySessionId") or "").strip()
        device_id = str(snapshot.get("DeviceId") or session.get("DeviceId") or "").strip()
        if kind == "virtual-import-promotion":
            return {"completed": True, "skipped": True, "reason": "feature_retired"}
        if kind in {"favorite-intro-detection", "halfway-intro-detection"}:
            result = dict((self._favorite if kind == "favorite-intro-detection" else self._halfway)(item))
            if result.get("ok") is False:
                raise MediaEffectDeliveryRetryable("片头检测任务暂时无法提交", error={"kind": kind})
            return result
        if kind in {"playback-cleanup", "proxy-concurrency-release"}:
            # 缺少身份时不得退化成按整部媒体/整个用户清理其他会话。
            if not user_id or not (session_id or item_id and device_id):
                return {"completed": True, "skipped": True, "reason": "playback_identity_incomplete"}
            if kind == "playback-cleanup":
                released = self._cleanup(snapshot)
            else:
                released = self._release_proxy(
                    user_id=user_id, item_id=item_id, play_session_id=session_id, device_id=device_id
                )
            logger.info("播放停止后处理完成：%s，项目=%s，释放会话=%s", kind, item_id, released)
            return {"completed": True, "released": released}
        if kind == "telegram-playback-notification":
            return self._notify(snapshot)
        raise ValueError(f"unsupported native user event effect: {kind}")

    def _notify(self, snapshot: Mapping[str, Any]) -> Mapping[str, Any]:
        settings = self._settings()
        if "playback" not in settings.telegram_notify_types:
            return {"completed": True, "skipped": True, "reason": "notification_disabled"}
        if not settings.telegram_bot_token.get_secret_value().strip():
            return {"completed": True, "skipped": True, "reason": "bot_not_configured"}
        targets = sorted(
            {str(value).strip() for value in (*self._admin_chat_ids(), settings.telegram_channel_id) if value}
        )
        if not targets:
            return {"completed": True, "skipped": True, "reason": "no_notification_targets"}
        item = _mapping(snapshot.get("Item"))
        user = _mapping(snapshot.get("User"))
        session = _mapping(snapshot.get("Session"))
        action = {"playback.start": "开始播放", "playback.pause": "暂停播放", "playback.stop": "停止播放"}.get(
            str(snapshot.get("Event")), "播放状态更新"
        )
        title = " · ".join(str(value) for value in (item.get("SeriesName"), item.get("Name")) if value) or "未知媒体"
        message = (
            f"{action}\n用户：{user.get('Name') or '未知用户'}\n媒体：{title}\n"
            f"设备：{session.get('DeviceName') or '未知设备'}（{session.get('Client') or '未知客户端'}）"
        )[:4096]
        sent = 0
        for target in targets:
            result = self._telegram.send_message(target, message)
            if result.disposition.value != "confirmed":
                # 多目标通知一旦部分送达，不自动重放已经发出的消息。
                if result.disposition.value == "retryable" and not sent:
                    raise MediaEffectDeliveryRetryable(
                        "Telegram 暂时拒绝播放通知", error={"status": result.status_code}
                    )
                raise MediaEffectDeliveryIndeterminate("Telegram 播放通知未全部确认", error={"sent": sent})
            sent += 1
        logger.info("播放通知发送完成：%s，接收目标=%s", action, sent)
        return {"completed": True, "sent": sent}


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
