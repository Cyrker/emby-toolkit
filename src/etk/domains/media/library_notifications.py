from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from etk.integrations.telegram import TelegramBotPort
from etk.platform import Settings
from etk.platform.tasks import TaskOutcome, TaskOutcomeStatus

from .effect_execution import MediaEffectDeliveryIndeterminate, PersistentMediaEffectExecutor
from .effects import MediaEffectProjection

logger = logging.getLogger(__name__)
LIBRARY_NOTIFICATION_EFFECT = "telegram.library.result"


class TelegramLibraryNotificationDelivery:
    effect_types = frozenset({LIBRARY_NOTIFICATION_EFFECT})

    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        telegram: TelegramBotPort,
        admin_chat_ids: Callable[[], Sequence[str]],
    ) -> None:
        self._settings = settings
        self._telegram = telegram
        self._admins = admin_chat_ids

    def deliver(self, effect: MediaEffectProjection) -> Mapping[str, Any]:
        if effect.effect_type != LIBRARY_NOTIFICATION_EFFECT:
            raise ValueError("unsupported library notification effect")
        settings = self._settings()
        if "library_new" not in settings.telegram_notify_types:
            return {"skipped": True, "reason": "notification_disabled"}
        if not settings.telegram_bot_token.get_secret_value().strip():
            return {"skipped": True, "reason": "bot_not_configured"}
        targets = sorted(
            {str(v).strip() for v in (*self._admins(), settings.telegram_channel_id) if str(v or "").strip()}
        )
        if not targets:
            return {"skipped": True, "reason": "no_notification_targets"}
        sent = 0
        for target in targets:
            result = self._telegram.send_message(target, str(effect.payload["message"]))
            if not result.successful:
                # Telegram 没有消息幂等键；结果不明或部分送达时不重发。
                raise MediaEffectDeliveryIndeterminate(
                    "Telegram 入库结果通知未全部确认", error={"sent": sent, "status": result.status_code}
                )
            sent += 1
        logger.info("入库结果通知已发送，接收目标=%s", sent)
        return {"sent": sent}


class PersistentLibraryResultNotification:
    def __init__(
        self, executor: PersistentMediaEffectExecutor, item: Callable[[str], Mapping[str, Any] | None]
    ) -> None:
        self._executor = executor
        self._item = item

    def __call__(self, item_id: str, outcome: TaskOutcome, operation_key: str) -> Mapping[str, Any]:
        key = f"telegram:library-result:{operation_key}:{item_id}"
        previous = self._executor.read(key)
        if previous is not None:
            effect = self._executor.execute(previous.id)
        else:
            item = self._item(item_id) or {}
            title = " · ".join(str(v) for v in (item.get("SeriesName"), item.get("Name")) if v) or f"Emby ID {item_id}"
            episode = (
                f"\n季集：S{item.get('ParentIndexNumber')} E{item.get('IndexNumber')}"
                if item.get("Type") == "Episode"
                else ""
            )
            status = (
                "刮削回填完成"
                if outcome.status is TaskOutcomeStatus.SUCCEEDED
                else "刮削回填未全部完成，请查看任务中心"
            )
            counts = outcome.details.get("metadata") or {}
            message = (
                f"ETK 入库处理结果\n媒体：{title}{episode}\n状态：{status}\n"
                f"确认 {counts.get('verified', 0)} 项，失败 {counts.get('failed', 0)} 项\nEmby ID：{item_id}"
            )[:4096]
            effect = self._executor.reserve_and_execute(
                effect_key=key,
                effect_type=LIBRARY_NOTIFICATION_EFFECT,
                aggregate_type="media_item",
                aggregate_id=item_id,
                payload={"message": message},
            )
        logger.info("入库结果通知处理结束，Emby ID=%s，状态=%s", item_id, effect.status.value)
        return {"effect_id": str(effect.id), "status": effect.status.value, "result": dict(effect.result or {})}
