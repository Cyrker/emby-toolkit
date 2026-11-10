from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Any

from etk.integrations.http_policy import ProviderDisposition
from etk.integrations.telegram import TelegramBotPort
from etk.platform import Settings
from etk.platform.tasks import TaskOutcome, TaskOutcomeStatus

from .effect_execution import (
    MediaEffectDeliveryIndeterminate,
    MediaEffectDeliveryRejected,
    PersistentMediaEffectExecutor,
)
from .effects import MediaEffectProjection
from .library_notification_format import library_card, tmdb_photo
from .library_notification_repository import LIBRARY_BATCH_EFFECT, SqlAlchemyLibraryNotificationRepository

logger = logging.getLogger(__name__)
LIBRARY_NOTIFICATION_EFFECT = "telegram.library.result"
LIBRARY_RETRY_SUCCESS_EFFECT = "telegram.library.retry-success"


class TelegramLibraryNotificationDelivery:
    effect_types = frozenset({LIBRARY_NOTIFICATION_EFFECT, LIBRARY_BATCH_EFFECT, LIBRARY_RETRY_SUCCESS_EFFECT})

    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        telegram: TelegramBotPort,
        admin_chat_ids: Callable[[], Sequence[str]],
        batches: SqlAlchemyLibraryNotificationRepository | None = None,
    ) -> None:
        self._settings = settings
        self._telegram = telegram
        self._admins = admin_chat_ids
        self._batches = batches

    def deliver(self, effect: MediaEffectProjection) -> Mapping[str, Any]:
        if effect.effect_type not in self.effect_types:
            raise ValueError("unsupported library notification effect")
        if effect.effect_type == LIBRARY_RETRY_SUCCESS_EFFECT:
            return self._deliver_retry_success(effect.payload)
        payload = effect.payload
        if effect.effect_type == LIBRARY_BATCH_EFFECT:
            if self._batches is None:
                raise RuntimeError("library notification batch repository is unavailable")
            payload = library_card(self._batches.snapshot(str(payload["batch_id"])))
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
        messages: list[dict[str, Any]] = []
        for target in targets:
            parse_mode = str(payload["parse_mode"]) if payload.get("parse_mode") else None
            if payload.get("photo"):
                result = self._telegram.send_photo(
                    target, str(payload["photo"]), str(payload["message"]), parse_mode=parse_mode
                )
                message_kind = "caption"
                # 仅明确拒收图片时回退文字；超时或结果不明时不能再发一遍。
                if result.disposition is ProviderDisposition.REJECTED and result.status_code == 400:
                    result = self._telegram.send_message(target, str(payload["message"]), parse_mode=parse_mode)
                    message_kind = "text"
            else:
                result = self._telegram.send_message(target, str(payload["message"]), parse_mode=parse_mode)
                message_kind = "text"
            if not result.successful:
                # Telegram 没有消息幂等键；结果不明或部分送达时不重发。
                raise MediaEffectDeliveryIndeterminate(
                    "Telegram 入库结果通知未全部确认", error={"sent": sent, "status": result.status_code}
                )
            message_id = _telegram_message_id(result)
            if message_id is not None:
                messages.append({"chat_id": target, "message_id": message_id, "kind": message_kind})
            sent += 1
        logger.info("入库结果通知已发送，接收目标=%s", sent)
        return {"sent": sent, "messages": messages}

    def _deliver_retry_success(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        if self._batches is None:
            return {"skipped": True, "reason": "notification_batch_repository_unavailable"}
        retry_count = payload.get("retry_count")
        if type(retry_count) is not int or not 1 <= retry_count <= 3:
            raise MediaEffectDeliveryRejected("元数据补抓次数无效")
        updated = self._batches.mark_retry_success(str(payload.get("member_key") or ""), retry_count)
        if not updated["updated"]:
            return {"skipped": True, "reason": "notification_batch_not_found"}
        settings = self._settings()
        if "library_new" not in settings.telegram_notify_types:
            return {"updated": True, "edited": 0, "reason": "notification_disabled"}
        if not settings.telegram_bot_token.get_secret_value().strip():
            return {"updated": True, "edited": 0, "reason": "bot_not_configured"}
        messages = []
        for value in updated["messages"]:
            if not isinstance(value, Mapping):
                continue
            chat_id = str(value.get("chat_id") or "").strip()
            message_id = value.get("message_id")
            kind = value.get("kind")
            if chat_id and type(message_id) is int and message_id > 0 and kind in {"text", "caption"}:
                messages.append({"chat_id": chat_id, "message_id": message_id, "kind": kind})
        if not messages:
            return {"updated": True, "edited": 0, "reason": "message_id_unavailable"}
        card = library_card(updated["members"])
        edited = 0
        for message in messages:
            if message["kind"] == "caption":
                result = self._telegram.edit_message_caption(
                    str(message["chat_id"]),
                    message["message_id"],
                    str(card["message"]),
                    parse_mode=str(card["parse_mode"]),
                )
            else:
                result = self._telegram.edit_message_text(
                    str(message["chat_id"]),
                    message["message_id"],
                    str(card["message"]),
                    parse_mode=str(card["parse_mode"]),
                )
            if not result.successful and not _telegram_message_unchanged(result):
                error = {"edited": edited, "total": len(messages), "status": result.status_code}
                if result.disposition is ProviderDisposition.REJECTED:
                    raise MediaEffectDeliveryRejected("Telegram 拒绝更新入库通知", error=error)
                raise MediaEffectDeliveryIndeterminate("Telegram 入库通知更新未全部确认", error=error)
            edited += 1
        return {"updated": True, "edited": edited, "retry_count": payload["retry_count"]}


class PersistentLibraryResultNotification:
    def __init__(
        self,
        executor: PersistentMediaEffectExecutor,
        item: Callable[[str], Mapping[str, Any] | None],
        *,
        batches: SqlAlchemyLibraryNotificationRepository | None = None,
    ) -> None:
        self._executor = executor
        self._item = item
        self._batches = batches

    def __call__(self, item_id: str, outcome: TaskOutcome, operation_key: str) -> Mapping[str, Any]:
        key = f"telegram:library-result:{operation_key}:{item_id}"
        previous = self._executor.read(key)
        if previous is not None:
            effect = self._executor.execute(previous.id)
        elif self._batches is not None:
            member = self._member(item_id, outcome)
            effect = self._batches.stage(member, operation_key=operation_key)
            logger.info("入库结果已加入合并通知批次：媒体=%s，批次=%s", member["title"], effect.aggregate_id)
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

    def _member(self, item_id: str, outcome: TaskOutcome) -> dict[str, Any]:
        assert self._batches is not None
        item = self._item(item_id) or {"Id": item_id}
        series_id = str(item.get("SeriesId") or "")
        root = (self._item(series_id) or {}) if series_id else item
        media = self._batches.media(item_id)
        root_media = self._batches.media(series_id) if series_id else media
        assets = [a for a in media.get("asset_details_json") or [] if isinstance(a, Mapping)]
        assets = [
            a
            for a in assets
            if str(a.get("emby_item_id") or "") == item_id or item.get("Path") and a.get("path") == item["Path"]
        ]
        sources = [s for s in item.get("MediaSources") or [] if isinstance(s, Mapping)]
        if not assets:
            assets = [
                {"emby_item_id": item_id, "path": s.get("Path"), "size_bytes": s.get("Size") or 0} for s in sources
            ] or [{"emby_item_id": item_id, "size_bytes": 0}]
        else:
            assets = [dict(a) for a in assets]
            for asset in assets:
                if asset.get("size_bytes") or asset.get("size") or asset.get("file_size"):
                    continue
                source = next((s for s in sources if s.get("Path") and s.get("Path") == asset.get("path")), None)
                if source is None and len(sources) == len(assets) == 1:
                    source = sources[0]
                if source is not None:
                    asset["size_bytes"] = source.get("Size") or 0
        now = datetime.now().astimezone()
        return {
            "item_id": item_id,
            "item_type": item.get("Type"),
            "group_key": f"Series:{series_id}" if series_id else f"Item:{item_id}",
            "group_path": root.get("Path") or item.get("Path") or "",
            "title": root.get("Name") or item.get("SeriesName") or item.get("Name") or "未知媒体",
            "year": root.get("ProductionYear") or item.get("ProductionYear"),
            "overview": root.get("Overview")
            or root_media.get("overview")
            or item.get("Overview")
            or media.get("overview"),
            "photo": tmdb_photo(root_media) or tmdb_photo(media),
            "season": item.get("ParentIndexNumber"),
            "episode": item.get("IndexNumber"),
            "assets": assets,
            "succeeded": outcome.status in {TaskOutcomeStatus.SUCCEEDED, TaskOutcomeStatus.SKIPPED},
            "completed_at": now.isoformat(),
            "display_time": now.strftime("%Y-%m-%d %H:%M:%S"),
        }


class PersistentLibraryRetrySuccessNotification:
    def __init__(self, executor: PersistentMediaEffectExecutor) -> None:
        self._executor = executor

    def __call__(self, member_key: str, retry_count: int) -> Mapping[str, Any]:
        effect = self._executor.reserve_and_execute(
            effect_key=f"telegram:library-retry-success:{member_key}:{retry_count}",
            effect_type=LIBRARY_RETRY_SUCCESS_EFFECT,
            aggregate_type="library_notification_member",
            aggregate_id=member_key,
            payload={"member_key": member_key, "retry_count": retry_count},
        )
        return {"effect_id": str(effect.id), "status": effect.status.value, "result": dict(effect.result or {})}


def _telegram_message_id(response: Any) -> int | None:
    payload = response.payload
    result = payload.get("result") if isinstance(payload, Mapping) else None
    message_id = result.get("message_id") if isinstance(result, Mapping) else None
    return message_id if type(message_id) is int and message_id > 0 else None


def _telegram_message_unchanged(response: Any) -> bool:
    if response.disposition is not ProviderDisposition.REJECTED or not isinstance(response.payload, Mapping):
        return False
    return "message is not modified" in str(response.payload.get("description") or "").lower()
