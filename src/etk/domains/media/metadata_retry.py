from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from etk.domains.operations.jobs import JobSubmissionService, TaskBusyError
from etk.platform.tasks import TaskOutcome

from .effect_execution import MediaEffectDeliveryRejected, MediaEffectDeliveryRetryable
from .effects import MediaEffectProjection, PostgresMediaEffectLedger
from .library_notification_repository import library_notification_member_key

METADATA_RETRY_EFFECT = "media.metadata.retry"
RETRY_HOURS = (6, 24, 48)


class MetadataRetryScheduler:
    def __init__(self, ledger: PostgresMediaEffectLedger) -> None:
        self._ledger = ledger

    def __call__(
        self, outcome: TaskOutcome, operation_key: str, retry_count: int, *,
        notification_member_key: str = "", notification_item_id: str = "",
    ) -> dict[str, Any]:
        metadata = outcome.details.get("metadata", {})
        failed_ids = {item.get("item_id") for item in metadata.get("failures", [])}
        incomplete = [item for item in metadata.get("incomplete", []) if item.get("item_id") not in failed_ids]
        if not incomplete:
            return {"status": "not_needed"}
        if retry_count >= len(RETRY_HOURS):
            return {"status": "exhausted", "message": "自动补抓已达 3 次，请待来源补齐后手动重新刮削。"}
        scheduled = []
        for item in incomplete:
            item_id = str(item["item_id"])
            effect = self._ledger.reserve(
                effect_key=f"{operation_key}:metadata-retry:{item_id}",
                effect_type=METADATA_RETRY_EFFECT,
                aggregate_type="EmbyItem",
                aggregate_id=item_id,
                payload={
                    "item_id": item_id,
                    "retry_count": retry_count + 1,
                    "notification_member_key": (
                        notification_member_key
                        if notification_member_key and item_id == notification_item_id
                        else library_notification_member_key(operation_key, item_id)
                    ),
                },
                available_at=datetime.now(UTC) + timedelta(hours=RETRY_HOURS[retry_count]),
            ).effect
            scheduled.append(
                {"item_id": item_id, "effect_id": str(effect.id), "available_at": effect.available_at.isoformat()}
            )
        return {"status": "scheduled", "items": scheduled}


class MetadataRetryDelivery:
    effect_types = frozenset({METADATA_RETRY_EFFECT})

    def __init__(self, jobs: JobSubmissionService) -> None:
        self._jobs = jobs

    def deliver(self, effect: MediaEffectProjection) -> dict[str, Any]:
        count = effect.payload.get("retry_count")
        if type(count) is not int or not 1 <= count <= len(RETRY_HOURS):
            raise MediaEffectDeliveryRejected("元数据补抓次数无效")
        scope, key = "metadata-completion-retry", str(effect.id)
        previous = self._jobs.get_idempotent(scope=scope, key=key)
        if previous is not None:
            return {"job_id": str(previous.job.id), "reused": True}
        parameters = {
            "item_id": str(effect.payload["item_id"]),
            "restore_media_info": False,
            "only_missing": True,
            "notify_library": False,
            "metadata_retry_count": count,
        }
        member_key = str(effect.payload.get("notification_member_key") or "").strip()
        if member_key:
            parameters["notification_member_key"] = member_key
        try:
            job = self._jobs.submit_native_operation(
                "media.reprocess-item",
                parameters,
                display_name=f"中文元数据补抓（第 {count}/3 次）",
                idempotency_scope=scope,
                idempotency_key=key,
                reject_when_busy=True,
            )
        except TaskBusyError as exc:
            if effect.attempts >= 24:
                raise MediaEffectDeliveryRejected("任务持续繁忙，自动补抓未入队，请手动重新刮削") from exc
            raise MediaEffectDeliveryRetryable("等待当前任务结束后补抓", retry_after_seconds=300) from exc
        return {"job_id": str(job.projection.job.id), "reused": job.reused}
