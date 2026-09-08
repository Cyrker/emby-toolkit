from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs import ActiveJobConflictError, normalize_json_object
from etk.platform.tasks import ProcessorIdentity, TaskOutcome, TaskOutcomeStatus

from .jobs import JobSubmissionService, SubmittedJob, TaskBusyError

REVIEW_REPROCESS_ONE_TASK_KEY = "p8-review-reprocess-one"
REVIEW_REPROCESS_ALL_TASK_KEY = "p8-review-reprocess-all"
P8_REVIEW_TASK_KEYS = frozenset({REVIEW_REPROCESS_ONE_TASK_KEY, REVIEW_REPROCESS_ALL_TASK_KEY})


@dataclass(frozen=True, slots=True)
class ReviewReprocessHttpResult:
    payload: Any
    status_code: int = 200
    location: str | None = None


class ReviewReprocessRepository(Protocol):
    def item(self, item_id: str) -> Mapping[str, Any] | None: ...

    def items(self, *, reason_filter: str | None = None) -> Sequence[Mapping[str, Any]]: ...


class ReviewItemReprocess(Protocol):
    def execute(
        self,
        item_id: str,
        progress: Any,
        *,
        operation_key: str,
        media_info_only: bool = False,
    ) -> TaskOutcome: ...


class ReviewJobPort(Protocol):
    def submit_one(
        self,
        *,
        item_id: str,
        item_name: str,
        reason: str | None,
        requested_by_user_id: str | None,
        trace_id: str | None,
        idempotency_key: str | None,
    ) -> SubmittedJob: ...

    def submit_all(
        self,
        *,
        reason_filter: str | None,
        requested_by_user_id: str | None,
        trace_id: str | None,
        idempotency_key: str | None,
    ) -> SubmittedJob: ...


class SqlAlchemyReviewReprocessRepository:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def item(self, item_id: str) -> Mapping[str, Any] | None:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT item_id, item_name, reason
                        FROM failed_log
                        WHERE item_id = :item_id
                        ORDER BY failed_at DESC NULLS LAST
                        LIMIT 1
                        """
                    ),
                    {"item_id": item_id},
                )
                .mappings()
                .first()
            )
        return dict(row) if row is not None else None

    def items(self, *, reason_filter: str | None = None) -> Sequence[Mapping[str, Any]]:
        normalized_reason = str(reason_filter or "").strip()
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT DISTINCT ON (item_id) item_id, item_name, reason
                        FROM failed_log
                        WHERE (:reason = '' OR reason LIKE :reason_prefix)
                        ORDER BY item_id, failed_at DESC NULLS LAST
                        """
                    ),
                    {"reason": normalized_reason, "reason_prefix": f"{normalized_reason}%"},
                )
                .mappings()
                .all()
            )
        return tuple(dict(row) for row in rows)


class NativeReviewReprocessExecutor:
    def __init__(self, repository: ReviewReprocessRepository, reprocess: ReviewItemReprocess) -> None:
        self._repository = repository
        self._reprocess = reprocess

    def execute(
        self,
        task_key: str,
        parameters: Mapping[str, Any],
        progress: Any,
        *,
        operation_key: str,
    ) -> TaskOutcome:
        if task_key == REVIEW_REPROCESS_ONE_TASK_KEY:
            item_id = str(parameters.get("item_id") or "").strip()
            media_info_only = "缺失媒体信息" in str(parameters.get("failure_reason") or "")
            return self._reprocess.execute(
                item_id,
                progress,
                operation_key=f"{operation_key}:{item_id}",
                media_info_only=media_info_only,
            )
        if task_key != REVIEW_REPROCESS_ALL_TASK_KEY:
            raise LookupError(f"unknown P8 review task key: {task_key}")

        rows = tuple(self._repository.items(reason_filter=parameters.get("reason_filter")))
        if not rows:
            progress.update(100, "列表为空。")
            return TaskOutcome.skipped("列表为空。", {"processed": 0, "failed": 0})
        outcomes: list[TaskOutcome] = []
        for index, row in enumerate(rows, 1):
            progress.checkpoint()
            item_id = str(row.get("item_id") or "").strip()
            progress.update(
                min(95, int((index - 1) * 95 / len(rows))),
                f"正在重新处理 {index}/{len(rows)}: {str(row.get('item_name') or item_id)}",
            )
            outcomes.append(
                self._reprocess.execute(
                    item_id,
                    progress,
                    operation_key=f"{operation_key}:{item_id}",
                    media_info_only="缺失媒体信息" in str(row.get("reason") or ""),
                )
            )
        failed = sum(outcome.status is TaskOutcomeStatus.FAILED for outcome in outcomes)
        partial = sum(outcome.status is TaskOutcomeStatus.PARTIAL for outcome in outcomes)
        details = {"processed": len(outcomes), "failed": failed, "partial": partial}
        progress.update(100, "批量重新处理完成。")
        if failed == len(outcomes):
            return TaskOutcome.failed("批量重新处理失败。", details)
        if failed or partial:
            return TaskOutcome.partial("批量重新处理部分完成。", details)
        return TaskOutcome.succeeded("批量重新处理完成。", details)


class PersistentReviewJobGateway:
    def __init__(self, jobs: JobSubmissionService) -> None:
        self._jobs = jobs

    def submit_one(
        self,
        *,
        item_id: str,
        item_name: str,
        reason: str | None,
        requested_by_user_id: str | None,
        trace_id: str | None,
        idempotency_key: str | None,
    ) -> SubmittedJob:
        return self._submit(
            REVIEW_REPROCESS_ONE_TASK_KEY,
            {
                "item_id": item_id,
                "item_name_for_ui": item_name,
                "failure_reason": reason,
            },
            display_name=f"重新处理: {item_name}",
            requested_by_user_id=requested_by_user_id,
            trace_id=trace_id,
            idempotency_key=idempotency_key,
            idempotency_scope=f"api.review.reprocess-one:{item_id}",
            timeout_seconds=3600,
        )

    def submit_all(
        self,
        *,
        reason_filter: str | None,
        requested_by_user_id: str | None,
        trace_id: str | None,
        idempotency_key: str | None,
    ) -> SubmittedJob:
        label = reason_filter or "全部"
        return self._submit(
            REVIEW_REPROCESS_ALL_TASK_KEY,
            {"reason_filter": reason_filter},
            display_name=f"批量重新处理 ({label})",
            requested_by_user_id=requested_by_user_id,
            trace_id=trace_id,
            idempotency_key=idempotency_key,
            idempotency_scope="api.review.reprocess-all",
            timeout_seconds=21600,
        )

    def _submit(
        self,
        task_key: str,
        parameters: Mapping[str, Any],
        *,
        display_name: str,
        requested_by_user_id: str | None,
        trace_id: str | None,
        idempotency_key: str | None,
        idempotency_scope: str,
        timeout_seconds: int,
    ) -> SubmittedJob:
        payload = {
            "payload_version": 1,
            "parameters": normalize_json_object(dict(parameters), field="review reprocess parameters"),
            "processor_type": ProcessorIdentity.MEDIA.value,
            "display_name": display_name,
            "silent": False,
        }
        try:
            result = self._jobs.store.create_job(
                task_key=task_key,
                payload=payload,
                queue_name="legacy.serial",
                trigger_type="manual",
                max_attempts=1,
                timeout_seconds=timeout_seconds,
                requested_by_user_id=requested_by_user_id,
                trace_id=trace_id,
                idempotency_key=idempotency_key,
                idempotency_scope=idempotency_scope if idempotency_key else None,
            )
        except ActiveJobConflictError as exc:
            raise TaskBusyError("another job is active") from exc
        return SubmittedJob(result.projection, result.reused)


class ReviewReprocessService:
    def __init__(
        self,
        repository: ReviewReprocessRepository,
        jobs: ReviewJobPort,
    ) -> None:
        self._repository = repository
        self._jobs = jobs

    def submit_one(
        self,
        item_id: str,
        payload: Any,
        *,
        requested_by_user_id: str | None,
        trace_id: str | None,
        idempotency_key: str | None,
    ) -> ReviewReprocessHttpResult:
        normalized_item_id = str(item_id or "").strip()
        if not normalized_item_id or len(normalized_item_id) > 200:
            return ReviewReprocessHttpResult({"error": "无效的项目 ID"}, 400)
        try:
            values = _payload(payload, allowed={"reason"})
            requested_reason = _optional_text(values.get("reason"), field="reason", limit=1000)
        except (TypeError, ValueError) as exc:
            return ReviewReprocessHttpResult({"error": str(exc)}, 400)
        item = self._repository.item(normalized_item_id)
        if item is None:
            return ReviewReprocessHttpResult({"error": "待复核项目不存在"}, 404)
        item_name = str(item.get("item_name") or f"ItemID: {normalized_item_id}").strip()
        reason = requested_reason or _optional_text(item.get("reason"), field="reason", limit=1000)
        try:
            submitted = self._jobs.submit_one(
                item_id=normalized_item_id,
                item_name=item_name,
                reason=reason,
                requested_by_user_id=requested_by_user_id,
                trace_id=trace_id,
                idempotency_key=idempotency_key,
            )
        except TaskBusyError:
            return ReviewReprocessHttpResult({"error": "提交任务失败，已有任务在运行。"}, 409)
        except Exception:
            return ReviewReprocessHttpResult({"error": "任务提交失败"}, 500)
        return _submitted_result(
            submitted,
            f"重新处理项目 '{item_name}' 的任务已提交。",
        )

    def submit_all(
        self,
        payload: Any,
        *,
        requested_by_user_id: str | None,
        trace_id: str | None,
        idempotency_key: str | None,
    ) -> ReviewReprocessHttpResult:
        try:
            values = _payload(payload, allowed={"reason"})
            reason_filter = _optional_text(values.get("reason"), field="reason", limit=1000)
        except (TypeError, ValueError) as exc:
            return ReviewReprocessHttpResult({"error": str(exc)}, 400)
        try:
            submitted = self._jobs.submit_all(
                reason_filter=reason_filter,
                requested_by_user_id=requested_by_user_id,
                trace_id=trace_id,
                idempotency_key=idempotency_key,
            )
        except TaskBusyError:
            return ReviewReprocessHttpResult({"error": "后台有任务正在运行，请稍后再试。"}, 409)
        except Exception:
            return ReviewReprocessHttpResult({"error": "任务提交失败"}, 500)
        label = reason_filter or "全部"
        return _submitted_result(submitted, f"任务 '批量重新处理 ({label})' 已成功提交！")


def _payload(value: Any, *, allowed: set[str]) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise TypeError("请求参数必须是对象")
    values = {str(key): item for key, item in value.items()}
    extras = set(values) - allowed
    if extras:
        raise ValueError(f"包含不支持的参数: {', '.join(sorted(extras))}")
    return values


def _optional_text(value: Any, *, field: str, limit: int) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{field} 必须是字符串")
    normalized = value.strip()
    if len(normalized) > limit:
        raise ValueError(f"{field} 过长")
    return normalized or None


def _submitted_result(submitted: SubmittedJob, message: str) -> ReviewReprocessHttpResult:
    job_id = str(submitted.projection.job.id)
    status_url = f"/api/jobs/{job_id}"
    return ReviewReprocessHttpResult(
        {
            "message": message,
            "job_id": job_id,
            "status_url": status_url,
            "reused": submitted.reused,
        },
        202,
        status_url,
    )


__all__ = [
    "P8_REVIEW_TASK_KEYS",
    "NativeReviewReprocessExecutor",
    "PersistentReviewJobGateway",
    "REVIEW_REPROCESS_ALL_TASK_KEY",
    "REVIEW_REPROCESS_ONE_TASK_KEY",
    "ReviewJobPort",
    "ReviewReprocessHttpResult",
    "ReviewReprocessRepository",
    "ReviewReprocessService",
    "SqlAlchemyReviewReprocessRepository",
]
