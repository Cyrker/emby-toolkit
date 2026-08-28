from __future__ import annotations

from collections.abc import Mapping
from importlib import import_module
from typing import Any, Protocol

from etk.domains.identity.user_sync import UserSyncService
from etk.domains.operations.review_reprocess import (
    P8_REVIEW_TASK_KEYS,
    REVIEW_REPROCESS_ALL_TASK_KEY,
    REVIEW_REPROCESS_ONE_TASK_KEY,
)
from etk.platform.jobs import normalize_json_object
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcome, TaskOutcomeStatus, TaskPayload
from etk.platform.tasks.catalog import P8_NATIVE_TASK_KEYS

P8_COMPATIBILITY_FALLBACK_UNTIL = "P11"

_DISPLAY_NAMES = {
    "check-expired-users": "检查过期用户",
    "custom-collections": "刷新自建合集",
    "execute-auto-tagging-rules": "执行自动打标",
    "fill-studio-images": "补全工作室图标",
    "generate-all-covers": "生成原生封面",
    "generate-custom-collection-covers": "生成合集封面",
    "generate_embeddings": "生成媒体向量",
    "process-single-custom-collection": "生成单个自建合集",
    "process_all_custom_collections": "生成所有自建合集",
    REVIEW_REPROCESS_ALL_TASK_KEY: "批量重新处理待复核项",
    REVIEW_REPROCESS_ONE_TASK_KEY: "重新处理待复核项",
    "refresh-collections": "刷新原生合集",
    "scan-cleanup-issues": "扫描重复媒体",
    "sync-all-user-data": "同步用户数据",
    "system-auto-update": "系统自动更新",
    "update-daily-theme": "更新每日主题",
}

_LEGACY_HANDLERS = {
    "check-expired-users": ("tasks.users", "task_check_expired_users"),
    "custom-collections": ("tasks.custom_collections", "task_process_all_custom_collections"),
    "execute-auto-tagging-rules": ("tasks.media", "task_execute_auto_tagging_rules"),
    "fill-studio-images": ("tasks.media", "task_fill_studio_images"),
    "generate-all-covers": ("tasks.covers", "task_generate_all_covers"),
    "generate-custom-collection-covers": ("tasks.covers", "task_generate_all_custom_collection_covers"),
    "generate_embeddings": ("tasks.vector_tasks", "task_generate_embeddings"),
    "process-single-custom-collection": ("tasks.custom_collections", "process_single_custom_collection"),
    "process_all_custom_collections": ("tasks.custom_collections", "task_process_all_custom_collections"),
    "refresh-collections": ("tasks.tmdb_collections", "task_refresh_collections"),
    "scan-cleanup-issues": ("tasks.cleanup", "task_scan_for_cleanup_issues"),
    "update-daily-theme": ("tasks.discover", "task_update_daily_theme"),
    REVIEW_REPROCESS_ONE_TASK_KEY: ("tasks.media", "task_reprocess_single_item"),
    REVIEW_REPROCESS_ALL_TASK_KEY: ("tasks.media", "task_reprocess_all_review_items"),
}


class P8TaskProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...

    def success_reported(self) -> bool: ...


class P8EffectsCompatibilityGateway(Protocol):
    def execute(
        self,
        task_key: str,
        processor: object,
        parameters: Mapping[str, Any],
    ) -> object: ...


class LegacyP8EffectsCompatibilityGateway:
    """Worker-only allowlist around established P8 task implementations."""

    def execute(
        self,
        task_key: str,
        processor: object,
        parameters: Mapping[str, Any],
    ) -> object:
        values = dict(parameters)
        target = _LEGACY_HANDLERS[task_key]
        if task_key == "execute-auto-tagging-rules" and values:
            operation = values.pop("operation")
            target = (
                ("tasks.media", "task_bulk_auto_tag")
                if operation == "add"
                else ("tasks.media", "task_bulk_remove_tags")
            )
        module_name, function_name = target
        handler = getattr(import_module(module_name), function_name)
        if not callable(handler):
            raise TypeError(f"P8 compatibility target {module_name}.{function_name} is not callable")
        return handler(processor, **values)


class P8EffectTaskRouter:
    def __init__(
        self,
        compatibility: P8EffectsCompatibilityGateway | None,
        user_sync: UserSyncService,
    ) -> None:
        self._compatibility = compatibility
        self._user_sync = user_sync

    def execute(
        self,
        message: TaskMessage,
        processor: object | None,
        progress: P8TaskProgress,
    ) -> TaskOutcome:
        if not isinstance(message, TaskMessage):
            raise TypeError("P8 effect task router requires a TaskMessage")
        if message.task_key not in P8_NATIVE_TASK_KEYS:
            raise LookupError(f"unknown P8 task key: {message.task_key}")
        if message.processor is not ProcessorIdentity.MEDIA:
            raise ValueError(f"task {message.task_key!r} requires the media processor")
        if message.payload.version != 1:
            raise ValueError(f"task {message.task_key!r} payload version is not supported")
        payload = TaskPayload.create(message.payload.data, version=message.payload.version).data
        parameters = _normalize_parameters(message.task_key, payload)

        if message.task_key == "system-auto-update":
            return TaskOutcome.skipped(
                "自动容器更新在明文主线中已禁用；请使用受控发布流程更新系统。",
                _details(message.task_key, compatibility=False),
            )
        if message.task_key == "sync-all-user-data":
            outcome = self._user_sync.execute(progress)
            return TaskOutcome(
                outcome.status,
                outcome.message,
                {**outcome.details, **_details(message.task_key, compatibility=False)},
            )
        if processor is None:
            raise RuntimeError(f"P8 task {message.task_key!r} media processor is unavailable")
        if self._compatibility is None:
            raise RuntimeError(f"P8 task {message.task_key!r} compatibility execution is unavailable")
        result = self._compatibility.execute(message.task_key, processor, parameters)
        return _explicit_outcome(message.task_key, result, progress)

    def execute_review(
        self,
        task_key: str,
        parameters: Mapping[str, Any],
        processor: object | None,
        progress: P8TaskProgress,
    ) -> TaskOutcome:
        if task_key not in P8_REVIEW_TASK_KEYS:
            raise LookupError(f"unknown P8 review task key: {task_key}")
        if processor is None:
            raise RuntimeError(f"P8 review task {task_key!r} media processor is unavailable")
        if self._compatibility is None:
            raise RuntimeError(f"P8 review task {task_key!r} compatibility execution is unavailable")
        normalized = _normalize_review_parameters(task_key, parameters)
        result = self._compatibility.execute(task_key, processor, normalized)
        return _explicit_outcome(task_key, result, progress)


def _normalize_parameters(task_key: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    if task_key == "process-single-custom-collection":
        if set(payload) != {"custom_collection_id"}:
            raise ValueError("process-single-custom-collection requires exactly custom_collection_id")
        value = payload["custom_collection_id"]
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError("custom_collection_id must be a positive integer")
        return {"custom_collection_id": value}
    if task_key == "execute-auto-tagging-rules" and payload:
        allowed = {"operation", "library_ids", "tags", "rating_filters"}
        extras = set(payload) - allowed
        if extras:
            raise ValueError(f"auto-tagging operation contains unsupported fields: {sorted(extras)!r}")
        operation = str(payload.get("operation") or "").strip().lower()
        if operation not in {"add", "remove"}:
            raise ValueError("auto-tagging operation must be add or remove")
        tags = _optional_string_list(payload.get("tags"), field="tags")
        if operation == "add" and not tags:
            raise ValueError("auto-tagging add operation requires tags")
        return {
            "operation": operation,
            "library_ids": _optional_string_list(payload.get("library_ids"), field="library_ids"),
            "tags": tags,
            "rating_filters": _optional_string_list(payload.get("rating_filters"), field="rating_filters"),
        }
    if payload:
        raise ValueError(f"task {task_key!r} does not accept parameters")
    return {}


def _optional_string_list(value: Any, *, field: str) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise TypeError(f"{field} must be a list")
    normalized = [str(item).strip() for item in value if str(item).strip()]
    if len(normalized) != len(set(normalized)):
        normalized = list(dict.fromkeys(normalized))
    return normalized


def _normalize_review_parameters(task_key: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    values = dict(payload)
    if task_key == REVIEW_REPROCESS_ONE_TASK_KEY:
        if set(values) != {"item_id", "item_name_for_ui", "failure_reason"}:
            raise ValueError("review item operation has an invalid parameter envelope")
        item_id = _required_text(values["item_id"], field="item_id", limit=200)
        item_name = _required_text(values["item_name_for_ui"], field="item_name_for_ui", limit=500)
        reason = _optional_text(values["failure_reason"], field="failure_reason", limit=1000)
        return {"item_id": item_id, "item_name_for_ui": item_name, "failure_reason": reason}
    if set(values) != {"reason_filter"}:
        raise ValueError("review batch operation has an invalid parameter envelope")
    return {"reason_filter": _optional_text(values["reason_filter"], field="reason_filter", limit=1000)}


def _required_text(value: Any, *, field: str, limit: int) -> str:
    normalized = _optional_text(value, field=field, limit=limit)
    if normalized is None:
        raise ValueError(f"{field} must not be empty")
    return normalized


def _optional_text(value: Any, *, field: str, limit: int) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    normalized = value.strip()
    if len(normalized) > limit:
        raise ValueError(f"{field} is too long")
    return normalized or None


def _explicit_outcome(
    task_key: str,
    result: object,
    progress: P8TaskProgress,
) -> TaskOutcome:
    details = _details(task_key, compatibility=True)
    if isinstance(result, TaskOutcome):
        return TaskOutcome(result.status, result.message, {**result.details, **details})
    if isinstance(result, Mapping):
        value = normalize_json_object(dict(result), field=f"{task_key} compatibility result")
        message = str(value.get("message") or f"{_DISPLAY_NAMES[task_key]}完成。")
        status = str(value.get("status") or "").strip().lower()
        outcome_details = {**details, "legacy_result": value}
        if value.get("ok") is False or status in {TaskOutcomeStatus.FAILED.value, "indeterminate"}:
            return TaskOutcome.failed(message, outcome_details)
        if value.get("skipped") is True or status == TaskOutcomeStatus.SKIPPED.value:
            return TaskOutcome.skipped(message, outcome_details)
        if status == TaskOutcomeStatus.PARTIAL.value:
            return TaskOutcome.partial(message, outcome_details)
        return TaskOutcome.succeeded(message, outcome_details)
    if result is False:
        return TaskOutcome.failed(f"{_DISPLAY_NAMES[task_key]}报告失败。", details)
    if result is True:
        return TaskOutcome.succeeded(f"{_DISPLAY_NAMES[task_key]}完成。", details)
    if result is None and progress.success_reported():
        return TaskOutcome.succeeded(
            f"{_DISPLAY_NAMES[task_key]}完成，并报告了终态进度。",
            details,
        )
    return TaskOutcome.failed(
        f"{_DISPLAY_NAMES[task_key]}未返回可判定结果，也未报告终态成功。",
        {**details, "legacy_result_type": type(result).__name__},
    )


def _details(task_key: str, *, compatibility: bool) -> dict[str, str | bool]:
    return {
        "task_key": task_key,
        "native_handler": "P8EffectTaskRouter",
        "compatibility_fallback": compatibility,
        "fallback_retirement_stage": P8_COMPATIBILITY_FALLBACK_UNTIL if compatibility else "not_applicable",
    }


__all__ = [
    "LegacyP8EffectsCompatibilityGateway",
    "P8_COMPATIBILITY_FALLBACK_UNTIL",
    "P8EffectTaskRouter",
    "P8EffectsCompatibilityGateway",
    "P8TaskProgress",
]
