from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import nullcontext
from importlib import import_module
from typing import Any, Protocol
from uuid import UUID

from etk.domains.subscription.assistant import (
    ASSISTANT_MAINTENANCE_PHASES,
    ASSISTANT_SNAPSHOT_CLAIM_LEASE_SECONDS,
    AssistantMaintenanceSession,
    SubscribeAssistantMaintenanceService,
)
from etk.domains.subscription.auto_subscribe import AutoSubscribeService, AutoSubscriptionExecution
from etk.domains.subscription.manual_batch import ManualSubscriptionBatchService
from etk.platform.jobs import normalize_json_object
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcome, TaskOutcomeStatus, TaskPayload
from etk.platform.tasks.catalog import P7_NATIVE_TASK_KEYS

from .p7_subscription_effects import bind_moviepilot_execution

P7_COMPATIBILITY_FALLBACK_UNTIL = "P11"

_DISPLAY_NAMES = {
    "enrich-aliases": "演员数据补充",
    "process-watchlist": "刷新智能追剧",
    "subscribe-assistant-maintenance": "订阅助手巡检",
    "actor-tracking": "刷新演员订阅",
    "auto-subscribe": "统一订阅处理",
    "refresh_completed_series": "已完结剧集复活检查",
    "add-all-series-to-watchlist": "一键扫描库内剧集",
    "resubscribe-library": "媒体订阅删除",
    "manual_subscribe_batch": "批量订阅",
    "scan_old_seasons_backfill": "扫描缺季的剧",
    "update-resubscribe-cache": "刷新媒体整理",
    "merge-duplicate-actors": "合并重复演员",
    "purge-ghost-actors": "清理幽灵演员",
    "recalculate_library_washing_priorities": "重新计算洗版优先级",
}

_LEGACY_HANDLERS = {
    "enrich-aliases": ("tasks.actors", "task_enrich_aliases"),
    "process-watchlist": ("tasks.watchlist", "task_process_watchlist"),
    "actor-tracking": ("tasks.actors", "task_process_actor_subscriptions"),
    "auto-subscribe": ("tasks.subscriptions", "task_auto_subscribe"),
    "refresh_completed_series": ("tasks.watchlist", "task_refresh_completed_series"),
    "add-all-series-to-watchlist": ("tasks.watchlist", "task_add_all_series_to_watchlist"),
    "resubscribe-library": ("tasks.resubscribe", "task_resubscribe_library"),
    "manual_subscribe_batch": ("tasks.subscriptions", "task_manual_subscribe_batch"),
    "update-resubscribe-cache": ("tasks.resubscribe", "task_update_resubscribe_cache"),
    "merge-duplicate-actors": ("tasks.actors", "task_merge_duplicate_actors"),
    "purge-ghost-actors": ("tasks.actors", "task_purge_ghost_actors"),
    "recalculate_library_washing_priorities": ("tasks.p115", "task_recalculate_library_washing_priorities"),
}

_EXPECTED_PROCESSORS = {
    "process-watchlist": ProcessorIdentity.WATCHLIST,
    "subscribe-assistant-maintenance": ProcessorIdentity.WATCHLIST,
    "refresh_completed_series": ProcessorIdentity.WATCHLIST,
    "add-all-series-to-watchlist": ProcessorIdentity.WATCHLIST,
    "scan_old_seasons_backfill": ProcessorIdentity.WATCHLIST,
    "actor-tracking": ProcessorIdentity.ACTOR,
}

_ALLOWED_PARAMETERS = {
    "enrich-aliases": frozenset({"force_full_update"}),
    "process-watchlist": frozenset({"tmdb_id", "item_name", "new_episode_ids", "subscription_triggering_episode_ids"}),
    "subscribe-assistant-maintenance": frozenset(),
    "actor-tracking": frozenset({"subscription_id", "actor_name"}),
    "auto-subscribe": frozenset(),
    "refresh_completed_series": frozenset(),
    "add-all-series-to-watchlist": frozenset(),
    "resubscribe-library": frozenset(),
    "manual_subscribe_batch": frozenset({"subscribe_requests"}),
    "scan_old_seasons_backfill": frozenset(),
    "update-resubscribe-cache": frozenset(),
    "merge-duplicate-actors": frozenset(),
    "purge-ghost-actors": frozenset(),
    "recalculate_library_washing_priorities": frozenset({"item_type", "limit", "affected_scope", "scope_revision"}),
}


class P7SubscriptionCompatibilityGateway(Protocol):
    def execute(
        self,
        task_key: str,
        processor: object,
        parameters: Mapping[str, Any],
    ) -> object: ...


class P7TaskProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


class P7MoviePilotEffects(Protocol):
    def create_subscription(
        self,
        payload: Mapping[str, Any],
        *,
        consume_quota: bool,
        apply_local_continuation: bool = True,
    ) -> bool: ...

    def delete_download_tasks(
        self,
        hashes: Sequence[str],
        *,
        operation_occurrence: str | None = None,
    ) -> bool: ...

    def delete_transfer_history(
        self,
        record: Mapping[str, Any],
        *,
        operation_occurrence: str | None = None,
    ) -> bool: ...


class P7ResubscribeCleanup(Protocol):
    def stage_version_deletion(self, emby_id: str) -> Mapping[str, Any]: ...


class P7WashingRecalculation(Protocol):
    def execute(
        self,
        progress: P7TaskProgress,
        *,
        item_type: object = "all",
        limit: object = None,
        affected_scope: object = None,
        scope_revision: object = None,
    ) -> Any: ...


class P7OldSeasonBackfill(Protocol):
    def execute(self, progress: P7TaskProgress) -> Any: ...


class LegacyP7SubscriptionCompatibilityGateway:
    """Worker-only P11 seam around the remaining established P7 task functions."""

    def execute(
        self,
        task_key: str,
        processor: object,
        parameters: Mapping[str, Any],
    ) -> object:
        values = dict(parameters)
        if task_key == "actor-tracking" and "subscription_id" in values:
            module_name, function_name = "tasks.actors", "task_scan_actor_media"
            values = {"subscription_id": values["subscription_id"]}
        else:
            module_name, function_name = _LEGACY_HANDLERS[task_key]
            if task_key == "process-watchlist":
                values.pop("item_name", None)
            elif task_key == "actor-tracking":
                values.pop("actor_name", None)
        handler = getattr(import_module(module_name), function_name)
        if not callable(handler):
            raise TypeError(f"P7 compatibility target {module_name}.{function_name} is not callable")
        return handler(processor, **values)


class LegacySubscribeAssistantSession:
    def __init__(
        self,
        processor: object,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
    ) -> None:
        manager_module = import_module("services.subscribe_assistant.manager")
        self._store = import_module("services.subscribe_assistant.store")
        self._job_id = job_id
        self._attempt_id = attempt_id
        self._worker_id = worker_id
        config = getattr(processor, "config", None)
        if not isinstance(config, Mapping):
            raise RuntimeError("subscribe assistant maintenance requires processor config")
        self._manager = manager_module.SubscribeAssistantManager(dict(config))

    def phases(self) -> Sequence[str]:
        phases: list[str] = list(ASSISTANT_MAINTENANCE_PHASES[:6])
        if self._manager.cfg.download_monitor_enabled:
            phases.append("download_check")
        if self._manager.cfg.verify_enabled:
            phases.append("snapshot_verify")
        return tuple(phases)

    def run_phase(self, phase: str, checkpoint: Callable[[], None]) -> int:
        checkpoint()
        if phase == "delete_records_cleanup":
            return int(self._store.cleanup_delete_records())
        if phase == "snapshot_cleanup":
            return int(self._store.cleanup_snapshots(self._manager.cfg.snapshot_retention_days))
        handlers = {
            "washing_timeout": self._manager.run_full_washing_timeout_check,
            "history_reconcile": self._manager.run_transfer_history_reconcile,
            "moviepilot_subscription_sync": self._manager.run_moviepilot_subscription_sync,
            "no_download_check": self._manager.run_no_download_check,
            "download_check": self._manager.run_download_check,
            "snapshot_verify": lambda: self._manager.run_snapshot_verify(
                job_id=self._job_id,
                attempt_id=self._attempt_id,
                worker_id=self._worker_id,
                lease_seconds=ASSISTANT_SNAPSHOT_CLAIM_LEASE_SECONDS,
                checkpoint=checkpoint,
            ),
        }
        try:
            handler = handlers[phase]
        except KeyError:
            raise LookupError(f"unknown subscribe assistant maintenance phase: {phase}") from None
        return max(int(handler()), 0)


class LegacySubscribeAssistantMaintenanceGateway:
    def begin(
        self,
        processor: object,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
    ) -> AssistantMaintenanceSession:
        return LegacySubscribeAssistantSession(
            processor,
            job_id=job_id,
            attempt_id=attempt_id,
            worker_id=worker_id,
        )


class P7SubscriptionTaskRouter:
    def __init__(
        self,
        compatibility: P7SubscriptionCompatibilityGateway | None,
        assistant: SubscribeAssistantMaintenanceService | None,
        moviepilot_effects: P7MoviePilotEffects | None = None,
        resubscribe_cleanup: P7ResubscribeCleanup | None = None,
        washing_recalculation: P7WashingRecalculation | None = None,
        old_season_backfill: P7OldSeasonBackfill | None = None,
        manual_batch: ManualSubscriptionBatchService | None = None,
        auto_subscribe: AutoSubscribeService | None = None,
    ) -> None:
        self._compatibility = compatibility
        self._assistant = assistant
        self._moviepilot_effects = moviepilot_effects
        self._resubscribe_cleanup = resubscribe_cleanup
        self._washing_recalculation = washing_recalculation
        self._old_season_backfill = old_season_backfill
        self._manual_batch = manual_batch
        self._auto_subscribe = auto_subscribe

    def execute(
        self,
        message: TaskMessage,
        processor: object | None,
        progress: P7TaskProgress,
    ) -> TaskOutcome:
        if not isinstance(message, TaskMessage):
            raise TypeError("P7 subscription task router requires a TaskMessage")
        if message.task_key not in P7_NATIVE_TASK_KEYS:
            raise LookupError(f"unknown P7 subscription task key: {message.task_key}")
        expected = _EXPECTED_PROCESSORS.get(message.task_key, ProcessorIdentity.MEDIA)
        if message.processor is not expected:
            raise ValueError(f"task {message.task_key!r} requires the {expected.value} processor identity")
        if message.payload.version != 1:
            raise ValueError(f"task {message.task_key!r} payload version is not supported")
        payload = TaskPayload.create(message.payload.data, version=message.payload.version).data
        parameters = _normalize_parameters(message.task_key, payload)
        if message.task_key == "auto-subscribe":
            if self._auto_subscribe is None:
                raise RuntimeError("P7 auto-subscribe use case is unavailable")
            job_id = _metadata_uuid(message.metadata, "job_id")
            attempt_id = _metadata_uuid(message.metadata, "attempt_id")
            worker_id = _metadata_text(message.metadata, "worker_id")
            with bind_moviepilot_execution(
                job_id=str(job_id),
                attempt_id=str(attempt_id),
                worker_id=worker_id,
                task_key=message.task_key,
                checkpoint=progress.checkpoint,
            ):
                run = self._auto_subscribe.execute(
                    progress,
                    AutoSubscriptionExecution(
                        str(job_id),
                        str(attempt_id),
                        worker_id,
                        lease_seconds=float(getattr(progress, "lease_seconds", 1_800)),
                    ),
                )
            details = {
                "task_key": message.task_key,
                "native_handler": type(self).__name__,
                "compatibility_fallback": False,
                **run.details(),
            }
            if run.status == TaskOutcomeStatus.PARTIAL.value:
                return TaskOutcome.partial(run.message, details)
            if run.status == TaskOutcomeStatus.FAILED.value:
                return TaskOutcome.failed(run.message, details)
            return TaskOutcome.succeeded(run.message, details)
        if message.task_key == "manual_subscribe_batch":
            if self._manual_batch is None:
                raise RuntimeError("P7 manual subscription batch use case is unavailable")
            job_id = _metadata_uuid(message.metadata, "job_id")
            attempt_id = _metadata_uuid(message.metadata, "attempt_id")
            worker_id = _metadata_text(message.metadata, "worker_id")
            with bind_moviepilot_execution(
                job_id=str(job_id),
                attempt_id=str(attempt_id),
                worker_id=worker_id,
                task_key=message.task_key,
                checkpoint=progress.checkpoint,
            ):
                run = self._manual_batch.execute(parameters["subscribe_requests"], progress)
            details = {
                "task_key": message.task_key,
                "native_handler": type(self).__name__,
                "compatibility_fallback": False,
                **run.details(),
            }
            if run.status == TaskOutcomeStatus.PARTIAL.value:
                return TaskOutcome.partial(run.message, details)
            if run.status == TaskOutcomeStatus.FAILED.value:
                return TaskOutcome.failed(run.message, details)
            return TaskOutcome.succeeded(run.message, details)
        if message.task_key == "recalculate_library_washing_priorities":
            if self._washing_recalculation is None:
                raise RuntimeError("P7 washing-priority recalculation use case is unavailable")
            result = self._washing_recalculation.execute(progress, **parameters)
            details = {
                "task_key": message.task_key,
                "native_handler": type(self).__name__,
                "compatibility_fallback": False,
                "stats": dict(result.stats),
            }
            if result.partial:
                return TaskOutcome.partial("洗版优先级重算部分完成。", details)
            return TaskOutcome.succeeded("洗版优先级重算完成。", details)
        if message.task_key == "scan_old_seasons_backfill":
            if self._old_season_backfill is None:
                raise RuntimeError("P7 old-season backfill use case is unavailable")
            result = self._old_season_backfill.execute(progress)
            return TaskOutcome.succeeded(
                result.message,
                {
                    "task_key": message.task_key,
                    "native_handler": type(self).__name__,
                    "compatibility_fallback": False,
                    "candidate_count": result.candidate_count,
                    "affected_count": result.affected_count,
                },
            )
        if processor is None:
            raise RuntimeError(f"P7 task {message.task_key!r} processor is unavailable")
        if self._moviepilot_effects is None:
            effects_context = nullcontext()
        else:
            from handler.moviepilot import persistent_moviepilot_effects

            effects_context = persistent_moviepilot_effects(self._moviepilot_effects)
        if self._resubscribe_cleanup is None:
            cleanup_context = nullcontext()
        else:
            cleanup_module = import_module("tasks.resubscribe")
            cleanup_context = cleanup_module.persistent_resubscribe_cleanup(self._resubscribe_cleanup)
        with effects_context, cleanup_context:
            if message.task_key == "subscribe-assistant-maintenance":
                if self._assistant is None:
                    raise RuntimeError("P7 subscribe-assistant maintenance use case is unavailable")
                run = self._assistant.execute(
                    job_id=_metadata_uuid(message.metadata, "job_id"),
                    attempt_id=_metadata_uuid(message.metadata, "attempt_id"),
                    worker_id=_metadata_text(message.metadata, "worker_id"),
                    processor=processor,
                    progress=progress,
                )
                return TaskOutcome.succeeded(
                    "订阅助手巡检完成。",
                    {
                        "task_key": message.task_key,
                        "native_handler": type(self).__name__,
                        "compatibility_fallback": False,
                        "durable_state": True,
                        "completed_phases": list(run.completed_phases),
                        "stats": run.stats,
                    },
                )
            if self._compatibility is None:
                raise RuntimeError(f"P7 task {message.task_key!r} compatibility owner is unavailable")
            result = self._compatibility.execute(message.task_key, processor, parameters)
            return _explicit_outcome(message.task_key, result)


def _normalize_parameters(task_key: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    extras = set(payload) - _ALLOWED_PARAMETERS[task_key]
    if extras:
        raise ValueError(f"task {task_key!r} contains unsupported parameters: {sorted(extras)!r}")
    values = dict(payload)
    if task_key == "enrich-aliases" and "force_full_update" in values:
        if not isinstance(values["force_full_update"], bool):
            raise TypeError("enrich-aliases force_full_update must be boolean")
    if task_key == "actor-tracking" and "subscription_id" in values:
        value = values["subscription_id"]
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError("actor-tracking subscription_id must be a positive integer")
    if task_key == "manual_subscribe_batch":
        requests = values.get("subscribe_requests")
        if not isinstance(requests, list) or not all(isinstance(item, Mapping) for item in requests):
            raise TypeError("manual_subscribe_batch subscribe_requests must be a list of objects")
        values["subscribe_requests"] = [dict(item) for item in requests]
    return values


def _metadata_uuid(values: Mapping[str, Any], field: str) -> UUID:
    raw = values.get(field)
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"task metadata field {field!r} must be a UUID string")
    try:
        return UUID(raw.strip())
    except ValueError as exc:
        raise ValueError(f"task metadata field {field!r} must be a UUID string") from exc


def _metadata_text(values: Mapping[str, Any], field: str) -> str:
    value = str(values.get(field) or "").strip()
    if not value:
        raise ValueError(f"task metadata field {field!r} must be a non-empty string")
    return value


def _explicit_outcome(task_key: str, result: object) -> TaskOutcome:
    details = {
        "task_key": task_key,
        "native_handler": "P7SubscriptionTaskRouter",
        "compatibility_fallback": True,
        "fallback_retirement_stage": P7_COMPATIBILITY_FALLBACK_UNTIL,
    }
    if isinstance(result, TaskOutcome):
        return TaskOutcome(result.status, result.message, {**result.details, **details})
    if result is None or result is True:
        return TaskOutcome.succeeded(f"{_DISPLAY_NAMES[task_key]}兼容处理完成，未报告失败。", details)
    if result is False:
        return TaskOutcome.failed(f"{_DISPLAY_NAMES[task_key]}兼容处理返回失败。", details)
    if isinstance(result, Mapping):
        legacy_result = normalize_json_object(dict(result), field=f"{task_key} compatibility result")
        outcome_details = {**details, "legacy_result": legacy_result}
        message = str(legacy_result.get("message") or f"{_DISPLAY_NAMES[task_key]}兼容处理完成。")
        status = str(legacy_result.get("status") or "").strip().lower()
        if legacy_result.get("skipped") is True or status in {TaskOutcomeStatus.SKIPPED.value, "not_applicable"}:
            return TaskOutcome.skipped(message, outcome_details)
        if status == "deferred":
            return TaskOutcome.skipped(message, outcome_details)
        if status == TaskOutcomeStatus.PARTIAL.value:
            return TaskOutcome.partial(message, outcome_details)
        if task_key == "recalculate_library_washing_priorities" and int(legacy_result.get("errors") or 0) > 0:
            return TaskOutcome.partial(message, outcome_details)
        if legacy_result.get("ok") is False or status in {TaskOutcomeStatus.FAILED.value, "indeterminate"}:
            return TaskOutcome.failed(message, outcome_details)
        return TaskOutcome.succeeded(message, outcome_details)
    return TaskOutcome.succeeded(
        f"{_DISPLAY_NAMES[task_key]}兼容处理完成，未报告失败。",
        {**details, "legacy_result_type": type(result).__name__},
    )


__all__ = [
    "LegacyP7SubscriptionCompatibilityGateway",
    "LegacySubscribeAssistantMaintenanceGateway",
    "P7_COMPATIBILITY_FALLBACK_UNTIL",
    "P7SubscriptionCompatibilityGateway",
    "P7ResubscribeCleanup",
    "P7SubscriptionTaskRouter",
    "P7TaskProgress",
]
