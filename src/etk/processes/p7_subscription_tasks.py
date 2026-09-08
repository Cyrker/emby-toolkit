from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol
from uuid import UUID

from etk.domains.subscription.assistant import SubscribeAssistantMaintenanceService
from etk.domains.subscription.auto_subscribe import AutoSubscribeService, AutoSubscriptionExecution
from etk.domains.subscription.manual_batch import ManualSubscriptionBatchService
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcome, TaskOutcomeStatus, TaskPayload
from etk.platform.tasks.catalog import P7_NATIVE_TASK_KEYS

from .p7_subscription_effects import bind_moviepilot_execution

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

WATCHLIST_TASK_KEYS = frozenset(
    {
        "process-watchlist",
        "refresh_completed_series",
        "add-all-series-to-watchlist",
    }
)
RESUBSCRIBE_TASK_KEYS = frozenset({"resubscribe-library", "update-resubscribe-cache"})


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


class P7PeopleTaskExecution(Protocol):
    def execute(
        self,
        task_key: str,
        progress: P7TaskProgress,
        parameters: Mapping[str, Any],
    ) -> TaskOutcome: ...


class P7NativeTaskExecution(Protocol):
    def execute(
        self,
        task_key: str,
        progress: P7TaskProgress,
        parameters: Mapping[str, Any],
    ) -> TaskOutcome: ...


class P7SubscriptionTaskRouter:
    def __init__(
        self,
        assistant: SubscribeAssistantMaintenanceService | None,
        moviepilot_effects: P7MoviePilotEffects | None = None,
        washing_recalculation: P7WashingRecalculation | None = None,
        old_season_backfill: P7OldSeasonBackfill | None = None,
        manual_batch: ManualSubscriptionBatchService | None = None,
        auto_subscribe: AutoSubscribeService | None = None,
        people_tasks: P7PeopleTaskExecution | None = None,
        watchlist_tasks: P7NativeTaskExecution | None = None,
        resubscribe_tasks: P7NativeTaskExecution | None = None,
        resubscribe_cache: P7NativeTaskExecution | None = None,
    ) -> None:
        self._assistant = assistant
        self._moviepilot_effects = moviepilot_effects
        self._washing_recalculation = washing_recalculation
        self._old_season_backfill = old_season_backfill
        self._manual_batch = manual_batch
        self._auto_subscribe = auto_subscribe
        self._people_tasks = people_tasks
        self._watchlist_tasks = watchlist_tasks
        self._resubscribe_tasks = resubscribe_tasks
        self._resubscribe_cache = resubscribe_cache

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
        if processor is not None:
            raise ValueError("P7 native task router does not accept a legacy processor")
        payload = TaskPayload.create(message.payload.data, version=message.payload.version).data
        parameters = _normalize_parameters(message.task_key, payload)
        if (
            message.task_key
            in {
                "actor-tracking",
                "enrich-aliases",
                "merge-duplicate-actors",
                "purge-ghost-actors",
            }
            and self._people_tasks is not None
        ):
            result = self._people_tasks.execute(message.task_key, progress, parameters)
            return _native_outcome(message.task_key, result, self._people_tasks)
        if message.task_key == "update-resubscribe-cache" and self._resubscribe_cache is not None:
            result = self._resubscribe_cache.execute(message.task_key, progress, parameters)
            return _native_outcome(message.task_key, result, self._resubscribe_cache)
        if message.task_key in WATCHLIST_TASK_KEYS:
            if self._watchlist_tasks is None:
                raise RuntimeError(f"P7 task {message.task_key!r} native owner is unavailable")
            if self._moviepilot_effects is None:
                result = self._watchlist_tasks.execute(message.task_key, progress, parameters)
            else:
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
                    result = self._watchlist_tasks.execute(message.task_key, progress, parameters)
            return _native_outcome(message.task_key, result, self._watchlist_tasks)
        if message.task_key in RESUBSCRIBE_TASK_KEYS:
            if self._resubscribe_tasks is None:
                raise RuntimeError(f"P7 task {message.task_key!r} native owner is unavailable")
            result = self._resubscribe_tasks.execute(message.task_key, progress, parameters)
            return _native_outcome(message.task_key, result, self._resubscribe_tasks)
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
                    "native_handler": type(self._assistant).__name__,
                    "compatibility_fallback": False,
                    "durable_state": True,
                    "completed_phases": list(run.completed_phases),
                    "stats": run.stats,
                },
            )
        raise RuntimeError(f"P7 task {message.task_key!r} native owner is unavailable")


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
    if task_key == "actor-tracking" and "actor_name" in values:
        actor_name = str(values["actor_name"] or "").strip()
        if not actor_name:
            raise ValueError("actor-tracking actor_name must be a non-empty string")
        values["actor_name"] = actor_name
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


def _native_outcome(task_key: str, result: TaskOutcome, owner: object) -> TaskOutcome:
    return TaskOutcome(
        result.status,
        result.message,
        {
            **result.details,
            "task_key": task_key,
            "native_handler": type(owner).__name__,
            "compatibility_fallback": False,
        },
    )


__all__ = [
    "P7PeopleTaskExecution",
    "P7NativeTaskExecution",
    "P7SubscriptionTaskRouter",
    "P7TaskProgress",
]
