from __future__ import annotations

from collections.abc import Mapping
from importlib import import_module
from typing import Any, Protocol
from uuid import UUID

from etk.platform.jobs import normalize_json_object
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcome, TaskOutcomeStatus, TaskPayload
from etk.platform.tasks.catalog import P5_NATIVE_TASK_KEYS

P5_COMPATIBILITY_FALLBACK_UNTIL = "P11"

_DISPLAY_NAMES = {
    "populate-metadata": "同步媒体数据",
    "backfill-media-metadata": "补齐媒体元数据",
    "role-translation": "中文化角色名",
    "actor-translation": "中文化人物名",
    "restore_mediainfo": "重建媒体信息",
    "restore-nfo-and-images": "补齐视频截图",
}

_LEGACY_HANDLERS = {
    "populate-metadata": ("tasks.media", "task_populate_metadata_cache"),
    "backfill-media-metadata": ("tasks.media", "task_backfill_media_metadata"),
    "role-translation": ("tasks.media", "task_role_translation"),
    "actor-translation": ("tasks.actors", "task_persons_translation"),
    "restore-nfo-and-images": ("tasks.media", "task_fill_missing_video_screenshots"),
}

_TARGETED_BACKFILL_HANDLER = ("tasks.media", "task_backfill_media_metadata_batch")

_ALLOWED_PARAMETERS = {
    "populate-metadata": frozenset({"batch_size", "force_full_update", "metadata_backfill_only"}),
    "backfill-media-metadata": frozenset({"targets"}),
    "role-translation": frozenset({"force_full_update"}),
    "actor-translation": frozenset(),
    "restore_mediainfo": frozenset({"force_full_update"}),
    "restore-nfo-and-images": frozenset(),
}


class P5MediaCompatibilityGateway(Protocol):
    def execute(
        self,
        task_key: str,
        processor: object,
        parameters: Mapping[str, Any],
    ) -> object: ...


class P5MediaTaskProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


class P5MediaInfoRestoreUseCase(Protocol):
    def execute(
        self,
        progress: P5MediaTaskProgress,
        *,
        operation_key: str,
        force_full_update: bool = False,
    ) -> TaskOutcome: ...


class LegacyP5MediaCompatibilityGateway:
    """Worker-only P11 fallback for P5 tasks whose use cases are still legacy callables."""

    def execute(
        self,
        task_key: str,
        processor: object,
        parameters: Mapping[str, Any],
    ) -> object:
        if task_key == "restore_mediainfo":
            raise LookupError("task 'restore_mediainfo' is native-only and cannot use the compatibility gateway")
        values = dict(parameters)
        handler_target = _LEGACY_HANDLERS[task_key]
        if task_key == "backfill-media-metadata" and "targets" in values:
            handler_target = _TARGETED_BACKFILL_HANDLER
        module_name, function_name = handler_target
        handler = getattr(import_module(module_name), function_name)
        if not callable(handler):
            raise TypeError(f"P5 compatibility target {module_name}.{function_name} is not callable")
        return handler(processor, **values)


class P5MediaTaskRouter:
    """Validate P5 task messages and return an explicit outcome around the P11 fallback."""

    def __init__(
        self,
        compatibility: P5MediaCompatibilityGateway | None,
        restore_mediainfo: P5MediaInfoRestoreUseCase | None = None,
    ) -> None:
        self._compatibility = compatibility
        self._restore_mediainfo = restore_mediainfo

    def execute(
        self,
        message: TaskMessage,
        processor: object | None,
        progress: P5MediaTaskProgress | None = None,
    ) -> TaskOutcome:
        if not isinstance(message, TaskMessage):
            raise TypeError("P5 media task router requires a TaskMessage")
        if message.task_key not in P5_NATIVE_TASK_KEYS:
            raise LookupError(f"unknown P5 media task key: {message.task_key}")
        if message.processor is not ProcessorIdentity.MEDIA:
            raise ValueError(f"task {message.task_key!r} requires the media processor")
        if message.payload.version != 1:
            raise ValueError(f"task {message.task_key!r} payload version is not supported")
        payload = TaskPayload.create(message.payload.data, version=message.payload.version).data
        parameters = _normalize_parameters(message.task_key, payload)
        if message.task_key == "restore_mediainfo":
            if self._restore_mediainfo is None:
                raise RuntimeError("native MediaInfo restore use case is unavailable")
            if progress is None:
                raise RuntimeError("native MediaInfo restore progress is unavailable")
            job_id = _metadata_uuid(message.metadata, "job_id")
            result = self._restore_mediainfo.execute(
                progress,
                operation_key=f"restore_mediainfo:{job_id}",
                force_full_update=bool(parameters.get("force_full_update", False)),
            )
            return TaskOutcome(
                result.status,
                result.message,
                {
                    **result.details,
                    "task_key": message.task_key,
                    "native_handler": type(self._restore_mediainfo).__name__,
                    "envelope_owner": type(self).__name__,
                    "compatibility_fallback": False,
                },
            )
        if processor is None:
            raise RuntimeError("P5 media processor is unavailable")
        if message.task_key == "backfill-media-metadata" and parameters.get("targets") == []:
            return TaskOutcome.skipped(
                "没有缺失元数据需要补齐。",
                _outcome_details(message.task_key),
            )
        if self._compatibility is None:
            raise RuntimeError(f"P5 task {message.task_key!r} compatibility execution is unavailable")
        result = self._compatibility.execute(message.task_key, processor, parameters)
        return _explicit_outcome(message.task_key, result)


def _normalize_parameters(task_key: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    extras = set(payload) - _ALLOWED_PARAMETERS[task_key]
    if extras:
        raise ValueError(f"task {task_key!r} contains unsupported parameters: {sorted(extras)!r}")
    values = dict(payload)
    if "batch_size" in values:
        batch_size = values["batch_size"]
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
            raise ValueError("populate-metadata batch_size must be a positive integer")
    for field in ("force_full_update", "metadata_backfill_only"):
        if field in values and not isinstance(values[field], bool):
            raise TypeError(f"task {task_key!r} parameter {field!r} must be boolean")
    if task_key == "backfill-media-metadata" and "targets" in values:
        values["targets"] = _normalize_backfill_targets(values["targets"])
    return values


def _normalize_backfill_targets(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise TypeError("backfill-media-metadata targets must be a list")
    targets: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for index, raw in enumerate(value):
        if not isinstance(raw, Mapping):
            raise TypeError(f"backfill target {index} must be an object")
        extras = set(raw) - {"tmdb_id", "media_type"}
        if extras:
            raise ValueError(f"backfill target {index} contains unsupported fields: {sorted(extras)!r}")
        tmdb_id = str(raw.get("tmdb_id") or "").strip()
        media_type = str(raw.get("media_type") or "").strip().lower()
        if not tmdb_id:
            raise ValueError(f"backfill target {index} requires tmdb_id")
        if media_type not in {"movie", "tv"}:
            raise ValueError(f"backfill target {index} media_type must be movie or tv")
        identity = (tmdb_id, media_type)
        if identity in seen:
            continue
        seen.add(identity)
        targets.append({"tmdb_id": tmdb_id, "media_type": media_type})
    return targets


def _metadata_uuid(values: Mapping[str, Any], field: str) -> UUID:
    raw = values.get(field)
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"task metadata field {field!r} must be a UUID string")
    try:
        return UUID(raw.strip())
    except ValueError as exc:
        raise ValueError(f"task metadata field {field!r} must be a UUID string") from exc


def _explicit_outcome(task_key: str, result: object) -> TaskOutcome:
    details = _outcome_details(task_key)
    if isinstance(result, TaskOutcome):
        return TaskOutcome(
            result.status,
            result.message,
            {**result.details, **details},
        )
    if result is None or result is True:
        return TaskOutcome.succeeded(
            f"{_DISPLAY_NAMES[task_key]}兼容处理完成，未报告失败。",
            details,
        )
    if result is False:
        return TaskOutcome.failed(
            f"{_DISPLAY_NAMES[task_key]}兼容处理返回失败。",
            details,
        )
    if isinstance(result, Mapping):
        legacy_result = normalize_json_object(dict(result), field=f"{task_key} compatibility result")
        outcome_details = {**details, "legacy_result": legacy_result}
        message = str(legacy_result.get("message") or f"{_DISPLAY_NAMES[task_key]}兼容处理完成。")
        status = str(legacy_result.get("status") or "").strip().lower()
        if legacy_result.get("skipped") is True or status in {TaskOutcomeStatus.SKIPPED.value, "not_applicable"}:
            return TaskOutcome.skipped(message, outcome_details)
        if legacy_result.get("ok") is False or status in {TaskOutcomeStatus.FAILED.value, "indeterminate"}:
            return TaskOutcome.failed(message, outcome_details)
        if status == TaskOutcomeStatus.PARTIAL.value:
            return TaskOutcome.partial(message, outcome_details)
        return TaskOutcome.succeeded(message, outcome_details)
    return TaskOutcome.succeeded(
        f"{_DISPLAY_NAMES[task_key]}兼容处理完成，未报告失败。",
        {**details, "legacy_result_type": type(result).__name__},
    )


def _outcome_details(task_key: str) -> dict[str, str | bool]:
    return {
        "task_key": task_key,
        "native_handler": "P5MediaTaskRouter",
        "compatibility_fallback": True,
        "fallback_retirement_stage": P5_COMPATIBILITY_FALLBACK_UNTIL,
    }


__all__ = [
    "LegacyP5MediaCompatibilityGateway",
    "P5_COMPATIBILITY_FALLBACK_UNTIL",
    "P5MediaCompatibilityGateway",
    "P5MediaInfoRestoreUseCase",
    "P5MediaTaskProgress",
    "P5MediaTaskRouter",
]
