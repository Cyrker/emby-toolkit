"""P9 Worker envelope for sharing, HDHive and Telegram effects."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, cast

from etk.integrations import (
    HDHivePort,
    ProviderDisposition,
    ProviderResponse,
    SharedCenterPort,
    TelegramBotPort,
)
from etk.platform.jobs import normalize_json_object
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcome, TaskPayload
from etk.platform.tasks.catalog import P9_NATIVE_TASK_KEYS

P9_COMPATIBILITY_FALLBACK_UNTIL = "P11"


class P9TaskProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


class P9SharingCompatibilityGateway(Protocol):
    def execute(self, task_key: str, processor: object, parameters: Mapping[str, Any]) -> object: ...


class LegacyP9SharingCompatibilityGateway:
    """Worker-only fallback while the remaining local projections are retired."""

    _HANDLERS = {
        "hdhive-auto-checkin": ("tasks.hdhive", "task_hdhive_auto_checkin"),
        "shared-resource-maintenance": ("tasks.shared_resource_tasks", "task_shared_resource_maintenance"),
        "share-all-library": ("tasks.shared_resource_tasks", "share_all_library"),
        "shared-share-status-sync": (
            "tasks.shared_resource_tasks",
            "task_shared_share_status_sync_high_freq",
        ),
    }

    def execute(self, task_key: str, processor: object, parameters: Mapping[str, Any]) -> object:
        from importlib import import_module

        module_name, function_name = self._HANDLERS[task_key]
        handler = getattr(import_module(module_name), function_name)
        if not callable(handler):
            raise TypeError(f"P9 compatibility target {module_name}.{function_name} is not callable")
        return handler(processor, **dict(parameters))


class P9SharingTaskRouter:
    """Validate persistent task envelopes and call typed provider ports.

    The router does not persist provider payloads.  A Worker receives only the
    task parameters and operation/job identifiers; provider-specific receipts
    are owned by the sharing operation/ledger repositories.
    """

    def __init__(
        self,
        *,
        shared_center: SharedCenterPort,
        hdhive: HDHivePort,
        telegram: TelegramBotPort | None = None,
        compatibility: P9SharingCompatibilityGateway | None = None,
    ) -> None:
        self._shared_center = shared_center
        self._hdhive = hdhive
        self._telegram = telegram
        self._compatibility = compatibility

    def execute(
        self,
        message: TaskMessage,
        processor: object | None,
        progress: P9TaskProgress,
    ) -> TaskOutcome:
        if not isinstance(message, TaskMessage):
            raise TypeError("P9 sharing task router requires a TaskMessage")
        if message.task_key not in P9_NATIVE_TASK_KEYS:
            raise LookupError(f"unknown P9 task key: {message.task_key}")
        if message.processor is not ProcessorIdentity.MEDIA:
            raise ValueError(f"task {message.task_key!r} requires the media processor")
        if message.payload.version != 1:
            raise ValueError(f"task {message.task_key!r} payload version is not supported")
        progress.checkpoint()
        payload = TaskPayload.create(message.payload.data, version=message.payload.version).data
        parameters = _normalize_parameters(message.task_key, payload)

        try:
            if message.task_key == "hdhive-auto-checkin":
                response = self._hdhive.checkin(is_gambler=bool(parameters.get("is_gambler", False)))
                return _provider_outcome(message.task_key, response, "影巢自动签到完成。")
            if message.task_key == "shared-resource-maintenance":
                response = _shared_maintenance(self._shared_center, parameters)
                return _provider_outcome(message.task_key, response, "共享资源维护完成。")
            if message.task_key == "share-all-library":
                return self._share_all_library(parameters, progress)
            if message.task_key == "shared-share-status-sync":
                response = self._shared_center.share_sync_heartbeat(parameters)
                return _provider_outcome(message.task_key, response, "共享分享状态同步完成。")
        except Exception as exc:
            if self._compatibility is not None and _is_unavailable(exc):
                result = self._compatibility.execute(message.task_key, processor, parameters)
                return _compatibility_outcome(message.task_key, result)
            return TaskOutcome.failed(
                f"{_display_name(message.task_key)}失败。",
                {
                    **_details(message.task_key, compatibility=False),
                    "error_type": type(exc).__name__,
                    "retryable": _retryable_exception(exc),
                },
            )
        raise AssertionError(f"P9 task {message.task_key!r} was not dispatched")

    def _share_all_library(self, parameters: Mapping[str, Any], progress: P9TaskProgress) -> TaskOutcome:
        sources = parameters.get("sources")
        if not sources:
            return TaskOutcome.skipped(
                "没有需要登记的共享资源。",
                {**_details("share-all-library", compatibility=False), "registered": 0},
            )
        registered = 0
        rejected = 0
        errors: list[dict[str, Any]] = []
        retryable_failure = False
        indeterminate_failure = False
        total = len(sources)
        for index, item in enumerate(sources, 1):
            progress.checkpoint()
            source = dict(item)
            kind = str(source.pop("source_kind", source.pop("kind", "movie")) or "movie").strip().lower()
            if kind in {"series", "season", "pack"}:
                kind = "episode"
            try:
                response = self._shared_center.register_source(kind, source)
            except Exception as exc:
                errors.append({"index": index, "error_type": type(exc).__name__})
                disposition = getattr(exc, "disposition", None)
                retryable_failure = retryable_failure or disposition is ProviderDisposition.RETRYABLE
                indeterminate_failure = indeterminate_failure or disposition is ProviderDisposition.INDETERMINATE
                continue
            if response.disposition is ProviderDisposition.CONFIRMED:
                registered += 1
            else:
                rejected += 1
                errors.append({"index": index, "provider_status": response.disposition.value})
                retryable_failure = retryable_failure or response.disposition is ProviderDisposition.RETRYABLE
                indeterminate_failure = (
                    indeterminate_failure or response.disposition is ProviderDisposition.INDETERMINATE
                )
            progress.update(int(index * 100 / total), f"共享资源登记 {index}/{total}")
        details = {
            **_details("share-all-library", compatibility=False),
            "registered": registered,
            "rejected": rejected,
            "errors": errors[:20],
            "retryable": retryable_failure and not indeterminate_failure,
        }
        if registered and not rejected and not errors:
            return TaskOutcome.succeeded("媒体库共享资源登记完成。", details)
        if registered:
            return TaskOutcome.partial("媒体库共享资源登记部分完成。", details)
        return TaskOutcome.failed("媒体库共享资源登记失败。", details)


def _normalize_parameters(task_key: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    values = dict(payload)
    if task_key == "hdhive-auto-checkin":
        extras = set(values) - {"is_gambler"}
        if extras:
            raise ValueError(f"task {task_key!r} contains unsupported parameters: {sorted(extras)!r}")
        if "is_gambler" in values and not isinstance(values["is_gambler"], bool):
            raise TypeError("is_gambler must be boolean")
        return values
    if task_key == "shared-resource-maintenance":
        extras = set(values) - {"source_ids", "limit", "maintenance_silent"}
        if extras:
            raise ValueError(f"task {task_key!r} contains unsupported parameters: {sorted(extras)!r}")
        if "source_ids" in values:
            values["source_ids"] = _string_list(values["source_ids"], "source_ids")
        if "limit" in values:
            values["limit"] = _bounded(values["limit"], 1, 100_000, "limit")
        if "maintenance_silent" in values and not isinstance(values["maintenance_silent"], bool):
            raise TypeError("maintenance_silent must be boolean")
        return values
    if task_key == "share-all-library":
        extras = set(values) - {"sources", "max_items"}
        if extras:
            raise ValueError(f"task {task_key!r} contains unsupported parameters: {sorted(extras)!r}")
        sources = values.get("sources", [])
        if not isinstance(sources, list) or not all(isinstance(item, Mapping) for item in sources):
            raise TypeError("sources must be a list of objects")
        max_items = _bounded(values.get("max_items", 100_000), 1, 100_000, "max_items")
        values["sources"] = [dict(item) for item in sources[:max_items]]
        values["max_items"] = max_items
        return values
    extras = set(values) - {"source_ids", "limit"}
    if extras:
        raise ValueError(f"task {task_key!r} contains unsupported parameters: {sorted(extras)!r}")
    if "source_ids" in values:
        values["source_ids"] = _string_list(values["source_ids"], "source_ids")
    if "limit" in values:
        values["limit"] = _bounded(values["limit"], 1, 100_000, "limit")
    return values


def _shared_maintenance(shared: SharedCenterPort, parameters: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]:
    run = getattr(shared, "run_maintenance", None)
    if callable(run):
        return cast(ProviderResponse[dict[str, Any]], run(parameters))
    return shared.share_sync_heartbeat(parameters)


def _provider_outcome(task_key: str, response: ProviderResponse[Any], success_message: str) -> TaskOutcome:
    details = {
        **_details(task_key, compatibility=False),
        "provider_status": response.disposition.value,
        "http_status": response.status_code,
        "retry_after_seconds": response.retry_after_seconds,
    }
    if response.disposition is ProviderDisposition.CONFIRMED:
        return TaskOutcome.succeeded(success_message, {**details, "provider_result": _safe_payload(response.payload)})
    if response.disposition is ProviderDisposition.NOT_FOUND:
        return TaskOutcome.skipped(f"{_display_name(task_key)}没有可处理目标。", details)
    return TaskOutcome.failed(
        f"{_display_name(task_key)}未完成。",
        {**details, "retryable": response.disposition is ProviderDisposition.RETRYABLE},
    )


def _compatibility_outcome(task_key: str, result: object) -> TaskOutcome:
    if isinstance(result, TaskOutcome):
        return TaskOutcome(result.status, result.message, {**result.details, **_details(task_key, compatibility=True)})
    if isinstance(result, Mapping):
        value = normalize_json_object(dict(result), field=f"{task_key} compatibility result")
        if value.get("ok") is False or str(value.get("status") or "").lower() in {"failed", "indeterminate"}:
            return TaskOutcome.failed(
                str(value.get("message") or f"{_display_name(task_key)}失败。"),
                {**_details(task_key, compatibility=True), "legacy_result": value},
            )
        return TaskOutcome.succeeded(
            str(value.get("message") or f"{_display_name(task_key)}完成。"),
            {**_details(task_key, compatibility=True), "legacy_result": value},
        )
    if result is True or result is None:
        return TaskOutcome.succeeded(f"{_display_name(task_key)}完成。", _details(task_key, compatibility=True))
    return TaskOutcome.failed(f"{_display_name(task_key)}未返回可判定结果。", _details(task_key, compatibility=True))


def _details(task_key: str, *, compatibility: bool) -> dict[str, Any]:
    return {
        "task_key": task_key,
        "native_handler": "P9SharingTaskRouter",
        "compatibility_fallback": compatibility,
        "fallback_retirement_stage": P9_COMPATIBILITY_FALLBACK_UNTIL if compatibility else "not_applicable",
    }


def _display_name(task_key: str) -> str:
    return {
        "hdhive-auto-checkin": "影巢自动签到",
        "shared-resource-maintenance": "共享资源维护",
        "share-all-library": "媒体库共享资源登记",
        "shared-share-status-sync": "共享分享状态同步",
    }.get(task_key, task_key)


def _safe_payload(value: Any) -> Any:
    try:
        return normalize_json_object(dict(value), field="provider result") if isinstance(value, Mapping) else value
    except Exception:
        return {"type": type(value).__name__}


def _string_list(value: Any, field: str) -> list[str]:
    if not isinstance(value, list):
        raise TypeError(f"{field} must be a list")
    result = [str(item).strip() for item in value if str(item).strip()]
    return list(dict.fromkeys(result))


def _bounded(value: Any, minimum: int, maximum: int, field: str) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if result < minimum or result > maximum:
        raise ValueError(f"{field} must be between {minimum} and {maximum}")
    return result


def _is_unavailable(exc: Exception) -> bool:
    return getattr(exc, "disposition", None) is ProviderDisposition.RETRYABLE


def _retryable_exception(exc: Exception) -> bool:
    disposition = getattr(exc, "disposition", None)
    return disposition is ProviderDisposition.RETRYABLE


__all__ = [
    "LegacyP9SharingCompatibilityGateway",
    "P9_COMPATIBILITY_FALLBACK_UNTIL",
    "P9SharingCompatibilityGateway",
    "P9SharingTaskRouter",
    "P9TaskProgress",
]
