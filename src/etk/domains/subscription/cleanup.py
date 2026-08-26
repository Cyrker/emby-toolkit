from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Protocol

from etk.domains.system.standalone import StandaloneSettingsRepository

CLEANUP_USE_CASE_PROVIDER = "subscription.cleanup"
CLEANUP_INDEX_REPOSITORY_PROVIDER = "subscription.cleanup-index"
CLEANUP_MEDIA_PROJECTION_PROVIDER = "media.cleanup-projection"
CLEANUP_EXECUTION_PROVIDER = "subscription.cleanup-execution"
CLEANUP_VERSION_DELETION_PROVIDER = "media.cleanup-version-deletion"

CLEANUP_CONFIG_SETTING = "media_cleanup_config"
LEGACY_CLEANUP_RULES_SETTING = "media_cleanup_rules"
LEGACY_CLEANUP_LIBRARY_IDS_SETTING = "media_cleanup_library_ids"
LEGACY_CLEANUP_KEEP_ONE_PER_RES_SETTING = "media_cleanup_keep_one_per_res"
LEGACY_CLEANUP_DELETE_DELAY_SETTING = "media_cleanup_delete_delay"
SCAN_CLEANUP_TASK_KEY = "scan-cleanup-issues"

_DEFAULT_RULES_MAP: dict[str, dict[str, Any]] = {
    "runtime": {"id": "runtime", "enabled": True, "priority": "desc"},
    "effect": {
        "id": "effect",
        "enabled": True,
        "priority": ["dovi_p8", "dovi_p7", "dovi_p5", "dovi_other", "hdr10+", "hdr", "sdr"],
    },
    "resolution": {
        "id": "resolution",
        "enabled": True,
        "priority": ["4K", "1080p", "720p", "480p"],
    },
    "bit_depth": {"id": "bit_depth", "enabled": True, "priority": "desc"},
    "codec": {
        "id": "codec",
        "enabled": True,
        "priority": ["AV1", "HEVC", "H.264", "VP9"],
    },
    "bitrate": {"id": "bitrate", "enabled": True, "priority": "desc"},
    "quality": {
        "id": "quality",
        "enabled": True,
        "priority": ["Remux", "BluRay", "WEB-DL", "HDTV"],
    },
    "subtitle": {"id": "subtitle", "enabled": True, "priority": "desc"},
    "frame_rate": {"id": "frame_rate", "enabled": False, "priority": "desc"},
    "filesize": {"id": "filesize", "enabled": True, "priority": "desc"},
    "date_added": {"id": "date_added", "enabled": True, "priority": "asc"},
}


class CleanupIndexRepository(Protocol):
    def list_pending_tasks(self) -> Sequence[Mapping[str, Any]]: ...

    def pending_task_ids(self) -> Sequence[int]: ...

    def ignore_tasks(self, task_ids: Sequence[Any]) -> int: ...

    def delete_tasks(self, task_ids: Sequence[Any]) -> int: ...

    def mark_processed(self, task_ids: Sequence[Any]) -> int: ...

    def remove_deleted_version(self, emby_id: str) -> int: ...


class CleanupJobSubmissionPort(Protocol):
    def is_busy(self) -> bool: ...

    def submit_task(
        self,
        task_key: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        trigger_type: str = "manual",
        requested_by_user_id: str | None = None,
        trace_id: str | None = None,
        reject_when_busy: bool = False,
    ) -> Any: ...


class CleanupProcessorReadinessPort(Protocol):
    def media_ready(self) -> bool: ...


class CleanupExecutionSubmissionPort(Protocol):
    """Durable P7 execution intent; the runtime adapter owns actual deletion work."""

    def submit_cleanup(
        self,
        task_ids: Sequence[Any],
        *,
        requested_by_user_id: str | None = None,
        trace_id: str | None = None,
    ) -> Any: ...

    def submit_version_deletion(
        self,
        emby_id: str,
        *,
        requested_by_user_id: str | None = None,
        trace_id: str | None = None,
    ) -> Any: ...


class CleanupVersionNotFoundError(LookupError):
    pass


class CleanupPhysicalDeletionError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class CleanupClearAllResult:
    task_count: int
    submitted: bool


class CleanupService:
    """P7 cleanup HTTP/state use case over P4 jobs and P5 deletion/projection ports."""

    def __init__(
        self,
        repository: CleanupIndexRepository,
        settings: StandaloneSettingsRepository,
        jobs: CleanupJobSubmissionPort,
        readiness: CleanupProcessorReadinessPort,
        execution: CleanupExecutionSubmissionPort,
    ) -> None:
        self._repository = repository
        self._settings = settings
        self._jobs = jobs
        self._readiness = readiness
        self._execution = execution

    def list_tasks(self) -> list[dict[str, Any]]:
        return [dict(task) for task in self._repository.list_pending_tasks()]

    def busy(self) -> bool:
        return self._jobs.is_busy()

    def processor_ready(self) -> bool:
        return self._readiness.media_ready()

    def execute(
        self,
        task_ids: Sequence[Any],
        *,
        requested_by_user_id: str | None = None,
        trace_id: str | None = None,
    ) -> Any:
        return self._execution.submit_cleanup(
            tuple(task_ids),
            requested_by_user_id=requested_by_user_id,
            trace_id=trace_id,
        )

    def ignore(self, task_ids: Sequence[Any]) -> int:
        return self._repository.ignore_tasks(tuple(task_ids))

    def delete(self, task_ids: Sequence[Any]) -> int:
        return self._repository.delete_tasks(tuple(task_ids))

    def clear_all(
        self,
        *,
        requested_by_user_id: str | None = None,
        trace_id: str | None = None,
    ) -> CleanupClearAllResult:
        task_ids = tuple(self._repository.pending_task_ids())
        if not task_ids:
            return CleanupClearAllResult(0, False)
        self._execution.submit_cleanup(
            task_ids,
            requested_by_user_id=requested_by_user_id,
            trace_id=trace_id,
        )
        return CleanupClearAllResult(len(task_ids), True)

    def get_settings(self) -> dict[str, Any]:
        config_data = self._settings.load(CLEANUP_CONFIG_SETTING)
        if config_data:
            if not isinstance(config_data, Mapping):
                raise TypeError("media_cleanup_config must be an object")
            saved_rules = config_data.get("rules", [])
            library_ids = deepcopy(config_data.get("library_ids", []))
            keep_one_per_res = deepcopy(config_data.get("keep_one_per_res", False))
            delete_delay = deepcopy(config_data.get("delete_delay", 0))
            manual_delete_without_confirm = deepcopy(config_data.get("manual_delete_without_confirm", False))
        else:
            saved_rules = self._settings.load(LEGACY_CLEANUP_RULES_SETTING) or []
            library_ids = deepcopy(self._settings.load(LEGACY_CLEANUP_LIBRARY_IDS_SETTING) or [])
            keep_one_per_res = deepcopy(self._settings.load(LEGACY_CLEANUP_KEEP_ONE_PER_RES_SETTING) or False)
            delete_delay = deepcopy(self._settings.load(LEGACY_CLEANUP_DELETE_DELAY_SETTING) or 0)
            manual_delete_without_confirm = False
        return {
            "rules": _merge_cleanup_rules(saved_rules),
            "library_ids": library_ids,
            "keep_one_per_res": keep_one_per_res,
            "delete_delay": delete_delay,
            "manual_delete_without_confirm": manual_delete_without_confirm,
        }

    def save_settings(self, data: Mapping[str, Any]) -> None:
        config_data = {
            "rules": deepcopy(data.get("rules")),
            "library_ids": deepcopy(data.get("library_ids")),
            "keep_one_per_res": bool(data.get("keep_one_per_res")),
            "delete_delay": int(data.get("delete_delay") or 0),
            "manual_delete_without_confirm": bool(data.get("manual_delete_without_confirm", False)),
        }
        self._settings.save(CLEANUP_CONFIG_SETTING, config_data)

    def scan(
        self,
        *,
        requested_by_user_id: str | None,
        trace_id: str | None,
    ) -> None:
        self._jobs.submit_task(
            SCAN_CLEANUP_TASK_KEY,
            {},
            trigger_type="manual",
            requested_by_user_id=requested_by_user_id,
            trace_id=trace_id,
            reject_when_busy=True,
        )

    def delete_version(
        self,
        emby_id: Any,
        *,
        requested_by_user_id: str | None = None,
        trace_id: str | None = None,
    ) -> Any:
        normalized_id = str(emby_id)
        try:
            return self._execution.submit_version_deletion(
                normalized_id,
                requested_by_user_id=requested_by_user_id,
                trace_id=trace_id,
            )
        except LookupError as exc:
            raise CleanupVersionNotFoundError(normalized_id) from exc


def _merge_cleanup_rules(value: Any) -> list[dict[str, Any]]:
    if not value:
        return [deepcopy(rule) for rule in _DEFAULT_RULES_MAP.values()]
    if not isinstance(value, list):
        raise TypeError("cleanup rules must be a list")

    saved_rules: list[Mapping[str, Any]] = []
    for raw_rule in value:
        if not isinstance(raw_rule, Mapping):
            raise TypeError("cleanup rule must be an object")
        saved_rules.append(raw_rule)
    saved_rules_map = {str(rule["id"]): rule for rule in saved_rules}
    final_rules: list[dict[str, Any]] = []
    for saved_rule in saved_rules:
        rule_id = str(saved_rule["id"])
        merged = {**deepcopy(_DEFAULT_RULES_MAP.get(rule_id, {})), **deepcopy(dict(saved_rule))}
        priority = merged.get("priority")
        if rule_id == "effect" and isinstance(priority, list):
            merged["priority"] = _effect_priority(priority)
        elif rule_id == "resolution" and isinstance(priority, list):
            merged["priority"] = _resolution_priority(priority)
        elif rule_id == "codec" and isinstance(priority, list):
            merged["priority"] = _codec_priority(priority)
        final_rules.append(merged)

    for rule_id, default_rule in _DEFAULT_RULES_MAP.items():
        if rule_id not in saved_rules_map:
            final_rules.append(deepcopy(default_rule))
    return final_rules


def _effect_priority(priority: Sequence[Any]) -> list[str]:
    final: list[str] = []
    seen: set[str] = set()
    for value in priority:
        normalized = str(value).lower().replace(" ", "_")
        if normalized in {"dovi", "dovi_other", "dovi(other)", "dovi_(other)"}:
            normalized = "dovi_other"
        if normalized not in seen:
            final.append(normalized)
            seen.add(normalized)
    for value in _DEFAULT_RULES_MAP["effect"]["priority"]:
        normalized = str(value)
        if normalized not in seen:
            final.append(normalized)
            seen.add(normalized)
    return final


def _resolution_priority(priority: Sequence[Any]) -> list[Any]:
    final: list[Any] = []
    for value in priority:
        normalized = str(value).lower()
        final.append("4K" if normalized in {"2160p", "4k"} else deepcopy(value))
    if "480p" not in final:
        final.append("480p")
    return final


def _codec_priority(priority: Sequence[Any]) -> list[Any]:
    normalized_values: list[Any] = []
    for value in priority:
        normalized = str(value).upper()
        if normalized in {"H265", "X265"}:
            normalized_values.append("HEVC")
        elif normalized in {"H264", "X264", "AVC"}:
            normalized_values.append("H.264")
        else:
            normalized_values.append(deepcopy(value))
    final: list[Any] = []
    for value in normalized_values:
        if value not in final:
            final.append(value)
    return final


__all__ = [
    "CLEANUP_CONFIG_SETTING",
    "CLEANUP_EXECUTION_PROVIDER",
    "CLEANUP_INDEX_REPOSITORY_PROVIDER",
    "CLEANUP_MEDIA_PROJECTION_PROVIDER",
    "CLEANUP_USE_CASE_PROVIDER",
    "CLEANUP_VERSION_DELETION_PROVIDER",
    "CleanupClearAllResult",
    "CleanupExecutionSubmissionPort",
    "CleanupIndexRepository",
    "CleanupJobSubmissionPort",
    "CleanupPhysicalDeletionError",
    "CleanupProcessorReadinessPort",
    "CleanupService",
    "CleanupVersionNotFoundError",
    "SCAN_CLEANUP_TASK_KEY",
]
