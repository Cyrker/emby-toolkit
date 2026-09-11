from __future__ import annotations

import json
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field

from etk.platform.observability.redaction import redact_sensitive
from etk.platform.tasks.catalog import LEGACY_TASK_KEY_ORDER, RETIRED_EXTERNAL_RESOURCE_TASK_KEYS


class RuntimeStatusResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    is_running: bool = False
    current_action: str = ""
    progress: int | float = 0
    message: str = ""
    last_action: str | None = None
    logs: list[Any] = Field(default_factory=list)


class TaskDefinitionResponse(BaseModel):
    key: str
    name: str
    help: str
    description: str
    processor_type: str | None = None


class RuntimeStatusReader(Protocol):
    def read_status(self) -> Mapping[str, Any]: ...


class TaskCatalog(Protocol):
    def available(self, context: str) -> list[dict[str, Any]]: ...


class LegacyRuntimeStatusReader:
    """P3 query adapter for process-local task state retained until P4."""

    def read_status(self) -> Mapping[str, Any]:
        import task_manager
        from logger_setup import frontend_log_queue

        payload = dict(task_manager.get_task_status())
        payload["logs"] = redact_sensitive(list(frontend_log_queue))
        return payload


class ManifestTaskCatalog:
    """Serve the frozen 50-key catalog without importing executable task modules."""

    def __init__(self, manifest_path: Path) -> None:
        self._manifest_path = manifest_path
        self._tasks: tuple[dict[str, Any], ...] | None = None

    def available(self, context: str) -> list[dict[str, Any]]:
        normalized = str(context or "chain").strip().lower()
        if normalized not in {"all", "chain", "manual"}:
            normalized = "chain"
        result: list[dict[str, Any]] = []
        for task in self._load_tasks():
            if task["task_key"] in RETIRED_EXTERNAL_RESOURCE_TASK_KEYS:
                continue
            if normalized not in task.get("visible_in", []):
                continue
            item = {
                "key": task["task_key"],
                "name": task["display_name"],
                "help": task["help"],
                "description": task["help"],
            }
            if normalized == "all":
                item["processor_type"] = task["processor_type"]
            result.append(item)
        return result

    def _load_tasks(self) -> tuple[dict[str, Any], ...]:
        if self._tasks is None:
            payload = json.loads(self._manifest_path.read_text(encoding="utf-8"))
            tasks = payload.get("tasks")
            if not isinstance(tasks, list) or len(tasks) != 50:
                raise RuntimeError("task ownership manifest must contain exactly 50 tasks")
            tasks_by_key = {task.get("task_key"): task for task in tasks if isinstance(task, dict)}
            if len(tasks_by_key) != len(tasks) or set(tasks_by_key) != set(LEGACY_TASK_KEY_ORDER):
                raise RuntimeError("task ownership manifest must contain the frozen 50 task keys exactly once")
            self._tasks = tuple(deepcopy(tasks_by_key[key]) for key in LEGACY_TASK_KEY_ORDER)
        return self._tasks
