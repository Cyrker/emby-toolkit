from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .catalog import (
    LEGACY_TASK_KEY_ORDER,
    P4_NATIVE_TASK_KEYS,
    P5_NATIVE_TASK_KEYS,
    P6_NATIVE_TASK_KEYS,
    P7_NATIVE_TASK_KEYS,
    P8_NATIVE_TASK_KEYS,
    P9_NATIVE_TASK_KEYS,
)
from .models import (
    JsonValue,
    ProcessorIdentity,
    TaskDefinition,
    TaskHandlerKind,
    TaskMessage,
    TaskPayload,
    TaskVisibility,
)
from .outcome import TaskOutcome, TaskOutcomeStatus
from .registry import NativeTaskHandler, TaskRegistry, task_definition_metadata

if TYPE_CHECKING:
    from etk.compatibility.legacy_jobs import LegacyCompatibilityResolver, LegacyTaskAdapter

__all__ = [
    "LEGACY_TASK_KEY_ORDER",
    "P4_NATIVE_TASK_KEYS",
    "P5_NATIVE_TASK_KEYS",
    "P6_NATIVE_TASK_KEYS",
    "P7_NATIVE_TASK_KEYS",
    "P8_NATIVE_TASK_KEYS",
    "P9_NATIVE_TASK_KEYS",
    "JsonValue",
    "LegacyCompatibilityResolver",
    "LegacyTaskAdapter",
    "NativeTaskHandler",
    "ProcessorIdentity",
    "TaskDefinition",
    "TaskHandlerKind",
    "TaskMessage",
    "TaskOutcome",
    "TaskOutcomeStatus",
    "TaskPayload",
    "TaskRegistry",
    "TaskVisibility",
    "task_definition_metadata",
]


def __getattr__(name: str) -> Any:
    if name in {"LegacyCompatibilityResolver", "LegacyTaskAdapter"}:
        from etk.compatibility.legacy_jobs import LegacyCompatibilityResolver, LegacyTaskAdapter

        return {
            "LegacyCompatibilityResolver": LegacyCompatibilityResolver,
            "LegacyTaskAdapter": LegacyTaskAdapter,
        }[name]
    raise AttributeError(name)
