from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from etk.platform.jobs import normalize_json_object


class TaskOutcomeStatus(StrEnum):
    SUCCEEDED = "succeeded"
    SKIPPED = "skipped"
    PARTIAL = "partial"
    FAILED = "failed"

    @property
    def successful(self) -> bool:
        return self in {TaskOutcomeStatus.SUCCEEDED, TaskOutcomeStatus.SKIPPED}


@dataclass(frozen=True, slots=True)
class TaskOutcome:
    status: TaskOutcomeStatus
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def succeeded(cls, message: str = "", details: Mapping[str, Any] | None = None) -> TaskOutcome:
        return cls(TaskOutcomeStatus.SUCCEEDED, str(message), _details(details))

    @classmethod
    def skipped(cls, message: str = "", details: Mapping[str, Any] | None = None) -> TaskOutcome:
        return cls(TaskOutcomeStatus.SKIPPED, str(message), _details(details))

    @classmethod
    def partial(cls, message: str = "", details: Mapping[str, Any] | None = None) -> TaskOutcome:
        return cls(TaskOutcomeStatus.PARTIAL, str(message), _details(details))

    @classmethod
    def failed(cls, message: str = "", details: Mapping[str, Any] | None = None) -> TaskOutcome:
        return cls(TaskOutcomeStatus.FAILED, str(message), _details(details))

    def to_result(self) -> dict[str, Any]:
        return {
            "ok": self.status.successful,
            "status": self.status.value,
            "message": self.message,
            "details": dict(self.details),
        }


def _details(value: Mapping[str, Any] | None) -> dict[str, Any]:
    return normalize_json_object(dict(value or {}), field="task outcome details")


__all__ = ["TaskOutcome", "TaskOutcomeStatus"]
