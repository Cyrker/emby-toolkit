from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol
from uuid import UUID

CLEANUP_EXECUTION_TASK_KEY = "p7-cleanup-execution"
DEFAULT_CLEANUP_RECOVERY_BUDGET = 3


class CleanupEffectStatus(StrEnum):
    CONFIRMED = "confirmed"
    RETRYABLE = "retryable"
    INDETERMINATE = "indeterminate"


@dataclass(frozen=True, slots=True)
class CleanupEffectResult:
    status: CleanupEffectStatus
    evidence: Mapping[str, Any]
    error: Mapping[str, Any] | None = None

    @classmethod
    def confirmed(cls, evidence: Mapping[str, Any] | None = None) -> CleanupEffectResult:
        return cls(CleanupEffectStatus.CONFIRMED, dict(evidence or {}))

    @classmethod
    def retryable(
        cls,
        error: Mapping[str, Any],
        evidence: Mapping[str, Any] | None = None,
    ) -> CleanupEffectResult:
        return cls(CleanupEffectStatus.RETRYABLE, dict(evidence or {}), dict(error))

    @classmethod
    def indeterminate(
        cls,
        error: Mapping[str, Any],
        evidence: Mapping[str, Any] | None = None,
    ) -> CleanupEffectResult:
        return cls(CleanupEffectStatus.INDETERMINATE, dict(evidence or {}), dict(error))


@dataclass(frozen=True, slots=True)
class CleanupExecutionItem:
    id: UUID
    job_id: UUID
    execution_key: str
    cleanup_index_id: int | None
    item_key: str
    snapshot: Mapping[str, Any]
    status: str
    revision: int
    attempt_id: UUID
    worker_id: str
    claim_token: UUID
    attempt_no: int
    max_attempts: int
    effect_started: bool
    receipt: Mapping[str, Any] | None


@dataclass(frozen=True, slots=True)
class CleanupExecutionSummary:
    confirmed: int
    indeterminate: int
    cancelled: int
    active: int

    @property
    def total(self) -> int:
        return self.confirmed + self.indeterminate + self.cancelled + self.active


class CleanupExecutionPending(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        item_id: UUID,
        phase: str,
        retry_delay_seconds: float | None = None,
    ) -> None:
        self.item_id = item_id
        self.phase = phase
        self.retry_delay_seconds = retry_delay_seconds
        self.post_effect = True
        super().__init__(message)


class CleanupExecutionProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...

    def control_state(self) -> Mapping[str, Any]: ...


class CleanupExecutionStore(Protocol):
    def claim_next(
        self,
        *,
        job_id: UUID,
        execution_key: str,
        attempt_id: UUID,
        worker_id: str,
        lease_seconds: int,
    ) -> CleanupExecutionItem | None: ...

    def save_receipt(
        self,
        item: CleanupExecutionItem,
        receipt: Mapping[str, Any],
        *,
        start_effect: bool,
        lease_seconds: int,
    ) -> CleanupExecutionItem: ...

    def mark_confirmed(self, item: CleanupExecutionItem, receipt: Mapping[str, Any]) -> None: ...

    def mark_indeterminate(
        self,
        item: CleanupExecutionItem,
        receipt: Mapping[str, Any],
        error: Mapping[str, Any],
    ) -> None: ...

    def release_for_retry(self, item: CleanupExecutionItem, error: Mapping[str, Any]) -> None: ...

    def cancel_unstarted_job(self, job_id: UUID, error: Mapping[str, Any]) -> int: ...

    def abort_unstarted(self, job_id: UUID, error: Mapping[str, Any]) -> int: ...

    def summary(self, job_id: UUID) -> CleanupExecutionSummary: ...


class CleanupLocalEffectPort(Protocol):
    def reconcile(self, target: Mapping[str, Any]) -> CleanupEffectResult: ...


class CleanupMediaEffectPort(Protocol):
    def reconcile(self, target: Mapping[str, Any], *, operation_id: str) -> CleanupEffectResult: ...


class CleanupProviderEffectPort(Protocol):
    def request(self, target: Mapping[str, Any]) -> CleanupEffectResult: ...

    def readback(self, target: Mapping[str, Any]) -> CleanupEffectResult: ...


_PHASES = ("local", "media", "p115", "emby")
_DISPLAY = {
    "local": "本地文件",
    "media": "媒体数据库",
    "p115": "115 联动删除",
    "emby": "Emby 删除回读",
}


class CleanupExecutionService:
    """Execute one frozen physical version at a time with durable phase receipts."""

    def __init__(
        self,
        repository: CleanupExecutionStore,
        *,
        local: CleanupLocalEffectPort,
        media: CleanupMediaEffectPort,
        p115: CleanupProviderEffectPort,
        emby: CleanupProviderEffectPort,
        lease_seconds: int = 420,
        readback_budget: int = DEFAULT_CLEANUP_RECOVERY_BUDGET,
    ) -> None:
        if lease_seconds < 1 or readback_budget < 1:
            raise ValueError("cleanup execution lease and readback budget must be positive")
        self._repository = repository
        self._local = local
        self._media = media
        self._p115 = p115
        self._emby = emby
        self._lease_seconds = lease_seconds
        self._readback_budget = readback_budget

    def execute(
        self,
        *,
        job_id: UUID,
        execution_key: str,
        attempt_id: UUID,
        worker_id: str,
        progress: CleanupExecutionProgress,
    ) -> Mapping[str, Any]:
        while True:
            item = self._repository.claim_next(
                job_id=job_id,
                execution_key=execution_key,
                attempt_id=attempt_id,
                worker_id=worker_id,
                lease_seconds=self._lease_seconds,
            )
            if item is None:
                break
            control = dict(progress.control_state())
            if not item.effect_started and not _receipt_started(item.receipt):
                if bool(control.get("cancel_requested")):
                    self._repository.cancel_unstarted_job(
                        job_id,
                        {"type": "cancelled_before_cleanup_effect"},
                    )
                    break
                if bool(control.get("timed_out")):
                    self._repository.release_for_retry(
                        item,
                        {"type": "timeout_before_cleanup_effect"},
                    )
                    progress.checkpoint()
                progress.checkpoint()

            terminal = self._execute_item(item, progress)
            if terminal == "indeterminate":
                self._repository.abort_unstarted(
                    job_id,
                    {"type": "batch_aborted_after_indeterminate", "item_id": str(item.id)},
                )
                break

        summary = self._repository.summary(job_id)
        if summary.total == 0:
            raise RuntimeError("cleanup execution Job has no durable items")
        if summary.active:
            raise RuntimeError("cleanup execution returned with active durable items")
        details = {
            "job_id": str(job_id),
            "execution_key": execution_key,
            "confirmed": summary.confirmed,
            "indeterminate": summary.indeterminate,
            "cancelled": summary.cancelled,
            "active": summary.active,
            "durable_effect_started": bool(summary.confirmed or summary.indeterminate),
        }
        if summary.indeterminate:
            return {
                "ok": False,
                "status": "indeterminate",
                "message": "媒体清理存在无法确认的外部效果，需要人工对账。",
                **details,
            }
        if summary.cancelled and not summary.confirmed:
            return {
                "ok": True,
                "skipped": True,
                "status": "cancelled",
                "message": "媒体清理在任何物理效果前取消。",
                **details,
            }
        if summary.cancelled:
            return {
                "ok": False,
                "status": "partial",
                "message": "媒体清理只完成了取消前已开始的项目。",
                **details,
            }
        return {
            "ok": True,
            "status": "confirmed",
            "message": f"媒体清理完成，共确认 {summary.confirmed} 个物理版本。",
            **details,
        }

    def _execute_item(self, initial: CleanupExecutionItem, progress: CleanupExecutionProgress) -> str:
        item = initial
        receipt = _receipt(item)
        target = _target(item.snapshot)
        for phase_index, phase in enumerate(_PHASES, 1):
            entry = _phase_entry(receipt, phase)
            if entry.get("status") == CleanupEffectStatus.CONFIRMED.value:
                continue
            if _phase_budget_exhausted(entry, phase, self._readback_budget):
                prior_error = entry.get("error")
                error: dict[str, Any] = (
                    dict(prior_error)
                    if isinstance(prior_error, Mapping)
                    else {"type": f"{phase}_recovery_budget_exhausted"}
                )
                error["recovery_budget_exhausted"] = True
                self._repository.mark_indeterminate(item, receipt, error)
                return "indeterminate"
            progress.update(
                min(95, 5 + phase_index * 20),
                f"媒体清理 {item.item_key}：{_DISPLAY[phase]}",
            )
            item, receipt, result = self._run_phase(item, receipt, phase, target)
            if result.status is CleanupEffectStatus.CONFIRMED:
                continue
            if result.status is CleanupEffectStatus.INDETERMINATE:
                error = dict(result.error or {"type": f"{phase}_indeterminate"})
                self._repository.mark_indeterminate(item, receipt, error)
                return "indeterminate"

            entry = _phase_entry(receipt, phase)
            exhausted = (
                int(entry.get("readback_attempts") or 0) >= self._readback_budget
                if phase in {"p115", "emby"}
                else int(entry.get("attempts") or 0) >= self._readback_budget
            )
            if exhausted:
                error = dict(result.error or {"type": f"{phase}_recovery_budget_exhausted"})
                error["recovery_budget_exhausted"] = True
                self._repository.mark_indeterminate(item, receipt, error)
                return "indeterminate"
            if bool(progress.control_state().get("cancel_requested")):
                self._repository.abort_unstarted(
                    item.job_id,
                    {"type": "cancelled_after_cleanup_effect", "item_id": str(item.id)},
                )
            raise CleanupExecutionPending(
                f"cleanup phase {phase!r} requires authoritative recovery",
                item_id=item.id,
                phase=phase,
                retry_delay_seconds=30 if phase == "emby" else None,
            )

        self._repository.mark_confirmed(item, receipt)
        progress.update(100, f"媒体清理 {item.item_key} 已确认")
        return "confirmed"

    def _run_phase(
        self,
        item: CleanupExecutionItem,
        receipt: dict[str, Any],
        phase: str,
        target: Mapping[str, Any],
    ) -> tuple[CleanupExecutionItem, dict[str, Any], CleanupEffectResult]:
        entry = _phase_entry(receipt, phase)
        request_started = bool(entry.get("request_started"))
        entry["request_started"] = True
        entry["attempts"] = int(entry.get("attempts") or 0) + 1
        if request_started and phase in {"p115", "emby"}:
            entry["readback_attempts"] = int(entry.get("readback_attempts") or 0) + 1
            entry["status"] = "readback"
        else:
            entry["status"] = "requesting"
        item = self._repository.save_receipt(
            item,
            receipt,
            start_effect=True,
            lease_seconds=self._lease_seconds,
        )
        try:
            if phase == "local":
                result = self._local.reconcile(target)
            elif phase == "media":
                result = self._media.reconcile(target, operation_id=f"p7-cleanup:{item.id}:media")
            elif phase == "p115":
                result = self._p115.readback(target) if request_started else self._p115.request(target)
            else:
                result = self._emby.readback(target) if request_started else self._emby.request(target)
        except Exception as exc:
            result = CleanupEffectResult.retryable(
                {"type": f"{phase}_effect_error", "message": str(exc)},
            )

        entry = _phase_entry(receipt, phase)
        entry["status"] = (
            CleanupEffectStatus.CONFIRMED.value
            if result.status is CleanupEffectStatus.CONFIRMED
            else (
                CleanupEffectStatus.INDETERMINATE.value
                if result.status is CleanupEffectStatus.INDETERMINATE
                else "awaiting_readback"
            )
        )
        entry["evidence"] = deepcopy(dict(result.evidence))
        entry["error"] = deepcopy(dict(result.error)) if result.error is not None else None
        item = self._repository.save_receipt(
            item,
            receipt,
            start_effect=True,
            lease_seconds=self._lease_seconds,
        )
        return item, receipt, result


def _receipt(item: CleanupExecutionItem) -> dict[str, Any]:
    if item.receipt is None:
        return {
            "schema_version": 1,
            "item_id": str(item.id),
            "item_key": item.item_key,
            "phases": {
                phase: {
                    "status": "pending",
                    "request_started": False,
                    "attempts": 0,
                    "readback_attempts": 0,
                    "evidence": {},
                    "error": None,
                }
                for phase in _PHASES
            },
        }
    value = deepcopy(dict(item.receipt))
    if int(value.get("schema_version") or 0) != 1 or str(value.get("item_id") or "") != str(item.id):
        raise RuntimeError("cleanup execution receipt identity is invalid")
    phases = value.get("phases")
    if not isinstance(phases, Mapping) or set(phases) != set(_PHASES):
        raise RuntimeError("cleanup execution receipt phases are invalid")
    return value


def _phase_entry(receipt: dict[str, Any], phase: str) -> dict[str, Any]:
    phases = receipt.get("phases")
    if not isinstance(phases, dict):
        raise RuntimeError("cleanup execution receipt phases are invalid")
    entry = phases.get(phase)
    if not isinstance(entry, dict):
        raise RuntimeError(f"cleanup execution phase {phase!r} is invalid")
    return entry


def _receipt_started(receipt: Mapping[str, Any] | None) -> bool:
    if not isinstance(receipt, Mapping):
        return False
    phases = receipt.get("phases")
    return bool(
        isinstance(phases, Mapping)
        and any(isinstance(value, Mapping) and value.get("request_started") is True for value in phases.values())
    )


def _target(snapshot: Mapping[str, Any]) -> Mapping[str, Any]:
    value = snapshot.get("target")
    if not isinstance(value, Mapping):
        raise RuntimeError("cleanup execution snapshot has no frozen target")
    return value


def _phase_budget_exhausted(entry: Mapping[str, Any], phase: str, budget: int) -> bool:
    counter = "readback_attempts" if phase in {"p115", "emby"} and entry.get("request_started") is True else "attempts"
    return int(entry.get(counter) or 0) >= budget


def cleanup_job_max_attempts(
    item_count: int,
    *,
    recovery_budget: int = DEFAULT_CLEANUP_RECOVERY_BUDGET,
) -> int:
    if item_count < 1 or recovery_budget < 1:
        raise ValueError("cleanup item count and recovery budget must be positive")
    per_item_pending = 2 * max(recovery_budget - 1, 0) + 2 * recovery_budget
    return max(3, 1 + item_count * per_item_pending)


__all__ = [
    "CLEANUP_EXECUTION_TASK_KEY",
    "DEFAULT_CLEANUP_RECOVERY_BUDGET",
    "CleanupEffectResult",
    "CleanupEffectStatus",
    "CleanupExecutionItem",
    "CleanupExecutionPending",
    "CleanupExecutionProgress",
    "CleanupExecutionService",
    "CleanupExecutionStore",
    "CleanupExecutionSummary",
    "CleanupLocalEffectPort",
    "CleanupMediaEffectPort",
    "CleanupProviderEffectPort",
    "cleanup_job_max_attempts",
]
