from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

from etk.domains.subscription.moviepilot_configured import MOVIEPILOT_CONFIG_SETTING
from etk.domains.subscription.operation_repository import PostgresSubscriptionOperationRepository
from etk.domains.subscription.operations import (
    SubscriptionOperation,
    SubscriptionOperationIntent,
    SubscriptionOperationKind,
    SubscriptionOperationStatus,
    SubscriptionQuotaRequirement,
    canonical_json,
)
from etk.domains.subscription.provider_execution import SubscriptionOperationExecutor
from etk.domains.system.standalone import StandaloneSettingsRepository

_P7_LOCAL_SUBSCRIPTION_TASKS = frozenset({"auto-subscribe", "manual_subscribe_batch"})
_P7_RESUBSCRIBE_TASK = "resubscribe-library"


@dataclass(frozen=True, slots=True)
class MoviePilotExecutionBinding:
    job_id: str
    attempt_id: str
    worker_id: str
    task_key: str
    checkpoint: Callable[[], None]

    def identity(self) -> dict[str, str]:
        return {
            "job_id": self.job_id,
            "attempt_id": self.attempt_id,
            "worker_id": self.worker_id,
            "task_key": self.task_key,
        }


_MOVIEPILOT_EXECUTION: ContextVar[MoviePilotExecutionBinding | None] = ContextVar(
    "etk_moviepilot_execution",
    default=None,
)
_MOVIEPILOT_OCCURRENCE: ContextVar[str | None] = ContextVar(
    "etk_moviepilot_operation_occurrence",
    default=None,
)


@contextmanager
def bind_moviepilot_execution(
    *,
    job_id: str,
    attempt_id: str,
    worker_id: str,
    task_key: str,
    checkpoint: Callable[[], None],
):
    values = {
        "job_id": str(job_id or "").strip(),
        "attempt_id": str(attempt_id or "").strip(),
        "worker_id": str(worker_id or "").strip(),
        "task_key": str(task_key or "").strip(),
    }
    if not all(values.values()):
        raise RuntimeError("durable MoviePilot execution identity is incomplete")
    binding = MoviePilotExecutionBinding(**values, checkpoint=checkpoint)
    token: Token[MoviePilotExecutionBinding | None] = _MOVIEPILOT_EXECUTION.set(binding)
    try:
        yield binding
    finally:
        _MOVIEPILOT_EXECUTION.reset(token)


@contextmanager
def bind_moviepilot_operation_occurrence(value: str):
    occurrence = _operation_occurrence(value)
    if occurrence is None:
        raise ValueError("durable MoviePilot operation occurrence must not be empty")
    token: Token[str | None] = _MOVIEPILOT_OCCURRENCE.set(occurrence)
    try:
        yield occurrence
    finally:
        _MOVIEPILOT_OCCURRENCE.reset(token)


class DurableMoviePilotEffectIndeterminateError(RuntimeError):
    pass


class PersistentMoviePilotEffectGateway:
    """Route legacy P7 MoviePilot writes through the durable operation ledger."""

    def __init__(
        self,
        repository: PostgresSubscriptionOperationRepository,
        executor: SubscriptionOperationExecutor,
        settings: StandaloneSettingsRepository,
        *,
        lease_seconds: float = 90,
        wait_timeout_seconds: float = 180,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._repository = repository
        self._executor = executor
        self._settings = settings
        self._lease_seconds = max(float(lease_seconds), 1)
        self._wait_timeout_seconds = max(float(wait_timeout_seconds), 1)
        self._sleep = sleep

    def create_subscription(
        self,
        payload: Mapping[str, Any],
        *,
        consume_quota: bool,
        apply_local_continuation: bool = True,
    ) -> bool:
        normalized = dict(payload)
        tmdb_id = _required_text(normalized.get("tmdbid") or normalized.get("tmdb_id"), "tmdb_id")
        season = _optional_nonnegative_int(normalized.get("season"), "season")
        identity = self._identity()
        request: dict[str, Any] = {
            "action": "create",
            "tmdb_id": tmdb_id,
            "payload": normalized,
            "expected": {},
            "origin": _origin(identity),
        }
        if season is not None:
            request["season"] = season
        continuation = (
            _local_continuation(identity["task_key"], tmdb_id, normalized, season) if apply_local_continuation else None
        )
        if continuation is not None:
            request["local_continuation"] = continuation
        return self._execute(
            SubscriptionOperationIntent.create(
                kind=SubscriptionOperationKind.CREATE,
                scope_key=_scope(identity, "create", tmdb_id, season, normalized),
                request=request,
                quota=self._quota() if consume_quota else None,
            )
        )

    def update_subscription(self, payload: Mapping[str, Any]) -> bool:
        normalized = dict(payload)
        tmdb_id = _required_text(normalized.get("tmdbid") or normalized.get("tmdb_id"), "tmdb_id")
        season = _optional_nonnegative_int(normalized.get("season"), "season")
        expected = {
            key: normalized[key]
            for key in ("state", "total_episode", "best_version", "best_version_full")
            if key in normalized
        }
        identity = self._identity()
        request: dict[str, Any] = {
            "action": "update",
            "tmdb_id": tmdb_id,
            "payload": normalized,
            "expected": expected,
            "origin": _origin(identity),
        }
        if season is not None:
            request["season"] = season
        return self._execute(
            SubscriptionOperationIntent.create(
                kind=SubscriptionOperationKind.UPDATE,
                scope_key=_scope(identity, "update", tmdb_id, season, normalized),
                request=request,
            )
        )

    def update_subscription_status(
        self,
        tmdb_id: int | str,
        *,
        season: int | None,
        status: str,
        total_episodes: int | None,
    ) -> bool:
        normalized_tmdb = _required_text(tmdb_id, "tmdb_id")
        normalized_season = _optional_nonnegative_int(season, "season")
        normalized_status = str(status or "").strip().upper()
        if normalized_status not in {"P", "R", "S"}:
            raise ValueError("MoviePilot subscription status must be P, R, or S")
        payload: dict[str, Any] = {"status": normalized_status}
        expected: dict[str, Any] = {"state": normalized_status}
        if total_episodes is not None:
            total = _optional_nonnegative_int(total_episodes, "total_episodes")
            payload["total_episodes"] = total
            expected["total_episode"] = total
        identity = self._identity()
        request: dict[str, Any] = {
            "action": "update_status",
            "tmdb_id": normalized_tmdb,
            "payload": payload,
            "expected": expected,
            "origin": _origin(identity),
        }
        if normalized_season is not None:
            request["season"] = normalized_season
        return self._execute(
            SubscriptionOperationIntent.create(
                kind=SubscriptionOperationKind.UPDATE,
                scope_key=_scope(identity, "update-status", normalized_tmdb, normalized_season, payload),
                request=request,
            )
        )

    def delete_subscription(self, tmdb_id: int | str, *, season: int | None = None) -> bool:
        normalized_tmdb = _required_text(tmdb_id, "tmdb_id")
        normalized_season = _optional_nonnegative_int(season, "season")
        identity = self._identity()
        request: dict[str, Any] = {
            "action": "delete",
            "tmdb_id": normalized_tmdb,
            "expected": {},
            "origin": _origin(identity),
        }
        if normalized_season is not None:
            request["season"] = normalized_season
        return self._execute(
            SubscriptionOperationIntent.create(
                kind=SubscriptionOperationKind.DELETE,
                scope_key=_scope(identity, "delete", normalized_tmdb, normalized_season, {}),
                request=request,
            )
        )

    def delete_subscription_by_id(self, subscription_id: int | str) -> bool:
        normalized_id = _required_text(subscription_id, "subscription_id")
        identity = self._identity()
        return self._execute(
            SubscriptionOperationIntent.create(
                kind=SubscriptionOperationKind.RECONCILE,
                scope_key=_scope(identity, "delete-by-id", normalized_id, None, {}),
                request={
                    "action": "delete_by_id",
                    "subscription_id": normalized_id,
                    "payload": {},
                    "expected": {},
                    "origin": _origin(identity),
                },
            )
        )

    def search_subscription(self, subscription_id: int | str) -> bool:
        normalized_id = _required_text(subscription_id, "subscription_id")
        identity = self._identity()
        return self._execute(
            SubscriptionOperationIntent.create(
                kind=SubscriptionOperationKind.RECONCILE,
                scope_key=_scope(identity, "search", normalized_id, None, {}),
                request={
                    "action": "search",
                    "subscription_id": normalized_id,
                    "payload": {},
                    "expected": {},
                    "origin": _origin(identity),
                },
            )
        )

    def delete_download_tasks(
        self,
        hashes: Sequence[str],
        *,
        operation_occurrence: str | None = None,
    ) -> bool:
        normalized = sorted({str(value or "").strip().casefold() for value in hashes if str(value or "").strip()})
        if not normalized:
            return False
        identity = self._identity()
        occurrence = _operation_occurrence(operation_occurrence)
        request: dict[str, Any] = {
            "action": "delete_downloads",
            "payload": {"hashes": normalized},
            "expected": {},
            "origin": _origin(identity),
        }
        if occurrence is not None:
            request["operation_occurrence"] = occurrence
        return self._execute(
            SubscriptionOperationIntent.create(
                kind=SubscriptionOperationKind.RECONCILE,
                scope_key=_scope(
                    identity,
                    "delete-downloads",
                    "downloads",
                    None,
                    normalized,
                    operation_occurrence=occurrence,
                ),
                request=request,
                idempotency_key=occurrence,
            )
        )

    def delete_transfer_history(
        self,
        record: Mapping[str, Any],
        *,
        operation_occurrence: str | None = None,
    ) -> bool:
        normalized = dict(record)
        if not normalized:
            return False
        identity = self._identity()
        occurrence = _operation_occurrence(operation_occurrence)
        request: dict[str, Any] = {
            "action": "delete_transfer_history",
            "payload": {"record": normalized},
            "expected": {},
            "origin": _origin(identity),
        }
        if occurrence is not None:
            request["operation_occurrence"] = occurrence
        return self._execute(
            SubscriptionOperationIntent.create(
                kind=SubscriptionOperationKind.RECONCILE,
                scope_key=_scope(
                    identity,
                    "delete-transfer-history",
                    "history",
                    None,
                    normalized,
                    operation_occurrence=occurrence,
                ),
                request=request,
                idempotency_key=occurrence,
            )
        )

    def redo_transfer_history(self, history_ids: Sequence[int]) -> bool:
        normalized = sorted({int(value) for value in history_ids if int(value) > 0})
        if not normalized:
            return False
        identity = self._identity()
        return self._execute(
            SubscriptionOperationIntent.create(
                kind=SubscriptionOperationKind.RECONCILE,
                scope_key=_scope(identity, "redo-transfer", "history", None, normalized),
                request={
                    "action": "redo_transfer_history",
                    "payload": {"history_ids": normalized},
                    "expected": {},
                    "origin": _origin(identity),
                },
            )
        )

    def _execute(self, intent: SubscriptionOperationIntent) -> bool:
        identity = self._identity()
        self._checkpoint_before_reserve()
        operation_id = self._repository.reserve(intent).operation.id
        deadline = time.monotonic() + self._wait_timeout_seconds
        while True:
            self._checkpoint_or_cancel(operation_id)
            current = self._repository.read(operation_id)
            if current is None:
                raise RuntimeError("durable MoviePilot operation disappeared")
            terminal = self._terminal_result(current)
            if terminal is not None:
                return terminal
            execution = self._executor.execute(
                operation_id,
                worker_id=identity["worker_id"],
                lease_seconds=self._lease_seconds,
            )
            if execution is not None:
                terminal = self._terminal_result(execution.operation)
                if terminal is not None:
                    return terminal
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    "durable MoviePilot operation did not reach a terminal state before the wait deadline"
                )
            current = self._repository.read(operation_id)
            delay = 0.05
            if current is not None and current.available_at > datetime.now(UTC):
                delay = min(max((current.available_at - datetime.now(UTC)).total_seconds(), 0.05), 5.0)
            self._sleep(delay)

    def _terminal_result(self, operation: SubscriptionOperation) -> bool | None:
        if operation.status is SubscriptionOperationStatus.CONFIRMED:
            return True
        if operation.status in {SubscriptionOperationStatus.REJECTED, SubscriptionOperationStatus.CANCELLED}:
            return False
        if operation.status is SubscriptionOperationStatus.INDETERMINATE:
            raise DurableMoviePilotEffectIndeterminateError(
                f"MoviePilot effect is indeterminate and will not be resent: {operation.scope_key}"
            )
        return None

    def _checkpoint_or_cancel(self, operation_id: UUID) -> None:
        try:
            binding = _MOVIEPILOT_EXECUTION.get()
            if binding is not None:
                binding.checkpoint()
            else:
                import task_manager

                task_manager.persistent_execution_checkpoint()
        except BaseException:
            self._repository.request_cancel(operation_id)
            raise

    @staticmethod
    def _checkpoint_before_reserve() -> None:
        binding = _MOVIEPILOT_EXECUTION.get()
        if binding is not None:
            binding.checkpoint()
            return
        import task_manager

        task_manager.persistent_execution_checkpoint()

    def _identity(self) -> dict[str, str]:
        binding = _MOVIEPILOT_EXECUTION.get()
        if binding is not None:
            return binding.identity()
        import task_manager

        identity = task_manager.current_persistent_execution_identity()
        if not isinstance(identity, Mapping):
            raise RuntimeError("durable MoviePilot effect requires a persistent Worker identity")
        required = {
            key: str(identity.get(key) or "").strip() for key in ("job_id", "attempt_id", "worker_id", "task_key")
        }
        if not all(required.values()):
            raise RuntimeError("durable MoviePilot effect identity is incomplete")
        return required

    def _quota(self) -> SubscriptionQuotaRequirement:
        value = self._settings.load(MOVIEPILOT_CONFIG_SETTING)
        config = dict(value) if isinstance(value, Mapping) else {}
        raw_cap = config.get("resubscribe_daily_cap", 200)
        cap = int(raw_cap) if not isinstance(raw_cap, bool) else 200
        return SubscriptionQuotaRequirement(
            quota_date=datetime.now(ZoneInfo("Asia/Shanghai")).date(),
            daily_cap=max(cap, 0),
            ttl_seconds=86_400,
        )


def _origin(identity: Mapping[str, str]) -> dict[str, str]:
    occurrence = _MOVIEPILOT_OCCURRENCE.get()
    if occurrence is not None:
        return {
            "task_key": identity["task_key"],
            "operation_occurrence": occurrence,
        }
    return {"task_key": identity["task_key"], "job_id": identity["job_id"]}


def _scope(
    identity: Mapping[str, str],
    action: str,
    target: str,
    season: int | None,
    payload: object,
    *,
    operation_occurrence: str | None = None,
) -> str:
    occurrence = operation_occurrence or _MOVIEPILOT_OCCURRENCE.get()
    season_scope = "all" if season is None else str(season)
    if occurrence is not None:
        digest = sha256(occurrence.encode()).hexdigest()[:24]
        return f"p7:{identity['task_key']}:occurrence:{digest}:{action}:{target}:{season_scope}"
    payload_digest = sha256(canonical_json(payload).encode()).hexdigest()[:24]
    return f"p7:{identity['task_key']}:{identity['job_id']}:{action}:{target}:{season_scope}:{payload_digest}"


def _operation_occurrence(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        raise ValueError("durable MoviePilot operation occurrence must not be empty")
    return normalized


def _local_continuation(
    task_key: str,
    tmdb_id: str,
    payload: Mapping[str, Any],
    season: int | None,
) -> dict[str, Any] | None:
    media_type = str(payload.get("type") or "").strip()
    if task_key in _P7_LOCAL_SUBSCRIPTION_TASKS:
        return {
            "type": "mark_media_subscribed",
            "tmdb_id": tmdb_id,
            "media_type": "Season" if season is not None else ("Movie" if media_type == "电影" else "Series"),
            "season": season,
        }
    if task_key == _P7_RESUBSCRIBE_TASK:
        return {
            "type": "mark_resubscribe_subscribed",
            "tmdb_id": tmdb_id,
            "item_type": "Season" if season is not None else "Movie",
            "season": season if season is not None else -1,
        }
    return None


def _required_text(value: object, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"durable MoviePilot {field} must not be empty")
    return normalized


def _optional_nonnegative_int(value: object, field: str) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool) or not isinstance(value, int | str):
        raise ValueError(f"durable MoviePilot {field} must be a non-negative integer")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"durable MoviePilot {field} must be a non-negative integer") from exc
    if normalized < 0:
        raise ValueError(f"durable MoviePilot {field} must be a non-negative integer")
    return normalized


__all__ = [
    "bind_moviepilot_execution",
    "bind_moviepilot_operation_occurrence",
    "DurableMoviePilotEffectIndeterminateError",
    "MoviePilotExecutionBinding",
    "PersistentMoviePilotEffectGateway",
]
