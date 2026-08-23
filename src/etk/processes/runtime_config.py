from __future__ import annotations

import os
from dataclasses import dataclass
from math import isfinite

_DEFAULT_BROKER_URL = "redis://localhost:6379/0"
_DEFAULT_QUEUE_NAME = "legacy.serial"
_DEFAULT_HEARTBEAT_SECONDS = 5
_DEFAULT_LEASE_SECONDS = 30
_DEFAULT_COORDINATOR_INTERVAL_SECONDS = 1.0
_DEFAULT_OUTBOX_BATCH_SIZE = 50
_DEFAULT_OUTBOX_CLAIM_TIMEOUT_SECONDS = 60
_DEFAULT_WORKER_CONCURRENCY = 1
_DEFAULT_WORKER_SHUTDOWN_TIMEOUT_SECONDS = 30
_DEFAULT_JOB_TERMINATION_GRACE_SECONDS = 5.0
_DEFAULT_SCHEDULES_ENABLED = True


@dataclass(frozen=True, slots=True)
class TaskRuntimeConfig:
    """Environment-only settings for the task control plane.

    These values are process/platform configuration, not legacy application
    settings, so they intentionally do not participate in ``app_settings``
    round-trips.
    """

    broker_url: str = _DEFAULT_BROKER_URL
    queue_name: str = _DEFAULT_QUEUE_NAME
    heartbeat_seconds: int = _DEFAULT_HEARTBEAT_SECONDS
    lease_seconds: int = _DEFAULT_LEASE_SECONDS
    coordinator_interval_seconds: float = _DEFAULT_COORDINATOR_INTERVAL_SECONDS
    outbox_batch_size: int = _DEFAULT_OUTBOX_BATCH_SIZE
    outbox_claim_timeout_seconds: int = _DEFAULT_OUTBOX_CLAIM_TIMEOUT_SECONDS
    worker_concurrency: int = _DEFAULT_WORKER_CONCURRENCY
    worker_shutdown_timeout_seconds: int = _DEFAULT_WORKER_SHUTDOWN_TIMEOUT_SECONDS
    job_termination_grace_seconds: float = _DEFAULT_JOB_TERMINATION_GRACE_SECONDS
    schedules_enabled: bool = _DEFAULT_SCHEDULES_ENABLED

    @classmethod
    def from_environ(cls) -> TaskRuntimeConfig:
        return cls(
            broker_url=_required_url(os.environ.get("ETK_REDIS_URL", _DEFAULT_BROKER_URL)),
            queue_name=_nonempty(os.environ.get("ETK_TASK_QUEUE", _DEFAULT_QUEUE_NAME), "ETK_TASK_QUEUE"),
            heartbeat_seconds=_positive_int(
                os.environ.get("ETK_JOB_HEARTBEAT_SECONDS"),
                _DEFAULT_HEARTBEAT_SECONDS,
                "ETK_JOB_HEARTBEAT_SECONDS",
            ),
            lease_seconds=_positive_int(
                os.environ.get("ETK_JOB_LEASE_SECONDS"),
                _DEFAULT_LEASE_SECONDS,
                "ETK_JOB_LEASE_SECONDS",
            ),
            coordinator_interval_seconds=_positive_float(
                os.environ.get("ETK_COORDINATOR_INTERVAL_SECONDS"),
                _DEFAULT_COORDINATOR_INTERVAL_SECONDS,
                "ETK_COORDINATOR_INTERVAL_SECONDS",
            ),
            outbox_batch_size=_positive_int(
                os.environ.get("ETK_OUTBOX_BATCH_SIZE"),
                _DEFAULT_OUTBOX_BATCH_SIZE,
                "ETK_OUTBOX_BATCH_SIZE",
            ),
            outbox_claim_timeout_seconds=_positive_int(
                os.environ.get("ETK_OUTBOX_CLAIM_TIMEOUT_SECONDS"),
                _DEFAULT_OUTBOX_CLAIM_TIMEOUT_SECONDS,
                "ETK_OUTBOX_CLAIM_TIMEOUT_SECONDS",
            ),
            worker_concurrency=_positive_int(
                os.environ.get("ETK_WORKER_CONCURRENCY"),
                _DEFAULT_WORKER_CONCURRENCY,
                "ETK_WORKER_CONCURRENCY",
            ),
            worker_shutdown_timeout_seconds=_positive_int(
                os.environ.get("ETK_WORKER_SHUTDOWN_TIMEOUT_SECONDS"),
                _DEFAULT_WORKER_SHUTDOWN_TIMEOUT_SECONDS,
                "ETK_WORKER_SHUTDOWN_TIMEOUT_SECONDS",
            ),
            job_termination_grace_seconds=_positive_float(
                os.environ.get("ETK_JOB_TERMINATION_GRACE_SECONDS"),
                _DEFAULT_JOB_TERMINATION_GRACE_SECONDS,
                "ETK_JOB_TERMINATION_GRACE_SECONDS",
            ),
            schedules_enabled=_boolean(
                os.environ.get("ETK_COORDINATOR_SCHEDULES_ENABLED"),
                _DEFAULT_SCHEDULES_ENABLED,
                "ETK_COORDINATOR_SCHEDULES_ENABLED",
            ),
        )

    def __post_init__(self) -> None:
        if self.lease_seconds <= self.heartbeat_seconds:
            raise ValueError("job lease must be longer than the heartbeat interval")


def _required_url(value: str) -> str:
    normalized = _nonempty(value, "ETK_REDIS_URL")
    if not normalized.startswith(("redis://", "rediss://", "unix://")):
        raise ValueError("ETK_REDIS_URL must use redis, rediss, or unix scheme")
    return normalized


def _nonempty(value: str, name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    return normalized


def _positive_int(value: str | None, default: int, name: str) -> int:
    if value is None or not value.strip():
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if parsed < 1:
        raise ValueError(f"{name} must be positive")
    return parsed


def _positive_float(value: str | None, default: float, name: str) -> float:
    if value is None or not value.strip():
        return default
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if not isfinite(parsed) or parsed <= 0:
        raise ValueError(f"{name} must be positive")
    return parsed


def _boolean(value: str | None, default: bool, name: str) -> bool:
    if value is None or not value.strip():
        return default
    normalized = value.strip().casefold()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")
