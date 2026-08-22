from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

RETRYABLE_TRANSACTION_SQLSTATES = frozenset({"40001", "40P01"})


@dataclass(frozen=True, slots=True)
class DatabaseRetryPolicy:
    """Bounded retry policy for whole serialization/deadlock transactions."""

    max_attempts: int = 3
    initial_delay_seconds: float = 0.05
    multiplier: float = 2.0
    max_delay_seconds: float = 0.5

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.initial_delay_seconds < 0 or self.max_delay_seconds < 0:
            raise ValueError("retry delays must not be negative")
        if self.multiplier < 1:
            raise ValueError("retry multiplier must be at least 1")

    def delay_after(self, failed_attempt: int) -> float:
        if failed_attempt < 1:
            raise ValueError("failed_attempt must be at least 1")
        delay = self.initial_delay_seconds * self.multiplier ** (failed_attempt - 1)
        return min(delay, self.max_delay_seconds)


_DEFAULT_RETRY_POLICY = DatabaseRetryPolicy()


def database_sqlstate(error: BaseException) -> str | None:
    """Find a psycopg SQLSTATE through SQLAlchemy's wrapped DBAPI error."""

    current: Any = error
    seen: set[int] = set()
    while isinstance(current, BaseException) and id(current) not in seen:
        seen.add(id(current))
        value = getattr(current, "sqlstate", None) or getattr(current, "pgcode", None)
        if value:
            return str(value)
        current = getattr(current, "orig", None) or current.__cause__
    return None


def run_retryable_transaction[T](
    operation: Callable[[], T],
    *,
    policy: DatabaseRetryPolicy | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Retry a callback that creates and closes a fresh UoW on every call.

    Only PostgreSQL serialization failures and deadlocks are retryable. Callers
    must keep external side effects outside ``operation`` and provide a new
    transaction boundary on every attempt.
    """

    resolved_policy = policy or _DEFAULT_RETRY_POLICY
    for attempt in range(1, resolved_policy.max_attempts + 1):
        try:
            return operation()
        except Exception as exc:
            if database_sqlstate(exc) not in RETRYABLE_TRANSACTION_SQLSTATES:
                raise
            if attempt >= resolved_policy.max_attempts:
                raise
            sleep(resolved_policy.delay_after(attempt))
    raise AssertionError("unreachable retry loop")
