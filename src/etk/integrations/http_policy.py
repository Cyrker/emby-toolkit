"""Small, dependency-light HTTP policy shared by P9 external adapters.

The adapters in this package deliberately keep provider-specific response
mapping at the edge.  This module only owns the transport boundary: bounded
timeouts, finite retries for idempotent reads, and a process-local circuit
breaker.  It never serializes a secret or stores a request body.
"""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, TypeVar

import httpx


class ProviderDisposition(StrEnum):
    """Provider outcome independent of any one external system."""

    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    NOT_FOUND = "not_found"
    RETRYABLE = "retryable"
    INDETERMINATE = "indeterminate"


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class ProviderResponse[T]:
    """Typed result returned by an external adapter."""

    disposition: ProviderDisposition
    status_code: int | None
    payload: Any = None
    retry_after_seconds: float | None = None

    @property
    def successful(self) -> bool:
        return self.disposition is ProviderDisposition.CONFIRMED


class ExternalAdapterError(RuntimeError):
    """A bounded transport failure with safe, non-sensitive context."""

    def __init__(
        self,
        message: str,
        *,
        disposition: ProviderDisposition,
        system: str,
        operation: str,
        status_code: int | None = None,
    ) -> None:
        self.disposition = disposition
        self.system = system
        self.operation = operation
        self.status_code = status_code
        super().__init__(str(message)[:500])


class CircuitOpenError(ExternalAdapterError):
    def __init__(self, *, system: str, operation: str) -> None:
        super().__init__(
            f"{system} circuit is open",
            disposition=ProviderDisposition.RETRYABLE,
            system=system,
            operation=operation,
        )


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Finite retry policy; retries are opt-in per request."""

    max_attempts: int = 3
    base_delay_seconds: float = 0.1
    max_delay_seconds: float = 2.0
    retry_statuses: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504})

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if not math.isfinite(self.base_delay_seconds) or self.base_delay_seconds < 0:
            raise ValueError("base_delay_seconds must be finite and non-negative")
        if not math.isfinite(self.max_delay_seconds) or self.max_delay_seconds < 0:
            raise ValueError("max_delay_seconds must be finite and non-negative")

    def delay(self, retry_number: int) -> float:
        """Return a bounded exponential delay for retry number 1..N."""

        number = max(1, int(retry_number))
        return min(self.max_delay_seconds, self.base_delay_seconds * (2 ** (number - 1)))


class CircuitBreaker:
    """Thread-safe closed/open/half-open breaker for one provider.

    Only transport and server-side failures should be recorded.  Provider
    validation errors (4xx) are caller mistakes and do not poison the circuit.
    """

    def __init__(
        self,
        *,
        failure_threshold: int = 3,
        reset_timeout_seconds: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be positive")
        if not math.isfinite(reset_timeout_seconds) or reset_timeout_seconds <= 0:
            raise ValueError("reset_timeout_seconds must be positive and finite")
        self._threshold = failure_threshold
        self._reset_timeout = reset_timeout_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._failures = 0
        self._opened_at: float | None = None
        self._half_open_probe = False

    @property
    def state(self) -> str:
        with self._lock:
            if self._opened_at is None:
                return "closed"
            if self._clock() - self._opened_at < self._reset_timeout:
                return "open"
            return "half_open" if self._half_open_probe else "open"

    @property
    def failures(self) -> int:
        with self._lock:
            return self._failures

    def allow(self) -> bool:
        with self._lock:
            now = self._clock()
            if self._opened_at is None:
                return True
            if now - self._opened_at < self._reset_timeout:
                return False
            if self._half_open_probe:
                return False
            self._half_open_probe = True
            return True

    def record_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._opened_at = None
            self._half_open_probe = False

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._failures >= self._threshold:
                self._opened_at = self._clock()
                self._half_open_probe = False

    def reset(self) -> None:
        self.record_success()


class HttpTransportPolicy:
    """Execute one httpx request with explicit retry and breaker semantics."""

    def __init__(
        self,
        *,
        system: str,
        retry_policy: RetryPolicy | None = None,
        breaker: CircuitBreaker | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.system = str(system or "external").strip() or "external"
        self.retry_policy = retry_policy or RetryPolicy()
        self.breaker = breaker or CircuitBreaker()
        self._sleep = sleep

    def request(
        self,
        invoke: Callable[[], httpx.Response],
        *,
        operation: str,
        retryable: bool = False,
    ) -> httpx.Response:
        operation_name = str(operation or "request").strip() or "request"
        if not self.breaker.allow():
            raise CircuitOpenError(system=self.system, operation=operation_name)
        policy = self.retry_policy
        last_error: BaseException | None = None
        for attempt in range(1, policy.max_attempts + 1):
            try:
                response = invoke()
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = exc
                self.breaker.record_failure()
                if retryable and attempt < policy.max_attempts:
                    self._sleep(policy.delay(attempt))
                    if not self.breaker.allow():
                        break
                    continue
                raise ExternalAdapterError(
                    f"{self.system} {operation_name} transport failed",
                    disposition=(ProviderDisposition.RETRYABLE if retryable else ProviderDisposition.INDETERMINATE),
                    system=self.system,
                    operation=operation_name,
                ) from exc
            if response.status_code in policy.retry_statuses:
                self.breaker.record_failure()
                if retryable and attempt < policy.max_attempts:
                    self._sleep(policy.delay(attempt))
                    if not self.breaker.allow():
                        break
                    continue
            else:
                # A response means the transport is healthy even when the
                # provider rejected the request.
                self.breaker.record_success()
            return response
        raise ExternalAdapterError(
            f"{self.system} {operation_name} transport is unavailable",
            disposition=ProviderDisposition.RETRYABLE if retryable else ProviderDisposition.INDETERMINATE,
            system=self.system,
            operation=operation_name,
        ) from last_error


def decode_json_object(response: httpx.Response, *, system: str, operation: str) -> dict[str, Any]:
    """Decode a JSON object and reject arrays/scalars without leaking body text."""

    try:
        payload: Any = response.json()
    except (TypeError, ValueError) as exc:
        raise ExternalAdapterError(
            f"{system} {operation} returned invalid JSON",
            disposition=ProviderDisposition.RETRYABLE,
            system=system,
            operation=operation,
            status_code=response.status_code,
        ) from exc
    if not isinstance(payload, dict):
        raise ExternalAdapterError(
            f"{system} {operation} returned a non-object payload",
            disposition=ProviderDisposition.RETRYABLE,
            system=system,
            operation=operation,
            status_code=response.status_code,
        )
    return dict(payload)


def classify_status(
    status_code: int,
    *,
    mutation: bool,
    not_found: bool = False,
    retry_statuses: Iterable[int] = RetryPolicy().retry_statuses,
) -> ProviderDisposition:
    if not_found and status_code == 404:
        return ProviderDisposition.NOT_FOUND
    if 200 <= status_code < 300:
        return ProviderDisposition.CONFIRMED
    if status_code in set(retry_statuses) or 300 <= status_code < 400:
        return ProviderDisposition.INDETERMINATE if mutation else ProviderDisposition.RETRYABLE
    if 400 <= status_code < 500:
        return ProviderDisposition.REJECTED
    return ProviderDisposition.INDETERMINATE if mutation else ProviderDisposition.RETRYABLE


__all__ = [
    "CircuitBreaker",
    "CircuitOpenError",
    "ExternalAdapterError",
    "HttpTransportPolicy",
    "ProviderDisposition",
    "ProviderResponse",
    "RetryPolicy",
    "classify_status",
    "decode_json_object",
]
