from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from psycopg import Connection
from psycopg_pool import ConnectionPool
from sqlalchemy.engine import make_url

from .engine import normalize_sqlalchemy_url


@dataclass(frozen=True, slots=True)
class PsycopgPoolConfig:
    """Dedicated low-level psycopg pool settings; credentials stay out of repr."""

    url: str
    min_size: int = 1
    max_size: int = 5
    timeout_seconds: float = 30.0
    open_timeout_seconds: float = 30.0
    close_timeout_seconds: float = 10.0
    application_name: str = "etk-raw"

    def __post_init__(self) -> None:
        normalize_sqlalchemy_url(self.url)
        if self.min_size < 0:
            raise ValueError("min_size must not be negative")
        if self.max_size < 1 or self.max_size < self.min_size:
            raise ValueError("max_size must be at least min_size and at least 1")
        if self.timeout_seconds <= 0 or self.open_timeout_seconds <= 0 or self.close_timeout_seconds <= 0:
            raise ValueError("pool timeouts must be positive")

    def __repr__(self) -> str:
        safe_url = make_url(self.url).render_as_string(hide_password=True)
        return (
            "PsycopgPoolConfig("
            f"url={safe_url!r}, min_size={self.min_size}, max_size={self.max_size}, "
            f"timeout_seconds={self.timeout_seconds}, open_timeout_seconds={self.open_timeout_seconds}, "
            f"close_timeout_seconds={self.close_timeout_seconds}, "
            f"application_name={self.application_name!r})"
        )


def _psycopg_conninfo(value: str) -> str:
    url = normalize_sqlalchemy_url(value)
    return url.set(drivername="postgresql").render_as_string(hide_password=False)


class PsycopgPool:
    """Lifecycle wrapper for low-level PostgreSQL operations.

    SQLAlchemy repositories do not borrow from this pool. Use it only for
    PostgreSQL-specific primitives that genuinely need a raw psycopg connection.
    """

    def __init__(self, config: PsycopgPoolConfig):
        self._config = config
        self._condition = threading.Condition()
        self._close_lock = threading.Lock()
        self._active_leases = 0
        self._closing = False
        self._pool = ConnectionPool(
            conninfo=_psycopg_conninfo(config.url),
            min_size=config.min_size,
            max_size=config.max_size,
            timeout=config.timeout_seconds,
            kwargs={"autocommit": False, "application_name": config.application_name},
            open=False,
        )

    @property
    def closed(self) -> bool:
        return self._pool.closed

    @property
    def active_leases(self) -> int:
        with self._condition:
            return self._active_leases

    def open(self) -> None:
        if self._pool.closed:
            self._pool.open(wait=True, timeout=self._config.open_timeout_seconds)

    @contextmanager
    def connection(self, *, timeout: float | None = None) -> Iterator[Connection[Any]]:
        with self._condition:
            if self._closing:
                raise RuntimeError("psycopg pool is closing")
            if self._pool.closed:
                raise RuntimeError("psycopg pool is not open")
            self._active_leases += 1
        try:
            with self._pool.connection(timeout=timeout) as connection:
                yield connection
        finally:
            with self._condition:
                self._active_leases -= 1
                self._condition.notify_all()

    def close(self, *, timeout: float | None = None) -> None:
        drain_timeout = self._config.close_timeout_seconds if timeout is None else timeout
        if drain_timeout <= 0:
            raise ValueError("close timeout must be positive")
        with self._close_lock:
            with self._condition:
                if self._pool.closed:
                    return
                self._closing = True
                deadline = time.monotonic() + drain_timeout
                while self._active_leases:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self._closing = False
                        self._condition.notify_all()
                        raise TimeoutError(f"psycopg pool still has {self._active_leases} active lease(s)")
                    self._condition.wait(remaining)
            try:
                self._pool.close()
            finally:
                with self._condition:
                    self._closing = False
                    self._condition.notify_all()
