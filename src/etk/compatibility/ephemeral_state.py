from __future__ import annotations

import threading
import time
from collections.abc import Hashable


class ExpiringSet:
    """Small process-local suppression set that needs no timer thread."""

    def __init__(self, *, default_ttl_seconds: float) -> None:
        if default_ttl_seconds <= 0:
            raise ValueError("default TTL must be positive")
        self._default_ttl_seconds = float(default_ttl_seconds)
        self._entries: dict[Hashable, float] = {}
        self._lock = threading.Lock()

    def add(self, value: Hashable) -> None:
        self.add_for(value, self._default_ttl_seconds)

    def add_for(self, value: Hashable, ttl_seconds: float) -> None:
        if ttl_seconds <= 0:
            raise ValueError("TTL must be positive")
        with self._lock:
            self._prune_locked()
            self._entries[value] = time.monotonic() + float(ttl_seconds)

    def discard(self, value: Hashable) -> None:
        with self._lock:
            self._entries.pop(value, None)

    def __contains__(self, value: object) -> bool:
        with self._lock:
            self._prune_locked()
            return value in self._entries

    def _prune_locked(self) -> None:
        now = time.monotonic()
        expired = tuple(value for value, deadline in self._entries.items() if deadline <= now)
        for value in expired:
            self._entries.pop(value, None)


__all__ = ["ExpiringSet"]
