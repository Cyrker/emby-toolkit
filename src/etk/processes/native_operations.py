from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol


class NativeOperationProgress(Protocol):
    def checkpoint(self) -> None: ...

    def update(self, progress: int, message: str) -> None: ...

    def record_started_effect(self, label: str) -> None: ...

    def record_committed_effect(self, label: str) -> None: ...

    def record_abandoned_effect(self, label: str) -> None: ...


class NativeOperationHandler(Protocol):
    def execute(
        self,
        parameters: Mapping[str, Any],
        progress: NativeOperationProgress,
    ) -> Mapping[str, Any]: ...


class NativeOperationRouter:
    """Finite Worker dispatcher for processor-free internal operations."""

    def __init__(self, handlers: Mapping[str, NativeOperationHandler]) -> None:
        self._handlers = dict(handlers)

    def execute(
        self,
        operation_key: str,
        parameters: Mapping[str, Any],
        progress: NativeOperationProgress,
    ) -> Mapping[str, Any]:
        key = str(operation_key or "").strip().lower()
        handler = self._handlers.get(key)
        if handler is None:
            raise LookupError(f"native operation {key!r} is not registered")
        result = handler.execute(dict(parameters), progress)
        if not isinstance(result, Mapping):
            raise TypeError(f"native operation {key!r} returned a non-object result")
        return dict(result)


__all__ = [
    "NativeOperationHandler",
    "NativeOperationProgress",
    "NativeOperationRouter",
]
