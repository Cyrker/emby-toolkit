from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from etk.platform.tasks.models import ProcessorIdentity, TaskHandlerKind, TaskMessage, TaskPayload
from etk.platform.tasks.registry import TaskRegistry

type LegacyTaskCallable = Callable[..., object]


class LegacyCompatibilityResolver(Protocol):
    """Worker-only resolver for legacy executable state."""

    def resolve(
        self,
        task_key: str,
        processor: ProcessorIdentity,
    ) -> tuple[LegacyTaskCallable, object]: ...


class LegacyTaskAdapter:
    """Invoke one symbolic legacy task without storing executable objects in messages."""

    def __init__(self, registry: TaskRegistry, resolver: LegacyCompatibilityResolver) -> None:
        self._registry = registry
        self._resolver = resolver

    def execute(self, message: TaskMessage) -> object:
        if not isinstance(message, TaskMessage):
            raise TypeError("legacy task adapter requires a TaskMessage")
        definition = self._registry.require(message.task_key)
        if definition.handler_kind is TaskHandlerKind.NATIVE:
            raise ValueError(f"task {message.task_key!r} requires a native P4 handler")
        if message.processor is not definition.processor:
            raise ValueError(f"task {message.task_key!r} processor identity does not match its registry definition")
        if message.payload.version != definition.payload_version:
            raise ValueError(f"task {message.task_key!r} payload version is not supported")

        payload = TaskPayload.create(message.payload.data, version=message.payload.version).data
        task_function, processor = self._resolver.resolve(definition.key, definition.processor)
        if not callable(task_function):
            raise TypeError(f"legacy resolver returned a non-callable for task {message.task_key!r}")
        if processor is None:
            raise RuntimeError(f"legacy resolver returned no processor for task {message.task_key!r}")
        return task_function(processor, **payload)
