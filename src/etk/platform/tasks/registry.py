from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Protocol

from .catalog import LEGACY_TASK_KEY_ORDER, NATIVE_TASK_KEYS
from .models import (
    JsonValue,
    TaskDefinition,
    TaskHandlerKind,
    TaskMessage,
    TaskVisibility,
)


class NativeTaskHandler(Protocol):
    def execute(self, message: TaskMessage) -> object: ...


class TaskRegistry:
    """Worker-side symbolic task registry with no executable imports."""

    def __init__(self, definitions: tuple[TaskDefinition, ...]) -> None:
        keys = tuple(definition.key for definition in definitions)
        if keys != LEGACY_TASK_KEY_ORDER:
            raise ValueError("task definitions must match the frozen legacy 50-key order")
        native_keys = {
            definition.key for definition in definitions if definition.handler_kind is TaskHandlerKind.NATIVE
        }
        if native_keys != NATIVE_TASK_KEYS:
            raise ValueError("native task definitions must match the staged native task catalog")
        self._definitions = definitions
        self._by_key = {definition.key: definition for definition in definitions}

    @classmethod
    def from_fixture(cls, path: Path) -> TaskRegistry:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("schema_version") != 1:
            raise ValueError("ordered task fixture schema version is not supported")
        tasks = payload.get("tasks")
        if not isinstance(tasks, list):
            raise TypeError("ordered task fixture must contain a tasks list")
        if any(not isinstance(item, Mapping) or item.get("order") != index for index, item in enumerate(tasks, 1)):
            raise ValueError("ordered task fixture must declare consecutive one-based task order")
        definitions = tuple(TaskDefinition.from_dict(item) for item in tasks)
        registry = cls(definitions)
        contexts = payload.get("contexts")
        if not isinstance(contexts, Mapping):
            raise TypeError("ordered task fixture must contain context key lists")
        for context in TaskVisibility:
            expected = list(contexts.get(context.value, ()))
            actual = [definition.key for definition in registry.available(context)]
            if actual != expected:
                raise ValueError(f"ordered task fixture context {context.value!r} does not match task metadata")
        return registry

    def definitions(self) -> tuple[TaskDefinition, ...]:
        return self._definitions

    def available(self, context: TaskVisibility | str = TaskVisibility.CHAIN) -> tuple[TaskDefinition, ...]:
        try:
            normalized = TaskVisibility(str(context or TaskVisibility.CHAIN.value).strip().lower())
        except ValueError:
            normalized = TaskVisibility.CHAIN
        return tuple(definition for definition in self._definitions if normalized in definition.visible_in)

    def require(self, task_key: str) -> TaskDefinition:
        try:
            return self._by_key[task_key]
        except KeyError:
            raise KeyError(f"unknown task key: {task_key}") from None

    def create_message(
        self,
        task_key: str,
        payload: Mapping[str, object] | None = None,
        *,
        payload_version: int | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> TaskMessage:
        definition = self.require(task_key)
        return TaskMessage.create(
            task_key=definition.key,
            processor=definition.processor,
            payload=payload,
            payload_version=payload_version if payload_version is not None else definition.payload_version,
            metadata=metadata,
        )


def task_definition_metadata(definition: TaskDefinition) -> dict[str, JsonValue]:
    return {
        "key": definition.key,
        "name": definition.name,
        "help": definition.help,
        "help_source": definition.help_source,
        "processor": definition.processor.value,
        "visible_in": [context.value for context in definition.visible_in],
        "chain_eligible": definition.chain_eligible,
        "payload_version": definition.payload_version,
        "legacy_function": definition.legacy_function,
        "legacy_signature": definition.legacy_signature,
        "legacy_definition": definition.legacy_definition,
        "registry_source": definition.registry_source,
        "target_stage": definition.target_stage,
        "handler_kind": definition.handler_kind.value,
    }
