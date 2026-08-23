from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]


class ProcessorIdentity(StrEnum):
    MEDIA = "media"
    WATCHLIST = "watchlist"
    ACTOR = "actor"


class TaskVisibility(StrEnum):
    ALL = "all"
    CHAIN = "chain"
    MANUAL = "manual"


class TaskHandlerKind(StrEnum):
    NATIVE = "native"
    LEGACY = "legacy"


@dataclass(frozen=True, slots=True)
class TaskDefinition:
    key: str
    name: str
    help: str
    help_source: str
    processor: ProcessorIdentity
    visible_in: tuple[TaskVisibility, ...]
    chain_eligible: bool
    payload_version: int
    legacy_function: str
    legacy_signature: str
    legacy_definition: str
    registry_source: str
    target_stage: str
    handler_kind: TaskHandlerKind

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> TaskDefinition:
        key = _stable_task_key(value.get("key"))
        visible_in = tuple(TaskVisibility(item) for item in value.get("visible_in", ()))
        if not visible_in or len(set(visible_in)) != len(visible_in):
            raise ValueError(f"task {key!r} must declare unique visibility contexts")
        chain_eligible = value.get("chain_eligible")
        if not isinstance(chain_eligible, bool):
            raise TypeError(f"task {key!r} chain_eligible must be boolean")
        return cls(
            key=key,
            name=_required_text(value, "name", key),
            help=_required_text(value, "help", key),
            help_source=_required_text(value, "help_source", key),
            processor=ProcessorIdentity(value.get("processor")),
            visible_in=visible_in,
            chain_eligible=chain_eligible,
            payload_version=_positive_version(value.get("payload_version"), label=f"task {key!r} payload_version"),
            legacy_function=_required_text(value, "legacy_function", key),
            legacy_signature=_required_text(value, "legacy_signature", key),
            legacy_definition=_required_text(value, "legacy_definition", key),
            registry_source=_required_text(value, "registry_source", key),
            target_stage=_required_text(value, "target_stage", key),
            handler_kind=TaskHandlerKind(value.get("handler_kind")),
        )


@dataclass(frozen=True, slots=True)
class TaskPayload:
    version: int
    data: dict[str, JsonValue]

    @classmethod
    def create(
        cls,
        data: Mapping[str, object] | None = None,
        *,
        version: int = 1,
    ) -> TaskPayload:
        return cls(
            version=_positive_version(version, label="payload version"),
            data=_json_object(data or {}, label="task payload"),
        )

    def to_dict(self) -> dict[str, object]:
        return {"version": self.version, "data": _json_object(self.data, label="task payload")}


@dataclass(frozen=True, slots=True)
class TaskMessage:
    task_key: str
    processor: ProcessorIdentity
    payload: TaskPayload
    metadata: dict[str, JsonValue]

    @classmethod
    def create(
        cls,
        *,
        task_key: str,
        processor: ProcessorIdentity | str,
        payload: Mapping[str, object] | None = None,
        payload_version: int = 1,
        metadata: Mapping[str, object] | None = None,
    ) -> TaskMessage:
        return cls(
            task_key=_stable_task_key(task_key),
            processor=ProcessorIdentity(processor),
            payload=TaskPayload.create(payload, version=payload_version),
            metadata=_json_object(metadata or {}, label="task metadata"),
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> TaskMessage:
        if not isinstance(value, Mapping):
            raise TypeError("task message must be a JSON object")
        allowed = {"task_key", "processor", "payload", "metadata"}
        extras = set(value) - allowed
        if extras:
            raise ValueError(f"task message contains unsupported fields: {sorted(extras)!r}")
        payload = value.get("payload")
        if not isinstance(payload, Mapping):
            raise TypeError("task message payload must be a versioned JSON object")
        payload_extras = set(payload) - {"version", "data"}
        if payload_extras:
            raise ValueError(f"task payload contains unsupported fields: {sorted(payload_extras)!r}")
        data = payload.get("data")
        if not isinstance(data, Mapping):
            raise TypeError("task payload data must be a JSON object")
        metadata = value.get("metadata", {})
        if not isinstance(metadata, Mapping):
            raise TypeError("task metadata must be a JSON object")
        return cls.create(
            task_key=str(value.get("task_key") or ""),
            processor=str(value.get("processor") or ""),
            payload=data,
            payload_version=_positive_version(payload.get("version"), label="payload version"),
            metadata=metadata,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "task_key": self.task_key,
            "processor": self.processor.value,
            "payload": self.payload.to_dict(),
            "metadata": _json_object(self.metadata, label="task metadata"),
        }


def _json_object(value: Mapping[str, object], *, label: str) -> dict[str, JsonValue]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{label} must be a JSON object")
    plain = dict(value)
    if not all(isinstance(key, str) for key in plain):
        raise TypeError(f"{label} keys must be strings")
    try:
        encoded = json.dumps(
            plain,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        decoded = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{label} must contain only finite JSON values") from exc
    if not isinstance(decoded, dict) or decoded != plain:
        raise TypeError(f"{label} must round-trip as a JSON object without type coercion")
    return decoded


def _stable_task_key(value: object) -> str:
    key = str(value or "").strip()
    if not key or any(character.isspace() for character in key):
        raise ValueError("task key must be a non-empty stable identifier")
    return key


def _positive_version(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _required_text(value: Mapping[str, Any], field: str, task_key: str) -> str:
    text = str(value.get(field) or "").strip()
    if not text:
        raise ValueError(f"task {task_key!r} must declare {field}")
    return text
