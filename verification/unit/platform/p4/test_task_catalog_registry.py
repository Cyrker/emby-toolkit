from __future__ import annotations

import ast
import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import cast

import pytest

from etk.platform.tasks import (
    LEGACY_TASK_KEY_ORDER,
    P4_NATIVE_TASK_KEYS,
    P5_NATIVE_TASK_KEYS,
    P6_NATIVE_TASK_KEYS,
    P7_NATIVE_TASK_KEYS,
    LegacyTaskAdapter,
    ProcessorIdentity,
    TaskHandlerKind,
    TaskMessage,
    TaskPayload,
    TaskRegistry,
    TaskVisibility,
    task_definition_metadata,
)

REPO_ROOT = Path(__file__).resolve().parents[4]
ORDERED_TASK_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "tasks" / "ordered-task-catalog.json"
TASK_MANIFEST = REPO_ROOT / "verification" / "contracts" / "task-ownership.yaml"


def _registry() -> TaskRegistry:
    return TaskRegistry.from_fixture(ORDERED_TASK_FIXTURE)


def test_frozen_registry_preserves_all_50_legacy_definitions_and_context_orders() -> None:
    fixture = json.loads(ORDERED_TASK_FIXTURE.read_text(encoding="utf-8"))
    registry = _registry()
    definitions = registry.definitions()

    assert tuple(definition.key for definition in definitions) == LEGACY_TASK_KEY_ORDER
    assert len(definitions) == 50
    assert {definition.processor for definition in definitions} == set(ProcessorIdentity)
    assert {definition.key for definition in definitions if definition.handler_kind is TaskHandlerKind.NATIVE} == (
        P4_NATIVE_TASK_KEYS | P5_NATIVE_TASK_KEYS | P6_NATIVE_TASK_KEYS | P7_NATIVE_TASK_KEYS
    )
    assert sum(definition.handler_kind is TaskHandlerKind.LEGACY for definition in definitions) == 19
    assert fixture["metrics"]["native_handler_count"] == 31
    assert fixture["metrics"]["legacy_adapter_count"] == 19
    assert [definition.key for definition in registry.available(TaskVisibility.ALL)] == fixture["contexts"]["all"]
    assert [definition.key for definition in registry.available(TaskVisibility.CHAIN)] == fixture["contexts"]["chain"]
    assert [definition.key for definition in registry.available(TaskVisibility.MANUAL)] == fixture["contexts"]["manual"]
    assert registry.available("invalid") == registry.available(TaskVisibility.CHAIN)

    for definition in definitions:
        metadata = task_definition_metadata(definition)
        assert metadata["key"] == definition.key
        assert metadata["name"]
        assert metadata["help"]
        assert metadata["help_source"]
        assert metadata["legacy_function"]
        assert cast(str, metadata["legacy_signature"]).startswith(f"{definition.legacy_function}(")
        assert metadata["legacy_definition"]
        assert metadata["registry_source"]
        assert metadata["target_stage"]


def test_ordered_fixture_matches_the_manifest_and_legacy_source_signatures() -> None:
    fixture = json.loads(ORDERED_TASK_FIXTURE.read_text(encoding="utf-8"))
    manifest = json.loads(TASK_MANIFEST.read_text(encoding="utf-8"))
    manifest_by_key = {entry["task_key"]: entry for entry in manifest["tasks"]}
    core_tree = ast.parse((REPO_ROOT / "tasks" / "core.py").read_text(encoding="utf-8-sig"))
    full_registry = next(
        node.value
        for node in ast.walk(core_tree)
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "full_registry" for target in node.targets)
        and isinstance(node.value, ast.Dict)
    )
    help_texts = ast.literal_eval(
        next(
            node.value
            for node in ast.walk(core_tree)
            if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "TASK_HELP_TEXTS" for target in node.targets)
            and isinstance(node.value, ast.Dict)
        )
    )
    source_order = tuple(key.value for key in full_registry.keys if isinstance(key, ast.Constant))

    assert source_order == LEGACY_TASK_KEY_ORDER
    assert tuple(item["key"] for item in fixture["tasks"]) == source_order
    for index, item in enumerate(fixture["tasks"], 1):
        manifest_entry = manifest_by_key[item["key"]]
        assert item["order"] == index
        assert item["name"] == manifest_entry["display_name"]
        assert item["help"] == manifest_entry["help"]
        assert item["help_source"] == manifest_entry["help_source"]
        if item["help_source"] == "TASK_HELP_TEXTS":
            assert item["help"] == help_texts[item["key"]]
        assert item["processor"] == manifest_entry["processor_type"]
        assert item["visible_in"] == manifest_entry["visible_in"]
        assert item["chain_eligible"] is manifest_entry["chain_eligible"]
        assert item["legacy_function"] == manifest_entry["function"]
        assert item["registry_source"] == manifest_entry["source"]
        assert item["target_stage"] == manifest_entry["target_stage"]
        assert manifest_entry["status"] == ("new" if item["handler_kind"] == "native" else "legacy")
        if item["key"] in P7_NATIVE_TASK_KEYS:
            assert manifest_entry["new_owner"] == ("etk.processes.p7_subscription_tasks.P7SubscriptionTaskRouter")
            assert manifest_entry["ownership_layers"]["envelope_owner"] == manifest_entry["new_owner"]

        source_path, source_line = item["legacy_definition"].rsplit(":", 1)
        source_tree = ast.parse((REPO_ROOT / source_path).read_text(encoding="utf-8-sig"))
        function = next(
            node
            for node in ast.walk(source_tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.lineno == int(source_line)
        )
        return_annotation = f" -> {ast.unparse(function.returns)}" if function.returns is not None else ""
        assert item["legacy_signature"] == f"{function.name}({ast.unparse(function.args)}){return_annotation}"


def test_registry_creates_versioned_messages_from_symbolic_metadata_only() -> None:
    registry = _registry()

    message = registry.create_message(
        "manual_subscribe_batch",
        {"subscribe_requests": [{"tmdb_id": 123, "media_type": "movie"}]},
        metadata={"source": "http", "silent": False},
    )

    assert message.task_key == "manual_subscribe_batch"
    assert message.processor is ProcessorIdentity.MEDIA
    assert message.payload.version == 1
    assert message.to_dict() == {
        "task_key": "manual_subscribe_batch",
        "processor": "media",
        "payload": {
            "version": 1,
            "data": {"subscribe_requests": [{"tmdb_id": 123, "media_type": "movie"}]},
        },
        "metadata": {"source": "http", "silent": False},
    }
    assert TaskMessage.from_dict(message.to_dict()) == message


@pytest.mark.parametrize(
    "value",
    [
        ["top-level-list"],
        {"tuple": (1, 2)},
        {"set": {1, 2}},
        {"callable": lambda: None},
        {"not_finite": float("nan")},
        {"not_finite": float("inf")},
    ],
)
def test_payload_rejects_non_json_round_trip_values(value: object) -> None:
    with pytest.raises((TypeError, ValueError), match="JSON object|finite JSON|round-trip"):
        TaskPayload.create(cast(Mapping[str, object] | None, value))


def test_payload_rejects_closures_processor_objects_and_database_connections() -> None:
    captured = "runtime-state"

    def closure() -> str:
        return captured

    class Processor:
        pass

    class DatabaseConnection:
        pass

    for value in (closure, Processor(), DatabaseConnection()):
        with pytest.raises(TypeError, match="finite JSON"):
            TaskPayload.create({"value": value})


def test_message_rejects_non_object_payloads_and_unknown_transport_fields() -> None:
    with pytest.raises(TypeError, match="versioned JSON object"):
        TaskMessage.from_dict(
            {
                "task_key": "populate-metadata",
                "processor": "media",
                "payload": [],
            }
        )
    with pytest.raises(ValueError, match="unsupported fields"):
        TaskMessage.from_dict(
            {
                "task_key": "populate-metadata",
                "processor": "media",
                "payload": {"version": 1, "data": {}},
                "callable": "tasks.core.task_populate_metadata_cache",
            }
        )


def test_legacy_adapter_resolves_callable_and_processor_only_during_execute() -> None:
    registry = _registry()
    calls: list[tuple[object, ...]] = []
    resolved_processor = object()

    class Resolver:
        def resolve(
            self,
            task_key: str,
            processor: ProcessorIdentity,
        ) -> tuple[Callable[..., object], object]:
            calls.append(("resolve", task_key, processor))

            def handler(processor_instance: object, **payload: object) -> str:
                calls.append(("handler", processor_instance, payload))
                return "completed"

            return handler, resolved_processor

    adapter = LegacyTaskAdapter(registry, Resolver())
    assert calls == []

    message = registry.create_message("process-single-custom-collection", {"custom_collection_id": 42})
    result = adapter.execute(message)

    assert result == "completed"
    assert calls == [
        ("resolve", "process-single-custom-collection", ProcessorIdentity.MEDIA),
        ("handler", resolved_processor, {"custom_collection_id": 42}),
    ]


def test_legacy_adapter_rejects_native_keys_and_registry_mismatches_before_resolution() -> None:
    registry = _registry()

    class Resolver:
        def resolve(self, task_key: str, processor: ProcessorIdentity):
            raise AssertionError(f"resolver must not run for {task_key}/{processor}")

    adapter = LegacyTaskAdapter(registry, Resolver())

    with pytest.raises(ValueError, match="native task handler"):
        adapter.execute(registry.create_message("task-chain-high-freq"))

    wrong_processor = TaskMessage.create(
        task_key="process-single-custom-collection",
        processor=ProcessorIdentity.ACTOR,
    )
    with pytest.raises(ValueError, match="processor identity"):
        adapter.execute(wrong_processor)

    wrong_version = TaskMessage.create(
        task_key="process-single-custom-collection",
        processor=ProcessorIdentity.MEDIA,
        payload_version=2,
    )
    with pytest.raises(ValueError, match="payload version"):
        adapter.execute(wrong_version)


def test_transport_and_adapter_revalidate_mutated_payloads_before_resolution() -> None:
    registry = _registry()
    message = registry.create_message("process-single-custom-collection", {"custom_collection_id": 42})
    cast(dict[str, object], message.payload.data)["callable"] = lambda: None

    class Resolver:
        def resolve(self, task_key: str, processor: ProcessorIdentity):
            raise AssertionError(f"resolver must not run for {task_key}/{processor}")

    with pytest.raises(TypeError, match="finite JSON"):
        message.to_dict()
    with pytest.raises(TypeError, match="finite JSON"):
        LegacyTaskAdapter(registry, Resolver()).execute(message)


def test_registry_rejects_unknown_keys_without_resolving_executable_state() -> None:
    registry = _registry()

    with pytest.raises(KeyError, match="unknown task key"):
        registry.require("not-a-task")
    with pytest.raises(KeyError, match="unknown task key"):
        registry.create_message("not-a-task")
