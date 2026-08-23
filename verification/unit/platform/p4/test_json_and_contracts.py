from __future__ import annotations

import math
from uuid import uuid4

import pytest

from etk.platform.jobs import (
    AttemptStatus,
    InvalidJsonPayloadError,
    JobStatus,
    PostgresJobStore,
    canonical_json_hash,
    normalize_json_object,
)
from etk.platform.jobs.guard import _advisory_keys


class ForbiddenConnection:
    def execute(self, _statement: str) -> None:
        pass


def test_json_payload_round_trips_to_a_detached_plain_object() -> None:
    source = {"task": "sync", "nested": {"items": [1, True, None, 1.25]}, "中文": "保留"}

    normalized = normalize_json_object(source)
    source["nested"]["items"].append("mutated")

    assert normalized == {"task": "sync", "nested": {"items": [1, True, None, 1.25]}, "中文": "保留"}
    assert normalized is not source
    assert canonical_json_hash(normalized) == canonical_json_hash(
        {"中文": "保留", "nested": {"items": [1, True, None, 1.25]}, "task": "sync"}
    )


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {"callable": lambda: None},
        {"connection": ForbiddenConnection()},
        {"tuple": (1, 2)},
        {"nan": math.nan},
        {"inf": math.inf},
        {1: "non-string-key"},
    ],
)
def test_json_payload_rejects_non_json_or_lossy_values(payload) -> None:
    with pytest.raises(InvalidJsonPayloadError):
        normalize_json_object(payload)


def test_store_rejects_invalid_inputs_before_materializing_a_uow() -> None:
    def forbidden_uow():
        raise AssertionError("database must not be touched for an invalid DTO")

    store = PostgresJobStore(forbidden_uow)

    with pytest.raises(InvalidJsonPayloadError):
        store.create_job(task_key="demo", payload={"connection": ForbiddenConnection()})
    with pytest.raises(ValueError, match="max_attempts"):
        store.create_job(task_key="demo", payload={}, max_attempts=0)
    with pytest.raises(ValueError, match="provided together"):
        store.create_job(task_key="demo", payload={}, parent_job_id=uuid4())


def test_state_enums_expose_distinct_terminal_semantics() -> None:
    assert JobStatus.SUCCEEDED.terminal
    assert JobStatus.RETRY_WAIT.terminal is False
    assert AttemptStatus.LOST.terminal
    assert AttemptStatus.RUNNING.terminal is False


def test_advisory_keys_are_stable_signed_int32_values() -> None:
    job_id = uuid4()
    first = _advisory_keys(job_id)
    second = _advisory_keys(job_id)
    other = _advisory_keys(uuid4())

    assert first == second
    assert first != other
    assert all(-(2**31) <= value < 2**31 for value in first)


def test_log_redaction_helper_preserves_structure_without_secrets() -> None:
    from etk.platform.jobs.store import _normalized_log_entry

    assert _normalized_log_entry(
        {"message": "request token=visible-secret", "api_key": "visible-secret"},
        field="log",
    ) == {"message": "request token=<redacted>", "api_key": "<redacted>"}
