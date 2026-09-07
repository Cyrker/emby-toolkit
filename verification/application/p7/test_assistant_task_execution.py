from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast
from uuid import uuid4

import pytest

from etk.domains.subscription.assistant_task_execution import (
    _assistant_history_identity,
    _assistant_query_found,
    _assistant_subscription_projection,
    _assistant_write_confirmed,
    _NativeAssistantSession,
)
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcomeStatus
from etk.processes.p7_subscription_tasks import P7SubscriptionTaskRouter


class _Progress:
    def __init__(self) -> None:
        self.checkpoints = 0
        self.updates: list[tuple[int, str]] = []

    def checkpoint(self) -> None:
        self.checkpoints += 1

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def success_reported(self) -> bool:
        return any(progress == 100 for progress, _message in self.updates)


@pytest.mark.parametrize("value", [{"found": False}, {"disposition": "empty"}])
def test_assistant_snapshot_readback_does_not_treat_empty_query_as_success(value: Mapping[str, Any]) -> None:
    assert _assistant_query_found(value) is False


def test_assistant_write_outcome_does_not_use_typed_result_truthiness() -> None:
    class _Result:
        def __init__(self, outcome: str) -> None:
            self.outcome = outcome

        def __bool__(self) -> bool:
            return True

    assert _assistant_write_confirmed(_Result("rejected")) is False
    assert _assistant_write_confirmed(_Result("confirmed")) is True
    with pytest.raises(RuntimeError, match="indeterminate"):
        _assistant_write_confirmed(_Result("indeterminate"))


def test_assistant_subscription_sync_is_bounded_and_excludes_provider_secrets() -> None:
    projected = _assistant_subscription_projection(
        {
            "id": 7,
            "tmdbid": "100",
            "season": 2,
            "name": "Fixture",
            "state": "R",
            "url": "https://provider.invalid/private",
            "cookie": "secret",
            "title": "x" * 700,
            "nested": {"token": "secret"},
        }
    )

    assert projected["id"] == 7
    assert projected["tmdbid"] == "100"
    assert projected["season"] == 2
    assert len(projected["title"]) == 500
    assert "url" not in projected
    assert "cookie" not in projected
    assert "nested" not in projected


def test_assistant_history_identity_deduplicates_ids_and_supports_idless_records() -> None:
    assert _assistant_history_identity({"id": 9, "title": "one"}) == "id:9"
    assert _assistant_history_identity({"pickcode": "pc", "fileid": "fid"}) == _assistant_history_identity(
        {"pick_code": "pc", "file_id": "fid"}
    )


def test_assistant_native_phase_plan_matches_enabled_features() -> None:
    base = _NativeAssistantSession(
        cast(Any, lambda: None),
        job_id=uuid4(),
        attempt_id=uuid4(),
        worker_id="fixture",
        provider=None,
        effects=None,
        configuration=lambda: {},
    )
    enabled = _NativeAssistantSession(
        cast(Any, lambda: None),
        job_id=uuid4(),
        attempt_id=uuid4(),
        worker_id="fixture",
        provider=None,
        effects=None,
        configuration=lambda: {"download_monitor_enabled": True, "verify_enabled": True},
    )

    assert "download_check" not in base.phases()
    assert base.phases()[-1] == "snapshot_verify"
    assert enabled.phases()[-2:] == ("download_check", "snapshot_verify")


class _NativeAssistantOwner:
    def __init__(self) -> None:
        self.calls: list[Mapping[str, Any]] = []

    def execute(self, **values: Any) -> Any:
        self.calls.append(dict(values))
        return type(
            "AssistantRun",
            (),
            {"completed_phases": ("snapshot_verify",), "stats": {"snapshots_checked": 1}},
        )()


def test_router_dispatches_assistant_without_resolving_a_legacy_processor() -> None:
    assistant = _NativeAssistantOwner()
    router = P7SubscriptionTaskRouter(compatibility=None, assistant=cast(Any, assistant))
    job_id = uuid4()
    attempt_id = uuid4()

    outcome = router.execute(
        TaskMessage.create(
            task_key="subscribe-assistant-maintenance",
            processor=ProcessorIdentity.WATCHLIST,
            metadata={
                "job_id": str(job_id),
                "attempt_id": str(attempt_id),
                "worker_id": "fixture-worker",
            },
        ),
        processor=None,
        progress=_Progress(),
    )

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["compatibility_fallback"] is False
    assert outcome.details["completed_phases"] == ["snapshot_verify"]
    assert assistant.calls[0]["job_id"] == job_id
    assert assistant.calls[0]["attempt_id"] == attempt_id
    assert assistant.calls[0]["processor"] is None
