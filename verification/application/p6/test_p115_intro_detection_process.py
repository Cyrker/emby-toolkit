from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from etk.domains.p115.intro_detection import (
    IntroBatchDisposition,
    IntroBatchResult,
    IntroDetectionClaimLostError,
    IntroDetectionIntent,
    IntroDetectionOperation,
    IntroDetectionOperationStatus,
    IntroDetectionPhase,
    IntroDetectionPolicySnapshot,
    IntroDetectionStateError,
    IntroEpisodeSnapshot,
    IntroJobTrigger,
    IntroTriggerMode,
)
from etk.processes.p6_intro_detection import P115IntroDetectionOperationExecutor


def _episode(number: int) -> IntroEpisodeSnapshot:
    return IntroEpisodeSnapshot.create(
        item_id=f"item-{number}",
        series_tmdb_id="series-42",
        season_number=1,
        episode_number=number,
        sha1=f"{number:040X}",
        pick_code=f"pick-{number}",
    )


def _operation() -> IntroDetectionOperation:
    now = datetime.now(UTC)
    intent = IntroDetectionIntent.create(
        account_id="115001",
        trigger=IntroJobTrigger.IMPORT,
        policy=IntroDetectionPolicySnapshot.create(
            trigger_mode=IntroTriggerMode.IMPORT,
            credits_enabled=True,
        ),
        episodes=(_episode(1), _episode(2)),
        available_at=now,
    )
    from uuid import uuid4

    return IntroDetectionOperation(
        id=uuid4(),
        sequence=1,
        operation_key=intent.operation_key,
        request_hash=intent.request_hash,
        source_set_revision=intent.source_set_revision,
        policy_revision=intent.policy_revision,
        series_tmdb_id=intent.series_tmdb_id,
        season_number=intent.season_number,
        algorithm_version=intent.algorithm_version,
        account_id=intent.account_id,
        trigger_mode=intent.trigger_mode,
        request=intent.request,
        status=IntroDetectionOperationStatus.CLAIMED,
        phase=IntroDetectionPhase.RESTORE,
        state_revision=1,
        progress={},
        attempts=1,
        available_at=now,
        claimed_by="fixture-worker",
        claim_token=uuid4(),
        claimed_at=now,
        lease_expires_at=now + timedelta(minutes=20),
        cancel_requested_at=None,
        result=None,
        error=None,
        finished_at=None,
        created_at=now,
        updated_at=now,
    )


@dataclass
class _Transition:
    operation: IntroDetectionOperation


class _Repository:
    def __init__(self, operation: IntroDetectionOperation) -> None:
        self.operation = operation
        self.transition: tuple[str, dict[str, Any]] | None = None

    def read(self, _operation_id: Any) -> IntroDetectionOperation:
        return self.operation

    def claim_by_id(self, _operation_id: Any, **_kwargs: Any) -> IntroDetectionOperation:
        return self.operation

    def heartbeat(self, *_args: Any, **_kwargs: Any) -> bool:
        return True

    def is_cancel_requested(self, *_args: Any, **_kwargs: Any) -> bool:
        return False

    def schedule_continuation(self, *_args: Any, **kwargs: Any) -> _Transition:
        self.transition = "continuation", kwargs
        self.operation = replace(
            self.operation,
            status=IntroDetectionOperationStatus.RETRYABLE,
            claimed_by=None,
            claim_token=None,
            claimed_at=None,
            lease_expires_at=None,
        )
        return _Transition(self.operation)

    def mark_confirmed(self, *_args: Any, **kwargs: Any) -> _Transition:
        self.transition = "confirmed", kwargs
        self.operation = replace(
            self.operation,
            status=IntroDetectionOperationStatus.CONFIRMED,
            claimed_by=None,
            claim_token=None,
            claimed_at=None,
            lease_expires_at=None,
        )
        return _Transition(self.operation)

    def mark_retryable(self, *_args: Any, **kwargs: Any) -> _Transition:
        self.transition = "retryable", kwargs
        self.operation = replace(
            self.operation,
            status=IntroDetectionOperationStatus.RETRYABLE,
            claimed_by=None,
            claim_token=None,
            claimed_at=None,
            lease_expires_at=None,
        )
        return _Transition(self.operation)

    def mark_rejected(self, *_args: Any, **kwargs: Any) -> _Transition:
        self.transition = "rejected", kwargs
        self.operation = replace(
            self.operation,
            status=IntroDetectionOperationStatus.REJECTED,
            claimed_by=None,
            claim_token=None,
            claimed_at=None,
            lease_expires_at=None,
        )
        return _Transition(self.operation)

    def mark_cancelled(self, *_args: Any, **kwargs: Any) -> _Transition:
        self.transition = "cancelled", kwargs
        self.operation = replace(
            self.operation,
            status=IntroDetectionOperationStatus.CANCELLED,
            claimed_by=None,
            claim_token=None,
            claimed_at=None,
            lease_expires_at=None,
        )
        return _Transition(self.operation)


class _Service:
    def __init__(self, result: IntroBatchResult) -> None:
        self.result = result

    def execute(self, _operation: IntroDetectionOperation, *, cancel: Any) -> IntroBatchResult:
        assert cancel() is False
        return self.result


class _RaisingService:
    def __init__(self, error: BaseException) -> None:
        self.error = error

    def execute(self, _operation: IntroDetectionOperation, *, cancel: Any) -> IntroBatchResult:
        assert cancel() is False
        raise self.error


def test_executor_requeues_same_operation_after_progressed_batch() -> None:
    operation = _operation()
    repository = _Repository(operation)
    service = _Service(
        IntroBatchResult(
            IntroBatchDisposition.CONTINUE,
            IntroDetectionPhase.PROJECT,
            True,
            progress={"intro_projected": 3},
        )
    )
    executor = P115IntroDetectionOperationExecutor(
        repository=cast(Any, repository),
        service=cast(Any, service),
        worker_id="fixture-worker",
    )

    result = executor.execute(operation.id)

    assert result is not None
    assert repository.transition is not None
    assert repository.transition[0] == "continuation"
    assert repository.transition[1]["delay_seconds"] == 3
    assert repository.transition[1]["progress"]["intro_projected"] == 3


def test_executor_confirms_only_after_batch_reports_durable_staging_complete() -> None:
    operation = _operation()
    repository = _Repository(operation)
    service = _Service(
        IntroBatchResult(
            IntroBatchDisposition.CONFIRMED,
            IntroDetectionPhase.PROJECT,
            False,
            result={"durable_effects_staged": True},
        )
    )
    executor = P115IntroDetectionOperationExecutor(
        repository=cast(Any, repository),
        service=cast(Any, service),
        worker_id="fixture-worker",
    )

    result = executor.execute(operation.id)

    assert result is not None
    assert repository.transition is not None
    assert repository.transition[0] == "confirmed"
    assert repository.transition[1]["result"] == {"durable_effects_staged": True}


def test_executor_retries_deterministic_state_error_instead_of_waiting_for_lease() -> None:
    operation = _operation()
    repository = _Repository(operation)
    executor = P115IntroDetectionOperationExecutor(
        repository=cast(Any, repository),
        service=cast(Any, _RaisingService(IntroDetectionStateError("missing MediaInfo cache"))),
        worker_id="fixture-worker",
    )

    result = executor.execute(operation.id)

    assert result is not None and result.status is IntroDetectionOperationStatus.RETRYABLE
    assert repository.transition is not None and repository.transition[0] == "retryable"
    assert repository.transition[1]["error"]["type"] == "intro_detection_state_error"


def test_executor_rejects_deterministic_state_error_when_attempt_budget_is_exhausted() -> None:
    operation = replace(_operation(), attempts=6)
    repository = _Repository(operation)
    executor = P115IntroDetectionOperationExecutor(
        repository=cast(Any, repository),
        service=cast(Any, _RaisingService(IntroDetectionStateError("invalid episode binding"))),
        worker_id="fixture-worker",
    )

    result = executor.execute(operation.id)

    assert result is not None and result.status is IntroDetectionOperationStatus.REJECTED
    assert repository.transition is not None and repository.transition[0] == "rejected"
    assert repository.transition[1]["error"]["attempts_exhausted"] is True


def test_executor_only_short_circuits_true_lost_claim_state_error() -> None:
    operation = _operation()
    repository = _Repository(operation)
    executor = P115IntroDetectionOperationExecutor(
        repository=cast(Any, repository),
        service=cast(Any, _RaisingService(IntroDetectionClaimLostError("stale claim"))),
        worker_id="fixture-worker",
    )

    result = executor.execute(operation.id)

    assert result == operation
    assert repository.transition is None
