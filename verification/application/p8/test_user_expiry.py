from __future__ import annotations

from datetime import UTC, datetime
from typing import cast
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from etk.domains.identity.effect_execution import (
    IdentityOperationExecution,
    IdentityUserMutationCoordinator,
)
from etk.domains.identity.operation_repository import IdentityOperationScopeBusyError
from etk.domains.identity.operations import (
    IdentityOperation,
    IdentityOperationKind,
    IdentityOperationReservation,
    IdentityOperationStatus,
    IdentityOperationStep,
)
from etk.domains.identity.user_expiry import IdentityUserExpiryService, UserExpiryRepository
from etk.platform.tasks import TaskOutcomeStatus

NOW = datetime(2026, 8, 29, 12, 0, tzinfo=UTC)
PARENT_JOB_ID = UUID("00000000-0000-0000-0000-000000000291")


class FakeProgress:
    def __init__(self, *, cancel_at_checkpoint: int | None = None) -> None:
        self.cancel_at_checkpoint = cancel_at_checkpoint
        self.checkpoints = 0
        self.updates: list[tuple[int, str]] = []

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        self.checkpoints += 1
        if self.checkpoints == self.cancel_at_checkpoint:
            raise RuntimeError("fixture parent cancellation")


class FakeRepository:
    def __init__(
        self,
        candidates: tuple[str, ...],
        reservations: dict[str, IdentityOperation | Exception | None] | None = None,
        *,
        query_error: Exception | None = None,
    ) -> None:
        self.candidates = candidates
        self.reservations = reservations or {}
        self.query_error = query_error
        self.reserve_calls: list[tuple[UUID, str, datetime | None]] = []
        self.operations: dict[UUID, IdentityOperation] = {}

    def expired_user_ids(self, parent_job_id: UUID) -> tuple[str, ...]:
        assert parent_job_id == PARENT_JOB_ID
        if self.query_error is not None:
            raise self.query_error
        return self.candidates

    def reserve_expired_user(
        self,
        parent_job_id: UUID,
        user_id: str,
        *,
        outbox_delay_seconds: float = 0,
        now: datetime | None = None,
    ) -> IdentityOperationReservation | None:
        assert outbox_delay_seconds == 0
        self.reserve_calls.append((parent_job_id, user_id, now))
        value = self.reservations.get(user_id, _operation(user_id, IdentityOperationStatus.READY))
        if isinstance(value, Exception):
            raise value
        if value is None:
            return None
        self.operations[value.id] = value
        return IdentityOperationReservation(value, created=True)

    def read(self, operation_id: UUID) -> IdentityOperation | None:
        return self.operations.get(operation_id)


class FakeMutations:
    def __init__(self, results: dict[str, IdentityOperation | Exception] | None = None) -> None:
        self.results = results or {}
        self.calls: list[tuple[UUID, str, float, datetime | None]] = []

    def execute(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> IdentityOperationExecution:
        self.calls.append((operation_id, worker_id, lease_seconds, now))
        user_id = _user_id(operation_id)
        value = self.results.get(user_id, _operation(user_id, IdentityOperationStatus.CONFIRMED))
        if isinstance(value, Exception):
            raise value
        return IdentityOperationExecution(value, claimed=True)


def _service(
    repository: FakeRepository,
    mutations: FakeMutations | None = None,
) -> tuple[IdentityUserExpiryService, FakeMutations]:
    executor = mutations or FakeMutations()
    return (
        IdentityUserExpiryService(
            cast(UserExpiryRepository, repository),
            cast(IdentityUserMutationCoordinator, executor),
            lease_seconds=lambda: 90,
            clock=lambda: NOW,
        ),
        executor,
    )


def _operation(user_id: str, status: IdentityOperationStatus) -> IdentityOperation:
    operation_id = uuid5(NAMESPACE_URL, f"fixture:identity-expiry:{user_id}")
    return IdentityOperation(
        id=operation_id,
        sequence=1,
        operation_key=operation_id.hex.ljust(64, "0"),
        request_hash=b"r" * 32,
        kind=IdentityOperationKind.EXPIRE_USER,
        scope_key=f"user:{user_id}",
        request={"user_id": user_id, "parent_job_id": str(PARENT_JOB_ID)},
        status=status,
        current_step=IdentityOperationStep.RESERVED,
        state_revision=0,
        attempts=0,
        available_at=NOW,
        claimed_by=None,
        claim_token=None,
        claimed_at=None,
        lease_expires_at=None,
        cancel_requested_at=None,
        step_started_at=None,
        state={},
        result=None,
        error=None,
        finished_at=NOW if status.terminal else None,
        created_at=NOW,
        updated_at=NOW,
    )


def _user_id(operation_id: UUID) -> str:
    for user_id in ("confirmed", "stale", "busy", "rejected", "pending", "cancelled", "first"):
        if _operation(user_id, IdentityOperationStatus.READY).id == operation_id:
            return user_id
    raise AssertionError(f"unknown fixture operation {operation_id}")


def test_query_failure_and_empty_candidate_set_have_explicit_outcomes() -> None:
    failed_service, failed_mutations = _service(FakeRepository((), query_error=RuntimeError("fixture query failure")))
    empty_service, empty_mutations = _service(FakeRepository(()))

    failed = failed_service.execute(
        FakeProgress(),
        parent_job_id=PARENT_JOB_ID,
        worker_id="fixture-worker",
    )
    empty = empty_service.execute(
        FakeProgress(),
        parent_job_id=PARENT_JOB_ID,
        worker_id="fixture-worker",
    )

    assert failed.status is TaskOutcomeStatus.FAILED
    assert failed.details == {"parent_job_id": str(PARENT_JOB_ID)}
    assert empty.status is TaskOutcomeStatus.SKIPPED
    assert empty.details == {"parent_job_id": str(PARENT_JOB_ID), "candidate_count": 0}
    assert failed_mutations.calls == []
    assert empty_mutations.calls == []


def test_mixed_child_states_return_retryable_partial_evidence() -> None:
    candidates = ("confirmed", "stale", "busy", "rejected", "pending")
    repository = FakeRepository(
        candidates,
        {
            "stale": None,
            "busy": IdentityOperationScopeBusyError("fixture busy scope"),
        },
    )
    mutations = FakeMutations(
        {
            "confirmed": _operation("confirmed", IdentityOperationStatus.CONFIRMED),
            "rejected": _operation("rejected", IdentityOperationStatus.REJECTED),
            "pending": _operation("pending", IdentityOperationStatus.AWAITING_READBACK),
        }
    )
    service, _executor = _service(repository, mutations)

    outcome = service.execute(
        FakeProgress(),
        parent_job_id=PARENT_JOB_ID,
        worker_id="fixture-worker",
    )

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details["candidate_count"] == 5
    assert outcome.details["successful"] == 1
    assert outcome.details["failed"] == 1
    assert outcome.details["pending"] == 1
    assert outcome.details["busy"] == 1
    assert outcome.details["skipped_due_to_state_change"] == 1
    assert outcome.details["failed_user_ids"] == ["rejected"]
    assert outcome.details["pending_user_ids"] == ["pending"]
    assert outcome.details["busy_user_ids"] == ["busy"]
    assert outcome.details["skipped_user_ids"] == ["stale"]
    assert all(call[1] == "fixture-worker:expiry" and call[2] == 90 for call in mutations.calls)


def test_confirmed_children_succeed_and_cancelled_state_drift_is_skipped() -> None:
    confirmed_service, _confirmed_mutations = _service(FakeRepository(("confirmed",)))
    cancelled_service, _cancelled_mutations = _service(
        FakeRepository(("cancelled",)),
        FakeMutations({"cancelled": _operation("cancelled", IdentityOperationStatus.CANCELLED)}),
    )

    confirmed = confirmed_service.execute(
        FakeProgress(),
        parent_job_id=PARENT_JOB_ID,
        worker_id="fixture-worker",
    )
    cancelled = cancelled_service.execute(
        FakeProgress(),
        parent_job_id=PARENT_JOB_ID,
        worker_id="fixture-worker",
    )

    assert confirmed.status is TaskOutcomeStatus.SUCCEEDED
    assert confirmed.details["successful"] == 1
    assert cancelled.status is TaskOutcomeStatus.SKIPPED
    assert cancelled.details["skipped_due_to_state_change"] == 1
    assert cancelled.details["skipped_user_ids"] == ["cancelled"]


def test_parent_cancellation_after_reservation_stops_new_children_but_keeps_reserved_child() -> None:
    repository = FakeRepository(("first", "confirmed"))
    service, mutations = _service(repository)
    progress = FakeProgress(cancel_at_checkpoint=3)

    with pytest.raises(RuntimeError, match="fixture parent cancellation"):
        service.execute(
            progress,
            parent_job_id=PARENT_JOB_ID,
            worker_id="fixture-worker",
        )

    assert [call[1] for call in repository.reserve_calls] == ["first"]
    assert len(repository.operations) == 1
    assert mutations.calls == []
