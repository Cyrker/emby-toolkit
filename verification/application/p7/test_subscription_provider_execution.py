from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from etk.domains.subscription.moviepilot import (
    MoviePilotProviderError,
    MoviePilotProviderErrorKind,
    MoviePilotQueryDisposition,
    MoviePilotQueryResult,
    MoviePilotWriteOutcome,
    MoviePilotWriteResult,
)
from etk.domains.subscription.operations import (
    SubscriptionOperation,
    SubscriptionOperationKind,
    SubscriptionOperationStatus,
)
from etk.domains.subscription.provider_execution import SubscriptionOperationExecutor

NOW = datetime(2026, 8, 26, tzinfo=UTC)


class FakeOperationRepository:
    def __init__(self, operation: SubscriptionOperation) -> None:
        self.operation = operation
        self.claim_token = uuid4()

    def request_cancel(self, *, now: datetime = NOW) -> SubscriptionOperation:
        if self.operation.provider_request_started_at is None:
            self.operation = replace(
                self.operation,
                status=SubscriptionOperationStatus.CANCELLED,
                claimed_by=None,
                claim_token=None,
                claimed_at=None,
                lease_expires_at=None,
                cancel_requested_at=now,
                error={"type": "cancel_requested_before_provider_request"},
                finished_at=now,
            )
        else:
            self.operation = replace(self.operation, cancel_requested_at=now)
        return self.operation

    def read(self, operation_id: UUID) -> SubscriptionOperation | None:
        return self.operation if operation_id == self.operation.id else None

    def claim_by_id(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> SubscriptionOperation | None:
        if operation_id != self.operation.id or self.operation.terminal:
            return None
        if self.operation.status not in {
            SubscriptionOperationStatus.READY,
            SubscriptionOperationStatus.RETRYABLE,
            SubscriptionOperationStatus.AWAITING_READBACK,
        }:
            return None
        timestamp = now or NOW
        self.claim_token = uuid4()
        self.operation = replace(
            self.operation,
            status=SubscriptionOperationStatus.CLAIMED,
            attempts=self.operation.attempts + 1,
            claimed_by=worker_id,
            claim_token=self.claim_token,
            claimed_at=timestamp,
            lease_expires_at=timestamp + timedelta(seconds=lease_seconds),
        )
        return self.operation

    def mark_requesting(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        now: datetime | None = None,
    ) -> SubscriptionOperation:
        self._claim(operation_id, claim_token)
        self.operation = replace(
            self.operation,
            status=SubscriptionOperationStatus.REQUESTING,
            provider_request_started_at=now or NOW,
        )
        return self.operation

    def mark_awaiting_readback(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any] | None = None,
        retry_after_seconds: float = 0,
        now: datetime | None = None,
    ) -> SubscriptionOperation:
        self._claim(operation_id, claim_token)
        self.operation = replace(
            self.operation,
            status=SubscriptionOperationStatus.AWAITING_READBACK,
            claimed_by=None,
            claim_token=None,
            claimed_at=None,
            lease_expires_at=None,
            error=dict(error) if error is not None else None,
            available_at=(now or NOW) + timedelta(seconds=retry_after_seconds),
        )
        return self.operation

    def mark_remote_confirmed(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        receipt: Mapping[str, Any],
        now: datetime | None = None,
    ) -> SubscriptionOperation:
        self._claim(operation_id, claim_token)
        self.operation = replace(
            self.operation,
            status=SubscriptionOperationStatus.REMOTE_CONFIRMED,
            provider_receipt=dict(receipt),
        )
        return self.operation

    def mark_confirmed(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        result: Mapping[str, Any],
        readback: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> SubscriptionOperation:
        return self._terminal(
            operation_id,
            claim_token,
            SubscriptionOperationStatus.CONFIRMED,
            result=dict(result),
            readback=dict(readback) if readback is not None else None,
        )

    def mark_rejected(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
        now: datetime | None = None,
    ) -> SubscriptionOperation:
        return self._terminal(
            operation_id,
            claim_token,
            SubscriptionOperationStatus.REJECTED,
            error=dict(error),
        )

    def mark_indeterminate(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        error: Mapping[str, Any],
        readback: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> SubscriptionOperation:
        return self._terminal(
            operation_id,
            claim_token,
            SubscriptionOperationStatus.INDETERMINATE,
            error=dict(error),
            readback=dict(readback) if readback is not None else None,
        )

    def mark_cancelled_before_write(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        evidence: Mapping[str, Any],
        now: datetime | None = None,
    ) -> SubscriptionOperation:
        return self._terminal(
            operation_id,
            claim_token,
            SubscriptionOperationStatus.CANCELLED,
            error={"type": "cancelled_before_provider_write"},
            readback=dict(evidence),
        )

    def is_cancel_requested(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        now: datetime | None = None,
    ) -> bool:
        return (
            operation_id != self.operation.id
            or claim_token != self.operation.claim_token
            or self.operation.cancel_requested_at is not None
        )

    def _claim(self, operation_id: UUID, claim_token: UUID) -> None:
        assert operation_id == self.operation.id
        assert claim_token == self.operation.claim_token

    def _terminal(
        self,
        operation_id: UUID,
        claim_token: UUID,
        status: SubscriptionOperationStatus,
        *,
        result: dict[str, Any] | None = None,
        readback: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
    ) -> SubscriptionOperation:
        self._claim(operation_id, claim_token)
        self.operation = replace(
            self.operation,
            status=status,
            claimed_by=None,
            claim_token=None,
            claimed_at=None,
            lease_expires_at=None,
            result=result,
            readback=readback,
            error=error,
            finished_at=NOW,
        )
        return self.operation


class FakeMoviePilotGateway:
    def __init__(self) -> None:
        self.writes: list[tuple[str, Any]] = []
        self.queries: list[tuple[str, int | None]] = []
        self.write_error: Exception | None = None
        self.on_write: Callable[[], object] | None = None
        self.query_errors: list[Exception] = []
        self.query_results: list[MoviePilotQueryResult] = []

    def query_subscription(self, tmdb_id: int | str, *, season: int | None = None) -> MoviePilotQueryResult:
        self.queries.append((str(tmdb_id), season))
        if self.query_errors:
            raise self.query_errors.pop(0)
        if self.query_results:
            return self.query_results.pop(0)
        return MoviePilotQueryResult(MoviePilotQueryDisposition.EMPTY, None)

    def create_subscription(self, payload: Mapping[str, Any]) -> MoviePilotWriteResult:
        return self._write("create", dict(payload))

    def update_subscription(self, payload: Mapping[str, Any]) -> MoviePilotWriteResult:
        return self._write("update", dict(payload))

    def update_subscription_status(
        self,
        tmdb_id: int | str,
        status: str,
        *,
        season: int | None = None,
        total_episodes: int | None = None,
    ) -> MoviePilotWriteResult:
        return self._write("update_status", (str(tmdb_id), status, season, total_episodes))

    def delete_subscription(self, tmdb_id: int | str, *, season: int | None = None) -> MoviePilotWriteResult:
        return self._write("delete", (str(tmdb_id), season))

    def _write(self, action: str, payload: Any) -> MoviePilotWriteResult:
        self.writes.append((action, payload))
        if self.on_write is not None:
            self.on_write()
        if self.write_error is not None:
            raise self.write_error
        return MoviePilotWriteResult(f"{action}-subscription", 200)


def _operation(request: dict[str, Any]) -> SubscriptionOperation:
    return SubscriptionOperation(
        id=uuid4(),
        sequence=1,
        operation_key="a" * 64,
        request_hash=b"a" * 32,
        kind=SubscriptionOperationKind.CREATE,
        scope_key="series:101:season:2",
        request=request,
        status=SubscriptionOperationStatus.READY,
        state_revision=0,
        attempts=0,
        available_at=NOW,
        claimed_by=None,
        claim_token=None,
        claimed_at=None,
        lease_expires_at=None,
        cancel_requested_at=None,
        provider_request_started_at=None,
        provider_receipt=None,
        readback=None,
        result=None,
        error=None,
        finished_at=None,
        created_at=NOW,
        updated_at=NOW,
    )


def _found(**values: Any) -> MoviePilotQueryResult:
    return MoviePilotQueryResult(MoviePilotQueryDisposition.FOUND, values)


def _provider_error(
    kind: MoviePilotProviderErrorKind,
    *,
    outcome: MoviePilotWriteOutcome | None,
) -> MoviePilotProviderError:
    return MoviePilotProviderError(kind, "fixture-operation", "fixture error", outcome=outcome)


def test_confirmed_create_is_read_back_and_persists_only_a_safe_projection() -> None:
    repository = FakeOperationRepository(
        _operation(
            {
                "action": "create",
                "tmdb_id": "101",
                "season": 2,
                "payload": {"tmdbid": 101, "season": 2, "name": "Fixture"},
                "expected": {"state": "R"},
            }
        )
    )
    gateway = FakeMoviePilotGateway()
    gateway.query_results = [_found(id=77, tmdbid=101, season=2, state="R", access_token="secret")]

    result = SubscriptionOperationExecutor(repository, gateway).execute(
        repository.operation.id,
        worker_id="fixture-worker",
        lease_seconds=60,
        now=NOW,
    )

    assert result is not None and result.operation.status is SubscriptionOperationStatus.CONFIRMED
    assert gateway.writes == [("create", {"tmdbid": 101, "season": 2, "name": "Fixture"})]
    assert result.operation.readback == {
        "disposition": "found",
        "subscription": {"id": 77, "tmdbid": 101, "season": 2, "state": "R"},
    }


def test_indeterminate_write_recovers_by_readback_without_resending() -> None:
    repository = FakeOperationRepository(_operation({"action": "create", "tmdb_id": "101", "payload": {"tmdbid": 101}}))
    gateway = FakeMoviePilotGateway()
    gateway.write_error = _provider_error(
        MoviePilotProviderErrorKind.TIMEOUT,
        outcome=MoviePilotWriteOutcome.INDETERMINATE,
    )
    gateway.query_errors = [
        _provider_error(MoviePilotProviderErrorKind.TIMEOUT, outcome=None),
    ]
    executor = SubscriptionOperationExecutor(repository, gateway, readback_delay_seconds=0)

    first = executor.execute(repository.operation.id, worker_id="fixture-worker", lease_seconds=60, now=NOW)
    assert first is not None and first.operation.status is SubscriptionOperationStatus.AWAITING_READBACK
    assert len(gateway.writes) == 1

    gateway.write_error = None
    gateway.query_results = [_found(id=77, tmdbid=101)]
    recovered = executor.execute(repository.operation.id, worker_id="fixture-worker", lease_seconds=60, now=NOW)

    assert recovered is not None and recovered.operation.status is SubscriptionOperationStatus.CONFIRMED
    assert len(gateway.writes) == 1
    assert len(gateway.queries) == 2


def test_readback_budget_terminalizes_unknown_effect_as_indeterminate() -> None:
    repository = FakeOperationRepository(
        _operation({"action": "update", "tmdb_id": "101", "payload": {"id": 77, "name": "Fixture"}})
    )
    gateway = FakeMoviePilotGateway()
    gateway.write_error = _provider_error(
        MoviePilotProviderErrorKind.TRANSPORT,
        outcome=MoviePilotWriteOutcome.INDETERMINATE,
    )
    gateway.query_errors = [
        _provider_error(MoviePilotProviderErrorKind.TIMEOUT, outcome=None),
        _provider_error(MoviePilotProviderErrorKind.TIMEOUT, outcome=None),
    ]
    executor = SubscriptionOperationExecutor(repository, gateway, max_attempts=2, readback_delay_seconds=0)

    first = executor.execute(repository.operation.id, worker_id="fixture-worker", lease_seconds=60, now=NOW)
    second = executor.execute(repository.operation.id, worker_id="fixture-worker", lease_seconds=60, now=NOW)

    assert first is not None and first.operation.status is SubscriptionOperationStatus.AWAITING_READBACK
    assert second is not None and second.operation.status is SubscriptionOperationStatus.INDETERMINATE
    assert len(gateway.writes) == 1


def test_explicit_provider_rejection_releases_without_readback() -> None:
    repository = FakeOperationRepository(_operation({"action": "create", "tmdb_id": "101", "payload": {"tmdbid": 101}}))
    gateway = FakeMoviePilotGateway()
    gateway.write_error = _provider_error(
        MoviePilotProviderErrorKind.RATE_LIMITED,
        outcome=MoviePilotWriteOutcome.REJECTED,
    )

    result = SubscriptionOperationExecutor(repository, gateway).execute(
        repository.operation.id,
        worker_id="fixture-worker",
        lease_seconds=60,
        now=NOW,
    )

    assert result is not None and result.operation.status is SubscriptionOperationStatus.REJECTED
    assert gateway.queries == []


def test_conflict_is_resolved_by_authoritative_existing_state() -> None:
    repository = FakeOperationRepository(_operation({"action": "create", "tmdb_id": "101", "payload": {"tmdbid": 101}}))
    gateway = FakeMoviePilotGateway()
    gateway.write_error = _provider_error(
        MoviePilotProviderErrorKind.CONFLICT,
        outcome=MoviePilotWriteOutcome.REJECTED,
    )
    gateway.query_results = [_found(id=77, tmdbid=101)]

    result = SubscriptionOperationExecutor(repository, gateway).execute(
        repository.operation.id,
        worker_id="fixture-worker",
        lease_seconds=60,
        now=NOW,
    )

    assert result is not None and result.operation.status is SubscriptionOperationStatus.CONFIRMED


def test_successful_write_with_concurrent_cancel_and_stale_readback_stays_awaiting() -> None:
    repository = FakeOperationRepository(_operation({"action": "create", "tmdb_id": "101", "payload": {"tmdbid": 101}}))
    gateway = FakeMoviePilotGateway()
    gateway.on_write = repository.request_cancel

    result = SubscriptionOperationExecutor(repository, gateway).execute(
        repository.operation.id,
        worker_id="fixture-worker",
        lease_seconds=60,
        now=NOW,
    )

    assert result is not None and result.operation.status is SubscriptionOperationStatus.AWAITING_READBACK
    assert result.operation.provider_request_started_at is not None
    assert result.operation.provider_receipt is not None
    assert result.operation.cancel_requested_at == NOW
    assert result.operation.finished_at is None
    assert result.operation.error is not None and result.operation.error["type"] == "desired_state_not_observed"
    assert gateway.writes == [("create", {"tmdbid": 101})]
    assert gateway.queries == [("101", None)]


def test_unknown_write_with_concurrent_cancel_and_stale_readback_stays_awaiting() -> None:
    repository = FakeOperationRepository(_operation({"action": "create", "tmdb_id": "101", "payload": {"tmdbid": 101}}))
    gateway = FakeMoviePilotGateway()
    gateway.on_write = repository.request_cancel
    gateway.write_error = _provider_error(
        MoviePilotProviderErrorKind.TIMEOUT,
        outcome=MoviePilotWriteOutcome.INDETERMINATE,
    )

    result = SubscriptionOperationExecutor(repository, gateway).execute(
        repository.operation.id,
        worker_id="fixture-worker",
        lease_seconds=60,
        now=NOW,
    )

    assert result is not None and result.operation.status is SubscriptionOperationStatus.AWAITING_READBACK
    assert result.operation.provider_request_started_at is not None
    assert result.operation.provider_receipt is None
    assert result.operation.cancel_requested_at == NOW
    assert result.operation.finished_at is None
    assert result.operation.error is not None and result.operation.error["outcome"] == "indeterminate"
    assert len(gateway.writes) == 1
    assert gateway.queries == [("101", None)]


def test_cancel_after_request_checkpoint_can_confirm_on_later_readback_without_resend() -> None:
    repository = FakeOperationRepository(_operation({"action": "create", "tmdb_id": "101", "payload": {"tmdbid": 101}}))
    gateway = FakeMoviePilotGateway()
    gateway.on_write = repository.request_cancel
    executor = SubscriptionOperationExecutor(repository, gateway, readback_delay_seconds=0)

    first = executor.execute(repository.operation.id, worker_id="fixture-worker", lease_seconds=60, now=NOW)
    gateway.query_results = [_found(id=77, tmdbid=101)]
    recovered = executor.execute(repository.operation.id, worker_id="fixture-worker", lease_seconds=60, now=NOW)

    assert first is not None and first.operation.status is SubscriptionOperationStatus.AWAITING_READBACK
    assert recovered is not None and recovered.operation.status is SubscriptionOperationStatus.CONFIRMED
    assert recovered.operation.result == {"action": "create", "readback_confirmed": True}
    assert recovered.operation.attempts == 2
    assert len(gateway.writes) == 1
    assert len(gateway.queries) == 2


def test_cancel_after_request_checkpoint_becomes_indeterminate_when_readback_budget_is_exhausted() -> None:
    repository = FakeOperationRepository(_operation({"action": "create", "tmdb_id": "101", "payload": {"tmdbid": 101}}))
    gateway = FakeMoviePilotGateway()
    gateway.on_write = repository.request_cancel
    executor = SubscriptionOperationExecutor(repository, gateway, max_attempts=2, readback_delay_seconds=0)

    first = executor.execute(repository.operation.id, worker_id="fixture-worker", lease_seconds=60, now=NOW)
    terminal = executor.execute(repository.operation.id, worker_id="fixture-worker", lease_seconds=60, now=NOW)

    assert first is not None and first.operation.status is SubscriptionOperationStatus.AWAITING_READBACK
    assert terminal is not None and terminal.operation.status is SubscriptionOperationStatus.INDETERMINATE
    assert terminal.operation.error is not None and terminal.operation.error["attempts"] == 2
    assert len(gateway.writes) == 1
    assert len(gateway.queries) == 2


def test_cancellation_before_provider_checkpoint_prevents_provider_write() -> None:
    repository = FakeOperationRepository(_operation({"action": "delete", "tmdb_id": "101", "season": 2}))
    repository.request_cancel()
    gateway = FakeMoviePilotGateway()

    result = SubscriptionOperationExecutor(repository, gateway).execute(
        repository.operation.id,
        worker_id="fixture-worker",
        lease_seconds=60,
        now=NOW,
    )

    assert result is not None and result.operation.status is SubscriptionOperationStatus.CANCELLED
    assert result.operation.provider_request_started_at is None
    assert result.operation.error == {"type": "cancel_requested_before_provider_request"}
    assert result.claimed is False
    assert gateway.writes == []
    assert gateway.queries == []


def test_invalid_durable_request_is_rejected_before_provider_checkpoint() -> None:
    repository = FakeOperationRepository(_operation({"action": "create", "tmdb_id": "101", "payload": {}}))
    gateway = FakeMoviePilotGateway()

    result = SubscriptionOperationExecutor(repository, gateway).execute(
        repository.operation.id,
        worker_id="fixture-worker",
        lease_seconds=60,
        now=NOW,
    )

    assert result is not None and result.operation.status is SubscriptionOperationStatus.REJECTED
    assert result.operation.provider_request_started_at is None
    assert gateway.writes == []


def test_update_status_maps_the_typed_payload_without_shape_drift() -> None:
    repository = FakeOperationRepository(
        _operation(
            {
                "action": "update_status",
                "tmdb_id": "101",
                "season": 2,
                "payload": {"status": "s", "total_episodes": 12},
                "expected": {"state": "S", "total_episode": 12},
            }
        )
    )
    gateway = FakeMoviePilotGateway()
    gateway.query_results = [_found(id=77, state="S", total_episode=12)]

    result = SubscriptionOperationExecutor(repository, gateway).execute(
        repository.operation.id,
        worker_id="fixture-worker",
        lease_seconds=60,
        now=NOW,
    )

    assert result is not None and result.operation.status is SubscriptionOperationStatus.CONFIRMED
    assert gateway.writes == [("update_status", ("101", "S", 2, 12))]
