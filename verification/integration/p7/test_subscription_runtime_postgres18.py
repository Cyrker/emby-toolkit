from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text

from etk.domains.subscription.moviepilot import (
    MoviePilotQueryDisposition,
    MoviePilotQueryResult,
    MoviePilotWriteResult,
)
from etk.domains.subscription.operation_repository import (
    PostgresSubscriptionOperationRepository,
    SubscriptionOperationClaimLostError,
    SubscriptionOperationStateError,
)
from etk.domains.subscription.operations import (
    SubscriptionOperationIntent,
    SubscriptionOperationKind,
    SubscriptionOperationStatus,
    SubscriptionQuotaRequirement,
)
from etk.domains.subscription.provider_execution import SubscriptionOperationExecutor
from etk.domains.subscription.runtime_repository import (
    PostgresSubscriptionRuntimeRepository,
    SubscriptionQuotaExhaustedError,
    SubscriptionQuotaStateError,
    SubscriptionQuotaStatus,
    SubscriptionRuntimeStateConflictError,
)
from etk.platform.db import DatabaseRuntime


def _repository(runtime: DatabaseRuntime) -> PostgresSubscriptionRuntimeRepository:
    return PostgresSubscriptionRuntimeRepository(runtime.unit_of_work)


def _operation_repository(runtime: DatabaseRuntime) -> PostgresSubscriptionOperationRepository:
    return PostgresSubscriptionOperationRepository(runtime.unit_of_work)


class _ScriptedMoviePilotGateway:
    def __init__(
        self,
        query_results: list[MoviePilotQueryResult],
        *,
        on_write: Callable[[], object] | None = None,
    ) -> None:
        self.query_results = list(query_results)
        self.on_write = on_write
        self.writes: list[tuple[str, Any]] = []
        self.queries: list[tuple[str, int | None]] = []

    def query_subscription(self, tmdb_id: int | str, *, season: int | None = None) -> MoviePilotQueryResult:
        self.queries.append((str(tmdb_id), season))
        return self.query_results.pop(0)

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

    def delete_subscription_by_id(self, subscription_id: int | str) -> MoviePilotWriteResult:
        return self._write("delete_by_id", str(subscription_id))

    def search_subscription(self, subscription_id: int | str) -> MoviePilotWriteResult:
        return self._write("search", str(subscription_id))

    def delete_download_tasks(self, hashes: Sequence[str]) -> MoviePilotWriteResult:
        return self._write("delete_downloads", tuple(hashes))

    def delete_transfer_history(self, record: Mapping[str, Any]) -> MoviePilotWriteResult:
        return self._write("delete_transfer_history", dict(record))

    def redo_transfer_history(self, history_ids: Sequence[int]) -> MoviePilotWriteResult:
        return self._write("redo_transfer_history", tuple(history_ids))

    def _write(self, action: str, payload: Any) -> MoviePilotWriteResult:
        self.writes.append((action, payload))
        if self.on_write is not None:
            self.on_write()
        return MoviePilotWriteResult(f"{action}-subscription", 200)


def test_quota_reservation_is_idempotent_and_projects_legacy_state(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p7_runtime)
    quota_day = date(2026, 8, 26)
    now = datetime(2026, 8, 26, 1, tzinfo=UTC)

    first = repository.reserve_quota(
        "fixture-subscribe-1",
        quota_date=quota_day,
        daily_cap=2,
        now=now,
    )
    replay = repository.reserve_quota(
        "fixture-subscribe-1",
        quota_date=quota_day,
        daily_cap=2,
        now=now,
    )

    assert replay == first
    assert first.status is SubscriptionQuotaStatus.RESERVED
    availability = repository.quota_availability(quota_day, daily_cap=2, now=now)
    assert (availability.reserved_units, availability.committed_units, availability.available_units) == (1, 0, 1)

    committed = repository.commit_quota(first.id, now=now + timedelta(seconds=1))
    assert committed.status is SubscriptionQuotaStatus.COMMITTED
    assert repository.commit_quota(first.id, now=now + timedelta(seconds=2)) == committed
    with pytest.raises(SubscriptionQuotaStateError):
        repository.release_quota(first.id, now=now + timedelta(seconds=3))

    with p7_runtime.engine.connect() as connection:
        projection = connection.execute(
            text("SELECT value_json FROM app_settings WHERE setting_key = 'subscription_quota_state'")
        ).scalar_one()
    assert projection == {
        "current_quota": 1,
        "last_reset_date": "2026-08-26",
        "max_quota_on_reset": 2,
    }


def test_quota_reservation_serializes_concurrent_workers_at_the_daily_cap(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p7_runtime)
    quota_day = date(2026, 8, 26)
    now = datetime(2026, 8, 26, 1, tzinfo=UTC)

    def reserve(index: int) -> str:
        try:
            repository.reserve_quota(
                f"fixture-concurrent-{index}",
                quota_date=quota_day,
                daily_cap=1,
                now=now,
            )
        except SubscriptionQuotaExhaustedError:
            return "exhausted"
        return "reserved"

    with ThreadPoolExecutor(max_workers=4) as executor:
        outcomes = list(executor.map(reserve, range(4)))

    assert outcomes.count("reserved") == 1
    assert outcomes.count("exhausted") == 3
    availability = repository.quota_availability(quota_day, daily_cap=1, now=now)
    assert availability.available_units == 0


def test_expired_quota_reservation_is_released_before_new_capacity_is_reported(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p7_runtime)
    quota_day = date(2026, 8, 26)
    now = datetime(2026, 8, 26, 1, tzinfo=UTC)
    reservation = repository.reserve_quota(
        "fixture-expiring",
        quota_date=quota_day,
        daily_cap=1,
        ttl_seconds=1,
        now=now,
    )

    availability = repository.quota_availability(
        quota_day,
        daily_cap=1,
        now=now + timedelta(seconds=2),
    )

    assert availability.available_units == 1
    with p7_runtime.engine.connect() as connection:
        status = connection.execute(
            text("SELECT status FROM subscription_quota_reservations WHERE id = :id"),
            {"id": reservation.id},
        ).scalar_one()
    assert status == SubscriptionQuotaStatus.RELEASED.value


def test_quota_replay_refreshes_projection_after_releasing_another_expired_reservation(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p7_runtime)
    quota_day = date(2026, 8, 26)
    now = datetime(2026, 8, 26, 1, tzinfo=UTC)
    expired = repository.reserve_quota(
        "fixture-expired-before-replay",
        quota_date=quota_day,
        daily_cap=3,
        ttl_seconds=1,
        now=now,
    )
    retained = repository.reserve_quota(
        "fixture-retained-replay",
        quota_date=quota_day,
        daily_cap=3,
        now=now,
    )

    replay = repository.reserve_quota(
        "fixture-retained-replay",
        quota_date=quota_day,
        daily_cap=3,
        now=now + timedelta(seconds=2),
    )

    assert replay == retained
    with p7_runtime.engine.connect() as connection:
        status_rows = (
            connection.execute(
                text(
                    """
                    SELECT id, status
                    FROM subscription_quota_reservations
                    WHERE id IN (:expired_id, :retained_id)
                    """
                ),
                {"expired_id": expired.id, "retained_id": retained.id},
            )
            .mappings()
            .all()
        )
        rows = {row["id"]: str(row["status"]) for row in status_rows}
        projection = connection.scalar(
            text("SELECT value_json FROM app_settings WHERE setting_key = 'subscription_quota_state'")
        )
    assert rows == {
        expired.id: SubscriptionQuotaStatus.RELEASED.value,
        retained.id: SubscriptionQuotaStatus.RESERVED.value,
    }
    assert projection == {
        "current_quota": 2,
        "last_reset_date": "2026-08-26",
        "max_quota_on_reset": 3,
    }


def test_dynamic_daily_cap_reduction_is_not_undone_by_old_reservation_commit(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p7_runtime)
    quota_day = date(2026, 8, 26)
    now = datetime(2026, 8, 26, 1, tzinfo=UTC)
    reservation = repository.reserve_quota(
        "fixture-dynamic-cap",
        quota_date=quota_day,
        daily_cap=3,
        now=now,
    )

    reduced = repository.quota_availability(
        quota_day,
        daily_cap=1,
        now=now + timedelta(seconds=1),
    )
    assert reduced.available_units == 0
    repository.commit_quota(reservation.id, now=now + timedelta(seconds=2))

    with p7_runtime.engine.connect() as connection:
        projection = connection.execute(
            text("SELECT value_json FROM app_settings WHERE setting_key = 'subscription_quota_state'")
        ).scalar_one()
    assert projection["current_quota"] == 0
    assert projection["max_quota_on_reset"] == 1


def test_runtime_state_requires_revision_cas_and_replay_does_not_overwrite(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p7_runtime)
    now = datetime(2026, 8, 26, 1, tzinfo=UTC)
    created = repository.create_runtime_state(
        "assistant:fixture",
        {"subscribes": {"1": {"status": "R"}}},
        schema_version=1,
        now=now,
    )
    replay = repository.create_runtime_state(
        "assistant:fixture",
        {"subscribes": {"2": {"status": "S"}}},
        schema_version=2,
        now=now + timedelta(seconds=1),
    )

    assert replay == created
    updated = repository.compare_and_swap_runtime_state(
        created.key,
        expected_revision=0,
        value={"subscribes": {"1": {"status": "P"}}},
        schema_version=2,
        now=now + timedelta(seconds=2),
    )
    assert updated.revision == 1
    assert updated.schema_version == 2
    assert updated.value["subscribes"]["1"]["status"] == "P"

    with pytest.raises(SubscriptionRuntimeStateConflictError):
        repository.compare_and_swap_runtime_state(
            created.key,
            expected_revision=0,
            value={"subscribes": {}},
            schema_version=2,
            now=now + timedelta(seconds=3),
        )
    assert repository.read_runtime_state(created.key) == updated


def test_runtime_state_concurrent_cas_allows_exactly_one_writer(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p7_runtime)
    now = datetime(2026, 8, 26, 1, tzinfo=UTC)
    repository.create_runtime_state("assistant:race", {"winner": None}, now=now)

    def update(winner: str) -> str:
        try:
            repository.compare_and_swap_runtime_state(
                "assistant:race",
                expected_revision=0,
                value={"winner": winner},
                schema_version=1,
                now=now + timedelta(seconds=1),
            )
        except SubscriptionRuntimeStateConflictError:
            return "conflict"
        return "updated"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(update, ("a", "b")))

    assert outcomes.count("updated") == 1
    assert outcomes.count("conflict") == 1


def test_operation_and_quota_reservation_are_atomic_idempotent_and_outboxed(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = _operation_repository(p7_runtime)
    now = datetime.now(UTC)
    intent = SubscriptionOperationIntent.create(
        kind=SubscriptionOperationKind.CREATE,
        scope_key="series:101:season:2",
        request={"tmdbid": 101, "season": 2, "name": "Fixture Series"},
        available_at=now,
        quota=SubscriptionQuotaRequirement(date(2026, 8, 26), daily_cap=1),
    )

    first = repository.reserve(intent)
    replay = repository.reserve(intent)

    assert first.created is True
    assert replay.created is False
    assert replay.operation == first.operation
    assert replay.quota_reservation_id == first.quota_reservation_id
    with p7_runtime.engine.connect() as connection:
        counts = (
            connection.execute(
                text(
                    """
                SELECT
                    (SELECT COUNT(*) FROM subscription_operations) AS operations,
                    (SELECT COUNT(*) FROM subscription_quota_reservations) AS quota,
                    (SELECT COUNT(*) FROM outbox_events WHERE topic = 'subscription.operation.requested') AS outbox
                """
                )
            )
            .mappings()
            .one()
        )
    assert dict(counts) == {"operations": 1, "quota": 1, "outbox": 1}


def test_quota_exhaustion_rolls_back_the_new_operation_and_outbox(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = _operation_repository(p7_runtime)
    now = datetime.now(UTC)
    quota = SubscriptionQuotaRequirement(date(2026, 8, 26), daily_cap=1)
    first = SubscriptionOperationIntent.create(
        kind="create",
        scope_key="series:101:season:1",
        request={"tmdbid": 101, "season": 1},
        available_at=now,
        quota=quota,
    )
    second = SubscriptionOperationIntent.create(
        kind="create",
        scope_key="series:101:season:2",
        request={"tmdbid": 101, "season": 2},
        available_at=now,
        quota=quota,
    )
    repository.reserve(first)

    with pytest.raises(SubscriptionQuotaExhaustedError):
        repository.reserve(second)

    assert repository.read(second.id) is None
    with p7_runtime.engine.connect() as connection:
        outbox = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM outbox_events
                WHERE topic = 'subscription.operation.requested'
                """
            )
        ).scalar_one()
    assert outbox == 1


def test_confirmed_operation_commits_quota_and_completes_outbox(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = _operation_repository(p7_runtime)
    now = datetime.now(UTC)
    intent = SubscriptionOperationIntent.create(
        kind="create",
        scope_key="series:101:season:2",
        request={"tmdbid": 101, "season": 2},
        available_at=now,
        quota=SubscriptionQuotaRequirement(now.date(), daily_cap=1),
    )
    repository.reserve(intent)
    claimed = repository.claim_by_id(intent.id, worker_id="fixture-worker", lease_seconds=60, now=now)
    assert claimed is not None and claimed.claim_token is not None
    requesting = repository.mark_requesting(intent.id, claimed.claim_token, now=now + timedelta(seconds=1))
    assert requesting.status is SubscriptionOperationStatus.REQUESTING
    remote = repository.mark_remote_confirmed(
        intent.id,
        claimed.claim_token,
        receipt={"provider_id": 42, "status_code": 201},
        now=now + timedelta(seconds=2),
    )
    assert remote.status is SubscriptionOperationStatus.REMOTE_CONFIRMED

    confirmed = repository.mark_confirmed(
        intent.id,
        claimed.claim_token,
        result={"provider_id": 42},
        readback={"id": 42, "tmdbid": 101, "season": 2},
        now=now + timedelta(seconds=3),
    )

    assert confirmed.status is SubscriptionOperationStatus.CONFIRMED
    assert confirmed.terminal is True
    with p7_runtime.engine.connect() as connection:
        state = (
            connection.execute(
                text(
                    """
                SELECT
                    (SELECT status FROM subscription_quota_reservations WHERE operation_id = :id) AS quota_status,
                    (SELECT status FROM outbox_events WHERE topic = 'subscription.operation.requested') AS outbox_status
                """
                ),
                {"id": intent.id},
            )
            .mappings()
            .one()
        )
    assert dict(state) == {"quota_status": "committed", "outbox_status": "processed"}


def test_rejected_operation_releases_quota_but_indeterminate_consumes_it(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = _operation_repository(p7_runtime)
    now = datetime.now(UTC)

    def reserve(scope: str, season: int) -> SubscriptionOperationIntent:
        intent = SubscriptionOperationIntent.create(
            kind="create",
            scope_key=scope,
            request={"tmdbid": 101, "season": season},
            available_at=now,
            quota=SubscriptionQuotaRequirement(now.date(), daily_cap=2),
        )
        repository.reserve(intent)
        return intent

    rejected_intent = reserve("series:101:season:1", 1)
    rejected_claim = repository.claim_by_id(
        rejected_intent.id,
        worker_id="fixture-worker",
        lease_seconds=60,
        now=now,
    )
    assert rejected_claim is not None and rejected_claim.claim_token is not None
    rejected = repository.mark_rejected(
        rejected_intent.id,
        rejected_claim.claim_token,
        error={"type": "provider_rejected", "status_code": 409},
        now=now + timedelta(seconds=1),
    )
    assert rejected.status is SubscriptionOperationStatus.REJECTED

    unknown_intent = reserve("series:101:season:2", 2)
    unknown_claim = repository.claim_by_id(
        unknown_intent.id,
        worker_id="fixture-worker",
        lease_seconds=60,
        now=now,
    )
    assert unknown_claim is not None and unknown_claim.claim_token is not None
    repository.mark_requesting(
        unknown_intent.id,
        unknown_claim.claim_token,
        now=now + timedelta(seconds=1),
    )
    unknown = repository.mark_indeterminate(
        unknown_intent.id,
        unknown_claim.claim_token,
        error={"type": "provider_timeout"},
        now=now + timedelta(seconds=2),
    )
    assert unknown.status is SubscriptionOperationStatus.INDETERMINATE

    with p7_runtime.engine.connect() as connection:
        status_rows = (
            connection.execute(
                text(
                    """
                SELECT operation_id, status
                FROM subscription_quota_reservations
                ORDER BY operation_id
                """
                )
            )
            .mappings()
            .all()
        )
    statuses: dict[object, str] = {row["operation_id"]: str(row["status"]) for row in status_rows}
    assert statuses[rejected_intent.id] == "released"
    assert statuses[unknown_intent.id] == "committed"


def test_retry_is_only_allowed_before_provider_request_started(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = _operation_repository(p7_runtime)
    now = datetime.now(UTC)
    intent = SubscriptionOperationIntent.create(
        kind="reconcile",
        scope_key="series:101",
        request={"tmdbid": 101},
        available_at=now,
    )
    repository.reserve(intent)
    first_claim = repository.claim_by_id(intent.id, worker_id="fixture-worker", lease_seconds=60, now=now)
    assert first_claim is not None and first_claim.claim_token is not None
    retryable = repository.mark_retryable(
        intent.id,
        first_claim.claim_token,
        error={"type": "provider_unavailable_before_request"},
        retry_after_seconds=1,
        now=now,
    )
    assert retryable.status is SubscriptionOperationStatus.RETRYABLE

    second_claim = repository.claim_by_id(
        intent.id,
        worker_id="fixture-worker",
        lease_seconds=60,
        now=now + timedelta(seconds=2),
    )
    assert second_claim is not None and second_claim.claim_token is not None
    repository.mark_requesting(
        intent.id,
        second_claim.claim_token,
        now=now + timedelta(seconds=3),
    )
    with pytest.raises(SubscriptionOperationClaimLostError):
        repository.mark_retryable(
            intent.id,
            second_claim.claim_token,
            error={"type": "unsafe_retry"},
            retry_after_seconds=1,
            now=now + timedelta(seconds=4),
        )


def test_readback_recovery_is_durable_and_cannot_resend_provider_write(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = _operation_repository(p7_runtime)
    now = datetime.now(UTC)
    intent = SubscriptionOperationIntent.create(
        kind="create",
        scope_key="series:202:season:1",
        request={"tmdbid": 202, "season": 1},
        available_at=now,
        quota=SubscriptionQuotaRequirement(now.date(), daily_cap=1),
    )
    repository.reserve(intent)
    claimed = repository.claim_by_id(intent.id, worker_id="fixture-worker", lease_seconds=60, now=now)
    assert claimed is not None and claimed.claim_token is not None
    repository.mark_requesting(intent.id, claimed.claim_token, now=now + timedelta(seconds=1))

    awaiting = repository.mark_awaiting_readback(
        intent.id,
        claimed.claim_token,
        error={"type": "provider_timeout"},
        retry_after_seconds=10,
        now=now + timedelta(seconds=2),
    )

    assert awaiting.status is SubscriptionOperationStatus.AWAITING_READBACK
    assert awaiting.claim_token is None
    assert (
        repository.claim_by_id(
            intent.id,
            worker_id="early-worker",
            lease_seconds=60,
            now=now + timedelta(seconds=5),
        )
        is None
    )
    recovered = repository.claim_by_id(
        intent.id,
        worker_id="readback-worker",
        lease_seconds=60,
        now=now + timedelta(seconds=13),
    )
    assert recovered is not None and recovered.claim_token is not None
    assert recovered.provider_request_started_at is not None
    with pytest.raises(SubscriptionOperationStateError, match="readback-only"):
        repository.mark_requesting(
            intent.id,
            recovered.claim_token,
            now=now + timedelta(seconds=14),
        )
    with pytest.raises(SubscriptionOperationClaimLostError, match="readback recovery"):
        repository.mark_retryable(
            intent.id,
            recovered.claim_token,
            error={"type": "unsafe_retry"},
            retry_after_seconds=1,
            now=now + timedelta(seconds=14),
        )

    repository.mark_remote_confirmed(
        intent.id,
        recovered.claim_token,
        receipt={"provider_id": 77},
        now=now + timedelta(seconds=14),
    )
    confirmed = repository.mark_confirmed(
        intent.id,
        recovered.claim_token,
        result={"provider_id": 77},
        readback={"id": 77, "tmdbid": 202, "season": 1},
        now=now + timedelta(seconds=15),
    )
    assert confirmed.status is SubscriptionOperationStatus.CONFIRMED


def test_stale_claim_recovery_retries_only_before_request_checkpoint(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = _operation_repository(p7_runtime)
    now = datetime.now(UTC)

    def reserve(scope: str, season: int) -> SubscriptionOperationIntent:
        intent = SubscriptionOperationIntent.create(
            kind="create",
            scope_key=scope,
            request={"tmdbid": 303, "season": season},
            available_at=now,
            quota=SubscriptionQuotaRequirement(now.date(), daily_cap=2, ttl_seconds=1),
        )
        repository.reserve(intent)
        return intent

    before_request = reserve("series:303:season:1", 1)
    after_request = reserve("series:303:season:2", 2)
    before_claim = repository.claim_by_id(
        before_request.id,
        worker_id="before-worker",
        lease_seconds=1,
        now=now,
    )
    after_claim = repository.claim_by_id(
        after_request.id,
        worker_id="after-worker",
        lease_seconds=1,
        now=now,
    )
    assert before_claim is not None and before_claim.claim_token is not None
    assert after_claim is not None and after_claim.claim_token is not None
    repository.mark_requesting(after_request.id, after_claim.claim_token, now=now)

    recovered_count = repository.expire_stale_claims(
        retry_delay_seconds=5,
        now=now + timedelta(seconds=2),
    )

    assert recovered_count == 2
    recovered_before = repository.read(before_request.id)
    recovered_after = repository.read(after_request.id)
    assert recovered_before is not None
    assert recovered_before.status is SubscriptionOperationStatus.RETRYABLE
    assert recovered_before.provider_request_started_at is None
    assert recovered_after is not None
    assert recovered_after.status is SubscriptionOperationStatus.AWAITING_READBACK
    assert recovered_after.provider_request_started_at is not None
    assert recovered_after.claim_token is None

    with p7_runtime.engine.connect() as connection:
        quota_rows = (
            connection.execute(
                text(
                    """
                SELECT operation_id, status, expires_at
                FROM subscription_quota_reservations
                ORDER BY operation_id
                """
                )
            )
            .mappings()
            .all()
        )
        outbox_rows = (
            connection.execute(
                text(
                    """
                SELECT aggregate_id, status, available_at
                FROM outbox_events
                WHERE topic = 'subscription.operation.requested'
                ORDER BY aggregate_id
                """
                )
            )
            .mappings()
            .all()
        )
    assert {str(row["status"]) for row in quota_rows} == {"reserved"}
    assert all(row["expires_at"] > now + timedelta(seconds=7) for row in quota_rows)
    assert {str(row["status"]) for row in outbox_rows} == {"pending"}
    assert all(row["available_at"] == now + timedelta(seconds=7) for row in outbox_rows)


def test_repository_cancellation_releases_only_before_provider_checkpoint(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = _operation_repository(p7_runtime)
    now = datetime.now(UTC)
    quota = SubscriptionQuotaRequirement(now.date(), daily_cap=2)

    before_intent = SubscriptionOperationIntent.create(
        kind="create",
        scope_key="series:404:season:1",
        request={"tmdbid": 404, "season": 1},
        available_at=now,
        quota=quota,
    )
    repository.reserve(before_intent)
    cancelled = repository.request_cancel(before_intent.id, now=now)
    assert cancelled.status is SubscriptionOperationStatus.CANCELLED
    assert cancelled.finished_at == now

    after_intent = SubscriptionOperationIntent.create(
        kind="create",
        scope_key="series:404:season:2",
        request={"tmdbid": 404, "season": 2},
        available_at=now,
        quota=quota,
    )
    repository.reserve(after_intent)
    claimed = repository.claim_by_id(after_intent.id, worker_id="fixture-worker", lease_seconds=60, now=now)
    assert claimed is not None and claimed.claim_token is not None
    repository.mark_requesting(after_intent.id, claimed.claim_token, now=now + timedelta(seconds=1))
    pending = repository.request_cancel(after_intent.id, now=now + timedelta(seconds=2))
    assert pending.status is SubscriptionOperationStatus.REQUESTING
    assert pending.cancel_requested_at == now + timedelta(seconds=2)
    with pytest.raises(SubscriptionOperationStateError, match="requires authoritative readback"):
        repository.mark_cancelled_before_write(
            after_intent.id,
            claimed.claim_token,
            evidence={"provider_write_invoked": False},
            now=now + timedelta(seconds=3),
        )
    awaiting = repository.mark_awaiting_readback(
        after_intent.id,
        claimed.claim_token,
        error={"type": "cancelled_during_request"},
        now=now + timedelta(seconds=4),
    )
    assert awaiting.status is SubscriptionOperationStatus.AWAITING_READBACK
    recovered = repository.claim_by_id(
        after_intent.id,
        worker_id="readback-worker",
        lease_seconds=60,
        now=now + timedelta(seconds=5),
    )
    assert recovered is not None and recovered.claim_token is not None
    indeterminate = repository.mark_indeterminate(
        after_intent.id,
        recovered.claim_token,
        error={"type": "readback_budget_exhausted"},
        now=now + timedelta(seconds=6),
    )
    assert indeterminate.status is SubscriptionOperationStatus.INDETERMINATE

    with p7_runtime.engine.connect() as connection:
        statuses = (
            connection.execute(
                text(
                    """
                SELECT operation_id, status
                FROM subscription_quota_reservations
                ORDER BY operation_id
                """
                )
            )
            .mappings()
            .all()
        )
    by_operation = {row["operation_id"]: str(row["status"]) for row in statuses}
    assert by_operation[before_intent.id] == "released"
    assert by_operation[after_intent.id] == "committed"


def test_executor_cancellation_readback_keeps_quota_and_outbox_consistent(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = _operation_repository(p7_runtime)
    now = datetime.now(UTC)
    quota = SubscriptionQuotaRequirement(now.date(), daily_cap=3)

    def reserve(season: int) -> SubscriptionOperationIntent:
        intent = SubscriptionOperationIntent.create(
            kind="create",
            scope_key=f"series:808:season:{season}",
            request={
                "action": "create",
                "tmdb_id": "808",
                "season": season,
                "payload": {"tmdbid": 808, "season": season},
                "expected": {"state": "R"},
            },
            available_at=now,
            quota=quota,
        )
        repository.reserve(intent)
        return intent

    cancelled_before_write = reserve(1)
    repository.request_cancel(cancelled_before_write.id, now=now)
    cancelled_gateway = _ScriptedMoviePilotGateway([])
    cancelled_result = SubscriptionOperationExecutor(repository, cancelled_gateway).execute(
        cancelled_before_write.id,
        worker_id="cancelled-worker",
        lease_seconds=60,
        now=now,
    )
    assert cancelled_result is not None
    assert cancelled_result.operation.status is SubscriptionOperationStatus.CANCELLED
    assert cancelled_gateway.writes == []
    assert cancelled_gateway.queries == []

    confirmed_after_cancel = reserve(2)
    confirmed_gateway = _ScriptedMoviePilotGateway(
        [
            MoviePilotQueryResult(MoviePilotQueryDisposition.EMPTY, None),
            MoviePilotQueryResult(
                MoviePilotQueryDisposition.FOUND,
                {"id": 82, "tmdbid": 808, "season": 2, "state": "R"},
            ),
        ],
        on_write=lambda: repository.request_cancel(confirmed_after_cancel.id, now=now),
    )
    confirmed_executor = SubscriptionOperationExecutor(
        repository,
        confirmed_gateway,
        max_attempts=2,
        readback_delay_seconds=0,
    )
    awaiting_confirmation = confirmed_executor.execute(
        confirmed_after_cancel.id,
        worker_id="confirmed-worker",
        lease_seconds=60,
        now=now,
    )
    assert awaiting_confirmation is not None
    assert awaiting_confirmation.operation.status is SubscriptionOperationStatus.AWAITING_READBACK
    with p7_runtime.engine.connect() as connection:
        awaiting_state = (
            connection.execute(
                text(
                    """
                    SELECT
                        (SELECT status FROM subscription_quota_reservations WHERE operation_id = :id) AS quota_status,
                        (SELECT status FROM outbox_events WHERE aggregate_id = :aggregate_id) AS outbox_status,
                        (SELECT processed_at FROM outbox_events WHERE aggregate_id = :aggregate_id) AS processed_at
                    """
                ),
                {"id": confirmed_after_cancel.id, "aggregate_id": str(confirmed_after_cancel.id)},
            )
            .mappings()
            .one()
        )
    assert dict(awaiting_state) == {
        "quota_status": "reserved",
        "outbox_status": "pending",
        "processed_at": None,
    }
    confirmed = confirmed_executor.execute(
        confirmed_after_cancel.id,
        worker_id="confirmed-worker",
        lease_seconds=60,
        now=now,
    )
    assert confirmed is not None and confirmed.operation.status is SubscriptionOperationStatus.CONFIRMED
    assert len(confirmed_gateway.writes) == 1

    indeterminate_after_cancel = reserve(3)
    indeterminate_gateway = _ScriptedMoviePilotGateway(
        [
            MoviePilotQueryResult(MoviePilotQueryDisposition.EMPTY, None),
            MoviePilotQueryResult(MoviePilotQueryDisposition.EMPTY, None),
        ],
        on_write=lambda: repository.request_cancel(indeterminate_after_cancel.id, now=now),
    )
    indeterminate_executor = SubscriptionOperationExecutor(
        repository,
        indeterminate_gateway,
        max_attempts=2,
        readback_delay_seconds=0,
    )
    awaiting_indeterminate = indeterminate_executor.execute(
        indeterminate_after_cancel.id,
        worker_id="indeterminate-worker",
        lease_seconds=60,
        now=now,
    )
    terminal = indeterminate_executor.execute(
        indeterminate_after_cancel.id,
        worker_id="indeterminate-worker",
        lease_seconds=60,
        now=now,
    )
    assert awaiting_indeterminate is not None
    assert awaiting_indeterminate.operation.status is SubscriptionOperationStatus.AWAITING_READBACK
    assert terminal is not None and terminal.operation.status is SubscriptionOperationStatus.INDETERMINATE
    assert len(indeterminate_gateway.writes) == 1

    with p7_runtime.engine.connect() as connection:
        quota_rows = (
            connection.execute(
                text(
                    """
                    SELECT operation_id, status
                    FROM subscription_quota_reservations
                    ORDER BY operation_id
                    """
                )
            )
            .mappings()
            .all()
        )
        outbox_rows = (
            connection.execute(
                text(
                    """
                    SELECT aggregate_id, status
                    FROM outbox_events
                    WHERE topic = 'subscription.operation.requested'
                    ORDER BY aggregate_id
                    """
                )
            )
            .mappings()
            .all()
        )
    quota_statuses = {row["operation_id"]: str(row["status"]) for row in quota_rows}
    assert quota_statuses == {
        cancelled_before_write.id: "released",
        confirmed_after_cancel.id: "committed",
        indeterminate_after_cancel.id: "committed",
    }
    outbox_statuses = {str(row["aggregate_id"]): str(row["status"]) for row in outbox_rows}
    assert outbox_statuses == {
        str(cancelled_before_write.id): "processed",
        str(confirmed_after_cancel.id): "processed",
        str(indeterminate_after_cancel.id): "processed",
    }


def test_operation_bound_quota_does_not_expire_while_operation_is_nonterminal(
    p7_runtime: DatabaseRuntime,
) -> None:
    runtime_repository = _repository(p7_runtime)
    operation_repository = _operation_repository(p7_runtime)
    now = datetime.now(UTC)
    intent = SubscriptionOperationIntent.create(
        kind="create",
        scope_key="series:505:season:1",
        request={"tmdbid": 505, "season": 1},
        available_at=now,
        quota=SubscriptionQuotaRequirement(now.date(), daily_cap=1, ttl_seconds=1),
    )
    operation_repository.reserve(intent)

    availability = runtime_repository.quota_availability(
        now.date(),
        daily_cap=1,
        now=now + timedelta(seconds=2),
    )

    assert availability.reserved_units == 1
    assert availability.available_units == 0
    assert runtime_repository.expire_quota_reservations(now=now + timedelta(seconds=2)) == 0
    claimed = operation_repository.claim_by_id(
        intent.id,
        worker_id="fixture-worker",
        lease_seconds=60,
        now=now + timedelta(seconds=2),
    )
    assert claimed is not None


def test_secret_terminal_payload_is_rejected_without_partial_transition(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = _operation_repository(p7_runtime)
    now = datetime.now(UTC)
    intent = SubscriptionOperationIntent.create(
        kind="reconcile",
        scope_key="series:606",
        request={"tmdbid": 606},
        available_at=now,
    )
    repository.reserve(intent)
    claimed = repository.claim_by_id(intent.id, worker_id="fixture-worker", lease_seconds=60, now=now)
    assert claimed is not None and claimed.claim_token is not None

    with pytest.raises(ValueError, match="secret field"):
        repository.mark_confirmed(
            intent.id,
            claimed.claim_token,
            result={"provider_id": 88, "Authorization": "fixture"},
            now=now + timedelta(seconds=1),
        )

    current = repository.read(intent.id)
    assert current is not None
    assert current.status is SubscriptionOperationStatus.CLAIMED
    assert current.result is None


def test_dead_dispatch_releases_pre_request_quota_but_consumes_unknown_remote_effect(
    p7_runtime: DatabaseRuntime,
) -> None:
    repository = _operation_repository(p7_runtime)
    now = datetime.now(UTC)
    quota = SubscriptionQuotaRequirement(now.date(), daily_cap=2)

    def reserve(season: int) -> SubscriptionOperationIntent:
        intent = SubscriptionOperationIntent.create(
            kind="create",
            scope_key=f"series:707:season:{season}",
            request={"tmdbid": 707, "season": season},
            available_at=now,
            quota=quota,
        )
        repository.reserve(intent)
        return intent

    before_request = reserve(1)
    after_request = reserve(2)
    claimed = repository.claim_by_id(
        after_request.id,
        worker_id="fixture-worker",
        lease_seconds=60,
        now=now,
    )
    assert claimed is not None and claimed.claim_token is not None
    repository.mark_requesting(after_request.id, claimed.claim_token, now=now + timedelta(seconds=1))
    repository.mark_awaiting_readback(
        after_request.id,
        claimed.claim_token,
        error={"type": "fixture_readback"},
        now=now + timedelta(seconds=2),
    )
    with p7_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE outbox_events
                SET status = 'dead', last_error = 'fixture dead dispatch'
                WHERE topic = 'subscription.operation.requested'
                """
            )
        )

    rejected = repository.reject_dead_dispatches(now=now + timedelta(seconds=3))

    assert rejected == 2
    before = repository.read(before_request.id)
    after = repository.read(after_request.id)
    assert before is not None and before.status is SubscriptionOperationStatus.REJECTED
    assert after is not None and after.status is SubscriptionOperationStatus.INDETERMINATE
    with p7_runtime.engine.connect() as connection:
        quota_rows = (
            connection.execute(
                text(
                    """
                SELECT operation_id, status
                FROM subscription_quota_reservations
                ORDER BY operation_id
                """
                )
            )
            .mappings()
            .all()
        )
    statuses = {row["operation_id"]: str(row["status"]) for row in quota_rows}
    assert statuses[before_request.id] == "released"
    assert statuses[after_request.id] == "committed"
