from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from etk.domains.sharing.operations import SharingOperationStatus
from etk.domains.subscription.resubscribe import (
    ResubscribeBatchReservation,
    ResubscribeItemKey,
    ResubscribePolicySnapshot,
)
from etk.domains.subscription.resubscribe_task_execution import (
    NativeResubscribeHDHiveGateway,
    NativeResubscribeTaskService,
    ResubscribeExecutionItem,
    ResubscribeHDHiveDisposition,
    ResubscribeHDHiveResult,
)
from etk.domains.subscription.runtime_repository import (
    SubscriptionQuotaExhaustedError,
    SubscriptionQuotaReservation,
    SubscriptionQuotaStatus,
)
from etk.integrations import ProviderDisposition, ProviderResponse
from etk.platform.tasks import TaskOutcomeStatus

NOW = datetime(2026, 9, 8, tzinfo=UTC)


class _Progress:
    def __init__(self, *, fail_after: int | None = None) -> None:
        self.checkpoints = 0
        self.updates: list[tuple[int, str]] = []
        self._fail_after = fail_after

    def checkpoint(self) -> None:
        self.checkpoints += 1
        if self._fail_after is not None and self.checkpoints > self._fail_after:
            raise RuntimeError("fixture cancellation")

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))


class _Repository:
    def __init__(self, items: Sequence[ResubscribeExecutionItem]) -> None:
        self.items = list(items)
        self.statuses: dict[ResubscribeItemKey, str] = {item.key: "needed" for item in items}

    def list_needed_items(self) -> Sequence[ResubscribeExecutionItem]:
        return tuple(item for item in self.items if self.statuses[item.key] == "needed")

    def is_current(self, item: ResubscribeExecutionItem) -> bool:
        return self.statuses.get(item.key) == "needed"

    def mark_status(self, key: ResubscribeItemKey, status: str) -> None:
        self.statuses[key] = status


class _MoviePilot:
    def __init__(self) -> None:
        self.calls: list[tuple[ResubscribeItemKey, ...]] = []

    def stage_replacements(self, keys: Sequence[ResubscribeItemKey]) -> ResubscribeBatchReservation:
        normalized = tuple(keys)
        self.calls.append(normalized)
        operation_ids = tuple(uuid5(NAMESPACE_URL, f"fixture:{key.item_id}") for key in normalized)
        return ResubscribeBatchReservation(operation_ids)


class _Cleanup:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def stage_version_deletion(self, emby_id: str, **_values: Any) -> Mapping[str, Any]:
        self.calls.append(emby_id)
        return {
            "job_id": str(uuid5(NAMESPACE_URL, f"cleanup:{emby_id}")),
            "execution_key": f"cleanup:{emby_id}",
            "item_count": 1,
            "reused": emby_id.endswith("replayed"),
        }


class _HDHive:
    def __init__(self, results: Sequence[ResubscribeHDHiveResult]) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, str]] = []

    def execute(
        self,
        item: ResubscribeExecutionItem,
        *,
        occurrence: str,
        progress: Any,
    ) -> ResubscribeHDHiveResult:
        del progress
        self.calls.append((item.key.item_id, occurrence))
        return self.results.pop(0)


class _Quota:
    def __init__(self, *, exhausted: bool = False) -> None:
        self.exhausted = exhausted
        self.reservations: dict[str, SubscriptionQuotaReservation] = {}
        self.commits: list[UUID] = []
        self.releases: list[UUID] = []

    def reserve_quota(
        self,
        idempotency_key: str,
        *,
        quota_date: date,
        daily_cap: int,
        units: int = 1,
        ttl_seconds: float = 3600,
        operation_id: UUID | None = None,
    ) -> SubscriptionQuotaReservation:
        del daily_cap, ttl_seconds, operation_id
        if self.exhausted:
            raise SubscriptionQuotaExhaustedError("fixture exhausted")
        existing = self.reservations.get(idempotency_key)
        if existing is not None:
            return existing
        reservation = SubscriptionQuotaReservation(
            id=uuid5(NAMESPACE_URL, f"quota:{idempotency_key}"),
            reservation_key=idempotency_key,
            operation_id=None,
            quota_date=quota_date,
            units=units,
            cap_snapshot=10,
            status=SubscriptionQuotaStatus.RESERVED,
            state_revision=0,
            expires_at=NOW + timedelta(days=1),
            committed_at=None,
            released_at=None,
        )
        self.reservations[idempotency_key] = reservation
        return reservation

    def commit_quota(self, reservation_id: UUID) -> SubscriptionQuotaReservation:
        self.commits.append(reservation_id)
        return self._transition(reservation_id, SubscriptionQuotaStatus.COMMITTED)

    def release_quota(self, reservation_id: UUID) -> SubscriptionQuotaReservation:
        self.releases.append(reservation_id)
        return self._transition(reservation_id, SubscriptionQuotaStatus.RELEASED)

    def _transition(
        self,
        reservation_id: UUID,
        status: SubscriptionQuotaStatus,
    ) -> SubscriptionQuotaReservation:
        key, current = next((key, value) for key, value in self.reservations.items() if value.id == reservation_id)
        transitioned = SubscriptionQuotaReservation(
            id=current.id,
            reservation_key=current.reservation_key,
            operation_id=current.operation_id,
            quota_date=current.quota_date,
            units=current.units,
            cap_snapshot=current.cap_snapshot,
            status=status,
            state_revision=current.state_revision + 1,
            expires_at=current.expires_at,
            committed_at=NOW if status is SubscriptionQuotaStatus.COMMITTED else None,
            released_at=NOW if status is SubscriptionQuotaStatus.RELEASED else None,
        )
        self.reservations[key] = transitioned
        return transitioned


class _Policy:
    def snapshot(self) -> ResubscribePolicySnapshot:
        return ResubscribePolicySnapshot(
            quota_date=date(2026, 9, 8),
            daily_cap=10,
            delay_seconds=0,
            language_keywords={},
            release_group_mapping={},
        )


def _item(
    value: str,
    *,
    action: str = "resubscribe",
    source: str = "moviepilot",
    cleanup_ids: Sequence[str] = (),
) -> ResubscribeExecutionItem:
    return ResubscribeExecutionItem(
        key=ResubscribeItemKey(value, "Movie"),
        action=action,
        source=source,
        generation="fixture-generation",
        item_name=f"Fixture {value}",
        cleanup_emby_ids=tuple(cleanup_ids),
    )


def _service(
    items: Sequence[ResubscribeExecutionItem],
    *,
    hdhive_results: Sequence[ResubscribeHDHiveResult] = (),
    exhausted: bool = False,
) -> tuple[NativeResubscribeTaskService, _Repository, _MoviePilot, _Cleanup, _HDHive, _Quota]:
    repository = _Repository(items)
    moviepilot = _MoviePilot()
    cleanup = _Cleanup()
    hdhive = _HDHive(hdhive_results)
    quota = _Quota(exhausted=exhausted)
    return (
        NativeResubscribeTaskService(repository, moviepilot, cleanup, hdhive, quota, _Policy()),
        repository,
        moviepilot,
        cleanup,
        hdhive,
        quota,
    )


def test_native_resubscribe_dispatches_moviepilot_cleanup_and_hdhive_without_processor() -> None:
    hdhive_operation = uuid5(NAMESPACE_URL, "fixture:hdhive")
    items = (
        _item("101"),
        _item("102", action="delete", cleanup_ids=("emby-old", "emby-replayed")),
        _item("103", source="hdhive"),
    )
    service, repository, moviepilot, cleanup, hdhive, quota = _service(
        items,
        hdhive_results=(ResubscribeHDHiveResult(ResubscribeHDHiveDisposition.CONFIRMED, hdhive_operation),),
    )

    outcome = service.execute("resubscribe-library", _Progress(), {})

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details == {
        "requested": 3,
        "moviepilot_staged": 1,
        "cleanup_staged": 2,
        "cleanup_reused": 1,
        "hdhive_confirmed": 1,
        "business_skipped": 0,
        "quota_exhausted": False,
        "provider_failures": [],
        "failed": 0,
        "retryable": False,
    }
    assert moviepilot.calls == [(items[0].key,)]
    assert cleanup.calls == ["emby-old", "emby-replayed"]
    assert len(hdhive.calls) == 1 and hdhive.calls[0][0] == "103-Movie"
    assert repository.statuses[items[2].key] == "subscribed"
    assert len(quota.commits) == 1 and quota.releases == []


def test_hdhive_indeterminate_consumes_quota_and_is_not_resent() -> None:
    item = _item("201", source="hdhive")
    service, repository, _moviepilot, _cleanup, hdhive, quota = _service(
        (item,),
        hdhive_results=(
            ResubscribeHDHiveResult(
                ResubscribeHDHiveDisposition.INDETERMINATE,
                uuid5(NAMESPACE_URL, "fixture:unknown"),
                {"type": "authoritative_readback_unavailable"},
            ),
        ),
    )

    first = service.execute("resubscribe-library", _Progress(), {})
    replay = service.execute("resubscribe-library", _Progress(), {})

    assert first.status is TaskOutcomeStatus.FAILED
    assert first.details["provider_failures"][0]["indeterminate"] is True
    assert replay.status is TaskOutcomeStatus.SKIPPED
    assert repository.statuses[item.key] == "indeterminate"
    assert len(hdhive.calls) == 1
    assert len(quota.commits) == 1


def test_hdhive_business_miss_releases_quota_without_false_failure() -> None:
    item = _item("301", source="hdhive")
    service, repository, _moviepilot, _cleanup, _hdhive, quota = _service(
        (item,),
        hdhive_results=(ResubscribeHDHiveResult(ResubscribeHDHiveDisposition.SKIPPED),),
    )

    outcome = service.execute("resubscribe-library", _Progress(), {})

    assert outcome.status is TaskOutcomeStatus.SKIPPED
    assert outcome.details["business_skipped"] == 1
    assert repository.statuses[item.key] == "needed"
    assert len(quota.releases) == 1 and quota.commits == []


def test_quota_exhaustion_stops_before_hdhive_provider_read() -> None:
    item = _item("401", source="hdhive")
    service, _repository, _moviepilot, _cleanup, hdhive, _quota = _service(
        (item,),
        hdhive_results=(ResubscribeHDHiveResult(ResubscribeHDHiveDisposition.CONFIRMED),),
        exhausted=True,
    )

    outcome = service.execute("resubscribe-library", _Progress(), {})

    assert outcome.status is TaskOutcomeStatus.SKIPPED
    assert outcome.details["quota_exhausted"] is True
    assert hdhive.calls == []


def test_cancellation_before_cleanup_effect_does_not_stage_a_child() -> None:
    item = _item("501", action="delete", cleanup_ids=("emby-old",))
    service, _repository, _moviepilot, cleanup, _hdhive, _quota = _service((item,))

    with pytest.raises(RuntimeError, match="fixture cancellation"):
        service.execute("resubscribe-library", _Progress(fail_after=2), {})

    assert cleanup.calls == []


def test_rule_identity_drift_fails_before_any_durable_effect() -> None:
    class _StaleRepository(_Repository):
        def list_needed_items(self) -> Sequence[ResubscribeExecutionItem]:
            items = tuple(super().list_needed_items())
            self.statuses[items[0].key] = "subscribed"
            return items

    item = _item("502", action="delete", cleanup_ids=("emby-old",))
    repository = _StaleRepository((item,))
    moviepilot = _MoviePilot()
    cleanup = _Cleanup()
    hdhive = _HDHive(())
    quota = _Quota()
    service = NativeResubscribeTaskService(repository, moviepilot, cleanup, hdhive, quota, _Policy())

    outcome = service.execute("resubscribe-library", _Progress(), {})

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert outcome.details["provider_failures"] == [
        {
            "item_id": item.key.item_id,
            "action": "delete",
            "source": "moviepilot",
            "error_type": "resubscribe_rule_changed",
            "retryable": False,
        }
    ]
    assert cleanup.calls == []
    assert moviepilot.calls == []
    assert hdhive.calls == []


class _HDHiveProvider:
    def get_resources(self, tmdb_id: str, media_type: str, *, season: int | None = None) -> Any:
        assert (tmdb_id, media_type, season) == ("601", "movie", None)
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {
                "resources": [
                    {"slug": "too-expensive", "unlock_points": 50, "title": "中字 4K"},
                    {"slug": "selected", "unlock_points": 1, "title": "中字 1080p"},
                ]
            },
        )


class _SharingOperations:
    def __init__(self) -> None:
        self.intents: list[Any] = []
        self.cancelled: list[UUID] = []

    def reserve_with_replay(self, intent: Any) -> tuple[Any, bool]:
        self.intents.append(intent)
        return SimpleNamespace(id=intent.id, status=SharingOperationStatus.READY, error=None), False

    def read(self, operation_id: UUID) -> Any:
        return SimpleNamespace(id=operation_id, status=SharingOperationStatus.CONFIRMED, error=None)

    def request_cancel(self, operation_id: UUID) -> None:
        self.cancelled.append(operation_id)


class _SharingExecutor:
    def execute(self, operation_id: UUID, **values: Any) -> Any:
        values["before_request"]()
        return SimpleNamespace(
            operation=SimpleNamespace(id=operation_id, status=SharingOperationStatus.CONFIRMED, error=None)
        )


def test_hdhive_gateway_filters_then_uses_identifier_only_p9_operation() -> None:
    operations = _SharingOperations()
    gateway = NativeResubscribeHDHiveGateway(
        cast(Any, _HDHiveProvider()),
        cast(Any, operations),
        cast(Any, _SharingExecutor()),
        lambda: {"filter": {"max_points": 5, "zh_sub_only": True}},
        worker_id="fixture-worker",
        lease_seconds=90,
    )

    result = gateway.execute(
        _item("601", source="hdhive"),
        occurrence="fixture-occurrence",
        progress=_Progress(),
    )

    assert result.disposition is ResubscribeHDHiveDisposition.CONFIRMED
    assert len(operations.intents) == 1
    intent = operations.intents[0]
    assert intent.request == {
        "provider": "auto_subscription",
        "action": "hdhive_transfer",
        "payload": {
            "slug": "selected",
            "slug_hash": hashlib.sha256(b"selected").hexdigest(),
            "tmdb_id": "601",
            "media_type": "movie",
            "season_number": None,
        },
    }
    assert set(intent.request) == {"provider", "action", "payload"}
