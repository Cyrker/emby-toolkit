from __future__ import annotations

from contextlib import nullcontext
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

import config_manager  # noqa: F401
from etk.domains.subscription.assistant import (
    AssistantMaintenanceRun,
    SqlAlchemyAssistantMaintenanceRepository,
    SubscribeAssistantMaintenanceService,
)
from etk.processes.p7_subscription_effects import DurableMoviePilotEffectIndeterminateError
from services.subscribe_assistant import manager as manager_module


class _Repository:
    def __init__(self) -> None:
        self.phases: tuple[str, ...] = ()
        self.completed_phases: tuple[str, ...] = ()
        self.results: dict[str, int] = {}
        self.completed = False

    def load_or_create(
        self,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        phases: tuple[str, ...],
    ) -> AssistantMaintenanceRun:
        assert worker_id
        if not self.phases:
            self.phases = tuple(phases)
        return self._run(job_id, attempt_id)

    def checkpoint_phase(
        self,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        phase: str,
        count: int,
    ) -> AssistantMaintenanceRun:
        assert worker_id
        if phase not in self.completed_phases:
            self.completed_phases = (*self.completed_phases, phase)
            self.results[phase] = count
        return self._run(job_id, attempt_id)

    def complete(self, job_id: UUID, attempt_id: UUID, worker_id: str) -> AssistantMaintenanceRun:
        assert worker_id
        self.completed = True
        return self._run(job_id, attempt_id)

    def _run(self, job_id: UUID, attempt_id: UUID) -> AssistantMaintenanceRun:
        return AssistantMaintenanceRun(
            job_id,
            self.phases,
            self.completed_phases,
            dict(self.results),
            self.completed,
            attempt_id,
        )


class _Session:
    def __init__(self, calls: list[str], *, fail_phase: str | None = None) -> None:
        self.calls = calls
        self.fail_phase = fail_phase

    def phases(self) -> tuple[str, ...]:
        return ("delete_records_cleanup", "snapshot_cleanup")

    def run_phase(self, phase: str, checkpoint) -> int:
        checkpoint()
        self.calls.append(phase)
        if phase == self.fail_phase:
            raise RuntimeError("fixture phase interruption")
        return 1


class _Gateway:
    def __init__(self, sessions: list[_Session]) -> None:
        self.sessions = sessions
        self.identities: list[tuple[UUID, UUID, str]] = []

    def begin(
        self,
        _processor: object,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
    ) -> _Session:
        self.identities.append((job_id, attempt_id, worker_id))
        return self.sessions.pop(0)


class _PersistentEffects:
    def __init__(self) -> None:
        self.history_calls: list[tuple[dict[str, Any], str | None]] = []
        self.download_calls: list[tuple[list[str], str | None]] = []
        self.history_result = True
        self.download_result = True
        self.error: Exception | None = None

    def delete_transfer_history(
        self,
        record: dict[str, Any],
        *,
        operation_occurrence: str | None = None,
    ) -> bool:
        if self.error is not None:
            raise self.error
        self.history_calls.append((dict(record), operation_occurrence))
        return self.history_result

    def delete_download_tasks(
        self,
        hashes: list[str],
        *,
        operation_occurrence: str | None = None,
    ) -> bool:
        if self.error is not None:
            raise self.error
        self.download_calls.append((list(hashes), operation_occurrence))
        return self.download_result


class _Progress:
    def __init__(self) -> None:
        self.checkpoints = 0
        self.updates: list[tuple[int, str]] = []

    def checkpoint(self) -> None:
        self.checkpoints += 1

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))


def test_completed_phases_are_skipped_after_attempt_restart() -> None:
    job_id = uuid4()
    first_attempt = uuid4()
    second_attempt = uuid4()
    third_attempt = uuid4()
    first_calls: list[str] = []
    second_calls: list[str] = []
    third_calls: list[str] = []
    repository = _Repository()
    gateway = _Gateway(
        [
            _Session(first_calls, fail_phase="snapshot_cleanup"),
            _Session(second_calls),
            _Session(third_calls),
        ]
    )
    service = SubscribeAssistantMaintenanceService(
        cast(SqlAlchemyAssistantMaintenanceRepository, repository),
        cast(Any, gateway),
    )

    with pytest.raises(RuntimeError, match="fixture phase interruption"):
        service.execute(
            job_id=job_id,
            attempt_id=first_attempt,
            worker_id="worker-a",
            processor=object(),
            progress=_Progress(),
        )

    recovered = service.execute(
        job_id=job_id,
        attempt_id=second_attempt,
        worker_id="worker-b",
        processor=object(),
        progress=_Progress(),
    )
    replay = service.execute(
        job_id=job_id,
        attempt_id=third_attempt,
        worker_id="worker-c",
        processor=object(),
        progress=_Progress(),
    )

    assert first_calls == ["delete_records_cleanup", "snapshot_cleanup"]
    assert second_calls == ["snapshot_cleanup"]
    assert third_calls == []
    assert recovered.completed is True
    assert replay.completed is True
    assert gateway.identities == [
        (job_id, first_attempt, "worker-a"),
        (job_id, second_attempt, "worker-b"),
        (job_id, third_attempt, "worker-c"),
    ]


def _manager(*, cfg: object | None = None) -> manager_module.SubscribeAssistantManager:
    manager = cast(Any, object.__new__(manager_module.SubscribeAssistantManager))
    manager.app_config = {}
    manager._series_title = lambda *_args: "Fixture"
    if cfg is not None:
        manager.cfg = cfg
    return cast(manager_module.SubscribeAssistantManager, manager)


def test_no_download_state_merge_preserves_concurrent_webhook_fields(monkeypatch) -> None:
    manager = _manager(cfg=SimpleNamespace(tv_no_download_days=1, no_download_actions=["pause"]))
    initial = {
        "7": {
            "subscribe_id": 7,
            "tmdb_id": "101",
            "season": 2,
            "created_at": 1,
            "subscribe_info": {"state": "R", "name": "Fixture"},
        }
    }
    merged: dict[str, Any] = {}
    monkeypatch.setattr(manager_module.time, "time", lambda: 200_000.0)
    monkeypatch.setattr(manager_module.store, "read_state", lambda _key: initial)
    monkeypatch.setattr(manager_module.moviepilot, "update_subscription_status", lambda *_args, **_kwargs: True)

    def update_state(_key, updater):
        current = {
            "7": {
                **initial["7"],
                "download_started_at": 199_999.0,
                "webhook_revision": 4,
            },
            "concurrent": {"preserved": True},
        }
        merged.update(updater(current))
        return merged

    monkeypatch.setattr(manager_module.store, "update_state", update_state)

    assert manager.run_no_download_check() == 1
    assert merged["7"]["download_started_at"] == 199_999.0
    assert merged["7"]["webhook_revision"] == 4
    assert merged["7"]["subscribe_info"]["state"] == "S"
    assert merged["concurrent"] == {"preserved": True}


def test_history_retry_merge_preserves_concurrent_retry_keys(monkeypatch) -> None:
    manager = _manager()
    monkeypatch.setattr(manager_module.time, "time", lambda: 200_000.0)
    monkeypatch.setattr(
        manager_module.store,
        "read_state",
        lambda _key: {"last_retry": {"existing": 199_000.0}},
    )
    monkeypatch.setattr(
        manager_module.moviepilot,
        "list_transfer_history",
        lambda *_args, **kwargs: [{"id": 9, "status": False}] if kwargs.get("status") is False else [],
    )
    monkeypatch.setattr(manager_module.moviepilot, "redo_transfer_history", lambda *_args, **_kwargs: True)
    empty_connection = SimpleNamespace(
        cursor=lambda: nullcontext(SimpleNamespace()),
        commit=lambda: None,
    )
    monkeypatch.setattr(manager_module.connection, "get_db_connection", lambda: nullcontext(empty_connection))
    merged: dict[str, Any] = {}

    def update_state(_key, updater):
        current = {"last_retry": {"existing": 199_000.0, "concurrent": 199_500.0}}
        merged.update(updater(current))
        return merged

    monkeypatch.setattr(manager_module.store, "update_state", update_state)

    assert manager.run_transfer_history_reconcile() == 1
    assert merged["last_retry"]["existing"] == 199_000.0
    assert merged["last_retry"]["concurrent"] == 199_500.0
    assert len(merged["last_retry"]) == 3


def test_suppression_cleanup_uses_atomic_merge(monkeypatch) -> None:
    manager = _manager()
    monkeypatch.setattr(manager_module.time, "time", lambda: 200_000.0)
    merged: dict[str, Any] = {}

    def update_state(_key, updater):
        current = {
            "tmdb:101:S2": {"expires_at": 201_000.0},
            "expired": {"expires_at": 199_000.0},
            "concurrent": {"expires_at": 202_000.0},
        }
        merged.update(updater(current))
        return merged

    monkeypatch.setattr(manager_module.store, "update_state", update_state)

    assert manager._snapshot_restore_suppressed("101", 2) is True
    assert "expired" not in merged
    assert merged["concurrent"] == {"expires_at": 202_000.0}


def _snapshot_claim(snapshot_id: int) -> manager_module.store.SnapshotClaim:
    now = datetime.now(UTC)
    return manager_module.store.SnapshotClaim(
        snapshot_id=snapshot_id,
        state_revision=1,
        job_id=uuid4(),
        attempt_id=uuid4(),
        worker_id="worker-a",
        claim_token=uuid4(),
        lease_expires_at=now + timedelta(minutes=5),
        snapshot={
            "id": snapshot_id,
            "tmdb_id": str(100 + snapshot_id),
            "season_number": 1,
            "scope_total": 10,
            "subscribe_json": {},
        },
    )


def test_snapshot_verify_claims_one_at_a_time_and_marks_latest_renewed_claim(monkeypatch) -> None:
    manager = _manager(cfg=SimpleNamespace(verify_interval_hours=1))
    first = _snapshot_claim(1)
    second = _snapshot_claim(2)
    claims = [first, second, None]
    renewed: list[manager_module.store.SnapshotClaim] = []
    marked: list[manager_module.store.SnapshotClaim] = []
    effects: list[int] = []
    checkpoints: list[str] = []

    monkeypatch.setattr(manager, "_season_total_locked", lambda *_args: None)

    def repair(snapshot, _tmdb_id, _season, _old_total, *, before_effect):
        before_effect()
        effects.append(int(snapshot["id"]))
        return True

    monkeypatch.setattr(manager, "_repair_snapshot_subscription", repair)
    monkeypatch.setattr(manager_module.store, "claim_next_snapshot", lambda *_args, **_kwargs: claims.pop(0))

    def renew(claim, *, lease_seconds):
        assert lease_seconds == 60
        value = replace(
            claim,
            state_revision=claim.state_revision + 1,
            lease_expires_at=claim.lease_expires_at + timedelta(minutes=1),
        )
        renewed.append(value)
        return value

    monkeypatch.setattr(manager_module.store, "renew_snapshot_claim", renew)
    monkeypatch.setattr(manager_module.store, "mark_snapshot_checked", marked.append)

    assert (
        manager.run_snapshot_verify(
            limit=3,
            job_id=first.job_id,
            attempt_id=first.attempt_id,
            worker_id=first.worker_id,
            lease_seconds=60,
            checkpoint=lambda: checkpoints.append("checkpoint"),
        )
        == 2
    )

    assert effects == [1, 2]
    assert len(renewed) == 4
    assert [claim.snapshot_id for claim in marked] == [1, 2]
    assert marked[0].state_revision == 3
    assert marked[1].state_revision == 3
    assert len(checkpoints) == 7


def test_snapshot_verify_does_not_start_effect_after_claim_renewal_is_lost(monkeypatch) -> None:
    manager = _manager(cfg=SimpleNamespace(verify_interval_hours=1))
    claim = _snapshot_claim(1)
    effects: list[int] = []
    marked: list[manager_module.store.SnapshotClaim] = []
    monkeypatch.setattr(manager_module.store, "claim_next_snapshot", lambda *_args, **_kwargs: claim)
    monkeypatch.setattr(
        manager_module.store,
        "renew_snapshot_claim",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            manager_module.store.SnapshotClaimLostError("fixture lost claim")
        ),
    )
    monkeypatch.setattr(manager_module.store, "mark_snapshot_checked", marked.append)
    monkeypatch.setattr(manager, "_repair_snapshot_subscription", lambda *_args, **_kwargs: effects.append(1))

    with pytest.raises(manager_module.store.SnapshotClaimLostError, match="fixture lost claim"):
        manager.run_snapshot_verify(
            limit=1,
            job_id=claim.job_id,
            attempt_id=claim.attempt_id,
            worker_id=claim.worker_id,
            lease_seconds=60,
        )

    assert effects == []
    assert marked == []


def test_snapshot_repair_renews_before_each_moviepilot_effect(monkeypatch) -> None:
    manager = _manager(cfg=SimpleNamespace(auto_search_when_delete=True))
    monkeypatch.setattr(manager, "_snapshot_restore_suppressed", lambda *_args: False)
    monkeypatch.setattr(
        manager_module.moviepilot,
        "find_subscriptions",
        lambda *_args, **_kwargs: [{"id": 7, "state": "R", "total_episode": 8}],
    )
    monkeypatch.setattr(manager_module.moviepilot, "update_subscription_status", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(manager_module.moviepilot, "search_subscription", lambda *_args, **_kwargs: True)
    checkpoints: list[str] = []

    assert (
        manager._repair_snapshot_subscription(
            {"subscribe_id": 7, "subscribe_json": {}},
            "101",
            2,
            10,
            before_effect=lambda: checkpoints.append("effect"),
        )
        is True
    )

    assert checkpoints == ["effect", "effect"]


def test_persistent_episode_residue_cleanup_routes_every_delete_through_durable_gateway(monkeypatch) -> None:
    gateway = _PersistentEffects()
    moviepilot = manager_module.moviepilot
    monkeypatch.setattr(moviepilot.settings_db, "get_setting", lambda *_args: {"moviepilot_url": "http://fixture"})
    monkeypatch.setattr(moviepilot, "_get_access_token", lambda *_args, **_kwargs: "fixture-token")
    monkeypatch.setattr(
        moviepilot,
        "analyze_mp_episode_residue_for_deletion",
        lambda *_args, **_kwargs: ([{"id": 7, "episodes": "E01"}], ["HASH-A"]),
    )
    monkeypatch.setattr(
        moviepilot,
        "analyze_mp_episode_residue_download_history",
        lambda *_args, **_kwargs: ["HASH-B"],
    )
    monkeypatch.setattr(moviepilot, "get_downloading_tasks", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(moviepilot, "get_all_torrents_from_downloaders", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        moviepilot.requests,
        "delete",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("direct DELETE bypassed ledger")),
    )

    with moviepilot.persistent_moviepilot_effects(gateway):
        result = moviepilot.smart_cleanup_mp_episode_residue("101", [2], "Fixture", {})

    assert result is True
    assert gateway.history_calls == [({"id": 7, "episodes": "E01"}, "episode-residue:101:2:history:7")]
    assert gateway.download_calls == [
        (["HASH-A"], "episode-residue:101:2:download:hash-a"),
        (["HASH-B"], "episode-residue:101:2:download:hash-b"),
    ]


def test_persistent_episode_residue_rejection_and_indeterminate_are_not_reported_as_success(monkeypatch) -> None:
    gateway = _PersistentEffects()
    moviepilot = manager_module.moviepilot
    monkeypatch.setattr(moviepilot.settings_db, "get_setting", lambda *_args: {"moviepilot_url": "http://fixture"})
    monkeypatch.setattr(moviepilot, "_get_access_token", lambda *_args, **_kwargs: "fixture-token")
    monkeypatch.setattr(
        moviepilot,
        "analyze_mp_episode_residue_for_deletion",
        lambda *_args, **_kwargs: ([{"id": 7}], []),
    )
    monkeypatch.setattr(moviepilot, "analyze_mp_episode_residue_download_history", lambda *_args, **_kwargs: [])
    gateway.history_result = False
    with moviepilot.persistent_moviepilot_effects(gateway):
        assert moviepilot.smart_cleanup_mp_episode_residue("101", [2], "Fixture", {}) is False

    gateway.error = DurableMoviePilotEffectIndeterminateError("fixture unknown effect")
    with moviepilot.persistent_moviepilot_effects(gateway):
        with pytest.raises(DurableMoviePilotEffectIndeterminateError, match="fixture unknown effect"):
            moviepilot.smart_cleanup_mp_episode_residue("101", [2], "Fixture", {})


def test_download_monitor_uses_distinct_stable_occurrences_for_same_hash_after_search(monkeypatch) -> None:
    manager = _manager(
        cfg=SimpleNamespace(
            skip_deletion=False,
            delete_record_retention_hours=24,
            auto_search_when_delete=True,
        )
    )
    calls: list[str | None] = []
    monkeypatch.setattr(manager_module.store, "has_deleted_resource", lambda *_args: False)
    monkeypatch.setattr(manager_module.store, "record_deleted_resource", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(manager_module.store, "update_state", lambda *_args, **_kwargs: {})

    def delete_download(*_args, **kwargs) -> bool:
        calls.append(kwargs.get("operation_occurrence"))
        return True

    monkeypatch.setattr(manager_module.moviepilot, "delete_download_tasks", delete_download)
    monkeypatch.setattr(manager_module.moviepilot, "search_subscription", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        manager_module.moviepilot,
        "get_downloading_tasks",
        lambda *_args, **_kwargs: [{"hash": "ABC"}],
    )

    assert (
        manager._delete_monitored_download_task(
            "ABC",
            {"tmdb_id": "101", "season": 2, "episodes": [1], "subscribe_id": 7},
            "fixture",
        )
        is True
    )
    assert calls == [
        "download-monitor:abc:initial",
        "download-monitor:abc:post-search",
    ]


def test_persistent_download_maintenance_does_not_checkpoint_rejected_cleanup(monkeypatch) -> None:
    manager = _manager(
        cfg=SimpleNamespace(
            manual_delete_listen=False,
            tracker_response_listen=True,
            tracker_keywords=["blocked"],
            delete_exclude_tags=[],
        )
    )
    torrents = {"abc": {"subscribe_id": 7}}
    monkeypatch.setattr(manager_module.store, "read_state", lambda _key: dict(torrents))
    monkeypatch.setattr(
        manager_module.store,
        "update_state",
        lambda _key, updater: updater(dict(torrents)),
    )
    monkeypatch.setattr(
        manager_module.moviepilot,
        "get_download_tasks_for_monitoring",
        lambda *_args, **_kwargs: [{"hash": "abc", "tag": "blocked"}],
    )
    monkeypatch.setattr(manager, "_register_untracked_download_tasks", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(manager, "_delete_monitored_download_task", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(manager_module, "_has_persistent_execution", lambda: True)

    with pytest.raises(RuntimeError, match="cleanup was rejected"):
        manager.run_download_check()
