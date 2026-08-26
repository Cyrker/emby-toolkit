from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest
from pydantic import SecretStr

from etk.domains.ingestion import (
    BindingStatus,
    FileObservation,
    MonitorClassification,
    MonitorEventType,
    MonitorItemProjection,
    MonitorItemStatus,
    MonitorSource,
    PendingBindingProjection,
)
from etk.platform import Settings
from etk.platform.jobs import OutboxClaim, OutboxEventRecord, OutboxStatus, PublishBatchResult
from etk.processes.celery_app import EXECUTE_INGESTION_MONITOR_TASK
from etk.processes.p6_ingestion import (
    CeleryIngestionMonitorPublisher,
    IngestionMonitorCoordinator,
    LegacyIngestionPreparationGateway,
    PersistentIngestionIngressGateway,
    PersistentPendingBindingGateway,
)
from etk.processes.runtime_config import TaskRuntimeConfig

NOW = datetime(2026, 8, 26, 1, 2, 3, tzinfo=UTC)
ITEM_ID = UUID("00000000-0000-0000-0000-000000000971")
BINDING_ID = UUID("00000000-0000-0000-0000-000000000972")
CLAIM_TOKEN = UUID("00000000-0000-0000-0000-000000000973")
OUTBOX_ID = UUID("00000000-0000-0000-0000-000000000974")


def _monitor_item(path: str, *, status: MonitorItemStatus = MonitorItemStatus.PENDING) -> MonitorItemProjection:
    return MonitorItemProjection(
        id=ITEM_ID,
        event_sequence=7,
        producer_key="fixture-event",
        request_hash=b"a" * 32,
        source=MonitorSource.WATCHDOG,
        event_type=MonitorEventType.UPSERT,
        path=path,
        path_key=path.casefold(),
        related_path=None,
        correlation_id=None,
        monitor_root="/fixture/library",
        config_revision="b" * 64,
        refresh_delay_seconds=0,
        classification=MonitorClassification.PHYSICAL,
        status=status,
        attempts=0,
        available_at=NOW,
        stability_deadline=NOW + timedelta(seconds=60),
        last_size=None,
        last_mtime_ns=None,
        stable_matches=0,
        claimed_by=None,
        claim_token=None,
        claimed_at=None,
        lease_expires_at=None,
        result=None,
        error=None,
        finished_at=None,
        created_at=NOW,
        updated_at=NOW,
    )


def _binding(*, status: BindingStatus = BindingStatus.PENDING) -> PendingBindingProjection:
    claimed = status is BindingStatus.CLAIMED
    terminal = status.terminal
    return PendingBindingProjection(
        id=BINDING_ID,
        binding_key=f"monitor:{ITEM_ID}",
        request_hash=b"c" * 32,
        monitor_item_id=ITEM_ID,
        path="/fixture/library/Movie.strm",
        path_key="/fixture/library/movie.strm",
        sha1="A" * 40,
        pick_code="pc-fixture",
        media_info={"MediaSourceInfo": {"MediaStreams": []}},
        status=status,
        attempts=1 if claimed or terminal else 0,
        available_at=NOW,
        claimed_by="fixture-binding" if claimed else None,
        claim_token=CLAIM_TOKEN if claimed else None,
        claimed_at=NOW if claimed else None,
        lease_expires_at=NOW + timedelta(seconds=90) if claimed else None,
        emby_item_id="101" if status is BindingStatus.CONFIRMED else None,
        result=({"ok": True, "accepted": True, "item_id": "101"} if status is BindingStatus.CONFIRMED else None),
        error=None,
        finished_at=NOW if terminal else None,
        created_at=NOW,
        updated_at=NOW,
    )


class RecordingIngressLedger:
    def __init__(self) -> None:
        self.batches: list[tuple[Any, ...]] = []

    def record(self, intents):
        values = tuple(intents)
        self.batches.append(values)
        return values


def test_ingress_preserves_watchdog_scan_and_active_sources_in_durable_policy() -> None:
    settings = Settings(
        monitor_enabled=True,
        monitor_paths=["/fixture/library"],
        monitor_extensions=[".mkv"],
    )
    ledger = RecordingIngressLedger()
    keys = iter(("move-event", "scan-event", "active-event"))
    gateway = PersistentIngestionIngressGateway(
        settings=lambda: settings,
        ledger=ledger,  # type: ignore[arg-type]
        event_key_factory=lambda: next(keys),
    )

    gateway.record_event(
        "moved",
        "/fixture/library/New.mkv",
        related_path="/fixture/library/Old.mkv",
        source="watchdog",
    )
    gateway.record_event("created", "/fixture/library/Scan.mkv", source="scan")
    gateway.record_event("created", "/fixture/library/Active.mkv", source="compatibility-active")

    moved = ledger.batches[0]
    assert [intent.event_type for intent in moved] == [MonitorEventType.DELETED, MonitorEventType.UPSERT]
    assert moved[0].correlation_id == moved[1].correlation_id
    assert all(intent.source is MonitorSource.WATCHDOG for intent in moved)
    assert ledger.batches[1][0].source is MonitorSource.SCAN
    assert ledger.batches[2][0].source is MonitorSource.ACTIVE
    assert ledger.batches[2][0].producer_key.startswith("active:created:")


class FakeCelery:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def send_task(self, name: str, **options: Any) -> None:
        self.calls.append((name, options))


def _outbox_claim() -> OutboxClaim:
    event = OutboxEventRecord(
        id=OUTBOX_ID,
        event_key=f"ingestion.monitor.requested:{ITEM_ID}",
        topic="ingestion.monitor.requested",
        aggregate_type="ingestion_monitor_item",
        aggregate_id=str(ITEM_ID),
        payload_version=1,
        payload={
            "item_id": str(ITEM_ID),
            "event_sequence": 7,
            "producer_key": "fixture-event",
            "request_hash": "ab" * 32,
        },
        status=OutboxStatus.PUBLISHING,
        available_at=NOW,
        claimed_by="fixture-coordinator",
        claim_token=CLAIM_TOKEN,
        claimed_at=NOW,
        published_at=None,
        processed_at=None,
        publish_attempts=1,
        max_publish_attempts=20,
        last_error=None,
        created_at=NOW,
        updated_at=NOW,
    )
    return OutboxClaim(event=event, claim_token=CLAIM_TOKEN)


def test_ingestion_outbox_publisher_uses_item_id_as_deterministic_task_id() -> None:
    celery = FakeCelery()
    publisher = CeleryIngestionMonitorPublisher(
        TaskRuntimeConfig(queue_name="fixture.serial"),
        celery_app=celery,
    )

    publisher.publish(_outbox_claim())

    assert celery.calls == [
        (
            EXECUTE_INGESTION_MONITOR_TASK,
            {
                "kwargs": {"item_id": str(ITEM_ID)},
                "task_id": str(ITEM_ID),
                "queue": "fixture.serial",
                "routing_key": "fixture.serial",
                "serializer": "json",
            },
        )
    ]


class CoordinatorLedger:
    def __init__(self, item: MonitorItemProjection) -> None:
        self.item = item
        self.samples: list[tuple[UUID, FileObservation]] = []
        self.suspend_calls = 0
        self.resume_calls = 0

    def resume_suspended(self, **_options: Any) -> int:
        self.resume_calls += 1
        return 2

    def suspend_active(self) -> int:
        self.suspend_calls += 1
        return 3

    def expire_stale_claims(self, **_options: Any) -> int:
        return 1

    def expire_stale_bindings(self, **_options: Any) -> int:
        return 1

    def due_for_stability(self, **_options: Any):
        return (self.item,)

    def record_stability_sample(self, item_id: UUID, observation: FileObservation):
        self.samples.append((item_id, observation))
        return self.item, None

    def reject_dead_dispatches(self, **_options: Any) -> int:
        return 0


class CoordinatorStore:
    def __init__(self) -> None:
        self.requeue: dict[str, Any] | None = None
        self.publish: dict[str, Any] | None = None

    def requeue_stale_published(self, **options: Any) -> None:
        self.requeue = options

    def publish_outbox_batch(self, _publisher: object, **options: Any) -> PublishBatchResult:
        self.publish = options
        return PublishBatchResult(claimed=0, published=0, failed=0, dead=0)


def test_coordinator_samples_files_and_pauses_backlog_on_enabled_transition(tmp_path) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    ledger = CoordinatorLedger(_monitor_item(str(path)))
    store = CoordinatorStore()
    runtime = IngestionMonitorCoordinator(
        store=store,  # type: ignore[arg-type]
        ledger=ledger,  # type: ignore[arg-type]
        publisher=SimpleNamespace(),  # type: ignore[arg-type]
        config=TaskRuntimeConfig(outbox_batch_size=7),
        publisher_id="fixture-ingestion",
    )

    enabled = runtime.tick(Settings(monitor_enabled=True))
    disabled = runtime.tick(Settings(monitor_enabled=False))

    assert enabled.resumed == 2
    assert enabled.expired_claims == enabled.expired_bindings == 1
    assert enabled.sampled == 1
    assert ledger.samples == [(ITEM_ID, FileObservation(True, size=7, mtime_ns=path.stat().st_mtime_ns))]
    assert disabled.suspended == 3
    assert ledger.resume_calls == ledger.suspend_calls == 1
    assert store.publish is not None and store.publish["topics"] == ("ingestion.monitor.requested",)


def test_disabled_coordinator_recovers_claims_before_suspending_and_does_not_publish() -> None:
    events: list[str] = []

    class DisabledLedger(CoordinatorLedger):
        def expire_stale_claims(self, **_options: Any) -> int:
            events.append("claims.expire")
            return 1

        def expire_stale_bindings(self, **_options: Any) -> int:
            events.append("bindings.expire")
            return 1

        def suspend_active(self) -> int:
            events.append("items.suspend")
            return 1

        def due_for_stability(self, **_options: Any):
            raise AssertionError("disabled ingestion must not sample files")

    ledger = DisabledLedger(_monitor_item("/fixture/library/Movie.mkv"))
    store = CoordinatorStore()
    runtime = IngestionMonitorCoordinator(
        store=store,  # type: ignore[arg-type]
        ledger=ledger,  # type: ignore[arg-type]
        publisher=SimpleNamespace(),  # type: ignore[arg-type]
        config=TaskRuntimeConfig(outbox_batch_size=7),
        publisher_id="fixture-ingestion",
    )

    result = runtime.tick(Settings(monitor_enabled=False))

    assert events == ["claims.expire", "bindings.expire", "items.suspend"]
    assert result.suspended == result.expired_claims == result.expired_bindings == 1
    assert result.publish == PublishBatchResult(claimed=0, published=0, failed=0, dead=0)
    assert store.requeue is None
    assert store.publish is None


def test_enabled_state_is_not_latched_when_the_database_transition_fails() -> None:
    class FlakyLedger(CoordinatorLedger):
        def __init__(self) -> None:
            super().__init__(_monitor_item("/fixture/library/Movie.mkv"))
            self.failures = 1

        def suspend_active(self) -> int:
            self.suspend_calls += 1
            if self.failures:
                self.failures -= 1
                raise RuntimeError("fixture database unavailable")
            return 1

    ledger = FlakyLedger()
    runtime = IngestionMonitorCoordinator(
        store=CoordinatorStore(),  # type: ignore[arg-type]
        ledger=ledger,  # type: ignore[arg-type]
        publisher=SimpleNamespace(),  # type: ignore[arg-type]
        config=TaskRuntimeConfig(),
        publisher_id="fixture-ingestion",
    )

    with pytest.raises(RuntimeError, match="fixture database unavailable"):
        runtime.synchronize_enabled(False)

    assert runtime.synchronize_enabled(False) == (1, 0)
    assert ledger.suspend_calls == 2


def test_file_observation_follows_symlink_targets_and_maps_os_errors_to_missing(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from etk.processes import p6_ingestion as ingestion_module

    target = tmp_path / "target.mkv"
    target.write_bytes(b"x" * 8192)
    link = tmp_path / "linked.mkv"
    link.symlink_to(target)
    item = _monitor_item(str(link))

    assert ingestion_module._observe(item).size == 8192
    monkeypatch.setattr(ingestion_module.os, "stat", lambda _path: (_ for _ in ()).throw(PermissionError()))
    assert ingestion_module._observe(item) == FileObservation(False)


class BindingLedger:
    def __init__(self) -> None:
        self.current = _binding()

    def lookup_for_item_ready(self, **_values: Any) -> PendingBindingProjection:
        return self.current

    def claim_binding_by_id(self, _binding_id: UUID, **_values: Any) -> PendingBindingProjection | None:
        if self.current.status not in {BindingStatus.PENDING, BindingStatus.RETRYABLE}:
            return None
        self.current = replace(
            self.current,
            status=BindingStatus.CLAIMED,
            attempts=self.current.attempts + 1,
            claimed_by="fixture-binding",
            claim_token=CLAIM_TOKEN,
            claimed_at=NOW,
            lease_expires_at=NOW + timedelta(seconds=90),
        )
        return self.current

    def read_binding(self, _binding_id: UUID) -> PendingBindingProjection:
        return self.current

    def mark_binding_confirmed(
        self,
        _binding_id: UUID,
        _claim_token: UUID,
        *,
        emby_item_id: str,
        result: dict[str, Any],
    ) -> object:
        self.current = replace(
            self.current,
            status=BindingStatus.CONFIRMED,
            claimed_by=None,
            claim_token=None,
            claimed_at=None,
            lease_expires_at=None,
            emby_item_id=emby_item_id,
            result=result,
            finished_at=NOW,
        )
        return SimpleNamespace(binding=self.current)


class BindingDelivery:
    def __init__(self) -> None:
        self.calls: list[tuple[PendingBindingProjection, dict[str, Any]]] = []

    def deliver(self, binding: PendingBindingProjection, item: Mapping[str, Any]) -> dict[str, Any]:
        self.calls.append((binding, dict(item)))
        return {"job_id": "fixture-job"}


def test_pending_binding_completion_is_single_use_and_replays_without_delivery() -> None:
    ledger = BindingLedger()
    delivery = BindingDelivery()
    gateway = PersistentPendingBindingGateway(
        ledger=ledger,  # type: ignore[arg-type]
        delivery=delivery,
        claimant_id="fixture-binding",
    )
    item = {"Id": "101", "Type": "Movie", "Path": "/fixture/library/Movie.strm"}

    accepted = gateway.accept_pending_binding(item, sha1="A" * 40, expected_pick_code="PC-FIXTURE")
    replay = gateway.accept_pending_binding(item, sha1="A" * 40, expected_pick_code="pc-fixture")
    preverified_replay = gateway.replay_pending_binding(
        emby_item_id="101",
        sha1="A" * 40,
        expected_pick_code="pc-fixture",
    )

    assert accepted == {"ok": True, "accepted": True, "item_id": "101"}
    assert replay == preverified_replay == {"ok": True, "skipped": True, "reason": "already_completed"}
    assert len(delivery.calls) == 1


def test_worker_timeout_does_not_steal_an_active_callback_binding_claim() -> None:
    ledger = BindingLedger()
    ledger.current = _binding(status=BindingStatus.CLAIMED)
    gateway = PersistentPendingBindingGateway(
        ledger=ledger,  # type: ignore[arg-type]
        delivery=BindingDelivery(),
        claimant_id="fixture-worker-fallback",
    )

    gateway.reject_unresolved(ledger.current, reason="emby_item_not_found")

    assert ledger.current == _binding(status=BindingStatus.CLAIMED)


def test_strm_polling_requires_the_current_pickcode_or_sha1_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from handler import emby

    binding = _binding()
    observed = {
        "Id": "101",
        "Path": binding.path,
        "MediaSources": [{"Path": "https://etk.example.invalid/api/p115/play/stale-pick"}],
    }
    monkeypatch.setattr(
        emby,
        "wait_for_emby_items_by_path",
        lambda *_args, **_kwargs: {binding.path: observed},
    )
    monkeypatch.setattr(emby, "_normalize_emby_media_path", lambda path: str(path))
    gateway = LegacyIngestionPreparationGateway(
        lambda: Settings(
            emby_server_url="http://emby.example.invalid",
            emby_api_key=SecretStr("fixture-token"),
            emby_user_id="fixture-user",
        )
    )

    assert gateway.poll(binding, object()) is None

    observed["MediaSources"] = [{"Path": "https://etk.example.invalid/api/p115/play/pc-fixture"}]
    assert gateway.poll(binding, object()) == observed


def test_coordinator_installs_ingress_before_watchdog_and_clears_it_after_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_manager
    from etk.processes import coordinator as coordinator_module

    events: list[str] = []

    class Supervisor:
        def start(self, **_options: Any) -> None:
            events.append("supervisor.start")

        def stop(self) -> None:
            events.append("supervisor.stop")

    class Schedules:
        def __init__(self, _service: object) -> None:
            return None

        def start(self) -> None:
            events.append("schedules.start")

        def stop(self) -> None:
            events.append("schedules.stop")

    monkeypatch.setattr(coordinator_module, "RuntimeSupervisor", Supervisor)
    monkeypatch.setattr(coordinator_module, "PersistentScheduleManager", Schedules)
    monkeypatch.setattr(
        coordinator_module,
        "install_persistent_ingestion_ingress",
        lambda _gateway: events.append("ingress.install"),
    )
    monkeypatch.setattr(
        coordinator_module,
        "clear_persistent_ingestion_ingress",
        lambda _gateway: events.append("ingress.clear"),
    )
    monkeypatch.setattr(
        coordinator_module,
        "install_persistent_upload_monitor_ingress",
        lambda _gateway: events.append("upload.install"),
    )
    monkeypatch.setattr(
        coordinator_module,
        "clear_persistent_upload_monitor_ingress",
        lambda _gateway: events.append("upload.clear"),
    )
    monkeypatch.setattr(
        task_manager,
        "install_persistent_submission_gateway",
        lambda _gateway: events.append("tasks.install"),
    )
    monkeypatch.setattr(
        task_manager,
        "clear_persistent_submission_gateway",
        lambda _gateway: events.append("tasks.clear"),
    )
    runtime = coordinator_module.CoordinatorRuntime.__new__(coordinator_module.CoordinatorRuntime)
    state = cast(Any, runtime)
    state._service = object()
    state._ingestion_ingress = object()
    state._ingestion_coordinator = SimpleNamespace(synchronize_enabled=lambda _enabled: events.append("ingestion.sync"))
    state._p115_upload_ingress = SimpleNamespace(
        ensure_config=lambda: events.append("upload.ensure_config"),
        enabled=lambda: True,
    )
    state._p115_upload_coordinator = object()
    state._config_adapter = SimpleNamespace(load=lambda: Settings(monitor_enabled=True))
    state._gateway = None
    state._schedules = None
    state._supervisor = None
    state._refresh_settings = lambda **_options: events.append("settings.refresh")

    runtime._become_leader()
    runtime._shutdown_leader()

    assert events.index("ingress.install") < events.index("supervisor.start")
    assert events.index("upload.ensure_config") < events.index("upload.install")
    assert events.index("upload.install") < events.index("supervisor.start")
    assert events.index("supervisor.stop") < events.index("ingress.clear")
    assert events.index("supervisor.stop") < events.index("upload.clear")
    assert events.index("ingress.clear") < events.index("tasks.clear")
