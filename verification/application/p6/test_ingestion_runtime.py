from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import httpx
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
from etk.domains.p115.media_info_adapters import ControlledProcessOutput, ControlledProcessProfile
from etk.domains.p115.media_info_formatter import P115MediaInfoPolicySnapshot
from etk.platform import Settings
from etk.platform.jobs import OutboxClaim, OutboxEventRecord, OutboxStatus, PublishBatchResult
from etk.processes.celery_app import EXECUTE_INGESTION_MONITOR_TASK
from etk.processes.p6_ingestion import (
    CeleryIngestionMonitorPublisher,
    IngestionMonitorCoordinator,
    NativeIngestionFingerprintGateway,
    NativeIngestionPreparationGateway,
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


def test_strm_polling_requires_the_current_pickcode_or_sha1_identity() -> None:
    binding = _binding()
    observed = {
        "Id": "101",
        "Type": "Movie",
        "Path": binding.path,
        "MediaSources": [{"Path": "https://etk.example.invalid/api/p115/play/stale-pick"}],
    }
    transport = httpx.MockTransport(lambda _request: httpx.Response(200, json={"Items": [observed]}))
    gateway = NativeIngestionPreparationGateway(
        settings=lambda: Settings(
            emby_server_url="http://emby.example.invalid",
            emby_api_key=SecretStr("fixture-token"),
            emby_user_id="fixture-user",
        ),
        pending_bindings=cast(Any, object()),
        processes=cast(Any, object()),
        policies=cast(Any, object()),
        client_factory=lambda: httpx.Client(transport=transport),
        sleep=lambda _delay: None,
    )

    assert gateway.poll(binding) is None

    observed["MediaSources"] = [{"Path": "https://etk.example.invalid/api/p115/play/pc-fixture"}]
    assert gateway.poll(binding) == observed


def test_native_ingestion_fingerprint_gateway_resolves_account_bound_strm_identity(tmp_path) -> None:
    path = tmp_path / "Movie.strm"
    path.write_text("https://etk.example.invalid/api/p115/play/pc%2Dfixture/Movie.mkv", encoding="utf-8")
    calls: list[tuple[str, str]] = []
    repository = SimpleNamespace(
        resolve_sha1_by_pick_code=lambda pick_code, *, account_id: calls.append((pick_code, account_id)) or "a" * 40
    )
    gateway = NativeIngestionFingerprintGateway(
        settings=lambda: Settings(monitor_sha1_pc_search=True),
        credentials=cast(Any, SimpleNamespace(load=lambda: SimpleNamespace(account_id="115001"))),
        repository=cast(Any, repository),
    )

    assert gateway.extract(str(path)) == ("pc-fixture", "A" * 40)
    assert calls == [("pc-fixture", "115001")]


def test_native_ingestion_fingerprint_gateway_supports_virtual_sha1_and_disabled_policy() -> None:
    repository = SimpleNamespace(
        resolve_sha1_by_pick_code=lambda *_args, **_options: (_ for _ in ()).throw(
            AssertionError("virtual SHA1 must not query the PickCode repository")
        )
    )
    url = f"https://etk.example.invalid/api/p115/virtual-play/item/{'a' * 40}"
    enabled = NativeIngestionFingerprintGateway(
        settings=lambda: Settings(monitor_sha1_pc_search=True),
        credentials=cast(Any, object()),
        repository=cast(Any, repository),
    )
    disabled = NativeIngestionFingerprintGateway(
        settings=lambda: Settings(monitor_sha1_pc_search=False),
        credentials=cast(Any, object()),
        repository=cast(Any, repository),
    )

    assert enabled.extract(url) == (None, "A" * 40)
    assert disabled.extract(url) == (None, None)


def test_native_ingestion_preparation_uses_bounded_ffprobe_and_reserves_physical_binding(tmp_path) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    bindings: list[tuple[str, Mapping[str, Any] | None]] = []

    class Processes:
        def run(self, *args: Any, **options: Any) -> Any:
            calls.append((args, dict(options)))
            return SimpleNamespace(
                succeeded=True,
                parsed_output={
                    "format": {"format_name": "matroska", "duration": "1", "size": "7"},
                    "streams": [{"index": 0, "codec_type": "video", "codec_name": "h264"}],
                    "chapters": [],
                },
            )

    pending = SimpleNamespace(
        enqueue_binding=lambda binding_path, *, mediainfo=None: bindings.append((binding_path, mediainfo))
    )
    gateway = NativeIngestionPreparationGateway(
        settings=Settings,
        pending_bindings=cast(Any, pending),
        processes=cast(Any, Processes()),
        policies=cast(
            Any,
            SimpleNamespace(
                snapshot=lambda: P115MediaInfoPolicySnapshot.create(
                    language_mapping=[{"label": "英语", "value": "en", "aliases": ["eng"]}],
                    default_stream_config={},
                    language_mapping_revision="language-v1",
                    default_stream_config_revision="streams-v1",
                )
            ),
        ),
    )

    assert gateway.prepare(_monitor_item(str(path))) is True
    assert calls[0][0][0] is ControlledProcessProfile.FFPROBE
    assert calls[0][0][1][-1] == str(path)
    assert calls[0][1] == {
        "max_stdin_bytes": 0,
        "max_stdout_bytes": 16 * 1024 * 1024,
        "max_stderr_bytes": 256 * 1024,
        "timeout_seconds": 300.0,
        "output": ControlledProcessOutput.JSON,
    }
    assert bindings[0][0] == str(path)
    assert bindings[0][1] is not None


@pytest.mark.parametrize(
    ("succeeded", "parsed_output"),
    ((False, {}), (True, [])),
)
def test_native_ingestion_preparation_rejects_failed_or_invalid_ffprobe(
    tmp_path,
    succeeded: bool,
    parsed_output: object,
) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    pending = SimpleNamespace(
        enqueue_binding=lambda *_args, **_options: (_ for _ in ()).throw(
            AssertionError("invalid ffprobe must not reserve a binding")
        )
    )
    processes = SimpleNamespace(
        run=lambda *_args, **_options: SimpleNamespace(succeeded=succeeded, parsed_output=parsed_output)
    )
    gateway = NativeIngestionPreparationGateway(
        settings=Settings,
        pending_bindings=cast(Any, pending),
        processes=cast(Any, processes),
        policies=cast(Any, object()),
    )

    assert gateway.prepare(_monitor_item(str(path))) is False


def test_native_ingestion_preparation_reserves_strm_without_ffprobe(tmp_path) -> None:
    path = tmp_path / "Movie.strm"
    path.write_text("https://etk.example.invalid/api/p115/play/pc-fixture", encoding="utf-8")
    calls: list[tuple[str, Mapping[str, Any] | None]] = []
    pending = SimpleNamespace(
        enqueue_binding=lambda binding_path, *, mediainfo=None: calls.append((binding_path, mediainfo))
    )
    gateway = NativeIngestionPreparationGateway(
        settings=Settings,
        pending_bindings=cast(Any, pending),
        processes=cast(
            Any,
            SimpleNamespace(
                run=lambda *_args, **_options: (_ for _ in ()).throw(
                    AssertionError("STRM preparation must not run ffprobe")
                )
            ),
        ),
        policies=cast(Any, object()),
    )

    item = replace(_monitor_item(str(path)), classification=MonitorClassification.STRM)
    assert gateway.prepare(item) is True
    assert calls == [(str(path), None)]


def test_native_ingestion_poll_distinguishes_authoritative_empty_from_transport_failure() -> None:
    settings = Settings(
        emby_server_url="http://emby.example.invalid",
        emby_api_key=SecretStr("fixture-token"),
        emby_user_id="fixture-user",
    )
    empty = NativeIngestionPreparationGateway(
        settings=lambda: settings,
        pending_bindings=cast(Any, object()),
        processes=cast(Any, object()),
        policies=cast(Any, object()),
        client_factory=lambda: httpx.Client(
            transport=httpx.MockTransport(lambda _request: httpx.Response(200, json={"Items": []}))
        ),
        sleep=lambda _delay: None,
    )
    failed = NativeIngestionPreparationGateway(
        settings=lambda: settings,
        pending_bindings=cast(Any, object()),
        processes=cast(Any, object()),
        policies=cast(Any, object()),
        client_factory=lambda: httpx.Client(
            transport=httpx.MockTransport(lambda _request: httpx.Response(503, json={"error": "fixture"}))
        ),
        sleep=lambda _delay: None,
    )

    assert empty.poll(_binding()) is None
    with pytest.raises(RuntimeError, match="Emby ingestion path query failed"):
        failed.poll(_binding())


def test_coordinator_leader_starts_and_stops_native_schedules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from etk.processes import coordinator as coordinator_module

    events: list[str] = []

    class Schedules:
        def __init__(self, _service: object) -> None:
            events.append("schedules.init")

        def start(self) -> None:
            events.append("schedules.start")

        def stop(self) -> None:
            events.append("schedules.stop")

    monkeypatch.setattr(coordinator_module, "PersistentScheduleManager", Schedules)
    runtime = coordinator_module.CoordinatorRuntime.__new__(coordinator_module.CoordinatorRuntime)
    state = cast(Any, runtime)
    state._service = object()
    state._schedules = None
    state._refresh_settings = lambda **_options: events.append("settings.refresh")

    runtime._become_native_leader()
    runtime._shutdown_native_leader()

    assert events == [
        "schedules.init",
        "schedules.start",
        "settings.refresh",
        "schedules.stop",
    ]
