from __future__ import annotations

import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text

from etk.domains.p115.upload_adapters import NativeP115UploadSourceCleanupGateway
from etk.domains.p115.upload_monitor import (
    UPLOAD_MONITOR_CONFIG_KEY,
    UPLOAD_MONITOR_TOPIC,
    P115UploadMonitorConfigService,
    UploadedSource,
    UploadEventType,
    UploadFileFingerprint,
    UploadMonitorIntent,
    UploadOperationStatus,
    UploadSource,
    upload_monitor_config_revision,
)
from etk.domains.p115.upload_repository import (
    P115UploadConflictError,
    P115UploadStateError,
    PostgresP115UploadMonitorRepository,
)
from etk.platform.db import DatabaseRuntime

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_RECORD_PREFIX = "p6-upload-fixture:"


@pytest.fixture(autouse=True)
def clean_upload_monitor_state(p6_runtime: DatabaseRuntime):
    def clean() -> None:
        with p6_runtime.engine.begin() as connection:
            connection.execute(
                text("DELETE FROM outbox_events WHERE topic = :topic"),
                {"topic": UPLOAD_MONITOR_TOPIC},
            )
            connection.execute(text("DELETE FROM p115_upload_operations"))
            connection.execute(
                text(
                    """
                    DELETE FROM p115_upload_records
                    WHERE record_type = 'completed' AND record_key LIKE :prefix
                    """
                ),
                {"prefix": f"{FIXTURE_RECORD_PREFIX}%"},
            )
            connection.execute(
                text("DELETE FROM app_settings WHERE setting_key = :setting_key"),
                {"setting_key": UPLOAD_MONITOR_CONFIG_KEY},
            )
            connection.execute(
                text(
                    """
                    UPDATE p115_upload_monitor_controls
                    SET config_revision = repeat('0', 64), applied_revision = NULL,
                        generation = 0, requested_at = NOW(), applied_at = NULL, updated_at = NOW()
                    WHERE singleton_key = 1
                    """
                )
            )

    clean()
    yield
    clean()


def _repository(runtime: DatabaseRuntime) -> PostgresP115UploadMonitorRepository:
    return PostgresP115UploadMonitorRepository(runtime.unit_of_work)


def _config() -> dict[str, object]:
    return {
        "enabled": True,
        "mappings": [
            {
                "id": "fixture-mapping",
                "enabled": True,
                "local_dir": "/fixture/upload",
                "target_cid": "1159001",
                "target_name": "Fixture",
                "mode": "keep",
                "extensions": [".mkv"],
            }
        ],
    }


def _save_config(repository: PostgresP115UploadMonitorRepository) -> str:
    config = _config()
    revision = upload_monitor_config_revision(config)
    repository.save_config(config, revision)
    return revision


def _intent(
    operation_key: str,
    revision: str,
    *,
    event_type: UploadEventType = UploadEventType.UPSERT,
    path: str | None = None,
    completed_source: UploadedSource | None = None,
    fingerprint: UploadFileFingerprint | None = None,
) -> UploadMonitorIntent:
    now = datetime.now(UTC)
    local_path = path or f"/fixture/upload/{operation_key}.mkv"
    observed_fingerprint = (
        fingerprint or UploadFileFingerprint(size=100, mtime_ns=200, device=3, inode=4)
        if event_type is UploadEventType.UPSERT
        else None
    )
    request_hash = hashlib.sha256(f"fixture:{operation_key}:{event_type.value}".encode()).digest()
    return UploadMonitorIntent(
        id=uuid4(),
        operation_key=operation_key,
        request_hash=request_hash,
        source=UploadSource.WATCHDOG,
        event_type=event_type,
        correlation_id=None,
        mapping_id="fixture-mapping",
        config_revision=revision,
        mapping=dict(_config()["mappings"][0]),  # type: ignore[index]
        path=local_path,
        path_key=os.path.normcase(os.path.abspath(local_path)),
        relative_path=os.path.basename(local_path),
        fingerprint=observed_fingerprint,
        available_at=now - timedelta(seconds=1),
        stability_deadline=now + timedelta(seconds=60),
        completed_source=completed_source.snapshot() if completed_source is not None else None,
    )


def _ready_operation(
    repository: PostgresP115UploadMonitorRepository,
    operation_key: str,
    revision: str,
    *,
    path: str | None = None,
    fingerprint: UploadFileFingerprint | None = None,
):
    operation = repository.reserve(_intent(operation_key, revision, path=path, fingerprint=fingerprint)).operation
    return repository.mark_ready(operation.id).operation


def _upload_receipt(pick_code: str) -> dict[str, object]:
    return {
        "fid": f"fid-{pick_code}",
        "pick_code": pick_code,
        "parent_id": "1159001",
        "backend": "openapi",
        "account_id": "115001",
    }


def _completed_source(operation, *, pick_code: str, record_key: str) -> UploadedSource:
    assert operation.fingerprint is not None
    return UploadedSource(
        record_key=record_key,
        operation_id=operation.id,
        mapping_id=operation.mapping_id,
        config_revision=operation.config_revision,
        local_path=operation.path,
        path_key=operation.path_key,
        relative_path=operation.relative_path,
        target_cid="1159001",
        mode="keep",
        fingerprint=operation.fingerprint,
        remote_receipt=_upload_receipt(pick_code),
        derived_paths=(),
        derived_ownership={},
        completed_at=datetime.now(UTC),
        operation_sequence=operation.sequence,
    )


def _confirm_source(
    repository: PostgresP115UploadMonitorRepository,
    revision: str,
    *,
    operation_key: str,
    path: str,
    pick_code: str,
    record_key: str,
    fingerprint: UploadFileFingerprint | None = None,
) -> UploadedSource:
    ready = _ready_operation(
        repository,
        operation_key,
        revision,
        path=path,
        fingerprint=fingerprint,
    )
    claimed = repository.claim_by_id(ready.id, worker_id=f"worker-{operation_key}", lease_seconds=30)
    assert claimed is not None and claimed.claim_token is not None
    repository.checkpoint_request(
        claimed.id,
        claimed.claim_token,
        backend="openapi",
        session={"backend": "openapi", "account_id": "115001", "request_sent": True},
    )
    receipt = _upload_receipt(pick_code)
    repository.mark_remote_confirmed(claimed.id, claimed.claim_token, receipt)
    repository.mark_projecting_local(claimed.id, claimed.claim_token)
    repository.mark_ingestion_pending(
        claimed.id,
        claimed.claim_token,
        {"derived_paths": [], "ingestion_event_key": None, "details": {}},
    )
    source = _completed_source(claimed, pick_code=pick_code, record_key=record_key)
    repository.mark_confirmed(claimed.id, claimed.claim_token, result={"ok": True}, completed=source)
    return source


def test_empty_database_upgrade_has_upload_schema_and_singleton_control(
    p6_runtime: DatabaseRuntime,
) -> None:
    inspector = inspect(p6_runtime.engine)
    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM etk_migrations.alembic_version")) == (
            "0013_p7_subscription_runtime"
        )
        assert connection.scalar(text("SELECT COUNT(*) FROM p115_upload_monitor_controls")) == 1

    assert {"p115_upload_monitor_controls", "p115_upload_operations"} <= set(inspector.get_table_names())
    operation_constraints = {
        constraint["name"] for constraint in inspector.get_check_constraints("p115_upload_operations")
    }
    assert {
        "ck_p115_upload_operation_status",
        "ck_p115_upload_operation_claim",
        "ck_p115_upload_operation_request_started",
        "ck_p115_upload_operation_finished",
    } <= operation_constraints
    operation_indexes = {index["name"] for index in inspector.get_indexes("p115_upload_operations")}
    assert {
        "ix_p115_upload_operation_stability",
        "ix_p115_upload_operation_claimable",
        "ix_p115_upload_operation_recovery",
    } <= operation_indexes


def test_config_save_and_apply_ack_are_revision_fenced_and_atomic(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    config = _config()
    revision = upload_monitor_config_revision(config)

    requested = repository.save_config(config, revision)

    assert requested.config_revision == revision
    assert requested.applied_revision is None
    assert requested.generation == 1
    assert repository.load_config() == config
    with p6_runtime.engine.connect() as connection:
        persisted = connection.execute(
            text(
                """
                SELECT setting.value_json, control.config_revision, control.generation
                FROM app_settings AS setting
                CROSS JOIN p115_upload_monitor_controls AS control
                WHERE setting.setting_key = :setting_key AND control.singleton_key = 1
                """
            ),
            {"setting_key": UPLOAD_MONITOR_CONFIG_KEY},
        ).one()
    assert dict(persisted.value_json) == config
    assert persisted.config_revision == revision
    assert persisted.generation == 1

    replay = repository.save_config(config, revision)
    assert replay.generation == 1
    applied = repository.apply_config(revision)
    assert applied.applied_revision == revision
    assert applied.applied
    with pytest.raises(P115UploadConflictError):
        repository.apply_config("f" * 64)


def test_coordinator_initializes_default_config_once_without_get_side_effects(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    service = P115UploadMonitorConfigService(repository, lambda: (".mkv",))
    default_config = {"enabled": False, "mappings": []}
    default_revision = upload_monitor_config_revision(default_config)

    assert service.get() == default_config
    assert repository.load_config() is None

    persisted, revision = service.ensure_persisted()
    control = repository.control()
    assert persisted == default_config
    assert revision == default_revision
    assert repository.load_config() == default_config
    assert control is not None
    assert control.config_revision == default_revision
    assert control.applied_revision is None
    assert control.generation == 1

    service.ensure_persisted()
    replay = repository.control()
    assert replay is not None and replay.generation == 1

    configured_revision = _save_config(repository)
    stale = repository.ensure_config(default_config, default_revision)
    assert stale.config_revision == configured_revision
    assert repository.load_config() == _config()


def test_duplicate_reservation_replays_same_hash_and_rejects_conflict(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    revision = _save_config(repository)
    intent = _intent("fixture-reservation", revision)

    created = repository.reserve(intent)
    replayed = repository.reserve(replace(intent, id=uuid4()))

    assert created.created
    assert not replayed.created
    assert replayed.operation.id == created.operation.id
    with pytest.raises(P115UploadConflictError):
        repository.reserve(replace(intent, id=uuid4(), request_hash=b"x" * 32))


def test_delete_reservation_rejects_non_keep_mapping_or_completed_source(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    revision = _save_config(repository)
    path = "/fixture/upload/mode-fence.mkv"
    path_key = os.path.normcase(os.path.abspath(path))
    source = UploadedSource(
        record_key=f"{FIXTURE_RECORD_PREFIX}mode-fence",
        operation_id=uuid4(),
        mapping_id="fixture-mapping",
        config_revision=revision,
        local_path=path,
        path_key=path_key,
        relative_path="mode-fence.mkv",
        target_cid="1159001",
        mode="keep",
        fingerprint=UploadFileFingerprint(size=100, mtime_ns=200, device=3, inode=4),
        remote_receipt=_upload_receipt("pick-mode-fence"),
        derived_paths=(),
        derived_ownership={},
        completed_at=datetime.now(UTC),
        operation_sequence=1,
    )
    source_mode_intent = _intent(
        "fixture-delete-source-mode",
        revision,
        event_type=UploadEventType.DELETED,
        path=path,
        completed_source=replace(source, mode="delete"),
    )
    mapping_mode_intent = _intent(
        "fixture-delete-mapping-mode",
        revision,
        event_type=UploadEventType.DELETED,
        path=path,
        completed_source=source,
    )
    mapping_mode_intent = replace(
        mapping_mode_intent,
        mapping={**mapping_mode_intent.mapping, "mode": "delete"},
    )

    with pytest.raises(ValueError, match="completed source snapshot does not match"):
        repository.reserve(source_mode_intent)
    with pytest.raises(ValueError, match="completed source snapshot does not match"):
        repository.reserve(mapping_mode_intent)


def test_only_one_worker_claims_and_old_claim_token_is_fenced(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    revision = _save_config(repository)
    ready = _ready_operation(repository, "fixture-claim", revision)

    def claim(worker: str):
        return _repository(p6_runtime).claim_by_id(ready.id, worker_id=worker, lease_seconds=30)

    with ThreadPoolExecutor(max_workers=2) as executor:
        claims = list(executor.map(claim, ("worker-a", "worker-b")))
    successful = [claim for claim in claims if claim is not None]
    assert len(successful) == 1
    first = successful[0]
    assert first.claim_token is not None

    repository.mark_retryable(
        first.id,
        first.claim_token,
        error={"type": "fixture_pre_request_retry"},
        retry_after_seconds=0,
    )
    second = repository.claim_by_id(first.id, worker_id="worker-c", lease_seconds=30)
    assert second is not None and second.claim_token is not None
    assert second.claim_token != first.claim_token
    with pytest.raises(P115UploadStateError):
        repository.mark_rejected(first.id, first.claim_token, error={"type": "stale_worker"})


def test_stability_sampling_promotes_after_three_identical_observations(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    revision = _save_config(repository)
    reserved = repository.reserve(_intent("fixture-stability", revision)).operation
    observation = UploadFileFingerprint(size=101, mtime_ns=201, device=3, inode=4)

    assert reserved in repository.due_for_stability()
    for expected_matches in (1, 2, 3):
        transition = repository.record_stability_sample(reserved.id, observation)
        assert transition.operation.stable_matches == expected_matches
    assert transition.operation.status is UploadOperationStatus.READY
    with p6_runtime.engine.connect() as connection:
        outbox = connection.execute(
            text("SELECT status, payload_json FROM outbox_events WHERE aggregate_id = :operation_id"),
            {"operation_id": str(reserved.id)},
        ).one()
    assert outbox.status == "pending"
    assert outbox.payload_json["request_hash"] == reserved.request_hash.hex()


def test_stale_claim_recovery_never_reissues_an_unknown_provider_request(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    revision = _save_config(repository)
    pre_request = _ready_operation(repository, "fixture-recovery-pre", revision)
    requesting = _ready_operation(repository, "fixture-recovery-requesting", revision)
    awaiting = _ready_operation(repository, "fixture-recovery-awaiting", revision)

    pre_claim = repository.claim_by_id(pre_request.id, worker_id="pre-worker", lease_seconds=30)
    request_claim = repository.claim_by_id(requesting.id, worker_id="request-worker", lease_seconds=30)
    awaiting_claim = repository.claim_by_id(awaiting.id, worker_id="await-worker", lease_seconds=30)
    assert pre_claim is not None and pre_claim.claim_token is not None
    assert request_claim is not None and request_claim.claim_token is not None
    assert awaiting_claim is not None and awaiting_claim.claim_token is not None
    repository.checkpoint_request(
        request_claim.id,
        request_claim.claim_token,
        backend="openapi",
        session={"backend": "openapi", "account_id": "115001", "upload_id": "requesting-session"},
    )
    repository.checkpoint_request(
        awaiting_claim.id,
        awaiting_claim.claim_token,
        backend="openapi",
        session={"backend": "openapi", "account_id": "115001", "upload_id": "awaiting-session"},
    )
    repository.mark_awaiting_readback(
        awaiting_claim.id,
        awaiting_claim.claim_token,
        session={
            "backend": "openapi",
            "account_id": "115001",
            "upload_id": "awaiting-session",
            "part": 2,
        },
    )
    awaiting_reclaim = repository.claim_by_id(
        awaiting_claim.id,
        worker_id="await-recovery-worker",
        lease_seconds=30,
    )
    assert awaiting_reclaim is not None and awaiting_reclaim.claim_token is not None
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                    UPDATE p115_upload_operations
                    SET claimed_at = NOW() - INTERVAL '2 seconds',
                        lease_expires_at = NOW() - INTERVAL '1 second'
                    WHERE id = ANY(CAST(:ids AS uuid[]))
                    """
            ),
            {"ids": [pre_claim.id, request_claim.id, awaiting_reclaim.id]},
        )

    assert repository.expire_stale_claims(limit=10, retry_delay_seconds=0) == 3
    recovered_pre = repository.read(pre_claim.id)
    recovered_request = repository.read(request_claim.id)
    recovered_awaiting = repository.read(awaiting_claim.id)
    assert recovered_pre is not None and recovered_pre.status is UploadOperationStatus.RETRYABLE
    assert recovered_request is not None
    assert recovered_request.status is UploadOperationStatus.AWAITING_READBACK
    assert recovered_request.upload_session == {
        "backend": "openapi",
        "account_id": "115001",
        "upload_id": "requesting-session",
    }
    assert recovered_awaiting is not None
    assert recovered_awaiting.status is UploadOperationStatus.AWAITING_READBACK
    assert recovered_awaiting.upload_session == {
        "backend": "openapi",
        "account_id": "115001",
        "upload_id": "awaiting-session",
        "part": 2,
    }
    assert recovered_request.claim_token is None and recovered_awaiting.claim_token is None
    with pytest.raises(P115UploadStateError):
        repository.mark_retryable(
            request_claim.id,
            request_claim.claim_token,
            error={"type": "unsafe_reissue"},
            retry_after_seconds=0,
        )


def test_request_checkpoint_rejects_backend_and_account_switches(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    revision = _save_config(repository)
    ready = _ready_operation(repository, "fixture-checkpoint-pinning", revision)
    claimed = repository.claim_by_id(ready.id, worker_id="checkpoint-worker", lease_seconds=30)
    assert claimed is not None and claimed.claim_token is not None
    original_session = {
        "backend": "openapi",
        "account_id": "115001",
        "upload_id": "fixture-session",
    }
    repository.checkpoint_request(
        claimed.id,
        claimed.claim_token,
        backend="openapi",
        session=original_session,
    )

    with pytest.raises(P115UploadStateError):
        repository.checkpoint_request(
            claimed.id,
            claimed.claim_token,
            backend="cookie",
            session={**original_session, "backend": "cookie"},
        )
    with pytest.raises(P115UploadStateError):
        repository.checkpoint_request(
            claimed.id,
            claimed.claim_token,
            backend="openapi",
            session={**original_session, "account_id": "115002"},
        )

    persisted = repository.read(claimed.id)
    assert persisted is not None
    assert persisted.upload_backend == "openapi"
    assert persisted.upload_session == original_session


def test_confirmed_transition_and_completed_source_share_one_transaction(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    revision = _save_config(repository)
    ready = _ready_operation(repository, "fixture-completed", revision)
    claimed = repository.claim_by_id(ready.id, worker_id="completion-worker", lease_seconds=30)
    assert claimed is not None and claimed.claim_token is not None
    repository.checkpoint_request(
        claimed.id,
        claimed.claim_token,
        backend="openapi",
        session={"backend": "openapi", "account_id": "115001", "upload_id": "completed-session"},
    )
    receipt = {
        "fid": "fixture-file-id",
        "pick_code": "fixture-pick-code",
        "parent_id": "1159001",
        "backend": "openapi",
        "account_id": "115001",
    }
    repository.mark_remote_confirmed(claimed.id, claimed.claim_token, receipt=receipt)
    repository.mark_projecting_local(
        claimed.id,
        claimed.claim_token,
        projection={"derived_paths": ["/fixture/output/movie.strm"]},
    )
    repository.mark_ingestion_pending(
        claimed.id,
        claimed.claim_token,
        projection={"derived_paths": ["/fixture/output/movie.strm"], "ingestion_event_key": "fixture-ingest"},
    )
    completed = UploadedSource(
        record_key=f"{FIXTURE_RECORD_PREFIX}completed",
        operation_id=claimed.id,
        mapping_id=claimed.mapping_id,
        config_revision=claimed.config_revision,
        local_path=claimed.path,
        path_key=claimed.path_key,
        relative_path=claimed.relative_path,
        target_cid="1159001",
        mode="keep",
        fingerprint=claimed.fingerprint or UploadFileFingerprint(0, 0, 0, 0),
        remote_receipt=receipt,
        derived_paths=("/fixture/output/movie.strm",),
        derived_ownership={"/fixture/output/movie.strm": "a" * 64},
        completed_at=datetime.now(UTC),
        operation_sequence=claimed.sequence,
    )

    transition = repository.mark_confirmed(
        claimed.id,
        claimed.claim_token,
        result={"ingestion_event_key": "fixture-ingest"},
        completed=completed,
    )

    assert transition.operation.status is UploadOperationStatus.CONFIRMED
    assert repository.source_by_path(claimed.mapping_id, claimed.path_key) == completed
    assert repository.sources_by_pick_codes(["fixture-pick-code"]) == (completed,)
    public_status = repository.status()
    assert public_status["operations"]["confirmed"] == 1
    assert public_status["completed"] >= 1
    assert repository.remove_sources([completed.record_key]) == 1
    assert repository.source_by_path(claimed.mapping_id, claimed.path_key) is None


def test_delete_operation_snapshot_cannot_remove_replacement_completed_source(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    revision = _save_config(repository)
    path = "/fixture/upload/identity-fence.mkv"
    record_key = f"{FIXTURE_RECORD_PREFIX}identity-fence"
    source_a = _confirm_source(
        repository,
        revision,
        operation_key="fixture-source-a",
        path=path,
        pick_code="pick-a",
        record_key=record_key,
    )
    delete = repository.reserve(
        _intent(
            "fixture-delete-a",
            revision,
            event_type=UploadEventType.DELETED,
            path=path,
            completed_source=source_a,
        )
    ).operation
    assert delete.completed_source == source_a.snapshot()

    source_b = _confirm_source(
        repository,
        revision,
        operation_key="fixture-source-b",
        path=path,
        pick_code="pick-b",
        record_key=record_key,
    )
    delete = repository.mark_ready(delete.id).operation
    claimed_delete = repository.claim_by_id(delete.id, worker_id="delete-worker", lease_seconds=30)
    assert claimed_delete is not None and claimed_delete.claim_token is not None
    repository.checkpoint_request(
        claimed_delete.id,
        claimed_delete.claim_token,
        backend="openapi",
        session={
            "backend": "openapi",
            "account_id": "115001",
            "request_sent": True,
            "readback_only": True,
        },
    )
    repository.mark_remote_confirmed(
        claimed_delete.id,
        claimed_delete.claim_token,
        {
            "fid": "fid-pick-a",
            "pick_code": "pick-a",
            "parent_id": "1159001",
            "backend": "openapi",
            "account_id": "115001",
            "deleted": True,
        },
    )
    repository.mark_confirmed(
        claimed_delete.id,
        claimed_delete.claim_token,
        result={"deleted": True},
        completed=None,
    )

    assert repository.source_by_path(source_b.mapping_id, source_b.path_key) == source_b
    assert repository.remove_sources_if_current([source_a]) == 0
    assert repository.source_by_path(source_b.mapping_id, source_b.path_key) == source_b


def test_older_local_projection_continuation_cannot_overwrite_newer_completed_source(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    revision = _save_config(repository)
    path = "/fixture/upload/sequence-fence.mkv"
    record_key = f"{FIXTURE_RECORD_PREFIX}sequence-fence"

    ready_a = _ready_operation(repository, "fixture-sequence-a", revision, path=path)
    claimed_a = repository.claim_by_id(ready_a.id, worker_id="worker-sequence-a", lease_seconds=30)
    assert claimed_a is not None and claimed_a.claim_token is not None
    repository.checkpoint_request(
        claimed_a.id,
        claimed_a.claim_token,
        backend="openapi",
        session={"backend": "openapi", "account_id": "115001", "request_sent": True},
    )
    repository.mark_remote_confirmed(
        claimed_a.id,
        claimed_a.claim_token,
        _upload_receipt("pick-sequence-a"),
    )
    repository.mark_projecting_local(claimed_a.id, claimed_a.claim_token)
    released_a = repository.mark_retryable(
        claimed_a.id,
        claimed_a.claim_token,
        error={"type": "fixture_local_retry"},
        retry_after_seconds=0,
    ).operation
    assert released_a.status is UploadOperationStatus.PROJECTING_LOCAL
    assert released_a.claim_token is None

    source_b = _confirm_source(
        repository,
        revision,
        operation_key="fixture-sequence-b",
        path=path,
        pick_code="pick-sequence-b",
        record_key=record_key,
    )
    reclaimed_a = repository.claim_by_id(
        released_a.id,
        worker_id="worker-sequence-a-continuation",
        lease_seconds=30,
    )
    assert reclaimed_a is not None and reclaimed_a.claim_token is not None
    assert reclaimed_a.sequence < source_b.operation_sequence
    repository.mark_ingestion_pending(
        reclaimed_a.id,
        reclaimed_a.claim_token,
        {"derived_paths": [], "ingestion_event_key": None, "details": {}},
    )
    source_a = _completed_source(
        reclaimed_a,
        pick_code="pick-sequence-a",
        record_key=record_key,
    )

    late = repository.mark_confirmed(
        reclaimed_a.id,
        reclaimed_a.claim_token,
        result={"ok": True},
        completed=source_a,
    ).operation

    assert late.status is UploadOperationStatus.SUPERSEDED
    assert late.error is not None and late.error["type"] == "newer_completed_source_exists"
    assert repository.source_by_path(source_b.mapping_id, source_b.path_key) == source_b


def test_deep_cleanup_keeps_replacement_ledger_created_after_local_delete(
    p6_runtime: DatabaseRuntime,
    tmp_path: Path,
) -> None:
    repository = _repository(p6_runtime)
    revision = _save_config(repository)
    path = tmp_path / "cleanup-cas.mkv"
    path.write_bytes(b"fixture")
    path_state = path.stat()
    source_a_fingerprint = UploadFileFingerprint(
        size=path_state.st_size,
        mtime_ns=path_state.st_mtime_ns,
        device=path_state.st_dev,
        inode=path_state.st_ino,
    )
    record_key = f"{FIXTURE_RECORD_PREFIX}cleanup-cas"
    _confirm_source(
        repository,
        revision,
        operation_key="fixture-cleanup-a",
        path=str(path),
        pick_code="pick-cleanup-a",
        record_key=record_key,
        fingerprint=source_a_fingerprint,
    )
    source_b: UploadedSource | None = None

    class ReplacingRepository:
        def sources_by_pick_codes(self, values):
            return repository.sources_by_pick_codes(values)

        def remove_sources_if_current(self, sources):
            nonlocal source_b
            assert not path.exists()
            source_b = _confirm_source(
                repository,
                revision,
                operation_key="fixture-cleanup-b",
                path=str(path),
                pick_code="pick-cleanup-b",
                record_key=record_key,
                fingerprint=UploadFileFingerprint(size=200, mtime_ns=300, device=4, inode=5),
            )
            return repository.remove_sources_if_current(sources)

    cleanup = NativeP115UploadSourceCleanupGateway(
        ReplacingRepository(),
        lambda: {
            "enabled": True,
            "mappings": [
                {
                    "id": "fixture-mapping",
                    "enabled": True,
                    "local_dir": str(tmp_path),
                    "target_cid": "1159001",
                    "mode": "keep",
                    "extensions": [".mkv"],
                }
            ],
        },
    )

    result = cleanup.delete_sources(("pick-cleanup-a",))

    assert result["deleted"] == 1
    assert result["records_removed"] == 0
    assert result["records_replaced"] == 1
    assert source_b is not None
    assert repository.source_by_path(source_b.mapping_id, source_b.path_key) == source_b


def test_upload_monitor_migration_downgrade_and_upgrade_remain_current(
    p6_runtime: DatabaseRuntime,
    migrated_p6_postgres_url: str,
) -> None:
    config = Config(str(REPO_ROOT / "alembic.ini"))
    previous = os.environ.get("ETK_DATABASE_URL")
    os.environ["ETK_DATABASE_URL"] = migrated_p6_postgres_url
    try:
        command.downgrade(config, "0009_p6_ingestion_monitor")
        inspector = inspect(p6_runtime.engine)
        assert "p115_upload_operations" not in inspector.get_table_names()
        assert "p115_upload_monitor_controls" not in inspector.get_table_names()
        with p6_runtime.engine.connect() as connection:
            assert connection.scalar(text("SELECT version_num FROM etk_migrations.alembic_version")) == (
                "0009_p6_ingestion_monitor"
            )

        command.upgrade(config, "head")
        inspector = inspect(p6_runtime.engine)
        assert {"p115_upload_operations", "p115_upload_monitor_controls"} <= set(inspector.get_table_names())
        with p6_runtime.engine.connect() as connection:
            assert connection.scalar(text("SELECT version_num FROM etk_migrations.alembic_version")) == (
                "0013_p7_subscription_runtime"
            )
    finally:
        if previous is None:
            os.environ.pop("ETK_DATABASE_URL", None)
        else:
            os.environ["ETK_DATABASE_URL"] = previous
