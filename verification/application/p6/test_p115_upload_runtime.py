from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from etk.domains.p115.upload_adapters import (
    LegacyP115UploadProjection,
    LegacyP115UploadProvider,
    NativeP115UploadSourceCleanupGateway,
)
from etk.domains.p115.upload_monitor import (
    UploadedSource,
    UploadEventType,
    UploadFileFingerprint,
    UploadMonitorControl,
    UploadOperation,
    UploadOperationStatus,
    UploadProjectionOutcome,
    UploadProviderDisposition,
    UploadProviderOutcome,
    UploadSource,
)
from etk.platform.jobs import OutboxClaim, OutboxEventRecord, OutboxStatus
from etk.processes.celery_app import EXECUTE_P115_UPLOAD_OPERATION_TASK
from etk.processes.p6_upload_monitor import CeleryP115UploadPublisher, P115UploadOperationExecutor
from etk.processes.runtime_config import TaskRuntimeConfig

NOW = datetime(2026, 8, 26, 2, 3, 4, tzinfo=UTC)
OPERATION_ID = UUID("00000000-0000-0000-0000-000000000811")
CLAIM_TOKEN = UUID("00000000-0000-0000-0000-000000000812")
OUTBOX_ID = UUID("00000000-0000-0000-0000-000000000813")
REVISION = "a" * 64


def _fingerprint(path: Path) -> UploadFileFingerprint:
    value = path.stat()
    return UploadFileFingerprint(value.st_size, value.st_mtime_ns, value.st_dev, value.st_ino)


def _operation(
    path: Path,
    *,
    status: UploadOperationStatus = UploadOperationStatus.READY,
    event_type: UploadEventType = UploadEventType.UPSERT,
    attempts: int = 0,
    remote_receipt: dict[str, Any] | None = None,
    projection: dict[str, Any] | None = None,
) -> UploadOperation:
    claimed = status in {
        UploadOperationStatus.CLAIMED,
        UploadOperationStatus.REQUESTING,
        UploadOperationStatus.REMOTE_CONFIRMED,
        UploadOperationStatus.PROJECTING_LOCAL,
        UploadOperationStatus.INGESTION_PENDING,
    }
    path_key = str(path).casefold()
    completed_source = None
    if event_type is UploadEventType.DELETED:
        completed_source = UploadedSource(
            record_key="fixture-mapping:movie",
            operation_id=UUID("00000000-0000-0000-0000-000000000814"),
            mapping_id="fixture-mapping",
            config_revision=REVISION,
            local_path=str(path),
            path_key=path_key,
            relative_path=path.name,
            target_cid="9001",
            mode="keep",
            fingerprint=_fingerprint(path),
            remote_receipt={
                "fid": "501",
                "pick_code": "pc-fixture",
                "parent_id": "9001",
                "backend": "openapi",
                "account_id": "115001",
            },
            derived_paths=(str(path) + ".strm",),
            derived_ownership={str(path) + ".strm": "d" * 64},
            completed_at=NOW,
        ).snapshot()
    return UploadOperation(
        id=OPERATION_ID,
        sequence=7,
        operation_key="b" * 64,
        request_hash=b"c" * 32,
        source=UploadSource.WATCHDOG,
        event_type=event_type,
        correlation_id=None,
        mapping_id="fixture-mapping",
        config_revision=REVISION,
        mapping={
            "id": "fixture-mapping",
            "enabled": True,
            "local_dir": str(path.parent),
            "target_cid": "9001",
            "target_name": "Fixture",
            "mode": "keep",
            "extensions": [".mkv"],
        },
        path=str(path),
        path_key=path_key,
        relative_path=path.name,
        fingerprint=_fingerprint(path),
        status=status,
        attempts=attempts,
        available_at=NOW,
        stability_deadline=NOW + timedelta(seconds=60),
        last_size=None,
        last_mtime_ns=None,
        stable_matches=3,
        claimed_by="fixture-worker" if claimed else None,
        claim_token=CLAIM_TOKEN if claimed else None,
        claimed_at=NOW if claimed else None,
        lease_expires_at=NOW + timedelta(hours=1) if claimed else None,
        upload_backend=None,
        upload_session=None,
        remote_receipt=remote_receipt,
        projection=projection,
        result=None,
        error=None,
        finished_at=None,
        created_at=NOW,
        updated_at=NOW,
        completed_source=completed_source,
    )


class FakeRepository:
    def __init__(self, operation: UploadOperation, source: UploadedSource | None = None) -> None:
        self.operation = operation
        self.source = source
        self.completed: UploadedSource | None = None
        self.calls: list[str] = []

    def read(self, _operation_id):
        return self.operation

    def control(self):
        return UploadMonitorControl(REVISION, REVISION, 1, NOW, NOW)

    def claim_by_id(self, _operation_id, **_options):
        if self.operation.status.terminal or self.operation.claim_token is not None:
            return None
        status = (
            UploadOperationStatus.CLAIMED
            if self.operation.status in {UploadOperationStatus.READY, UploadOperationStatus.RETRYABLE}
            else self.operation.status
        )
        self.operation = replace(
            self.operation,
            status=status,
            attempts=self.operation.attempts + 1,
            claimed_by="fixture-worker",
            claim_token=CLAIM_TOKEN,
            claimed_at=NOW,
            lease_expires_at=NOW + timedelta(hours=1),
        )
        self.calls.append("claim")
        return self.operation

    def checkpoint_request(self, _operation_id, _token, *, backend, session, **_options):
        self.operation = replace(
            self.operation,
            status=UploadOperationStatus.REQUESTING,
            upload_backend=backend,
            upload_session=dict(session or {}),
        )
        self.calls.append("checkpoint")
        return _transition(self.operation)

    def mark_remote_confirmed(self, _operation_id, _token, receipt):
        self.operation = replace(
            self.operation,
            status=UploadOperationStatus.REMOTE_CONFIRMED,
            remote_receipt=dict(receipt),
        )
        self.calls.append("remote-confirmed")
        return _transition(self.operation)

    def mark_projecting_local(self, _operation_id, _token):
        self.operation = replace(self.operation, status=UploadOperationStatus.PROJECTING_LOCAL)
        self.calls.append("projecting")
        return _transition(self.operation)

    def mark_ingestion_pending(self, _operation_id, _token, projection):
        self.operation = replace(
            self.operation,
            status=UploadOperationStatus.INGESTION_PENDING,
            projection=dict(projection),
        )
        self.calls.append("ingestion")
        return _transition(self.operation)

    def mark_confirmed(self, _operation_id, _token, *, result, completed):
        self.completed = completed
        self.operation = replace(
            self.operation,
            status=UploadOperationStatus.CONFIRMED,
            result=dict(result),
            claimed_by=None,
            claim_token=None,
            claimed_at=None,
            lease_expires_at=None,
            finished_at=NOW,
        )
        self.calls.append("confirmed")
        return _transition(self.operation)

    def mark_retryable(self, _operation_id, _token, *, error, **_options):
        status = self.operation.status if self.operation.remote_receipt is not None else UploadOperationStatus.RETRYABLE
        self.operation = replace(
            self.operation,
            status=status,
            error=dict(error),
            claimed_by=None,
            claim_token=None,
            claimed_at=None,
            lease_expires_at=None,
        )
        self.calls.append("retryable")
        return _transition(self.operation)

    def mark_awaiting_readback(self, _operation_id, _token, *, session, error, **_options):
        self.operation = replace(
            self.operation,
            status=UploadOperationStatus.AWAITING_READBACK,
            upload_session=dict(session or {}),
            error=dict(error),
            claimed_by=None,
            claim_token=None,
            claimed_at=None,
            lease_expires_at=None,
        )
        self.calls.append("awaiting-readback")
        return _transition(self.operation)

    def mark_rejected(self, _operation_id, _token, *, error):
        return self._terminal(UploadOperationStatus.REJECTED, error)

    def mark_indeterminate(self, _operation_id, _token, *, error):
        return self._terminal(UploadOperationStatus.INDETERMINATE, error)

    def mark_superseded(self, _operation_id, _token, *, error):
        return self._terminal(UploadOperationStatus.SUPERSEDED, error)

    def _terminal(self, status, error):
        self.operation = replace(
            self.operation,
            status=status,
            error=dict(error),
            claimed_by=None,
            claim_token=None,
            claimed_at=None,
            lease_expires_at=None,
            finished_at=NOW,
        )
        self.calls.append(status.value)
        return _transition(self.operation)

    def source_by_path(self, _mapping_id, _path_key):
        return self.source


def _transition(operation: UploadOperation):
    return type("Transition", (), {"operation": operation})()


class FakeProvider:
    def __init__(self, outcomes: list[UploadProviderOutcome]) -> None:
        self.outcomes = outcomes
        self.upload_calls = 0
        self.readback_calls = 0
        self.delete_calls = 0
        self.delete_readback_calls = 0

    def execute(
        self,
        operation: UploadOperation,
        *,
        checkpoint: Callable[[str, Mapping[str, Any]], None],
    ) -> UploadProviderOutcome:
        del operation
        self.upload_calls += 1
        outcome = self.outcomes.pop(0)
        if outcome.request_sent and outcome.backend and outcome.session is not None:
            checkpoint(outcome.backend, outcome.session)
        return outcome

    def readback(self, operation: UploadOperation) -> UploadProviderOutcome:
        del operation
        self.readback_calls += 1
        return self.outcomes.pop(0)

    def delete(
        self,
        operation: UploadOperation,
        source: UploadedSource,
        *,
        checkpoint: Callable[[str, Mapping[str, Any]], None],
    ) -> UploadProviderOutcome:
        del operation, source
        self.delete_calls += 1
        outcome = self.outcomes.pop(0)
        if outcome.request_sent and outcome.backend and outcome.session is not None:
            checkpoint(outcome.backend, outcome.session)
        return outcome

    def readback_delete(self, operation: UploadOperation, source: UploadedSource) -> UploadProviderOutcome:
        del operation, source
        self.delete_readback_calls += 1
        return self.outcomes.pop(0)


class FakeProjection:
    def __init__(self, *, fail_once: bool = False) -> None:
        self.fail_once = fail_once
        self.project_calls = 0
        self.delete_source_calls = 0
        self.delete_uploaded_calls = 0

    def project(self, operation: UploadOperation) -> UploadProjectionOutcome:
        self.project_calls += 1
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("fixture local projection failure")
        return UploadProjectionOutcome(
            derived_paths=(f"{operation.path}.strm",),
            ingestion_event_key=f"p115-upload:{operation.id}",
            details={"derived_ownership": {f"{operation.path}.strm": "d" * 64}},
        )

    def delete_source(self, operation: UploadOperation) -> Mapping[str, Any]:
        del operation
        self.delete_source_calls += 1
        return {"deleted": True}

    def delete_uploaded_source(
        self,
        operation: UploadOperation,
        source: UploadedSource,
    ) -> Mapping[str, Any]:
        del operation, source
        self.delete_uploaded_calls += 1
        return {"deleted": ["fixture.strm"]}


def _confirmed_outcome(*, deleted: bool = False) -> UploadProviderOutcome:
    return UploadProviderOutcome(
        disposition=UploadProviderDisposition.CONFIRMED,
        backend="openapi",
        request_sent=True,
        session={"backend": "openapi", "account_id": "115001", "readback_only": False},
        receipt={
            "fid": "501",
            "parent_id": "9001",
            "pick_code": "pc-fixture",
            "sha1": "A" * 40,
            "size": 7,
            "backend": "openapi",
            "account_id": "115001",
            "deleted": deleted,
        },
    )


def test_executor_persists_remote_receipt_before_local_projection(tmp_path: Path) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    repository = FakeRepository(_operation(path))
    provider = FakeProvider([_confirmed_outcome()])
    projection = FakeProjection()
    executor = P115UploadOperationExecutor(
        repository=repository,
        provider=provider,
        projection=projection,
        worker_id="fixture-worker",
    )

    completed = executor.execute(OPERATION_ID)

    assert completed is not None and completed.status is UploadOperationStatus.CONFIRMED
    assert repository.calls.index("remote-confirmed") < repository.calls.index("projecting")
    assert repository.completed is not None
    assert repository.completed.pick_code == "pc-fixture"
    assert provider.upload_calls == 1
    assert projection.project_calls == 1


def test_unknown_provider_outcome_retries_readback_without_blind_upload(tmp_path: Path) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    repository = FakeRepository(_operation(path))
    provider = FakeProvider(
        [
            UploadProviderOutcome(
                UploadProviderDisposition.INDETERMINATE,
                "openapi",
                True,
                session={"backend": "openapi", "account_id": "115001", "readback_only": True},
                error={"type": "timeout"},
            ),
            _confirmed_outcome(),
        ]
    )
    projection = FakeProjection()
    executor = P115UploadOperationExecutor(
        repository=repository,
        provider=provider,
        projection=projection,
        worker_id="fixture-worker",
    )

    first = executor.execute(OPERATION_ID)
    second = executor.execute(OPERATION_ID)

    assert first is not None and first.status is UploadOperationStatus.AWAITING_READBACK
    assert second is not None and second.status is UploadOperationStatus.CONFIRMED
    assert provider.upload_calls == 1
    assert provider.readback_calls == 1
    assert projection.project_calls == 1


def test_request_not_sent_retryable_or_indeterminate_outcome_retries_without_checkpoint(
    tmp_path: Path,
) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    for disposition in (
        UploadProviderDisposition.RETRYABLE,
        UploadProviderDisposition.INDETERMINATE,
    ):
        repository = FakeRepository(_operation(path))
        provider = FakeProvider(
            [
                UploadProviderOutcome(
                    disposition,
                    "",
                    False,
                    session={},
                    error={"type": "upload_backend_unavailable"},
                )
            ]
        )
        executor = P115UploadOperationExecutor(
            repository=repository,
            provider=provider,
            projection=FakeProjection(),
            worker_id="fixture-worker",
        )

        retried = executor.execute(OPERATION_ID)

        assert retried is not None and retried.status is UploadOperationStatus.RETRYABLE
        assert "checkpoint" not in repository.calls
        assert provider.readback_calls == 0


def test_local_projection_retry_never_replays_confirmed_remote_upload(tmp_path: Path) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    repository = FakeRepository(_operation(path))
    provider = FakeProvider([_confirmed_outcome()])
    projection = FakeProjection(fail_once=True)
    executor = P115UploadOperationExecutor(
        repository=repository,
        provider=provider,
        projection=projection,
        worker_id="fixture-worker",
    )

    first = executor.execute(OPERATION_ID)
    second = executor.execute(OPERATION_ID)

    assert first is not None and first.status is UploadOperationStatus.PROJECTING_LOCAL
    assert second is not None and second.status is UploadOperationStatus.CONFIRMED
    assert provider.upload_calls == 1
    assert projection.project_calls == 2


def test_delete_event_confirms_remote_absence_before_removing_local_projection(tmp_path: Path) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    operation = _operation(path, event_type=UploadEventType.DELETED)
    assert operation.completed_source is not None
    source = UploadedSource.from_snapshot(operation.completed_source)
    repository = FakeRepository(operation, source)
    provider = FakeProvider([_confirmed_outcome(deleted=True)])
    projection = FakeProjection()
    executor = P115UploadOperationExecutor(
        repository=repository,
        provider=provider,
        projection=projection,
        worker_id="fixture-worker",
    )

    completed = executor.execute(OPERATION_ID)

    assert completed is not None and completed.status is UploadOperationStatus.CONFIRMED
    assert provider.delete_calls == 1
    assert projection.delete_uploaded_calls == 1
    assert repository.calls.index("remote-confirmed") < repository.calls.index("projecting")


def test_unknown_delete_outcome_uses_only_absence_readback_on_retry(tmp_path: Path) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    operation = _operation(path, event_type=UploadEventType.DELETED)
    assert operation.completed_source is not None
    source = UploadedSource.from_snapshot(operation.completed_source)
    repository = FakeRepository(operation, source)
    provider = FakeProvider(
        [
            UploadProviderOutcome(
                UploadProviderDisposition.INDETERMINATE,
                "openapi",
                True,
                session={"backend": "openapi", "account_id": "115001", "readback_only": True},
                error={"type": "remote_delete_failed"},
            ),
            _confirmed_outcome(deleted=True),
        ]
    )
    projection = FakeProjection()
    executor = P115UploadOperationExecutor(
        repository=repository,
        provider=provider,
        projection=projection,
        worker_id="fixture-worker",
    )

    first = executor.execute(OPERATION_ID)
    second = executor.execute(OPERATION_ID)

    assert first is not None and first.status is UploadOperationStatus.AWAITING_READBACK
    assert second is not None and second.status is UploadOperationStatus.CONFIRMED
    assert provider.delete_calls == 1
    assert provider.delete_readback_calls == 1
    assert projection.delete_uploaded_calls == 1


def test_delete_operation_never_uses_replacement_completed_source(tmp_path: Path) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    operation = _operation(path, event_type=UploadEventType.DELETED)
    assert operation.completed_source is not None
    expected = UploadedSource.from_snapshot(operation.completed_source)
    replacement = replace(
        expected,
        operation_id=UUID("00000000-0000-0000-0000-000000000899"),
        remote_receipt={
            "fid": "999",
            "pick_code": "pc-replacement",
            "parent_id": "9001",
            "backend": "openapi",
            "account_id": "115001",
        },
    )
    repository = FakeRepository(operation, replacement)
    provider = FakeProvider([_confirmed_outcome(deleted=True)])
    projection = FakeProjection()
    executor = P115UploadOperationExecutor(
        repository=repository,
        provider=provider,
        projection=projection,
        worker_id="fixture-worker",
    )

    completed = executor.execute(OPERATION_ID)

    assert completed is not None and completed.status is UploadOperationStatus.CONFIRMED
    assert provider.delete_calls == 1
    assert projection.delete_uploaded_calls == 0
    assert completed.result is not None
    assert completed.result["local_projection"]["reason"] == "completed_source_replaced_or_missing"


class FakeCelery:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def send_task(self, name: str, **options: Any) -> None:
        self.calls.append((name, options))


def test_upload_outbox_publisher_uses_operation_id_as_task_id() -> None:
    event = OutboxEventRecord(
        id=OUTBOX_ID,
        event_key=f"p115.upload.requested:{OPERATION_ID}",
        topic="p115.upload.requested",
        aggregate_type="p115_upload_operation",
        aggregate_id=str(OPERATION_ID),
        payload_version=1,
        payload={
            "operation_id": str(OPERATION_ID),
            "sequence": 7,
            "operation_key": "b" * 64,
            "request_hash": "c" * 64,
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
    celery = FakeCelery()
    publisher = CeleryP115UploadPublisher(TaskRuntimeConfig(queue_name="fixture.serial"), celery_app=celery)

    publisher.publish(OutboxClaim(event=event, claim_token=CLAIM_TOKEN))

    assert celery.calls == [
        (
            EXECUTE_P115_UPLOAD_OPERATION_TASK,
            {
                "kwargs": {"operation_id": str(OPERATION_ID)},
                "task_id": str(OPERATION_ID),
                "queue": "fixture.serial",
                "routing_key": "fixture.serial",
                "serializer": "json",
            },
        )
    ]


def test_provider_readback_rejects_same_name_ambiguity_and_confirms_full_identity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    operation = replace(
        _operation(path),
        upload_backend="openapi",
        upload_session={
            "backend": "openapi",
            "account_id": "115001",
            "resolved_target_cid": "9001",
            "filename": path.name,
            "filesize": len(b"fixture"),
            "content_sha1": "A" * 40,
            "preid": "B" * 40,
        },
    )

    class Client:
        items: list[dict[str, Any]] = []

        def fs_files_backend(self, _payload, *, backend):
            assert backend == "openapi"
            return {"state": True, "data": list(self.items)}

    client = Client()
    monkeypatch.setattr(
        LegacyP115UploadProvider,
        "_client",
        staticmethod(lambda expected_account_id: client if expected_account_id == "115001" else None),
    )
    provider = LegacyP115UploadProvider()
    item = {
        "fid": "501",
        "fn": path.name,
        "fs": len(b"fixture"),
        "sha1": "A" * 40,
        "pc": "pc-fixture",
        "fc": "1",
    }
    client.items = [item, {**item, "fid": "502", "pc": "pc-other"}]
    ambiguous = provider.readback(operation)
    client.items = [item]
    confirmed = provider.readback(operation)

    assert ambiguous.disposition is UploadProviderDisposition.INDETERMINATE
    assert ambiguous.error == {"type": "upload_readback_ambiguous", "matches": 2}
    assert confirmed.disposition is UploadProviderDisposition.CONFIRMED
    assert confirmed.receipt is not None and confirmed.receipt["fid"] == "501"


def test_pinned_upload_account_drift_fails_before_runtime_request(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import handler.p115_service as p115_service

    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    operation = replace(
        _operation(path),
        upload_backend="openapi",
        upload_session={"backend": "openapi", "account_id": "115001"},
    )
    monkeypatch.setattr(p115_service, "get_115_account_id", lambda: "115002")
    checkpoints: list[tuple[str, Mapping[str, Any]]] = []

    outcome = LegacyP115UploadProvider().execute(
        operation,
        checkpoint=lambda backend, session: checkpoints.append((backend, session)),
    )

    assert outcome.disposition is UploadProviderDisposition.RETRYABLE
    assert outcome.request_sent is False
    assert outcome.error == {"type": "upload_backend_unavailable"}
    assert outcome.session is not None and outcome.session["account_id"] == "115001"
    assert checkpoints == []


def test_delete_readback_keeps_moved_fid_present_on_pinned_backend_and_account(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    operation = _operation(path, event_type=UploadEventType.DELETED)
    assert operation.completed_source is not None
    source = UploadedSource.from_snapshot(operation.completed_source)

    class Client:
        def fs_get_info_backend(self, fid, *, backend):
            assert fid == "501"
            assert backend == "openapi"
            return {
                "state": True,
                "data": {"fid": "501", "cid": "different-parent", "fn": "Movie.mkv"},
            }

    client = Client()
    monkeypatch.setattr(
        LegacyP115UploadProvider,
        "_client",
        staticmethod(lambda expected_account_id: client if expected_account_id == "115001" else None),
    )

    outcome = LegacyP115UploadProvider().readback_delete(operation, source)

    assert outcome.disposition is UploadProviderDisposition.INDETERMINATE
    assert outcome.error == {"type": "remote_delete_still_present"}
    assert outcome.receipt is None


def test_delete_mode_source_unlink_is_inode_and_fingerprint_fenced(tmp_path: Path) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    operation = _operation(path)
    projection = LegacyP115UploadProjection(ingestion=object())

    path.write_bytes(b"replacement")
    changed = projection.delete_source(operation)
    assert changed == {"deleted": False, "missing": False, "changed": True}
    assert path.exists()

    current = replace(operation, fingerprint=_fingerprint(path))
    deleted = projection.delete_source(current)
    assert deleted == {"deleted": True, "missing": False, "changed": False}
    assert not path.exists()


def test_delete_mode_source_unlink_restores_replacement_racing_after_fingerprint_check(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "Movie.mkv"
    replacement = tmp_path / "replacement.mkv"
    path.write_bytes(b"fixture")
    replacement.write_bytes(b"replacement")
    operation = _operation(path)
    projection = LegacyP115UploadProjection(ingestion=object())
    real_rename = __import__("os").rename
    real_replace = __import__("os").replace
    raced = False

    def replace_before_quarantine(source, target, **kwargs):
        nonlocal raced
        if not raced and source == path.name and target == "source":
            raced = True
            real_replace(replacement, path)
        return real_rename(source, target, **kwargs)

    monkeypatch.setattr("etk.domains.p115.upload_adapters.os.rename", replace_before_quarantine)

    outcome = projection.delete_source(operation)

    assert raced is True
    assert outcome == {"deleted": False, "missing": False, "changed": True}
    assert path.read_bytes() == b"replacement"
    assert not tuple(tmp_path.glob(".etk-upload-delete-*"))


def test_delete_mode_source_unlink_retains_quarantine_when_restore_name_is_occupied(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "Movie.mkv"
    replacement = tmp_path / "replacement.mkv"
    path.write_bytes(b"fixture")
    replacement.write_bytes(b"replacement")
    operation = _operation(path)
    projection = LegacyP115UploadProjection(ingestion=object())
    os_module = __import__("os")
    real_rename = os_module.rename
    real_replace = os_module.replace
    real_link = os_module.link
    raced = False
    occupied = False

    def replace_before_quarantine(source, target, **kwargs):
        nonlocal raced
        if not raced and source == path.name and target == "source":
            raced = True
            real_replace(replacement, path)
        return real_rename(source, target, **kwargs)

    def occupy_before_restore(source, target, **kwargs):
        nonlocal occupied
        if not occupied and source == "source" and target == path.name:
            occupied = True
            path.write_bytes(b"newer")
        return real_link(source, target, **kwargs)

    monkeypatch.setattr("etk.domains.p115.upload_adapters.os.rename", replace_before_quarantine)
    monkeypatch.setattr("etk.domains.p115.upload_adapters.os.link", occupy_before_restore)

    with pytest.raises(RuntimeError, match="retained at .etk-upload-delete-"):
        projection.delete_source(operation)

    quarantines = tuple(tmp_path.glob(".etk-upload-delete-*"))
    assert raced is True and occupied is True
    assert path.read_bytes() == b"newer"
    assert len(quarantines) == 1
    assert (quarantines[0] / "source").read_bytes() == b"replacement"


class FakeCompletedSourceRepository:
    def __init__(self, sources: tuple[UploadedSource, ...]) -> None:
        self.sources = sources
        self.removed: list[str] = []

    def sources_by_pick_codes(self, values: Sequence[str]) -> tuple[UploadedSource, ...]:
        del values
        return self.sources

    def remove_sources_if_current(self, sources: Sequence[UploadedSource]) -> int:
        self.removed.extend(source.record_key for source in sources)
        return len(sources)


def _uploaded_source(path: Path, *, record_key: str, fingerprint: UploadFileFingerprint) -> UploadedSource:
    return UploadedSource(
        record_key=record_key,
        operation_id=UUID("00000000-0000-0000-0000-000000000815"),
        mapping_id="fixture-mapping",
        config_revision=REVISION,
        local_path=str(path),
        path_key=str(path.resolve()),
        relative_path=path.name,
        target_cid="9001",
        mode="keep",
        fingerprint=fingerprint,
        remote_receipt={
            "fid": "501",
            "pick_code": "pc-fixture",
            "parent_id": "9001",
            "backend": "openapi",
            "account_id": "115001",
        },
        derived_paths=(),
        derived_ownership={},
        completed_at=NOW,
    )


def _active_upload_config(root: Path) -> dict[str, Any]:
    return {
        "enabled": True,
        "mappings": [
            {
                "id": "fixture-mapping",
                "enabled": True,
                "local_dir": str(root),
                "target_cid": "9001",
                "mode": "keep",
                "extensions": [".mkv"],
            }
        ],
    }


def test_native_deep_delete_cleanup_removes_fenced_and_missing_completed_sources(tmp_path: Path) -> None:
    existing = tmp_path / "Existing.mkv"
    missing = tmp_path / "Missing.mkv"
    existing.write_bytes(b"fixture")
    existing_source = _uploaded_source(existing, record_key="source:existing", fingerprint=_fingerprint(existing))
    missing_source = _uploaded_source(
        missing,
        record_key="source:missing",
        fingerprint=UploadFileFingerprint(size=7, mtime_ns=1, device=2, inode=3),
    )
    repository = FakeCompletedSourceRepository((existing_source, missing_source))
    cleanup = NativeP115UploadSourceCleanupGateway(repository, lambda: _active_upload_config(tmp_path))

    result = cleanup.delete_sources(("pc-fixture",))

    assert result == {
        "ok": True,
        "matched": 2,
        "deleted": 1,
        "missing": 1,
        "changed": 0,
        "failed": 0,
        "records_removed": 2,
        "records_replaced": 0,
    }
    assert not existing.exists()
    assert repository.removed == ["source:existing", "source:missing"]


def test_native_deep_delete_cleanup_preserves_changed_source_and_ledger(tmp_path: Path) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    source = _uploaded_source(path, record_key="source:changed", fingerprint=_fingerprint(path))
    path.write_bytes(b"replacement")
    repository = FakeCompletedSourceRepository((source,))
    cleanup = NativeP115UploadSourceCleanupGateway(repository, lambda: _active_upload_config(tmp_path))

    result = cleanup.delete_sources(("pc-fixture",))

    assert result["changed"] == 1
    assert result["records_removed"] == 0
    assert path.read_bytes() == b"replacement"
    assert repository.removed == []
