from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

import etk.domains.p115.maintenance_transport as maintenance_transport_module
from etk.domains.p115 import upload_adapters as upload_adapters_module
from etk.domains.p115 import upload_projection as upload_projection_module
from etk.domains.p115.maintenance_transport import (
    P115MaintenanceTransportError,
    P115PinnedOperationsClient,
)
from etk.domains.p115.media_info_formatter import P115MediaInfoPolicySnapshot
from etk.domains.p115.models import P115CredentialSnapshot
from etk.domains.p115.upload_adapters import (
    NativeP115UploadProvider,
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
from etk.domains.p115.upload_projection import NativeP115UploadProjection
from etk.platform import Settings
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

        account_id = "115001"
        backend = "openapi"
        credential_fingerprint = "f" * 64

        def list_children(self, parent_id: str):
            assert parent_id == "9001"
            return tuple(self.items)

    class Operations:
        def open(self, **options):
            assert options["expected_account_id"] == "115001"
            assert options["expected_backend"] == "openapi"
            return client

    client = Client()
    provider = NativeP115UploadProvider(Operations())  # type: ignore[arg-type]
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
) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    operation = replace(
        _operation(path),
        upload_backend="openapi",
        upload_session={"backend": "openapi", "account_id": "115001"},
    )
    checkpoints: list[tuple[str, Mapping[str, Any]]] = []

    class DriftedOperations:
        def open(self, **_options):
            raise RuntimeError("115 account changed before provider client open")

    outcome = NativeP115UploadProvider(DriftedOperations()).execute(  # type: ignore[arg-type]
        operation,
        checkpoint=lambda backend, session: checkpoints.append((backend, session)),
    )

    assert outcome.disposition is UploadProviderDisposition.RETRYABLE
    assert outcome.request_sent is False
    assert outcome.error is not None and outcome.error["type"] == "upload_backend_unavailable"
    assert outcome.session is not None and outcome.session["account_id"] == "115001"
    assert checkpoints == []


def test_delete_readback_keeps_moved_fid_present_on_pinned_backend_and_account(
    tmp_path: Path,
) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    operation = _operation(path, event_type=UploadEventType.DELETED)
    assert operation.completed_source is not None
    source = UploadedSource.from_snapshot(operation.completed_source)

    class Client:
        account_id = "115001"
        backend = "openapi"
        credential_fingerprint = "f" * 64

        def file_info(self, fid):
            assert fid == "501"
            return {
                "state": True,
                "data": {"fid": "501", "cid": "different-parent", "fn": "Movie.mkv"},
            }

    client = Client()

    class Operations:
        def open(self, **options):
            assert options["expected_account_id"] == "115001"
            assert options["expected_backend"] == "openapi"
            return client

    outcome = NativeP115UploadProvider(Operations()).readback_delete(operation, source)  # type: ignore[arg-type]

    assert outcome.disposition is UploadProviderDisposition.INDETERMINATE
    assert outcome.error == {"type": "remote_delete_still_present"}
    assert outcome.receipt is None


def _native_upload_projection(root: Path) -> NativeP115UploadProjection:
    return NativeP115UploadProjection(
        settings=lambda: Settings(local_strm_root=str(root)),
        credentials=cast(Any, object()),
        repository=cast(Any, object()),
        sorting_rules=cast(Any, object()),
        rename_config=cast(Any, object()),
        ingestion=cast(Any, object()),
        processes=cast(Any, object()),
        policies=cast(Any, object()),
    )


def test_native_upload_projection_does_not_match_a_root_level_file(tmp_path: Path) -> None:
    projection = NativeP115UploadProjection(
        settings=lambda: Settings(local_strm_root=str(tmp_path)),
        credentials=cast(Any, object()),
        repository=cast(Any, object()),
        sorting_rules=cast(
            Any,
            SimpleNamespace(
                load=lambda: SimpleNamespace(
                    account_id="115001",
                    rules=({"category_path": "Movies"},),
                )
            ),
        ),
        rename_config=cast(Any, object()),
        ingestion=cast(Any, object()),
        processes=cast(Any, object()),
        policies=cast(Any, object()),
    )

    assert projection._matches_sorting_rule("", "115001", "媒体库") is False


def test_native_upload_projection_persists_cache_writes_strm_and_enqueues_ingestion(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    source = source_root / "Movie.mkv"
    source.write_bytes(b"fixture")
    strm_root = tmp_path / "strm"
    strm_root.mkdir()
    operation = replace(
        _operation(source),
        remote_receipt={
            "fid": "501",
            "parent_id": "9001",
            "file_name": "Movie.mkv",
            "sha1": "A" * 40,
            "pick_code": "pc-fixture",
            "size": 7,
            "backend": "openapi",
            "account_id": "115001",
        },
    )

    class Repository:
        remote: dict[str, Any] | None = None
        media: dict[str, Any] | None = None

        def target_relative_path(self, _target_cid: str, *, account_id: str) -> str:
            assert account_id == "115001"
            return "媒体库/Movies"

        def persist_remote_file(self, **values: Any) -> None:
            self.remote = dict(values)

        def persist_media_info(self, **values: Any) -> None:
            self.media = dict(values)

    class Processes:
        def run(self, *_args: Any, **_options: Any) -> Any:
            return SimpleNamespace(
                succeeded=True,
                parsed_output={
                    "format": {"format_name": "matroska", "duration": "1", "size": "7"},
                    "streams": [
                        {
                            "index": 0,
                            "codec_type": "video",
                            "codec_name": "h264",
                            "width": 1920,
                            "height": 1080,
                        }
                    ],
                    "chapters": [],
                },
            )

    class Ingestion:
        calls: list[tuple[str, str, dict[str, Any]]] = []

        def record_event(self, event_type: str, path: str, **options: Any) -> None:
            self.calls.append((event_type, path, dict(options)))

    repository = Repository()
    ingestion = Ingestion()
    projection = NativeP115UploadProjection(
        settings=lambda: Settings(
            local_strm_root=str(strm_root),
            etk_server_url="http://etk.fixture.invalid",
            p115_media_root_name="媒体库",
        ),
        credentials=cast(Any, SimpleNamespace(load=lambda: SimpleNamespace(account_id="115001"))),
        repository=cast(Any, repository),
        sorting_rules=cast(
            Any,
            SimpleNamespace(
                load=lambda: SimpleNamespace(
                    account_id="115001",
                    rules=({"category_path": "Movies"},),
                )
            ),
        ),
        rename_config=cast(Any, SimpleNamespace(load=lambda: SimpleNamespace(config={}))),
        ingestion=cast(Any, ingestion),
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

    result = projection.project(operation)

    expected = strm_root / "Movies" / "Movie.strm"
    assert expected.read_text() == "http://etk.fixture.invalid/api/p115/play/pc-fixture"
    assert result.derived_paths == (str(expected),)
    assert result.ingestion_event_key == f"p115-upload:{operation.id}:created"
    assert repository.remote is not None and repository.remote["local_path"] == "Movies/Movie.mkv"
    assert repository.media is not None and repository.media["sha1"] == "A" * 40
    assert ingestion.calls == [
        (
            "created",
            str(expected),
            {"source": "active", "source_event_key": f"p115-upload:{operation.id}:created"},
        )
    ]


def test_delete_mode_source_unlink_is_inode_and_fingerprint_fenced(tmp_path: Path) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    operation = _operation(path)
    projection = _native_upload_projection(tmp_path)

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
    projection = _native_upload_projection(tmp_path)
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
    projection = _native_upload_projection(tmp_path)
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


class NativeUploadClient:
    account_id = "115001"
    backend = "openapi"
    credential_fingerprint = "f" * 64

    def __init__(self, path: Path) -> None:
        self.path = path
        self.created_directory = False
        self.deleted = False
        self.upload_mode = "success"
        self.delete_accepted = True
        self.checkpoints: list[Mapping[str, Any]] = []

    @property
    def item(self) -> dict[str, Any]:
        content = self.path.read_bytes()
        import hashlib

        return {
            "fid": "501",
            "fn": self.path.name,
            "fs": len(content),
            "sha1": hashlib.sha1(content).hexdigest().upper(),
            "pc": "pc-fixture",
            "fc": "1",
        }

    def list_children(self, parent_id: str) -> tuple[dict[str, Any], ...]:
        if parent_id == "9001":
            return ({"cid": "9100", "fn": "Season", "fc": "0"},) if self.created_directory else ()
        if parent_id == "9100" and self.upload_mode != "missing-readback":
            return (self.item,)
        return ()

    def create_directory(self, parent_id: str, name: str) -> Mapping[str, Any]:
        assert parent_id == "9001" and name == "Season"
        self.created_directory = True
        return {"state": True}

    def upload_local_file(
        self,
        path: str,
        target_cid: str,
        *,
        session: Mapping[str, Any],
        checkpoint: Callable[[Mapping[str, Any]], None],
        progress: Callable[[int], None],
    ) -> Mapping[str, Any]:
        assert path == str(self.path)
        assert target_cid in {"9001", "9100"}
        assert session["phase"] in {"prepared", "mkdir"}
        if self.upload_mode in {"failed-before-send", "failed-after-send", "confirmed-after-error"}:
            if self.upload_mode != "failed-before-send":
                checkpoint({"phase": "request-sent"})
            raise RuntimeError("fixture upload failure")
        progress(cast(Any, "invalid"))
        progress(1)
        progress(self.path.stat().st_size)
        return {"state": True}

    def file_info(self, fid: str) -> Mapping[str, Any]:
        assert fid == "501"
        if self.deleted:
            return {"state": False, "code": "404", "message": "not found"}
        return {"state": True, "data": [{"fid": "501", "fn": self.path.name}]}

    def delete_files(self, fids: Sequence[str]) -> Mapping[str, Any]:
        assert fids == ["501"]
        if self.delete_accepted:
            self.deleted = True
            return {"state": True}
        return {"state": False, "code": "500"}


class NativeUploadOperations:
    def __init__(self, clients: Sequence[Any]) -> None:
        self.clients = list(clients)
        self.calls: list[Mapping[str, Any]] = []

    def open(self, **options: Any) -> Any:
        self.calls.append(dict(options))
        if len(self.clients) > 1:
            return self.clients.pop(0)
        return self.clients[0] if self.clients else None


def test_native_upload_provider_rejects_source_and_backend_preconditions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    operation = _operation(path)

    readback_only = replace(operation, upload_session={"readback_only": True})
    outcome = NativeP115UploadProvider(cast(Any, NativeUploadOperations([]))).execute(
        readback_only,
        checkpoint=lambda *_args: None,
    )
    assert outcome.error == {"type": "upload_readback_identity_missing"}

    path.unlink()
    unavailable = NativeP115UploadProvider(cast(Any, NativeUploadOperations([]))).execute(
        operation,
        checkpoint=lambda *_args: None,
    )
    assert unavailable.disposition is UploadProviderDisposition.REJECTED
    assert unavailable.error is not None and unavailable.error["type"] == "source_unavailable"

    path.write_bytes(b"changed")
    changed = NativeP115UploadProvider(cast(Any, NativeUploadOperations([]))).execute(
        operation,
        checkpoint=lambda *_args: None,
    )
    assert changed.error == {"type": "source_fingerprint_changed"}

    operation = _operation(path)
    no_client = NativeP115UploadProvider(cast(Any, NativeUploadOperations([]))).execute(
        operation,
        checkpoint=lambda *_args: None,
    )
    assert no_client.disposition is UploadProviderDisposition.RETRYABLE
    assert no_client.error == {"type": "upload_backend_unavailable"}

    client = NativeUploadClient(path)
    provider = NativeP115UploadProvider(cast(Any, NativeUploadOperations([client])))

    def fail_content_identity(*_args: Any) -> Any:
        raise OSError("fixture read failure")

    monkeypatch.setattr(provider, "_content_identity", fail_content_identity)
    failed_read = provider.execute(operation, checkpoint=lambda *_args: None)
    assert failed_read.disposition is UploadProviderDisposition.RETRYABLE
    assert failed_read.error is not None and failed_read.error["type"] == "source_read_failed"


def test_native_upload_provider_creates_directory_uploads_and_confirms_authoritative_receipt(tmp_path: Path) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    operation = replace(_operation(path), relative_path="Season/Movie.mkv")
    client = NativeUploadClient(path)
    operations = NativeUploadOperations([client])
    checkpoints: list[tuple[str, dict[str, Any]]] = []

    outcome = NativeP115UploadProvider(cast(Any, operations)).execute(
        operation,
        checkpoint=lambda backend, session: checkpoints.append((backend, dict(session))),
    )

    assert outcome.disposition is UploadProviderDisposition.CONFIRMED
    assert outcome.receipt is not None and outcome.receipt["parent_id"] == "9100"
    assert outcome.receipt["pick_code"] == "pc-fixture"
    assert client.created_directory is True
    assert any(session["phase"] == "mkdir" for _backend, session in checkpoints)
    assert any(session["phase"] == "uploading" for _backend, session in checkpoints)
    assert operations.calls[-1] == {
        "expected_account_id": "115001",
        "expected_backend": "openapi",
        "expected_credential_fingerprint": "f" * 64,
    }


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        ("failed-before-send", UploadProviderDisposition.RETRYABLE),
        ("failed-after-send", UploadProviderDisposition.INDETERMINATE),
        ("confirmed-after-error", UploadProviderDisposition.CONFIRMED),
        ("missing-readback", UploadProviderDisposition.INDETERMINATE),
    ],
)
def test_native_upload_provider_classifies_upload_failure_and_receipt_readback(
    tmp_path: Path,
    mode: str,
    expected: UploadProviderDisposition,
) -> None:
    path = tmp_path / f"{mode}.mkv"
    path.write_bytes(b"fixture")
    operation = _operation(path)
    client = NativeUploadClient(path)
    client.upload_mode = mode
    if mode == "confirmed-after-error":
        client.list_children = lambda _parent_id: (client.item,)  # type: ignore[method-assign]
    checkpoints: list[tuple[str, Mapping[str, Any]]] = []

    outcome = NativeP115UploadProvider(cast(Any, NativeUploadOperations([client]))).execute(
        operation,
        checkpoint=lambda backend, session: checkpoints.append((backend, session)),
    )

    assert outcome.disposition is expected
    if expected is UploadProviderDisposition.INDETERMINATE:
        assert outcome.error is not None and outcome.error["type"] in {
            "upload_request_failed",
            "upload_receipt_not_confirmed",
        }


def test_native_upload_readback_rejects_missing_identity_backend_and_incomplete_receipt(tmp_path: Path) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    operation = _operation(path)
    provider = NativeP115UploadProvider(cast(Any, NativeUploadOperations([])))

    missing_identity = provider.readback(replace(operation, upload_session={}))
    assert missing_identity.error == {"type": "upload_readback_identity_missing"}

    pinned_but_unavailable = provider.readback(
        replace(
            operation,
            upload_backend="openapi",
            upload_session={
                "account_id": "115001",
                "backend": "openapi",
                "content_sha1": "A" * 40,
                "filesize": path.stat().st_size,
            },
        )
    )
    assert pinned_but_unavailable.error is not None
    assert pinned_but_unavailable.error["type"] == "upload_readback_failed"

    client = NativeUploadClient(path)
    incomplete_item = {**client.item, "pc": "", "fid": ""}
    client.list_children = lambda _parent_id: (incomplete_item,)  # type: ignore[method-assign]
    incomplete = NativeP115UploadProvider(cast(Any, NativeUploadOperations([client]))).readback(
        replace(
            operation,
            upload_backend="openapi",
            upload_session={
                "account_id": "115001",
                "backend": "openapi",
                "content_sha1": client.item["sha1"],
                "filesize": path.stat().st_size,
            },
        )
    )
    assert incomplete.error == {"type": "upload_readback_receipt_incomplete"}


def test_native_upload_delete_confirms_absence_and_classifies_failures(tmp_path: Path) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    operation = _operation(path, event_type=UploadEventType.DELETED)
    assert operation.completed_source is not None
    source = UploadedSource.from_snapshot(operation.completed_source)

    client = NativeUploadClient(path)
    checkpoints: list[tuple[str, Mapping[str, Any]]] = []
    provider = NativeP115UploadProvider(cast(Any, NativeUploadOperations([client])))
    confirmed = provider.delete(
        operation,
        source,
        checkpoint=lambda backend, session: checkpoints.append((backend, session)),
    )
    assert confirmed.disposition is UploadProviderDisposition.CONFIRMED
    assert confirmed.receipt is not None and confirmed.receipt["deleted"] is True
    assert checkpoints and checkpoints[0][1]["phase"] == "remote_delete"

    rejected_source = replace(source, remote_receipt={})
    rejected = provider.readback_delete(operation, rejected_source)
    assert rejected.disposition is UploadProviderDisposition.REJECTED

    readback_failure_client = NativeUploadClient(path)
    readback_failure_client.file_info = lambda _fid: (_ for _ in ()).throw(  # type: ignore[method-assign]
        RuntimeError("fixture readback failure")
    )
    readback_failure = NativeP115UploadProvider(
        cast(Any, NativeUploadOperations([readback_failure_client]))
    ).readback_delete(operation, source)
    assert readback_failure.error is not None
    assert readback_failure.error["type"] == "remote_delete_readback_failed"

    unavailable_client = NativeUploadClient(path)
    unavailable = NativeP115UploadProvider(cast(Any, NativeUploadOperations([unavailable_client, None]))).delete(
        operation, source, checkpoint=lambda *_args: None
    )
    assert unavailable.disposition is UploadProviderDisposition.RETRYABLE
    assert unavailable.error is not None and unavailable.error["type"] == "delete_backend_unavailable"

    failed_client = NativeUploadClient(path)
    failed_client.delete_accepted = False
    failed = NativeP115UploadProvider(cast(Any, NativeUploadOperations([failed_client]))).delete(
        operation,
        source,
        checkpoint=lambda *_args: None,
    )
    assert failed.disposition is UploadProviderDisposition.INDETERMINATE
    assert failed.error is not None and failed.error["type"] == "remote_delete_failed"


def test_native_upload_provider_directory_and_content_helpers_cover_fail_closed_edges(tmp_path: Path) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    operation = _operation(path)
    provider = NativeP115UploadProvider(cast(Any, NativeUploadOperations([])))
    with pytest.raises(RuntimeError, match="identity is not pinned"):
        provider._open(operation, {}, require_pinned=True)

    cached = provider._content_identity(
        operation,
        {"content_sha1": "a" * 40, "preid": "b" * 40},
    )
    assert cached["content_sha1"] == "A" * 40
    assert cached["preid"] == "B" * 40

    session = {"resolved_target_cid": "already"}
    client = NativeUploadClient(path)
    assert (
        provider._resolve_target_directory(
            operation,
            client=cast(Any, client),
            session=session,
            checkpoint=lambda _values: None,
        )
        == "already"
    )

    ambiguous_operation = replace(operation, relative_path="Season/Movie.mkv")
    client.created_directory = True
    client.list_children = lambda _parent_id: (  # type: ignore[method-assign]
        {"cid": "one", "fn": "Season", "fc": "0"},
        {"cid": "two", "fn": "Season", "fc": "0"},
    )
    with pytest.raises(RuntimeError, match="ambiguous"):
        provider._resolve_target_directory(
            ambiguous_operation,
            client=cast(Any, client),
            session={},
            checkpoint=lambda _values: None,
        )


def test_native_upload_response_helpers_cover_provider_shapes() -> None:
    assert upload_adapters_module._item_name({"file_name": "Movie.mkv"}) == "Movie.mkv"
    assert upload_adapters_module._item_id({"file_id": "501"}) == "501"
    assert upload_adapters_module._item_pick_code({"pickcode": "pc"}) == "pc"
    assert upload_adapters_module._item_sha1({"file_sha1": "aa"}) == "AA"
    assert upload_adapters_module._item_size({"size": "invalid"}) == -1
    assert upload_adapters_module._is_directory({"is_dir": True}) is True
    assert upload_adapters_module._provider_response_succeeded({"success": True}) is True
    assert upload_adapters_module._provider_response_succeeded({"code": 200}) is True
    assert upload_adapters_module._provider_response_succeeded({"code": 500}) is False
    assert upload_adapters_module._remote_file_info_exists({"state": True, "data": []}, "501") is True
    assert (
        upload_adapters_module._remote_file_info_exists(
            {"state": True, "data": {"fid": "other"}},
            "501",
        )
        is False
    )
    assert upload_adapters_module._remote_file_info_exists({"code": "20004"}, "501") is False
    assert upload_adapters_module._remote_file_info_exists({"message": "file not exist"}, "501") is False
    with pytest.raises(RuntimeError, match="not authoritative"):
        upload_adapters_module._remote_file_info_exists({"code": "500", "message": "unknown"}, "501")


class ProjectionRepository:
    def __init__(self, base: str | None = "媒体库/Movies") -> None:
        self.base = base
        self.remote: list[dict[str, Any]] = []
        self.media: list[dict[str, Any]] = []

    def target_relative_path(self, _target_cid: str, *, account_id: str) -> str | None:
        assert account_id == "115001"
        return self.base

    def persist_remote_file(self, **values: Any) -> None:
        self.remote.append(dict(values))

    def persist_media_info(self, **values: Any) -> None:
        self.media.append(dict(values))


class ProjectionProcess:
    def __init__(self, *, succeeded: bool = True, payload: Any | None = None) -> None:
        self.succeeded = succeeded
        self.payload = (
            payload
            if payload is not None
            else {
                "format": {"format_name": "matroska", "duration": "1", "size": "7"},
                "streams": [{"index": 0, "codec_type": "video", "codec_name": "h264"}],
                "chapters": [],
            }
        )

    def run(self, *_args: Any, **_values: Any) -> Any:
        return SimpleNamespace(succeeded=self.succeeded, parsed_output=self.payload)


class ProjectionLibrary:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.delete_mode = "deleted"

    def write_strm(self, relative: str, content: str) -> Any:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return SimpleNamespace(absolute_path=str(path))

    def plan_delete(self, relative: str, **_values: Any) -> Any:
        if self.delete_mode == "error":
            raise OSError("fixture delete plan failure")
        if self.delete_mode == "missing":
            return None
        return SimpleNamespace(relative_path=relative)

    def reconcile(self, **values: Any) -> Any:
        action = values["allowed_deletions"][0]
        paths = (action.relative_path,) if self.delete_mode == "deleted" else ()
        return SimpleNamespace(deleted_paths=paths)


class ProjectionLibraries:
    def __init__(self, library: ProjectionLibrary | None) -> None:
        self.library = library

    def open(self, _root: str) -> ProjectionLibrary | None:
        return self.library


class ProjectionIngestion:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def record_event(self, event_type: str, path: str, **values: Any) -> None:
        self.calls.append((event_type, path, dict(values)))


def _projection_components(
    root: Path,
    *,
    path: Path,
    account_id: str = "115001",
    base: str | None = "媒体库/Movies",
    rules: Sequence[Mapping[str, Any]] = ({"category_path": "Movies"},),
    process: ProjectionProcess | None = None,
    library: ProjectionLibrary | None | object = ...,
    etk_url: str = "http://etk.fixture.invalid",
    strm_url_fmt: str = "",
) -> tuple[NativeP115UploadProjection, ProjectionRepository, ProjectionIngestion, ProjectionLibrary | None]:
    repository = ProjectionRepository(base)
    ingestion = ProjectionIngestion()
    selected_library = ProjectionLibrary(root) if library is ... else cast(ProjectionLibrary | None, library)
    projection = NativeP115UploadProjection(
        settings=lambda: Settings(
            local_strm_root=str(root),
            etk_server_url=etk_url,
            p115_media_root_name="媒体库",
        ),
        credentials=cast(Any, SimpleNamespace(load=lambda: SimpleNamespace(account_id=account_id))),
        repository=cast(Any, repository),
        sorting_rules=cast(Any, SimpleNamespace(load=lambda: SimpleNamespace(account_id="115001", rules=rules))),
        rename_config=cast(Any, SimpleNamespace(load=lambda: SimpleNamespace(config={"strm_url_fmt": strm_url_fmt}))),
        ingestion=cast(Any, ingestion),
        processes=cast(Any, process or ProjectionProcess()),
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
        libraries=cast(Any, ProjectionLibraries(selected_library)),
    )
    return projection, repository, ingestion, selected_library


def _projection_operation(path: Path) -> UploadOperation:
    return _operation(
        path,
        remote_receipt={
            "fid": "501",
            "pick_code": "pc-fixture",
            "sha1": "A" * 40,
            "parent_id": "9001",
            "file_name": path.name,
            "account_id": "115001",
        },
    )


def test_native_upload_projection_validates_account_extension_rule_and_ffprobe(tmp_path: Path) -> None:
    media = tmp_path / "Movie.mkv"
    media.write_bytes(b"fixture")
    operation = _projection_operation(media)

    drifted, _repository, _ingestion, _library = _projection_components(
        tmp_path,
        path=media,
        account_id="115002",
    )
    with pytest.raises(RuntimeError, match="account changed"):
        drifted.project(operation)

    text_file = tmp_path / "Notes.txt"
    text_file.write_text("fixture", encoding="utf-8")
    cached, repository, _ingestion, _library = _projection_components(
        tmp_path,
        path=text_file,
        base=None,
    )
    cached_result = cached.project(_projection_operation(text_file))
    assert cached_result.details == {"cached": True, "derived_ownership": {}}
    assert repository.remote[0]["local_path"] == "Fixture/Notes.txt"

    unmatched, _repository, _ingestion, _library = _projection_components(
        tmp_path,
        path=media,
        base="媒体库/Other",
    )
    unmatched_result = unmatched.project(operation)
    assert unmatched_result.details["skipped"] == "sorting_rule_not_matched"

    failed_probe, _repository, _ingestion, _library = _projection_components(
        tmp_path,
        path=media,
        process=ProjectionProcess(succeeded=False),
    )
    with pytest.raises(RuntimeError, match="ffprobe"):
        failed_probe.project(operation)

    invalid_probe, _repository, _ingestion, _library = _projection_components(
        tmp_path,
        path=media,
        process=ProjectionProcess(payload=[]),
    )
    with pytest.raises(RuntimeError, match="ffprobe"):
        invalid_probe.project(operation)


def test_native_upload_projection_validates_library_url_and_named_strm_content(tmp_path: Path) -> None:
    media = tmp_path / "Movie.mkv"
    media.write_bytes(b"fixture")
    operation = _projection_operation(media)

    unavailable, _repository, _ingestion, _library = _projection_components(
        tmp_path,
        path=media,
        library=None,
    )
    with pytest.raises(RuntimeError, match="STRM root"):
        unavailable.project(operation)

    invalid_url, _repository, _ingestion, _library = _projection_components(
        tmp_path,
        path=media,
        etk_url="invalid",
    )
    with pytest.raises(RuntimeError, match="server URL"):
        invalid_url.project(operation)

    named, _repository, ingestion, _library = _projection_components(
        tmp_path,
        path=media,
        strm_url_fmt="with_name",
    )
    result = named.project(operation)
    strm_path = Path(result.derived_paths[0])
    assert strm_path.read_text(encoding="utf-8").endswith("/pc-fixture/Movie.mkv")
    assert ingestion.calls[0][0] == "created"


def test_native_upload_projection_delete_source_requires_fingerprint(tmp_path: Path) -> None:
    media = tmp_path / "Movie.mkv"
    media.write_bytes(b"fixture")
    projection = _native_upload_projection(tmp_path)
    with pytest.raises(RuntimeError, match="no fingerprint fence"):
        projection.delete_source(replace(_operation(media), fingerprint=None))


@pytest.mark.parametrize(
    ("mode", "expected_key"),
    [("missing", "missing"), ("deleted", "deleted")],
)
def test_native_upload_projection_deletes_or_observes_derived_strm(
    tmp_path: Path,
    mode: str,
    expected_key: str,
) -> None:
    source_file = tmp_path / "Movie.mkv"
    source_file.write_bytes(b"fixture")
    strm = tmp_path / "Movies" / "Movie.strm"
    strm.parent.mkdir()
    strm.write_text("fixture", encoding="utf-8")
    operation = _projection_operation(source_file)
    projection, _repository, ingestion, library = _projection_components(tmp_path, path=source_file)
    assert library is not None
    library.delete_mode = mode
    source = _uploaded_source(source_file, record_key="source:delete", fingerprint=_fingerprint(source_file))
    source = replace(
        source,
        derived_paths=(str(strm),),
        derived_ownership={str(strm): "d" * 64},
    )

    result = projection.delete_uploaded_source(operation, source)

    assert result[expected_key] == [str(strm)]
    assert ingestion.calls[0][0] == "deleted"


@pytest.mark.parametrize("mode", ["deferred", "error"])
def test_native_upload_projection_defers_changed_or_unreadable_derived_path(tmp_path: Path, mode: str) -> None:
    source_file = tmp_path / "Movie.mkv"
    source_file.write_bytes(b"fixture")
    strm = tmp_path / "Movies" / "Movie.strm"
    strm.parent.mkdir()
    strm.write_text("fixture", encoding="utf-8")
    operation = _projection_operation(source_file)
    projection, _repository, ingestion, library = _projection_components(tmp_path, path=source_file)
    assert library is not None
    library.delete_mode = mode
    source = replace(
        _uploaded_source(source_file, record_key="source:deferred", fingerprint=_fingerprint(source_file)),
        derived_paths=(str(strm),),
        derived_ownership={str(strm): "d" * 64},
    )
    with pytest.raises(RuntimeError, match="changed before delete"):
        projection.delete_uploaded_source(operation, source)
    assert ingestion.calls == []

    projection_without_library, *_ = _projection_components(tmp_path, path=source_file, library=None)
    with pytest.raises(RuntimeError, match="STRM root"):
        projection_without_library.delete_uploaded_source(operation, source)


def test_native_upload_projection_sorting_and_path_helpers_fail_closed() -> None:
    projection = NativeP115UploadProjection(
        settings=lambda: Settings(),
        credentials=cast(Any, object()),
        repository=cast(Any, object()),
        sorting_rules=cast(
            Any,
            SimpleNamespace(
                load=lambda: SimpleNamespace(
                    account_id="115002",
                    rules=({"category_path": ""}, {"category_path": "Movies"}),
                )
            ),
        ),
        rename_config=cast(Any, object()),
        ingestion=cast(Any, object()),
        processes=cast(Any, object()),
        policies=cast(Any, object()),
    )
    with pytest.raises(RuntimeError, match="another account"):
        projection._matches_sorting_rule("Movies", "115001", "媒体库")

    assert upload_projection_module._strip_root("媒体库", "媒体库") == ""
    assert upload_projection_module._strip_root("媒体库/Movies", "媒体库") == "Movies"
    assert upload_projection_module._strip_root("Other", "媒体库") == "Other"
    with pytest.raises(ValueError, match="must not be empty"):
        upload_projection_module._required("", "field")
    with pytest.raises(ValueError, match="numeric"):
        upload_projection_module._account_id("account")
    with pytest.raises(ValueError, match="non-negative"):
        upload_projection_module._nonnegative(True, "size")
    with pytest.raises(ValueError, match="non-negative"):
        upload_projection_module._nonnegative(-1, "size")
    assert upload_projection_module._nonnegative("2", "size") == 2
    for path in ("", "../escape", "a/../b"):
        with pytest.raises(ValueError, match="safe relative path"):
            upload_projection_module._relative_path(path)


class PinnedUploadCredentials:
    def __init__(self, snapshot: P115CredentialSnapshot) -> None:
        self.snapshot = snapshot

    def load(self) -> P115CredentialSnapshot:
        return self.snapshot


class PinnedUploadSdk:
    user_id = "115001"
    user_key = "fixture-user-key"


def _pinned_upload_operations(
    credentials: PinnedUploadCredentials,
    *,
    backend: str = "openapi",
) -> P115PinnedOperationsClient:
    return P115PinnedOperationsClient(
        credentials=cast(Any, credentials),
        snapshot=credentials.snapshot,
        backend=backend,
        sdk_factory=cast(Any, lambda _snapshot, _backend: PinnedUploadSdk()),
        directory=cast(Any, object()),
        timeout_seconds=9,
    )


def test_pinned_upload_rejects_missing_source_or_target(tmp_path: Path) -> None:
    source = tmp_path / "Movie.mkv"
    source.write_bytes(b"fixture")
    credentials = PinnedUploadCredentials(
        P115CredentialSnapshot(access_token="fixture-access-token", account_id="115001")
    )
    operations = _pinned_upload_operations(credentials)

    for path, target in ((tmp_path / "missing.mkv", "9001"), (source, "")):
        with pytest.raises(ValueError, match="source and target directory"):
            operations.upload_local_file(
                str(path),
                target,
                session={},
                checkpoint=lambda _value: None,
            )


def test_pinned_openapi_upload_initializes_and_completes_direct_upload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "Movie.mkv"
    source.write_bytes(b"fixture")
    credentials = PinnedUploadCredentials(
        P115CredentialSnapshot(access_token="fixture-access-token", account_id="115001")
    )
    operations = _pinned_upload_operations(credentials)
    calls: dict[str, Any] = {}
    checkpoints: list[Mapping[str, Any]] = []

    def initialize(path: str, **kwargs: Any) -> Mapping[str, Any]:
        calls["initialize"] = (path, kwargs)
        return {
            "state": True,
            "data": {"callback": {"callback": "fixture"}, "object": "object-key", "bucket": "bucket"},
        }

    def direct_upload(object_key: str, stream: Any, **kwargs: Any) -> Mapping[str, Any]:
        calls["direct"] = (object_key, stream.read(), kwargs)
        return {"state": True, "fid": "501"}

    monkeypatch.setattr("p115oss.upload_file_init", initialize)
    monkeypatch.setattr("p115oss.oss_upload", direct_upload)
    result = operations.upload_local_file(
        str(source),
        "9001",
        session={},
        checkpoint=lambda value: checkpoints.append(dict(value)),
        progress=lambda _value: None,
    )

    assert result == {"state": True, "fid": "501"}
    assert calls["initialize"] == (
        str(source),
        {
            "pid": "9001",
            "filename": "Movie.mkv",
            "filesize": 7,
            "headers": {"authorization": "Bearer fixture-access-token"},
        },
    )
    assert calls["direct"][0:2] == ("object-key", b"fixture")
    assert calls["direct"][2]["headers"] == {"authorization": "Bearer fixture-access-token"}
    assert calls["direct"][2]["endpoint"] == maintenance_transport_module._UPLOAD_ENDPOINT
    assert [value["phase"] for value in checkpoints] == ["upload_init", "direct_upload"]


def test_pinned_cookie_upload_passes_sdk_identity_and_accepts_reuse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "Movie.mkv"
    source.write_bytes(b"fixture")
    credentials = PinnedUploadCredentials(
        P115CredentialSnapshot(cookie="UID=fixture", app_type="web", account_id="115001")
    )
    operations = _pinned_upload_operations(credentials, backend="cookie")
    calls: list[dict[str, Any]] = []

    def initialize(_path: str, **kwargs: Any) -> Mapping[str, Any]:
        calls.append(kwargs)
        return {"state": True, "reuse": True, "fid": "501"}

    monkeypatch.setattr("p115oss.upload_file_init", initialize)
    monkeypatch.setattr("p115oss.oss_upload", lambda *_args, **_kwargs: pytest.fail("unexpected upload"))
    monkeypatch.setattr("p115oss.upload_file", lambda *_args, **_kwargs: pytest.fail("unexpected upload"))
    result = operations.upload_local_file(
        str(source),
        "9001",
        session={},
        checkpoint=lambda _value: None,
    )

    assert result == {"state": True, "reuse": True, "fid": "501"}
    assert calls[0]["user_id"] == "115001"
    assert calls[0]["user_key"] == "fixture-user-key"
    assert "headers" not in calls[0]


@pytest.mark.parametrize(
    ("initialized", "message"),
    [
        (None, "initialization was rejected"),
        ({"state": False}, "initialization was rejected"),
        ({"state": True, "data": []}, "payload is invalid"),
        ({"state": True, "data": {"object": "key", "bucket": "bucket"}}, "no resumable OSS identity"),
        (
            {"state": True, "data": {"callback": {"value": "fixture"}, "bucket": "bucket"}},
            "no resumable OSS identity",
        ),
        (
            {"state": True, "data": {"callback": {"value": "fixture"}, "object": "key"}},
            "no resumable OSS identity",
        ),
    ],
)
def test_pinned_upload_rejects_invalid_initialization_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    initialized: object,
    message: str,
) -> None:
    source = tmp_path / "Movie.mkv"
    source.write_bytes(b"fixture")
    credentials = PinnedUploadCredentials(
        P115CredentialSnapshot(access_token="fixture-access-token", account_id="115001")
    )
    operations = _pinned_upload_operations(credentials)
    monkeypatch.setattr("p115oss.upload_file_init", lambda *_args, **_kwargs: initialized)

    with pytest.raises(P115MaintenanceTransportError, match=message):
        operations.upload_local_file(
            str(source),
            "9001",
            session={},
            checkpoint=lambda _value: None,
        )


def test_pinned_upload_creates_and_resumes_multipart_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "Movie.mkv"
    with source.open("wb") as stream:
        stream.truncate(maintenance_transport_module._UPLOAD_PART_SIZE + 1)
    credentials = PinnedUploadCredentials(
        P115CredentialSnapshot(access_token="fixture-access-token", account_id="115001")
    )
    operations = _pinned_upload_operations(credentials)
    checkpoints: list[Mapping[str, Any]] = []
    uploads: list[dict[str, Any]] = []

    monkeypatch.setattr(
        "p115oss.upload_file_init",
        lambda *_args, **_kwargs: {
            "state": True,
            "data": {
                "callback": {"value": "fixture"},
                "object": "object-key",
                "bucket": "bucket",
                "filesha1": "A" * 40,
            },
        },
    )
    monkeypatch.setattr(
        "p115oss.oss_multipart_upload_init",
        lambda object_key, _token, **kwargs: (
            "upload-1"
            if object_key == "object-key"
            and kwargs["bucket"] == "bucket"
            and kwargs["headers"] == {"authorization": "Bearer fixture-access-token"}
            else pytest.fail("unexpected multipart initialization")
        ),
    )

    def upload(_path: str, **kwargs: Any) -> Mapping[str, Any]:
        uploads.append(kwargs)
        return {"state": True, "fid": "501"}

    monkeypatch.setattr("p115oss.upload_file", upload)
    result = operations.upload_local_file(
        str(source),
        "9001",
        session={},
        checkpoint=lambda value: checkpoints.append(dict(value)),
    )

    assert result == {"state": True, "fid": "501"}
    assert checkpoints[0]["phase"] == "upload_init"
    assert checkpoints[1] == {
        "filename": "Movie.mkv",
        "filesha1": "A" * 40,
        "filesize": maintenance_transport_module._UPLOAD_PART_SIZE + 1,
        "partsize": maintenance_transport_module._UPLOAD_PART_SIZE,
        "callback": {"value": "fixture"},
        "upload_id": "upload-1",
        "bucket": "bucket",
    }
    assert uploads[0]["upload_id"] == "upload-1"
    assert uploads[0]["headers"] == {"authorization": "Bearer fixture-access-token"}

    monkeypatch.setattr("p115oss.upload_file_init", lambda *_args, **_kwargs: pytest.fail("unexpected init"))
    checkpoints.clear()
    operations.upload_local_file(
        str(source),
        "9001",
        session={
            "filename": "resumed.mkv",
            "filesize": 5,
            "partsize": 1024,
            "filesha1": "B" * 40,
            "callback": {"value": "resume"},
            "upload_id": "upload-2",
        },
        checkpoint=lambda value: checkpoints.append(dict(value)),
    )
    assert checkpoints[0]["phase"] == "multipart_upload"
    assert uploads[1]["filename"] == "resumed.mkv"
    assert uploads[1]["bucket"] == "fhnfile"
    assert uploads[1]["upload_id"] == "upload-2"


@pytest.mark.parametrize("when", ["before", "after"])
def test_pinned_upload_fences_credential_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    when: str,
) -> None:
    source = tmp_path / "Movie.mkv"
    source.write_bytes(b"fixture")
    credentials = PinnedUploadCredentials(
        P115CredentialSnapshot(access_token="fixture-access-token", account_id="115001")
    )
    operations = _pinned_upload_operations(credentials)

    def rotate() -> None:
        credentials.snapshot = P115CredentialSnapshot(access_token="rotated", account_id="115001")

    if when == "before":
        rotate()
    monkeypatch.setattr(
        "p115oss.upload_file_init",
        lambda *_args, **_kwargs: {
            "state": True,
            "data": {"callback": {"value": "fixture"}, "object": "key", "bucket": "bucket"},
        },
    )

    def direct_upload(*_args: Any, **_kwargs: Any) -> Mapping[str, Any]:
        rotate()
        return {"state": True}

    monkeypatch.setattr("p115oss.oss_upload", direct_upload)
    with pytest.raises(P115MaintenanceTransportError, match="credential changed"):
        operations.upload_local_file(
            str(source),
            "9001",
            session={},
            checkpoint=lambda _value: None,
        )
