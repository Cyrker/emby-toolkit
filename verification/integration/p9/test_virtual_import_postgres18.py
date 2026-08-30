from __future__ import annotations

import hashlib
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import TracebackType
from typing import Any, cast

import pytest
from sqlalchemy import text

from etk.domains.p115 import AtomicP115LocalStrmLibraryGateway
from etk.domains.sharing import InMemoryContributionLedger, SharingService
from etk.domains.sharing.execution import SharingOperationExecutor
from etk.domains.sharing.operation_delivery import SharingProviderOperationDelivery
from etk.domains.sharing.operations import SharingOperationIntent, SharingOperationKind
from etk.domains.sharing.repository import PostgresSharingOperationRepository
from etk.domains.sharing.virtual_import import (
    AtomicVirtualImportProjector,
    SqlAlchemyVirtualImportRepository,
    VirtualImportBundle,
    VirtualImportOperationDelivery,
    VirtualImportProjection,
    VirtualImportProjectionReconcileRequired,
    VirtualImportService,
    virtual_import_state_hash,
)
from etk.platform import Settings
from etk.platform.db import DatabaseRuntime


def _bundle(source_id: str, sha1: str) -> VirtualImportBundle:
    source = {
        "source_kind": "movie",
        "source_id": source_id,
        "item_type": "Movie",
        "title": f"Fixture {source_id}",
        "tmdb_id": source_id,
        "sha1": sha1,
        "size": 1024,
        "file_name": f"{source_id}.mkv",
    }
    return VirtualImportBundle("movie", source_id, source, (dict(source),))


def _projector(root: Path) -> AtomicVirtualImportProjector:
    return AtomicVirtualImportProjector(
        settings=lambda: Settings(local_strm_root=str(root), etk_server_url="https://etk.example.test"),
        libraries=AtomicP115LocalStrmLibraryGateway(),
    )


def test_virtual_import_concurrent_replay_and_owned_cleanup_are_atomic(
    migrated_p9_runtime: DatabaseRuntime,
    tmp_path: Path,
) -> None:
    repository = SqlAlchemyVirtualImportRepository(migrated_p9_runtime.unit_of_work)
    projector = _projector(tmp_path)
    bundle = _bundle("901", "A" * 40)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(lambda _index: repository.create(bundle, projector), range(2)))

    assert sorted(result.created for result in results) == [False, True]
    rows = repository.active_by_source("movie", "901")
    assert len(rows) == 1
    assert len(list(tmp_path.rglob("*.strm"))) == 1

    row = rows[0]
    result = repository.remove(
        int(row["id"]),
        expected_state_hash=virtual_import_state_hash(row),
        projector=projector,
    )

    assert result["removed"] is True
    assert repository.get(int(row["id"])) is None
    assert not list(tmp_path.rglob("*.strm"))


def test_virtual_import_digest_drift_preserves_file_and_database_row(
    migrated_p9_runtime: DatabaseRuntime,
    tmp_path: Path,
) -> None:
    repository = SqlAlchemyVirtualImportRepository(migrated_p9_runtime.unit_of_work)
    projector = _projector(tmp_path)
    creation = repository.create(_bundle("902", "B" * 40), projector)
    row = creation.item
    path = Path(row["strm_paths_json"][0])
    path.write_text("changed-by-user", encoding="utf-8")

    with pytest.raises(RuntimeError, match="ownership"):
        repository.remove(
            int(row["id"]),
            expected_state_hash=virtual_import_state_hash(row),
            projector=projector,
        )

    assert path.read_text(encoding="utf-8") == "changed-by-user"
    assert repository.get(int(row["id"])) is not None
    with migrated_p9_runtime.unit_of_work() as uow:
        count = uow.session.scalar(
            text("SELECT COUNT(*) FROM shared_virtual_imports WHERE id = :id"),
            {"id": int(row["id"])},
        )
    assert int(count or 0) == 1


def test_virtual_import_commit_failure_rolls_back_database_and_projection(
    migrated_p9_runtime: DatabaseRuntime,
    tmp_path: Path,
) -> None:
    class FailingCommitUnitOfWork:
        def __init__(self) -> None:
            self._inner = migrated_p9_runtime.unit_of_work()

        @property
        def session(self) -> Any:
            return self._inner.session

        def __enter__(self) -> FailingCommitUnitOfWork:
            self._inner.__enter__()
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: TracebackType | None,
        ) -> None:
            self._inner.__exit__(exc_type, exc_value, traceback)

        def commit(self) -> None:
            raise RuntimeError("fixture commit failure")

        def rollback(self) -> None:
            self._inner.rollback()

    repository = SqlAlchemyVirtualImportRepository(lambda: FailingCommitUnitOfWork())  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="commit failure"):
        repository.create(_bundle("903", "C" * 40), _projector(tmp_path))

    assert not list(tmp_path.rglob("*.strm"))
    with migrated_p9_runtime.unit_of_work() as uow:
        count = uow.session.scalar(text("SELECT COUNT(*) FROM shared_virtual_imports WHERE source_id = '903'"))
    assert int(count or 0) == 0


def test_virtual_delete_terminal_and_row_deletion_commit_through_one_operation_uow(
    migrated_p9_runtime: DatabaseRuntime,
    tmp_path: Path,
) -> None:
    virtual_repository = SqlAlchemyVirtualImportRepository(migrated_p9_runtime.unit_of_work)
    projector = _projector(tmp_path)
    creation = virtual_repository.create(_bundle("904", "D" * 40), projector)
    row = creation.item
    expected_hash = virtual_import_state_hash(row)
    service = VirtualImportService(
        resolver=cast(Any, object()),
        repository=virtual_repository,
        projector=projector,
        configuration=lambda: {},
    )

    class AbsentEmby:
        def exact_item_ids(self, paths: Any) -> tuple[str, ...]:
            del paths
            return ()

        def delete_items(self, item_ids: Any) -> None:
            del item_ids
            return None

    operation_repository = PostgresSharingOperationRepository(migrated_p9_runtime.unit_of_work)
    operation = operation_repository.reserve(
        SharingOperationIntent.create(
            kind=SharingOperationKind.VIRTUAL_IMPORT,
            scope_key=f"virtual-import:{row['id']}",
            principal_id="virtual-delete-fixture",
            idempotency_key="virtual-delete-fixture",
            request={
                "provider": "virtual_import",
                "action": "delete",
                "payload": {"virtual_id": int(row["id"]), "state_hash": expected_hash},
                "local_continuation": {
                    "type": "finalize_virtual_import_deletion",
                    "virtual_id": int(row["id"]),
                    "expected_state_hash": expected_hash,
                },
            },
        )
    )
    executor = SharingOperationExecutor(
        operation_repository,
        SharingProviderOperationDelivery(
            shared_center=cast(Any, object()),
            hdhive=cast(Any, object()),
            virtual_imports=VirtualImportOperationDelivery(service, AbsentEmby()),
        ),
    )

    execution = executor.execute(operation.id, worker_id="virtual-delete-worker", lease_seconds=30)

    assert execution is not None and execution.operation.status.value == "confirmed"
    assert virtual_repository.get(int(row["id"])) is None
    assert not list(tmp_path.rglob("*.strm"))
    with migrated_p9_runtime.unit_of_work() as uow:
        outbox_status = uow.session.scalar(
            text("SELECT status FROM outbox_events WHERE aggregate_id = :operation_id"),
            {"operation_id": str(operation.id)},
        )
    assert outbox_status == "processed"

    replayed = SharingService(
        InMemoryContributionLedger(),
        cast(Any, object()),
        operation_repository,
        virtual_imports=service,
    ).delete_virtual_import(
        int(row["id"]),
        principal_id="virtual-delete-fixture",
        idempotency_key="virtual-delete-fixture",
    )
    assert replayed.status_code == 200
    assert replayed.payload["operation_id"] == str(operation.id)
    assert replayed.payload["replayed"] is True


def test_virtual_projection_compensation_failure_keeps_reconcile_record(
    migrated_p9_runtime: DatabaseRuntime,
    tmp_path: Path,
) -> None:
    target = tmp_path / "reconcile.strm"
    content = "https://etk.example.test/api/p115/virtual-play/reconcile"
    digest = hashlib.sha256(content.encode()).hexdigest()

    class ReconcileProjector:
        def project(self, virtual_id: int, bundle: VirtualImportBundle) -> VirtualImportProjection:
            del virtual_id
            target.write_text(content, encoding="utf-8")
            file = {
                **bundle.files[0],
                "virtual_relative_path": "reconcile.strm",
                "virtual_file_name": "reconcile.mkv",
                "virtual_content_sha256": digest,
            }
            projection = VirtualImportProjection((str(target),), (file,))
            raise VirtualImportProjectionReconcileRequired("fixture compensation failure", projection)

        def cleanup(self, item: Mapping[str, Any]) -> Mapping[str, Any]:
            del item
            raise AssertionError("repository must preserve the failed compensation snapshot")

    repository = SqlAlchemyVirtualImportRepository(migrated_p9_runtime.unit_of_work)

    with pytest.raises(VirtualImportProjectionReconcileRequired, match="compensation failure"):
        repository.create(_bundle("905", "E" * 40), ReconcileProjector())

    rows = repository.active_by_source("movie", "905")
    assert len(rows) == 1
    assert rows[0]["status"] == "reconcile_required"
    assert rows[0]["source_payload_json"]["_etk_reconcile"]["type"] == "projection_compensation_failed"
    assert rows[0]["files_json"][0]["virtual_content_sha256"] == digest
    assert target.read_text(encoding="utf-8") == content
