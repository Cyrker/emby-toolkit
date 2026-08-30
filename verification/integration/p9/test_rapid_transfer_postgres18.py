from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, cast

from sqlalchemy import text

from etk.domains.p115 import P115CredentialSnapshot
from etk.domains.sharing.execution import SharingOperationExecutor
from etk.domains.sharing.operation_delivery import SharingProviderOperationDelivery
from etk.domains.sharing.operations import SharingOperationIntent, SharingOperationKind
from etk.domains.sharing.rapid_transfer import (
    RapidTransferOperationDelivery,
    RapidTransferReadback,
    RapidUploadOutcome,
)
from etk.domains.sharing.repository import PostgresSharingOperationRepository
from etk.domains.sharing.virtual_import import (
    VirtualImportBundle,
    freeze_virtual_import_source,
    virtual_import_state_hash,
)
from etk.integrations import ProviderDisposition, ProviderResponse
from etk.platform import Settings
from etk.platform.db import DatabaseRuntime


class _Credentials:
    def load(self) -> P115CredentialSnapshot:
        return P115CredentialSnapshot(access_token="fixture-token", account_id="115001")


class _VirtualImports:
    def prepare(self, source: Mapping[str, Any]) -> VirtualImportBundle:
        return VirtualImportBundle("movie", "source-recovery", dict(source), (dict(source),))

    def get(self, _virtual_id: int) -> None:
        return None


class _Center:
    def __init__(self) -> None:
        self.lease_calls = 0

    def acquire_transfer_lease(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]:
        self.lease_calls += 1
        assert payload["request_meta_json"]["operation_id"]
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"allow": True, "lease_id": "lease-recovery"})


class _UnknownFinalizeP115:
    def __init__(self) -> None:
        self.readback_calls = 0
        self.upload_calls = 0

    def readback(self, identity: Mapping[str, Any]) -> RapidTransferReadback:
        self.readback_calls += 1
        if self.readback_calls == 1:
            return RapidTransferReadback("partial", missing_count=1)
        file = identity["files"][0]
        return RapidTransferReadback(
            "confirmed",
            receipts=(
                {
                    "fid": "remote-recovery",
                    "pick_code": "pick-recovery",
                    "sha1": file["sha1"],
                    "size": file["size"],
                    "target_cid": identity["target_cid"],
                },
            ),
        )

    def upload_init(
        self,
        identity: Mapping[str, Any],
        file: Mapping[str, Any],
        *,
        sign_key: str = "",
        sign_val: str = "",
    ) -> RapidUploadOutcome:
        del identity, file, sign_key, sign_val
        self.upload_calls += 1
        raise RuntimeError("fixture response lost after upload init")


def test_postgres_rapid_transfer_recovers_by_readback_without_resending_mutations(
    migrated_p9_runtime: DatabaseRuntime,
) -> None:
    source = {
        "source_kind": "movie",
        "source_id": "source-recovery",
        "item_type": "Movie",
        "title": "Recovery Fixture",
        "sha1": "D" * 40,
        "size": 2048,
        "file_name": "Recovery Fixture.mkv",
        "preid": "E" * 40,
    }
    frozen = freeze_virtual_import_source(source)
    repository = PostgresSharingOperationRepository(migrated_p9_runtime.unit_of_work)
    operation = repository.reserve(
        SharingOperationIntent.create(
            kind=SharingOperationKind.TRANSFER,
            scope_key="center-transfer:movie:source-recovery",
            principal_id="rapid-recovery-fixture",
            idempotency_key="rapid-recovery-fixture",
            request={
                "provider": "p115",
                "action": "transfer_center_source",
                "payload": {
                    "source_kind": frozen.source_kind,
                    "source_id": frozen.source_id,
                    "source": frozen.source,
                    "source_state_hash": frozen.state_hash,
                    "mode": "rapid",
                },
                "local_continuation": {
                    "type": "finalize_center_transfer",
                    "source_kind": frozen.source_kind,
                    "source_id": frozen.source_id,
                },
            },
        )
    )
    center = _Center()
    p115 = _UnknownFinalizeP115()
    rapid = RapidTransferOperationDelivery(
        repository=repository,
        virtual_imports=cast(Any, _VirtualImports()),
        credentials=cast(Any, _Credentials()),
        settings=lambda: Settings(p115_api_priority="openapi", p115_save_path_cid="target-recovery"),
        center=cast(Any, center),
        p115=p115,
    )
    executor = SharingOperationExecutor(
        repository,
        SharingProviderOperationDelivery(
            shared_center=cast(Any, center),
            hdhive=cast(Any, object()),
            rapid_transfers=rapid,
        ),
        retry_delay_seconds=0,
    )

    first = executor.execute(operation.id, worker_id="rapid-recovery-worker-1", lease_seconds=30)
    second = executor.execute(operation.id, worker_id="rapid-recovery-worker-2", lease_seconds=30)

    assert first is not None and first.operation.status.value == "awaiting_readback"
    assert second is not None and second.operation.status.value == "confirmed"
    assert center.lease_calls == 1
    assert p115.upload_calls == 1
    assert p115.readback_calls == 2
    assert second.operation.state["phase"] == "upload_init_pending"
    assert second.operation.state["lease_id"] == "lease-recovery"
    with migrated_p9_runtime.unit_of_work() as uow:
        report = (
            uow.session.execute(
                text("SELECT request_json, status FROM sharing_operations WHERE scope_key = :scope_key"),
                {"scope_key": f"center-transfer-report:{operation.id}"},
            )
            .mappings()
            .one()
        )
    assert report["status"] == "ready"
    assert report["request_json"]["payload"]["lease_id"] == "lease-recovery"
    assert report["request_json"]["payload"]["files_manifest_hash"] == second.operation.state["files_manifest_hash"]


def test_postgres_rejected_rapid_upload_stages_failure_report_for_acquired_lease(
    migrated_p9_runtime: DatabaseRuntime,
) -> None:
    repository = PostgresSharingOperationRepository(migrated_p9_runtime.unit_of_work)
    operation = repository.reserve(
        SharingOperationIntent.create(
            kind=SharingOperationKind.TRANSFER,
            scope_key="center-transfer:movie:source-rejected",
            principal_id="rapid-rejected-fixture",
            idempotency_key="rapid-rejected-fixture",
            request={
                "provider": "p115",
                "action": "transfer_center_source",
                "payload": {"source": {"source_kind": "movie", "source_id": "source-rejected"}},
                "local_continuation": {
                    "type": "finalize_center_transfer",
                    "source_kind": "movie",
                    "source_id": "source-rejected",
                },
            },
        )
    )
    claimed = repository.claim_by_id(operation.id, worker_id="rapid-rejected-worker", lease_seconds=30)
    assert claimed is not None and claimed.claim_token is not None
    repository.save_prewrite_state(
        claimed.id,
        claimed.claim_token,
        {
            "kind": "rapid_transfer",
            "lease_id": "lease-rejected",
            "files": [{"sha1": "A" * 40, "size": 1024, "file_name": "Rejected.mkv", "preid": "B" * 40}],
            "completed_file_indexes": [],
        },
    )
    repository.mark_requesting(claimed.id, claimed.claim_token)

    rejected = repository.mark_rejected(
        claimed.id,
        claimed.claim_token,
        error={"type": "rapid_plain_upload_rejected"},
    )

    assert rejected.status.value == "rejected"
    with migrated_p9_runtime.unit_of_work() as uow:
        report = (
            uow.session.execute(
                text("SELECT request_json, status FROM sharing_operations WHERE scope_key = :scope_key"),
                {"scope_key": f"center-transfer-report:{operation.id}"},
            )
            .mappings()
            .one()
        )
    assert report["status"] == "ready"
    assert report["request_json"]["payload"] == {
        "source_kind": "movie",
        "source_id": "source-rejected",
        "result": "failed",
        "success_count": 0,
        "total_count": 1,
        "transfer_mode": "rapid",
        "lease_id": "lease-rejected",
        "message": "native rapid transfer rejected: rapid_plain_upload_rejected",
    }


def test_confirmed_promotion_marks_washing_and_stages_durable_transfer_report(
    migrated_p9_runtime: DatabaseRuntime,
) -> None:
    with migrated_p9_runtime.unit_of_work() as uow:
        uow.session.execute(
            text(
                """
                INSERT INTO media_metadata(
                    tmdb_id, item_type, parent_series_tmdb_id, season_number, episode_number,
                    active_washing
                ) VALUES ('episode-1', 'Episode', 'series-901', 1, 1, FALSE)
                """
            )
        )
        row = (
            uow.session.execute(
                text(
                    """
                    INSERT INTO shared_virtual_imports(
                        source_kind, source_id, tmdb_id, item_type, parent_series_tmdb_id,
                        season_number, file_count, total_size, status, source_payload_json,
                        files_json, strm_paths_json
                    ) VALUES (
                        'episode', 'source-901', 'series-901', 'Episode', 'series-901',
                        1, 1, 1024, 'virtual',
                        CAST(:source_payload AS jsonb),
                        CAST(:files AS jsonb),
                        CAST(:paths AS jsonb)
                    ) RETURNING *
                    """
                ),
                {
                    "source_payload": json.dumps(
                        {"source_kind": "episode", "source_id": "source-901"},
                        separators=(",", ":"),
                    ),
                    "files": json.dumps(
                        [
                            {
                                "sha1": "A" * 40,
                                "size": 1024,
                                "season_number": 1,
                                "episode_number": 1,
                            }
                        ],
                        separators=(",", ":"),
                    ),
                    "paths": json.dumps(["/library/Fixture.strm"], separators=(",", ":")),
                },
            )
            .mappings()
            .one()
        )
        virtual = dict(row)
        uow.commit()

    expected_hash = virtual_import_state_hash(virtual)
    repository = PostgresSharingOperationRepository(migrated_p9_runtime.unit_of_work)
    intent = SharingOperationIntent.create(
        kind=SharingOperationKind.VIRTUAL_IMPORT,
        scope_key=f"virtual-import:{virtual['id']}",
        principal_id="admin-fixture",
        idempotency_key="promotion-fixture",
        request={
            "provider": "p115",
            "action": "transfer_center_source",
            "payload": {"source": {"source_kind": "episode", "source_id": "source-901"}},
            "local_continuation": {
                "type": "finalize_center_transfer",
                "source_kind": "episode",
                "source_id": "source-901",
                "virtual_id": int(virtual["id"]),
                "expected_state_hash": expected_hash,
            },
        },
    )
    operation = repository.reserve(intent)
    claimed = repository.claim_by_id(operation.id, worker_id="worker-fixture", lease_seconds=30)
    assert claimed is not None and claimed.claim_token is not None
    repository.save_prewrite_state(
        claimed.id,
        claimed.claim_token,
        {
            "kind": "rapid_transfer",
            "account_id": "115001",
            "backend": "openapi",
            "target_cid": "target-fixture",
            "files": [{"sha1": "A" * 40, "size": 1024, "file_name": "Fixture.mkv", "preid": "B" * 40}],
        },
    )
    repository.mark_requesting(claimed.id, claimed.claim_token)

    confirmed = repository.mark_confirmed(
        claimed.id,
        claimed.claim_token,
        result={
            "authoritative_readback": True,
            "transfer_confirmed": True,
            "existing": False,
            "lease_id": "lease-fixture",
            "success_count": 1,
            "total_count": 1,
        },
    )

    assert confirmed.status.value == "confirmed"
    with migrated_p9_runtime.unit_of_work() as uow:
        virtual_state = (
            uow.session.execute(
                text("SELECT status, promoted_at FROM shared_virtual_imports WHERE id = :id"),
                {"id": int(virtual["id"])},
            )
            .mappings()
            .one()
        )
        active_washing = uow.session.scalar(
            text(
                """
                SELECT active_washing FROM media_metadata
                WHERE parent_series_tmdb_id = 'series-901' AND season_number = 1 AND episode_number = 1
                """
            )
        )
        report = (
            uow.session.execute(
                text(
                    """
                    SELECT id, request_json, status FROM sharing_operations
                    WHERE scope_key = :scope_key
                    """
                ),
                {"scope_key": f"center-transfer-report:{operation.id}"},
            )
            .mappings()
            .one()
        )
        outbox_count = uow.session.scalar(
            text(
                """
                SELECT COUNT(*) FROM outbox_events
                WHERE aggregate_id = :aggregate_id AND status = 'pending'
                """
            ),
            {"aggregate_id": str(report["id"])},
        )

    assert virtual_state["status"] == "promoting"
    assert virtual_state["promoted_at"] is not None
    assert active_washing is True
    assert report["status"] == "ready"
    assert report["request_json"]["action"] == "report_transfer"
    assert report["request_json"]["payload"]["lease_id"] == "lease-fixture"
    assert int(outbox_count or 0) == 1


def test_confirmed_transfer_marks_drifted_virtual_row_for_reconciliation(
    migrated_p9_runtime: DatabaseRuntime,
) -> None:
    with migrated_p9_runtime.unit_of_work() as uow:
        virtual = dict(
            (
                uow.session.execute(
                    text(
                        """
                        INSERT INTO shared_virtual_imports(
                            source_kind, source_id, tmdb_id, item_type, file_count, total_size,
                            status, source_payload_json, files_json, strm_paths_json
                        ) VALUES (
                            'movie', 'source-drift', '905', 'Movie', 1, 1024, 'virtual',
                            CAST(:source AS jsonb), CAST(:files AS jsonb), CAST(:paths AS jsonb)
                        ) RETURNING *
                        """
                    ),
                    {
                        "source": json.dumps({"source_kind": "movie", "source_id": "source-drift"}),
                        "files": json.dumps([{"sha1": "F" * 40, "size": 1024}]),
                        "paths": json.dumps(["/library/Drift.strm"]),
                    },
                )
            )
            .mappings()
            .one()
        )
        uow.commit()
    expected_hash = virtual_import_state_hash(virtual)
    repository = PostgresSharingOperationRepository(migrated_p9_runtime.unit_of_work)
    operation = repository.reserve(
        SharingOperationIntent.create(
            kind=SharingOperationKind.VIRTUAL_IMPORT,
            scope_key=f"virtual-import:{virtual['id']}",
            principal_id="drift-fixture",
            idempotency_key="drift-fixture",
            request={
                "provider": "p115",
                "action": "transfer_center_source",
                "payload": {"source": {"source_kind": "movie", "source_id": "source-drift"}},
                "local_continuation": {
                    "type": "finalize_center_transfer",
                    "source_kind": "movie",
                    "source_id": "source-drift",
                    "virtual_id": int(virtual["id"]),
                    "expected_state_hash": expected_hash,
                },
            },
        )
    )
    claimed = repository.claim_by_id(operation.id, worker_id="drift-worker", lease_seconds=30)
    assert claimed is not None and claimed.claim_token is not None
    repository.save_prewrite_state(
        claimed.id,
        claimed.claim_token,
        {
            "kind": "rapid_transfer",
            "account_id": "115001",
            "backend": "openapi",
            "target_cid": "target-drift",
            "files": [{"sha1": "F" * 40, "size": 1024, "file_name": "Drift.mkv", "preid": "A" * 40}],
        },
    )
    repository.mark_requesting(claimed.id, claimed.claim_token)
    with migrated_p9_runtime.unit_of_work() as uow:
        uow.session.execute(
            text(
                """
                UPDATE shared_virtual_imports
                SET source_payload_json = source_payload_json || CAST(:drift AS jsonb)
                WHERE id = :id
                """
            ),
            {"id": int(virtual["id"]), "drift": json.dumps({"drifted": True})},
        )
        uow.commit()

    confirmed = repository.mark_confirmed(
        claimed.id,
        claimed.claim_token,
        result={
            "authoritative_readback": True,
            "transfer_confirmed": True,
            "existing": False,
            "lease_id": "lease-drift",
            "success_count": 1,
            "total_count": 1,
        },
    )

    assert confirmed.status.value == "confirmed"
    with migrated_p9_runtime.unit_of_work() as uow:
        saved = (
            uow.session.execute(
                text("SELECT status, source_payload_json FROM shared_virtual_imports WHERE id = :id"),
                {"id": int(virtual["id"])},
            )
            .mappings()
            .one()
        )
        report_count = uow.session.scalar(
            text("SELECT COUNT(*) FROM sharing_operations WHERE scope_key = :scope_key"),
            {"scope_key": f"center-transfer-report:{operation.id}"},
        )
    assert saved["status"] == "reconcile_required"
    assert saved["source_payload_json"]["_etk_reconcile"]["type"] == "promotion_state_changed"
    assert int(report_count or 0) == 1
