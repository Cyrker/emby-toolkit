from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, cast

from sqlalchemy import text

from etk.domains.p115 import P115CredentialSnapshot
from etk.domains.sharing.execution import SharingOperationExecutor
from etk.domains.sharing.operation_delivery import SharingProviderOperationDelivery
from etk.domains.sharing.operations import SharingOperationIntent, SharingOperationKind
from etk.domains.sharing.repository import PostgresSharingOperationRepository
from etk.domains.sharing.share_transfer import (
    LogicalSeasonShareTransferOperationDelivery,
    ShareImportOutcome,
    ShareTarget,
    ShareTransferReadback,
)
from etk.domains.sharing.virtual_import import VirtualImportBundle, freeze_virtual_import_source
from etk.integrations import ProviderDisposition, ProviderResponse
from etk.platform import Settings
from etk.platform.db import DatabaseRuntime


def _source() -> dict[str, Any]:
    return {
        "source_kind": "logical_season",
        "source_id": "logical-season-recovery",
        "item_type": "Episode",
        "title": "Recovery Series",
        "release_year": 2026,
        "parent_series_tmdb_id": "42",
        "season_number": 1,
        "files": [
            {
                "source_kind": "logical_episode",
                "source_id": "logical-episode-recovery",
                "sha1": "A" * 40,
                "size": 1024,
                "file_name": "Recovery.S01E01.mkv",
                "season_number": 1,
                "episode_number": 1,
            }
        ],
    }


class _Credentials:
    def load(self) -> P115CredentialSnapshot:
        return P115CredentialSnapshot(cookie="UID=fixture", app_type="web", account_id="115001")


class _VirtualImports:
    def prepare(self, source: Mapping[str, Any]) -> VirtualImportBundle:
        file = dict(cast(list[Mapping[str, Any]], source["files"])[0])
        return VirtualImportBundle("logical_season", "logical-season-recovery", dict(source), (file,))


class _Center:
    def logical_season_manifest(self, group_id: str) -> ProviderResponse[dict[str, Any]]:
        assert group_id == "logical-season-recovery"
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {"item": {"files": [{"sha1": "A" * 40, "size": 1024}]}},
        )

    def get_logical_season_share_channel(self, group_id: str) -> ProviderResponse[dict[str, Any]]:
        assert group_id == "logical-season-recovery"
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {
                "item": {
                    "channel_id": "channel-recovery",
                    "status": "valid",
                    "share_code": "provider-share-secret",
                    "receive_code": "provider-receive-secret",
                }
            },
        )


class _LostShareResponseP115:
    def __init__(self) -> None:
        self.import_calls = 0
        self.readback_calls = 0

    def find_target(self, identity: Mapping[str, Any]) -> ShareTarget:
        del identity
        return ShareTarget("missing")

    def create_target(self, identity: Mapping[str, Any]) -> ShareTarget:
        del identity
        return ShareTarget("found", "season-folder-recovery")

    def import_share(
        self,
        identity: Mapping[str, Any],
        *,
        share_code: str,
        receive_code: str,
    ) -> ShareImportOutcome:
        del identity
        assert (share_code, receive_code) == ("provider-share-secret", "provider-receive-secret")
        self.import_calls += 1
        raise RuntimeError("fixture lost response after share import")

    def readback(self, identity: Mapping[str, Any]) -> ShareTransferReadback:
        self.readback_calls += 1
        file = cast(list[Mapping[str, Any]], identity["files"])[0]
        return ShareTransferReadback(
            "confirmed",
            receipts=(
                {
                    "fid": "remote-recovery",
                    "sha1": file["sha1"],
                    "size": file["size"],
                    "target_cid": identity["target_folder_cid"],
                },
            ),
        )


def test_postgres_share_transfer_recovers_by_readback_and_stages_separate_report(
    migrated_p9_runtime: DatabaseRuntime,
) -> None:
    source = _source()
    frozen = freeze_virtual_import_source(source)
    repository = PostgresSharingOperationRepository(migrated_p9_runtime.unit_of_work)
    operation = repository.reserve(
        SharingOperationIntent.create(
            kind=SharingOperationKind.TRANSFER,
            scope_key="center-share-transfer:logical_season:logical-season-recovery",
            principal_id="share-recovery-fixture",
            idempotency_key="share-recovery-fixture",
            request={
                "provider": "p115",
                "action": "transfer_logical_season_share",
                "payload": {
                    "source_kind": frozen.source_kind,
                    "source_id": frozen.source_id,
                    "source": frozen.source,
                    "source_state_hash": frozen.state_hash,
                    "mode": "share",
                    "expected_channel_id": "channel-recovery",
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
    p115 = _LostShareResponseP115()
    delivery = LogicalSeasonShareTransferOperationDelivery(
        repository=repository,
        virtual_imports=cast(Any, _VirtualImports()),
        credentials=cast(Any, _Credentials()),
        settings=lambda: Settings(p115_save_path_cid="target-recovery"),
        center=cast(Any, center),
        p115=p115,
    )
    executor = SharingOperationExecutor(
        repository,
        SharingProviderOperationDelivery(
            shared_center=cast(Any, center),
            hdhive=cast(Any, object()),
            extra_deliveries=(delivery,),
        ),
        retry_delay_seconds=0,
    )

    first = executor.execute(operation.id, worker_id="share-recovery-worker-1", lease_seconds=30)
    second = executor.execute(operation.id, worker_id="share-recovery-worker-2", lease_seconds=30)

    assert first is not None and first.operation.status.value == "awaiting_readback"
    assert second is not None and second.operation.status.value == "confirmed"
    assert p115.import_calls == 1
    assert p115.readback_calls == 1
    with migrated_p9_runtime.unit_of_work() as uow:
        rows = (
            uow.session.execute(
                text(
                    "SELECT scope_key, request_json, state_json, result_json FROM sharing_operations "
                    "WHERE id = :id OR scope_key = :report_scope ORDER BY scope_key"
                ),
                {"id": operation.id, "report_scope": f"center-transfer-report:{operation.id}"},
            )
            .mappings()
            .all()
        )
    assert len(rows) == 2
    report = next(row for row in rows if row["scope_key"].startswith("center-transfer-report:"))
    assert report["request_json"]["payload"]["transfer_mode"] == "share"
    assert report["request_json"]["payload"]["share_channel_id"] == "channel-recovery"
    assert report["request_json"]["payload"]["success_count"] == 1
    durable = json.dumps([dict(row) for row in rows], ensure_ascii=False, default=str)
    assert "provider-share-secret" not in durable
    assert "provider-receive-secret" not in durable


def test_postgres_definitive_dead_share_stages_channel_status_without_credentials(
    migrated_p9_runtime: DatabaseRuntime,
) -> None:
    repository = PostgresSharingOperationRepository(migrated_p9_runtime.unit_of_work)
    operation = repository.reserve(
        SharingOperationIntent.create(
            kind=SharingOperationKind.TRANSFER,
            scope_key="center-share-transfer:logical_season:dead-fixture",
            principal_id="share-dead-fixture",
            idempotency_key="share-dead-fixture",
            request={
                "provider": "p115",
                "action": "transfer_logical_season_share",
                "payload": {"source": {"source_kind": "logical_season", "source_id": "dead-fixture"}},
                "local_continuation": {
                    "type": "finalize_center_transfer",
                    "source_kind": "logical_season",
                    "source_id": "dead-fixture",
                },
            },
        )
    )
    claimed = repository.claim_by_id(operation.id, worker_id="share-dead-worker", lease_seconds=30)
    assert claimed is not None and claimed.claim_token is not None
    repository.save_prewrite_state(
        claimed.id,
        claimed.claim_token,
        {
            "kind": "logical_season_share_transfer",
            "channel_id": "channel-dead",
            "channel_failure_status": "expired",
            "files": [{"sha1": "A" * 40, "size": 1024, "file_name": "Dead.mkv"}],
            "completed_file_indexes": [],
        },
    )
    repository.mark_requesting(claimed.id, claimed.claim_token)
    repository.mark_rejected(claimed.id, claimed.claim_token, error={"type": "share_import_rejected"})

    with migrated_p9_runtime.unit_of_work() as uow:
        rows = (
            uow.session.execute(
                text(
                    "SELECT scope_key, request_json FROM sharing_operations "
                    "WHERE scope_key IN (:report_scope, :status_scope) ORDER BY scope_key"
                ),
                {
                    "report_scope": f"center-transfer-report:{operation.id}",
                    "status_scope": f"center-share-channel-status:{operation.id}",
                },
            )
            .mappings()
            .all()
        )
    assert len(rows) == 2
    status = next(row for row in rows if row["request_json"]["action"] == "update_logical_season_share_status")
    assert status["request_json"]["payload"] == {
        "channel_id": "channel-dead",
        "body": {
            "status": "expired",
            "review_status": "expired",
            "status_message": "native share import rejected by authoritative provider response",
        },
    }
    assert "share_code" not in json.dumps([dict(row) for row in rows], default=str)
