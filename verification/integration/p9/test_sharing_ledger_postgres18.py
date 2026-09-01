from __future__ import annotations

import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from functools import partial
from threading import Event
from typing import Any, cast
from uuid import UUID

import pytest
from sqlalchemy import text

from etk.domains.p115.media_info_repository import SqlAlchemyP115MediaInfoRepository
from etk.domains.sharing import (
    InMemoryContributionLedger,
    InsufficientContributionError,
    PostgresContributionLedger,
    SharingConfigurationService,
    SharingDeliveryOutcome,
    SharingLedgerService,
    SharingOperation,
    SharingOperationExecutor,
    SharingServerIdentity,
    SharingService,
    SqlAlchemySharingProjectionRepository,
)
from etk.domains.sharing.operation_delivery import SharingProviderOperationDelivery
from etk.domains.sharing.operations import (
    SharingOperationConflictError,
    SharingOperationIntent,
    SharingOperationKind,
    SharingOperationScopeBusyError,
    SharingOperationStateError,
)
from etk.domains.sharing.repository import PostgresSharingOperationRepository
from etk.domains.sharing.source_registration import freeze_shared_raw_ffprobe, source_registration_state_hash
from etk.domains.system.repository import SqlAlchemyAppSettingsRepository
from etk.integrations import ProviderDisposition, ProviderResponse
from etk.processes.worker import _load_sharing_raw_ffprobe


class _Delivery:
    def __init__(
        self,
        requests: list[SharingDeliveryOutcome],
        *,
        readbacks: list[SharingDeliveryOutcome] | None = None,
        supports_readback: bool = True,
        checkpoint: bool = True,
    ) -> None:
        self.requests = list(requests)
        self.readbacks = list(readbacks or [])
        self.supports_readback = supports_readback
        self.checkpoint = checkpoint
        self.request_calls = 0
        self.readback_calls = 0

    def supports_authoritative_readback(self, operation: SharingOperation) -> bool:
        del operation
        return self.supports_readback

    def request(
        self,
        operation: SharingOperation,
        *,
        checkpoint: Callable[[], None],
    ) -> SharingDeliveryOutcome:
        del operation
        self.request_calls += 1
        if self.checkpoint:
            checkpoint()
        return self.requests.pop(0)

    def readback(self, operation: SharingOperation) -> SharingDeliveryOutcome:
        del operation
        self.readback_calls += 1
        return self.readbacks.pop(0)


class _MalformedDelivery(_Delivery):
    def request(
        self,
        operation: SharingOperation,
        *,
        checkpoint: Callable[[], None],
    ) -> SharingDeliveryOutcome:
        del operation
        self.request_calls += 1
        checkpoint()
        return cast(SharingDeliveryOutcome, object())


class _BlockingDelivery(_Delivery):
    def __init__(self) -> None:
        super().__init__([])
        self.entered = Event()
        self.release = Event()

    def request(
        self,
        operation: SharingOperation,
        *,
        checkpoint: Callable[[], None],
    ) -> SharingDeliveryOutcome:
        del operation
        self.request_calls += 1
        checkpoint()
        self.entered.set()
        if not self.release.wait(5):
            raise TimeoutError("concurrent sharing delivery test was not released")
        return SharingDeliveryOutcome.confirmed({"receipt_id": "concurrent"})


class _CancelBeforeCheckpointDelivery(_Delivery):
    def __init__(self, repository: PostgresSharingOperationRepository) -> None:
        super().__init__([])
        self.repository = repository
        self.provider_called = False

    def request(
        self,
        operation: SharingOperation,
        *,
        checkpoint: Callable[[], None],
    ) -> SharingDeliveryOutcome:
        self.request_calls += 1
        cancelled = self.repository.request_cancel(operation.id)
        assert cancelled is not None and cancelled.status.value == "cancelled"
        checkpoint()
        self.provider_called = True
        return SharingDeliveryOutcome.confirmed({"receipt_id": "must-not-run"})


class _CancelAfterCheckpointDelivery(_Delivery):
    def __init__(self, repository: PostgresSharingOperationRepository) -> None:
        super().__init__(
            [SharingDeliveryOutcome.retryable({"type": "transport_unknown"})],
            readbacks=[SharingDeliveryOutcome.confirmed({"receipt_id": "authoritative"})],
        )
        self.repository = repository

    def request(
        self,
        operation: SharingOperation,
        *,
        checkpoint: Callable[[], None],
    ) -> SharingDeliveryOutcome:
        self.request_calls += 1
        checkpoint()
        requested = self.repository.request_cancel(operation.id)
        assert requested is not None
        assert requested.status.value == "requesting"
        assert requested.cancel_requested_at is not None
        return self.requests.pop(0)


def _sharing_operation(
    repository: PostgresSharingOperationRepository,
    suffix: str,
    *,
    available_at: datetime | None = None,
) -> SharingOperation:
    return repository.reserve(
        SharingOperationIntent.create(
            kind=SharingOperationKind.TRANSFER,
            scope_key=f"source:executor:{suffix}",
            principal_id=f"device-executor-{suffix}",
            request={"source_id": f"executor-{suffix}", "amount": 1},
            idempotency_key=f"executor-{suffix}",
            available_at=available_at,
        )
    )


def test_postgres_concurrent_debit_and_replay_are_atomic(p9_runtime) -> None:
    ledger = PostgresContributionLedger(p9_runtime.unit_of_work)
    ledger.ensure_account("device-1", 100)

    def debit(index: int) -> bool:
        try:
            ledger.debit(
                "device-1",
                10,
                idempotency_key=f"debit-{index}",
                reason="share_request_escrow",
            )
            return True
        except Exception:
            return False

    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(debit, range(20)))

    assert sum(results) == 10
    assert ledger.balance("device-1") == 0
    replay = ledger.debit(
        "device-1",
        10,
        idempotency_key="debit-0",
        reason="share_request_escrow",
    )
    assert replay.delta == -10
    assert len(ledger.entries("device-1", limit=100)) == 10


def test_postgres_shared_config_and_server_identity_cache_are_native_owned(p9_runtime) -> None:
    settings = SqlAlchemyAppSettingsRepository(p9_runtime.unit_of_work)
    configuration = SharingConfigurationService(settings)

    saved = configuration.save(
        {
            "p115_shared_center_url": "https://center.fixture/",
            "p115_shared_resource_enabled": "yes",
            "p115_shared_resource_mode": "legacy",
            "p115_shared_virtual_auto_promote_movie_percent": 120,
        }
    )
    settings.save("emby_server_id_cache", "postgres-server-id")
    identity = SharingServerIdentity({}, settings)

    assert saved["p115_shared_center_url"] == "https://center.fixture"
    assert saved["p115_shared_resource_enabled"] is True
    assert saved["p115_shared_resource_mode"] == "rapid"
    assert saved["p115_shared_virtual_auto_promote_movie_percent"] == 100
    assert configuration.load() == saved
    assert identity.server_id_hash() == "db0369e380270562a4c69214abbbbe7a7715bce8c0f86deadcc2e4918a7ed4ca"


def test_postgres_transfer_operation_ledger_and_outbox_share_one_uow(p9_runtime) -> None:
    ledger = PostgresContributionLedger(p9_runtime.unit_of_work)
    operations = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    ledger.ensure_account("device-transfer", 5)
    service = SharingLedgerService(
        ledger,
        operations,
        unit_of_work=p9_runtime.unit_of_work,
    )

    result = service.reserve_transfer(
        account_id="device-transfer",
        source_id="source-transfer",
        amount=3,
        principal_id="principal-transfer",
        idempotency_key="transfer-uow",
    )

    assert result.status_code == 202
    assert ledger.balance("device-transfer") == 2
    operation_id = UUID(result.payload["operation_id"])
    operation = operations.read(operation_id)
    assert operation is not None
    assert operation.request == {
        "provider": "shared_center",
        "action": "acquire_transfer_lease",
        "payload": {
            "account_id": "device-transfer",
            "source_id": "source-transfer",
            "amount": 3,
        },
        "ledger_compensation": {
            "type": "release_transfer_escrow",
            "account_id": "device-transfer",
            "amount": 3,
            "debit_idempotency_key": "transfer:transfer-uow",
            "reference": "source-transfer",
        },
    }
    with p9_runtime.engine.connect() as connection:
        outbox = connection.execute(
            text("SELECT status FROM outbox_events WHERE aggregate_id = :operation_id"),
            {"operation_id": str(operation_id)},
        ).scalar_one()
    assert outbox == "pending"


def test_postgres_sharing_cancellation_is_on_the_single_migration_head(p9_runtime) -> None:
    with p9_runtime.engine.connect() as connection:
        head = connection.scalar(text("SELECT version_num FROM etk_migrations.alembic_version"))
        constraint = connection.scalar(
            text(
                """
                SELECT pg_get_constraintdef(oid)
                FROM pg_constraint
                WHERE conname = 'ck_sharing_operation_kind'
                """
            )
        )
        status_constraint = connection.scalar(
            text(
                """
                SELECT pg_get_constraintdef(oid)
                FROM pg_constraint
                WHERE conname = 'ck_sharing_operation_status'
                """
            )
        )
        cancel_column = connection.scalar(
            text(
                """
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'sharing_operations'
                  AND column_name = 'cancel_requested_at'
                """
            )
        )

    assert head == "0025_p11_collection_activation"
    assert "register_device" in str(constraint)
    assert "cancelled" in str(status_constraint)
    assert cancel_column == 1


def test_postgres_sharing_operation_replay_and_identity_scope_conflicts(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    intent = SharingOperationIntent.create(
        kind=SharingOperationKind.REGISTER_SOURCE,
        scope_key="source-registration:fixture-root",
        principal_id="principal-source",
        request={"provider": "shared_center", "action": "register_source", "payload": {"root_fid": "root-1"}},
        idempotency_key="source-registration-1",
    )

    operation, replayed = repository.reserve_with_replay(intent)
    replay, replayed_again = repository.reserve_with_replay(intent)

    assert replay == operation
    assert replayed is False
    assert replayed_again is True
    with pytest.raises(SharingOperationConflictError):
        repository.reserve(
            SharingOperationIntent.create(
                kind=SharingOperationKind.REGISTER_SOURCE,
                scope_key=intent.scope_key,
                principal_id="principal-source",
                request={
                    "provider": "shared_center",
                    "action": "register_source",
                    "payload": {"root_fid": "root-1", "title": "changed"},
                },
                idempotency_key="source-registration-1",
            )
        )
    with pytest.raises(SharingOperationScopeBusyError):
        repository.reserve(
            SharingOperationIntent.create(
                kind=SharingOperationKind.REGISTER_SOURCE,
                scope_key=intent.scope_key,
                principal_id="principal-source",
                request={
                    "provider": "shared_center",
                    "action": "register_source",
                    "payload": {"root_fid": "root-1", "title": "changed"},
                },
                idempotency_key="source-registration-2",
            )
        )

    with p9_runtime.engine.connect() as connection:
        operation_count = connection.scalar(
            text("SELECT COUNT(*) FROM sharing_operations WHERE scope_key = :scope_key"),
            {"scope_key": intent.scope_key},
        )
        outbox_count = connection.scalar(
            text("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = :operation_id"),
            {"operation_id": str(operation.id)},
        )
    assert operation_count == 1
    assert outbox_count == 1


def test_postgres_share_request_participant_operations_replay_conflict_and_terminalize(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    service = SharingService(
        PostgresContributionLedger(p9_runtime.unit_of_work),
        object(),
        repository,
        unit_of_work=p9_runtime.unit_of_work,
    )

    joined = service.co_request_share_request(
        "group-participant",
        {},
        principal_id="principal-participant",
        idempotency_key="participant-occurrence",
    )
    replayed = service.co_request_share_request(
        "group-participant",
        {},
        principal_id="principal-participant",
        idempotency_key="participant-occurrence",
    )
    conflict = service.cancel_share_request(
        "group-participant",
        {},
        principal_id="principal-participant",
        idempotency_key="participant-occurrence",
    )

    assert joined.status_code == 202
    assert replayed.status_code == 202
    assert replayed.payload["replayed"] is True
    assert conflict.status_code == 409
    joined_id = UUID(str(joined.payload["operation_id"]))
    confirmed_join = SharingOperationExecutor(
        repository,
        _Delivery([SharingDeliveryOutcome.confirmed({"group_id": "group-participant"})], supports_readback=False),
    ).execute(joined_id, worker_id="participant-worker", lease_seconds=60)
    assert confirmed_join is not None
    assert confirmed_join.operation.status.value == "confirmed"

    cancelled = service.cancel_share_request(
        "group-participant",
        {"reason": "fixture"},
        principal_id="principal-participant",
        idempotency_key="participant-cancel",
    )
    assert cancelled.status_code == 202
    cancelled_id = UUID(str(cancelled.payload["operation_id"]))
    confirmed_cancel = SharingOperationExecutor(
        repository,
        _Delivery([SharingDeliveryOutcome.confirmed({"group_id": "group-participant"})], supports_readback=False),
    ).execute(cancelled_id, worker_id="participant-worker", lease_seconds=60)
    assert confirmed_cancel is not None
    assert confirmed_cancel.operation.status.value == "confirmed"

    with p9_runtime.engine.connect() as connection:
        rows = (
            connection.execute(
                text(
                    """
                SELECT request_json->>'action' AS action, status
                FROM sharing_operations
                WHERE scope_key = :scope_key
                ORDER BY event_sequence
                """
                ),
                {"scope_key": "share-request-participant:group-participant:principal-participant"},
            )
            .mappings()
            .all()
        )
        pending_outbox = connection.scalar(
            text(
                """
                SELECT COUNT(*) FROM outbox_events
                WHERE aggregate_id = ANY(CAST(:operation_ids AS text[]))
                  AND status <> 'processed'
                """
            ),
            {"operation_ids": [str(joined_id), str(cancelled_id)]},
        )
    assert [dict(row) for row in rows] == [
        {"action": "co_request_share_request", "status": "confirmed"},
        {"action": "cancel_share_request", "status": "confirmed"},
    ]
    assert pending_outbox == 0


def test_postgres_local_source_lifecycle_applies_only_after_provider_confirmation(p9_runtime) -> None:
    with p9_runtime.engine.begin() as connection:
        source_ids = {}
        for key, center_id in (
            ("disable", "center-disable"),
            ("delete", "center-delete"),
            ("unknown", "center-unknown"),
            ("local", ""),
        ):
            source_ids[key] = int(
                connection.scalar(
                    text(
                        """
                        INSERT INTO shared_rapid_sources(
                            source_key, source_kind, center_source_id, tmdb_id, item_type,
                            title, status, center_status, raw_json
                        ) VALUES (
                            :source_key, 'movie', :center_source_id, :tmdb_id, 'Movie',
                            :title, 'active', :center_status, '{}'::jsonb
                        )
                        RETURNING id
                        """
                    ),
                    {
                        "source_key": f"lifecycle-{key}",
                        "center_source_id": center_id,
                        "tmdb_id": str(900 + len(source_ids)),
                        "title": f"Lifecycle {key}",
                        "center_status": "reported" if center_id else "local",
                    },
                )
            )
        connection.execute(
            text(
                """
                INSERT INTO shared_rapid_source_files(local_source_id, fid, sha1, file_name, raw_json)
                VALUES (:source_id, 'file-delete', :sha1, 'delete.mkv', '{}'::jsonb)
                """
            ),
            {"source_id": source_ids["delete"], "sha1": "D" * 40},
        )

    operations = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    service = SharingService(
        PostgresContributionLedger(p9_runtime.unit_of_work),
        object(),
        operations,
        unit_of_work=p9_runtime.unit_of_work,
        projection=SqlAlchemySharingProjectionRepository(p9_runtime.unit_of_work),
    )

    staged_disable = service.disable_local_source(
        source_ids["disable"],
        principal_id="lifecycle-principal",
        idempotency_key="lifecycle-disable",
    )
    staged_delete = service.delete_local_source(
        source_ids["delete"],
        principal_id="lifecycle-principal",
        idempotency_key="lifecycle-delete",
    )
    staged_unknown = service.disable_local_source(
        source_ids["unknown"],
        principal_id="lifecycle-principal",
        idempotency_key="lifecycle-unknown",
    )
    local_delete = service.delete_local_source(
        source_ids["local"],
        principal_id="lifecycle-principal",
        idempotency_key="lifecycle-local",
    )

    assert staged_disable.status_code == 202
    assert staged_delete.status_code == 202
    assert staged_unknown.status_code == 202
    assert local_delete.status_code == 200
    disable_id = UUID(str(staged_disable.payload["operation_id"]))
    delete_id = UUID(str(staged_delete.payload["operation_id"]))
    unknown_id = UUID(str(staged_unknown.payload["operation_id"]))

    confirmed_disable = SharingOperationExecutor(
        operations,
        _Delivery([SharingDeliveryOutcome.confirmed({"source_id": "center-disable"})], supports_readback=False),
    ).execute(disable_id, worker_id="lifecycle-worker", lease_seconds=60)
    confirmed_delete = SharingOperationExecutor(
        operations,
        _Delivery([SharingDeliveryOutcome.confirmed({"source_id": "center-delete"})], supports_readback=False),
    ).execute(delete_id, worker_id="lifecycle-worker", lease_seconds=60)
    unknown_delivery = _Delivery(
        [SharingDeliveryOutcome.indeterminate({"type": "transport_unknown"})],
        supports_readback=False,
    )
    unknown_executor = SharingOperationExecutor(operations, unknown_delivery)
    indeterminate = unknown_executor.execute(
        unknown_id,
        worker_id="lifecycle-worker",
        lease_seconds=60,
    )
    replayed_unknown = unknown_executor.execute(
        unknown_id,
        worker_id="replacement-lifecycle-worker",
        lease_seconds=60,
    )

    assert confirmed_disable is not None and confirmed_disable.operation.status.value == "confirmed"
    assert confirmed_delete is not None and confirmed_delete.operation.status.value == "confirmed"
    assert indeterminate is not None and indeterminate.operation.status.value == "indeterminate"
    assert replayed_unknown is not None and replayed_unknown.operation.status.value == "indeterminate"
    assert unknown_delivery.request_calls == 1
    assert unknown_delivery.readback_calls == 0

    with p9_runtime.engine.connect() as connection:
        disabled = (
            connection.execute(
                text("SELECT status, center_status, raw_json FROM shared_rapid_sources WHERE id = :id"),
                {"id": source_ids["disable"]},
            )
            .mappings()
            .one()
        )
        deleted_count = connection.scalar(
            text("SELECT COUNT(*) FROM shared_rapid_sources WHERE id = :id"),
            {"id": source_ids["delete"]},
        )
        deleted_file_count = connection.scalar(
            text("SELECT COUNT(*) FROM shared_rapid_source_files WHERE local_source_id = :id"),
            {"id": source_ids["delete"]},
        )
        unknown_status = connection.scalar(
            text("SELECT status FROM shared_rapid_sources WHERE id = :id"),
            {"id": source_ids["unknown"]},
        )
        local_count = connection.scalar(
            text("SELECT COUNT(*) FROM shared_rapid_sources WHERE id = :id"),
            {"id": source_ids["local"]},
        )
        pending_outbox = connection.scalar(
            text(
                """
                SELECT COUNT(*) FROM outbox_events
                WHERE aggregate_id = ANY(CAST(:operation_ids AS text[]))
                  AND status <> 'processed'
                """
            ),
            {"operation_ids": [str(disable_id), str(delete_id), str(unknown_id)]},
        )
    assert disabled["status"] == "disabled"
    assert disabled["center_status"] == "disabled"
    assert disabled["raw_json"]["lifecycle_operation_id"] == str(disable_id)
    assert deleted_count == 0
    assert deleted_file_count == 0
    assert unknown_status == "active"
    assert local_count == 0
    assert pending_outbox == 0


def test_postgres_confirmed_device_enables_config_in_terminal_uow(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    with p9_runtime.engine.connect() as connection:
        previous = (
            connection.execute(
                text(
                    """
                SELECT value_json, last_updated_at
                FROM app_settings
                WHERE setting_key = 'shared_resource_config'
                """
                )
            )
            .mappings()
            .one_or_none()
        )
    try:
        with p9_runtime.engine.begin() as connection:
            connection.execute(text("DELETE FROM app_settings WHERE setting_key = 'shared_resource_config'"))
            connection.execute(
                text(
                    """
                    INSERT INTO app_settings(setting_key, value_json)
                    VALUES ('shared_resource_config', '{"p115_shared_resource_enabled": false, "keep": "value"}'::jsonb)
                    """
                )
            )
        operation = repository.reserve(
            SharingOperationIntent.create(
                kind=SharingOperationKind.REGISTER_DEVICE,
                scope_key="device-registration:fixture-hash",
                principal_id="principal-device",
                request={
                    "provider": "shared_center",
                    "action": "register_device",
                    "payload": {"name": "Fixture", "server_id_hash": "a" * 64},
                    "local_continuation": {"type": "enable_shared_resource"},
                },
                idempotency_key="device-registration-1",
            )
        )
        execution = SharingOperationExecutor(
            repository,
            _Delivery([SharingDeliveryOutcome.confirmed({"device_id": "device-1"})]),
        ).execute(operation.id, worker_id="device-worker", lease_seconds=60)

        assert execution is not None
        assert execution.operation.status.value == "confirmed"
        with p9_runtime.engine.connect() as connection:
            config = connection.scalar(
                text("SELECT value_json FROM app_settings WHERE setting_key = 'shared_resource_config'")
            )
            outbox = connection.execute(
                text("SELECT status, processed_at FROM outbox_events WHERE aggregate_id = :operation_id"),
                {"operation_id": str(operation.id)},
            ).one()
        assert config == {"p115_shared_resource_enabled": True, "keep": "value"}
        assert outbox[0] == "processed"
        assert outbox[1] is not None
    finally:
        with p9_runtime.engine.begin() as connection:
            connection.execute(text("DELETE FROM app_settings WHERE setting_key = 'shared_resource_config'"))
            if previous is not None:
                connection.execute(
                    text(
                        """
                        INSERT INTO app_settings(setting_key, value_json, last_updated_at)
                        VALUES ('shared_resource_config', CAST(:value_json AS jsonb), :last_updated_at)
                        """
                    ),
                    {
                        "value_json": json.dumps(previous["value_json"], ensure_ascii=False),
                        "last_updated_at": previous["last_updated_at"],
                    },
                )


def test_postgres_unsupported_local_continuation_rolls_back_terminal_transition(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = repository.reserve(
        SharingOperationIntent.create(
            kind=SharingOperationKind.REGISTER_DEVICE,
            scope_key="device-registration:unsupported-continuation",
            principal_id="principal-device",
            request={
                "provider": "shared_center",
                "action": "register_device",
                "payload": {"name": "Fixture", "server_id_hash": "b" * 64},
                "local_continuation": {"type": "unsupported"},
            },
            idempotency_key="device-registration-unsupported",
        )
    )
    claimed = repository.claim_by_id(operation.id, worker_id="device-worker", lease_seconds=60)
    assert claimed is not None and claimed.claim_token is not None
    requesting = repository.mark_requesting(operation.id, claimed.claim_token)

    with pytest.raises(SharingOperationStateError, match="unsupported"):
        repository.mark_confirmed(operation.id, claimed.claim_token, result={"device_id": "device-1"})

    rolled_back = repository.read(operation.id)
    assert rolled_back is not None
    assert rolled_back.status.value == "requesting"
    assert rolled_back.state_revision == requesting.state_revision
    assert rolled_back.result is None


def test_postgres_source_registration_prerequisite_releases_and_confirms_local_projection(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    sha1 = "D" * 40
    raw = {"format": {"size": "1024"}, "streams": []}
    _sanitized, raw_digest, raw_size_bytes = freeze_shared_raw_ffprobe(raw)
    raw_payload = {
        "sha1": sha1,
        "raw_reference": {"sha1": sha1, "sha256": raw_digest, "size_bytes": raw_size_bytes},
    }
    with p9_runtime.engine.begin() as connection:
        source = (
            connection.execute(
                text(
                    """
                    INSERT INTO shared_rapid_sources(
                        source_key, source_kind, center_source_id, tmdb_id, item_type,
                        title, sha1, file_name, source_provider, status, center_status,
                        file_count, total_size, rapid_meta_json, media_signature_json
                    ) VALUES (
                        'native-register-source', 'movie', 'center-old', '901', 'Movie',
                        'Native Register', :sha1, 'Native Register.mkv', 'manual_rapid',
                        'active', 'reported', 1, 1024,
                        '{"fid":"fid-native","pick_code":"pc-native"}'::jsonb,
                        '{"resolution":"4K","video_codec":"hevc"}'::jsonb
                    ) RETURNING *
                    """
                ),
                {"sha1": sha1},
            )
            .mappings()
            .one()
        )
        file = (
            connection.execute(
                text(
                    """
                    INSERT INTO shared_rapid_source_files(
                        local_source_id, fid, pick_code, sha1, preid, size, file_name,
                        tmdb_id, item_type, raw_ffprobe_uploaded, rapid_meta_json,
                        media_signature_json
                    ) VALUES (
                        :source_id, 'fid-native', 'pc-native', :sha1, 'preid-native',
                        1024, 'Native Register.mkv', '901', 'Movie', FALSE,
                        '{"fid":"fid-native","pick_code":"pc-native"}'::jsonb,
                        '{"resolution":"4K","video_codec":"hevc"}'::jsonb
                    ) RETURNING *
                    """
                ),
                {"source_id": source["id"], "sha1": sha1},
            )
            .mappings()
            .one()
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_mediainfo_cache(sha1, raw_ffprobe_json)
                VALUES (:sha1, CAST(:raw AS jsonb))
                """
            ),
            {"sha1": sha1, "raw": json.dumps(raw)},
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache(
                    id, parent_id, name, local_path, sha1, pick_code, size, account_id, entry_kind
                ) VALUES (
                    'native-register-holder', 'native-register-parent', 'Native Register.mkv',
                    '/Movies/Native Register.mkv', :sha1, 'pc-native', 1024, '1', 'file'
                )
                """
            ),
            {"sha1": sha1},
        )
    state_hash = source_registration_state_hash(dict(source), [dict(file)])
    raw_seed = SharingOperationIntent.create(
        kind=SharingOperationKind.REGISTER_SOURCE,
        scope_key=f"fixture-source-raw:{source['id']}:{sha1}",
        principal_id="fixture-source-register",
        request={
            "provider": "shared_center",
            "action": "upload_raw_ffprobe",
            "payload": raw_payload,
            "readback": {
                "provider": "shared_center",
                "action": "get_raw_ffprobe",
                "query": {"sha1": sha1},
                "match_key": "sha1",
                "match_value": sha1,
            },
        },
        idempotency_key="fixture-source-register:raw",
    )
    dependent = SharingOperationIntent.create(
        kind=SharingOperationKind.REGISTER_SOURCE,
        scope_key=f"fixture-source-register:{source['id']}",
        principal_id="fixture-source-register",
        request={
            "provider": "shared_center",
            "action": "register_source",
            "payload": {"source_kind": "movie", "body": {"tmdb_id": "901", "sha1": sha1}},
            "blocked_by_operation_id": str(raw_seed.id),
            "local_continuation": {
                "type": "confirm_local_source_registration",
                "source_id": source["id"],
                "expected_source_state_hash": state_hash,
                "expected_center_source_id": "center-old",
                "source_kind": "movie",
                "local_sign_holder": {"account_id": "1", "pick_code": "pc-native", "sha1": sha1},
                "source_record": {"id": source["id"], "source_key": source["source_key"]},
                "file_record": {"id": file["id"], "sha1": sha1, "file_name": file["file_name"]},
            },
        },
        idempotency_key="fixture-source-register:register",
    )
    prerequisite = SharingOperationIntent.create(
        kind=SharingOperationKind.REGISTER_SOURCE,
        scope_key=f"fixture-source-raw:{source['id']}:{sha1}",
        principal_id="fixture-source-register",
        request={
            "provider": "shared_center",
            "action": "upload_raw_ffprobe",
            "payload": raw_payload,
            "readback": {
                "provider": "shared_center",
                "action": "get_raw_ffprobe",
                "query": {"sha1": sha1},
                "match_key": "sha1",
                "match_value": sha1,
            },
            "local_continuation": {
                "type": "release_dependent_operation",
                "operation_id": str(dependent.id),
            },
        },
        idempotency_key="fixture-source-register:raw",
    )
    assert prerequisite.id == raw_seed.id

    parent, raw_operation, replayed = repository.reserve_prerequisite_chain(
        dependent_intent=dependent,
        prerequisite_intent=prerequisite,
    )
    replay_parent, replay_raw, replayed_again = repository.reserve_prerequisite_chain(
        dependent_intent=dependent,
        prerequisite_intent=prerequisite,
    )

    assert replayed is False
    assert replayed_again is True
    assert replay_parent.id == parent.id
    assert replay_raw.id == raw_operation.id
    with p9_runtime.engine.connect() as connection:
        initial_outboxes = connection.execute(
            text("SELECT aggregate_id, status FROM outbox_events WHERE aggregate_id IN (:parent, :raw)"),
            {"parent": str(parent.id), "raw": str(raw_operation.id)},
        ).all()
    assert initial_outboxes == [(str(raw_operation.id), "pending")]

    raw_execution = SharingOperationExecutor(
        repository,
        _Delivery([SharingDeliveryOutcome.confirmed({"provider_result": {"sha1": sha1}})]),
    ).execute(raw_operation.id, worker_id="raw-worker", lease_seconds=60)
    assert raw_execution is not None
    assert raw_execution.operation.status.value == "confirmed"
    with p9_runtime.engine.connect() as connection:
        released_status = connection.scalar(
            text("SELECT status FROM outbox_events WHERE aggregate_id = :parent"),
            {"parent": str(parent.id)},
        )
    assert released_status == "pending"

    registration_execution = SharingOperationExecutor(
        repository,
        _Delivery([SharingDeliveryOutcome.confirmed({"provider_result": {"source_id": "center-native"}})]),
    ).execute(parent.id, worker_id="registration-worker", lease_seconds=60)
    assert registration_execution is not None
    assert registration_execution.operation.status.value == "confirmed"
    with p9_runtime.engine.connect() as connection:
        saved_source = connection.execute(
            text(
                """
                SELECT center_source_id, status, center_status, last_error,
                       raw_json->'native_registration'->>'operation_id'
                FROM shared_rapid_sources WHERE id = :id
                """
            ),
            {"id": source["id"]},
        ).one()
        saved_file = connection.execute(
            text(
                """
                SELECT raw_ffprobe_uploaded, raw_json->>'registration_operation_id'
                FROM shared_rapid_source_files WHERE id = :id
                """
            ),
            {"id": file["id"]},
        ).one()
        final_outboxes = dict(
            connection.execute(
                text("SELECT aggregate_id, status FROM outbox_events WHERE aggregate_id IN (:parent, :raw)"),
                {"parent": str(parent.id), "raw": str(raw_operation.id)},
            ).all()
        )
    assert saved_source == ("center-native", "active", "reported", None, str(parent.id))
    assert saved_file == (True, str(parent.id))
    assert final_outboxes == {str(parent.id): "processed", str(raw_operation.id): "processed"}

    with p9_runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM shared_rapid_source_files WHERE local_source_id = :id"), {"id": source["id"]}
        )
        connection.execute(text("DELETE FROM shared_rapid_sources WHERE id = :id"), {"id": source["id"]})
        connection.execute(text("DELETE FROM p115_mediainfo_cache WHERE sha1 = :sha1"), {"sha1": sha1})
        connection.execute(text("DELETE FROM p115_filesystem_cache WHERE id = 'native-register-holder'"))


def test_postgres_source_only_registration_persists_current_holder_pick_code(p9_runtime) -> None:
    sha1 = "B" * 40
    raw = {
        "format": {"size": "4096"},
        "streams": [{"codec_type": "video", "codec_name": "hevc", "width": 1920, "height": 1080}],
    }
    with p9_runtime.engine.begin() as connection:
        source_id = connection.scalar(
            text(
                """
                INSERT INTO shared_rapid_sources(
                    source_key, source_kind, tmdb_id, item_type, title, sha1, file_name,
                    source_provider, status, center_status, file_count, total_size,
                    rapid_meta_json, media_signature_json
                ) VALUES (
                    'source-only-registration', 'movie', '904', 'Movie', 'Source Only', :sha1,
                    'Source Only.mkv', 'auto_ingest', 'active', 'local', 1, 4096,
                    '{"fid":"source-only-fid"}'::jsonb,
                    '{"resolution":"1080p","video_codec":"hevc"}'::jsonb
                ) RETURNING id
                """
            ),
            {"sha1": sha1},
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_mediainfo_cache(sha1, raw_ffprobe_json)
                VALUES (:sha1, CAST(:raw AS jsonb))
                """
            ),
            {"sha1": sha1, "raw": json.dumps(raw)},
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache(
                    id, parent_id, name, local_path, sha1, pick_code, size, account_id, entry_kind
                ) VALUES (
                    'source-only-holder', 'source-only-parent', 'Source Only.mkv',
                    '/Movies/Source Only.mkv', :sha1, 'source-only-current-pc', 4096, '1', 'file'
                )
                """
            ),
            {"sha1": sha1},
        )
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    service = SharingService(
        InMemoryContributionLedger(),
        object(),
        repository,
        projection=SqlAlchemySharingProjectionRepository(p9_runtime.unit_of_work, account_id=lambda: "1"),
    )

    staged = service.reregister_local_source(
        int(source_id),
        principal_id="source-only-admin",
        idempotency_key="source-only-registration",
    )
    assert staged.status_code == 202
    parent_id = UUID(staged.payload["operation_id"])
    raw_id = UUID(staged.payload["prerequisite_operation_id"])
    with p9_runtime.engine.connect() as connection:
        provider = connection.scalar(
            text("SELECT request_json->'payload'->'body'->>'source_provider' FROM sharing_operations WHERE id = :id"),
            {"id": parent_id},
        )
    assert provider == "auto_ingest"

    class Center:
        def __init__(self) -> None:
            self.uploads: list[dict[str, Any]] = []

        def validate_configuration(self) -> None:
            return None

        def upload_raw_ffprobe(self, payload: dict[str, Any]) -> ProviderResponse[dict[str, Any]]:
            self.uploads.append(payload)
            return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"sha1": payload["sha1"]})

        def get_raw_ffprobe(self, value: str) -> ProviderResponse[dict[str, Any]]:
            return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"sha1": value})

    center = Center()
    media_info_repository = SqlAlchemyP115MediaInfoRepository(p9_runtime.unit_of_work)
    raw_execution = SharingOperationExecutor(
        repository,
        SharingProviderOperationDelivery(
            shared_center=cast(Any, center),
            hdhive=cast(Any, object()),
            raw_ffprobe_loader=partial(_load_sharing_raw_ffprobe, media_info_repository),
        ),
    ).execute(raw_id, worker_id="source-only-raw-worker", lease_seconds=60)
    assert raw_execution is not None and raw_execution.operation.status.value == "confirmed"
    assert center.uploads[0]["raw_ffprobe_json"] == raw
    assert "raw_reference" not in center.uploads[0]
    registered = SharingOperationExecutor(
        repository,
        _Delivery([SharingDeliveryOutcome.confirmed({"provider_result": {"source_id": "source-only-center"}})]),
    ).execute(parent_id, worker_id="source-only-register-worker", lease_seconds=60)
    assert registered is not None and registered.operation.status.value == "confirmed"

    with p9_runtime.engine.connect() as connection:
        saved = connection.execute(
            text(
                """
                SELECT pick_code, sha1, raw_ffprobe_uploaded
                FROM shared_rapid_source_files WHERE local_source_id = :source_id
                """
            ),
            {"source_id": source_id},
        ).one()
    assert saved == ("source-only-current-pc", sha1, True)

    with p9_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM shared_rapid_source_files WHERE local_source_id = :id"), {"id": source_id})
        connection.execute(text("DELETE FROM shared_rapid_sources WHERE id = :id"), {"id": source_id})
        connection.execute(text("DELETE FROM p115_mediainfo_cache WHERE sha1 = :sha1"), {"sha1": sha1})
        connection.execute(text("DELETE FROM p115_filesystem_cache WHERE id = 'source-only-holder'"))


@pytest.mark.parametrize("dependent_status", ["requesting", "awaiting_readback", "retryable"])
def test_postgres_confirmed_prerequisite_replay_does_not_restage_active_dependent(
    p9_runtime,
    dependent_status: str,
) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    sha1 = "C" * 40
    key = f"fixture-prerequisite-replay:{dependent_status}"
    raw_seed = SharingOperationIntent.create(
        kind=SharingOperationKind.REGISTER_SOURCE,
        scope_key=f"{key}:raw",
        principal_id="fixture-prerequisite-replay",
        request={
            "provider": "shared_center",
            "action": "upload_raw_ffprobe",
            "payload": {"sha1": sha1},
        },
        idempotency_key=f"{key}:raw",
    )
    dependent = SharingOperationIntent.create(
        kind=SharingOperationKind.REGISTER_SOURCE,
        scope_key=f"{key}:dependent",
        principal_id="fixture-prerequisite-replay",
        request={
            "provider": "shared_center",
            "action": "register_source",
            "payload": {"source_kind": "movie", "body": {"tmdb_id": "903", "sha1": sha1}},
            "blocked_by_operation_id": str(raw_seed.id),
        },
        idempotency_key=f"{key}:dependent",
    )
    prerequisite = SharingOperationIntent.create(
        kind=SharingOperationKind.REGISTER_SOURCE,
        scope_key=f"{key}:raw",
        principal_id="fixture-prerequisite-replay",
        request={
            "provider": "shared_center",
            "action": "upload_raw_ffprobe",
            "payload": {"sha1": sha1},
            "local_continuation": {
                "type": "release_dependent_operation",
                "operation_id": str(dependent.id),
            },
        },
        idempotency_key=f"{key}:raw",
    )
    parent, raw_operation, replayed = repository.reserve_prerequisite_chain(
        dependent_intent=dependent,
        prerequisite_intent=prerequisite,
    )
    assert replayed is False
    delivery = _Delivery([SharingDeliveryOutcome.confirmed({"provider_result": {"sha1": sha1}})])
    raw_execution = SharingOperationExecutor(repository, delivery).execute(
        raw_operation.id,
        worker_id=f"{dependent_status}-raw-worker",
        lease_seconds=60,
    )
    assert raw_execution is not None and raw_execution.operation.status.value == "confirmed"
    assert delivery.request_calls == 1

    claimed = repository.claim_by_id(parent.id, worker_id=f"{dependent_status}-worker", lease_seconds=60)
    assert claimed is not None and claimed.claim_token is not None
    if dependent_status == "requesting":
        before = repository.mark_requesting(parent.id, claimed.claim_token)
    elif dependent_status == "awaiting_readback":
        repository.mark_requesting(parent.id, claimed.claim_token)
        before = repository.mark_awaiting_readback(
            parent.id,
            claimed.claim_token,
            error={"type": "fixture_unknown"},
            retry_after_seconds=0,
        )
    else:
        before = repository.mark_retryable(
            parent.id,
            claimed.claim_token,
            error={"type": "fixture_prewrite"},
            retry_after_seconds=0,
        )
    with p9_runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE outbox_events SET status = 'dead' WHERE aggregate_id = :aggregate_id"),
            {"aggregate_id": str(parent.id)},
        )

    replay_parent, replay_raw, replayed_again = repository.reserve_prerequisite_chain(
        dependent_intent=dependent,
        prerequisite_intent=prerequisite,
    )

    assert replayed_again is True
    assert replay_parent.id == parent.id
    assert replay_raw.id == raw_operation.id
    after = repository.read(parent.id)
    assert after is not None
    assert after.status.value == dependent_status
    assert after.state_revision == before.state_revision
    with p9_runtime.engine.connect() as connection:
        outboxes = (
            connection.execute(
                text("SELECT status FROM outbox_events WHERE aggregate_id = :aggregate_id"),
                {"aggregate_id": str(parent.id)},
            )
            .scalars()
            .all()
        )
    assert outboxes == ["dead"]
    assert delivery.request_calls == 1


@pytest.mark.parametrize(
    "drift_kind",
    [
        "source",
        "file",
        "holder_deleted",
        "holder_changed",
        "between_validation_and_checkpoint",
        "holder_after_checkpoint",
    ],
)
def test_postgres_source_registration_confirmation_fails_closed_on_local_identity_drift(
    p9_runtime,
    drift_kind: str,
) -> None:
    sha1 = "E" * 40
    raw = {
        "format": {"size": "2048", "bit_rate": "8000000"},
        "streams": [{"codec_type": "video", "codec_name": "hevc", "width": 1920, "height": 1080}],
    }
    with p9_runtime.engine.begin() as connection:
        source_id = connection.scalar(
            text(
                """
                INSERT INTO shared_rapid_sources(
                    source_key, source_kind, center_source_id, tmdb_id, item_type,
                    title, sha1, file_name, source_provider, status, center_status,
                    file_count, total_size, rapid_meta_json, media_signature_json
                ) VALUES (
                    'native-register-drift', 'movie', 'center-drift-old', '902', 'Movie',
                    'Before Drift', :sha1, 'Before Drift.mkv', 'manual_rapid',
                    'active', 'reported', 1, 2048,
                    '{"fid":"fid-drift","pick_code":"pc-drift"}'::jsonb,
                    '{"resolution":"1080p","video_codec":"hevc"}'::jsonb
                ) RETURNING id
                """
            ),
            {"sha1": sha1},
        )
        connection.execute(
            text(
                """
                INSERT INTO shared_rapid_source_files(
                    local_source_id, fid, pick_code, sha1, preid, size, file_name,
                    tmdb_id, item_type, rapid_meta_json, media_signature_json
                ) VALUES (
                    :source_id, 'fid-drift', 'pc-drift', :sha1, 'preid-drift',
                    2048, 'Before Drift.mkv', '902', 'Movie',
                    '{"fid":"fid-drift","pick_code":"pc-drift"}'::jsonb,
                    '{"resolution":"1080p","video_codec":"hevc"}'::jsonb
                )
                """
            ),
            {"source_id": source_id, "sha1": sha1},
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_mediainfo_cache(sha1, raw_ffprobe_json)
                VALUES (:sha1, CAST(:raw AS jsonb))
                """
            ),
            {"sha1": sha1, "raw": json.dumps(raw)},
        )
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache(
                    id, parent_id, name, local_path, sha1, pick_code, size, account_id, entry_kind
                ) VALUES (
                    'drift-file', 'drift-parent', 'Before Drift.mkv', '/Movies/Before Drift.mkv',
                    :sha1, 'pc-drift', 2048, '1', 'file'
                )
                """
            ),
            {"sha1": sha1},
        )
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    service = SharingService(
        InMemoryContributionLedger(),
        object(),
        repository,
        projection=SqlAlchemySharingProjectionRepository(p9_runtime.unit_of_work, account_id=lambda: "1"),
    )

    staged = service.reregister_local_source(
        int(source_id),
        principal_id="drift-admin",
        idempotency_key=f"drift-registration:{drift_kind}",
    )
    assert staged.status_code == 202
    parent_id = UUID(staged.payload["operation_id"])
    raw_id = UUID(staged.payload["prerequisite_operation_id"])
    with p9_runtime.engine.connect() as connection:
        durable_raw_payload = connection.execute(
            text(
                """
                SELECT request_json->'payload'->'raw_reference',
                       request_json->'payload' ? 'raw_ffprobe_json'
                FROM sharing_operations WHERE id = :id
                """
            ),
            {"id": raw_id},
        ).one()
    assert durable_raw_payload[0]["sha1"] == sha1
    assert len(durable_raw_payload[0]["sha256"]) == 64
    assert durable_raw_payload[0]["size_bytes"] > 0
    assert durable_raw_payload[1] is False
    raw_execution = SharingOperationExecutor(
        repository,
        _Delivery([SharingDeliveryOutcome.confirmed({"provider_result": {"sha1": sha1}})]),
    ).execute(raw_id, worker_id="drift-raw-worker", lease_seconds=60)
    assert raw_execution is not None and raw_execution.operation.status.value == "confirmed"

    if drift_kind == "between_validation_and_checkpoint":

        class DriftBeforeCheckpointDelivery(_Delivery):
            def __init__(self) -> None:
                super().__init__([SharingDeliveryOutcome.confirmed({"provider_result": {"source_id": "unused"}})])
                self.provider_called = False

            def request(
                self,
                operation: SharingOperation,
                *,
                checkpoint: Callable[[], None],
            ) -> SharingDeliveryOutcome:
                del operation
                self.request_calls += 1
                with p9_runtime.engine.begin() as connection:
                    connection.execute(text("DELETE FROM p115_filesystem_cache WHERE id = 'drift-file'"))
                checkpoint()
                self.provider_called = True
                return self.requests.pop(0)

        delivery = DriftBeforeCheckpointDelivery()
        execution = SharingOperationExecutor(repository, delivery).execute(
            parent_id,
            worker_id="drift-fenced-worker",
            lease_seconds=60,
        )
        assert execution is not None
        assert execution.operation.status.value == "rejected"
        assert execution.operation.step_started_at is None
        assert delivery.request_calls == 1
        assert delivery.provider_called is False
    elif drift_kind == "holder_after_checkpoint":
        delivery = _Delivery(
            [SharingDeliveryOutcome.retryable({"type": "fixture_transport_unknown"})],
            readbacks=[SharingDeliveryOutcome.confirmed({"provider_result": {"source_id": "center-drift-new"}})],
        )
        executor = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0)
        awaiting = executor.execute(parent_id, worker_id="drift-registration-worker", lease_seconds=60)
        assert awaiting is not None and awaiting.operation.status.value == "awaiting_readback"
        with p9_runtime.engine.begin() as connection:
            connection.execute(text("DELETE FROM p115_filesystem_cache WHERE id = 'drift-file'"))
        with pytest.raises(SharingOperationStateError, match="sign-holder changed"):
            executor.execute(parent_id, worker_id="drift-readback-worker", lease_seconds=60)
        current = repository.read(parent_id)
        assert current is not None
        assert current.status.value == "claimed"
        assert current.step_started_at is not None
        assert delivery.request_calls == 1
        assert delivery.readback_calls == 1
        with p9_runtime.engine.connect() as connection:
            saved = connection.execute(
                text("SELECT center_source_id FROM shared_rapid_sources WHERE id = :id"),
                {"id": source_id},
            ).scalar_one()
        assert saved == "center-drift-old"
    else:
        with p9_runtime.engine.begin() as connection:
            if drift_kind == "source":
                connection.execute(
                    text("UPDATE shared_rapid_sources SET title = 'After Drift', updated_at = NOW() WHERE id = :id"),
                    {"id": source_id},
                )
            elif drift_kind == "file":
                connection.execute(
                    text(
                        "UPDATE shared_rapid_source_files SET file_name = 'After Drift.mkv', updated_at = NOW() "
                        "WHERE local_source_id = :id"
                    ),
                    {"id": source_id},
                )
            elif drift_kind == "holder_deleted":
                connection.execute(text("DELETE FROM p115_filesystem_cache WHERE id = 'drift-file'"))
            else:
                connection.execute(
                    text("UPDATE p115_filesystem_cache SET pick_code = 'pc-drift-changed' WHERE id = 'drift-file'")
                )

        delivery = _Delivery([SharingDeliveryOutcome.confirmed({"provider_result": {"source_id": "center-drift-new"}})])
        execution = SharingOperationExecutor(repository, delivery).execute(
            parent_id,
            worker_id="drift-registration-worker",
            lease_seconds=60,
        )

        assert execution is not None
        assert execution.operation.status.value == "rejected"
        assert execution.operation.step_started_at is None
        assert delivery.request_calls == 0
        with p9_runtime.engine.connect() as connection:
            saved = connection.execute(
                text("SELECT title, center_source_id FROM shared_rapid_sources WHERE id = :id"),
                {"id": source_id},
            ).one()
        assert saved[1] == "center-drift-old"

    with p9_runtime.engine.begin() as connection:
        connection.execute(text("DELETE FROM shared_rapid_source_files WHERE local_source_id = :id"), {"id": source_id})
        connection.execute(text("DELETE FROM shared_rapid_sources WHERE id = :id"), {"id": source_id})
        connection.execute(text("DELETE FROM p115_mediainfo_cache WHERE sha1 = :sha1"), {"sha1": sha1})
        connection.execute(text("DELETE FROM p115_filesystem_cache WHERE id = 'drift-file'"))


def test_postgres_compensation_without_matching_debit_fails_closed(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    ledger = PostgresContributionLedger(p9_runtime.unit_of_work)
    ledger.ensure_account("device-missing-debit", 0)
    operation = repository.reserve(
        SharingOperationIntent.create(
            kind=SharingOperationKind.TRANSFER,
            scope_key="transfer:missing-debit",
            principal_id="principal-missing-debit",
            request={
                "provider": "shared_center",
                "action": "acquire_transfer_lease",
                "payload": {"source_id": "source-missing-debit"},
                "ledger_compensation": {
                    "type": "release_transfer_escrow",
                    "account_id": "device-missing-debit",
                    "amount": 2,
                    "debit_idempotency_key": "transfer:missing-debit",
                    "reference": "source-missing-debit",
                },
            },
            idempotency_key="transfer-missing-debit",
        )
    )
    claimed = repository.claim_by_id(operation.id, worker_id="transfer-worker", lease_seconds=60)
    assert claimed is not None and claimed.claim_token is not None
    requesting = repository.mark_requesting(operation.id, claimed.claim_token)

    with pytest.raises(SharingOperationStateError, match="matching transfer escrow debit"):
        repository.mark_rejected(operation.id, claimed.claim_token, error={"type": "provider_rejected"})

    rolled_back = repository.read(operation.id)
    assert rolled_back is not None
    assert rolled_back.status.value == "requesting"
    assert rolled_back.state_revision == requesting.state_revision
    assert ledger.balance("device-missing-debit") == 0


def test_postgres_prewrite_cancellation_compensates_once_and_fences_provider(p9_runtime) -> None:
    ledger = PostgresContributionLedger(p9_runtime.unit_of_work)
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    service = SharingLedgerService(ledger, repository, unit_of_work=p9_runtime.unit_of_work)
    ledger.ensure_account("device-cancel-prewrite", 5)
    reserved = service.reserve_transfer(
        account_id="device-cancel-prewrite",
        source_id="source-cancel-prewrite",
        amount=2,
        principal_id="principal-cancel-prewrite",
        idempotency_key="transfer-cancel-prewrite",
    )
    operation_id = UUID(reserved.payload["operation_id"])
    delivery = _CancelBeforeCheckpointDelivery(repository)

    execution = SharingOperationExecutor(repository, delivery).execute(
        operation_id,
        worker_id="cancel-prewrite-worker",
        lease_seconds=60,
    )

    assert execution is not None
    assert execution.operation.status.value == "cancelled"
    assert execution.operation.cancel_requested_at is not None
    assert delivery.provider_called is False
    assert ledger.balance("device-cancel-prewrite") == 5
    replay = repository.request_cancel(operation_id)
    assert replay is not None and replay.status.value == "cancelled"
    assert ledger.balance("device-cancel-prewrite") == 5
    assert [entry.delta for entry in ledger.entries("device-cancel-prewrite", limit=10)] == [2, -2]
    with p9_runtime.engine.connect() as connection:
        outbox = connection.execute(
            text("SELECT status, processed_at FROM outbox_events WHERE aggregate_id = :operation_id"),
            {"operation_id": str(operation_id)},
        ).one()
    assert outbox[0] == "processed"
    assert outbox[1] is not None


def test_postgres_post_checkpoint_cancel_uses_readback_without_resend_or_compensation(p9_runtime) -> None:
    ledger = PostgresContributionLedger(p9_runtime.unit_of_work)
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    service = SharingLedgerService(ledger, repository, unit_of_work=p9_runtime.unit_of_work)
    ledger.ensure_account("device-cancel-readback", 5)
    reserved = service.reserve_transfer(
        account_id="device-cancel-readback",
        source_id="source-cancel-readback",
        amount=2,
        principal_id="principal-cancel-readback",
        idempotency_key="transfer-cancel-readback",
    )
    operation_id = UUID(reserved.payload["operation_id"])
    delivery = _CancelAfterCheckpointDelivery(repository)
    executor = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0)

    awaiting = executor.execute(operation_id, worker_id="cancel-readback-worker", lease_seconds=60)
    assert awaiting is not None
    assert awaiting.operation.status.value == "awaiting_readback"
    assert awaiting.operation.cancel_requested_at is not None
    assert ledger.balance("device-cancel-readback") == 3

    confirmed = executor.execute(operation_id, worker_id="cancel-readback-recovery", lease_seconds=60)
    assert confirmed is not None
    assert confirmed.operation.status.value == "confirmed"
    assert confirmed.operation.cancel_requested_at is not None
    assert delivery.request_calls == 1
    assert delivery.readback_calls == 1
    assert ledger.balance("device-cancel-readback") == 3


def test_postgres_post_checkpoint_cancel_indeterminate_retains_escrow(p9_runtime) -> None:
    ledger = PostgresContributionLedger(p9_runtime.unit_of_work)
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    service = SharingLedgerService(ledger, repository, unit_of_work=p9_runtime.unit_of_work)
    ledger.ensure_account("device-cancel-indeterminate", 5)
    reserved = service.reserve_transfer(
        account_id="device-cancel-indeterminate",
        source_id="source-cancel-indeterminate",
        amount=2,
        principal_id="principal-cancel-indeterminate",
        idempotency_key="transfer-cancel-indeterminate",
    )
    operation_id = UUID(reserved.payload["operation_id"])

    class _CancelThenUnknown(_Delivery):
        def request(
            self,
            operation: SharingOperation,
            *,
            checkpoint: Callable[[], None],
        ) -> SharingDeliveryOutcome:
            self.request_calls += 1
            checkpoint()
            requested = repository.request_cancel(operation.id)
            assert requested is not None and requested.cancel_requested_at is not None
            return self.requests.pop(0)

    delivery = _CancelThenUnknown(
        [SharingDeliveryOutcome.indeterminate({"type": "transport_unknown"})],
        supports_readback=False,
    )
    execution = SharingOperationExecutor(repository, delivery).execute(
        operation_id,
        worker_id="cancel-indeterminate-worker",
        lease_seconds=60,
    )

    assert execution is not None
    assert execution.operation.status.value == "indeterminate"
    assert execution.operation.cancel_requested_at is not None
    assert ledger.balance("device-cancel-indeterminate") == 3
    terminal_cancel = repository.request_cancel(operation_id)
    assert terminal_cancel is not None and terminal_cancel.status.value == "indeterminate"
    assert ledger.balance("device-cancel-indeterminate") == 3


def test_postgres_rejected_transfer_compensates_once_but_indeterminate_retains_escrow(p9_runtime) -> None:
    ledger = PostgresContributionLedger(p9_runtime.unit_of_work)
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    service = SharingLedgerService(ledger, repository, unit_of_work=p9_runtime.unit_of_work)
    ledger.ensure_account("device-compensation", 10)

    rejected = service.reserve_transfer(
        account_id="device-compensation",
        source_id="source-rejected",
        amount=3,
        principal_id="principal-compensation",
        idempotency_key="transfer-rejected",
    )
    rejected_id = UUID(rejected.payload["operation_id"])
    rejected_execution = SharingOperationExecutor(
        repository,
        _Delivery([SharingDeliveryOutcome.rejected({"type": "provider_rejected"})]),
    ).execute(rejected_id, worker_id="transfer-worker", lease_seconds=60)

    assert rejected_execution is not None
    assert rejected_execution.operation.status.value == "rejected"
    assert ledger.balance("device-compensation") == 10
    replay = SharingOperationExecutor(repository, _Delivery([])).execute(
        rejected_id,
        worker_id="replacement-worker",
        lease_seconds=60,
    )
    assert replay is not None and replay.claimed is False
    assert ledger.balance("device-compensation") == 10

    indeterminate = service.reserve_transfer(
        account_id="device-compensation",
        source_id="source-indeterminate",
        amount=4,
        principal_id="principal-compensation",
        idempotency_key="transfer-indeterminate",
    )
    indeterminate_id = UUID(indeterminate.payload["operation_id"])
    indeterminate_execution = SharingOperationExecutor(
        repository,
        _Delivery([SharingDeliveryOutcome.indeterminate({"type": "transport_unknown"})], supports_readback=False),
    ).execute(indeterminate_id, worker_id="transfer-worker-2", lease_seconds=60)

    assert indeterminate_execution is not None
    assert indeterminate_execution.operation.status.value == "indeterminate"
    assert ledger.balance("device-compensation") == 6
    entries = ledger.entries("device-compensation", limit=20)
    assert [entry.delta for entry in entries] == [-4, 3, -3]


def test_postgres_dead_prewrite_transfer_dispatch_compensates_exactly_once(p9_runtime) -> None:
    ledger = PostgresContributionLedger(p9_runtime.unit_of_work)
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    service = SharingLedgerService(ledger, repository, unit_of_work=p9_runtime.unit_of_work)
    ledger.ensure_account("device-dead-dispatch", 5)
    reserved = service.reserve_transfer(
        account_id="device-dead-dispatch",
        source_id="source-dead-dispatch",
        amount=2,
        principal_id="principal-dead-dispatch",
        idempotency_key="transfer-dead-dispatch",
    )
    operation_id = UUID(reserved.payload["operation_id"])
    with p9_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE outbox_events
                SET status = 'dead', last_error = 'publish_exhausted'
                WHERE aggregate_id = :operation_id
                """
            ),
            {"operation_id": str(operation_id)},
        )

    assert ledger.balance("device-dead-dispatch") == 3
    assert repository.reject_dead_dispatches() == 1
    assert repository.reject_dead_dispatches() == 0
    assert ledger.balance("device-dead-dispatch") == 5
    operation = repository.read(operation_id)
    assert operation is not None
    assert operation.status.value == "rejected"
    assert [entry.delta for entry in ledger.entries("device-dead-dispatch", limit=10)] == [2, -2]


def test_postgres_exhausted_prewrite_transfer_claim_compensates_escrow(p9_runtime) -> None:
    ledger = PostgresContributionLedger(p9_runtime.unit_of_work)
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    service = SharingLedgerService(ledger, repository, unit_of_work=p9_runtime.unit_of_work)
    ledger.ensure_account("device-expired-claim", 5)
    reserved = service.reserve_transfer(
        account_id="device-expired-claim",
        source_id="source-expired-claim",
        amount=2,
        principal_id="principal-expired-claim",
        idempotency_key="transfer-expired-claim",
    )
    operation_id = UUID(reserved.payload["operation_id"])
    claim_time = datetime.now(UTC)
    first = repository.claim_by_id(
        operation_id,
        worker_id="crashing-transfer-worker-1",
        lease_seconds=1,
        now=claim_time,
    )
    assert first is not None and first.lease_expires_at is not None
    first_expiry = first.lease_expires_at + timedelta(seconds=1)
    assert repository.expire_stale_claims(now=first_expiry, max_attempts=2) == 1
    second = repository.claim_by_id(
        operation_id,
        worker_id="crashing-transfer-worker-2",
        lease_seconds=1,
        now=first_expiry,
    )
    assert second is not None and second.lease_expires_at is not None
    second_expiry = second.lease_expires_at + timedelta(seconds=1)

    assert repository.expire_stale_claims(now=second_expiry, max_attempts=2) == 1
    assert ledger.balance("device-expired-claim") == 5
    terminal = repository.read(operation_id)
    assert terminal is not None
    assert terminal.status.value == "rejected"
    assert [entry.delta for entry in ledger.entries("device-expired-claim", limit=10)] == [2, -2]


def test_postgres_transfer_reservation_rolls_back_when_ledger_rejects(p9_runtime) -> None:
    ledger = PostgresContributionLedger(p9_runtime.unit_of_work)
    operations = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    service = SharingLedgerService(
        ledger,
        operations,
        unit_of_work=p9_runtime.unit_of_work,
    )

    with pytest.raises(InsufficientContributionError):
        service.reserve_transfer(
            account_id="device-no-credit",
            source_id="source-no-credit",
            amount=1,
            principal_id="principal-no-credit",
            idempotency_key="transfer-no-credit",
        )

    with p9_runtime.engine.connect() as connection:
        counts = connection.execute(
            text(
                """
                SELECT
                    (SELECT COUNT(*) FROM sharing_operations
                     WHERE scope_key = 'account:device-no-credit:source:source-no-credit'),
                    (SELECT COUNT(*) FROM sharing_credit_ledger
                     WHERE account_id = 'device-no-credit'),
                    (SELECT COUNT(*) FROM outbox_events
                     WHERE aggregate_type = 'sharing-operation'
                       AND aggregate_id IN (
                           SELECT id::text FROM sharing_operations
                           WHERE scope_key = 'account:device-no-credit:source:source-no-credit'
                       ))
                """
            )
        ).one()
    assert tuple(counts) == (0, 0, 0)


def test_postgres_sharing_operation_stages_identifier_only_outbox(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = repository.reserve(
        SharingOperationIntent.create(
            kind=SharingOperationKind.TRANSFER,
            scope_key="source:source-1",
            principal_id="device-1",
            request={"source_id": "source-1", "amount": 5},
            idempotency_key="transfer-1",
        )
    )
    replay = repository.reserve(
        SharingOperationIntent.create(
            kind=SharingOperationKind.TRANSFER,
            scope_key="source:source-1",
            principal_id="device-1",
            request={"source_id": "source-1", "amount": 5},
            idempotency_key="transfer-1",
        )
    )
    assert replay == operation
    with p9_runtime.engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT payload_json
                FROM outbox_events
                WHERE aggregate_id = :operation_id
                """
            ),
            {"operation_id": str(operation.id)},
        ).scalar_one()
    assert set(row) == {"operation_id", "operation_key", "request_hash", "sequence"}


def test_postgres_sharing_operation_outbox_claim_retry_expiry_and_terminal_lifecycle(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = repository.reserve(
        SharingOperationIntent.create(
            kind=SharingOperationKind.TRANSFER,
            scope_key="source:lifecycle",
            principal_id="device-lifecycle",
            request={"source_id": "lifecycle", "amount": 1},
            idempotency_key="lifecycle-1",
        )
    )

    def outbox_status() -> tuple[str, object]:
        with p9_runtime.engine.connect() as connection:
            return connection.execute(
                text("SELECT status, processed_at FROM outbox_events WHERE aggregate_id = :operation_id"),
                {"operation_id": str(operation.id)},
            ).one()

    status, processed_at = outbox_status()
    assert status == "pending"
    assert processed_at is None

    claimed = repository.claim_by_id(operation.id, worker_id="worker-lifecycle", lease_seconds=60)
    assert claimed is not None
    assert claimed.claim_token is not None
    status, processed_at = outbox_status()
    assert status == "processed"
    assert processed_at is not None

    retried = repository.mark_retryable(
        operation.id,
        claimed.claim_token,
        error={"type": "temporary"},
        retry_after_seconds=0,
    )
    assert retried.status.value == "retryable"
    status, processed_at = outbox_status()
    assert status == "pending"
    assert processed_at is None

    reclaimed = repository.claim_by_id(operation.id, worker_id="worker-lifecycle-2", lease_seconds=1)
    assert reclaimed is not None
    assert reclaimed.claim_token is not None
    expired_at = (reclaimed.lease_expires_at or datetime.now(UTC)) + timedelta(seconds=1)
    assert repository.expire_stale_claims(now=expired_at) == 1
    expired = repository.read(operation.id)
    assert expired is not None
    assert expired.error == {
        "type": "sharing_claim_expired",
        "provider_may_have_been_called": False,
        "attempts": 2,
    }
    status, processed_at = outbox_status()
    assert status == "pending"
    assert processed_at is None

    terminal = repository.claim_by_id(operation.id, worker_id="worker-lifecycle-3", lease_seconds=60, now=expired_at)
    assert terminal is not None
    assert terminal.claim_token is not None
    repository.mark_indeterminate(
        operation.id,
        terminal.claim_token,
        error={"type": "provider_unknown"},
    )
    status, processed_at = outbox_status()
    assert status == "processed"
    assert processed_at is not None


def test_postgres_stale_requesting_sharing_operation_is_not_replayed(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    claim_time = datetime.now(UTC) - timedelta(seconds=5)
    operation = _sharing_operation(
        repository,
        "requesting-expiry",
        available_at=claim_time,
    )
    claimed = repository.claim_by_id(
        operation.id,
        worker_id="worker-requesting-expiry",
        lease_seconds=1,
        now=claim_time,
    )
    assert claimed is not None
    assert claimed.claim_token is not None
    repository.mark_requesting(operation.id, claimed.claim_token, now=claim_time)

    expired_at = datetime.now(UTC)
    assert repository.expire_stale_claims(now=expired_at) == 0
    assert repository.requeue_stale_readback_claims(now=expired_at) == 1

    delivery = _Delivery([], supports_readback=False)
    execution = SharingOperationExecutor(repository, delivery).execute(
        operation.id,
        worker_id="replacement-worker",
        lease_seconds=60,
    )
    assert execution is not None
    assert execution.operation.status.value == "indeterminate"
    assert execution.operation.error == {
        "type": "authoritative_readback_unavailable",
        "provider_may_have_been_called": True,
    }
    assert delivery.request_calls == 0
    assert delivery.readback_calls == 0
    with p9_runtime.engine.connect() as connection:
        outbox = connection.execute(
            text(
                """
                SELECT status, processed_at
                FROM outbox_events
                WHERE aggregate_id = :operation_id
                """
            ),
            {"operation_id": str(operation.id)},
        ).one()
    assert outbox[0] == "processed"
    assert outbox[1] is not None


def test_postgres_sharing_executor_confirms_only_after_provider_checkpoint(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "confirmed")
    delivery = _Delivery([SharingDeliveryOutcome.confirmed({"receipt_id": "receipt-1"})])

    execution = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0).execute(
        operation.id,
        worker_id="sharing-worker",
        lease_seconds=60,
    )

    assert execution is not None
    assert execution.operation.status.value == "confirmed"
    assert execution.operation.result == {"receipt_id": "receipt-1"}
    assert delivery.request_calls == 1
    assert delivery.readback_calls == 0


def test_postgres_sharing_executor_recovers_checkpointed_unknown_without_resend(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "readback")
    delivery = _Delivery(
        [SharingDeliveryOutcome.indeterminate({"type": "transport_unknown"})],
        readbacks=[SharingDeliveryOutcome.confirmed({"receipt_id": "receipt-recovered"})],
    )
    executor = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0)

    first = executor.execute(operation.id, worker_id="sharing-worker-1", lease_seconds=60)
    second = executor.execute(operation.id, worker_id="sharing-worker-2", lease_seconds=60)

    assert first is not None
    assert first.operation.status.value == "awaiting_readback"
    assert second is not None
    assert second.operation.status.value == "confirmed"
    assert second.operation.result == {"receipt_id": "receipt-recovered"}
    assert delivery.request_calls == 1
    assert delivery.readback_calls == 1


def test_postgres_sharing_executor_fences_unknown_write_without_readback(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "no-readback")
    delivery = _Delivery(
        [SharingDeliveryOutcome.indeterminate({"type": "unlock_unknown"})],
        supports_readback=False,
    )

    execution = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0).execute(
        operation.id,
        worker_id="sharing-worker",
        lease_seconds=60,
    )

    assert execution is not None
    assert execution.operation.status.value == "indeterminate"
    assert execution.operation.error == {
        "type": "unlock_unknown",
        "authoritative_readback": False,
        "provider_may_have_been_called": True,
    }
    assert delivery.request_calls == 1
    assert repository.claim_by_id(operation.id, worker_id="replacement", lease_seconds=60) is None


def test_postgres_sharing_executor_rearms_only_a_proven_prewrite_failure(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "prewrite")
    delivery = _Delivery(
        [SharingDeliveryOutcome.retryable({"type": "preflight_unavailable"})],
        checkpoint=False,
    )

    execution = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0).execute(
        operation.id,
        worker_id="sharing-worker",
        lease_seconds=60,
    )

    assert execution is not None
    assert execution.operation.status.value == "retryable"
    assert execution.operation.step_started_at is None
    with p9_runtime.engine.connect() as connection:
        status = connection.execute(
            text("SELECT status FROM outbox_events WHERE aggregate_id = :operation_id"),
            {"operation_id": str(operation.id)},
        ).scalar_one()
    assert status == "pending"


def test_postgres_sharing_executor_rejects_confirmation_without_checkpoint(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "missing-checkpoint")
    delivery = _Delivery([SharingDeliveryOutcome.confirmed()], checkpoint=False)

    execution = SharingOperationExecutor(repository, delivery).execute(
        operation.id,
        worker_id="sharing-worker",
        lease_seconds=60,
    )

    assert execution is not None
    assert execution.operation.status.value == "indeterminate"
    assert execution.operation.error == {
        "type": "provider_checkpoint_missing",
        "provider_may_have_been_called": True,
    }


def test_postgres_sharing_executor_recovers_expired_readback_capable_request(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    claim_time = datetime.now(UTC) - timedelta(seconds=5)
    operation = _sharing_operation(repository, "expired-readback", available_at=claim_time)
    claimed = repository.claim_by_id(
        operation.id,
        worker_id="stale-worker",
        lease_seconds=1,
        now=claim_time,
    )
    assert claimed is not None
    assert claimed.claim_token is not None
    requesting = repository.mark_requesting(operation.id, claimed.claim_token, now=claim_time)
    expired_at = datetime.now(UTC)

    assert (
        repository.requeue_stale_readback_claims(
            now=expired_at,
        )
        == 1
    )
    recovered = repository.read(operation.id)
    assert recovered is not None
    assert recovered.status.value == "awaiting_readback"
    assert recovered.step_started_at == requesting.step_started_at
    assert recovered.error == {
        "type": "sharing_request_claim_expired",
        "provider_may_have_been_called": True,
        "authoritative_readback_required": True,
        "attempts": 1,
    }

    delivery = _Delivery(
        [],
        readbacks=[SharingDeliveryOutcome.confirmed({"receipt_id": "after-crash"})],
    )
    execution = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0).execute(
        operation.id,
        worker_id="replacement-worker",
        lease_seconds=60,
    )

    assert execution is not None
    assert execution.operation.status.value == "confirmed"
    assert execution.operation.result == {"receipt_id": "after-crash"}
    assert delivery.request_calls == 0
    assert delivery.readback_calls == 1


def test_postgres_sharing_executor_malformed_checkpointed_outcome_fails_to_readback(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "malformed-outcome")
    delivery = _MalformedDelivery([])

    execution = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0).execute(
        operation.id,
        worker_id="sharing-worker",
        lease_seconds=60,
    )

    assert execution is not None
    assert execution.operation.status.value == "awaiting_readback"
    assert execution.operation.step_started_at is not None
    assert execution.operation.error == {
        "type": "invalid_checkpointed_delivery",
        "error_class": "TypeError",
    }


def test_postgres_sharing_executor_recovers_when_checkpoint_commit_ack_is_lost(
    p9_runtime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "checkpoint-ack-lost")
    delivery = _Delivery(
        [SharingDeliveryOutcome.confirmed({"receipt_id": "unused"})],
        readbacks=[SharingDeliveryOutcome.confirmed({"receipt_id": "readback-confirmed"})],
    )
    original_mark_requesting = repository.mark_requesting

    def commit_then_lose_ack(
        operation_id: UUID,
        claim_token: UUID,
        *,
        now: datetime | None = None,
    ) -> SharingOperation:
        original_mark_requesting(operation_id, claim_token, now=now)
        raise ConnectionError("checkpoint commit acknowledgement was lost")

    monkeypatch.setattr(repository, "mark_requesting", commit_then_lose_ack)
    executor = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0)

    first = executor.execute(operation.id, worker_id="request-worker", lease_seconds=60)
    second = executor.execute(operation.id, worker_id="readback-worker", lease_seconds=60)

    assert first is not None
    assert first.operation.status.value == "awaiting_readback"
    assert first.operation.step_started_at is not None
    assert second is not None
    assert second.operation.status.value == "confirmed"
    assert second.operation.result == {"receipt_id": "readback-confirmed"}
    assert delivery.request_calls == 1
    assert delivery.readback_calls == 1


def test_postgres_sharing_executor_duplicate_workers_call_provider_once(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "concurrent")
    delivery = _BlockingDelivery()
    executor = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first_future = pool.submit(
            executor.execute,
            operation.id,
            worker_id="sharing-worker-1",
            lease_seconds=60,
        )
        assert delivery.entered.wait(5)
        second = pool.submit(
            executor.execute,
            operation.id,
            worker_id="sharing-worker-2",
            lease_seconds=60,
        ).result(timeout=5)
        delivery.release.set()
        first = first_future.result(timeout=5)

    assert first is not None
    assert first.claimed is True
    assert first.operation.status.value == "confirmed"
    assert second is not None
    assert second.claimed is False
    assert second.operation.status.value == "requesting"
    assert delivery.request_calls == 1
    with p9_runtime.engine.connect() as connection:
        outbox = connection.execute(
            text("SELECT status, processed_at FROM outbox_events WHERE aggregate_id = :operation_id"),
            {"operation_id": str(operation.id)},
        ).one()
    assert outbox[0] == "processed"
    assert outbox[1] is not None


def test_postgres_stale_readback_claim_never_rearms_provider_request(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "stale-readback-claim")
    delivery = _Delivery(
        [SharingDeliveryOutcome.indeterminate({"type": "write_unknown"})],
        readbacks=[SharingDeliveryOutcome.retryable({"type": "readback_unavailable"})],
    )
    executor = SharingOperationExecutor(repository, delivery, retry_delay_seconds=0)
    first = executor.execute(operation.id, worker_id="request-worker", lease_seconds=60)
    assert first is not None
    assert first.operation.status.value == "awaiting_readback"

    readback_claim = repository.claim_by_id(
        operation.id,
        worker_id="readback-worker",
        lease_seconds=1,
    )
    assert readback_claim is not None
    assert readback_claim.step_started_at is not None
    expired_at = (readback_claim.lease_expires_at or datetime.now(UTC)) + timedelta(seconds=1)
    assert repository.expire_stale_claims(now=expired_at) == 0
    assert repository.requeue_stale_readback_claims(now=expired_at, max_attempts=2) == 1

    terminal = repository.read(operation.id)
    assert terminal is not None
    assert terminal.status.value == "indeterminate"
    assert terminal.step_started_at is not None
    assert terminal.error == {
        "type": "authoritative_readback_exhausted",
        "provider_may_have_been_called": True,
        "authoritative_readback_required": True,
        "attempts": 2,
    }
    replay = executor.execute(operation.id, worker_id="replacement-worker", lease_seconds=60)
    assert replay is not None
    assert replay.claimed is False
    assert replay.operation.status.value == "indeterminate"
    assert delivery.request_calls == 1
    assert delivery.readback_calls == 0


def test_postgres_sharing_stale_claim_recovery_has_attempt_ceiling(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    claim_time = datetime.now(UTC)
    operation = _sharing_operation(repository, "attempt-ceiling", available_at=claim_time)

    first = repository.claim_by_id(
        operation.id,
        worker_id="crashing-worker-1",
        lease_seconds=1,
        now=claim_time,
    )
    assert first is not None
    first_expiry = (first.lease_expires_at or claim_time) + timedelta(seconds=1)
    assert repository.expire_stale_claims(now=first_expiry, max_attempts=2) == 1
    retryable = repository.read(operation.id)
    assert retryable is not None
    assert retryable.status.value == "retryable"

    second = repository.claim_by_id(
        operation.id,
        worker_id="crashing-worker-2",
        lease_seconds=1,
        now=first_expiry,
    )
    assert second is not None
    second_expiry = (second.lease_expires_at or first_expiry) + timedelta(seconds=1)
    assert repository.expire_stale_claims(now=second_expiry, max_attempts=2) == 1
    exhausted = repository.read(operation.id)
    assert exhausted is not None
    assert exhausted.status.value == "rejected"
    assert exhausted.error == {
        "type": "sharing_claim_recovery_exhausted",
        "provider_may_have_been_called": False,
        "attempts": 2,
    }


def test_postgres_sharing_transition_rejects_an_expired_claim_token(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    claim_time = datetime.now(UTC) - timedelta(seconds=5)
    operation = _sharing_operation(repository, "expired-token", available_at=claim_time)
    claimed = repository.claim_by_id(
        operation.id,
        worker_id="expired-worker",
        lease_seconds=1,
        now=claim_time,
    )
    assert claimed is not None
    assert claimed.claim_token is not None
    expired_at = claim_time + timedelta(seconds=2)

    with pytest.raises(SharingOperationStateError, match="claim is no longer valid"):
        repository.mark_requesting(operation.id, claimed.claim_token, now=expired_at)

    stale = repository.read(operation.id)
    assert stale is not None
    assert stale.status.value == "claimed"
    assert stale.step_started_at is None
    assert repository.expire_stale_claims(now=expired_at) == 1
    recovered = repository.read(operation.id)
    assert recovered is not None
    assert recovered.status.value == "retryable"


def test_postgres_dead_sharing_dispatch_does_not_terminate_active_claim(p9_runtime) -> None:
    repository = PostgresSharingOperationRepository(p9_runtime.unit_of_work)
    operation = _sharing_operation(repository, "dead-active")
    with p9_runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE outbox_events SET status = 'dead' WHERE aggregate_id = :operation_id"),
            {"operation_id": str(operation.id)},
        )
    claimed = repository.claim_by_id(operation.id, worker_id="active-worker", lease_seconds=60)
    assert claimed is not None
    assert claimed.claim_token is not None

    assert repository.reject_dead_dispatches() == 0
    active = repository.read(operation.id)
    assert active is not None
    assert active.status.value == "claimed"
    assert active.claim_token == claimed.claim_token

    repository.mark_retryable(
        operation.id,
        claimed.claim_token,
        error={"type": "prewrite_failure"},
        retry_after_seconds=0,
    )
    assert repository.reject_dead_dispatches() == 1
    rejected = repository.read(operation.id)
    assert rejected is not None
    assert rejected.status.value == "rejected"
    assert rejected.claim_token is None
    assert rejected.error == {
        "type": "sharing_dispatch_dead",
        "provider_may_have_been_called": False,
    }
