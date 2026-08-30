from __future__ import annotations

import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event
from typing import cast
from uuid import UUID

import pytest
from sqlalchemy import text

from etk.domains.sharing import (
    InsufficientContributionError,
    PostgresContributionLedger,
    SharingConfigurationService,
    SharingDeliveryOutcome,
    SharingLedgerService,
    SharingOperation,
    SharingOperationExecutor,
    SharingServerIdentity,
)
from etk.domains.sharing.operations import (
    SharingOperationConflictError,
    SharingOperationIntent,
    SharingOperationKind,
    SharingOperationScopeBusyError,
    SharingOperationStateError,
)
from etk.domains.sharing.repository import PostgresSharingOperationRepository
from etk.domains.system.repository import SqlAlchemyAppSettingsRepository


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

    assert head == "0024_p11_sharing_cancellation"
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
