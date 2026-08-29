from __future__ import annotations

import hashlib
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event
from typing import Any, cast
from uuid import UUID

import pytest
from sqlalchemy import text

from etk.domains.identity.effect_execution import IdentityUserMutationCoordinator
from etk.domains.identity.effect_ports import ProviderWriteOutcome, ProviderWriteResult
from etk.domains.identity.operation_repository import (
    IdentityOperationScopeBusyError,
    PostgresIdentityOperationRepository,
)
from etk.domains.identity.operations import (
    IdentityOperationKind,
    IdentityOperationStatus,
    identity_operation_key,
)
from etk.domains.identity.repository import SqlAlchemyIdentityRepository
from etk.domains.identity.users_effects import IdentityEffectsService
from etk.domains.identity.users_repository import (
    IdentityReservationConflictError,
    IdentityUserLifecycleBusyError,
    SqlAlchemyUsersRepository,
)
from etk.platform import Settings
from etk.platform.db import DatabaseRuntime
from etk.platform.jobs.json_payload import canonical_json

NOW = datetime(2026, 8, 27, 12, 0, tzinfo=UTC)


class FakeIdentityProvider:
    def __init__(self) -> None:
        self.users: dict[str, dict[str, Any]] = {}
        self.avatar_content: dict[str, bytes] = {}
        self.policy_outcomes: list[ProviderWriteOutcome] = []
        self.configuration_outcomes: list[ProviderWriteOutcome] = []
        self.delete_outcomes: list[ProviderWriteOutcome] = []
        self.avatar_outcomes: list[ProviderWriteOutcome] = []
        self.calls: list[str] = []

    def list_users(self) -> tuple[dict[str, Any], ...]:
        return tuple(dict(user) for user in self.users.values())

    def get_user(self, user_id: str) -> dict[str, Any] | None:
        value = self.users.get(user_id)
        return _copy_user(value) if value is not None else None

    def find_user_by_name(self, username: str) -> dict[str, Any] | None:
        return next(
            (_copy_user(user) for user in self.users.values() if user["Name"].casefold() == username.casefold()),
            None,
        )

    def create_user(self, username: str) -> tuple[ProviderWriteResult, str | None]:
        del username
        raise AssertionError("registration is outside this fixture")

    def set_password(self, user_id: str, password: str) -> ProviderWriteResult:
        del user_id, password
        raise AssertionError("registration is outside this fixture")

    def verify_password(self, username: str, password: str) -> bool | None:
        del username, password
        raise AssertionError("registration is outside this fixture")

    def set_policy(self, user_id: str, policy: Mapping[str, object]) -> ProviderWriteResult:
        self.calls.append(f"policy:{bool(policy.get('CanDownload'))}:{bool(policy.get('IsDisabled'))}")
        outcome = self.policy_outcomes.pop(0) if self.policy_outcomes else ProviderWriteOutcome.CONFIRMED
        if outcome is ProviderWriteOutcome.CONFIRMED:
            self.users[user_id]["Policy"] = dict(policy)
        return ProviderWriteResult(outcome, 204)

    def set_configuration(
        self,
        user_id: str,
        configuration: Mapping[str, object],
    ) -> ProviderWriteResult:
        self.calls.append(f"configuration:{configuration.get('AudioLanguagePreference')}")
        outcome = self.configuration_outcomes.pop(0) if self.configuration_outcomes else ProviderWriteOutcome.CONFIRMED
        if outcome is ProviderWriteOutcome.CONFIRMED:
            self.users[user_id]["Configuration"] = dict(configuration)
        return ProviderWriteResult(outcome, 204)

    def delete_user(self, user_id: str) -> ProviderWriteResult:
        self.calls.append("delete")
        outcome = self.delete_outcomes.pop(0) if self.delete_outcomes else ProviderWriteOutcome.CONFIRMED
        if outcome is ProviderWriteOutcome.CONFIRMED:
            self.users.pop(user_id, None)
        return ProviderWriteResult(outcome, 204)

    def upload_avatar(
        self,
        user_id: str,
        content: bytes,
        content_type: str,
    ) -> ProviderWriteResult:
        del content_type
        self.calls.append("avatar")
        outcome = self.avatar_outcomes.pop(0) if self.avatar_outcomes else ProviderWriteOutcome.CONFIRMED
        if outcome is ProviderWriteOutcome.CONFIRMED:
            self.avatar_content[user_id] = bytes(content)
            self.users[user_id]["PrimaryImageTag"] = hashlib.sha256(content).hexdigest()[:16]
        return ProviderWriteResult(outcome, 204)

    def avatar_sha256(self, user_id: str) -> bytes | None:
        content = self.avatar_content.get(user_id)
        return hashlib.sha256(content).digest() if content is not None else None


class BlockingSnapshotProvider(FakeIdentityProvider):
    def __init__(self) -> None:
        super().__init__()
        self.snapshot_started = Event()
        self.release_snapshot = Event()
        self.snapshot_calls = 0

    def get_user(self, user_id: str) -> dict[str, Any] | None:
        self.snapshot_calls += 1
        if self.snapshot_calls == 1:
            self.snapshot_started.set()
            if not self.release_snapshot.wait(timeout=5):
                raise TimeoutError("fixture snapshot was not released")
        return super().get_user(user_id)


class BlockingExpiryPolicyProvider(FakeIdentityProvider):
    def __init__(self) -> None:
        super().__init__()
        self.policy_started = Event()
        self.release_policy = Event()

    def set_policy(self, user_id: str, policy: Mapping[str, object]) -> ProviderWriteResult:
        self.policy_started.set()
        if not self.release_policy.wait(timeout=5):
            raise TimeoutError("fixture expiry policy was not released")
        return super().set_policy(user_id, policy)


class MissingExpiryPolicyProvider(FakeIdentityProvider):
    def set_policy(self, user_id: str, policy: Mapping[str, object]) -> ProviderWriteResult:
        del user_id, policy
        self.calls.append("policy:404")
        return ProviderWriteResult(ProviderWriteOutcome.UNKNOWN, 404)


def _copy_user(value: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(value)
    for key in ("Policy", "Configuration"):
        if isinstance(result.get(key), Mapping):
            result[key] = dict(result[key])
    return result


def _seed_user(runtime: DatabaseRuntime, *, template_id: int = 1) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO emby_users (id, name, is_administrator, profile_image_tag)
                VALUES ('fixture-user', 'Fixture User', FALSE, 'old-tag')
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO user_templates (
                    id, name, emby_policy_json, default_expiration_days,
                    source_emby_user_id, emby_configuration_json
                ) VALUES (
                    :id, :name, CAST(:policy AS jsonb), 30,
                    :source_user_id, CAST(:configuration AS jsonb)
                )
                """
            ),
            {
                "id": template_id,
                "name": f"Template {template_id}",
                "policy": '{"CanDownload": false, "IsDisabled": false}',
                "source_user_id": f"source-{template_id}",
                "configuration": '{"AudioLanguagePreference": "jpn"}',
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO emby_users_extended (emby_user_id, status, template_id)
                VALUES ('fixture-user', 'active', :template_id)
                """
            ),
            {"template_id": template_id},
        )


def _seed_expiry_job(runtime: DatabaseRuntime, job_id: str, *, created_at: datetime = NOW) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO job_runs (
                    id, task_key, trigger_type, queue_name, payload_version,
                    request_payload_json, status, max_attempts, current_attempt,
                    scheduled_for, created_at, updated_at
                ) VALUES (
                    CAST(:job_id AS uuid), 'check-expired-users', 'manual', 'fixture.serial', 1,
                    '{}'::jsonb, 'queued', 4, 1, :created_at, :created_at, :created_at
                )
                """
            ),
            {"job_id": job_id, "created_at": created_at},
        )


def _seed_expiry_user(
    runtime: DatabaseRuntime,
    user_id: str,
    *,
    status: str = "active",
    expiration_date: datetime | None,
) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO emby_users (id, name, is_administrator, policy_json)
                VALUES (:user_id, :user_id, FALSE, CAST(:policy AS jsonb))
                """
            ),
            {
                "user_id": user_id,
                "policy": canonical_json({"CanDownload": False, "IsDisabled": False}),
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO emby_users_extended (emby_user_id, status, expiration_date)
                VALUES (:user_id, :status, :expiration_date)
                """
            ),
            {"user_id": user_id, "status": status, "expiration_date": expiration_date},
        )


def _coordinator(
    runtime: DatabaseRuntime,
    provider: FakeIdentityProvider,
    *,
    max_attempts: int = 4,
) -> tuple[PostgresIdentityOperationRepository, IdentityUserMutationCoordinator]:
    repository = PostgresIdentityOperationRepository(runtime.unit_of_work)
    return repository, IdentityUserMutationCoordinator(
        repository,
        provider,
        max_attempts=max_attempts,
        readback_delay_seconds=0,
    )


def test_expiry_uses_parent_job_cutoff_and_replays_one_durable_child_per_user(
    p8_runtime: DatabaseRuntime,
) -> None:
    job_id = "00000000-0000-0000-0000-000000000891"
    _seed_expiry_job(p8_runtime, job_id)
    _seed_expiry_user(p8_runtime, "expired-user", expiration_date=NOW - timedelta(seconds=1))
    _seed_expiry_user(p8_runtime, "cutoff-user", expiration_date=NOW)
    _seed_expiry_user(p8_runtime, "future-user", expiration_date=NOW + timedelta(seconds=1))
    _seed_expiry_user(p8_runtime, "permanent-user", expiration_date=None)
    _seed_expiry_user(
        p8_runtime,
        "disabled-user",
        status="disabled",
        expiration_date=NOW - timedelta(days=1),
    )
    provider = FakeIdentityProvider()
    provider.users["expired-user"] = {
        "Id": "expired-user",
        "Name": "expired-user",
        "Policy": {"CanDownload": False, "IsDisabled": False},
        "Configuration": {},
    }
    repository, coordinator = _coordinator(p8_runtime, provider)
    parent_job_id = UUID(job_id)

    assert repository.expired_user_ids(parent_job_id) == ("expired-user",)
    reservation = repository.reserve_expired_user(
        parent_job_id,
        "expired-user",
        now=NOW + timedelta(seconds=1),
    )
    assert reservation is not None and reservation.created is True
    assert reservation.operation.request["expiration_cutoff"] == NOW.isoformat()

    execution = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW + timedelta(seconds=2),
    )

    assert execution is not None
    assert execution.operation.status is IdentityOperationStatus.CONFIRMED
    assert provider.calls == ["policy:False:True"]
    assert provider.users["expired-user"]["Policy"] == {
        "CanDownload": False,
        "IsDisabled": True,
    }
    assert repository.expired_user_ids(parent_job_id) == ("expired-user",)
    replay = repository.reserve_expired_user(
        parent_job_id,
        "expired-user",
        now=NOW + timedelta(seconds=3),
    )
    assert replay is not None
    assert replay.created is False
    assert replay.operation.id == reservation.operation.id
    assert replay.operation.status is IdentityOperationStatus.CONFIRMED
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(text("SELECT status FROM emby_users_extended WHERE emby_user_id = 'expired-user'"))
            == "expired"
        )
        assert connection.scalar(text("SELECT COUNT(*) FROM identity_operations")) == 1
        assert (
            connection.scalar(
                text(
                    """
                SELECT COUNT(*) FROM outbox_events
                WHERE event_key = :event_key AND status = 'processed'
                """
                ),
                {"event_key": f"identity.operation.requested:{reservation.operation.id}"},
            )
            == 1
        )


def test_renewal_committed_before_provider_boundary_cancels_expiry_without_remote_write(
    p8_runtime: DatabaseRuntime,
) -> None:
    job_id = UUID("00000000-0000-0000-0000-000000000892")
    _seed_expiry_job(p8_runtime, str(job_id))
    _seed_expiry_user(p8_runtime, "renewed-user", expiration_date=NOW - timedelta(seconds=1))
    provider = FakeIdentityProvider()
    provider.users["renewed-user"] = {
        "Id": "renewed-user",
        "Name": "renewed-user",
        "Policy": {"CanDownload": False, "IsDisabled": False},
        "Configuration": {},
    }
    repository, coordinator = _coordinator(p8_runtime, provider)
    reservation = repository.reserve_expired_user(job_id, "renewed-user", now=NOW)
    assert reservation is not None

    SqlAlchemyUsersRepository(p8_runtime.unit_of_work).set_expiration(
        "renewed-user",
        (NOW + timedelta(days=30)).isoformat(),
    )
    execution = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW + timedelta(seconds=1),
    )

    assert execution is not None
    assert execution.operation.status is IdentityOperationStatus.CANCELLED
    assert execution.operation.result == {
        "user_id": "renewed-user",
        "skipped": "expiry_snapshot_stale",
    }
    assert provider.calls == []
    with p8_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                SELECT status, expiration_date
                FROM emby_users_extended
                WHERE emby_user_id = 'renewed-user'
                """
                )
            )
            .mappings()
            .one()
        )
    assert row["status"] == "active"
    assert row["expiration_date"] == NOW + timedelta(days=30)


def test_expiry_unknown_write_recovers_by_readback_without_resending_and_fences_late_renewal(
    p8_runtime: DatabaseRuntime,
) -> None:
    job_id = UUID("00000000-0000-0000-0000-000000000893")
    _seed_expiry_job(p8_runtime, str(job_id))
    _seed_expiry_user(p8_runtime, "unknown-user", expiration_date=NOW - timedelta(seconds=1))
    provider = FakeIdentityProvider()
    provider.users["unknown-user"] = {
        "Id": "unknown-user",
        "Name": "unknown-user",
        "Policy": {"CanDownload": False, "IsDisabled": False},
        "Configuration": {},
    }
    provider.policy_outcomes = [ProviderWriteOutcome.UNKNOWN]
    repository, coordinator = _coordinator(p8_runtime, provider)
    reservation = repository.reserve_expired_user(job_id, "unknown-user", now=NOW)
    assert reservation is not None

    first = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )
    assert first is not None
    assert first.operation.status is IdentityOperationStatus.AWAITING_READBACK
    with pytest.raises(IdentityUserLifecycleBusyError):
        SqlAlchemyUsersRepository(p8_runtime.unit_of_work).set_expiration(
            "unknown-user",
            (NOW + timedelta(days=30)).isoformat(),
        )

    provider.users["unknown-user"]["Policy"] = {"CanDownload": False, "IsDisabled": True}
    second = coordinator.execute(
        reservation.operation.id,
        worker_id="replacement-worker",
        lease_seconds=90,
        now=NOW + timedelta(seconds=1),
    )

    assert second is not None
    assert second.operation.status is IdentityOperationStatus.CONFIRMED
    assert provider.calls == ["policy:False:True"]
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(text("SELECT status FROM emby_users_extended WHERE emby_user_id = 'unknown-user'"))
            == "expired"
        )


def test_expiry_policy_404_is_rejected_without_local_expired_commit(
    p8_runtime: DatabaseRuntime,
) -> None:
    job_id = UUID("00000000-0000-0000-0000-000000000896")
    _seed_expiry_job(p8_runtime, str(job_id))
    _seed_expiry_user(p8_runtime, "missing-policy-user", expiration_date=NOW - timedelta(seconds=1))
    provider = MissingExpiryPolicyProvider()
    provider.users["missing-policy-user"] = {
        "Id": "missing-policy-user",
        "Name": "missing-policy-user",
        "Policy": {"CanDownload": False, "IsDisabled": False},
        "Configuration": {},
    }
    repository, coordinator = _coordinator(p8_runtime, provider)
    reservation = repository.reserve_expired_user(job_id, "missing-policy-user", now=NOW)
    assert reservation is not None

    execution = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )

    assert execution is not None
    assert execution.operation.status is IdentityOperationStatus.REJECTED
    assert execution.operation.error == {
        "type": "expiry_policy_rejected",
        "status_code": 404,
    }
    assert provider.calls == ["policy:404"]
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(text("SELECT status FROM emby_users_extended WHERE emby_user_id = 'missing-policy-user'"))
            == "active"
        )


def test_expiry_provider_boundary_linearizes_against_concurrent_renewal(
    p8_runtime: DatabaseRuntime,
) -> None:
    job_id = UUID("00000000-0000-0000-0000-000000000894")
    _seed_expiry_job(p8_runtime, str(job_id))
    _seed_expiry_user(p8_runtime, "racing-user", expiration_date=NOW - timedelta(seconds=1))
    provider = BlockingExpiryPolicyProvider()
    provider.users["racing-user"] = {
        "Id": "racing-user",
        "Name": "racing-user",
        "Policy": {"CanDownload": False, "IsDisabled": False},
        "Configuration": {},
    }
    repository, coordinator = _coordinator(p8_runtime, provider)
    reservation = repository.reserve_expired_user(job_id, "racing-user", now=NOW)
    assert reservation is not None

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            coordinator.execute,
            reservation.operation.id,
            worker_id="fixture-worker",
            lease_seconds=90,
            now=NOW,
        )
        assert provider.policy_started.wait(timeout=5)
        with pytest.raises(IdentityUserLifecycleBusyError):
            SqlAlchemyUsersRepository(p8_runtime.unit_of_work).set_expiration(
                "racing-user",
                (NOW + timedelta(days=30)).isoformat(),
            )
        provider.release_policy.set()
        execution = future.result(timeout=5)

    assert execution is not None
    assert execution.operation.status is IdentityOperationStatus.CONFIRMED
    assert provider.calls == ["policy:False:True"]


def test_expiry_local_commit_rolls_back_and_retries_without_repeating_provider_write(
    p8_runtime: DatabaseRuntime,
) -> None:
    job_id = UUID("00000000-0000-0000-0000-000000000895")
    _seed_expiry_job(p8_runtime, str(job_id))
    _seed_expiry_user(p8_runtime, "rollback-user", expiration_date=NOW - timedelta(seconds=1))
    provider = FakeIdentityProvider()
    provider.users["rollback-user"] = {
        "Id": "rollback-user",
        "Name": "rollback-user",
        "Policy": {"CanDownload": False, "IsDisabled": False},
        "Configuration": {},
    }
    repository, coordinator = _coordinator(p8_runtime, provider)
    reservation = repository.reserve_expired_user(job_id, "rollback-user", now=NOW)
    assert reservation is not None
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE FUNCTION p8_reject_expiry_commit() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    IF NEW.status = 'expired' THEN
                        RAISE EXCEPTION 'fixture expiry local commit rejection';
                    END IF;
                    RETURN NEW;
                END;
                $$
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TRIGGER p8_reject_expiry_commit
                BEFORE UPDATE ON emby_users_extended
                FOR EACH ROW EXECUTE FUNCTION p8_reject_expiry_commit()
                """
            )
        )
    try:
        first = coordinator.execute(
            reservation.operation.id,
            worker_id="fixture-worker",
            lease_seconds=90,
            now=NOW,
        )
        assert first is not None
        assert first.operation.status is IdentityOperationStatus.AWAITING_READBACK
        with p8_runtime.engine.connect() as connection:
            assert (
                connection.scalar(text("SELECT status FROM emby_users_extended WHERE emby_user_id = 'rollback-user'"))
                == "active"
            )
    finally:
        with p8_runtime.engine.begin() as connection:
            connection.execute(text("DROP TRIGGER IF EXISTS p8_reject_expiry_commit ON emby_users_extended"))
            connection.execute(text("DROP FUNCTION IF EXISTS p8_reject_expiry_commit()"))

    second = coordinator.execute(
        reservation.operation.id,
        worker_id="replacement-worker",
        lease_seconds=90,
        now=NOW + timedelta(seconds=1),
    )

    assert second is not None
    assert second.operation.status is IdentityOperationStatus.CONFIRMED
    assert provider.calls == ["policy:False:True"]
    with p8_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                SELECT operation.status AS operation_status,
                       extended.status AS user_status,
                       outbox.status AS outbox_status
                FROM identity_operations AS operation
                JOIN emby_users_extended AS extended
                  ON extended.emby_user_id = operation.request_json ->> 'user_id'
                JOIN outbox_events AS outbox
                  ON outbox.event_key = 'identity.operation.requested:' || operation.id::text
                WHERE operation.id = :operation_id
                """
                ),
                {"operation_id": reservation.operation.id},
            )
            .mappings()
            .one()
        )
    assert row == {
        "operation_status": "confirmed",
        "user_status": "expired",
        "outbox_status": "processed",
    }


def test_assign_template_confirms_remote_before_atomic_local_commit(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime, template_id=1)
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO user_templates (
                    id, name, emby_policy_json, default_expiration_days,
                    source_emby_user_id, emby_configuration_json
                ) VALUES (
                    2, 'Template 2', '{"CanDownload": true, "IsDisabled": false}'::jsonb,
                    30, 'source-2', '{"AudioLanguagePreference": "eng"}'::jsonb
                )
                """
            )
        )
    provider = FakeIdentityProvider()
    provider.users["fixture-user"] = {
        "Id": "fixture-user",
        "Name": "Fixture User",
        "Policy": {"CanDownload": False, "IsDisabled": False},
        "Configuration": {"AudioLanguagePreference": "jpn"},
    }
    repository, coordinator = _coordinator(p8_runtime, provider)
    reservation = coordinator.reserve(
        kind=IdentityOperationKind.ASSIGN_TEMPLATE,
        user_id="fixture-user",
        request={
            "request_id": "assign-template-1",
            "template_id": 2,
            "template_policy": {"CanDownload": True, "IsDisabled": False},
            "template_configuration": {"AudioLanguagePreference": "eng"},
            "previous_policy": {"CanDownload": False, "IsDisabled": False},
            "previous_configuration": {"AudioLanguagePreference": "jpn"},
        },
        now=NOW,
    )

    execution = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )

    assert execution is not None
    assert execution.operation.status is IdentityOperationStatus.CONFIRMED
    assert provider.users["fixture-user"]["Policy"]["CanDownload"] is True
    assert provider.users["fixture-user"]["Configuration"]["AudioLanguagePreference"] == "eng"
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(text("SELECT template_id FROM emby_users_extended WHERE emby_user_id = 'fixture-user'"))
            == 2
        )
        assert (
            connection.scalar(
                text(
                    """
                SELECT status FROM outbox_events
                WHERE event_key = :event_key
                """
                ),
                {"event_key": f"identity.operation.requested:{reservation.operation.id}"},
            )
            == "processed"
        )
    assert repository.read(reservation.operation.id) == execution.operation


def test_assign_template_configuration_rejection_restores_prior_remote_snapshot(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    provider = FakeIdentityProvider()
    provider.users["fixture-user"] = {
        "Id": "fixture-user",
        "Name": "Fixture User",
        "Policy": {"CanDownload": False, "IsDisabled": False},
        "Configuration": {"AudioLanguagePreference": "jpn"},
    }
    provider.configuration_outcomes = [ProviderWriteOutcome.REJECTED, ProviderWriteOutcome.CONFIRMED]
    _repository, coordinator = _coordinator(p8_runtime, provider)
    reservation = coordinator.reserve(
        kind=IdentityOperationKind.ASSIGN_TEMPLATE,
        user_id="fixture-user",
        request={
            "request_id": "assign-template-compensation",
            "template_id": 1,
            "template_policy": {"CanDownload": True, "IsDisabled": False},
            "template_configuration": {"AudioLanguagePreference": "eng"},
            "previous_policy": {"CanDownload": False, "IsDisabled": False},
            "previous_configuration": {"AudioLanguagePreference": "jpn"},
        },
        now=NOW,
    )

    execution = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )

    assert execution is not None
    assert execution.operation.status is IdentityOperationStatus.REJECTED
    assert execution.operation.error == {
        "type": "assign_template_compensated",
        "cause": "configuration_rejected_after_prior_effect",
    }
    assert provider.users["fixture-user"]["Policy"] == {
        "CanDownload": False,
        "IsDisabled": False,
    }
    assert provider.users["fixture-user"]["Configuration"] == {"AudioLanguagePreference": "jpn"}


def test_status_unknown_old_readback_then_later_confirmation_does_not_repeat_write(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    provider = FakeIdentityProvider()
    provider.users["fixture-user"] = {
        "Id": "fixture-user",
        "Name": "Fixture User",
        "Policy": {"CanDownload": False, "IsDisabled": False},
        "Configuration": {},
    }
    provider.policy_outcomes = [ProviderWriteOutcome.UNKNOWN]
    _repository, coordinator = _coordinator(p8_runtime, provider)
    desired = {"CanDownload": False, "IsDisabled": True}
    reservation = coordinator.reserve(
        kind=IdentityOperationKind.SET_STATUS,
        user_id="fixture-user",
        request={
            "request_id": "status-unknown",
            "disable": True,
            "desired_policy": desired,
            "previous_policy": {"CanDownload": False, "IsDisabled": False},
        },
        now=NOW,
    )

    first = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )
    assert first is not None
    assert first.operation.status is IdentityOperationStatus.AWAITING_READBACK
    provider.users["fixture-user"]["Policy"] = desired

    second = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW + timedelta(seconds=1),
    )

    assert second is not None
    assert second.operation.status is IdentityOperationStatus.CONFIRMED
    assert provider.calls == ["policy:False:True"]
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(text("SELECT status FROM emby_users_extended WHERE emby_user_id = 'fixture-user'"))
            == "disabled"
        )


def test_concurrent_same_idempotency_key_reuses_first_stable_caller_intent(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    provider = FakeIdentityProvider()
    repository, coordinator = _coordinator(p8_runtime, provider)

    def reserve(previous_download: bool):
        return coordinator.reserve(
            kind=IdentityOperationKind.SET_STATUS,
            user_id="fixture-user",
            request={
                "request_id": "stable-idempotency-request",
                "disable": True,
                "desired_policy": {"CanDownload": previous_download, "IsDisabled": True},
                "previous_policy": {"CanDownload": previous_download, "IsDisabled": False},
            },
            idempotency_key="concurrent-status-request",
            now=NOW,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        reservations = list(pool.map(reserve, (False, True)))

    assert reservations[0].operation.id == reservations[1].operation.id
    assert sum(1 for reservation in reservations if reservation.created) == 1
    assert (
        repository.read_idempotent(
            kind=IdentityOperationKind.SET_STATUS,
            scope_key="user:fixture-user",
            idempotency_key="concurrent-status-request",
        )
        is not None
    )
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM identity_operations")) == 1
        assert connection.scalar(text("SELECT COUNT(*) FROM outbox_events")) == 1


def test_active_or_indeterminate_user_scope_fences_overlapping_mutations(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    provider = FakeIdentityProvider()
    repository, coordinator = _coordinator(p8_runtime, provider)
    coordinator.reserve(
        kind=IdentityOperationKind.SET_STATUS,
        user_id="fixture-user",
        request={
            "request_id": "scope-fence-status",
            "disable": True,
            "desired_policy": {"IsDisabled": True},
            "previous_policy": {"IsDisabled": False},
        },
        now=NOW,
    )

    with pytest.raises(IdentityOperationScopeBusyError):
        coordinator.reserve(
            kind=IdentityOperationKind.DELETE_USER,
            user_id="fixture-user",
            request={"request_id": "scope-fence-delete"},
            now=NOW,
        )

    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM identity_operations")) == 1


def test_unkeyed_same_intent_retry_recovers_active_operation_reference(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    repository, coordinator = _coordinator(p8_runtime, FakeIdentityProvider())
    first = coordinator.reserve(
        kind=IdentityOperationKind.SET_STATUS,
        user_id="fixture-user",
        request={
            "request_id": "first-network-attempt",
            "disable": True,
            "snapshot_pending": True,
        },
        now=NOW,
    )
    recovered = coordinator.reserve(
        kind=IdentityOperationKind.SET_STATUS,
        user_id="fixture-user",
        request={
            "request_id": "retry-after-lost-response",
            "disable": True,
            "snapshot_pending": True,
        },
        now=NOW,
    )

    assert recovered.operation.id == first.operation.id
    assert recovered.created is False
    assert repository.read_active_scope("user:fixture-user") == first.operation


def test_assign_snapshot_reservation_fences_concurrent_status_before_second_provider_read(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime, template_id=1)
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO user_templates (
                    id, name, emby_policy_json, default_expiration_days,
                    source_emby_user_id, emby_configuration_json
                ) VALUES (
                    2, 'Template 2', '{"CanDownload": true, "IsDisabled": false}'::jsonb,
                    30, 'source-2', '{"AudioLanguagePreference": "eng"}'::jsonb
                )
                """
            )
        )
    provider = BlockingSnapshotProvider()
    provider.users["fixture-user"] = {
        "Id": "fixture-user",
        "Name": "Fixture User",
        "Policy": {"CanDownload": False, "IsDisabled": False},
        "Configuration": {"AudioLanguagePreference": "jpn"},
    }
    _repository, coordinator = _coordinator(p8_runtime, provider)
    users = SqlAlchemyUsersRepository(p8_runtime.unit_of_work, clock=lambda: NOW)
    service = IdentityEffectsService(
        registration=cast(Any, object()),
        mutations=coordinator,
        secrets=cast(Any, object()),
        repository=users,
        emby=provider,
        settings=lambda: Settings.model_validate({}),
        worker_id="fixture-http",
        lease_seconds=90,
    )

    with ThreadPoolExecutor(max_workers=1) as pool:
        assignment_future = pool.submit(
            service.assign_template,
            "fixture-user",
            {"template_id": 2},
        )
        assert provider.snapshot_started.wait(timeout=5)
        status_result = service.set_status("fixture-user", {"disable": True})
        assert status_result.status_code == 409
        assert provider.snapshot_calls == 1
        provider.release_snapshot.set()
        assignment_result = assignment_future.result(timeout=5)

    assert assignment_result.status_code == 200
    with p8_runtime.engine.connect() as connection:
        projected = (
            connection.execute(
                text(
                    """
                    SELECT template_id, status
                    FROM emby_users_extended WHERE emby_user_id = 'fixture-user'
                    """
                )
            )
            .mappings()
            .one()
        )
    assert dict(projected) == {"template_id": 2, "status": "active"}


def test_pending_snapshot_worker_rejects_before_provider_write(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    provider = FakeIdentityProvider()
    repository, coordinator = _coordinator(p8_runtime, provider)
    reservation = coordinator.reserve(
        kind=IdentityOperationKind.SET_STATUS,
        user_id="fixture-user",
        request={
            "request_id": "pending-snapshot-worker",
            "disable": True,
            "snapshot_pending": True,
        },
        outbox_delay_seconds=90,
        now=NOW,
    )

    execution = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )

    assert execution is not None
    assert execution.operation.status is IdentityOperationStatus.REJECTED
    assert execution.operation.error == {"type": "provider_snapshot_unavailable_before_write"}
    assert provider.calls == []
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT status FROM outbox_events WHERE event_key = :event_key"),
                {"event_key": f"identity.operation.requested:{reservation.operation.id}"},
            )
            == "processed"
        )
    assert repository.read(reservation.operation.id) == execution.operation


def test_disabled_admin_policy_is_projected_and_revokes_existing_session_authorization(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE emby_users
                SET is_administrator = TRUE,
                    policy_json = '{"IsAdministrator": true, "IsDisabled": false}'::jsonb
                WHERE id = 'fixture-user'
                """
            )
        )
    provider = FakeIdentityProvider()
    provider.users["fixture-user"] = {
        "Id": "fixture-user",
        "Name": "Fixture User",
        "Policy": {"IsAdministrator": True, "IsDisabled": False},
        "Configuration": {},
    }
    _repository, coordinator = _coordinator(p8_runtime, provider)
    identity = SqlAlchemyIdentityRepository(p8_runtime.unit_of_work)
    stale_projection_revision = identity.projection_revision()
    reservation = coordinator.reserve(
        kind=IdentityOperationKind.SET_STATUS,
        user_id="fixture-user",
        request={
            "request_id": "disable-admin",
            "disable": True,
            "desired_policy": {"IsAdministrator": True, "IsDisabled": True},
            "previous_policy": {"IsAdministrator": True, "IsDisabled": False},
        },
        now=NOW,
    )

    execution = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )

    assert execution is not None and execution.operation.status is IdentityOperationStatus.CONFIRMED
    assert (
        identity.upsert_user(
            provider.users["fixture-user"],
            expected_revision=stale_projection_revision,
        )
        is False
    )
    assert identity.is_admin("fixture-user") is False
    with p8_runtime.engine.connect() as connection:
        projected = (
            connection.execute(
                text(
                    """
                    SELECT users.is_administrator, users.policy_json, extended.status
                    FROM emby_users AS users
                    JOIN emby_users_extended AS extended ON extended.emby_user_id = users.id
                    WHERE users.id = 'fixture-user'
                    """
                )
            )
            .mappings()
            .one()
        )
    assert projected["is_administrator"] is True
    assert projected["policy_json"] == {"IsAdministrator": True, "IsDisabled": True}
    assert projected["status"] == "disabled"
    assert identity.is_active("fixture-user") is False

    with p8_runtime.engine.begin() as connection:
        connection.execute(text("UPDATE emby_users_extended SET status = 'active' WHERE emby_user_id = 'fixture-user'"))
    assert identity.is_admin("fixture-user") is False
    assert identity.is_active("fixture-user") is False

    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE emby_users_extended SET status = 'expired' WHERE emby_user_id = 'fixture-user'")
        )
    assert identity.is_admin("fixture-user") is False
    assert identity.is_active("fixture-user") is False


def test_snapshot_finalize_updates_operation_and_outbox_hash_without_changing_identity(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    repository, coordinator = _coordinator(p8_runtime, FakeIdentityProvider())
    request_id = "snapshot-finalize-ledger"
    reservation = coordinator.reserve(
        kind=IdentityOperationKind.SET_STATUS,
        user_id="fixture-user",
        request={
            "request_id": request_id,
            "disable": True,
            "snapshot_pending": True,
        },
        outbox_delay_seconds=90,
        now=NOW,
    )
    final_request = {
        "user_id": "fixture-user",
        "request_id": request_id,
        "disable": True,
        "desired_policy": {"CanDownload": False, "IsDisabled": True},
        "previous_policy": {"CanDownload": False, "IsDisabled": False},
    }

    finalized = coordinator.finalize_request(reservation.operation.id, final_request, now=NOW)
    expected_hash = hashlib.sha256(canonical_json(finalized.request).encode("utf-8")).digest()

    assert finalized.request_hash == expected_hash
    assert finalized.operation_key == reservation.operation.operation_key
    assert finalized.operation_key == identity_operation_key(
        kind=IdentityOperationKind.SET_STATUS,
        scope_key="user:fixture-user",
        idempotency_key=request_id,
    )
    with p8_runtime.engine.connect() as connection:
        outbox_hash = connection.scalar(
            text("SELECT payload_json ->> 'request_hash' FROM outbox_events WHERE event_key = :event_key"),
            {"event_key": f"identity.operation.requested:{finalized.id}"},
        )
    assert outbox_hash == expected_hash.hex()


def test_active_assignment_blocks_template_delete(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime, template_id=1)
    users = SqlAlchemyUsersRepository(p8_runtime.unit_of_work, clock=lambda: NOW)
    template_id = 2
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO user_templates (
                    id, name, emby_policy_json, default_expiration_days,
                    source_emby_user_id, emby_configuration_json
                ) VALUES (
                    :template_id, 'Assignment Target', '{"CanDownload": true}'::jsonb,
                    30, 'source-2', NULL
                )
                """
            ),
            {"template_id": template_id},
        )
    _repository, coordinator = _coordinator(p8_runtime, FakeIdentityProvider())
    coordinator.reserve(
        kind=IdentityOperationKind.ASSIGN_TEMPLATE,
        user_id="fixture-user",
        request={
            "request_id": "active-assignment-template-delete",
            "template_id": template_id,
            "snapshot_pending": True,
        },
        outbox_delay_seconds=90,
        now=NOW,
    )

    with pytest.raises(IdentityReservationConflictError):
        users.delete_template(template_id)

    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT COUNT(*) FROM user_templates WHERE id = :template_id"),
                {"template_id": template_id},
            )
            == 1
        )


def test_fresh_avatar_retry_gets_new_generation_after_rejected_write(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    provider = FakeIdentityProvider()
    provider.users["fixture-user"] = {
        "Id": "fixture-user",
        "Name": "Fixture User",
        "Policy": {},
        "Configuration": {},
        "PrimaryImageTag": "old-tag",
    }
    provider.avatar_content["fixture-user"] = b"old-avatar"
    provider.avatar_outcomes = [ProviderWriteOutcome.REJECTED, ProviderWriteOutcome.REJECTED]
    repository, coordinator = _coordinator(p8_runtime, provider)
    content = b"fresh-avatar-retry"

    first_reservation = coordinator.reserve_avatar(
        user_id="fixture-user",
        content=content,
        content_type="image/png",
        previous_tag="old-tag",
        now=NOW,
    )
    first = coordinator.execute(
        first_reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )
    assert first is not None and first.operation.status is IdentityOperationStatus.REJECTED

    second_reservation = coordinator.reserve_avatar(
        user_id="fixture-user",
        content=content,
        content_type="image/png",
        previous_tag="old-tag",
        now=NOW + timedelta(seconds=1),
    )

    assert second_reservation.operation.id != first_reservation.operation.id
    assert second_reservation.operation.request["request_id"] != first_reservation.operation.request["request_id"]
    assert repository.read(second_reservation.operation.id) is not None


def test_avatar_idempotency_replays_success_and_rejects_different_content(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    provider = FakeIdentityProvider()
    provider.users["fixture-user"] = {
        "Id": "fixture-user",
        "Name": "Fixture User",
        "Policy": {},
        "Configuration": {},
        "PrimaryImageTag": "old-tag",
    }
    provider.avatar_content["fixture-user"] = b"old-avatar"
    _repository, coordinator = _coordinator(p8_runtime, provider)
    content = b"idempotent-avatar"
    first_reservation = coordinator.reserve_avatar(
        user_id="fixture-user",
        content=content,
        content_type="image/png",
        previous_tag="old-tag",
        idempotency_key="avatar-http-request",
        now=NOW,
    )
    first = coordinator.execute(
        first_reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )
    assert first is not None and first.operation.status is IdentityOperationStatus.CONFIRMED

    replay = coordinator.reserve_avatar(
        user_id="fixture-user",
        content=content,
        content_type="image/png",
        previous_tag="new-provider-tag",
        idempotency_key="avatar-http-request",
        now=NOW + timedelta(seconds=1),
    )
    assert replay.operation.id == first_reservation.operation.id
    assert replay.created is False

    with pytest.raises(ValueError, match="different content"):
        coordinator.reserve_avatar(
            user_id="fixture-user",
            content=b"different-avatar",
            content_type="image/png",
            previous_tag="new-provider-tag",
            idempotency_key="avatar-http-request",
            now=NOW + timedelta(seconds=2),
        )
    assert provider.calls == ["avatar"]


def test_delete_unknown_old_readback_then_absence_commits_local_cleanup_once(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO user_media_data (user_id, item_id, played)
                VALUES ('fixture-user', 'item-1', TRUE)
                """
            )
        )
    provider = FakeIdentityProvider()
    provider.users["fixture-user"] = {
        "Id": "fixture-user",
        "Name": "Fixture User",
        "Policy": {},
        "Configuration": {},
    }
    provider.delete_outcomes = [ProviderWriteOutcome.UNKNOWN]
    _repository, coordinator = _coordinator(p8_runtime, provider)
    identity = SqlAlchemyIdentityRepository(p8_runtime.unit_of_work)
    stale_projection_revision = identity.projection_revision()
    reservation = coordinator.reserve(
        kind=IdentityOperationKind.DELETE_USER,
        user_id="fixture-user",
        request={"request_id": "delete-unknown"},
        now=NOW,
    )

    first = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )
    assert first is not None
    assert first.operation.status is IdentityOperationStatus.AWAITING_READBACK
    provider.users.pop("fixture-user")
    second = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW + timedelta(seconds=1),
    )

    assert second is not None
    assert second.operation.status is IdentityOperationStatus.CONFIRMED
    assert provider.calls == ["delete"]
    stale_applied = identity.upsert_user(
        {
            "Id": "fixture-user",
            "Name": "Stale Admin Snapshot",
            "Policy": {"IsAdministrator": True},
        },
        expected_revision=stale_projection_revision,
    )
    assert stale_applied is False
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM emby_users WHERE id = 'fixture-user'")) == 0
        assert connection.scalar(text("SELECT COUNT(*) FROM user_media_data WHERE user_id = 'fixture-user'")) == 0
        tombstone = connection.execute(
            text(
                """
                SELECT generation, last_operation_id
                FROM identity_user_tombstones
                WHERE user_id = 'fixture-user'
                """
            )
        ).one()
    assert tombstone.generation == 1
    assert tombstone.last_operation_id == reservation.operation.id


def test_avatar_unknown_then_digest_readback_consumes_bound_artifact_without_reupload(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    provider = FakeIdentityProvider()
    provider.users["fixture-user"] = {
        "Id": "fixture-user",
        "Name": "Fixture User",
        "Policy": {},
        "Configuration": {},
        "PrimaryImageTag": "old-tag",
    }
    provider.avatar_content["fixture-user"] = b"old-avatar"
    provider.avatar_outcomes = [ProviderWriteOutcome.UNKNOWN]
    _repository, coordinator = _coordinator(p8_runtime, provider)
    content = b"fixture-avatar-content"
    reservation = coordinator.reserve_avatar(
        user_id="fixture-user",
        content=content,
        content_type="image/png",
        previous_tag="old-tag",
        now=NOW,
    )

    first = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )
    assert first is not None
    assert first.operation.status is IdentityOperationStatus.AWAITING_READBACK
    provider.avatar_content["fixture-user"] = content
    provider.users["fixture-user"]["PrimaryImageTag"] = "new-tag"
    second = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW + timedelta(seconds=1),
    )

    assert second is not None
    assert second.operation.status is IdentityOperationStatus.CONFIRMED
    assert provider.calls == ["avatar"]
    with p8_runtime.engine.connect() as connection:
        artifact = (
            connection.execute(
                text(
                    """
                    SELECT operation_id, status, content_bytes
                    FROM identity_avatar_artifacts
                    """
                )
            )
            .mappings()
            .one()
        )
        assert str(artifact["operation_id"]) == str(reservation.operation.id)
        assert artifact["status"] == "consumed"
        assert artifact["content_bytes"] is None
        assert (
            connection.scalar(text("SELECT profile_image_tag FROM emby_users WHERE id = 'fixture-user'")) == "new-tag"
        )


def test_avatar_unknown_can_confirm_after_artifact_expiry_without_reupload(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    provider = FakeIdentityProvider()
    provider.users["fixture-user"] = {
        "Id": "fixture-user",
        "Name": "Fixture User",
        "Policy": {},
        "Configuration": {},
        "PrimaryImageTag": "old-tag",
    }
    provider.avatar_content["fixture-user"] = b"old-avatar"
    provider.avatar_outcomes = [ProviderWriteOutcome.UNKNOWN]
    repository, coordinator = _coordinator(p8_runtime, provider)
    content = b"fixture-avatar-expiry-readback"
    reservation = coordinator.reserve_avatar(
        user_id="fixture-user",
        content=content,
        content_type="image/png",
        previous_tag="old-tag",
        now=NOW,
    )

    first = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )
    assert first is not None
    assert first.operation.status is IdentityOperationStatus.AWAITING_READBACK
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE identity_avatar_artifacts
                SET created_at = :created_at, expires_at = :expired_at
                WHERE operation_id = :operation_id
                """
            ),
            {
                "created_at": NOW - timedelta(seconds=1),
                "expired_at": NOW,
                "operation_id": reservation.operation.id,
            },
        )
    assert repository.purge_expired_avatar_artifacts(now=NOW + timedelta(seconds=1)) == 1

    provider.avatar_content["fixture-user"] = content
    provider.users["fixture-user"]["PrimaryImageTag"] = "expiry-confirmed-tag"
    second = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW + timedelta(seconds=2),
    )

    assert second is not None
    assert second.operation.status is IdentityOperationStatus.CONFIRMED
    assert provider.calls == ["avatar"]
    with p8_runtime.engine.connect() as connection:
        artifact = (
            connection.execute(text("SELECT status, content_bytes FROM identity_avatar_artifacts")).mappings().one()
        )
        assert artifact["status"] == "consumed"
        assert artifact["content_bytes"] is None


def test_confirmed_receipts_survive_local_commit_retry_without_repeating_provider_writes(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    provider = FakeIdentityProvider()
    provider.users["fixture-user"] = {
        "Id": "fixture-user",
        "Name": "Fixture User",
        "Policy": {"CanDownload": False, "IsDisabled": False},
        "Configuration": {"AudioLanguagePreference": "jpn"},
    }
    repository, coordinator = _coordinator(p8_runtime, provider)
    reservation = coordinator.reserve(
        kind=IdentityOperationKind.ASSIGN_TEMPLATE,
        user_id="fixture-user",
        request={
            "request_id": "assign-template-local-retry",
            "template_id": 1,
            "template_policy": {"CanDownload": True, "IsDisabled": False},
            "template_configuration": {"AudioLanguagePreference": "eng"},
            "previous_policy": {"CanDownload": False, "IsDisabled": False},
            "previous_configuration": {"AudioLanguagePreference": "jpn"},
        },
        now=NOW,
    )
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE FUNCTION p8_reject_template_projection() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    RAISE EXCEPTION 'fixture local projection rejection';
                END;
                $$
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TRIGGER p8_reject_template_projection
                BEFORE UPDATE ON emby_users_extended
                FOR EACH ROW EXECUTE FUNCTION p8_reject_template_projection()
                """
            )
        )
    try:
        first = coordinator.execute(
            reservation.operation.id,
            worker_id="fixture-worker",
            lease_seconds=90,
            now=NOW,
        )
        assert first is not None
        assert first.operation.status is IdentityOperationStatus.AWAITING_READBACK
    finally:
        with p8_runtime.engine.begin() as connection:
            connection.execute(text("DROP TRIGGER IF EXISTS p8_reject_template_projection ON emby_users_extended"))
            connection.execute(text("DROP FUNCTION IF EXISTS p8_reject_template_projection()"))

    second = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW + timedelta(seconds=1),
    )

    assert second is not None
    assert second.operation.status is IdentityOperationStatus.CONFIRMED
    assert provider.calls == ["policy:True:False", "configuration:eng"]
    persisted = repository.read(reservation.operation.id)
    assert persisted is not None
    assert persisted.status is IdentityOperationStatus.CONFIRMED


def test_dead_dispatch_after_confirmed_provider_receipt_is_indeterminate(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    provider = FakeIdentityProvider()
    repository, coordinator = _coordinator(p8_runtime, provider)
    reservation = coordinator.reserve(
        kind=IdentityOperationKind.SET_STATUS,
        user_id="fixture-user",
        request={
            "request_id": "dead-after-provider-receipt",
            "disable": True,
            "desired_policy": {"IsDisabled": True},
            "previous_policy": {"IsDisabled": False},
        },
        now=NOW,
    )
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE identity_operations
                SET state_json = CAST(:state_json AS jsonb),
                    current_step = 'local_commit', step_started_at = NULL
                WHERE id = :operation_id
                """
            ),
            {
                "operation_id": reservation.operation.id,
                "state_json": '{"receipts":{"status_policy":{"confirmed":true}}}',
            },
        )
        connection.execute(
            text(
                """
                UPDATE outbox_events SET status = 'dead'
                WHERE event_key = :event_key
                """
            ),
            {"event_key": f"identity.operation.requested:{reservation.operation.id}"},
        )

    assert repository.reject_dead_dispatches(now=NOW) == 1
    operation = repository.read(reservation.operation.id)
    assert operation is not None
    assert operation.status is IdentityOperationStatus.INDETERMINATE


def test_avatar_artifact_ownership_digest_and_expiry_are_enforced(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    provider = FakeIdentityProvider()
    provider.users["fixture-user"] = {
        "Id": "fixture-user",
        "Name": "Fixture User",
        "Policy": {},
        "Configuration": {},
        "PrimaryImageTag": "old-tag",
    }
    repository, coordinator = _coordinator(p8_runtime, provider)
    content = b"fixture-avatar-content"
    reservation = coordinator.reserve_avatar(
        user_id="fixture-user",
        content=content,
        content_type="image/png",
        previous_tag="old-tag",
        now=NOW,
    )
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE identity_avatar_artifacts
                SET content_bytes = :tampered, created_at = :created_at, expires_at = :expires_at
                WHERE operation_id = :operation_id
                """
            ),
            {
                "tampered": b"x" * len(content),
                "created_at": NOW - timedelta(seconds=1),
                "expires_at": NOW + timedelta(seconds=1),
                "operation_id": reservation.operation.id,
            },
        )

    execution = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )
    assert execution is not None
    assert execution.operation.status is IdentityOperationStatus.REJECTED
    assert execution.operation.error == {"type": "avatar_artifact_integrity_failure"}
    assert provider.calls == []

    assert repository.purge_expired_avatar_artifacts(now=NOW + timedelta(seconds=2)) == 1
    with p8_runtime.engine.connect() as connection:
        artifact = (
            connection.execute(text("SELECT status, content_bytes FROM identity_avatar_artifacts")).mappings().one()
        )
        assert artifact["status"] == "purged"
        assert artifact["content_bytes"] is None


def test_avatar_digest_without_authoritative_tag_never_clears_local_tag(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    provider = FakeIdentityProvider()
    provider.users["fixture-user"] = {
        "Id": "fixture-user",
        "Name": "Fixture User",
        "Policy": {},
        "Configuration": {},
    }
    provider.avatar_content["fixture-user"] = b"old-avatar"
    provider.avatar_outcomes = [ProviderWriteOutcome.UNKNOWN]
    repository, coordinator = _coordinator(p8_runtime, provider, max_attempts=2)
    content = b"fixture-avatar-content"
    reservation = coordinator.reserve_avatar(
        user_id="fixture-user",
        content=content,
        content_type="image/png",
        previous_tag="old-tag",
        now=NOW,
    )

    first = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )
    assert first is not None
    assert first.operation.status is IdentityOperationStatus.AWAITING_READBACK
    provider.avatar_content["fixture-user"] = content
    second = coordinator.execute(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW + timedelta(seconds=1),
    )

    assert second is not None
    assert second.operation.status is IdentityOperationStatus.INDETERMINATE
    assert second.operation.error == {"type": "avatar_tag_not_observed"}
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(text("SELECT profile_image_tag FROM emby_users WHERE id = 'fixture-user'")) == "old-tag"
        )
