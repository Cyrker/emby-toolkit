from __future__ import annotations

from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from etk.domains.identity.effect_adapters import InProcessRegistrationSecretPort
from etk.domains.identity.effect_execution import (
    IdentityUserMutationCoordinator,
    RegistrationSagaCoordinator,
)
from etk.domains.identity.effect_ports import ProviderWriteOutcome, ProviderWriteResult
from etk.domains.identity.operation_repository import (
    IdentityOperationScopeBusyError,
    PostgresIdentityOperationRepository,
)
from etk.domains.identity.operations import IdentityOperationKind, IdentityOperationStatus
from etk.domains.identity.users_repository import (
    IdentityReservationConflictError,
    SqlAlchemyUsersRepository,
)
from etk.platform.db import DatabaseRuntime

NOW = datetime(2026, 8, 27, 10, 0, tzinfo=UTC)
PASSWORD = "fixture-registration-password-9f5c"
TOKEN = "fixture-registration-invite-token"


class FakeEmbyRegistration:
    def __init__(self) -> None:
        self.users: dict[str, dict[str, Any]] = {}
        self.passwords: dict[str, str] = {}
        self.create_outcome = ProviderWriteOutcome.CONFIRMED
        self.password_outcome = ProviderWriteOutcome.CONFIRMED
        self.preflight_error = False
        self.raise_after_create = False
        self.calls: list[str] = []

    def list_users(self) -> tuple[dict[str, Any], ...]:
        return tuple(dict(user) for user in self.users.values())

    def find_user_by_name(self, username: str) -> dict[str, Any] | None:
        if self.preflight_error:
            raise RuntimeError("fixture username preflight unavailable")
        return next(
            (dict(user) for user in self.users.values() if user["Name"].casefold() == username.casefold()),
            None,
        )

    def get_user(self, user_id: str) -> dict[str, Any] | None:
        value = self.users.get(user_id)
        return dict(value) if value is not None else None

    def create_user(self, username: str) -> tuple[ProviderWriteResult, str | None]:
        self.calls.append("create")
        user_id = "remote-user-1"
        self.users[user_id] = {"Id": user_id, "Name": username, "Policy": {}, "Configuration": {}}
        if self.raise_after_create:
            raise RuntimeError("simulated transport loss after create")
        returned_id = user_id if self.create_outcome is ProviderWriteOutcome.CONFIRMED else None
        return ProviderWriteResult(self.create_outcome, 200), returned_id

    def set_password(self, user_id: str, password: str) -> ProviderWriteResult:
        self.calls.append("password")
        if self.password_outcome is ProviderWriteOutcome.CONFIRMED:
            self.passwords[user_id] = password
        return ProviderWriteResult(self.password_outcome, 204)

    def verify_password(self, username: str, password: str) -> bool | None:
        user = self.find_user_by_name(username)
        if user is None:
            return False
        return self.passwords.get(str(user["Id"])) == password

    def set_policy(self, user_id: str, policy: Mapping[str, object]) -> ProviderWriteResult:
        self.calls.append("policy")
        self.users[user_id]["Policy"] = dict(policy)
        return ProviderWriteResult(ProviderWriteOutcome.CONFIRMED, 204)

    def set_configuration(self, user_id: str, configuration: Mapping[str, object]) -> ProviderWriteResult:
        self.calls.append("configuration")
        self.users[user_id]["Configuration"] = dict(configuration)
        return ProviderWriteResult(ProviderWriteOutcome.CONFIRMED, 204)

    def delete_user(self, user_id: str) -> ProviderWriteResult:
        self.calls.append("delete")
        self.users.pop(user_id, None)
        self.passwords.pop(user_id, None)
        return ProviderWriteResult(ProviderWriteOutcome.CONFIRMED, 204)


def _invitation(runtime: DatabaseRuntime) -> None:
    users = SqlAlchemyUsersRepository(
        runtime.unit_of_work,
        clock=lambda: NOW,
        token_factory=lambda: TOKEN,
    )
    template_id = users.create_template(
        name="Registration Template",
        description="Fixture registration",
        policy={"EnableAllFolders": False, "EnabledFolders": ["library-a"]},
        default_expiration_days=30,
        source_emby_user_id="template-source",
        configuration={"AudioLanguagePreference": "jpn"},
        allow_unrestricted_subscriptions=False,
        personal_play_account_only=False,
        max_concurrent_streams=1,
    )
    assert users.create_invitations(
        template_id=template_id,
        expiration_days=7,
        link_expires_in_days=3,
        count=1,
    ) == [TOKEN]


def _saga(
    runtime: DatabaseRuntime,
    provider: FakeEmbyRegistration,
) -> tuple[PostgresIdentityOperationRepository, RegistrationSagaCoordinator]:
    repository = PostgresIdentityOperationRepository(runtime.unit_of_work)
    return repository, RegistrationSagaCoordinator(
        repository,
        provider,
        max_attempts=3,
        readback_delay_seconds=0,
    )


def test_registration_commits_user_invitation_operation_and_outbox_atomically_without_secrets(
    p8_runtime: DatabaseRuntime,
) -> None:
    _invitation(p8_runtime)
    provider = FakeEmbyRegistration()
    repository, saga = _saga(p8_runtime, provider)
    reservation = saga.reserve(invitation_token=TOKEN, username="fixture-user", now=NOW)

    with InProcessRegistrationSecretPort().lease(SecretStr(PASSWORD)) as secret:
        execution = saga.execute_with_secret(
            reservation.operation.id,
            secret,
            worker_id="fixture-api",
            lease_seconds=60,
            now=NOW,
        )

    assert execution is not None
    assert execution.operation.status is IdentityOperationStatus.CONFIRMED
    assert provider.calls == ["create", "password", "policy", "configuration"]
    with p8_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                    SELECT operation.status, operation.request_json::text AS request_text,
                           operation.state_json::text AS state_text,
                           invitation.status AS invitation_status,
                           invitation.used_by_user_id,
                           outbox.status AS outbox_status
                    FROM identity_operations AS operation
                    JOIN invitations AS invitation ON invitation.reservation_id = operation.id
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
        assert connection.scalar(text("SELECT COUNT(*) FROM emby_users WHERE id = 'remote-user-1'")) == 1
        assert (
            connection.scalar(text("SELECT COUNT(*) FROM emby_users_extended WHERE emby_user_id = 'remote-user-1'"))
            == 1
        )
    assert row["status"] == "confirmed"
    assert row["invitation_status"] == "used"
    assert row["used_by_user_id"] == "remote-user-1"
    assert row["outbox_status"] == "processed"
    durable_text = f"{row['request_text']}\n{row['state_text']}"
    assert PASSWORD not in durable_text
    assert TOKEN not in durable_text
    assert "secret_ref" not in durable_text


def test_password_failure_deletes_created_user_before_releasing_invitation(
    p8_runtime: DatabaseRuntime,
) -> None:
    _invitation(p8_runtime)
    provider = FakeEmbyRegistration()
    provider.password_outcome = ProviderWriteOutcome.REJECTED
    repository, saga = _saga(p8_runtime, provider)
    reservation = saga.reserve(invitation_token=TOKEN, username="fixture-user", now=NOW)

    with InProcessRegistrationSecretPort().lease(SecretStr(PASSWORD)) as secret:
        execution = saga.execute_with_secret(
            reservation.operation.id,
            secret,
            worker_id="fixture-api",
            lease_seconds=60,
            now=NOW,
        )

    assert execution is not None
    assert execution.operation.status is IdentityOperationStatus.COMPENSATED
    assert provider.users == {}
    assert provider.calls == ["create", "password", "delete"]
    with p8_runtime.engine.connect() as connection:
        invitation_status = connection.scalar(
            text("SELECT status FROM invitations WHERE token = :token"),
            {"token": TOKEN},
        )
        assert invitation_status == "active"


def test_compensated_registration_can_retry_same_username_with_new_generation(
    p8_runtime: DatabaseRuntime,
) -> None:
    _invitation(p8_runtime)
    provider = FakeEmbyRegistration()
    provider.password_outcome = ProviderWriteOutcome.REJECTED
    repository, saga = _saga(p8_runtime, provider)
    first_reservation = saga.reserve(invitation_token=TOKEN, username="fixture-user", now=NOW)

    with InProcessRegistrationSecretPort().lease(SecretStr(PASSWORD)) as secret:
        first = saga.execute_with_secret(
            first_reservation.operation.id,
            secret,
            worker_id="fixture-http",
            lease_seconds=90,
            now=NOW,
        )
    assert first is not None
    assert first.operation.status is IdentityOperationStatus.COMPENSATED

    provider.password_outcome = ProviderWriteOutcome.CONFIRMED
    second_reservation = saga.reserve(
        invitation_token=TOKEN,
        username="fixture-user",
        now=NOW + timedelta(seconds=1),
    )
    with InProcessRegistrationSecretPort().lease(SecretStr(PASSWORD)) as secret:
        second = saga.execute_with_secret(
            second_reservation.operation.id,
            secret,
            worker_id="fixture-http",
            lease_seconds=90,
            now=NOW + timedelta(seconds=1),
        )

    assert second is not None
    assert second.operation.status is IdentityOperationStatus.CONFIRMED
    assert second_reservation.operation.id != first_reservation.operation.id
    assert second_reservation.operation.request["reservation_generation"] == 2
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM identity_operations WHERE kind = 'register'")) == 2


def test_secretless_recovery_before_create_releases_invitation_without_provider_write(
    p8_runtime: DatabaseRuntime,
) -> None:
    _invitation(p8_runtime)
    provider = FakeEmbyRegistration()
    repository, saga = _saga(p8_runtime, provider)
    reservation = saga.reserve(invitation_token=TOKEN, username="fixture-user", now=NOW)

    recovery = saga.execute_recovery(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )

    assert recovery is not None
    assert recovery.operation.status is IdentityOperationStatus.REJECTED
    assert recovery.operation.error == {"type": "registration_secret_unavailable_before_create"}
    assert provider.calls == []
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT status FROM invitations WHERE token = :token"),
                {"token": TOKEN},
            )
            == "active"
        )
        assert (
            connection.scalar(
                text(
                    """
                SELECT available_at > created_at
                FROM outbox_events
                WHERE event_key = :event_key
                """
                ),
                {"event_key": f"identity.operation.requested:{reservation.operation.id}"},
            )
            is True
        )


def test_username_preflight_retry_budget_exhaustion_releases_invitation(
    p8_runtime: DatabaseRuntime,
) -> None:
    _invitation(p8_runtime)
    provider = FakeEmbyRegistration()
    provider.preflight_error = True
    repository, saga = _saga(p8_runtime, provider)
    reservation = saga.reserve(invitation_token=TOKEN, username="fixture-user", now=NOW)

    first = saga.execute_recovery(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )
    second = saga.execute_recovery(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW + timedelta(seconds=1),
    )
    exhausted = saga.execute_recovery(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW + timedelta(seconds=2),
    )

    assert first is not None and first.operation.status is IdentityOperationStatus.RETRYABLE
    assert second is not None and second.operation.status is IdentityOperationStatus.RETRYABLE
    assert exhausted is not None and exhausted.operation.status is IdentityOperationStatus.REJECTED
    assert exhausted.operation.attempts == 3
    assert exhausted.operation.error == {"type": "username_preflight_unavailable"}
    assert provider.calls == []
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT status FROM invitations WHERE token = :token"),
                {"token": TOKEN},
            )
            == "active"
        )
        assert (
            connection.scalar(
                text("SELECT status FROM outbox_events WHERE event_key = :event_key"),
                {"event_key": f"identity.operation.requested:{reservation.operation.id}"},
            )
            == "processed"
        )


def test_claim_checkpoints_renew_the_original_lease_interval(
    p8_runtime: DatabaseRuntime,
) -> None:
    _invitation(p8_runtime)
    repository, saga = _saga(p8_runtime, FakeEmbyRegistration())
    reservation = saga.reserve(invitation_token=TOKEN, username="fixture-user", now=NOW)
    claimed = repository.claim_by_id(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=2,
        now=NOW,
    )
    assert claimed is not None and claimed.claim_token is not None

    started = repository.mark_step_started(
        claimed.id,
        claimed.claim_token,
        step="preflight_username",
        state=claimed.state,
        now=NOW + timedelta(seconds=1),
    )
    assert started.lease_expires_at == NOW + timedelta(seconds=3)
    continued = repository.continue_claim(
        started.id,
        claimed.claim_token,
        next_step="create",
        state=started.state,
        now=NOW + timedelta(seconds=2, milliseconds=500),
    )
    assert continued.lease_expires_at == NOW + timedelta(seconds=4, milliseconds=500)
    assert repository.expire_stale_claims(now=NOW + timedelta(seconds=3, milliseconds=500)) == 0


def test_dead_dispatch_after_read_only_username_preflight_releases_invitation(
    p8_runtime: DatabaseRuntime,
) -> None:
    _invitation(p8_runtime)
    repository, saga = _saga(p8_runtime, FakeEmbyRegistration())
    reservation = saga.reserve(invitation_token=TOKEN, username="fixture-user", now=NOW)
    claimed = repository.claim_by_id(
        reservation.operation.id,
        worker_id="fixture-worker",
        lease_seconds=90,
        now=NOW,
    )
    assert claimed is not None and claimed.claim_token is not None
    repository.mark_step_started(
        claimed.id,
        claimed.claim_token,
        step="preflight_username",
        state=claimed.state,
        now=NOW,
    )
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE outbox_events SET status = 'dead' WHERE event_key = :event_key"),
            {"event_key": f"identity.operation.requested:{claimed.id}"},
        )

    assert repository.reject_dead_dispatches(now=NOW) == 1
    operation = repository.read(claimed.id)
    assert operation is not None
    assert operation.status is IdentityOperationStatus.REJECTED
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT status FROM invitations WHERE token = :token"),
                {"token": TOKEN},
            )
            == "active"
        )


def test_reserved_invitation_and_template_cannot_be_deleted_mid_registration(
    p8_runtime: DatabaseRuntime,
) -> None:
    _invitation(p8_runtime)
    users = SqlAlchemyUsersRepository(p8_runtime.unit_of_work, clock=lambda: NOW)
    repository, saga = _saga(p8_runtime, FakeEmbyRegistration())
    reservation = saga.reserve(invitation_token=TOKEN, username="fixture-user", now=NOW)
    with p8_runtime.engine.connect() as connection:
        invitation_id = int(
            connection.scalar(text("SELECT id FROM invitations WHERE token = :token"), {"token": TOKEN})
        )
        template_id = int(
            connection.scalar(
                text("SELECT template_id FROM invitations WHERE id = :id"),
                {"id": invitation_id},
            )
        )

    with pytest.raises(IdentityReservationConflictError):
        users.delete_invitation(invitation_id)
    with pytest.raises(IdentityReservationConflictError):
        users.delete_template(template_id)

    assert repository.read(reservation.operation.id) is not None
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM invitations WHERE id = :id"), {"id": invitation_id}) == 1
        assert connection.scalar(text("SELECT COUNT(*) FROM user_templates WHERE id = :id"), {"id": template_id}) == 1
        assert connection.scalar(text("SELECT COUNT(*) FROM emby_users")) == 0


@pytest.mark.parametrize("failure_mode", ["exception", "unknown"])
def test_unverifiable_create_is_indeterminate_without_claiming_or_deleting_same_name_user(
    p8_runtime: DatabaseRuntime,
    failure_mode: str,
) -> None:
    _invitation(p8_runtime)
    provider = FakeEmbyRegistration()
    provider.raise_after_create = failure_mode == "exception"
    if failure_mode == "unknown":
        provider.create_outcome = ProviderWriteOutcome.UNKNOWN
    repository, saga = _saga(p8_runtime, provider)
    reservation = saga.reserve(invitation_token=TOKEN, username="fixture-user", now=NOW)

    with InProcessRegistrationSecretPort().lease(SecretStr(PASSWORD)) as secret:
        execution = saga.execute_with_secret(
            reservation.operation.id,
            secret,
            worker_id="fixture-api",
            lease_seconds=1,
            now=NOW,
        )

    assert execution is not None
    assert execution.operation.status is IdentityOperationStatus.INDETERMINATE
    persisted = repository.read(reservation.operation.id)
    assert persisted is not None
    assert persisted.status is IdentityOperationStatus.INDETERMINATE
    assert persisted.error == {"type": "remote_user_create_ownership_unverifiable"}
    assert provider.users == {
        "remote-user-1": {
            "Id": "remote-user-1",
            "Name": "fixture-user",
            "Policy": {},
            "Configuration": {},
        }
    }
    assert provider.passwords == {}
    assert provider.calls == ["create"]
    with p8_runtime.engine.connect() as connection:
        invitation = (
            connection.execute(
                text("SELECT status, reservation_id FROM invitations WHERE token = :token"),
                {"token": TOKEN},
            )
            .mappings()
            .one()
        )
    assert invitation["status"] == "reserved"
    assert str(invitation["reservation_id"]) == str(reservation.operation.id)


def test_concurrent_same_invitation_reuses_one_operation_and_one_outbox(
    p8_runtime: DatabaseRuntime,
) -> None:
    _invitation(p8_runtime)
    repository = PostgresIdentityOperationRepository(p8_runtime.unit_of_work)

    def reserve() -> tuple[str, bool]:
        result = repository.reserve_registration(
            invitation_token=TOKEN,
            username="fixture-user",
            now=NOW,
        )
        return str(result.operation.id), result.created

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: reserve(), range(2)))

    assert len({operation_id for operation_id, _created in results}) == 1
    assert sorted(created for _operation_id, created in results) == [False, True]
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM identity_operations")) == 1
        assert (
            connection.scalar(text("SELECT COUNT(*) FROM outbox_events WHERE topic = 'identity.operation.requested'"))
            == 1
        )

    with pytest.raises(DBAPIError):
        with p8_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE invitations SET status = 'active'
                    WHERE token = :token
                    """
                ),
                {"token": TOKEN},
            )


def test_concurrent_invitations_casefold_same_username_allow_one_nonterminal_operation(
    p8_runtime: DatabaseRuntime,
) -> None:
    _invitation(p8_runtime)
    second_token = f"{TOKEN}-second"
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO invitations (
                    token, template_id, expiration_days, expires_at, status
                )
                SELECT :second_token, template_id, expiration_days, expires_at, 'active'
                FROM invitations WHERE token = :first_token
                """
            ),
            {"second_token": second_token, "first_token": TOKEN},
        )
    repository = PostgresIdentityOperationRepository(p8_runtime.unit_of_work)

    def reserve(values: tuple[str, str]) -> tuple[str, str]:
        token, username = values
        try:
            result = repository.reserve_registration(
                invitation_token=token,
                username=username,
                now=NOW,
            )
        except IdentityOperationScopeBusyError:
            return token, "busy"
        return token, str(result.operation.id)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(reserve, ((TOKEN, "Fixture-User"), (second_token, "fixture-user"))))

    assert sum(result == "busy" for _token, result in results) == 1
    assert sum(result != "busy" for _token, result in results) == 1
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text(
                    """
                    SELECT COUNT(*) FROM identity_operations
                    WHERE kind = 'register'
                      AND lower(request_json ->> 'username') = 'fixture-user'
                      AND status NOT IN ('confirmed','rejected','compensated','cancelled')
                    """
                )
            )
            == 1
        )
        invitation_statuses = {
            str(row["token"]): str(row["status"])
            for row in connection.execute(
                text("SELECT token, status FROM invitations WHERE token IN (:first_token, :second_token)"),
                {"first_token": TOKEN, "second_token": second_token},
            ).mappings()
        }
    assert sorted(invitation_statuses.values()) == ["active", "reserved"]


def test_registration_username_fences_user_mutation_after_external_sync_materializes_user(
    p8_runtime: DatabaseRuntime,
) -> None:
    _invitation(p8_runtime)
    provider = FakeEmbyRegistration()
    repository, saga = _saga(p8_runtime, provider)
    registration = saga.reserve(invitation_token=TOKEN, username="fixture-user", now=NOW)
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO emby_users (id, name, is_administrator)
                VALUES ('remote-user-1', 'Fixture-User', FALSE)
                """
            )
        )
    mutations = IdentityUserMutationCoordinator(
        repository,
        cast(Any, provider),
        max_attempts=3,
        readback_delay_seconds=0,
    )

    with pytest.raises(IdentityOperationScopeBusyError):
        mutations.reserve(
            kind=IdentityOperationKind.SET_STATUS,
            user_id="remote-user-1",
            request={
                "request_id": "registration-user-scope-fence",
                "disable": True,
                "desired_policy": {"IsDisabled": True},
                "previous_policy": {"IsDisabled": False},
            },
            now=NOW,
        )

    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM identity_operations")) == 1
    assert repository.read(registration.operation.id) is not None


def test_outbox_failure_rolls_back_operation_and_invitation_reservation(
    p8_runtime: DatabaseRuntime,
) -> None:
    _invitation(p8_runtime)
    repository = PostgresIdentityOperationRepository(p8_runtime.unit_of_work)
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE FUNCTION p8_reject_identity_outbox() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    IF NEW.topic = 'identity.operation.requested' THEN
                        RAISE EXCEPTION 'fixture identity outbox rejection';
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
                CREATE TRIGGER p8_reject_identity_outbox
                BEFORE INSERT ON outbox_events
                FOR EACH ROW EXECUTE FUNCTION p8_reject_identity_outbox()
                """
            )
        )
    try:
        with pytest.raises(DBAPIError):
            repository.reserve_registration(invitation_token=TOKEN, username="fixture-user", now=NOW)
        with p8_runtime.engine.connect() as connection:
            assert connection.scalar(text("SELECT COUNT(*) FROM identity_operations")) == 0
            invitation = (
                connection.execute(
                    text("SELECT status, reservation_id FROM invitations WHERE token = :token"),
                    {"token": TOKEN},
                )
                .mappings()
                .one()
            )
        assert dict(invitation) == {"status": "active", "reservation_id": None}
    finally:
        with p8_runtime.engine.begin() as connection:
            connection.execute(text("DROP TRIGGER IF EXISTS p8_reject_identity_outbox ON outbox_events"))
            connection.execute(text("DROP FUNCTION IF EXISTS p8_reject_identity_outbox()"))
