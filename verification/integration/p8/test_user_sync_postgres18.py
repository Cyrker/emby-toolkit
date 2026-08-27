from __future__ import annotations

from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from threading import Event
from typing import Any

import pytest
from sqlalchemy import text

from etk.domains.identity.user_sync import SqlAlchemyUserSyncRepository, UserSyncService
from etk.platform.db import DatabaseRuntime
from etk.platform.jobs.execution import PersistentExecutionCancelled
from etk.platform.tasks import TaskOutcomeStatus


class BlockingUserSyncProvider:
    def __init__(
        self,
        users: Sequence[Mapping[str, Any]],
        details: Mapping[str, Mapping[str, Any] | None],
        *,
        block_user_id: str | None = None,
    ) -> None:
        self._users = tuple(deepcopy(dict(user)) for user in users)
        self._details = {
            str(user_id): deepcopy(dict(value)) if value is not None else None for user_id, value in details.items()
        }
        self._block_user_id = block_user_id
        self.detail_started = Event()
        self.release_detail = Event()
        self.items: dict[str, tuple[dict[str, Any], ...]] = {}

    def list_users(self) -> Sequence[Mapping[str, Any]]:
        return deepcopy(self._users)

    def get_user(self, user_id: str) -> Mapping[str, Any] | None:
        if user_id == self._block_user_id:
            self.detail_started.set()
            if not self.release_detail.wait(timeout=5):
                raise TimeoutError("fixture detail read was not released")
        value = self._details.get(user_id)
        return deepcopy(value)

    def user_items(self, user_id: str) -> Sequence[Mapping[str, Any]]:
        return deepcopy(self.items.get(user_id, ()))


class RecordingProgress:
    def __init__(self, *, cancel_at_checkpoint: int | None = None) -> None:
        self.cancel_at_checkpoint = cancel_at_checkpoint
        self.checkpoints = 0
        self.updates: list[tuple[int, str]] = []

    def checkpoint(self) -> None:
        self.checkpoints += 1
        if self.checkpoints == self.cancel_at_checkpoint:
            raise PersistentExecutionCancelled("fixture cancellation")

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))


def _seed_user(runtime: DatabaseRuntime, *, template_id: int = 1) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO user_templates (
                    id, name, emby_policy_json, default_expiration_days,
                    source_emby_user_id, emby_configuration_json
                ) VALUES (
                    :template_id, :name, '{"CanDownload": false}'::jsonb,
                    30, :source_user_id, '{}'::jsonb
                )
                """
            ),
            {
                "template_id": template_id,
                "name": f"Template {template_id}",
                "source_user_id": f"source-{template_id}",
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO emby_users (
                    id, name, is_administrator, policy_json, profile_image_tag
                ) VALUES (
                    'fixture-user', 'Fixture User', FALSE,
                    '{"CanDownload": false, "IsDisabled": false}'::jsonb,
                    'old-tag'
                )
                """
            )
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


def _service(runtime: DatabaseRuntime, provider: BlockingUserSyncProvider) -> UserSyncService:
    return UserSyncService(provider, SqlAlchemyUserSyncRepository(runtime.unit_of_work))


def _stale_remote_user() -> dict[str, Any]:
    return {
        "Id": "fixture-user",
        "Name": "Stale Remote User",
        "Policy": {
            "CanDownload": False,
            "IsAdministrator": False,
            "IsDisabled": False,
        },
        "PrimaryImageTag": "stale-tag",
    }


def _advance_projection_clock(connection: Any) -> None:
    connection.execute(
        text(
            """
            UPDATE identity_projection_clock
            SET revision = revision + 1, updated_at = NOW()
            WHERE singleton_id = 1
            """
        )
    )


def test_delete_commit_between_provider_read_and_local_upsert_cannot_resurrect_user(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    stale = _stale_remote_user()
    provider = BlockingUserSyncProvider(
        [stale],
        {"fixture-user": stale},
        block_user_id="fixture-user",
    )
    service = _service(p8_runtime, provider)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(service.execute, RecordingProgress())
        assert provider.detail_started.wait(timeout=5)
        with p8_runtime.engine.begin() as connection:
            _advance_projection_clock(connection)
            connection.execute(
                text(
                    """
                    INSERT INTO identity_user_tombstones (
                        user_id, generation, last_operation_id, created_at, updated_at
                    ) VALUES ('fixture-user', 1, NULL, NOW(), NOW())
                    """
                )
            )
            connection.execute(text("DELETE FROM emby_users WHERE id = 'fixture-user'"))
        provider.release_detail.set()
        outcome = future.result(timeout=5)

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["synchronized"] == 0
    assert outcome.details["skipped_due_to_state_change"] == 1
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM emby_users WHERE id = 'fixture-user'")) == 0
        assert (
            connection.scalar(text("SELECT generation FROM identity_user_tombstones WHERE user_id = 'fixture-user'"))
            == 1
        )


def test_tombstone_fences_provider_snapshot_that_is_stale_before_sync_starts(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    with p8_runtime.engine.begin() as connection:
        _advance_projection_clock(connection)
        connection.execute(
            text(
                """
                INSERT INTO identity_user_tombstones (
                    user_id, generation, last_operation_id, created_at, updated_at
                ) VALUES ('fixture-user', 1, NULL, NOW(), NOW())
                """
            )
        )
        connection.execute(text("DELETE FROM emby_users WHERE id = 'fixture-user'"))
    stale = _stale_remote_user()
    provider = BlockingUserSyncProvider([stale], {"fixture-user": stale})

    outcome = _service(p8_runtime, provider).execute(RecordingProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["synchronized"] == 0
    assert outcome.details["skipped_due_to_state_change"] == 1
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM emby_users WHERE id = 'fixture-user'")) == 0
        assert connection.scalar(text("SELECT COUNT(*) FROM identity_user_tombstones")) == 1


def test_registration_commit_during_absence_read_cannot_be_deleted_by_old_snapshot(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    provider = BlockingUserSyncProvider(
        [],
        {"fixture-user": None},
        block_user_id="fixture-user",
    )
    service = _service(p8_runtime, provider)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(service.execute, RecordingProgress())
        assert provider.detail_started.wait(timeout=5)
        with p8_runtime.engine.begin() as connection:
            _advance_projection_clock(connection)
            connection.execute(
                text(
                    """
                    UPDATE emby_users
                    SET name = 'Registered User',
                        policy_json = '{"CanDownload": true, "IsDisabled": false}'::jsonb,
                        last_updated_at = NOW()
                    WHERE id = 'fixture-user'
                    """
                )
            )
            connection.execute(
                text(
                    """
                    UPDATE emby_users_extended
                    SET status = 'active', created_by = 'self-registered'
                    WHERE emby_user_id = 'fixture-user'
                    """
                )
            )
        provider.release_detail.set()
        outcome = future.result(timeout=5)

    assert outcome.details["deleted"] == 0
    assert outcome.details["skipped_due_to_state_change"] == 1
    with p8_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                    SELECT users.name, users.policy_json, extended.created_by
                    FROM emby_users AS users
                    JOIN emby_users_extended AS extended ON extended.emby_user_id = users.id
                    WHERE users.id = 'fixture-user'
                    """
                )
            )
            .mappings()
            .one()
        )
    assert dict(row) == {
        "name": "Registered User",
        "policy_json": {"CanDownload": True, "IsDisabled": False},
        "created_by": "self-registered",
    }


@pytest.mark.parametrize("mutation", ["assign", "status"])
def test_template_or_status_commit_during_provider_read_wins_over_stale_sync(
    p8_runtime: DatabaseRuntime,
    mutation: str,
) -> None:
    _seed_user(p8_runtime)
    if mutation == "assign":
        with p8_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO user_templates (
                        id, name, emby_policy_json, default_expiration_days,
                        source_emby_user_id, emby_configuration_json
                    ) VALUES (
                        2, 'Template 2', '{"CanDownload": true}'::jsonb,
                        30, 'source-2', '{}'::jsonb
                    )
                    """
                )
            )
    stale = _stale_remote_user()
    provider = BlockingUserSyncProvider(
        [stale],
        {"fixture-user": stale},
        block_user_id="fixture-user",
    )
    service = _service(p8_runtime, provider)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(service.execute, RecordingProgress())
        assert provider.detail_started.wait(timeout=5)
        with p8_runtime.engine.begin() as connection:
            _advance_projection_clock(connection)
            if mutation == "assign":
                connection.execute(
                    text(
                        """
                        UPDATE emby_users
                        SET policy_json = '{"CanDownload": true, "IsDisabled": false}'::jsonb,
                            last_updated_at = NOW()
                        WHERE id = 'fixture-user'
                        """
                    )
                )
                connection.execute(
                    text(
                        """
                        UPDATE emby_users_extended
                        SET template_id = 2, status = 'active'
                        WHERE emby_user_id = 'fixture-user'
                        """
                    )
                )
            else:
                connection.execute(
                    text(
                        """
                        UPDATE emby_users
                        SET policy_json = '{"CanDownload": false, "IsDisabled": true}'::jsonb,
                            last_updated_at = NOW()
                        WHERE id = 'fixture-user'
                        """
                    )
                )
                connection.execute(
                    text(
                        """
                        UPDATE emby_users_extended
                        SET status = 'disabled'
                        WHERE emby_user_id = 'fixture-user'
                        """
                    )
                )
        provider.release_detail.set()
        outcome = future.result(timeout=5)

    assert outcome.details["synchronized"] == 0
    assert outcome.details["skipped_due_to_state_change"] == 1
    with p8_runtime.engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                    SELECT users.policy_json, extended.status, extended.template_id
                    FROM emby_users AS users
                    JOIN emby_users_extended AS extended ON extended.emby_user_id = users.id
                    WHERE users.id = 'fixture-user'
                    """
                )
            )
            .mappings()
            .one()
        )
    if mutation == "assign":
        assert dict(row) == {
            "policy_json": {"CanDownload": True, "IsDisabled": False},
            "status": "active",
            "template_id": 2,
        }
    else:
        assert dict(row) == {
            "policy_json": {"CanDownload": False, "IsDisabled": True},
            "status": "disabled",
            "template_id": 1,
        }


def test_partial_user_detail_does_not_replace_existing_policy(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_user(p8_runtime)
    incomplete = {"Id": "fixture-user", "Name": "Incomplete User"}
    provider = BlockingUserSyncProvider([incomplete], {"fixture-user": incomplete})

    outcome = _service(p8_runtime, provider).execute(RecordingProgress())

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details["failed_user_ids"] == ["fixture-user"]
    assert outcome.details["synchronized"] == 0
    with p8_runtime.engine.connect() as connection:
        row = (
            connection.execute(text("SELECT name, policy_json FROM emby_users WHERE id = 'fixture-user'"))
            .mappings()
            .one()
        )
    assert dict(row) == {
        "name": "Fixture User",
        "policy_json": {"CanDownload": False, "IsDisabled": False},
    }


def test_cancellation_before_user_write_is_restart_safe_and_replay_converges(
    p8_runtime: DatabaseRuntime,
) -> None:
    remote = {
        "Id": "restart-user",
        "Name": "Restart User",
        "Policy": {"CanDownload": True, "IsDisabled": False},
    }
    provider = BlockingUserSyncProvider([remote], {"restart-user": remote})
    service = _service(p8_runtime, provider)

    with pytest.raises(PersistentExecutionCancelled):
        service.execute(RecordingProgress(cancel_at_checkpoint=2))
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM emby_users WHERE id = 'restart-user'")) == 0

    first_restart = service.execute(RecordingProgress())
    second_restart = service.execute(RecordingProgress())

    assert first_restart.status is TaskOutcomeStatus.SUCCEEDED
    assert first_restart.details["synchronized"] == 1
    assert second_restart.status is TaskOutcomeStatus.SUCCEEDED
    assert second_restart.details["synchronized"] == 1
    with p8_runtime.engine.connect() as connection:
        row = (
            connection.execute(text("SELECT name, policy_json FROM emby_users WHERE id = 'restart-user'"))
            .mappings()
            .one()
        )
    assert dict(row) == {
        "name": "Restart User",
        "policy_json": {"CanDownload": True, "IsDisabled": False},
    }
