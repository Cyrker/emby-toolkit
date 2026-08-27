from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

import pytest
from sqlalchemy import text

from etk.domains.identity.repository import SqlAlchemyIdentityRepository
from etk.domains.identity.user_activity_effects import (
    LegacyUserEventEffectsAdapter,
    P8UserEventEffectDelivery,
    SqlAlchemyTemplatePropagationRepository,
    UserPolicyEffectService,
)
from etk.domains.media.effect_execution import PersistentMediaEffectExecutor
from etk.domains.media.effects import MediaEffectStatus, PostgresMediaEffectLedger
from etk.platform.db import DatabaseRuntime


class FixtureProvider:
    def __init__(self, users: Mapping[str, Mapping[str, Any]]) -> None:
        self.users = {user_id: dict(value) for user_id, value in users.items()}
        self.calls: list[str] = []
        self.error: BaseException | None = None

    def get_user(self, user_id: str) -> Mapping[str, Any] | None:
        self.calls.append(user_id)
        if self.error is not None:
            raise self.error
        value = self.users.get(user_id)
        return dict(value) if value is not None else None


def _service(runtime: DatabaseRuntime, provider: FixtureProvider) -> UserPolicyEffectService:
    return UserPolicyEffectService(
        provider,
        SqlAlchemyIdentityRepository(runtime.unit_of_work),
        SqlAlchemyTemplatePropagationRepository(runtime.unit_of_work),
    )


def _seed_template_scope(runtime: DatabaseRuntime, *, template_id: int = 801) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO emby_users (id, name, is_administrator, policy_json)
                VALUES
                    ('source-user', 'Source User', FALSE, '{"CanDownload": false}'::jsonb),
                    ('target-a', 'Target A', FALSE, '{"IsDisabled": true}'::jsonb),
                    ('target-b', 'Target B', FALSE, '{"IsDisabled": false}'::jsonb)
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO user_templates (
                    id, name, emby_policy_json, emby_configuration_json,
                    default_expiration_days, source_emby_user_id
                ) VALUES (
                    :template_id, 'Fixture Template',
                    '{"CanDownload": false}'::jsonb,
                    '{"AudioLanguagePreference": "eng"}'::jsonb,
                    30, 'source-user'
                )
                """
            ),
            {"template_id": template_id},
        )
        connection.execute(
            text(
                """
                INSERT INTO emby_users_extended (emby_user_id, status, template_id)
                VALUES
                    ('target-a', 'active', :template_id),
                    ('target-b', 'active', :template_id)
                """
            ),
            {"template_id": template_id},
        )


def _provider() -> FixtureProvider:
    return FixtureProvider(
        {
            "source-user": {
                "Id": "source-user",
                "Name": "Source User",
                "Policy": {"CanDownload": True, "IsDisabled": False},
                "Configuration": {"AudioLanguagePreference": "jpn"},
            },
            "target-a": {
                "Id": "target-a",
                "Name": "Target A",
                "Policy": {"CanDownload": False, "IsDisabled": True},
                "Configuration": {"AudioLanguagePreference": "eng"},
            },
            "target-b": {
                "Id": "target-b",
                "Name": "Target B",
                "Policy": {"CanDownload": False, "IsDisabled": False},
                "Configuration": {"AudioLanguagePreference": "eng"},
            },
        }
    )


def test_template_policy_snapshot_and_identity_children_commit_atomically(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_template_scope(p8_runtime)
    provider = _provider()
    service = _service(p8_runtime, provider)

    first = service.sync_template_policy("source-user", source_event_key="policy-event")
    replay = service.sync_template_policy("source-user", source_event_key="policy-event")

    assert first == replay
    assert first["templates"] == 1
    assert len(first["operations"]) == 2
    with p8_runtime.engine.connect() as connection:
        template = connection.execute(
            text(
                """
                SELECT emby_policy_json, emby_configuration_json
                FROM user_templates WHERE id = 801
                """
            )
        ).one()
        operations = connection.execute(
            text(
                """
                SELECT request_json
                FROM identity_operations
                WHERE kind = 'assign_template'
                ORDER BY request_json ->> 'user_id'
                """
            )
        ).all()
        assert (
            connection.scalar(text("SELECT COUNT(*) FROM outbox_events WHERE topic = 'identity.operation.requested'"))
            == 2
        )
    assert template.emby_policy_json == {"CanDownload": True, "IsDisabled": False}
    assert template.emby_configuration_json == {"AudioLanguagePreference": "jpn"}
    assert operations[0].request_json["user_id"] == "target-a"
    assert operations[0].request_json["template_policy"]["IsDisabled"] is True
    assert operations[1].request_json["user_id"] == "target-b"
    assert operations[1].request_json["template_policy"]["IsDisabled"] is False
    assert provider.calls == [
        "source-user",
        "target-a",
        "target-b",
        "source-user",
        "target-a",
        "target-b",
    ]


def test_template_policy_outbox_failure_rolls_back_snapshot_and_all_children(
    p8_runtime: DatabaseRuntime,
) -> None:
    _seed_template_scope(p8_runtime, template_id=802)
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE FUNCTION p8_reject_template_child_outbox() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    IF NEW.topic = 'identity.operation.requested' THEN
                        RAISE EXCEPTION 'fixture identity outbox rejected';
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
                CREATE TRIGGER p8_reject_template_child_outbox
                BEFORE INSERT ON outbox_events
                FOR EACH ROW EXECUTE FUNCTION p8_reject_template_child_outbox()
                """
            )
        )
    try:
        with pytest.raises(Exception, match="fixture identity outbox rejected"):
            _service(p8_runtime, _provider()).sync_template_policy(
                "source-user",
                source_event_key="rollback-policy-event",
            )
        with p8_runtime.engine.connect() as connection:
            template = connection.execute(
                text(
                    """
                    SELECT emby_policy_json, emby_configuration_json
                    FROM user_templates WHERE id = 802
                    """
                )
            ).one()
            assert connection.scalar(text("SELECT COUNT(*) FROM identity_operations")) == 0
            assert connection.scalar(text("SELECT COUNT(*) FROM outbox_events")) == 0
        assert template.emby_policy_json == {"CanDownload": False}
        assert template.emby_configuration_json == {"AudioLanguagePreference": "eng"}
    finally:
        with p8_runtime.engine.begin() as connection:
            connection.execute(text("DROP TRIGGER IF EXISTS p8_reject_template_child_outbox ON outbox_events"))
            connection.execute(text("DROP FUNCTION IF EXISTS p8_reject_template_child_outbox()"))


def test_refresh_user_policy_uses_projection_revision_and_authoritative_snapshot(
    p8_runtime: DatabaseRuntime,
) -> None:
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO emby_users (id, name, is_administrator, policy_json)
                VALUES ('refresh-user', 'Old Name', FALSE, '{}'::jsonb)
                """
            )
        )
    provider = FixtureProvider(
        {
            "refresh-user": {
                "Id": "refresh-user",
                "Name": "Fresh Name",
                "Policy": {"IsAdministrator": True, "IsDisabled": False},
            }
        }
    )

    result = _service(p8_runtime, provider).refresh_user_policy("refresh-user")

    assert result == {"user_id": "refresh-user", "updated": True}
    with p8_runtime.engine.connect() as connection:
        assert connection.execute(
            text("SELECT name, is_administrator, policy_json FROM emby_users WHERE id = 'refresh-user'")
        ).one() == ("Fresh Name", True, {"IsAdministrator": True, "IsDisabled": False})


def test_policy_media_effect_retries_prewrite_provider_failure_then_confirms(
    p8_runtime: DatabaseRuntime,
) -> None:
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO emby_users (id, name, is_administrator, policy_json)
                VALUES ('retry-user', 'Retry User', FALSE, '{}'::jsonb)
                """
            )
        )
    provider = FixtureProvider(
        {
            "retry-user": {
                "Id": "retry-user",
                "Name": "Retry User",
                "Policy": {"IsAdministrator": False, "IsDisabled": False},
            }
        }
    )
    provider.error = RuntimeError("fixture Emby timeout before write")
    ledger = PostgresMediaEffectLedger(p8_runtime.unit_of_work)
    reservation = ledger.reserve(
        effect_key="p8:retry-user-policy",
        effect_type="emby.event.user.refresh-user-policy",
        aggregate_type="emby_user_event_effect",
        aggregate_id="retry-user",
        payload={"source_event_key": "retry-event", "user_id": "retry-user"},
    )
    executor = PersistentMediaEffectExecutor(
        ledger,
        P8UserEventEffectDelivery(
            _service(p8_runtime, provider),
            cast(LegacyUserEventEffectsAdapter, object()),
        ),
        worker_id="p8-user-effect",
        lease_seconds=30,
    )

    first = executor.execute(reservation.effect.id)
    assert first.status is MediaEffectStatus.RETRYABLE
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE media_effects SET available_at = NOW() WHERE id = :id"),
            {"id": first.id},
        )
    provider.error = None
    second = executor.execute(reservation.effect.id)

    assert second.status is MediaEffectStatus.CONFIRMED
    assert second.attempts == 2
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT status FROM outbox_events WHERE aggregate_id = :id"),
                {"id": str(second.id)},
            )
            == "processed"
        )
