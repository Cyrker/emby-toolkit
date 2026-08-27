from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Any

from sqlalchemy import bindparam, text

from etk.platform.db import SqlAlchemyUnitOfWork


class SqlAlchemyIdentityRepository:
    """Identity reads and login-time user synchronization on the P2 UoW."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def projection_revision(self) -> int:
        with self._unit_of_work() as uow:
            value = uow.session.scalar(text("SELECT revision FROM identity_projection_clock WHERE singleton_id = 1"))
        if value is None:
            raise RuntimeError("identity projection clock is missing")
        return int(value)

    def upsert_user(self, user: Mapping[str, Any], *, expected_revision: int) -> bool:
        user_id = str(user.get("Id") or "").strip()
        name = str(user.get("Name") or "").strip()
        if not user_id or not name:
            raise ValueError("Emby user identity requires Id and Name")
        raw_policy = user.get("Policy")
        policy: Mapping[str, Any] = raw_policy if isinstance(raw_policy, Mapping) else {}
        with self._unit_of_work() as uow:
            revision = uow.session.scalar(
                text(
                    """
                    SELECT revision FROM identity_projection_clock
                    WHERE singleton_id = 1
                    FOR UPDATE
                    """
                )
            )
            if revision is None or int(revision) != int(expected_revision):
                return False
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
                {"scope": f"identity:user:{user_id}"},
            )
            blocked = uow.session.scalar(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM identity_operations
                        WHERE request_json ->> 'user_id' = :user_id
                          AND status IN (
                              'ready','claimed','requesting','awaiting_readback','retryable',
                              'compensating','remote_confirmed','indeterminate'
                          )
                        UNION ALL
                        SELECT 1 FROM identity_user_tombstones WHERE user_id = :user_id
                    )
                    """
                ),
                {"user_id": user_id},
            )
            if blocked:
                return False
            uow.session.execute(
                text(
                    """
                    INSERT INTO emby_users (
                        id, name, is_administrator, last_seen_at,
                        profile_image_tag, policy_json, last_updated_at
                    ) VALUES (
                        :id, :name, :is_administrator, :last_seen_at,
                        :profile_image_tag, CAST(:policy_json AS jsonb), NOW()
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        is_administrator = EXCLUDED.is_administrator,
                        last_seen_at = EXCLUDED.last_seen_at,
                        profile_image_tag = EXCLUDED.profile_image_tag,
                        policy_json = EXCLUDED.policy_json,
                        last_updated_at = NOW()
                    """
                ),
                {
                    "id": user_id,
                    "name": name,
                    "is_administrator": bool(policy.get("IsAdministrator", False)),
                    "last_seen_at": _optional_datetime(user.get("LastActivityDate")),
                    "profile_image_tag": user.get("PrimaryImageTag"),
                    "policy_json": json.dumps(dict(policy), ensure_ascii=False),
                },
            )
            uow.commit()
        return True

    def registration_pending(self, username: str) -> bool:
        normalized_username = str(username or "").strip()
        if not normalized_username:
            return False
        with self._unit_of_work() as uow:
            return bool(
                uow.session.scalar(
                    text(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM identity_operations
                            WHERE kind = 'register'
                              AND lower(request_json ->> 'username') = :username
                              AND status IN (
                                  'ready','claimed','requesting','awaiting_readback','retryable',
                                  'compensating','remote_confirmed','indeterminate'
                              )
                        )
                        """
                    ),
                    {"username": normalized_username.casefold()},
                )
            )

    def is_admin(self, user_id: str) -> bool:
        if not user_id:
            return False
        value: Any = False
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text(
                    """
                    SELECT users.is_administrator
                    FROM emby_users AS users
                    LEFT JOIN emby_users_extended AS extended
                      ON extended.emby_user_id = users.id
                    WHERE users.id = :user_id
                      AND COALESCE(extended.status, 'active') = 'active'
                      AND lower(COALESCE(users.policy_json ->> 'IsDisabled', 'false'))
                          NOT IN ('true','1','yes','on')
                    """
                ),
                {"user_id": user_id},
            )
        return bool(value)

    def is_active(self, user_id: str) -> bool:
        if not user_id:
            return False
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM emby_users AS users
                        LEFT JOIN emby_users_extended AS extended
                          ON extended.emby_user_id = users.id
                        WHERE users.id = :user_id
                          AND COALESCE(extended.status, 'active') = 'active'
                          AND lower(COALESCE(users.policy_json ->> 'IsDisabled', 'false'))
                              NOT IN ('true','1','yes','on')
                    )
                    """
                ),
                {"user_id": user_id},
            )
        return bool(value)

    def subscription_permission(self, user_id: str) -> bool:
        if not user_id:
            return False
        value: Any = False
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text(
                    """
                    SELECT templates.allow_unrestricted_subscriptions
                    FROM emby_users_extended AS users
                    JOIN user_templates AS templates ON users.template_id = templates.id
                    WHERE users.emby_user_id = :user_id
                    """
                ),
                {"user_id": user_id},
            )
        return bool(value)

    def display_names(self, user_ids: Sequence[str]) -> Mapping[str, str]:
        normalized = tuple(dict.fromkeys(str(user_id).strip() for user_id in user_ids if str(user_id).strip()))
        if not normalized:
            return {}
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text("SELECT id, name FROM emby_users WHERE id IN :ids").bindparams(bindparam("ids", expanding=True)),
                {"ids": normalized},
            ).mappings()
            names = {str(row["id"]): str(row["name"]) for row in rows}
        return names


def _optional_datetime(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    text_value = str(value).strip()
    if not text_value:
        return None
    try:
        return datetime.fromisoformat(text_value.replace("Z", "+00:00"))
    except ValueError:
        return None
