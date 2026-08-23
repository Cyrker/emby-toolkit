from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork


class SqlAlchemyIdentityRepository:
    """Identity reads and login-time user synchronization on the P2 UoW."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def upsert_user(self, user: Mapping[str, Any]) -> None:
        user_id = str(user.get("Id") or "").strip()
        name = str(user.get("Name") or "").strip()
        if not user_id or not name:
            raise ValueError("Emby user identity requires Id and Name")
        raw_policy = user.get("Policy")
        policy: Mapping[str, Any] = raw_policy if isinstance(raw_policy, Mapping) else {}
        with self._unit_of_work() as uow:
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

    def is_admin(self, user_id: str) -> bool:
        if not user_id:
            return False
        value: Any = False
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text("SELECT is_administrator FROM emby_users WHERE id = :user_id"),
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
