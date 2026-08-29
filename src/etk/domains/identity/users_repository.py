from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etk.platform.db import SqlAlchemyUnitOfWork


class UserTemplateNameConflictError(RuntimeError):
    pass


class IdentityReservationConflictError(RuntimeError):
    pass


class IdentityUserLifecycleBusyError(RuntimeError):
    pass


class SqlAlchemyUsersRepository:
    """P8 identity owner for templates, invitations, profiles, and user lifecycle state."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        token_factory: Callable[[], str] = lambda: str(uuid4()),
    ) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._token_factory = token_factory

    def list_templates(self) -> list[dict[str, Any]]:
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    """
                    SELECT id, name, description, default_expiration_days,
                           source_emby_user_id, allow_unrestricted_subscriptions,
                           COALESCE(personal_play_account_only, FALSE) AS personal_play_account_only,
                           COALESCE(max_concurrent_streams, 0) AS max_concurrent_streams
                    FROM user_templates
                    ORDER BY name
                    """
                )
            ).mappings()
            return [dict(row) for row in rows]

    def template_effect(self, template_id: int) -> dict[str, Any] | None:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT id, name, emby_policy_json, emby_configuration_json
                        FROM user_templates WHERE id = :template_id
                        """
                    ),
                    {"template_id": template_id},
                )
                .mappings()
                .first()
            )
            return dict(row) if row is not None else None

    def managed_user_projection(self) -> tuple[set[str], dict[str, dict[str, Any]]]:
        with self._unit_of_work() as uow:
            source_ids = {
                str(row[0])
                for row in uow.session.execute(
                    text(
                        """
                        SELECT source_emby_user_id FROM user_templates
                        WHERE source_emby_user_id IS NOT NULL
                        """
                    )
                )
            }
            rows = uow.session.execute(
                text(
                    """
                    SELECT extended.*, templates.name AS template_name,
                           COALESCE(templates.max_concurrent_streams, 0)
                               AS max_concurrent_streams
                    FROM emby_users_extended AS extended
                    LEFT JOIN user_templates AS templates ON extended.template_id = templates.id
                    """
                )
            ).mappings()
            return source_ids, {str(row["emby_user_id"]): dict(row) for row in rows}

    def managed_user_exists(self, user_id: str) -> bool:
        normalized_user_id = str(user_id or "").strip()
        if not normalized_user_id:
            return False
        with self._unit_of_work() as uow:
            return bool(
                uow.session.scalar(
                    text("SELECT EXISTS (SELECT 1 FROM emby_users WHERE id = :user_id)"),
                    {"user_id": normalized_user_id},
                )
            )

    def adopt_remote_user(self, user: Mapping[str, Any]) -> bool:
        user_id = str(user.get("Id") or "").strip()
        username = str(user.get("Name") or "").strip()
        if not user_id or not username:
            raise ValueError("remote Emby user requires Id and Name")
        raw_policy = user.get("Policy")
        policy = dict(raw_policy) if isinstance(raw_policy, Mapping) else {}
        with self._unit_of_work() as uow:
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:username_scope, 0))"),
                {"username_scope": f"identity:registration:username:{username.casefold()}"},
            )
            registration_pending = uow.session.scalar(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM identity_operations
                        WHERE kind = 'register'
                          AND lower(request_json ->> 'username') = :username
                          AND status IN (
                              'ready','claimed','requesting','awaiting_readback','retryable',
                              'compensating','remote_confirmed','indeterminate'
                          )
                    )
                    """
                ),
                {"username": username.casefold()},
            )
            if registration_pending:
                return False
            uow.session.execute(
                text(
                    """
                    INSERT INTO emby_users (
                        id, name, is_administrator, profile_image_tag,
                        policy_json, last_updated_at
                    ) VALUES (
                        :id, :name, :is_administrator, :profile_image_tag,
                        CAST(:policy_json AS jsonb), NOW()
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        is_administrator = EXCLUDED.is_administrator,
                        profile_image_tag = EXCLUDED.profile_image_tag,
                        policy_json = EXCLUDED.policy_json,
                        last_updated_at = NOW()
                    """
                ),
                {
                    "id": user_id,
                    "name": username,
                    "is_administrator": bool(policy.get("IsAdministrator", False)),
                    "profile_image_tag": user.get("PrimaryImageTag"),
                    "policy_json": _json_text(policy),
                },
            )
            uow.commit()
            return True

    def profile_image_tag(self, user_id: str) -> str | None:
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text("SELECT profile_image_tag FROM emby_users WHERE id = :user_id"),
                {"user_id": user_id},
            )
            return str(value).strip() if value is not None and str(value).strip() else None

    def create_template(
        self,
        *,
        name: str,
        description: str | None,
        policy: Mapping[str, Any],
        default_expiration_days: int,
        source_emby_user_id: str,
        configuration: Mapping[str, Any] | None,
        allow_unrestricted_subscriptions: bool,
        personal_play_account_only: bool,
        max_concurrent_streams: int,
    ) -> int:
        try:
            with self._unit_of_work() as uow:
                value = uow.session.scalar(
                    text(
                        """
                        INSERT INTO user_templates (
                            name, description, emby_policy_json,
                            default_expiration_days, source_emby_user_id,
                            emby_configuration_json, allow_unrestricted_subscriptions,
                            personal_play_account_only, max_concurrent_streams
                        ) VALUES (
                            :name, :description, CAST(:policy AS jsonb),
                            :default_expiration_days, :source_emby_user_id,
                            CAST(:configuration AS jsonb), :allow_unrestricted_subscriptions,
                            :personal_play_account_only, :max_concurrent_streams
                        )
                        RETURNING id
                        """
                    ),
                    {
                        "name": name,
                        "description": description,
                        "policy": _json_text(policy),
                        "default_expiration_days": default_expiration_days,
                        "source_emby_user_id": source_emby_user_id,
                        "configuration": _json_text(configuration) if configuration is not None else None,
                        "allow_unrestricted_subscriptions": allow_unrestricted_subscriptions,
                        "personal_play_account_only": personal_play_account_only,
                        "max_concurrent_streams": max_concurrent_streams,
                    },
                )
                uow.commit()
                if value is None:
                    raise RuntimeError("user template insert did not return an id")
                return int(value)
        except IntegrityError as exc:
            raise UserTemplateNameConflictError(name) from exc

    def update_template(
        self,
        template_id: int,
        *,
        name: str,
        description: str | None,
        default_expiration_days: int | None,
        allow_unrestricted_subscriptions: bool,
        personal_play_account_only: bool,
        max_concurrent_streams: int,
    ) -> bool:
        try:
            with self._unit_of_work() as uow:
                result = uow.session.execute(
                    text(
                        """
                        UPDATE user_templates
                        SET name = :name,
                            description = :description,
                            default_expiration_days = :default_expiration_days,
                            allow_unrestricted_subscriptions = :allow_unrestricted_subscriptions,
                            personal_play_account_only = :personal_play_account_only,
                            max_concurrent_streams = :max_concurrent_streams
                        WHERE id = :template_id
                        """
                    ),
                    {
                        "template_id": template_id,
                        "name": name,
                        "description": description,
                        "default_expiration_days": default_expiration_days,
                        "allow_unrestricted_subscriptions": allow_unrestricted_subscriptions,
                        "personal_play_account_only": personal_play_account_only,
                        "max_concurrent_streams": max_concurrent_streams,
                    },
                )
                uow.commit()
                return bool(int(getattr(result, "rowcount", 0) or 0))
        except IntegrityError as exc:
            raise UserTemplateNameConflictError(name) from exc

    def delete_template(self, template_id: int) -> bool:
        with self._unit_of_work() as uow:
            template_exists = uow.session.scalar(
                text("SELECT id FROM user_templates WHERE id = :template_id FOR UPDATE"),
                {"template_id": template_id},
            )
            if template_exists is None:
                return False
            tuple(
                uow.session.scalars(
                    text(
                        """
                        SELECT id FROM invitations
                        WHERE template_id = :template_id
                        ORDER BY id FOR UPDATE
                        """
                    ),
                    {"template_id": template_id},
                )
            )
            blocked = uow.session.scalar(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM invitations AS invitation
                        JOIN identity_operations AS operation
                          ON operation.id = invitation.reservation_id
                        WHERE invitation.template_id = :template_id
                          AND operation.status NOT IN (
                              'confirmed','rejected','compensated','cancelled'
                          )
                    )
                    """
                ),
                {"template_id": template_id},
            )
            if blocked:
                raise IdentityReservationConflictError("template has an active or indeterminate invitation operation")
            active_assignment = uow.session.scalar(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM identity_operations
                        WHERE kind = 'assign_template'
                          AND request_json ->> 'template_id' = CAST(:template_id AS text)
                          AND status IN (
                              'ready','claimed','requesting','awaiting_readback','retryable',
                              'compensating','remote_confirmed','indeterminate'
                          )
                    )
                    """
                ),
                {"template_id": template_id},
            )
            if active_assignment:
                raise IdentityReservationConflictError("template has an active or indeterminate assignment")
            result = uow.session.execute(
                text("DELETE FROM user_templates WHERE id = :template_id"),
                {"template_id": template_id},
            )
            uow.commit()
            return bool(int(getattr(result, "rowcount", 0) or 0))

    def create_invitations(
        self,
        *,
        template_id: int,
        expiration_days: int | None,
        link_expires_in_days: int,
        count: int,
    ) -> list[str]:
        with self._unit_of_work() as uow:
            template_default = uow.session.scalar(
                text("SELECT default_expiration_days FROM user_templates WHERE id = :template_id"),
                {"template_id": template_id},
            )
            if template_default is None:
                raise LookupError("模板不存在")
            final_expiration_days = int(template_default if expiration_days is None else expiration_days)
            expires_at = self._clock() + timedelta(days=link_expires_in_days)
            tokens: list[str] = []
            for _ in range(count):
                token = str(self._token_factory()).strip()
                if not token:
                    raise RuntimeError("invitation token factory returned an empty value")
                uow.session.execute(
                    text(
                        """
                        INSERT INTO invitations (
                            token, template_id, expiration_days, expires_at, status
                        ) VALUES (
                            :token, :template_id, :expiration_days, :expires_at, 'active'
                        )
                        """
                    ),
                    {
                        "token": token,
                        "template_id": template_id,
                        "expiration_days": final_expiration_days,
                        "expires_at": expires_at,
                    },
                )
                tokens.append(token)
            uow.commit()
        return tokens

    def list_invitations(self) -> list[dict[str, Any]]:
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    """
                    SELECT invitations.*, templates.name AS template_name,
                           users.name AS used_by_username
                    FROM invitations
                    JOIN user_templates AS templates ON invitations.template_id = templates.id
                    LEFT JOIN emby_users AS users ON invitations.used_by_user_id = users.id
                    ORDER BY invitations.created_at DESC
                    """
                )
            ).mappings()
            return [dict(row) for row in rows]

    def delete_invitation(self, invitation_id: int) -> bool:
        with self._unit_of_work() as uow:
            invitation_exists = uow.session.scalar(
                text("SELECT id FROM invitations WHERE id = :invitation_id FOR UPDATE"),
                {"invitation_id": invitation_id},
            )
            if invitation_exists is None:
                return False
            blocked = uow.session.scalar(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM invitations AS invitation
                        JOIN identity_operations AS operation
                          ON operation.id = invitation.reservation_id
                        WHERE invitation.id = :invitation_id
                          AND operation.status NOT IN (
                              'confirmed','rejected','compensated','cancelled'
                          )
                    )
                    """
                ),
                {"invitation_id": invitation_id},
            )
            if blocked:
                raise IdentityReservationConflictError("invitation has an active or indeterminate operation")
            result = uow.session.execute(
                text("DELETE FROM invitations WHERE id = :invitation_id"),
                {"invitation_id": invitation_id},
            )
            uow.commit()
            return bool(int(getattr(result, "rowcount", 0) or 0))

    def invitation_status(self, token: str) -> Mapping[str, Any] | None:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text("SELECT status, expires_at FROM invitations WHERE token = :token"),
                    {"token": token},
                )
                .mappings()
                .first()
            )
            return dict(row) if row is not None else None

    def set_expiration(self, user_id: str, expiration_date: str | None) -> None:
        normalized_user_id = str(user_id or "").strip()
        if not normalized_user_id:
            raise ValueError("identity user id must not be empty")
        with self._unit_of_work() as uow:
            scope_key = f"user:{normalized_user_id}"
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:scope_key, 0))"),
                {"scope_key": scope_key},
            )
            expiry_provider_started = uow.session.scalar(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM identity_operations
                        WHERE scope_key = :scope_key
                          AND kind = 'expire_user'
                          AND status IN (
                              'claimed','requesting','awaiting_readback','retryable',
                              'remote_confirmed','indeterminate'
                          )
                          AND (
                              current_step <> 'reserved'
                              OR step_started_at IS NOT NULL
                          )
                    )
                    """
                ),
                {"scope_key": scope_key},
            )
            if expiry_provider_started:
                raise IdentityUserLifecycleBusyError("identity user expiry has reached the provider-write boundary")
            uow.session.execute(
                text(
                    """
                    INSERT INTO emby_users_extended (
                        emby_user_id, status, created_by, expiration_date
                    ) VALUES (
                        :user_id, 'active', 'admin-assigned', :expiration_date
                    )
                    ON CONFLICT (emby_user_id) DO UPDATE SET
                        expiration_date = EXCLUDED.expiration_date
                    """
                ),
                {"user_id": normalized_user_id, "expiration_date": expiration_date or None},
            )
            uow.commit()

    def account_info(self, user_id: str) -> dict[str, Any] | None:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT users.id, users.name, users.is_administrator,
                               users.profile_image_tag, extended.status,
                               extended.registration_date, extended.expiration_date,
                               extended.notes, extended.telegram_chat_id,
                               templates.name AS template_name,
                               templates.description AS template_description,
                               templates.allow_unrestricted_subscriptions,
                               COALESCE(templates.personal_play_account_only, FALSE)
                                   AS personal_play_account_only,
                               COALESCE(templates.max_concurrent_streams, 0)
                                   AS max_concurrent_streams
                        FROM emby_users AS users
                        LEFT JOIN emby_users_extended AS extended
                          ON users.id = extended.emby_user_id
                        LEFT JOIN user_templates AS templates
                          ON extended.template_id = templates.id
                        WHERE users.id = :user_id
                        """
                    ),
                    {"user_id": user_id},
                )
                .mappings()
                .first()
            )
            return dict(row) if row is not None else None

    def set_telegram_chat_id(self, user_id: str, chat_id: str | None) -> None:
        with self._unit_of_work() as uow:
            uow.session.execute(
                text(
                    """
                    INSERT INTO emby_users_extended (emby_user_id, telegram_chat_id)
                    VALUES (:user_id, :chat_id)
                    ON CONFLICT (emby_user_id) DO UPDATE SET
                        telegram_chat_id = EXCLUDED.telegram_chat_id
                    """
                ),
                {"user_id": user_id, "chat_id": chat_id or None},
            )
            uow.commit()

    def update_profile_image_tag(self, user_id: str, image_tag: str | None) -> bool:
        with self._unit_of_work() as uow:
            result = uow.session.execute(
                text(
                    """
                    UPDATE emby_users
                    SET profile_image_tag = :image_tag, last_updated_at = NOW()
                    WHERE id = :user_id
                    """
                ),
                {"user_id": user_id, "image_tag": image_tag},
            )
            uow.commit()
            return bool(int(getattr(result, "rowcount", 0) or 0))

    def delete_local_user(self, user_id: str) -> bool:
        """Delete the identity and its non-FK activity rows in one transaction."""
        with self._unit_of_work() as uow:
            uow.session.execute(text("DELETE FROM emby_favorite_items WHERE user_id = :user_id"), {"user_id": user_id})
            uow.session.execute(text("DELETE FROM user_media_data WHERE user_id = :user_id"), {"user_id": user_id})
            result = uow.session.execute(
                text("DELETE FROM emby_users WHERE id = :user_id"),
                {"user_id": user_id},
            )
            uow.commit()
            return bool(int(getattr(result, "rowcount", 0) or 0))


def _json_text(value: Mapping[str, Any]) -> str:
    import json

    return json.dumps(dict(value), ensure_ascii=False)


__all__ = [
    "IdentityReservationConflictError",
    "IdentityUserLifecycleBusyError",
    "SqlAlchemyUsersRepository",
    "UserTemplateNameConflictError",
]
