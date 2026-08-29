from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Any, Protocol

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.tasks import TaskOutcome


class UserSyncProvider(Protocol):
    def list_users(self) -> Sequence[Mapping[str, Any]]: ...

    def get_user(self, user_id: str) -> Mapping[str, Any] | None: ...

    def user_items(self, user_id: str) -> Sequence[Mapping[str, Any]]: ...


class UserSyncProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


class SqlAlchemyUserSyncRepository:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def projection_revision(self) -> int:
        with self._unit_of_work() as uow:
            value = uow.session.scalar(text("SELECT revision FROM identity_projection_clock WHERE singleton_id = 1"))
        if value is None:
            raise RuntimeError("identity projection clock is missing")
        return int(value)

    def local_user_ids(self) -> set[str]:
        with self._unit_of_work() as uow:
            return {str(value) for value in uow.session.scalars(text("SELECT id FROM emby_users"))}

    def upsert_snapshot(
        self,
        user: Mapping[str, Any],
        items: Sequence[Mapping[str, Any]],
        *,
        expected_revision: int,
    ) -> bool:
        user_id = str(user.get("Id") or "").strip()
        username = str(user.get("Name") or "").strip()
        if not user_id or not username:
            raise ValueError("user sync snapshot requires Id and Name")
        raw_policy = user.get("Policy")
        if not isinstance(raw_policy, Mapping):
            raise ValueError("user sync snapshot requires an authoritative Policy")
        policy = dict(raw_policy)
        disabled = bool(policy.get("IsDisabled", False))
        rows = _aggregate_user_items(items)
        with self._unit_of_work() as uow:
            if not _lock_expected_revision(uow, expected_revision):
                return False
            _lock_user_scope(uow, user_id)
            if _has_active_operation(uow, user_id) or _has_user_tombstone(uow, user_id):
                return False
            uow.session.execute(
                text(
                    """
                    INSERT INTO emby_users (
                        id, name, is_administrator, last_seen_at,
                        profile_image_tag, policy_json, last_updated_at
                    ) VALUES (
                        :user_id, :name, :is_administrator, :last_seen_at,
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
                    "user_id": user_id,
                    "name": username,
                    "is_administrator": bool(policy.get("IsAdministrator", False)),
                    "last_seen_at": _optional_datetime(user.get("LastActivityDate")),
                    "profile_image_tag": user.get("PrimaryImageTag"),
                    "policy_json": json.dumps(policy, ensure_ascii=False, separators=(",", ":")),
                },
            )
            uow.session.execute(
                text(
                    """
                    INSERT INTO emby_users_extended (
                        emby_user_id, registration_date, status, created_by
                    ) VALUES (
                        :user_id, :registration_date, :status, 'system_sync'
                    )
                    ON CONFLICT (emby_user_id) DO UPDATE SET
                        registration_date = COALESCE(
                            EXCLUDED.registration_date,
                            emby_users_extended.registration_date
                        ),
                        status = CASE
                            WHEN :disabled AND emby_users_extended.status <> 'expired' THEN 'disabled'
                            ELSE emby_users_extended.status
                        END
                    """
                ),
                {
                    "user_id": user_id,
                    "registration_date": _optional_datetime(user.get("DateCreated")),
                    "status": "disabled" if disabled else "active",
                    "disabled": disabled,
                },
            )
            for row in rows:
                uow.session.execute(
                    text(
                        """
                        INSERT INTO user_media_data (
                            user_id, item_id, is_favorite, played,
                            playback_position_ticks, play_count, last_played_date,
                            last_updated_at
                        ) VALUES (
                            :user_id, :item_id, :is_favorite, :played,
                            :playback_position_ticks, :play_count, :last_played_date,
                            NOW()
                        )
                        ON CONFLICT (user_id, item_id) DO UPDATE SET
                            is_favorite = EXCLUDED.is_favorite,
                            played = EXCLUDED.played,
                            playback_position_ticks = EXCLUDED.playback_position_ticks,
                            play_count = EXCLUDED.play_count,
                            last_played_date = EXCLUDED.last_played_date,
                            last_updated_at = NOW()
                        """
                    ),
                    {"user_id": user_id, **row},
                )
            _advance_projection_revision(uow)
            uow.commit()
        return True

    def delete_absent_user(self, user_id: str, *, expected_revision: int) -> bool:
        normalized = str(user_id or "").strip()
        if not normalized:
            return False
        with self._unit_of_work() as uow:
            if not _lock_expected_revision(uow, expected_revision):
                return False
            _lock_user_scope(uow, normalized)
            if _has_active_operation(uow, normalized):
                return False
            deleted = uow.session.execute(
                text("DELETE FROM emby_users WHERE id = :user_id"),
                {"user_id": normalized},
            )
            if int(getattr(deleted, "rowcount", 0) or 0) != 1:
                return False
            uow.session.execute(
                text(
                    """
                    INSERT INTO identity_user_tombstones (
                        user_id, generation, last_operation_id, created_at, updated_at
                    ) VALUES (:user_id, 1, NULL, NOW(), NOW())
                    ON CONFLICT (user_id) DO UPDATE SET
                        generation = identity_user_tombstones.generation + 1,
                        last_operation_id = NULL,
                        updated_at = NOW()
                    """
                ),
                {"user_id": normalized},
            )
            _advance_projection_revision(uow)
            uow.commit()
        return True


class UserSyncService:
    def __init__(self, provider: UserSyncProvider, repository: SqlAlchemyUserSyncRepository) -> None:
        self._provider = provider
        self._repository = repository

    def execute(self, progress: UserSyncProgress) -> TaskOutcome:
        progress.checkpoint()
        progress.update(0, "正在读取 Emby 用户列表")
        users = tuple(dict(user) for user in self._provider.list_users())
        remote_ids = {str(user.get("Id") or "").strip() for user in users}
        remote_ids.discard("")
        local_ids = self._repository.local_user_ids()
        deleted = 0
        synchronized = 0
        skipped = 0
        failed: list[str] = []

        for user_id in sorted(local_ids - remote_ids):
            progress.checkpoint()
            revision = self._repository.projection_revision()
            try:
                current = self._provider.get_user(user_id)
            except Exception:
                failed.append(user_id)
                continue
            if current is None and self._repository.delete_absent_user(
                user_id,
                expected_revision=revision,
            ):
                deleted += 1
            else:
                skipped += 1

        total = max(len(users), 1)
        for index, basic in enumerate(users, 1):
            progress.checkpoint()
            user_id = str(basic.get("Id") or "").strip()
            if not user_id:
                skipped += 1
                continue
            revision = self._repository.projection_revision()
            try:
                current = self._provider.get_user(user_id)
                if current is None:
                    skipped += 1
                    continue
                items = self._provider.user_items(user_id)
                if self._repository.upsert_snapshot(
                    current,
                    items,
                    expected_revision=revision,
                ):
                    synchronized += 1
                else:
                    skipped += 1
            except Exception:
                failed.append(user_id)
            progress.update(
                min(99, int(index / total * 100)),
                f"正在同步用户 {index}/{len(users)}",
            )

        details = {
            "remote_users": len(users),
            "synchronized": synchronized,
            "deleted": deleted,
            "skipped_due_to_state_change": skipped,
            "failed_user_ids": failed,
        }
        if failed:
            return TaskOutcome.partial("用户同步部分完成", details)
        progress.update(100, "用户同步完成")
        return TaskOutcome.succeeded("用户同步完成", details)


def _lock_expected_revision(uow: SqlAlchemyUnitOfWork, expected_revision: int) -> bool:
    value = uow.session.scalar(
        text(
            """
            SELECT revision FROM identity_projection_clock
            WHERE singleton_id = 1
            FOR UPDATE
            """
        )
    )
    return value is not None and int(value) == int(expected_revision)


def _advance_projection_revision(uow: SqlAlchemyUnitOfWork) -> None:
    updated = uow.session.execute(
        text(
            """
            UPDATE identity_projection_clock
            SET revision = revision + 1, updated_at = NOW()
            WHERE singleton_id = 1
            """
        )
    )
    if int(getattr(updated, "rowcount", 0) or 0) != 1:
        raise RuntimeError("identity projection clock update failed")


def _lock_user_scope(uow: SqlAlchemyUnitOfWork, user_id: str) -> None:
    uow.session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
        {"scope": f"identity:user:{user_id}"},
    )


def _has_active_operation(uow: SqlAlchemyUnitOfWork, user_id: str) -> bool:
    return bool(
        uow.session.scalar(
            text(
                """
                SELECT EXISTS (
                    SELECT 1 FROM identity_operations
                    WHERE request_json ->> 'user_id' = :user_id
                      AND status IN (
                          'ready','claimed','requesting','awaiting_readback','retryable',
                          'compensating','remote_confirmed','indeterminate'
                      )
                )
                """
            ),
            {"user_id": user_id},
        )
    )


def _has_user_tombstone(uow: SqlAlchemyUnitOfWork, user_id: str) -> bool:
    return bool(
        uow.session.scalar(
            text("SELECT EXISTS (SELECT 1 FROM identity_user_tombstones WHERE user_id = :user_id)"),
            {"user_id": user_id},
        )
    )


def _aggregate_user_items(items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    aggregated: dict[str, dict[str, Any]] = {}
    for item in items:
        item_type = str(item.get("Type") or "")
        item_id = str(item.get("Id") or "").strip()
        target_id = item_id if item_type in {"Movie", "Series"} else str(item.get("SeriesId") or "").strip()
        if not target_id:
            continue
        raw_data = item.get("UserData")
        user_data = raw_data if isinstance(raw_data, Mapping) else {}
        play_count = max(int(user_data.get("PlayCount") or 0), int(bool(user_data.get("Played"))))
        current = aggregated.setdefault(
            target_id,
            {
                "item_id": target_id,
                "is_favorite": False,
                "played": False,
                "playback_position_ticks": 0,
                "play_count": 0,
                "last_played_date": None,
            },
        )
        current["is_favorite"] = bool(current["is_favorite"] or user_data.get("IsFavorite"))
        current["played"] = bool(current["played"] or user_data.get("Played"))
        current["playback_position_ticks"] = max(
            int(current["playback_position_ticks"]),
            int(user_data.get("PlaybackPositionTicks") or 0),
        )
        current["play_count"] = int(current["play_count"]) + play_count
        observed = _optional_datetime(user_data.get("LastPlayedDate"))
        existing = current["last_played_date"]
        if observed is not None and (existing is None or observed > existing):
            current["last_played_date"] = observed
    return list(aggregated.values())


def _optional_datetime(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    normalized = str(value).strip()
    if not normalized:
        return None
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None


__all__ = [
    "SqlAlchemyUserSyncRepository",
    "UserSyncProgress",
    "UserSyncProvider",
    "UserSyncService",
]
