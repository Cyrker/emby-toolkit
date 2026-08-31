from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, text

from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs.json_payload import canonical_json

from .playback import has_account_auth, normalize_owner_type, safe_bool

_REWARD_TYPE = "play_pool_cookie_daily"
_REWARD_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


class TelegramMessageGateway(Protocol):
    def send_message(
        self,
        chat_id: str,
        text: str,
        *,
        disable_notification: bool = False,
    ) -> object: ...


class SqlAlchemyP115PlaybackRewardRepository:
    """Atomic play-pool reward receipts and local notification identities."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def display_names(self, user_ids: Sequence[str]) -> dict[str, str]:
        normalized = tuple(dict.fromkeys(str(value or "").strip() for value in user_ids if str(value or "").strip()))
        if not normalized:
            return {}
        statement = text("SELECT id, name FROM emby_users WHERE id IN :ids").bindparams(
            bindparam("ids", expanding=True)
        )
        with self._unit_of_work() as uow:
            rows = uow.session.execute(statement, {"ids": normalized}).mappings()
            return {str(row["id"]): str(row.get("name") or "") for row in rows}

    def expand_user_ids(self, user_ids: Sequence[str]) -> tuple[str, ...]:
        normalized = tuple(dict.fromkeys(str(value or "").strip() for value in user_ids if str(value or "").strip()))
        if not normalized:
            return ()
        source_statement = text("SELECT id FROM user_templates WHERE source_emby_user_id IN :ids").bindparams(
            bindparam("ids", expanding=True)
        )
        with self._unit_of_work() as uow:
            template_ids = tuple(int(row[0]) for row in uow.session.execute(source_statement, {"ids": normalized}))
            expanded = set(normalized)
            if template_ids:
                child_statement = text(
                    "SELECT emby_user_id FROM emby_users_extended WHERE template_id IN :ids"
                ).bindparams(bindparam("ids", expanding=True))
                expanded.update(
                    str(row[0]) for row in uow.session.execute(child_statement, {"ids": template_ids}) if row[0]
                )
        return tuple(sorted(expanded))

    def user_chat_id(self, user_id: str) -> str:
        normalized = str(user_id or "").strip()
        if not normalized:
            return ""
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text("SELECT telegram_chat_id FROM emby_users_extended WHERE emby_user_id = :user_id"),
                {"user_id": normalized},
            )
        return str(value or "").strip()

    def admin_chat_ids(self) -> tuple[str, ...]:
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    """
                    SELECT extended.telegram_chat_id
                    FROM emby_users AS users
                    JOIN emby_users_extended AS extended
                      ON extended.emby_user_id = users.id
                    WHERE users.is_administrator = TRUE
                      AND extended.telegram_chat_id IS NOT NULL
                      AND extended.telegram_chat_id <> ''
                    ORDER BY extended.telegram_chat_id
                    """
                )
            )
            return tuple(str(row[0]) for row in rows if str(row[0] or "").strip())

    def grant_once(
        self,
        user_id: str,
        *,
        reward_date: str,
        reward_days: float,
        reward_mode: str,
    ) -> dict[str, Any]:
        normalized_user = str(user_id or "").strip()
        normalized_date = str(reward_date or "").strip()
        days = float(reward_days)
        mode = str(reward_mode or "").strip().casefold()
        if not normalized_user:
            raise ValueError("play-pool reward user id is required")
        if _REWARD_DATE.fullmatch(normalized_date) is None:
            raise ValueError("play-pool reward date must use YYYY-MM-DD")
        try:
            parsed_date = date.fromisoformat(normalized_date)
        except ValueError as exc:
            raise ValueError("play-pool reward date must be a real calendar date") from exc
        if parsed_date.isoformat() != normalized_date:
            raise ValueError("play-pool reward date must use YYYY-MM-DD")
        if days <= 0 or mode not in {"private", "shared"}:
            raise ValueError("play-pool reward value is invalid")

        user_hash = hashlib.sha256(normalized_user.encode()).hexdigest()
        setting_key = f"etk:p4:play-pool-reward:v1:{user_hash}:{normalized_date}:{_REWARD_TYPE}"
        receipt: dict[str, Any] = {
            "version": 1,
            "user_id_hash": user_hash,
            "reward_date": normalized_date,
            "reward_type": _REWARD_TYPE,
            "reward_days": days,
            "reward_mode": mode,
            "expiration_date": "",
        }
        with self._unit_of_work() as uow:
            inserted = uow.session.scalar(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES (:setting_key, CAST(:value_json AS jsonb), NOW())
                    ON CONFLICT (setting_key) DO NOTHING
                    RETURNING setting_key
                    """
                ),
                {
                    "setting_key": setting_key,
                    "value_json": canonical_json(receipt, field="play-pool reward receipt"),
                },
            )
            if inserted is None:
                existing = uow.session.scalar(
                    text("SELECT value_json FROM app_settings WHERE setting_key = :setting_key"),
                    {"setting_key": setting_key},
                )
                value = dict(existing) if isinstance(existing, Mapping) else {}
                uow.commit()
                return {
                    "granted": False,
                    "setting_key": setting_key,
                    "expiration_date": str(value.get("expiration_date") or ""),
                    "receipt": value,
                }

            uow.session.execute(
                text(
                    """
                    INSERT INTO emby_users_extended (emby_user_id, status, created_by)
                    VALUES (:user_id, 'active', 'cookie-reward')
                    ON CONFLICT (emby_user_id) DO NOTHING
                    """
                ),
                {"user_id": normalized_user},
            )
            expiration = uow.session.scalar(
                text(
                    """
                    UPDATE emby_users_extended
                    SET expiration_date = GREATEST(COALESCE(expiration_date, NOW()), NOW())
                        + (:days * INTERVAL '1 day')
                    WHERE emby_user_id = :user_id
                    RETURNING expiration_date
                    """
                ),
                {"days": days, "user_id": normalized_user},
            )
            if expiration is None:
                raise RuntimeError("play-pool reward entitlement row was not updated")
            receipt["expiration_date"] = expiration.astimezone(UTC).isoformat()
            uow.session.execute(
                text(
                    """
                    UPDATE app_settings
                    SET value_json = CAST(:value_json AS jsonb), last_updated_at = NOW()
                    WHERE setting_key = :setting_key
                    """
                ),
                {
                    "setting_key": setting_key,
                    "value_json": canonical_json(receipt, field="play-pool reward receipt"),
                },
            )
            uow.commit()
        return {
            "granted": True,
            "setting_key": setting_key,
            "expiration_date": receipt["expiration_date"],
            "receipt": receipt,
        }

    def summary(self, user_id: str) -> dict[str, Any]:
        normalized = str(user_id or "").strip()
        if not normalized:
            return {}
        user_hash = hashlib.sha256(normalized.encode()).hexdigest()
        prefix = f"etk:p4:play-pool-reward:v1:{user_hash}:"
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    """
                    SELECT value_json
                    FROM app_settings
                    WHERE setting_key LIKE :prefix
                    ORDER BY setting_key DESC
                    """
                ),
                {"prefix": f"{prefix}%"},
            )
            receipts = [dict(row[0]) for row in rows if isinstance(row[0], Mapping)]
        if not receipts:
            return {}
        latest = receipts[0]
        return {
            "total_days": round(sum(float(item.get("reward_days") or 0) for item in receipts), 2),
            "last_reward_days": float(latest.get("reward_days") or 0),
            "last_reward_date": str(latest.get("reward_date") or ""),
            "last_reward_mode": str(latest.get("reward_mode") or ""),
            "last_expiration_date": str(latest.get("expiration_date") or ""),
        }


class SqlAlchemyP115PlaybackUserGateway:
    """Native user, reward, and best-effort Telegram adapter for play-pool tasks."""

    def __init__(
        self,
        repository: SqlAlchemyP115PlaybackRewardRepository,
        telegram: TelegramMessageGateway,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        timezone: str = "Asia/Shanghai",
    ) -> None:
        self._repository = repository
        self._telegram = telegram
        self._clock = clock
        self._timezone = ZoneInfo(timezone)

    def display_name(self, user_id: str) -> str:
        normalized = str(user_id or "").strip()
        return self._repository.display_names([normalized]).get(normalized, "") if normalized else ""

    def expand_allowed_user_ids(self, user_ids: Sequence[str]) -> Sequence[str]:
        return self._repository.expand_user_ids(user_ids)

    def reward_summary(self, user_id: str) -> Mapping[str, Any]:
        return self._repository.summary(user_id)

    def grant_reward(
        self,
        account: Mapping[str, Any],
        *,
        notify_user: bool,
        speed_text: str,
        error_text: str,
    ) -> Mapping[str, Any]:
        if normalize_owner_type(account.get("owner_type")) != "user":
            return {"rewarded": False, "reward_days": 0.0, "summary": {}}
        user_id = str(account.get("owner_user_id") or "").strip()
        if not user_id:
            return {"rewarded": False, "reward_days": 0.0, "summary": {}}

        usable = bool(account.get("enabled") and has_account_auth(account) and not error_text)
        shared = safe_bool(account.get("shared"), False)
        reward_days = 0.8 if shared else 0.5
        rewarded = False
        if usable:
            grant = self._repository.grant_once(
                user_id,
                reward_date=self._clock().astimezone(self._timezone).date().isoformat(),
                reward_days=reward_days,
                reward_mode="shared" if shared else "private",
            )
            rewarded = bool(grant.get("granted"))
        summary = self._repository.summary(user_id)
        if notify_user:
            chat_id = self._repository.user_chat_id(user_id)
            if chat_id:
                self._best_effort_send(
                    chat_id,
                    self._user_message(
                        account,
                        usable=usable,
                        rewarded=rewarded,
                        reward_days=reward_days,
                        speed_text=speed_text,
                        error_text=error_text,
                        summary=summary,
                    ),
                )
        return {
            "rewarded": rewarded,
            "reward_days": reward_days if rewarded else 0.0,
            "summary": summary,
        }

    def notify_daily_results(self, results: Sequence[Mapping[str, Any]]) -> None:
        lines = ["小号池每日测速结果"]
        for item in results:
            state = "通过" if item.get("success") else "失败"
            detail = item.get("speed_text") or item.get("error") or "-"
            reward = float(item.get("reward_days") or 0)
            reward_text = f"，奖励 +{reward:g} 天" if reward else ""
            lines.append(f"{state} {item.get('alias')}（{item.get('owner_name')}）：{detail}{reward_text}")
        message = "\n".join(lines)
        for chat_id in self._repository.admin_chat_ids():
            self._best_effort_send(chat_id, message, disable_notification=True)

    def _best_effort_send(self, chat_id: str, message: str, *, disable_notification: bool = False) -> None:
        try:
            self._telegram.send_message(
                chat_id,
                message,
                disable_notification=disable_notification,
            )
        except Exception:
            return

    @staticmethod
    def _user_message(
        account: Mapping[str, Any],
        *,
        usable: bool,
        rewarded: bool,
        reward_days: float,
        speed_text: str,
        error_text: str,
        summary: Mapping[str, Any],
    ) -> str:
        alias = str(account.get("alias") or "用户小号")
        if usable:
            reward_note = f"今日奖励：+{reward_days:g} 天" if rewarded else "今日奖励：已发放过"
            return (
                f"115 小号每日测速通过\n账号：{alias}\n测速：{speed_text or '通过'}\n{reward_note}\n"
                f"累计奖励：{float(summary.get('total_days') or 0):g} 天"
            )
        return (
            "115 小号每日测速未通过，本日会员时长奖励未发放。\n"
            f"账号：{alias}\n原因：{error_text or account.get('last_error') or '账号不可用'}\n"
            f"累计奖励：{float(summary.get('total_days') or 0):g} 天"
        )


__all__ = [
    "SqlAlchemyP115PlaybackRewardRepository",
    "SqlAlchemyP115PlaybackUserGateway",
    "TelegramMessageGateway",
]
