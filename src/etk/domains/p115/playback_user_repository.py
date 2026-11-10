from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol

from sqlalchemy import bindparam, text

from etk.platform.db import SqlAlchemyUnitOfWork


class TelegramMessageGateway(Protocol):
    def send_message(
        self,
        chat_id: str,
        text: str,
        *,
        disable_notification: bool = False,
    ) -> object: ...


class SqlAlchemyP115PlaybackUserRepository:
    """Local user and notification identities for play-pool tasks."""

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


class SqlAlchemyP115PlaybackUserGateway:
    """Native user and best-effort Telegram adapter for play-pool tasks."""

    def __init__(
        self,
        repository: SqlAlchemyP115PlaybackUserRepository,
        telegram: TelegramMessageGateway,
    ) -> None:
        self._repository = repository
        self._telegram = telegram

    def display_name(self, user_id: str) -> str:
        normalized = str(user_id or "").strip()
        return self._repository.display_names([normalized]).get(normalized, "") if normalized else ""

    def expand_allowed_user_ids(self, user_ids: Sequence[str]) -> Sequence[str]:
        return self._repository.expand_user_ids(user_ids)

    def notify_daily_results(self, results: Sequence[Mapping[str, Any]]) -> None:
        lines = ["小号池每日测速结果"]
        for item in results:
            state = "通过" if item.get("success") else "失败"
            detail = item.get("speed_text") or item.get("error") or "-"
            lines.append(f"{state} {item.get('alias')}（{item.get('owner_name')}）：{detail}")
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


__all__ = [
    "SqlAlchemyP115PlaybackUserRepository",
    "SqlAlchemyP115PlaybackUserGateway",
    "TelegramMessageGateway",
]
