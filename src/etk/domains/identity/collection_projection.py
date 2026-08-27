from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from sqlalchemy import bindparam, text

from etk.platform.db import SqlAlchemyUnitOfWork


class SqlAlchemyCollectionUserProjection:
    """Identity-owned user and template projection used by P8 collections."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def expand_template_users(self, selected_user_ids: Sequence[str] | None) -> list[str]:
        selected = tuple(dict.fromkeys(str(value) for value in selected_user_ids or () if str(value)))
        if not selected:
            return []
        with self._unit_of_work() as uow:
            template_ids = tuple(
                int(value)
                for value in uow.session.scalars(
                    text("SELECT id FROM user_templates WHERE source_emby_user_id IN :user_ids").bindparams(
                        bindparam("user_ids", expanding=True)
                    ),
                    {"user_ids": selected},
                )
            )
            expanded = set(selected)
            if template_ids:
                expanded.update(
                    str(value)
                    for value in uow.session.scalars(
                        text(
                            "SELECT emby_user_id FROM emby_users_extended WHERE template_id IN :template_ids"
                        ).bindparams(bindparam("template_ids", expanding=True)),
                        {"template_ids": template_ids},
                    )
                )
        return sorted(expanded)

    def collection_user_options(self) -> list[dict[str, Any]]:
        with self._unit_of_work() as uow:
            users = [
                dict(row)
                for row in uow.session.execute(
                    text(
                        """
                        SELECT users.id, users.name, extended.template_id
                        FROM emby_users AS users
                        LEFT JOIN emby_users_extended AS extended
                          ON extended.emby_user_id = users.id
                        ORDER BY users.name
                        """
                    )
                ).mappings()
            ]
            source_ids = {
                str(value)
                for value in uow.session.scalars(
                    text("SELECT source_emby_user_id FROM user_templates WHERE source_emby_user_id IS NOT NULL")
                )
            }
        return [
            {
                "label": row.get("name"),
                "value": row.get("id"),
                "is_template_source": str(row.get("id")) in source_ids,
            }
            for row in users
            if str(row.get("id")) in source_ids or row.get("template_id") is None
        ]


__all__ = ["SqlAlchemyCollectionUserProjection"]
