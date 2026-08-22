from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session


class SqlAlchemyRepository[EntityT]:
    """Minimal repository base; transaction ownership remains with the UoW."""

    def __init__(self, session: Session, model: type[EntityT]):
        self.session = session
        self.model = model

    def add(self, entity: EntityT) -> EntityT:
        self.session.add(entity)
        return entity

    def add_all(self, entities: Sequence[EntityT]) -> None:
        self.session.add_all(list(entities))

    def get(self, identity: Any) -> EntityT | None:
        return self.session.get(self.model, identity)

    def get_for_update(self, identity: Any) -> EntityT | None:
        """Lock correctness-sensitive rows until the surrounding UoW ends."""

        return self.session.get(self.model, identity, with_for_update=True)

    def remove(self, entity: EntityT) -> None:
        self.session.delete(entity)

    def flush(self) -> None:
        """Make pending writes visible without committing the surrounding UoW."""

        self.session.flush()

    def list(self, *, limit: int | None = None, offset: int = 0) -> Sequence[EntityT]:
        if offset < 0:
            raise ValueError("offset must not be negative")
        statement: Select[tuple[EntityT]] = select(self.model).offset(offset)
        if limit is not None:
            if limit < 1:
                raise ValueError("limit must be positive")
            statement = statement.limit(limit)
        return tuple(self.session.scalars(statement))

    def count(self) -> int:
        value = self.session.scalar(select(func.count()).select_from(self.model))
        return int(value or 0)
