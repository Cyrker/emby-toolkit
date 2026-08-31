from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from etk.platform.db import SqlAlchemyUnitOfWork

from .repository import (
    SqlAlchemyP115RenameConfigRepository,
    SqlAlchemyP115SortingRulesRepository,
)
from .washing_priority_repository import SqlAlchemyP115WashingPriorityRepository


class SqlAlchemyP115QuickDeployConfigurationRepository:
    """Persist the local Quick Deploy configuration as one transaction."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work
        self._rename = SqlAlchemyP115RenameConfigRepository(unit_of_work)
        self._sorting = SqlAlchemyP115SortingRulesRepository(unit_of_work)
        self._washing = SqlAlchemyP115WashingPriorityRepository(unit_of_work)

    def save_bundle(
        self,
        *,
        rename_config: Mapping[str, Any],
        sorting_rules: Sequence[Mapping[str, Any]],
        account_id: str,
        washing_config: Mapping[str, Any],
        washing_groups: Sequence[object],
    ) -> None:
        # P115QuickDeployLock owns the auth advisory lock for the whole operation.
        # Reacquiring that session lock from this SQLAlchemy connection would deadlock.
        with self._unit_of_work() as uow:
            self._rename.save_in_uow(uow, rename_config)
            self._washing.save_config_in_uow(uow, washing_config)
            self._sorting.save_in_uow(uow, sorting_rules, account_id=account_id)
            self._washing.replace_groups_in_uow(uow, washing_groups)
            uow.commit()


__all__ = ["SqlAlchemyP115QuickDeployConfigurationRepository"]
