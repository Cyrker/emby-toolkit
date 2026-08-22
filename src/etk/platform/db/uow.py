from __future__ import annotations

from contextlib import AbstractContextManager
from enum import StrEnum
from types import TracebackType

from sqlalchemy.orm import Session

from .session import SessionFactory


class UnitOfWorkState(StrEnum):
    NEW = "new"
    ACTIVE = "active"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"
    CLOSED = "closed"


class SqlAlchemyUnitOfWork(AbstractContextManager["SqlAlchemyUnitOfWork"]):
    """One explicit SQLAlchemy transaction boundary per application use case.

    Successful context exit does not imply commit. Callers must invoke
    ``commit()`` explicitly; otherwise the pending transaction is rolled back.
    External HTTP or subprocess work should be performed outside this context.
    """

    def __init__(self, session_factory: SessionFactory):
        self._session_factory = session_factory
        self._session: Session | None = None
        self._state = UnitOfWorkState.NEW

    @property
    def state(self) -> UnitOfWorkState:
        return self._state

    @property
    def session(self) -> Session:
        if self._session is None or self._state is not UnitOfWorkState.ACTIVE:
            raise RuntimeError("unit of work is not active")
        return self._session

    def __enter__(self) -> SqlAlchemyUnitOfWork:
        if self._state is not UnitOfWorkState.NEW:
            raise RuntimeError(f"unit of work cannot enter from state {self._state}")
        self._session = self._session_factory()
        self._state = UnitOfWorkState.ACTIVE
        return self

    def commit(self) -> None:
        session = self.session
        session.commit()
        self._state = UnitOfWorkState.COMMITTED

    def rollback(self) -> None:
        session = self.session
        session.rollback()
        self._state = UnitOfWorkState.ROLLED_BACK

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        session = self._session
        if session is None:
            return False
        try:
            if self._state is UnitOfWorkState.ACTIVE:
                session.rollback()
                self._state = UnitOfWorkState.ROLLED_BACK
        finally:
            session.close()
            self._session = None
            self._state = UnitOfWorkState.CLOSED
        return False
