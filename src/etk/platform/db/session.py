from __future__ import annotations

from collections.abc import Callable

from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

SessionFactory = Callable[[], Session]


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Build sessions with explicit commit semantics and stable loaded values."""

    return sessionmaker(
        bind=engine,
        class_=Session,
        autoflush=False,
        expire_on_commit=False,
    )
