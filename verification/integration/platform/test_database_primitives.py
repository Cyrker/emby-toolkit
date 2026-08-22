from __future__ import annotations

import pytest
from sqlalchemy import Integer, String, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.pool import StaticPool

from etk.platform.db import (
    DatabaseRetryPolicy,
    EngineConfig,
    PsycopgPoolConfig,
    SqlAlchemyRepository,
    SqlAlchemyUnitOfWork,
    create_session_factory,
    normalize_sqlalchemy_url,
    run_retryable_transaction,
)


class Base(DeclarativeBase):
    pass


class Example(Base):
    __tablename__ = "examples"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    value: Mapped[str] = mapped_column(String, nullable=False)


@pytest.fixture
def session_factory():
    engine = create_engine(
        "sqlite+pysqlite://",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)
    try:
        yield create_session_factory(engine)
    finally:
        engine.dispose()


def test_uow_requires_explicit_commit(session_factory) -> None:
    with SqlAlchemyUnitOfWork(session_factory) as uow:
        SqlAlchemyRepository(uow.session, Example).add(Example(id=1, value="rolled-back"))

    with session_factory() as session:
        assert session.get(Example, 1) is None

    with SqlAlchemyUnitOfWork(session_factory) as uow:
        SqlAlchemyRepository(uow.session, Example).add(Example(id=1, value="committed"))
        uow.commit()

    with session_factory() as session:
        assert session.scalar(select(Example.value).where(Example.id == 1)) == "committed"


def test_uow_rolls_back_exception_and_repository_never_commits(session_factory) -> None:
    with pytest.raises(RuntimeError, match="fixture failure"):
        with SqlAlchemyUnitOfWork(session_factory) as uow:
            repository = SqlAlchemyRepository(uow.session, Example)
            repository.add(Example(id=2, value="temporary"))
            repository.flush()
            assert repository.count() == 1
            raise RuntimeError("fixture failure")

    with session_factory() as session:
        assert session.get(Example, 2) is None


def test_database_configuration_requires_psycopg3_and_hides_credentials() -> None:
    url = "postgresql://etk:database-secret@localhost:5432/etk"

    assert normalize_sqlalchemy_url(url).drivername == "postgresql+psycopg"
    assert "database-secret" not in repr(EngineConfig(url))
    assert "database-secret" not in repr(PsycopgPoolConfig(url))
    with pytest.raises(ValueError, match="PostgreSQL"):
        normalize_sqlalchemy_url("sqlite:///fixture.db")
    with pytest.raises(ValueError, match="psycopg 3"):
        normalize_sqlalchemy_url("postgresql+psycopg2://etk:secret@localhost/etk")


def test_transaction_retry_is_bounded_and_only_accepts_safe_sqlstates() -> None:
    class DatabaseFailure(RuntimeError):
        def __init__(self, sqlstate: str) -> None:
            super().__init__(sqlstate)
            self.sqlstate = sqlstate

    attempts = 0
    delays: list[float] = []

    def eventually_succeeds() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise DatabaseFailure("40001")
        return "committed"

    result = run_retryable_transaction(
        eventually_succeeds,
        policy=DatabaseRetryPolicy(max_attempts=3, initial_delay_seconds=0.1),
        sleep=delays.append,
    )

    assert result == "committed"
    assert attempts == 3
    assert delays == [0.1, 0.2]
    with pytest.raises(DatabaseFailure, match="23505"):
        run_retryable_transaction(lambda: (_ for _ in ()).throw(DatabaseFailure("23505")))
