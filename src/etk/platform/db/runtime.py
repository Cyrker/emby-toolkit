from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import Engine

from .engine import EngineConfig, create_sync_engine
from .pool import PsycopgPool, PsycopgPoolConfig
from .session import SessionFactory, create_session_factory
from .uow import SqlAlchemyUnitOfWork


@dataclass(slots=True)
class DatabaseRuntime:
    """Process-owned database resources with explicit startup and shutdown."""

    engine: Engine
    session_factory: SessionFactory
    raw_pool: PsycopgPool | None = None

    @classmethod
    def create(
        cls,
        engine_config: EngineConfig,
        *,
        raw_pool_config: PsycopgPoolConfig | None = None,
    ) -> DatabaseRuntime:
        engine = create_sync_engine(engine_config)
        return cls(
            engine=engine,
            session_factory=create_session_factory(engine),
            raw_pool=PsycopgPool(raw_pool_config) if raw_pool_config is not None else None,
        )

    def start(self) -> None:
        if self.raw_pool is not None:
            self.raw_pool.open()

    def unit_of_work(self) -> SqlAlchemyUnitOfWork:
        return SqlAlchemyUnitOfWork(self.session_factory)

    def shutdown(self) -> None:
        if self.raw_pool is not None:
            self.raw_pool.close()
        self.engine.dispose()

    def close(self) -> None:
        """Expose the container resource protocol without hiding shutdown semantics."""

        self.shutdown()
