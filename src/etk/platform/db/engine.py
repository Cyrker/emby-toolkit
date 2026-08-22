from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import Engine, create_engine
from sqlalchemy.engine import URL, make_url


@dataclass(frozen=True, slots=True)
class EngineConfig:
    """SQLAlchemy sync-engine settings without exposing credentials in repr output."""

    url: str
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout_seconds: float = 30.0
    pool_recycle_seconds: int = 1800
    pool_pre_ping: bool = True
    echo: bool = False
    application_name: str = "etk"

    def __post_init__(self) -> None:
        if not self.url.strip():
            raise ValueError("database URL must not be empty")
        if self.pool_size < 1:
            raise ValueError("pool_size must be at least 1")
        if self.max_overflow < 0:
            raise ValueError("max_overflow must not be negative")
        if self.pool_timeout_seconds <= 0:
            raise ValueError("pool_timeout_seconds must be positive")
        if self.pool_recycle_seconds < 0:
            raise ValueError("pool_recycle_seconds must not be negative")
        if not self.application_name.strip():
            raise ValueError("application_name must not be empty")

    def __repr__(self) -> str:
        safe_url = normalize_sqlalchemy_url(self.url).render_as_string(hide_password=True)
        return (
            "EngineConfig("
            f"url={safe_url!r}, pool_size={self.pool_size}, max_overflow={self.max_overflow}, "
            f"pool_timeout_seconds={self.pool_timeout_seconds}, pool_recycle_seconds={self.pool_recycle_seconds}, "
            f"pool_pre_ping={self.pool_pre_ping}, echo={self.echo}, application_name={self.application_name!r})"
        )


def normalize_sqlalchemy_url(value: str | URL) -> URL:
    """Require PostgreSQL and select psycopg 3 as SQLAlchemy's sync driver."""

    url = make_url(value)
    if url.get_backend_name() != "postgresql":
        raise ValueError("ETK data platform requires a PostgreSQL database URL")
    if url.drivername not in {"postgresql", "postgresql+psycopg"}:
        raise ValueError("ETK data platform requires the psycopg 3 SQLAlchemy driver")
    return url.set(drivername="postgresql+psycopg")


def create_sync_engine(config: EngineConfig, **overrides: Any) -> Engine:
    """Create the process-owned SQLAlchemy 2 engine.

    Repositories use SQLAlchemy's QueuePool. ``psycopg_pool`` is exposed
    separately for low-level PostgreSQL operations so a DBAPI connection is
    never accidentally owned by two pool implementations.
    """

    options: dict[str, Any] = {
        "pool_size": config.pool_size,
        "max_overflow": config.max_overflow,
        "pool_timeout": config.pool_timeout_seconds,
        "pool_recycle": config.pool_recycle_seconds,
        "pool_pre_ping": config.pool_pre_ping,
        "echo": config.echo,
        "connect_args": {"application_name": config.application_name},
    }
    options.update(overrides)
    return create_engine(normalize_sqlalchemy_url(config.url), **options)
