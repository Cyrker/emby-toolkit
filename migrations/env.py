from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import MetaData, create_engine, pool

from etk.platform.db.engine import normalize_sqlalchemy_url

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = MetaData()
VERSION_SCHEMA = "etk_migrations"


def _database_url() -> str:
    value = os.environ.get("ETK_DATABASE_URL") or config.get_main_option("sqlalchemy.url")
    if not value:
        raise RuntimeError("ETK_DATABASE_URL or alembic sqlalchemy.url is required")
    return normalize_sqlalchemy_url(value).render_as_string(hide_password=False)


def run_migrations_offline() -> None:
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table_schema=VERSION_SCHEMA,
        include_schemas=True,
    )
    context.execute(f"CREATE SCHEMA IF NOT EXISTS {VERSION_SCHEMA}")
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = create_engine(_database_url(), poolclass=pool.NullPool)
    try:
        with connectable.connect() as connection:
            connection.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {VERSION_SCHEMA}")
            connection.commit()
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                version_table_schema=VERSION_SCHEMA,
                include_schemas=True,
                compare_type=True,
                compare_server_default=True,
            )
            with context.begin_transaction():
                context.run_migrations()
    finally:
        connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
