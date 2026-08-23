from __future__ import annotations

import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from uuid import UUID

from etk.platform.db.pool import PsycopgPool

from .types import ExecutionGuardUnavailableError


class PostgresExecutionGuard:
    """Hold a session advisory lock for the complete external execution window."""

    def __init__(self, raw_pool: PsycopgPool | None):
        self._raw_pool = raw_pool

    @contextmanager
    def acquire(self, job_id: UUID) -> Iterator[bool]:
        if self._raw_pool is None:
            raise ExecutionGuardUnavailableError("database runtime has no raw PostgreSQL pool")
        first, second = _advisory_keys(job_id)
        with self._raw_pool.connection() as connection:
            row = connection.execute("SELECT pg_try_advisory_lock(%s, %s)", (first, second)).fetchone()
            acquired = bool(row and row[0])
            connection.commit()
            try:
                yield acquired
            finally:
                if acquired:
                    connection.execute("SELECT pg_advisory_unlock(%s, %s)", (first, second))
                connection.commit()


def _advisory_keys(job_id: UUID) -> tuple[int, int]:
    digest = hashlib.sha256(b"etk-job-execution\0" + job_id.bytes).digest()
    return (
        int.from_bytes(digest[:4], byteorder="big", signed=True),
        int.from_bytes(digest[4:8], byteorder="big", signed=True),
    )
