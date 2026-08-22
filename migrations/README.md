# ETK Alembic ledger

This directory is the forward-only schema ledger for the plaintext mainline.
The P2 baseline recreates the committed 39-table PostgreSQL contract in a new,
empty PostgreSQL 18 database. It never calls legacy `init_db()` and never reads
from `verification/` at migration runtime.

The Alembic version table lives in `etk_migrations`, keeping `public` limited to
the 39 application tables used by schema contract comparison.
