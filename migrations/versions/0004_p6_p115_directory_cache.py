"""Add account-bound P6 directory cache ownership markers.

Revision ID: 0004_p6_p115_directory_cache
Revises: 0003_p5_media_effects
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_p6_p115_directory_cache"
down_revision: str | Sequence[str] | None = "0003_p5_media_effects"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.add_column("p115_filesystem_cache", sa.Column("account_id", sa.Text(), nullable=True))
    op.add_column("p115_filesystem_cache", sa.Column("entry_kind", sa.Text(), nullable=True))
    op.create_check_constraint(
        "ck_p115_filesystem_cache_account_id",
        "p115_filesystem_cache",
        "account_id IS NULL OR account_id ~ '^[0-9]+$'",
    )
    op.create_check_constraint(
        "ck_p115_filesystem_cache_entry_kind",
        "p115_filesystem_cache",
        "entry_kind IS NULL OR entry_kind IN ('directory', 'file')",
    )
    op.create_index(
        "idx_p115_account_parent_kind",
        "p115_filesystem_cache",
        ["account_id", "parent_id", "entry_kind"],
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.drop_index("idx_p115_account_parent_kind", table_name="p115_filesystem_cache")
    op.drop_constraint(
        "ck_p115_filesystem_cache_entry_kind",
        "p115_filesystem_cache",
        type_="check",
    )
    op.drop_constraint(
        "ck_p115_filesystem_cache_account_id",
        "p115_filesystem_cache",
        type_="check",
    )
    op.drop_column("p115_filesystem_cache", "entry_kind")
    op.drop_column("p115_filesystem_cache", "account_id")
