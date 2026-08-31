"""Allow durable native collection activation refresh operations."""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0025_p11_collection_activation"
down_revision: str | Sequence[str] | None = "0024_p11_sharing_cancellation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KINDS = (
    "collection.activation.refresh",
    "collection.native.delete",
    "collection.custom.delete",
    "collection.missing.cleanup",
    "collection.image.archive",
    "collection.removal.survival-check",
)


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.drop_constraint("ck_collection_operation_kind", "collection_operations", type_="check")
    op.create_check_constraint(
        "ck_collection_operation_kind",
        "collection_operations",
        "kind IN (" + ",".join(f"'{kind}'" for kind in _KINDS) + ")",
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    if op.get_bind().scalar(
        sa.text(
            """
            SELECT EXISTS(
                SELECT 1 FROM collection_operations
                WHERE kind = 'collection.activation.refresh'
            )
            """
        )
    ):
        raise RuntimeError("cannot downgrade while collection activation operations exist")
    op.drop_constraint("ck_collection_operation_kind", "collection_operations", type_="check")
    op.create_check_constraint(
        "ck_collection_operation_kind",
        "collection_operations",
        "kind IN (" + ",".join(f"'{kind}'" for kind in _KINDS[1:]) + ")",
    )
