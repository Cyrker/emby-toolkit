"""Add the durable custom-collection refresh operation kind."""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0026_p11_custom_refresh"
down_revision: str | Sequence[str] | None = "0025_p11_collection_activation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.drop_constraint("ck_collection_operation_kind", "collection_operations", type_="check")
    op.create_check_constraint(
        "ck_collection_operation_kind",
        "collection_operations",
        "kind IN ('collection.activation.refresh','collection.custom.refresh',"
        "'collection.native.delete','collection.custom.delete',"
        "'collection.missing.cleanup','collection.image.archive',"
        "'collection.removal.survival-check')",
    )
    op.drop_constraint("ck_collection_operation_step", "collection_operations", type_="check")
    op.create_check_constraint(
        "ck_collection_operation_step",
        "collection_operations",
        "current_step IN ('reserved','read_remote','create_collection','remove_members',"
        "'add_members','delete_shell','discover_missing_targets','archive_images',"
        "'cleanup_placeholders','cleanup_local','survival_readback')",
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    if op.get_bind().scalar(
        sa.text(
            """
            SELECT EXISTS(
                SELECT 1 FROM collection_operations
                WHERE kind = 'collection.custom.refresh'
            )
            """
        )
    ):
        raise RuntimeError("cannot downgrade while custom collection refresh operations exist")
    op.drop_constraint("ck_collection_operation_step", "collection_operations", type_="check")
    op.create_check_constraint(
        "ck_collection_operation_step",
        "collection_operations",
        "current_step IN ('reserved','read_remote','remove_members','delete_shell',"
        "'discover_missing_targets','archive_images','cleanup_placeholders',"
        "'cleanup_local','survival_readback')",
    )
    op.drop_constraint("ck_collection_operation_kind", "collection_operations", type_="check")
    op.create_check_constraint(
        "ck_collection_operation_kind",
        "collection_operations",
        "kind IN ('collection.activation.refresh','collection.native.delete',"
        "'collection.custom.delete','collection.missing.cleanup',"
        "'collection.image.archive','collection.removal.survival-check')",
    )
