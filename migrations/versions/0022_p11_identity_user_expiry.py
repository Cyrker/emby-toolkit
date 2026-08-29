"""Allow durable identity user-expiry operations.

Revision ID: 0022_p11_identity_user_expiry
Revises: 0021_p11_license_alias_cutover
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0022_p11_identity_user_expiry"
down_revision: str | Sequence[str] | None = "0021_p11_license_alias_cutover"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_OLD_KINDS = "'register','assign_template','set_status','delete_user','upload_avatar'"
_NEW_KINDS = "'register','assign_template','set_status','expire_user','delete_user','upload_avatar'"


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.drop_constraint("ck_identity_operation_kind", "identity_operations", type_="check")
    op.create_check_constraint(
        "ck_identity_operation_kind",
        "identity_operations",
        f"kind IN ({_NEW_KINDS})",
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM identity_operations WHERE kind = 'expire_user') THEN
                RAISE EXCEPTION 'cannot downgrade while expire_user identity operations exist';
            END IF;
        END
        $$
        """
    )
    op.drop_constraint("ck_identity_operation_kind", "identity_operations", type_="check")
    op.create_check_constraint(
        "ck_identity_operation_kind",
        "identity_operations",
        f"kind IN ({_OLD_KINDS})",
    )
