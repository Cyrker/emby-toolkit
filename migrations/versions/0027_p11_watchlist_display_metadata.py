"""Allow durable Watchlist display-metadata operations."""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0027_p11_watchlist_display"
down_revision: str | Sequence[str] | None = "0026_p11_custom_refresh"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KINDS = (
    "register_device",
    "register_source",
    "display_metadata",
    "transfer",
    "share_request",
    "virtual_import",
    "hdhive_checkin",
    "telegram_delivery",
    "proxy_play",
)


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.drop_constraint("ck_sharing_operation_kind", "sharing_operations", type_="check")
    op.create_check_constraint(
        "ck_sharing_operation_kind",
        "sharing_operations",
        "kind IN (" + ",".join(f"'{kind}'" for kind in _KINDS) + ")",
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    if op.get_bind().scalar(
        sa.text(
            """
            SELECT EXISTS(
                SELECT 1 FROM sharing_operations
                WHERE kind = 'display_metadata'
            )
            """
        )
    ):
        raise RuntimeError("cannot downgrade while Watchlist display metadata operations exist")
    op.drop_constraint("ck_sharing_operation_kind", "sharing_operations", type_="check")
    op.create_check_constraint(
        "ck_sharing_operation_kind",
        "sharing_operations",
        "kind IN (" + ",".join(f"'{kind}'" for kind in _KINDS if kind != "display_metadata") + ")",
    )
