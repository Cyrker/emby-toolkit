"""Add durable pre-request cancellation to sharing operations."""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0024_p11_sharing_cancellation"
down_revision: str | Sequence[str] | None = "0023_p11_sharing_device_kind"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_STATUSES = (
    "ready",
    "claimed",
    "requesting",
    "awaiting_readback",
    "retryable",
    "confirmed",
    "rejected",
    "indeterminate",
    "cancelled",
)


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.add_column(
        "sharing_operations",
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.drop_constraint("ck_sharing_operation_status", "sharing_operations", type_="check")
    op.create_check_constraint(
        "ck_sharing_operation_status",
        "sharing_operations",
        "status IN (" + ",".join(f"'{status}'" for status in _STATUSES) + ")",
    )
    op.drop_constraint("ck_sharing_operation_finished", "sharing_operations", type_="check")
    op.create_check_constraint(
        "ck_sharing_operation_finished",
        "sharing_operations",
        "(status IN ('confirmed','rejected','indeterminate','cancelled')) = (finished_at IS NOT NULL)",
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.execute(
        """
        UPDATE sharing_operations
        SET status = 'rejected',
            error_json = jsonb_build_object(
                'type', 'cancelled_before_provider_request',
                'provider_may_have_been_called', FALSE
            )
        WHERE status = 'cancelled'
        """
    )
    op.drop_constraint("ck_sharing_operation_finished", "sharing_operations", type_="check")
    op.create_check_constraint(
        "ck_sharing_operation_finished",
        "sharing_operations",
        "(status IN ('confirmed','rejected','indeterminate')) = (finished_at IS NOT NULL)",
    )
    op.drop_constraint("ck_sharing_operation_status", "sharing_operations", type_="check")
    op.create_check_constraint(
        "ck_sharing_operation_status",
        "sharing_operations",
        "status IN (" + ",".join(f"'{status}'" for status in _STATUSES if status != "cancelled") + ")",
    )
    op.drop_column("sharing_operations", "cancel_requested_at")
