"""Add the explicit Shared Center device-registration operation kind."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0023_p11_sharing_device_kind"
down_revision: str | Sequence[str] | None = "0022_p11_identity_user_expiry"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KINDS = (
    "register_device",
    "register_source",
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
    op.execute("UPDATE sharing_operations SET kind = 'register_source' WHERE kind = 'register_device'")
    op.drop_constraint("ck_sharing_operation_kind", "sharing_operations", type_="check")
    op.create_check_constraint(
        "ck_sharing_operation_kind",
        "sharing_operations",
        "kind IN (" + ",".join(f"'{kind}'" for kind in _KINDS if kind != "register_device") + ")",
    )
