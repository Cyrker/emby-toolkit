"""Persist ASGI proxy playback leases for P9."""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0019_p9_proxy_leases"
down_revision: str | Sequence[str] | None = "0018_p9_sharing_ledger"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.create_table(
        "proxy_playback_leases",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("lease_key", sa.Text(), nullable=False),
        sa.Column("user_id", sa.Text(), nullable=False),
        sa.Column("item_id", sa.Text(), nullable=False),
        sa.Column("play_session_id", sa.Text(), nullable=True),
        sa.Column("device_id", sa.Text(), nullable=True),
        sa.Column("client_signature", sa.Text(), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column(
            "metadata_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.CheckConstraint("length(btrim(lease_key)) > 0", name="ck_proxy_lease_key"),
        sa.CheckConstraint("length(btrim(user_id)) > 0", name="ck_proxy_lease_user"),
        sa.CheckConstraint("length(btrim(item_id)) > 0", name="ck_proxy_lease_item"),
        sa.CheckConstraint("expires_at > created_at", name="ck_proxy_lease_expiry"),
        sa.CheckConstraint("jsonb_typeof(metadata_json) = 'object'", name="ck_proxy_lease_metadata"),
        sa.UniqueConstraint("lease_key", name="uq_proxy_playback_lease_key"),
    )
    op.create_index(
        "ix_proxy_playback_lease_user_expiry",
        "proxy_playback_leases",
        ["user_id", "expires_at"],
    )
    op.create_index(
        "ix_proxy_playback_lease_expiry",
        "proxy_playback_leases",
        ["expires_at"],
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.drop_table("proxy_playback_leases")
