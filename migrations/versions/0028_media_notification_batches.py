"""Persist library notification batches before Telegram delivery."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0028_media_notice_batches"
down_revision = "0027_p11_watchlist_display"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "media_library_notification_batches",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("group_key", sa.Text(), nullable=False),
        sa.Column("group_path", sa.Text(), nullable=False),
        sa.Column("members_json", postgresql.JSONB(), nullable=False),
        sa.Column("quiet_until", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sealed_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("jsonb_typeof(members_json) = 'object'", name="ck_media_notice_members"),
    )
    op.create_index(
        "uq_media_notice_open_group",
        "media_library_notification_batches",
        ["group_key"],
        unique=True,
        postgresql_where=sa.text("sealed_at IS NULL"),
    )
    op.create_index(
        "ix_media_notice_members", "media_library_notification_batches", ["members_json"], postgresql_using="gin"
    )


def downgrade() -> None:
    if op.get_bind().scalar(sa.text("SELECT EXISTS(SELECT 1 FROM media_library_notification_batches)")):
        raise RuntimeError("cannot downgrade while durable library notification batches exist")
    op.drop_table("media_library_notification_batches")
