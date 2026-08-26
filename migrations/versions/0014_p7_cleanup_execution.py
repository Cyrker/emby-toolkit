"""Add durable P7 cleanup execution items.

Revision ID: 0014_p7_cleanup_execution
Revises: 0013_p7_subscription_runtime
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0014_p7_cleanup_execution"
down_revision: str | Sequence[str] | None = "0013_p7_subscription_runtime"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.create_table(
        "cleanup_execution_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("execution_key", sa.Text(), nullable=False),
        sa.Column(
            "job_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("job_runs.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "cleanup_index_id",
            sa.Integer(),
            sa.ForeignKey("cleanup_index.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("item_key", sa.Text(), nullable=False),
        sa.Column("snapshot_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'queued'")),
        sa.Column("state_revision", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "current_attempt_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("job_attempts.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("claimed_by", sa.Text(), nullable=True),
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("effect_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("receipt_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("execution_key ~ '^[0-9a-f]{64}$'", name="ck_cleanup_execution_key"),
        sa.CheckConstraint("length(btrim(item_key)) > 0", name="ck_cleanup_execution_item_key"),
        sa.CheckConstraint("jsonb_typeof(snapshot_json) = 'object'", name="ck_cleanup_execution_snapshot"),
        sa.CheckConstraint(
            "status IN ('queued','claimed','effect_started','confirmed','indeterminate','cancelled')",
            name="ck_cleanup_execution_status",
        ),
        sa.CheckConstraint("state_revision >= 0", name="ck_cleanup_execution_revision"),
        sa.CheckConstraint(
            "(status IN ('claimed','effect_started') "
            "AND current_attempt_id IS NOT NULL "
            "AND claimed_by IS NOT NULL AND length(btrim(claimed_by)) > 0 "
            "AND claim_token IS NOT NULL AND claimed_at IS NOT NULL AND lease_expires_at > claimed_at) OR "
            "(status NOT IN ('claimed','effect_started') "
            "AND current_attempt_id IS NULL AND claimed_by IS NULL AND claim_token IS NULL "
            "AND claimed_at IS NULL AND lease_expires_at IS NULL)",
            name="ck_cleanup_execution_claim",
        ),
        sa.CheckConstraint(
            "status <> 'effect_started' OR effect_started_at IS NOT NULL",
            name="ck_cleanup_execution_effect_started",
        ),
        sa.CheckConstraint(
            "(status IN ('confirmed','indeterminate','cancelled')) = (finished_at IS NOT NULL)",
            name="ck_cleanup_execution_finished",
        ),
        sa.CheckConstraint(
            "receipt_json IS NULL OR jsonb_typeof(receipt_json) = 'object'",
            name="ck_cleanup_execution_receipt",
        ),
        sa.CheckConstraint(
            "error_json IS NULL OR jsonb_typeof(error_json) = 'object'",
            name="ck_cleanup_execution_error",
        ),
        sa.UniqueConstraint("job_id", "item_key", name="uq_cleanup_execution_job_item"),
    )
    op.create_index(
        "ix_cleanup_execution_job",
        "cleanup_execution_items",
        ["job_id", "status", "created_at"],
    )
    op.create_index(
        "ix_cleanup_execution_key",
        "cleanup_execution_items",
        ["execution_key", "created_at"],
    )
    op.create_index(
        "uq_cleanup_execution_active_item",
        "cleanup_execution_items",
        ["item_key"],
        unique=True,
        postgresql_where=sa.text("status IN ('queued','claimed','effect_started')"),
    )
    op.create_index(
        "ix_cleanup_execution_claim_recovery",
        "cleanup_execution_items",
        ["lease_expires_at", "created_at"],
        postgresql_where=sa.text("status IN ('claimed','effect_started')"),
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.drop_table("cleanup_execution_items")
