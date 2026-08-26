"""Add durable claims for P7 subscribe assistant snapshots.

Revision ID: 0015_p7_assistant_claims
Revises: 0014_p7_cleanup_execution
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0015_p7_assistant_claims"
down_revision: str | Sequence[str] | None = "0014_p7_cleanup_execution"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.add_column(
        "subscribe_assistant_snapshots",
        sa.Column("state_revision", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
    )
    op.add_column(
        "subscribe_assistant_snapshots",
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "subscribe_assistant_snapshots",
        sa.Column("attempt_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "subscribe_assistant_snapshots",
        sa.Column("worker_id", sa.Text(), nullable=True),
    )
    op.add_column(
        "subscribe_assistant_snapshots",
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "subscribe_assistant_snapshots",
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "subscribe_assistant_snapshots",
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_sas_snapshot_job",
        "subscribe_assistant_snapshots",
        "job_runs",
        ["job_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_unique_constraint(
        "uq_job_attempts_id_job",
        "job_attempts",
        ["id", "job_id"],
    )
    op.create_foreign_key(
        "fk_sas_snapshot_attempt_job",
        "subscribe_assistant_snapshots",
        "job_attempts",
        ["attempt_id", "job_id"],
        ["id", "job_id"],
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        "ck_sas_snapshot_revision",
        "subscribe_assistant_snapshots",
        "state_revision >= 0",
    )
    op.create_check_constraint(
        "ck_sas_snapshot_claim",
        "subscribe_assistant_snapshots",
        "(claim_token IS NULL AND job_id IS NULL AND attempt_id IS NULL AND worker_id IS NULL "
        "AND claimed_at IS NULL AND lease_expires_at IS NULL) OR "
        "(claim_token IS NOT NULL AND job_id IS NOT NULL AND attempt_id IS NOT NULL "
        "AND worker_id IS NOT NULL AND length(btrim(worker_id)) > 0 "
        "AND claimed_at IS NOT NULL AND lease_expires_at > claimed_at)",
    )
    op.create_index(
        "ix_sas_snapshot_claim_recovery",
        "subscribe_assistant_snapshots",
        ["lease_expires_at", "id"],
        postgresql_where=sa.text("claim_token IS NOT NULL"),
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.drop_index("ix_sas_snapshot_claim_recovery", table_name="subscribe_assistant_snapshots")
    op.drop_constraint(
        "fk_sas_snapshot_attempt_job",
        "subscribe_assistant_snapshots",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_sas_snapshot_job",
        "subscribe_assistant_snapshots",
        type_="foreignkey",
    )
    op.drop_constraint("uq_job_attempts_id_job", "job_attempts", type_="unique")
    op.drop_constraint(
        "ck_sas_snapshot_claim",
        "subscribe_assistant_snapshots",
        type_="check",
    )
    op.drop_constraint(
        "ck_sas_snapshot_revision",
        "subscribe_assistant_snapshots",
        type_="check",
    )
    for column in (
        "lease_expires_at",
        "claimed_at",
        "claim_token",
        "worker_id",
        "attempt_id",
        "job_id",
        "state_revision",
    ):
        op.drop_column("subscribe_assistant_snapshots", column)
