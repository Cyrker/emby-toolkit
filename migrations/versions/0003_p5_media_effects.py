"""Add the P5 durable media effect ledger.

Revision ID: 0003_p5_media_effects
Revises: 0002_p4_persistent_jobs
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003_p5_media_effects"
down_revision: str | Sequence[str] | None = "0002_p4_persistent_jobs"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


MEDIA_EFFECT_STATUSES = (
    "pending",
    "claimed",
    "retryable",
    "confirmed",
    "rejected",
    "indeterminate",
)


def _values(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def upgrade() -> None:
    # The pg_dump-based P2 baseline clears search_path on the migration connection.
    op.execute("SET LOCAL search_path TO public")
    op.create_table(
        "media_effects",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("effect_key", sa.Text(), nullable=False),
        sa.Column("effect_type", sa.Text(), nullable=False),
        sa.Column("aggregate_type", sa.Text(), nullable=False),
        sa.Column("aggregate_id", sa.Text(), nullable=False),
        sa.Column("payload_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("request_hash", postgresql.BYTEA(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("claimed_by", sa.Text(), nullable=True),
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(f"status IN ({_values(MEDIA_EFFECT_STATUSES)})", name="ck_media_effects_status"),
        sa.CheckConstraint("length(btrim(effect_key)) BETWEEN 1 AND 512", name="ck_media_effects_effect_key"),
        sa.CheckConstraint("length(btrim(effect_type)) > 0", name="ck_media_effects_effect_type"),
        sa.CheckConstraint("length(btrim(aggregate_type)) > 0", name="ck_media_effects_aggregate_type"),
        sa.CheckConstraint("length(btrim(aggregate_id)) > 0", name="ck_media_effects_aggregate_id"),
        sa.CheckConstraint("payload_version >= 1", name="ck_media_effects_payload_version"),
        sa.CheckConstraint("jsonb_typeof(payload_json) = 'object'", name="ck_media_effects_payload_object"),
        sa.CheckConstraint("octet_length(request_hash) = 32", name="ck_media_effects_request_hash"),
        sa.CheckConstraint("attempts >= 0", name="ck_media_effects_attempts"),
        sa.CheckConstraint(
            "(attempts = 0) = (last_attempt_at IS NULL)",
            name="ck_media_effects_attempt_timestamp",
        ),
        sa.CheckConstraint(
            "status = 'pending' OR attempts >= 1",
            name="ck_media_effects_nonpending_attempt",
        ),
        sa.CheckConstraint(
            "(status = 'claimed' AND claimed_by IS NOT NULL AND length(btrim(claimed_by)) > 0 "
            "AND claim_token IS NOT NULL AND claimed_at IS NOT NULL AND lease_expires_at IS NOT NULL) OR "
            "(status <> 'claimed' AND claimed_by IS NULL AND claim_token IS NULL "
            "AND claimed_at IS NULL AND lease_expires_at IS NULL)",
            name="ck_media_effects_claim",
        ),
        sa.CheckConstraint(
            "status <> 'claimed' OR lease_expires_at > claimed_at",
            name="ck_media_effects_claim_lease",
        ),
        sa.CheckConstraint(
            "(status IN ('confirmed', 'rejected', 'indeterminate')) = (finished_at IS NOT NULL)",
            name="ck_media_effects_terminal_finished",
        ),
        sa.CheckConstraint(
            "result_json IS NULL OR jsonb_typeof(result_json) = 'object'",
            name="ck_media_effects_result_object",
        ),
        sa.CheckConstraint(
            "error_json IS NULL OR jsonb_typeof(error_json) = 'object'",
            name="ck_media_effects_error_object",
        ),
        sa.UniqueConstraint("effect_key", name="uq_media_effects_effect_key"),
    )
    op.create_index(
        "ix_media_effects_aggregate",
        "media_effects",
        ["aggregate_type", "aggregate_id", sa.text("created_at DESC")],
    )
    op.create_index(
        "ix_media_effects_claimable",
        "media_effects",
        ["status", "available_at", "created_at"],
        postgresql_where=sa.text("status IN ('pending', 'retryable')"),
    )
    op.create_index(
        "ix_media_effects_claim_recovery",
        "media_effects",
        ["status", "lease_expires_at"],
        postgresql_where=sa.text("status = 'claimed'"),
    )
    op.create_index("ix_media_effects_status_updated", "media_effects", ["status", "updated_at"])


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.execute("DELETE FROM outbox_events WHERE topic = 'media.effect.requested'")
    op.drop_table("media_effects")
