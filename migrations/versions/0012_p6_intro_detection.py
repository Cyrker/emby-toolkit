"""Add durable P6 intro and credits season operations.

Revision ID: 0012_p6_intro_detection
Revises: 0011_p6_playback
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0012_p6_intro_detection"
down_revision: str | Sequence[str] | None = "0011_p6_playback"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.create_table(
        "p115_intro_detection_operations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_sequence", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("operation_key", sa.Text(), nullable=False),
        sa.Column("request_hash", postgresql.BYTEA(), nullable=False),
        sa.Column("source_set_revision", postgresql.BYTEA(), nullable=False),
        sa.Column("policy_revision", postgresql.BYTEA(), nullable=False),
        sa.Column("series_tmdb_id", sa.Text(), nullable=False),
        sa.Column("season_number", sa.Integer(), nullable=False),
        sa.Column("algorithm_version", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.Text(), nullable=False),
        sa.Column("trigger_mode", sa.Text(), nullable=False),
        sa.Column("request_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'ready'")),
        sa.Column("phase", sa.Text(), nullable=False, server_default=sa.text("'restore'")),
        sa.Column("state_revision", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "progress_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("claimed_by", sa.Text(), nullable=True),
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("operation_key ~ '^[0-9a-f]{64}$'", name="ck_p115_intro_operation_key"),
        sa.CheckConstraint("octet_length(request_hash) = 32", name="ck_p115_intro_request_hash"),
        sa.CheckConstraint("octet_length(source_set_revision) = 32", name="ck_p115_intro_source_revision"),
        sa.CheckConstraint("octet_length(policy_revision) = 32", name="ck_p115_intro_policy_revision"),
        sa.CheckConstraint("length(btrim(series_tmdb_id)) > 0", name="ck_p115_intro_series_tmdb_id"),
        sa.CheckConstraint("season_number >= 0", name="ck_p115_intro_season_number"),
        sa.CheckConstraint("algorithm_version >= 1", name="ck_p115_intro_algorithm_version"),
        sa.CheckConstraint("account_id ~ '^[0-9]+$'", name="ck_p115_intro_account_id"),
        sa.CheckConstraint(
            "trigger_mode IN ('off','import','favorite','playback')",
            name="ck_p115_intro_trigger_mode",
        ),
        sa.CheckConstraint("jsonb_typeof(request_json) = 'object'", name="ck_p115_intro_request_json"),
        sa.CheckConstraint("jsonb_typeof(progress_json) = 'object'", name="ck_p115_intro_progress_json"),
        sa.CheckConstraint(
            "status IN ('ready','claimed','retryable','confirmed','rejected','cancelled','superseded')",
            name="ck_p115_intro_status",
        ),
        sa.CheckConstraint(
            "phase IN ('restore','intro_extract','intro_match','credits_extract','credits_match','project')",
            name="ck_p115_intro_phase",
        ),
        sa.CheckConstraint("state_revision >= 0 AND attempts >= 0", name="ck_p115_intro_counters"),
        sa.CheckConstraint(
            "(status = 'claimed' AND claimed_by IS NOT NULL AND length(btrim(claimed_by)) > 0 "
            "AND claim_token IS NOT NULL AND claimed_at IS NOT NULL AND lease_expires_at > claimed_at) OR "
            "(status <> 'claimed' AND claimed_by IS NULL AND claim_token IS NULL "
            "AND claimed_at IS NULL AND lease_expires_at IS NULL)",
            name="ck_p115_intro_claim",
        ),
        sa.CheckConstraint(
            "(status IN ('confirmed','rejected','cancelled','superseded')) = (finished_at IS NOT NULL)",
            name="ck_p115_intro_finished",
        ),
        sa.CheckConstraint(
            "result_json IS NULL OR jsonb_typeof(result_json) = 'object'",
            name="ck_p115_intro_result_json",
        ),
        sa.CheckConstraint(
            "error_json IS NULL OR jsonb_typeof(error_json) = 'object'",
            name="ck_p115_intro_error_json",
        ),
        sa.UniqueConstraint("event_sequence", name="uq_p115_intro_event_sequence"),
        sa.UniqueConstraint("operation_key", name="uq_p115_intro_operation_key"),
    )
    op.create_index(
        "uq_p115_intro_active_season",
        "p115_intro_detection_operations",
        ["series_tmdb_id", "season_number"],
        unique=True,
        postgresql_where=sa.text("status IN ('ready','claimed','retryable')"),
    )
    op.create_index(
        "ix_p115_intro_due",
        "p115_intro_detection_operations",
        ["status", "available_at", "event_sequence"],
        postgresql_where=sa.text("status IN ('ready','retryable')"),
    )
    op.create_index(
        "ix_p115_intro_claim_recovery",
        "p115_intro_detection_operations",
        ["lease_expires_at", "event_sequence"],
        postgresql_where=sa.text("status = 'claimed'"),
    )
    op.create_index(
        "ix_p115_intro_season_history",
        "p115_intro_detection_operations",
        ["series_tmdb_id", "season_number", sa.text("created_at DESC")],
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.execute("DELETE FROM outbox_events WHERE topic = 'p115.intro-detection.requested'")
    op.drop_table("p115_intro_detection_operations")
