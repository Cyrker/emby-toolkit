"""Add durable P6 playback operations, artifacts, leases, and QR sessions.

Revision ID: 0011_p6_playback
Revises: 0010_p6_upload_monitor
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0011_p6_playback"
down_revision: str | Sequence[str] | None = "0010_p6_upload_monitor"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.create_table(
        "p115_playback_operations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_sequence", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("operation_key", sa.Text(), nullable=False),
        sa.Column("request_hash", postgresql.BYTEA(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("account_id", sa.Text(), nullable=False),
        sa.Column("provider_backend", sa.Text(), nullable=False),
        sa.Column("request_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("artifact_key", sa.Text(), nullable=True),
        sa.Column("artifact_generation", sa.BigInteger(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'ready'")),
        sa.Column("state_revision", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("claimed_by", sa.Text(), nullable=True),
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("provider_request_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("provider_session_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("remote_receipt_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("readback_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("octet_length(request_hash) = 32", name="ck_p115_playback_operation_hash"),
        sa.CheckConstraint("operation_key ~ '^[0-9a-f]{64}$'", name="ck_p115_playback_operation_key"),
        sa.CheckConstraint(
            "kind IN ('prepare','ensure_temp_dir','speedtest','reclaim')",
            name="ck_p115_playback_operation_kind",
        ),
        sa.CheckConstraint(
            "mode IN ('pool','copy','virtual','maintenance')",
            name="ck_p115_playback_operation_mode",
        ),
        sa.CheckConstraint(
            "provider_backend IN ('openapi','cookie')",
            name="ck_p115_playback_operation_backend",
        ),
        sa.CheckConstraint(
            "status IN ('ready','claimed','requesting','awaiting_readback','remote_confirmed',"
            "'retryable','confirmed','rejected','indeterminate')",
            name="ck_p115_playback_operation_status",
        ),
        sa.CheckConstraint("state_revision >= 0 AND attempts >= 0", name="ck_p115_playback_operation_counters"),
        sa.CheckConstraint("jsonb_typeof(request_json) = 'object'", name="ck_p115_playback_operation_request"),
        sa.CheckConstraint(
            "artifact_key IS NULL OR artifact_key ~ '^[0-9a-f]{64}$'",
            name="ck_p115_playback_operation_artifact_key",
        ),
        sa.CheckConstraint(
            "(artifact_key IS NULL) = (artifact_generation IS NULL)",
            name="ck_p115_playback_operation_artifact_pair",
        ),
        sa.CheckConstraint(
            "artifact_generation IS NULL OR artifact_generation > 0",
            name="ck_p115_playback_operation_artifact_generation",
        ),
        sa.CheckConstraint(
            "(claimed_by IS NULL AND claim_token IS NULL AND claimed_at IS NULL AND lease_expires_at IS NULL) OR "
            "(length(btrim(claimed_by)) > 0 AND claim_token IS NOT NULL AND claimed_at IS NOT NULL "
            "AND lease_expires_at > claimed_at)",
            name="ck_p115_playback_operation_claim",
        ),
        sa.CheckConstraint(
            "status NOT IN ('requesting','awaiting_readback','remote_confirmed') "
            "OR provider_request_started_at IS NOT NULL",
            name="ck_p115_playback_operation_provider_started",
        ),
        sa.CheckConstraint(
            "(status IN ('confirmed','rejected','indeterminate')) = (finished_at IS NOT NULL)",
            name="ck_p115_playback_operation_finished",
        ),
        sa.UniqueConstraint("event_sequence", name="uq_p115_playback_operation_sequence"),
        sa.UniqueConstraint("operation_key", name="uq_p115_playback_operation_key"),
    )
    op.create_index("ix_p115_playback_operation_due", "p115_playback_operations", ["status", "available_at"])
    op.create_index("ix_p115_playback_operation_claim", "p115_playback_operations", ["lease_expires_at"])

    op.create_table(
        "p115_playback_artifacts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("artifact_key", sa.Text(), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column(
            "operation_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("p115_playback_operations.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("account_id", sa.Text(), nullable=False),
        sa.Column("provider_backend", sa.Text(), nullable=False),
        sa.Column("target_cid", sa.Text(), nullable=False),
        sa.Column("source_sha1", sa.Text(), nullable=False),
        sa.Column("source_size", sa.BigInteger(), nullable=False),
        sa.Column("source_pick_code", sa.Text(), nullable=False),
        sa.Column("source_fid", sa.Text(), nullable=False),
        sa.Column("file_name", sa.Text(), nullable=False),
        sa.Column("remote_fid", sa.Text(), nullable=False),
        sa.Column("remote_pick_code", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'available'")),
        sa.Column("state_revision", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("remote_receipt_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("reclaim_not_before", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reclaimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("artifact_key ~ '^[0-9a-f]{64}$'", name="ck_p115_playback_artifact_key"),
        sa.CheckConstraint("generation > 0 AND state_revision >= 0", name="ck_p115_playback_artifact_counters"),
        sa.CheckConstraint("mode IN ('pool','copy','virtual','maintenance')", name="ck_p115_playback_artifact_mode"),
        sa.CheckConstraint("provider_backend IN ('openapi','cookie')", name="ck_p115_playback_artifact_backend"),
        sa.CheckConstraint("source_sha1 ~ '^[A-F0-9]{40}$'", name="ck_p115_playback_artifact_sha1"),
        sa.CheckConstraint("source_size > 0", name="ck_p115_playback_artifact_size"),
        sa.CheckConstraint(
            "status IN ('available','reclaim_pending','reclaimed','indeterminate')",
            name="ck_p115_playback_artifact_status",
        ),
        sa.CheckConstraint("jsonb_typeof(remote_receipt_json) = 'object'", name="ck_p115_playback_artifact_receipt"),
        sa.CheckConstraint(
            "(status = 'reclaimed') = (reclaimed_at IS NOT NULL)",
            name="ck_p115_playback_artifact_reclaimed",
        ),
        sa.UniqueConstraint("artifact_key", "generation", name="uq_p115_playback_artifact_generation"),
    )
    op.create_index("ix_p115_playback_artifact_reclaim", "p115_playback_artifacts", ["status", "reclaim_not_before"])

    op.create_table(
        "p115_playback_leases",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("lease_key", sa.Text(), nullable=False),
        sa.Column(
            "artifact_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("p115_playback_artifacts.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("owner_user_id", sa.Text(), nullable=False),
        sa.Column("item_id", sa.Text(), nullable=False),
        sa.Column("play_session_id", sa.Text(), nullable=False),
        sa.Column("client_key", sa.Text(), nullable=False),
        sa.Column("user_agent_hash", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'active'")),
        sa.Column("state_revision", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("cleanup_not_before", sa.DateTime(timezone=True), nullable=False),
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("lease_key ~ '^[0-9a-f]{64}$'", name="ck_p115_playback_lease_key"),
        sa.CheckConstraint("user_agent_hash ~ '^[0-9a-f]{64}$'", name="ck_p115_playback_lease_ua_hash"),
        sa.CheckConstraint("mode IN ('pool','copy','virtual','maintenance')", name="ck_p115_playback_lease_mode"),
        sa.CheckConstraint("status IN ('active','released','expired')", name="ck_p115_playback_lease_status"),
        sa.CheckConstraint("state_revision >= 0", name="ck_p115_playback_lease_revision"),
        sa.CheckConstraint("cleanup_not_before >= created_at", name="ck_p115_playback_lease_cleanup"),
        sa.CheckConstraint(
            "(status = 'active' AND released_at IS NULL) OR (status <> 'active' AND released_at IS NOT NULL)",
            name="ck_p115_playback_lease_released",
        ),
        sa.UniqueConstraint("lease_key", name="uq_p115_playback_lease_key"),
    )
    op.create_index("ix_p115_playback_lease_active", "p115_playback_leases", ["artifact_id", "status", "expires_at"])

    op.create_table(
        "p115_playback_qr_sessions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("owner_user_id", sa.Text(), nullable=False),
        sa.Column("provider_uid", sa.Text(), nullable=False),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("app_type", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'waiting'")),
        sa.Column("provider_session_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("credentials_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=False, server_default=sa.text("''")),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("polled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("status IN ('waiting','succeeded','expired','error')", name="ck_p115_playback_qr_status"),
        sa.CheckConstraint(
            "jsonb_typeof(provider_session_json) = 'object'",
            name="ck_p115_playback_qr_provider_session",
        ),
        sa.CheckConstraint(
            "credentials_json IS NULL OR jsonb_typeof(credentials_json) = 'object'",
            name="ck_p115_playback_qr_credentials",
        ),
        sa.CheckConstraint(
            "(status = 'succeeded') = (credentials_json IS NOT NULL)",
            name="ck_p115_playback_qr_credentials_status",
        ),
        sa.UniqueConstraint("provider_uid", name="uq_p115_playback_qr_provider_uid"),
    )
    op.create_index("ix_p115_playback_qr_owner", "p115_playback_qr_sessions", ["owner_user_id", "created_at"])
    op.create_index("ix_p115_playback_qr_expiry", "p115_playback_qr_sessions", ["status", "expires_at"])


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.drop_table("p115_playback_qr_sessions")
    op.drop_table("p115_playback_leases")
    op.drop_table("p115_playback_artifacts")
    op.drop_table("p115_playback_operations")
