"""Add the P6 durable 115 upload monitor control and operation ledgers.

Revision ID: 0010_p6_upload_monitor
Revises: 0009_p6_ingestion_monitor
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0010_p6_upload_monitor"
down_revision: str | Sequence[str] | None = "0009_p6_ingestion_monitor"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


UPLOAD_OPERATION_STATUSES = (
    "observed",
    "stability_wait",
    "ready",
    "claimed",
    "requesting",
    "awaiting_readback",
    "remote_confirmed",
    "projecting_local",
    "ingestion_pending",
    "retryable",
    "suspended",
    "confirmed",
    "rejected",
    "indeterminate",
    "superseded",
)


def _values(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.create_table(
        "p115_upload_monitor_controls",
        sa.Column("singleton_key", sa.SmallInteger(), primary_key=True, server_default=sa.text("1")),
        sa.Column("config_revision", sa.Text(), nullable=False, server_default=sa.text("repeat('0', 64)")),
        sa.Column("applied_revision", sa.Text(), nullable=True),
        sa.Column("generation", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("applied_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("singleton_key = 1", name="ck_p115_upload_control_singleton"),
        sa.CheckConstraint("config_revision ~ '^[0-9a-f]{64}$'", name="ck_p115_upload_control_config_rev"),
        sa.CheckConstraint(
            "applied_revision IS NULL OR applied_revision ~ '^[0-9a-f]{64}$'",
            name="ck_p115_upload_control_applied_rev",
        ),
        sa.CheckConstraint("generation >= 0", name="ck_p115_upload_control_generation"),
    )
    op.execute(
        """
        INSERT INTO p115_upload_monitor_controls (singleton_key)
        VALUES (1)
        ON CONFLICT (singleton_key) DO NOTHING
        """
    )

    op.create_table(
        "p115_upload_operations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_sequence", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("operation_key", sa.Text(), nullable=False),
        sa.Column("request_hash", postgresql.BYTEA(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("mapping_id", sa.Text(), nullable=False),
        sa.Column("mapping_snapshot_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("config_revision", sa.Text(), nullable=False),
        sa.Column("config_generation", sa.BigInteger(), nullable=False),
        sa.Column("config_snapshot_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("path_key", sa.Text(), nullable=False),
        sa.Column("relative_path", sa.Text(), nullable=False),
        sa.Column("target_cid", sa.Text(), nullable=False),
        sa.Column("fingerprint_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("file_identity_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("completed_source_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'observed'")),
        sa.Column("state_revision", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("stability_deadline", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_size", sa.BigInteger(), nullable=True),
        sa.Column("last_mtime_ns", sa.BigInteger(), nullable=True),
        sa.Column("stable_matches", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("claimed_by", sa.Text(), nullable=True),
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("provider_backend", sa.Text(), nullable=True),
        sa.Column("upload_session_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("provider_request_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("remote_receipt_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("remote_confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("local_projection_state", sa.Text(), nullable=True),
        sa.Column("local_projection_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("local_projected_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ingestion_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(f"status IN ({_values(UPLOAD_OPERATION_STATUSES)})", name="ck_p115_upload_operation_status"),
        sa.CheckConstraint("source IN ('watchdog', 'scan')", name="ck_p115_upload_operation_source"),
        sa.CheckConstraint("event_type IN ('upsert', 'deleted')", name="ck_p115_upload_operation_event_type"),
        sa.CheckConstraint("length(btrim(operation_key)) BETWEEN 1 AND 512", name="ck_p115_upload_operation_key"),
        sa.CheckConstraint("octet_length(request_hash) = 32", name="ck_p115_upload_operation_request_hash"),
        sa.CheckConstraint("length(btrim(mapping_id)) BETWEEN 1 AND 512", name="ck_p115_upload_operation_mapping"),
        sa.CheckConstraint("config_revision ~ '^[0-9a-f]{64}$'", name="ck_p115_upload_operation_config_rev"),
        sa.CheckConstraint("config_generation >= 0", name="ck_p115_upload_operation_generation"),
        sa.CheckConstraint("length(btrim(path)) BETWEEN 1 AND 4096", name="ck_p115_upload_operation_path"),
        sa.CheckConstraint("length(btrim(path_key)) BETWEEN 1 AND 4096", name="ck_p115_upload_operation_path_key"),
        sa.CheckConstraint("length(relative_path) <= 4096", name="ck_p115_upload_operation_relative"),
        sa.CheckConstraint("length(btrim(target_cid)) BETWEEN 1 AND 255", name="ck_p115_upload_operation_target"),
        sa.CheckConstraint("state_revision >= 0", name="ck_p115_upload_operation_state_rev"),
        sa.CheckConstraint("attempts >= 0", name="ck_p115_upload_operation_attempts"),
        sa.CheckConstraint("stable_matches >= 0", name="ck_p115_upload_operation_stable_matches"),
        sa.CheckConstraint("last_size IS NULL OR last_size >= 0", name="ck_p115_upload_operation_last_size"),
        sa.CheckConstraint("last_mtime_ns IS NULL OR last_mtime_ns >= 0", name="ck_p115_upload_operation_last_mtime"),
        sa.CheckConstraint(
            "(claimed_by IS NULL AND claim_token IS NULL AND claimed_at IS NULL AND lease_expires_at IS NULL) OR "
            "(claimed_by IS NOT NULL AND length(btrim(claimed_by)) > 0 AND claim_token IS NOT NULL "
            "AND claimed_at IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_p115_upload_operation_claim",
        ),
        sa.CheckConstraint(
            "claim_token IS NULL OR lease_expires_at > claimed_at",
            name="ck_p115_upload_operation_claim_lease",
        ),
        sa.CheckConstraint(
            "status <> 'claimed' OR claim_token IS NOT NULL",
            name="ck_p115_upload_operation_claimed_state",
        ),
        sa.CheckConstraint(
            "status NOT IN ('requesting', 'awaiting_readback') OR provider_request_started_at IS NOT NULL",
            name="ck_p115_upload_operation_request_started",
        ),
        sa.CheckConstraint(
            "(status IN ('confirmed', 'rejected', 'indeterminate', 'superseded')) = (finished_at IS NOT NULL)",
            name="ck_p115_upload_operation_finished",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(mapping_snapshot_json) = 'object'",
            name="ck_p115_upload_operation_mapping_obj",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(config_snapshot_json) = 'object'",
            name="ck_p115_upload_operation_config_obj",
        ),
        sa.CheckConstraint(
            "fingerprint_json IS NULL OR jsonb_typeof(fingerprint_json) = 'object'",
            name="ck_p115_upload_operation_fingerprint_obj",
        ),
        sa.CheckConstraint(
            "file_identity_json IS NULL OR jsonb_typeof(file_identity_json) = 'object'",
            name="ck_p115_upload_operation_identity_obj",
        ),
        sa.CheckConstraint(
            "completed_source_json IS NULL OR jsonb_typeof(completed_source_json) = 'object'",
            name="ck_p115_upload_operation_completed_source_obj",
        ),
        sa.CheckConstraint(
            "(event_type = 'deleted') = (completed_source_json IS NOT NULL)",
            name="ck_p115_upload_operation_completed_source_event",
        ),
        sa.CheckConstraint(
            "upload_session_json IS NULL OR jsonb_typeof(upload_session_json) = 'object'",
            name="ck_p115_upload_operation_session_obj",
        ),
        sa.CheckConstraint(
            "provider_backend IS NULL OR provider_backend IN ('openapi', 'cookie')",
            name="ck_p115_upload_operation_backend",
        ),
        sa.CheckConstraint(
            "upload_session_json IS NULL OR "
            "(upload_session_json ? 'account_id' AND upload_session_json ? 'backend' "
            "AND upload_session_json ->> 'account_id' ~ '^[0-9]+$' "
            "AND upload_session_json ->> 'backend' = provider_backend)",
            name="ck_p115_upload_operation_session_pin",
        ),
        sa.CheckConstraint(
            "remote_receipt_json IS NULL OR jsonb_typeof(remote_receipt_json) = 'object'",
            name="ck_p115_upload_operation_receipt_obj",
        ),
        sa.CheckConstraint(
            "remote_receipt_json IS NULL OR "
            "(remote_receipt_json ? 'account_id' AND remote_receipt_json ? 'backend' "
            "AND remote_receipt_json ->> 'account_id' ~ '^[0-9]+$' "
            "AND remote_receipt_json ->> 'backend' IN ('openapi', 'cookie'))",
            name="ck_p115_upload_operation_receipt_pin",
        ),
        sa.CheckConstraint(
            "local_projection_json IS NULL OR jsonb_typeof(local_projection_json) = 'object'",
            name="ck_p115_upload_operation_projection_obj",
        ),
        sa.CheckConstraint(
            "result_json IS NULL OR jsonb_typeof(result_json) = 'object'",
            name="ck_p115_upload_operation_result_obj",
        ),
        sa.CheckConstraint(
            "error_json IS NULL OR jsonb_typeof(error_json) = 'object'",
            name="ck_p115_upload_operation_error_obj",
        ),
        sa.UniqueConstraint("event_sequence", name="uq_p115_upload_operation_sequence"),
        sa.UniqueConstraint("operation_key", name="uq_p115_upload_operation_key"),
    )
    op.create_index(
        "ix_p115_upload_operation_stability",
        "p115_upload_operations",
        ["status", "available_at", "event_sequence", "id"],
        postgresql_where=sa.text("status IN ('observed', 'stability_wait')"),
    )
    op.create_index(
        "ix_p115_upload_operation_claimable",
        "p115_upload_operations",
        ["status", "available_at", "event_sequence", "id"],
        postgresql_where=sa.text(
            "claim_token IS NULL AND status IN "
            "('ready', 'retryable', 'awaiting_readback', 'remote_confirmed', 'projecting_local', 'ingestion_pending')"
        ),
    )
    op.create_index(
        "ix_p115_upload_operation_recovery",
        "p115_upload_operations",
        ["lease_expires_at", "event_sequence", "id"],
        postgresql_where=sa.text("claim_token IS NOT NULL"),
    )
    op.create_index(
        "ix_p115_upload_operation_mapping_path",
        "p115_upload_operations",
        ["mapping_id", "path_key", "created_at"],
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.execute("DELETE FROM outbox_events WHERE topic = 'p115.upload.requested'")
    op.drop_table("p115_upload_operations")
    op.drop_table("p115_upload_monitor_controls")
