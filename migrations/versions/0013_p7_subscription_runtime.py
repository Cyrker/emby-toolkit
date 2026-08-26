"""Add durable P7 subscription operations, quota reservations, and runtime state.

Revision ID: 0013_p7_subscription_runtime
Revises: 0012_p6_intro_detection
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0013_p7_subscription_runtime"
down_revision: str | Sequence[str] | None = "0012_p6_intro_detection"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.create_table(
        "subscription_operations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_sequence", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("operation_key", sa.Text(), nullable=False),
        sa.Column("request_hash", postgresql.BYTEA(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("scope_key", sa.Text(), nullable=False),
        sa.Column("request_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'ready'")),
        sa.Column("state_revision", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("claimed_by", sa.Text(), nullable=True),
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("provider_request_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("provider_receipt_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("readback_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("operation_key ~ '^[0-9a-f]{64}$'", name="ck_subscription_operation_key"),
        sa.CheckConstraint("octet_length(request_hash) = 32", name="ck_subscription_operation_hash"),
        sa.CheckConstraint(
            "kind IN ('create','update','delete','resubscribe','reconcile')",
            name="ck_subscription_operation_kind",
        ),
        sa.CheckConstraint("length(btrim(scope_key)) > 0", name="ck_subscription_operation_scope"),
        sa.CheckConstraint("jsonb_typeof(request_json) = 'object'", name="ck_subscription_operation_request"),
        sa.CheckConstraint(
            "status IN ('ready','claimed','requesting','awaiting_readback','remote_confirmed',"
            "'retryable','confirmed','rejected','indeterminate','cancelled')",
            name="ck_subscription_operation_status",
        ),
        sa.CheckConstraint("state_revision >= 0 AND attempts >= 0", name="ck_subscription_operation_counters"),
        sa.CheckConstraint(
            "(status IN ('claimed','requesting','remote_confirmed') "
            "AND claimed_by IS NOT NULL AND length(btrim(claimed_by)) > 0 "
            "AND claim_token IS NOT NULL AND claimed_at IS NOT NULL AND lease_expires_at > claimed_at) OR "
            "(status NOT IN ('claimed','requesting','remote_confirmed') "
            "AND claimed_by IS NULL AND claim_token IS NULL AND claimed_at IS NULL AND lease_expires_at IS NULL)",
            name="ck_subscription_operation_claim",
        ),
        sa.CheckConstraint(
            "status NOT IN ('requesting','awaiting_readback','remote_confirmed') "
            "OR provider_request_started_at IS NOT NULL",
            name="ck_subscription_operation_provider_started",
        ),
        sa.CheckConstraint(
            "(status IN ('confirmed','rejected','indeterminate','cancelled')) = (finished_at IS NOT NULL)",
            name="ck_subscription_operation_finished",
        ),
        sa.CheckConstraint(
            "provider_receipt_json IS NULL OR jsonb_typeof(provider_receipt_json) = 'object'",
            name="ck_subscription_operation_receipt",
        ),
        sa.CheckConstraint(
            "readback_json IS NULL OR jsonb_typeof(readback_json) = 'object'",
            name="ck_subscription_operation_readback",
        ),
        sa.CheckConstraint(
            "result_json IS NULL OR jsonb_typeof(result_json) = 'object'",
            name="ck_subscription_operation_result",
        ),
        sa.CheckConstraint(
            "error_json IS NULL OR jsonb_typeof(error_json) = 'object'",
            name="ck_subscription_operation_error",
        ),
        sa.UniqueConstraint("event_sequence", name="uq_subscription_operation_sequence"),
        sa.UniqueConstraint("operation_key", name="uq_subscription_operation_key"),
    )
    op.create_index(
        "ix_subscription_operation_due",
        "subscription_operations",
        ["status", "available_at", "event_sequence"],
        postgresql_where=sa.text("status IN ('ready','retryable')"),
    )
    op.create_index(
        "ix_subscription_operation_claim_recovery",
        "subscription_operations",
        ["lease_expires_at", "event_sequence"],
        postgresql_where=sa.text("status IN ('claimed','requesting','awaiting_readback','remote_confirmed')"),
    )
    op.create_index(
        "ix_subscription_operation_scope_history",
        "subscription_operations",
        ["scope_key", sa.text("created_at DESC")],
    )

    op.create_table(
        "subscription_quota_reservations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("reservation_key", sa.Text(), nullable=False),
        sa.Column(
            "operation_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("subscription_operations.id", ondelete="RESTRICT"),
            nullable=True,
        ),
        sa.Column("quota_date", sa.Date(), nullable=False),
        sa.Column("units", sa.Integer(), nullable=False),
        sa.Column("cap_snapshot", sa.Integer(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'reserved'")),
        sa.Column("state_revision", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("committed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("reservation_key ~ '^[0-9a-f]{64}$'", name="ck_subscription_quota_key"),
        sa.CheckConstraint("units > 0 AND cap_snapshot >= 0", name="ck_subscription_quota_values"),
        sa.CheckConstraint(
            "status IN ('reserved','committed','released')",
            name="ck_subscription_quota_status",
        ),
        sa.CheckConstraint("state_revision >= 0", name="ck_subscription_quota_revision"),
        sa.CheckConstraint(
            "(status = 'reserved' AND committed_at IS NULL AND released_at IS NULL) OR "
            "(status = 'committed' AND committed_at IS NOT NULL AND released_at IS NULL) OR "
            "(status = 'released' AND committed_at IS NULL AND released_at IS NOT NULL)",
            name="ck_subscription_quota_terminal",
        ),
        sa.UniqueConstraint("reservation_key", name="uq_subscription_quota_key"),
        sa.UniqueConstraint("operation_id", name="uq_subscription_quota_operation"),
    )
    op.create_index(
        "ix_subscription_quota_active",
        "subscription_quota_reservations",
        ["quota_date", "status", "expires_at"],
        postgresql_where=sa.text("status IN ('reserved','committed')"),
    )

    op.create_table(
        "subscription_runtime_states",
        sa.Column("state_key", sa.Text(), primary_key=True),
        sa.Column("state_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("state_revision", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("length(btrim(state_key)) > 0", name="ck_subscription_runtime_state_key"),
        sa.CheckConstraint("jsonb_typeof(state_json) = 'object'", name="ck_subscription_runtime_state_json"),
        sa.CheckConstraint(
            "schema_version >= 1 AND state_revision >= 0",
            name="ck_subscription_runtime_state_versions",
        ),
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.execute("DELETE FROM outbox_events WHERE topic = 'subscription.operation.requested'")
    op.drop_table("subscription_runtime_states")
    op.drop_table("subscription_quota_reservations")
    op.drop_table("subscription_operations")
