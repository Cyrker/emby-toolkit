"""Add P9 sharing operation and contribution ledger state."""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0018_p9_sharing_ledger"
down_revision: str | Sequence[str] | None = "0017_p8_effects_and_maintenance"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.create_table(
        "sharing_operations",
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
        sa.Column("step_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "state_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")
        ),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("operation_key ~ '^[0-9a-f]{64}$'", name="ck_sharing_operation_key"),
        sa.CheckConstraint("octet_length(request_hash) = 32", name="ck_sharing_operation_hash"),
        sa.CheckConstraint(
            "kind IN ('register_source','transfer','share_request','virtual_import',"
            "'hdhive_checkin','telegram_delivery','proxy_play')",
            name="ck_sharing_operation_kind",
        ),
        sa.CheckConstraint("length(btrim(scope_key)) > 0", name="ck_sharing_operation_scope"),
        sa.CheckConstraint("jsonb_typeof(request_json) = 'object'", name="ck_sharing_operation_request"),
        sa.CheckConstraint(
            "status IN ('ready','claimed','requesting','awaiting_readback',"
            "'retryable','confirmed','rejected','indeterminate')",
            name="ck_sharing_operation_status",
        ),
        sa.CheckConstraint("state_revision >= 0 AND attempts >= 0", name="ck_sharing_operation_counters"),
        sa.CheckConstraint(
            "(status IN ('claimed','requesting') AND claimed_by IS NOT NULL AND length(btrim(claimed_by)) > 0 "
            "AND claim_token IS NOT NULL AND claimed_at IS NOT NULL AND lease_expires_at > claimed_at) OR "
            "(status NOT IN ('claimed','requesting') AND claimed_by IS NULL AND claim_token IS NULL "
            "AND claimed_at IS NULL AND lease_expires_at IS NULL)",
            name="ck_sharing_operation_claim",
        ),
        sa.CheckConstraint(
            "status NOT IN ('requesting','awaiting_readback') OR step_started_at IS NOT NULL",
            name="ck_sharing_operation_step_started",
        ),
        sa.CheckConstraint(
            "(status IN ('confirmed','rejected','indeterminate')) = (finished_at IS NOT NULL)",
            name="ck_sharing_operation_finished",
        ),
        sa.CheckConstraint("jsonb_typeof(state_json) = 'object'", name="ck_sharing_operation_state"),
        sa.CheckConstraint(
            "result_json IS NULL OR jsonb_typeof(result_json) = 'object'", name="ck_sharing_operation_result"
        ),
        sa.CheckConstraint(
            "error_json IS NULL OR jsonb_typeof(error_json) = 'object'", name="ck_sharing_operation_error"
        ),
        sa.UniqueConstraint("event_sequence", name="uq_sharing_operation_sequence"),
        sa.UniqueConstraint("operation_key", name="uq_sharing_operation_key"),
    )
    op.create_index(
        "ix_sharing_operation_due",
        "sharing_operations",
        ["status", "available_at", "event_sequence"],
        postgresql_where=sa.text("status IN ('ready','retryable','awaiting_readback')"),
    )
    op.create_index(
        "ix_sharing_operation_claim_recovery",
        "sharing_operations",
        ["lease_expires_at", "event_sequence"],
        postgresql_where=sa.text("status IN ('claimed','requesting')"),
    )
    op.create_index(
        "ix_sharing_operation_scope_history",
        "sharing_operations",
        ["scope_key", sa.text("event_sequence DESC")],
    )
    op.create_index(
        "uq_sharing_operation_active_scope",
        "sharing_operations",
        ["scope_key"],
        unique=True,
        postgresql_where=sa.text(
            "status IN ('ready','claimed','requesting','awaiting_readback','retryable','indeterminate')"
        ),
    )

    op.create_table(
        "sharing_credit_accounts",
        sa.Column("account_id", sa.Text(), primary_key=True),
        sa.Column("balance", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("revision", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("length(btrim(account_id)) > 0", name="ck_sharing_credit_account_id"),
        sa.CheckConstraint("balance >= 0", name="ck_sharing_credit_account_balance"),
        sa.CheckConstraint("revision >= 0", name="ck_sharing_credit_account_revision"),
    )

    op.create_table(
        "sharing_credit_ledger",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_sequence", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("account_id", sa.Text(), nullable=False),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("delta", sa.BigInteger(), nullable=False),
        sa.Column("balance_after", sa.BigInteger(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("reference", sa.Text(), nullable=True),
        sa.Column("operation_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("idempotency_key ~ '^[0-9a-f]{64}$'", name="ck_sharing_credit_entry_key"),
        sa.CheckConstraint("delta <> 0", name="ck_sharing_credit_entry_delta"),
        sa.CheckConstraint("balance_after >= 0", name="ck_sharing_credit_entry_balance"),
        sa.CheckConstraint("length(btrim(reason)) > 0", name="ck_sharing_credit_entry_reason"),
        sa.CheckConstraint("jsonb_typeof(metadata) = 'object'", name="ck_sharing_credit_entry_metadata"),
        sa.UniqueConstraint("event_sequence", name="uq_sharing_credit_entry_sequence"),
        sa.UniqueConstraint("account_id", "idempotency_key", name="uq_sharing_credit_entry_idempotency"),
        sa.ForeignKeyConstraint(
            ["account_id"],
            ["public.sharing_credit_accounts.account_id"],
            name="fk_sharing_credit_entry_account",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["operation_id"],
            ["public.sharing_operations.id"],
            name="fk_sharing_credit_entry_operation",
            ondelete="RESTRICT",
        ),
    )
    op.create_index(
        "ix_sharing_credit_ledger_account_created",
        "sharing_credit_ledger",
        ["account_id", sa.text("created_at DESC"), sa.text("event_sequence DESC")],
    )
    op.create_index("ix_sharing_credit_ledger_operation", "sharing_credit_ledger", ["operation_id"])


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.drop_table("sharing_credit_ledger")
    op.drop_table("sharing_credit_accounts")
    op.drop_table("sharing_operations")
