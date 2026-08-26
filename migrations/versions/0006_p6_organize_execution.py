"""Add durable P6 organization execution, effect, and reconciliation ledgers.

Revision ID: 0006_p6_organize_execution
Revises: 0005_p6_organize_plans
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0006_p6_organize_execution"
down_revision: str | Sequence[str] | None = "0005_p6_organize_plans"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.create_table(
        "p115_organize_plan_executions",
        sa.Column("execution_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "plan_id",
            sa.Text(),
            sa.ForeignKey("p115_organize_plans.plan_id", ondelete="RESTRICT"),
            nullable=False,
            unique=True,
        ),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("approved_intent_id", sa.Text(), nullable=False, unique=True),
        sa.Column("request_hash", sa.Text(), nullable=False),
        sa.Column("account_id", sa.Text(), nullable=False),
        sa.Column("provider_backend", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("preflight_hash", sa.Text(), nullable=False),
        sa.Column("preflight_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "job_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("job_runs.id", ondelete="RESTRICT"),
            nullable=False,
            unique=True,
        ),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("schema_version = 1", name="ck_p115_organize_executions_schema_version"),
        sa.CheckConstraint(
            "status IN ('prepared', 'running', 'confirmed', 'rejected', 'indeterminate', 'manual_review')",
            name="ck_p115_organize_executions_status",
        ),
        sa.CheckConstraint("account_id ~ '^[0-9]+$'", name="ck_p115_organize_executions_account"),
        sa.CheckConstraint(
            "provider_backend IN ('openapi', 'cookie')",
            name="ck_p115_organize_executions_backend",
        ),
        sa.CheckConstraint(
            "request_hash ~ '^[0-9a-f]{64}$' AND preflight_hash ~ '^[0-9a-f]{64}$'",
            name="ck_p115_organize_executions_hashes",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(preflight_json) = 'object'",
            name="ck_p115_organize_executions_preflight_object",
        ),
        sa.CheckConstraint(
            "(status IN ('confirmed', 'rejected', 'indeterminate', 'manual_review')) = (finished_at IS NOT NULL)",
            name="ck_p115_organize_executions_terminal_finished",
        ),
        sa.UniqueConstraint(
            "execution_id",
            "plan_id",
            name="uq_p115_organize_executions_id_plan",
        ),
    )
    op.create_index(
        "ix_p115_organize_executions_status_updated",
        "p115_organize_plan_executions",
        ["status", "updated_at"],
    )
    op.create_index(
        "ix_p115_organize_executions_account_created",
        "p115_organize_plan_executions",
        ["account_id", sa.text("created_at DESC")],
    )

    op.create_table(
        "p115_organize_effects",
        sa.Column("effect_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("effect_key", sa.Text(), nullable=False, unique=True),
        sa.Column("request_hash", sa.Text(), nullable=False),
        sa.Column("request_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("origin_plan_id", sa.Text(), nullable=False),
        sa.Column("origin_action_id", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("provider_backend", sa.Text(), nullable=False),
        sa.Column("sponsor_execution_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("sponsor_action_index", sa.Integer(), nullable=True),
        sa.Column("sponsor_account_id", sa.Text(), nullable=True),
        sa.Column("claimed_by", sa.Text(), nullable=True),
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("provider_call_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("receipt_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("receipt_persisted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("readback_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("readback_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_object_id", sa.Text(), nullable=True),
        sa.Column("resolved_parent_cid", sa.Text(), nullable=True),
        sa.Column("completion_mode", sa.Text(), nullable=True),
        sa.Column("resolution_status", sa.Text(), nullable=True),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["origin_plan_id", "origin_action_id"],
            ["p115_organize_plan_actions.plan_id", "p115_organize_plan_actions.action_id"],
            name="fk_p115_organize_effects_origin_action",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("effect_key ~ '^[0-9a-f]{64}$'", name="ck_p115_organize_effects_key"),
        sa.CheckConstraint("request_hash ~ '^[0-9a-f]{64}$'", name="ck_p115_organize_effects_request_hash"),
        sa.CheckConstraint(
            "jsonb_typeof(request_json) = 'object'",
            name="ck_p115_organize_effects_request_object",
        ),
        sa.CheckConstraint(
            "kind IN ('ENSURE_DIRECTORY', 'MOVE', 'RENAME')",
            name="ck_p115_organize_effects_kind",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'claimed', 'requesting', 'awaiting_readback', 'retryable', "
            "'confirmed', 'rejected', 'indeterminate')",
            name="ck_p115_organize_effects_status",
        ),
        sa.CheckConstraint("attempts >= 0", name="ck_p115_organize_effects_attempts"),
        sa.CheckConstraint(
            "(attempts = 0) = (last_attempt_at IS NULL)",
            name="ck_p115_organize_effects_attempt_timestamp",
        ),
        sa.CheckConstraint(
            "provider_backend IN ('openapi', 'cookie')",
            name="ck_p115_organize_effects_backend",
        ),
        sa.CheckConstraint(
            "(status IN ('claimed', 'requesting', 'awaiting_readback')) = "
            "(claim_token IS NOT NULL AND claimed_by IS NOT NULL AND claimed_at IS NOT NULL "
            "AND lease_expires_at IS NOT NULL)",
            name="ck_p115_organize_effects_claim",
        ),
        sa.CheckConstraint(
            "claim_token IS NULL OR lease_expires_at > claimed_at",
            name="ck_p115_organize_effects_lease_window",
        ),
        sa.CheckConstraint(
            "(sponsor_execution_id IS NULL AND sponsor_action_index IS NULL AND sponsor_account_id IS NULL) OR "
            "(sponsor_execution_id IS NOT NULL AND sponsor_action_index IS NOT NULL "
            "AND sponsor_account_id IS NOT NULL)",
            name="ck_p115_organize_effects_sponsor_pair",
        ),
        sa.CheckConstraint(
            "status NOT IN ('claimed', 'requesting', 'awaiting_readback') OR sponsor_execution_id IS NOT NULL",
            name="ck_p115_organize_effects_active_sponsor",
        ),
        sa.CheckConstraint(
            "status NOT IN ('requesting', 'awaiting_readback') OR provider_call_started_at IS NOT NULL",
            name="ck_p115_organize_effects_request_started",
        ),
        sa.CheckConstraint(
            "status <> 'awaiting_readback' OR receipt_persisted_at IS NOT NULL",
            name="ck_p115_organize_effects_receipt_persisted",
        ),
        sa.CheckConstraint(
            "(status IN ('confirmed', 'rejected', 'indeterminate')) = (finished_at IS NOT NULL)",
            name="ck_p115_organize_effects_terminal_finished",
        ),
        sa.CheckConstraint(
            "status <> 'confirmed' OR "
            "(readback_json IS NOT NULL AND readback_at IS NOT NULL AND completion_mode IS NOT NULL "
            "AND resolved_parent_cid IS NOT NULL)",
            name="ck_p115_organize_effects_confirmed_evidence",
        ),
        sa.CheckConstraint(
            "status NOT IN ('rejected', 'indeterminate') OR error_json IS NOT NULL",
            name="ck_p115_organize_effects_terminal_error",
        ),
        sa.CheckConstraint(
            "completion_mode IS NULL OR completion_mode IN "
            "('provider_applied', 'observed_preexisting', 'reused_confirmed', 'reconciled')",
            name="ck_p115_organize_effects_completion_mode",
        ),
        sa.CheckConstraint(
            "resolution_status IS NULL OR resolution_status IN ('applied', 'absent', 'conflict', 'unknown')",
            name="ck_p115_organize_effects_resolution_status",
        ),
        sa.CheckConstraint(
            "receipt_json IS NULL OR jsonb_typeof(receipt_json) = 'object'",
            name="ck_p115_organize_effects_receipt_object",
        ),
        sa.CheckConstraint(
            "readback_json IS NULL OR jsonb_typeof(readback_json) = 'object'",
            name="ck_p115_organize_effects_readback_object",
        ),
        sa.CheckConstraint(
            "result_json IS NULL OR jsonb_typeof(result_json) = 'object'",
            name="ck_p115_organize_effects_result_object",
        ),
        sa.CheckConstraint(
            "error_json IS NULL OR jsonb_typeof(error_json) = 'object'",
            name="ck_p115_organize_effects_error_object",
        ),
    )
    op.create_index(
        "ix_p115_organize_effects_claimable",
        "p115_organize_effects",
        ["status", "available_at", "created_at"],
        postgresql_where=sa.text("status IN ('pending', 'retryable')"),
    )
    op.create_index(
        "ix_p115_organize_effects_active_lease",
        "p115_organize_effects",
        ["lease_expires_at", "created_at"],
        postgresql_where=sa.text("status IN ('claimed', 'requesting', 'awaiting_readback')"),
    )
    op.create_index(
        "ix_p115_organize_effects_origin",
        "p115_organize_effects",
        ["origin_plan_id", "origin_action_id"],
    )

    op.create_unique_constraint(
        "uq_p115_organize_plan_action_effect",
        "p115_organize_plan_actions",
        ["plan_id", "action_index", "effect_key"],
    )
    op.create_table(
        "p115_organize_execution_actions",
        sa.Column(
            "execution_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("p115_organize_plan_executions.execution_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("plan_id", sa.Text(), nullable=False),
        sa.Column("action_index", sa.Integer(), nullable=False),
        sa.Column("action_id", sa.Text(), nullable=False),
        sa.Column("effect_key", sa.Text(), nullable=False),
        sa.Column("binding_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("disposition", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("verification_status", sa.Text(), nullable=False),
        sa.Column("verification_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint(
            "execution_id",
            "action_index",
            name="pk_p115_organize_execution_actions",
        ),
        sa.UniqueConstraint("execution_id", "action_id", name="uq_p115_organize_execution_action_id"),
        sa.UniqueConstraint("execution_id", "effect_key", name="uq_p115_organize_execution_effect"),
        sa.ForeignKeyConstraint(
            ["execution_id", "plan_id"],
            ["p115_organize_plan_executions.execution_id", "p115_organize_plan_executions.plan_id"],
            name="fk_p115_organize_execution_action_execution",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["plan_id", "action_index", "effect_key"],
            [
                "p115_organize_plan_actions.plan_id",
                "p115_organize_plan_actions.action_index",
                "p115_organize_plan_actions.effect_key",
            ],
            name="fk_p115_organize_execution_action_plan",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["effect_key"],
            ["p115_organize_effects.effect_key"],
            name="fk_p115_organize_execution_action_effect",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("action_index >= 0", name="ck_p115_organize_execution_action_index"),
        sa.CheckConstraint("action_id ~ '^[0-9a-f]{64}$'", name="ck_p115_organize_execution_action_id"),
        sa.CheckConstraint("effect_key ~ '^[0-9a-f]{64}$'", name="ck_p115_organize_execution_effect_key"),
        sa.CheckConstraint(
            "jsonb_typeof(binding_json) = 'object'",
            name="ck_p115_organize_execution_binding_object",
        ),
        sa.CheckConstraint(
            "disposition IN ('pending', 'observed_preexisting', 'reused_confirmed', "
            "'reused_reconciled', 'reopened', 'blocked')",
            name="ck_p115_organize_execution_disposition",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'confirmed', 'rejected', 'indeterminate', 'manual_review')",
            name="ck_p115_organize_execution_action_status",
        ),
        sa.CheckConstraint(
            "verification_status IN ('pending', 'verified', 'postcondition_drift', 'reconciled_applied', "
            "'reconciled_absent', 'conflict', 'unknown', 'rejected')",
            name="ck_p115_organize_execution_verification_status",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(verification_json) = 'object'",
            name="ck_p115_organize_execution_verification_object",
        ),
    )
    op.create_index(
        "ix_p115_organize_execution_actions_effect",
        "p115_organize_execution_actions",
        ["effect_key", "execution_id"],
    )
    op.create_foreign_key(
        "fk_p115_organize_effects_sponsor_action",
        "p115_organize_effects",
        "p115_organize_execution_actions",
        ["sponsor_execution_id", "sponsor_action_index"],
        ["execution_id", "action_index"],
        ondelete="RESTRICT",
    )

    op.create_table(
        "p115_organize_reconciliation_intents",
        sa.Column("reconciliation_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("approved_intent_id", sa.Text(), nullable=False, unique=True),
        sa.Column("request_hash", sa.Text(), nullable=False),
        sa.Column("execution_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("action_index", sa.Integer(), nullable=False),
        sa.Column("effect_key", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("outcome", sa.Text(), nullable=True),
        sa.Column("actor", sa.Text(), nullable=True),
        sa.Column("evidence_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["execution_id", "action_index"],
            ["p115_organize_execution_actions.execution_id", "p115_organize_execution_actions.action_index"],
            name="fk_p115_organize_reconciliation_action",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["effect_key"],
            ["p115_organize_effects.effect_key"],
            name="fk_p115_organize_reconciliation_effect",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "request_hash ~ '^[0-9a-f]{64}$'",
            name="ck_p115_organize_reconciliation_request_hash",
        ),
        sa.CheckConstraint(
            "status IN ('running', 'completed')",
            name="ck_p115_organize_reconciliation_status",
        ),
        sa.CheckConstraint(
            "outcome IS NULL OR outcome IN ('applied', 'absent', 'conflict', 'unknown')",
            name="ck_p115_organize_reconciliation_outcome",
        ),
        sa.CheckConstraint(
            "(status = 'completed') = (finished_at IS NOT NULL AND outcome IS NOT NULL AND evidence_json IS NOT NULL)",
            name="ck_p115_organize_reconciliation_completed",
        ),
        sa.CheckConstraint(
            "evidence_json IS NULL OR jsonb_typeof(evidence_json) = 'object'",
            name="ck_p115_organize_reconciliation_evidence_object",
        ),
    )
    op.create_index(
        "uq_p115_organize_reconciliation_active_effect",
        "p115_organize_reconciliation_intents",
        ["effect_key"],
        unique=True,
        postgresql_where=sa.text("status = 'running'"),
    )

    op.create_table(
        "p115_organize_effect_events",
        sa.Column("event_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "effect_key",
            sa.Text(),
            sa.ForeignKey("p115_organize_effects.effect_key", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "execution_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("p115_organize_plan_executions.execution_id", ondelete="RESTRICT"),
            nullable=True,
        ),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("event_seq", sa.Integer(), nullable=False),
        sa.Column("previous_status", sa.Text(), nullable=True),
        sa.Column("new_status", sa.Text(), nullable=True),
        sa.Column("attempt_no", sa.Integer(), nullable=True),
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "reconciliation_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("p115_organize_reconciliation_intents.reconciliation_id", ondelete="RESTRICT"),
            nullable=True,
        ),
        sa.Column("actor", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("evidence_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(
            "attempt_no IS NULL OR attempt_no >= 1",
            name="ck_p115_organize_effect_events_attempt",
        ),
        sa.CheckConstraint("event_seq >= 1", name="ck_p115_organize_effect_events_seq"),
        sa.CheckConstraint(
            "jsonb_typeof(evidence_json) = 'object'",
            name="ck_p115_organize_effect_events_evidence_object",
        ),
        sa.UniqueConstraint(
            "effect_key",
            "event_seq",
            name="uq_p115_organize_effect_events_effect_seq",
        ),
    )
    op.create_index(
        "ix_p115_organize_effect_events_effect_created",
        "p115_organize_effect_events",
        ["effect_key", "created_at"],
    )
    op.create_index(
        "ix_p115_organize_effect_events_execution_created",
        "p115_organize_effect_events",
        ["execution_id", "created_at"],
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.drop_index(
        "ix_p115_organize_effect_events_execution_created",
        table_name="p115_organize_effect_events",
    )
    op.drop_index(
        "ix_p115_organize_effect_events_effect_created",
        table_name="p115_organize_effect_events",
    )
    op.drop_table("p115_organize_effect_events")
    op.drop_index(
        "uq_p115_organize_reconciliation_active_effect",
        table_name="p115_organize_reconciliation_intents",
    )
    op.drop_table("p115_organize_reconciliation_intents")
    op.drop_constraint(
        "fk_p115_organize_effects_sponsor_action",
        "p115_organize_effects",
        type_="foreignkey",
    )
    op.drop_index(
        "ix_p115_organize_execution_actions_effect",
        table_name="p115_organize_execution_actions",
    )
    op.drop_table("p115_organize_execution_actions")
    op.drop_constraint(
        "uq_p115_organize_plan_action_effect",
        "p115_organize_plan_actions",
        type_="unique",
    )
    op.drop_index("ix_p115_organize_effects_origin", table_name="p115_organize_effects")
    op.drop_index("ix_p115_organize_effects_active_lease", table_name="p115_organize_effects")
    op.drop_index("ix_p115_organize_effects_claimable", table_name="p115_organize_effects")
    op.drop_table("p115_organize_effects")
    op.drop_index(
        "ix_p115_organize_executions_account_created",
        table_name="p115_organize_plan_executions",
    )
    op.drop_index(
        "ix_p115_organize_executions_status_updated",
        table_name="p115_organize_plan_executions",
    )
    op.drop_table("p115_organize_plan_executions")
