"""Add immutable P6 organization dry-run plans and action documents.

Revision ID: 0005_p6_organize_plans
Revises: 0004_p6_p115_directory_cache
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0005_p6_organize_plans"
down_revision: str | Sequence[str] | None = "0004_p6_p115_directory_cache"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.create_table(
        "p115_organize_plans",
        sa.Column("plan_id", sa.Text(), primary_key=True),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("planner_version", sa.Text(), nullable=False),
        sa.Column("request_intent_id", sa.Text(), nullable=False, unique=True),
        sa.Column("request_hash", sa.Text(), nullable=False),
        sa.Column("input_fingerprint", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("account_id", sa.Text(), nullable=False),
        sa.Column("source_root_cid", sa.Text(), nullable=False),
        sa.Column("source_root_name", sa.Text(), nullable=False),
        sa.Column("sorting_rules_revision", sa.Text(), nullable=False),
        sa.Column("rename_config_revision", sa.Text(), nullable=False),
        sa.Column("episode_regex_revision", sa.Text(), nullable=False),
        sa.Column("policy_revision", sa.Text(), nullable=False),
        sa.Column("runtime_config_revision", sa.Text(), nullable=False),
        sa.Column("config_snapshot_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("source_snapshot_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("provider_evidence_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("recognition_evidence_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("warnings_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("errors_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("risk_summary_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("schema_version = 1", name="ck_p115_organize_plans_schema_version"),
        sa.CheckConstraint(
            "status IN ('ready', 'blocked')",
            name="ck_p115_organize_plans_status",
        ),
        sa.CheckConstraint(
            "account_id = '' OR account_id ~ '^[0-9]+$'",
            name="ck_p115_organize_plans_account_id",
        ),
        sa.CheckConstraint(
            "request_hash ~ '^[0-9a-f]{64}$'",
            name="ck_p115_organize_plans_request_hash",
        ),
        sa.CheckConstraint(
            "input_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_p115_organize_plans_input_fingerprint",
        ),
        sa.CheckConstraint(
            "sorting_rules_revision ~ '^[0-9a-f]{64}$'",
            name="ck_p115_organize_plans_sorting_revision",
        ),
        sa.CheckConstraint(
            "rename_config_revision ~ '^[0-9a-f]{64}$'",
            name="ck_p115_organize_plans_rename_revision",
        ),
        sa.CheckConstraint(
            "episode_regex_revision ~ '^[0-9a-f]{64}$'",
            name="ck_p115_organize_plans_episode_revision",
        ),
        sa.CheckConstraint(
            "policy_revision ~ '^[0-9a-f]{64}$'",
            name="ck_p115_organize_plans_policy_revision",
        ),
        sa.CheckConstraint(
            "runtime_config_revision ~ '^[0-9a-f]{64}$'",
            name="ck_p115_organize_plans_runtime_revision",
        ),
        sa.CheckConstraint(
            "expires_at > created_at",
            name="ck_p115_organize_plans_expiry",
        ),
    )
    op.create_index(
        "idx_p115_organize_plans_status_expiry",
        "p115_organize_plans",
        ["status", "expires_at"],
    )
    op.create_index(
        "idx_p115_organize_plans_account_source",
        "p115_organize_plans",
        ["account_id", "source_root_cid", "created_at"],
    )

    op.create_table(
        "p115_organize_plan_actions",
        sa.Column(
            "plan_id",
            sa.Text(),
            sa.ForeignKey("p115_organize_plans.plan_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("action_index", sa.Integer(), nullable=False),
        sa.Column("action_id", sa.Text(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("subject_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("destination_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("dependencies_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("preconditions_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("effect_key", sa.Text(), nullable=False),
        sa.Column("decision_basis_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("risk_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("destructive", sa.Boolean(), nullable=False),
        sa.Column("expected_postcondition_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("deferred_slice", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("plan_id", "action_index", name="pk_p115_organize_plan_actions"),
        sa.UniqueConstraint("plan_id", "action_id", name="uq_p115_organize_plan_action_id"),
        sa.CheckConstraint("action_index >= 0", name="ck_p115_organize_plan_action_index"),
        sa.CheckConstraint(
            "kind IN ('ENSURE_DIRECTORY', 'MOVE', 'RENAME')",
            name="ck_p115_organize_plan_action_kind",
        ),
        sa.CheckConstraint(
            "action_id ~ '^[0-9a-f]{64}$'",
            name="ck_p115_organize_plan_action_id",
        ),
        sa.CheckConstraint(
            "effect_key ~ '^[0-9a-f]{64}$'",
            name="ck_p115_organize_plan_effect_key",
        ),
    )
    op.create_index(
        "idx_p115_organize_plan_actions_effect",
        "p115_organize_plan_actions",
        ["effect_key"],
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.drop_index(
        "idx_p115_organize_plan_actions_effect",
        table_name="p115_organize_plan_actions",
    )
    op.drop_table("p115_organize_plan_actions")
    op.drop_index(
        "idx_p115_organize_plans_account_source",
        table_name="p115_organize_plans",
    )
    op.drop_index(
        "idx_p115_organize_plans_status_expiry",
        table_name="p115_organize_plans",
    )
    op.drop_table("p115_organize_plans")
