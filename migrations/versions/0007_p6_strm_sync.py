"""Add durable STRM-content ownership to the account-bound 115 cache.

Revision ID: 0007_p6_strm_sync
Revises: 0006_p6_organize_execution
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0007_p6_strm_sync"
down_revision: str | Sequence[str] | None = "0006_p6_organize_execution"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.add_column("p115_filesystem_cache", sa.Column("sync_root_id", sa.Text(), nullable=True))
    op.add_column("p115_filesystem_cache", sa.Column("strm_content_sha256", sa.Text(), nullable=True))
    op.create_check_constraint(
        "ck_p115_filesystem_cache_account_id_present",
        "p115_filesystem_cache",
        "account_id IS NOT NULL",
        postgresql_not_valid=True,
    )
    op.create_check_constraint(
        "ck_p115_filesystem_cache_entry_kind_present",
        "p115_filesystem_cache",
        "entry_kind IS NOT NULL",
        postgresql_not_valid=True,
    )
    op.create_check_constraint(
        "ck_p115_filesystem_cache_strm_content_sha256",
        "p115_filesystem_cache",
        "strm_content_sha256 IS NULL OR strm_content_sha256 ~ '^[0-9a-f]{64}$'",
    )
    op.create_check_constraint(
        "ck_p115_filesystem_cache_strm_hash_file_only",
        "p115_filesystem_cache",
        "strm_content_sha256 IS NULL OR entry_kind = 'file'",
    )
    op.create_index(
        "idx_p115_account_sync_root_kind",
        "p115_filesystem_cache",
        ["account_id", "sync_root_id", "entry_kind"],
    )
    op.create_table(
        "p115_strm_sync_plans",
        sa.Column("plan_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("plan_key", sa.Text(), nullable=False, unique=True),
        sa.Column("account_id", sa.Text(), nullable=False),
        sa.Column("scope", sa.Text(), nullable=False),
        sa.Column("provider_backend", sa.Text(), nullable=False),
        sa.Column("expected_cache_revision", sa.Text(), nullable=False),
        sa.Column("operation_revision", sa.Text(), nullable=False),
        sa.Column("plan_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("local_effects_started", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("last_error_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("plan_key ~ '^[0-9a-f]{64}$'", name="ck_p115_strm_sync_plans_key"),
        sa.CheckConstraint("account_id ~ '^[0-9]+$'", name="ck_p115_strm_sync_plans_account"),
        sa.CheckConstraint("scope IN ('full', 'home_video')", name="ck_p115_strm_sync_plans_scope"),
        sa.CheckConstraint(
            "provider_backend IN ('openapi', 'cookie')",
            name="ck_p115_strm_sync_plans_backend",
        ),
        sa.CheckConstraint(
            "expected_cache_revision ~ '^[0-9a-f]{64}$'",
            name="ck_p115_strm_sync_plans_cache_revision",
        ),
        sa.CheckConstraint(
            "operation_revision ~ '^[0-9a-f]{64}$'",
            name="ck_p115_strm_sync_plans_operation_revision",
        ),
        sa.CheckConstraint("jsonb_typeof(plan_json) = 'object'", name="ck_p115_strm_sync_plans_plan_object"),
        sa.CheckConstraint(
            "plan_json ? 'schema_version' AND plan_json ->> 'schema_version' = '2'",
            name="ck_p115_strm_sync_plans_schema_version",
        ),
        sa.CheckConstraint(
            "plan_json ? 'library_root' AND jsonb_typeof(plan_json -> 'library_root') = 'string'",
            name="ck_p115_strm_sync_plans_library_root",
        ),
        sa.CheckConstraint(
            "plan_json ? 'desired_cache_revision' AND plan_json ->> 'desired_cache_revision' ~ '^[0-9a-f]{64}$'",
            name="ck_p115_strm_sync_plans_desired_revision",
        ),
        sa.CheckConstraint(
            "plan_json ? 'roots' AND jsonb_typeof(plan_json -> 'roots') = 'array' "
            "AND jsonb_array_length(plan_json -> 'roots') > 0",
            name="ck_p115_strm_sync_plans_roots_array",
        ),
        sa.CheckConstraint(
            "plan_json ? 'managed_roots' AND jsonb_typeof(plan_json -> 'managed_roots') = 'array' "
            "AND jsonb_array_length(plan_json -> 'managed_roots') > 0",
            name="ck_p115_strm_sync_plans_managed_roots_array",
        ),
        sa.CheckConstraint(
            "plan_json ? 'writes' AND jsonb_typeof(plan_json -> 'writes') = 'array'",
            name="ck_p115_strm_sync_plans_writes_array",
        ),
        sa.CheckConstraint(
            "plan_json ? 'deletes' AND jsonb_typeof(plan_json -> 'deletes') = 'array'",
            name="ck_p115_strm_sync_plans_deletes_array",
        ),
        sa.CheckConstraint(
            "plan_json ? 'notifications' AND jsonb_typeof(plan_json -> 'notifications') = 'object'",
            name="ck_p115_strm_sync_plans_notifications_object",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'applying', 'retryable', 'superseded', 'confirmed')",
            name="ck_p115_strm_sync_plans_status",
        ),
        sa.CheckConstraint("attempts >= 0", name="ck_p115_strm_sync_plans_attempts"),
        sa.CheckConstraint(
            "last_error_json IS NULL OR jsonb_typeof(last_error_json) = 'object'",
            name="ck_p115_strm_sync_plans_error_object",
        ),
        sa.CheckConstraint(
            "(status = 'confirmed') = (finished_at IS NOT NULL)",
            name="ck_p115_strm_sync_plans_finished",
        ),
    )
    op.create_index(
        "idx_p115_strm_sync_plans_account_status",
        "p115_strm_sync_plans",
        ["account_id", "status", "created_at"],
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.drop_index("idx_p115_strm_sync_plans_account_status", table_name="p115_strm_sync_plans")
    op.drop_table("p115_strm_sync_plans")
    op.drop_index("idx_p115_account_sync_root_kind", table_name="p115_filesystem_cache")
    op.drop_constraint(
        "ck_p115_filesystem_cache_strm_hash_file_only",
        "p115_filesystem_cache",
        type_="check",
    )
    op.drop_constraint(
        "ck_p115_filesystem_cache_strm_content_sha256",
        "p115_filesystem_cache",
        type_="check",
    )
    op.drop_constraint(
        "ck_p115_filesystem_cache_entry_kind_present",
        "p115_filesystem_cache",
        type_="check",
    )
    op.drop_constraint(
        "ck_p115_filesystem_cache_account_id_present",
        "p115_filesystem_cache",
        type_="check",
    )
    op.drop_column("p115_filesystem_cache", "strm_content_sha256")
    op.drop_column("p115_filesystem_cache", "sync_root_id")
