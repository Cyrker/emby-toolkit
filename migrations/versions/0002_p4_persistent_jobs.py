"""Add the P4 persistent job, attempt, outbox, and idempotency ledger.

Revision ID: 0002_p4_persistent_jobs
Revises: 0001_p2_legacy_baseline
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_p4_persistent_jobs"
down_revision: str | Sequence[str] | None = "0001_p2_legacy_baseline"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


JOB_STATUSES = (
    "queued",
    "running",
    "retry_wait",
    "cancel_requested",
    "succeeded",
    "failed",
    "timed_out",
    "cancelled",
)
ATTEMPT_STATUSES = (
    "created",
    "claimed",
    "running",
    "succeeded",
    "failed",
    "timed_out",
    "lost",
    "cancelled",
)
OUTBOX_STATUSES = ("pending", "publishing", "published", "processed", "cancelled", "dead")
IDEMPOTENCY_STATUSES = ("reserved", "completed", "failed", "indeterminate")


def _values(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def upgrade() -> None:
    # The pg_dump-based P2 baseline clears search_path on the migration connection.
    op.execute("SET LOCAL search_path TO public")
    op.create_table(
        "job_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("task_key", sa.Text(), nullable=False),
        sa.Column("trigger_type", sa.Text(), nullable=False),
        sa.Column("queue_name", sa.Text(), nullable=False),
        sa.Column("payload_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "request_payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("requested_by_user_id", sa.Text(), nullable=True),
        sa.Column("parent_job_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("workflow_step", sa.Integer(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'queued'")),
        sa.Column("progress", sa.SmallInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("progress_message", sa.Text(), nullable=False, server_default=sa.text("''")),
        sa.Column(
            "logs_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("current_attempt", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("timeout_seconds", sa.Integer(), nullable=True),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_summary_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("trace_id", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(f"status IN ({_values(JOB_STATUSES)})", name="ck_job_runs_status"),
        sa.CheckConstraint(
            "trigger_type IN ('manual', 'schedule', 'workflow', 'system', 'monitor')",
            name="ck_job_runs_trigger_type",
        ),
        sa.CheckConstraint("payload_version >= 1", name="ck_job_runs_payload_version"),
        sa.CheckConstraint(
            "jsonb_typeof(request_payload_json) = 'object'",
            name="ck_job_runs_request_payload_object",
        ),
        sa.CheckConstraint("progress BETWEEN 0 AND 100", name="ck_job_runs_progress"),
        sa.CheckConstraint("jsonb_typeof(logs_json) = 'array'", name="ck_job_runs_logs_array"),
        sa.CheckConstraint("max_attempts >= 1", name="ck_job_runs_max_attempts"),
        sa.CheckConstraint(
            "current_attempt >= 1 AND current_attempt <= max_attempts",
            name="ck_job_runs_current_attempt",
        ),
        sa.CheckConstraint("timeout_seconds IS NULL OR timeout_seconds > 0", name="ck_job_runs_timeout"),
        sa.CheckConstraint("workflow_step IS NULL OR workflow_step > 0", name="ck_job_runs_workflow_step"),
        sa.CheckConstraint(
            "(parent_job_id IS NULL) = (workflow_step IS NULL)",
            name="ck_job_runs_workflow_parent_pair",
        ),
        sa.CheckConstraint(
            "(status IN ('succeeded', 'failed', 'timed_out', 'cancelled')) = (finished_at IS NOT NULL)",
            name="ck_job_runs_terminal_finished",
        ),
        sa.CheckConstraint(
            "status <> 'retry_wait' OR next_attempt_at IS NOT NULL",
            name="ck_job_runs_retry_wait_time",
        ),
        sa.CheckConstraint(
            "status <> 'cancel_requested' OR cancel_requested_at IS NOT NULL",
            name="ck_job_runs_cancel_requested_time",
        ),
        sa.ForeignKeyConstraint(
            ["requested_by_user_id"],
            ["public.emby_users.id"],
            name="fk_job_runs_requested_by_user",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["parent_job_id"],
            ["public.job_runs.id"],
            name="fk_job_runs_parent",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("parent_job_id", "workflow_step", name="uq_job_runs_parent_workflow_step"),
    )
    op.create_index("ix_job_runs_task_created", "job_runs", ["task_key", sa.text("created_at DESC")])
    op.create_index("ix_job_runs_status_updated", "job_runs", ["status", "updated_at"])
    op.create_index("ix_job_runs_parent_step", "job_runs", ["parent_job_id", "workflow_step"])
    op.create_index(
        "ix_job_runs_runnable",
        "job_runs",
        ["status", "scheduled_for", "created_at"],
        postgresql_where=sa.text("status IN ('queued', 'retry_wait')"),
    )
    op.create_index(
        "uq_job_runs_single_active_root_serial",
        "job_runs",
        [sa.text("(1)")],
        unique=True,
        postgresql_where=sa.text(
            "queue_name = 'legacy.serial' AND parent_job_id IS NULL "
            "AND status IN ('queued', 'running', 'retry_wait', 'cancel_requested')"
        ),
    )

    op.create_table(
        "job_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'created'")),
        sa.Column("celery_task_id", sa.Text(), nullable=False),
        sa.Column("worker_id", sa.Text(), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("retryable", sa.Boolean(), nullable=True),
        sa.Column("error_type", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("error_details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(f"status IN ({_values(ATTEMPT_STATUSES)})", name="ck_job_attempts_status"),
        sa.CheckConstraint("attempt_no >= 1", name="ck_job_attempts_attempt_no"),
        sa.CheckConstraint(
            "(status IN ('succeeded', 'failed', 'timed_out', 'lost', 'cancelled')) = (finished_at IS NOT NULL)",
            name="ck_job_attempts_terminal_finished",
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["public.job_runs.id"],
            name="fk_job_attempts_job",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("job_id", "attempt_no", name="uq_job_attempts_job_attempt_no"),
        sa.UniqueConstraint("celery_task_id", name="uq_job_attempts_celery_task_id"),
    )
    op.create_index("ix_job_attempts_job_attempt", "job_attempts", ["job_id", sa.text("attempt_no DESC")])
    op.create_index("ix_job_attempts_worker_status", "job_attempts", ["worker_id", "status"])
    op.create_index(
        "ix_job_attempts_active_lease",
        "job_attempts",
        ["status", "lease_expires_at"],
        postgresql_where=sa.text("status IN ('claimed', 'running')"),
    )

    op.create_table(
        "outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_key", sa.Text(), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("aggregate_type", sa.Text(), nullable=False),
        sa.Column("aggregate_id", sa.Text(), nullable=False),
        sa.Column("payload_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("claimed_by", sa.Text(), nullable=True),
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("publish_attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("max_publish_attempts", sa.Integer(), nullable=False, server_default=sa.text("20")),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(f"status IN ({_values(OUTBOX_STATUSES)})", name="ck_outbox_events_status"),
        sa.CheckConstraint("payload_version >= 1", name="ck_outbox_events_payload_version"),
        sa.CheckConstraint("jsonb_typeof(payload_json) = 'object'", name="ck_outbox_events_payload_object"),
        sa.CheckConstraint("publish_attempts >= 0", name="ck_outbox_events_publish_attempts"),
        sa.CheckConstraint("max_publish_attempts >= 1", name="ck_outbox_events_max_publish_attempts"),
        sa.CheckConstraint(
            "(status = 'publishing') = (claim_token IS NOT NULL AND claimed_by IS NOT NULL AND claimed_at IS NOT NULL)",
            name="ck_outbox_events_claim",
        ),
        sa.UniqueConstraint("event_key", name="uq_outbox_events_event_key"),
    )
    op.create_index("ix_outbox_events_aggregate", "outbox_events", ["aggregate_type", "aggregate_id", "created_at"])
    op.create_index("ix_outbox_events_claimable", "outbox_events", ["status", "available_at", "created_at"])
    op.create_index(
        "ix_outbox_events_claim_recovery",
        "outbox_events",
        ["status", "claimed_at"],
        postgresql_where=sa.text("status = 'publishing'"),
    )
    op.create_index(
        "ix_outbox_events_delivery_recovery",
        "outbox_events",
        ["status", "published_at"],
        postgresql_where=sa.text("status = 'published' AND processed_at IS NULL"),
    )
    op.create_index(
        "ix_outbox_events_processed",
        "outbox_events",
        ["processed_at"],
        postgresql_where=sa.text("processed_at IS NOT NULL"),
    )

    op.create_table(
        "idempotency_records",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("scope", sa.Text(), nullable=False),
        sa.Column("key_hash", postgresql.BYTEA(), nullable=False),
        sa.Column("request_hash", postgresql.BYTEA(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("attempt_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("lease_owner", sa.Text(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(f"status IN ({_values(IDEMPOTENCY_STATUSES)})", name="ck_idempotency_records_status"),
        sa.CheckConstraint("octet_length(key_hash) = 32", name="ck_idempotency_records_key_hash"),
        sa.CheckConstraint("octet_length(request_hash) = 32", name="ck_idempotency_records_request_hash"),
        sa.CheckConstraint(
            "status <> 'completed' OR completed_at IS NOT NULL",
            name="ck_idempotency_records_completed_at",
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["public.job_runs.id"],
            name="fk_idempotency_records_job",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["attempt_id"],
            ["public.job_attempts.id"],
            name="fk_idempotency_records_attempt",
            ondelete="SET NULL",
        ),
        sa.UniqueConstraint("scope", "key_hash", name="uq_idempotency_records_scope_key_hash"),
    )
    op.create_index("ix_idempotency_records_job", "idempotency_records", ["job_id"])
    op.create_index("ix_idempotency_records_lease", "idempotency_records", ["status", "lease_expires_at"])
    op.create_index(
        "ix_idempotency_records_expires",
        "idempotency_records",
        ["expires_at"],
        postgresql_where=sa.text("expires_at IS NOT NULL"),
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.drop_table("idempotency_records")
    op.drop_table("outbox_events")
    op.drop_table("job_attempts")
    op.drop_table("job_runs")
