"""Add the P6 durable ingestion monitor and pending binding ledgers.

Revision ID: 0009_p6_ingestion_monitor
Revises: 0008_p6_mediainfo_cache
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0009_p6_ingestion_monitor"
down_revision: str | Sequence[str] | None = "0008_p6_mediainfo_cache"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


MONITOR_STATUSES = (
    "pending",
    "stability_wait",
    "ready",
    "claimed",
    "retryable",
    "suspended",
    "confirmed",
    "rejected",
    "indeterminate",
    "superseded",
)

BINDING_STATUSES = (
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
    op.execute("SET LOCAL search_path TO public")
    op.create_table(
        "ingestion_monitor_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_sequence", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("producer_key", sa.Text(), nullable=False),
        sa.Column("request_hash", postgresql.BYTEA(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("path_key", sa.Text(), nullable=False),
        sa.Column("related_path", sa.Text(), nullable=True),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("monitor_root", sa.Text(), nullable=False),
        sa.Column("config_revision", sa.Text(), nullable=False),
        sa.Column("refresh_delay_seconds", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("classification", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'pending'")),
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
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(f"status IN ({_values(MONITOR_STATUSES)})", name="ck_ingestion_monitor_status"),
        sa.CheckConstraint("source IN ('watchdog', 'scan', 'active')", name="ck_ingestion_monitor_source"),
        sa.CheckConstraint("event_type IN ('upsert', 'deleted')", name="ck_ingestion_monitor_event_type"),
        sa.CheckConstraint(
            "classification IN ('physical', 'strm', 'refresh_only', 'deleted')",
            name="ck_ingestion_monitor_classification",
        ),
        sa.CheckConstraint(
            "(event_type = 'deleted') = (classification = 'deleted')",
            name="ck_ingestion_monitor_event_classification",
        ),
        sa.CheckConstraint("length(btrim(producer_key)) BETWEEN 1 AND 512", name="ck_ingestion_monitor_producer"),
        sa.CheckConstraint("octet_length(request_hash) = 32", name="ck_ingestion_monitor_request_hash"),
        sa.CheckConstraint("length(btrim(path)) BETWEEN 1 AND 4096", name="ck_ingestion_monitor_path"),
        sa.CheckConstraint("length(btrim(path_key)) BETWEEN 1 AND 4096", name="ck_ingestion_monitor_path_key"),
        sa.CheckConstraint("length(btrim(monitor_root)) BETWEEN 1 AND 4096", name="ck_ingestion_monitor_root"),
        sa.CheckConstraint("config_revision ~ '^[0-9a-f]{64}$'", name="ck_ingestion_monitor_config_revision"),
        sa.CheckConstraint("refresh_delay_seconds >= 0", name="ck_ingestion_monitor_refresh_delay"),
        sa.CheckConstraint("attempts >= 0", name="ck_ingestion_monitor_attempts"),
        sa.CheckConstraint("stable_matches >= 0", name="ck_ingestion_monitor_stable_matches"),
        sa.CheckConstraint("last_size IS NULL OR last_size >= 0", name="ck_ingestion_monitor_last_size"),
        sa.CheckConstraint("last_mtime_ns IS NULL OR last_mtime_ns >= 0", name="ck_ingestion_monitor_last_mtime"),
        sa.CheckConstraint(
            "(status = 'claimed' AND claimed_by IS NOT NULL AND length(btrim(claimed_by)) > 0 "
            "AND claim_token IS NOT NULL AND claimed_at IS NOT NULL AND lease_expires_at IS NOT NULL) OR "
            "(status <> 'claimed' AND claimed_by IS NULL AND claim_token IS NULL "
            "AND claimed_at IS NULL AND lease_expires_at IS NULL)",
            name="ck_ingestion_monitor_claim",
        ),
        sa.CheckConstraint(
            "status <> 'claimed' OR lease_expires_at > claimed_at",
            name="ck_ingestion_monitor_claim_lease",
        ),
        sa.CheckConstraint(
            "(status IN ('confirmed', 'rejected', 'indeterminate', 'superseded')) = (finished_at IS NOT NULL)",
            name="ck_ingestion_monitor_terminal_finished",
        ),
        sa.CheckConstraint(
            "result_json IS NULL OR jsonb_typeof(result_json) = 'object'",
            name="ck_ingestion_monitor_result_object",
        ),
        sa.CheckConstraint(
            "error_json IS NULL OR jsonb_typeof(error_json) = 'object'",
            name="ck_ingestion_monitor_error_object",
        ),
        sa.UniqueConstraint("event_sequence", name="uq_ingestion_monitor_event_sequence"),
        sa.UniqueConstraint("producer_key", name="uq_ingestion_monitor_producer_key"),
    )
    op.create_index(
        "uq_ingestion_monitor_unclaimed_path",
        "ingestion_monitor_items",
        ["path_key"],
        unique=True,
        postgresql_where=sa.text("status IN ('pending', 'stability_wait', 'ready', 'retryable', 'suspended')"),
    )
    op.create_index(
        "uq_ingestion_monitor_claimed_path",
        "ingestion_monitor_items",
        ["path_key"],
        unique=True,
        postgresql_where=sa.text("status = 'claimed'"),
    )
    op.create_index(
        "ix_ingestion_monitor_claimable",
        "ingestion_monitor_items",
        ["status", "available_at", "event_sequence", "id"],
        postgresql_where=sa.text("status IN ('ready', 'retryable')"),
    )
    op.create_index(
        "ix_ingestion_monitor_stability",
        "ingestion_monitor_items",
        ["status", "available_at", "event_sequence"],
        postgresql_where=sa.text("status IN ('pending', 'stability_wait')"),
    )
    op.create_index(
        "ix_ingestion_monitor_claim_recovery",
        "ingestion_monitor_items",
        ["status", "lease_expires_at", "event_sequence"],
        postgresql_where=sa.text("status = 'claimed'"),
    )
    op.create_index(
        "uq_ingestion_monitor_notification_effect",
        "media_effects",
        ["aggregate_type", "aggregate_id", "effect_type"],
        unique=True,
        postgresql_where=sa.text("aggregate_type = 'ingestion_monitor' AND effect_type = 'emby.library.paths_changed'"),
    )

    op.create_table(
        "ingestion_pending_bindings",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("binding_key", sa.Text(), nullable=False),
        sa.Column("request_hash", postgresql.BYTEA(), nullable=False),
        sa.Column("monitor_item_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("path_key", sa.Text(), nullable=False),
        sa.Column("sha1", sa.Text(), nullable=True),
        sa.Column("pick_code", sa.Text(), nullable=True),
        sa.Column("media_info_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("claimed_by", sa.Text(), nullable=True),
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("emby_item_id", sa.Text(), nullable=True),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(f"status IN ({_values(BINDING_STATUSES)})", name="ck_ingestion_binding_status"),
        sa.CheckConstraint("length(btrim(binding_key)) BETWEEN 1 AND 512", name="ck_ingestion_binding_key"),
        sa.CheckConstraint("octet_length(request_hash) = 32", name="ck_ingestion_binding_request_hash"),
        sa.CheckConstraint("length(btrim(path)) BETWEEN 1 AND 4096", name="ck_ingestion_binding_path"),
        sa.CheckConstraint("length(btrim(path_key)) BETWEEN 1 AND 4096", name="ck_ingestion_binding_path_key"),
        sa.CheckConstraint("sha1 IS NULL OR sha1 ~ '^[0-9A-F]{40}$'", name="ck_ingestion_binding_sha1"),
        sa.CheckConstraint(
            "pick_code IS NULL OR length(btrim(pick_code)) BETWEEN 1 AND 255",
            name="ck_ingestion_binding_pick_code",
        ),
        sa.CheckConstraint("attempts >= 0", name="ck_ingestion_binding_attempts"),
        sa.CheckConstraint(
            "(status = 'claimed' AND claimed_by IS NOT NULL AND length(btrim(claimed_by)) > 0 "
            "AND claim_token IS NOT NULL AND claimed_at IS NOT NULL AND lease_expires_at IS NOT NULL) OR "
            "(status <> 'claimed' AND claimed_by IS NULL AND claim_token IS NULL "
            "AND claimed_at IS NULL AND lease_expires_at IS NULL)",
            name="ck_ingestion_binding_claim",
        ),
        sa.CheckConstraint(
            "status <> 'claimed' OR lease_expires_at > claimed_at",
            name="ck_ingestion_binding_claim_lease",
        ),
        sa.CheckConstraint(
            "(status IN ('confirmed', 'rejected', 'indeterminate')) = (finished_at IS NOT NULL)",
            name="ck_ingestion_binding_terminal_finished",
        ),
        sa.CheckConstraint(
            "(status = 'confirmed') = (emby_item_id IS NOT NULL AND length(btrim(emby_item_id)) > 0)",
            name="ck_ingestion_binding_confirmed_item",
        ),
        sa.CheckConstraint(
            "media_info_json IS NULL OR jsonb_typeof(media_info_json) = 'object'",
            name="ck_ingestion_binding_mediainfo_object",
        ),
        sa.CheckConstraint(
            "result_json IS NULL OR jsonb_typeof(result_json) = 'object'",
            name="ck_ingestion_binding_result_object",
        ),
        sa.CheckConstraint(
            "error_json IS NULL OR jsonb_typeof(error_json) = 'object'",
            name="ck_ingestion_binding_error_object",
        ),
        sa.ForeignKeyConstraint(
            ["monitor_item_id"],
            ["public.ingestion_monitor_items.id"],
            name="fk_ingestion_binding_monitor_item",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("binding_key", name="uq_ingestion_binding_key"),
    )
    op.create_index(
        "ix_ingestion_binding_path_status",
        "ingestion_pending_bindings",
        ["path_key", "status", "available_at", "created_at"],
    )
    op.create_index(
        "ix_ingestion_binding_fingerprints",
        "ingestion_pending_bindings",
        ["sha1", "pick_code", "status", "created_at"],
    )
    op.create_index(
        "ix_ingestion_binding_claim_recovery",
        "ingestion_pending_bindings",
        ["status", "lease_expires_at", "created_at"],
        postgresql_where=sa.text("status = 'claimed'"),
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.execute("DELETE FROM outbox_events WHERE topic = 'ingestion.monitor.requested'")
    op.drop_table("ingestion_pending_bindings")
    op.drop_table("ingestion_monitor_items")
    op.drop_index("uq_ingestion_monitor_notification_effect", table_name="media_effects")
