"""Add durable P8 collection operations and database maintenance state.

Revision ID: 0017_p8_effects_and_maintenance
Revises: 0016_p8_identity_operations
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0017_p8_effects_and_maintenance"
down_revision: str | Sequence[str] | None = "0016_p8_identity_operations"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.create_table(
        "collection_operations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_sequence", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("operation_key", sa.Text(), nullable=False),
        sa.Column("request_hash", postgresql.BYTEA(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("scope_key", sa.Text(), nullable=False),
        sa.Column("request_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'ready'")),
        sa.Column("current_step", sa.Text(), nullable=False, server_default=sa.text("'reserved'")),
        sa.Column("state_revision", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("claimed_by", sa.Text(), nullable=True),
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("step_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "state_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("operation_key ~ '^[0-9a-f]{64}$'", name="ck_collection_operation_key"),
        sa.CheckConstraint("octet_length(request_hash) = 32", name="ck_collection_operation_hash"),
        sa.CheckConstraint(
            "kind IN ('collection.native.delete','collection.custom.delete',"
            "'collection.missing.cleanup','collection.image.archive',"
            "'collection.removal.survival-check')",
            name="ck_collection_operation_kind",
        ),
        sa.CheckConstraint("length(btrim(scope_key)) > 0", name="ck_collection_operation_scope"),
        sa.CheckConstraint("jsonb_typeof(request_json) = 'object'", name="ck_collection_operation_request"),
        sa.CheckConstraint(
            "status IN ('ready','claimed','requesting','awaiting_readback','retryable',"
            "'confirmed','rejected','indeterminate')",
            name="ck_collection_operation_status",
        ),
        sa.CheckConstraint(
            "current_step IN ('reserved','read_remote','remove_members','delete_shell',"
            "'discover_missing_targets','archive_images','cleanup_placeholders',"
            "'cleanup_local','survival_readback')",
            name="ck_collection_operation_step",
        ),
        sa.CheckConstraint(
            "state_revision >= 0 AND attempts >= 0",
            name="ck_collection_operation_counters",
        ),
        sa.CheckConstraint(
            "(status IN ('claimed','requesting') "
            "AND claimed_by IS NOT NULL AND length(btrim(claimed_by)) > 0 "
            "AND claim_token IS NOT NULL AND claimed_at IS NOT NULL "
            "AND lease_expires_at > claimed_at) OR "
            "(status NOT IN ('claimed','requesting') "
            "AND claimed_by IS NULL AND claim_token IS NULL "
            "AND claimed_at IS NULL AND lease_expires_at IS NULL)",
            name="ck_collection_operation_claim",
        ),
        sa.CheckConstraint(
            "status NOT IN ('requesting','awaiting_readback') OR step_started_at IS NOT NULL",
            name="ck_collection_operation_step_started",
        ),
        sa.CheckConstraint(
            "(status IN ('confirmed','rejected','indeterminate')) = (finished_at IS NOT NULL)",
            name="ck_collection_operation_finished",
        ),
        sa.CheckConstraint("jsonb_typeof(state_json) = 'object'", name="ck_collection_operation_state"),
        sa.CheckConstraint(
            "result_json IS NULL OR jsonb_typeof(result_json) = 'object'",
            name="ck_collection_operation_result",
        ),
        sa.CheckConstraint(
            "error_json IS NULL OR jsonb_typeof(error_json) = 'object'",
            name="ck_collection_operation_error",
        ),
        sa.UniqueConstraint("event_sequence", name="uq_collection_operation_sequence"),
        sa.UniqueConstraint("operation_key", name="uq_collection_operation_key"),
    )
    op.create_index(
        "ix_collection_operation_due",
        "collection_operations",
        ["status", "available_at", "event_sequence"],
        postgresql_where=sa.text("status IN ('ready','retryable','awaiting_readback')"),
    )
    op.create_index(
        "ix_collection_operation_claim_recovery",
        "collection_operations",
        ["lease_expires_at", "event_sequence"],
        postgresql_where=sa.text("status IN ('claimed','requesting')"),
    )
    op.create_index(
        "ix_collection_operation_scope_history",
        "collection_operations",
        ["scope_key", sa.text("event_sequence DESC")],
    )
    op.create_index(
        "uq_collection_operation_active_scope",
        "collection_operations",
        ["scope_key"],
        unique=True,
        postgresql_where=sa.text(
            "status IN ('ready','claimed','requesting','awaiting_readback','retryable','indeterminate')"
        ),
    )

    # Database maintenance is deliberately separate from the generic Job and
    # Outbox tables.  Backup bytes live only in this immutable artifact ledger;
    # operation/job payloads carry opaque identifiers and never JSON/bytes.
    op.create_table(
        "database_backup_artifacts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("format_version", sa.Integer(), nullable=False),
        sa.Column("schema_fingerprint", sa.Text(), nullable=False),
        sa.Column("source_filename", sa.Text(), nullable=False, server_default=sa.text("''")),
        sa.Column("compressed_bytes", postgresql.BYTEA(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'available'")),
        sa.Column("compressed_size", sa.BigInteger(), nullable=False),
        sa.Column("expanded_size", sa.BigInteger(), nullable=False),
        sa.Column("compression_ratio", sa.Numeric(precision=12, scale=4), nullable=False),
        sa.Column("compressed_sha256", postgresql.BYTEA(), nullable=False),
        sa.Column("expanded_sha256", postgresql.BYTEA(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("format_version >= 1", name="ck_database_backup_artifact_format_version"),
        sa.CheckConstraint("length(btrim(schema_fingerprint)) = 64", name="ck_database_backup_artifact_fingerprint"),
        sa.CheckConstraint(
            "compressed_size > 0 AND compressed_size <= 67108864", name="ck_database_backup_artifact_compressed_size"
        ),
        sa.CheckConstraint(
            "expanded_size > 0 AND expanded_size <= 268435456", name="ck_database_backup_artifact_expanded_size"
        ),
        sa.CheckConstraint(
            "compression_ratio >= 1 AND compression_ratio <= 20",
            name="ck_database_backup_artifact_ratio",
        ),
        sa.CheckConstraint(
            "compressed_bytes IS NULL OR octet_length(compressed_bytes) = compressed_size",
            name="ck_database_backup_artifact_bytes",
        ),
        sa.CheckConstraint("status IN ('available','expired')", name="ck_database_backup_artifact_status"),
        sa.CheckConstraint(
            "(status = 'available') = (compressed_bytes IS NOT NULL)",
            name="ck_database_backup_artifact_content_lifecycle",
        ),
        sa.CheckConstraint("octet_length(compressed_sha256) = 32", name="ck_database_backup_artifact_compressed_hash"),
        sa.CheckConstraint("octet_length(expanded_sha256) = 32", name="ck_database_backup_artifact_expanded_hash"),
        sa.CheckConstraint("expires_at > created_at", name="ck_database_backup_artifact_expiry"),
    )
    op.create_index(
        "ix_database_backup_artifacts_expiry",
        "database_backup_artifacts",
        ["expires_at"],
    )
    op.create_index(
        "uq_database_backup_artifact_compressed_hash",
        "database_backup_artifacts",
        ["compressed_sha256"],
        unique=True,
        postgresql_where=sa.text("status = 'available'"),
    )

    op.create_table(
        "database_maintenance_operations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_sequence", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("operation_key", sa.Text(), nullable=False),
        sa.Column("request_hash", postgresql.BYTEA(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("scope_key", sa.Text(), nullable=False),
        sa.Column("request_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("artifact_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'ready'")),
        sa.Column("current_step", sa.Text(), nullable=False, server_default=sa.text("'reserved'")),
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
        sa.CheckConstraint("operation_key ~ '^[0-9a-f]{64}$'", name="ck_database_maintenance_operation_key"),
        sa.CheckConstraint("octet_length(request_hash) = 32", name="ck_database_maintenance_operation_hash"),
        sa.CheckConstraint(
            "kind IN ('import','clear_tables','prepare_for_library_rebuild','cleanup_offline_media',"
            "'clear_vectors','correct_sequences')",
            name="ck_database_maintenance_operation_kind",
        ),
        sa.CheckConstraint("length(btrim(scope_key)) > 0", name="ck_database_maintenance_operation_scope"),
        sa.CheckConstraint("jsonb_typeof(request_json) = 'object'", name="ck_database_maintenance_operation_request"),
        sa.CheckConstraint(
            "status IN ('ready','claimed','requesting','retryable','confirmed','rejected','indeterminate')",
            name="ck_database_maintenance_operation_status",
        ),
        sa.CheckConstraint(
            "current_step IN ('reserved','validate_artifact','mutate','reconcile','cleanup_artifact')",
            name="ck_database_maintenance_operation_step",
        ),
        sa.CheckConstraint("state_revision >= 0 AND attempts >= 0", name="ck_database_maintenance_operation_counters"),
        sa.CheckConstraint(
            "(status IN ('claimed','requesting') AND claimed_by IS NOT NULL AND length(btrim(claimed_by)) > 0 "
            "AND claim_token IS NOT NULL AND claimed_at IS NOT NULL AND lease_expires_at > claimed_at) OR "
            "(status NOT IN ('claimed','requesting') AND claimed_by IS NULL AND claim_token IS NULL "
            "AND claimed_at IS NULL AND lease_expires_at IS NULL)",
            name="ck_database_maintenance_operation_claim",
        ),
        sa.CheckConstraint(
            "status <> 'requesting' OR step_started_at IS NOT NULL",
            name="ck_database_maintenance_operation_step_started",
        ),
        sa.CheckConstraint(
            "(status IN ('confirmed','rejected','indeterminate')) = (finished_at IS NOT NULL)",
            name="ck_database_maintenance_operation_finished",
        ),
        sa.CheckConstraint("jsonb_typeof(state_json) = 'object'", name="ck_database_maintenance_operation_state"),
        sa.CheckConstraint(
            "result_json IS NULL OR jsonb_typeof(result_json) = 'object'",
            name="ck_database_maintenance_operation_result",
        ),
        sa.CheckConstraint(
            "error_json IS NULL OR jsonb_typeof(error_json) = 'object'",
            name="ck_database_maintenance_operation_error",
        ),
        sa.UniqueConstraint("event_sequence", name="uq_database_maintenance_operation_sequence"),
        sa.UniqueConstraint("operation_key", name="uq_database_maintenance_operation_key"),
        sa.ForeignKeyConstraint(
            ["artifact_id"],
            ["public.database_backup_artifacts.id"],
            name="fk_database_maintenance_operation_artifact",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["public.job_runs.id"],
            name="fk_database_maintenance_operation_job",
            ondelete="RESTRICT",
        ),
    )
    op.create_index(
        "ix_database_maintenance_operation_due",
        "database_maintenance_operations",
        ["status", "available_at", "event_sequence"],
        postgresql_where=sa.text("status IN ('ready','retryable')"),
    )
    op.create_index(
        "ix_database_maintenance_operation_claim_recovery",
        "database_maintenance_operations",
        ["lease_expires_at", "event_sequence"],
        postgresql_where=sa.text("status IN ('claimed','requesting')"),
    )
    op.create_index(
        "ix_database_maintenance_operation_scope_history",
        "database_maintenance_operations",
        ["scope_key", sa.text("event_sequence DESC")],
    )
    op.create_index(
        "uq_database_maintenance_operation_active_scope",
        "database_maintenance_operations",
        ["scope_key"],
        unique=True,
        postgresql_where=sa.text("status IN ('ready','claimed','requesting','retryable')"),
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM collection_operations
            ) THEN
                RAISE EXCEPTION
                    'cannot downgrade P8 collection operations while durable operation history exists';
            END IF;
            IF EXISTS (
                SELECT 1 FROM outbox_events
                WHERE topic = 'collection.operation.requested'
            ) THEN
                RAISE EXCEPTION
                    'cannot downgrade P8 collection operations while durable dispatch history exists';
            END IF;
        END;
        $$
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM database_maintenance_operations) THEN
                RAISE EXCEPTION
                    'cannot downgrade database maintenance operations while durable history exists';
            END IF;
            IF EXISTS (SELECT 1 FROM database_backup_artifacts) THEN
                RAISE EXCEPTION
                    'cannot downgrade database backup artifacts while artifact history exists';
            END IF;
        END;
        $$
        """
    )
    op.drop_table("database_maintenance_operations")
    op.drop_table("database_backup_artifacts")
    op.drop_table("collection_operations")
