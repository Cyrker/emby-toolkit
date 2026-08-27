"""Add durable P8 identity operations, artifacts, and portal request receipts.

Revision ID: 0016_p8_identity_operations
Revises: 0015_p7_assistant_claims
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0016_p8_identity_operations"
down_revision: str | Sequence[str] | None = "0015_p7_assistant_claims"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.create_table(
        "identity_operations",
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
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
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
        sa.CheckConstraint("operation_key ~ '^[0-9a-f]{64}$'", name="ck_identity_operation_key"),
        sa.CheckConstraint("octet_length(request_hash) = 32", name="ck_identity_operation_hash"),
        sa.CheckConstraint(
            "kind IN ('register','assign_template','set_status','delete_user','upload_avatar')",
            name="ck_identity_operation_kind",
        ),
        sa.CheckConstraint("length(btrim(scope_key)) > 0", name="ck_identity_operation_scope"),
        sa.CheckConstraint("jsonb_typeof(request_json) = 'object'", name="ck_identity_operation_request"),
        sa.CheckConstraint(
            "status IN ('ready','claimed','requesting','awaiting_readback','retryable',"
            "'compensating','remote_confirmed','confirmed','rejected','compensated',"
            "'indeterminate','cancelled')",
            name="ck_identity_operation_status",
        ),
        sa.CheckConstraint(
            "current_step IN ('reserved','preflight_username','create','password','policy',"
            "'configuration','local_commit','delete_remote_user','write','upload_avatar','readback')",
            name="ck_identity_operation_step",
        ),
        sa.CheckConstraint("state_revision >= 0 AND attempts >= 0", name="ck_identity_operation_counters"),
        sa.CheckConstraint(
            "(status IN ('claimed','requesting','compensating','remote_confirmed') "
            "AND claimed_by IS NOT NULL AND length(btrim(claimed_by)) > 0 "
            "AND claim_token IS NOT NULL AND claimed_at IS NOT NULL AND lease_expires_at > claimed_at) OR "
            "(status NOT IN ('claimed','requesting','compensating','remote_confirmed') "
            "AND claimed_by IS NULL AND claim_token IS NULL AND claimed_at IS NULL AND lease_expires_at IS NULL)",
            name="ck_identity_operation_claim",
        ),
        sa.CheckConstraint(
            "status NOT IN ('requesting','awaiting_readback','compensating','remote_confirmed') "
            "OR step_started_at IS NOT NULL",
            name="ck_identity_operation_step_started",
        ),
        sa.CheckConstraint(
            "(status IN ('confirmed','rejected','compensated','indeterminate','cancelled')) "
            "= (finished_at IS NOT NULL)",
            name="ck_identity_operation_finished",
        ),
        sa.CheckConstraint("jsonb_typeof(state_json) = 'object'", name="ck_identity_operation_state"),
        sa.CheckConstraint(
            "result_json IS NULL OR jsonb_typeof(result_json) = 'object'",
            name="ck_identity_operation_result",
        ),
        sa.CheckConstraint(
            "error_json IS NULL OR jsonb_typeof(error_json) = 'object'",
            name="ck_identity_operation_error",
        ),
        sa.UniqueConstraint("event_sequence", name="uq_identity_operation_sequence"),
        sa.UniqueConstraint("operation_key", name="uq_identity_operation_key"),
    )
    op.create_index(
        "ix_identity_operation_due",
        "identity_operations",
        ["status", "available_at", "event_sequence"],
        postgresql_where=sa.text("status IN ('ready','retryable','awaiting_readback')"),
    )
    op.create_index(
        "ix_identity_operation_claim_recovery",
        "identity_operations",
        ["lease_expires_at", "event_sequence"],
        postgresql_where=sa.text("status IN ('claimed','requesting','compensating','remote_confirmed')"),
    )
    op.create_index(
        "ix_identity_operation_scope_history",
        "identity_operations",
        ["scope_key", sa.text("created_at DESC")],
    )
    op.create_index(
        "uq_identity_operation_active_user_scope",
        "identity_operations",
        ["scope_key"],
        unique=True,
        postgresql_where=sa.text(
            "kind <> 'register' AND status IN ("
            "'ready','claimed','requesting','awaiting_readback','retryable',"
            "'compensating','remote_confirmed','indeterminate')"
        ),
    )
    op.create_index(
        "uq_identity_operation_active_registration_username",
        "identity_operations",
        [sa.text("lower(request_json ->> 'username')")],
        unique=True,
        postgresql_where=sa.text(
            "kind = 'register' AND status IN ("
            "'ready','claimed','requesting','awaiting_readback','retryable',"
            "'compensating','remote_confirmed','indeterminate')"
        ),
    )

    op.create_table(
        "identity_user_tombstones",
        sa.Column("user_id", sa.Text(), primary_key=True),
        sa.Column("generation", sa.BigInteger(), nullable=False, server_default=sa.text("1")),
        sa.Column("last_operation_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("length(btrim(user_id)) > 0", name="ck_identity_user_tombstone_user"),
        sa.CheckConstraint("generation > 0", name="ck_identity_user_tombstone_generation"),
        sa.ForeignKeyConstraint(
            ["last_operation_id"],
            ["identity_operations.id"],
            name="fk_identity_user_tombstone_operation",
            ondelete="RESTRICT",
        ),
    )
    op.create_table(
        "identity_projection_clock",
        sa.Column("singleton_id", sa.SmallInteger(), primary_key=True),
        sa.Column("revision", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("singleton_id = 1", name="ck_identity_projection_clock_singleton"),
        sa.CheckConstraint("revision >= 0", name="ck_identity_projection_clock_revision"),
    )
    op.execute("INSERT INTO identity_projection_clock (singleton_id, revision) VALUES (1, 0)")

    op.add_column("invitations", sa.Column("reservation_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column("invitations", sa.Column("reserved_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("invitations", sa.Column("reservation_expires_at", sa.DateTime(timezone=True), nullable=True))
    op.create_foreign_key(
        "fk_invitations_reservation_identity_operation",
        "invitations",
        "identity_operations",
        ["reservation_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index(
        "uq_invitations_reservation_id",
        "invitations",
        ["reservation_id"],
        unique=True,
        postgresql_where=sa.text("reservation_id IS NOT NULL"),
    )
    op.create_check_constraint(
        "ck_invitations_reservation_fields",
        "invitations",
        "(status IN ('active','expired') AND reservation_id IS NULL "
        "AND reserved_at IS NULL AND reservation_expires_at IS NULL) OR "
        "(status = 'reserved' AND reservation_id IS NOT NULL AND reserved_at IS NOT NULL "
        "AND reservation_expires_at > reserved_at) OR "
        "(status = 'used' AND ((reservation_id IS NULL AND reserved_at IS NULL "
        "AND reservation_expires_at IS NULL) OR (reservation_id IS NOT NULL "
        "AND reserved_at IS NOT NULL AND reservation_expires_at > reserved_at)))",
    )

    op.create_table(
        "identity_avatar_artifacts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("operation_id", postgresql.UUID(as_uuid=True), nullable=False, unique=True),
        sa.Column("owner_user_id", sa.Text(), nullable=False),
        sa.Column("sha256", postgresql.BYTEA(), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("content_type", sa.Text(), nullable=False),
        sa.Column("content_bytes", postgresql.BYTEA(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'staged'")),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("length(btrim(owner_user_id)) > 0", name="ck_identity_avatar_owner"),
        sa.CheckConstraint("octet_length(sha256) = 32", name="ck_identity_avatar_sha256"),
        sa.CheckConstraint(
            "size_bytes > 0 AND size_bytes <= 5242880",
            name="ck_identity_avatar_size",
        ),
        sa.CheckConstraint(
            "content_type IN ('image/jpeg','image/png','image/webp')",
            name="ck_identity_avatar_content_type",
        ),
        sa.CheckConstraint(
            "content_bytes IS NULL OR octet_length(content_bytes) = size_bytes",
            name="ck_identity_avatar_content_size",
        ),
        sa.CheckConstraint(
            "status IN ('staged','claimed','consumed','purge_pending','purged')",
            name="ck_identity_avatar_status",
        ),
        sa.CheckConstraint(
            "(status IN ('consumed','purge_pending','purged')) = (consumed_at IS NOT NULL)",
            name="ck_identity_avatar_consumed",
        ),
        sa.CheckConstraint(
            "(status IN ('staged','claimed')) = (content_bytes IS NOT NULL)",
            name="ck_identity_avatar_content_lifecycle",
        ),
        sa.CheckConstraint("expires_at > created_at", name="ck_identity_avatar_expiry_window"),
        sa.ForeignKeyConstraint(
            ["operation_id"],
            ["identity_operations.id"],
            name="fk_identity_avatar_operation",
            ondelete="RESTRICT",
        ),
    )
    op.create_index(
        "ix_identity_avatar_expiry",
        "identity_avatar_artifacts",
        ["status", "expires_at"],
        postgresql_where=sa.text("status IN ('staged','claimed','purge_pending')"),
    )
    op.create_table(
        "portal_subscription_requests",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("request_key_hash", sa.Text(), nullable=False, unique=True),
        sa.Column("request_fingerprint", sa.Text(), nullable=False),
        sa.Column("user_id", sa.Text(), nullable=False),
        sa.Column(
            "response_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "operation_ids",
            postgresql.ARRAY(postgresql.UUID(as_uuid=True)),
            nullable=False,
            server_default=sa.text("'{}'::uuid[]"),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "request_key_hash ~ '^[0-9a-f]{64}$'",
            name="ck_portal_subscription_request_key_hash",
        ),
        sa.CheckConstraint(
            "request_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_portal_subscription_request_fingerprint",
        ),
        sa.CheckConstraint("length(btrim(user_id)) > 0", name="ck_portal_subscription_request_user"),
        sa.CheckConstraint(
            "jsonb_typeof(response_json) = 'object'",
            name="ck_portal_subscription_request_response",
        ),
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM portal_subscription_requests) THEN
                RAISE EXCEPTION
                    'cannot downgrade P8 portal request receipts while durable receipts exist';
            END IF;
            IF EXISTS (SELECT 1 FROM identity_user_tombstones) THEN
                RAISE EXCEPTION
                    'cannot downgrade P8 identity operations while user tombstones exist';
            END IF;
            IF EXISTS (
                SELECT 1
                FROM identity_operations
                WHERE status NOT IN ('confirmed','rejected','compensated','cancelled')
            ) THEN
                RAISE EXCEPTION
                    'cannot downgrade P8 identity operations while nonterminal or indeterminate state exists';
            END IF;
        END;
        $$
        """
    )
    op.execute(
        """
        UPDATE invitations
        SET status = CASE
                WHEN expires_at IS NOT NULL AND expires_at < NOW() THEN 'expired'
                ELSE 'active'
            END,
            used_by_user_id = NULL
        WHERE status = 'reserved' AND reservation_id IS NOT NULL
        """
    )
    op.execute("DELETE FROM outbox_events WHERE topic = 'identity.operation.requested'")
    op.drop_table("portal_subscription_requests")
    op.drop_table("identity_avatar_artifacts")
    op.drop_constraint("ck_invitations_reservation_fields", "invitations", type_="check")
    op.drop_index("uq_invitations_reservation_id", table_name="invitations")
    op.drop_constraint("fk_invitations_reservation_identity_operation", "invitations", type_="foreignkey")
    op.drop_column("invitations", "reservation_expires_at")
    op.drop_column("invitations", "reserved_at")
    op.drop_column("invitations", "reservation_id")
    op.drop_table("identity_projection_clock")
    op.drop_table("identity_user_tombstones")
    op.drop_table("identity_operations")
