"""Add revisioned P6 MediaInfo cache and persistent probe item state.

Revision ID: 0008_p6_mediainfo_cache
Revises: 0007_p6_strm_sync
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0008_p6_mediainfo_cache"
down_revision: str | Sequence[str] | None = "0007_p6_strm_sync"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT upper(sha1)
                FROM p115_mediainfo_cache
                GROUP BY upper(sha1)
                HAVING count(*) > 1
            ) THEN
                RAISE EXCEPTION 'p115_mediainfo_cache contains case-insensitive SHA1 duplicates';
            END IF;
        END
        $$
        """
    )
    op.execute(
        """
        UPDATE p115_mediainfo_cache
        SET sha1 = upper(sha1)
        WHERE sha1 ~* '^[0-9a-f]{40}$'
          AND sha1 <> upper(sha1)
        """
    )
    op.add_column(
        "p115_mediainfo_cache",
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.add_column(
        "p115_mediainfo_cache",
        sa.Column("cache_revision", sa.BigInteger(), nullable=False, server_default="0"),
    )
    op.add_column(
        "p115_mediainfo_cache",
        sa.Column("raw_schema_version", sa.Integer(), nullable=False, server_default="1"),
    )
    op.add_column(
        "p115_mediainfo_cache",
        sa.Column("formatter_revision", sa.Text(), nullable=False, server_default="legacy-v1"),
    )
    op.add_column(
        "p115_mediainfo_cache",
        sa.Column("probe_revision", sa.Text(), nullable=False, server_default="legacy-v1"),
    )
    op.create_check_constraint(
        "ck_p115_mediainfo_cache_sha1_canonical",
        "p115_mediainfo_cache",
        "sha1 ~ '^[0-9A-F]{40}$'",
        postgresql_not_valid=True,
    )
    op.create_check_constraint(
        "ck_p115_mediainfo_cache_revision",
        "p115_mediainfo_cache",
        "cache_revision >= 0",
    )
    op.create_check_constraint(
        "ck_p115_mediainfo_cache_raw_schema_version",
        "p115_mediainfo_cache",
        "raw_schema_version >= 1",
    )
    op.create_check_constraint(
        "ck_p115_mediainfo_cache_formatter_revision",
        "p115_mediainfo_cache",
        "formatter_revision ~ '^[A-Za-z0-9._:-]{1,64}$'",
    )
    op.create_check_constraint(
        "ck_p115_mediainfo_cache_probe_revision",
        "p115_mediainfo_cache",
        "probe_revision ~ '^[A-Za-z0-9._:-]{1,64}$'",
    )
    op.create_index(
        "uq_p115_mediainfo_cache_upper_sha1",
        "p115_mediainfo_cache",
        [sa.text("upper(sha1)")],
        unique=True,
    )
    op.execute(
        """
        CREATE FUNCTION p115_mediainfo_cache_revision_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.mediainfo_json IS DISTINCT FROM OLD.mediainfo_json
               OR NEW.raw_ffprobe_json IS DISTINCT FROM OLD.raw_ffprobe_json THEN
                IF NEW.cache_revision <= OLD.cache_revision THEN
                    NEW.cache_revision := OLD.cache_revision + 1;
                END IF;
                NEW.updated_at := NOW();
            END IF;
            RETURN NEW;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_p115_mediainfo_cache_revision_guard
        BEFORE UPDATE OF mediainfo_json, raw_ffprobe_json
        ON p115_mediainfo_cache
        FOR EACH ROW
        EXECUTE FUNCTION p115_mediainfo_cache_revision_guard()
        """
    )

    op.create_table(
        "p115_media_probe_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("operation_key", sa.Text(), nullable=False),
        sa.Column("provenance_key", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("account_id", sa.Text(), nullable=False),
        sa.Column("sha1", sa.Text(), nullable=False),
        sa.Column("pick_code", sa.Text(), nullable=False),
        sa.Column("file_name", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expected_cache_revision", sa.BigInteger(), nullable=True),
        sa.Column("provider_backend", sa.Text(), nullable=True),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("last_error_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            "operation_key",
            "sha1",
            "provenance_key",
            name="uq_p115_media_probe_operation_sha1_provenance",
        ),
        sa.CheckConstraint(
            "length(operation_key) BETWEEN 1 AND 255",
            name="ck_p115_media_probe_operation_key",
        ),
        sa.CheckConstraint(
            "provenance_key ~ '^[0-9a-f]{64}$'",
            name="ck_p115_media_probe_provenance_key",
        ),
        sa.CheckConstraint(
            "source IN ('strm_sync', 'restore_mediainfo')",
            name="ck_p115_media_probe_source",
        ),
        sa.CheckConstraint("account_id ~ '^[0-9]+$'", name="ck_p115_media_probe_account"),
        sa.CheckConstraint("sha1 ~ '^[0-9A-F]{40}$'", name="ck_p115_media_probe_sha1"),
        sa.CheckConstraint("length(pick_code) BETWEEN 1 AND 255", name="ck_p115_media_probe_pick_code"),
        sa.CheckConstraint("length(file_name) BETWEEN 1 AND 1024", name="ck_p115_media_probe_file_name"),
        sa.CheckConstraint(
            "status IN ('pending', 'probing', 'retryable', 'confirmed', 'rejected', 'cancelled')",
            name="ck_p115_media_probe_status",
        ),
        sa.CheckConstraint("attempts >= 0", name="ck_p115_media_probe_attempts"),
        sa.CheckConstraint(
            "(status = 'probing') = (claim_token IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_p115_media_probe_claim",
        ),
        sa.CheckConstraint(
            "expected_cache_revision IS NULL OR expected_cache_revision >= 0",
            name="ck_p115_media_probe_expected_revision",
        ),
        sa.CheckConstraint(
            "provider_backend IS NULL OR provider_backend IN ('openapi', 'cookie', 'cache', 'raw')",
            name="ck_p115_media_probe_backend",
        ),
        sa.CheckConstraint(
            "result_json IS NULL OR jsonb_typeof(result_json) = 'object'",
            name="ck_p115_media_probe_result",
        ),
        sa.CheckConstraint(
            "last_error_json IS NULL OR jsonb_typeof(last_error_json) = 'object'",
            name="ck_p115_media_probe_error",
        ),
        sa.CheckConstraint(
            "(status IN ('confirmed', 'rejected', 'cancelled')) = (finished_at IS NOT NULL)",
            name="ck_p115_media_probe_finished",
        ),
    )
    op.create_index(
        "idx_p115_media_probe_account_status",
        "p115_media_probe_items",
        ["account_id", "status", "updated_at"],
    )


def downgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.drop_index("idx_p115_media_probe_account_status", table_name="p115_media_probe_items")
    op.drop_table("p115_media_probe_items")
    op.execute("DROP TRIGGER IF EXISTS trg_p115_mediainfo_cache_revision_guard ON p115_mediainfo_cache")
    op.execute("DROP FUNCTION IF EXISTS p115_mediainfo_cache_revision_guard()")
    op.drop_index("uq_p115_mediainfo_cache_upper_sha1", table_name="p115_mediainfo_cache")
    op.drop_constraint(
        "ck_p115_mediainfo_cache_probe_revision",
        "p115_mediainfo_cache",
        type_="check",
    )
    op.drop_constraint(
        "ck_p115_mediainfo_cache_formatter_revision",
        "p115_mediainfo_cache",
        type_="check",
    )
    op.drop_constraint(
        "ck_p115_mediainfo_cache_raw_schema_version",
        "p115_mediainfo_cache",
        type_="check",
    )
    op.drop_constraint(
        "ck_p115_mediainfo_cache_revision",
        "p115_mediainfo_cache",
        type_="check",
    )
    op.drop_constraint(
        "ck_p115_mediainfo_cache_sha1_canonical",
        "p115_mediainfo_cache",
        type_="check",
    )
    op.drop_column("p115_mediainfo_cache", "probe_revision")
    op.drop_column("p115_mediainfo_cache", "formatter_revision")
    op.drop_column("p115_mediainfo_cache", "raw_schema_version")
    op.drop_column("p115_mediainfo_cache", "cache_revision")
    op.drop_column("p115_mediainfo_cache", "updated_at")
