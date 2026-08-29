"""Cut legacy cleanup setting rows over to the canonical object."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0020_p11_cleanup_config_cutover"
down_revision: str | Sequence[str] | None = "0019_p9_proxy_leases"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CLEANUP_CONFIG_CUTOVER_SQL = """
WITH legacy AS (
    SELECT
        COALESCE(
            (SELECT value_json FROM app_settings WHERE setting_key = 'media_cleanup_rules'),
            '[]'::jsonb
        ) AS rules,
        COALESCE(
            (SELECT value_json FROM app_settings WHERE setting_key = 'media_cleanup_library_ids'),
            '[]'::jsonb
        ) AS library_ids,
        COALESCE(
            (SELECT value_json FROM app_settings WHERE setting_key = 'media_cleanup_keep_one_per_res'),
            'false'::jsonb
        ) AS keep_one_per_res,
        COALESCE(
            (SELECT value_json FROM app_settings WHERE setting_key = 'media_cleanup_delete_delay'),
            '0'::jsonb
        ) AS delete_delay
)
INSERT INTO app_settings (setting_key, value_json, last_updated_at)
SELECT
    'media_cleanup_config',
    jsonb_build_object(
        'rules', rules,
        'library_ids', library_ids,
        'keep_one_per_res', keep_one_per_res,
        'delete_delay', delete_delay,
        'manual_delete_without_confirm', false
    ),
    NOW()
FROM legacy
ON CONFLICT (setting_key) DO NOTHING
"""


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.execute(CLEANUP_CONFIG_CUTOVER_SQL)


def downgrade() -> None:
    # Preserve the canonical row: deleting it could discard settings saved after upgrade.
    pass
