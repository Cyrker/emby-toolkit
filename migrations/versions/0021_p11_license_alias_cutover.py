"""Cut the historical license key alias over to the canonical setting."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0021_p11_license_alias_cutover"
down_revision: str | Sequence[str] | None = "0020_p11_cleanup_config_cutover"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

LICENSE_ALIAS_CUTOVER_SQL = """
WITH legacy AS (
    SELECT value_json
    FROM app_settings
    WHERE setting_key = 'license_key'
      AND jsonb_typeof(value_json) = 'string'
      AND BTRIM(value_json #>> '{}') <> ''
)
INSERT INTO app_settings (setting_key, value_json, last_updated_at)
SELECT 'pro_license_key', value_json, NOW()
FROM legacy
ON CONFLICT (setting_key) DO UPDATE
SET
    value_json = EXCLUDED.value_json,
    last_updated_at = NOW()
WHERE app_settings.value_json IS NULL
   OR app_settings.value_json = 'null'::jsonb
   OR (
       jsonb_typeof(app_settings.value_json) = 'string'
       AND BTRIM(app_settings.value_json #>> '{}') = ''
   )
"""


def upgrade() -> None:
    op.execute("SET LOCAL search_path TO public")
    op.execute(LICENSE_ALIAS_CUTOVER_SQL)


def downgrade() -> None:
    # Preserve the canonical row: deleting it could discard a later activation.
    pass
