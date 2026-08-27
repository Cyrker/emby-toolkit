from __future__ import annotations

import gzip
import json
from decimal import Decimal

import pytest

import etk.domains.operations.database_maintenance as maintenance


def _backup(*, format_version: int = 1, fingerprint: str | None = None) -> bytes:
    payload = {
        "format_version": format_version,
        "schema_fingerprint": fingerprint or "a" * 64,
        "metadata": {"source_emby_server_id": "fixture"},
        "data": {
            "translation_cache": [
                {
                    "original_text": "fixture source",
                    "translated_text": "fixture translation",
                    "engine_used": "stub",
                }
            ]
        },
    }
    return gzip.compress(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), mtime=0)


def test_inspect_backup_enforces_format_and_integrity() -> None:
    decoded, stored, expanded_size, compressed_size, ratio, compressed_hash, expanded_hash = (
        maintenance.inspect_backup_bytes(_backup())
    )

    assert decoded["format_version"] == maintenance.BACKUP_FORMAT_VERSION
    assert stored.startswith(b"\x1f\x8b")
    assert expanded_size > compressed_size > 0
    assert Decimal("1") <= ratio <= maintenance.MAX_COMPRESSION_RATIO
    assert len(compressed_hash) == len(expanded_hash) == 32

    with pytest.raises(maintenance.DatabaseMaintenanceValidationError, match="unsupported"):
        maintenance.inspect_backup_bytes(_backup(format_version=2))
    with pytest.raises(maintenance.DatabaseMaintenanceValidationError, match="valid gzip"):
        maintenance.inspect_backup_bytes(b"\x1f\x8b\x08\x00broken")
    with pytest.raises(maintenance.DatabaseMaintenanceValidationError, match="malformed"):
        maintenance.inspect_backup_bytes(b"not-json")


def test_inspect_backup_enforces_compressed_expanded_and_ratio_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    content = _backup()

    monkeypatch.setattr(maintenance, "MAX_COMPRESSED_SIZE", len(content) - 1)
    with pytest.raises(maintenance.DatabaseMaintenanceSizeLimitError, match="compressed"):
        maintenance.inspect_backup_bytes(content)

    monkeypatch.setattr(maintenance, "MAX_COMPRESSED_SIZE", 64 * 1024 * 1024)
    monkeypatch.setattr(maintenance, "MAX_EXPANDED_SIZE", 10)
    with pytest.raises(maintenance.DatabaseMaintenanceSizeLimitError, match="expanded"):
        maintenance.inspect_backup_bytes(content)

    monkeypatch.setattr(maintenance, "MAX_EXPANDED_SIZE", 256 * 1024 * 1024)
    monkeypatch.setattr(maintenance, "MAX_COMPRESSION_RATIO", Decimal("1"))
    with pytest.raises(maintenance.DatabaseMaintenanceSizeLimitError, match="ratio"):
        maintenance.inspect_backup_bytes(content)


def test_share_allowlist_and_overwrite_denylist_are_exact() -> None:
    with pytest.raises(maintenance.DatabaseMaintenanceValidationError, match="exact allowlist"):
        maintenance._validate_mutation_tables(["media_metadata", "app_settings"], allow_share=True)
    with pytest.raises(maintenance.DatabaseMaintenanceValidationError, match="control-plane"):
        maintenance._validate_mutation_tables(["database_backup_artifacts"], allow_share=False)
    assert maintenance.OVERWRITE_DENYLIST == frozenset({"emby_users", "cleanup_index", "invitations"})
