from __future__ import annotations

import gzip
import json
from decimal import Decimal

import pytest

import etk.domains.operations.database_maintenance as maintenance


class _MutationResult:
    rowcount = 1


class _RecordingSession:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: object) -> _MutationResult:
        self.statements.append(str(statement))
        return _MutationResult()


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


def test_prepare_library_rebuild_mutates_media_before_cleanup_index(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _RecordingSession()
    monkeypatch.setattr(maintenance, "_table_exists", lambda _session, _table: True)
    monkeypatch.setattr(maintenance, "_preflight_foreign_keys", lambda _session, _tables: None)

    result = maintenance._prepare_for_library_rebuild(session)

    media_update = next(
        index for index, statement in enumerate(session.statements) if "UPDATE media_metadata" in statement
    )
    cleanup_delete = next(
        index for index, statement in enumerate(session.statements) if 'DELETE FROM "cleanup_index"' in statement
    )
    assert media_update < cleanup_delete
    assert result["truncated_tables"] == ["user_media_data", "resubscribe_index", "cleanup_index"]
