from __future__ import annotations

import hashlib
import json
from pathlib import Path

import config_manager
from database import settings_db
from etk.bootstrap.legacy_runtime import LegacyRuntimeAdapter
from handler import shared_center_client

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_shared_center_quota_report_reads_only_the_canonical_license(monkeypatch) -> None:
    calls: list[str] = []
    values = {
        "pro_license_key": "ETK-Y-CANONICAL",
        "license_key": "ETK-L-RETIRED",
        "pro_expire_time": "2099-12-31T23:59:59Z",
    }

    def get_setting(key: str):
        calls.append(key)
        return values.get(key)

    monkeypatch.setattr(shared_center_client.settings_db, "get_setting", get_setting)
    monkeypatch.setattr(shared_center_client.config_manager, "APP_CONFIG", {})
    monkeypatch.setattr(shared_center_client, "_shared_cfg", lambda: {})
    monkeypatch.setattr(shared_center_client, "_current_server_id_hash", lambda: "fixture-server-hash")
    monkeypatch.setattr(shared_center_client, "_app_version", lambda: "fixture-version")

    payload = shared_center_client.build_current_pro_quota_report_payload()

    assert payload["pro_tier"] == "Y"
    assert payload["pro_license_hash"] == hashlib.sha256(b"ETK-Y-CANONICAL").hexdigest()
    assert payload["pro_server_id_hash"] == "fixture-server-hash"
    assert payload["auth_state"] == "grace_local_unverified"
    assert calls == ["pro_license_key", "pro_expire_time"]


def test_legacy_runtime_tier_hint_reads_only_the_canonical_license(monkeypatch) -> None:
    calls: list[str] = []
    values = {
        "pro_license_key": "ETK-M-CANONICAL",
        "license_key": "ETK-L-RETIRED",
    }

    def get_setting(key: str):
        calls.append(key)
        return values.get(key)

    monkeypatch.setattr(settings_db, "get_setting", get_setting)
    monkeypatch.setattr(config_manager, "APP_CONFIG", {"pro_license_key": "ETK-Y-IN-MEMORY"})

    assert LegacyRuntimeAdapter()._current_license_tier_hint() == "M"
    assert calls == ["pro_license_key"]


def test_license_alias_is_retired_and_historical_migration_is_retained() -> None:
    manifest = json.loads((REPO_ROOT / "verification/contracts/config-ownership.yaml").read_text(encoding="utf-8"))
    settings = {item["key"]: item for item in manifest["standalone_app_settings"]}
    alias = settings["license_key"]

    assert alias["contract_id"] == "setting-ab3540f4bf93"
    assert alias["status"] == "retired-approved"
    assert alias["retirement_approval"]
    assert alias["accesses"] == []

    migration = (REPO_ROOT / "migrations/versions/0021_p11_license_alias_cutover.py").read_text(encoding="utf-8")
    assert "license_key" in migration
    assert "pro_license_key" in migration
