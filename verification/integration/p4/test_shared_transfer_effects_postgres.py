from __future__ import annotations

from sqlalchemy import text

from etk.compatibility.persistent_runtime_state import PostgresRuntimeStateBackend
from etk.compatibility.shared_transfer_effects import (
    SharedTransferEffectStore,
    SharedTransferSecretStore,
)
from etk.platform.db import DatabaseRuntime


def test_postgres_shared_transfer_effect_survives_runtime_recreation(p4_runtime: DatabaseRuntime) -> None:
    assert p4_runtime.raw_pool is not None
    backend = PostgresRuntimeStateBackend(p4_runtime.raw_pool.connection)
    first = SharedTransferEffectStore(backend)
    second = SharedTransferEffectStore(backend)
    key = "9" * 64
    evidence = {
        "account_id": "account-fixture",
        "action": "cloud_download",
        "intent_id": "intent-one",
        "mode": "rapid",
        "source_hash": "1" * 64,
        "source_id_hash": "2" * 64,
        "source_kind": "movie",
        "target_cid": "target-fixture",
        "virtual_id": None,
    }

    prepared, created = first.prepare(key, evidence)
    reused, reused_created = second.prepare(key, {**evidence, "intent_id": "intent-two"})
    succeeded = second.mark_succeeded(key)

    assert created is True
    assert reused_created is False
    assert prepared.status == reused.status == "prepared"
    assert first.read(key) == succeeded
    with p4_runtime.engine.connect() as connection:
        stored = connection.execute(
            text("SELECT value_json FROM app_settings WHERE setting_key = :key"),
            {"key": f"etk:p4:shared-transfer-effect:v1:{key}"},
        ).scalar_one()
    serialized = repr(stored)
    assert "share_code" not in serialized
    assert "receive_code" not in serialized
    assert "magnet:" not in serialized


def test_postgres_shared_transfer_secret_is_owner_bound_and_destroyed(p4_runtime: DatabaseRuntime) -> None:
    assert p4_runtime.raw_pool is not None
    backend = PostgresRuntimeStateBackend(p4_runtime.raw_pool.connection)
    first = SharedTransferSecretStore(backend)
    second = SharedTransferSecretStore(backend)
    reference = "6" * 64
    owner_key = "7" * 64
    payload = {
        "source": {
            "source_kind": "logical_season",
            "source_id": "fixture-season",
            "share_code": "raw-share",
            "receive_code": "raw-password",
        }
    }

    first.create(payload, owner_key=owner_key, reference=reference)
    assert second.consume(reference, owner_key).payload == payload
    with p4_runtime.engine.connect() as connection:
        stored = connection.execute(
            text("SELECT value_json FROM app_settings WHERE setting_key = :key"),
            {"key": f"etk:p4:shared-transfer-secret:v1:{reference}"},
        ).scalar_one()
    assert "raw-share" in repr(stored)

    assert second.destroy(reference, owner_key) is True
    with p4_runtime.engine.connect() as connection:
        remaining = connection.execute(
            text("SELECT COUNT(*) FROM app_settings WHERE setting_key = :key"),
            {"key": f"etk:p4:shared-transfer-secret:v1:{reference}"},
        ).scalar_one()
    assert remaining == 0
