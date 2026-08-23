from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from etk.compatibility.shared_transfer_effects import (
    SharedTransferEffectStore,
    SharedTransferSecretExpiredError,
    SharedTransferSecretOwnershipError,
    SharedTransferSecretStore,
)

EFFECT_KEY = "7" * 64


class MemoryBackend:
    def __init__(self) -> None:
        self.values: dict[str, dict] = {}
        self.now = datetime(2026, 8, 23, tzinfo=UTC)

    def mutate(self, setting_key, mutation):
        current = deepcopy(self.values.get(setting_key))
        next_state, result, should_write = mutation(current, self.now)
        if should_write:
            if next_state is None:
                self.values.pop(setting_key, None)
            else:
                self.values[setting_key] = deepcopy(next_state)
        return result


def _evidence(intent_id: str = "intent-one") -> dict[str, object]:
    return {
        "account_id": "account-fixture",
        "action": "center_import",
        "intent_id": intent_id,
        "mode": "rapid",
        "source_hash": "1" * 64,
        "source_id_hash": "2" * 64,
        "source_kind": "movie",
        "target_cid": "target-fixture",
        "virtual_id": None,
    }


def test_shared_transfer_effect_reuses_semantic_identity_across_delivery_intents() -> None:
    backend = MemoryBackend()
    store = SharedTransferEffectStore(backend)

    prepared, created = store.prepare(EFFECT_KEY, _evidence())
    reused, reused_created = store.prepare(EFFECT_KEY, _evidence("intent-two"))
    succeeded = store.mark_succeeded(EFFECT_KEY)

    assert created is True
    assert reused_created is False
    assert prepared.status == reused.status == "prepared"
    assert succeeded.status == "succeeded"
    assert store.read(EFFECT_KEY) == succeeded


def test_shared_transfer_effect_records_only_allowlisted_hashed_evidence() -> None:
    backend = MemoryBackend()
    store = SharedTransferEffectStore(backend)
    store.prepare(EFFECT_KEY, _evidence())
    indeterminate = store.mark_indeterminate(EFFECT_KEY, {"error_hash": "3" * 64})

    assert indeterminate.status == "indeterminate"
    serialized = repr(backend.values)
    assert "share_code" not in serialized
    assert "receive_code" not in serialized
    assert "magnet:" not in serialized

    with pytest.raises(ValueError, match="unsupported keys"):
        SharedTransferEffectStore(MemoryBackend()).prepare(
            "8" * 64,
            {**_evidence(), "share_code": "must-not-persist"},
        )


def test_shared_transfer_effect_rejects_account_or_target_drift_for_same_key() -> None:
    store = SharedTransferEffectStore(MemoryBackend())
    store.prepare(EFFECT_KEY, _evidence())

    with pytest.raises(RuntimeError, match="changed evidence"):
        store.prepare(EFFECT_KEY, {**_evidence("intent-two"), "account_id": "other-account"})
    with pytest.raises(RuntimeError, match="changed evidence"):
        store.prepare(EFFECT_KEY, {**_evidence("intent-two"), "target_cid": "other-target"})


def test_shared_transfer_prepared_receipt_can_only_be_discarded_before_transition() -> None:
    store = SharedTransferEffectStore(MemoryBackend())
    store.prepare(EFFECT_KEY, _evidence())

    assert store.discard_prepared(EFFECT_KEY) is True
    assert store.read(EFFECT_KEY) is None

    store.prepare(EFFECT_KEY, _evidence())
    store.mark_succeeded(EFFECT_KEY)
    assert store.discard_prepared(EFFECT_KEY) is False
    succeeded = store.read(EFFECT_KEY)
    assert succeeded is not None
    assert succeeded.status == "succeeded"


def test_shared_transfer_secret_is_owner_bound_expiring_and_destroyable() -> None:
    backend = MemoryBackend()
    store = SharedTransferSecretStore(backend)
    reference = "4" * 64
    payload = {
        "source": {
            "source_kind": "movie",
            "source_id": "fixture-source",
            "share_code": "raw-share",
            "receive_code": "raw-password",
            "url": "magnet:?xt=urn:btih:PRIVATE",
        }
    }

    created_ref, created = store.create(
        payload,
        owner_key=EFFECT_KEY,
        reference=reference,
        ttl_seconds=60,
    )
    assert created is True
    assert created_ref == reference
    assert store.consume(reference, EFFECT_KEY).payload == payload
    with pytest.raises(SharedTransferSecretOwnershipError):
        store.consume(reference, "8" * 64)
    assert store.destroy(reference, EFFECT_KEY) is True
    with pytest.raises(SharedTransferSecretExpiredError):
        store.consume(reference, EFFECT_KEY)

    store.create(payload, owner_key=EFFECT_KEY, reference=reference, ttl_seconds=60)
    backend.now += timedelta(seconds=61)
    with pytest.raises(SharedTransferSecretExpiredError):
        store.consume(reference, EFFECT_KEY)
    assert "raw-share" not in repr(backend.values)
