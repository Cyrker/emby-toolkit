from __future__ import annotations

import hashlib
import json
import re
import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from etk.compatibility.persistent_runtime_state import (
    PostgresRuntimeStateBackend,
    RuntimeStateBackend,
)
from etk.platform.jobs import normalize_json_object

_KEY_PATTERN = re.compile(r"[0-9a-f]{64}")
_STATE_VERSION = 1
_DEFAULT_TTL_SECONDS = 30 * 24 * 60 * 60
_ALLOWED_STATUSES = frozenset({"prepared", "succeeded", "indeterminate"})


@dataclass(frozen=True, slots=True)
class SharedTransferEffect:
    key: str
    status: str
    prepared_at: datetime
    evidence: dict[str, Any]


@dataclass(frozen=True, slots=True)
class SharedTransferSecret:
    reference: str
    owner_key: str
    expires_at: datetime
    payload: dict[str, Any]


class SharedTransferSecretExpiredError(RuntimeError):
    pass


class SharedTransferSecretOwnershipError(RuntimeError):
    pass


class SharedTransferEffectStore:
    """Durable fail-closed receipt for one account/target/source transfer effect."""

    def __init__(self, backend: RuntimeStateBackend | None = None) -> None:
        self._backend = backend or PostgresRuntimeStateBackend()

    def read(self, effect_key: str) -> SharedTransferEffect | None:
        key = _effect_key(effect_key)

        def mutate(current: dict[str, Any] | None, now: datetime):
            if _expired(current, now):
                return None, None, current is not None
            return current, _record(key, current), False

        return self._backend.mutate(_setting_key(key), mutate)

    def prepare(
        self,
        effect_key: str,
        evidence: Mapping[str, Any],
        *,
        ttl_seconds: float = _DEFAULT_TTL_SECONDS,
    ) -> tuple[SharedTransferEffect, bool]:
        key = _effect_key(effect_key)
        ttl = _positive_ttl(ttl_seconds)
        normalized = _evidence(evidence)

        def mutate(current: dict[str, Any] | None, now: datetime):
            if _expired(current, now):
                current = None
            existing = _record(key, current)
            if existing is not None:
                stable_fields = (
                    "account_id",
                    "action",
                    "mode",
                    "source_hash",
                    "source_id_hash",
                    "source_kind",
                    "target_cid",
                    "virtual_id",
                )
                if any(existing.evidence.get(field) != normalized.get(field) for field in stable_fields):
                    raise RuntimeError("shared transfer effect identity changed evidence")
                return current, (existing, False), False
            state = {
                "version": _STATE_VERSION,
                "status": "prepared",
                "prepared_at": _iso(now),
                "updated_at": _iso(now),
                "expires_at": _iso(now + timedelta(seconds=ttl)),
                "evidence": normalized,
            }
            return state, (_record(key, state), True), True

        record, created = self._backend.mutate(_setting_key(key), mutate)
        if record is None:
            raise RuntimeError("shared transfer effect was not prepared")
        return record, bool(created)

    def mark_succeeded(
        self,
        effect_key: str,
        *,
        ttl_seconds: float = _DEFAULT_TTL_SECONDS,
    ) -> SharedTransferEffect:
        return self._transition(effect_key, "succeeded", ttl_seconds=ttl_seconds)

    def mark_indeterminate(
        self,
        effect_key: str,
        evidence: Mapping[str, Any] | None = None,
        *,
        ttl_seconds: float = _DEFAULT_TTL_SECONDS,
    ) -> SharedTransferEffect:
        return self._transition(
            effect_key,
            "indeterminate",
            evidence=evidence,
            ttl_seconds=ttl_seconds,
        )

    def discard_prepared(self, effect_key: str) -> bool:
        """Remove a receipt only when this process proved the provider was not entered."""

        key = _effect_key(effect_key)

        def mutate(current: dict[str, Any] | None, now: datetime):
            if _expired(current, now):
                return None, False, current is not None
            record = _record(key, current)
            if record is None or record.status != "prepared":
                return current, False, False
            return None, True, True

        return bool(self._backend.mutate(_setting_key(key), mutate))

    def _transition(
        self,
        effect_key: str,
        status: str,
        *,
        evidence: Mapping[str, Any] | None = None,
        ttl_seconds: float,
    ) -> SharedTransferEffect:
        key = _effect_key(effect_key)
        ttl = _positive_ttl(ttl_seconds)
        additions = _evidence(evidence or {})

        def mutate(current: dict[str, Any] | None, now: datetime):
            record = _record(key, current)
            if record is None:
                raise RuntimeError("shared transfer effect is not prepared")
            state = dict(current or {})
            state["status"] = status
            state["updated_at"] = _iso(now)
            state["expires_at"] = _iso(now + timedelta(seconds=ttl))
            state["evidence"] = {**record.evidence, **additions}
            return state, _record(key, state), True

        record = self._backend.mutate(_setting_key(key), mutate)
        if record is None:
            raise RuntimeError("shared transfer effect transition was not persisted")
        return record


class SharedTransferSecretStore:
    """Opaque, expiring source payload storage bound to one transfer effect."""

    def __init__(self, backend: RuntimeStateBackend | None = None) -> None:
        self._backend = backend or PostgresRuntimeStateBackend()

    def create(
        self,
        payload: Mapping[str, Any],
        *,
        owner_key: str,
        reference: str | None = None,
        ttl_seconds: float = _DEFAULT_TTL_SECONDS,
    ) -> tuple[str, bool]:
        normalized = normalize_json_object(dict(payload), field="shared transfer secret")
        if not normalized:
            raise ValueError("shared transfer secret payload cannot be empty")
        owner = _effect_key(owner_key)
        ref = _effect_key(reference) if reference is not None else secrets.token_hex(32)
        ttl = _positive_ttl(ttl_seconds)
        payload_hash = _canonical_hash(normalized)

        def mutate(current: dict[str, Any] | None, now: datetime):
            if _expired(current, now):
                current = None
            if current is not None:
                record = _secret_record(ref, current)
                if record.owner_key != owner:
                    raise SharedTransferSecretOwnershipError("shared transfer secret belongs to another effect")
                if str(current.get("payload_hash") or "") != payload_hash:
                    raise RuntimeError("shared transfer secret reference was reused with different content")
                return current, (ref, False), False
            state = {
                "version": _STATE_VERSION,
                "owner_key": owner,
                "payload": normalized,
                "payload_hash": payload_hash,
                "created_at": _iso(now),
                "updated_at": _iso(now),
                "expires_at": _iso(now + timedelta(seconds=ttl)),
            }
            return state, (ref, True), True

        return self._backend.mutate(_secret_setting_key(ref), mutate)

    def consume(self, reference: str, owner_key: str) -> SharedTransferSecret:
        ref = _effect_key(reference)
        owner = _effect_key(owner_key)

        def mutate(current: dict[str, Any] | None, now: datetime):
            if current is None or _expired(current, now):
                return None, None, current is not None
            record = _secret_record(ref, current)
            if record.owner_key != owner:
                raise SharedTransferSecretOwnershipError("shared transfer secret belongs to another effect")
            return current, record, False

        record = self._backend.mutate(_secret_setting_key(ref), mutate)
        if record is None:
            raise SharedTransferSecretExpiredError("shared transfer secret is missing or expired")
        return record

    def destroy(self, reference: str, owner_key: str) -> bool:
        ref = _effect_key(reference)
        owner = _effect_key(owner_key)

        def mutate(current: dict[str, Any] | None, now: datetime):
            if current is None:
                return None, False, False
            if _expired(current, now):
                return None, False, True
            record = _secret_record(ref, current)
            if record.owner_key != owner:
                raise SharedTransferSecretOwnershipError("shared transfer secret belongs to another effect")
            return None, True, True

        return bool(self._backend.mutate(_secret_setting_key(ref), mutate))


def _evidence(value: Mapping[str, Any]) -> dict[str, Any]:
    normalized = normalize_json_object(dict(value), field="shared transfer effect evidence")
    allowed = {
        "account_id",
        "action",
        "error_hash",
        "intent_id",
        "mode",
        "source_hash",
        "source_id_hash",
        "source_kind",
        "target_cid",
        "virtual_id",
    }
    unexpected = sorted(set(normalized) - allowed)
    if unexpected:
        raise ValueError(f"shared transfer effect evidence contains unsupported keys: {unexpected}")
    return normalized


def _record(key: str, state: Mapping[str, Any] | None) -> SharedTransferEffect | None:
    if not isinstance(state, Mapping):
        return None
    status = str(state.get("status") or "")
    if status not in _ALLOWED_STATUSES:
        raise RuntimeError("shared transfer effect has an invalid status")
    evidence = state.get("evidence")
    if not isinstance(evidence, Mapping):
        raise RuntimeError("shared transfer effect evidence is invalid")
    return SharedTransferEffect(
        key=key,
        status=status,
        prepared_at=_parse_time(state.get("prepared_at")),
        evidence=dict(evidence),
    )


def _secret_record(reference: str, state: Mapping[str, Any]) -> SharedTransferSecret:
    if int(state.get("version") or 0) != _STATE_VERSION:
        raise RuntimeError("shared transfer secret has an unsupported version")
    owner_key = _effect_key(str(state.get("owner_key") or ""))
    payload = state.get("payload")
    if not isinstance(payload, Mapping):
        raise RuntimeError("shared transfer secret payload is invalid")
    return SharedTransferSecret(
        reference=reference,
        owner_key=owner_key,
        expires_at=_parse_time(state.get("expires_at")),
        payload=normalize_json_object(dict(payload), field="shared transfer secret"),
    )


def _expired(state: Mapping[str, Any] | None, now: datetime) -> bool:
    if not isinstance(state, Mapping):
        return False
    return _parse_time(state.get("expires_at")) <= now


def _effect_key(value: str) -> str:
    key = str(value or "").strip().lower()
    if _KEY_PATTERN.fullmatch(key) is None:
        raise ValueError("shared transfer effect key must be a SHA-256 hex digest")
    return key


def _setting_key(key: str) -> str:
    return f"etk:p4:shared-transfer-effect:v1:{key}"


def _secret_setting_key(reference: str) -> str:
    return f"etk:p4:shared-transfer-secret:v1:{reference}"


def _canonical_hash(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        normalize_json_object(dict(value), field="shared transfer secret hash"),
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _positive_ttl(value: float) -> float:
    ttl = float(value)
    if ttl <= 0:
        raise ValueError("shared transfer effect TTL must be positive")
    return ttl


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _parse_time(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("shared transfer effect timestamp is invalid") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


__all__ = [
    "SharedTransferEffect",
    "SharedTransferEffectStore",
    "SharedTransferSecret",
    "SharedTransferSecretExpiredError",
    "SharedTransferSecretOwnershipError",
    "SharedTransferSecretStore",
]
