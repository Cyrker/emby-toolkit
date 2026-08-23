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

_EFFECT_KEY_PATTERN = re.compile(r"[0-9a-f]{64}")
_STATE_VERSION = 1
_EFFECT_TTL_SECONDS = 7 * 24 * 60 * 60
_SECRET_TTL_SECONDS = 24 * 60 * 60
_REJECTED_EFFECT_TTL_SECONDS = 15 * 60
_FORBIDDEN_EVIDENCE_KEYS = frozenset(
    {
        "access_token",
        "bot_token",
        "cookie",
        "ed2k",
        "magnet",
        "password",
        "receive_code",
        "refresh_token",
        "secret",
        "share_code",
        "source_url",
        "token",
        "url",
    }
)


@dataclass(frozen=True, slots=True)
class TelegramResourceEffect:
    key: str
    action: str
    status: str
    prepared_at: datetime
    evidence: dict[str, Any]


@dataclass(frozen=True, slots=True)
class TelegramResourceEffectBinding:
    delivery_key: str
    effect_key: str
    action: str
    semantic_source_hash: str
    account_id: str
    target_cid: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class TelegramResourceSecret:
    reference: str
    expires_at: datetime
    consumed_by: str | None
    payload: dict[str, Any]


class TelegramResourceSecretExpiredError(RuntimeError):
    pass


class TelegramResourceSecretOwnershipError(RuntimeError):
    pass


class TelegramResourceEffectStore:
    """Small durable ledger for reconciling manual Telegram P115 effects."""

    def __init__(self, backend: RuntimeStateBackend | None = None) -> None:
        self._backend = backend or PostgresRuntimeStateBackend()

    def read(self, effect_key: str) -> TelegramResourceEffect | None:
        key = _effect_key(effect_key)

        def mutate(current: dict[str, Any] | None, now: datetime):
            if _state_expired(current, now):
                return None, None, True
            return current, _record(key, current), False

        return self._backend.mutate(_setting_key(key), mutate)

    def prepare(
        self,
        effect_key: str,
        action: str,
        evidence: Mapping[str, Any] | None = None,
        *,
        ttl_seconds: float = _EFFECT_TTL_SECONDS,
    ) -> tuple[TelegramResourceEffect, bool]:
        key = _effect_key(effect_key)
        action_name = str(action or "").strip()
        if not action_name:
            raise ValueError("Telegram resource effect action is required")
        ttl = _positive_ttl(ttl_seconds)
        _reject_sensitive_evidence(evidence or {}, path="evidence")
        normalized_evidence = normalize_json_object(dict(evidence or {}), field="Telegram resource effect evidence")

        def mutate(current: dict[str, Any] | None, now: datetime):
            if _state_expired(current, now):
                current = None
            existing = _record(key, current)
            if existing is not None:
                if existing.action != action_name:
                    raise RuntimeError("Telegram resource effect identity changed action")
                return current, (existing, False), False
            state = {
                "version": _STATE_VERSION,
                "action": action_name,
                "status": "prepared",
                "prepared_at": _iso(now),
                "updated_at": _iso(now),
                "expires_at": _iso(now + timedelta(seconds=ttl)),
                "evidence": normalized_evidence,
            }
            return state, (_record(key, state), True), True

        record, created = self._backend.mutate(_setting_key(key), mutate)
        if record is None:
            raise RuntimeError("Telegram resource effect was not prepared")
        return record, bool(created)

    def mark_succeeded(
        self,
        effect_key: str,
        action: str,
        evidence: Mapping[str, Any] | None = None,
        *,
        ttl_seconds: float = _EFFECT_TTL_SECONDS,
    ) -> TelegramResourceEffect:
        key = _effect_key(effect_key)
        action_name = str(action or "").strip()
        ttl = _positive_ttl(ttl_seconds)
        _reject_sensitive_evidence(evidence or {}, path="evidence")
        normalized_evidence = normalize_json_object(dict(evidence or {}), field="Telegram resource effect evidence")

        def mutate(current: dict[str, Any] | None, now: datetime):
            if _state_expired(current, now):
                current = None
            record = _record(key, current)
            if record is None:
                raise RuntimeError("Telegram resource effect is not prepared")
            if record.action != action_name:
                raise RuntimeError("Telegram resource effect identity changed action")
            if record.status == "succeeded" and not normalized_evidence:
                return current, record, False
            state = dict(current or {})
            state["status"] = "succeeded"
            state["updated_at"] = _iso(now)
            state["expires_at"] = _iso(now + timedelta(seconds=ttl))
            state["evidence"] = {
                **dict(record.evidence),
                **normalized_evidence,
            }
            return state, _record(key, state), True

        record = self._backend.mutate(_setting_key(key), mutate)
        if record is None:
            raise RuntimeError("Telegram resource effect success was not persisted")
        return record

    def mark_rejected(
        self,
        effect_key: str,
        action: str,
        evidence: Mapping[str, Any] | None = None,
        *,
        ttl_seconds: float = _REJECTED_EFFECT_TTL_SECONDS,
    ) -> TelegramResourceEffect:
        key = _effect_key(effect_key)
        action_name = str(action or "").strip()
        ttl = _positive_ttl(ttl_seconds)
        _reject_sensitive_evidence(evidence or {}, path="evidence")
        normalized_evidence = normalize_json_object(dict(evidence or {}), field="Telegram resource effect evidence")

        def mutate(current: dict[str, Any] | None, now: datetime):
            if _state_expired(current, now):
                current = None
            record = _record(key, current)
            if record is None:
                raise RuntimeError("Telegram resource effect is not prepared")
            if record.action != action_name:
                raise RuntimeError("Telegram resource effect identity changed action")
            state = dict(current or {})
            state["status"] = "rejected"
            state["updated_at"] = _iso(now)
            state["expires_at"] = _iso(now + timedelta(seconds=ttl))
            state["evidence"] = {
                **dict(record.evidence),
                **normalized_evidence,
            }
            return state, _record(key, state), True

        record = self._backend.mutate(_setting_key(key), mutate)
        if record is None:
            raise RuntimeError("Telegram resource effect rejection was not persisted")
        return record

    def clear_prepared(self, effect_key: str, action: str) -> bool:
        key = _effect_key(effect_key)
        action_name = str(action or "").strip()

        def mutate(current: dict[str, Any] | None, now: datetime):
            if _state_expired(current, now):
                return None, False, current is not None
            record = _record(key, current)
            if record is None or record.status == "succeeded":
                return current, False, False
            if record.action != action_name:
                raise RuntimeError("Telegram resource effect identity changed action")
            return None, True, True

        return bool(self._backend.mutate(_setting_key(key), mutate))


class TelegramResourceEffectBindingStore:
    """Bind one delivery to the stable account/target effect selected on its first attempt."""

    def __init__(self, backend: RuntimeStateBackend | None = None) -> None:
        self._backend = backend or PostgresRuntimeStateBackend()

    def read(self, delivery_key: str) -> TelegramResourceEffectBinding | None:
        key = _effect_key(delivery_key)

        def mutate(current: dict[str, Any] | None, now: datetime):
            if _state_expired(current, now):
                return None, None, current is not None
            return current, _binding_record(key, current), False

        return self._backend.mutate(_binding_setting_key(key), mutate)

    def bind(
        self,
        delivery_key: str,
        action: str,
        semantic_source_hash: str,
        account_id: str,
        target_cid: str,
        *,
        ttl_seconds: float = _EFFECT_TTL_SECONDS,
    ) -> tuple[TelegramResourceEffectBinding, bool]:
        key = _effect_key(delivery_key)
        action_name = str(action or "").strip()
        semantic_hash = _effect_key(semantic_source_hash)
        stable_account_id = str(account_id or "").strip()
        stable_target_cid = str(target_cid or "").strip()
        if not action_name or not stable_account_id or not stable_target_cid:
            raise ValueError("Telegram resource effect binding requires action, account_id, and target_cid")
        ttl = _positive_ttl(ttl_seconds)
        effect_key = semantic_effect_key(
            action_name,
            semantic_hash,
            stable_account_id,
            stable_target_cid,
        )

        def mutate(current: dict[str, Any] | None, now: datetime):
            if _state_expired(current, now):
                current = None
            existing = _binding_record(key, current)
            if existing is not None:
                expected = (action_name, semantic_hash, stable_account_id, stable_target_cid, effect_key)
                actual = (
                    existing.action,
                    existing.semantic_source_hash,
                    existing.account_id,
                    existing.target_cid,
                    existing.effect_key,
                )
                if actual != expected:
                    raise RuntimeError("Telegram resource delivery is already bound to another account or target")
                return current, (existing, False), False
            state = {
                "version": _STATE_VERSION,
                "effect_key": effect_key,
                "action": action_name,
                "semantic_source_hash": semantic_hash,
                "account_id": stable_account_id,
                "target_cid": stable_target_cid,
                "created_at": _iso(now),
                "expires_at": _iso(now + timedelta(seconds=ttl)),
            }
            return state, (_binding_record(key, state), True), True

        binding, created = self._backend.mutate(_binding_setting_key(key), mutate)
        if binding is None:
            raise RuntimeError("Telegram resource delivery binding was not persisted")
        return binding, bool(created)


class TelegramResourceSecretStore:
    """Opaque, expiring, owner-bound storage for raw resource credentials and links."""

    def __init__(self, backend: RuntimeStateBackend | None = None) -> None:
        self._backend = backend or PostgresRuntimeStateBackend()

    def create(
        self,
        payload: Mapping[str, Any],
        *,
        reference: str | None = None,
        owner_key: str | None = None,
        ttl_seconds: float = _SECRET_TTL_SECONDS,
    ) -> tuple[str, bool]:
        normalized = normalize_json_object(dict(payload), field="Telegram resource secret")
        if not normalized:
            raise ValueError("Telegram resource secret payload cannot be empty")
        ref = _effect_key(reference) if reference is not None else secrets.token_hex(32)
        owner = _effect_key(owner_key) if owner_key is not None else None
        ttl = _positive_ttl(ttl_seconds)
        payload_hash = _canonical_hash(normalized)

        def mutate(current: dict[str, Any] | None, now: datetime):
            if _state_expired(current, now):
                current = None
            if current is not None:
                record = _secret_record(ref, current)
                if current.get("payload_hash") != payload_hash:
                    raise RuntimeError("Telegram resource secret reference was reused with different content")
                if owner is not None and record.consumed_by not in {None, owner}:
                    raise TelegramResourceSecretOwnershipError("Telegram resource secret belongs to another operation")
                return current, (ref, False), False
            state = {
                "version": _STATE_VERSION,
                "payload": normalized,
                "payload_hash": payload_hash,
                "created_at": _iso(now),
                "expires_at": _iso(now + timedelta(seconds=ttl)),
                "consumed_by": owner,
            }
            return state, (ref, True), True

        return self._backend.mutate(_secret_setting_key(ref), mutate)

    def consume(self, reference: str, owner_key: str) -> TelegramResourceSecret:
        ref = _effect_key(reference)
        owner = _effect_key(owner_key)

        def mutate(current: dict[str, Any] | None, now: datetime):
            if current is None or _state_expired(current, now):
                return None, None, current is not None
            record = _secret_record(ref, current)
            if record.consumed_by not in {None, owner}:
                raise TelegramResourceSecretOwnershipError("Telegram resource secret belongs to another operation")
            if record.consumed_by == owner:
                return current, record, False
            state = dict(current)
            state["consumed_by"] = owner
            state["consumed_at"] = _iso(now)
            return state, _secret_record(ref, state), True

        record = self._backend.mutate(_secret_setting_key(ref), mutate)
        if record is None:
            raise TelegramResourceSecretExpiredError("Telegram resource secret is missing or expired")
        return record

    def destroy(self, reference: str, owner_key: str | None = None) -> bool:
        ref = _effect_key(reference)
        owner = _effect_key(owner_key) if owner_key is not None else None

        def mutate(current: dict[str, Any] | None, now: datetime):
            if current is None:
                return None, False, False
            if _state_expired(current, now):
                return None, False, True
            record = _secret_record(ref, current)
            if owner is not None and record.consumed_by not in {None, owner}:
                raise TelegramResourceSecretOwnershipError("Telegram resource secret belongs to another operation")
            return None, True, True

        return bool(self._backend.mutate(_secret_setting_key(ref), mutate))


_DEFAULT_STORE = TelegramResourceEffectStore()
_DEFAULT_BINDING_STORE = TelegramResourceEffectBindingStore()
_DEFAULT_SECRET_STORE = TelegramResourceSecretStore()


def read_effect(effect_key: str) -> TelegramResourceEffect | None:
    return _DEFAULT_STORE.read(effect_key)


def prepare_effect(
    effect_key: str,
    action: str,
    evidence: Mapping[str, Any] | None = None,
    *,
    ttl_seconds: float = _EFFECT_TTL_SECONDS,
) -> tuple[TelegramResourceEffect, bool]:
    return _DEFAULT_STORE.prepare(effect_key, action, evidence, ttl_seconds=ttl_seconds)


def mark_effect_succeeded(
    effect_key: str,
    action: str,
    evidence: Mapping[str, Any] | None = None,
    *,
    ttl_seconds: float = _EFFECT_TTL_SECONDS,
) -> TelegramResourceEffect:
    return _DEFAULT_STORE.mark_succeeded(effect_key, action, evidence, ttl_seconds=ttl_seconds)


def mark_effect_rejected(
    effect_key: str,
    action: str,
    evidence: Mapping[str, Any] | None = None,
    *,
    ttl_seconds: float = _REJECTED_EFFECT_TTL_SECONDS,
) -> TelegramResourceEffect:
    return _DEFAULT_STORE.mark_rejected(effect_key, action, evidence, ttl_seconds=ttl_seconds)


def clear_prepared_effect(effect_key: str, action: str) -> bool:
    return _DEFAULT_STORE.clear_prepared(effect_key, action)


def read_effect_binding(delivery_key: str) -> TelegramResourceEffectBinding | None:
    return _DEFAULT_BINDING_STORE.read(delivery_key)


def bind_effect_delivery(
    delivery_key: str,
    action: str,
    semantic_source_hash: str,
    account_id: str,
    target_cid: str,
) -> tuple[TelegramResourceEffectBinding, bool]:
    return _DEFAULT_BINDING_STORE.bind(
        delivery_key,
        action,
        semantic_source_hash,
        account_id,
        target_cid,
    )


def create_secret(
    payload: Mapping[str, Any],
    *,
    reference: str | None = None,
    owner_key: str | None = None,
    ttl_seconds: float = _SECRET_TTL_SECONDS,
) -> tuple[str, bool]:
    return _DEFAULT_SECRET_STORE.create(
        payload,
        reference=reference,
        owner_key=owner_key,
        ttl_seconds=ttl_seconds,
    )


def consume_secret(reference: str, owner_key: str) -> TelegramResourceSecret:
    return _DEFAULT_SECRET_STORE.consume(reference, owner_key)


def destroy_secret(reference: str, owner_key: str | None = None) -> bool:
    return _DEFAULT_SECRET_STORE.destroy(reference, owner_key)


def semantic_effect_key(action: str, semantic_source_hash: str, account_id: str, target_cid: str) -> str:
    identity = {
        "action": str(action or "").strip(),
        "semantic_source_hash": _effect_key(semantic_source_hash),
        "account_id": str(account_id or "").strip(),
        "target_cid": str(target_cid or "").strip(),
    }
    if not identity["action"] or not identity["account_id"] or not identity["target_cid"]:
        raise ValueError("Telegram resource effect identity is incomplete")
    return _canonical_hash(identity)


def _record(effect_key: str, state: dict[str, Any] | None) -> TelegramResourceEffect | None:
    if state is None:
        return None
    if int(state.get("version") or 0) != _STATE_VERSION:
        raise ValueError("unsupported Telegram resource effect state version")
    action = str(state.get("action") or "").strip()
    status = str(state.get("status") or "").strip()
    if not action or status not in {"prepared", "succeeded", "rejected"}:
        raise ValueError("invalid Telegram resource effect state")
    evidence = state.get("evidence")
    if not isinstance(evidence, Mapping):
        raise ValueError("Telegram resource effect evidence must be an object")
    return TelegramResourceEffect(
        key=effect_key,
        action=action,
        status=status,
        prepared_at=_parse_datetime(state.get("prepared_at")),
        evidence=normalize_json_object(dict(evidence), field="Telegram resource effect evidence"),
    )


def _binding_record(
    delivery_key: str,
    state: dict[str, Any] | None,
) -> TelegramResourceEffectBinding | None:
    if state is None:
        return None
    if int(state.get("version") or 0) != _STATE_VERSION:
        raise ValueError("unsupported Telegram resource effect binding version")
    action = str(state.get("action") or "").strip()
    account_id = str(state.get("account_id") or "").strip()
    target_cid = str(state.get("target_cid") or "").strip()
    if not action or not account_id or not target_cid:
        raise ValueError("Telegram resource effect binding is incomplete")
    return TelegramResourceEffectBinding(
        delivery_key=delivery_key,
        effect_key=_effect_key(str(state.get("effect_key") or "")),
        action=action,
        semantic_source_hash=_effect_key(str(state.get("semantic_source_hash") or "")),
        account_id=account_id,
        target_cid=target_cid,
        expires_at=_parse_datetime(state.get("expires_at")),
    )


def _secret_record(reference: str, state: dict[str, Any]) -> TelegramResourceSecret:
    if int(state.get("version") or 0) != _STATE_VERSION:
        raise ValueError("unsupported Telegram resource secret version")
    payload = state.get("payload")
    if not isinstance(payload, Mapping):
        raise ValueError("Telegram resource secret payload must be an object")
    consumed_by = state.get("consumed_by")
    if consumed_by is not None:
        consumed_by = _effect_key(str(consumed_by))
    return TelegramResourceSecret(
        reference=reference,
        expires_at=_parse_datetime(state.get("expires_at")),
        consumed_by=consumed_by,
        payload=normalize_json_object(dict(payload), field="Telegram resource secret"),
    )


def _effect_key(value: str) -> str:
    key = str(value or "").strip().casefold()
    if not _EFFECT_KEY_PATTERN.fullmatch(key):
        raise ValueError("Telegram resource effect key must be a SHA-256 hex digest")
    return key


def _reject_sensitive_evidence(value: Any, *, path: str) -> None:
    if isinstance(value, Mapping):
        for raw_key, nested in value.items():
            key = str(raw_key).strip().casefold()
            if key in _FORBIDDEN_EVIDENCE_KEYS:
                raise ValueError(f"Telegram resource effect cannot persist {path}.{raw_key}")
            _reject_sensitive_evidence(nested, path=f"{path}.{raw_key}")
    elif isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            _reject_sensitive_evidence(nested, path=f"{path}[{index}]")
    elif isinstance(value, str):
        lowered = value.casefold()
        if "magnet:?" in lowered or "ed2k://" in lowered or "115.com/s/" in lowered:
            raise ValueError(f"Telegram resource effect cannot persist raw link at {path}")


def _setting_key(effect_key: str) -> str:
    return f"runtime.effect.telegram-resource.{effect_key}"


def _binding_setting_key(delivery_key: str) -> str:
    return f"runtime.effect-binding.telegram-resource.{delivery_key}"


def _secret_setting_key(reference: str) -> str:
    return f"runtime.secret.telegram-resource.{reference}"


def _state_expired(state: Mapping[str, Any] | None, now: datetime) -> bool:
    if state is None:
        return False
    raw = state.get("expires_at")
    if not raw:
        return False
    return now >= _parse_datetime(raw)


def _positive_ttl(value: float) -> float:
    ttl = float(value)
    if ttl <= 0:
        raise ValueError("Telegram resource TTL must be positive")
    return ttl


def _canonical_hash(value: Mapping[str, Any]) -> str:
    normalized = normalize_json_object(dict(value), field="Telegram resource identity")
    encoded = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _parse_datetime(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError("Telegram resource effect timestamp is missing")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


__all__ = [
    "TelegramResourceEffect",
    "TelegramResourceEffectBinding",
    "TelegramResourceEffectBindingStore",
    "TelegramResourceEffectStore",
    "TelegramResourceSecret",
    "TelegramResourceSecretExpiredError",
    "TelegramResourceSecretOwnershipError",
    "TelegramResourceSecretStore",
    "bind_effect_delivery",
    "clear_prepared_effect",
    "consume_secret",
    "create_secret",
    "destroy_secret",
    "mark_effect_rejected",
    "mark_effect_succeeded",
    "prepare_effect",
    "read_effect",
    "read_effect_binding",
    "semantic_effect_key",
]
