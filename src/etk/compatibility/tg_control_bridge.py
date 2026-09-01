from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from collections.abc import Callable, Collection, Mapping
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from etk.compatibility import tg_resource_effects
from etk.platform.jobs import normalize_json_object

logger = logging.getLogger(__name__)

_SETTING_KEY = "tg_userbot_control_bridge"
_STATE_VERSION = 1
_TERMINAL_STATES = frozenset({"succeeded", "failed", "indeterminate"})
_COMMAND_TYPES = frozenset({"send_code", "login", "logout", "clear_session", "search", "listener_action"})
_MAX_TERMINAL_COMMANDS = 200
_MAX_IDEMPOTENCY_RECORDS = 2_000
_AUTO_SEARCH_SECRET_TTL_SECONDS = 7 * 24 * 60 * 60
_MAX_AUTO_SEARCH_RESULT_BYTES = 2 * 1024 * 1024
_LISTENER_ACTION_MAX_ATTEMPTS = 8
_LISTENER_ACTION_MAX_RETRY_SECONDS = 300
_RESOURCE_ACTION_VERSION = 1
_RESOURCE_ACTIONS = frozenset(
    {
        "shared_pool_transfer",
        "hdhive_transfer",
        "subscription",
        "p115_share_import",
        "p115_offline_add",
    }
)
_RESOURCE_ACTION_TASK_NAMES = {
    "shared_pool_transfer": "TG共享池资源转存",
    "hdhive_transfer": "TG影巢资源转存",
    "subscription": "TG搜索订阅",
    "p115_share_import": "TG手动115分享转存",
    "p115_offline_add": "TG手动离线下载",
}
_RESOURCE_SECRET_ACTIONS = frozenset({"p115_share_import", "p115_offline_add"})
_RESOURCE_LINK_PATTERN = re.compile(r"(?i)(?:magnet:\?|ed2k://|115(?:cdn)?\.com/s/)")
_CHANNEL_SECRET_KEYS = frozenset(
    {
        "full_url",
        "magnet_url",
        "password",
        "raw_text",
        "receive_code",
        "share_code",
        "target_link",
        "text",
        "url",
    }
)
_CHANNEL_DELIVERY_SCOPE = "telegram-channel-transfer:v2"
_RESOURCE_ACTION_SCOPE = "telegram-resource-action:v2"
_FORBIDDEN_RESOURCE_ACTION_KEYS = frozenset(
    {
        "api_key",
        "apikey",
        "access_token",
        "refresh_token",
        "bot_token",
        "cookie",
        "cookies",
        "password",
        "passwd",
        "secret",
        "credential",
        "credentials",
        "processor",
        "client",
    }
)


class TelegramCommandTimeoutError(TimeoutError):
    pass


class TelegramCommandFailedError(RuntimeError):
    pass


class TelegramCommandIndeterminateError(RuntimeError):
    pass


class TelegramCommandIdempotencyConflictError(RuntimeError):
    pass


class PersistentTaskGatewayUnavailableError(RuntimeError):
    pass


class TelegramControlBridge:
    """Small PostgreSQL-backed command/status bridge for the Coordinator-owned UserBot."""

    def __init__(
        self,
        *,
        connection_factory: Callable[[], AbstractContextManager[Any]] | None = None,
        clock: Callable[[], datetime] | None = None,
        resource_secrets: tg_resource_effects.TelegramResourceSecretStore | None = None,
    ) -> None:
        if connection_factory is None:
            from database.connection import get_db_connection

            connection_factory = get_db_connection
        self._connection_factory = connection_factory
        self._clock = clock or (lambda: datetime.now(UTC))
        self._resource_secrets = resource_secrets or tg_resource_effects.TelegramResourceSecretStore()

    def enqueue(self, command_type: str, payload: Mapping[str, Any] | None = None) -> str:
        command_type = str(command_type or "").strip().casefold()
        if command_type not in _COMMAND_TYPES:
            raise ValueError(f"unsupported Telegram command: {command_type!r}")
        normalized_payload = normalize_json_object(dict(payload or {}), field="telegram_command.payload")
        now = self._now()
        command_id = str(uuid4())

        def mutate(state: dict[str, Any]) -> None:
            state["commands"].append(
                {
                    "id": command_id,
                    "type": command_type,
                    "payload": normalized_payload,
                    "status": "pending",
                    "created_at": now.isoformat(),
                    "claimed_at": None,
                    "claimed_by": "",
                    "lease_expires_at": None,
                    "attempt_count": 0,
                    "next_attempt_at": None,
                    "completed_at": None,
                    "result": {},
                    "error": "",
                }
            )
            self._trim_terminal_commands(state)

        self._mutate(mutate)
        return command_id

    def enqueue_once(
        self,
        command_type: str,
        payload: Mapping[str, Any] | None = None,
        *,
        idempotency_key: str,
        result_owner_key: str | None = None,
    ) -> str:
        prepared = self._prepare_once(
            command_type,
            payload,
            idempotency_key=idempotency_key,
            result_owner_key=result_owner_key,
        )

        def mutate(state: dict[str, Any]) -> bool:
            return self._reserve_once(state, prepared)

        self._mutate(mutate)
        return str(prepared["command_id"])

    def enqueue_listener_action_once(
        self,
        payload: Mapping[str, Any],
        *,
        secret_payload: Mapping[str, Any],
        reference: str,
        owner_key: str,
        ttl_seconds: float,
        idempotency_key: str,
    ) -> str:
        prepared = self._prepare_once(
            "listener_action",
            payload,
            idempotency_key=idempotency_key,
            result_owner_key=None,
        )
        normalized_secret = normalize_json_object(
            dict(secret_payload),
            field="Telegram listener candidate secret",
        )
        if not normalized_secret:
            raise ValueError("Telegram listener candidate secret cannot be empty")
        normalized_reference = str(reference or "").strip().casefold()
        normalized_owner = str(owner_key or "").strip().casefold()
        if not re.fullmatch(r"[0-9a-f]{64}", normalized_reference):
            raise ValueError("Telegram listener candidate reference is invalid")
        if not re.fullmatch(r"[0-9a-f]{64}", normalized_owner):
            raise ValueError("Telegram listener candidate owner is invalid")
        if normalized_owner != str(prepared["key_hash"]):
            raise ValueError("Telegram listener candidate owner does not match command identity")
        ttl = float(ttl_seconds)
        if ttl <= 0:
            raise ValueError("Telegram listener candidate TTL must be positive")
        self._mutate_listener_action(
            prepared,
            secret_payload=normalized_secret,
            reference=normalized_reference,
            owner_key=normalized_owner,
            ttl_seconds=ttl,
        )
        return str(prepared["command_id"])

    def _prepare_once(
        self,
        command_type: str,
        payload: Mapping[str, Any] | None,
        *,
        idempotency_key: str,
        result_owner_key: str | None,
    ) -> dict[str, Any]:
        command_type = str(command_type or "").strip().casefold()
        if command_type not in _COMMAND_TYPES:
            raise ValueError(f"unsupported Telegram command: {command_type!r}")
        normalized_payload = normalize_json_object(dict(payload or {}), field="telegram_command.payload")
        normalized_key = str(idempotency_key or "").strip()
        if not normalized_key:
            raise ValueError("Telegram command idempotency_key is required")
        if len(normalized_key.encode("utf-8")) > 512:
            raise ValueError("Telegram command idempotency_key is too long")
        normalized_result_owner = str(result_owner_key or "").strip().casefold()
        if command_type == "search" and not re.fullmatch(r"[0-9a-f]{64}", normalized_result_owner):
            raise ValueError("replay-safe Telegram search requires a SHA256 result_owner_key")
        if command_type != "search" and normalized_result_owner:
            raise ValueError("Telegram result_owner_key is only supported for search commands")
        key_hash = hashlib.sha256(normalized_key.encode("utf-8")).hexdigest()
        request_hash = _canonical_hash(
            {
                "type": command_type,
                "payload": normalized_payload,
                "result_owner_key": normalized_result_owner,
            }
        )
        command_id = str(uuid5(NAMESPACE_URL, f"etk:telegram-command:{key_hash}"))
        now = self._now()
        return {
            "command_id": command_id,
            "command_type": command_type,
            "payload": normalized_payload,
            "key_hash": key_hash,
            "request_hash": request_hash,
            "result_owner_key": normalized_result_owner,
            "now": now,
        }

    def _reserve_once(self, state: dict[str, Any], prepared: Mapping[str, Any]) -> bool:
        command_id = str(prepared["command_id"])
        command_type = str(prepared["command_type"])
        key_hash = str(prepared["key_hash"])
        request_hash = str(prepared["request_hash"])
        now = prepared["now"]
        if not isinstance(now, datetime):
            raise TypeError("Telegram command timestamp is invalid")
        records = state.setdefault("idempotency", [])
        if not isinstance(records, list):
            raise ValueError("Telegram control bridge idempotency state is invalid")
        matching = next(
            (item for item in records if isinstance(item, Mapping) and str(item.get("key_hash") or "") == key_hash),
            None,
        )
        if matching is not None:
            if (
                str(matching.get("command_type") or "") != command_type
                or str(matching.get("request_hash") or "") != request_hash
                or str(matching.get("command_id") or "") != command_id
            ):
                raise TelegramCommandIdempotencyConflictError(
                    "Telegram command idempotency key was already used for another request"
                )
            return False
        existing_command = next(
            (
                item
                for item in state["commands"]
                if isinstance(item, Mapping) and str(item.get("id") or "") == command_id
            ),
            None,
        )
        if existing_command is not None and (
            str(existing_command.get("type") or "") != command_type
            or str(existing_command.get("request_hash") or "") != request_hash
        ):
            raise TelegramCommandIdempotencyConflictError(
                "Telegram command idempotency key was already used for another request"
            )
        if existing_command is None:
            state["commands"].append(
                {
                    "id": command_id,
                    "type": command_type,
                    "payload": prepared["payload"],
                    "status": "pending",
                    "created_at": now.isoformat(),
                    "claimed_at": None,
                    "claimed_by": "",
                    "lease_expires_at": None,
                    "attempt_count": 0,
                    "next_attempt_at": None,
                    "completed_at": None,
                    "result": {},
                    "error": "",
                    "idempotency_key_hash": key_hash,
                    "request_hash": request_hash,
                    "result_owner_key": str(prepared["result_owner_key"]),
                }
            )
        records.append(
            {
                "key_hash": key_hash,
                "command_id": command_id,
                "command_type": command_type,
                "request_hash": request_hash,
                "created_at": now.isoformat(),
            }
        )
        self._trim_terminal_commands(state)
        self._trim_idempotency_records(state)
        return True

    def claim_next(
        self,
        owner_id: str,
        *,
        lease_seconds: int = 60,
        command_types: Collection[str] | None = None,
    ) -> dict[str, Any] | None:
        owner_id = str(owner_id or "").strip()
        if not owner_id:
            raise ValueError("Telegram command owner_id is required")
        if lease_seconds <= 0:
            raise ValueError("Telegram command lease_seconds must be positive")
        allowed_types = None
        if command_types is not None:
            allowed_types = {str(value or "").strip().casefold() for value in command_types}
            if not allowed_types or not allowed_types <= _COMMAND_TYPES:
                raise ValueError("Telegram command type filter is invalid")
        now = self._now()
        claimed: dict[str, Any] | None = None

        def mutate(state: dict[str, Any]) -> bool:
            nonlocal claimed
            changed = False
            for command in state["commands"]:
                if allowed_types is not None and str(command.get("type") or "") not in allowed_types:
                    continue
                if command.get("status") not in {"claimed", "requesting"}:
                    continue
                if self._expired(command.get("lease_expires_at"), now):
                    if command.get("status") == "requesting":
                        command.update(
                            {
                                "status": "indeterminate",
                                "completed_at": now.isoformat(),
                                "lease_expires_at": None,
                                "payload": {},
                                "result": {},
                                "error": "Telegram control command outcome is indeterminate",
                            }
                        )
                    else:
                        command.update(
                            {
                                "status": "pending",
                                "claimed_at": None,
                                "claimed_by": "",
                                "lease_expires_at": None,
                            }
                        )
                    changed = True
            for command in state["commands"]:
                if allowed_types is not None and str(command.get("type") or "") not in allowed_types:
                    continue
                if command.get("status") != "pending":
                    continue
                next_attempt_at = command.get("next_attempt_at")
                if next_attempt_at and not self._expired(next_attempt_at, now):
                    continue
                try:
                    attempt_count = max(0, int(command.get("attempt_count") or 0))
                except (TypeError, ValueError):
                    attempt_count = 0
                command.update(
                    {
                        "status": "claimed",
                        "claimed_at": now.isoformat(),
                        "claimed_by": owner_id,
                        "lease_expires_at": (now + timedelta(seconds=lease_seconds)).isoformat(),
                        "attempt_count": attempt_count + 1,
                        "next_attempt_at": None,
                    }
                )
                claimed = normalize_json_object(dict(command), field="telegram_command")
                changed = True
                break
            return changed

        self._mutate(mutate)
        return claimed

    def checkpoint_request(self, command_id: str, owner_id: str) -> dict[str, Any]:
        normalized_id = str(command_id or "").strip()
        normalized_owner = str(owner_id or "").strip()
        checkpointed: dict[str, Any] | None = None

        def mutate(state: dict[str, Any]) -> None:
            nonlocal checkpointed
            for command in state["commands"]:
                if command.get("id") != normalized_id:
                    continue
                if command.get("status") != "claimed" or command.get("claimed_by") != normalized_owner:
                    raise RuntimeError("Telegram command is not claimed by this owner")
                command["status"] = "requesting"
                command["provider_request_started_at"] = self._now().isoformat()
                checkpointed = normalize_json_object(dict(command), field="telegram_command")
                return
            raise LookupError(f"Telegram command {normalized_id!r} was not found")

        self._mutate(mutate)
        if checkpointed is None:
            raise RuntimeError("Telegram command request checkpoint was not persisted")
        return checkpointed

    def complete(self, command_id: str, owner_id: str, result: Mapping[str, Any] | None = None) -> dict[str, Any]:
        normalized_result = normalize_json_object(dict(result or {}), field="telegram_command.result")
        return self._finish(command_id, owner_id, status="succeeded", result=normalized_result, error="")

    def fail(self, command_id: str, owner_id: str, error: str) -> dict[str, Any]:
        message = str(error or "Telegram command failed")[:2000]
        return self._finish(command_id, owner_id, status="failed", result={}, error=message)

    def release(self, command_id: str, owner_id: str, error_class: str) -> dict[str, Any]:
        normalized_id = str(command_id or "").strip()
        normalized_owner = str(owner_id or "").strip()
        released: dict[str, Any] | None = None
        terminal_secret: tuple[str, str] | None = None
        now = self._now()

        def mutate(state: dict[str, Any]) -> None:
            nonlocal released, terminal_secret
            for command in state["commands"]:
                if command.get("id") != normalized_id:
                    continue
                if command.get("status") != "claimed" or command.get("claimed_by") != normalized_owner:
                    raise RuntimeError("Telegram command is not claimed by this owner")
                try:
                    attempt_count = max(1, int(command.get("attempt_count") or 1))
                except (TypeError, ValueError):
                    attempt_count = 1
                if command.get("type") == "listener_action" and attempt_count >= _LISTENER_ACTION_MAX_ATTEMPTS:
                    payload = command.get("payload")
                    payload = payload if isinstance(payload, Mapping) else {}
                    reference = str(payload.get("candidate_ref") or "").strip().casefold()
                    owner_key = str(command.get("idempotency_key_hash") or "").strip().casefold()
                    if re.fullmatch(r"[0-9a-f]{64}", reference) and re.fullmatch(r"[0-9a-f]{64}", owner_key):
                        terminal_secret = (reference, owner_key)
                    command.update(
                        {
                            "status": "failed",
                            "claimed_at": None,
                            "claimed_by": "",
                            "lease_expires_at": None,
                            "next_attempt_at": None,
                            "completed_at": now.isoformat(),
                            "payload": {},
                            "result": {},
                            "error": (
                                f"retry_exhausted:{str(error_class or 'Exception')[:200]}:attempts={attempt_count}"
                            ),
                        }
                    )
                    self._trim_terminal_commands(state)
                else:
                    delay_seconds = min(
                        _LISTENER_ACTION_MAX_RETRY_SECONDS,
                        2 ** min(max(attempt_count - 1, 0), 8),
                    )
                    command.update(
                        {
                            "status": "pending",
                            "claimed_at": None,
                            "claimed_by": "",
                            "lease_expires_at": None,
                            "next_attempt_at": (now + timedelta(seconds=delay_seconds)).isoformat()
                            if command.get("type") == "listener_action"
                            else None,
                            "error": f"retryable:{str(error_class or 'Exception')[:200]}",
                        }
                    )
                released = normalize_json_object(dict(command), field="telegram_command")
                return
            raise LookupError(f"Telegram command {normalized_id!r} was not found")

        self._mutate(mutate)
        if released is None:
            raise RuntimeError("Telegram command release was not persisted")
        if terminal_secret is not None:
            try:
                self.destroy_resource_secret(terminal_secret[0], owner_key=terminal_secret[1])
            except Exception as exc:
                logger.warning(
                    "Telegram listener terminal secret cleanup failed: %s",
                    type(exc).__name__,
                )
        return released

    def indeterminate(self, command_id: str, owner_id: str, error_class: str) -> dict[str, Any]:
        normalized_class = str(error_class or "Exception").strip()[:200]
        return self._finish(
            command_id,
            owner_id,
            status="indeterminate",
            result={},
            error=f"Telegram control command outcome is indeterminate ({normalized_class})",
        )

    def get_command(self, command_id: str) -> dict[str, Any] | None:
        state = self._read()
        for command in state["commands"]:
            if command.get("id") == command_id:
                return normalize_json_object(dict(command), field="telegram_command")
        return None

    def consume_resource_secret(self, reference: str, owner_key: str) -> dict[str, Any]:
        record = self._resource_secrets.consume(reference, owner_key)
        return normalize_json_object(dict(record.payload), field="telegram resource secret")

    def create_resource_secret(
        self,
        payload: Mapping[str, Any],
        *,
        reference: str,
        owner_key: str,
        ttl_seconds: float,
    ) -> tuple[str, bool]:
        return self._resource_secrets.create(
            payload,
            reference=reference,
            owner_key=owner_key,
            ttl_seconds=ttl_seconds,
        )

    def destroy_resource_secret(self, reference: str, *, owner_key: str | None = None) -> bool:
        return self._resource_secrets.destroy(reference, owner_key)

    def wait(self, command_id: str, *, timeout_seconds: float = 45, poll_seconds: float = 0.1) -> dict[str, Any]:
        if timeout_seconds <= 0 or poll_seconds <= 0:
            raise ValueError("Telegram command wait durations must be positive")
        deadline = time.monotonic() + timeout_seconds
        while True:
            command = self.get_command(command_id)
            if command is None:
                raise LookupError(f"Telegram command {command_id!r} was not found")
            status = command.get("status")
            if status == "succeeded":
                return command
            if status == "failed":
                raise TelegramCommandFailedError(str(command.get("error") or "Telegram command failed"))
            if status == "indeterminate":
                raise TelegramCommandIndeterminateError(
                    str(command.get("error") or "Telegram command outcome is indeterminate")
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TelegramCommandTimeoutError(f"Telegram command {command_id!r} timed out")
            time.sleep(min(poll_seconds, remaining))

    def get_runtime_status(self) -> dict[str, Any]:
        state = self._read()
        return normalize_json_object(dict(state["runtime_status"]), field="telegram_runtime_status")

    def update_runtime_status(self, status: Mapping[str, Any]) -> dict[str, Any]:
        normalized = normalize_json_object(dict(status), field="telegram_runtime_status")
        normalized["updated_at"] = self._now().isoformat()

        def mutate(state: dict[str, Any]) -> None:
            state["runtime_status"] = normalized

        self._mutate(mutate)
        return normalized

    def _finish(
        self,
        command_id: str,
        owner_id: str,
        *,
        status: str,
        result: dict[str, Any],
        error: str,
    ) -> dict[str, Any]:
        owner_id = str(owner_id or "").strip()
        completed: dict[str, Any] | None = None

        def mutate(state: dict[str, Any]) -> None:
            nonlocal completed
            for command in state["commands"]:
                if command.get("id") != command_id:
                    continue
                if command.get("status") not in {"claimed", "requesting"} or command.get("claimed_by") != owner_id:
                    raise RuntimeError("Telegram command is not claimed by this owner")
                safe_result = result
                if status == "succeeded" and command.get("type") == "search" and command.get("idempotency_key_hash"):
                    safe_result = _externalize_auto_search_result(
                        result,
                        command_id=command_id,
                        owner_key=str(command.get("result_owner_key") or ""),
                        secrets_store=self._resource_secrets,
                    )
                command.update(
                    {
                        "status": status,
                        "completed_at": self._now().isoformat(),
                        "lease_expires_at": None,
                        "payload": {},
                        "result": safe_result,
                        "error": error,
                    }
                )
                completed = normalize_json_object(dict(command), field="telegram_command")
                self._trim_terminal_commands(state)
                return
            raise LookupError(f"Telegram command {command_id!r} was not found")

        self._mutate(mutate)
        if completed is None:
            raise RuntimeError("Telegram command completion was not persisted")
        return completed

    def _read(self) -> dict[str, Any]:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT value_json FROM app_settings WHERE setting_key = %s", (_SETTING_KEY,))
                row = cursor.fetchone()
        return self._decode_state(self._row_value(row) if row else None)

    def _mutate(self, mutation: Callable[[dict[str, Any]], bool | None]) -> None:
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                initial = json.dumps(self._initial_state(), ensure_ascii=False, separators=(",", ":"))
                cursor.execute(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES (%s, %s::jsonb, NOW())
                    ON CONFLICT (setting_key) DO NOTHING
                    """,
                    (_SETTING_KEY, initial),
                )
                cursor.execute(
                    "SELECT value_json FROM app_settings WHERE setting_key = %s FOR UPDATE",
                    (_SETTING_KEY,),
                )
                row = cursor.fetchone()
                state = self._decode_state(self._row_value(row) if row else None)
                changed = mutation(state)
                if changed is False:
                    connection.commit()
                    return
                state["revision"] = int(state.get("revision") or 0) + 1
                normalized = normalize_json_object(state, field="telegram_bridge_state")
                cursor.execute(
                    """
                    UPDATE app_settings
                    SET value_json = %s::jsonb, last_updated_at = NOW()
                    WHERE setting_key = %s
                    """,
                    (json.dumps(normalized, ensure_ascii=False, separators=(",", ":")), _SETTING_KEY),
                )
            connection.commit()

    def _mutate_listener_action(
        self,
        prepared: Mapping[str, Any],
        *,
        secret_payload: Mapping[str, Any],
        reference: str,
        owner_key: str,
        ttl_seconds: float,
    ) -> None:
        secret_setting_key = f"runtime.secret.telegram-resource.{reference}"
        secret_hash = _canonical_hash(secret_payload)
        now = prepared.get("now")
        if not isinstance(now, datetime):
            raise TypeError("Telegram listener command timestamp is invalid")
        with self._connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (secret_setting_key,),
                )
                initial = json.dumps(self._initial_state(), ensure_ascii=False, separators=(",", ":"))
                cursor.execute(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES (%s, %s::jsonb, NOW())
                    ON CONFLICT (setting_key) DO NOTHING
                    """,
                    (_SETTING_KEY, initial),
                )
                cursor.execute(
                    "SELECT value_json FROM app_settings WHERE setting_key = %s FOR UPDATE",
                    (_SETTING_KEY,),
                )
                row = cursor.fetchone()
                state = self._decode_state(self._row_value(row) if row else None)
                changed = self._reserve_once(state, prepared)
                command = next(
                    (
                        item
                        for item in state["commands"]
                        if isinstance(item, Mapping) and str(item.get("id") or "") == str(prepared["command_id"])
                    ),
                    None,
                )
                needs_secret = command is not None and str(command.get("status") or "") not in _TERMINAL_STATES

                cursor.execute(
                    "SELECT value_json FROM app_settings WHERE setting_key = %s FOR UPDATE",
                    (secret_setting_key,),
                )
                secret_row = cursor.fetchone()
                raw_secret = self._row_value(secret_row) if secret_row else None
                if isinstance(raw_secret, str):
                    raw_secret = json.loads(raw_secret)
                if raw_secret is not None and type(raw_secret) is not dict:
                    raise ValueError("Telegram listener candidate secret state is invalid")
                current_secret = (
                    normalize_json_object(raw_secret, field="Telegram listener candidate secret state")
                    if isinstance(raw_secret, dict)
                    else None
                )
                if current_secret is not None and self._expired(current_secret.get("expires_at"), now):
                    cursor.execute(
                        "DELETE FROM app_settings WHERE setting_key = %s",
                        (secret_setting_key,),
                    )
                    current_secret = None
                if current_secret is not None:
                    if (
                        current_secret.get("version") != 1
                        or str(current_secret.get("payload_hash") or "") != secret_hash
                    ):
                        raise RuntimeError("Telegram listener candidate reference was reused with different content")
                    if str(current_secret.get("consumed_by") or "") not in {"", owner_key}:
                        raise tg_resource_effects.TelegramResourceSecretOwnershipError(
                            "Telegram listener candidate belongs to another operation"
                        )

                if needs_secret and current_secret is None:
                    secret_state = {
                        "version": 1,
                        "payload": dict(secret_payload),
                        "payload_hash": secret_hash,
                        "created_at": now.isoformat(),
                        "expires_at": (now + timedelta(seconds=ttl_seconds)).isoformat(),
                        "consumed_by": owner_key,
                    }
                    cursor.execute(
                        """
                        INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                        VALUES (%s, %s::jsonb, NOW())
                        ON CONFLICT (setting_key) DO UPDATE SET
                            value_json = EXCLUDED.value_json,
                            last_updated_at = NOW()
                        """,
                        (
                            secret_setting_key,
                            json.dumps(secret_state, ensure_ascii=False, separators=(",", ":")),
                        ),
                    )
                elif not needs_secret and current_secret is not None:
                    cursor.execute(
                        "DELETE FROM app_settings WHERE setting_key = %s",
                        (secret_setting_key,),
                    )

                if changed is not False:
                    state["revision"] = int(state.get("revision") or 0) + 1
                    normalized = normalize_json_object(state, field="telegram_bridge_state")
                    cursor.execute(
                        """
                        UPDATE app_settings
                        SET value_json = %s::jsonb, last_updated_at = NOW()
                        WHERE setting_key = %s
                        """,
                        (json.dumps(normalized, ensure_ascii=False, separators=(",", ":")), _SETTING_KEY),
                    )
            connection.commit()

    @staticmethod
    def _row_value(row: Any) -> Any:
        if isinstance(row, Mapping):
            return row.get("value_json")
        return row[0]

    @staticmethod
    def _initial_state() -> dict[str, Any]:
        return {
            "version": _STATE_VERSION,
            "revision": 0,
            "commands": [],
            "idempotency": [],
            "runtime_status": {"status": "stopped", "msg": "Coordinator 尚未上报状态", "updated_at": None},
        }

    @classmethod
    def _decode_state(cls, raw: Any) -> dict[str, Any]:
        if raw is None:
            return cls._initial_state()
        if isinstance(raw, str):
            raw = json.loads(raw)
        if type(raw) is not dict or raw.get("version") != _STATE_VERSION:
            raise ValueError("Telegram control bridge state is invalid or unsupported")
        state = normalize_json_object(raw, field="telegram_bridge_state")
        state.setdefault("idempotency", [])
        if (
            not isinstance(state.get("commands"), list)
            or not isinstance(state.get("idempotency"), list)
            or not isinstance(state.get("runtime_status"), dict)
        ):
            raise ValueError("Telegram control bridge state has an invalid shape")
        return state

    def _now(self) -> datetime:
        value = self._clock()
        return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)

    @staticmethod
    def _expired(value: Any, now: datetime) -> bool:
        if not value:
            return True
        try:
            parsed = datetime.fromisoformat(str(value))
            parsed = parsed.astimezone(UTC) if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
        except ValueError:
            return True
        return parsed <= now

    @staticmethod
    def _trim_terminal_commands(state: dict[str, Any]) -> None:
        commands = state["commands"]
        terminal_indexes = [index for index, item in enumerate(commands) if item.get("status") in _TERMINAL_STATES]
        remove_count = max(0, len(terminal_indexes) - _MAX_TERMINAL_COMMANDS)
        if remove_count:
            removed = set(terminal_indexes[:remove_count])
            state["commands"] = [item for index, item in enumerate(commands) if index not in removed]

    @staticmethod
    def _trim_idempotency_records(state: dict[str, Any]) -> None:
        records = state["idempotency"]
        remove_count = max(0, len(records) - _MAX_IDEMPOTENCY_RECORDS)
        if not remove_count:
            return
        active_ids = {
            str(item.get("id") or "")
            for item in state["commands"]
            if isinstance(item, Mapping) and item.get("status") not in _TERMINAL_STATES
        }
        retained: list[Any] = []
        for item in records:
            command_id = str(item.get("command_id") or "") if isinstance(item, Mapping) else ""
            if remove_count and command_id not in active_ids:
                remove_count -= 1
                continue
            retained.append(item)
        state["idempotency"] = retained


class TelegramCommandExecutor:
    """Execute persisted commands through a Coordinator-owned manager port."""

    def __init__(
        self,
        bridge: TelegramControlBridge,
        *,
        owner_id: str,
        lease_seconds: int = 60,
        command_types: Collection[str] | None = None,
    ) -> None:
        self._bridge = bridge
        self._owner_id = owner_id
        self._lease_seconds = lease_seconds
        self._command_types = tuple(command_types) if command_types is not None else None

    def process_pending(self, manager: Any, *, limit: int = 8) -> int:
        processed = 0
        for _ in range(max(0, limit)):
            if self._command_types is None:
                command = self._bridge.claim_next(self._owner_id, lease_seconds=self._lease_seconds)
            else:
                command = self._bridge.claim_next(
                    self._owner_id,
                    lease_seconds=self._lease_seconds,
                    command_types=self._command_types,
                )
            if command is None:
                break
            active_command = command
            command_id = str(active_command["id"])
            checkpointed = False

            def checkpoint(command_id: str = command_id) -> None:
                nonlocal active_command, checkpointed
                try:
                    active_command = self._bridge.checkpoint_request(command_id, self._owner_id)
                except Exception:
                    checkpointed = True
                    try:
                        durable = self._bridge.get_command(command_id)
                    except Exception:
                        pass
                    else:
                        if durable is not None:
                            active_command = durable
                    raise
                else:
                    checkpointed = True

            try:
                result = self._execute(manager, active_command, checkpoint=checkpoint)
            except Exception as exc:
                if active_command.get("type") == "listener_action" and not checkpointed:
                    self._bridge.release(command_id, self._owner_id, type(exc).__name__)
                elif checkpointed:
                    self._bridge.indeterminate(command_id, self._owner_id, type(exc).__name__)
                else:
                    self._bridge.fail(command_id, self._owner_id, str(exc))
            else:
                self._bridge.complete(command_id, self._owner_id, result)
            processed += 1
        self.refresh_status(manager)
        return processed

    def refresh_status(self, manager: Any) -> dict[str, Any]:
        try:
            status = manager.get_status()
            if not isinstance(status, Mapping):
                raise TypeError("TGUserBotManager.get_status() must return an object")
            return self._bridge.update_runtime_status(dict(status))
        except Exception as exc:
            return self._bridge.update_runtime_status({"status": "error", "msg": str(exc)[:2000]})

    def _execute(
        self,
        manager: Any,
        command: Mapping[str, Any],
        *,
        checkpoint: Callable[[], None],
    ) -> dict[str, Any]:
        command_type = command.get("type")
        payload = command.get("payload")
        if not isinstance(payload, Mapping):
            raise TypeError("Telegram command payload must be an object")
        if command_type == "send_code":
            checkpoint()
            manager.send_login_code()
            return {"success": True}
        if command_type == "login":
            code = str(payload.get("code") or "")
            reference = str(payload.get("code_ref") or "").strip()
            owner_key = ""
            if reference:
                owner_key = str(command.get("idempotency_key_hash") or "").strip().casefold()
            try:
                if reference:
                    secret = self._bridge.consume_resource_secret(reference, owner_key)
                    expected_hash = str(payload.get("code_hash") or "").strip().casefold()
                    if _canonical_hash(secret) != expected_hash:
                        raise ValueError("Telegram login code secret changed")
                    code = str(secret.get("code") or "")
                checkpoint()
                result = manager.submit_login_code(code)
            finally:
                if reference:
                    self._bridge.destroy_resource_secret(reference, owner_key=owner_key)
        elif command_type == "logout":
            checkpoint()
            manager.logout()
            result = {"success": True}
        elif command_type == "clear_session":
            checkpoint()
            result = manager.clear_session_files()
        elif command_type == "search":
            result = manager.search_channel_resources(**dict(payload))
        elif command_type == "listener_action":
            reference = str(payload.get("candidate_ref") or "").strip().casefold()
            expected_hash = str(payload.get("candidate_hash") or "").strip().casefold()
            owner_key = str(command.get("idempotency_key_hash") or "").strip().casefold()
            if not re.fullmatch(r"[0-9a-f]{64}", reference) or not re.fullmatch(r"[0-9a-f]{64}", expected_hash):
                raise ValueError("Telegram listener action reference is invalid")
            secret = self._bridge.consume_resource_secret(reference, owner_key)
            candidate = secret.get("candidate")
            if not isinstance(candidate, Mapping) or _canonical_hash(candidate) != expected_hash:
                raise ValueError("Telegram listener action candidate changed")
            result = manager.stage_listener_action(
                dict(candidate),
                occurrence=str(payload.get("occurrence") or ""),
                is_brainless=bool(payload.get("is_brainless")),
                is_keyword_matched=bool(payload.get("is_keyword_matched")),
                is_subscribe=bool(payload.get("is_subscribe")),
                is_hdhive_push=bool(payload.get("is_hdhive_push")),
            )
            self._bridge.destroy_resource_secret(reference, owner_key=owner_key)
        else:
            raise ValueError(f"unsupported Telegram command: {command_type!r}")
        if not isinstance(result, Mapping):
            raise TypeError(f"Telegram command {command_type!r} returned a non-object result")
        return normalize_json_object(dict(result), field="telegram_command.result")


def submit_persistent_legacy_task(
    task_function: Callable[..., Any],
    *,
    task_name: str,
    processor_type: str = "media",
    kwargs: Mapping[str, Any] | None = None,
    silent: bool = False,
    max_attempts: int = 1,
    idempotency_key: str | None = None,
    idempotency_scope: str | None = None,
) -> bool:
    """Submit through the installed persistent gateway without a process-local fallback."""

    normalized_kwargs = normalize_json_object(dict(kwargs or {}), field="persistent_task.parameters")
    import task_manager

    lock = task_manager._persistent_gateway_lock
    with lock:
        gateway = getattr(task_manager, "_persistent_gateway", None)
        if gateway is None:
            role = os.environ.get("ETK_PROCESS_ROLE", "unknown")
            raise PersistentTaskGatewayUnavailableError(
                f"persistent task gateway is unavailable for process role {role!r}"
            )
        accepted = bool(
            gateway.submit(
                task_function,
                task_name,
                processor_type,
                (),
                normalized_kwargs,
                silent=silent,
                max_attempts=max_attempts,
                idempotency_key=idempotency_key,
                idempotency_scope=idempotency_scope,
            )
        )
    if not accepted:
        raise RuntimeError(f"persistent task submission was rejected: {task_name}")
    return True


def normalize_telegram_resource_action(action: Mapping[str, Any]) -> dict[str, Any]:
    """Freeze one credential-free resource action for persistent Worker execution."""

    if not isinstance(action, Mapping):
        raise TypeError("Telegram resource action must be an object")
    action_name = str(action.get("action") or "").strip()
    if action_name not in _RESOURCE_ACTIONS:
        raise ValueError(f"unsupported Telegram resource action: {action_name!r}")
    try:
        version = int(str(action.get("version")))
    except (TypeError, ValueError) as exc:
        raise ValueError("Telegram resource action version is invalid") from exc
    if version != _RESOURCE_ACTION_VERSION:
        raise ValueError(f"unsupported Telegram resource action version: {version}")

    chat_id = str(action.get("chat_id") or "").strip()
    update_id = str(action.get("update_id") or "").strip()
    source_identity = str(action.get("source_identity") or "").strip()
    payload = action.get("payload")
    if not chat_id or not update_id or not source_identity:
        raise ValueError("Telegram resource action requires chat_id, update_id, and source_identity")
    if _RESOURCE_LINK_PATTERN.search(source_identity):
        raise ValueError("Telegram resource action source_identity cannot persist a raw resource link")
    if not isinstance(payload, Mapping):
        raise TypeError("Telegram resource action payload must be an object")
    _reject_resource_action_runtime_state(payload, path="payload")
    if action_name in _RESOURCE_SECRET_ACTIONS:
        if any(key in payload for key in ("share_code", "receive_code", "url", "urls")):
            raise ValueError("Telegram resource action payload must use an opaque secret_ref")
        secret_ref = str(payload.get("secret_ref") or "").strip().casefold()
        semantic_source_hash = str(payload.get("semantic_source_hash") or "").strip().casefold()
        if not re.fullmatch(r"[0-9a-f]{64}", secret_ref) or not re.fullmatch(r"[0-9a-f]{64}", semantic_source_hash):
            raise ValueError("Telegram resource action requires secret_ref and semantic_source_hash")
    return normalize_json_object(
        {
            "version": version,
            "action": action_name,
            "chat_id": chat_id,
            "update_id": update_id,
            "source_identity": source_identity,
            "payload": dict(payload),
        },
        field="telegram_resource_action",
    )


def submit_telegram_resource_action(action: Mapping[str, Any]) -> bool:
    """Submit one strict-JSON Telegram resource action without a local fallback."""

    action_key = telegram_resource_action_key(action)
    normalized, secret_created = _externalize_resource_action(action, action_key)
    from etk.compatibility.tg_resource_operations import task_execute_telegram_resource_action

    try:
        return submit_persistent_legacy_task(
            task_execute_telegram_resource_action,
            task_name=_RESOURCE_ACTION_TASK_NAMES[normalized["action"]],
            kwargs={"action": normalized},
            max_attempts=6 if normalized["action"] in _RESOURCE_SECRET_ACTIONS else 3,
            idempotency_key=action_key,
            idempotency_scope=_RESOURCE_ACTION_SCOPE,
        )
    except Exception:
        if secret_created:
            tg_resource_effects.destroy_secret(action_key, action_key)
        raise


def telegram_resource_action_key(action: Mapping[str, Any]) -> str:
    if not isinstance(action, Mapping):
        raise TypeError("Telegram resource action must be an object")
    identity = {
        "version": int(str(action.get("version") or 0)),
        "action": str(action.get("action") or "").strip(),
        "chat_id": str(action.get("chat_id") or "").strip(),
        "update_id": str(action.get("update_id") or "").strip(),
        "source_identity": str(action.get("source_identity") or "").strip(),
    }
    if identity["version"] != _RESOURCE_ACTION_VERSION or identity["action"] not in _RESOURCE_ACTIONS:
        raise ValueError("Telegram resource delivery identity is invalid")
    if not identity["chat_id"] or not identity["update_id"] or not identity["source_identity"]:
        raise ValueError("Telegram resource delivery identity is incomplete")
    if _RESOURCE_LINK_PATTERN.search(identity["source_identity"]):
        raise ValueError("Telegram resource delivery identity cannot include a raw resource link")
    encoded = json.dumps(identity, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _externalize_resource_action(
    action: Mapping[str, Any],
    delivery_key: str,
) -> tuple[dict[str, Any], bool]:
    action_name = str(action.get("action") or "").strip()
    payload = action.get("payload")
    if not isinstance(payload, Mapping):
        raise TypeError("Telegram resource action payload must be an object")
    safe_action = dict(action)
    safe_payload = dict(payload)
    secret_created = False
    if action_name == "p115_share_import" and "secret_ref" not in safe_payload:
        share_code = str(safe_payload.pop("share_code", "") or "").strip()
        receive_code = str(safe_payload.pop("receive_code", "") or "").strip()
        if not share_code:
            raise ValueError("Telegram 115 share action requires share_code")
        secret_payload = {"share_code": share_code, "receive_code": receive_code}
        semantic_hash = _canonical_hash(secret_payload)
        secret_ref = delivery_key
        if not _delivery_already_bound(delivery_key, action_name, semantic_hash):
            secret_ref, secret_created = tg_resource_effects.create_secret(
                secret_payload,
                reference=delivery_key,
                owner_key=delivery_key,
            )
        safe_payload.update({"secret_ref": secret_ref, "semantic_source_hash": semantic_hash})
    elif action_name == "p115_offline_add" and "secret_ref" not in safe_payload:
        urls = _offline_urls(safe_payload.pop("urls", None) or safe_payload.pop("url", None))
        if not urls:
            raise ValueError("Telegram offline action requires a magnet or ED2K URL")
        secret_payload = {"urls": urls}
        semantic_hash = _canonical_hash(secret_payload)
        secret_ref = delivery_key
        if not _delivery_already_bound(delivery_key, action_name, semantic_hash):
            secret_ref, secret_created = tg_resource_effects.create_secret(
                secret_payload,
                reference=delivery_key,
                owner_key=delivery_key,
            )
        safe_payload.update({"secret_ref": secret_ref, "semantic_source_hash": semantic_hash})
    safe_payload.setdefault("delivery_key", delivery_key)
    safe_action["payload"] = safe_payload
    return normalize_telegram_resource_action(safe_action), secret_created


def _offline_urls(value: Any) -> list[str]:
    values = value if isinstance(value, (list, tuple)) else [value]
    urls: list[str] = []
    for item in values:
        text = str(item or "").strip()
        if not text.startswith(("magnet:?", "ed2k://")):
            continue
        if text not in urls:
            urls.append(text)
    return urls


def _canonical_hash(value: Mapping[str, Any]) -> str:
    normalized = normalize_json_object(dict(value), field="Telegram resource semantic identity")
    encoded = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _externalize_auto_search_result(
    result: Mapping[str, Any],
    *,
    command_id: str,
    owner_key: str,
    secrets_store: tg_resource_effects.TelegramResourceSecretStore,
) -> dict[str, Any]:
    normalized_owner = str(owner_key or "").strip().casefold()
    if not re.fullmatch(r"[0-9a-f]{64}", normalized_owner):
        raise ValueError("Telegram auto-subscribe search result owner is invalid")
    normalized = normalize_json_object(dict(result), field="telegram auto-subscribe search result")
    if normalized.get("ok") is not True:
        return {"ok": False, "error_type": "telegram_search_rejected"}
    encoded = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    if len(encoded) > _MAX_AUTO_SEARCH_RESULT_BYTES:
        raise ValueError("Telegram auto-subscribe search result exceeds the durable secret limit")
    result_hash = _canonical_hash(normalized)
    reference = _canonical_hash(
        {
            "kind": "auto-subscribe-telegram-search-result",
            "command_id": command_id,
        }
    )
    secrets_store.create(
        normalized,
        reference=reference,
        owner_key=normalized_owner,
        ttl_seconds=_AUTO_SEARCH_SECRET_TTL_SECONDS,
    )
    return {
        "ok": True,
        "search_result_ref": reference,
        "search_result_hash": result_hash,
        "candidate_count": _candidate_count(normalized),
    }


def _candidate_count(result: Mapping[str, Any]) -> int:
    queue: list[Any] = [result]
    seen: set[int] = set()
    while queue:
        value = queue.pop(0)
        if id(value) in seen:
            continue
        seen.add(id(value))
        if isinstance(value, (list, tuple)):
            rows = [item for item in value if isinstance(item, Mapping)]
            if rows:
                return len(rows)
        if isinstance(value, Mapping):
            for key in ("results", "items", "data"):
                nested = value.get(key)
                if isinstance(nested, (Mapping, list, tuple)):
                    queue.append(nested)
    return 0


def _delivery_already_bound(delivery_key: str, action: str, semantic_source_hash: str) -> bool:
    binding = tg_resource_effects.read_effect_binding(delivery_key)
    if binding is None:
        return False
    if binding.action != action or binding.semantic_source_hash != semantic_source_hash:
        raise RuntimeError("Telegram resource delivery changed semantic effect identity")
    return True


def _reject_resource_action_runtime_state(value: Any, *, path: str) -> None:
    if isinstance(value, Mapping):
        for raw_key, nested in value.items():
            key = str(raw_key).strip().casefold()
            if key in _FORBIDDEN_RESOURCE_ACTION_KEYS:
                raise ValueError(f"Telegram resource action cannot persist {path}.{raw_key}")
            _reject_resource_action_runtime_state(nested, path=f"{path}.{raw_key}")
    elif isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            _reject_resource_action_runtime_state(nested, path=f"{path}[{index}]")
    elif isinstance(value, str) and _RESOURCE_LINK_PATTERN.search(value):
        raise ValueError(f"Telegram resource action cannot persist raw resource link at {path}")


class DurableTelegramTaskQueue:
    """Compatibility ``put`` facade that persists every TG transfer as a Worker Job."""

    def put(self, payload: Mapping[str, Any]) -> bool:
        normalized_input = normalize_json_object(dict(payload), field="telegram_transfer_payload")
        delivery_key = _channel_delivery_key(normalized_input)
        normalized, secret_created = _externalize_channel_payload(normalized_input, delivery_key)
        from tasks.subscriptions import task_process_channel_transfer

        try:
            return submit_persistent_legacy_task(
                task_process_channel_transfer,
                task_name="TG频道资源转存",
                kwargs={"payload": normalized},
                max_attempts=3,
                idempotency_key=delivery_key,
                idempotency_scope=_CHANNEL_DELIVERY_SCOPE,
            )
        except Exception:
            if secret_created:
                tg_resource_effects.destroy_secret(delivery_key, delivery_key)
            raise


def _channel_delivery_key(payload: Mapping[str, Any]) -> str:
    candidate = payload.get("candidate")
    candidate = candidate if isinstance(candidate, Mapping) else {}
    source_chat_id = str(candidate.get("source_chat_id") or payload.get("source_chat_id") or "").strip()
    message_id = str(candidate.get("message_id") or payload.get("message_id") or "").strip()
    if source_chat_id and message_id:
        identity = {"source_chat_id": source_chat_id, "message_id": message_id}
    else:
        semantic = _channel_secret_payload(payload)[0]
        identity = {
            "semantic_source_hash": _canonical_hash(semantic),
            "five_minute_bucket": int(time.time() // 300),
        }
    return _canonical_hash({"kind": "telegram-channel-delivery", **identity})


def _externalize_channel_payload(
    payload: Mapping[str, Any],
    delivery_key: str,
) -> tuple[dict[str, Any], bool]:
    secret_payload, resource_kind, safe_fields = _channel_secret_payload(payload)
    safe_payload = _scrub_channel_payload(payload)
    safe_payload.update(safe_fields)
    safe_payload["delivery_key"] = delivery_key
    safe_payload["resource_kind"] = resource_kind
    semantic_hash = _canonical_hash(secret_payload)
    safe_payload["semantic_source_hash"] = semantic_hash
    secret_created = False
    if secret_payload and resource_kind != "hdhive":
        effect_action = {
            "p115_share": "p115_share_import",
            "p115_offline": "p115_offline_add",
        }[resource_kind]
        secret_ref = delivery_key
        if not _delivery_already_bound(delivery_key, effect_action, semantic_hash):
            secret_ref, secret_created = tg_resource_effects.create_secret(
                secret_payload,
                reference=delivery_key,
                owner_key=delivery_key,
            )
        safe_payload["secret_ref"] = secret_ref
    _reject_resource_action_runtime_state(safe_payload, path="telegram_transfer_payload")
    return normalize_json_object(safe_payload, field="telegram_transfer_payload"), secret_created


def _channel_secret_payload(payload: Mapping[str, Any]) -> tuple[dict[str, Any], str, dict[str, Any]]:
    candidate = payload.get("candidate")
    candidate = candidate if isinstance(candidate, Mapping) else {}
    task_type = str(payload.get("type") or "").strip()
    target_link = str(payload.get("target_link") or candidate.get("target_link") or "").strip()
    magnet_url = str(payload.get("magnet_url") or payload.get("url") or candidate.get("magnet_url") or "").strip()
    receive_code = str(payload.get("receive_code") or candidate.get("receive_code") or "").strip()
    if task_type == "offline_download" or magnet_url.startswith(("magnet:?", "ed2k://")):
        urls = _offline_urls(magnet_url)
        if not urls:
            raise ValueError("Telegram channel offline transfer requires a magnet or ED2K URL")
        return {"urls": urls}, "p115_offline", {}
    if "hdhive.com/resource/" in target_link.casefold():
        match = re.search(
            r"(?i)hdhive\.com/resource/(?:115|magnet|ed2k|bt)?/?([a-f0-9-]{32,36})",
            target_link,
        )
        if not match:
            raise ValueError("Telegram channel HDHive resource is missing a valid slug")
        slug = match.group(1)
        return {"slug": slug}, "hdhive", {"slug": slug}
    share_match = re.search(r"(?i)115(?:cdn)?\.com/s/([a-z0-9]+)", target_link)
    if share_match:
        return (
            {
                "share_code": share_match.group(1),
                "receive_code": receive_code,
            },
            "p115_share",
            {},
        )
    raise ValueError("Telegram channel transfer has no supported resource source")


def _scrub_channel_payload(value: Mapping[str, Any]) -> dict[str, Any]:
    def scrub(item: Any) -> Any:
        if isinstance(item, Mapping):
            result: dict[str, Any] = {}
            for raw_key, nested in item.items():
                key = str(raw_key)
                if key.strip().casefold() in _CHANNEL_SECRET_KEYS:
                    continue
                result[key] = scrub(nested)
            return result
        if isinstance(item, list):
            return [scrub(nested) for nested in item]
        if isinstance(item, tuple):
            return [scrub(nested) for nested in item]
        if isinstance(item, str) and _RESOURCE_LINK_PATTERN.search(item):
            return "[resource-redacted]"
        return item

    return normalize_json_object(scrub(value), field="Telegram channel safe payload")


__all__ = [
    "DurableTelegramTaskQueue",
    "PersistentTaskGatewayUnavailableError",
    "TelegramCommandExecutor",
    "TelegramCommandFailedError",
    "TelegramCommandIdempotencyConflictError",
    "TelegramCommandTimeoutError",
    "TelegramControlBridge",
    "normalize_telegram_resource_action",
    "submit_persistent_legacy_task",
    "submit_telegram_resource_action",
    "telegram_resource_action_key",
]
