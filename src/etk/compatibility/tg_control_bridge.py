from __future__ import annotations

import hashlib
import json
import os
import re
import time
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from etk.compatibility import tg_resource_effects
from etk.platform.jobs import normalize_json_object

_SETTING_KEY = "tg_userbot_control_bridge"
_STATE_VERSION = 1
_TERMINAL_STATES = frozenset({"succeeded", "failed"})
_COMMAND_TYPES = frozenset({"send_code", "login", "logout", "clear_session", "search"})
_MAX_TERMINAL_COMMANDS = 200
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


class PersistentTaskGatewayUnavailableError(RuntimeError):
    pass


class TelegramControlBridge:
    """Small PostgreSQL-backed command/status bridge for the Coordinator-owned UserBot."""

    def __init__(
        self,
        *,
        connection_factory: Callable[[], AbstractContextManager[Any]] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if connection_factory is None:
            from database.connection import get_db_connection

            connection_factory = get_db_connection
        self._connection_factory = connection_factory
        self._clock = clock or (lambda: datetime.now(UTC))

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
                    "completed_at": None,
                    "result": {},
                    "error": "",
                }
            )
            self._trim_terminal_commands(state)

        self._mutate(mutate)
        return command_id

    def claim_next(self, owner_id: str, *, lease_seconds: int = 60) -> dict[str, Any] | None:
        owner_id = str(owner_id or "").strip()
        if not owner_id:
            raise ValueError("Telegram command owner_id is required")
        if lease_seconds <= 0:
            raise ValueError("Telegram command lease_seconds must be positive")
        now = self._now()
        claimed: dict[str, Any] | None = None

        def mutate(state: dict[str, Any]) -> None:
            nonlocal claimed
            for command in state["commands"]:
                if command.get("status") != "claimed":
                    continue
                if self._expired(command.get("lease_expires_at"), now):
                    command.update(
                        {
                            "status": "pending",
                            "claimed_at": None,
                            "claimed_by": "",
                            "lease_expires_at": None,
                        }
                    )
            for command in state["commands"]:
                if command.get("status") != "pending":
                    continue
                command.update(
                    {
                        "status": "claimed",
                        "claimed_at": now.isoformat(),
                        "claimed_by": owner_id,
                        "lease_expires_at": (now + timedelta(seconds=lease_seconds)).isoformat(),
                    }
                )
                claimed = normalize_json_object(dict(command), field="telegram_command")
                break

        self._mutate(mutate)
        return claimed

    def complete(self, command_id: str, owner_id: str, result: Mapping[str, Any] | None = None) -> dict[str, Any]:
        normalized_result = normalize_json_object(dict(result or {}), field="telegram_command.result")
        return self._finish(command_id, owner_id, status="succeeded", result=normalized_result, error="")

    def fail(self, command_id: str, owner_id: str, error: str) -> dict[str, Any]:
        message = str(error or "Telegram command failed")[:2000]
        return self._finish(command_id, owner_id, status="failed", result={}, error=message)

    def get_command(self, command_id: str) -> dict[str, Any] | None:
        state = self._read()
        for command in state["commands"]:
            if command.get("id") == command_id:
                return normalize_json_object(dict(command), field="telegram_command")
        return None

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
                if command.get("status") != "claimed" or command.get("claimed_by") != owner_id:
                    raise RuntimeError("Telegram command is not claimed by this owner")
                command.update(
                    {
                        "status": status,
                        "completed_at": self._now().isoformat(),
                        "lease_expires_at": None,
                        "payload": {},
                        "result": result,
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

    def _mutate(self, mutation: Callable[[dict[str, Any]], None]) -> None:
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
                mutation(state)
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
        if not isinstance(state.get("commands"), list) or not isinstance(state.get("runtime_status"), dict):
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


class TelegramCommandExecutor:
    """Execute persisted commands through a Coordinator-owned manager port."""

    def __init__(self, bridge: TelegramControlBridge, *, owner_id: str, lease_seconds: int = 60) -> None:
        self._bridge = bridge
        self._owner_id = owner_id
        self._lease_seconds = lease_seconds

    def process_pending(self, manager: Any, *, limit: int = 8) -> int:
        processed = 0
        for _ in range(max(0, limit)):
            command = self._bridge.claim_next(self._owner_id, lease_seconds=self._lease_seconds)
            if command is None:
                break
            try:
                result = self._execute(manager, command)
            except Exception as exc:
                self._bridge.fail(str(command["id"]), self._owner_id, str(exc))
            else:
                self._bridge.complete(str(command["id"]), self._owner_id, result)
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

    @staticmethod
    def _execute(manager: Any, command: Mapping[str, Any]) -> dict[str, Any]:
        command_type = command.get("type")
        payload = command.get("payload")
        if not isinstance(payload, Mapping):
            raise TypeError("Telegram command payload must be an object")
        if command_type == "send_code":
            manager.send_login_code()
            return {"success": True}
        if command_type == "login":
            result = manager.submit_login_code(str(payload.get("code") or ""))
        elif command_type == "logout":
            manager.logout()
            result = {"success": True}
        elif command_type == "clear_session":
            result = manager.clear_session_files()
        elif command_type == "search":
            result = manager.search_channel_resources(**dict(payload))
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
    "TelegramCommandTimeoutError",
    "TelegramControlBridge",
    "normalize_telegram_resource_action",
    "submit_persistent_legacy_task",
    "submit_telegram_resource_action",
    "telegram_resource_action_key",
]
