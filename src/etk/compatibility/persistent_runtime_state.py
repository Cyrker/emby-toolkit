from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from etk.platform.jobs import InvalidJsonPayloadError, normalize_json_object

_NAMESPACE_PATTERN = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}")


type StateMutation = Callable[
    [dict[str, Any] | None, datetime],
    tuple[dict[str, Any] | None, Any, bool],
]


class RuntimeStateBackend(Protocol):
    def mutate(self, setting_key: str, mutation: StateMutation) -> Any: ...


class PostgresRuntimeStateBackend:
    """Serialize one compatibility state key inside a PostgreSQL transaction."""

    def __init__(self, connection_factory: Callable[[], Any] | None = None) -> None:
        self._connection_factory = connection_factory

    def mutate(self, setting_key: str, mutation: StateMutation) -> Any:
        with self._connection_context() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (setting_key,),
                )
                cursor.execute("SELECT CURRENT_TIMESTAMP AS current_time")
                current_time = _as_utc(_row_value(cursor.fetchone(), "current_time", 0))
                cursor.execute(
                    "SELECT value_json FROM app_settings WHERE setting_key = %s",
                    (setting_key,),
                )
                row = cursor.fetchone()
                current = _coerce_state(_row_value(row, "value_json", 0) if row else None)
                next_state, result, should_write = mutation(current, current_time)

                if should_write:
                    if next_state is None:
                        cursor.execute(
                            "DELETE FROM app_settings WHERE setting_key = %s",
                            (setting_key,),
                        )
                    else:
                        normalized = normalize_json_object(next_state, field="runtime state")
                        cursor.execute(
                            """
                            INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                            VALUES (%s, CAST(%s AS jsonb), CURRENT_TIMESTAMP)
                            ON CONFLICT (setting_key) DO UPDATE SET
                                value_json = EXCLUDED.value_json,
                                last_updated_at = CURRENT_TIMESTAMP
                            """,
                            (
                                setting_key,
                                json.dumps(normalized, ensure_ascii=False, separators=(",", ":")),
                            ),
                        )
            connection.commit()
            return result

    def _connection_context(self) -> Any:
        if self._connection_factory is not None:
            return self._connection_factory()
        from database.connection import get_db_connection

        return get_db_connection()


@dataclass(frozen=True, slots=True)
class PendingWake:
    batch_key: str
    version: int
    delay_seconds: float
    deadline_at: str


@dataclass(frozen=True, slots=True)
class PendingClaim:
    batch_key: str
    version: int
    owner_token: str
    lease_expires_at: str
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class DeliveryClaim:
    owner_token: str
    lease_expires_at: str


class PendingLeaseActiveError(RuntimeError):
    def __init__(self, retry_after_seconds: float) -> None:
        self.retry_after_seconds = max(0.0, float(retry_after_seconds))
        super().__init__(f"pending runtime state is processing; retry after {self.retry_after_seconds:.3f}s")


class PersistentRuntimeStateStore:
    def __init__(self, backend: RuntimeStateBackend | None = None) -> None:
        self._backend = backend or PostgresRuntimeStateBackend()

    def merge_moviepilot_batch(
        self,
        batch_key: str,
        batch_values: Sequence[Any],
        file_info: Mapping[str, Any],
        *,
        is_video: bool,
        video_delay_seconds: float = 5,
        subtitle_delay_seconds: float = 7200,
    ) -> PendingWake:
        normalized_file = normalize_json_object(dict(file_info), field="MoviePilot file")
        normalized_batch = normalize_json_object(
            {"values": list(batch_values)},
            field="MoviePilot batch key",
        )["values"]

        def mutate(current: dict[str, Any] | None, now: datetime):
            payload = _pending_payload(current)
            processing = _processing_state(current) or {}
            raw_processing_payload = processing.get("payload")
            processing_payload = dict(raw_processing_payload) if type(raw_processing_payload) is dict else {}
            files = [dict(item) for item in payload.get("files", []) if type(item) is dict]
            incoming_identity = _moviepilot_file_identity(normalized_file)
            for index, existing in enumerate(files):
                if _moviepilot_file_identity(existing) == incoming_identity:
                    files[index] = normalize_json_object(
                        {**existing, **normalized_file},
                        field="MoviePilot merged file",
                    )
                    break
            else:
                files.append(normalized_file)

            has_video = bool(payload.get("has_video")) or bool(processing_payload.get("has_video")) or bool(is_video)
            delay = _positive_seconds(
                video_delay_seconds if has_video else subtitle_delay_seconds,
                field="MoviePilot debounce",
            )
            merged_payload = {
                "batch_values": normalized_batch,
                "files": files,
                "has_video": has_video,
            }
            return _next_pending_state(batch_key, current, merged_payload, now, delay)

        return self._backend.mutate(_setting_key("pending", "moviepilot", batch_key), mutate)

    def merge_metadata_update(
        self,
        item_id: str,
        item_name: str,
        *,
        delay_seconds: float,
    ) -> PendingWake:
        batch_key = str(item_id).strip()
        delay = _positive_seconds(delay_seconds, field="metadata debounce")

        def mutate(current: dict[str, Any] | None, now: datetime):
            payload = {"item_id": batch_key, "item_name": str(item_name or "")}
            return _next_pending_state(batch_key, current, payload, now, delay)

        return self._backend.mutate(_setting_key("pending", "emby-metadata-update", batch_key), mutate)

    def merge_active_emby_series(
        self,
        series_id: str,
        series_name: str,
        episode_ids: Sequence[Any],
        *,
        delay_seconds: float,
    ) -> PendingWake:
        batch_key = str(series_id).strip()
        incoming_ids = {str(value).strip() for value in episode_ids if str(value or "").strip()}
        delay = _positive_seconds(delay_seconds, field="active Emby dispatch debounce")

        def mutate(current: dict[str, Any] | None, now: datetime):
            payload = _pending_payload(current)
            merged_ids = {str(value).strip() for value in payload.get("episode_ids", []) if str(value or "").strip()}
            merged_ids.update(incoming_ids)
            merged_payload = {
                "series_id": batch_key,
                "series_name": str(series_name or payload.get("series_name") or ""),
                "episode_ids": sorted(merged_ids),
            }
            return _next_pending_state(batch_key, current, merged_payload, now, delay)

        return self._backend.mutate(
            _setting_key("pending", "active-emby-series-dispatch", batch_key),
            mutate,
        )

    def claim_pending(
        self,
        namespace: str,
        batch_key: str,
        wake_version: int,
        *,
        lease_seconds: float,
    ) -> PendingClaim | None:
        if int(wake_version) <= 0:
            raise ValueError("wake version must be positive")
        lease = _positive_seconds(lease_seconds, field="processing lease")

        def mutate(current: dict[str, Any] | None, now: datetime):
            if current is None:
                return None, None, False
            current_version = int(current.get("version") or 0)
            if int(wake_version) > current_version:
                return current, None, False

            processing = _processing_state(current)
            if processing is not None:
                lease_expires_at = _parse_deadline(processing.get("lease_expires_at"))
                if now < lease_expires_at:
                    raise PendingLeaseActiveError((lease_expires_at - now).total_seconds())
                pending_payload = _merge_namespace_payloads(
                    namespace,
                    processing.get("payload"),
                    _pending_payload(current),
                )
            else:
                if now < _parse_deadline(current.get("deadline_at")):
                    return current, None, False
                pending_payload = _pending_payload(current)

            if not pending_payload:
                return current, None, False

            owner_token = uuid.uuid4().hex
            lease_expires_at = now + timedelta(seconds=lease)
            next_state = {
                **current,
                "payload": {},
                "processing": {
                    "owner_token": owner_token,
                    "claimed_version": current_version,
                    "lease_expires_at": _iso(lease_expires_at),
                    "payload": normalize_json_object(pending_payload, field="claimed runtime payload"),
                },
            }
            claim = PendingClaim(
                batch_key=str(batch_key),
                version=current_version,
                owner_token=owner_token,
                lease_expires_at=_iso(lease_expires_at),
                payload=normalize_json_object(pending_payload, field="claimed runtime payload"),
            )
            return next_state, claim, True

        return self._backend.mutate(_setting_key("pending", namespace, str(batch_key)), mutate)

    def finalize_pending(self, namespace: str, batch_key: str, owner_token: str) -> bool:
        def mutate(current: dict[str, Any] | None, _now: datetime):
            processing = _processing_state(current)
            if processing is None or processing.get("owner_token") != owner_token:
                return current, False, False
            if _pending_payload(current):
                next_state = dict(current or {})
                next_state.pop("processing", None)
                return next_state, True, True
            return None, True, True

        return bool(self._backend.mutate(_setting_key("pending", namespace, str(batch_key)), mutate))

    def release_pending(self, namespace: str, batch_key: str, owner_token: str) -> bool:
        def mutate(current: dict[str, Any] | None, now: datetime):
            processing = _processing_state(current)
            if processing is None or processing.get("owner_token") != owner_token:
                return current, False, False
            payload = _merge_namespace_payloads(
                namespace,
                processing.get("payload"),
                _pending_payload(current),
            )
            next_state = dict(current or {})
            next_state.pop("processing", None)
            next_state["payload"] = payload
            next_state["deadline_at"] = _iso(now)
            return next_state, True, True

        return bool(self._backend.mutate(_setting_key("pending", namespace, str(batch_key)), mutate))

    def put_ttl_marker(self, namespace: str, key: str, ttl_seconds: float) -> None:
        ttl = _positive_seconds(ttl_seconds, field="marker TTL")

        def mutate(_current: dict[str, Any] | None, now: datetime):
            return {"expires_at": _iso(now + timedelta(seconds=ttl))}, None, True

        self._backend.mutate(_setting_key("marker", namespace, str(key)), mutate)

    def consume_ttl_marker(self, namespace: str, key: str, ttl_seconds: float) -> bool:
        _positive_seconds(ttl_seconds, field="marker TTL")

        def mutate(current: dict[str, Any] | None, now: datetime):
            if current is None:
                return None, False, False
            active = now < _parse_deadline(current.get("expires_at"))
            return None, active, True

        return bool(self._backend.mutate(_setting_key("marker", namespace, str(key)), mutate))

    def has_ttl_marker(self, namespace: str, key: str) -> bool:
        def mutate(current: dict[str, Any] | None, now: datetime):
            if current is None:
                return None, False, False
            if now >= _parse_deadline(current.get("expires_at")):
                return None, False, True
            return current, True, False

        return bool(self._backend.mutate(_setting_key("marker", namespace, str(key)), mutate))

    def reserve_once(self, namespace: str, key: str, ttl_seconds: float) -> str | None:
        ttl = _positive_seconds(ttl_seconds, field="reservation TTL")

        def mutate(current: dict[str, Any] | None, now: datetime):
            if current is not None and now < _parse_deadline(current.get("expires_at")):
                return current, None, False
            token = uuid.uuid4().hex
            state = {
                "token": token,
                "expires_at": _iso(now + timedelta(seconds=ttl)),
            }
            return state, token, True

        return self._backend.mutate(_setting_key("reservation", namespace, str(key)), mutate)

    def release_once(self, namespace: str, key: str, token: str | None = None) -> bool:
        def mutate(current: dict[str, Any] | None, _now: datetime):
            if current is None:
                return None, False, False
            if token is not None and current.get("token") != token:
                return current, False, False
            return None, True, True

        return bool(self._backend.mutate(_setting_key("reservation", namespace, str(key)), mutate))

    def claim_delivery(self, namespace: str, key: str, *, lease_seconds: float) -> DeliveryClaim | None:
        lease = _positive_seconds(lease_seconds, field="delivery lease")

        def mutate(current: dict[str, Any] | None, now: datetime):
            if current is not None and current.get("status") == "delivered":
                return current, None, False
            if current is not None and current.get("status") == "sending":
                lease_expires_at = _parse_deadline(current.get("lease_expires_at"))
                if now < lease_expires_at:
                    raise PendingLeaseActiveError((lease_expires_at - now).total_seconds())

            owner_token = uuid.uuid4().hex
            lease_expires_at = now + timedelta(seconds=lease)
            state = {
                "status": "sending",
                "owner_token": owner_token,
                "lease_expires_at": _iso(lease_expires_at),
            }
            return (
                state,
                DeliveryClaim(owner_token=owner_token, lease_expires_at=state["lease_expires_at"]),
                True,
            )

        return self._backend.mutate(_setting_key("delivery", namespace, str(key)), mutate)

    def complete_delivery(
        self,
        namespace: str,
        key: str,
        owner_token: str,
        *,
        result: Mapping[str, Any] | None = None,
    ) -> bool:
        normalized_result = normalize_json_object(dict(result or {}), field="delivery result")

        def mutate(current: dict[str, Any] | None, now: datetime):
            if current is None or current.get("status") != "sending":
                return current, False, False
            if current.get("owner_token") != owner_token:
                return current, False, False
            return (
                {
                    "status": "delivered",
                    "delivered_at": _iso(now),
                    "result": normalized_result,
                },
                True,
                True,
            )

        return bool(self._backend.mutate(_setting_key("delivery", namespace, str(key)), mutate))

    def release_delivery(self, namespace: str, key: str, owner_token: str) -> bool:
        def mutate(current: dict[str, Any] | None, _now: datetime):
            if current is None or current.get("status") != "sending":
                return current, False, False
            if current.get("owner_token") != owner_token:
                return current, False, False
            return None, True, True

        return bool(self._backend.mutate(_setting_key("delivery", namespace, str(key)), mutate))


class PersistentExpiringSet:
    """Set-shaped compatibility facade backed by persistent TTL markers."""

    def __init__(
        self,
        namespace: str,
        *,
        default_ttl_seconds: float,
        store: PersistentRuntimeStateStore | None = None,
    ) -> None:
        self._namespace = namespace
        self._default_ttl_seconds = _positive_seconds(default_ttl_seconds, field="default TTL")
        self._store = store or _DEFAULT_STORE

    def add(self, value: Any) -> None:
        self.add_for(value, self._default_ttl_seconds)

    def add_for(self, value: Any, ttl_seconds: float) -> None:
        self._store.put_ttl_marker(self._namespace, str(value), ttl_seconds)

    def discard(self, value: Any) -> None:
        self._store.consume_ttl_marker(self._namespace, str(value), self._default_ttl_seconds)

    def __contains__(self, value: object) -> bool:
        return self._store.has_ttl_marker(self._namespace, str(value))


_DEFAULT_STORE = PersistentRuntimeStateStore()


def merge_moviepilot_batch(*args: Any, **kwargs: Any) -> PendingWake:
    return _DEFAULT_STORE.merge_moviepilot_batch(*args, **kwargs)


def merge_metadata_update(*args: Any, **kwargs: Any) -> PendingWake:
    return _DEFAULT_STORE.merge_metadata_update(*args, **kwargs)


def merge_active_emby_series(*args: Any, **kwargs: Any) -> PendingWake:
    return _DEFAULT_STORE.merge_active_emby_series(*args, **kwargs)


def claim_pending(
    namespace: str,
    batch_key: str,
    wake_version: int,
    *,
    lease_seconds: float,
) -> PendingClaim | None:
    return _DEFAULT_STORE.claim_pending(
        namespace,
        batch_key,
        wake_version,
        lease_seconds=lease_seconds,
    )


def finalize_pending(namespace: str, batch_key: str, owner_token: str) -> bool:
    return _DEFAULT_STORE.finalize_pending(namespace, batch_key, owner_token)


def release_pending(namespace: str, batch_key: str, owner_token: str) -> bool:
    return _DEFAULT_STORE.release_pending(namespace, batch_key, owner_token)


def put_ttl_marker(namespace: str, key: str, ttl_seconds: float) -> None:
    _DEFAULT_STORE.put_ttl_marker(namespace, key, ttl_seconds)


def consume_ttl_marker(namespace: str, key: str, ttl_seconds: float) -> bool:
    return _DEFAULT_STORE.consume_ttl_marker(namespace, key, ttl_seconds)


def reserve_once(namespace: str, key: str, ttl_seconds: float) -> str | None:
    return _DEFAULT_STORE.reserve_once(namespace, key, ttl_seconds)


def release_once(namespace: str, key: str, token: str | None = None) -> bool:
    return _DEFAULT_STORE.release_once(namespace, key, token)


def claim_delivery(namespace: str, key: str, *, lease_seconds: float) -> DeliveryClaim | None:
    return _DEFAULT_STORE.claim_delivery(namespace, key, lease_seconds=lease_seconds)


def complete_delivery(
    namespace: str,
    key: str,
    owner_token: str,
    *,
    result: Mapping[str, Any] | None = None,
) -> bool:
    return _DEFAULT_STORE.complete_delivery(namespace, key, owner_token, result=result)


def release_delivery(namespace: str, key: str, owner_token: str) -> bool:
    return _DEFAULT_STORE.release_delivery(namespace, key, owner_token)


def _next_pending_state(
    batch_key: str,
    current: dict[str, Any] | None,
    payload: Mapping[str, Any],
    now: datetime,
    delay_seconds: float,
) -> tuple[dict[str, Any], PendingWake, bool]:
    version = int((current or {}).get("version") or 0) + 1
    deadline = now + timedelta(seconds=delay_seconds)
    state = {
        "version": version,
        "deadline_at": _iso(deadline),
        "payload": normalize_json_object(dict(payload), field="pending runtime payload"),
    }
    processing = _processing_state(current)
    if processing is not None:
        state["processing"] = processing
    wake = PendingWake(
        batch_key=str(batch_key),
        version=version,
        delay_seconds=delay_seconds,
        deadline_at=state["deadline_at"],
    )
    return state, wake, True


def _pending_payload(current: dict[str, Any] | None) -> dict[str, Any]:
    payload = (current or {}).get("payload")
    return dict(payload) if type(payload) is dict else {}


def _processing_state(current: dict[str, Any] | None) -> dict[str, Any] | None:
    processing = (current or {}).get("processing")
    return dict(processing) if type(processing) is dict else None


def _merge_namespace_payloads(namespace: str, older: Any, newer: Any) -> dict[str, Any]:
    older_payload = dict(older) if type(older) is dict else {}
    newer_payload = dict(newer) if type(newer) is dict else {}
    if namespace == "moviepilot":
        files: list[dict[str, Any]] = []
        identities: dict[str, int] = {}
        for raw_file in [*older_payload.get("files", []), *newer_payload.get("files", [])]:
            if type(raw_file) is not dict:
                continue
            file_info = normalize_json_object(raw_file, field="MoviePilot requeued file")
            identity = _moviepilot_file_identity(file_info)
            existing_index = identities.get(identity)
            if existing_index is None:
                identities[identity] = len(files)
                files.append(file_info)
            else:
                files[existing_index] = normalize_json_object(
                    {**files[existing_index], **file_info},
                    field="MoviePilot requeued file",
                )
        payload = {
            "batch_values": newer_payload.get("batch_values") or older_payload.get("batch_values") or [],
            "files": files,
            "has_video": bool(older_payload.get("has_video")) or bool(newer_payload.get("has_video")),
        }
    elif namespace == "emby-metadata-update":
        payload = newer_payload or older_payload
    elif namespace == "active-emby-series-dispatch":
        episode_ids = {
            str(value).strip()
            for value in [*older_payload.get("episode_ids", []), *newer_payload.get("episode_ids", [])]
            if str(value or "").strip()
        }
        payload = {
            "series_id": newer_payload.get("series_id") or older_payload.get("series_id") or "",
            "series_name": newer_payload.get("series_name") or older_payload.get("series_name") or "",
            "episode_ids": sorted(episode_ids),
        }
    else:
        raise ValueError(f"unsupported pending runtime namespace: {namespace!r}")
    return normalize_json_object(payload, field="requeued runtime payload")


def _moviepilot_file_identity(file_info: Mapping[str, Any]) -> str:
    file_id = str(file_info.get("file_id") or "").strip()
    if file_id:
        return f"file:{file_id}"
    pickcode = str(file_info.get("pickcode") or file_info.get("pick_code") or "").strip()
    if pickcode:
        return f"pickcode:{pickcode}"
    parent_id = str(file_info.get("parent_id") or "").strip()
    name = str(file_info.get("name") or file_info.get("file_name") or "").strip()
    if parent_id or name:
        return f"parent-name:{parent_id}\0{name}"
    normalized = normalize_json_object(dict(file_info), field="MoviePilot file identity")
    encoded = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return f"payload:{hashlib.sha256(encoded.encode('utf-8')).hexdigest()}"


def _setting_key(category: str, namespace: str, logical_key: str) -> str:
    normalized_namespace = str(namespace or "").strip().casefold()
    if not _NAMESPACE_PATTERN.fullmatch(normalized_namespace):
        raise ValueError(f"invalid runtime state namespace: {namespace!r}")
    digest = hashlib.sha256(f"{normalized_namespace}\0{str(logical_key)}".encode()).hexdigest()
    return f"runtime.{category}.{normalized_namespace}.{digest}"


def _coerce_state(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = json.loads(value)
    if type(value) is not dict:
        raise InvalidJsonPayloadError("runtime state must be a JSON object")
    return normalize_json_object(value, field="runtime state")


def _row_value(row: Any, key: str, index: int) -> Any:
    if isinstance(row, Mapping):
        return row[key]
    return row[index]


def _as_utc(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("database current time must be a datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_deadline(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        raise InvalidJsonPayloadError("runtime state deadline is missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidJsonPayloadError("runtime state deadline is invalid") from exc
    return _as_utc(parsed)


def _iso(value: datetime) -> str:
    return _as_utc(value).isoformat().replace("+00:00", "Z")


def _positive_seconds(value: float, *, field: str) -> float:
    seconds = float(value)
    if seconds <= 0:
        raise ValueError(f"{field} must be positive")
    return seconds


__all__ = [
    "DeliveryClaim",
    "PendingClaim",
    "PendingLeaseActiveError",
    "PendingWake",
    "PersistentExpiringSet",
    "PersistentRuntimeStateStore",
    "PostgresRuntimeStateBackend",
    "RuntimeStateBackend",
    "claim_pending",
    "claim_delivery",
    "complete_delivery",
    "consume_ttl_marker",
    "finalize_pending",
    "merge_active_emby_series",
    "merge_metadata_update",
    "merge_moviepilot_batch",
    "put_ttl_marker",
    "release_pending",
    "release_delivery",
    "release_once",
    "reserve_once",
]
