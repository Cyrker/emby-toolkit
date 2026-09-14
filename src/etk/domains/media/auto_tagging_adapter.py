from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager, nullcontext
from typing import Any
from urllib.parse import quote

import httpx
from sqlalchemy import text
from sqlalchemy.engine import Engine

from etk.platform import Settings

from .auto_tagging import AUTO_TAGGING_EFFECT_TYPE
from .effect_execution import (
    MediaEffectDeliveryIndeterminate,
    MediaEffectDeliveryRejected,
    MediaEffectDeliveryRetryable,
    PersistentMediaEffectExecutor,
)
from .effects import MediaEffectProjection

_TRANSIENT_HTTP_STATUSES = frozenset({408, 425, 429})


class _AutoTaggingQueryError(RuntimeError):
    pass


class _AutoTaggingQueryRejected(_AutoTaggingQueryError):
    pass


class _AutoTaggingQueryRetryable(_AutoTaggingQueryError):
    pass


class PostgresAutoTaggingItemLock:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    @contextmanager
    def __call__(self, item_id: str) -> Iterator[None]:
        lock_key = f"auto-tagging:{str(item_id).strip()}"
        with self._engine.connect() as connection:
            connection.execute(
                text("SELECT pg_advisory_lock(hashtextextended(:lock_key, 0))"),
                {"lock_key": lock_key},
            )
            try:
                yield
            finally:
                connection.execute(
                    text("SELECT pg_advisory_unlock(hashtextextended(:lock_key, 0))"),
                    {"lock_key": lock_key},
                )


class HttpxAutoTaggingAdapter:
    effect_types = frozenset({AUTO_TAGGING_EFFECT_TYPE})

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        item_lock: Callable[[str], AbstractContextManager[None]] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client
        self._item_lock = item_lock or (lambda _item_id: nullcontext())

    def list_libraries(self) -> Sequence[Mapping[str, Any]]:
        _settings, base_url, token, _user_id = self._identity()
        payload = self._get(f"{base_url}/Library/VirtualFolders", token=token)
        if not isinstance(payload, list):
            raise RuntimeError("Emby library query returned an invalid payload")
        return tuple(dict(item) for item in payload if isinstance(item, Mapping))

    def list_items(self, library_id: str) -> Sequence[Mapping[str, Any]]:
        _settings, base_url, token, user_id = self._identity()
        payload = self._get(
            f"{base_url}/Users/{quote(user_id, safe='')}/Items",
            token=token,
            params={
                "ParentId": str(library_id),
                "Recursive": "true",
                "IncludeItemTypes": "Movie,Series,Episode",
                "Fields": "Id,Name,OfficialRating,CustomRating",
            },
        )
        if not isinstance(payload, Mapping) or not isinstance(payload.get("Items"), list):
            raise RuntimeError("Emby library item query returned an invalid payload")
        return tuple(dict(item) for item in payload["Items"] if isinstance(item, Mapping))

    def deliver(self, effect: MediaEffectProjection) -> Mapping[str, Any]:
        payload = effect.payload
        item_id = str(payload.get("item_id") or "").strip()
        operation = str(payload.get("operation") or "").strip().casefold()
        tags = _strings(payload.get("tags"))
        if not item_id or operation not in {"add", "remove"}:
            raise MediaEffectDeliveryRejected("auto-tagging effect payload is invalid")
        _settings, base_url, token, user_id = self._identity()
        with self._item_lock(item_id):
            return self._deliver_locked(item_id, operation, tags, base_url, token, user_id)

    def _deliver_locked(
        self,
        item_id: str,
        operation: str,
        tags: Sequence[str],
        base_url: str,
        token: str,
        user_id: str,
    ) -> Mapping[str, Any]:
        try:
            current = self._item(base_url, token, user_id, item_id, fields=None)
        except _AutoTaggingQueryRejected as exc:
            raise MediaEffectDeliveryRejected(str(exc)) from exc
        except _AutoTaggingQueryRetryable as exc:
            raise MediaEffectDeliveryRetryable(str(exc), retry_after_seconds=5) from exc
        current_tags = _current_tags(current)
        desired = current_tags | set(tags) if operation == "add" else current_tags - set(tags)
        if desired == current_tags:
            return {"item_id": item_id, "operation": operation, "changed": False, "readback": "confirmed"}
        update = dict(current)
        update["Tags"] = sorted(desired)
        for key in (
            "TagItems",
            "MediaStreams",
            "MediaSources",
            "Chapters",
            "RecursiveItemCount",
            "ChildCount",
            "ImageTags",
            "SeriesTimerId",
            "RunTimeTicks",
        ):
            update.pop(key, None)
        current_locked_fields = current.get("LockedFields")
        if operation == "add" and isinstance(current_locked_fields, list) and "Tags" in current_locked_fields:
            update["LockedFields"] = [value for value in current_locked_fields if value != "Tags"]
        response_error: Exception | None = None
        try:
            with self._client_factory() as client:
                response = client.post(
                    f"{base_url}/Items/{quote(item_id, safe='')}",
                    headers={"Accept": "application/json", "Content-Type": "application/json", "X-Emby-Token": token},
                    json=update,
                )
                classification = _mutation_status(response.status_code)
                if classification == "rejected":
                    raise MediaEffectDeliveryRejected("Emby rejected the tag update")
                if classification == "unknown":
                    response_error = RuntimeError(f"Emby tag update returned HTTP {response.status_code}")
        except MediaEffectDeliveryRejected:
            raise
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            response_error = exc
        try:
            observed = _current_tags(self._item(base_url, token, user_id, item_id))
        except _AutoTaggingQueryError as exc:
            raise MediaEffectDeliveryIndeterminate("Emby tag update readback is unavailable") from exc
        reached = desired <= observed if operation == "add" else not (set(tags) & observed)
        if not reached:
            raise MediaEffectDeliveryIndeterminate(
                "Emby tag update outcome is unknown",
                error={"response_lost": response_error is not None},
            )
        return {"item_id": item_id, "operation": operation, "changed": True, "readback": "confirmed"}

    def _item(
        self,
        base_url: str,
        token: str,
        user_id: str,
        item_id: str,
        *,
        fields: str | None = "Tags,TagItems,LockedFields",
    ) -> Mapping[str, Any]:
        payload = self._get(
            f"{base_url}/Users/{quote(user_id, safe='')}/Items/{quote(item_id, safe='')}",
            token=token,
            params={"Fields": fields} if fields is not None else None,
        )
        if not isinstance(payload, Mapping) or str(payload.get("Id") or "").strip() != item_id:
            raise _AutoTaggingQueryRejected("Emby tag readback returned an invalid item")
        return dict(payload)

    def _get(
        self,
        url: str,
        *,
        token: str,
        params: Mapping[str, str] | None = None,
    ) -> Any:
        try:
            with self._client_factory() as client:
                response = client.get(
                    url,
                    headers={"Accept": "application/json", "X-Emby-Token": token},
                    params=dict(params or {}),
                )
        except httpx.HTTPError as exc:
            raise _AutoTaggingQueryRetryable("Emby tag query is unavailable") from exc
        if _is_transient_status(response.status_code):
            raise _AutoTaggingQueryRetryable("Emby tag query is unavailable")
        if 400 <= response.status_code < 500:
            raise _AutoTaggingQueryRejected("Emby tag query was rejected")
        try:
            return response.json()
        except (TypeError, ValueError) as exc:
            raise _AutoTaggingQueryRejected("Emby tag query returned invalid JSON") from exc

    def _identity(self) -> tuple[Settings, str, str, str]:
        settings = self._settings()
        base_url = settings.emby_server_url.strip().rstrip("/")
        token = settings.emby_api_key.get_secret_value().strip()
        user_id = settings.emby_user_id.strip()
        if not base_url or not token or not user_id:
            raise RuntimeError("Emby is not configured")
        return settings, base_url, token, user_id

    def _default_client(self) -> httpx.Client:
        return httpx.Client(timeout=httpx.Timeout(20.0, connect=5.0), trust_env=False)


class PersistentAutoTaggingEffects:
    def __init__(self, executor: PersistentMediaEffectExecutor) -> None:
        self._executor = executor

    def apply(
        self,
        *,
        effect_key: str,
        item_id: str,
        operation: str,
        tags: Sequence[str],
    ) -> MediaEffectProjection:
        return self._executor.reserve_and_execute(
            effect_key=effect_key,
            effect_type=AUTO_TAGGING_EFFECT_TYPE,
            aggregate_type="emby_item",
            aggregate_id=str(item_id),
            payload={"item_id": str(item_id), "operation": str(operation), "tags": list(tags)},
        )


def _strings(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError("auto-tagging tags must be a list")
    return tuple(dict.fromkeys(str(item).strip() for item in value if str(item).strip()))


def _current_tags(item: Mapping[str, Any]) -> set[str]:
    tags = {str(value).strip() for value in item.get("Tags", []) if str(value).strip()}
    raw_tag_items = item.get("TagItems")
    if isinstance(raw_tag_items, list):
        tags.update(
            str(value.get("Name") or "").strip()
            for value in raw_tag_items
            if isinstance(value, Mapping) and str(value.get("Name") or "").strip()
        )
    return tags


def _is_transient_status(status_code: int) -> bool:
    return status_code in _TRANSIENT_HTTP_STATUSES or status_code >= 500 or 300 <= status_code < 400


def _mutation_status(status_code: int) -> str | None:
    if 200 <= status_code < 300:
        return None
    if 400 <= status_code < 500 and status_code not in _TRANSIENT_HTTP_STATUSES:
        return "rejected"
    return "unknown"


__all__ = [
    "HttpxAutoTaggingAdapter",
    "PersistentAutoTaggingEffects",
    "PostgresAutoTaggingItemLock",
]
