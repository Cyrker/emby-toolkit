from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from typing import Any
from urllib.parse import unquote

import httpx

from etk.domains.operations.jobs import JobSubmissionService
from etk.platform import Settings

from .deep_delete import (
    P115_DEEP_DELETE_OPERATION,
    P115DeepDeleteJob,
)

_EMBY_ITEM_FIELDS = (
    "Path,MediaSources,Chapters,ProviderIds,SeriesId,SeasonId,SeriesName,ParentIndexNumber,IndexNumber,Type,Name"
)


class HttpxP115BridgeEmbyGateway:
    """Bounded read-only Emby identity and hierarchy adapter for Bridge callbacks."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def verified_item(
        self,
        sha1: str,
        item_id: str,
        *,
        expected_pick_code: str = "",
    ) -> Mapping[str, Any] | None:
        item = self._item(item_id)
        return item if item is not None and p115_item_matches(item, sha1, expected_pick_code) else None

    def root_item(self, item_id: str) -> Mapping[str, Any] | None:
        return self._item(item_id)

    def child_items(self, root_item_id: str) -> Sequence[Mapping[str, Any]]:
        settings = self._settings()
        base_url, api_key = _emby_authorization(settings)
        if not base_url or not api_key:
            return ()
        output: list[Mapping[str, Any]] = []
        seen: set[str] = set()
        start = 0
        limit = 500
        with self._client_factory() as client:
            while start < 100_000:
                try:
                    response = client.get(
                        f"{base_url}/Items",
                        headers={"X-Emby-Token": api_key, "Accept": "application/json"},
                        params={
                            "ParentId": root_item_id,
                            "Recursive": "true",
                            "StartIndex": str(start),
                            "Limit": str(limit),
                            "Fields": _EMBY_ITEM_FIELDS,
                        },
                    )
                    if response.status_code != 200:
                        return ()
                    payload = response.json()
                except (httpx.HTTPError, ValueError, TypeError):
                    return ()
                items = payload.get("Items") if isinstance(payload, Mapping) else None
                if not isinstance(items, list):
                    return ()
                for item in items:
                    if not isinstance(item, Mapping):
                        continue
                    item_id = str(item.get("Id") or "").strip()
                    if not item_id or item_id in seen:
                        continue
                    seen.add(item_id)
                    output.append(dict(item))
                start += len(items)
                total = _integer(payload.get("TotalRecordCount")) if isinstance(payload, Mapping) else None
                if not items or len(items) < limit or (total is not None and start >= total):
                    break
        return tuple(output)

    def _item(self, item_id: str) -> Mapping[str, Any] | None:
        normalized_item_id = str(item_id or "").strip()
        if not re.fullmatch(r"\d{1,20}", normalized_item_id):
            return None
        settings = self._settings()
        base_url, api_key = _emby_authorization(settings)
        if not base_url or not api_key:
            return None
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}/Items",
                    headers={"X-Emby-Token": api_key, "Accept": "application/json"},
                    params={"Ids": normalized_item_id, "Limit": "1", "Fields": _EMBY_ITEM_FIELDS},
                )
                if response.status_code != 200:
                    return None
                payload = response.json()
        except (httpx.HTTPError, ValueError, TypeError):
            return None
        items = payload.get("Items") if isinstance(payload, Mapping) else payload
        if not isinstance(items, list):
            return None
        return next(
            (
                dict(item)
                for item in items
                if isinstance(item, Mapping) and str(item.get("Id") or "").strip() == normalized_item_id
            ),
            None,
        )

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(
            proxy=proxy or None,
            timeout=max(1.0, float(settings.emby_api_timeout)),
        )


class LegacyP115DeepDeleteScopeGateway:
    """Narrow P11 seam around the established hierarchy and P115 cache readers."""

    def pickcodes_for_deleted_item(self, emby_item_id: str, item_type: str) -> Sequence[str]:
        from database import media_db

        return tuple(media_db.get_pickcodes_for_deleted_emby_item(emby_item_id, item_type) or ())

    def pick_code_by_sha1(self, sha1: str) -> str | None:
        from handler.p115_service import P115CacheManager

        row = P115CacheManager.get_file_cache_by_sha1(sha1) or {}
        value = str(row.get("pick_code") or "").strip()
        return value or None

    def file_snapshots(self, pickcodes: Sequence[str]) -> Sequence[Mapping[str, Any]]:
        from handler.p115_service import P115CacheManager

        snapshots: list[dict[str, str]] = []
        for pick_code in pickcodes:
            normalized = str(pick_code or "").strip()
            row = P115CacheManager.get_file_cache_by_pickcode(normalized) or {}
            fid = str(row.get("id") or row.get("fid") or "").strip()
            if not normalized or not fid:
                continue
            snapshots.append(
                {
                    "pick_code": str(row.get("pick_code") or normalized).strip(),
                    "fid": fid,
                    "parent_id": str(row.get("parent_id") or "").strip(),
                }
            )
        return tuple(snapshots)


class P115DeepDeleteJobGateway:
    def __init__(self, jobs: Callable[[], JobSubmissionService]) -> None:
        self._jobs = jobs

    def submit(
        self,
        operation: str,
        payload: Mapping[str, Any],
        *,
        max_attempts: int,
        timeout_seconds: int,
        idempotency_scope: str,
        idempotency_key: str,
    ) -> P115DeepDeleteJob:
        if operation != P115_DEEP_DELETE_OPERATION:
            raise ValueError(f"unsupported P115 internal operation: {operation}")
        from etk.processes.p6_deep_delete import execute_p115_deep_delete_operation

        submitted = self._jobs().submit_background_operation(
            execute_p115_deep_delete_operation,
            (dict(payload),),
            {},
            max_attempts=max_attempts,
            timeout_seconds=timeout_seconds,
            idempotency_scope=idempotency_scope,
            idempotency_key=idempotency_key,
        )
        return P115DeepDeleteJob(str(submitted.projection.job.id), dict(payload))

    def lookup(self, *, idempotency_scope: str, idempotency_key: str) -> P115DeepDeleteJob | None:
        projection = self._jobs().get_idempotent(scope=idempotency_scope, key=idempotency_key)
        if projection is None:
            return None
        request = projection.job.request_payload
        positional = request.get("positional_parameters") if isinstance(request, Mapping) else None
        payload = (
            positional[0] if isinstance(positional, list) and positional and isinstance(positional[0], Mapping) else {}
        )
        return P115DeepDeleteJob(str(projection.job.id), dict(payload))


class LegacyP115UploadSourceCleanupGateway:
    def delete_sources(self, pickcodes: Sequence[str]) -> Mapping[str, Any]:
        from handler.p115_upload_monitor import delete_local_sources_by_pickcodes

        result = delete_local_sources_by_pickcodes(list(pickcodes))
        return dict(result) if isinstance(result, Mapping) else {"ok": True, "legacy_result": None}


class LegacyP115DeleteReadbackGateway:
    def delete_and_reconcile(
        self,
        pickcodes: Sequence[str],
        file_snapshots: Sequence[Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        import config_manager
        import constants
        from handler.p115_service import WebhookDeleteBuffer

        if not config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_115_ENABLE_SYNC_DELETE, False):
            return {"ok": True, "skipped": True, "reason": "sync_delete_disabled"}
        snapshot_map = {
            str(snapshot.get("pick_code") or "").strip(): {
                "id": str(snapshot.get("fid") or snapshot.get("id") or "").strip(),
                "parent_id": str(snapshot.get("parent_id") or "").strip(),
                "pick_code": str(snapshot.get("pick_code") or "").strip(),
            }
            for snapshot in file_snapshots
            if str(snapshot.get("pick_code") or "").strip()
            and str(snapshot.get("fid") or snapshot.get("id") or "").strip()
        }
        retry = WebhookDeleteBuffer._process_batch(list(pickcodes), file_snapshots=snapshot_map) or []
        if retry:
            return {"ok": False, "retryable": True, "remaining": len(retry)}
        return {"ok": True, "confirmed": True, "pickcode_count": len(pickcodes)}


class LegacyP115MediaCleanupReceiptGateway:
    def cleanup(
        self,
        *,
        item_id: str,
        item_name: str,
        item_type: str,
        series_id: str,
        local_only: bool,
        operation_id: str,
    ) -> Mapping[str, Any]:
        from database import maintenance_db

        result = maintenance_db.cleanup_deleted_media_item(
            item_id=item_id,
            item_name=item_name,
            item_type=item_type,
            series_id_from_webhook=series_id or None,
            local_only=local_only,
            operation_id=operation_id,
        )
        return dict(result) if isinstance(result, Mapping) else {"ok": False, "error": "missing outcome"}


class EmbeddedP115SharedCleanupGateway:
    """P5 receipt still owns shared cleanup until its compatibility transaction is extracted."""

    def cleanup(
        self,
        snapshot: Mapping[str, Any],
        media_result: Mapping[str, Any],
        *,
        operation_id: str,
    ) -> Mapping[str, Any]:
        del snapshot, operation_id
        if media_result.get("ok") is not True:
            return {"ok": False, "error": "P5 media cleanup receipt was not confirmed"}
        shared_result = media_result.get("shared_cleanup")
        if not isinstance(shared_result, Mapping):
            return {"ok": False, "error": "P5 media cleanup receipt has no shared cleanup result"}
        return dict(shared_result)


def _emby_authorization(settings: Settings) -> tuple[str, str]:
    return (
        str(settings.emby_server_url or "").strip().rstrip("/"),
        settings.emby_api_key.get_secret_value().strip(),
    )


def p115_item_matches(item: Mapping[str, Any], sha1: str, expected_pick_code: str) -> bool:
    values: list[object] = [item.get("Path")]
    sources = item.get("MediaSources")
    if isinstance(sources, Sequence) and not isinstance(sources, (str, bytes)):
        values.extend(source.get("Path") for source in sources if isinstance(source, Mapping))
    normalized_sha1 = str(sha1 or "").strip().upper()
    normalized_pick_code = str(expected_pick_code or "").strip().casefold()
    for value in values:
        path = unquote(str(value or ""))
        play = re.search(r"/api/p115/play/([^/?#]+)", path, flags=re.IGNORECASE)
        if play and normalized_pick_code and play.group(1).strip().casefold() == normalized_pick_code:
            return True
        virtual = re.search(
            r"/api/p115/virtual-play/[^/?#]+/([A-Fa-f0-9]{40})(?:[/?#]|$)",
            path,
            flags=re.IGNORECASE,
        )
        if virtual and normalized_sha1 and virtual.group(1).upper() == normalized_sha1:
            return True
    return False


def _integer(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


__all__ = [
    "EmbeddedP115SharedCleanupGateway",
    "HttpxP115BridgeEmbyGateway",
    "LegacyP115DeepDeleteScopeGateway",
    "LegacyP115DeleteReadbackGateway",
    "p115_item_matches",
    "LegacyP115MediaCleanupReceiptGateway",
    "LegacyP115UploadSourceCleanupGateway",
    "P115DeepDeleteJobGateway",
]
