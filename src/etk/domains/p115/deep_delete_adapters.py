from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol

from sqlalchemy import text

from etk.domains.media.cleanup_projection import SqlAlchemyCleanupMediaProjection
from etk.domains.subscription.cleanup_execution import CleanupEffectResult, CleanupEffectStatus
from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs import PersistentEffectIndeterminate

from .auth_directory import P115CredentialStore
from .maintenance_transport import P115PrimaryOperationsGateway, provider_succeeded


class _CleanupMediaGateway(Protocol):
    def reconcile(self, target: Mapping[str, Any], *, operation_id: str) -> CleanupEffectResult: ...


class SqlAlchemyP115DeepDeleteScopeGateway:
    """Freeze local media and account-bound P115 identities before Emby deletion."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        credentials: P115CredentialStore,
        provider: P115PrimaryOperationsGateway,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._credentials = credentials
        self._provider = provider
        self._media = SqlAlchemyCleanupMediaProjection()

    def media_cleanup_target(self, emby_item_id: str, item_type: str) -> Mapping[str, Any] | None:
        normalized_id = str(emby_item_id or "").strip()
        normalized_type = str(item_type or "").strip().title()
        if not normalized_id or not normalized_type:
            return None
        with self._unit_of_work() as uow:
            candidate_count = int(
                uow.session.scalar(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM (
                            SELECT 1
                            FROM media_metadata
                            WHERE (
                                    jsonb_typeof(asset_details_json) = 'array'
                                AND EXISTS (
                                    SELECT 1
                                    FROM jsonb_array_elements(asset_details_json) AS asset(value)
                                    WHERE COALESCE(asset.value->>'emby_item_id', asset.value->>'id') = :emby_id
                                )
                            ) OR (
                                    jsonb_typeof(emby_item_ids_json) = 'array'
                                AND emby_item_ids_json ? :emby_id
                            )
                            LIMIT 2
                        ) AS candidates
                        """
                    ),
                    {"emby_id": normalized_id},
                )
                or 0
            )
            if candidate_count != 1:
                return None
            identity = self._media.execution_identity(
                uow,
                normalized_id,
                fallback_item_type=normalized_type,
            )
        if identity is None or str(identity.get("item_type") or "").strip().title() != normalized_type:
            return None
        target = {
            key: identity.get(key)
            for key in (
                "emby_id",
                "tmdb_id",
                "item_type",
                "title",
                "series_id",
                "season_number",
                "episode_number",
            )
        }
        files = identity.get("files")
        target["local"] = {
            "files": [
                {
                    "sha1": str(value.get("sha1") or "").strip().upper(),
                    "pick_code": str(value.get("pick_code") or "").strip(),
                }
                for value in files
                if isinstance(value, Mapping)
                and (str(value.get("sha1") or "").strip() or str(value.get("pick_code") or "").strip())
            ]
            if isinstance(files, Sequence) and not isinstance(files, str | bytes)
            else []
        }
        return target

    def pickcodes_for_deleted_item(self, emby_item_id: str, item_type: str) -> Sequence[str]:
        normalized_id = str(emby_item_id or "").strip()
        normalized_type = str(item_type or "").strip().title()
        if not normalized_id or not normalized_type:
            return ()
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        WITH target_item AS (
                            SELECT tmdb_id, item_type, parent_series_tmdb_id, season_number,
                                   COUNT(*) OVER () AS target_count
                            FROM media_metadata
                            WHERE emby_item_ids_json @> CAST(:emby_ids AS jsonb)
                            ORDER BY tmdb_id, item_type
                            LIMIT 2
                        )
                        SELECT t.item_type AS target_type,
                               t.target_count,
                               m.file_pickcode_json, m.file_sha1_json
                        FROM media_metadata AS m
                        JOIN target_item AS t ON
                             (m.tmdb_id = t.tmdb_id AND m.item_type = t.item_type)
                          OR (t.item_type = 'Series'
                              AND m.parent_series_tmdb_id = t.tmdb_id
                              AND m.item_type = 'Episode')
                          OR (t.item_type = 'Season'
                              AND m.parent_series_tmdb_id = t.parent_series_tmdb_id
                              AND m.season_number = t.season_number
                              AND m.item_type = 'Episode')
                        ORDER BY m.item_type, m.tmdb_id
                        """
                    ),
                    {"emby_ids": json.dumps([normalized_id])},
                )
                .mappings()
                .all()
            )
        if not rows:
            return ()
        if {int(row.get("target_count") or 0) for row in rows} != {1}:
            raise RuntimeError("deep-delete has ambiguous media identity")
        target_types = {str(row.get("target_type") or "").strip().title() for row in rows}
        if target_types != {normalized_type}:
            raise RuntimeError("deep-delete media scope type changed before snapshot")
        output: list[str] = []
        seen: set[str] = set()
        for row in rows:
            pickcodes = _strings(row.get("file_pickcode_json"))
            sha1s = _strings(row.get("file_sha1_json"))
            for index in range(max(len(pickcodes), len(sha1s))):
                pick_code = pickcodes[index] if index < len(pickcodes) else ""
                if not pick_code and index < len(sha1s) and sha1s[index]:
                    pick_code = self.pick_code_by_sha1(sha1s[index]) or ""
                key = pick_code.casefold()
                if pick_code and key not in seen:
                    seen.add(key)
                    output.append(pick_code)
        return tuple(output)

    def pick_code_by_sha1(self, sha1: str) -> str | None:
        normalized_sha1 = str(sha1 or "").strip().upper()
        account_id = str(self._credentials.load().account_id or "").strip()
        if not normalized_sha1 or not account_id:
            return None
        with self._unit_of_work() as uow:
            values = tuple(
                str(value or "").strip()
                for value in uow.session.scalars(
                    text(
                        """
                        SELECT pick_code
                        FROM p115_filesystem_cache
                        WHERE account_id = :account_id
                          AND entry_kind = 'file'
                          AND UPPER(sha1) = :sha1
                          AND NULLIF(BTRIM(pick_code), '') IS NOT NULL
                        ORDER BY updated_at DESC NULLS LAST, id
                        LIMIT 2
                        """
                    ),
                    {"account_id": account_id, "sha1": normalized_sha1},
                )
                if str(value or "").strip()
            )
        return values[0] if len(values) == 1 else None

    def file_snapshots(self, pickcodes: Sequence[str]) -> Sequence[Mapping[str, Any]]:
        requested = tuple(dict.fromkeys(str(value or "").strip() for value in pickcodes if str(value or "").strip()))
        if not requested:
            return ()
        account_id = str(self._credentials.load().account_id or "").strip()
        if not account_id:
            raise RuntimeError("deep-delete P115 account identity is unavailable")
        keys = [value.casefold() for value in requested]
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT id, parent_id, pick_code, sha1, account_id
                        FROM p115_filesystem_cache
                        WHERE account_id = :account_id
                          AND entry_kind = 'file'
                          AND LOWER(pick_code) = ANY(CAST(:pickcodes AS text[]))
                        ORDER BY LOWER(pick_code), updated_at DESC NULLS LAST, id
                        """
                    ),
                    {"account_id": account_id, "pickcodes": keys},
                )
                .mappings()
                .all()
            )
        by_pickcode: dict[str, Mapping[str, Any]] = {}
        for row in rows:
            key = str(row.get("pick_code") or "").strip().casefold()
            if key in by_pickcode:
                raise RuntimeError("deep-delete PickCode has ambiguous P115 cache identities")
            by_pickcode[key] = dict(row)
        if set(by_pickcode) != set(keys):
            raise RuntimeError("deep-delete P115 file identity is incomplete")
        client = self._provider.open(expected_account_id=account_id)
        if client is None:
            raise RuntimeError("deep-delete P115 provider identity is unavailable")
        return tuple(
            {
                "pick_code": str(by_pickcode[key]["pick_code"]),
                "fid": str(by_pickcode[key]["id"]),
                "parent_id": str(by_pickcode[key].get("parent_id") or ""),
                "sha1": str(by_pickcode[key].get("sha1") or "").strip().upper(),
                "account_id": client.account_id,
                "backend": client.backend,
                "credential_fingerprint": client.credential_fingerprint,
            }
            for key in keys
        )


class NativeP115DeleteReadbackGateway:
    def __init__(self, provider: P115PrimaryOperationsGateway) -> None:
        self._provider = provider

    def delete_and_reconcile(
        self,
        pickcodes: Sequence[str],
        file_snapshots: Sequence[Mapping[str, Any]],
        *,
        enabled: bool,
    ) -> Mapping[str, Any]:
        if not enabled:
            return {"ok": True, "skipped": True, "reason": "sync_delete_disabled"}
        requested = {str(value or "").strip().casefold() for value in pickcodes if str(value or "").strip()}
        snapshots = tuple(dict(value) for value in file_snapshots)
        observed = {str(value.get("pick_code") or "").strip().casefold() for value in snapshots}
        if not requested or observed != requested:
            raise RuntimeError("deep-delete P115 snapshot identity is incomplete")
        account_ids = {str(value.get("account_id") or "").strip() for value in snapshots}
        backends = {str(value.get("backend") or "").strip().casefold() for value in snapshots}
        fingerprints = {str(value.get("credential_fingerprint") or "").strip().casefold() for value in snapshots}
        if len(account_ids) != 1 or "" in account_ids or len(backends) != 1 or backends - {"openapi", "cookie"}:
            raise RuntimeError("deep-delete P115 provider identity is invalid")
        if len(fingerprints) != 1 or "" in fingerprints:
            raise RuntimeError("deep-delete P115 credential identity is invalid")
        account_id = next(iter(account_ids))
        backend = next(iter(backends))
        credential_fingerprint = next(iter(fingerprints))
        client = self._provider.open(
            expected_account_id=account_id,
            expected_backend=backend,
            expected_credential_fingerprint=credential_fingerprint,
        )
        if client is None:
            raise RuntimeError("deep-delete P115 pinned provider is unavailable")
        fids = tuple(dict.fromkeys(str(value.get("fid") or "").strip() for value in snapshots))
        if len(fids) != len(snapshots) or any(not value for value in fids):
            raise RuntimeError("deep-delete P115 FID identity is invalid")

        initial = _presence(client, fids)
        present = [fid for fid, exists in initial.items() if exists]
        if not present:
            return {
                "ok": True,
                "confirmed": True,
                "already_absent": True,
                "presence": initial,
            }
        try:
            response = client.delete_files(present)
        except Exception as exc:
            try:
                readback = _presence(client, fids)
            except Exception as readback_exc:
                raise PersistentEffectIndeterminate("115 deep-delete outcome could not be read back") from readback_exc
            if all(exists is False for exists in readback.values()):
                return {"ok": True, "confirmed": True, "reconciled": True, "presence": readback}
            raise RuntimeError("115 deep-delete request failed and targets remain present") from exc
        try:
            readback = _presence(client, fids)
        except Exception as exc:
            raise PersistentEffectIndeterminate("115 deep-delete outcome could not be read back") from exc
        if all(exists is False for exists in readback.values()):
            return {
                "ok": True,
                "confirmed": True,
                "provider_accepted": provider_succeeded(response),
                "presence": readback,
            }
        if not provider_succeeded(response):
            raise RuntimeError("115 deep-delete request was rejected")
        raise RuntimeError("115 deep-delete targets remain present after an accepted request")


class NativeP115MediaCleanupReceiptGateway:
    def __init__(self, gateway: _CleanupMediaGateway) -> None:
        self._gateway = gateway

    def cleanup(
        self,
        *,
        item_id: str,
        item_name: str,
        item_type: str,
        series_id: str,
        local_only: bool,
        operation_id: str,
        target: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        del item_name, series_id, local_only
        frozen = dict(target)
        if (
            str(frozen.get("emby_id") or "").strip() != str(item_id or "").strip()
            or str(frozen.get("item_type") or "").strip().title() != str(item_type or "").strip().title()
        ):
            return {"ok": False, "indeterminate": True, "error": "media cleanup identity changed"}
        outcome = self._gateway.reconcile(frozen, operation_id=operation_id)
        if outcome.status is CleanupEffectStatus.INDETERMINATE:
            return {
                "ok": False,
                "indeterminate": True,
                "error": dict(outcome.error or {}),
                "evidence": dict(outcome.evidence),
            }
        if outcome.status is CleanupEffectStatus.RETRYABLE:
            delegated = _durable_shared_cleanup_receipt(outcome)
            if delegated is not None:
                return {
                    **dict(outcome.evidence),
                    "ok": True,
                    "shared_cleanup": delegated,
                }
            return {
                "ok": False,
                "retryable": True,
                "error": dict(outcome.error or {}),
                "evidence": dict(outcome.evidence),
            }
        evidence = dict(outcome.evidence)
        return {
            "ok": True,
            **evidence,
            "shared_cleanup": {
                "ok": True,
                "confirmed": True,
                "shared_source_count": int(evidence.get("shared_source_count") or 0),
            },
        }


class NativeP115SharedCleanupGateway:
    def cleanup(
        self,
        snapshot: Mapping[str, Any],
        media_result: Mapping[str, Any],
        *,
        operation_id: str,
    ) -> Mapping[str, Any]:
        del snapshot, operation_id
        result = media_result.get("shared_cleanup")
        if not isinstance(result, Mapping):
            return {"ok": False, "error": "native media cleanup has no shared cleanup receipt"}
        return dict(result)


def _strings(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return ()
    return tuple(str(item or "").strip() for item in value)


def _durable_shared_cleanup_receipt(outcome: CleanupEffectResult) -> dict[str, Any] | None:
    error = outcome.error or {}
    if str(error.get("type") or "") != "cleanup_shared_source_pending":
        return None
    raw_pending = outcome.evidence.get("pending")
    if not isinstance(raw_pending, list) or not raw_pending:
        return None
    pending: list[dict[str, Any]] = []
    for raw in raw_pending:
        if not isinstance(raw, Mapping) or "remaining_sources" in raw:
            return None
        source_id = raw.get("source_id")
        operation_id = str(raw.get("operation_id") or "").strip()
        if isinstance(source_id, bool) or not isinstance(source_id, int) or source_id < 1 or not operation_id:
            return None
        pending.append(
            {
                "source_id": source_id,
                "operation_id": operation_id,
                "status": str(raw.get("status") or "pending").strip().casefold() or "pending",
            }
        )
    return {
        "ok": True,
        "confirmed": False,
        "delegated": True,
        "shared_source_count": int(outcome.evidence.get("shared_source_count") or len(pending)),
        "pending": pending,
    }


def _presence(client: Any, fids: Sequence[str]) -> dict[str, bool]:
    return {fid: _remote_file_info_exists(client.file_info(fid), fid) for fid in fids}


def _remote_file_info_exists(value: Mapping[str, Any], expected_fid: str) -> bool:
    if provider_succeeded(value):
        data: object = value.get("data")
        if isinstance(data, list):
            data = data[0] if data else None
        if isinstance(data, Mapping):
            observed = str(data.get("fid") or data.get("file_id") or data.get("id") or "").strip()
            return not observed or observed == expected_fid
        return True
    code = str(value.get("code") or value.get("errno") or "").strip()
    message = str(
        value.get("error_msg") or value.get("message") or value.get("msg") or value.get("error") or ""
    ).casefold()
    if code in {"20004", "70004", "404"} or any(
        marker in message for marker in ("不存在", "not found", "no such file", "file not exist")
    ):
        return False
    raise RuntimeError("115 deep-delete file readback was not authoritative")


__all__ = [
    "NativeP115DeleteReadbackGateway",
    "NativeP115MediaCleanupReceiptGateway",
    "NativeP115SharedCleanupGateway",
    "SqlAlchemyP115DeepDeleteScopeGateway",
]
