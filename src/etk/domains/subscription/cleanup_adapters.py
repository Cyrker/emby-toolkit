from __future__ import annotations

import hashlib
import json
import os
import secrets
import stat
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol, cast
from urllib.parse import quote

import httpx
from sqlalchemy import text

from etk.domains.media.bridge_client import emby_authorization_revision
from etk.domains.p115.adapters import LegacyP115CredentialStore
from etk.domains.p115.auth_directory import P115CredentialStore
from etk.domains.p115.bridge_compatibility import (
    LegacyP115MediaCleanupReceiptGateway,
)
from etk.domains.p115.maintenance_transport import (
    P115MaintenanceTransportError,
    P115PrimaryOperationsGateway,
    provider_succeeded,
)
from etk.platform import Settings
from etk.platform.db import SqlAlchemyUnitOfWork

from .cleanup_execution import CleanupEffectResult

_SIDECAR_EXTENSIONS = frozenset({".srt", ".ass", ".ssa", ".sub", ".vtt", ".sup", ".nfo", ".jpg", ".png"})


class SettingsCleanupExecutionPolicy:
    """Freeze non-secret filesystem/provider identity before a Cleanup Job is staged."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        credentials: P115CredentialStore | None = None,
    ) -> None:
        self._settings = settings
        self._credentials = credentials

    def freeze_target(self, identity: Mapping[str, Any]) -> Mapping[str, Any]:
        settings = self._settings()
        root = _absolute_root(settings.local_strm_root)
        raw_files = identity.get("files")
        if not isinstance(raw_files, Sequence) or isinstance(raw_files, str | bytes):
            raise ValueError("cleanup execution identity files must be an array")
        files: list[dict[str, Any]] = []
        p115_nodes: list[dict[str, str | int]] = []
        seen_paths: set[str] = set()
        seen_pickcodes: set[str] = set()
        for raw_file in raw_files:
            if not isinstance(raw_file, Mapping):
                raise TypeError("cleanup execution file identity must be an object")
            path = str(raw_file.get("path") or "").strip()
            if path:
                frozen = _freeze_local_file(root, path)
                frozen["sha1"] = str(raw_file.get("sha1") or "").strip().upper()
                frozen["pick_code"] = str(raw_file.get("pick_code") or "").strip()
                if frozen["path"] not in seen_paths:
                    seen_paths.add(str(frozen["path"]))
                    files.append(frozen)
            pick_code = str(raw_file.get("pick_code") or "").strip()
            p115 = raw_file.get("p115")
            if pick_code and pick_code not in seen_pickcodes:
                if settings.p115_enable_sync_delete and not isinstance(p115, Mapping):
                    raise ValueError(f"cleanup target {pick_code!r} has no frozen 115 file identity")
                if isinstance(p115, Mapping):
                    fid = str(p115.get("fid") or p115.get("id") or "").strip()
                    if not fid:
                        raise ValueError(f"cleanup target {pick_code!r} has no frozen 115 FID")
                    account_id = str(p115.get("account_id") or "").strip()
                    if settings.p115_enable_sync_delete and not account_id:
                        raise ValueError(f"cleanup target {pick_code!r} has no frozen 115 account identity")
                    parent_id = str(p115.get("parent_id") or "").strip()
                    if settings.p115_enable_sync_delete and not parent_id:
                        raise ValueError(f"cleanup target {pick_code!r} has no frozen 115 parent identity")
                    p115_nodes.append(
                        {
                            "fid": fid,
                            "parent_id": parent_id,
                            "account_id": account_id,
                            "pick_code": pick_code,
                            "sha1": str(p115.get("sha1") or "").strip(),
                            "size": int(p115.get("size") or 0),
                        }
                    )
                seen_pickcodes.add(pick_code)

        if not files:
            raise ValueError("cleanup target requires a frozen local path for Emby deletion notification")

        revision = ""
        if settings.emby_server_url.strip() and settings.emby_api_key.get_secret_value().strip():
            revision = emby_authorization_revision(settings)
        if not revision:
            raise ValueError("cleanup target requires configured Emby authorization")
        p115_identity: dict[str, str] = {}
        if p115_nodes and self._credentials is not None:
            credential_snapshot = self._credentials.load()
            account_id = str(credential_snapshot.account_id or "").strip()
            if not account_id:
                raise ValueError("cleanup target requires a stable 115 account identity")
            node_accounts = {str(node["account_id"]).strip() for node in p115_nodes}
            if node_accounts != {account_id}:
                raise ValueError("cleanup target 115 account identity changed before staging")
            backend = _select_p115_backend(credential_snapshot, settings.p115_api_priority)
            if not backend:
                raise ValueError("cleanup target requires an available 115 provider backend")
            p115_identity = {
                "account_id": account_id,
                "backend": backend,
                "credential_fingerprint": _p115_credential_fingerprint(credential_snapshot, backend),
            }
        return {
            "schema_version": 1,
            "emby_id": str(identity.get("emby_id") or "").strip(),
            "tmdb_id": str(identity.get("tmdb_id") or "").strip(),
            "item_type": str(identity.get("item_type") or "").strip(),
            "title": str(identity.get("title") or ""),
            "series_id": str(identity.get("series_id") or "").strip(),
            "season_number": _optional_int(identity.get("season_number")),
            "episode_number": _optional_int(identity.get("episode_number")),
            "local": {"root": root, "files": files},
            "p115": {
                "enabled": bool(settings.p115_enable_sync_delete),
                **p115_identity,
                "nodes": p115_nodes,
            },
            "emby": {"authorization_revision": revision},
        }


class CleanupEmbyInventoryUnavailable(RuntimeError):
    pass


class CleanupEmbyInventoryCredentialChanged(RuntimeError):
    pass


class HttpxCleanupEmbyInventoryGateway:
    """Authoritative, read-only Emby ItemID inventory for duplicate scanning."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        batch_size: int = 100,
    ) -> None:
        if batch_size < 1:
            raise ValueError("cleanup Emby inventory batch_size must be positive")
        self._settings = settings
        self._client_factory = client_factory or self._default_client
        self._batch_size = batch_size

    def existing_item_ids(self, item_ids: Sequence[str]) -> frozenset[str]:
        normalized = tuple(dict.fromkeys(str(value).strip() for value in item_ids if str(value).strip()))
        if not normalized:
            return frozenset()
        settings = self._settings()
        base_url = settings.emby_server_url.strip().rstrip("/")
        token = settings.emby_api_key.get_secret_value().strip()
        if not base_url or not token:
            raise CleanupEmbyInventoryUnavailable("cleanup scan requires configured Emby authorization")
        revision = emby_authorization_revision(settings)
        requested = set(normalized)
        existing: set[str] = set()
        try:
            with self._client_factory() as client:
                for offset in range(0, len(normalized), self._batch_size):
                    self._assert_revision(revision)
                    batch = normalized[offset : offset + self._batch_size]
                    response = client.get(
                        f"{base_url}/Items",
                        params={
                            "Ids": ",".join(batch),
                            "Limit": len(batch),
                            "Fields": "Id",
                        },
                        headers={"X-Emby-Token": token},
                    )
                    response.raise_for_status()
                    try:
                        payload = response.json()
                    except ValueError as exc:
                        raise CleanupEmbyInventoryUnavailable("cleanup Emby inventory returned invalid JSON") from exc
                    if not isinstance(payload, Mapping) or not isinstance(payload.get("Items"), list):
                        raise CleanupEmbyInventoryUnavailable(
                            "cleanup Emby inventory returned an invalid response object"
                        )
                    for raw_item in payload["Items"]:
                        if not isinstance(raw_item, Mapping):
                            raise CleanupEmbyInventoryUnavailable("cleanup Emby inventory returned a non-object item")
                        item_id = str(raw_item.get("Id") or "").strip()
                        if not item_id or item_id not in requested:
                            raise CleanupEmbyInventoryUnavailable(
                                "cleanup Emby inventory returned an unexpected item identity"
                            )
                        existing.add(item_id)
        except CleanupEmbyInventoryCredentialChanged:
            raise
        except CleanupEmbyInventoryUnavailable:
            raise
        except (httpx.HTTPError, httpx.TimeoutException, httpx.TransportError) as exc:
            raise CleanupEmbyInventoryUnavailable("cleanup Emby inventory request failed") from exc
        finally:
            self._assert_revision(revision)
        return frozenset(existing)

    def _assert_revision(self, expected: str) -> None:
        if emby_authorization_revision(self._settings()) != expected:
            raise CleanupEmbyInventoryCredentialChanged("Emby authorization changed during cleanup inventory read")

    def _default_client(self) -> httpx.Client:
        return httpx.Client(timeout=float(self._settings().emby_api_timeout))


class SafeLocalCleanupGateway:
    def reconcile(self, target: Mapping[str, Any]) -> CleanupEffectResult:
        local = _mapping(target.get("local"), "cleanup local snapshot")
        root = _absolute_root(str(local.get("root") or ""))
        raw_files = local.get("files")
        if not isinstance(raw_files, list):
            return CleanupEffectResult.indeterminate({"type": "local_snapshot_invalid"})
        deleted: list[str] = []
        absent: list[str] = []
        try:
            for raw_file in raw_files:
                frozen = _mapping(raw_file, "cleanup local file")
                main = _reconcile_frozen_path(root, frozen)
                deleted.extend(main["deleted"])
                absent.extend(main["absent"])
                raw_sidecars = frozen.get("sidecars")
                if not isinstance(raw_sidecars, list):
                    return CleanupEffectResult.indeterminate(
                        {"type": "local_sidecar_snapshot_invalid", "path": str(frozen.get("path") or "")}
                    )
                for sidecar in raw_sidecars:
                    result = _reconcile_frozen_path(root, _mapping(sidecar, "cleanup sidecar"))
                    deleted.extend(result["deleted"])
                    absent.extend(result["absent"])
            for raw_file in raw_files:
                path = str(_mapping(raw_file, "cleanup local file").get("path") or "")
                if path:
                    _prune_empty_parents(root, os.path.dirname(path))
        except CleanupLocalIdentityChanged as exc:
            return CleanupEffectResult.indeterminate(
                {"type": "local_target_identity_changed", "message": str(exc)},
                {"deleted": deleted, "absent": absent},
            )
        except OSError as exc:
            return CleanupEffectResult.retryable(
                {"type": "local_delete_failed", "message": str(exc)},
                {"deleted": deleted, "absent": absent},
            )
        return CleanupEffectResult.confirmed({"deleted": deleted, "absent": absent})


class CleanupSharedSourceLifecyclePort(Protocol):
    def delete_local_source(
        self,
        source_id: int,
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> Any: ...


class SqlAlchemyCleanupMediaGateway:
    """Remove one frozen Emby version and converge native sharing projections."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        shared_sources: CleanupSharedSourceLifecyclePort | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._shared_sources = shared_sources

    def reconcile(self, target: Mapping[str, Any], *, operation_id: str) -> CleanupEffectResult:
        emby_id = str(target.get("emby_id") or "").strip()
        tmdb_id = str(target.get("tmdb_id") or "").strip()
        item_type = str(target.get("item_type") or "").strip()
        normalized_operation = str(operation_id or "").strip()
        if not emby_id or not tmdb_id or not item_type or not normalized_operation:
            return CleanupEffectResult.indeterminate({"type": "media_cleanup_identity_missing"})

        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, item_type, emby_item_ids_json,
                               asset_details_json, file_sha1_json, file_pickcode_json
                        FROM media_metadata
                        WHERE tmdb_id = :tmdb_id
                          AND item_type = :item_type
                        FOR UPDATE
                        """
                    ),
                    {"tmdb_id": tmdb_id, "item_type": item_type},
                )
                .mappings()
                .first()
            )
            if row is None:
                offline_stats = _reconcile_offline_media_scope(uow, target)
                shared_source_ids = _cleanup_shared_source_ids(uow, target, scope_offline=True)
                uow.commit()
                return self._finish_shared_sources(
                    shared_source_ids,
                    normalized_operation,
                    {
                        "operation_id": normalized_operation,
                        "already_absent": True,
                        "media_row_absent": True,
                        **offline_stats,
                    },
                )

            aligned = _aligned_media_versions(dict(row))
            if aligned is None:
                return CleanupEffectResult.indeterminate(
                    {
                        "type": "media_cleanup_versions_unaligned",
                        "tmdb_id": tmdb_id,
                        "item_type": item_type,
                    }
                )
            assets, emby_ids, sha1s, pick_codes = aligned
            matches = [index for index, value in enumerate(emby_ids) if value == emby_id]
            if not matches:
                cleanup_stats = _reconcile_cleanup_indexes(uow, tmdb_id, item_type, emby_id)
                offline_stats = _reconcile_offline_media_scope(uow, target) if not emby_ids else {}
                shared_source_ids = _cleanup_shared_source_ids(
                    uow,
                    target,
                    scope_offline=not emby_ids,
                )
                uow.commit()
                return self._finish_shared_sources(
                    shared_source_ids,
                    normalized_operation,
                    {
                        "operation_id": normalized_operation,
                        "already_absent": True,
                        "remaining_versions": len(emby_ids),
                        **cleanup_stats,
                        **offline_stats,
                    },
                )
            if len(matches) != 1:
                return CleanupEffectResult.indeterminate(
                    {"type": "media_cleanup_item_identity_ambiguous", "emby_id": emby_id}
                )

            index = matches[0]
            asset = assets[index]
            asset_emby_id = str(asset.get("emby_item_id") or asset.get("id") or "").strip()
            if asset_emby_id and asset_emby_id != emby_id:
                return CleanupEffectResult.indeterminate(
                    {
                        "type": "media_cleanup_asset_identity_mismatch",
                        "emby_id": emby_id,
                        "asset_emby_id": asset_emby_id,
                    }
                )
            if not _frozen_media_version_matches(target, sha1s[index], pick_codes[index]):
                return CleanupEffectResult.indeterminate(
                    {
                        "type": "media_cleanup_version_identity_changed",
                        "emby_id": emby_id,
                    }
                )
            removed_asset = dict(asset)
            assets.pop(index)
            emby_ids.pop(index)
            sha1s.pop(index)
            pick_codes.pop(index)
            remaining = len(emby_ids)
            uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET emby_item_ids_json = CAST(:emby_ids AS jsonb),
                        asset_details_json = CASE
                            WHEN :remaining = 0 THEN NULL
                            ELSE CAST(:assets AS jsonb)
                        END,
                        file_sha1_json = CAST(:sha1s AS jsonb),
                        file_pickcode_json = CAST(:pick_codes AS jsonb),
                        in_library = CASE WHEN :remaining = 0 THEN FALSE ELSE in_library END,
                        washing_level = CASE WHEN :remaining = 0 THEN NULL ELSE washing_level END,
                        washing_snapshot_json = CASE
                            WHEN :remaining = 0 THEN '{}'::jsonb
                            ELSE washing_snapshot_json
                        END,
                        last_updated_at = NOW()
                    WHERE tmdb_id = :tmdb_id
                      AND item_type = :item_type
                    """
                ),
                {
                    "emby_ids": json.dumps(emby_ids, ensure_ascii=False),
                    "assets": json.dumps(assets, ensure_ascii=False),
                    "sha1s": json.dumps(sha1s, ensure_ascii=False),
                    "pick_codes": json.dumps(pick_codes, ensure_ascii=False),
                    "remaining": remaining,
                    "tmdb_id": tmdb_id,
                    "item_type": item_type,
                },
            )
            cleanup_stats = _reconcile_cleanup_indexes(uow, tmdb_id, item_type, emby_id)
            virtual_stats = _reconcile_virtual_imports(uow, removed_asset)
            offline_stats = _reconcile_offline_media_scope(uow, target) if remaining == 0 else {}
            shared_source_ids = _cleanup_shared_source_ids(
                uow,
                target,
                scope_offline=remaining == 0,
            )
            uow.commit()
        return self._finish_shared_sources(
            shared_source_ids,
            normalized_operation,
            {
                "operation_id": normalized_operation,
                "removed_emby_id": emby_id,
                "remaining_versions": remaining,
                **cleanup_stats,
                **virtual_stats,
                **offline_stats,
            },
        )

    def _finish_shared_sources(
        self,
        source_ids: Sequence[int],
        operation_id: str,
        evidence: Mapping[str, Any],
    ) -> CleanupEffectResult:
        normalized_ids = tuple(dict.fromkeys(int(value) for value in source_ids if int(value) > 0))
        if not normalized_ids:
            return CleanupEffectResult.confirmed(dict(evidence))
        if self._shared_sources is None:
            return CleanupEffectResult.indeterminate(
                {"type": "cleanup_shared_source_lifecycle_unavailable"},
                {**dict(evidence), "shared_source_count": len(normalized_ids)},
            )

        pending: list[dict[str, Any]] = []
        for source_id in normalized_ids[:200]:
            result = self._shared_sources.delete_local_source(
                source_id,
                principal_id="system:cleanup",
                idempotency_key=f"{operation_id}:shared-source:{source_id}",
            )
            status_code = int(getattr(result, "status_code", 500) or 500)
            raw_payload = getattr(result, "payload", None)
            payload = dict(raw_payload) if isinstance(raw_payload, Mapping) else {}
            status = str(payload.get("status") or "").strip().casefold()
            staged_operation_id = str(payload.get("operation_id") or "").strip()
            if status_code == 404:
                continue
            if status == "confirmed" or (
                status_code < 300 and payload.get("accepted") is not True and payload.get("success") is True
            ):
                continue
            if status in {"rejected", "indeterminate", "cancelled"}:
                return CleanupEffectResult.indeterminate(
                    {
                        "type": "cleanup_shared_source_terminal_failure",
                        "source_id": source_id,
                        "status": status,
                        "operation_id": staged_operation_id or None,
                    },
                    {**dict(evidence), "shared_source_count": len(normalized_ids)},
                )
            if status_code >= 500:
                return CleanupEffectResult.retryable(
                    {
                        "type": "cleanup_shared_source_unavailable",
                        "source_id": source_id,
                        "status_code": status_code,
                    },
                    {**dict(evidence), "shared_source_count": len(normalized_ids)},
                )
            pending.append(
                {
                    "source_id": source_id,
                    "operation_id": staged_operation_id or None,
                    "status": status or "pending",
                }
            )
        if len(normalized_ids) > 200:
            pending.append({"remaining_sources": len(normalized_ids) - 200})
        if pending:
            return CleanupEffectResult.retryable(
                {"type": "cleanup_shared_source_pending"},
                {
                    **dict(evidence),
                    "shared_source_count": len(normalized_ids),
                    "pending": pending,
                },
            )
        return CleanupEffectResult.confirmed({**dict(evidence), "shared_source_count": len(normalized_ids)})


class NativeCleanupP115Gateway:
    """Delete frozen 115 FIDs through a credential-pinned native client."""

    def __init__(self, provider: P115PrimaryOperationsGateway) -> None:
        self._provider = provider

    def request(self, target: Mapping[str, Any]) -> CleanupEffectResult:
        snapshot = _p115_snapshot(target)
        if snapshot["enabled"] is False:
            return CleanupEffectResult.confirmed({"skipped": True, "reason": "sync_delete_disabled"})
        nodes = snapshot["nodes"]
        if not nodes:
            return CleanupEffectResult.confirmed({"skipped": True, "reason": "no_remote_identity"})
        client, error = self._client(snapshot)
        if error is not None:
            return CleanupEffectResult.indeterminate(error)
        if client is None:
            return CleanupEffectResult.retryable({"type": "p115_delete_client_unavailable"})

        try:
            presence = _p115_presence_by_parent(client, nodes)
        except P115MaintenanceTransportError as exc:
            return CleanupEffectResult.indeterminate({"type": "p115_credential_revision_changed", "message": str(exc)})
        except Exception as exc:
            return CleanupEffectResult.retryable({"type": "p115_delete_preflight_failed", "message": str(exc)})
        present = [fid for fid, exists in presence.items() if exists]
        if not present:
            return CleanupEffectResult.confirmed(
                {"deleted": True, "already_absent": True, "fids": sorted(str(node["fid"]) for node in nodes)}
            )
        try:
            response = client.delete_files(present)
        except P115MaintenanceTransportError as exc:
            return CleanupEffectResult.indeterminate(
                {"type": "p115_credential_revision_changed", "message": str(exc)},
                {"fids": sorted(present)},
            )
        except Exception as exc:
            return CleanupEffectResult.retryable(
                {"type": "p115_delete_request_failed", "message": str(exc)},
                {"fids": sorted(present)},
            )
        if not provider_succeeded(response):
            return CleanupEffectResult.retryable(
                {"type": "p115_delete_rejected"},
                {"fids": sorted(present), "provider_response": _safe_provider_response(response)},
            )
        return self._readback(client, nodes, request_accepted=True)

    def readback(self, target: Mapping[str, Any]) -> CleanupEffectResult:
        snapshot = _p115_snapshot(target)
        if snapshot["enabled"] is False:
            return CleanupEffectResult.confirmed({"skipped": True, "reason": "sync_delete_disabled"})
        nodes = snapshot["nodes"]
        if not nodes:
            return CleanupEffectResult.confirmed({"skipped": True, "reason": "no_remote_identity"})
        client, error = self._client(snapshot)
        if error is not None:
            return CleanupEffectResult.indeterminate(error)
        if client is None:
            return CleanupEffectResult.retryable({"type": "p115_readback_client_unavailable"})
        return self._readback(client, nodes, request_accepted=False)

    def _readback(
        self,
        client: Any,
        nodes: Sequence[Mapping[str, Any]],
        *,
        request_accepted: bool,
    ) -> CleanupEffectResult:
        try:
            presence = _p115_presence_by_parent(client, nodes)
        except P115MaintenanceTransportError as exc:
            return CleanupEffectResult.indeterminate(
                {"type": "p115_credential_revision_changed", "message": str(exc)},
                {"request_accepted": request_accepted},
            )
        except Exception as exc:
            return CleanupEffectResult.retryable(
                {"type": "p115_readback_unknown", "message": str(exc)},
                {"request_accepted": request_accepted},
            )
        if all(value is False for value in presence.values()):
            return CleanupEffectResult.confirmed(
                {
                    "deleted": True,
                    "presence": presence,
                    "request_accepted": request_accepted,
                    "evidence": "authoritative_absence",
                }
            )
        return CleanupEffectResult.retryable(
            {"type": "p115_target_still_present"},
            {"presence": presence, "request_accepted": request_accepted},
        )

    def _client(
        self,
        snapshot: Mapping[str, Any],
    ) -> tuple[Any | None, Mapping[str, Any] | None]:
        nodes = snapshot["nodes"]
        fids = [str(node.get("fid") or "").strip() for node in nodes]
        parents = [str(node.get("parent_id") or "").strip() for node in nodes]
        accounts = {str(node.get("account_id") or "").strip() for node in nodes}
        if any(not fid for fid in fids) or any(not parent for parent in parents) or "" in accounts:
            return None, {"type": "p115_snapshot_identity_missing"}
        if len(accounts) != 1:
            return None, {"type": "p115_snapshot_has_multiple_accounts"}
        snapshot_account = str(snapshot.get("account_id") or "").strip()
        if snapshot_account and snapshot_account not in accounts:
            return None, {"type": "p115_snapshot_account_identity_changed"}
        expected_backend = str(snapshot.get("backend") or "").strip().lower()
        expected_fingerprint = str(snapshot.get("credential_fingerprint") or "").strip().lower()
        if bool(expected_backend) != bool(expected_fingerprint):
            return None, {"type": "p115_snapshot_provider_identity_missing"}
        if expected_backend and expected_backend not in {"openapi", "cookie"}:
            return None, {"type": "p115_snapshot_provider_backend_invalid"}
        if expected_fingerprint and (
            len(expected_fingerprint) != 64 or any(value not in "0123456789abcdef" for value in expected_fingerprint)
        ):
            return None, {"type": "p115_snapshot_credential_identity_invalid"}
        kwargs: dict[str, Any] = {"expected_account_id": snapshot_account or next(iter(accounts))}
        if expected_backend:
            kwargs["expected_backend"] = expected_backend
        if expected_fingerprint:
            kwargs["expected_credential_fingerprint"] = expected_fingerprint
        try:
            return self._provider.open(**kwargs), None
        except P115MaintenanceTransportError as exc:
            return None, {"type": "p115_credential_revision_changed", "message": str(exc)}


class LegacyCleanupMediaGateway:
    def __init__(
        self,
        gateway: LegacyP115MediaCleanupReceiptGateway | None = None,
        *,
        local_only: bool = False,
    ) -> None:
        self._gateway = gateway or LegacyP115MediaCleanupReceiptGateway()
        self._local_only = local_only

    def reconcile(self, target: Mapping[str, Any], *, operation_id: str) -> CleanupEffectResult:
        result = self._gateway.cleanup(
            item_id=str(target.get("emby_id") or ""),
            item_name=str(target.get("title") or ""),
            item_type=str(target.get("item_type") or ""),
            series_id=str(target.get("series_id") or ""),
            local_only=self._local_only,
            operation_id=operation_id,
            target=target,
        )
        if result.get("ok") is True:
            return CleanupEffectResult.confirmed(dict(result))
        if result.get("indeterminate") is True:
            return CleanupEffectResult.indeterminate(
                {
                    "type": "media_cleanup_receipt_indeterminate",
                    "message": str(result.get("error") or ""),
                },
                dict(result),
            )
        return CleanupEffectResult.retryable(
            {"type": "media_cleanup_receipt_unconfirmed", "message": str(result.get("error") or "")},
            dict(result),
        )


class LegacyCleanupP115Gateway:
    def __init__(
        self,
        credentials: P115CredentialStore | None = None,
    ) -> None:
        self._credentials = credentials or LegacyP115CredentialStore()

    def request(self, target: Mapping[str, Any]) -> CleanupEffectResult:
        snapshot = _p115_snapshot(target)
        if snapshot["enabled"] is False:
            return CleanupEffectResult.confirmed({"skipped": True, "reason": "sync_delete_disabled"})
        nodes = snapshot["nodes"]
        if not nodes:
            return CleanupEffectResult.confirmed({"skipped": True, "reason": "no_remote_identity"})
        account_error = self._account_error(nodes)
        if account_error is not None:
            return CleanupEffectResult.indeterminate(account_error)
        expected_account = next((str(node.get("account_id") or "") for node in nodes if node.get("account_id")), "")
        from handler.p115_service import P115Service, _p115_delete_nodes_with_reconciliation

        client = P115Service.get_client(expected_account_id=expected_account or None)
        if client is None:
            return CleanupEffectResult.retryable({"type": "p115_delete_client_unavailable"})
        node_parent_map = {str(node["fid"]): str(node.get("parent_id") or "") for node in nodes}
        if _p115_delete_nodes_with_reconciliation(client, node_parent_map, set(node_parent_map)):
            return CleanupEffectResult.confirmed(
                {"confirmed": True, "fids": sorted(node_parent_map), "evidence": "exact_fid_reconciliation"}
            )
        return CleanupEffectResult.retryable(
            {"type": "p115_delete_unconfirmed", "remaining": len(nodes)},
            {"fids": sorted(node_parent_map)},
        )

    def readback(self, target: Mapping[str, Any]) -> CleanupEffectResult:
        snapshot = _p115_snapshot(target)
        if snapshot["enabled"] is False:
            return CleanupEffectResult.confirmed({"skipped": True, "reason": "sync_delete_disabled"})
        nodes = snapshot["nodes"]
        if not nodes:
            return CleanupEffectResult.confirmed({"skipped": True, "reason": "no_remote_identity"})
        account_error = self._account_error(nodes)
        if account_error is not None:
            return CleanupEffectResult.indeterminate(account_error)
        expected_account = next((str(node.get("account_id") or "") for node in nodes if node.get("account_id")), "")
        from handler.p115_service import P115Service, _p115_remote_node_exists

        client = P115Service.get_client(expected_account_id=expected_account or None)
        if client is None:
            return CleanupEffectResult.retryable({"type": "p115_readback_client_unavailable"})
        states: dict[str, bool | None] = {}
        for node in nodes:
            fid = str(node.get("fid") or "")
            states[fid] = _p115_remote_node_exists(client, fid, str(node.get("parent_id") or ""))
        if all(value is False for value in states.values()):
            return CleanupEffectResult.confirmed({"presence": states, "evidence": "authoritative_absence"})
        if any(value is None for value in states.values()):
            return CleanupEffectResult.retryable(
                {"type": "p115_readback_unknown"},
                {"presence": states},
            )
        return CleanupEffectResult.retryable(
            {"type": "p115_target_still_present"},
            {"presence": states},
        )

    def _account_error(self, nodes: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
        if any(not str(node.get("account_id") or "").strip() for node in nodes):
            return {"type": "p115_snapshot_account_identity_missing"}
        expected = {str(node.get("account_id") or "").strip() for node in nodes if node.get("account_id")}
        if len(expected) > 1:
            return {"type": "p115_snapshot_has_multiple_accounts"}
        if not expected:
            return None
        current = self._credentials.load().account_id
        if current not in expected:
            return {"type": "p115_account_revision_changed"}
        return None


class ConfiguredCleanupEmbyGateway:
    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def request(self, target: Mapping[str, Any]) -> CleanupEffectResult:
        settings, error = self._validated_settings(target)
        if error is not None:
            return CleanupEffectResult.indeterminate(error)
        local = _mapping(target.get("local"), "cleanup local snapshot")
        files = local.get("files")
        if not isinstance(files, list):
            return CleanupEffectResult.indeterminate({"type": "local_snapshot_invalid"})
        paths = [str(item.get("path") or "") for item in files if isinstance(item, Mapping) and item.get("path")]
        try:
            with self._client_factory() as client:
                response = client.post(
                    f"{settings.emby_server_url.rstrip('/')}/Library/Media/Updated",
                    headers={"X-Emby-Token": settings.emby_api_key.get_secret_value()},
                    json={"Updates": [{"Path": path, "UpdateType": "Deleted"} for path in paths]},
                )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            return CleanupEffectResult.retryable({"type": "emby_delete_notification_failed", "message": str(exc)})
        revision_error = self._revision_error(target)
        if revision_error is not None:
            return CleanupEffectResult.indeterminate(revision_error)
        if response.status_code not in {200, 204}:
            return CleanupEffectResult.retryable(
                {"type": "emby_delete_notification_http", "status_code": response.status_code}
            )
        return self._readback(settings, target, notification_accepted=True)

    def readback(self, target: Mapping[str, Any]) -> CleanupEffectResult:
        settings, error = self._validated_settings(target)
        if error is not None:
            return CleanupEffectResult.indeterminate(error)
        return self._readback(settings, target, notification_accepted=False)

    def _readback(
        self,
        settings: Settings,
        target: Mapping[str, Any],
        *,
        notification_accepted: bool,
    ) -> CleanupEffectResult:
        item_id = str(target.get("emby_id") or "").strip()
        if not item_id:
            return CleanupEffectResult.indeterminate({"type": "emby_item_identity_missing"})
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{settings.emby_server_url.rstrip('/')}/Items/{quote(item_id, safe='')}",
                    headers={"X-Emby-Token": settings.emby_api_key.get_secret_value()},
                )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            return CleanupEffectResult.retryable(
                {"type": "emby_delete_readback_failed", "message": str(exc)},
                {"notification_accepted": notification_accepted},
            )
        revision_error = self._revision_error(target)
        if revision_error is not None:
            return CleanupEffectResult.indeterminate(
                revision_error,
                {"notification_accepted": notification_accepted},
            )
        if response.status_code == 404:
            return CleanupEffectResult.confirmed({"item_absent": True, "notification_accepted": notification_accepted})
        if response.status_code == 200:
            try:
                payload = response.json()
            except ValueError as exc:
                return CleanupEffectResult.indeterminate(
                    {"type": "emby_delete_readback_invalid_json", "message": str(exc)},
                    {"notification_accepted": notification_accepted},
                )
            if not isinstance(payload, Mapping) or str(payload.get("Id") or payload.get("id") or "").strip() != item_id:
                return CleanupEffectResult.indeterminate(
                    {"type": "emby_delete_readback_identity_mismatch"},
                    {"notification_accepted": notification_accepted},
                )
            return CleanupEffectResult.retryable(
                {"type": "emby_item_still_present"},
                {"item_absent": False, "notification_accepted": notification_accepted},
            )
        return CleanupEffectResult.retryable(
            {"type": "emby_delete_readback_http", "status_code": response.status_code},
            {"notification_accepted": notification_accepted},
        )

    def _validated_settings(self, target: Mapping[str, Any]) -> tuple[Settings, Mapping[str, Any] | None]:
        settings = self._settings()
        snapshot = _mapping(target.get("emby"), "cleanup Emby snapshot")
        expected_revision = str(snapshot.get("authorization_revision") or "")
        if not expected_revision:
            return settings, {"type": "emby_authorization_missing_at_submission"}
        if expected_revision != emby_authorization_revision(settings):
            return settings, {"type": "emby_authorization_revision_changed"}
        return settings, None

    def _revision_error(self, target: Mapping[str, Any]) -> Mapping[str, Any] | None:
        _settings, error = self._validated_settings(target)
        return error

    def _default_client(self) -> httpx.Client:
        return httpx.Client(timeout=float(self._settings().emby_api_timeout))


class CleanupLocalIdentityChanged(RuntimeError):
    pass


def _absolute_root(value: str) -> str:
    raw = os.path.expanduser(str(value or "").strip())
    if not raw or not os.path.isabs(raw):
        raise ValueError("cleanup local root must be an absolute path")
    root = os.path.abspath(raw)
    if os.path.lexists(root) and stat.S_ISLNK(os.lstat(root).st_mode):
        raise ValueError("cleanup local root must not be a symlink")
    return root


def _freeze_local_file(root: str, value: str) -> dict[str, Any]:
    path = os.path.abspath(os.path.expanduser(value))
    _require_within(root, path)
    _reject_symlink_chain(root, path)
    frozen = _freeze_path(path, root)
    sidecars: list[dict[str, Any]] = []
    if frozen["exists"]:
        base = Path(path).stem
        try:
            candidates = tuple(Path(path).parent.iterdir())
        except OSError as exc:
            raise ValueError(f"unable to freeze cleanup sidecars for {path}: {exc}") from exc
        for candidate in candidates:
            if candidate == Path(path) or candidate.suffix.lower() not in _SIDECAR_EXTENSIONS:
                continue
            if candidate.name != base and not candidate.name.startswith(f"{base}."):
                continue
            candidate_path = os.path.abspath(str(candidate))
            _require_within(root, candidate_path)
            _reject_symlink_chain(root, candidate_path)
            sidecars.append(_freeze_path(candidate_path, root))
    frozen["sidecars"] = sorted(sidecars, key=lambda item: str(item["path"]))
    return frozen


def _freeze_path(path: str, root: str) -> dict[str, Any]:
    if not os.path.lexists(path):
        return {
            "path": path,
            "relative_path": os.path.relpath(path, root),
            "exists": False,
            "identity": None,
        }
    current = os.lstat(path)
    if stat.S_ISLNK(current.st_mode) or not stat.S_ISREG(current.st_mode):
        raise ValueError(f"cleanup target is not a regular non-symlink file: {path}")
    return {
        "path": path,
        "relative_path": os.path.relpath(path, root),
        "exists": True,
        "identity": {
            "device": int(current.st_dev),
            "inode": int(current.st_ino),
            "size": int(current.st_size),
            "mtime_ns": int(current.st_mtime_ns),
        },
    }


def _reconcile_frozen_path(root: str, frozen: Mapping[str, Any]) -> dict[str, list[str]]:
    path = os.path.abspath(str(frozen.get("path") or ""))
    _require_within(root, path)
    relative_path = str(frozen.get("relative_path") or "").strip().replace("\\", "/")
    parts = _safe_relative_parts(relative_path)
    if os.path.abspath(os.path.join(root, *parts)) != path:
        raise CleanupLocalIdentityChanged(f"cleanup target path identity changed: {path}")
    expected_exists = frozen.get("exists") is True
    expected = frozen.get("identity")
    descriptors: list[int] = []
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        try:
            root_descriptor = os.open(root, flags)
        except OSError as exc:
            raise CleanupLocalIdentityChanged(f"cleanup root is unavailable or unsafe: {root}") from exc
        descriptors.append(root_descriptor)
        parent_descriptor = root_descriptor
        for component in parts[:-1]:
            try:
                parent_descriptor = os.open(component, flags, dir_fd=parent_descriptor)
            except FileNotFoundError:
                return {"deleted": [], "absent": [path]}
            except OSError as exc:
                raise CleanupLocalIdentityChanged(f"cleanup parent path became unsafe: {path}") from exc
            descriptors.append(parent_descriptor)

        quarantine = f".etk-cleanup-delete-{secrets.token_hex(12)}"
        quarantine_descriptor: int | None = None
        source_quarantined = False
        try:
            os.mkdir(quarantine, mode=0o700, dir_fd=parent_descriptor)
            quarantine_descriptor = os.open(quarantine, flags, dir_fd=parent_descriptor)
            try:
                os.rename(
                    parts[-1],
                    "source",
                    src_dir_fd=parent_descriptor,
                    dst_dir_fd=quarantine_descriptor,
                )
            except FileNotFoundError:
                return {"deleted": [], "absent": [path]}
            source_quarantined = True
            current = os.stat("source", dir_fd=quarantine_descriptor, follow_symlinks=False)
            identity = {
                "device": int(current.st_dev),
                "inode": int(current.st_ino),
                "size": int(current.st_size),
                "mtime_ns": int(current.st_mtime_ns),
            }
            if not (
                expected_exists
                and isinstance(expected, Mapping)
                and stat.S_ISREG(current.st_mode)
                and not stat.S_ISLNK(current.st_mode)
                and identity == dict(expected)
                and _root_descriptor_is_attached(root, root_descriptor)
                and _parent_descriptor_is_attached(root_descriptor, parent_descriptor, parts[:-1])
            ):
                if not expected_exists:
                    raise CleanupLocalIdentityChanged(f"a new file appeared after Cleanup staging: {path}")
                raise CleanupLocalIdentityChanged(f"cleanup target identity changed: {path}")
            os.unlink("source", dir_fd=quarantine_descriptor)
            source_quarantined = False
            os.fsync(quarantine_descriptor)
            os.fsync(parent_descriptor)
            return {"deleted": [path], "absent": []}
        except BaseException:
            if source_quarantined and quarantine_descriptor is not None:
                _restore_quarantined_cleanup_file(
                    quarantine_descriptor,
                    parent_descriptor,
                    parts[-1],
                    quarantine,
                    path,
                )
                source_quarantined = False
            raise
        finally:
            if quarantine_descriptor is not None:
                os.close(quarantine_descriptor)
            if not source_quarantined:
                try:
                    os.rmdir(quarantine, dir_fd=parent_descriptor)
                except FileNotFoundError:
                    pass
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _safe_relative_parts(relative_path: str) -> tuple[str, ...]:
    parts = tuple(relative_path.split("/"))
    if (
        not relative_path
        or relative_path.startswith("/")
        or not parts
        or any(part in {"", ".", ".."} or "\x00" in part for part in parts)
    ):
        raise CleanupLocalIdentityChanged("cleanup relative path is invalid")
    return parts


def _root_descriptor_is_attached(root: str, descriptor: int) -> bool:
    try:
        observed = os.stat(root, follow_symlinks=False)
        opened = os.fstat(descriptor)
    except OSError:
        return False
    return (
        stat.S_ISDIR(observed.st_mode)
        and not stat.S_ISLNK(observed.st_mode)
        and (observed.st_dev, observed.st_ino) == (opened.st_dev, opened.st_ino)
    )


def _parent_descriptor_is_attached(root: int, parent: int, components: Sequence[str]) -> bool:
    current = os.dup(root)
    try:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        for component in components:
            child = os.open(component, flags, dir_fd=current)
            os.close(current)
            current = child
        expected = os.fstat(current)
        observed = os.fstat(parent)
        return (expected.st_dev, expected.st_ino) == (observed.st_dev, observed.st_ino)
    except OSError:
        return False
    finally:
        os.close(current)


def _restore_quarantined_cleanup_file(
    quarantine_descriptor: int,
    parent: int,
    name: str,
    quarantine: str,
    path: str,
) -> None:
    try:
        os.link(
            "source",
            name,
            src_dir_fd=quarantine_descriptor,
            dst_dir_fd=parent,
            follow_symlinks=False,
        )
    except FileExistsError as exc:
        raise CleanupLocalIdentityChanged(
            f"cleanup target changed during atomic quarantine; retained as {quarantine}: {path}"
        ) from exc
    except OSError as exc:
        raise CleanupLocalIdentityChanged(
            f"cleanup target cannot be safely restored; retained as {quarantine}: {path}"
        ) from exc
    os.unlink("source", dir_fd=quarantine_descriptor)
    os.fsync(quarantine_descriptor)
    os.fsync(parent)


def _prune_empty_parents(root: str, start: str) -> None:
    current = os.path.abspath(start)
    while current != root:
        _require_within(root, current)
        if os.path.islink(current):
            raise CleanupLocalIdentityChanged(f"cleanup parent became a symlink: {current}")
        try:
            os.rmdir(current)
        except FileNotFoundError:
            pass
        except OSError:
            break
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent


def _reject_symlink_chain(root: str, path: str) -> None:
    relative = os.path.relpath(path, root)
    current = root
    for component in Path(relative).parts:
        current = os.path.join(current, component)
        if os.path.lexists(current) and stat.S_ISLNK(os.lstat(current).st_mode):
            raise ValueError(f"cleanup target traverses a symlink: {current}")


def _require_within(root: str, path: str) -> None:
    try:
        common = os.path.commonpath((root, path))
    except ValueError as exc:
        raise ValueError("cleanup path and root are on different filesystems") from exc
    if common != root or path == root:
        raise ValueError(f"cleanup path is outside the configured root: {path}")


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field} must be an object")
    return value


def _cleanup_shared_source_ids(
    uow: SqlAlchemyUnitOfWork,
    target: Mapping[str, Any],
    *,
    scope_offline: bool,
) -> tuple[int, ...]:
    sha1s: set[str] = set()
    pick_codes: set[str] = set()
    local = target.get("local")
    if isinstance(local, Mapping) and isinstance(local.get("files"), list):
        for value in local["files"]:
            if not isinstance(value, Mapping):
                continue
            sha1 = str(value.get("sha1") or "").strip().upper()
            pick_code = str(value.get("pick_code") or "").strip().casefold()
            if sha1:
                sha1s.add(sha1)
            if pick_code:
                pick_codes.add(pick_code)
    p115 = target.get("p115")
    if isinstance(p115, Mapping) and isinstance(p115.get("nodes"), list):
        for value in p115["nodes"]:
            if not isinstance(value, Mapping):
                continue
            sha1 = str(value.get("sha1") or "").strip().upper()
            pick_code = str(value.get("pick_code") or "").strip().casefold()
            if sha1:
                sha1s.add(sha1)
            if pick_code:
                pick_codes.add(pick_code)
    if not sha1s and not pick_codes and not scope_offline:
        return ()

    tmdb_id = str(target.get("tmdb_id") or "").strip()
    series_id = str(target.get("series_id") or "").strip()
    item_type = str(target.get("item_type") or "").strip().casefold()
    season_number = _optional_int(target.get("season_number"))
    episode_number = _optional_int(target.get("episode_number"))
    rows = uow.session.scalars(
        text(
            """
            SELECT DISTINCT source.id
            FROM shared_rapid_sources AS source
            LEFT JOIN shared_rapid_source_files AS file
              ON file.local_source_id = source.id
            WHERE NOT (
                    lower(COALESCE(source.status, '')) IN ('disabled', 'cancelled', 'canceled', 'deleted')
                 OR lower(COALESCE(source.center_status, '')) = 'disabled'
            )
              AND (
                    upper(COALESCE(source.sha1, '')) = ANY(CAST(:sha1s AS text[]))
                 OR upper(COALESCE(file.sha1, '')) = ANY(CAST(:sha1s AS text[]))
                 OR lower(COALESCE(file.pick_code, '')) = ANY(CAST(:pick_codes AS text[]))
                 OR (
                        :scope_offline IS TRUE
                    AND (
                           (:item_type = 'movie'
                            AND source.tmdb_id = :tmdb_id
                            AND lower(COALESCE(source.item_type, '')) = 'movie')
                        OR (:item_type = 'series'
                            AND source.tmdb_id = :tmdb_id)
                        OR (:item_type IN ('season', 'episode')
                            AND source.tmdb_id = ANY(CAST(:series_ids AS text[]))
                            AND (CAST(:season_number AS integer) IS NULL
                                 OR source.season_number = CAST(:season_number AS integer))
                            AND (:item_type <> 'episode'
                                 OR CAST(:episode_number AS integer) IS NULL
                                 OR source.episode_number = CAST(:episode_number AS integer)))
                        OR (:item_type NOT IN ('movie', 'series', 'season', 'episode')
                            AND source.tmdb_id = :tmdb_id
                            AND lower(COALESCE(source.item_type, '')) = :item_type)
                    )
                 )
              )
            ORDER BY source.id
            """
        ),
        {
            "sha1s": sorted(sha1s),
            "pick_codes": sorted(pick_codes),
            "scope_offline": bool(scope_offline),
            "item_type": item_type,
            "tmdb_id": tmdb_id,
            "series_ids": list(dict.fromkeys(value for value in (series_id, tmdb_id) if value)),
            "season_number": season_number,
            "episode_number": episode_number,
        },
    ).all()
    return tuple(int(value) for value in rows)


def _optional_int(value: object) -> int | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _aligned_media_versions(
    row: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[str], list[str], list[str]] | None:
    raw_assets = row.get("asset_details_json")
    raw_emby_ids = row.get("emby_item_ids_json")
    raw_sha1s = row.get("file_sha1_json")
    raw_pick_codes = row.get("file_pickcode_json")
    if not all(isinstance(value, list) for value in (raw_assets, raw_emby_ids, raw_sha1s, raw_pick_codes)):
        return None
    assets_raw = cast(list[Any], raw_assets)
    emby_ids_raw = cast(list[Any], raw_emby_ids)
    sha1s_raw = cast(list[Any], raw_sha1s)
    pick_codes_raw = cast(list[Any], raw_pick_codes)
    assets = [dict(value) for value in assets_raw if isinstance(value, Mapping)]
    if len(assets) != len(assets_raw):
        return None
    emby_ids = [str(value or "").strip() for value in emby_ids_raw]
    sha1s = [str(value or "").strip().upper() for value in sha1s_raw]
    pick_codes = [str(value or "").strip() for value in pick_codes_raw]
    if len({len(assets), len(emby_ids), len(sha1s), len(pick_codes)}) != 1:
        return None
    return assets, emby_ids, sha1s, pick_codes


def _frozen_media_version_matches(target: Mapping[str, Any], sha1: str, pick_code: str) -> bool:
    local = target.get("local")
    if not isinstance(local, Mapping):
        return True
    raw_files = local.get("files")
    if not isinstance(raw_files, list):
        return True
    frozen = [
        (
            str(value.get("sha1") or "").strip().upper(),
            str(value.get("pick_code") or "").strip().casefold(),
        )
        for value in raw_files
        if isinstance(value, Mapping)
        and (str(value.get("sha1") or "").strip() or str(value.get("pick_code") or "").strip())
    ]
    if not frozen:
        return True
    current = (str(sha1 or "").strip().upper(), str(pick_code or "").strip().casefold())
    return len(frozen) == 1 and all(
        not expected or expected == observed for expected, observed in zip(frozen[0], current, strict=True)
    )


def _reconcile_offline_media_scope(
    uow: SqlAlchemyUnitOfWork,
    target: Mapping[str, Any],
) -> dict[str, int]:
    tmdb_id = str(target.get("tmdb_id") or "").strip()
    item_type = str(target.get("item_type") or "").strip()
    series_id = str(target.get("series_id") or "").strip()
    season_number = _optional_int(target.get("season_number"))
    episode_number = _optional_int(target.get("episode_number"))
    if not tmdb_id or not item_type:
        return {}

    stats = {
        "offline_rows_updated": 0,
        "offline_cleanup_indexes_deleted": 0,
        "resubscribe_rows_deleted": 0,
        "collections_deleted": 0,
        "scope_virtual_imports_deleted": 0,
        "scope_virtual_cache_deleted": 0,
    }
    if item_type in {"Movie", "Series"}:
        stats["offline_rows_updated"] += _mark_media_rows_offline(
            uow,
            "tmdb_id = :tmdb_id AND item_type = :item_type",
            {"tmdb_id": tmdb_id, "item_type": item_type},
        )

    cascade_series = item_type == "Series"
    if item_type == "Season" and series_id and season_number is not None:
        stats["offline_rows_updated"] += _mark_media_rows_offline(
            uow,
            "parent_series_tmdb_id = :series_id AND season_number = :season_number "
            "AND item_type IN ('Season', 'Episode')",
            {"series_id": series_id, "season_number": season_number},
        )
        stats["offline_cleanup_indexes_deleted"] += _delete_cleanup_indexes_for_media_filter(
            uow,
            "media.parent_series_tmdb_id = :series_id "
            "AND media.season_number = :season_number "
            "AND media.item_type IN ('Season', 'Episode')",
            {"series_id": series_id, "season_number": season_number},
        )
        stats["resubscribe_rows_deleted"] += _delete_resubscribe_season(uow, series_id, season_number)
        cascade_series = not _has_live_episode(uow, series_id)
    elif item_type == "Episode" and series_id:
        if season_number is not None and not _has_live_episode(uow, series_id, season_number=season_number):
            stats["offline_rows_updated"] += _mark_media_rows_offline(
                uow,
                "parent_series_tmdb_id = :series_id AND season_number = :season_number AND item_type = 'Season'",
                {"series_id": series_id, "season_number": season_number},
            )
            stats["offline_cleanup_indexes_deleted"] += _delete_cleanup_indexes_for_media_filter(
                uow,
                "media.parent_series_tmdb_id = :series_id "
                "AND media.season_number = :season_number "
                "AND media.item_type IN ('Season', 'Episode')",
                {"series_id": series_id, "season_number": season_number},
            )
            stats["resubscribe_rows_deleted"] += _delete_resubscribe_season(
                uow,
                series_id,
                season_number,
            )
        cascade_series = not _has_live_episode(uow, series_id)

    if cascade_series:
        root_id = tmdb_id if item_type == "Series" else series_id
        if root_id:
            stats["offline_rows_updated"] += _mark_media_rows_offline(
                uow,
                "(tmdb_id = :series_id AND item_type = 'Series') "
                "OR (parent_series_tmdb_id = :series_id AND item_type IN ('Season', 'Episode'))",
                {"series_id": root_id},
            )
            stats["offline_cleanup_indexes_deleted"] += _delete_cleanup_indexes_for_media_filter(
                uow,
                "(media.tmdb_id = :series_id AND media.item_type = 'Series') "
                "OR (media.parent_series_tmdb_id = :series_id "
                "AND media.item_type IN ('Season', 'Episode'))",
                {"series_id": root_id},
            )
            result = uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET watching_status = 'NONE', last_updated_at = NOW()
                    WHERE (
                           (tmdb_id = :series_id AND item_type = 'Series')
                        OR (parent_series_tmdb_id = :series_id AND item_type = 'Season')
                    )
                      AND watching_status NOT IN ('NONE', 'Completed')
                    """
                ),
                {"series_id": root_id},
            )
            stats["offline_rows_updated"] += max(int(getattr(result, "rowcount", 0) or 0), 0)
            result = uow.session.execute(
                text("DELETE FROM resubscribe_index WHERE tmdb_id = :tmdb_id AND item_type = 'Season'"),
                {"tmdb_id": root_id},
            )
            stats["resubscribe_rows_deleted"] += max(int(getattr(result, "rowcount", 0) or 0), 0)
    elif item_type == "Movie":
        result = uow.session.execute(
            text("DELETE FROM resubscribe_index WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'"),
            {"tmdb_id": tmdb_id},
        )
        stats["resubscribe_rows_deleted"] += max(int(getattr(result, "rowcount", 0) or 0), 0)
        result = uow.session.execute(
            text(
                """
                DELETE FROM collections_info AS collection
                WHERE collection.is_active IS TRUE
                  AND jsonb_typeof(collection.all_tmdb_ids_json) = 'array'
                  AND collection.all_tmdb_ids_json ? :tmdb_id
                  AND NOT EXISTS (
                      SELECT 1
                      FROM jsonb_array_elements_text(collection.all_tmdb_ids_json) AS member(tmdb_id)
                      JOIN media_metadata AS media
                        ON media.tmdb_id = member.tmdb_id
                       AND media.in_library IS TRUE
                  )
                """
            ),
            {"tmdb_id": tmdb_id},
        )
        stats["collections_deleted"] += max(int(getattr(result, "rowcount", 0) or 0), 0)

    virtual = _reconcile_virtual_import_scope(
        uow,
        tmdb_id=tmdb_id,
        item_type=item_type,
        series_id=series_id,
        season_number=season_number,
        episode_number=episode_number,
    )
    stats.update(virtual)
    return stats


def _mark_media_rows_offline(
    uow: SqlAlchemyUnitOfWork,
    predicate: str,
    parameters: Mapping[str, Any],
) -> int:
    result = uow.session.execute(
        text(
            f"""
            UPDATE media_metadata
            SET in_library = FALSE,
                emby_item_ids_json = '[]'::jsonb,
                asset_details_json = NULL,
                file_sha1_json = '[]'::jsonb,
                file_pickcode_json = '[]'::jsonb,
                washing_level = NULL,
                washing_snapshot_json = '{{}}'::jsonb,
                last_updated_at = NOW()
            WHERE {predicate}
            """
        ),
        dict(parameters),
    )
    return max(int(getattr(result, "rowcount", 0) or 0), 0)


def _delete_cleanup_indexes_for_media_filter(
    uow: SqlAlchemyUnitOfWork,
    predicate: str,
    parameters: Mapping[str, Any],
) -> int:
    result = uow.session.execute(
        text(
            f"""
            DELETE FROM cleanup_index AS cleanup
            USING media_metadata AS media
            WHERE cleanup.tmdb_id = media.tmdb_id
              AND cleanup.item_type = media.item_type
              AND cleanup.status IN ('pending', 'ignored')
              AND ({predicate})
            """
        ),
        dict(parameters),
    )
    return max(int(getattr(result, "rowcount", 0) or 0), 0)


def _has_live_episode(
    uow: SqlAlchemyUnitOfWork,
    series_id: str,
    *,
    season_number: int | None = None,
) -> bool:
    return bool(
        uow.session.scalar(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM media_metadata
                    WHERE parent_series_tmdb_id = :series_id
                      AND item_type = 'Episode'
                      AND in_library IS TRUE
                      AND (CAST(:season_number AS integer) IS NULL
                           OR season_number = CAST(:season_number AS integer))
                )
                """
            ),
            {"series_id": series_id, "season_number": season_number},
        )
    )


def _delete_resubscribe_season(
    uow: SqlAlchemyUnitOfWork,
    series_id: str,
    season_number: int,
) -> int:
    result = uow.session.execute(
        text(
            """
            DELETE FROM resubscribe_index
            WHERE tmdb_id = :tmdb_id
              AND item_type = 'Season'
              AND season_number = :season_number
            """
        ),
        {"tmdb_id": series_id, "season_number": season_number},
    )
    return max(int(getattr(result, "rowcount", 0) or 0), 0)


def _reconcile_virtual_import_scope(
    uow: SqlAlchemyUnitOfWork,
    *,
    tmdb_id: str,
    item_type: str,
    series_id: str,
    season_number: int | None,
    episode_number: int | None,
) -> dict[str, int]:
    rows = uow.session.scalars(
        text(
            """
            SELECT id
            FROM shared_virtual_imports
            WHERE (
                   (:item_type IN ('Movie', 'Video')
                    AND tmdb_id = :tmdb_id AND item_type = :item_type)
                OR (:item_type = 'Series'
                    AND (tmdb_id = :tmdb_id OR parent_series_tmdb_id = :tmdb_id))
                OR (:item_type = 'Season'
                    AND parent_series_tmdb_id = :series_id
                    AND season_number = CAST(:season_number AS integer))
                OR (:item_type = 'Episode'
                    AND (
                           tmdb_id = :tmdb_id
                        OR (parent_series_tmdb_id = :series_id
                            AND season_number = CAST(:season_number AS integer)
                            AND episode_number = CAST(:episode_number AS integer))
                    ))
            )
            ORDER BY id
            FOR UPDATE
            """
        ),
        {
            "tmdb_id": tmdb_id,
            "item_type": item_type,
            "series_id": series_id,
            "season_number": season_number,
            "episode_number": episode_number,
        },
    ).all()
    cache_deleted = 0
    imports_deleted = 0
    for virtual_id in (int(value) for value in rows):
        result = uow.session.execute(
            text(
                """
                DELETE FROM p115_filesystem_cache
                WHERE parent_id = :parent_id OR id LIKE :id_pattern
                """
            ),
            {"parent_id": f"virtual:{virtual_id}", "id_pattern": f"virtual:{virtual_id}:%"},
        )
        cache_deleted += max(int(getattr(result, "rowcount", 0) or 0), 0)
        result = uow.session.execute(
            text("DELETE FROM shared_virtual_imports WHERE id = :id"),
            {"id": virtual_id},
        )
        imports_deleted += max(int(getattr(result, "rowcount", 0) or 0), 0)
    return {
        "scope_virtual_imports_deleted": imports_deleted,
        "scope_virtual_cache_deleted": cache_deleted,
    }


def _reconcile_cleanup_indexes(
    uow: SqlAlchemyUnitOfWork,
    tmdb_id: str,
    item_type: str,
    emby_id: str,
) -> dict[str, int]:
    rows = (
        uow.session.execute(
            text(
                """
                SELECT id, versions_info_json, best_version_json
                FROM cleanup_index
                WHERE tmdb_id = :tmdb_id
                  AND item_type = :item_type
                  AND status IN ('pending', 'ignored')
                ORDER BY id
                FOR UPDATE
                """
            ),
            {"tmdb_id": tmdb_id, "item_type": item_type},
        )
        .mappings()
        .all()
    )
    deleted = 0
    updated = 0
    for row in rows:
        raw_versions = row.get("versions_info_json")
        if not isinstance(raw_versions, list) or any(not isinstance(value, Mapping) for value in raw_versions):
            raise RuntimeError("cleanup index versions are invalid")
        versions = [dict(value) for value in raw_versions if isinstance(value, Mapping)]
        remaining = [
            value for value in versions if str(value.get("id") or value.get("emby_item_id") or "").strip() != emby_id
        ]
        if len(remaining) == len(versions):
            continue
        if len(remaining) <= 1:
            result = uow.session.execute(
                text("DELETE FROM cleanup_index WHERE id = :id"),
                {"id": int(row["id"])},
            )
            deleted += max(int(getattr(result, "rowcount", 0) or 0), 0)
        else:
            best_version = _reconciled_best_version(row.get("best_version_json"), remaining)
            result = uow.session.execute(
                text(
                    """
                    UPDATE cleanup_index
                    SET versions_info_json = CAST(:versions AS jsonb),
                        best_version_json = CAST(:best_version AS jsonb),
                        last_updated_at = NOW()
                    WHERE id = :id
                    """
                ),
                {
                    "id": int(row["id"]),
                    "versions": json.dumps(remaining, ensure_ascii=False),
                    "best_version": json.dumps(best_version, ensure_ascii=False),
                },
            )
            updated += max(int(getattr(result, "rowcount", 0) or 0), 0)
    return {"cleanup_indexes_deleted": deleted, "cleanup_indexes_updated": updated}


def _reconciled_best_version(raw_best: object, remaining: Sequence[Mapping[str, Any]]) -> str | list[str]:
    remaining_ids = [value for value in (_cleanup_version_id(item) for item in remaining) if value]
    if not remaining_ids:
        raise RuntimeError("cleanup index has no remaining version identity")
    raw_ids = raw_best if isinstance(raw_best, list) else [raw_best]
    surviving = [str(value or "").strip() for value in raw_ids if str(value or "").strip() in remaining_ids]
    if not surviving:
        surviving = [remaining_ids[0]]
    return surviving if isinstance(raw_best, list) else surviving[0]


def _cleanup_version_id(value: Mapping[str, Any]) -> str:
    return str(value.get("id") or value.get("emby_item_id") or "").strip()


def _reconcile_virtual_imports(
    uow: SqlAlchemyUnitOfWork,
    removed_asset: Mapping[str, Any],
) -> dict[str, int]:
    deleted_path = _normalized_cleanup_path(removed_asset.get("path") or removed_asset.get("Path"))
    if not deleted_path:
        return {"virtual_imports_deleted": 0, "virtual_cache_deleted": 0}
    rows = (
        uow.session.execute(text("SELECT id, strm_paths_json FROM shared_virtual_imports ORDER BY id FOR UPDATE"))
        .mappings()
        .all()
    )
    virtual_ids = [
        int(row["id"])
        for row in rows
        if isinstance(row.get("strm_paths_json"), list)
        and deleted_path
        in {_normalized_cleanup_path(value) for value in row["strm_paths_json"] if str(value or "").strip()}
    ]
    cache_deleted = 0
    imports_deleted = 0
    for virtual_id in virtual_ids:
        result = uow.session.execute(
            text(
                """
                DELETE FROM p115_filesystem_cache
                WHERE parent_id = :parent_id
                   OR id LIKE :id_pattern
                """
            ),
            {"parent_id": f"virtual:{virtual_id}", "id_pattern": f"virtual:{virtual_id}:%"},
        )
        cache_deleted += max(int(getattr(result, "rowcount", 0) or 0), 0)
        result = uow.session.execute(
            text("DELETE FROM shared_virtual_imports WHERE id = :id"),
            {"id": virtual_id},
        )
        imports_deleted += max(int(getattr(result, "rowcount", 0) or 0), 0)
    return {
        "virtual_imports_deleted": imports_deleted,
        "virtual_cache_deleted": cache_deleted,
    }


def _normalized_cleanup_path(value: object) -> str:
    return "/".join(part for part in str(value or "").strip().replace("\\", "/").split("/") if part).casefold()


def _p115_presence_by_parent(
    client: Any,
    nodes: Sequence[Mapping[str, Any]],
) -> dict[str, bool]:
    wanted_by_parent: dict[str, set[str]] = {}
    for node in nodes:
        fid = str(node.get("fid") or "").strip()
        parent_id = str(node.get("parent_id") or "").strip()
        if not fid or not parent_id:
            raise ValueError("cleanup 115 node requires frozen FID and parent identity")
        wanted_by_parent.setdefault(parent_id, set()).add(fid)
    presence: dict[str, bool] = {}
    for parent_id, wanted in wanted_by_parent.items():
        children = client.list_children(parent_id)
        observed = {_p115_item_id(value) for value in children if isinstance(value, Mapping)}
        if "" in observed:
            raise RuntimeError("115 directory readback returned an item without identity")
        for fid in wanted:
            presence[fid] = fid in observed
    return presence


def _p115_item_id(value: Mapping[str, Any]) -> str:
    return str(value.get("fid") or value.get("file_id") or value.get("id") or value.get("cid") or "").strip()


def _safe_provider_response(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value[key]
        for key in ("state", "success", "code", "errno", "message", "error_msg")
        if key in value and isinstance(value[key], bool | int | float | str | type(None))
    }


def _p115_snapshot(target: Mapping[str, Any]) -> dict[str, Any]:
    value = _mapping(target.get("p115"), "cleanup 115 snapshot")
    raw_nodes = value.get("nodes")
    if not isinstance(raw_nodes, list) or not all(isinstance(node, Mapping) for node in raw_nodes):
        raise TypeError("cleanup 115 nodes must be an array of objects")
    return {
        "enabled": bool(value.get("enabled")),
        "account_id": str(value.get("account_id") or "").strip(),
        "backend": str(value.get("backend") or "").strip().lower(),
        "credential_fingerprint": str(value.get("credential_fingerprint") or "").strip().lower(),
        "nodes": [dict(node) for node in raw_nodes],
    }


def _select_p115_backend(snapshot: Any, priority: object) -> str:
    preferred = str(priority or "openapi").strip().lower()
    order = ("cookie", "openapi") if preferred == "cookie" else ("openapi", "cookie")
    for backend in order:
        credential = snapshot.cookie if backend == "cookie" else snapshot.access_token
        if str(credential or "").strip():
            return backend
    return ""


def _p115_credential_fingerprint(snapshot: Any, backend: str) -> str:
    credential = snapshot.access_token if backend == "openapi" else snapshot.cookie
    normalized = str(credential or "").strip()
    if not normalized:
        return ""
    app_type = str(snapshot.app_type or "").strip() if backend == "cookie" else ""
    return hashlib.sha256(f"{backend}\0{app_type}\0{normalized}".encode()).hexdigest()


__all__ = [
    "CleanupEmbyInventoryCredentialChanged",
    "CleanupEmbyInventoryUnavailable",
    "CleanupSharedSourceLifecyclePort",
    "ConfiguredCleanupEmbyGateway",
    "HttpxCleanupEmbyInventoryGateway",
    "LegacyCleanupMediaGateway",
    "LegacyCleanupP115Gateway",
    "NativeCleanupP115Gateway",
    "SafeLocalCleanupGateway",
    "SqlAlchemyCleanupMediaGateway",
    "SettingsCleanupExecutionPolicy",
]
