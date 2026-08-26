from __future__ import annotations

import os
import secrets
import stat
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from etk.domains.media.bridge_client import emby_authorization_revision
from etk.domains.p115.adapters import LegacyP115CredentialStore
from etk.domains.p115.bridge_compatibility import (
    LegacyP115MediaCleanupReceiptGateway,
)
from etk.platform import Settings

from .cleanup_execution import CleanupEffectResult

_SIDECAR_EXTENSIONS = frozenset({".srt", ".ass", ".ssa", ".sub", ".vtt", ".sup", ".nfo", ".jpg", ".png"})


class SettingsCleanupExecutionPolicy:
    """Freeze non-secret filesystem/provider identity before a Cleanup Job is staged."""

    def __init__(self, settings: Callable[[], Settings]) -> None:
        self._settings = settings

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
                    p115_nodes.append(
                        {
                            "fid": fid,
                            "parent_id": str(p115.get("parent_id") or "").strip(),
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
        return {
            "schema_version": 1,
            "emby_id": str(identity.get("emby_id") or "").strip(),
            "tmdb_id": str(identity.get("tmdb_id") or "").strip(),
            "item_type": str(identity.get("item_type") or "").strip(),
            "title": str(identity.get("title") or ""),
            "series_id": str(identity.get("series_id") or "").strip(),
            "local": {"root": root, "files": files},
            "p115": {
                "enabled": bool(settings.p115_enable_sync_delete),
                "nodes": p115_nodes,
            },
            "emby": {"authorization_revision": revision},
        }


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
        credentials: LegacyP115CredentialStore | None = None,
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
            from handler.emby import notify_emby_file_changes

            accepted = notify_emby_file_changes(
                paths,
                settings.emby_server_url,
                settings.emby_api_key.get_secret_value(),
                "Deleted",
            )
        except Exception as exc:
            return CleanupEffectResult.retryable({"type": "emby_delete_notification_failed", "message": str(exc)})
        if accepted is not True:
            return CleanupEffectResult.retryable({"type": "emby_delete_notification_unconfirmed"})
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
        if response.status_code == 404:
            return CleanupEffectResult.confirmed({"item_absent": True, "notification_accepted": notification_accepted})
        if response.status_code == 200:
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


def _p115_snapshot(target: Mapping[str, Any]) -> dict[str, Any]:
    value = _mapping(target.get("p115"), "cleanup 115 snapshot")
    raw_nodes = value.get("nodes")
    if not isinstance(raw_nodes, list) or not all(isinstance(node, Mapping) for node in raw_nodes):
        raise TypeError("cleanup 115 nodes must be an array of objects")
    return {"enabled": bool(value.get("enabled")), "nodes": [dict(node) for node in raw_nodes]}


__all__ = [
    "ConfiguredCleanupEmbyGateway",
    "LegacyCleanupMediaGateway",
    "LegacyCleanupP115Gateway",
    "SafeLocalCleanupGateway",
    "SettingsCleanupExecutionPolicy",
]
