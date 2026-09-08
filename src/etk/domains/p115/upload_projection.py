from __future__ import annotations

import hashlib
import json
import os
import posixpath
from collections.abc import Callable, Mapping, Sequence
from pathlib import PurePosixPath
from typing import Any, Protocol

from sqlalchemy import text

from etk.platform import Settings
from etk.platform.config.locking import DYNAMIC_CONFIG_LOCK_NAMESPACE, P115_AUTH_LOCK_KEY
from etk.platform.db import SqlAlchemyUnitOfWork

from .auth_directory import P115CredentialStore
from .fingerprints import canonical_sha1
from .media_info_adapters import ControlledProcessOutput, ControlledProcessProfile, ControlledProcessRunner
from .media_info_formatter import P115MediaInfoPolicySnapshot, format_raw_ffprobe_mediainfo
from .media_probe import media_info_formatter_revision
from .rename_config import P115RenameConfigRepository
from .sorting_rules import P115SortingRulesRepository
from .strm_adapters import AtomicP115LocalStrmLibraryGateway
from .strm_sync import P115LocalStrmLibraryGateway
from .upload_adapters import _unlink_fenced
from .upload_monitor import (
    P115UploadProjection,
    UploadedSource,
    UploadOperation,
    UploadProjectionOutcome,
)

_MEDIA_EXTENSIONS = frozenset(
    {
        ".aac",
        ".ape",
        ".avi",
        ".flac",
        ".flv",
        ".iso",
        ".m2ts",
        ".m4a",
        ".mkv",
        ".mov",
        ".mp3",
        ".mp4",
        ".mpeg",
        ".mpg",
        ".ogg",
        ".opus",
        ".rmvb",
        ".ts",
        ".wav",
        ".webm",
        ".wmv",
    }
)
_FFPROBE_STDOUT_LIMIT = 16 * 1024 * 1024
_FFPROBE_STDERR_LIMIT = 256 * 1024
_FFPROBE_TIMEOUT_SECONDS = 300.0


class P115UploadMediaInfoPolicyProvider(Protocol):
    def snapshot(self) -> P115MediaInfoPolicySnapshot: ...


class P115UploadIngestionIngress(Protocol):
    def record_event(
        self,
        event_type: str,
        path: str,
        *,
        source: str,
        source_event_key: str,
    ) -> object: ...


class P115UploadProjectionRepository(Protocol):
    def target_relative_path(self, target_cid: str, *, account_id: str) -> str | None: ...

    def persist_remote_file(
        self,
        *,
        account_id: str,
        file_id: str,
        parent_id: str,
        name: str,
        sha1: str,
        pick_code: str,
        size: int,
        local_path: str,
    ) -> None: ...

    def persist_media_info(
        self,
        *,
        account_id: str,
        sha1: str,
        formatted: Sequence[Mapping[str, Any]],
        raw: Mapping[str, Any],
        formatter_revision: str,
    ) -> None: ...


class SqlAlchemyP115UploadProjectionRepository:
    """Commit one uploaded file and its locally generated MediaInfo under the auth lock."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
    ) -> None:
        self._unit_of_work = unit_of_work

    def target_relative_path(self, target_cid: str, *, account_id: str) -> str | None:
        target = str(target_cid or "").strip()
        account = str(account_id or "").strip()
        if not target or not account:
            return None
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT local_path, account_id
                        FROM p115_filesystem_cache
                        WHERE id = :target_cid AND entry_kind = 'directory'
                        """
                    ),
                    {"target_cid": target},
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        if str(row.get("account_id") or "").strip() != account:
            raise RuntimeError("115 upload target directory cache belongs to another account")
        return str(row.get("local_path") or "").strip().replace("\\", "/").strip("/")

    def persist_remote_file(
        self,
        *,
        account_id: str,
        file_id: str,
        parent_id: str,
        name: str,
        sha1: str,
        pick_code: str,
        size: int,
        local_path: str,
    ) -> None:
        account = _account_id(account_id)
        values = {
            "account_id": account,
            "file_id": _required(file_id, "file_id"),
            "parent_id": _required(parent_id, "parent_id"),
            "name": _required(name, "name"),
            "sha1": canonical_sha1(sha1),
            "pick_code": _required(pick_code, "pick_code"),
            "size": _nonnegative(size, "size"),
            "local_path": _relative_path(local_path),
        }
        with self._unit_of_work() as uow:
            self._lock_account(uow, account)
            conflict = uow.session.scalar(
                text(
                    """
                    SELECT id
                    FROM p115_filesystem_cache
                    WHERE lower(pick_code) = lower(:pick_code) AND id <> :file_id
                    LIMIT 1
                    FOR UPDATE
                    """
                ),
                values,
            )
            if conflict is not None:
                raise RuntimeError("115 upload pick-code identity conflicts with another cache row")
            updated = uow.session.execute(
                text(
                    """
                    UPDATE p115_filesystem_cache
                    SET parent_id = :parent_id,
                        name = :name,
                        local_path = :local_path,
                        sha1 = :sha1,
                        pick_code = :pick_code,
                        size = :size,
                        account_id = :account_id,
                        entry_kind = 'file',
                        updated_at = NOW()
                    WHERE id = :file_id
                      AND account_id IS NOT DISTINCT FROM :account_id
                    """
                ),
                values,
            )
            if int(getattr(updated, "rowcount", 0) or 0) == 0:
                existing_account = uow.session.scalar(
                    text("SELECT account_id FROM p115_filesystem_cache WHERE id = :file_id FOR UPDATE"),
                    values,
                )
                if existing_account is not None:
                    raise RuntimeError("115 upload file cache belongs to another account")
                uow.session.execute(
                    text(
                        """
                        INSERT INTO p115_filesystem_cache (
                            id, parent_id, name, local_path, sha1, pick_code,
                            size, account_id, entry_kind
                        ) VALUES (
                            :file_id, :parent_id, :name, :local_path, :sha1, :pick_code,
                            :size, :account_id, 'file'
                        )
                        """
                    ),
                    values,
                )
            uow.commit()

    def persist_media_info(
        self,
        *,
        account_id: str,
        sha1: str,
        formatted: Sequence[Mapping[str, Any]],
        raw: Mapping[str, Any],
        formatter_revision: str,
    ) -> None:
        account = _account_id(account_id)
        normalized_sha1 = canonical_sha1(sha1)
        formatted_json = json.dumps([dict(value) for value in formatted], ensure_ascii=False, separators=(",", ":"))
        raw_json = json.dumps(dict(raw), ensure_ascii=False, separators=(",", ":"))
        revision = _required(formatter_revision, "formatter_revision")
        with self._unit_of_work() as uow:
            self._lock_account(uow, account)
            uow.session.execute(
                text(
                    """
                    INSERT INTO p115_mediainfo_cache (
                        sha1, mediainfo_json, raw_ffprobe_json, cache_revision,
                        raw_schema_version, formatter_revision, probe_revision
                    ) VALUES (
                        :sha1, CAST(:formatted AS jsonb), CAST(:raw AS jsonb), 1,
                        1, :formatter_revision, 'p6-local-upload-ffprobe-v1'
                    )
                    ON CONFLICT (sha1) DO UPDATE SET
                        mediainfo_json = EXCLUDED.mediainfo_json,
                        raw_ffprobe_json = COALESCE(p115_mediainfo_cache.raw_ffprobe_json, EXCLUDED.raw_ffprobe_json),
                        cache_revision = p115_mediainfo_cache.cache_revision + 1,
                        raw_schema_version = GREATEST(p115_mediainfo_cache.raw_schema_version, 1),
                        formatter_revision = EXCLUDED.formatter_revision,
                        probe_revision = EXCLUDED.probe_revision,
                        updated_at = NOW()
                    """
                ),
                {
                    "sha1": normalized_sha1,
                    "formatted": formatted_json,
                    "raw": raw_json,
                    "formatter_revision": revision,
                },
            )
            uow.commit()

    @staticmethod
    def _lock_account(uow: SqlAlchemyUnitOfWork, account_id: str) -> None:
        uow.session.execute(
            text("SELECT pg_advisory_xact_lock(:namespace, :lock_key)"),
            {"namespace": DYNAMIC_CONFIG_LOCK_NAMESPACE, "lock_key": P115_AUTH_LOCK_KEY},
        )
        current = str(
            uow.session.scalar(
                text(
                    """
                    SELECT value_json ->> 'account_id'
                    FROM app_settings
                    WHERE setting_key = 'p115_auth_tokens'
                    """
                )
            )
            or ""
        ).strip()
        if current != account_id:
            raise RuntimeError("115 account changed before upload projection commit")


class NativeP115UploadProjection(P115UploadProjection):
    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        credentials: P115CredentialStore,
        repository: P115UploadProjectionRepository,
        sorting_rules: P115SortingRulesRepository,
        rename_config: P115RenameConfigRepository,
        ingestion: P115UploadIngestionIngress,
        processes: ControlledProcessRunner,
        policies: P115UploadMediaInfoPolicyProvider,
        libraries: P115LocalStrmLibraryGateway | None = None,
    ) -> None:
        self._settings = settings
        self._credentials = credentials
        self._repository = repository
        self._sorting_rules = sorting_rules
        self._rename_config = rename_config
        self._ingestion = ingestion
        self._processes = processes
        self._policies = policies
        self._libraries = libraries or AtomicP115LocalStrmLibraryGateway()

    def project(self, operation: UploadOperation) -> UploadProjectionOutcome:
        receipt = dict(operation.remote_receipt or {})
        account_id = _account_id(receipt.get("account_id"))
        current_account = str(self._credentials.load().account_id or "").strip()
        if current_account != account_id:
            raise RuntimeError("115 account changed before upload local projection")
        file_id = _required(receipt.get("fid"), "receipt.fid")
        pick_code = _required(receipt.get("pick_code"), "receipt.pick_code")
        sha1 = canonical_sha1(receipt.get("sha1"))
        parent_id = _required(receipt.get("parent_id") or operation.mapping.get("target_cid"), "receipt.parent_id")
        file_name = _required(receipt.get("file_name") or os.path.basename(operation.path), "receipt.file_name")
        settings = self._settings()
        base = self._repository.target_relative_path(
            str(operation.mapping.get("target_cid") or ""),
            account_id=account_id,
        )
        if base is None:
            base = str(operation.mapping.get("target_name") or "").strip()
        base = _strip_root(base, settings.p115_media_root_name)
        remote_relative_path = _relative_path(posixpath.join(base, operation.relative_path))
        size = operation.fingerprint.size if operation.fingerprint is not None else os.path.getsize(operation.path)
        self._repository.persist_remote_file(
            account_id=account_id,
            file_id=file_id,
            parent_id=parent_id,
            name=file_name,
            sha1=sha1,
            pick_code=pick_code,
            size=size,
            local_path=remote_relative_path,
        )

        extension = os.path.splitext(operation.path)[1].casefold()
        if extension not in _MEDIA_EXTENSIONS:
            return UploadProjectionOutcome((), None, {"cached": True, "derived_ownership": {}})
        output_directory = posixpath.dirname(remote_relative_path)
        if not self._matches_sorting_rule(output_directory, account_id, settings.p115_media_root_name):
            return UploadProjectionOutcome(
                (),
                None,
                {"cached": True, "derived_ownership": {}, "skipped": "sorting_rule_not_matched"},
            )

        policy = self._policies.snapshot()
        outcome = self._processes.run(
            ControlledProcessProfile.FFPROBE,
            (
                "-hide_banner",
                "-v",
                "error",
                "-print_format",
                "json",
                "-show_format",
                "-show_streams",
                "-show_chapters",
                operation.path,
            ),
            max_stdin_bytes=0,
            max_stdout_bytes=_FFPROBE_STDOUT_LIMIT,
            max_stderr_bytes=_FFPROBE_STDERR_LIMIT,
            timeout_seconds=_FFPROBE_TIMEOUT_SECONDS,
            output=ControlledProcessOutput.JSON,
        )
        raw = outcome.parsed_output
        if not outcome.succeeded or not isinstance(raw, Mapping):
            raise RuntimeError("uploaded media ffprobe did not produce valid JSON")
        formatted = format_raw_ffprobe_mediainfo(
            raw,
            file_name=file_name,
            file_size=size,
            sha1=sha1,
            policy=policy,
        )
        self._repository.persist_media_info(
            account_id=account_id,
            sha1=sha1,
            formatted=formatted,
            raw=raw,
            formatter_revision=media_info_formatter_revision(policy),
        )

        root = os.path.abspath(str(settings.local_strm_root or "").strip())
        library = self._libraries.open(root)
        if library is None:
            raise RuntimeError("local STRM root is unavailable or unsafe")
        etk_url = str(settings.etk_server_url or "").strip().rstrip("/")
        if not etk_url.startswith(("http://", "https://")):
            raise RuntimeError("ETK server URL is invalid for upload STRM projection")
        content = f"{etk_url}/api/p115/play/{pick_code}"
        if self._rename_config.load().config.get("strm_url_fmt") == "with_name":
            content = f"{content}/{file_name}"
        stem = os.path.splitext(posixpath.basename(remote_relative_path))[0]
        relative_strm = _relative_path(posixpath.join(output_directory, f"{stem}.strm"))
        written = library.write_strm(relative_strm, content)
        absolute_path = written.absolute_path
        digest = hashlib.sha256(content.encode()).hexdigest()
        event_key = f"p115-upload:{operation.id}:created"
        self._ingestion.record_event(
            "created",
            absolute_path,
            source="active",
            source_event_key=event_key,
        )
        return UploadProjectionOutcome(
            (absolute_path,),
            event_key,
            {"cached": True, "derived_ownership": {absolute_path: digest}},
        )

    def delete_source(self, operation: UploadOperation) -> Mapping[str, Any]:
        if operation.fingerprint is None:
            raise RuntimeError("source delete operation has no fingerprint fence")
        return _unlink_fenced(
            root=str(operation.mapping["local_dir"]),
            relative_path=operation.relative_path,
            fingerprint=operation.fingerprint,
        )

    def delete_uploaded_source(self, operation: UploadOperation, source: UploadedSource) -> Mapping[str, Any]:
        root = os.path.abspath(str(self._settings().local_strm_root or "").strip())
        library = self._libraries.open(root)
        if library is None:
            raise RuntimeError("local STRM root is unavailable or unsafe")
        deleted: list[str] = []
        missing: list[str] = []
        deferred: list[str] = []
        for absolute_path in source.derived_paths:
            relative = os.path.relpath(absolute_path, root).replace("\\", "/")
            try:
                relative = _relative_path(relative)
                expected = _required(source.derived_ownership.get(absolute_path), "derived ownership")
                action = library.plan_delete(
                    relative,
                    kind="file",
                    source="cache_projection",
                    expected_sha256=expected,
                )
                if action is None:
                    missing.append(absolute_path)
                else:
                    result = library.reconcile(
                        roots=(posixpath.dirname(relative) or ".",),
                        desired_paths=(),
                        cleanup=False,
                        allowed_deletions=(action,),
                    )
                    if relative in result.deleted_paths:
                        deleted.append(absolute_path)
                    else:
                        deferred.append(absolute_path)
            except (OSError, RuntimeError, ValueError):
                deferred.append(absolute_path)
            if absolute_path.casefold().endswith(".strm") and absolute_path not in deferred:
                digest = hashlib.sha256(relative.encode()).hexdigest()
                self._ingestion.record_event(
                    "deleted",
                    absolute_path,
                    source="active",
                    source_event_key=f"p115-upload:{operation.id}:deleted:{digest}",
                )
        if deferred:
            raise RuntimeError("uploaded source local projection changed before delete")
        return {"deleted": deleted, "missing": missing, "deferred": deferred}

    def _matches_sorting_rule(self, relative_directory: str, account_id: str, media_root_name: str) -> bool:
        snapshot = self._sorting_rules.load()
        if snapshot.account_id and snapshot.account_id != account_id:
            raise RuntimeError("115 sorting rules belong to another account")
        if not str(relative_directory or "").strip().strip("/"):
            return False
        path = _relative_path(relative_directory)
        for rule in snapshot.rules:
            category = _strip_root(rule.get("category_path") or rule.get("dir_name") or "", media_root_name)
            if not category:
                continue
            category = _relative_path(category)
            if path.casefold() == category.casefold() or path.casefold().startswith(f"{category.casefold()}/"):
                return True
        return False


def _required(value: object, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return normalized


def _account_id(value: object) -> str:
    account = _required(value, "account_id")
    if not account.isdigit():
        raise ValueError("account_id must be numeric")
    return account


def _nonnegative(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be non-negative")
    parsed = int(value)  # type: ignore[arg-type]
    if parsed < 0:
        raise ValueError(f"{field} must be non-negative")
    return parsed


def _relative_path(value: object) -> str:
    raw = str(value or "").strip().replace("\\", "/").strip("/")
    path = PurePosixPath(raw)
    if not raw or path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("upload projection path must be a safe relative path")
    return path.as_posix()


def _strip_root(value: object, root: object) -> str:
    path = str(value or "").strip().replace("\\", "/").strip("/")
    normalized_root = str(root or "").strip().replace("\\", "/").strip("/")
    if normalized_root and path == normalized_root:
        return ""
    if normalized_root and path.casefold().startswith(f"{normalized_root.casefold()}/"):
        return path[len(normalized_root) + 1 :]
    return path


__all__ = [
    "NativeP115UploadProjection",
    "P115UploadProjectionRepository",
    "SqlAlchemyP115UploadProjectionRepository",
]
