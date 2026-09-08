from __future__ import annotations

import hashlib
import os
import secrets
import stat
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from typing import Any, Protocol

from .maintenance_transport import P115PinnedOperationsClient, P115PrimaryOperationsGateway
from .strm_adapters import AtomicP115LocalStrmLibraryGateway
from .upload_monitor import (
    P115UploadProjection,
    P115UploadProvider,
    UploadedSource,
    UploadFileFingerprint,
    UploadOperation,
    UploadProjectionOutcome,
    UploadProviderDisposition,
    UploadProviderOutcome,
    observe_upload_file,
)

_VIDEO_EXTENSIONS = frozenset(
    {
        ".avi",
        ".flv",
        ".iso",
        ".m2ts",
        ".mkv",
        ".mov",
        ".mp4",
        ".mpeg",
        ".mpg",
        ".rmvb",
        ".ts",
        ".webm",
        ".wmv",
    }
)


class P115CompletedUploadSourceRepository(Protocol):
    def sources_by_pick_codes(self, values: Sequence[str]) -> tuple[UploadedSource, ...]: ...

    def remove_sources_if_current(self, sources: Sequence[UploadedSource]) -> int: ...


class NativeP115UploadSourceCleanupGateway:
    """Delete unchanged keep-mode sources without process-local suppression state."""

    def __init__(
        self,
        repository: P115CompletedUploadSourceRepository,
        config: Callable[[], Mapping[str, Any]],
    ) -> None:
        self._repository = repository
        self._config = config

    def delete_sources(self, pickcodes: Sequence[str]) -> Mapping[str, Any]:
        normalized_pickcodes = tuple(
            sorted({str(value or "").strip() for value in pickcodes if str(value or "").strip()})
        )
        result: dict[str, Any] = {
            "ok": True,
            "matched": 0,
            "deleted": 0,
            "missing": 0,
            "changed": 0,
            "failed": 0,
            "records_removed": 0,
            "records_replaced": 0,
        }
        if not normalized_pickcodes:
            return result

        config = self._config()
        raw_mappings = config.get("mappings") if isinstance(config, Mapping) else None
        active_mappings = (
            {
                str(mapping.get("id") or ""): mapping
                for mapping in raw_mappings
                if isinstance(mapping, Mapping)
                and bool(config.get("enabled"))
                and bool(mapping.get("enabled"))
                and str(mapping.get("mode") or "keep") == "keep"
                and str(mapping.get("id") or "")
            }
            if isinstance(raw_mappings, list)
            else {}
        )
        if not active_mappings:
            return result

        removable: list[UploadedSource] = []
        for source in self._repository.sources_by_pick_codes(normalized_pickcodes):
            mapping = active_mappings.get(source.mapping_id)
            if mapping is None or source.mode != "keep":
                continue
            result["matched"] += 1
            root = str(mapping.get("local_dir") or "").strip()
            expected_path = os.path.normcase(os.path.abspath(os.path.join(root, source.relative_path)))
            if (
                not root
                or expected_path != os.path.normcase(os.path.abspath(source.local_path))
                or expected_path != source.path_key
            ):
                result["failed"] += 1
                continue
            try:
                outcome = _unlink_fenced(
                    root=root,
                    relative_path=source.relative_path,
                    fingerprint=source.fingerprint,
                )
            except (OSError, RuntimeError, ValueError):
                result["failed"] += 1
                continue
            if outcome.get("deleted"):
                result["deleted"] += 1
                removable.append(source)
            elif outcome.get("missing"):
                result["missing"] += 1
                removable.append(source)
            else:
                result["changed"] += 1
        result["records_removed"] = self._repository.remove_sources_if_current(removable)
        result["records_replaced"] = len(removable) - result["records_removed"]
        result["ok"] = result["failed"] == 0
        return result


class NativeP115UploadProvider(P115UploadProvider):
    """Account/backend-pinned upload transport with authoritative readback."""

    def __init__(self, operations: P115PrimaryOperationsGateway) -> None:
        self._operations = operations

    def execute(
        self,
        operation: UploadOperation,
        *,
        checkpoint: Callable[[str, Mapping[str, Any]], None],
    ) -> UploadProviderOutcome:
        session = dict(operation.upload_session or {})
        if session.get("readback_only"):
            return self.readback(operation)
        try:
            observed = observe_upload_file(operation.path, str(operation.mapping["local_dir"]))
        except Exception as exc:
            return _outcome(
                UploadProviderDisposition.REJECTED,
                operation,
                request_sent=False,
                session=session,
                error={"type": "source_unavailable", "message": str(exc)},
            )
        if operation.fingerprint is None or observed != operation.fingerprint:
            return _outcome(
                UploadProviderDisposition.REJECTED,
                operation,
                request_sent=False,
                session=session,
                error={"type": "source_fingerprint_changed"},
            )

        try:
            client = self._open(operation, session)
        except Exception as exc:
            return _outcome(
                UploadProviderDisposition.RETRYABLE,
                operation,
                request_sent=False,
                session=session,
                error={"type": "upload_backend_unavailable", "message": str(exc)},
            )
        if client is None:
            return _outcome(
                UploadProviderDisposition.RETRYABLE,
                operation,
                request_sent=False,
                session=session,
                error={"type": "upload_backend_unavailable"},
            )
        try:
            identity = self._content_identity(operation, session)
        except OSError as exc:
            return _outcome(
                UploadProviderDisposition.RETRYABLE,
                operation,
                request_sent=False,
                session=session,
                error={"type": "source_read_failed", "message": str(exc)},
            )
        session.update(identity)
        session.update(
            {
                "backend": client.backend,
                "account_id": client.account_id,
                "credential_fingerprint": client.credential_fingerprint,
                "phase": "prepared",
                "request_sent": False,
                "requested_target_cid": str(operation.mapping["target_cid"]),
            }
        )
        request_sent = False
        uploaded_bytes = int(session.get("uploaded_bytes") or 0)
        last_progress_at = 0.0

        def persist(values: Mapping[str, Any]) -> None:
            nonlocal request_sent
            request_sent = True
            session.update(dict(values))
            session.update(
                {
                    "backend": client.backend,
                    "account_id": client.account_id,
                    "credential_fingerprint": client.credential_fingerprint,
                    "request_sent": True,
                    "readback_only": True,
                    "content_sha1": identity["content_sha1"],
                    "preid": identity["preid"],
                }
            )
            checkpoint(client.backend, session)

        def progress(delta: int) -> None:
            nonlocal uploaded_bytes, last_progress_at
            try:
                uploaded_bytes = min(int(identity["filesize"]), uploaded_bytes + max(0, int(delta or 0)))
            except (TypeError, ValueError):
                return
            now = time.monotonic()
            if uploaded_bytes < int(identity["filesize"]) and now - last_progress_at < 1:
                return
            persist(
                {
                    **session,
                    "phase": "uploading",
                    "uploaded_bytes": uploaded_bytes,
                    "total_bytes": int(identity["filesize"]),
                    "progress": round(uploaded_bytes / max(int(identity["filesize"]), 1) * 100, 1),
                }
            )
            last_progress_at = now

        try:
            target_cid = self._resolve_target_directory(
                operation,
                client=client,
                session=session,
                checkpoint=persist,
            )
            session["resolved_target_cid"] = target_cid
            result = client.upload_local_file(
                operation.path,
                target_cid,
                session=session,
                checkpoint=persist,
                progress=progress,
            )
        except Exception as exc:
            replay = replace(
                operation,
                upload_backend=client.backend,
                upload_session={**session, "readback_only": request_sent},
            )
            observed_outcome = self.readback(replay) if request_sent else None
            if observed_outcome is not None and observed_outcome.disposition is UploadProviderDisposition.CONFIRMED:
                return observed_outcome
            return _outcome(
                UploadProviderDisposition.INDETERMINATE if request_sent else UploadProviderDisposition.RETRYABLE,
                replay,
                request_sent=request_sent,
                session={**session, "readback_only": request_sent},
                error={"type": "upload_request_failed", "message": str(exc)},
            )

        replay = replace(
            operation,
            upload_backend=client.backend,
            upload_session={**session, "upload_response_received": bool(result)},
        )
        observed_outcome = self.readback(replay)
        if observed_outcome.disposition is UploadProviderDisposition.CONFIRMED:
            return observed_outcome
        return _outcome(
            UploadProviderDisposition.INDETERMINATE,
            replay,
            request_sent=True,
            session={**session, "readback_only": True},
            error={"type": "upload_receipt_not_confirmed"},
        )

    def readback(self, operation: UploadOperation) -> UploadProviderOutcome:
        session = dict(operation.upload_session or {})
        target_cid = str(
            session.get("resolved_target_cid")
            or session.get("requested_target_cid")
            or operation.mapping.get("target_cid")
            or ""
        ).strip()
        filename = str(session.get("filename") or os.path.basename(operation.path)).strip()
        content_sha1 = str(session.get("content_sha1") or "").strip().upper()
        expected_size = int(session.get("filesize") or (operation.fingerprint.size if operation.fingerprint else 0))
        if not target_cid or not filename or not content_sha1 or expected_size < 0:
            return _outcome(
                UploadProviderDisposition.INDETERMINATE,
                operation,
                request_sent=True,
                session={**session, "readback_only": True},
                error={"type": "upload_readback_identity_missing"},
            )
        try:
            client = self._open(operation, session, require_pinned=True)
            if client is None:
                raise RuntimeError("115 pinned upload client is unavailable")
            matches = [
                item
                for item in client.list_children(target_cid)
                if not _is_directory(item)
                and _item_name(item) == filename
                and _item_size(item) == expected_size
                and _item_sha1(item) == content_sha1
            ]
        except Exception as exc:
            return _outcome(
                UploadProviderDisposition.INDETERMINATE,
                operation,
                request_sent=True,
                session={**session, "readback_only": True},
                error={"type": "upload_readback_failed", "message": str(exc)},
            )
        if len(matches) != 1:
            return _outcome(
                UploadProviderDisposition.INDETERMINATE,
                operation,
                request_sent=True,
                session={**session, "readback_only": True},
                error={
                    "type": "upload_readback_ambiguous" if matches else "upload_readback_not_found",
                    "matches": len(matches),
                },
            )
        item = matches[0]
        fid = _item_id(item)
        pick_code = _item_pick_code(item)
        if not fid or not pick_code:
            return _outcome(
                UploadProviderDisposition.INDETERMINATE,
                operation,
                request_sent=True,
                session={**session, "readback_only": True},
                error={"type": "upload_readback_receipt_incomplete"},
            )
        receipt = {
            "fid": fid,
            "parent_id": target_cid,
            "file_name": filename,
            "sha1": content_sha1,
            "preid": str(session.get("preid") or "").upper(),
            "pick_code": pick_code,
            "size": expected_size,
            "backend": client.backend,
            "account_id": client.account_id,
            "credential_fingerprint": client.credential_fingerprint,
            "evidence": "authoritative_parent_listing",
        }
        return _outcome(
            UploadProviderDisposition.CONFIRMED,
            replace(operation, upload_backend=client.backend),
            request_sent=True,
            session={**session, "readback_only": False, "phase": "remote_confirmed"},
            receipt=receipt,
        )

    def delete(
        self,
        operation: UploadOperation,
        source: UploadedSource,
        *,
        checkpoint: Callable[[str, Mapping[str, Any]], None],
    ) -> UploadProviderOutcome:
        before = self.readback_delete(operation, source)
        if before.disposition in {UploadProviderDisposition.CONFIRMED, UploadProviderDisposition.REJECTED}:
            return before
        session = self._delete_session(operation, source)
        fid = str(session.get("fid") or "")
        try:
            client = self._open(operation, session, require_pinned=True)
            if client is None:
                raise RuntimeError("115 pinned delete client is unavailable")
        except Exception as exc:
            return _outcome(
                UploadProviderDisposition.RETRYABLE,
                operation,
                request_sent=False,
                session=session,
                error={"type": "delete_backend_unavailable", "message": str(exc)},
            )
        session.update(
            {
                "phase": "remote_delete",
                "request_sent": True,
                "readback_only": True,
                "credential_fingerprint": client.credential_fingerprint,
            }
        )
        checkpoint(client.backend, session)
        try:
            response = client.delete_files([fid])
            if not _provider_response_succeeded(response):
                raise RuntimeError("115 remote delete was not accepted")
        except Exception as exc:
            replay = replace(operation, upload_backend=client.backend, upload_session=session)
            after = self.readback_delete(replay, source)
            if after.disposition is UploadProviderDisposition.CONFIRMED:
                return after
            return _outcome(
                UploadProviderDisposition.INDETERMINATE,
                operation,
                request_sent=True,
                session=session,
                error={"type": "remote_delete_failed", "message": str(exc)},
            )
        return self.readback_delete(replace(operation, upload_backend=client.backend, upload_session=session), source)

    def readback_delete(self, operation: UploadOperation, source: UploadedSource) -> UploadProviderOutcome:
        session = self._delete_session(operation, source)
        fid = str(session.get("fid") or "")
        if (
            not fid
            or str(session.get("backend") or "") not in {"openapi", "cookie"}
            or not str(session.get("account_id") or "")
        ):
            return _outcome(
                UploadProviderDisposition.REJECTED,
                operation,
                request_sent=False,
                session=session,
                error={"type": "delete_remote_identity_missing_or_unpinned"},
            )
        try:
            client = self._open(operation, session, require_pinned=True)
            if client is None:
                raise RuntimeError("115 pinned delete readback client is unavailable")
            response = client.file_info(fid)
            exists = _remote_file_info_exists(response, fid)
        except Exception as exc:
            return _outcome(
                UploadProviderDisposition.INDETERMINATE,
                operation,
                request_sent=True,
                session=session,
                error={"type": "remote_delete_readback_failed", "message": str(exc)},
            )
        if exists is False:
            return _outcome(
                UploadProviderDisposition.CONFIRMED,
                replace(operation, upload_backend=client.backend),
                request_sent=True,
                session=session,
                receipt={
                    "fid": fid,
                    "parent_id": str(session.get("parent_id") or ""),
                    "pick_code": source.pick_code,
                    "backend": client.backend,
                    "account_id": client.account_id,
                    "deleted": True,
                    "evidence": "authoritative_absence_readback",
                },
            )
        return _outcome(
            UploadProviderDisposition.INDETERMINATE,
            operation,
            request_sent=True,
            session=session,
            error={"type": "remote_delete_still_present"},
        )

    def _open(
        self,
        operation: UploadOperation,
        session: Mapping[str, Any],
        *,
        require_pinned: bool = False,
    ) -> P115PinnedOperationsClient | None:
        account_id = str(session.get("account_id") or "").strip()
        backend = str(operation.upload_backend or session.get("backend") or "").strip().casefold()
        fingerprint = str(session.get("credential_fingerprint") or "").strip()
        if require_pinned and (not account_id or backend not in {"openapi", "cookie"}):
            raise RuntimeError("115 upload provider identity is not pinned")
        return self._operations.open(
            expected_account_id=account_id or None,
            expected_backend=backend or None,
            expected_credential_fingerprint=fingerprint or None,
        )

    @staticmethod
    def _resolve_target_directory(
        operation: UploadOperation,
        *,
        client: P115PinnedOperationsClient,
        session: dict[str, Any],
        checkpoint: Callable[[Mapping[str, Any]], None],
    ) -> str:
        existing = str(session.get("resolved_target_cid") or "").strip()
        if existing:
            return existing
        current_cid = str(operation.mapping["target_cid"])
        relative_dir = os.path.dirname(operation.relative_path)
        for name in [part for part in relative_dir.replace("\\", "/").split("/") if part not in {"", ".", ".."}]:
            matches = [
                item for item in client.list_children(current_cid) if _is_directory(item) and _item_name(item) == name
            ]
            if len(matches) > 1:
                raise RuntimeError(f"115 target directory readback is ambiguous: {name}")
            if not matches:
                checkpoint(
                    {
                        **session,
                        "phase": "mkdir",
                        "mkdir_parent_cid": current_cid,
                        "mkdir_name": name,
                    }
                )
                response = client.create_directory(current_cid, name)
                if not _provider_response_succeeded(response):
                    raise RuntimeError(f"115 target directory creation was rejected: {name}")
                matches = [
                    item
                    for item in client.list_children(current_cid)
                    if _is_directory(item) and _item_name(item) == name
                ]
            if len(matches) != 1 or not _item_id(matches[0]):
                raise RuntimeError(f"115 target directory was not authoritatively confirmed: {name}")
            current_cid = _item_id(matches[0])
        return current_cid

    @staticmethod
    def _content_identity(operation: UploadOperation, session: Mapping[str, Any]) -> dict[str, Any]:
        content_sha1 = str(session.get("content_sha1") or "").strip().upper()
        preid = str(session.get("preid") or "").strip().upper()
        if content_sha1 and preid:
            return {
                "content_sha1": content_sha1,
                "preid": preid,
                "filename": os.path.basename(operation.path),
                "filesize": operation.fingerprint.size if operation.fingerprint else os.path.getsize(operation.path),
            }
        digest = hashlib.sha1()
        first = b""
        with open(operation.path, "rb") as stream:
            while chunk := stream.read(1024 * 1024):
                if not first:
                    first = chunk[:131072]
                digest.update(chunk)
        return {
            "content_sha1": digest.hexdigest().upper(),
            "preid": hashlib.sha1(first).hexdigest().upper(),
            "filename": os.path.basename(operation.path),
            "filesize": operation.fingerprint.size if operation.fingerprint else os.path.getsize(operation.path),
        }

    @staticmethod
    def _delete_session(operation: UploadOperation, source: UploadedSource) -> dict[str, Any]:
        return {
            **dict(operation.upload_session or {}),
            "backend": str(source.remote_receipt.get("backend") or "").strip().casefold(),
            "account_id": str(source.remote_receipt.get("account_id") or "").strip(),
            "credential_fingerprint": str(source.remote_receipt.get("credential_fingerprint") or "").strip(),
            "fid": str(source.remote_receipt.get("fid") or "").strip(),
            "parent_id": str(source.remote_receipt.get("parent_id") or source.target_cid).strip(),
            "pick_code": source.pick_code,
            "readback_only": True,
        }


class LegacyP115UploadProvider(P115UploadProvider):
    """Compatibility SDK adapter with backend pinning and authoritative readback."""

    def execute(
        self,
        operation: UploadOperation,
        *,
        checkpoint: Callable[[str, Mapping[str, Any]], None],
    ) -> UploadProviderOutcome:
        session = dict(operation.upload_session or {})
        if session.get("readback_only"):
            return self.readback(operation)
        try:
            observed = observe_upload_file(operation.path, str(operation.mapping["local_dir"]))
        except Exception as exc:
            return _outcome(
                UploadProviderDisposition.REJECTED,
                operation,
                request_sent=False,
                session=session,
                error={"type": "source_unavailable", "message": str(exc)},
            )
        if operation.fingerprint is None or observed != operation.fingerprint:
            return _outcome(
                UploadProviderDisposition.REJECTED,
                operation,
                request_sent=False,
                session=session,
                error={"type": "source_fingerprint_changed"},
            )

        account_id = str(session.get("account_id") or self._current_account_id() or "").strip()
        if not account_id:
            return _outcome(
                UploadProviderDisposition.RETRYABLE,
                operation,
                request_sent=False,
                session=session,
                error={"type": "upload_account_unavailable"},
            )
        session["account_id"] = account_id
        backend = self._select_backend(operation, expected_account_id=account_id)
        if not backend:
            return _outcome(
                UploadProviderDisposition.RETRYABLE,
                operation,
                request_sent=False,
                session=session,
                error={"type": "upload_backend_unavailable"},
            )
        try:
            identity = self._content_identity(operation, session)
        except OSError as exc:
            return _outcome(
                UploadProviderDisposition.RETRYABLE,
                operation,
                request_sent=False,
                session=session,
                error={"type": "source_read_failed", "message": str(exc)},
            )
        session.update(identity)
        session.update(
            {
                "backend": backend,
                "account_id": account_id,
                "phase": "prepared",
                "request_sent": False,
                "requested_target_cid": str(operation.mapping["target_cid"]),
            }
        )
        request_sent = False
        uploaded_bytes = int(session.get("uploaded_bytes") or 0)
        last_progress_at = 0.0

        def persist(candidate_backend: str, values: Mapping[str, Any]) -> None:
            nonlocal request_sent
            request_sent = True
            session.update(dict(values))
            session.update(
                {
                    "backend": candidate_backend,
                    "account_id": account_id,
                    "request_sent": True,
                    "readback_only": True,
                    "content_sha1": identity["content_sha1"],
                    "preid": identity["preid"],
                }
            )
            checkpoint(candidate_backend, session)

        def progress(delta: int) -> None:
            nonlocal uploaded_bytes, last_progress_at
            try:
                uploaded_bytes = min(identity["filesize"], uploaded_bytes + max(0, int(delta or 0)))
            except (TypeError, ValueError):
                return
            now = time.monotonic()
            if uploaded_bytes < identity["filesize"] and now - last_progress_at < 1:
                return
            session.update(
                {
                    "phase": "uploading",
                    "request_sent": True,
                    "readback_only": True,
                    "uploaded_bytes": uploaded_bytes,
                    "total_bytes": identity["filesize"],
                    "progress": round(uploaded_bytes / max(identity["filesize"], 1) * 100, 1),
                }
            )
            checkpoint(backend, session)
            last_progress_at = now

        try:
            from handler.p115_service import p115_expected_account
            from handler.p115_upload_monitor import UploadMonitorRuntime

            with p115_expected_account(account_id):
                target_cid = self._resolve_target_directory(
                    operation,
                    backend=backend,
                    session=session,
                    checkpoint=persist,
                )
                session["resolved_target_cid"] = target_cid
                session["phase"] = "upload_init"
                runtime = UploadMonitorRuntime.instance()
                runtime._upload_file_resumable(
                    operation.path,
                    target_cid,
                    session,
                    fixed_backend=backend,
                    expected_account_id=account_id,
                    checkpoint_hook=persist,
                    external_progress_hook=progress,
                )
        except Exception as exc:
            replay = replace(
                operation,
                upload_backend=backend,
                upload_session={**session, "readback_only": request_sent},
            )
            observed_outcome = self.readback(replay) if request_sent else None
            if observed_outcome is not None and observed_outcome.disposition is UploadProviderDisposition.CONFIRMED:
                return observed_outcome
            return _outcome(
                UploadProviderDisposition.INDETERMINATE if request_sent else UploadProviderDisposition.RETRYABLE,
                replay,
                request_sent=request_sent,
                session={**session, "readback_only": request_sent},
                error={"type": "upload_request_failed", "message": str(exc)},
            )

        replay = replace(operation, upload_backend=backend, upload_session=dict(session))
        observed_outcome = self.readback(replay)
        if observed_outcome.disposition is UploadProviderDisposition.CONFIRMED:
            return observed_outcome
        return _outcome(
            UploadProviderDisposition.INDETERMINATE,
            replay,
            request_sent=True,
            session={**session, "readback_only": True},
            error={"type": "upload_receipt_not_confirmed"},
        )

    def readback(self, operation: UploadOperation) -> UploadProviderOutcome:
        session = dict(operation.upload_session or {})
        backend = str(operation.upload_backend or session.get("backend") or "").strip()
        account_id = str(session.get("account_id") or "").strip()
        target_cid = str(
            session.get("resolved_target_cid")
            or session.get("requested_target_cid")
            or operation.mapping.get("target_cid")
            or ""
        ).strip()
        filename = str(session.get("filename") or os.path.basename(operation.path)).strip()
        content_sha1 = str(session.get("content_sha1") or "").strip().upper()
        expected_size = int(session.get("filesize") or (operation.fingerprint.size if operation.fingerprint else 0))
        if not account_id or not backend or not target_cid or not filename or not content_sha1 or expected_size < 0:
            return _outcome(
                UploadProviderDisposition.INDETERMINATE,
                operation,
                request_sent=True,
                session={**session, "readback_only": True},
                error={"type": "upload_readback_identity_missing"},
            )
        try:
            client = self._client(account_id)
            matches = [
                item
                for item in self._list_children(client, target_cid, filename, backend=backend)
                if not _is_directory(item)
                and _item_name(item) == filename
                and _item_size(item) == expected_size
                and _item_sha1(item) == content_sha1
            ]
        except Exception as exc:
            return _outcome(
                UploadProviderDisposition.INDETERMINATE,
                operation,
                request_sent=True,
                session={**session, "readback_only": True},
                error={"type": "upload_readback_failed", "message": str(exc)},
            )
        if len(matches) != 1:
            return _outcome(
                UploadProviderDisposition.INDETERMINATE,
                operation,
                request_sent=True,
                session={**session, "readback_only": True},
                error={
                    "type": "upload_readback_ambiguous" if matches else "upload_readback_not_found",
                    "matches": len(matches),
                },
            )
        item = matches[0]
        fid = _item_id(item)
        pick_code = _item_pick_code(item)
        if not fid or not pick_code:
            return _outcome(
                UploadProviderDisposition.INDETERMINATE,
                operation,
                request_sent=True,
                session={**session, "readback_only": True},
                error={"type": "upload_readback_receipt_incomplete"},
            )
        receipt = {
            "fid": fid,
            "parent_id": target_cid,
            "file_name": filename,
            "sha1": content_sha1,
            "preid": str(session.get("preid") or "").upper(),
            "pick_code": pick_code,
            "size": expected_size,
            "backend": backend,
            "account_id": account_id,
            "evidence": "authoritative_parent_listing",
        }
        return _outcome(
            UploadProviderDisposition.CONFIRMED,
            operation,
            request_sent=True,
            session={**session, "readback_only": False, "phase": "remote_confirmed"},
            receipt=receipt,
        )

    def delete(
        self,
        operation: UploadOperation,
        source: UploadedSource,
        *,
        checkpoint: Callable[[str, Mapping[str, Any]], None],
    ) -> UploadProviderOutcome:
        before = self.readback_delete(operation, source)
        if before.disposition in {
            UploadProviderDisposition.CONFIRMED,
            UploadProviderDisposition.REJECTED,
        }:
            return before
        fid = str(source.remote_receipt.get("fid") or "").strip()
        parent_id = str(source.remote_receipt.get("parent_id") or source.target_cid).strip()
        backend = str(source.remote_receipt.get("backend") or "").strip().casefold()
        account_id = str(source.remote_receipt.get("account_id") or "").strip()
        if not fid or backend not in {"openapi", "cookie"} or not account_id:
            return _outcome(
                UploadProviderDisposition.REJECTED,
                operation,
                request_sent=False,
                error={"type": "delete_remote_identity_missing_or_unpinned"},
            )
        session = {
            **dict(operation.upload_session or {}),
            "backend": backend,
            "account_id": account_id,
            "phase": "remote_delete",
            "request_sent": True,
            "readback_only": True,
            "fid": fid,
            "parent_id": parent_id,
            "pick_code": source.pick_code,
        }
        checkpoint(backend, session)
        try:
            self._client(account_id).fs_delete_backend([fid], backend=backend)
        except Exception as exc:
            after = self.readback_delete(replace(operation, upload_backend=backend, upload_session=session), source)
            if after.disposition is UploadProviderDisposition.CONFIRMED:
                return after
            return _outcome(
                UploadProviderDisposition.INDETERMINATE,
                operation,
                request_sent=True,
                session=session,
                error={"type": "remote_delete_failed", "message": str(exc)},
            )
        return self.readback_delete(replace(operation, upload_backend=backend, upload_session=session), source)

    def readback_delete(self, operation: UploadOperation, source: UploadedSource) -> UploadProviderOutcome:
        fid = str(source.remote_receipt.get("fid") or "").strip()
        parent_id = str(source.remote_receipt.get("parent_id") or source.target_cid).strip()
        backend = str(source.remote_receipt.get("backend") or "").strip().casefold()
        account_id = str(source.remote_receipt.get("account_id") or "").strip()
        session = {
            **dict(operation.upload_session or {}),
            "backend": backend,
            "account_id": account_id,
            "fid": fid,
            "parent_id": parent_id,
            "pick_code": source.pick_code,
            "readback_only": True,
        }
        if not fid or backend not in {"openapi", "cookie"} or not account_id:
            return _outcome(
                UploadProviderDisposition.REJECTED,
                operation,
                request_sent=False,
                session=session,
                error={"type": "delete_remote_identity_missing_or_unpinned"},
            )
        try:
            exists = self._remote_node_exists(
                self._client(account_id),
                fid,
                parent_id,
                backend=backend,
            )
        except Exception as exc:
            return _outcome(
                UploadProviderDisposition.INDETERMINATE,
                operation,
                request_sent=True,
                session=session,
                error={"type": "remote_delete_readback_failed", "message": str(exc)},
            )
        if exists is False:
            return _outcome(
                UploadProviderDisposition.CONFIRMED,
                operation,
                request_sent=True,
                session=session,
                receipt={
                    "fid": fid,
                    "parent_id": parent_id,
                    "pick_code": source.pick_code,
                    "backend": backend,
                    "account_id": account_id,
                    "deleted": True,
                    "evidence": "authoritative_absence_readback",
                },
            )
        return _outcome(
            UploadProviderDisposition.INDETERMINATE,
            operation,
            request_sent=True,
            session=session,
            error={
                "type": "remote_delete_still_present" if exists else "remote_delete_presence_unknown",
            },
        )

    @staticmethod
    def _client(expected_account_id: str) -> Any:
        from handler.p115_service import P115Service

        client = P115Service.get_client(expected_account_id=expected_account_id)
        if client is None:
            raise RuntimeError("115 client is unavailable")
        return client

    @staticmethod
    def _current_account_id() -> str:
        from handler.p115_service import get_115_account_id

        return str(get_115_account_id() or "").strip()

    @staticmethod
    def _select_backend(operation: UploadOperation, *, expected_account_id: str) -> str:
        session = dict(operation.upload_session or {})
        pinned = str(operation.upload_backend or session.get("backend") or "").strip().casefold()
        from handler.p115_service import P115Service, get_115_account_id, get_115_api_priority, get_115_tokens

        if str(get_115_account_id() or "").strip() != expected_account_id:
            return ""

        access_token, _, cookie, _ = get_115_tokens()
        client = P115Service.get_client(expected_account_id=expected_account_id)
        raw_client = getattr(client, "raw_client", None) if client is not None else None
        available = {
            "openapi": bool(access_token),
            "cookie": bool(cookie and raw_client),
        }
        if pinned:
            return pinned if pinned in available and available[pinned] else ""
        preferred = str(get_115_api_priority() or "").strip().casefold()
        order = ("cookie", "openapi") if preferred == "cookie" else ("openapi", "cookie")
        return next((candidate for candidate in order if available[candidate]), "")

    def _resolve_target_directory(
        self,
        operation: UploadOperation,
        *,
        backend: str,
        session: dict[str, Any],
        checkpoint: Callable[[str, Mapping[str, Any]], None],
    ) -> str:
        existing = str(session.get("resolved_target_cid") or "").strip()
        if existing:
            return existing
        account_id = str(session.get("account_id") or "").strip()
        if not account_id:
            raise RuntimeError("115 upload account identity is missing")
        client = self._client(account_id)
        current_cid = str(operation.mapping["target_cid"])
        relative_dir = os.path.dirname(operation.relative_path)
        for name in [part for part in relative_dir.replace("\\", "/").split("/") if part not in {"", ".", ".."}]:
            matches = [
                item
                for item in self._list_children(client, current_cid, name, backend=backend)
                if _is_directory(item) and _item_name(item) == name
            ]
            if len(matches) > 1:
                raise RuntimeError(f"115 target directory readback is ambiguous: {name}")
            if not matches:
                checkpoint(
                    backend,
                    {
                        **session,
                        "phase": "mkdir",
                        "request_sent": True,
                        "readback_only": True,
                        "mkdir_parent_cid": current_cid,
                        "mkdir_name": name,
                    },
                )
                client.fs_mkdir_backend(name, current_cid, backend=backend)
                matches = [
                    item
                    for item in self._list_children(client, current_cid, name, backend=backend)
                    if _is_directory(item) and _item_name(item) == name
                ]
            if len(matches) != 1 or not _item_id(matches[0]):
                raise RuntimeError(f"115 target directory was not authoritatively confirmed: {name}")
            current_cid = _item_id(matches[0])
        return current_cid

    @staticmethod
    def _list_children(
        client: Any,
        parent_cid: str,
        name: str,
        *,
        backend: str,
    ) -> list[Mapping[str, Any]]:
        response = client.fs_files_backend(
            {
                "cid": parent_cid,
                "search_value": name,
                "limit": 1000,
                "record_open_time": 0,
                "count_folders": 1,
            },
            backend=backend,
        )
        if not isinstance(response, Mapping) or response.get("state") is not True:
            raise RuntimeError("115 parent listing was not authoritative")
        values = response.get("data")
        if not isinstance(values, list):
            raise RuntimeError("115 parent listing payload is invalid")
        return [item for item in values if isinstance(item, Mapping)]

    @staticmethod
    def _remote_node_exists(
        client: Any,
        fid: str,
        parent_id: str,
        *,
        backend: str,
    ) -> bool:
        del parent_id
        from handler.p115_service import _p115_delete_not_found_response

        response = client.fs_get_info_backend(fid, backend=backend)
        if isinstance(response, Mapping) and response.get("state") is True:
            data = response.get("data")
            if isinstance(data, list):
                data = data[0] if data else {}
            if isinstance(data, Mapping):
                observed_id = _item_id(data) or fid
                return observed_id == fid
            return True
        if _p115_delete_not_found_response(response):
            return False
        raise RuntimeError("115 remote delete FID readback was not authoritative")

    @staticmethod
    def _content_identity(operation: UploadOperation, session: Mapping[str, Any]) -> dict[str, Any]:
        content_sha1 = str(session.get("content_sha1") or "").strip().upper()
        preid = str(session.get("preid") or "").strip().upper()
        if content_sha1 and preid:
            return {
                "content_sha1": content_sha1,
                "preid": preid,
                "filename": os.path.basename(operation.path),
                "filesize": operation.fingerprint.size if operation.fingerprint else os.path.getsize(operation.path),
            }
        digest = hashlib.sha1()
        first = b""
        with open(operation.path, "rb") as file_obj:
            while chunk := file_obj.read(1024 * 1024):
                if not first:
                    first = chunk[:131072]
                digest.update(chunk)
        return {
            "content_sha1": digest.hexdigest().upper(),
            "preid": hashlib.sha1(first).hexdigest().upper(),
            "filename": os.path.basename(operation.path),
            "filesize": operation.fingerprint.size if operation.fingerprint else os.path.getsize(operation.path),
        }


class LegacyP115UploadProjection(P115UploadProjection):
    def __init__(
        self,
        *,
        ingestion: Any,
        submit_screenshot: Callable[[UploadOperation, str], str | None] | None = None,
    ) -> None:
        self._ingestion = ingestion
        self._submit_screenshot = submit_screenshot

    def project(self, operation: UploadOperation) -> UploadProjectionOutcome:
        receipt = dict(operation.remote_receipt or {})
        if not receipt.get("fid") or not receipt.get("pick_code") or not receipt.get("sha1"):
            raise RuntimeError("confirmed upload receipt is incomplete")
        from handler.p115_upload_monitor import UploadMonitorRuntime

        runtime = UploadMonitorRuntime.instance()
        target_cid = str(receipt.get("parent_id") or operation.mapping.get("target_cid") or "").strip()
        job = {
            "id": str(operation.id),
            "path": operation.path,
            "relative_path": operation.relative_path,
            "mapping_id": operation.mapping_id,
            "target_cid": str(operation.mapping["target_cid"]),
            "target_name": str(operation.mapping.get("target_name") or operation.mapping["target_cid"]),
            "mode": str(operation.mapping.get("mode") or "keep"),
            "fingerprint": operation.fingerprint.to_dict() if operation.fingerprint is not None else {},
        }
        uploaded = runtime._cache_uploaded_file(job, receipt, target_cid)
        if not runtime._create_local_outputs(
            job,
            receipt,
            target_cid,
            uploaded_file=uploaded,
            enqueue_ingestion=False,
            schedule_screenshot=False,
        ):
            raise RuntimeError("confirmed upload local projection failed")
        derived_paths = tuple(str(path) for path in job.get("derived_paths") or ())
        ownership: dict[str, str] = {}
        ingestion_event_key: str | None = None
        screenshot_job_id: str | None = None
        for path in derived_paths:
            with open(path, "rb") as file_obj:
                ownership[path] = hashlib.sha256(file_obj.read()).hexdigest()
            if path.casefold().endswith(".strm"):
                ingestion_event_key = f"p115-upload:{operation.id}:created"
                self._ingestion.record_event(
                    "created",
                    path,
                    source="active",
                    source_event_key=ingestion_event_key,
                )
                if (
                    self._submit_screenshot is not None
                    and os.path.splitext(operation.path)[1].casefold() in _VIDEO_EXTENSIONS
                ):
                    screenshot_job_id = self._submit_screenshot(operation, path)
        return UploadProjectionOutcome(
            derived_paths=derived_paths,
            ingestion_event_key=ingestion_event_key,
            details={
                "cached": True,
                "derived_ownership": ownership,
                "screenshot_job_id": screenshot_job_id,
            },
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
        import config_manager
        import constants

        root = str(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT) or "").strip()
        library = AtomicP115LocalStrmLibraryGateway().open(root)
        if library is None:
            raise RuntimeError("local STRM root is unavailable or unsafe")
        deleted: list[str] = []
        missing: list[str] = []
        deferred: list[str] = []
        for absolute_path in source.derived_paths:
            relative = os.path.relpath(absolute_path, root).replace("\\", "/")
            expected = str(source.derived_ownership.get(absolute_path) or "").strip().casefold()
            action = library.plan_delete(
                relative,
                kind="file",
                source="cache_projection",
                expected_sha256=expected,
            )
            if action is None:
                missing.append(absolute_path)
            else:
                outcome = library.reconcile(
                    roots=(os.path.dirname(relative) or ".",),
                    desired_paths=(),
                    cleanup=False,
                    allowed_deletions=(action,),
                )
                if relative in outcome.deleted_paths:
                    deleted.append(absolute_path)
                else:
                    deferred.append(absolute_path)
            if absolute_path.casefold().endswith(".strm"):
                self._ingestion.record_event(
                    "deleted",
                    absolute_path,
                    source="active",
                    source_event_key=f"p115-upload:{operation.id}:deleted:{hashlib.sha256(relative.encode()).hexdigest()}",
                )
        if deferred:
            raise RuntimeError("uploaded source local projection changed before delete")
        return {"deleted": deleted, "missing": missing, "deferred": deferred}


def _unlink_fenced(
    *,
    root: str,
    relative_path: str,
    fingerprint: UploadFileFingerprint,
) -> Mapping[str, Any]:
    root_path = os.path.abspath(root)
    root_state = os.lstat(root_path)
    if stat.S_ISLNK(root_state.st_mode) or not stat.S_ISDIR(root_state.st_mode):
        raise RuntimeError("upload source root is unavailable or unsafe")
    parts = [part for part in relative_path.replace("\\", "/").split("/") if part not in {"", "."}]
    if not parts or any(part == ".." for part in parts):
        raise RuntimeError("upload source path escapes its mapping root")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptors: list[int] = [os.open(root_path, flags)]
    try:
        opened_root = os.fstat(descriptors[0])
        if (opened_root.st_dev, opened_root.st_ino) != (root_state.st_dev, root_state.st_ino):
            raise RuntimeError("upload source root changed while opening")
        for part in parts[:-1]:
            descriptors.append(os.open(part, flags, dir_fd=descriptors[-1]))
        parent = descriptors[-1]
        name = parts[-1]
        try:
            current = os.stat(name, dir_fd=parent, follow_symlinks=False)
        except FileNotFoundError:
            return {"deleted": False, "missing": True, "changed": False}
        observed = UploadFileFingerprint(
            size=int(current.st_size),
            mtime_ns=int(current.st_mtime_ns),
            device=int(current.st_dev),
            inode=int(current.st_ino),
        )
        if not stat.S_ISREG(current.st_mode) or observed != fingerprint:
            return {"deleted": False, "missing": False, "changed": True}

        quarantine = f".etk-upload-delete-{secrets.token_hex(12)}"
        os.mkdir(quarantine, mode=0o700, dir_fd=parent)
        quarantine_descriptor: int | None = None
        retained = False
        try:
            quarantine_descriptor = os.open(quarantine, flags, dir_fd=parent)
            try:
                os.rename(
                    name,
                    "source",
                    src_dir_fd=parent,
                    dst_dir_fd=quarantine_descriptor,
                )
            except FileNotFoundError:
                return {"deleted": False, "missing": True, "changed": False}

            try:
                quarantined = os.stat("source", dir_fd=quarantine_descriptor, follow_symlinks=False)
            except OSError as exc:
                try:
                    _restore_quarantined_upload_source(
                        quarantine_descriptor=quarantine_descriptor,
                        parent=parent,
                        name=name,
                        quarantine=quarantine,
                    )
                except RuntimeError:
                    retained = True
                    raise
                raise RuntimeError("quarantined upload source identity cannot be verified") from exc
            quarantined_fingerprint = UploadFileFingerprint(
                size=int(quarantined.st_size),
                mtime_ns=int(quarantined.st_mtime_ns),
                device=int(quarantined.st_dev),
                inode=int(quarantined.st_ino),
            )
            attached = _upload_source_parent_is_attached(
                root_path=root_path,
                root=descriptors[0],
                parent=parent,
                parts=parts,
            )
            quarantined_matches = (
                stat.S_ISREG(quarantined.st_mode) and quarantined_fingerprint == fingerprint and attached
            )
            if not quarantined_matches:
                try:
                    _restore_quarantined_upload_source(
                        quarantine_descriptor=quarantine_descriptor,
                        parent=parent,
                        name=name,
                        quarantine=quarantine,
                    )
                except RuntimeError:
                    retained = True
                    raise
                return {"deleted": False, "missing": False, "changed": True}

            os.unlink("source", dir_fd=quarantine_descriptor)
            os.fsync(quarantine_descriptor)
            return {"deleted": True, "missing": False, "changed": False}
        finally:
            if quarantine_descriptor is not None:
                os.close(quarantine_descriptor)
            if not retained:
                try:
                    os.rmdir(quarantine, dir_fd=parent)
                except FileNotFoundError:
                    pass
                os.fsync(parent)
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _restore_quarantined_upload_source(
    *,
    quarantine_descriptor: int,
    parent: int,
    name: str,
    quarantine: str,
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
        message = f"upload source changed during atomic quarantine; retained at {quarantine}/source"
        raise RuntimeError(message) from exc
    except OSError as exc:
        message = f"upload source cannot be safely restored; retained at {quarantine}/source"
        raise RuntimeError(message) from exc
    os.unlink("source", dir_fd=quarantine_descriptor)
    os.fsync(quarantine_descriptor)
    os.fsync(parent)


def _upload_source_parent_is_attached(
    *,
    root_path: str,
    root: int,
    parent: int,
    parts: Sequence[str],
) -> bool:
    try:
        current_root = os.lstat(root_path)
        opened_root = os.fstat(root)
        if (
            stat.S_ISLNK(current_root.st_mode)
            or not stat.S_ISDIR(current_root.st_mode)
            or (current_root.st_dev, current_root.st_ino) != (opened_root.st_dev, opened_root.st_ino)
        ):
            return False
        current = os.dup(root)
        try:
            flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
            for part in parts[:-1]:
                child = os.open(part, flags, dir_fd=current)
                os.close(current)
                current = child
            expected = os.fstat(current)
            observed = os.fstat(parent)
            return (expected.st_dev, expected.st_ino) == (observed.st_dev, observed.st_ino)
        finally:
            os.close(current)
    except OSError:
        return False


def _outcome(
    disposition: UploadProviderDisposition,
    operation: UploadOperation,
    *,
    request_sent: bool,
    session: Mapping[str, Any] | None = None,
    receipt: Mapping[str, Any] | None = None,
    error: Mapping[str, Any] | None = None,
) -> UploadProviderOutcome:
    return UploadProviderOutcome(
        disposition=disposition,
        backend=str(operation.upload_backend or (session or {}).get("backend") or ""),
        request_sent=request_sent,
        session=dict(session) if session is not None else None,
        receipt=dict(receipt) if receipt is not None else None,
        error=dict(error) if error is not None else None,
    )


def _item_name(item: Mapping[str, Any]) -> str:
    return str(item.get("fn") or item.get("file_name") or item.get("n") or item.get("name") or "")


def _item_id(item: Mapping[str, Any]) -> str:
    return str(item.get("fid") or item.get("file_id") or item.get("id") or item.get("cid") or "").strip()


def _item_pick_code(item: Mapping[str, Any]) -> str:
    return str(item.get("pc") or item.get("pick_code") or item.get("pickcode") or "").strip()


def _item_sha1(item: Mapping[str, Any]) -> str:
    return str(item.get("sha1") or item.get("sha") or item.get("file_sha1") or "").strip().upper()


def _item_size(item: Mapping[str, Any]) -> int:
    try:
        return int(item.get("fs") or item.get("size") or item.get("file_size") or 0)
    except (TypeError, ValueError):
        return -1


def _is_directory(item: Mapping[str, Any]) -> bool:
    category = item.get("fc") if item.get("fc") is not None else item.get("file_category")
    return str(category) == "0" or str(item.get("is_dir") or "").casefold() in {"1", "true"}


def _provider_response_succeeded(value: Mapping[str, Any]) -> bool:
    if value.get("state") is True or value.get("success") is True:
        return True
    return str(value.get("code") or "").strip() in {"0", "200"}


def _remote_file_info_exists(value: Mapping[str, Any], expected_fid: str) -> bool:
    if _provider_response_succeeded(value):
        data: object = value.get("data")
        if isinstance(data, list):
            data = data[0] if data else None
        if isinstance(data, Mapping):
            observed = _item_id(data)
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
    raise RuntimeError("115 remote file readback was not authoritative")


__all__ = [
    "LegacyP115UploadProjection",
    "LegacyP115UploadProvider",
    "NativeP115UploadProvider",
    "NativeP115UploadSourceCleanupGateway",
    "P115CompletedUploadSourceRepository",
]
