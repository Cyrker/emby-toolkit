from __future__ import annotations

import json
import re
import secrets
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import unquote

from sqlalchemy import text

from etk.domains.media.models import ServiceResult
from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs import PersistentEffectIndeterminate

from .item_ready import P115BridgeEmbyItemVerifier, P115BridgeIdentityResolver
from .media_info import P115MediaInfoError

P115_DEEP_DELETE_TTL_SECONDS = 300
P115_DEEP_DELETE_SETTING_PREFIX = "runtime.deep_delete_snapshot."
P115_DEEP_DELETE_OPERATION = "p115.deep-delete.execute"
P115_DEEP_DELETE_SCHEMA_VERSION = 1

_SUPPORTED_ITEM_TYPES = frozenset({"Movie", "Series", "Season", "Episode", "Video"})
_EXACT_VERSION_TYPES = frozenset({"Movie", "Episode", "Video"})


class P115DeepDeleteEmbyGateway(Protocol):
    def root_item(self, item_id: str) -> Mapping[str, Any] | None: ...

    def child_items(self, root_item_id: str) -> Sequence[Mapping[str, Any]]: ...


class P115DeepDeleteScopePort(Protocol):
    def pickcodes_for_deleted_item(self, emby_item_id: str, item_type: str) -> Sequence[str]: ...

    def pick_code_by_sha1(self, sha1: str) -> str | None: ...

    def file_snapshots(self, pickcodes: Sequence[str]) -> Sequence[Mapping[str, Any]]: ...


class P115DeepDeleteSnapshotRepository(Protocol):
    def store(self, token: str, snapshot: Mapping[str, Any]) -> None: ...

    def claim(self, token: str) -> Mapping[str, Any] | None: ...


@dataclass(frozen=True, slots=True)
class P115DeepDeleteJob:
    job_id: str
    payload: Mapping[str, Any]


class P115DeepDeleteSubmissionPort(Protocol):
    def submit(
        self,
        operation: str,
        payload: Mapping[str, Any],
        *,
        max_attempts: int,
        timeout_seconds: int,
        idempotency_scope: str,
        idempotency_key: str,
    ) -> P115DeepDeleteJob: ...

    def lookup(self, *, idempotency_scope: str, idempotency_key: str) -> P115DeepDeleteJob | None: ...


class P115DeleteReadbackPort(Protocol):
    def delete_and_reconcile(
        self,
        pickcodes: Sequence[str],
        file_snapshots: Sequence[Mapping[str, Any]],
    ) -> Mapping[str, Any]: ...


class P115UploadSourceCleanupPort(Protocol):
    def delete_sources(self, pickcodes: Sequence[str]) -> Mapping[str, Any]: ...


class P115MediaCleanupReceiptPort(Protocol):
    def cleanup(
        self,
        *,
        item_id: str,
        item_name: str,
        item_type: str,
        series_id: str,
        local_only: bool,
        operation_id: str,
    ) -> Mapping[str, Any]: ...


class P115SharedCleanupPort(Protocol):
    def cleanup(
        self,
        snapshot: Mapping[str, Any],
        media_result: Mapping[str, Any],
        *,
        operation_id: str,
    ) -> Mapping[str, Any]: ...


class SqlAlchemyP115DeepDeleteSnapshotRepository:
    """PostgreSQL-backed, single-claim capability snapshot storage."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        *,
        ttl_seconds: int = P115_DEEP_DELETE_TTL_SECONDS,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._ttl_seconds = int(ttl_seconds)

    def store(self, token: str, snapshot: Mapping[str, Any]) -> None:
        normalized_token = str(token or "").strip()
        if not normalized_token or not isinstance(snapshot, Mapping):
            raise ValueError("deep-delete snapshot requires a token and object payload")
        with self._unit_of_work() as uow:
            uow.session.execute(
                text(
                    """
                    DELETE FROM app_settings
                    WHERE setting_key LIKE :prefix
                      AND last_updated_at < NOW() - make_interval(secs => :ttl_seconds)
                    """
                ),
                {
                    "prefix": f"{P115_DEEP_DELETE_SETTING_PREFIX}%",
                    "ttl_seconds": self._ttl_seconds,
                },
            )
            uow.session.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES (:setting_key, CAST(:value_json AS jsonb), NOW())
                    ON CONFLICT (setting_key) DO UPDATE SET
                        value_json = EXCLUDED.value_json,
                        last_updated_at = NOW()
                    """
                ),
                {
                    "setting_key": f"{P115_DEEP_DELETE_SETTING_PREFIX}{normalized_token}",
                    "value_json": json.dumps(dict(snapshot), ensure_ascii=False),
                },
            )
            uow.commit()

    def claim(self, token: str) -> Mapping[str, Any] | None:
        normalized_token = str(token or "").strip()
        if not normalized_token:
            return None
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        DELETE FROM app_settings
                        WHERE setting_key = :setting_key
                        RETURNING value_json
                        """
                    ),
                    {"setting_key": f"{P115_DEEP_DELETE_SETTING_PREFIX}{normalized_token}"},
                )
                .mappings()
                .one_or_none()
            )
            uow.commit()
        if row is None:
            return None
        value = row.get("value_json")
        if isinstance(value, str):
            value = json.loads(value)
        return dict(value) if isinstance(value, Mapping) else None


class P115DeepDeleteService:
    """Native two-phase Bridge delete orchestration; path identity remains separate from token capability."""

    def __init__(
        self,
        *,
        identities: P115BridgeIdentityResolver,
        verifier: P115BridgeEmbyItemVerifier,
        emby: P115DeepDeleteEmbyGateway,
        scope: P115DeepDeleteScopePort,
        snapshots: P115DeepDeleteSnapshotRepository,
        submissions: P115DeepDeleteSubmissionPort,
        clock: Callable[[], float] = time.time,
        token_factory: Callable[[], str] = lambda: secrets.token_urlsafe(24),
    ) -> None:
        self._identities = identities
        self._verifier = verifier
        self._emby = emby
        self._scope = scope
        self._snapshots = snapshots
        self._submissions = submissions
        self._clock = clock
        self._token_factory = token_factory

    def prepare_by_pick_code(self, pick_code: object, payload: Mapping[str, Any]) -> ServiceResult:
        normalized_pick_code = str(pick_code or "").strip()
        sha1 = self._sha1_by_pick_code(normalized_pick_code)
        if not sha1:
            return ServiceResult({"error": "media identity not found"}, 404)
        return self._prepare(sha1, payload, expected_pick_code=normalized_pick_code)

    def prepare_by_sha1(self, sha1: object, payload: Mapping[str, Any]) -> ServiceResult:
        return self._prepare(_canonical_sha1(sha1), payload)

    def commit_by_pick_code(self, pick_code: object, payload: Mapping[str, Any]) -> ServiceResult:
        if not self._sha1_by_pick_code(str(pick_code or "").strip()):
            return ServiceResult({"error": "media identity not found"}, 404)
        return self._commit(payload)

    def commit_by_sha1(self, sha1: object, payload: Mapping[str, Any]) -> ServiceResult:
        if not _canonical_sha1(sha1):
            return ServiceResult({"error": "media identity not found"}, 404)
        return self._commit(payload)

    def _sha1_by_pick_code(self, pick_code: str) -> str:
        try:
            return _canonical_sha1(self._identities.sha1_by_pick_code(pick_code))
        except (P115MediaInfoError, ValueError):
            return ""

    def _prepare(
        self,
        sha1: str,
        payload: Mapping[str, Any],
        *,
        expected_pick_code: str = "",
    ) -> ServiceResult:
        root_item_id = str(payload.get("root_item_id") or "").strip()
        anchor_item_id = str(payload.get("anchor_item_id") or "").strip()
        requested_type = str(payload.get("item_type") or "").strip().title()
        if not re.fullmatch(r"\d{1,20}", root_item_id) or not re.fullmatch(r"\d{1,20}", anchor_item_id):
            return ServiceResult({"ok": False, "error": "invalid item id"}, 400)
        if requested_type not in _SUPPORTED_ITEM_TYPES:
            return ServiceResult({"ok": False, "error": "unsupported item type"}, 400)
        if not sha1:
            return ServiceResult({"ok": False, "error": "anchor identity mismatch"}, 409)

        anchor = self._verifier.verified_item(
            sha1,
            anchor_item_id,
            expected_pick_code=expected_pick_code,
        )
        if not isinstance(anchor, Mapping) or not anchor:
            return ServiceResult({"ok": False, "error": "anchor identity mismatch"}, 409)
        root = self._emby.root_item(root_item_id)
        if not isinstance(root, Mapping) or not root:
            return ServiceResult({"ok": False, "error": "root item not found"}, 404)
        actual_type = str(root.get("Type") or requested_type).strip().title()
        if actual_type != requested_type:
            return ServiceResult({"ok": False, "error": "root item type mismatch"}, 409)

        token = str(self._token_factory() or "").strip()
        if not token:
            raise RuntimeError("deep-delete token factory returned an empty token")
        created_at = float(self._clock())
        item_name = str(root.get("Name") or payload.get("item_name") or root_item_id)
        series_id = str(root.get("SeriesId") or payload.get("series_id") or "").strip()
        virtual_id = _extract_virtual_id(anchor, sha1)
        if virtual_id:
            snapshot = {
                "created_at": created_at,
                "delete_token": token,
                "item_id": root_item_id,
                "item_name": item_name,
                "item_type": actual_type,
                "series_id": series_id,
                "pickcodes": [],
                "p115_file_snapshots": [],
                "local_only": True,
                "virtual_id": virtual_id,
            }
            self._snapshots.store(token, snapshot)
            return ServiceResult(
                {
                    "ok": True,
                    "token": token,
                    "pickcode_count": 0,
                    "local_only": True,
                    "virtual_id": virtual_id,
                }
            )

        pickcodes = _unique_pickcodes(self._scope.pickcodes_for_deleted_item(root_item_id, actual_type))
        anchor_pick_code = str(expected_pick_code or self._scope.pick_code_by_sha1(sha1) or "").strip()
        if not pickcodes:
            pickcodes = _fallback_pickcodes(anchor, self._emby, root_item_id, actual_type)
            if not anchor_pick_code and pickcodes:
                anchor_pick_code = pickcodes[0]
        matched_anchor = (
            next(
                (pick_code for pick_code in pickcodes if pick_code.casefold() == anchor_pick_code.casefold()),
                "",
            )
            if anchor_pick_code
            else ""
        )
        if not pickcodes or not matched_anchor:
            return ServiceResult({"ok": False, "error": "delete scope identity mismatch"}, 409)
        if actual_type in _EXACT_VERSION_TYPES:
            pickcodes = (matched_anchor,)

        file_snapshots = _normalize_file_snapshots(self._scope.file_snapshots(pickcodes), pickcodes)
        snapshot = {
            "created_at": created_at,
            "delete_token": token,
            "item_id": root_item_id,
            "item_name": item_name,
            "item_type": actual_type,
            "series_id": series_id,
            "pickcodes": list(pickcodes),
            "p115_file_snapshots": file_snapshots,
        }
        self._snapshots.store(token, snapshot)
        return ServiceResult({"ok": True, "token": token, "pickcode_count": len(pickcodes)})

    def _commit(self, payload: Mapping[str, Any]) -> ServiceResult:
        token = str(payload.get("token") or "").strip()
        if not token:
            return ServiceResult({"ok": False, "error": "missing token"}, 400)
        claimed = self._snapshots.claim(token)
        if claimed is None:
            existing = self._submissions.lookup(
                idempotency_scope="deep-delete",
                idempotency_key=token,
            )
            if existing is None:
                return ServiceResult({"ok": False, "error": "delete snapshot not found"}, 404)
            return _accepted(existing, _job_snapshot(existing))

        original_snapshot = dict(claimed)
        try:
            created_at = float(original_snapshot.get("created_at") or 0)
        except (TypeError, ValueError):
            created_at = 0
        if self._clock() - created_at > P115_DEEP_DELETE_TTL_SECONDS:
            return ServiceResult({"ok": False, "error": "delete snapshot expired"}, 410)

        execution_snapshot = {**original_snapshot, "delete_token": token}
        operation_payload = {
            "schema_version": P115_DEEP_DELETE_SCHEMA_VERSION,
            "snapshot": execution_snapshot,
        }
        try:
            job = self._submissions.submit(
                P115_DEEP_DELETE_OPERATION,
                operation_payload,
                max_attempts=5,
                timeout_seconds=900,
                idempotency_scope="deep-delete",
                idempotency_key=token,
            )
        except Exception as exc:
            self._snapshots.store(token, original_snapshot)
            return ServiceResult(
                {"ok": False, "error": f"failed to submit delete job: {exc}"},
                500,
            )
        return _accepted(job, execution_snapshot)


class P115DeepDeleteExecutionService:
    """Importable native operation target with all external effects behind explicit ports."""

    def __init__(
        self,
        *,
        p115: P115DeleteReadbackPort,
        upload_sources: P115UploadSourceCleanupPort,
        media: P115MediaCleanupReceiptPort,
        shared: P115SharedCleanupPort,
    ) -> None:
        self._p115 = p115
        self._upload_sources = upload_sources
        self._media = media
        self._shared = shared

    def execute(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        snapshot = _execution_snapshot(payload)
        operation_id = str(snapshot.get("delete_token") or "").strip()
        item_id = str(snapshot.get("item_id") or "").strip()
        item_name = str(snapshot.get("item_name") or item_id)
        item_type = str(snapshot.get("item_type") or "").strip()
        series_id = str(snapshot.get("series_id") or "").strip()
        raw_local_only = snapshot.get("local_only", False)
        if not isinstance(raw_local_only, bool):
            raise ValueError("deep-delete execution local_only must be a boolean")
        local_only = raw_local_only
        raw_pickcodes = _sequence(snapshot.get("pickcodes"), label="pickcodes")
        raw_file_snapshots = _mapping_sequence(
            snapshot.get("p115_file_snapshots"),
            label="p115_file_snapshots",
        )
        pickcodes = _unique_pickcodes(raw_pickcodes)
        file_snapshots = _normalize_file_snapshots(raw_file_snapshots, pickcodes)
        if not operation_id or not item_id or item_type not in _SUPPORTED_ITEM_TYPES:
            raise ValueError("deep-delete execution snapshot identity is invalid")
        if not local_only and not pickcodes:
            raise ValueError("non-local deep-delete execution requires PickCodes")

        upload_result: Mapping[str, Any]
        p115_result: Mapping[str, Any]
        if local_only:
            upload_result = {"ok": True, "skipped": True, "local_only": True}
            p115_result = {"ok": True, "skipped": True, "local_only": True}
        else:
            try:
                upload_result = self._upload_sources.delete_sources(pickcodes)
            except Exception as exc:
                upload_result = {"ok": False, "error": str(exc)}
            p115_result = self._p115.delete_and_reconcile(pickcodes, file_snapshots)
            if not isinstance(p115_result, Mapping) or p115_result.get("ok") is not True:
                raise RuntimeError("115 delete was not confirmed")

        media_result = self._media.cleanup(
            item_id=item_id,
            item_name=item_name,
            item_type=item_type,
            series_id=series_id,
            local_only=local_only,
            operation_id=operation_id,
        )
        if isinstance(media_result, Mapping) and media_result.get("indeterminate") is True:
            raise PersistentEffectIndeterminate("shared cleanup outcome is unknown; automatic replay is forbidden")
        if not isinstance(media_result, Mapping) or media_result.get("ok") is not True:
            raise RuntimeError("local media cleanup was not confirmed")

        if local_only:
            shared_result: Mapping[str, Any] = {"ok": True, "skipped": True, "local_only": True}
        else:
            shared_result = self._shared.cleanup(snapshot, media_result, operation_id=operation_id)
            if not isinstance(shared_result, Mapping) or shared_result.get("ok") is not True:
                raise RuntimeError("shared cleanup was not confirmed")
        return {
            "ok": True,
            "local_only": local_only,
            "item_id": item_id,
            "upload_source": dict(upload_result),
            "p115_delete": dict(p115_result),
            "media_cleanup": dict(media_result),
            "shared_cleanup": dict(shared_result),
        }


def _accepted(job: P115DeepDeleteJob, snapshot: Mapping[str, Any]) -> ServiceResult:
    return ServiceResult(
        {
            "ok": True,
            "accepted": True,
            "job_id": str(job.job_id),
            "pickcode_count": len(snapshot.get("pickcodes") or []),
            "local_only": bool(snapshot.get("local_only")),
        },
        202,
    )


def _job_snapshot(job: P115DeepDeleteJob) -> Mapping[str, Any]:
    snapshot = job.payload.get("snapshot")
    return dict(snapshot) if isinstance(snapshot, Mapping) else {}


def _execution_snapshot(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping) or payload.get("schema_version") != P115_DEEP_DELETE_SCHEMA_VERSION:
        raise ValueError("deep-delete operation payload schema is not supported")
    snapshot = payload.get("snapshot")
    if not isinstance(snapshot, Mapping):
        raise ValueError("deep-delete operation payload requires a snapshot")
    return dict(snapshot)


def _canonical_sha1(value: object) -> str:
    normalized = str(value or "").strip().upper()
    return normalized if re.fullmatch(r"[A-F0-9]{40}", normalized) else ""


def _unique_pickcodes(values: Sequence[object]) -> tuple[str, ...]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        pick_code = str(value or "").strip()
        key = pick_code.casefold()
        if pick_code and key not in seen:
            seen.add(key)
            output.append(pick_code)
    return tuple(output)


def _sequence(value: object, *, label: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"deep-delete execution {label} must be an array")
    return value


def _mapping_sequence(value: object, *, label: str) -> tuple[Mapping[str, Any], ...]:
    values = _sequence(value, label=label)
    if any(not isinstance(entry, Mapping) for entry in values):
        raise ValueError(f"deep-delete execution {label} must contain objects")
    return tuple(entry for entry in values if isinstance(entry, Mapping))


def _item_pickcodes(item: Mapping[str, Any]) -> tuple[str, ...]:
    values: list[object] = [item.get("Path")]
    sources = item.get("MediaSources")
    if isinstance(sources, Sequence) and not isinstance(sources, (str, bytes)):
        values.extend(source.get("Path") for source in sources if isinstance(source, Mapping))
    pickcodes: list[str] = []
    for value in values:
        match = re.search(r"/api/p115/play/([^/?#]+)", unquote(str(value or "")), flags=re.IGNORECASE)
        if match:
            pickcodes.append(match.group(1).strip())
    return _unique_pickcodes(pickcodes)


def _extract_virtual_id(item: Mapping[str, Any], sha1: str) -> int:
    values: list[object] = [item.get("Path")]
    sources = item.get("MediaSources")
    if isinstance(sources, Sequence) and not isinstance(sources, (str, bytes)):
        values.extend(source.get("Path") for source in sources if isinstance(source, Mapping))
    for value in values:
        match = re.search(
            r"/api/p115/virtual-play/(\d+)/([A-Fa-f0-9]{40})(?:[/?#]|$)",
            unquote(str(value or "")),
            flags=re.IGNORECASE,
        )
        if match and match.group(2).upper() == sha1:
            return int(match.group(1))
    return 0


def _fallback_pickcodes(
    anchor: Mapping[str, Any],
    emby: P115DeepDeleteEmbyGateway,
    root_item_id: str,
    item_type: str,
) -> tuple[str, ...]:
    pickcodes = list(_item_pickcodes(anchor))
    if item_type in {"Series", "Season"}:
        for child in emby.child_items(root_item_id):
            pickcodes.extend(_item_pickcodes(child))
    return _unique_pickcodes(pickcodes)


def _normalize_file_snapshots(
    snapshots: Sequence[Mapping[str, Any]],
    pickcodes: Sequence[str],
) -> list[dict[str, str]]:
    allowed = {pick_code.casefold() for pick_code in pickcodes}
    output: list[dict[str, str]] = []
    for snapshot in snapshots:
        pick_code = str(snapshot.get("pick_code") or "").strip()
        fid = str(snapshot.get("fid") or snapshot.get("id") or "").strip()
        if not pick_code or not fid or pick_code.casefold() not in allowed:
            continue
        output.append(
            {
                "pick_code": pick_code,
                "fid": fid,
                "parent_id": str(snapshot.get("parent_id") or "").strip(),
            }
        )
    return output


__all__ = [
    "P115_DEEP_DELETE_OPERATION",
    "P115_DEEP_DELETE_SCHEMA_VERSION",
    "P115_DEEP_DELETE_SETTING_PREFIX",
    "P115_DEEP_DELETE_TTL_SECONDS",
    "P115DeepDeleteEmbyGateway",
    "P115DeepDeleteExecutionService",
    "P115DeepDeleteJob",
    "P115DeepDeleteScopePort",
    "P115DeepDeleteService",
    "P115DeepDeleteSnapshotRepository",
    "P115DeepDeleteSubmissionPort",
    "P115DeleteReadbackPort",
    "P115MediaCleanupReceiptPort",
    "P115SharedCleanupPort",
    "P115UploadSourceCleanupPort",
    "SqlAlchemyP115DeepDeleteSnapshotRepository",
]
