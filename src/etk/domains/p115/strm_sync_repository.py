from __future__ import annotations

import hashlib
import posixpath
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session

from etk.platform.config.locking import DYNAMIC_CONFIG_LOCK_NAMESPACE, P115_AUTH_LOCK_KEY
from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs.json_payload import canonical_json, normalize_json_object
from etk.platform.observability.redaction import redact_sensitive

from .strm_effects import EMBY_PATHS_CHANGED_EFFECT, stage_emby_path_change_effects
from .strm_sync import (
    P115StrmCacheEntry,
    P115StrmCacheReconcileResult,
    P115StrmCacheSnapshot,
    P115StrmNotificationRecovery,
    P115StrmSyncPlan,
    P115StrmSyncRoot,
    cache_snapshot_revision,
)


class SqlAlchemyP115StrmSyncRepository:
    """Promote one complete remote snapshot and its Emby notification intents atomically."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def snapshot(
        self,
        roots: Sequence[P115StrmSyncRoot],
        *,
        account_id: str,
    ) -> P115StrmCacheSnapshot:
        root_ids = _root_ids(roots)
        account = _account_id(account_id)
        with self._unit_of_work() as uow:
            _lock_and_assert_account(uow, account)
            _assert_no_unowned_cache_rows(uow.session)
            entries = _load_entries(uow.session, root_ids, account_id=account)
        return P115StrmCacheSnapshot(entries, cache_snapshot_revision(entries))

    def recovery_snapshot(
        self,
        *,
        account_id: str,
        authorization_revision: str,
    ) -> P115StrmNotificationRecovery:
        account = _account_id(account_id)
        authorization = _sha256(authorization_revision, "authorization_revision")
        with self._unit_of_work() as uow:
            _lock_and_assert_account(uow, account)
            _assert_no_unowned_cache_rows(uow.session)
            active_rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT *
                        FROM p115_strm_sync_plans
                        WHERE account_id = :account_id
                          AND status IN ('applying', 'retryable')
                        ORDER BY created_at, plan_id
                        """
                    ),
                    {"account_id": account},
                )
                .mappings()
                .all()
            )
            modified, deleted = _authorization_drift_recovery_paths(
                uow.session,
                account_id=account,
                authorization_revision=authorization,
            )
            deleted_path_roots = _notification_path_roots(
                uow.session,
                account_id=account,
                paths=deleted,
            )
        return P115StrmNotificationRecovery(
            active_plans=tuple(_plan_projection(row) for row in active_rows),
            modified_paths=modified,
            deleted_paths=deleted,
            deleted_path_roots=deleted_path_roots,
        )

    def stage_plan(
        self,
        *,
        account_id: str,
        scope: str,
        provider_backend: str,
        expected_cache_revision: str,
        operation_revision: str,
        plan: Mapping[str, Any],
    ) -> P115StrmSyncPlan:
        account = _account_id(account_id)
        normalized_scope = str(scope or "").strip()
        if normalized_scope not in {"full", "home_video"}:
            raise ValueError("STRM sync plan scope is invalid")
        backend = str(provider_backend or "").strip().lower()
        if backend not in {"openapi", "cookie"}:
            raise ValueError("STRM sync plan backend is invalid")
        cache_revision = _sha256(expected_cache_revision, "expected_cache_revision")
        operation = _sha256(operation_revision, "operation_revision")
        normalized_plan = _validated_plan(plan)
        with self._unit_of_work() as uow:
            _lock_and_assert_account(uow, account)
            _assert_no_unowned_cache_rows(uow.session)
            active_rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT *
                        FROM p115_strm_sync_plans
                        WHERE account_id = :account_id
                          AND status IN ('applying', 'retryable')
                        ORDER BY created_at, plan_id
                        FOR UPDATE
                        """
                    ),
                    {"account_id": account},
                )
                .mappings()
                .all()
            )
            notifications = normalized_plan["notifications"]
            assert isinstance(notifications, dict)
            authorization = _sha256(
                notifications.get("emby_authorization_revision"),
                "emby_authorization_revision",
            )
            recoverable_modified, recoverable_deleted = _recoverable_notification_paths(
                uow.session,
                active_rows=active_rows,
                account_id=account,
                authorization_revision=authorization,
            )
            _assert_recovered_notifications_authorized(
                normalized_plan,
                modified_paths=recoverable_modified,
                deleted_paths=recoverable_deleted,
            )

            envelope_matches = [
                candidate
                for candidate in active_rows
                if str(candidate["scope"]) == normalized_scope
                and str(candidate["expected_cache_revision"]) == cache_revision
                and str(candidate["operation_revision"]) == operation
            ]
            exact_matches = [
                candidate
                for candidate in envelope_matches
                if str(candidate["provider_backend"]) == backend
                and _validated_plan(candidate["plan_json"]) == normalized_plan
            ]
            if len(exact_matches) > 1:
                raise RuntimeError("存在重复的相同 active STRM sync action plan")
            row = exact_matches[0] if exact_matches else None

            if row is None:
                plan_id = uuid4()
                plan_key = hashlib.sha256(
                    canonical_json(
                        {
                            "plan_id": str(plan_id),
                            "account_id": account,
                            "scope": normalized_scope,
                            "provider_backend": backend,
                            "expected_cache_revision": cache_revision,
                            "operation_revision": operation,
                            "plan": normalized_plan,
                        },
                        field="STRM sync plan envelope",
                    ).encode()
                ).hexdigest()
                row = (
                    uow.session.execute(
                        text(
                            """
                            INSERT INTO p115_strm_sync_plans (
                                plan_id, plan_key, account_id, scope, provider_backend,
                                expected_cache_revision, operation_revision, plan_json,
                                status, attempts, updated_at
                            ) VALUES (
                                :plan_id, :plan_key, :account_id, :scope, :provider_backend,
                                :expected_cache_revision, :operation_revision,
                                CAST(:plan_json AS jsonb), 'applying', 1, NOW()
                            )
                            RETURNING *
                            """
                        ),
                        {
                            "plan_id": plan_id,
                            "plan_key": plan_key,
                            "account_id": account,
                            "scope": normalized_scope,
                            "provider_backend": backend,
                            "expected_cache_revision": cache_revision,
                            "operation_revision": operation,
                            "plan_json": canonical_json(normalized_plan, field="STRM sync action plan"),
                        },
                    )
                    .mappings()
                    .one()
                )
            else:
                row = (
                    uow.session.execute(
                        text(
                            """
                            UPDATE p115_strm_sync_plans
                            SET status = 'applying',
                                attempts = attempts + 1,
                                last_error_json = NULL,
                                updated_at = NOW()
                            WHERE plan_id = :plan_id
                            RETURNING *
                            """
                        ),
                        {"plan_id": row["plan_id"]},
                    )
                    .mappings()
                    .one()
                )
            uow.commit()
        if row is None:
            raise RuntimeError("STRM sync action plan staging returned no row")
        return _plan_projection(row)

    def reconcile(
        self,
        roots: Sequence[P115StrmSyncRoot],
        entries: Sequence[P115StrmCacheEntry],
        *,
        plan_id: UUID,
        account_id: str,
        expected_revision: str,
        operation_revision: str,
    ) -> P115StrmCacheReconcileResult:
        root_ids = _root_ids(roots)
        account = _account_id(account_id)
        desired = _validated_entries(entries, root_ids)
        expected = _sha256(expected_revision, "expected_revision")
        with self._unit_of_work() as uow:
            _lock_and_assert_account(uow, account)
            _assert_no_unowned_cache_rows(uow.session)
            plan_row = (
                uow.session.execute(
                    text("SELECT * FROM p115_strm_sync_plans WHERE plan_id = :plan_id FOR UPDATE"),
                    {"plan_id": plan_id},
                )
                .mappings()
                .one_or_none()
            )
            if plan_row is None:
                raise RuntimeError("STRM sync action plan 不存在")
            if str(plan_row["account_id"]) != account:
                raise RuntimeError("STRM sync action plan 账号不匹配")
            if str(plan_row["expected_cache_revision"]) != expected:
                raise RuntimeError("STRM sync action plan 缓存前置版本不匹配")
            if str(plan_row["operation_revision"]) != _sha256(operation_revision, "operation_revision"):
                raise RuntimeError("STRM sync action plan 目标版本不匹配")
            if str(plan_row["status"]) not in {"applying", "retryable"}:
                raise RuntimeError("STRM sync action plan 当前状态不能确认")
            plan_json = _validated_plan(plan_row["plan_json"])
            expected_roots = [{"cid": root.cid, "relative_path": root.relative_path} for root in roots]
            if plan_json.get("roots") != expected_roots:
                raise RuntimeError("STRM sync action plan 同步根目录与目标提交不匹配")
            if plan_json.get("desired_cache_revision") != cache_snapshot_revision(desired):
                raise RuntimeError("STRM sync action plan 目标缓存投影与提交内容不匹配")
            current = _load_entries(uow.session, root_ids, account_id=account)
            if cache_snapshot_revision(current) != expected:
                raise RuntimeError("STRM 缓存在本地文件应用期间发生变化，事务已回滚")

            cleaned = 0
            desired_ids = {entry.id for entry in desired}
            for entry in desired:
                values = _entry_values(entry, account)
                conflict = uow.session.execute(
                    text(
                        """
                        DELETE FROM p115_filesystem_cache
                        WHERE parent_id = :parent_id
                          AND name = :name
                          AND id <> :id
                          AND account_id = :account_id
                          AND sync_root_id = :sync_root_id
                        """
                    ),
                    values,
                )
                cleaned += _rowcount(conflict)
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
                            entry_kind = :entry_kind,
                            sync_root_id = :sync_root_id,
                            strm_content_sha256 = :strm_content_sha256,
                            washing_level = CASE WHEN :entry_kind = 'directory' THEN NULL ELSE washing_level END,
                            washing_snapshot_json = CASE
                                WHEN :entry_kind = 'directory' THEN '{}'::jsonb
                                ELSE washing_snapshot_json
                            END,
                            updated_at = NOW()
                        WHERE id = :id
                          AND account_id = :account_id
                        """
                    ),
                    values,
                )
                if _rowcount(updated):
                    continue
                uow.session.execute(
                    text(
                        """
                        INSERT INTO p115_filesystem_cache (
                            id, parent_id, name, local_path, sha1, pick_code, size,
                            account_id, entry_kind, sync_root_id, strm_content_sha256
                        ) VALUES (
                            :id, :parent_id, :name, :local_path, :sha1, :pick_code, :size,
                            :account_id, :entry_kind, :sync_root_id, :strm_content_sha256
                        )
                        """
                    ),
                    values,
                )

            stale_ids = sorted({entry.id for entry in current} - desired_ids)
            if stale_ids:
                deleted = uow.session.execute(
                    text(
                        """
                        DELETE FROM p115_filesystem_cache
                        WHERE id = ANY(CAST(:stale_ids AS text[]))
                          AND account_id = :account_id
                        """
                    ),
                    {"stale_ids": stale_ids, "account_id": account},
                )
                cleaned += _rowcount(deleted)

            observed = _load_entries(uow.session, root_ids, account_id=account)
            if observed != desired:
                raise RuntimeError("STRM 缓存提交后校验不一致，事务已回滚")
            notifications = plan_json.get("notifications")
            assert isinstance(notifications, dict)
            recovered_notifications = bool(
                _path_array(
                    notifications.get("recovered_modified_paths"),
                    "recovered_modified_paths",
                )
                or _path_array(
                    notifications.get("recovered_deleted_paths"),
                    "recovered_deleted_paths",
                )
            )
            effect_ids = stage_emby_path_change_effects(
                uow.session,
                account_id=account,
                operation_revision=_sha256(operation_revision, "operation_revision"),
                authorization_revision=str(notifications.get("emby_authorization_revision") or ""),
                modified_paths=_path_array(notifications.get("modified_paths"), "modified_paths"),
                deleted_paths=_path_array(notifications.get("deleted_paths"), "deleted_paths"),
                plan_revision=_sha256(plan_row.get("plan_key"), "plan_key"),
                recovery_intent=recovered_notifications,
            )
            uow.session.execute(
                text(
                    """
                    UPDATE p115_strm_sync_plans
                    SET status = 'confirmed',
                        local_effects_started = local_effects_started OR :has_local_actions,
                        last_error_json = NULL,
                        finished_at = NOW(),
                        updated_at = NOW()
                    WHERE plan_id = :plan_id
                    """
                ),
                {
                    "plan_id": plan_id,
                    "has_local_actions": _plan_has_local_actions(plan_json),
                },
            )
            uow.session.execute(
                text(
                    """
                    UPDATE p115_strm_sync_plans
                    SET status = 'superseded',
                        updated_at = NOW()
                    WHERE account_id = :account_id
                      AND plan_id <> :plan_id
                      AND status IN ('applying', 'retryable')
                    """
                ),
                {"account_id": account, "plan_id": plan_id},
            )
            uow.commit()
        return P115StrmCacheReconcileResult(
            cached=len(desired),
            cleaned=cleaned,
            effect_ids=tuple(str(effect_id) for effect_id in effect_ids),
        )

    def mark_plan_retryable(
        self,
        plan_id: UUID,
        *,
        local_effects_started: bool,
        error: Mapping[str, Any] | Any,
    ) -> None:
        raw_error = dict(error) if isinstance(error, Mapping) else {"message": str(error)}
        safe_error = redact_sensitive(raw_error)
        normalized_error = normalize_json_object(
            dict(safe_error) if isinstance(safe_error, dict) else {"message": str(safe_error)},
            field="STRM sync plan error",
        )
        with self._unit_of_work() as uow:
            uow.session.execute(
                text(
                    """
                    UPDATE p115_strm_sync_plans
                    SET status = 'retryable',
                        local_effects_started = local_effects_started OR :local_effects_started,
                        last_error_json = CAST(:error_json AS jsonb),
                        finished_at = NULL,
                        updated_at = NOW()
                    WHERE plan_id = :plan_id
                      AND status IN ('applying', 'retryable')
                    """
                ),
                {
                    "plan_id": plan_id,
                    "local_effects_started": bool(local_effects_started),
                    "error_json": canonical_json(normalized_error, field="STRM sync plan error"),
                },
            )
            uow.commit()


def _lock_and_assert_account(uow: SqlAlchemyUnitOfWork, account_id: str) -> None:
    uow.session.execute(
        text("SELECT pg_advisory_xact_lock(:namespace, :lock_key)"),
        {
            "namespace": DYNAMIC_CONFIG_LOCK_NAMESPACE,
            "lock_key": P115_AUTH_LOCK_KEY,
        },
    )
    persisted_account_id = str(
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
    if persisted_account_id != account_id:
        raise RuntimeError("115 账号在 STRM action plan 提交前发生变化，事务已回滚")


def _assert_no_unowned_cache_rows(session: Session) -> None:
    unowned = int(
        session.scalar(
            text(
                """
                SELECT count(*)
                FROM p115_filesystem_cache
                WHERE account_id IS NULL
                   OR entry_kind IS NULL
                """
            )
        )
        or 0
    )
    if unowned:
        raise RuntimeError("检测到未归属账号的 legacy 115 缓存行；为避免本地写入后数据库冲突，STRM 同步已在投影前停止")


def _plan_projection(row: Mapping[Any, Any]) -> P115StrmSyncPlan:
    raw_plan_id = row.get("plan_id")
    plan_id = raw_plan_id if isinstance(raw_plan_id, UUID) else UUID(str(raw_plan_id))
    return P115StrmSyncPlan(
        plan_id=plan_id,
        plan_key=_sha256(row.get("plan_key"), "plan_key"),
        plan=deepcopy(_validated_plan(row.get("plan_json"))),
        attempts=max(int(row.get("attempts") or 0), 0),
    )


def _validated_plan(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("STRM sync action plan must be an object")
    plan = normalize_json_object(dict(value), field="STRM sync action plan")
    if plan.get("schema_version") != 2:
        raise ValueError("STRM sync action plan schema_version is unsupported")
    library_root = _absolute_root(plan.get("library_root"))
    _sha256(plan.get("desired_cache_revision"), "desired_cache_revision")

    roots = plan.get("roots")
    managed_roots = plan.get("managed_roots")
    writes = plan.get("writes")
    deletes = plan.get("deletes")
    notifications = plan.get("notifications")
    if not isinstance(roots, list) or not roots:
        raise ValueError("STRM sync action plan roots must be a non-empty array")
    if not isinstance(managed_roots, list) or not managed_roots:
        raise ValueError("STRM sync action plan managed_roots must be a non-empty array")
    if not isinstance(writes, list) or not isinstance(deletes, list):
        raise ValueError("STRM sync action plan writes/deletes must be arrays")
    if not isinstance(notifications, dict):
        raise ValueError("STRM sync action plan notifications must be an object")

    root_ids: set[str] = set()
    root_paths: set[str] = set()
    for root in roots:
        if not isinstance(root, dict):
            raise ValueError("STRM sync action plan root is invalid")
        cid = str(root.get("cid") or "").strip()
        path = _relative_path(root.get("relative_path"), "root relative_path")
        if not cid or cid in root_ids or path.casefold() in root_paths:
            raise ValueError("STRM sync action plan roots are empty or duplicated")
        root_ids.add(cid)
        root_paths.add(path.casefold())

    normalized_managed_roots = tuple(_relative_path(path, "managed root") for path in managed_roots)
    managed_root_keys = {path.casefold() for path in normalized_managed_roots}
    if len(managed_root_keys) != len(normalized_managed_roots):
        raise ValueError("STRM sync action plan managed_roots are duplicated")
    if not root_paths.issubset(managed_root_keys):
        raise ValueError("STRM sync action plan current roots are outside managed_roots")

    write_paths: set[str] = set()
    write_ids: set[tuple[str, str]] = set()
    required_notifications: set[str] = set()
    for action in writes:
        if not isinstance(action, dict):
            raise ValueError("STRM sync action plan write is invalid")
        kind = str(action.get("kind") or "").strip().lower()
        if kind not in {"strm", "subtitle"}:
            raise ValueError("STRM sync action plan write kind is invalid")
        path = _relative_path(action.get("path"), "write path")
        if not _path_is_within_any(path, normalized_managed_roots):
            raise ValueError("STRM sync action plan write is outside managed_roots")
        absolute_path = _absolute_action_path(
            action.get("absolute_path"),
            relative_path=path,
            library_root=library_root,
        )
        remote_file_id = str(action.get("remote_file_id") or "").strip()
        pick_code = str(action.get("pick_code") or "").strip()
        if not remote_file_id or not pick_code:
            raise ValueError("STRM sync action plan write identity is incomplete")
        content_hash = action.get("content_sha256")
        if kind == "strm":
            _sha256(content_hash, "content_sha256")
        elif content_hash is not None:
            raise ValueError("subtitle action cannot claim an unknown content hash")
        expected = _validated_path_state(action.get("expected"), field="write expected")
        if expected["kind"] not in {"absent", "file"}:
            raise ValueError("STRM sync action plan write expected state is unsafe")
        write_required = action.get("write_required")
        if not isinstance(write_required, bool):
            raise ValueError("STRM sync action plan write_required must be boolean")
        if not write_required:
            if kind == "strm" and (expected["kind"] != "file" or expected["sha256"] != str(content_hash)):
                raise ValueError("non-writing STRM action does not prove desired bytes")
            if kind == "subtitle" and expected["kind"] != "file":
                raise ValueError("non-writing subtitle action does not prove an existing file")
        elif kind == "subtitle" and expected["kind"] != "absent":
            raise ValueError("writing subtitle action must freeze an absent target")
        path_key = path.casefold()
        identity = (kind, remote_file_id)
        if path_key in write_paths or identity in write_ids:
            raise ValueError("STRM sync action plan writes are duplicated")
        write_paths.add(path_key)
        write_ids.add(identity)
        if write_required:
            required_notifications.add(absolute_path)

    delete_paths: set[str] = set()
    delete_file_paths: set[str] = set()
    delete_directory_paths: set[str] = set()
    required_deleted_notifications: set[str] = set()
    for action in deletes:
        if not isinstance(action, dict):
            raise ValueError("STRM sync action plan delete is invalid")
        kind = str(action.get("kind") or "").strip().lower()
        if kind not in {"file", "directory"}:
            raise ValueError("STRM sync action plan delete kind is invalid")
        path = _relative_path(action.get("path"), "delete path")
        if not _path_is_within_any(path, normalized_managed_roots):
            raise ValueError("STRM sync action plan delete is outside managed_roots")
        if path.casefold() in managed_root_keys:
            raise ValueError("STRM sync action plan cannot delete a managed root")
        absolute_path = _absolute_action_path(
            action.get("absolute_path"),
            relative_path=path,
            library_root=library_root,
        )
        source = str(action.get("source") or "").strip().lower()
        if source not in {"reconcile", "cache_projection"}:
            raise ValueError("STRM sync action plan delete source is invalid")
        expected = _validated_path_state(action.get("expected"), field="delete expected")
        if expected["kind"] != kind:
            raise ValueError("STRM sync action plan delete identity type is inconsistent")
        path_key = path.casefold()
        if path_key in delete_paths:
            raise ValueError("STRM sync action plan deletes conflict or are duplicated")
        delete_paths.add(path_key)
        (delete_file_paths if kind == "file" else delete_directory_paths).add(path_key)
        required_deleted_notifications.add(absolute_path)

    if write_paths & delete_paths:
        raise ValueError("STRM sync action plan cannot write and delete the same path")
    for write_path in write_paths:
        if any(_path_key_is_within(write_path, directory) for directory in delete_directory_paths):
            raise ValueError("STRM sync action plan cannot delete an ancestor of a write path")

    _sha256(
        notifications.get("emby_authorization_revision"),
        "emby_authorization_revision",
    )
    modified_paths = set(_absolute_path_array(notifications.get("modified_paths"), "modified_paths"))
    deleted_paths = set(_absolute_path_array(notifications.get("deleted_paths"), "deleted_paths"))
    current_modified = set(
        _absolute_path_array(
            notifications.get("current_modified_paths"),
            "current_modified_paths",
        )
    )
    current_deleted = set(
        _absolute_path_array(
            notifications.get("current_deleted_paths"),
            "current_deleted_paths",
        )
    )
    recovered_modified = set(
        _absolute_path_array(
            notifications.get("recovered_modified_paths"),
            "recovered_modified_paths",
        )
    )
    recovered_deleted = set(
        _absolute_path_array(
            notifications.get("recovered_deleted_paths"),
            "recovered_deleted_paths",
        )
    )
    if any(not _absolute_path_is_within(path, library_root) for path in current_modified | current_deleted):
        raise ValueError("STRM sync action plan current notification is outside library_root")
    if modified_paths != current_modified | recovered_modified:
        raise ValueError("STRM sync action plan Modified notification projection is inconsistent")
    if deleted_paths != current_deleted | recovered_deleted:
        raise ValueError("STRM sync action plan Deleted notification projection is inconsistent")
    if modified_paths & deleted_paths:
        raise ValueError("STRM sync action plan cannot notify the same path as Modified and Deleted")
    if not required_notifications.issubset(current_modified):
        raise ValueError("STRM sync action plan can write a path without a persisted Modified intent")
    if not required_deleted_notifications.issubset(current_deleted):
        raise ValueError("STRM sync action plan can delete a path without a persisted Deleted intent")
    return plan


def _validated_path_state(value: object, *, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"STRM sync action plan {field} must be an object")
    expected_keys = {"kind", "size", "sha256", "device", "inode", "mtime_ns"}
    if set(value) != expected_keys:
        raise ValueError(f"STRM sync action plan {field} fields are invalid")
    kind = str(value.get("kind") or "").strip().lower()
    if kind not in {"absent", "file", "directory"}:
        raise ValueError(f"STRM sync action plan {field} kind is invalid")
    size = _non_negative_int(value.get("size"), f"{field}.size")
    numeric = {
        key: None if value.get(key) is None else _non_negative_int(value.get(key), f"{field}.{key}")
        for key in ("device", "inode", "mtime_ns")
    }
    sha256 = None if value.get("sha256") is None else _sha256(value.get("sha256"), f"{field}.sha256")
    if kind == "absent":
        if size or sha256 is not None or any(number is not None for number in numeric.values()):
            raise ValueError(f"STRM sync action plan {field} absent state carries identity")
    elif kind == "file":
        if sha256 is None or any(number is None for number in numeric.values()):
            raise ValueError(f"STRM sync action plan {field} file state is incomplete")
    elif sha256 is not None or any(number is None for number in numeric.values()):
        raise ValueError(f"STRM sync action plan {field} directory state is invalid")
    return {"kind": kind, "size": size, "sha256": sha256, **numeric}


def _non_negative_int(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"STRM sync action plan {field} must be a non-negative integer")
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"STRM sync action plan {field} must be a non-negative integer") from exc
    if parsed < 0:
        raise ValueError(f"STRM sync action plan {field} must be a non-negative integer")
    return parsed


def _absolute_root(value: object) -> str:
    path = str(value or "")
    normalized = posixpath.normpath(path)
    if not path.startswith("/") or path == "/" or path != normalized or "\\" in path or "\x00" in path:
        raise ValueError("STRM sync action plan library_root is unsafe")
    return path


def _absolute_action_path(
    value: object,
    *,
    relative_path: str,
    library_root: str,
) -> str:
    path = _absolute_path(value, "absolute action path")
    expected = posixpath.join(library_root, relative_path)
    if path != expected:
        raise ValueError("STRM sync action plan absolute path does not match its relative path")
    return path


def _absolute_path(value: object, field: str) -> str:
    path = str(value or "")
    normalized = posixpath.normpath(path)
    if not path.startswith("/") or path == "/" or path != normalized or "\\" in path or "\x00" in path:
        raise ValueError(f"STRM sync action plan {field} is unsafe")
    return path


def _absolute_path_array(value: object, field: str) -> tuple[str, ...]:
    paths = _path_array(value, field)
    return tuple(_absolute_path(path, field) for path in paths)


def _absolute_path_is_within(path: str, root: str) -> bool:
    return path.startswith(f"{root}/")


def _path_is_within_any(path: str, roots: Sequence[str]) -> bool:
    key = path.casefold()
    return any(_path_key_is_within(key, root.casefold()) for root in roots)


def _path_key_is_within(path: str, root: str) -> bool:
    return path == root or path.startswith(f"{root}/")


def _relative_path(value: object, field: str) -> str:
    path = str(value or "")
    parts = path.split("/")
    if (
        not path
        or path.startswith("/")
        or "\\" in path
        or "\x00" in path
        or any(part in {"", ".", ".."} for part in parts)
    ):
        raise ValueError(f"STRM sync action plan {field} is unsafe")
    return path


def _path_array(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError(f"STRM sync action plan {field} must be an array")
    paths = tuple(str(path or "").strip() for path in value)
    if any(not path for path in paths) or len(set(paths)) != len(paths):
        raise ValueError(f"STRM sync action plan {field} contains empty or duplicate paths")
    return paths


def _recoverable_notification_paths(
    session: Session,
    *,
    active_rows: Sequence[Mapping[Any, Any]],
    account_id: str,
    authorization_revision: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    modified: set[str] = set()
    deleted: set[str] = set()
    for row in active_rows:
        plan = _validated_plan(row.get("plan_json"))
        notifications = plan.get("notifications")
        assert isinstance(notifications, dict)
        modified.update(_absolute_path_array(notifications.get("modified_paths"), "modified_paths"))
        deleted.update(_absolute_path_array(notifications.get("deleted_paths"), "deleted_paths"))
    drift_modified, drift_deleted = _authorization_drift_recovery_paths(
        session,
        account_id=account_id,
        authorization_revision=authorization_revision,
    )
    _notification_path_roots(
        session,
        account_id=account_id,
        paths=drift_deleted,
    )
    modified.update(drift_modified)
    deleted.update(drift_deleted)
    return tuple(sorted(modified)), tuple(sorted(deleted))


def _notification_path_roots(
    session: Session,
    *,
    account_id: str,
    paths: Sequence[str],
) -> tuple[tuple[str, str], ...]:
    expected = set(paths)
    if not expected:
        return ()
    candidates: dict[str, set[str]] = {path: set() for path in expected}
    rows = (
        session.execute(
            text(
                """
                SELECT plan_json
                FROM p115_strm_sync_plans
                WHERE account_id = :account_id
                ORDER BY created_at, plan_id
                """
            ),
            {"account_id": account_id},
        )
        .mappings()
        .all()
    )
    for row in rows:
        plan = _validated_plan(row.get("plan_json"))
        notifications = plan.get("notifications")
        assert isinstance(notifications, dict)
        current_deleted = set(
            _absolute_path_array(
                notifications.get("current_deleted_paths"),
                "current_deleted_paths",
            )
        )
        root = _absolute_root(plan.get("library_root"))
        for path in expected & current_deleted:
            if not _absolute_path_is_within(path, root):
                raise RuntimeError("历史 STRM Deleted 意图超出其本地库根目录")
            candidates[path].add(root)

    resolved: list[tuple[str, str]] = []
    for path in sorted(expected):
        roots = candidates[path]
        if len(roots) != 1:
            raise RuntimeError(f"待恢复 STRM Deleted 意图缺少唯一可信的本地库根目录: {path}")
        resolved.append((path, next(iter(roots))))
    return tuple(resolved)


def _authorization_drift_recovery_paths(
    session: Session,
    *,
    account_id: str,
    authorization_revision: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    rows = (
        session.execute(
            text(
                """
                SELECT effect.id, effect.created_at, effect.status,
                       effect.error_json, effect.payload_json
                FROM media_effects AS effect
                WHERE effect.effect_type = :effect_type
                  AND effect.aggregate_type = 'p115_strm_sync'
                  AND effect.aggregate_id = :account_id
                ORDER BY effect.created_at, effect.id
                """
            ),
            {
                "effect_type": EMBY_PATHS_CHANGED_EFFECT,
                "account_id": account_id,
            },
        )
        .mappings()
        .all()
    )
    latest: dict[str, str | None] = {}
    for row in rows:
        payload = row.get("payload_json")
        if not isinstance(payload, Mapping):
            raise RuntimeError("Emby path-change recovery effect payload is invalid")
        update_type = str(payload.get("update_type") or "").strip().title()
        paths = _absolute_path_array(payload.get("paths"), "recovery effect paths")
        if update_type not in {"Modified", "Deleted"}:
            raise RuntimeError("Emby path-change recovery effect update_type is invalid")
        effect_authorization = _sha256(
            payload.get("emby_authorization_revision"),
            "recovery effect emby_authorization_revision",
        )
        status = str(row.get("status") or "").strip().lower()
        error = row.get("error_json")
        reason = str(error.get("reason") or "").strip() if isinstance(error, Mapping) else ""
        recoverable = (
            status in {"pending", "claimed", "retryable"} and effect_authorization != authorization_revision
        ) or (status == "rejected" and reason == "authorization_changed")
        for path in paths:
            if recoverable:
                latest[path] = update_type
            else:
                latest[path] = None

    modified = {path for path, update_type in latest.items() if update_type == "Modified"}
    deleted = {path for path, update_type in latest.items() if update_type == "Deleted"}
    return tuple(sorted(modified)), tuple(sorted(deleted))


def _assert_recovered_notifications_authorized(
    plan: Mapping[str, Any],
    *,
    modified_paths: Sequence[str],
    deleted_paths: Sequence[str],
) -> None:
    notifications = plan.get("notifications")
    if not isinstance(notifications, Mapping):
        raise ValueError("STRM sync action plan notifications must be an object")
    recovered_modified = set(
        _absolute_path_array(
            notifications.get("recovered_modified_paths"),
            "recovered_modified_paths",
        )
    )
    recovered_deleted = set(
        _absolute_path_array(
            notifications.get("recovered_deleted_paths"),
            "recovered_deleted_paths",
        )
    )
    if not recovered_modified.issubset(set(modified_paths)):
        raise ValueError("STRM sync action plan contains an unauthorized recovered Modified intent")
    if not recovered_deleted.issubset(set(deleted_paths)):
        raise ValueError("STRM sync action plan contains an unauthorized recovered Deleted intent")


def _plan_has_local_actions(plan: Mapping[str, Any]) -> bool:
    writes = plan.get("writes")
    deletes = plan.get("deletes")
    if not isinstance(writes, list) or not isinstance(deletes, list):
        raise ValueError("STRM sync action plan writes/deletes must be arrays")
    return bool(deletes) or any(
        isinstance(action, Mapping) and action.get("write_required") is True for action in writes
    )


def _load_entries(
    session: Session,
    root_ids: Sequence[str],
    *,
    account_id: str,
) -> tuple[P115StrmCacheEntry, ...]:
    rows = session.execute(
        text(
            """
            WITH RECURSIVE managed AS (
                SELECT c.*, c.parent_id AS inferred_root_id
                FROM p115_filesystem_cache c
                WHERE c.account_id = :account_id
                  AND (
                      c.parent_id = ANY(CAST(:root_ids AS text[]))
                      OR c.sync_root_id = ANY(CAST(:root_ids AS text[]))
                  )
                UNION
                SELECT child.*, managed.inferred_root_id
                FROM p115_filesystem_cache child
                JOIN managed ON child.parent_id = managed.id
                WHERE child.account_id = :account_id
            )
            SELECT *
            FROM managed
            """
        ),
        {"root_ids": list(root_ids), "account_id": account_id},
    ).mappings()
    entries: dict[str, P115StrmCacheEntry] = {}
    for row in rows:
        root_id = str(row.get("sync_root_id") or row.get("inferred_root_id") or "").strip()
        if root_id not in root_ids:
            continue
        entry = P115StrmCacheEntry(
            id=str(row["id"]),
            parent_id=str(row["parent_id"]),
            name=str(row["name"]),
            local_path=str(row.get("local_path") or ""),
            root_id=root_id,
            entry_kind=str(row.get("entry_kind") or ("file" if row.get("sha1") else "directory")),
            sha1=str(row.get("sha1") or "").upper() or None,
            pick_code=str(row.get("pick_code") or "").strip() or None,
            size=int(row.get("size") or 0),
            strm_content_sha256=str(row.get("strm_content_sha256") or "").lower() or None,
        )
        previous = entries.get(entry.id)
        if previous is not None and previous != entry:
            raise RuntimeError(f"STRM 缓存记录属于多个同步根目录: {entry.id}")
        entries[entry.id] = entry
    return tuple(sorted(entries.values(), key=lambda entry: (entry.root_id, entry.local_path.casefold(), entry.id)))


def _validated_entries(
    values: Sequence[P115StrmCacheEntry],
    root_ids: Sequence[str],
) -> tuple[P115StrmCacheEntry, ...]:
    entries = tuple(values)
    if len({entry.id for entry in entries}) != len(entries):
        raise ValueError("STRM 缓存快照包含重复 ID")
    if len({(entry.parent_id, entry.name) for entry in entries}) != len(entries):
        raise ValueError("STRM 缓存快照包含同目录重名项目")
    for entry in entries:
        if entry.root_id not in root_ids:
            raise ValueError("STRM 缓存项目超出目标根目录")
        if entry.entry_kind not in {"directory", "file"}:
            raise ValueError("STRM 缓存项目类型无效")
        if not all((entry.id, entry.parent_id, entry.name, entry.local_path)):
            raise ValueError("STRM 缓存项目缺少稳定身份或路径")
        if entry.entry_kind == "directory":
            if any((entry.sha1, entry.pick_code, entry.size, entry.strm_content_sha256)):
                raise ValueError("STRM 目录缓存不能携带文件身份")
            continue
        if not entry.pick_code or not entry.sha1 or entry.size <= 0:
            raise ValueError("STRM 文件缓存缺少 PickCode/SHA1/size")
        _sha256(entry.strm_content_sha256, "strm_content_sha256")
    return tuple(sorted(entries, key=lambda entry: (entry.root_id, entry.local_path.casefold(), entry.id)))


def _entry_values(entry: P115StrmCacheEntry, account_id: str) -> dict[str, Any]:
    return {
        "id": entry.id,
        "parent_id": entry.parent_id,
        "name": entry.name,
        "local_path": entry.local_path,
        "sha1": entry.sha1,
        "pick_code": entry.pick_code,
        "size": entry.size,
        "account_id": account_id,
        "entry_kind": entry.entry_kind,
        "sync_root_id": entry.root_id,
        "strm_content_sha256": entry.strm_content_sha256,
    }


def _root_ids(roots: Sequence[P115StrmSyncRoot]) -> tuple[str, ...]:
    values = tuple(str(root.cid or "").strip() for root in roots)
    if not values or any(not value for value in values) or len(set(values)) != len(values):
        raise ValueError("STRM 同步根目录为空或重复")
    return values


def _account_id(value: object) -> str:
    account = str(value or "").strip()
    if not account.isdigit():
        raise ValueError("STRM 缓存需要稳定的数字 115 账号 ID")
    return account


def _sha256(value: object, field: str) -> str:
    digest = str(value or "").strip().lower()
    if len(digest) != 64:
        raise ValueError(f"{field} must be a SHA-256 hex digest")
    try:
        bytes.fromhex(digest)
    except ValueError as exc:
        raise ValueError(f"{field} must be a SHA-256 hex digest") from exc
    return digest


def _rowcount(result: Any) -> int:
    return max(int(getattr(result, "rowcount", 0) or 0), 0)


__all__ = ["SqlAlchemyP115StrmSyncRepository"]
