from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from copy import deepcopy
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError

from etk.domains.media.effects import MEDIA_EFFECT_TOPIC, media_effect_event_key
from etk.domains.p115 import (
    EMBY_PATHS_CHANGED_EFFECT,
    P115StrmCacheEntry,
    P115StrmSyncRoot,
    SqlAlchemyP115StrmSyncRepository,
)
from etk.domains.p115.strm_sync import cache_snapshot_revision
from etk.platform.db import DatabaseRuntime
from etk.platform.jobs.json_payload import canonical_json

REPO_ROOT = Path(__file__).resolve().parents[3]
ACCOUNT_ID = "115001"
AUTHORIZATION_REVISION = "c" * 64
EMPTY_OPERATION_REVISION = "d" * 64


def _repository(runtime: DatabaseRuntime) -> SqlAlchemyP115StrmSyncRepository:
    return SqlAlchemyP115StrmSyncRepository(runtime.unit_of_work)


def _roots(*, home_video: bool = False) -> tuple[P115StrmSyncRoot, ...]:
    return (
        P115StrmSyncRoot(
            cid="root-home" if home_video else "root-movies",
            relative_path="Home" if home_video else "Movies",
            media_type="home_video" if home_video else "movie",
        ),
    )


def _entries(*, home_video: bool = False) -> tuple[P115StrmCacheEntry, ...]:
    root_id = "root-home" if home_video else "root-movies"
    root_path = "Home" if home_video else "Movies"
    directory_id = "dir-home" if home_video else "dir-feature"
    file_id = "video-home" if home_video else "video-feature"
    return (
        P115StrmCacheEntry(
            id=directory_id,
            parent_id=root_id,
            name="Feature",
            local_path=f"{root_path}/Feature",
            root_id=root_id,
            entry_kind="directory",
        ),
        P115StrmCacheEntry(
            id=file_id,
            parent_id=directory_id,
            name="Movie.mkv",
            local_path=f"{root_path}/Feature/Movie.mkv",
            root_id=root_id,
            entry_kind="file",
            sha1="A" * 40,
            pick_code="pick-feature",
            size=1024,
            strm_content_sha256="b" * 64,
        ),
    )


def _action_plan(
    roots: Sequence[P115StrmSyncRoot],
    *,
    modified_paths: Sequence[str] | None = None,
    deleted_paths: Sequence[str] = (),
    recovered_modified_paths: Sequence[str] = (),
    recovered_deleted_paths: Sequence[str] = (),
    authorization_revision: str = AUTHORIZATION_REVISION,
    write_required: bool = True,
) -> dict[str, Any]:
    root = roots[0]
    relative_path = f"{root.relative_path}/Feature/Movie.strm"
    absolute_path = f"/library/{relative_path}"
    current_modified = tuple((absolute_path,) if modified_paths is None else modified_paths)
    current_deleted = tuple(deleted_paths)
    expected = (
        {
            "kind": "absent",
            "size": 0,
            "sha256": None,
            "device": None,
            "inode": None,
            "mtime_ns": None,
        }
        if write_required
        else {
            "kind": "file",
            "size": 1024,
            "sha256": "b" * 64,
            "device": 1,
            "inode": 2,
            "mtime_ns": 3,
        }
    )
    return {
        "schema_version": 2,
        "library_root": "/library",
        "desired_cache_revision": cache_snapshot_revision(_entries(home_video=root.media_type == "home_video")),
        "roots": [{"cid": root.cid, "relative_path": root.relative_path}],
        "managed_roots": [root.relative_path],
        "writes": [
            {
                "kind": "strm",
                "path": relative_path,
                "absolute_path": absolute_path,
                "remote_file_id": "video-home" if root.media_type == "home_video" else "video-feature",
                "pick_code": "pick-feature",
                "content_sha256": "b" * 64,
                "expected": expected,
                "write_required": write_required,
            }
        ],
        "deletes": [],
        "notifications": {
            "modified_paths": sorted(set(current_modified) | set(recovered_modified_paths)),
            "deleted_paths": sorted(set(current_deleted) | set(recovered_deleted_paths)),
            "current_modified_paths": list(current_modified),
            "current_deleted_paths": list(current_deleted),
            "recovered_modified_paths": list(recovered_modified_paths),
            "recovered_deleted_paths": list(recovered_deleted_paths),
            "emby_authorization_revision": authorization_revision,
        },
    }


def _stage(
    repository: SqlAlchemyP115StrmSyncRepository,
    roots: Sequence[P115StrmSyncRoot],
    *,
    expected_revision: str,
    operation_revision: str = EMPTY_OPERATION_REVISION,
    scope: str = "full",
    plan: Mapping[str, Any] | None = None,
):
    return repository.stage_plan(
        account_id=ACCOUNT_ID,
        scope=scope,
        provider_backend="openapi",
        expected_cache_revision=expected_revision,
        operation_revision=operation_revision,
        plan=plan or _action_plan(roots),
    )


def _effect_event_key(
    *,
    operation_revision: str,
    paths: Sequence[str],
    update_type: str,
) -> str:
    payload = {
        "paths": sorted(set(paths)),
        "update_type": update_type,
        "emby_authorization_revision": AUTHORIZATION_REVISION,
        "operation_revision": operation_revision,
    }
    digest = hashlib.sha256(canonical_json(payload, field="Emby path-change payload").encode()).hexdigest()
    effect_key = f"p6:{EMBY_PATHS_CHANGED_EFFECT}:{ACCOUNT_ID}:{digest}"
    return media_effect_event_key(effect_key)


def test_strm_sync_migration_schema_is_present_on_postgres18(
    p6_runtime: DatabaseRuntime,
) -> None:
    inspector = inspect(p6_runtime.engine)
    with p6_runtime.engine.connect() as connection:
        assert str(connection.scalar(text("SHOW server_version_num"))).startswith("18")
    assert "p115_strm_sync_plans" in inspector.get_table_names()
    assert {column["name"] for column in inspector.get_columns("p115_filesystem_cache")} >= {
        "sync_root_id",
        "strm_content_sha256",
    }
    assert {column["name"] for column in inspector.get_columns("p115_strm_sync_plans")} == {
        "plan_id",
        "plan_key",
        "account_id",
        "scope",
        "provider_backend",
        "expected_cache_revision",
        "operation_revision",
        "plan_json",
        "status",
        "attempts",
        "local_effects_started",
        "last_error_json",
        "created_at",
        "updated_at",
        "finished_at",
    }
    constraint_names = {constraint["name"] for constraint in inspector.get_check_constraints("p115_strm_sync_plans")}
    assert {
        "ck_p115_strm_sync_plans_account",
        "ck_p115_strm_sync_plans_backend",
        "ck_p115_strm_sync_plans_deletes_array",
        "ck_p115_strm_sync_plans_finished",
        "ck_p115_strm_sync_plans_managed_roots_array",
        "ck_p115_strm_sync_plans_notifications_object",
        "ck_p115_strm_sync_plans_schema_version",
        "ck_p115_strm_sync_plans_library_root",
        "ck_p115_strm_sync_plans_desired_revision",
        "ck_p115_strm_sync_plans_roots_array",
        "ck_p115_strm_sync_plans_status",
        "ck_p115_strm_sync_plans_writes_array",
    } <= constraint_names
    cache_constraint_names = {
        constraint["name"] for constraint in inspector.get_check_constraints("p115_filesystem_cache")
    }
    assert {
        "ck_p115_filesystem_cache_account_id_present",
        "ck_p115_filesystem_cache_entry_kind_present",
    } <= cache_constraint_names

    legacy_plan = _action_plan(_roots())
    legacy_plan["schema_version"] = 1
    missing_schema_version = _action_plan(_roots())
    missing_schema_version.pop("schema_version")
    missing_desired_revision = _action_plan(_roots())
    missing_desired_revision.pop("desired_cache_revision")
    for invalid_plan in (legacy_plan, missing_schema_version, missing_desired_revision):
        with pytest.raises(IntegrityError):
            with p6_runtime.engine.begin() as connection:
                connection.execute(
                    text(
                        """
                        INSERT INTO p115_strm_sync_plans (
                            plan_id, plan_key, account_id, scope, provider_backend,
                            expected_cache_revision, operation_revision, plan_json
                        ) VALUES (
                            :plan_id, :plan_key, :account_id, 'full', 'openapi',
                                :revision, :revision, CAST(:plan_json AS jsonb)
                        )
                        """
                    ),
                    {
                        "plan_id": uuid4(),
                        "plan_key": "a" * 64,
                        "account_id": ACCOUNT_ID,
                        "plan_json": canonical_json(invalid_plan, field="invalid STRM plan fixture"),
                        "revision": "f" * 64,
                    },
                )


def test_account_and_root_bound_reconcile_commits_cache_plan_effect_and_outbox_atomically(
    p6_runtime: DatabaseRuntime,
) -> None:
    roots = _roots()
    desired = _entries()
    repository = _repository(p6_runtime)
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache (
                    id, parent_id, name, local_path, account_id, entry_kind,
                    sync_root_id, strm_content_sha256
                ) VALUES
                    ('other-root-file', 'root-other', 'Other.mkv', 'Other/Other.mkv',
                     '115001', 'file', 'root-other', :hash),
                    ('other-account-file', 'root-movies', 'Retired.mkv', 'Movies/Retired.mkv',
                     '115000', 'file', 'root-movies', :hash)
                """
            ),
            {"hash": "e" * 64},
        )

    before = repository.snapshot(roots, account_id=ACCOUNT_ID)
    assert before.entries == ()
    plan = _stage(
        repository,
        roots,
        expected_revision=before.revision,
        plan=_action_plan(
            roots,
            deleted_paths=("/library/Movies/Stale.strm",),
        ),
    )
    result = repository.reconcile(
        roots,
        desired,
        plan_id=plan.plan_id,
        account_id=ACCOUNT_ID,
        expected_revision=before.revision,
        operation_revision=EMPTY_OPERATION_REVISION,
    )

    assert result.cached == 2
    assert len(result.effect_ids) == 2
    with p6_runtime.engine.connect() as connection:
        cache_rows = (
            connection.execute(
                text(
                    """
                SELECT id, account_id, entry_kind, sync_root_id, strm_content_sha256
                FROM p115_filesystem_cache
                ORDER BY id
                """
                )
            )
            .mappings()
            .all()
        )
        plan_row = (
            connection.execute(
                text(
                    """
                SELECT status, local_effects_started, finished_at
                FROM p115_strm_sync_plans
                WHERE plan_id = :plan_id
                """
                ),
                {"plan_id": plan.plan_id},
            )
            .mappings()
            .one()
        )
        effect_rows = (
            connection.execute(text("SELECT effect_type, status FROM media_effects ORDER BY effect_key"))
            .mappings()
            .all()
        )
        outbox_rows = (
            connection.execute(text("SELECT topic, status FROM outbox_events ORDER BY event_key")).mappings().all()
        )

    assert [row["id"] for row in cache_rows] == [
        "dir-feature",
        "other-account-file",
        "other-root-file",
        "video-feature",
    ]
    desired_rows = [row for row in cache_rows if row["id"] in {"dir-feature", "video-feature"}]
    assert all(row["account_id"] == ACCOUNT_ID for row in desired_rows)
    assert all(row["sync_root_id"] == "root-movies" for row in desired_rows)
    assert plan_row["status"] == "confirmed"
    assert plan_row["local_effects_started"] is True
    assert plan_row["finished_at"] is not None
    assert effect_rows == [
        {"effect_type": EMBY_PATHS_CHANGED_EFFECT, "status": "pending"},
        {"effect_type": EMBY_PATHS_CHANGED_EFFECT, "status": "pending"},
    ]
    assert outbox_rows == [
        {"topic": MEDIA_EFFECT_TOPIC, "status": "pending"},
        {"topic": MEDIA_EFFECT_TOPIC, "status": "pending"},
    ]


def test_expected_cache_revision_cas_rolls_back_without_pruning_other_scope(
    p6_runtime: DatabaseRuntime,
) -> None:
    roots = _roots(home_video=True)
    repository = _repository(p6_runtime)
    before = repository.snapshot(roots, account_id=ACCOUNT_ID)
    plan = _stage(
        repository,
        roots,
        expected_revision=before.revision,
        operation_revision="1" * 64,
        scope="home_video",
        plan=_action_plan(roots),
    )
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache (
                    id, parent_id, name, local_path, account_id, entry_kind, sync_root_id
                ) VALUES (
                    'concurrent-home', 'root-home', 'Concurrent', 'Home/Concurrent',
                    '115001', 'directory', 'root-home'
                )
                """
            )
        )

    with pytest.raises(RuntimeError, match="缓存.*发生变化"):
        repository.reconcile(
            roots,
            _entries(home_video=True),
            plan_id=plan.plan_id,
            account_id=ACCOUNT_ID,
            expected_revision=before.revision,
            operation_revision="1" * 64,
        )

    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM p115_filesystem_cache")) == 1
        assert connection.scalar(text("SELECT status FROM p115_strm_sync_plans")) == "applying"
        assert connection.scalar(text("SELECT count(*) FROM media_effects")) == 0


def test_outbox_conflict_rolls_back_cache_effect_and_plan_confirmation(
    p6_runtime: DatabaseRuntime,
) -> None:
    roots = _roots()
    repository = _repository(p6_runtime)
    before = repository.snapshot(roots, account_id=ACCOUNT_ID)
    modified = ("/library/Movies/Feature/Movie.strm",)
    operation_revision = "2" * 64
    plan = _stage(
        repository,
        roots,
        expected_revision=before.revision,
        operation_revision=operation_revision,
        plan=_action_plan(roots, modified_paths=modified),
    )
    event_key = _effect_event_key(
        operation_revision=operation_revision,
        paths=modified,
        update_type="Modified",
    )
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO outbox_events (
                    id, event_key, topic, aggregate_type, aggregate_id, payload_json
                ) VALUES (
                    :id, :event_key, :topic, 'fixture', 'conflict', '{}'::jsonb
                )
                """
            ),
            {"id": uuid4(), "event_key": event_key, "topic": MEDIA_EFFECT_TOPIC},
        )

    with pytest.raises(IntegrityError):
        repository.reconcile(
            roots,
            _entries(),
            plan_id=plan.plan_id,
            account_id=ACCOUNT_ID,
            expected_revision=before.revision,
            operation_revision=operation_revision,
        )

    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM p115_filesystem_cache")) == 0
        assert connection.scalar(text("SELECT count(*) FROM media_effects")) == 0
        assert connection.scalar(text("SELECT count(*) FROM outbox_events")) == 1
        assert (
            connection.scalar(
                text("SELECT status FROM p115_strm_sync_plans WHERE plan_id = :plan_id"),
                {"plan_id": plan.plan_id},
            )
            == "applying"
        )


def test_retry_reuses_original_plan_notifications_and_replay_reuses_effect_intent(
    p6_runtime: DatabaseRuntime,
) -> None:
    roots = _roots()
    desired = _entries()
    repository = _repository(p6_runtime)
    before = repository.snapshot(roots, account_id=ACCOUNT_ID)
    original_notifications = ("/library/Movies/Feature/Movie.strm",)
    first = _stage(
        repository,
        roots,
        expected_revision=before.revision,
        operation_revision="3" * 64,
        plan=_action_plan(roots, modified_paths=original_notifications),
    )
    repository.mark_plan_retryable(
        first.plan_id,
        local_effects_started=True,
        error={"message": "fixture finalize failure", "api_key": "must-be-redacted"},
    )
    with p6_runtime.engine.connect() as connection:
        retryable_row = (
            connection.execute(
                text(
                    """
                SELECT status, local_effects_started, last_error_json
                FROM p115_strm_sync_plans
                WHERE plan_id = :plan_id
                """
                ),
                {"plan_id": first.plan_id},
            )
            .mappings()
            .one()
        )
    assert retryable_row["status"] == "retryable"
    assert retryable_row["local_effects_started"] is True
    assert retryable_row["last_error_json"] == {
        "message": "fixture finalize failure",
        "api_key": "<redacted>",
    }
    retried = _stage(
        repository,
        roots,
        expected_revision=before.revision,
        operation_revision="3" * 64,
        plan=_action_plan(
            roots,
            modified_paths=original_notifications,
        ),
    )
    assert retried.plan_id == first.plan_id
    assert retried.attempts == 2
    assert retried.plan["notifications"]["modified_paths"] == list(original_notifications)
    repository.reconcile(
        roots,
        desired,
        plan_id=retried.plan_id,
        account_id=ACCOUNT_ID,
        expected_revision=before.revision,
        operation_revision="3" * 64,
    )

    after = repository.snapshot(roots, account_id=ACCOUNT_ID)
    replay = _stage(
        repository,
        roots,
        expected_revision=after.revision,
        operation_revision="3" * 64,
        plan=_action_plan(roots, modified_paths=original_notifications),
    )
    repository.reconcile(
        roots,
        desired,
        plan_id=replay.plan_id,
        account_id=ACCOUNT_ID,
        expected_revision=after.revision,
        operation_revision="3" * 64,
    )

    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM p115_strm_sync_plans")) == 2
        assert connection.scalar(text("SELECT count(*) FROM media_effects")) == 1
        assert connection.scalar(text("SELECT count(*) FROM outbox_events")) == 1
        error_json = connection.scalar(
            text(
                """
                SELECT last_error_json
                FROM p115_strm_sync_plans
                WHERE plan_id = :plan_id
                """
            ),
            {"plan_id": first.plan_id},
        )
    assert error_json is None


def test_changed_local_precondition_or_emby_authorization_versions_the_plan_without_losing_intent(
    p6_runtime: DatabaseRuntime,
) -> None:
    roots = _roots()
    desired = _entries()
    repository = _repository(p6_runtime)
    before = repository.snapshot(roots, account_id=ACCOUNT_ID)
    original_path = "/library/Movies/Feature/Movie.strm"
    first = _stage(
        repository,
        roots,
        expected_revision=before.revision,
        operation_revision="4" * 64,
        plan=_action_plan(roots, modified_paths=(original_path,)),
    )
    repository.mark_plan_retryable(
        first.plan_id,
        local_effects_started=True,
        error={"message": "fixture interruption after local write"},
    )

    current_authorization = "e" * 64
    rebased = _stage(
        repository,
        roots,
        expected_revision=before.revision,
        operation_revision="4" * 64,
        plan=_action_plan(
            roots,
            modified_paths=(),
            recovered_modified_paths=(original_path,),
            authorization_revision=current_authorization,
            write_required=False,
        ),
    )

    assert rebased.plan_id != first.plan_id
    assert rebased.plan["notifications"] == {
        "modified_paths": [original_path],
        "deleted_paths": [],
        "current_modified_paths": [],
        "current_deleted_paths": [],
        "recovered_modified_paths": [original_path],
        "recovered_deleted_paths": [],
        "emby_authorization_revision": current_authorization,
    }
    repository.reconcile(
        roots,
        desired,
        plan_id=rebased.plan_id,
        account_id=ACCOUNT_ID,
        expected_revision=before.revision,
        operation_revision="4" * 64,
    )

    with p6_runtime.engine.connect() as connection:
        statuses = {
            row["plan_id"]: str(row["status"])
            for row in connection.execute(text("SELECT plan_id, status FROM p115_strm_sync_plans")).mappings().all()
        }
        authorization = connection.scalar(
            text(
                """
                SELECT payload_json ->> 'emby_authorization_revision'
                FROM media_effects
                """
            )
        )
    assert statuses[first.plan_id] == "superseded"
    assert statuses[rebased.plan_id] == "confirmed"
    assert authorization == current_authorization


def test_confirmed_plan_authorization_drift_is_recovered_once_with_current_authorization(
    p6_runtime: DatabaseRuntime,
) -> None:
    roots = _roots()
    desired = _entries()
    repository = _repository(p6_runtime)
    before = repository.snapshot(roots, account_id=ACCOUNT_ID)
    path = "/library/Movies/Feature/Movie.strm"
    first = _stage(
        repository,
        roots,
        expected_revision=before.revision,
        operation_revision="5" * 64,
        plan=_action_plan(roots, modified_paths=(path,)),
    )
    repository.reconcile(
        roots,
        desired,
        plan_id=first.plan_id,
        account_id=ACCOUNT_ID,
        expected_revision=before.revision,
        operation_revision="5" * 64,
    )
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE media_effects
                SET status = 'rejected',
                    attempts = 1,
                    last_attempt_at = NOW(),
                    error_json = '{"reason":"authorization_changed"}'::jsonb,
                    finished_at = NOW(),
                    updated_at = NOW()
                WHERE aggregate_type = 'p115_strm_sync'
                  AND aggregate_id = :account_id
                """
            ),
            {"account_id": ACCOUNT_ID},
        )

    current_authorization = "e" * 64
    recovery = repository.recovery_snapshot(
        account_id=ACCOUNT_ID,
        authorization_revision=current_authorization,
    )
    assert recovery.modified_paths == (path,)
    assert recovery.deleted_paths == ()
    after = repository.snapshot(roots, account_id=ACCOUNT_ID)
    successor = _stage(
        repository,
        roots,
        expected_revision=after.revision,
        operation_revision="6" * 64,
        plan=_action_plan(
            roots,
            modified_paths=(),
            recovered_modified_paths=(path,),
            authorization_revision=current_authorization,
            write_required=False,
        ),
    )
    repository.reconcile(
        roots,
        desired,
        plan_id=successor.plan_id,
        account_id=ACCOUNT_ID,
        expected_revision=after.revision,
        operation_revision="6" * 64,
    )

    recovered_again = repository.recovery_snapshot(
        account_id=ACCOUNT_ID,
        authorization_revision=current_authorization,
    )
    assert recovered_again.modified_paths == ()
    with p6_runtime.engine.connect() as connection:
        revisions = connection.scalars(
            text(
                """
                SELECT payload_json ->> 'emby_authorization_revision'
                FROM media_effects
                ORDER BY created_at
                """
            )
        ).all()
    assert revisions == [AUTHORIZATION_REVISION, current_authorization]


def test_recovery_source_plan_remains_active_until_its_successor_confirms(
    p6_runtime: DatabaseRuntime,
) -> None:
    roots = _roots()
    desired = _entries()
    repository = _repository(p6_runtime)
    before = repository.snapshot(roots, account_id=ACCOUNT_ID)
    path = "/library/Movies/Feature/Movie.strm"
    first = _stage(
        repository,
        roots,
        expected_revision=before.revision,
        operation_revision="8" * 64,
        plan=_action_plan(roots, modified_paths=(path,)),
    )
    repository.mark_plan_retryable(
        first.plan_id,
        local_effects_started=True,
        error={"message": "fixture first local effect applied"},
    )
    successor_plan = _action_plan(
        roots,
        modified_paths=(),
        recovered_modified_paths=(path,),
        write_required=False,
    )
    successor = _stage(
        repository,
        roots,
        expected_revision=before.revision,
        operation_revision="8" * 64,
        plan=successor_plan,
    )
    repository.mark_plan_retryable(
        successor.plan_id,
        local_effects_started=False,
        error={"message": "fixture successor failed before reconcile"},
    )

    recovery = repository.recovery_snapshot(
        account_id=ACCOUNT_ID,
        authorization_revision=AUTHORIZATION_REVISION,
    )
    assert {plan.plan_id for plan in recovery.active_plans} == {first.plan_id, successor.plan_id}
    retried = _stage(
        repository,
        roots,
        expected_revision=before.revision,
        operation_revision="8" * 64,
        plan=successor_plan,
    )
    assert retried.plan_id == successor.plan_id
    assert retried.attempts == 2
    repository.reconcile(
        roots,
        desired,
        plan_id=retried.plan_id,
        account_id=ACCOUNT_ID,
        expected_revision=before.revision,
        operation_revision="8" * 64,
    )

    with p6_runtime.engine.connect() as connection:
        statuses = {
            row["plan_id"]: str(row["status"])
            for row in connection.execute(text("SELECT plan_id, status FROM p115_strm_sync_plans")).mappings().all()
        }
    assert statuses[first.plan_id] == "superseded"
    assert statuses[successor.plan_id] == "confirmed"


def test_authorization_drift_recovery_uses_the_latest_semantics_per_path(
    p6_runtime: DatabaseRuntime,
) -> None:
    roots = _roots()
    desired = _entries()
    repository = _repository(p6_runtime)
    before = repository.snapshot(roots, account_id=ACCOUNT_ID)
    path = "/library/Movies/Feature/Movie.strm"
    modified = _stage(
        repository,
        roots,
        expected_revision=before.revision,
        operation_revision="9" * 64,
        plan=_action_plan(roots, modified_paths=(path,)),
    )
    repository.reconcile(
        roots,
        desired,
        plan_id=modified.plan_id,
        account_id=ACCOUNT_ID,
        expected_revision=before.revision,
        operation_revision="9" * 64,
    )
    after = repository.snapshot(roots, account_id=ACCOUNT_ID)
    deleted_authorization = "d" * 64
    deleted = _stage(
        repository,
        roots,
        expected_revision=after.revision,
        operation_revision="a" * 64,
        plan=_action_plan(
            roots,
            modified_paths=(),
            deleted_paths=(path,),
            authorization_revision=deleted_authorization,
            write_required=False,
        ),
    )
    repository.reconcile(
        roots,
        desired,
        plan_id=deleted.plan_id,
        account_id=ACCOUNT_ID,
        expected_revision=after.revision,
        operation_revision="a" * 64,
    )
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE media_effects
                SET status = 'rejected', attempts = 1, last_attempt_at = NOW(),
                    error_json = '{"reason":"authorization_changed"}'::jsonb,
                    finished_at = NOW(), updated_at = NOW()
                WHERE aggregate_type = 'p115_strm_sync'
                  AND aggregate_id = :account_id
                """
            ),
            {"account_id": ACCOUNT_ID},
        )

    recovery = repository.recovery_snapshot(
        account_id=ACCOUNT_ID,
        authorization_revision="e" * 64,
    )

    assert recovery.modified_paths == ()
    assert recovery.deleted_paths == (path,)
    assert recovery.deleted_path_roots == ((path, "/library"),)

    current_authorization = "e" * 64
    current = repository.snapshot(roots, account_id=ACCOUNT_ID)
    successor = _stage(
        repository,
        roots,
        expected_revision=current.revision,
        operation_revision="b" * 64,
        plan=_action_plan(
            roots,
            modified_paths=(),
            recovered_deleted_paths=(path,),
            authorization_revision=current_authorization,
            write_required=False,
        ),
    )
    repository.reconcile(
        roots,
        desired,
        plan_id=successor.plan_id,
        account_id=ACCOUNT_ID,
        expected_revision=current.revision,
        operation_revision="b" * 64,
    )

    recovered_again = repository.recovery_snapshot(
        account_id=ACCOUNT_ID,
        authorization_revision=current_authorization,
    )
    assert recovered_again.modified_paths == ()
    assert recovered_again.deleted_paths == ()
    assert recovered_again.deleted_path_roots == ()


def test_authorization_drift_deleted_recovery_fails_without_plan_root_provenance(
    p6_runtime: DatabaseRuntime,
) -> None:
    roots = _roots()
    desired = _entries()
    repository = _repository(p6_runtime)
    before = repository.snapshot(roots, account_id=ACCOUNT_ID)
    path = "/library/Movies/Feature/Movie.strm"
    plan = _stage(
        repository,
        roots,
        expected_revision=before.revision,
        operation_revision="f" * 64,
        plan=_action_plan(
            roots,
            modified_paths=(),
            deleted_paths=(path,),
            write_required=False,
        ),
    )
    repository.reconcile(
        roots,
        desired,
        plan_id=plan.plan_id,
        account_id=ACCOUNT_ID,
        expected_revision=before.revision,
        operation_revision="f" * 64,
    )
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM p115_strm_sync_plans WHERE plan_id = :plan_id"),
            {"plan_id": plan.plan_id},
        )

    with pytest.raises(RuntimeError, match="缺少唯一可信的本地库根目录"):
        repository.recovery_snapshot(
            account_id=ACCOUNT_ID,
            authorization_revision="e" * 64,
        )


@pytest.mark.parametrize(
    ("old_update_type", "current_update_type"),
    (("Modified", "Deleted"), ("Deleted", "Modified")),
)
def test_current_authorization_effect_supersedes_opposite_old_authorization_semantics(
    p6_runtime: DatabaseRuntime,
    old_update_type: str,
    current_update_type: str,
) -> None:
    roots = _roots()
    desired = _entries()
    repository = _repository(p6_runtime)
    before = repository.snapshot(roots, account_id=ACCOUNT_ID)
    path = "/library/Movies/Feature/Movie.strm"
    old = _stage(
        repository,
        roots,
        expected_revision=before.revision,
        operation_revision="1" * 64,
        plan=_action_plan(
            roots,
            modified_paths=(path,) if old_update_type == "Modified" else (),
            deleted_paths=(path,) if old_update_type == "Deleted" else (),
            write_required=old_update_type == "Modified",
        ),
    )
    repository.reconcile(
        roots,
        desired,
        plan_id=old.plan_id,
        account_id=ACCOUNT_ID,
        expected_revision=before.revision,
        operation_revision="1" * 64,
    )
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE media_effects
                SET status = 'rejected', attempts = 1, last_attempt_at = NOW(),
                    error_json = '{"reason":"authorization_changed"}'::jsonb,
                    finished_at = NOW(), updated_at = NOW()
                WHERE aggregate_type = 'p115_strm_sync'
                  AND aggregate_id = :account_id
                """
            ),
            {"account_id": ACCOUNT_ID},
        )

    current_authorization = "e" * 64
    current = repository.snapshot(roots, account_id=ACCOUNT_ID)
    successor = _stage(
        repository,
        roots,
        expected_revision=current.revision,
        operation_revision="2" * 64,
        plan=_action_plan(
            roots,
            modified_paths=(path,) if current_update_type == "Modified" else (),
            deleted_paths=(path,) if current_update_type == "Deleted" else (),
            authorization_revision=current_authorization,
            write_required=current_update_type == "Modified",
        ),
    )
    repository.reconcile(
        roots,
        desired,
        plan_id=successor.plan_id,
        account_id=ACCOUNT_ID,
        expected_revision=current.revision,
        operation_revision="2" * 64,
    )

    recovery = repository.recovery_snapshot(
        account_id=ACCOUNT_ID,
        authorization_revision=current_authorization,
    )
    assert recovery.modified_paths == ()
    assert recovery.deleted_paths == ()
    assert recovery.deleted_path_roots == ()


def test_authorization_cycle_creates_a_new_recovery_effect_instead_of_reusing_terminal_intent(
    p6_runtime: DatabaseRuntime,
) -> None:
    roots = _roots()
    desired = _entries()
    repository = _repository(p6_runtime)
    before = repository.snapshot(roots, account_id=ACCOUNT_ID)
    path = "/library/Movies/Feature/Movie.strm"
    operation_revision = "3" * 64
    original = _stage(
        repository,
        roots,
        expected_revision=before.revision,
        operation_revision=operation_revision,
        plan=_action_plan(roots, modified_paths=(path,)),
    )
    repository.reconcile(
        roots,
        desired,
        plan_id=original.plan_id,
        account_id=ACCOUNT_ID,
        expected_revision=before.revision,
        operation_revision=operation_revision,
    )
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE media_effects
                SET status = 'rejected', attempts = 1, last_attempt_at = NOW(),
                    error_json = '{"reason":"authorization_changed"}'::jsonb,
                    finished_at = NOW(), updated_at = NOW()
                WHERE aggregate_type = 'p115_strm_sync'
                  AND aggregate_id = :account_id
                """
            ),
            {"account_id": ACCOUNT_ID},
        )

    recovery = repository.recovery_snapshot(
        account_id=ACCOUNT_ID,
        authorization_revision=AUTHORIZATION_REVISION,
    )
    assert recovery.modified_paths == (path,)

    current = repository.snapshot(roots, account_id=ACCOUNT_ID)
    successor = _stage(
        repository,
        roots,
        expected_revision=current.revision,
        operation_revision=operation_revision,
        plan=_action_plan(
            roots,
            modified_paths=(path,),
        ),
    )
    repository.reconcile(
        roots,
        desired,
        plan_id=successor.plan_id,
        account_id=ACCOUNT_ID,
        expected_revision=current.revision,
        operation_revision=operation_revision,
    )

    with p6_runtime.engine.connect() as connection:
        effects = (
            connection.execute(
                text(
                    """
                SELECT effect_key, status, payload_json ->> 'recovery_revision' AS recovery_revision
                FROM media_effects
                ORDER BY created_at, id
                """
                )
            )
            .mappings()
            .all()
        )
    assert len(effects) == 2
    assert effects[0]["status"] == "rejected"
    assert effects[0]["recovery_revision"] is None
    assert effects[1]["status"] == "pending"
    assert effects[1]["recovery_revision"] == successor.plan_key
    assert effects[0]["effect_key"] != effects[1]["effect_key"]

    recovered_again = repository.recovery_snapshot(
        account_id=ACCOUNT_ID,
        authorization_revision=AUTHORIZATION_REVISION,
    )
    assert recovered_again.modified_paths == ()
    assert recovered_again.deleted_paths == ()


def test_reconcile_binds_roots_and_desired_cache_projection_to_the_staged_plan(
    p6_runtime: DatabaseRuntime,
) -> None:
    roots = _roots()
    repository = _repository(p6_runtime)
    before = repository.snapshot(roots, account_id=ACCOUNT_ID)
    plan = _stage(repository, roots, expected_revision=before.revision, operation_revision="7" * 64)

    with pytest.raises(RuntimeError, match="同步根目录.*不匹配"):
        repository.reconcile(
            _roots(home_video=True),
            _entries(home_video=True),
            plan_id=plan.plan_id,
            account_id=ACCOUNT_ID,
            expected_revision=before.revision,
            operation_revision="7" * 64,
        )
    with pytest.raises(RuntimeError, match="目标缓存投影.*不匹配"):
        repository.reconcile(
            roots,
            (),
            plan_id=plan.plan_id,
            account_id=ACCOUNT_ID,
            expected_revision=before.revision,
            operation_revision="7" * 64,
        )

    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM p115_filesystem_cache")) == 0
        assert connection.scalar(text("SELECT count(*) FROM media_effects")) == 0
        assert connection.scalar(text("SELECT status FROM p115_strm_sync_plans")) == "applying"


def test_unowned_legacy_cache_rows_fail_before_a_strm_plan_can_be_staged(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    roots = _roots()
    before = repository.snapshot(roots, account_id=ACCOUNT_ID)
    plan = _stage(repository, roots, expected_revision=before.revision)
    insert_legacy = text(
        """
        INSERT INTO p115_filesystem_cache (id, parent_id, name, local_path)
        VALUES ('legacy-null-owner', 'root-movies', 'Legacy.mkv', 'Movies/Legacy.mkv')
        """
    )
    with pytest.raises(IntegrityError):
        with p6_runtime.engine.begin() as connection:
            connection.execute(insert_legacy)

    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                ALTER TABLE p115_filesystem_cache
                    DROP CONSTRAINT ck_p115_filesystem_cache_account_id_present,
                    DROP CONSTRAINT ck_p115_filesystem_cache_entry_kind_present
                """
            )
        )
        connection.execute(insert_legacy)
    try:
        with pytest.raises(RuntimeError, match="legacy 115 缓存行"):
            repository.snapshot(roots, account_id=ACCOUNT_ID)
        with pytest.raises(RuntimeError, match="legacy 115 缓存行"):
            repository.recovery_snapshot(
                account_id=ACCOUNT_ID,
                authorization_revision=AUTHORIZATION_REVISION,
            )
        with pytest.raises(RuntimeError, match="legacy 115 缓存行"):
            _stage(repository, roots, expected_revision=before.revision)
        with pytest.raises(RuntimeError, match="legacy 115 缓存行"):
            repository.reconcile(
                roots,
                _entries(),
                plan_id=plan.plan_id,
                account_id=ACCOUNT_ID,
                expected_revision=before.revision,
                operation_revision=EMPTY_OPERATION_REVISION,
            )
    finally:
        with p6_runtime.engine.begin() as connection:
            connection.execute(text("DELETE FROM p115_filesystem_cache WHERE id = 'legacy-null-owner'"))
            connection.execute(
                text(
                    """
                    ALTER TABLE p115_filesystem_cache
                        ADD CONSTRAINT ck_p115_filesystem_cache_account_id_present
                            CHECK (account_id IS NOT NULL) NOT VALID,
                        ADD CONSTRAINT ck_p115_filesystem_cache_entry_kind_present
                            CHECK (entry_kind IS NOT NULL) NOT VALID
                    """
                )
            )

    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM p115_strm_sync_plans")) == 1
        assert connection.scalar(text("SELECT status FROM p115_strm_sync_plans")) == "applying"
        assert connection.scalar(text("SELECT count(*) FROM media_effects")) == 0
        assert connection.scalar(text("SELECT count(*) FROM outbox_events")) == 0


def test_plan_validator_rejects_write_delete_and_root_containment_conflicts(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    roots = _roots()
    before = repository.snapshot(roots, account_id=ACCOUNT_ID)
    expected_file = {
        "kind": "file",
        "size": 7,
        "sha256": "f" * 64,
        "device": 1,
        "inode": 2,
        "mtime_ns": 3,
    }
    expected_directory = {
        "kind": "directory",
        "size": 64,
        "sha256": None,
        "device": 1,
        "inode": 4,
        "mtime_ns": 5,
    }

    same_path = _action_plan(roots)
    same_path["deletes"] = [
        {
            "kind": "file",
            "path": "Movies/Feature/Movie.strm",
            "absolute_path": "/library/Movies/Feature/Movie.strm",
            "source": "reconcile",
            "expected": expected_file,
        }
    ]
    same_path["notifications"]["deleted_paths"] = ["/library/Movies/Feature/Movie.strm"]
    same_path["notifications"]["current_deleted_paths"] = ["/library/Movies/Feature/Movie.strm"]
    with pytest.raises(ValueError, match="write and delete the same path"):
        _stage(repository, roots, expected_revision=before.revision, plan=same_path)

    ancestor = _action_plan(roots)
    ancestor["deletes"] = [
        {
            "kind": "directory",
            "path": "Movies/Feature",
            "absolute_path": "/library/Movies/Feature",
            "source": "reconcile",
            "expected": expected_directory,
        }
    ]
    ancestor["notifications"]["deleted_paths"] = ["/library/Movies/Feature"]
    ancestor["notifications"]["current_deleted_paths"] = ["/library/Movies/Feature"]
    with pytest.raises(ValueError, match="ancestor of a write path"):
        _stage(repository, roots, expected_revision=before.revision, plan=ancestor)

    outside = deepcopy(_action_plan(roots))
    outside["deletes"] = [
        {
            "kind": "file",
            "path": "Other/Stale.strm",
            "absolute_path": "/library/Other/Stale.strm",
            "source": "reconcile",
            "expected": expected_file,
        }
    ]
    outside["notifications"]["deleted_paths"] = ["/library/Other/Stale.strm"]
    outside["notifications"]["current_deleted_paths"] = ["/library/Other/Stale.strm"]
    with pytest.raises(ValueError, match="outside managed_roots"):
        _stage(repository, roots, expected_revision=before.revision, plan=outside)

    wrong_absolute = deepcopy(_action_plan(roots))
    wrong_absolute["writes"][0]["absolute_path"] = "/other/library/Movies/Feature/Movie.strm"
    with pytest.raises(ValueError, match="absolute path does not match"):
        _stage(repository, roots, expected_revision=before.revision, plan=wrong_absolute)

    outside_notification = deepcopy(_action_plan(roots))
    outside_notification["notifications"]["modified_paths"].append("/outside/Extra.strm")
    outside_notification["notifications"]["current_modified_paths"].append("/outside/Extra.strm")
    with pytest.raises(ValueError, match="notification is outside library_root"):
        _stage(repository, roots, expected_revision=before.revision, plan=outside_notification)

    unauthorized_recovery = _action_plan(
        roots,
        modified_paths=(),
        recovered_modified_paths=("/old-library/Recovered.strm",),
        write_required=False,
    )
    with pytest.raises(ValueError, match="unauthorized recovered Modified"):
        _stage(repository, roots, expected_revision=before.revision, plan=unauthorized_recovery)


def test_strm_sync_revision_downgrades_to_0006_and_reapplies(
    p6_runtime: DatabaseRuntime,
) -> None:
    config = Config(str(REPO_ROOT / "alembic.ini"))
    expected_head = ScriptDirectory.from_config(config).get_current_head()
    assert expected_head is not None
    try:
        command.downgrade(config, "0006_p6_organize_execution")
        inspector = inspect(p6_runtime.engine)
        assert "p115_strm_sync_plans" not in inspector.get_table_names()
        assert {column["name"] for column in inspector.get_columns("p115_filesystem_cache")}.isdisjoint(
            {"sync_root_id", "strm_content_sha256"}
        )
    finally:
        command.upgrade(config, "head")
    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM etk_migrations.alembic_version")) == expected_head
