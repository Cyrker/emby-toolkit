from __future__ import annotations

import builtins
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import httpx
import pytest
from p115pickcode import to_pickcode

from etk.domains.p115 import (
    HttpxP115DirectoryTreeGateway,
    HttpxP115FingerprintRemoteGateway,
    LocalP115FingerprintHintGateway,
    P115CredentialSnapshot,
    P115DirectoryCacheEntry,
    P115DirectoryReconcileResult,
    P115DirectoryRoot,
    P115DirectorySyncService,
    P115FileFingerprint,
    P115FingerprintRepairRow,
    P115FingerprintRepairService,
    P115OrganizeRecordTmdbBackfillService,
)
from etk.platform.jobs import JobStatus
from etk.platform.tasks import TaskMessage, TaskOutcome, TaskRegistry
from etk.processes import worker as worker_module
from etk.processes.p6_p115_tasks import P6P115TaskRouter
from etk.processes.runtime_config import TaskRuntimeConfig
from etk.processes.worker import WorkerRuntime

REPO_ROOT = Path(__file__).resolve().parents[3]
TASK_FIXTURE = REPO_ROOT / "verification/fixtures/tasks/ordered-task-catalog.json"


class ExecutionStore:
    def __init__(self, projection: Any) -> None:
        self.projection = projection
        self.terminals: list[tuple[str, dict[str, Any]]] = []

    def start_attempt(self, **_values: Any) -> Any:
        return self.projection

    def get_job(self, _job_id: UUID) -> Any:
        return self.projection

    def heartbeat(self, **_values: Any) -> Any:
        return SimpleNamespace(cancel_requested=False)

    def succeed_attempt(self, **values: Any) -> Any:
        self.terminals.append(("succeeded", values))
        return self.projection

    def fail_attempt(self, **values: Any) -> Any:
        self.terminals.append(("failed", values))
        return self.projection

    def timeout_attempt(self, **values: Any) -> Any:
        self.terminals.append(("timeout", values))
        return self.projection

    def cancel_attempt(self, **values: Any) -> Any:
        self.terminals.append(("cancelled", values))
        return self.projection


class FailingLegacyAdapter:
    def execute(self, _message: object) -> object:
        raise AssertionError("P6 native task must not execute through LegacyTaskAdapter")


class FakeP6Router:
    def __init__(self, outcome: TaskOutcome | None = None) -> None:
        self.outcome = outcome or TaskOutcome.succeeded("directory sync complete", {"cached": 2})
        self.calls: list[tuple[TaskMessage, object]] = []

    def execute(self, message: TaskMessage, progress: object) -> TaskOutcome:
        self.calls.append((message, progress))
        return self.outcome


class FakeOrganizeRecordRepository:
    def __init__(self) -> None:
        self.calls = 0

    def backfill_missing_tmdb_ids(self) -> int:
        self.calls += 1
        return 2


class NativeDirectorySettings:
    def load(self, key: str) -> object:
        if key == "p115_sorting_rules_account_id":
            return "115001"
        if key == "p115_sorting_rules":
            return [{"cid": "root-a", "category_path": "Movies"}]
        raise AssertionError(f"unexpected directory setting {key}")


class NativeDirectoryCredentials:
    def load(self) -> P115CredentialSnapshot:
        return P115CredentialSnapshot(
            access_token="fixture-openapi-token",
            account_id="115001",
        )

    def save_openapi(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("directory sync must not save credentials")

    def save_cookie(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("directory sync must not save credentials")

    def app_label(self, app_type: str) -> str:
        return app_type


class NativeDirectoryRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[P115DirectoryRoot, tuple[P115DirectoryCacheEntry, ...], str]] = []

    def reconcile_children(
        self,
        root: P115DirectoryRoot,
        entries: Sequence[P115DirectoryCacheEntry],
        *,
        account_id: str,
    ) -> P115DirectoryReconcileResult:
        frozen = tuple(entries)
        self.calls.append((root, frozen, account_id))
        return P115DirectoryReconcileResult(cached=len(frozen), cleaned=1)


class NativeFingerprintRepository:
    def __init__(self, row: P115FingerprintRepairRow) -> None:
        self.row = row
        self.commits: list[tuple[int, P115FileFingerprint, str]] = []

    def list_repair_rows(self) -> tuple[P115FingerprintRepairRow, ...]:
        return (self.row,)

    def find_fingerprint(
        self,
        *,
        account_id: str,
        pick_code: str = "",
        sha1: str = "",
        file_id: str = "",
        local_paths: Sequence[str] = (),
    ) -> P115FileFingerprint | None:
        del account_id, pick_code, sha1, file_id, local_paths
        return None

    def commit_repair(
        self,
        row: P115FingerprintRepairRow,
        asset_index: int,
        fingerprint: P115FileFingerprint,
        *,
        account_id: str,
    ) -> bool:
        assert row == self.row
        self.commits.append((asset_index, fingerprint, account_id))
        return True


def _projection(task_key: str = "sync-115-directory-tree") -> Any:
    return SimpleNamespace(
        job=SimpleNamespace(
            id=UUID("00000000-0000-0000-0000-000000000601"),
            task_key=task_key,
            request_payload={
                "payload_version": 1,
                "parameters": {},
                "processor_type": "watchlist" if task_key == "intro-fingerprint-backfill" else "media",
                "display_name": "同步网盘目录",
                "silent": True,
                "call_style": "catalog",
            },
            timeout_seconds=None,
            max_attempts=1,
            parent_job_id=None,
            status=JobStatus.RUNNING,
        ),
        current_attempt=SimpleNamespace(
            id=UUID("00000000-0000-0000-0000-000000000602"),
            attempt_no=1,
        ),
    )


@pytest.mark.parametrize(
    ("task_key", "native_operation"),
    [
        ("populate-metadata", "p115.quick-deploy"),
        ("native-operation:p115.quick-deploy", None),
    ],
    ids=["payload-task-mismatch", "missing-native-payload"],
)
def test_worker_rejects_native_identity_mismatch_before_legacy_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    task_key: str,
    native_operation: str | None,
) -> None:
    projection = _projection(task_key)
    if native_operation is not None:
        projection.job.request_payload["native_operation"] = native_operation
    store = ExecutionStore(projection)
    runtime = WorkerRuntime(config=TaskRuntimeConfig(heartbeat_seconds=60, lease_seconds=120))
    state = cast(Any, runtime)
    state._store = store
    state._registry = TaskRegistry.from_fixture(TASK_FIXTURE)
    state._adapter = None
    monkeypatch.setattr(
        worker_module,
        "_processor",
        lambda _identity: (_ for _ in ()).throw(AssertionError("identity mismatch resolved a legacy processor")),
    )

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == ["failed"]
    failure = store.terminals[0][1]
    assert failure["error_type"] == "NativeOperationIdentityMismatch"
    assert failure["retryable"] is False


@pytest.mark.parametrize(
    "task_key",
    [
        "backfill-organize-record-tmdb-ids",
        "sync-115-directory-tree",
        "full-sync-strm",
        "intro-fingerprint-backfill",
        "monitor-115-life-events",
        "play-pool-daily-speedtest",
        "repair-p115-fingerprints",
    ],
)
def test_worker_executes_p6_sync_without_resolving_legacy_processor(
    monkeypatch: pytest.MonkeyPatch,
    task_key: str,
) -> None:
    monkeypatch.setattr(
        worker_module,
        "_processor",
        lambda _identity: (_ for _ in ()).throw(AssertionError("native P6 sync must not resolve a legacy processor")),
    )
    projection = _projection(task_key)
    store = ExecutionStore(projection)
    router = FakeP6Router()
    runtime = WorkerRuntime(config=TaskRuntimeConfig(heartbeat_seconds=60, lease_seconds=120))
    state = cast(Any, runtime)
    state._store = store
    state._registry = TaskRegistry.from_fixture(TASK_FIXTURE)
    state._adapter = None
    state._p6_p115_tasks = router

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == ["succeeded"]
    assert store.terminals[0][1]["result"] == {
        "ok": True,
        "status": "succeeded",
        "message": "directory sync complete",
        "details": {"cached": 2},
    }
    assert len(router.calls) == 1
    assert router.calls[0][0].task_key == task_key


def test_native_only_worker_dispatches_approved_organize_execution_without_legacy_processor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection = _projection("scan-organize-115")
    projection.job.request_payload["parameters"] = {"execution_id": "00000000-0000-0000-0000-000000000603"}
    store = ExecutionStore(projection)
    router = FakeP6Router(TaskOutcome.succeeded("organize confirmed", {"p4_terminalized": False}))
    runtime = WorkerRuntime(config=TaskRuntimeConfig(heartbeat_seconds=60, lease_seconds=120))
    state = cast(Any, runtime)
    state._store = store
    state._registry = TaskRegistry.from_fixture(TASK_FIXTURE)
    state._adapter = None
    state._native_only = True
    state._p6_p115_tasks = router
    monkeypatch.setattr(
        worker_module,
        "_processor",
        lambda _identity: (_ for _ in ()).throw(AssertionError("native organize resolved a legacy processor")),
    )

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == ["succeeded"]
    assert len(router.calls) == 1
    assert router.calls[0][0].payload.data == {"execution_id": "00000000-0000-0000-0000-000000000603"}


def test_native_worker_executes_organize_record_backfill_without_legacy_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection = _projection("backfill-organize-record-tmdb-ids")
    store = ExecutionStore(projection)
    repository = FakeOrganizeRecordRepository()
    router = P6P115TaskRouter(
        organize_record_tmdb_backfill=P115OrganizeRecordTmdbBackfillService(repository),
    )
    runtime = WorkerRuntime(config=TaskRuntimeConfig(heartbeat_seconds=60, lease_seconds=120))
    state = cast(Any, runtime)
    state._store = store
    state._registry = TaskRegistry.from_fixture(TASK_FIXTURE)
    state._adapter = None
    state._native_only = True
    state._p6_p115_tasks = router
    monkeypatch.setattr(
        worker_module,
        "_processor",
        lambda _identity: (_ for _ in ()).throw(AssertionError("native backfill resolved a legacy processor")),
    )
    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals: Mapping[str, object] | None = None,
        locals: Mapping[str, object] | None = None,
        fromlist: Sequence[str] | None = (),
        level: int = 0,
    ) -> Any:
        if name in {"database", "handler", "routes", "task_manager"} or name.startswith(
            ("database.", "handler.", "routes.")
        ):
            raise AssertionError(f"native backfill imported legacy runtime {name}")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert repository.calls == 1
    assert [status for status, _values in store.terminals] == ["succeeded"]
    assert store.terminals[0][1]["result"] == {
        "ok": True,
        "status": "succeeded",
        "message": "整理记录 TMDb ID 补齐完成，共更新 2 条记录。",
        "details": {
            "updated_count": 2,
            "task_key": "backfill-organize-record-tmdb-ids",
            "native_handler": "P6P115TaskRouter",
            "compatibility_fallback": False,
        },
    }


def test_native_worker_executes_real_directory_service_without_legacy_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(
        (
            httpx.Response(
                200,
                json={
                    "state": True,
                    "count": 2,
                    "data": [
                        {"file_id": "dir-a", "file_name": "A", "type": 0},
                        {"file_id": "movie", "file_name": "movie.mkv", "type": 1},
                    ],
                },
            ),
            httpx.Response(200, json={"state": True, "count": 2, "data": []}),
        )
    )
    transport = httpx.MockTransport(lambda _request: next(responses))
    credentials = NativeDirectoryCredentials()
    repository = NativeDirectoryRepository()
    directory_sync = P115DirectorySyncService(
        settings=NativeDirectorySettings(),
        credentials=credentials,
        directories=HttpxP115DirectoryTreeGateway(
            credentials=credentials,
            priority=lambda: "openapi",
            client_factory=lambda **kwargs: httpx.Client(transport=transport, **kwargs),
        ),
        repository=repository,
    )
    projection = _projection("sync-115-directory-tree")
    store = ExecutionStore(projection)
    runtime = WorkerRuntime(config=TaskRuntimeConfig(heartbeat_seconds=60, lease_seconds=120))
    state = cast(Any, runtime)
    state._store = store
    state._registry = TaskRegistry.from_fixture(TASK_FIXTURE)
    state._adapter = None
    state._native_only = True
    state._p6_p115_tasks = P6P115TaskRouter(directory_sync=directory_sync)
    monkeypatch.setattr(
        worker_module,
        "_processor",
        lambda _identity: (_ for _ in ()).throw(AssertionError("native directory sync resolved a processor")),
    )
    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals: Mapping[str, object] | None = None,
        locals: Mapping[str, object] | None = None,
        fromlist: Sequence[str] | None = (),
        level: int = 0,
    ) -> Any:
        if level == 0 and (
            name in {"database", "extensions", "handler", "routes", "task_manager", "tasks"}
            or name.startswith(("database.", "handler.", "routes.", "tasks."))
        ):
            raise AssertionError(f"native directory sync imported legacy runtime {name}")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert repository.calls == [
        (
            P115DirectoryRoot("root-a", "Movies"),
            (P115DirectoryCacheEntry("dir-a", "root-a", "A", "Movies/A"),),
            "115001",
        )
    ]
    assert [status for status, _values in store.terminals] == ["succeeded"]
    assert store.terminals[0][1]["result"] == {
        "ok": True,
        "status": "succeeded",
        "message": "同步结束：共更新 1 个目录，清理 1 个失效缓存",
        "details": {
            "roots_total": 1,
            "roots_completed": 1,
            "roots_failed": 0,
            "cached": 1,
            "cleaned": 1,
            "errors": [],
            "account_bound": True,
            "authoritative_cleanup_only": True,
            "task_key": "sync-115-directory-tree",
            "native_handler": "P6P115TaskRouter",
            "compatibility_fallback": False,
        },
    }


def test_native_worker_executes_real_fingerprint_service_without_legacy_runtime(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    file_id = "9001"
    pick_code = str(to_pickcode(int(file_id)))
    sha1 = "A" * 40
    strm_path = tmp_path / "Movies" / "Fixture.strm"
    strm_path.parent.mkdir()
    strm_path.write_text(
        f"https://fixture.invalid/p115/play/{pick_code}/Fixture.mkv",
        encoding="utf-8",
    )
    row = P115FingerprintRepairRow.create(
        tmdb_id="101",
        item_type="Movie",
        title="Fixture",
        assets=({"path": str(strm_path)},),
        sha1s=("",),
        pick_codes=(pick_code,),
        revision="1" * 64,
    )
    repository = NativeFingerprintRepository(row)
    credentials = NativeDirectoryCredentials()
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "state": True,
                "data": {
                    "file_id": file_id,
                    "parent_id": "8001",
                    "file_name": "Fixture.mkv",
                    "sha1": sha1,
                    "pick_code": pick_code,
                    "size": 123,
                },
            },
        )

    repair = P115FingerprintRepairService(
        credentials=credentials,
        repository=repository,
        hints=LocalP115FingerprintHintGateway(),
        remote=HttpxP115FingerprintRemoteGateway(
            credentials=credentials,
            priority=lambda: "openapi",
            client_factory=lambda **kwargs: httpx.Client(
                transport=httpx.MockTransport(respond),
                **kwargs,
            ),
        ),
        local_root=lambda: str(tmp_path),
    )
    projection = _projection("repair-p115-fingerprints")
    store = ExecutionStore(projection)
    runtime = WorkerRuntime(config=TaskRuntimeConfig(heartbeat_seconds=60, lease_seconds=120))
    state = cast(Any, runtime)
    state._store = store
    state._registry = TaskRegistry.from_fixture(TASK_FIXTURE)
    state._adapter = None
    state._native_only = True
    state._p6_p115_tasks = P6P115TaskRouter(fingerprint_repair=repair)
    monkeypatch.setattr(
        worker_module,
        "_processor",
        lambda _identity: (_ for _ in ()).throw(AssertionError("native fingerprint repair resolved a processor")),
    )
    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals: Mapping[str, object] | None = None,
        locals: Mapping[str, object] | None = None,
        fromlist: Sequence[str] | None = (),
        level: int = 0,
    ) -> Any:
        if level == 0 and (
            name in {"database", "extensions", "handler", "routes", "task_manager", "tasks"}
            or name.startswith(("database.", "handler.", "routes.", "tasks."))
        ):
            raise AssertionError(f"native fingerprint repair imported legacy runtime {name}")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert len(requests) == 1
    assert requests[0].url.path == "/open/folder/get_info"
    assert requests[0].headers["Authorization"] == "Bearer fixture-openapi-token"
    assert repository.commits == [
        (
            0,
            P115FileFingerprint(
                file_id=file_id,
                parent_id="8001",
                name="Fixture.mkv",
                sha1=sha1,
                pick_code=pick_code,
                size=123,
                account_id="115001",
                local_path="Movies/Fixture.mkv",
            ),
            "115001",
        )
    ]
    assert [status for status, _values in store.terminals] == ["succeeded"]
    result = store.terminals[0][1]["result"]
    assert result["status"] == "succeeded"
    assert result["details"]["fixed_assets"] == 1
    assert result["details"]["remote_fixed_assets"] == 1
    assert result["details"]["task_key"] == "repair-p115-fingerprints"
    assert result["details"]["native_handler"] == "P6P115TaskRouter"
    assert result["details"]["compatibility_fallback"] is False


def test_worker_persists_partial_directory_sync_details_on_failed_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(worker_module, "_processor", lambda _identity: None)
    projection = _projection()
    store = ExecutionStore(projection)
    router = FakeP6Router(
        TaskOutcome.partial(
            "one root failed",
            {"roots_completed": 1, "roots_failed": 1, "errors": [{"cid": "root-b"}]},
        )
    )
    runtime = WorkerRuntime(config=TaskRuntimeConfig(heartbeat_seconds=60, lease_seconds=120))
    state = cast(Any, runtime)
    state._store = store
    state._registry = TaskRegistry.from_fixture(TASK_FIXTURE)
    state._adapter = None
    state._p6_p115_tasks = router

    runtime._execute_claimed(SimpleNamespace(projection=projection))  # type: ignore[arg-type]

    assert [status for status, _values in store.terminals] == ["failed"]
    assert store.terminals[0][1]["error_details"]["task_outcome"] == {
        "ok": False,
        "status": "partial",
        "message": "one root failed",
        "details": {
            "roots_completed": 1,
            "roots_failed": 1,
            "errors": [{"cid": "root-b"}],
        },
    }
