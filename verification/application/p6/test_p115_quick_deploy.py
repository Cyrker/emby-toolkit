from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import httpx
import pytest

import etk.domains.p115.quick_deploy as quick_deploy
import etk.domains.p115.router as p115_router
from etk.domains.operations.jobs import JobSubmissionService
from etk.domains.p115 import (
    EmbyQuickDeployIndeterminate,
    EmbyQuickDeployRejected,
    HttpxEmbyQuickDeployGateway,
    P115CredentialSnapshot,
    P115LocalMirror,
    P115QuickDeployIntentService,
    P115QuickDeployOperation,
)
from etk.platform import Settings
from etk.platform.jobs import JobStatus, PersistentEffectIndeterminate
from etk.platform.jobs.json_payload import canonical_json_hash
from etk.platform.jobs.store import _idempotency_request_envelope
from etk.platform.tasks import TaskRegistry

REPO_ROOT = Path(__file__).resolve().parents[3]
TASK_FIXTURE = REPO_ROOT / "verification" / "fixtures" / "tasks" / "ordered-task-catalog.json"
JOB_ID = UUID("00000000-0000-0000-0000-000000000115")


class FakeCredentials:
    def __init__(self, snapshot: P115CredentialSnapshot) -> None:
        self.snapshot = snapshot

    def load(self) -> P115CredentialSnapshot:
        return self.snapshot


class FakeAppSettings:
    def __init__(
        self,
        revision: str = "revision-1",
        *,
        fail_merge: bool = False,
        persisted: Mapping[str, Any] | None = None,
    ) -> None:
        self.current_revision = revision
        self.fail_merge = fail_merge
        self.persisted = dict(persisted) if persisted is not None else None
        self.merges: list[tuple[dict[str, Any], str]] = []

    def revision(self, key: str) -> str:
        assert key == "dynamic_app_config"
        return self.current_revision

    def merge_mapping_if_revision(
        self,
        key: str,
        updates: Mapping[str, Any],
        *,
        expected_revision: str,
        remove_keys: Sequence[str] = (),
    ) -> Mapping[str, Any]:
        assert key == "dynamic_app_config"
        assert tuple(remove_keys) == ()
        self.merges.append((dict(updates), expected_revision))
        if self.fail_merge or expected_revision != self.current_revision:
            raise RuntimeError("dynamic config revision changed")
        return dict(self.persisted) if self.persisted is not None else dict(updates)


class FakeProgress:
    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []

    def checkpoint(self) -> None:
        self.events.append(("checkpoint", None))

    def update(self, progress: int, message: str) -> None:
        self.events.append(("update", (progress, message)))

    def record_started_effect(self, label: str) -> None:
        self.events.append(("started", label))

    def record_committed_effect(self, label: str) -> None:
        self.events.append(("committed", label))

    def record_abandoned_effect(self, label: str) -> None:
        self.events.append(("abandoned", label))


class FakeLock:
    @contextmanager
    def hold(self):
        yield


class FakeProvider:
    def __init__(self, client: object | None) -> None:
        self.client = client
        self.opens: list[tuple[str | None, str | None]] = []

    def open(
        self,
        *,
        expected_account_id: str | None = None,
        expected_backend: str | None = None,
    ) -> object | None:
        self.opens.append((expected_account_id, expected_backend))
        return self.client


class FakeLocalConfiguration:
    def __init__(self) -> None:
        self.bundles: list[dict[str, Any]] = []

    def save_bundle(self, **bundle: Any) -> None:
        self.bundles.append(dict(bundle))


class FakeEmby:
    def __init__(self, *, duplicate_ids: bool = False) -> None:
        self.duplicate_ids = duplicate_ids
        self.preflight_calls = 0
        self.libraries: list[tuple[str, str, str]] = []

    def preflight(self, **_identity: object) -> None:
        self.preflight_calls += 1

    def ensure_library(
        self,
        *,
        name: str,
        collection_type: str,
        path: str,
        **_identity: object,
    ) -> Mapping[str, Any]:
        self.libraries.append((name, collection_type, path))
        return {
            "name": name,
            "path": path,
            "id": "duplicate" if self.duplicate_ids else f"library-{len(self.libraries)}",
            "created": False,
        }


class ExistingDirectoryClient:
    def __init__(self) -> None:
        self.mkdir_calls: list[tuple[str, str]] = []
        self._tree = {
            "0": [
                {"cid": "100", "fn": "ETK媒体库", "fc": "0"},
                {"cid": "200", "fn": "ETK待整理", "fc": "0"},
                {"cid": "300", "fn": "ETK未识别", "fc": "0"},
            ],
            "100": [
                {"cid": "110", "fn": "动漫", "fc": "0"},
                {"cid": "120", "fn": "电影", "fc": "0"},
                {"cid": "130", "fn": "剧集", "fc": "0"},
            ],
            "110": [
                {"cid": "111", "fn": "国漫", "fc": "0"},
                {"cid": "112", "fn": "日番", "fc": "0"},
                {"cid": "113", "fn": "美漫", "fc": "0"},
            ],
            "120": [
                {"cid": "121", "fn": "国产片", "fc": "0"},
                {"cid": "122", "fn": "日韩片", "fc": "0"},
                {"cid": "123", "fn": "欧美片", "fc": "0"},
            ],
            "130": [
                {"cid": "131", "fn": "国产剧", "fc": "0"},
                {"cid": "132", "fn": "日韩剧", "fc": "0"},
                {"cid": "133", "fn": "欧美剧", "fc": "0"},
            ],
        }

    def list_children(self, parent_id: str) -> tuple[dict[str, Any], ...]:
        return tuple(dict(item) for item in self._tree.get(parent_id, []))

    def create_directory(self, parent_id: str, name: str) -> Mapping[str, Any]:
        self.mkdir_calls.append((parent_id, name))
        raise AssertionError("exact existing directories must not be recreated")


class SequencedDirectoryClient:
    def __init__(
        self,
        listings: Sequence[Sequence[Mapping[str, Any]]],
        *,
        create_error: Exception | None = None,
    ) -> None:
        self.listings = [tuple(dict(item) for item in values) for values in listings]
        self.create_error = create_error
        self.mkdir_calls: list[tuple[str, str]] = []

    def list_children(self, parent_id: str) -> tuple[dict[str, Any], ...]:
        del parent_id
        return self.listings.pop(0)

    def create_directory(self, parent_id: str, name: str) -> Mapping[str, Any]:
        self.mkdir_calls.append((parent_id, name))
        if self.create_error is not None:
            raise self.create_error
        return {"state": True, "cid": "new-directory"}


def _settings(root: Path, *, emby_api_key: str = "fixture-token") -> Settings:
    return Settings.model_validate(
        {
            "emby_server_url": "http://emby.example.test",
            "emby_api_key": emby_api_key,
            "emby_auth_mode": "user_token",
            "emby_user_id": "admin-1",
            "etk_server_url": "http://etk.example.test",
            "local_strm_root": str(root),
            "monitor_paths": [str(root / "existing")],
        }
    )


def _credentials(token: str = "token-1") -> FakeCredentials:
    return FakeCredentials(
        P115CredentialSnapshot(
            account_id="115001",
            access_token=token,
            refresh_token="refresh-1",
            cookie="UID=115001; CID=fixture",
            app_type="web",
        )
    )


def _intent(
    settings_provider: Any,
    credentials: FakeCredentials,
    app_settings: FakeAppSettings,
) -> P115QuickDeployIntentService:
    return P115QuickDeployIntentService(
        settings=settings_provider,
        credentials=credentials,  # type: ignore[arg-type]
        app_settings=app_settings,
        priority=lambda: "openapi",
    )


def _operation(
    *,
    settings_provider: Any,
    credentials: FakeCredentials,
    app_settings: FakeAppSettings,
    provider: FakeProvider,
    emby: FakeEmby,
) -> tuple[P115QuickDeployOperation, FakeLocalConfiguration]:
    local_configuration = FakeLocalConfiguration()
    operation = P115QuickDeployOperation(
        settings=settings_provider,
        credentials=credentials,  # type: ignore[arg-type]
        provider=provider,  # type: ignore[arg-type]
        account_lock=FakeLock(),  # type: ignore[arg-type]
        app_settings=app_settings,
        local_configuration=local_configuration,
        emby=emby,  # type: ignore[arg-type]
    )
    return operation, local_configuration


def test_quick_deploy_intent_freezes_non_secret_credential_identity(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    parameters = _intent(lambda: settings, _credentials(), FakeAppSettings()).parameters()

    assert parameters["account_id"] == "115001"
    assert parameters["backend"] == "openapi"
    assert parameters["dynamic_config_revision"] == "revision-1"
    assert len(parameters["credential_fingerprint"]) == 64
    assert "token-1" not in json.dumps(parameters)


@pytest.mark.parametrize(
    "drift",
    ["credential", "desired-state", "dynamic-revision", "emby-credential"],
)
def test_quick_deploy_drift_blocks_before_provider_write(tmp_path: Path, drift: str) -> None:
    current = {"settings": _settings(tmp_path)}
    credentials = _credentials()
    app_settings = FakeAppSettings()

    def settings_provider() -> Settings:
        return current["settings"]

    parameters = _intent(settings_provider, credentials, app_settings).parameters()
    if drift == "credential":
        credentials.snapshot = P115CredentialSnapshot(account_id="115001", access_token="token-2")
    elif drift == "desired-state":
        current["settings"] = _settings(tmp_path / "changed")
    elif drift == "emby-credential":
        current["settings"] = _settings(tmp_path, emby_api_key="changed-emby-token")
    else:
        app_settings.current_revision = "revision-2"
    provider = FakeProvider(ExistingDirectoryClient())
    emby = FakeEmby()
    operation, local_configuration = _operation(
        settings_provider=settings_provider,
        credentials=credentials,
        app_settings=app_settings,
        provider=provider,
        emby=emby,
    )

    with pytest.raises(RuntimeError, match="发生变化"):
        operation.execute(parameters, FakeProgress())

    assert provider.opens == []
    assert emby.preflight_calls == 0
    assert local_configuration.bundles == []
    assert app_settings.merges == []


def test_quick_deploy_exact_directory_reuse_does_not_write() -> None:
    client = SequencedDirectoryClient([[{"cid": "200", "fn": "电影", "fc": "0"}]])
    result = P115QuickDeployOperation._ensure_directory(client, "100", "电影", FakeProgress())

    assert result == {"cid": "200", "name": "电影", "created": False}
    assert client.mkdir_calls == []


def test_quick_deploy_directory_write_requires_authoritative_readback() -> None:
    client = SequencedDirectoryClient([[], []])

    with pytest.raises(PersistentEffectIndeterminate, match="无法确认"):
        P115QuickDeployOperation._ensure_directory(client, "100", "电影", FakeProgress())

    assert client.mkdir_calls == [("100", "电影")]


def test_quick_deploy_directory_timeout_confirms_unique_readback_without_resend() -> None:
    client = SequencedDirectoryClient(
        [[], [{"cid": "200", "fn": "电影", "fc": "0"}]],
        create_error=TimeoutError("provider timeout"),
    )

    result = P115QuickDeployOperation._ensure_directory(client, "100", "电影", FakeProgress())

    assert result == {"cid": "200", "name": "电影", "created": True}
    assert client.mkdir_calls == [("100", "电影")]


def test_quick_deploy_directory_readback_rejects_file_and_directory_collision() -> None:
    client = SequencedDirectoryClient(
        [
            [],
            [
                {"fid": "file-1", "fn": "电影", "fc": "1"},
                {"cid": "200", "fn": "电影", "fc": "0"},
            ],
        ]
    )

    with pytest.raises(PersistentEffectIndeterminate, match="无法确认"):
        P115QuickDeployOperation._ensure_directory(client, "100", "电影", FakeProgress())

    assert client.mkdir_calls == [("100", "电影")]


def test_quick_deploy_result_exposes_only_allowlisted_dynamic_updates(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    credentials = _credentials()
    app_settings = FakeAppSettings(
        persisted={
            "ai_api_key": "fixture-secret-ai-key",
            "telegram_bot_token": "fixture-secret-bot-token",
        }
    )
    operation, local_configuration = _operation(
        settings_provider=lambda: settings,
        credentials=credentials,
        app_settings=app_settings,
        provider=FakeProvider(ExistingDirectoryClient()),
        emby=FakeEmby(),
    )

    result = operation.execute(
        _intent(lambda: settings, credentials, app_settings).parameters(),
        FakeProgress(),
    )

    serialized = json.dumps(result, ensure_ascii=False)
    assert "fixture-secret-ai-key" not in serialized
    assert "fixture-secret-bot-token" not in serialized
    assert result["config"] == app_settings.merges[0][0]
    assert len(local_configuration.bundles) == 1


def test_quick_deploy_local_mirror_rejects_symlink_escape(tmp_path: Path) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "动漫").symlink_to(outside, target_is_directory=True)
    category_dirs = {
        name: {"path": name if name in quick_deploy._CATEGORY_CHILDREN else f"动漫/{name}"}
        for name in (*quick_deploy._CATEGORY_CHILDREN, *quick_deploy._CATEGORY_CHILDREN["动漫"])
    }
    for name in (*quick_deploy._CATEGORY_CHILDREN["电影"], *quick_deploy._CATEGORY_CHILDREN["剧集"]):
        parent = "电影" if name in quick_deploy._CATEGORY_CHILDREN["电影"] else "剧集"
        category_dirs[name] = {"path": f"{parent}/{name}"}

    with pytest.raises(RuntimeError, match="越过配置根目录"):
        P115LocalMirror().create(str(root), category_dirs)


def _emby_folder(
    *,
    name: str = "国漫",
    path: str = "/media/国漫",
    collection_type: str = "tvshows",
    options: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "Name": name,
        "CollectionType": collection_type,
        "Locations": [path],
        "ItemId": "library-1",
        "LibraryOptions": dict(options or {}),
    }


def _emby_gateway(handler: Any, *, settings: Settings) -> HttpxEmbyQuickDeployGateway:
    transport = httpx.MockTransport(handler)
    return HttpxEmbyQuickDeployGateway(
        lambda: settings,
        client_factory=lambda: httpx.Client(transport=transport),
        reconcile_timeout_seconds=0.1,
        sleeper=lambda _seconds: None,
    )


def test_emby_exact_reuse_and_conflict_are_fail_closed(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    desired = quick_deploy._library_options("tvshows")
    posts: list[str] = []

    def exact_handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            posts.append(request.url.path)
        return httpx.Response(200, json=[_emby_folder(options=desired)])

    reused = _emby_gateway(exact_handler, settings=settings).ensure_library(
        name="国漫",
        collection_type="tvshows",
        path="/media/国漫",
    )
    assert reused["created"] is False
    assert posts == []

    def conflict_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[_emby_folder(path="/other/path", options=desired)])

    with pytest.raises(EmbyQuickDeployRejected, match="冲突"):
        _emby_gateway(conflict_handler, settings=settings).ensure_library(
            name="国漫",
            collection_type="tvshows",
            path="/media/国漫",
        )


def test_emby_create_timeout_uses_readback_without_resend(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    desired = quick_deploy._library_options("tvshows")
    gets = 0
    posts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal gets, posts
        if request.method == "POST":
            posts += 1
            raise httpx.ReadTimeout("fixture timeout", request=request)
        gets += 1
        return httpx.Response(200, json=[] if gets == 1 else [_emby_folder(options=desired)])

    result = _emby_gateway(handler, settings=settings).ensure_library(
        name="国漫",
        collection_type="tvshows",
        path="/media/国漫",
    )

    assert result["created"] is True
    assert posts == 1
    assert gets == 2


def test_emby_library_options_are_written_and_read_back(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    desired = quick_deploy._library_options("tvshows")
    current_options: dict[str, Any] = {}
    writes: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal current_options
        if request.method == "POST":
            payload = json.loads(request.content)
            writes.append(payload)
            current_options = dict(payload["LibraryOptions"])
            return httpx.Response(204)
        return httpx.Response(200, json=[_emby_folder(options=current_options)])

    result = _emby_gateway(handler, settings=settings).ensure_library(
        name="国漫",
        collection_type="tvshows",
        path="/media/国漫",
    )

    assert result["id"] == "library-1"
    assert writes == [{"Id": "library-1", "LibraryOptions": desired}]


def test_emby_library_options_timeout_uses_readback_without_resend(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    desired = quick_deploy._library_options("tvshows")
    current_options: dict[str, Any] = {}
    posts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal current_options, posts
        if request.method == "POST":
            posts += 1
            current_options = desired
            raise httpx.ReadTimeout("fixture timeout", request=request)
        return httpx.Response(200, json=[_emby_folder(options=current_options)])

    result = _emby_gateway(handler, settings=settings).ensure_library(
        name="国漫",
        collection_type="tvshows",
        path="/media/国漫",
    )

    assert result["id"] == "library-1"
    assert posts == 1


@pytest.mark.parametrize("failure", ["duplicate-libraries", "cas-conflict"])
def test_quick_deploy_does_not_publish_unconverged_dynamic_config(tmp_path: Path, failure: str) -> None:
    settings = _settings(tmp_path)
    credentials = _credentials()
    app_settings = FakeAppSettings(fail_merge=failure == "cas-conflict")
    provider = FakeProvider(ExistingDirectoryClient())
    emby = FakeEmby(duplicate_ids=failure == "duplicate-libraries")
    operation, _repositories = _operation(
        settings_provider=lambda: settings,
        credentials=credentials,
        app_settings=app_settings,
        provider=provider,
        emby=emby,
    )
    parameters = _intent(lambda: settings, credentials, app_settings).parameters()

    with pytest.raises(RuntimeError):
        operation.execute(parameters, FakeProgress())

    assert len(emby.libraries) == 9
    if failure == "duplicate-libraries":
        assert app_settings.merges == []
    else:
        assert len(app_settings.merges) == 1


def _job(
    status: JobStatus,
    *,
    progress: int = 0,
    message: str | None = None,
    result: Mapping[str, Any] | None = None,
    error: Mapping[str, Any] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=JOB_ID,
        status=status,
        progress=progress,
        progress_message=message,
        result=dict(result or {}),
        error_summary=dict(error or {}),
    )


def test_quick_deploy_ndjson_emits_accepted_progress_and_confirmed_done(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initial = SimpleNamespace(job=_job(JobStatus.QUEUED))
    values = [
        SimpleNamespace(job=_job(JobStatus.RUNNING, progress=40, message="working")),
        SimpleNamespace(job=_job(JobStatus.SUCCEEDED, progress=100, result={"config": {"ok": True}})),
    ]

    class Jobs:
        def get(self, _job_id: UUID) -> SimpleNamespace:
            return values.pop(0)

    async def no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(p115_router.asyncio, "sleep", no_sleep)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(configuration_synchronizer=None)))

    async def collect() -> list[dict[str, Any]]:
        return [
            json.loads(line)
            async for line in p115_router._quick_deploy_events(request, Jobs(), initial)  # type: ignore[arg-type]
        ]

    events = asyncio.run(collect())

    assert [event["type"] for event in events] == ["accepted", "progress", "done"]
    assert events[-1]["success"] is True
    assert events[-1]["status"] == "confirmed"


def test_quick_deploy_queued_timeout_waits_for_racing_worker_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initial = SimpleNamespace(job=_job(JobStatus.QUEUED))
    values = [
        SimpleNamespace(job=_job(JobStatus.QUEUED)),
        SimpleNamespace(
            job=_job(
                JobStatus.FAILED,
                error={
                    "type": "PersistentEffectIndeterminate",
                    "message": "provider effect outcome is unknown",
                },
            )
        ),
    ]

    class Jobs:
        def __init__(self) -> None:
            self.cancel_calls = 0

        def get(self, _job_id: UUID) -> SimpleNamespace:
            return values.pop(0)

        def cancel(self, _job_id: UUID) -> SimpleNamespace:
            self.cancel_calls += 1
            return SimpleNamespace(job=_job(JobStatus.CANCEL_REQUESTED))

    async def no_sleep(_seconds: float) -> None:
        return None

    jobs = Jobs()
    clock = iter((0.0, 31.0, 31.0, 31.0)).__next__
    monkeypatch.setattr(p115_router, "time", SimpleNamespace(monotonic=clock))
    monkeypatch.setattr(p115_router.asyncio, "sleep", no_sleep)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    async def collect() -> list[dict[str, Any]]:
        return [
            json.loads(line)
            async for line in p115_router._quick_deploy_events(
                cast(Any, request),
                cast(Any, jobs),
                cast(Any, initial),
            )
        ]

    events = asyncio.run(collect())

    assert jobs.cancel_calls == 1
    assert all(event.get("status") != "cancelled" for event in events)
    assert events[-1]["status"] == "indeterminate"
    assert events[-1]["job_id"] == str(JOB_ID)


def test_quick_deploy_cancel_requested_stream_has_finite_indeterminate_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initial = SimpleNamespace(job=_job(JobStatus.QUEUED))
    values = [
        SimpleNamespace(job=_job(JobStatus.QUEUED)),
        SimpleNamespace(job=_job(JobStatus.CANCEL_REQUESTED)),
    ]

    class Jobs:
        def get(self, _job_id: UUID) -> SimpleNamespace:
            return values.pop(0)

        def cancel(self, _job_id: UUID) -> SimpleNamespace:
            return SimpleNamespace(job=_job(JobStatus.CANCEL_REQUESTED))

    async def no_sleep(_seconds: float) -> None:
        return None

    clock = iter((0.0, 31.0, 31.0, 31.0, 62.0)).__next__
    monkeypatch.setattr(p115_router, "time", SimpleNamespace(monotonic=clock))
    monkeypatch.setattr(p115_router.asyncio, "sleep", no_sleep)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    async def collect() -> list[dict[str, Any]]:
        return [
            json.loads(line)
            async for line in p115_router._quick_deploy_events(
                cast(Any, request),
                cast(Any, Jobs()),
                cast(Any, initial),
            )
        ]

    events = asyncio.run(collect())

    assert events[-1] == {
        "type": "done",
        "success": False,
        "status": "indeterminate",
        "message": "一键部署任务取消后仍未到达终态，请通过 job_id 查询最终状态",
        "job_id": str(JOB_ID),
    }


def test_quick_deploy_running_stream_has_finite_indeterminate_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initial = SimpleNamespace(job=_job(JobStatus.RUNNING))

    class Jobs:
        def get(self, _job_id: UUID) -> SimpleNamespace:
            return SimpleNamespace(job=_job(JobStatus.RUNNING))

    async def no_sleep(_seconds: float) -> None:
        return None

    clock = iter((0.0, 1806.0)).__next__
    monkeypatch.setattr(p115_router, "time", SimpleNamespace(monotonic=clock))
    monkeypatch.setattr(p115_router.asyncio, "sleep", no_sleep)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    async def collect() -> list[dict[str, Any]]:
        return [
            json.loads(line)
            async for line in p115_router._quick_deploy_events(
                cast(Any, request),
                cast(Any, Jobs()),
                cast(Any, initial),
            )
        ]

    events = asyncio.run(collect())

    assert events[-1] == {
        "type": "done",
        "success": False,
        "status": "indeterminate",
        "message": "一键部署任务仍在运行或状态无法推进，请通过 job_id 查询最终状态",
        "job_id": str(JOB_ID),
    }


def test_quick_deploy_worker_loss_is_reported_indeterminate() -> None:
    projection = SimpleNamespace(
        job=_job(
            JobStatus.FAILED,
            error={
                "type": "PersistentEffectIndeterminate",
                "message": "worker lease expired",
            },
        )
    )
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    event = asyncio.run(
        p115_router._quick_deploy_terminal_event(
            cast(Any, request),
            cast(Any, projection),
        )
    )

    assert event["success"] is False
    assert event["status"] == "indeterminate"


class CaptureStore:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def create_job(self, **values: object) -> SimpleNamespace:
        self.calls.append(dict(values))
        projection = SimpleNamespace(job=SimpleNamespace(id=JOB_ID))
        return SimpleNamespace(projection=projection, reused=False)


def test_native_operation_submission_persists_only_bounded_dispatch_identity() -> None:
    store = CaptureStore()
    service = JobSubmissionService(store, TaskRegistry.from_fixture(TASK_FIXTURE))  # type: ignore[arg-type]

    service.submit_native_operation(
        "p115.mkdir",
        {"parent_id": "100", "name": "电影"},
        display_name="创建目录",
        idempotency_key="fixture-key",
    )

    call = store.calls[0]
    assert call["task_key"] == "native-operation:p115.mkdir"
    assert call["max_attempts"] == 1
    assert call["payload"] == {
        "payload_version": 1,
        "parameters": {"parent_id": "100", "name": "电影"},
        "processor_type": "media",
        "display_name": "创建目录",
        "silent": True,
        "call_style": "direct",
        "native_operation": "p115.mkdir",
    }
    assert "module" not in json.dumps(call["payload"])
    with pytest.raises(ValueError, match="exactly one"):
        service.submit_native_operation("p115.mkdir", {}, display_name="invalid", max_attempts=2)


def test_quick_deploy_idempotency_ignores_only_execution_time_revision() -> None:
    envelope = {
        "task_key": "native-operation:p115.quick-deploy",
        "payload": {
            "parameters": {
                "desired_state_hash": "stable-target",
                "dynamic_config_revision": "revision-1",
            }
        },
    }
    replay = {
        **envelope,
        "payload": {
            "parameters": {
                "desired_state_hash": "stable-target",
                "dynamic_config_revision": "revision-2",
            }
        },
    }

    assert canonical_json_hash(
        _idempotency_request_envelope(envelope["task_key"], envelope),
        field="job request",
    ) == canonical_json_hash(
        _idempotency_request_envelope(replay["task_key"], replay),
        field="job request",
    )


def test_emby_create_without_authoritative_readback_is_indeterminate(tmp_path: Path) -> None:
    settings = _settings(tmp_path)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(204)
        return httpx.Response(200, json=[])

    gateway = HttpxEmbyQuickDeployGateway(
        lambda: settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
        clock=iter((0.0, 1.0)).__next__,
        sleeper=lambda _seconds: None,
        reconcile_timeout_seconds=0.1,
    )

    with pytest.raises(EmbyQuickDeployIndeterminate, match="无法确认"):
        gateway.ensure_library(name="国漫", collection_type="tvshows", path="/media/国漫")
