from __future__ import annotations

import hashlib
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from etk.domains.p115 import (
    AtomicP115LocalStrmLibraryGateway,
    P115CredentialSnapshot,
    P115LocalDeleteAction,
    P115LocalPathState,
    P115LocalReconcileResult,
    P115LocalWriteResult,
    P115RenameConfigSnapshot,
    P115SortingRulesSnapshot,
    P115StrmCacheEntry,
    P115StrmCacheReconcileResult,
    P115StrmCacheSnapshot,
    P115StrmNotificationRecovery,
    P115StrmSyncPlan,
    P115StrmSyncRoot,
    P115StrmSyncService,
)
from etk.domains.p115.media_probe import P115MediaProbeRequest, P115MediaProbeStatus
from etk.domains.p115.strm_sync import P115StrmMediaInfoEnrichmentResult, cache_snapshot_revision
from etk.platform import Settings
from etk.platform.tasks import TaskOutcomeStatus


class FakeCredentials:
    def __init__(self, account_ids: Sequence[str] = ("115001",)) -> None:
        self.account_ids = list(account_ids)
        self.calls = 0

    def load(self) -> P115CredentialSnapshot:
        index = min(self.calls, len(self.account_ids) - 1)
        self.calls += 1
        return P115CredentialSnapshot(account_id=self.account_ids[index])

    def save_openapi(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("STRM sync must not mutate credentials")

    def save_cookie(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("STRM sync must not mutate credentials")

    def app_label(self, app_type: str) -> str:
        return app_type


class FakeSortingRules:
    def __init__(self, rules: Sequence[Mapping[str, Any]], account_id: str = "115001") -> None:
        self.snapshot = P115SortingRulesSnapshot(tuple(dict(rule) for rule in rules), account_id)

    def load(self) -> P115SortingRulesSnapshot:
        return self.snapshot

    def save(self, *_args: Any, **_kwargs: Any) -> P115SortingRulesSnapshot:
        raise AssertionError("STRM sync must not save sorting rules")


class FakeRenameConfig:
    def __init__(self, config: Mapping[str, Any] | None = None) -> None:
        self.snapshot = P115RenameConfigSnapshot(dict(config or {}))

    def load(self) -> P115RenameConfigSnapshot:
        return self.snapshot

    def save(self, *_args: Any, **_kwargs: Any) -> P115RenameConfigSnapshot:
        raise AssertionError("STRM sync must not save rename config")


class FakeRemoteClient:
    backend = "openapi"

    def __init__(
        self,
        pages: Mapping[tuple[str, int, int], Mapping[str, Any]],
        paths: Mapping[str, Sequence[Mapping[str, Any]]],
    ) -> None:
        self.pages = dict(pages)
        self.paths = dict(paths)
        self.list_calls: list[tuple[str, int, int, int]] = []
        self.path_calls: list[str] = []
        self.download_calls: list[tuple[str, int]] = []

    def list_page(
        self,
        *,
        cid: str,
        file_type: int,
        limit: int,
        offset: int,
    ) -> Mapping[str, Any]:
        self.list_calls.append((cid, file_type, limit, offset))
        return self.pages[(cid, file_type, offset)]

    def directory_path(self, directory_id: str) -> Sequence[Mapping[str, Any]]:
        self.path_calls.append(directory_id)
        return self.paths[directory_id]

    def download_subtitle(self, pick_code: str, *, max_bytes: int) -> bytes:
        self.download_calls.append((pick_code, max_bytes))
        return f"subtitle:{pick_code}".encode()


class FakeRemoteGateway:
    def __init__(self, client: FakeRemoteClient | None) -> None:
        self.client = client
        self.calls: list[str] = []

    def open(self, *, expected_account_id: str) -> FakeRemoteClient | None:
        self.calls.append(expected_account_id)
        return self.client


class FakeLibrary:
    def __init__(
        self,
        *,
        existing: Sequence[str] = (),
        deleted: Sequence[str] = (),
    ) -> None:
        self.existing = set(existing)
        self.deleted = tuple(deleted)
        self.contents = {path: b"fixture-existing" for path in existing}
        self.inodes = {path: index for index, path in enumerate(existing, start=1)}
        self.versions = {path: 1 for path in existing}
        self.strm_writes: list[tuple[str, str]] = []
        self.subtitle_writes: list[tuple[str, bytes]] = []
        self.preview_calls: list[dict[str, Any]] = []
        self.reconcile_calls: list[dict[str, Any]] = []
        self.adapter_calls: list[str] = []

    def root_path(self) -> str:
        self.adapter_calls.append("root_path")
        return "/fixture/library"

    def absolute_path(self, relative_path: str) -> str:
        self.adapter_calls.append("absolute_path")
        return f"/fixture/library/{relative_path}"

    def path_state(self, relative_path: str) -> P115LocalPathState:
        self.adapter_calls.append("path_state")
        if relative_path not in self.existing:
            return P115LocalPathState(kind="absent")
        content = self.contents.get(relative_path, b"fixture-existing")
        return P115LocalPathState(
            kind="file",
            size=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
            device=1,
            inode=self.inodes.setdefault(relative_path, len(self.inodes) + 1),
            mtime_ns=self.versions.setdefault(relative_path, 1),
        )

    def plan_delete(
        self,
        relative_path: str,
        *,
        kind: str,
        source: str,
        expected_sha256: str | None = None,
    ) -> P115LocalDeleteAction | None:
        self.adapter_calls.append("plan_delete")
        state = self.path_state(relative_path)
        if state.kind == "absent":
            return None
        if kind != "file":
            raise AssertionError("FakeLibrary only models file deletion")
        if expected_sha256 is not None and state.sha256 != expected_sha256:
            raise RuntimeError("fixture cache ownership mismatch")
        return P115LocalDeleteAction(
            kind,
            relative_path,
            self.absolute_path(relative_path),
            source,
            state,
        )

    def exists(self, relative_path: str) -> bool:
        self.adapter_calls.append("exists")
        return relative_path in self.existing

    def strm_matches(self, relative_path: str, content: str) -> bool:
        self.adapter_calls.append("strm_matches")
        state = self.path_state(relative_path)
        encoded = content.encode()
        return state.kind == "file" and state.sha256 == hashlib.sha256(encoded).hexdigest()

    def write_strm(self, relative_path: str, content: str) -> P115LocalWriteResult:
        self.adapter_calls.append("write_strm")
        self.strm_writes.append((relative_path, content))
        encoded = content.encode()
        changed = self.contents.get(relative_path) != encoded
        self.existing.add(relative_path)
        if changed:
            self.contents[relative_path] = encoded
            self.inodes[relative_path] = len(self.inodes) + 1
            self.versions[relative_path] = self.versions.get(relative_path, 0) + 1
        return P115LocalWriteResult(changed, self.absolute_path(relative_path))

    def write_subtitle(self, relative_path: str, content: bytes) -> P115LocalWriteResult:
        self.adapter_calls.append("write_subtitle")
        self.subtitle_writes.append((relative_path, content))
        changed = relative_path not in self.existing
        if changed:
            self.existing.add(relative_path)
            self.contents[relative_path] = bytes(content)
            self.inodes[relative_path] = len(self.inodes) + 1
            self.versions[relative_path] = self.versions.get(relative_path, 0) + 1
        return P115LocalWriteResult(changed, self.absolute_path(relative_path))

    def reconcile(
        self,
        *,
        roots: Sequence[str],
        desired_paths: Sequence[str],
        cleanup: bool,
        allowed_deletions: Sequence[P115LocalDeleteAction],
    ) -> P115LocalReconcileResult:
        self.adapter_calls.append("reconcile")
        allowed_deleted_paths = tuple(action.path for action in allowed_deletions if action.kind == "file")
        allowed_directory_paths = tuple(action.path for action in allowed_deletions if action.kind == "directory")
        self.reconcile_calls.append(
            {
                "roots": tuple(roots),
                "desired_paths": tuple(desired_paths),
                "cleanup": cleanup,
                "allowed_deleted_paths": tuple(allowed_deleted_paths),
                "allowed_directory_paths": tuple(allowed_directory_paths),
            }
        )
        deleted = tuple(path for path in self.deleted if path in allowed_deleted_paths)
        for path in deleted:
            self.existing.discard(path)
            self.contents.pop(path, None)
        return P115LocalReconcileResult(deleted)

    def preview_reconcile(
        self,
        *,
        roots: Sequence[str],
        desired_paths: Sequence[str],
        cleanup: bool,
    ) -> P115LocalReconcileResult:
        self.adapter_calls.append("preview_reconcile")
        self.preview_calls.append({"roots": tuple(roots), "desired_paths": tuple(desired_paths), "cleanup": cleanup})
        planned = tuple(
            action
            for path in self.deleted
            if (action := self.plan_delete(path, kind="file", source="reconcile")) is not None
        )
        return P115LocalReconcileResult(self.deleted, planned_deletions=planned)

    def replace_strm(self, **_values: Any) -> tuple[int, int]:
        raise AssertionError("full sync must not execute bulk replacement")


class FakeLibraryGateway:
    def __init__(self, library: FakeLibrary | None) -> None:
        self.library = library
        self.calls: list[str] = []

    def open(self, root: str) -> FakeLibrary | None:
        self.calls.append(root)
        return self.library


class FakeRepository:
    def __init__(
        self,
        entries: Sequence[P115StrmCacheEntry] = (),
        *,
        recovery: P115StrmNotificationRecovery | None = None,
    ) -> None:
        frozen = tuple(entries)
        self.before = P115StrmCacheSnapshot(frozen, cache_snapshot_revision(frozen))
        self.snapshot_calls: list[tuple[tuple[P115StrmSyncRoot, ...], str]] = []
        self.stage_calls: list[dict[str, Any]] = []
        self.reconcile_calls: list[dict[str, Any]] = []
        self.retryable_calls: list[dict[str, Any]] = []
        self.recovery = recovery or P115StrmNotificationRecovery()

    def snapshot(
        self,
        roots: Sequence[P115StrmSyncRoot],
        *,
        account_id: str,
    ) -> P115StrmCacheSnapshot:
        self.snapshot_calls.append((tuple(roots), account_id))
        return self.before

    def recovery_snapshot(
        self,
        *,
        account_id: str,
        authorization_revision: str,
    ) -> P115StrmNotificationRecovery:
        assert account_id == "115001"
        assert len(authorization_revision) == 64
        return self.recovery

    def stage_plan(self, **values: Any) -> P115StrmSyncPlan:
        self.stage_calls.append(deepcopy(values))
        return P115StrmSyncPlan(
            plan_id=uuid4(),
            plan_key="a" * 64,
            plan=deepcopy(dict(values["plan"])),
            attempts=1,
        )

    def reconcile(
        self,
        roots: Sequence[P115StrmSyncRoot],
        entries: Sequence[P115StrmCacheEntry],
        **values: Any,
    ) -> P115StrmCacheReconcileResult:
        self.reconcile_calls.append({"roots": tuple(roots), "entries": tuple(entries), **values})
        return P115StrmCacheReconcileResult(len(entries), 0, ("effect-fixture",))

    def mark_plan_retryable(
        self,
        plan_id: UUID,
        *,
        local_effects_started: bool,
        error: Mapping[str, Any],
    ) -> None:
        self.retryable_calls.append(
            {
                "plan_id": plan_id,
                "local_effects_started": local_effects_started,
                "error": dict(error),
            }
        )


class FakeCoordinator:
    def __init__(self) -> None:
        self.scopes: list[str] = []

    @contextmanager
    def hold(self, scope: str) -> Iterator[None]:
        self.scopes.append(scope)
        yield


class FakeProgress:
    def __init__(self) -> None:
        self.updates: list[tuple[int, str]] = []
        self.checkpoints = 0

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        self.checkpoints += 1


class FakePersistentMediaInfoEnricher:
    def __init__(
        self,
        results: Mapping[str, P115StrmMediaInfoEnrichmentResult] | None = None,
    ) -> None:
        self.results = dict(results or {})
        self.calls: list[dict[str, Any]] = []
        self.persisted: list[tuple[str, P115MediaProbeStatus]] = []
        self.remote: FakeRemoteClient | None = None
        self.library: FakeLibrary | None = None
        self.repository: FakeRepository | None = None

    def bind(
        self,
        remote: FakeRemoteClient,
        library: FakeLibrary,
        repository: FakeRepository,
    ) -> None:
        self.remote = remote
        self.library = library
        self.repository = repository

    def enrich(
        self,
        request: P115MediaProbeRequest,
        *,
        operation_key: str,
        progress: Any,
    ) -> P115StrmMediaInfoEnrichmentResult:
        assert self.remote is not None
        assert self.library is not None
        assert self.repository is not None
        assert self.remote.list_calls
        last_cid, last_type, _limit, last_offset = self.remote.list_calls[-1]
        assert self.remote.pages[(last_cid, last_type, last_offset)]["data"] == []
        assert self.library.adapter_calls == []
        assert self.repository.stage_calls == []
        result = self.results.get(
            request.file_name,
            P115StrmMediaInfoEnrichmentResult(P115MediaProbeStatus.CONFIRMED),
        )
        self.persisted.append((operation_key, result.status))
        self.calls.append(
            {
                "request": request,
                "operation_key": operation_key,
                "progress": progress,
                "result": result,
            }
        )
        return result


def _settings(**values: Any) -> Settings:
    defaults: dict[str, Any] = {
        "local_strm_root": "/fixture/library",
        "etk_server_url": "https://etk.example.test/base/",
        "p115_download_subs": True,
        "p115_extensions": ["mkv", "srt"],
        "p115_min_video_size": 10,
        "emby_server_url": "https://emby.example.test",
        "emby_api_key": "fixture-token",
    }
    defaults.update(values)
    return Settings(**defaults)


def _pages(*, path_cid: str = "100") -> dict[tuple[str, int, int], Mapping[str, Any]]:
    path = [{"cid": path_cid, "name": "Movies"}]
    return {
        ("100", 4, 0): {
            "state": True,
            "_etk_authoritative_list_payload": True,
            "_etk_api_label_lower": "openapi",
            "count": 1,
            "path": path,
            "data": [
                {
                    "fid": "video-1",
                    "pid": "200",
                    "fn": "Movie Name.mkv",
                    "pc": "pick-video",
                    "sha1": "A" * 40,
                    "fs": 20 * 1024 * 1024,
                }
            ],
        },
        ("100", 4, 1): {
            "state": True,
            "_etk_authoritative_list_payload": True,
            "_etk_api_label_lower": "openapi",
            "count": 1,
            "path": path,
            "data": [],
        },
        ("100", 1, 0): {
            "state": True,
            "_etk_authoritative_list_payload": True,
            "_etk_api_label_lower": "openapi",
            "count": 1,
            "path": path,
            "data": [
                {
                    "fid": "subtitle-1",
                    "pid": "200",
                    "fn": "Movie Name.chs.srt",
                    "pc": "pick-subtitle",
                }
            ],
        },
        ("100", 1, 1): {
            "state": True,
            "_etk_authoritative_list_payload": True,
            "_etk_api_label_lower": "openapi",
            "count": 1,
            "path": path,
            "data": [],
        },
    }


def _two_video_pages() -> dict[tuple[str, int, int], Mapping[str, Any]]:
    pages = _pages()
    pages.pop(("100", 4, 1))
    pages[("100", 4, 0)] = {
        **pages[("100", 4, 0)],
        "count": 2,
        "data": [
            {
                "fid": "video-zulu",
                "pid": "200",
                "fn": "Zulu.mkv",
                "pc": "pick-zulu",
                "sha1": "B" * 40,
                "fs": 30 * 1024 * 1024,
            },
            {
                "fid": "video-alpha",
                "pid": "200",
                "fn": "Alpha.mkv",
                "pc": "pick-alpha",
                "sha1": "C" * 40,
                "fs": 40 * 1024 * 1024,
            },
        ],
    }
    pages[("100", 4, 2)] = {
        **pages[("100", 4, 0)],
        "data": [],
    }
    return pages


def _cache_entries(*, root_path: str = "Movies") -> tuple[P115StrmCacheEntry, ...]:
    content = "https://etk.example.test/base/api/p115/play/pick-video/Movie Name.mkv"
    return (
        P115StrmCacheEntry(
            id="200",
            parent_id="100",
            name="Feature",
            local_path=f"{root_path}/Feature",
            root_id="100",
            entry_kind="directory",
        ),
        P115StrmCacheEntry(
            id="video-1",
            parent_id="200",
            name="Movie Name.mkv",
            local_path=f"{root_path}/Feature/Movie Name.mkv",
            root_id="100",
            entry_kind="file",
            sha1="A" * 40,
            pick_code="pick-video",
            size=20 * 1024 * 1024,
            strm_content_sha256=hashlib.sha256(content.encode()).hexdigest(),
        ),
    )


def _service(
    *,
    pages: Mapping[tuple[str, int, int], Mapping[str, Any]] | None = None,
    library: FakeLibrary | None = None,
    repository: FakeRepository | None = None,
    rules: Sequence[Mapping[str, Any]] | None = None,
    settings: Settings | None = None,
    mediainfo: FakePersistentMediaInfoEnricher | None = None,
) -> tuple[P115StrmSyncService, FakeRemoteClient, FakeLibrary, FakeRepository, FakeCoordinator]:
    remote = FakeRemoteClient(
        pages or _pages(),
        {"200": ({"cid": "100", "name": "Movies"}, {"cid": "200", "name": "Feature"})},
    )
    local = library or FakeLibrary()
    cache = repository or FakeRepository()
    coordinator = FakeCoordinator()
    service = P115StrmSyncService(
        settings=lambda: settings or _settings(),
        credentials=FakeCredentials(),
        sorting_rules=FakeSortingRules(
            rules
            or (
                {
                    "cid": "100",
                    "enabled": True,
                    "category_path": "Movies",
                    "media_type": "movie",
                },
            )
        ),
        rename_config=FakeRenameConfig({"strm_url_fmt": "with_name"}),
        remote=FakeRemoteGateway(remote),
        libraries=FakeLibraryGateway(local),
        repository=cache,
        coordinator=coordinator,
        mediainfo=mediainfo,
    )
    if mediainfo is not None:
        mediainfo.bind(remote, local, cache)
    return service, remote, local, cache, coordinator


def test_full_sync_scans_every_page_before_exact_atomic_projection() -> None:
    service, remote, library, repository, coordinator = _service()
    progress = FakeProgress()

    outcome = service.execute(progress)

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert remote.list_calls == [
        ("100", 4, 1000, 0),
        ("100", 4, 1000, 1),
        ("100", 1, 1000, 0),
        ("100", 1, 1000, 1),
    ]
    assert remote.path_calls == ["200"]
    assert remote.download_calls[0][0] == "pick-subtitle"
    assert library.strm_writes == [
        (
            "Movies/Feature/Movie Name.strm",
            "https://etk.example.test/base/api/p115/play/pick-video/Movie Name.mkv",
        )
    ]
    assert library.subtitle_writes == [("Movies/Feature/Movie Name.chs.srt", b"subtitle:pick-subtitle")]
    assert len(repository.reconcile_calls) == 1
    call = repository.reconcile_calls[0]
    file_entry = next(entry for entry in call["entries"] if entry.entry_kind == "file")
    assert file_entry == P115StrmCacheEntry(
        id="video-1",
        parent_id="200",
        name="Movie Name.mkv",
        local_path="Movies/Feature/Movie Name.mkv",
        root_id="100",
        entry_kind="file",
        sha1="A" * 40,
        pick_code="pick-video",
        size=20 * 1024 * 1024,
        strm_content_sha256=file_entry.strm_content_sha256,
    )
    notifications = repository.stage_calls[0]["plan"]["notifications"]
    assert notifications["modified_paths"] == [
        "/fixture/library/Movies/Feature/Movie Name.chs.srt",
        "/fixture/library/Movies/Feature/Movie Name.strm",
    ]
    assert notifications["deleted_paths"] == []
    assert coordinator.scopes == ["p115-strm-sync:local"]
    assert progress.updates[-1][0] == 100
    assert outcome.details["mediainfo_deferred_to_p6_6"] is True
    assert outcome.details["watchdog_deferred_to_p6_7"] is True


def test_mediainfo_enrichment_persists_after_complete_scan_before_local_projection() -> None:
    first_enricher = FakePersistentMediaInfoEnricher()
    first_service, _remote, first_library, first_repository, _coordinator = _service(
        mediainfo=first_enricher,
    )

    first_outcome = first_service.execute(FakeProgress())

    assert first_outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert len(first_enricher.calls) == 1
    first_call = first_enricher.calls[0]
    assert first_call["request"] == P115MediaProbeRequest(
        account_id="115001",
        sha1="A" * 40,
        pick_code="pick-video",
        file_name="Movie Name.mkv",
        file_size=20 * 1024 * 1024,
    )
    assert len(first_call["operation_key"]) == 64
    assert first_enricher.persisted == [(first_call["operation_key"], P115MediaProbeStatus.CONFIRMED)]
    assert first_library.strm_writes
    assert first_repository.reconcile_calls
    assert first_outcome.details["mediainfo_deferred_to_p6_6"] is False
    assert first_outcome.details["mediainfo_enrichment_before_local_effects"] is True
    assert first_outcome.details["mediainfo_enrichment_targets"] == 1
    assert first_outcome.details["mediainfo_enrichment_confirmed"] == 1
    assert first_outcome.details["mediainfo_enrichment_failed"] == 0

    replay_enricher = FakePersistentMediaInfoEnricher(
        {
            "Movie Name.mkv": P115StrmMediaInfoEnrichmentResult(
                P115MediaProbeStatus.CONFIRMED,
                cache_reused=True,
            )
        }
    )
    replay_service, _remote, _library, _repository, _coordinator = _service(mediainfo=replay_enricher)

    replay_outcome = replay_service.execute(FakeProgress())

    assert replay_outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert replay_enricher.calls[0]["operation_key"] == first_call["operation_key"]
    assert replay_outcome.details["mediainfo_cache_reused"] == 1


def test_single_mediainfo_failure_is_partial_and_continues_in_provider_order() -> None:
    enricher = FakePersistentMediaInfoEnricher(
        {
            "Zulu.mkv": P115StrmMediaInfoEnrichmentResult(
                P115MediaProbeStatus.RETRYABLE,
                diagnostic="fixture probe timeout",
                provider_backend="openapi",
            )
        }
    )
    service, _remote, library, repository, _coordinator = _service(
        pages=_two_video_pages(),
        settings=_settings(p115_download_subs=False),
        mediainfo=enricher,
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert [call["request"].file_name for call in enricher.calls] == ["Zulu.mkv", "Alpha.mkv"]
    assert [status for _operation_key, status in enricher.persisted] == [
        P115MediaProbeStatus.RETRYABLE,
        P115MediaProbeStatus.CONFIRMED,
    ]
    assert [path for path, _content in library.strm_writes] == [
        "Movies/Feature/Zulu.strm",
        "Movies/Feature/Alpha.strm",
    ]
    assert len(repository.reconcile_calls) == 1
    assert outcome.details["mediainfo_enrichment_targets"] == 2
    assert outcome.details["mediainfo_enrichment_confirmed"] == 1
    assert outcome.details["mediainfo_enrichment_failed"] == 1
    assert outcome.details["mediainfo_failures"] == [
        {
            "file_id": "video-zulu",
            "sha1": "B" * 40,
            "status": "retryable",
            "diagnostic": "fixture probe timeout",
        }
    ]
    assert outcome.details["local_files_deleted"] == 0


def test_missing_emby_notification_configuration_fails_before_scan_or_local_projection() -> None:
    incomplete_settings = (
        _settings(emby_server_url="", emby_api_key="fixture-token"),
        _settings(emby_server_url="https://emby.example.test", emby_api_key=""),
    )
    for settings in incomplete_settings:
        service, remote, library, repository, _coordinator = _service(settings=settings)

        outcome = service.execute(FakeProgress())

        assert outcome.status is TaskOutcomeStatus.FAILED
        assert "Emby URL 或 API token 未配置" in outcome.message
        assert remote.list_calls == []
        assert library.strm_writes == []
        assert library.subtitle_writes == []
        assert library.preview_calls == []
        assert library.reconcile_calls == []
        assert repository.snapshot_calls == []
        assert repository.stage_calls == []
        assert repository.reconcile_calls == []


def test_missing_subtitle_is_a_persisted_modified_intent_even_when_strm_and_cache_match() -> None:
    strm_path = "Movies/Feature/Movie Name.strm"
    strm_content = "https://etk.example.test/base/api/p115/play/pick-video/Movie Name.mkv"
    library = FakeLibrary(existing=(strm_path,))
    library.contents[strm_path] = strm_content.encode()
    repository = FakeRepository(_cache_entries())
    service, _remote, _library, _repository, _coordinator = _service(
        library=library,
        repository=repository,
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert library.strm_writes == []
    assert library.subtitle_writes == [("Movies/Feature/Movie Name.chs.srt", b"subtitle:pick-subtitle")]
    plan = repository.stage_calls[0]["plan"]
    assert plan["notifications"]["modified_paths"] == ["/fixture/library/Movies/Feature/Movie Name.chs.srt"]
    required_paths = {action["absolute_path"] for action in plan["writes"] if action["write_required"]}
    assert required_paths <= set(plan["notifications"]["modified_paths"])


def test_write_precondition_drift_fails_before_an_unplanned_local_rewrite() -> None:
    strm_path = "Movies/Feature/Movie Name.strm"
    subtitle_path = "Movies/Feature/Movie Name.chs.srt"
    strm_content = "https://etk.example.test/base/api/p115/play/pick-video/Movie Name.mkv"
    library = FakeLibrary(existing=(strm_path, subtitle_path))
    library.contents[strm_path] = strm_content.encode()

    class DriftingRepository(FakeRepository):
        def stage_plan(self, **values: Any) -> P115StrmSyncPlan:
            plan = super().stage_plan(**values)
            library.existing.remove(strm_path)
            library.contents.pop(strm_path, None)
            return plan

    repository = DriftingRepository(_cache_entries())
    service, _remote, _library, _repository, _coordinator = _service(
        library=library,
        repository=repository,
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert "action plan 固定后发生变化" in outcome.message
    assert library.strm_writes == []
    assert library.subtitle_writes == []
    assert repository.reconcile_calls == []
    assert repository.retryable_calls[0]["local_effects_started"] is False


def test_wrong_provider_root_fails_before_any_local_or_database_effect() -> None:
    service, _remote, library, repository, _coordinator = _service(pages=_pages(path_cid="wrong"))

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert "路径不包含目标分类 CID" in outcome.message
    assert library.strm_writes == []
    assert library.subtitle_writes == []
    assert library.reconcile_calls == []
    assert repository.reconcile_calls == []


def test_incomplete_reported_count_fails_closed_before_projection() -> None:
    pages = _pages()
    pages[("100", 4, 1)] = {**pages[("100", 4, 1)], "count": 2}
    service, _remote, library, repository, _coordinator = _service(pages=pages)

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert library.strm_writes == []
    assert repository.reconcile_calls == []


def test_remote_path_traversal_is_rejected_before_projection() -> None:
    pages = _pages()
    video = dict(pages[("100", 4, 0)]["data"][0])  # type: ignore[index]
    video["fn"] = "../escape.mkv"
    pages[("100", 4, 0)] = {**pages[("100", 4, 0)], "data": [video]}
    service, _remote, library, repository, _coordinator = _service(pages=pages)

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert "不安全路径" in outcome.message
    assert library.strm_writes == []
    assert repository.reconcile_calls == []


def test_cache_projection_recovers_durable_deleted_notification_after_file_is_already_absent() -> None:
    stale = P115StrmCacheEntry(
        id="stale-video",
        parent_id="100",
        name="Stale.mkv",
        local_path="Movies/Stale.mkv",
        root_id="100",
        entry_kind="file",
        sha1="B" * 40,
        pick_code="stale-pick",
        size=30 * 1024 * 1024,
        strm_content_sha256="c" * 64,
    )
    repository = FakeRepository((stale,))
    service, _remote, _library, repository, _coordinator = _service(repository=repository)

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert repository.stage_calls[0]["plan"]["notifications"]["deleted_paths"] == ["/fixture/library/Movies/Stale.strm"]


def test_absent_logical_delete_is_rechecked_if_the_path_reappears_before_database_commit() -> None:
    stale = P115StrmCacheEntry(
        id="stale-video",
        parent_id="100",
        name="Stale.mkv",
        local_path="Movies/Stale.mkv",
        root_id="100",
        entry_kind="file",
        sha1="B" * 40,
        pick_code="stale-pick",
        size=30 * 1024 * 1024,
        strm_content_sha256="c" * 64,
    )
    library = FakeLibrary()

    class ReappearingPathRepository(FakeRepository):
        def stage_plan(self, **values: Any) -> P115StrmSyncPlan:
            plan = super().stage_plan(**values)
            path = "Movies/Stale.strm"
            library.existing.add(path)
            library.contents[path] = b"new file after the persisted absent projection"
            library.inodes[path] = 999
            library.versions[path] = 2
            return plan

    repository = ReappearingPathRepository((*_cache_entries(), stale))
    service, _remote, _library, _repository, _coordinator = _service(
        library=library,
        repository=repository,
        settings=_settings(p115_download_subs=False),
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert "路径已重新出现" in outcome.message
    assert repository.reconcile_calls == []
    assert "Movies/Stale.strm" in library.existing


def test_stale_cached_strm_uses_one_fresh_reconcile_delete_action_when_sources_overlap(
    tmp_path: Path,
) -> None:
    stale_path = tmp_path / "Movies/Stale.strm"
    stale_path.parent.mkdir(parents=True)
    stale_path.write_text("https://etk.example.test/api/p115/play/stale", encoding="utf-8")
    stale = P115StrmCacheEntry(
        id="stale-video",
        parent_id="100",
        name="Stale.mkv",
        local_path="Movies/Stale.mkv",
        root_id="100",
        entry_kind="file",
        sha1="B" * 40,
        pick_code="stale-pick",
        size=30 * 1024 * 1024,
        strm_content_sha256=hashlib.sha256(stale_path.read_bytes()).hexdigest(),
    )
    repository = FakeRepository((*_cache_entries(), stale))
    remote = FakeRemoteClient(
        _pages(),
        {"200": ({"cid": "100", "name": "Movies"}, {"cid": "200", "name": "Feature"})},
    )
    service = P115StrmSyncService(
        settings=lambda: _settings(local_strm_root=str(tmp_path), p115_download_subs=False),
        credentials=FakeCredentials(),
        sorting_rules=FakeSortingRules(({"cid": "100", "category_path": "Movies", "media_type": "movie"},)),
        rename_config=FakeRenameConfig({"strm_url_fmt": "with_name"}),
        remote=FakeRemoteGateway(remote),
        libraries=AtomicP115LocalStrmLibraryGateway(),
        repository=repository,
        coordinator=FakeCoordinator(),
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert not stale_path.exists()
    deletes = repository.stage_calls[0]["plan"]["deletes"]
    assert [(action["path"], action["source"]) for action in deletes] == [("Movies/Stale.strm", "reconcile")]


def test_home_video_key_scopes_remote_scan_to_home_video_rules() -> None:
    rules = (
        {"cid": "ignored", "category_path": "Movies", "media_type": "movie"},
        {"cid": "100", "category_path": "Home", "media_type": "home_video"},
    )
    service, remote, _library, _repository, _coordinator = _service(rules=rules)

    outcome = service.execute(FakeProgress(), home_video_only=True)

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert {call[0] for call in remote.list_calls} == {"100"}
    assert outcome.details["scope"] == "home_video"
    assert outcome.details["home_video_enrichment_deferred_to_p6_6"] is True
    assert "life_event_daemon_deferred_to_p6_8" not in outcome.details


def test_action_plan_is_staged_before_any_local_write() -> None:
    timeline: list[str] = []

    class OrderedRepository(FakeRepository):
        def stage_plan(self, **values: Any) -> P115StrmSyncPlan:
            timeline.append("plan-staged")
            return super().stage_plan(**values)

    class OrderedLibrary(FakeLibrary):
        def write_strm(self, relative_path: str, content: str) -> P115LocalWriteResult:
            assert timeline == ["plan-staged"]
            timeline.append("strm-written")
            return super().write_strm(relative_path, content)

        def write_subtitle(self, relative_path: str, content: bytes) -> P115LocalWriteResult:
            assert timeline == ["plan-staged", "strm-written"]
            timeline.append("subtitle-written")
            return super().write_subtitle(relative_path, content)

    repository = OrderedRepository()
    library = OrderedLibrary()
    service, _remote, _library, _repository, _coordinator = _service(
        repository=repository,
        library=library,
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert timeline == ["plan-staged", "strm-written", "subtitle-written"]


def test_database_finalize_failure_marks_staged_plan_retryable_after_local_effects() -> None:
    class FailingRepository(FakeRepository):
        def reconcile(
            self,
            roots: Sequence[P115StrmSyncRoot],
            entries: Sequence[P115StrmCacheEntry],
            **values: Any,
        ) -> P115StrmCacheReconcileResult:
            self.reconcile_calls.append({"roots": tuple(roots), "entries": tuple(entries), **values})
            raise RuntimeError("fixture transaction rollback")

    repository = FailingRepository()
    service, _remote, library, _repository, _coordinator = _service(repository=repository)

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert library.strm_writes
    assert library.subtitle_writes
    assert len(repository.retryable_calls) == 1
    assert repository.retryable_calls[0]["plan_id"] == repository.reconcile_calls[0]["plan_id"]
    assert repository.retryable_calls[0]["local_effects_started"] is True
    assert outcome.details["local_effects_may_have_been_applied"] is True


def test_new_operation_recovers_only_observed_local_effect_notifications_from_active_plan() -> None:
    class FailingRepository(FakeRepository):
        def reconcile(
            self,
            roots: Sequence[P115StrmSyncRoot],
            entries: Sequence[P115StrmCacheEntry],
            **values: Any,
        ) -> P115StrmCacheReconcileResult:
            self.reconcile_calls.append({"roots": tuple(roots), "entries": tuple(entries), **values})
            raise RuntimeError("fixture transaction rollback")

    cached = _cache_entries()
    library = FakeLibrary(
        existing=("Movies/Orphan.strm",),
        deleted=("Movies/Orphan.strm",),
    )
    first_repository = FailingRepository(cached)
    first_service, _remote, _library, _repository, _coordinator = _service(
        library=library,
        repository=first_repository,
        settings=_settings(p115_download_subs=False, p115_local_cleanup=False),
    )
    first_outcome = first_service.execute(FakeProgress())
    assert first_outcome.status is TaskOutcomeStatus.FAILED
    prior = P115StrmSyncPlan(
        plan_id=first_repository.stage_calls[0]["plan_id"]
        if "plan_id" in first_repository.stage_calls[0]
        else first_repository.retryable_calls[0]["plan_id"],
        plan_key="b" * 64,
        plan=deepcopy(first_repository.stage_calls[0]["plan"]),
        attempts=1,
    )
    second_repository = FakeRepository(
        cached,
        recovery=P115StrmNotificationRecovery(active_plans=(prior,)),
    )
    second_service, _remote, _library, _repository, _coordinator = _service(
        library=library,
        repository=second_repository,
        settings=_settings(p115_download_subs=False, p115_local_cleanup=True),
    )

    second_outcome = second_service.execute(FakeProgress())

    assert second_outcome.status is TaskOutcomeStatus.SUCCEEDED
    notifications = second_repository.stage_calls[0]["plan"]["notifications"]
    assert notifications["current_modified_paths"] == []
    assert notifications["current_deleted_paths"] == []
    assert notifications["recovered_modified_paths"] == ["/fixture/library/Movies/Feature/Movie Name.strm"]
    assert notifications["recovered_deleted_paths"] == ["/fixture/library/Movies/Orphan.strm"]


def test_current_deleted_notification_overrides_recovered_modified_semantics() -> None:
    relative_path = "Movies/Orphan.strm"
    absolute_path = f"/fixture/library/{relative_path}"
    library = FakeLibrary(existing=(relative_path,), deleted=(relative_path,))
    repository = FakeRepository(
        recovery=P115StrmNotificationRecovery(modified_paths=(absolute_path,)),
    )
    service, _remote, _library, _repository, _coordinator = _service(
        library=library,
        repository=repository,
        settings=_settings(p115_download_subs=False, p115_local_cleanup=True),
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    notifications = repository.stage_calls[0]["plan"]["notifications"]
    assert notifications["current_deleted_paths"] == [absolute_path]
    assert notifications["recovered_modified_paths"] == []
    assert absolute_path not in notifications["modified_paths"]
    assert notifications["deleted_paths"] == [absolute_path]


def test_current_modified_notification_overrides_recovered_deleted_semantics() -> None:
    relative_path = "Movies/Feature/Movie Name.strm"
    absolute_path = f"/fixture/library/{relative_path}"
    repository = FakeRepository(
        recovery=P115StrmNotificationRecovery(
            deleted_paths=(absolute_path,),
            deleted_path_roots=((absolute_path, "/fixture/library"),),
        ),
    )
    service, _remote, _library, _repository, _coordinator = _service(
        repository=repository,
        settings=_settings(p115_download_subs=False),
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    notifications = repository.stage_calls[0]["plan"]["notifications"]
    assert notifications["current_modified_paths"] == [absolute_path]
    assert notifications["recovered_deleted_paths"] == []
    assert notifications["modified_paths"] == [absolute_path]
    assert absolute_path not in notifications["deleted_paths"]


def test_recovered_deleted_notification_is_rechecked_after_local_actions() -> None:
    relative_path = "Movies/Reappeared.strm"
    absolute_path = f"/fixture/library/{relative_path}"
    library = FakeLibrary()

    class ReappearingRepository(FakeRepository):
        def stage_plan(self, **values: Any) -> P115StrmSyncPlan:
            plan = super().stage_plan(**values)
            library.existing.add(relative_path)
            library.contents[relative_path] = b"fixture-reappeared"
            return plan

    repository = ReappearingRepository(
        recovery=P115StrmNotificationRecovery(
            deleted_paths=(absolute_path,),
            deleted_path_roots=((absolute_path, "/fixture/library"),),
        ),
    )
    service, _remote, _library, _repository, _coordinator = _service(
        library=library,
        repository=repository,
        settings=_settings(p115_download_subs=False),
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert "待恢复 STRM Deleted 意图提交前路径已重新出现" in outcome.message
    assert repository.stage_calls[0]["plan"]["notifications"]["recovered_deleted_paths"] == [absolute_path]
    assert repository.reconcile_calls == []
    assert repository.retryable_calls[0]["local_effects_started"] is True


def test_recovered_deleted_notification_requires_complete_root_provenance() -> None:
    absolute_path = "/fixture/library/Movies/Missing.strm"
    repository = FakeRepository(
        recovery=P115StrmNotificationRecovery(deleted_paths=(absolute_path,)),
    )
    service, _remote, _library, _repository, _coordinator = _service(
        repository=repository,
        settings=_settings(p115_download_subs=False),
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert "缺少完整的本地库根目录映射" in outcome.message
    assert repository.stage_calls == []
    assert repository.reconcile_calls == []


def test_emby_authorization_drift_after_local_write_blocks_stale_effect_confirmation() -> None:
    initial = _settings(
        p115_download_subs=False,
        emby_server_url="https://emby.example.test",
        emby_api_key="fixture-token-a",
    )
    changed = _settings(
        p115_download_subs=False,
        emby_server_url="https://emby.example.test",
        emby_api_key="fixture-token-b",
    )

    class MutableSettings:
        current = initial

        def __call__(self) -> Settings:
            return self.current

    provider = MutableSettings()

    class AuthorizationDriftRepository(FakeRepository):
        def stage_plan(self, **values: Any) -> P115StrmSyncPlan:
            plan = super().stage_plan(**values)
            provider.current = changed
            return plan

    repository = AuthorizationDriftRepository()
    remote = FakeRemoteClient(
        _pages(),
        {"200": ({"cid": "100", "name": "Movies"}, {"cid": "200", "name": "Feature"})},
    )
    library = FakeLibrary()
    service = P115StrmSyncService(
        settings=provider,
        credentials=FakeCredentials(),
        sorting_rules=FakeSortingRules(({"cid": "100", "category_path": "Movies", "media_type": "movie"},)),
        rename_config=FakeRenameConfig({"strm_url_fmt": "with_name"}),
        remote=FakeRemoteGateway(remote),
        libraries=FakeLibraryGateway(library),
        repository=repository,
        coordinator=FakeCoordinator(),
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert "Emby 授权" in outcome.message
    assert library.strm_writes
    assert repository.reconcile_calls == []
    assert repository.retryable_calls[0]["local_effects_started"] is True


def test_subtitle_download_interruption_leaves_no_final_or_temporary_subtitle(
    tmp_path: Path,
) -> None:
    class InterruptedSubtitleClient(FakeRemoteClient):
        def download_subtitle(self, pick_code: str, *, max_bytes: int) -> bytes:
            self.download_calls.append((pick_code, max_bytes))
            raise RuntimeError("fixture subtitle stream interrupted")

    remote = InterruptedSubtitleClient(
        _pages(),
        {"200": ({"cid": "100", "name": "Movies"}, {"cid": "200", "name": "Feature"})},
    )
    repository = FakeRepository()
    service = P115StrmSyncService(
        settings=lambda: _settings(local_strm_root=str(tmp_path)),
        credentials=FakeCredentials(),
        sorting_rules=FakeSortingRules(({"cid": "100", "category_path": "Movies", "media_type": "movie"},)),
        rename_config=FakeRenameConfig({"strm_url_fmt": "with_name"}),
        remote=FakeRemoteGateway(remote),
        libraries=AtomicP115LocalStrmLibraryGateway(),
        repository=repository,
        coordinator=FakeCoordinator(),
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert (tmp_path / "Movies/Feature/Movie Name.strm").is_file()
    assert not (tmp_path / "Movies/Feature/Movie Name.chs.srt").exists()
    assert not list(tmp_path.rglob(".Movie Name.chs.srt.*"))
    assert repository.retryable_calls[0]["local_effects_started"] is True


def test_rule_and_provider_order_is_preserved_for_local_projection() -> None:
    pages: dict[tuple[str, int, int], Mapping[str, Any]] = {}
    for cid, name in (("200", "Second.mkv"), ("100", "First.mkv")):
        path = [{"cid": cid, "name": cid}]
        pages[(cid, 4, 0)] = {
            "state": True,
            "_etk_authoritative_list_payload": True,
            "_etk_api_label_lower": "openapi",
            "count": 1,
            "path": path,
            "data": [
                {
                    "fid": f"video-{cid}",
                    "pid": cid,
                    "fn": name,
                    "pc": f"pick-{cid}",
                    "sha1": "A" * 40,
                    "fs": 20 * 1024 * 1024,
                }
            ],
        }
        pages[(cid, 4, 1)] = {
            "state": True,
            "_etk_authoritative_list_payload": True,
            "_etk_api_label_lower": "openapi",
            "count": 1,
            "path": path,
            "data": [],
        }
        pages[(cid, 1, 0)] = {
            "state": True,
            "_etk_authoritative_list_payload": True,
            "_etk_api_label_lower": "openapi",
            "count": 0,
            "path": path,
            "data": [],
        }
    service, remote, library, _repository, _coordinator = _service(
        pages=pages,
        rules=(
            {"cid": "200", "category_path": "Zeta", "media_type": "movie"},
            {"cid": "100", "category_path": "Alpha", "media_type": "movie"},
        ),
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert [(cid, file_type, offset) for cid, file_type, _limit, offset in remote.list_calls] == [
        ("200", 4, 0),
        ("200", 4, 1),
        ("200", 1, 0),
        ("100", 4, 0),
        ("100", 4, 1),
        ("100", 1, 0),
    ]
    assert [path for path, _content in library.strm_writes] == [
        "Zeta/Second.strm",
        "Alpha/First.strm",
    ]


def test_with_name_preserves_remote_filename_whitespace_without_url_encoding() -> None:
    pages = _pages()
    video = dict(pages[("100", 4, 0)]["data"][0])  # type: ignore[index]
    video["fn"] = " Movie Name.mkv "
    pages[("100", 4, 0)] = {**pages[("100", 4, 0)], "data": [video]}
    service, _remote, library, _repository, _coordinator = _service(
        pages=pages,
        settings=_settings(p115_download_subs=False),
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert library.strm_writes == [
        (
            "Movies/Feature/ Movie Name.strm",
            "https://etk.example.test/base/api/p115/play/pick-video/ Movie Name.mkv ",
        )
    ]


def test_same_cid_root_path_remap_deletes_the_audited_old_strm_and_notifies_both_paths(
    tmp_path: Path,
) -> None:
    old_path = tmp_path / "Movies/Feature/Movie Name.strm"
    old_path.parent.mkdir(parents=True)
    content = "https://etk.example.test/base/api/p115/play/pick-video/Movie Name.mkv"
    old_path.write_text(content, encoding="utf-8")
    repository = FakeRepository(_cache_entries(root_path="Movies"))
    remote = FakeRemoteClient(
        _pages(),
        {"200": ({"cid": "100", "name": "Movies"}, {"cid": "200", "name": "Feature"})},
    )
    service = P115StrmSyncService(
        settings=lambda: _settings(
            local_strm_root=str(tmp_path),
            p115_download_subs=False,
        ),
        credentials=FakeCredentials(),
        sorting_rules=FakeSortingRules(({"cid": "100", "category_path": "Films", "media_type": "movie"},)),
        rename_config=FakeRenameConfig({"strm_url_fmt": "with_name"}),
        remote=FakeRemoteGateway(remote),
        libraries=AtomicP115LocalStrmLibraryGateway(),
        repository=repository,
        coordinator=FakeCoordinator(),
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert not old_path.exists()
    assert (tmp_path / "Films/Feature/Movie Name.strm").read_text(encoding="utf-8") == content
    plan = repository.stage_calls[0]["plan"]
    assert plan["managed_roots"] == ["Films", "Movies"]
    assert plan["notifications"]["modified_paths"] == [str(tmp_path / "Films/Feature/Movie Name.strm")]
    assert plan["notifications"]["deleted_paths"] == [str(old_path)]
    assert plan["deletes"][0]["path"] == "Movies/Feature/Movie Name.strm"
    assert plan["deletes"][0]["expected"]["sha256"] == hashlib.sha256(content.encode()).hexdigest()


def test_changed_root_remap_delete_identity_defers_and_keeps_cache_unconfirmed(
    tmp_path: Path,
) -> None:
    old_path = tmp_path / "Movies/Feature/Movie Name.strm"
    old_path.parent.mkdir(parents=True)
    old_path.write_text(
        "https://etk.example.test/base/api/p115/play/pick-video/Movie Name.mkv",
        encoding="utf-8",
    )

    class DeleteIdentityDriftRepository(FakeRepository):
        def stage_plan(self, **values: Any) -> P115StrmSyncPlan:
            plan = super().stage_plan(**values)
            old_path.unlink()
            old_path.write_text("replacement after persisted preview", encoding="utf-8")
            return plan

    repository = DeleteIdentityDriftRepository(_cache_entries(root_path="Movies"))
    remote = FakeRemoteClient(
        _pages(),
        {"200": ({"cid": "100", "name": "Movies"}, {"cid": "200", "name": "Feature"})},
    )
    service = P115StrmSyncService(
        settings=lambda: _settings(
            local_strm_root=str(tmp_path),
            p115_download_subs=False,
        ),
        credentials=FakeCredentials(),
        sorting_rules=FakeSortingRules(({"cid": "100", "category_path": "Films", "media_type": "movie"},)),
        rename_config=FakeRenameConfig({"strm_url_fmt": "with_name"}),
        remote=FakeRemoteGateway(remote),
        libraries=AtomicP115LocalStrmLibraryGateway(),
        repository=repository,
        coordinator=FakeCoordinator(),
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert "重新规划" in outcome.message
    assert old_path.read_text(encoding="utf-8") == "replacement after persisted preview"
    assert repository.reconcile_calls == []
    assert repository.retryable_calls[0]["local_effects_started"] is True


def test_root_remap_fails_before_projection_when_old_path_no_longer_matches_cached_ownership(
    tmp_path: Path,
) -> None:
    old_path = tmp_path / "Movies/Feature/Movie Name.strm"
    old_path.parent.mkdir(parents=True)
    old_path.write_text("user replacement outside cached ownership", encoding="utf-8")
    repository = FakeRepository(_cache_entries(root_path="Movies"))
    remote = FakeRemoteClient(
        _pages(),
        {"200": ({"cid": "100", "name": "Movies"}, {"cid": "200", "name": "Feature"})},
    )
    service = P115StrmSyncService(
        settings=lambda: _settings(
            local_strm_root=str(tmp_path),
            p115_download_subs=False,
        ),
        credentials=FakeCredentials(),
        sorting_rules=FakeSortingRules(({"cid": "100", "category_path": "Films", "media_type": "movie"},)),
        rename_config=FakeRenameConfig({"strm_url_fmt": "with_name"}),
        remote=FakeRemoteGateway(remote),
        libraries=AtomicP115LocalStrmLibraryGateway(),
        repository=repository,
        coordinator=FakeCoordinator(),
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert "ownership 摘要不匹配" in outcome.message
    assert repository.stage_calls == []
    assert not (tmp_path / "Films").exists()
    assert old_path.read_text(encoding="utf-8") == "user replacement outside cached ownership"


def test_root_remap_fails_closed_when_legacy_cache_has_no_strm_ownership_digest(
    tmp_path: Path,
) -> None:
    old_path = tmp_path / "Movies/Feature/Movie Name.strm"
    old_path.parent.mkdir(parents=True)
    old_path.write_text(
        "https://etk.example.test/base/api/p115/play/pick-video/Movie Name.mkv",
        encoding="utf-8",
    )
    legacy_entries = tuple(
        P115StrmCacheEntry(
            id=entry.id,
            parent_id=entry.parent_id,
            name=entry.name,
            local_path=entry.local_path,
            root_id=entry.root_id,
            entry_kind=entry.entry_kind,
            sha1=entry.sha1,
            pick_code=entry.pick_code,
            size=entry.size,
            strm_content_sha256=None if entry.entry_kind == "file" else entry.strm_content_sha256,
        )
        for entry in _cache_entries(root_path="Movies")
    )
    repository = FakeRepository(legacy_entries)
    remote = FakeRemoteClient(
        _pages(),
        {"200": ({"cid": "100", "name": "Movies"}, {"cid": "200", "name": "Feature"})},
    )
    service = P115StrmSyncService(
        settings=lambda: _settings(local_strm_root=str(tmp_path), p115_download_subs=False),
        credentials=FakeCredentials(),
        sorting_rules=FakeSortingRules(({"cid": "100", "category_path": "Films", "media_type": "movie"},)),
        rename_config=FakeRenameConfig({"strm_url_fmt": "with_name"}),
        remote=FakeRemoteGateway(remote),
        libraries=AtomicP115LocalStrmLibraryGateway(),
        repository=repository,
        coordinator=FakeCoordinator(),
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert "ownership" in outcome.message
    assert repository.stage_calls == []
    assert old_path.exists()
    assert not (tmp_path / "Films").exists()


def test_empty_authoritative_manifest_suppresses_orphan_cleanup_but_removes_stale_strm(
    tmp_path: Path,
) -> None:
    movies = tmp_path / "Movies"
    movies.mkdir()
    stale = movies / "Stale.strm"
    stale.write_text("https://etk.example.test/api/p115/play/stale", encoding="utf-8")
    orphan = movies / "orphan.bin"
    orphan.write_bytes(b"orphan")
    path = [{"cid": "100", "name": "Movies"}]
    empty_page = {
        "state": True,
        "_etk_authoritative_list_payload": True,
        "_etk_api_label_lower": "openapi",
        "count": 0,
        "path": path,
        "data": [],
    }
    remote = FakeRemoteClient(
        {
            ("100", 4, 0): empty_page,
            ("100", 1, 0): empty_page,
        },
        {},
    )
    service = P115StrmSyncService(
        settings=lambda: _settings(local_strm_root=str(tmp_path), p115_local_cleanup=True),
        credentials=FakeCredentials(),
        sorting_rules=FakeSortingRules(({"cid": "100", "category_path": "Movies", "media_type": "movie"},)),
        rename_config=FakeRenameConfig(),
        remote=FakeRemoteGateway(remote),
        libraries=AtomicP115LocalStrmLibraryGateway(),
        repository=FakeRepository(),
        coordinator=FakeCoordinator(),
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert not stale.exists()
    assert orphan.exists()
    assert outcome.details["local_cleanup_suppressed_empty_manifest"] is True
