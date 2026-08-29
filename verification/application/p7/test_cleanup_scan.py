from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast
from uuid import UUID

import httpx
import pytest
from pydantic import SecretStr

from etk.domains.identity.user_sync import UserSyncService
from etk.domains.media.cleanup_projection import _cleanup_scan_row_visible
from etk.domains.subscription.cleanup_adapters import (
    CleanupEmbyInventoryCredentialChanged,
    CleanupEmbyInventoryUnavailable,
    HttpxCleanupEmbyInventoryGateway,
)
from etk.domains.subscription.cleanup_scan import (
    CleanupScanEntry,
    CleanupScanItem,
    CleanupScanPolicy,
    CleanupScanReconcileResult,
    CleanupScanResult,
    CleanupScanService,
    CleanupScanSnapshot,
    effective_cleanup_scan_policy,
    evaluate_cleanup_scan_item,
)
from etk.platform import Settings
from etk.platform.jobs import PersistentExecutionCancelled
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcomeStatus
from etk.processes.p8_effect_tasks import P8EffectTaskRouter


class FakeProgress:
    def __init__(self, *, cancel_at_checkpoint: int | None = None) -> None:
        self.checkpoints = 0
        self.updates: list[tuple[int, str]] = []
        self.effects: list[tuple[str, str]] = []
        self.cancel_at_checkpoint = cancel_at_checkpoint

    def checkpoint(self) -> None:
        self.checkpoints += 1
        if self.checkpoints == self.cancel_at_checkpoint:
            raise PersistentExecutionCancelled("fixture cancellation")

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def success_reported(self) -> bool:
        return bool(self.updates and self.updates[-1][0] == 100)

    def record_started_effect(self, label: str) -> None:
        self.effects.append(("started", label))

    def record_committed_effect(self, label: str) -> None:
        self.effects.append(("committed", label))

    def record_abandoned_effect(self, label: str) -> None:
        self.effects.append(("abandoned", label))


class FakeRepository:
    def __init__(self, snapshot: CleanupScanSnapshot) -> None:
        self.snapshot = snapshot
        self.users: list[str | None] = []
        self.reconciliations: list[tuple[CleanupScanSnapshot, tuple[CleanupScanEntry, ...]]] = []
        self.job_ids: list[UUID | None] = []

    def load_snapshot(self, *, user_id: str | None) -> CleanupScanSnapshot:
        self.users.append(user_id)
        return self.snapshot

    def reconcile(
        self,
        snapshot: CleanupScanSnapshot,
        entries: Sequence[CleanupScanEntry],
        *,
        job_id: UUID | None,
        cancellation_check: Any,
    ) -> CleanupScanReconcileResult:
        self.job_ids.append(job_id)
        cancellation_check()
        self.reconciliations.append((snapshot, tuple(entries)))
        cancellation_check()
        return CleanupScanReconcileResult(deleted_pending=2, upserted_items=len(entries))


class FakeInventory:
    def __init__(self, existing: Sequence[str], error: Exception | None = None) -> None:
        self.existing = frozenset(existing)
        self.error = error
        self.calls: list[tuple[str, ...]] = []

    def existing_item_ids(self, item_ids: Sequence[str]) -> frozenset[str]:
        self.calls.append(tuple(item_ids))
        if self.error is not None:
            raise self.error
        return self.existing


class FakeCompatibility:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def execute(self, task_key: str, processor: object, parameters: Mapping[str, Any]) -> object:
        del processor, parameters
        self.calls.append(task_key)
        return True


class FakeUserSync:
    def execute(self, progress: object) -> object:
        del progress
        raise AssertionError("cleanup scan must not call user sync")


class FakeCleanupScan:
    def __init__(self) -> None:
        self.calls: list[tuple[object, object]] = []

    def execute(
        self,
        progress: object,
        *,
        requested_by_user_id: object = None,
        execution_job_id: object = None,
    ) -> CleanupScanResult:
        del progress
        self.calls.append((requested_by_user_id, execution_job_id))
        return CleanupScanResult("fixture cleanup scan", {"indexed_items": 3})


def _policy(
    *,
    rules: Sequence[Mapping[str, Any]] | None = None,
    keep_one_per_resolution: bool = False,
) -> CleanupScanPolicy:
    return CleanupScanPolicy(
        rules=tuple(rules or ({"id": "resolution", "enabled": True, "priority": ["4k", "1080p"]},)),
        library_ids=(),
        keep_one_per_resolution=keep_one_per_resolution,
        rating_mapping={},
    )


def _item(
    *versions: Mapping[str, Any],
    original_language: str = "en",
    countries: object = (),
) -> CleanupScanItem:
    return CleanupScanItem(
        tmdb_id="fixture-movie",
        item_type="Movie",
        title="Fixture Movie",
        original_language=original_language,
        countries=countries,
        asset_details=tuple(versions),
    )


def _version(
    item_id: str,
    *,
    resolution: str,
    subtitles: Sequence[str] = (),
    size: int = 100,
) -> Mapping[str, Any]:
    return {
        "emby_item_id": item_id,
        "path": f"/fixture/{item_id}.mkv",
        "resolution_display": resolution,
        "quality_display": "WEB-DL",
        "effect_display": "SDR",
        "codec_display": "HEVC",
        "size_bytes": size,
        "video_bitrate_mbps": 10,
        "bit_depth": 10,
        "frame_rate": 24,
        "runtime_minutes": 100,
        "date_added_to_library": f"2026-01-{int(item_id[-1]) + 1:02d}",
        "subtitle_languages_raw": list(subtitles),
    }


def _snapshot(*items: CleanupScanItem, visible: bool = True) -> CleanupScanSnapshot:
    return CleanupScanSnapshot(
        user_id="fixture-user",
        policy=_policy(),
        visible_movie_ids=("fixture-movie",) if visible else (),
        visible_series_ids=(),
        items=tuple(items),
        source_fingerprint="a" * 64,
    )


def test_cleanup_scan_policy_preserves_canonical_and_legacy_fallback_order() -> None:
    legacy = effective_cleanup_scan_policy(
        {
            "media_cleanup_config": {"rules": [], "library_ids": [], "keep_one_per_res": None},
            "media_cleanup_rules": [{"id": "filesize", "enabled": True, "priority": "asc"}],
            "media_cleanup_library_ids": ["legacy-library", "legacy-library"],
            "media_cleanup_keep_one_per_res": True,
        }
    )
    canonical = effective_cleanup_scan_policy(
        {
            "media_cleanup_config": {
                "rules": [{"id": "runtime", "enabled": True}],
                "library_ids": ["canonical-library"],
                "keep_one_per_res": False,
            }
        }
    )

    assert legacy.rules == ({"id": "filesize", "enabled": True, "priority": "asc"},)
    assert legacy.library_ids == ("legacy-library",)
    assert legacy.keep_one_per_resolution is True
    assert canonical.rules == ({"id": "runtime", "enabled": True},)
    assert canonical.library_ids == ("canonical-library",)
    assert canonical.keep_one_per_resolution is False


@pytest.mark.parametrize("value", [[], "", 0, False])
def test_cleanup_scan_policy_rejects_falsy_non_object_canonical_config(value: object) -> None:
    with pytest.raises(TypeError, match="media_cleanup_config"):
        effective_cleanup_scan_policy({"media_cleanup_config": value})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("EnabledFolders", "library-visible"),
        ("ExcludedSubFolders", {"folder": "library-hidden"}),
        ("BlockedTags", ["blocked", {"unexpected": True}]),
        ("BlockUnratedItems", ["Movie", ["Episode"]]),
    ],
)
def test_cleanup_scan_user_policy_rejects_malformed_permission_arrays(field: str, value: object) -> None:
    policy: dict[str, Any] = {
        "EnableAllFolders": True,
        "EnabledFolders": [],
        "ExcludedSubFolders": [],
        "BlockedTags": [],
        "BlockUnratedItems": [],
    }
    policy[field] = value
    row = {
        "item_type": "Movie",
        "asset_details_json": [{"source_library_id": "library-visible", "ancestor_ids": ["library-visible"]}],
        "tags_json": [],
        "official_rating_json": {"US": "PG-13"},
    }

    with pytest.raises(TypeError, match=field):
        _cleanup_scan_row_visible(row, user_policy=policy, library_ids=(), rating_mapping={})


def test_cleanup_scan_user_policy_preserves_json_scalar_text_coercion() -> None:
    row = {
        "item_type": "Movie",
        "asset_details_json": [{"source_library_id": "42", "ancestor_ids": ["42"]}],
        "tags_json": ["42"],
        "official_rating_json": {"US": "PG-13"},
    }
    policy = {
        "EnableAllFolders": False,
        "EnabledFolders": [42],
        "ExcludedSubFolders": None,
        "BlockedTags": [],
        "BlockUnratedItems": [],
    }

    assert _cleanup_scan_row_visible(row, user_policy=policy, library_ids=(), rating_mapping={}) is True
    policy["BlockedTags"] = [42]
    assert _cleanup_scan_row_visible(row, user_policy=policy, library_ids=(), rating_mapping={}) is False


def test_cleanup_evaluator_uses_authoritative_ids_and_legacy_subtitle_rule() -> None:
    item = _item(
        _version("emby-1", resolution="1080p", subtitles=[]),
        _version("emby-2", resolution="1080p", subtitles=["chi"]),
        _version("emby-3", resolution="4k", subtitles=["chi"]),
    )
    policy = _policy(rules=({"id": "subtitle", "enabled": True, "priority": "desc"},))

    entry = evaluate_cleanup_scan_item(
        item,
        existing_item_ids=frozenset({"emby-1", "emby-2"}),
        policy=policy,
    )

    assert entry is not None
    assert entry.best_version == "emby-2"
    assert [version["id"] for version in entry.versions] == ["emby-1", "emby-2"]
    assert set(entry.versions[0]) == {
        "id",
        "path",
        "filesize",
        "quality",
        "resolution",
        "effect",
        "video_bitrate_mbps",
        "bit_depth",
        "frame_rate",
        "runtime_minutes",
        "codec",
        "subtitle_count",
        "subtitle_languages",
    }


def test_cleanup_evaluator_preserves_chinese_subtitle_bypass_and_resolution_slots() -> None:
    chinese = _item(
        _version("emby-1", resolution="1080p", subtitles=[]),
        _version("emby-2", resolution="1080p", subtitles=["chi"]),
        original_language="zh",
        countries=["CN"],
    )
    subtitle_only = _policy(rules=({"id": "subtitle", "enabled": True},))
    per_resolution = _policy(
        rules=({"id": "filesize", "enabled": True, "priority": "desc"},),
        keep_one_per_resolution=True,
    )
    grouped = _item(
        _version("emby-1", resolution="4k", size=400),
        _version("emby-2", resolution="1080p", size=200),
        _version("emby-3", resolution="1080p", size=300),
    )

    chinese_entry = evaluate_cleanup_scan_item(
        chinese,
        existing_item_ids=frozenset({"emby-1", "emby-2"}),
        policy=subtitle_only,
    )
    grouped_entry = evaluate_cleanup_scan_item(
        grouped,
        existing_item_ids=frozenset({"emby-1", "emby-2", "emby-3"}),
        policy=per_resolution,
    )

    assert chinese_entry is not None
    assert chinese_entry.best_version == "emby-1"
    assert grouped_entry is not None
    assert grouped_entry.best_version == ("emby-1", "emby-3")


def test_cleanup_scan_service_reconciles_only_after_authoritative_read_and_checkpoint() -> None:
    item = _item(
        _version("emby-1", resolution="1080p"),
        _version("emby-2", resolution="4k"),
    )
    repository = FakeRepository(_snapshot(item))
    inventory = FakeInventory(["emby-1", "emby-2"])
    progress = FakeProgress()
    service = CleanupScanService(
        repository,
        inventory,
        lambda: Settings(emby_user_id="configured-user"),
    )

    result = service.execute(
        progress,
        requested_by_user_id="request-user",
        execution_job_id="11111111-1111-1111-1111-111111111111",
    )

    assert repository.users == ["request-user"]
    assert inventory.calls == [("emby-1", "emby-2")]
    assert len(repository.reconciliations) == 1
    assert repository.job_ids == [UUID("11111111-1111-1111-1111-111111111111")]
    assert repository.reconciliations[0][1][0].best_version == "emby-2"
    assert result.stats == {
        "requested_by_user_id": "request-user",
        "visible_movies": 1,
        "visible_series": 0,
        "scanned_items": 1,
        "checked_emby_ids": 2,
        "missing_emby_ids": 0,
        "indexed_items": 1,
        "deleted_pending": 2,
        "upserted_items": 1,
        "authoritative_emby_read": True,
        "atomic_reconcile": True,
    }
    assert progress.checkpoints >= 4
    assert progress.effects == [
        ("started", "cleanup-index-publication"),
        ("committed", "cleanup-index-publication"),
    ]
    assert progress.updates[-1] == (100, "扫描完成！共发现 1 组需要清理的多版本媒体。")


def test_cleanup_scan_abandons_publication_when_repository_checkpoint_observes_cancellation() -> None:
    item = _item(
        _version("emby-1", resolution="1080p"),
        _version("emby-2", resolution="4k"),
    )
    repository = FakeRepository(_snapshot(item))
    progress = FakeProgress(cancel_at_checkpoint=6)
    service = CleanupScanService(
        repository,
        FakeInventory(["emby-1", "emby-2"]),
        lambda: Settings(emby_user_id="configured-user"),
    )

    with pytest.raises(PersistentExecutionCancelled, match="fixture cancellation"):
        service.execute(progress)

    assert progress.effects == [
        ("started", "cleanup-index-publication"),
        ("abandoned", "cleanup-index-publication"),
    ]


def test_cleanup_scan_preserves_pending_index_when_scope_or_emby_read_is_unavailable() -> None:
    empty_repository = FakeRepository(_snapshot(visible=False))
    empty_inventory = FakeInventory([])
    service = CleanupScanService(
        empty_repository,
        empty_inventory,
        lambda: Settings(emby_user_id="configured-user"),
    )

    result = service.execute(FakeProgress())

    assert result.message == "扫描中止：当前用户视角下没有可见的媒体项。"
    assert empty_inventory.calls == []
    assert empty_repository.reconciliations == []

    item = _item(
        _version("emby-1", resolution="1080p"),
        _version("emby-2", resolution="4k"),
    )
    failed_repository = FakeRepository(_snapshot(item))
    failed_service = CleanupScanService(
        failed_repository,
        FakeInventory([], CleanupEmbyInventoryUnavailable("fixture unavailable")),
        lambda: Settings(emby_user_id="configured-user"),
    )
    with pytest.raises(CleanupEmbyInventoryUnavailable):
        failed_service.execute(FakeProgress())
    assert failed_repository.reconciliations == []


def test_cleanup_emby_inventory_is_batched_authoritative_and_fail_closed() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        ids = request.url.params["Ids"]
        payload = {"Items": [{"Id": ids}]} if ids == "emby-1" else {"Items": []}
        return httpx.Response(200, json=payload)

    settings = Settings(
        emby_server_url="http://emby.fixture.invalid",
        emby_api_key=SecretStr("fixture-token"),
        emby_api_timeout=3,
    )
    gateway = HttpxCleanupEmbyInventoryGateway(
        lambda: settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
        batch_size=1,
    )

    existing = gateway.existing_item_ids(["emby-1", "emby-2"])

    assert existing == frozenset({"emby-1"})
    assert [request.url.params["Ids"] for request in requests] == ["emby-1", "emby-2"]
    assert all(request.headers["X-Emby-Token"] == "fixture-token" for request in requests)

    malformed = HttpxCleanupEmbyInventoryGateway(
        lambda: settings,
        client_factory=lambda: httpx.Client(
            transport=httpx.MockTransport(lambda _request: httpx.Response(200, json={"Items": {}}))
        ),
    )
    unavailable = HttpxCleanupEmbyInventoryGateway(
        lambda: settings,
        client_factory=lambda: httpx.Client(
            transport=httpx.MockTransport(lambda _request: httpx.Response(503, json={"error": "down"}))
        ),
    )
    with pytest.raises(CleanupEmbyInventoryUnavailable, match="invalid response"):
        malformed.existing_item_ids(["emby-1"])
    with pytest.raises(CleanupEmbyInventoryUnavailable, match="request failed"):
        unavailable.existing_item_ids(["emby-1"])


def test_cleanup_emby_inventory_rejects_authorization_drift() -> None:
    current = [
        Settings(
            emby_server_url="http://emby.fixture.invalid",
            emby_api_key=SecretStr("fixture-token-a"),
        )
    ]

    def handler(_request: httpx.Request) -> httpx.Response:
        current[0] = Settings(
            emby_server_url="http://emby.fixture.invalid",
            emby_api_key=SecretStr("fixture-token-b"),
        )
        return httpx.Response(200, json={"Items": [{"Id": "emby-1"}]})

    gateway = HttpxCleanupEmbyInventoryGateway(
        lambda: current[0],
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )

    with pytest.raises(CleanupEmbyInventoryCredentialChanged):
        gateway.existing_item_ids(["emby-1"])


def test_p8_router_executes_cleanup_scan_without_processor_or_compatibility() -> None:
    compatibility = FakeCompatibility()
    cleanup_scan = FakeCleanupScan()
    router = P8EffectTaskRouter(
        compatibility,
        cast(UserSyncService, FakeUserSync()),
        cleanup_scan,
    )
    message = TaskMessage.create(
        task_key="scan-cleanup-issues",
        processor=ProcessorIdentity.MEDIA,
        metadata={
            "requested_by_user_id": "fixture-admin",
            "job_id": "22222222-2222-2222-2222-222222222222",
        },
    )

    outcome = router.execute(message, None, FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["indexed_items"] == 3
    assert outcome.details["compatibility_fallback"] is False
    assert cleanup_scan.calls == [("fixture-admin", "22222222-2222-2222-2222-222222222222")]
    assert compatibility.calls == []
