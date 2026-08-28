from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.operations.jobs import JobSubmissionService, SubmittedJob, TaskBusyError
from etk.domains.p115.router import router
from etk.domains.p115.washing_evaluator import (
    WashingMediaItem,
    WashingVersionSource,
    build_washing_policy_snapshot,
    clean_version_result,
    evaluate_washing_item,
    load_washing_priorities,
    match_washing_priority,
    normalize_washing_info,
    resolve_washing_version_slot,
    safe_parse_jsonish,
)
from etk.domains.p115.washing_priority import P115WashingPriorityService
from etk.domains.p115.washing_recalculation import (
    P115WashingPriorityRecalculationService,
    normalize_washing_item_types,
    normalize_washing_limit,
)
from etk.domains.p115.washing_recalculation_adapters import HttpxP115EpisodeRuntimeGateway
from etk.platform import AppContainer, Settings
from etk.platform.jobs import IdempotencyConflictError
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcomeStatus
from etk.platform.web import CookieSessionMiddleware
from etk.processes.p7_subscription_tasks import P7SubscriptionTaskRouter

NOW = datetime(2026, 8, 29, 8, 30, tzinfo=UTC)
REPO_ROOT = Path(__file__).resolve().parents[3]


class FakePermissions:
    def is_admin(self, user_id: str) -> bool:
        return user_id == "fixture-admin"


class FakeWashingState:
    def __init__(self) -> None:
        self.pending = {
            "revision": 7,
            "media_types": {"Movie": {"all": False, "target_cids": ["100"]}},
        }
        self.acknowledgements: list[tuple[int, object]] = []
        self.load_calls = 0
        self.load_error: Exception | None = None

    def list_groups(self) -> list[dict[str, Any]]:
        return []

    def replace_groups(self, groups: Sequence[object]) -> tuple[dict[str, Any], dict[str, Any]]:
        del groups
        return {}, {}

    def save_config(self, value: Mapping[str, Any]) -> dict[str, Any]:
        del value
        return {}

    def load_recalculate_state(self) -> dict[str, Any]:
        self.load_calls += 1
        if self.load_error is not None:
            raise self.load_error
        return deepcopy(self.pending)

    def acknowledge_recalculate_scope(self, expected_revision: int, processed_scope: object) -> bool:
        self.acknowledgements.append((expected_revision, deepcopy(processed_scope)))
        return expected_revision == self.pending["revision"]


class FakeSettingsStore:
    def load_many(self, keys: Sequence[str]) -> dict[str, Any]:
        del keys
        return {}

    def save(self, key: str, value: Any) -> None:
        del key, value
        return None


class RecordingJobs(JobSubmissionService):
    def __init__(self) -> None:
        self.calls: list[tuple[str, Mapping[str, Any], dict[str, Any]]] = []
        self.busy = False
        self.reused = False
        self.error: Exception | None = None
        self.idempotent_projection: object | None = None

    def submit_task(  # type: ignore[override]
        self,
        task_key: str,
        parameters: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> SubmittedJob:
        if self.error is not None:
            raise self.error
        if self.busy:
            raise TaskBusyError("fixture busy")
        self.calls.append((task_key, deepcopy(dict(parameters or {})), deepcopy(kwargs)))
        projection = SimpleNamespace(job=SimpleNamespace(id=UUID("00000000-0000-0000-0000-000000000711")))
        return SubmittedJob(cast(Any, projection), self.reused)

    def get_idempotent(self, *, scope: str, key: str) -> Any:  # type: ignore[override]
        assert scope == "api.p115.washing_priority_recalculate"
        assert key
        return self.idempotent_projection


class FakeRecalculationRepository:
    def __init__(
        self,
        item: WashingMediaItem,
        *,
        groups: Sequence[Mapping[str, Any]] | None = None,
    ) -> None:
        self.item = item
        self.candidates = [(item.tmdb_id, item.item_type)]
        self.persisted: list[tuple[WashingMediaItem, object]] = []
        self.candidate_calls: list[dict[str, Any]] = []
        self.persist_error: Exception | None = None
        self.groups: list[dict[str, Any]] = (
            [dict(group) for group in groups]
            if groups is not None
            else [
                {
                    "name": "fixture",
                    "media_type": "Movie",
                    "target_cids": [],
                    "priorities": [{"resolution": ["1080p"]}],
                    "sort_order": 0,
                }
            ]
        )

    def load_policy_source(self) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        return (
            {
                "p115_washing_priority_config": {
                    "version_slots_enabled": False,
                    "version_slots": [],
                },
                "p115_sorting_rules": [],
            },
            deepcopy(self.groups),
        )

    def list_candidates(
        self,
        *,
        item_types: Sequence[str],
        affected_scope: object,
        limit: int | None,
    ) -> list[tuple[str, str]]:
        self.candidate_calls.append(
            {
                "item_types": tuple(item_types),
                "affected_scope": deepcopy(affected_scope),
                "limit": limit,
            }
        )
        return list(self.candidates)

    def load_item(self, tmdb_id: str, item_type: str) -> WashingMediaItem | None:
        return self.item if (tmdb_id, item_type) == (self.item.tmdb_id, self.item.item_type) else None

    def persist_item(self, item: WashingMediaItem, mutation: object) -> int:
        if self.persist_error is not None:
            raise self.persist_error
        self.persisted.append((item, mutation))
        return 0


class FakeRuntime:
    def __init__(self, value: float | None = None, *, error: Exception | None = None) -> None:
        self.value = value
        self.error = error
        self.calls: list[tuple[str, int, int]] = []

    def episode_runtime_minutes(
        self,
        series_tmdb_id: str,
        season_number: int,
        episode_number: int,
    ) -> float | None:
        self.calls.append((series_tmdb_id, season_number, episode_number))
        if self.error is not None:
            raise self.error
        return self.value


class FakeProgress:
    def __init__(self) -> None:
        self.updates: list[tuple[int, str]] = []
        self.checkpoints = 0

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        self.checkpoints += 1


def _formatted_info(
    *,
    width: int = 1920,
    height: int = 1080,
    audio: str = "chi",
    runtime_minutes: float = 45,
) -> list[dict[str, Any]]:
    return [
        {
            "MediaSourceInfo": {
                "Size": 10 * 1024**3,
                "RunTimeTicks": int(runtime_minutes * 60 * 10_000_000),
                "MediaStreams": [
                    {"Type": "Video", "Width": width, "Height": height, "Codec": "hevc"},
                    {"Type": "Audio", "Language": audio},
                ],
            }
        }
    ]


def _version(
    *,
    cache_id: str = "file-1",
    sha1: str = "A" * 40,
    target_cid: str = "100",
    mediainfo: object | None = None,
    local_path: str = "/mnt/media/电影/fixture.mkv",
) -> WashingVersionSource:
    return WashingVersionSource(
        cache_id=cache_id,
        sha1=sha1,
        pick_code=f"pc-{cache_id}",
        file_name="Fixture.1080p.WEB-DL-GROUP.mkv",
        file_size=10 * 1024**3,
        local_path=local_path,
        cache_snapshot={"target_cid": target_cid},
        organize_target_cid=target_cid,
        mediainfo=_formatted_info() if mediainfo is None else mediainfo,
    )


def _item(
    *,
    item_type: str = "Movie",
    versions: Sequence[WashingVersionSource] | None = None,
    runtime_minutes: float = 0,
    average_runtime: float = 0,
    parent_genres: object = None,
) -> WashingMediaItem:
    return WashingMediaItem(
        tmdb_id="711",
        item_type=item_type,
        parent_series_tmdb_id="700" if item_type == "Episode" else "",
        season_number=1 if item_type == "Episode" else None,
        episode_number=2 if item_type == "Episode" else None,
        title="Fixture",
        original_language="eng",
        runtime_minutes=runtime_minutes,
        average_episode_runtime_minutes=average_runtime,
        parent_genres=parent_genres,
        asset_details=[],
        washing_snapshot={"target_cid": "100"},
        versions=tuple(versions if versions is not None else [_version()]),
        source_fingerprint="fixture-fingerprint",
    )


def _policy(
    groups: Sequence[Mapping[str, Any]],
    *,
    slots: Sequence[Mapping[str, Any]] = (),
    sorting_rules: Sequence[Mapping[str, Any]] = (),
) -> Any:
    return build_washing_policy_snapshot(
        settings={
            "p115_washing_priority_config": {
                "version_slots_enabled": bool(slots),
                "version_slots": list(slots),
            },
            "p115_sorting_rules": list(sorting_rules),
        },
        groups=groups,
        local_root="/mnt/media",
        media_root_name="媒体库",
    )


def test_evaluator_preserves_legacy_any_language_match() -> None:
    groups = [
        {
            "name": "fixture",
            "media_type": "Movie",
            "target_cids": [],
            "priorities": [
                {"resolution": ["4k"], "is_exclude": True},
                {"audio": ["chi", "eng"], "resolution": ["1080p"]},
            ],
        }
    ]
    mutation = evaluate_washing_item(
        _item(versions=[_version(cache_id="normal", mediainfo=_formatted_info())]),
        _policy(groups),
        evaluated_at=NOW,
    )

    levels = [version["level"] for version in mutation.media_snapshot["versions"]]  # type: ignore[index]
    assert levels == [1]
    assert mutation.media_level == 1
    assert mutation.media_snapshot["evaluated_at"] == "2026-08-29T08:30:00Z"


def test_evaluator_preserves_legacy_zero_before_excluded_best_order() -> None:
    groups = [
        {
            "name": "fixture",
            "media_type": "Movie",
            "target_cids": [],
            "priorities": [
                {"resolution": ["4k"], "is_exclude": True},
                {"resolution": ["1080p"]},
            ],
        }
    ]
    excluded = _version(
        cache_id="excluded",
        mediainfo=_formatted_info(width=3840, height=2160),
    )
    unmatched = _version(
        cache_id="unmatched",
        mediainfo=_formatted_info(width=1280, height=720),
    )
    mutation = evaluate_washing_item(
        _item(versions=[excluded, unmatched]),
        _policy(groups),
        evaluated_at=NOW,
    )
    levels = [version["level"] for version in mutation.media_snapshot["versions"]]  # type: ignore[index]
    assert levels == [-1, 0]
    assert mutation.media_level == 0


@pytest.mark.parametrize(
    "raw_info",
    [
        pytest.param(
            [
                {
                    "MediaSourceInfo": {
                        "Path": "Fixture.2160p.UHD.BluRay.DV.HDR-GROUP.mkv",
                        "Size": 10 * 1024**3,
                        "MediaStreams": [
                            {
                                "Type": "Video",
                                "Width": 3840,
                                "Height": 2160,
                                "Codec": "h265",
                                "ExtendedVideoSubType": "doviprofile81",
                            },
                            {"Type": "Audio", "Title": "粤语", "Language": "chi"},
                            {"Type": "Subtitle", "Title": "特效简中", "Language": "chi"},
                        ],
                    },
                    "_original_lang": "eng",
                }
            ],
            id="list-wrapper",
        ),
        pytest.param(
            {
                "MediaSourceInfo": {
                    "Path": "Fixture.2160p.UHD.BluRay.DV.HDR-GROUP.mkv",
                    "Size": 10 * 1024**3,
                    "MediaStreams": [
                        {
                            "Type": "Video",
                            "Width": 3840,
                            "Height": 2160,
                            "Codec": "h265",
                            "ExtendedVideoSubType": "doviprofile81",
                        },
                        {"Type": "Audio", "Title": "粤语", "Language": "chi"},
                        {"Type": "Subtitle", "Title": "特效简中", "Language": "chi"},
                    ],
                },
                "_original_lang": "eng",
            },
            id="mapping-wrapper",
        ),
        pytest.param(
            {
                "Path": "Fixture.2160p.UHD.BluRay.DV.HDR-GROUP.mkv",
                "Size": 10 * 1024**3,
                "MediaStreams": [
                    {
                        "Type": "Video",
                        "Width": 3840,
                        "Height": 2160,
                        "Codec": "h265",
                        "ExtendedVideoSubType": "doviprofile81",
                    },
                    {"Type": "Audio", "Title": "粤语", "Language": "chi"},
                    {"Type": "Subtitle", "Title": "特效简中", "Language": "chi"},
                ],
                "_original_lang": "eng",
            },
            id="direct-media-source",
        ),
    ],
)
def test_evaluator_normalizes_legacy_mediainfo_shapes(raw_info: object) -> None:
    policy = _policy([])
    normalized = normalize_washing_info(
        raw_info,
        language_mapping=policy.language_mapping,
        release_group_mapping={"FixtureGroup": ("GROUP",)},
        item=_item(),
        remote_episode_runtime_minutes=None,
    )

    assert normalized["resolution"] == "4k"
    assert normalized["codec"] == "hevc"
    assert normalized["source"] == "UHD BluRay"
    assert normalized["effect"] == "dovi_p8"
    assert normalized["release_group"] == "FixtureGroup"
    assert normalized["audio_langs"] == {"yue"}
    assert normalized["sub_langs"] == {"chi"}
    assert normalized["has_effect_subtitle"] is True
    assert normalized["size_gb"] == 10


def test_evaluator_preserves_json_ast_and_nonempty_malformed_raw_semantics() -> None:
    payload = {"MediaSourceInfo": {"MediaStreams": [{"Type": "Video", "Width": 1920, "Height": 1080}]}}
    assert safe_parse_jsonish(json.dumps(payload)) == payload
    assert safe_parse_jsonish(repr(payload)) == payload
    assert safe_parse_jsonish("not-json-or-python") is None

    mutation = evaluate_washing_item(
        _item(versions=[_version(mediainfo="not-json-or-python")]),
        _policy(
            [
                {
                    "name": "empty-normal-rule",
                    "media_type": "Movie",
                    "target_cids": [],
                    "priorities": [{}],
                }
            ]
        ),
        evaluated_at=NOW,
    )
    assert mutation.stats["missing_raw"] == 0
    assert mutation.stats["evaluated_versions"] == 1
    assert mutation.media_level == 1
    assert mutation.media_snapshot["versions"][0]["reason"] == (  # type: ignore[index]
        "规则组[empty-normal-rule] -> 优先级 1"
    )


def test_priority_match_preserves_exclude_external_subtitle_and_size_boundaries() -> None:
    normalized = {
        "resolution": "1080p",
        "source": "WEB-DL",
        "codec": "hevc",
        "effect": "sdr",
        "release_group": "",
        "original_lang": "eng",
        "audio_langs": {"chi"},
        "sub_langs": set(),
        "has_external_subtitle": True,
        "has_effect_subtitle": False,
        "size_gb": 10.0,
        "is_clean_version": None,
        "clean_version_detail": {"reason": "missing_official_runtime"},
    }
    assert match_washing_priority(normalized, {"subtitle": ["chi"]}) == (True, "匹配成功")
    assert match_washing_priority(
        normalized,
        {"subtitle": ["chi"], "is_exclude": True},
    ) == (False, "未命中任何排除条件")
    assert (
        match_washing_priority(
            normalized,
            {"min_size_gb": 10, "is_exclude": True},
        )[0]
        is True
    )
    assert match_washing_priority(normalized, {"min_size_gb": 10}) == (True, "匹配成功")
    assert match_washing_priority(normalized, {"clean_version": True}) == (
        False,
        "无法判断纯净版 (missing_official_runtime)",
    )
    assert match_washing_priority(
        normalized,
        {"clean_version": True, "is_exclude": "truthy"},
    ) == (False, "未命中任何排除条件")


def test_priority_group_order_target_scope_and_version_slot_fallbacks() -> None:
    groups = [
        {
            "name": "global",
            "media_type": "All",
            "target_cids": [],
            "priorities": [{"resolution": ["4k"], "is_exclude": True}],
        },
        {
            "name": "target",
            "media_type": "Movie",
            "target_cids": ["100"],
            "priorities": [{"resolution": ["1080p"]}],
        },
        {
            "name": "other",
            "media_type": "Movie",
            "target_cids": ["200"],
            "priorities": [{}],
        },
    ]
    priorities = load_washing_priorities(groups, "Movie", "100")
    assert [rule["_group_name"] for rule in priorities] == ["global", "target"]

    normalized = {
        "resolution": "1080p",
        "source": "WEB-DL",
        "codec": "hevc",
        "effect": "sdr",
        "release_group": "",
        "original_lang": "eng",
        "audio_langs": {"chi"},
        "sub_langs": {"chi"},
        "size_gb": 10.0,
        "has_effect_subtitle": False,
        "is_clean_version": False,
    }
    slots = [
        {"id": "fallback", "name": "兜底", "fallback": True},
        {"id": "conditionless", "name": "空条件", "match": {}},
        {"id": "first", "name": "首个", "match": {"resolution": ["1080p"]}},
        {"id": "second", "name": "第二个", "match": {"codec": ["hevc"]}},
    ]
    config = {"version_slots_enabled": True, "version_slots": slots}
    assert resolve_washing_version_slot(normalized, config)["id"] == "first"  # type: ignore[index]
    normalized["resolution"] = "720p"
    normalized["codec"] = "h264"
    assert resolve_washing_version_slot(normalized, config)["id"] == "fallback"  # type: ignore[index]
    assert (
        resolve_washing_version_slot(
            normalized,
            {"version_slots_enabled": True, "version_slots": slots[1:]},
        )
        is None
    )
    assert resolve_washing_version_slot(
        normalized,
        {"version_slots_enabled": False, "version_slots": slots},
    ) == {"enabled": False, "id": "__single__", "name": "主版本", "suffix": ""}


def test_evaluator_writes_exact_legacy_snapshot_shapes() -> None:
    mutation = evaluate_washing_item(
        _item(),
        _policy(
            [
                {
                    "name": "fixture",
                    "media_type": "Movie",
                    "target_cids": [],
                    "priorities": [{"resolution": ["1080p"]}],
                }
            ]
        ),
        evaluated_at=NOW,
    )
    version = mutation.media_snapshot["versions"][0]  # type: ignore[index]
    assert set(version) == {
        "fid",
        "sha1",
        "size",
        "level",
        "reason",
        "identity",
        "file_name",
        "pick_code",
        "media_type",
        "target_cid",
        "version_slot",
        "evaluated_at",
    }
    assert set(mutation.cache_mutations[0].snapshot) == {
        "reason",
        "target_cid",
        "media_type",
        "version_slot",
        "identity",
        "evaluated_at",
    }
    assert set(mutation.media_snapshot) == {
        "versions",
        "slot_levels",
        "reason",
        "sha1",
        "target_cid",
        "media_type",
        "version_slot",
        "evaluated_at",
    }


def test_evaluator_preserves_missing_identity_raw_and_local_path_backfill() -> None:
    no_identity = evaluate_washing_item(
        _item(versions=[]),
        _policy([]),
        evaluated_at=NOW,
    )
    assert no_identity.media_level is None
    assert no_identity.stats["missing_identity"] == 1

    groups = [
        {
            "name": "directory",
            "media_type": "Movie",
            "target_cids": ["200"],
            "priorities": [{"resolution": ["1080p"]}],
        }
    ]
    source = _version(target_cid="", local_path="/mnt/media/电影/fixture.mkv")
    mutation = evaluate_washing_item(
        _item(versions=[source]),
        _policy(
            groups,
            sorting_rules=[{"cid": "200", "category_path": "媒体库/电影", "enabled": True}],
        ),
        evaluated_at=NOW,
    )
    version = mutation.media_snapshot["versions"][0]  # type: ignore[index]
    assert version["target_cid"] == "200"
    assert mutation.organize_backfills[0].target_cid == "200"

    missing_raw = evaluate_washing_item(
        _item(versions=[_version(mediainfo={})]),
        _policy(groups),
        evaluated_at=NOW,
    )
    assert missing_raw.stats["missing_raw"] == 1
    assert missing_raw.media_level == 0


def test_clean_version_runtime_sources_and_boundaries() -> None:
    episode = _item(
        item_type="Episode",
        versions=[_version(mediainfo=_formatted_info(runtime_minutes=43.5))],
        runtime_minutes=45,
    )
    detail = clean_version_result(
        episode,
        _formatted_info(runtime_minutes=43.5),
        remote_episode_runtime_minutes=None,
    )
    assert detail["checked"] is True
    assert detail["is_clean"] is True
    assert detail["runtime_source"] == "local_media_metadata"

    fallback = _item(
        item_type="Episode",
        versions=[_version(mediainfo=_formatted_info(runtime_minutes=43))],
        average_runtime=45,
    )
    detail = clean_version_result(
        fallback,
        _formatted_info(runtime_minutes=43),
        remote_episode_runtime_minutes=None,
    )
    assert detail["runtime_source"] == "local_media_metadata_average"
    assert detail["is_clean"] is True

    animation = _item(item_type="Episode", parent_genres=[{"id": 16, "name": "Animation"}])
    assert (
        clean_version_result(
            animation,
            _formatted_info(runtime_minutes=45),
            remote_episode_runtime_minutes=47,
        )["reason"]
        == "animation_genre_skipped"
    )


def test_recalculation_service_preserves_limit_and_revision_ack_semantics() -> None:
    item = _item()
    repository = FakeRecalculationRepository(item)
    state = FakeWashingState()
    runtime = FakeRuntime()
    progress = FakeProgress()
    service = P115WashingPriorityRecalculationService(
        repository=repository,
        state=state,
        settings=lambda: Settings(local_strm_root="/mnt/media", p115_media_root_name="媒体库"),
        episode_runtime=runtime,
        clock=lambda: NOW,
    )

    result = service.execute(
        progress,
        item_type="movies",
        limit="0",
        affected_scope=state.pending["media_types"],
        scope_revision=7,
    )

    assert result.partial is False
    assert repository.candidate_calls == [
        {
            "item_types": ("Movie",),
            "affected_scope": state.pending["media_types"],
            "limit": None,
        }
    ]
    assert state.acknowledgements == []
    assert result.stats["scope_acknowledged"] is False
    assert progress.checkpoints == 1

    repository.candidate_calls.clear()
    state.acknowledgements.clear()
    result = service.execute(
        progress,
        item_type="all",
        limit=None,
        affected_scope=state.pending["media_types"],
        scope_revision=7,
    )
    assert result.stats["scope_acknowledged"] is True
    assert state.acknowledgements == [(7, {"Movie": {"all": False, "target_cids": ["100"]}})]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("movie", ("Movie",)),
        ("movies", ("Movie",)),
        ("episode", ("Episode",)),
        ("episodes", ("Episode",)),
        ("series", ("Episode",)),
        ("tv", ("Episode",)),
        ("all", ("Movie", "Episode")),
        ("unsupported", ("Movie", "Episode")),
    ],
)
def test_recalculation_item_type_aliases(value: object, expected: tuple[str, ...]) -> None:
    assert normalize_washing_item_types(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("", None),
        (0, None),
        (-1, None),
        ("0", None),
        ("-1", None),
        ("bad", None),
        (5, 5),
        ("5", 5),
    ],
)
def test_recalculation_limit_compatibility(value: object, expected: int | None) -> None:
    assert normalize_washing_limit(value) == expected


def test_recalculation_service_uses_remote_runtime_then_local_average_fallback() -> None:
    groups = [
        {
            "name": "clean",
            "media_type": "Series",
            "target_cids": [],
            "priorities": [{"clean_version": True}],
        }
    ]
    episode = _item(
        item_type="Episode",
        versions=[_version(mediainfo=_formatted_info(runtime_minutes=43))],
    )
    repository = FakeRecalculationRepository(episode, groups=groups)
    runtime = FakeRuntime(45)
    service = P115WashingPriorityRecalculationService(
        repository=repository,
        state=FakeWashingState(),
        settings=lambda: Settings(local_strm_root="/mnt/media", p115_media_root_name="媒体库"),
        episode_runtime=runtime,
        clock=lambda: NOW,
    )

    result = service.execute(FakeProgress(), item_type="episode")

    assert result.partial is False
    assert runtime.calls == [("700", 1, 2)]
    mutation = cast(Any, repository.persisted[0][1])
    assert mutation.media_level == 1

    fallback_episode = _item(
        item_type="Episode",
        versions=[_version(mediainfo=_formatted_info(runtime_minutes=43))],
        average_runtime=45,
    )
    fallback_repository = FakeRecalculationRepository(fallback_episode, groups=groups)
    failing_runtime = FakeRuntime(error=RuntimeError("fixture TMDb unavailable"))
    fallback_service = P115WashingPriorityRecalculationService(
        repository=fallback_repository,
        state=FakeWashingState(),
        settings=lambda: Settings(local_strm_root="/mnt/media", p115_media_root_name="媒体库"),
        episode_runtime=failing_runtime,
        clock=lambda: NOW,
    )

    fallback = fallback_service.execute(FakeProgress(), item_type="tv")

    assert fallback.partial is False
    assert failing_runtime.calls == [("700", 1, 2)]
    fallback_mutation = cast(Any, fallback_repository.persisted[0][1])
    assert fallback_mutation.media_level == 1

    missing_identity = replace(
        fallback_episode,
        parent_series_tmdb_id="",
        season_number=None,
        episode_number=None,
    )
    missing_repository = FakeRecalculationRepository(missing_identity, groups=groups)
    unused_runtime = FakeRuntime(error=AssertionError("missing identity must not query TMDb"))
    missing_service = P115WashingPriorityRecalculationService(
        repository=missing_repository,
        state=FakeWashingState(),
        settings=lambda: Settings(local_strm_root="/mnt/media", p115_media_root_name="媒体库"),
        episode_runtime=unused_runtime,
        clock=lambda: NOW,
    )

    missing_result = missing_service.execute(FakeProgress(), item_type="episode")

    assert missing_result.partial is False
    assert unused_runtime.calls == []
    missing_mutation = cast(Any, missing_repository.persisted[0][1])
    assert missing_mutation.media_level == 0


def test_episode_runtime_adapter_uses_stubbed_tmdb_and_maps_failures_to_none() -> None:
    requests: list[httpx.Request] = []

    def success(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "episodes": [
                    {"episode_number": 1, "runtime": 44},
                    {"episode_number": 2, "runtime": 45},
                ]
            },
        )

    def settings() -> Settings:
        return Settings(
            tmdb_api_key=SecretStr("fixture-key"),
            tmdb_api_base_url="https://tmdb.invalid/3",
        )

    gateway = HttpxP115EpisodeRuntimeGateway(
        settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(success)),
    )

    assert gateway.episode_runtime_minutes("700", 1, 2) == 45
    assert requests[0].url.path == "/3/tv/700/season/1"
    assert requests[0].url.params["api_key"] == "fixture-key"

    failing = HttpxP115EpisodeRuntimeGateway(
        settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(503))),
    )
    invalid = HttpxP115EpisodeRuntimeGateway(
        settings,
        client_factory=lambda: httpx.Client(
            transport=httpx.MockTransport(lambda _request: httpx.Response(200, json=[]))
        ),
    )
    assert failing.episode_runtime_minutes("700", 1, 2) is None
    assert invalid.episode_runtime_minutes("700", 1, 2) is None


def test_recalculation_service_rejects_target_or_source_drift_without_ack() -> None:
    drifted = _item(
        versions=[
            WashingVersionSource(
                cache_id="drifted",
                sha1="B" * 40,
                pick_code="pc-drifted",
                file_name="Drifted.1080p.mkv",
                file_size=1024,
                local_path="/mnt/media/other/Drifted.1080p.mkv",
                cache_snapshot={"target_cid": "999"},
                organize_target_cid="999",
                mediainfo=_formatted_info(),
            )
        ]
    )
    drifted = replace(drifted, washing_snapshot={"target_cid": "999"})
    state = FakeWashingState()
    repository = FakeRecalculationRepository(drifted)
    service = P115WashingPriorityRecalculationService(
        repository=repository,
        state=state,
        settings=lambda: Settings(local_strm_root="/mnt/media", p115_media_root_name="媒体库"),
        episode_runtime=FakeRuntime(),
        clock=lambda: NOW,
    )

    result = service.execute(
        FakeProgress(),
        item_type="movie",
        affected_scope=state.pending["media_types"],
        scope_revision=7,
    )

    assert result.partial is True
    assert result.stats["errors"] == 1
    assert repository.persisted == []
    assert state.acknowledgements == []

    source_repository = FakeRecalculationRepository(_item())
    source_repository.persist_error = RuntimeError("source fingerprint changed")
    source_service = P115WashingPriorityRecalculationService(
        repository=source_repository,
        state=state,
        settings=lambda: Settings(local_strm_root="/mnt/media", p115_media_root_name="媒体库"),
        episode_runtime=FakeRuntime(),
        clock=lambda: NOW,
    )
    source_result = source_service.execute(
        FakeProgress(),
        affected_scope=state.pending["media_types"],
        scope_revision=7,
    )
    assert source_result.partial is True
    assert source_result.stats["scope_acknowledged"] is False
    assert state.acknowledgements == []


class FakeRouterRecalculation:
    def __init__(self, errors: int = 0) -> None:
        self.errors = errors
        self.calls: list[dict[str, Any]] = []

    def execute(
        self,
        progress: object,
        *,
        item_type: object = "all",
        limit: object = None,
        affected_scope: object = None,
        scope_revision: object = None,
    ) -> Any:
        del progress
        self.calls.append(
            deepcopy(
                {
                    "item_type": item_type,
                    "limit": limit,
                    "affected_scope": affected_scope,
                    "scope_revision": scope_revision,
                }
            )
        )
        return SimpleNamespace(stats={"errors": self.errors}, partial=self.errors > 0)


def test_p7_router_executes_washing_without_processor_or_legacy_gateway() -> None:
    recalculation = FakeRouterRecalculation()
    task_router = P7SubscriptionTaskRouter(
        None,
        None,
        washing_recalculation=recalculation,
    )
    message = TaskMessage.create(
        task_key="recalculate_library_washing_priorities",
        processor=ProcessorIdentity.MEDIA,
        payload={
            "item_type": "all",
            "limit": None,
            "affected_scope": {"Movie": {"all": True, "target_cids": []}},
            "scope_revision": 3,
        },
    )
    outcome = task_router.execute(message, None, FakeProgress())
    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["compatibility_fallback"] is False
    assert recalculation.calls[0]["scope_revision"] == 3

    recalculation.errors = 1
    outcome = task_router.execute(message, None, FakeProgress())
    assert outcome.status is TaskOutcomeStatus.PARTIAL


def _native_client(
    state: FakeWashingState,
    jobs: RecordingJobs,
    *,
    auth_enabled: bool = False,
) -> TestClient:
    washing = P115WashingPriorityService(
        settings=FakeSettingsStore(),
        repository=state,
    )
    container = AppContainer(
        Settings(auth_enabled=auth_enabled),
        repository_providers={"identity": lambda _: FakePermissions()},
        use_case_providers={"operations.jobs": lambda _: jobs},
    )
    app = FastAPI()
    app.state.container = container
    app.state.p115_washing_priority_service = washing
    app.add_middleware(
        CookieSessionMiddleware,
        codec_provider=lambda: LegacySessionCodec.from_secret_key(b"p11-washing-recalculation"),
    )
    app.include_router(router)
    return TestClient(app)


def test_recalculation_http_submits_durable_job_and_preserves_empty_scope() -> None:
    state = FakeWashingState()
    jobs = RecordingJobs()
    with _native_client(state, jobs) as client:
        response = client.post(
            "/api/p115/washing_priority_recalculate",
            json={"item_type": "all", "background": True},
            headers={"idempotency-key": "fixture-recalculate"},
        )
    assert response.status_code == 202
    assert response.json()["data"] == {
        "job_id": "00000000-0000-0000-0000-000000000711",
        "reused": False,
    }
    assert jobs.calls[0][0] == "recalculate_library_washing_priorities"
    assert jobs.calls[0][1] == {
        "item_type": "all",
        "limit": None,
        "affected_scope": state.pending["media_types"],
        "scope_revision": 7,
    }
    assert jobs.calls[0][2]["idempotency_scope"] == "api.p115.washing_priority_recalculate"
    assert jobs.calls[0][2]["reject_when_busy"] is False

    state.pending = {"revision": 8, "media_types": {}}
    jobs.calls.clear()
    with _native_client(state, jobs) as client:
        empty = client.post("/api/p115/washing_priority_recalculate", json={})
    assert empty.status_code == 200
    assert empty.json() == {
        "success": True,
        "message": "当前没有受规则变更影响的媒体项",
        "data": {"scanned_items": 0, "updated_items": 0},
    }
    assert jobs.calls == []

    jobs.idempotent_projection = SimpleNamespace(
        job=SimpleNamespace(
            id=UUID("00000000-0000-0000-0000-000000000711"),
            task_key="recalculate_library_washing_priorities",
            request_payload={
                "parameters": {
                    "item_type": "all",
                    "limit": None,
                    "affected_scope": {"Movie": {"all": False, "target_cids": ["100"]}},
                    "scope_revision": 7,
                }
            },
        )
    )
    with _native_client(state, jobs) as client:
        replay_after_ack = client.post(
            "/api/p115/washing_priority_recalculate",
            json={"item_type": "all"},
            headers={"idempotency-key": "fixture-recalculate"},
        )
    assert replay_after_ack.status_code == 200
    assert replay_after_ack.json()["data"] == {
        "job_id": "00000000-0000-0000-0000-000000000711",
        "reused": True,
    }

    jobs.idempotent_projection = SimpleNamespace(
        job=SimpleNamespace(
            id=UUID("00000000-0000-0000-0000-000000000711"),
            task_key="recalculate_library_washing_priorities",
            request_payload={
                "parameters": {
                    "item_type": "all",
                    "limit": None,
                    "affected_scope": None,
                    "scope_revision": None,
                }
            },
        )
    )
    with _native_client(state, jobs) as client:
        incompatible_replay = client.post(
            "/api/p115/washing_priority_recalculate",
            json={"item_type": "all"},
            headers={"idempotency-key": "fixture-recalculate"},
        )
    assert incompatible_replay.status_code == 409


def test_recalculation_http_full_skips_pending_state_and_replay_returns_200() -> None:
    state = FakeWashingState()
    state.load_error = AssertionError("full recalculation must not read pending scope")
    jobs = RecordingJobs()
    with _native_client(state, jobs) as client:
        submitted = client.post(
            "/api/p115/washing_priority_recalculate",
            json={"item_type": "series", "full": True},
            headers={"idempotency-key": "fixture-full"},
        )
        jobs.reused = True
        replayed = client.post(
            "/api/p115/washing_priority_recalculate",
            json={"item_type": "series", "full": True},
            headers={"idempotency-key": "fixture-full"},
        )

    assert state.load_calls == 0
    assert submitted.status_code == 202
    assert replayed.status_code == 200
    assert replayed.json()["data"] == {
        "job_id": "00000000-0000-0000-0000-000000000711",
        "reused": True,
    }
    assert jobs.calls[0][1] == {
        "item_type": "series",
        "limit": None,
        "affected_scope": None,
        "scope_revision": None,
    }


def test_recalculation_http_maps_idempotency_and_json_transport_contracts() -> None:
    state = FakeWashingState()
    jobs = RecordingJobs()
    jobs.error = IdempotencyConflictError("fixture conflict")
    with _native_client(state, jobs) as client:
        conflict = client.post(
            "/api/p115/washing_priority_recalculate",
            json={},
            headers={"idempotency-key": "fixture-conflict"},
        )
    assert conflict.status_code == 409
    assert conflict.json() == {
        "success": False,
        "message": "幂等键已用于不同的洗版优先级重算请求。",
    }

    jobs.error = None
    with _native_client(state, jobs) as client:
        malformed = client.post(
            "/api/p115/washing_priority_recalculate",
            content="{",
            headers={"content-type": "application/json"},
        )
        unsupported = client.post(
            "/api/p115/washing_priority_recalculate",
            content="{}",
            headers={"content-type": "text/plain"},
        )
        non_object = client.post(
            "/api/p115/washing_priority_recalculate",
            json=["not", "an", "object"],
        )
    assert malformed.status_code == 400
    assert malformed.headers["content-type"].startswith("text/html")
    assert unsupported.status_code == 415
    assert unsupported.headers["content-type"].startswith("text/html")
    assert non_object.status_code == 500
    assert non_object.headers["content-type"].startswith("text/html")


def test_recalculation_http_denies_before_parsing_and_maps_busy_to_409() -> None:
    state = FakeWashingState()
    jobs = RecordingJobs()
    with _native_client(state, jobs, auth_enabled=True) as client:
        denied = client.post(
            "/api/p115/washing_priority_recalculate",
            content="{",
            headers={"content-type": "application/json"},
        )
    assert denied.status_code == 403
    assert jobs.calls == []

    jobs.busy = True
    with _native_client(state, jobs) as client:
        busy = client.post("/api/p115/washing_priority_recalculate", json={})
    assert busy.status_code == 409
    assert busy.json() == {"success": False, "message": "后台有任务正在运行，请稍后再试。"}


def test_native_washing_processed_path_imports_no_legacy_runtime_modules() -> None:
    program = """
import json
import sys
from datetime import UTC, datetime
from etk.domains.p115.washing_evaluator import (
    WashingMediaItem,
    WashingVersionSource,
    build_washing_policy_snapshot,
    evaluate_washing_item,
)

version = WashingVersionSource(
    cache_id='fixture',
    sha1='A' * 40,
    pick_code='fixture-pc',
    file_name='Fixture.1080p.WEB-DL.mkv',
    file_size=1024,
    local_path='/fixture/Fixture.1080p.WEB-DL.mkv',
    cache_snapshot={'target_cid': '100'},
    organize_target_cid='100',
    mediainfo=[{'MediaSourceInfo': {'MediaStreams': [
        {'Type': 'Video', 'Width': 1920, 'Height': 1080, 'Codec': 'hevc'}
    ]}}],
)
item = WashingMediaItem(
    tmdb_id='1',
    item_type='Movie',
    parent_series_tmdb_id='',
    season_number=None,
    episode_number=None,
    title='Fixture',
    original_language='eng',
    runtime_minutes=0,
    average_episode_runtime_minutes=0,
    parent_genres=[],
    asset_details=[],
    washing_snapshot={'target_cid': '100'},
    versions=(version,),
    source_fingerprint='fixture',
)
policy = build_washing_policy_snapshot(
    settings={'p115_washing_priority_config': {'version_slots_enabled': False}},
    groups=[{'name': 'fixture', 'media_type': 'Movie', 'target_cids': [],
             'priorities': [{'resolution': ['1080p']}]}],
    local_root='/fixture',
    media_root_name='media',
)
mutation = evaluate_washing_item(item, policy, evaluated_at=datetime(2026, 8, 29, tzinfo=UTC))
forbidden = sorted(
    name for name in sys.modules
    if name in {'task_manager', 'config_manager', 'database', 'handler', 'tasks'}
    or name.startswith(('database.', 'handler.', 'tasks.'))
)
print(json.dumps({'forbidden': forbidden, 'level': mutation.media_level}))
"""
    environment = os.environ.copy()
    python_path = [str(REPO_ROOT / "src"), str(REPO_ROOT)]
    if environment.get("PYTHONPATH"):
        python_path.append(environment["PYTHONPATH"])
    environment["PYTHONPATH"] = os.pathsep.join(python_path)

    completed = subprocess.run(
        [sys.executable, "-c", program],
        cwd=REPO_ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(completed.stdout) == {"forbidden": [], "level": 1}
