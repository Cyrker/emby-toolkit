from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.p115 import (
    NativeP115PlanNamingGateway,
    NativeP115RuleRecognitionGateway,
    P115CredentialSnapshot,
    P115EpisodeRegexSnapshot,
    P115OrganizePlan,
    P115OrganizePlanService,
    P115PlanCommitPreconditions,
    P115PlanConflict,
    P115PlanningSupportSnapshot,
    P115RenameConfigSnapshot,
    P115Result,
    P115SortingRulesSnapshot,
    P115SourceNode,
    policy_revision,
    runtime_config_revision,
)
from etk.domains.p115 import adapters as p115_adapters_module
from etk.domains.p115 import rename_renderer as rename_renderer_module
from etk.domains.p115.rename_renderer import P115RenameRenderer
from etk.domains.p115.router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = json.loads((REPO_ROOT / "verification/fixtures/http/p6-p115-organize-plan.json").read_text())
FIXTURE_CASES = {case["case_id"]: case for case in FIXTURE["cases"]}
FIXED_NOW = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)


class FakePermissions:
    def is_admin(self, user_id: str) -> bool:
        return user_id == "fixture-admin"


class FakeCredentials:
    def __init__(self, account_ids: Sequence[str] = ("115001",)) -> None:
        self.account_ids = list(account_ids)
        self.calls = 0

    def load(self) -> P115CredentialSnapshot:
        index = min(self.calls, len(self.account_ids) - 1)
        self.calls += 1
        return P115CredentialSnapshot(account_id=self.account_ids[index])

    def save_openapi(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("Dry Run must not save credentials")

    def save_cookie(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("Dry Run must not save credentials")

    def app_label(self, app_type: str) -> str:
        return app_type


class FakeDirectoryClient:
    def __init__(self, responses: Mapping[tuple[str, int], object]) -> None:
        self.responses = dict(responses)
        self.calls: list[tuple[str, int, int]] = []
        self.write_calls: list[str] = []

    def list_page(self, *, cid: str, limit: int, offset: int) -> Mapping[str, Any]:
        self.calls.append((cid, limit, offset))
        value = self.responses[(cid, offset)]
        if isinstance(value, Exception):
            raise value
        if not isinstance(value, Mapping):
            raise TypeError("fixture directory response must be an object")
        return deepcopy(dict(value))

    def __getattr__(self, name: str) -> Any:
        if name.startswith(("fs_mkdir", "fs_move", "fs_rename", "fs_delete", "fs_copy")):
            self.write_calls.append(name)
            raise AssertionError(f"Dry Run attempted provider write: {name}")
        raise AttributeError(name)


class FakeDirectoryGateway:
    def __init__(self, client: FakeDirectoryClient | None) -> None:
        self.client = client
        self.open_calls: list[str] = []

    def open(self, *, expected_account_id: str) -> FakeDirectoryClient | None:
        self.open_calls.append(expected_account_id)
        return self.client


class FakeSortingRepository:
    def __init__(self, snapshot: P115SortingRulesSnapshot) -> None:
        self.snapshot = snapshot

    def load(self) -> P115SortingRulesSnapshot:
        return self.snapshot

    def save(self, *_args: Any, **_kwargs: Any) -> P115SortingRulesSnapshot:
        raise AssertionError("Dry Run must not save sorting rules")


class FakeRenameRepository:
    def __init__(self, snapshot: P115RenameConfigSnapshot) -> None:
        self.snapshot = snapshot

    def load(self) -> P115RenameConfigSnapshot:
        return self.snapshot

    def save(self, *_args: Any, **_kwargs: Any) -> P115RenameConfigSnapshot:
        raise AssertionError("Dry Run must not save rename config")


class FakeEpisodeRepository:
    def __init__(self, snapshot: P115EpisodeRegexSnapshot | None = None) -> None:
        self.snapshot = snapshot or P115EpisodeRegexSnapshot(())

    def load(self) -> P115EpisodeRegexSnapshot:
        return self.snapshot

    def save(self, *_args: Any, **_kwargs: Any) -> P115EpisodeRegexSnapshot:
        raise AssertionError("Dry Run must not save episode rules")


class FakePlanRepository:
    def __init__(
        self,
        *,
        media_info: Mapping[str, Mapping[str, Any]] | None = None,
        policy: Mapping[str, Any] | None = None,
        commit_error: Exception | None = None,
    ) -> None:
        self.policy = dict(policy or {})
        self.media_info = {str(key): dict(value) for key, value in (media_info or {}).items()}
        self.commit_error = commit_error
        self.plans: dict[str, P115OrganizePlan] = {}
        self.by_intent: dict[str, P115OrganizePlan] = {}
        self.support_calls: list[tuple[str, ...]] = []
        self.save_calls: list[tuple[P115OrganizePlan, P115PlanCommitPreconditions]] = []

    def load_support_snapshot(self, sha1s: Sequence[str]) -> P115PlanningSupportSnapshot:
        self.support_calls.append(tuple(sha1s))
        return P115PlanningSupportSnapshot(
            policy=deepcopy(self.policy),
            policy_revision=policy_revision(self.policy),
            runtime_config_revision=runtime_config_revision({}),
            media_info=deepcopy(self.media_info),
        )

    def save(
        self,
        plan: P115OrganizePlan,
        *,
        preconditions: P115PlanCommitPreconditions,
    ) -> P115OrganizePlan:
        self.save_calls.append((plan, preconditions))
        if self.commit_error is not None:
            raise self.commit_error
        existing = self.by_intent.get(plan.request_intent_id)
        if existing is not None:
            if existing.request_hash != plan.request_hash or existing.input_fingerprint != plan.input_fingerprint:
                raise P115PlanConflict("request_intent_id 已用于不同的整理计划输入")
            return existing
        self.by_intent[plan.request_intent_id] = plan
        self.plans[plan.plan_id] = plan
        return plan

    def get(self, plan_id: str) -> P115OrganizePlan | None:
        return self.plans.get(plan_id)

    def assert_current(self, plan: P115OrganizePlan) -> None:
        if self.plans.get(plan.plan_id) is not plan:
            raise P115PlanConflict("fixture Plan is no longer current")


class FakeRecognition:
    def recognize(
        self,
        node: P115SourceNode,
        *,
        hints: Mapping[str, Any],
        episode_rules: Sequence[Mapping[str, Any]],
        policy: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        del node, episode_rules, policy
        return deepcopy(dict(hints))


class FakeNaming:
    def render_names(
        self,
        node: P115SourceNode,
        *,
        recognition: Mapping[str, Any],
        config: Mapping[str, Any],
    ) -> Mapping[str, str]:
        del config
        title = str(recognition["title"])
        year = str(recognition.get("year") or "")
        main_dir = f"{title} ({year})" if year else title
        season = recognition.get("season_number")
        episode = recognition.get("episode_number")
        is_tv = recognition.get("media_type") == "tv"
        season_value = int(str(season)) if season is not None else 0
        episode_value = int(str(episode)) if episode is not None else 0
        season_dir = f"Season {season_value:02d}" if is_tv else ""
        suffix = Path(node.name).suffix
        file_name = f"{title} S{season_value:02d}E{episode_value:02d}{suffix}" if is_tv else f"{title}{suffix}"
        return {"main_dir": main_dir, "season_dir": season_dir, "file_name": file_name}


def _file(
    file_id: str,
    name: str,
    sha1: str,
    *,
    size: int = 20 * 1024 * 1024,
) -> dict[str, Any]:
    return {"fid": file_id, "fn": name, "fc": 1, "sha": sha1, "pc": f"pc-{file_id}", "fs": size}


def _result_payload(result: P115Result) -> dict[str, Any]:
    assert isinstance(result.payload, dict)
    return cast(dict[str, Any], result.payload)


def _plan_data(result: P115Result) -> dict[str, Any]:
    payload = _result_payload(result)
    assert isinstance(payload.get("data"), Mapping)
    return dict(cast(Mapping[str, Any], payload["data"]))


def _service(
    *,
    responses: Mapping[tuple[str, int], object] | None = None,
    account_ids: Sequence[str] = ("115001",),
    rules_account_id: str = "115001",
    rules: Sequence[Mapping[str, Any]] | None = None,
    media_info: Mapping[str, Mapping[str, Any]] | None = None,
    repository: FakePlanRepository | None = None,
    recognition: Any | None = None,
    naming: Any | None = None,
    rename_config: Mapping[str, Any] | None = None,
    extensions: Sequence[str] = ("mkv", "mp4", "srt"),
    minimum_video_size: int = 0,
    auth_enabled: bool = False,
) -> tuple[
    P115OrganizePlanService,
    FakeDirectoryClient | None,
    FakeDirectoryGateway,
    FakePlanRepository,
    AppContainer,
]:
    client = FakeDirectoryClient(responses) if responses is not None else None
    directory_gateway = FakeDirectoryGateway(client)
    plan_repository = repository or FakePlanRepository(media_info=media_info)
    container = AppContainer(
        Settings.model_validate(
            {
                "auth_enabled": auth_enabled,
                "p115_extensions": list(extensions),
                "p115_min_video_size": minimum_video_size,
                "p115_save_path_cid": "source-root",
                "p115_save_path_name": "待整理",
            }
        ),
        repository_providers={"identity": lambda _: FakePermissions()},
    )
    service = P115OrganizePlanService(
        settings=lambda: container.settings,
        credentials=FakeCredentials(account_ids),
        directories=directory_gateway,
        sorting_rules=FakeSortingRepository(
            P115SortingRulesSnapshot(
                tuple(
                    deepcopy(dict(rule))
                    for rule in (
                        rules
                        or [
                            {
                                "id": "movies",
                                "name": "电影",
                                "cid": "target-movies",
                                "category_path": "电影",
                                "media_type": "movie",
                            }
                        ]
                    )
                ),
                rules_account_id,
            )
        ),
        rename_config=FakeRenameRepository(
            P115RenameConfigSnapshot(
                dict(rename_config)
                if rename_config is not None
                else {
                    "main_dir_template": "{{title}}",
                    "season_dir_template": "Season {{season_no}}",
                    "movie_file_template": "{{title}}{{fileExt}}",
                    "tv_file_template": "{{title}} {{season_episode}}{{fileExt}}",
                }
            )
        ),
        episode_regex=FakeEpisodeRepository(),
        repository=plan_repository,
        recognition=recognition or FakeRecognition(),
        naming=naming or FakeNaming(),
        clock=lambda: FIXED_NOW,
    )
    return service, client, directory_gateway, plan_repository, container


def _ready_movie() -> tuple[P115OrganizePlanService, FakePlanRepository, dict[str, Any]]:
    sha1 = "A" * 40
    service, _client, _gateway, repository, _container = _service(
        responses={
            ("source-root", 0): {"state": True, "data": [_file("movie-1", "Parasite.2019.mkv", sha1)], "count": 1},
            ("source-root", 1): {"state": True, "data": [], "count": 1},
        },
        media_info={sha1: {"snapshot_hash": "b" * 64, "has_video": True, "stream_count": 2}},
    )
    payload = {
        "request_intent_id": "fixture-ready-movie",
        "recognition_hints": {
            "movie-1": {
                "title": "寄生虫",
                "year": 2019,
                "media_type": "movie",
                "tmdb_id": "496243",
            }
        },
    }
    return service, repository, payload


def test_ready_movie_plan_matches_the_frozen_no_effect_contract() -> None:
    case = FIXTURE_CASES["ready-movie"]
    service, repository, payload = _ready_movie()

    result = service.create(payload)

    assert result.status_code == 200
    data = _plan_data(result)
    assert data["status"] == case["expected_status"]
    assert [action["kind"] for action in data["actions"]] == case["expected_action_kinds"]
    assert data["risk_summary"]["external_writes_executed"] == case["expected_external_writes"]
    assert data["revisions"] == {
        "sorting_rules": repository.save_calls[0][1].sorting_rules_revision,
        "rename_config": repository.save_calls[0][1].rename_config_revision,
        "episode_regex": repository.save_calls[0][1].episode_regex_revision,
        "policy": repository.save_calls[0][1].policy_revision,
        "runtime_config": repository.save_calls[0][1].runtime_config_revision,
    }
    assert data["actions"][1]["dependencies"] == [data["actions"][0]["action_id"]]
    assert data["actions"][2]["dependencies"] == [data["actions"][1]["action_id"]]
    assert data["actions"][1]["preconditions"]["destination_must_be_absent"] is True
    rename_preconditions = data["actions"][2]["preconditions"]
    assert rename_preconditions["source_parent_action_id"] == data["actions"][0]["action_id"]
    assert "source_parent_id" not in rename_preconditions
    assert "source_raw_hash" not in rename_preconditions
    rename_subject = data["actions"][2]["subject"]
    assert rename_subject["origin_parent_id"] == "source-root"
    assert rename_subject["origin_name"] == "Parasite.2019.mkv"
    assert rename_subject["source_parent_action_id"] == data["actions"][0]["action_id"]
    assert "parent_id" not in rename_subject
    assert all(action["deferred_slice"] == "P6.4" for action in data["actions"])
    assert all(action["risk"]["requires_readback"] for action in data["actions"])


def test_plan_can_freeze_one_selected_moviepilot_file_without_claiming_siblings() -> None:
    sha1_a = "A" * 40
    sha1_b = "B" * 40
    service, _client, _gateway, repository, _container = _service(
        responses={
            ("source-root", 0): {
                "state": True,
                "data": [
                    _file("movie-1", "Fixture.2026.mkv", sha1_a),
                    _file("movie-2", "Unrelated.2025.mkv", sha1_b),
                ],
                "count": 2,
            },
            ("source-root", 2): {"state": True, "data": [], "count": 2},
        },
        media_info={sha1_a: {"snapshot_hash": "c" * 64, "has_video": True}},
    )

    result = service.create(
        {
            "request_intent_id": "moviepilot-selected-file",
            "source_file_ids": ["movie-1"],
            "recognition_hints": {
                "movie-1": {
                    "title": "Fixture",
                    "year": 2026,
                    "media_type": "movie",
                    "tmdb_id": "100",
                }
            },
        }
    )

    data = _plan_data(result)
    assert data["status"] == "ready"
    assert [node["id"] for node in data["source_snapshot"]] == ["movie-1"]
    assert data["provider_evidence"]["node_count"] == 2
    assert data["provider_evidence"]["selected_file_ids"] == ["movie-1"]
    assert data["provider_evidence"]["selected_node_count"] == 1
    assert repository.support_calls == [(sha1_a,)]


def test_selected_source_file_ids_reject_duplicates_and_missing_nodes() -> None:
    service, _client, _gateway, _repository, _container = _service(
        responses={
            ("source-root", 0): {"state": True, "data": [], "count": 0},
        }
    )

    duplicate = service.create({"source_file_ids": ["movie-1", "movie-1"]})
    missing = service.create({"source_file_ids": ["movie-1"]})

    assert duplicate.status_code == 400
    assert _result_payload(duplicate)["success"] is False
    assert "unique" in str(_result_payload(duplicate)["message"])
    assert missing.status_code == 200
    assert _plan_data(missing)["status"] == "blocked"
    assert _plan_data(missing)["errors"][0]["code"] == "SOURCE_SNAPSHOT_FAILED"


def test_explicit_sidecar_hint_moves_subtitle_without_requiring_video_mediainfo() -> None:
    video_sha1 = "A" * 40
    subtitle_sha1 = "B" * 40
    service, _client, _gateway, _repository, _container = _service(
        responses={
            ("source-root", 0): {
                "state": True,
                "data": [
                    _file("video", "Fixture.S01E02.mkv", video_sha1),
                    _file("subtitle", "Fixture.S01E02.zh.srt", subtitle_sha1, size=1000),
                ],
                "count": 2,
            },
            ("source-root", 2): {"state": True, "data": [], "count": 2},
        },
        media_info={video_sha1: {"snapshot_hash": "d" * 64, "has_video": True}},
        rules=[
            {
                "id": "tv",
                "name": "剧集",
                "cid": "target-tv",
                "category_path": "剧集",
                "media_type": "tv",
                "file_extensions": ["mkv"],
            }
        ],
    )
    common = {
        "title": "Fixture",
        "media_type": "tv",
        "tmdb_id": "100",
        "season_number": 1,
        "episode_number": 2,
    }

    result = service.create(
        {
            "request_intent_id": "moviepilot-video-sidecar",
            "source_file_ids": ["video", "subtitle"],
            "recognition_hints": {
                "video": common,
                "subtitle": {**common, "sidecar": True},
            },
        }
    )

    data = _plan_data(result)
    assert data["status"] == "ready"
    assert data["recognition_evidence"]["subtitle"]["sidecar"] is True
    moved_ids = {action["subject"].get("file_id") for action in data["actions"] if action["kind"] == "MOVE"}
    assert moved_ids == {"video", "subtitle"}


def test_tv_plan_deduplicates_directory_actions_and_is_byte_deterministic() -> None:
    sha1_a = "A" * 40
    sha1_b = "B" * 40
    responses = {
        ("source-root", 0): {
            "state": True,
            "data": [{"fid": "show-dir", "fn": "Show", "fc": 0}],
            "count": 1,
        },
        ("source-root", 1): {"state": True, "data": [], "count": 1},
        ("show-dir", 0): {
            "state": True,
            "data": [
                _file("ep-1", "Show.S01E01.mkv", sha1_a),
                _file("ep-2", "Show.S01E02.mkv", sha1_b),
            ],
            "count": 2,
        },
        ("show-dir", 2): {"state": True, "data": [], "count": 2},
    }
    media_info = {
        sha1_a: {"snapshot_hash": "1" * 64, "has_video": True, "stream_count": 1},
        sha1_b: {"snapshot_hash": "2" * 64, "has_video": True, "stream_count": 1},
    }
    rules = [
        {
            "id": "tv",
            "name": "剧集",
            "cid": "target-tv",
            "category_path": "剧集",
            "media_type": "tv",
        }
    ]
    hints = {
        "ep-1": {"title": "Show", "year": 2020, "media_type": "tv", "season_number": 1, "episode_number": 1},
        "ep-2": {"title": "Show", "year": 2020, "media_type": "tv", "season_number": 1, "episode_number": 2},
    }
    first, *_ = _service(responses=responses, rules=rules, media_info=media_info)
    second, *_ = _service(responses=responses, rules=rules, media_info=media_info)
    payload = {"request_intent_id": "fixture-tv", "recognition_hints": hints}

    first_data = _plan_data(first.create(payload))
    second_data = _plan_data(second.create(payload))

    assert first_data["status"] == "ready"
    assert [action["kind"] for action in first_data["actions"]] == [
        "ENSURE_DIRECTORY",
        "ENSURE_DIRECTORY",
        "MOVE",
        "RENAME",
        "MOVE",
        "RENAME",
    ]
    assert first_data["input_fingerprint"] == second_data["input_fingerprint"]
    assert first_data["plan_id"] == second_data["plan_id"]
    assert first_data["actions"] == second_data["actions"]


def test_missing_mediainfo_persists_a_blocked_plan_without_actions() -> None:
    case = FIXTURE_CASES["missing-mediainfo"]
    sha1 = "C" * 40
    service, _client, _gateway, repository, _container = _service(
        responses={
            ("source-root", 0): {"state": True, "data": [_file("movie-1", "Movie.mkv", sha1)]},
            ("source-root", 1): {"state": True, "data": []},
        },
    )
    result = service.create(
        {
            "request_intent_id": "fixture-missing-mediainfo",
            "recognition_hints": {"movie-1": {"title": "Movie", "media_type": "movie"}},
        }
    )

    data = _plan_data(result)
    assert data["status"] == case["expected_status"]
    assert [error["code"] for error in data["errors"]] == case["expected_error_codes"]
    assert [action["kind"] for action in data["actions"]] == case["expected_action_kinds"]
    assert repository.save_calls


def test_mixed_valid_and_blocked_media_never_persists_partial_actions() -> None:
    sha1_ready = "1" * 40
    sha1_missing = "2" * 40
    service, *_ = _service(
        responses={
            ("source-root", 0): {
                "state": True,
                "data": [
                    _file("ready", "Ready.mkv", sha1_ready),
                    _file("missing", "Missing.mkv", sha1_missing),
                ],
                "count": 2,
            },
            ("source-root", 2): {"state": True, "data": [], "count": 2},
        },
        media_info={sha1_ready: {"snapshot_hash": "a" * 64, "has_video": True, "stream_count": 1}},
    )

    data = _plan_data(
        service.create(
            {
                "request_intent_id": "fixture-mixed-blocked",
                "recognition_hints": {
                    "ready": {"title": "Ready", "media_type": "movie"},
                    "missing": {"title": "Missing", "media_type": "movie"},
                },
            }
        )
    )

    assert data["status"] == "blocked"
    assert {error["code"] for error in data["errors"]} == {"MEDIAINFO_REQUIRED"}
    assert data["actions"] == []


def test_duplicate_final_destination_blocks_the_whole_plan() -> None:
    sha1_a = "3" * 40
    sha1_b = "4" * 40
    service, *_ = _service(
        responses={
            ("source-root", 0): {
                "state": True,
                "data": [_file("a", "A.mkv", sha1_a), _file("b", "B.mkv", sha1_b)],
                "count": 2,
            },
            ("source-root", 2): {"state": True, "data": [], "count": 2},
        },
        media_info={
            sha1_a: {"snapshot_hash": "a" * 64, "has_video": True, "stream_count": 1},
            sha1_b: {"snapshot_hash": "b" * 64, "has_video": True, "stream_count": 1},
        },
    )

    data = _plan_data(
        service.create(
            {
                "request_intent_id": "fixture-duplicate-target",
                "recognition_hints": {
                    "a": {"title": "Same", "media_type": "movie"},
                    "b": {"title": "Same", "media_type": "movie"},
                },
            }
        )
    )

    assert data["status"] == "blocked"
    assert "DUPLICATE_TARGET_PATH" in {error["code"] for error in data["errors"]}
    assert data["actions"] == []


def test_cross_phase_destination_collision_blocks_the_whole_plan() -> None:
    class CrossPhaseNaming:
        def render_names(
            self,
            node: P115SourceNode,
            *,
            recognition: Mapping[str, Any],
            config: Mapping[str, Any],
        ) -> Mapping[str, str]:
            del recognition, config
            target_name = "C.mkv" if node.name == "B.mkv" else "D.mkv"
            return {"main_dir": "Movies", "season_dir": "", "file_name": target_name}

    sha1_b = "7" * 40
    sha1_c = "8" * 40
    service, *_ = _service(
        responses={
            ("source-root", 0): {
                "state": True,
                "data": [_file("b", "B.mkv", sha1_b), _file("c", "C.mkv", sha1_c)],
                "count": 2,
            },
            ("source-root", 2): {"state": True, "data": [], "count": 2},
        },
        media_info={
            sha1_b: {"snapshot_hash": "e" * 64, "has_video": True, "stream_count": 1},
            sha1_c: {"snapshot_hash": "f" * 64, "has_video": True, "stream_count": 1},
        },
        naming=CrossPhaseNaming(),
    )

    data = _plan_data(
        service.create(
            {
                "request_intent_id": "fixture-cross-phase-target",
                "recognition_hints": {
                    "b": {"title": "B", "media_type": "movie"},
                    "c": {"title": "C", "media_type": "movie"},
                },
            }
        )
    )

    collision = next(error for error in data["errors"] if error["code"] == "DUPLICATE_TARGET_PATH")
    assert data["status"] == "blocked"
    assert collision["phases"] == ["final", "move"]
    assert collision["source_ids"] == ["b", "c"]
    assert data["actions"] == []


def test_collision_identity_uses_target_cid_not_stale_category_display_paths() -> None:
    sha1_a = "A1" * 20
    sha1_b = "B2" * 20
    service, *_ = _service(
        responses={
            ("source-root", 0): {
                "state": True,
                "data": [_file("a", "A.mkv", sha1_a), _file("b", "B.mkv", sha1_b)],
                "count": 2,
            },
            ("source-root", 2): {"state": True, "data": [], "count": 2},
        },
        rules=[
            {
                "id": "stale-a",
                "cid": "same-target",
                "category_path": "旧路径一",
                "media_type": "movie",
                "genres": ["a"],
            },
            {
                "id": "stale-b",
                "cid": "same-target",
                "category_path": "旧路径二",
                "media_type": "movie",
                "genres": ["b"],
            },
        ],
        media_info={
            sha1_a: {"snapshot_hash": "1" * 64, "has_video": True, "stream_count": 1},
            sha1_b: {"snapshot_hash": "2" * 64, "has_video": True, "stream_count": 1},
        },
    )

    data = _plan_data(
        service.create(
            {
                "request_intent_id": "fixture-stale-category-path",
                "recognition_hints": {
                    "a": {"title": "Same", "media_type": "movie", "genre_ids": ["a"]},
                    "b": {"title": "Same", "media_type": "movie", "genre_ids": ["b"]},
                },
            }
        )
    )

    assert data["status"] == "blocked"
    assert "DUPLICATE_TARGET_PATH" in {error["code"] for error in data["errors"]}
    assert data["actions"] == []


def test_file_target_cannot_compete_with_an_ensured_directory_path() -> None:
    class FileDirectoryConflictNaming:
        def render_names(
            self,
            node: P115SourceNode,
            *,
            recognition: Mapping[str, Any],
            config: Mapping[str, Any],
        ) -> Mapping[str, str]:
            del recognition, config
            if node.id == "file-target":
                return {"main_dir": "Movies", "season_dir": "", "file_name": "Season.mkv"}
            return {"main_dir": "Movies/Season.mkv", "season_dir": "", "file_name": "Episode.mkv"}

    sha1_a = "C3" * 20
    sha1_b = "D4" * 20
    service, *_ = _service(
        responses={
            ("source-root", 0): {
                "state": True,
                "data": [
                    _file("file-target", "A.mkv", sha1_a),
                    _file("directory-target", "B.mkv", sha1_b),
                ],
                "count": 2,
            },
            ("source-root", 2): {"state": True, "data": [], "count": 2},
        },
        media_info={
            sha1_a: {"snapshot_hash": "3" * 64, "has_video": True, "stream_count": 1},
            sha1_b: {"snapshot_hash": "4" * 64, "has_video": True, "stream_count": 1},
        },
        naming=FileDirectoryConflictNaming(),
    )

    data = _plan_data(
        service.create(
            {
                "request_intent_id": "fixture-file-directory-conflict",
                "recognition_hints": {
                    "file-target": {"title": "A", "media_type": "movie"},
                    "directory-target": {"title": "B", "media_type": "movie"},
                },
            }
        )
    )

    collision = next(error for error in data["errors"] if error["code"] == "DUPLICATE_TARGET_PATH")
    assert data["status"] == "blocked"
    assert collision["phases"] == ["directory", "final"]
    assert collision["target_path"] == "movies/season.mkv"
    assert data["actions"] == []


def test_nested_directories_are_explicit_and_a_noop_rename_is_omitted() -> None:
    class NestedNoopNaming:
        def render_names(
            self,
            node: P115SourceNode,
            *,
            recognition: Mapping[str, Any],
            config: Mapping[str, Any],
        ) -> Mapping[str, str]:
            del recognition, config
            return {"main_dir": "Movies/2026", "season_dir": "", "file_name": node.name}

    sha1 = "5" * 40
    service, *_ = _service(
        responses={
            ("source-root", 0): {"state": True, "data": [_file("movie", "Movie.mkv", sha1)], "count": 1},
            ("source-root", 1): {"state": True, "data": [], "count": 1},
        },
        media_info={sha1: {"snapshot_hash": "c" * 64, "has_video": True, "stream_count": 1}},
        naming=NestedNoopNaming(),
    )

    data = _plan_data(
        service.create(
            {
                "request_intent_id": "fixture-nested-noop",
                "recognition_hints": {"movie": {"title": "Movie", "media_type": "movie"}},
            }
        )
    )

    assert [action["kind"] for action in data["actions"]] == ["ENSURE_DIRECTORY", "ENSURE_DIRECTORY", "MOVE"]
    assert data["actions"][1]["dependencies"] == [data["actions"][0]["action_id"]]
    assert data["actions"][2]["dependencies"] == [data["actions"][1]["action_id"]]


def test_sorting_account_mismatch_blocks_before_opening_provider() -> None:
    case = FIXTURE_CASES["sorting-account-mismatch"]
    service, _client, gateway, _repository, _container = _service(
        responses={},
        rules_account_id="115000",
    )

    data = _plan_data(service.create({"request_intent_id": "fixture-account-mismatch"}))

    assert data["status"] == case["expected_status"]
    assert [error["code"] for error in data["errors"]] == case["expected_error_codes"]
    assert len(gateway.open_calls) == case["provider_open_calls"]


def test_incomplete_or_repeated_provider_pages_never_produce_a_ready_plan() -> None:
    repeated = [_file("movie-1", "Movie.mkv", "D" * 40)]
    service, client, _gateway, _repository, _container = _service(
        responses={
            ("source-root", 0): {"state": True, "data": repeated, "count": 2},
            ("source-root", 1): {"state": True, "data": repeated, "count": 2},
        },
    )

    data = _plan_data(service.create({"request_intent_id": "fixture-repeated-page"}))

    assert data["status"] == "blocked"
    assert [error["code"] for error in data["errors"]] == ["SOURCE_SNAPSHOT_FAILED"]
    assert data["source_snapshot"] == []
    assert client is not None
    assert client.write_calls == []


def test_recognition_secrets_are_removed_before_hash_persistence_and_response() -> None:
    service, repository, payload = _ready_movie()
    repository.policy = {
        "studio_mapping": [
            {
                "label": "safe",
                "apiKey": "policy-secret",
                "nested": [[{"x-api-key": "nested-policy-secret"}]],
            }
        ]
    }
    payload["request_intent_id"] = "fixture-secret-redaction"
    payload["recognition_hints"]["movie-1"].update(
        {
            "access_token": "must-not-persist",
            "nested": {
                "cookie": "must-not-persist",
                "safe": "kept",
                "list": [[{"apiKey": "nested-caller-secret", "value": "kept"}]],
            },
        }
    )

    result = service.create(payload)
    rendered = json.dumps(result.payload, ensure_ascii=False)

    assert result.status_code == 200
    assert "must-not-persist" not in rendered
    assert "policy-secret" not in rendered
    assert "nested-policy-secret" not in rendered
    assert "nested-caller-secret" not in rendered
    persisted = json.dumps(repository.save_calls[0][0].to_dict(), ensure_ascii=False)
    assert "must-not-persist" not in persisted
    assert _plan_data(result)["recognition_evidence"]["movie-1"]["nested"] == {
        "safe": "kept",
        "list": [[{"value": "kept"}]],
    }


def test_auto_intent_creates_a_fresh_replan_while_effect_keys_remain_stable() -> None:
    service, repository, payload = _ready_movie()
    payload.pop("request_intent_id")

    first = _plan_data(service.create(payload))
    second = _plan_data(service.create(payload))

    assert first["request_intent_id"].startswith("auto-")
    assert second["request_intent_id"].startswith("auto-")
    assert first["request_intent_id"] != second["request_intent_id"]
    assert first["plan_id"] != second["plan_id"]
    assert [action["effect_key"] for action in first["actions"]] == [
        action["effect_key"] for action in second["actions"]
    ]
    assert len(repository.plans) == 2


def test_effect_keys_ignore_policy_only_fingerprint_drift() -> None:
    sha1 = "6" * 40
    responses = {
        ("source-root", 0): {"state": True, "data": [_file("movie", "Movie.mkv", sha1)], "count": 1},
        ("source-root", 1): {"state": True, "data": [], "count": 1},
    }
    media_info = {sha1: {"snapshot_hash": "d" * 64, "has_video": True, "stream_count": 1}}
    first_repository = FakePlanRepository(media_info=media_info, policy={"country_mapping": [{"label": "A"}]})
    second_repository = FakePlanRepository(media_info=media_info, policy={"country_mapping": [{"label": "B"}]})
    first, *_ = _service(responses=responses, repository=first_repository)
    second, *_ = _service(responses=responses, repository=second_repository)
    hints = {"movie": {"title": "Movie", "media_type": "movie"}}

    first_data = _plan_data(first.create({"request_intent_id": "policy-a", "recognition_hints": hints}))
    second_data = _plan_data(second.create({"request_intent_id": "policy-b", "recognition_hints": hints}))

    assert first_data["input_fingerprint"] != second_data["input_fingerprint"]
    assert [action["effect_key"] for action in first_data["actions"]] == [
        action["effect_key"] for action in second_data["actions"]
    ]


def test_effect_keys_include_the_stable_target_root_identity() -> None:
    sha1 = "9" * 40
    responses = {
        ("source-root", 0): {"state": True, "data": [_file("movie", "Movie.mkv", sha1)], "count": 1},
        ("source-root", 1): {"state": True, "data": [], "count": 1},
    }
    media_info = {sha1: {"snapshot_hash": "0" * 64, "has_video": True, "stream_count": 1}}
    first, *_ = _service(
        responses=responses,
        media_info=media_info,
        rules=[
            {
                "id": "movies-a",
                "name": "电影",
                "cid": "target-a",
                "category_path": "电影",
                "media_type": "movie",
            }
        ],
    )
    second, *_ = _service(
        responses=responses,
        media_info=media_info,
        rules=[
            {
                "id": "movies-b",
                "name": "电影",
                "cid": "target-b",
                "category_path": "电影",
                "media_type": "movie",
            }
        ],
    )
    payload = {"recognition_hints": {"movie": {"title": "Movie", "media_type": "movie"}}}

    first_actions = _plan_data(first.create(payload))["actions"]
    second_actions = _plan_data(second.create(payload))["actions"]

    assert [action["destination"]["target_root_cid"] for action in first_actions] == [
        "target-a",
        "target-a",
    ]
    assert [action["destination"]["target_root_cid"] for action in second_actions] == [
        "target-b",
        "target-b",
    ]
    assert [action["effect_key"] for action in first_actions] != [action["effect_key"] for action in second_actions]


def test_idempotent_replay_returns_the_existing_plan_and_conflicting_intent_is_409() -> None:
    service, repository, payload = _ready_movie()
    first = service.create(payload)
    replay = service.create(payload)
    conflicting = service.create(
        {
            **payload,
            "source_name": "另一个语义源",
        }
    )

    assert replay.payload == first.payload
    assert len(repository.plans) == 1
    assert conflicting.status_code == 409
    assert _result_payload(conflicting)["success"] is False


def test_http_create_and_get_plan_contract_and_json_transport_errors() -> None:
    service, repository, payload = _ready_movie()
    _service_value, _client, _gateway, _repository_value, container = _service()
    app = FastAPI()
    app.state.container = container
    app.state.p115_organize_plan_service = service
    codec = LegacySessionCodec.from_secret_key(b"p6-organize-plan-session")
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)

    with TestClient(app) as client:
        created = client.post("/api/p115/organize/plans/dry-run", json=payload)
        plan_id = created.json()["data"]["plan_id"]
        fetched = client.get(f"/api/p115/organize/plans/{plan_id}")
        missing = client.get("/api/p115/organize/plans/00000000-0000-0000-0000-000000000000")
        unsupported = client.post(
            "/api/p115/organize/plans/dry-run",
            content="{}",
            headers={"content-type": "text/plain"},
        )
        malformed = client.post(
            "/api/p115/organize/plans/dry-run",
            content="{",
            headers={"content-type": "application/json"},
        )
        scalar = client.post("/api/p115/organize/plans/dry-run", json=1)
        array = client.post("/api/p115/organize/plans/dry-run", json=[])
        wrong_field_type = client.post(
            "/api/p115/organize/plans/dry-run",
            json={"source_cid": {"not": "a string"}},
        )

    assert created.status_code == 200
    assert fetched.status_code == 200
    assert fetched.json() == created.json()
    assert missing.status_code == 404
    assert unsupported.status_code == 415
    assert unsupported.headers["content-type"].startswith("text/html")
    assert malformed.status_code == 400
    assert malformed.headers["content-type"].startswith("text/html")
    assert scalar.status_code == 400
    assert scalar.json() == {"success": False, "message": "整理计划请求格式错误"}
    assert array.status_code == 400
    assert wrong_field_type.status_code == 400
    assert len(repository.plans) == 1


def test_organize_plan_admin_denial_precedes_body_validation_and_planning() -> None:
    service, _client, gateway, repository, container = _service(auth_enabled=True)
    app = FastAPI()
    app.state.container = container
    app.state.p115_organize_plan_service = service
    codec = LegacySessionCodec.from_secret_key(b"p6-organize-plan-denial-session")
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)

    with TestClient(app) as client:
        response = client.post(
            "/api/p115/organize/plans/dry-run",
            content="{",
            headers={"content-type": "application/json"},
        )

    assert response.status_code == 403
    assert response.json()["status"] == "error"
    assert repository.plans == {}
    assert gateway.open_calls == []


def test_pure_compatibility_recognition_and_naming_adapters_never_construct_an_organizer() -> None:
    node = P115SourceNode(
        id="episode",
        parent_id="root",
        name="Breaking.Bad.S01E01.2160p.mkv",
        entry_kind="file",
        path="待整理/Breaking.Bad.S01E01.2160p.mkv",
        sha1="E" * 40,
        size=20 * 1024 * 1024,
        raw_hash="f" * 64,
    )
    recognition = NativeP115RuleRecognitionGateway().recognize(
        node,
        hints={
            "title": "绝命毒师",
            "year": 2008,
            "media_type": "tv",
            "tmdb_id": "1396",
            "season_number": 1,
            "episode_number": 1,
        },
        episode_rules=(),
        policy={},
    )
    names = NativeP115PlanNamingGateway().render_names(
        node,
        recognition=recognition,
        config={
            "main_dir_template": "{{title}} ({{year}}) {tmdb={{tmdbid}}}",
            "season_dir_template": "Season {{season_no}}",
            "tv_file_template": "{{title}} {{season_episode}}{{fileExt}}",
        },
    )

    assert recognition["media_type"] == "tv"
    assert recognition["season_number"] == 1
    assert names == {
        "main_dir": "绝命毒师 (2008) {tmdb=1396}",
        "season_dir": "Season 01",
        "file_name": "绝命毒师 S01E01.mkv",
    }


def test_cached_video_info_reaches_the_real_default_template_renderer() -> None:
    sha1 = "7" * 40
    service, *_ = _service(
        responses={
            ("source-root", 0): {
                "state": True,
                "data": [_file("movie", "Parasite.2019.mkv", sha1)],
                "count": 1,
            },
            ("source-root", 1): {"state": True, "data": [], "count": 1},
        },
        media_info={
            sha1: {
                "snapshot_hash": "e" * 64,
                "has_video": True,
                "stream_count": 2,
                "video_info": {
                    "resolution": "2160p",
                    "codec": "HEVC",
                    "audio": "TrueHD Atmos 7.1",
                },
            }
        },
        naming=NativeP115PlanNamingGateway(),
        rename_config={
            "main_dir_template": "{{title}}",
            "movie_file_template": "{{title}} {{resolution}} {{videoCodec}} {{audioCodec}}{{fileExt}}",
        },
    )

    data = _plan_data(
        service.create(
            {
                "request_intent_id": "fixture-real-media-naming",
                "recognition_hints": {
                    "movie": {"title": "寄生虫", "year": 2019, "media_type": "movie", "tmdb_id": "496243"}
                },
            }
        )
    )

    rename_action = next(action for action in data["actions"] if action["kind"] == "RENAME")
    assert rename_action["destination"]["name"] == "寄生虫 2160p HEVC TrueHD Atmos 7.1.mkv"


def test_rename_renderer_normalizes_templates_and_customization_rules(caplog: pytest.LogCaptureFixture) -> None:
    assert P115RenameRenderer.normalize_mp_jinja_template(None) == ""
    assert P115RenameRenderer.normalize_mp_jinja_template("{{ episode|string }.zfill(2) }} {{ fps }}FPS") == (
        "{{ (episode|string).zfill(2) }} {{ fps }}"
    )
    assert P115RenameRenderer.extract_customization("AA", "\nA\nA\n(?=A)\n[") == "A"
    assert P115RenameRenderer.extract_customization("NF AMZN", ["NF", "AMZN"]) == "NF@AMZN"
    assert P115RenameRenderer.extract_customization("NF", object()) == ""
    assert "忽略无效" in caplog.text


def test_rename_renderer_separator_cleanup_reaches_stable_and_bounded_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert P115RenameRenderer.cleanup_empty_separators("Movie -- .mkv") == "Movie.mkv"

    calls = 0

    def never_stable(_pattern: str, _replacement: str, value: str, *_args: Any, **_kwargs: Any) -> str:
        nonlocal calls
        calls += 1
        return f"{value}x"

    monkeypatch.setattr(rename_renderer_module.re, "sub", never_stable)
    assert P115RenameRenderer.cleanup_empty_separators("Movie")
    assert calls > 30


def test_rename_renderer_title_initial_and_label_fallbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    assert P115RenameRenderer.get_title_initial("---") == ""
    assert P115RenameRenderer.get_title_initial("-alpha") == "A"

    def fail_pinyin(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("fixture pinyin failure")

    monkeypatch.setattr(rename_renderer_module, "pinyin", fail_pinyin)
    assert P115RenameRenderer.get_title_initial("中") == "中"
    assert P115RenameRenderer.format_video_codec_label("HEVC AVC", "h265") == "H265 H264"
    assert P115RenameRenderer.format_video_codec_label("", "hevc") == ""
    assert P115RenameRenderer.format_audio_label("TrueHD 7.1", True) == "TrueHD"
    assert P115RenameRenderer.format_audio_label("TrueHD 7.1", False) == "TrueHD 7.1"
    assert P115RenameRenderer.format_fps_label("variable") == ("variable", "variable")


def test_rename_renderer_builds_actor_context_for_string_list_and_unknown_values() -> None:
    string_context = P115RenameRenderer({"actors": "Actor One"}).build_template_context()
    assert string_context["actors"] == "Actor One"

    list_context = P115RenameRenderer(
        {
            "actors": [
                {"name": "Actor One"},
                {"original_name": "Actor Two"},
                {},
                "Actor Three",
                "",
                "Ignored Sixth Actor",
            ]
        }
    ).build_template_context(is_tv=True, season_num=2, episode_num=3, original_name="Show.Part.2.mkv")
    assert list_context["actors"] == "Actor One、Actor Two、Actor Three"
    assert list_context["season_episode"] == "S02E03"
    assert list_context["part"] == "2"

    unknown_context = P115RenameRenderer({"actors": {"name": "ignored"}}).build_template_context()
    assert unknown_context["actors"] == ""


def test_rename_renderer_returns_empty_for_blank_or_failed_template() -> None:
    renderer = P115RenameRenderer({"title": "Fixture"})
    assert renderer.render_template(None) == ""
    assert renderer.render_template("{{ 1 / 0 }}") == ""


def test_default_language_mapping_participates_in_real_rule_recognition() -> None:
    sha1 = "8" * 40
    service, *_ = _service(
        responses={
            ("source-root", 0): {"state": True, "data": [_file("movie", "Movie.mkv", sha1)], "count": 1},
            ("source-root", 1): {"state": True, "data": [], "count": 1},
        },
        rules=[
            {
                "id": "japanese",
                "cid": "target-japanese",
                "category_path": "电影/日本",
                "media_type": "movie",
                "languages": ["日语"],
            }
        ],
        media_info={sha1: {"snapshot_hash": "f" * 64, "has_video": True, "stream_count": 1}},
        recognition=NativeP115RuleRecognitionGateway(),
    )

    data = _plan_data(
        service.create(
            {
                "request_intent_id": "fixture-language-mapping",
                "recognition_hints": {
                    "movie": {
                        "title": "Movie",
                        "media_type": "movie",
                        "lang_code": "ja",
                    }
                },
            }
        )
    )

    assert data["status"] == "ready"
    assert data["recognition_evidence"]["movie"]["languages"] == ["日语"]


def test_legacy_video_extension_and_unknown_size_remain_eligible() -> None:
    sha1 = "9" * 40
    service, *_ = _service(
        responses={
            ("source-root", 0): {
                "state": True,
                "data": [_file("movie", "Movie.flv", sha1, size=0)],
                "count": 1,
            },
            ("source-root", 1): {"state": True, "data": [], "count": 1},
        },
        media_info={sha1: {"snapshot_hash": "1" * 64, "has_video": True, "stream_count": 1}},
        extensions=("flv",),
        minimum_video_size=10,
    )

    data = _plan_data(
        service.create(
            {
                "request_intent_id": "fixture-flv-unknown-size",
                "recognition_hints": {"movie": {"title": "Movie", "media_type": "movie"}},
            }
        )
    )

    assert data["status"] == "ready"
    assert [action["kind"] for action in data["actions"]] == ["ENSURE_DIRECTORY", "MOVE"]


@pytest.mark.parametrize(
    ("filename", "main_dir", "season_subdirs", "forced_type", "expected"),
    [
        (
            "Movie.2020.tmdb-123.mkv",
            "",
            False,
            None,
            {"tmdb_id": "123", "media_type": "movie", "year": 2020, "confidence": "high"},
        ),
        (
            "Show Episode 3.mkv",
            "Show",
            True,
            None,
            {"media_type": "tv", "season_number": 1, "episode_number": 3, "confidence": "medium"},
        ),
        (
            "Film 2021.mkv",
            "",
            False,
            None,
            {"media_type": "movie", "year": 2021, "confidence": "medium"},
        ),
        (
            "OVA.mkv",
            "",
            False,
            None,
            {"media_type": "tv", "season_number": 0, "is_special": True, "confidence": "low"},
        ),
        (
            "Title.mkv",
            "",
            False,
            "invalid",
            {"media_type": None, "confidence": "low"},
        ),
        (
            "",
            "合集",
            False,
            "movies",
            {"title": "合集", "media_type": "movie", "confidence": "medium"},
        ),
    ],
)
def test_rule_parser_classifies_identity_structure_title_and_confidence(
    filename: str,
    main_dir: str,
    season_subdirs: bool,
    forced_type: str | None,
    expected: Mapping[str, Any],
) -> None:
    result = p115_adapters_module._build_rule_parse_result(
        filename,
        main_dir,
        season_subdirs,
        forced_type,
    )
    assert {key: result[key] for key in expected} == expected


def test_rule_parser_number_title_and_identity_helpers_cover_noise_boundaries() -> None:
    assert p115_adapters_module._normalize_rule_media_type("电影") == "movie"
    assert p115_adapters_module._normalize_rule_media_type("番剧") == "tv"
    assert p115_adapters_module._normalize_rule_media_type("music") is None

    assert p115_adapters_module._parse_rule_cn_number("") is None
    assert p115_adapters_module._parse_rule_cn_number("12") == 12
    assert p115_adapters_module._parse_rule_cn_number("十二") == 12
    assert p115_adapters_module._parse_rule_cn_number("两百三") == 203
    assert p115_adapters_module._parse_rule_cn_number("unknown") is None

    assert p115_adapters_module._clean_rule_title("") == ""
    assert p115_adapters_module._clean_rule_title("Movie.2020.2160p.WEB-DL.mkv") == "Movie"
    assert p115_adapters_module._clean_rule_title("2020.2160p.mkv") == ""
    assert p115_adapters_module._clean_rule_title("Clean_Title.mkv") == "Clean Title"
    assert p115_adapters_module._extract_rule_year("Movie 1999 Remaster 2020") == 2020
    assert p115_adapters_module._extract_rule_year("No year") is None
    assert p115_adapters_module._rule_detect_special("Show OVA") is True
    assert p115_adapters_module._rule_detect_special("Regular episode") is False
    assert p115_adapters_module._rule_extract_tmdb_id("Movie tmdb=12345") == "12345"
    assert p115_adapters_module._rule_extract_tmdb_id("Movie") is None
    assert p115_adapters_module._rule_extract_date_episode("News 2026-09-08") == 20260908
    assert p115_adapters_module._rule_extract_date_episode("News 20260908") == 20260908
    assert p115_adapters_module._rule_extract_date_episode("News") is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("", (None, None, [])),
        ("Show S01E02", (1, 2, ["sxe"])),
        ("Show Season 3 Episode 4", (3, 4, ["season_word", "episode_word"])),
        ("Show 第三季 第十二集", (3, 12, ["season_zh", "episode_zh"])),
        ("Show E5", (None, 5, ["episode_e"])),
        ("Show EP6", (1, 6, ["episode_word"])),
        ("News 2026-09-08", (None, 20260908, ["episode_date"])),
        ("Show OVA", (0, None, ["special"])),
        ("Show SP7", (None, 7, ["special", "episode_special_code"])),
    ],
)
def test_rule_parser_extracts_episode_notations(
    value: str,
    expected: tuple[int | None, int | None, list[str]],
) -> None:
    assert p115_adapters_module._rule_extract_season_episode_from_text(value) == expected
