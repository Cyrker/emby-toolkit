from __future__ import annotations

import hashlib
import json
import os
import posixpath
import re
import uuid
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath
from typing import Any, Protocol, cast

from etk.platform import Settings

from .auth_directory import P115CredentialStore
from .directory_sync import P115DirectoryTreeClient, P115DirectoryTreeGateway
from .episode_regex import P115EpisodeRegexRepository
from .models import P115Result
from .rename_config import P115RenameConfigRepository, effective_rename_config
from .sorting_rules import P115SortingRulesRepository

P115_ORGANIZE_PLAN_SCHEMA_VERSION = 1
P115_ORGANIZE_PLANNER_VERSION = "p6.3.v2"
P115_ORGANIZE_PLAN_TTL = timedelta(hours=24)

_PAGE_LIMIT = 1000
_MAX_PAGES_PER_DIRECTORY = 1000
_MAX_SOURCE_NODES = 100_000
_MAX_SOURCE_DEPTH = 32
_VIDEO_EXTENSIONS = {"avi", "flv", "iso", "m2ts", "mkv", "mov", "mp4", "mpg", "rmvb", "ts", "wmv"}

P115_PLANNING_POLICY_KEYS = (
    "ai_recognition",
    "country_mapping",
    "junk_file_patterns",
    "keyword_mapping",
    "language_mapping",
    "p115_washing_priority_config",
    "rating_mapping",
    "rating_priority",
    "release_group_mapping",
    "studio_mapping",
)
P115_RUNTIME_CONFIG_KEYS = (
    "p115_download_subs",
    "p115_extensions",
    "p115_media_root_cid",
    "p115_media_root_name",
    "p115_mediainfo_assisted_recognition",
    "p115_min_video_size",
    "p115_save_path_cid",
    "p115_save_path_name",
    "p115_unrecognized_cid",
    "p115_unrecognized_name",
)
_SECRET_KEY_FRAGMENTS = ("authorization", "cookie", "password", "secret", "token", "api_key")


@dataclass(frozen=True, slots=True)
class P115SourceNode:
    id: str
    parent_id: str
    name: str
    entry_kind: str
    path: str
    sha1: str = ""
    pick_code: str = ""
    size: int = 0
    updated_at: str = ""
    raw_hash: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "parent_id": self.parent_id,
            "name": self.name,
            "entry_kind": self.entry_kind,
            "path": self.path,
            "sha1": self.sha1,
            "pick_code": self.pick_code,
            "size": self.size,
            "updated_at": self.updated_at,
            "raw_hash": self.raw_hash,
        }


@dataclass(frozen=True, slots=True)
class P115PlanAction:
    action_index: int
    action_id: str
    kind: str
    subject: dict[str, Any]
    destination: dict[str, Any]
    dependencies: tuple[str, ...]
    preconditions: dict[str, Any]
    effect_key: str
    decision_basis: dict[str, Any]
    risk: dict[str, Any]
    destructive: bool
    expected_postcondition: dict[str, Any]
    deferred_slice: str = "P6.4"

    def to_dict(self) -> dict[str, Any]:
        return {
            "action_index": self.action_index,
            "action_id": self.action_id,
            "kind": self.kind,
            "subject": deepcopy(self.subject),
            "destination": deepcopy(self.destination),
            "dependencies": list(self.dependencies),
            "preconditions": deepcopy(self.preconditions),
            "effect_key": self.effect_key,
            "decision_basis": deepcopy(self.decision_basis),
            "risk": deepcopy(self.risk),
            "destructive": self.destructive,
            "expected_postcondition": deepcopy(self.expected_postcondition),
            "deferred_slice": self.deferred_slice,
        }


@dataclass(frozen=True, slots=True)
class P115PlanCommitPreconditions:
    account_id: str
    sorting_rules_revision: str
    rename_config_revision: str
    episode_regex_revision: str
    policy_revision: str
    runtime_config_revision: str
    media_info_revisions: dict[str, str]


@dataclass(frozen=True, slots=True)
class P115PlanningSupportSnapshot:
    policy: dict[str, Any]
    policy_revision: str
    runtime_config_revision: str
    media_info: dict[str, dict[str, Any]]


@dataclass(frozen=True, slots=True)
class P115OrganizePlan:
    plan_id: str
    request_intent_id: str
    request_hash: str
    input_fingerprint: str
    status: str
    account_id: str
    source_root_cid: str
    source_root_name: str
    sorting_rules_revision: str
    rename_config_revision: str
    episode_regex_revision: str
    policy_revision: str
    runtime_config_revision: str
    config_snapshot: dict[str, Any]
    source_snapshot: tuple[dict[str, Any], ...]
    provider_evidence: dict[str, Any]
    recognition_evidence: dict[str, Any]
    warnings: tuple[dict[str, Any], ...]
    errors: tuple[dict[str, Any], ...]
    risk_summary: dict[str, Any]
    actions: tuple[P115PlanAction, ...]
    created_at: datetime
    expires_at: datetime
    schema_version: int = P115_ORGANIZE_PLAN_SCHEMA_VERSION
    planner_version: str = P115_ORGANIZE_PLANNER_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "schema_version": self.schema_version,
            "planner_version": self.planner_version,
            "request_intent_id": self.request_intent_id,
            "request_hash": self.request_hash,
            "input_fingerprint": self.input_fingerprint,
            "status": self.status,
            "account_id": self.account_id,
            "source_root": {"cid": self.source_root_cid, "name": self.source_root_name},
            "revisions": {
                "sorting_rules": self.sorting_rules_revision,
                "rename_config": self.rename_config_revision,
                "episode_regex": self.episode_regex_revision,
                "policy": self.policy_revision,
                "runtime_config": self.runtime_config_revision,
            },
            "config_snapshot": deepcopy(self.config_snapshot),
            "source_snapshot": deepcopy(list(self.source_snapshot)),
            "provider_evidence": deepcopy(self.provider_evidence),
            "recognition_evidence": deepcopy(self.recognition_evidence),
            "warnings": deepcopy(list(self.warnings)),
            "errors": deepcopy(list(self.errors)),
            "risk_summary": deepcopy(self.risk_summary),
            "actions": [action.to_dict() for action in self.actions],
            "created_at": self.created_at.astimezone(UTC).isoformat(),
            "expires_at": self.expires_at.astimezone(UTC).isoformat(),
        }


class P115OrganizePlanRepository(Protocol):
    def load_support_snapshot(self, sha1s: Sequence[str]) -> P115PlanningSupportSnapshot: ...

    def save(
        self,
        plan: P115OrganizePlan,
        *,
        preconditions: P115PlanCommitPreconditions,
    ) -> P115OrganizePlan: ...

    def get(self, plan_id: str) -> P115OrganizePlan | None: ...

    def assert_current(self, plan: P115OrganizePlan) -> None: ...


class P115PlanRecognitionGateway(Protocol):
    def recognize(
        self,
        node: P115SourceNode,
        *,
        hints: Mapping[str, Any],
        episode_rules: Sequence[Mapping[str, Any]],
        policy: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...


class P115PlanNamingGateway(Protocol):
    def render_names(
        self,
        node: P115SourceNode,
        *,
        recognition: Mapping[str, Any],
        config: Mapping[str, Any],
    ) -> Mapping[str, str]: ...


class P115PlanConflict(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class _PlannedMedia:
    node: P115SourceNode
    recognition: dict[str, Any]
    rule: dict[str, Any]
    names: dict[str, str]

    def fingerprint_value(self) -> dict[str, Any]:
        return {
            "source_id": self.node.id,
            "recognition": self.recognition,
            "rule": self.rule,
            "names": self.names,
        }


class P115OrganizePlanService:
    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        credentials: P115CredentialStore,
        directories: P115DirectoryTreeGateway,
        sorting_rules: P115SortingRulesRepository,
        rename_config: P115RenameConfigRepository,
        episode_regex: P115EpisodeRegexRepository,
        repository: P115OrganizePlanRepository,
        recognition: P115PlanRecognitionGateway,
        naming: P115PlanNamingGateway,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._settings = settings
        self._credentials = credentials
        self._directories = directories
        self._sorting_rules = sorting_rules
        self._rename_config = rename_config
        self._episode_regex = episode_regex
        self._repository = repository
        self._recognition = recognition
        self._naming = naming
        self._clock = clock or (lambda: datetime.now(UTC))

    def create(self, raw_payload: object) -> P115Result:
        payload = dict(raw_payload) if isinstance(raw_payload, Mapping) else {}
        sanitized_hints = _sanitize_mapping(payload.get("recognition_hints"))
        settings = self._settings()
        runtime_config = _runtime_config_snapshot(settings)
        credentials = self._credentials.load()
        account_id = str(credentials.account_id or "").strip()
        source_root_cid = str(payload.get("source_cid") or settings.p115_save_path_cid or "0").strip()
        source_root_name = str(payload.get("source_name") or settings.p115_save_path_name or "根目录").strip()
        request_semantics = {
            "source_cid": source_root_cid,
            "source_name": source_root_name,
            "recognition_hints": sanitized_hints,
        }
        request_hash = _hash_json(request_semantics)
        explicit_intent = str(payload.get("request_intent_id") or "").strip()
        if "request_intent_id" in payload and (not explicit_intent or len(explicit_intent) > 255):
            return P115Result(
                {"success": False, "message": "request_intent_id 必须是 1-255 个字符"},
                400,
            )

        sorting = self._sorting_rules.load()
        rename = self._rename_config.load()
        episode = self._episode_regex.load()
        effective_rename = effective_rename_config(rename.config)
        errors: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []
        nodes: tuple[P115SourceNode, ...] = ()
        provider_evidence: dict[str, Any] = {"complete": False, "directories": []}

        if not account_id:
            errors.append(_issue("ACCOUNT_REQUIRED", "无法确认稳定的 115 账号身份"))
        if sorting.account_id != account_id:
            errors.append(_issue("SORTING_RULES_ACCOUNT_MISMATCH", "分类规则未绑定当前 115 账号"))
        if not sorting.rules:
            errors.append(_issue("SORTING_RULES_REQUIRED", "未配置可用于规划的分类规则"))

        if not errors:
            client = self._directories.open(expected_account_id=account_id)
            if client is None:
                errors.append(_issue("DIRECTORY_CLIENT_UNAVAILABLE", "无法初始化只读 115 目录客户端"))
            else:
                try:
                    nodes, provider_evidence = self._read_source_snapshot(
                        client,
                        account_id=account_id,
                        root_cid=source_root_cid,
                        root_name=source_root_name,
                    )
                except Exception as exc:
                    errors.append(
                        _issue(
                            "SOURCE_SNAPSHOT_FAILED",
                            f"只读源快照失败: {type(exc).__name__}",
                        )
                    )

        support = self._repository.load_support_snapshot(sorted({node.sha1 for node in nodes if node.sha1}))
        if _runtime_config_snapshot(self._settings()) != runtime_config:
            return P115Result({"success": False, "message": "115 运行配置在 Dry Run 期间发生变化"}, 409)
        config_snapshot = {
            "runtime": runtime_config,
            "sorting_rules": [_sanitize_mapping(rule) for rule in sorting.rules],
            "rename_config": _sanitize_mapping(effective_rename),
            "episode_regex_rules": [_sanitize_mapping(rule) for rule in episode.rules],
            "policy": _sanitize_mapping(support.policy),
            "media_info": deepcopy(support.media_info),
        }
        planned_media: list[_PlannedMedia] = []
        recognition_evidence: dict[str, Any] = {}
        if not errors:
            planned_media = self._plan_media(
                nodes,
                sorting_rules=sorting.rules,
                rename_config=effective_rename,
                episode_rules=episode.rules,
                media_info=support.media_info,
                hints=sanitized_hints,
                policy=support.policy,
                settings=settings,
                recognition_evidence=recognition_evidence,
                warnings=warnings,
                errors=errors,
            )
        errors.extend(_target_collision_issues(planned_media))
        if not planned_media and not errors:
            errors.append(_issue("NO_ELIGIBLE_MEDIA", "源快照中没有可生成整理计划的媒体文件"))

        semantic_config = {key: value for key, value in runtime_config.items() if key != "p115_max_workers"}
        fingerprint_value = {
            "account_id": account_id,
            "source_root": {"cid": source_root_cid, "name": source_root_name},
            "source_snapshot": [node.to_dict() for node in nodes],
            "provider_evidence": provider_evidence,
            "semantic_runtime_config": semantic_config,
            "sorting_rules_revision": sorting.revision,
            "rename_config_revision": rename.revision,
            "episode_regex_revision": episode.revision,
            "policy_revision": support.policy_revision,
            "runtime_config_revision": support.runtime_config_revision,
            "recognition_hints": sanitized_hints,
            "planned_media": [media.fingerprint_value() for media in planned_media],
            "errors": errors,
            "warnings": warnings,
        }
        input_fingerprint = _hash_json(fingerprint_value)
        request_intent_id = explicit_intent or f"auto-{uuid.uuid4()}"
        plan_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"etk:p115-organize-plan:{request_intent_id}:{input_fingerprint}",
            )
        )
        actions = (
            ()
            if errors
            else _build_actions(
                planned_media,
                account_id=account_id,
                input_fingerprint=input_fingerprint,
                sorting_revision=sorting.revision,
                rename_revision=rename.revision,
                episode_revision=episode.revision,
                policy_revision_value=support.policy_revision,
                runtime_revision=support.runtime_config_revision,
            )
        )
        created_at = self._clock().astimezone(UTC)
        risk_summary = {
            "external_writes_executed": 0,
            "destructive_actions": sum(1 for action in actions if action.destructive),
            "planned_remote_mutations": sum(
                1 for action in actions if action.kind in {"ENSURE_DIRECTORY", "MOVE", "RENAME"}
            ),
            "blocked_items": len(errors),
            "requires_human_review": bool(actions or errors),
            "target_collision_policy": "fail_closed",
        }
        plan = P115OrganizePlan(
            plan_id=plan_id,
            request_intent_id=request_intent_id,
            request_hash=request_hash,
            input_fingerprint=input_fingerprint,
            status="blocked" if errors else "ready",
            account_id=account_id,
            source_root_cid=source_root_cid,
            source_root_name=source_root_name,
            sorting_rules_revision=sorting.revision,
            rename_config_revision=rename.revision,
            episode_regex_revision=episode.revision,
            policy_revision=support.policy_revision,
            runtime_config_revision=support.runtime_config_revision,
            config_snapshot=config_snapshot,
            source_snapshot=tuple(node.to_dict() for node in nodes),
            provider_evidence=provider_evidence,
            recognition_evidence=recognition_evidence,
            warnings=tuple(warnings),
            errors=tuple(errors),
            risk_summary=risk_summary,
            actions=actions,
            created_at=created_at,
            expires_at=created_at + P115_ORGANIZE_PLAN_TTL,
        )
        preconditions = P115PlanCommitPreconditions(
            account_id=account_id,
            sorting_rules_revision=sorting.revision,
            rename_config_revision=rename.revision,
            episode_regex_revision=episode.revision,
            policy_revision=support.policy_revision,
            runtime_config_revision=support.runtime_config_revision,
            media_info_revisions={
                node.sha1: str(support.media_info.get(node.sha1, {}).get("snapshot_hash") or "")
                for node in nodes
                if node.sha1
            },
        )
        try:
            saved = self._repository.save(plan, preconditions=preconditions)
        except P115PlanConflict as exc:
            return P115Result({"success": False, "message": str(exc)}, 409)
        except RuntimeError as exc:
            return P115Result({"success": False, "message": str(exc)}, 409)
        return P115Result({"success": True, "data": saved.to_dict()})

    def get(self, plan_id: str) -> P115Result:
        plan = self._repository.get(str(plan_id or "").strip())
        if plan is None:
            return P115Result({"success": False, "message": "整理计划不存在"}, 404)
        return P115Result({"success": True, "data": plan.to_dict()})

    def _plan_media(
        self,
        nodes: Sequence[P115SourceNode],
        *,
        sorting_rules: Sequence[Mapping[str, Any]],
        rename_config: Mapping[str, Any],
        episode_rules: Sequence[Mapping[str, Any]],
        media_info: Mapping[str, Mapping[str, Any]],
        hints: Mapping[str, Any],
        policy: Mapping[str, Any],
        settings: Settings,
        recognition_evidence: dict[str, Any],
        warnings: list[dict[str, Any]],
        errors: list[dict[str, Any]],
    ) -> list[_PlannedMedia]:
        planned: list[_PlannedMedia] = []
        allowed_extensions = {
            str(value).strip().lower().lstrip(".") for value in settings.p115_extensions if str(value).strip()
        }
        minimum_size = max(int(settings.p115_min_video_size), 0) * 1024 * 1024
        for node in sorted(nodes, key=lambda value: (value.path.casefold(), value.id)):
            if node.entry_kind != "file":
                continue
            extension = PurePosixPath(node.name).suffix.lower().lstrip(".")
            if extension not in allowed_extensions or extension not in _VIDEO_EXTENSIONS:
                continue
            if 0 < node.size < minimum_size:
                warnings.append(
                    _issue(
                        "SOURCE_BELOW_MINIMUM_SIZE",
                        "媒体文件小于配置的最小视频大小",
                        source_id=node.id,
                    )
                )
                continue
            if not node.sha1:
                errors.append(_issue("SOURCE_SHA1_REQUIRED", "源文件缺少 SHA1", source_id=node.id))
                continue
            media_evidence = media_info.get(node.sha1.upper()) or media_info.get(node.sha1)
            if not media_evidence or not media_evidence.get("has_video"):
                errors.append(
                    _issue(
                        "MEDIAINFO_REQUIRED",
                        "现有 MediaInfo 缓存缺失或不含视频轨，需在 P6.6 补齐后重新规划",
                        source_id=node.id,
                    )
                )
                continue
            hint_value = hints.get(node.id)
            raw_hint: Mapping[str, Any] = cast(Mapping[str, Any], hint_value) if isinstance(hint_value, Mapping) else {}
            try:
                recognition = _sanitize_mapping(
                    self._recognition.recognize(
                        node,
                        hints=raw_hint,
                        episode_rules=episode_rules,
                        policy=policy,
                    )
                )
            except Exception as exc:
                errors.append(
                    _issue(
                        "RECOGNITION_FAILED",
                        f"媒体识别失败: {type(exc).__name__}",
                        source_id=node.id,
                    )
                )
                continue
            recognition["media_info_evidence"] = deepcopy(dict(media_evidence))
            video_info = _filename_video_info(node.name)
            raw_recognition_video = recognition.get("video_info")
            if isinstance(raw_recognition_video, Mapping):
                video_info.update(
                    {
                        str(key): deepcopy(value)
                        for key, value in raw_recognition_video.items()
                        if value not in (None, "")
                    }
                )
            raw_cached_video = media_evidence.get("video_info")
            if isinstance(raw_cached_video, Mapping):
                video_info.update(
                    {str(key): deepcopy(value) for key, value in raw_cached_video.items() if value not in (None, "")}
                )
            recognition["video_info"] = video_info
            recognition_evidence[node.id] = deepcopy(recognition)
            if not recognition.get("title") or recognition.get("media_type") not in {"movie", "tv"}:
                errors.append(
                    _issue(
                        "RECOGNITION_INCOMPLETE",
                        "无法确定媒体标题或类型",
                        source_id=node.id,
                    )
                )
                continue
            matched_rule = _match_sorting_rule(sorting_rules, recognition, node.name)
            if matched_rule is None:
                errors.append(
                    _issue(
                        "SORTING_RULE_NOT_MATCHED",
                        "没有分类规则命中识别结果",
                        source_id=node.id,
                    )
                )
                continue
            target_cid = str(matched_rule.get("cid") or "").strip()
            if not target_cid or target_cid == "0":
                errors.append(
                    _issue(
                        "SORTING_TARGET_INVALID",
                        "命中的分类规则没有有效目标 CID",
                        source_id=node.id,
                    )
                )
                continue
            try:
                names = {
                    str(key): str(value)
                    for key, value in self._naming.render_names(
                        node,
                        recognition=recognition,
                        config=rename_config,
                    ).items()
                }
            except Exception as exc:
                errors.append(
                    _issue(
                        "NAMING_FAILED",
                        f"命名渲染失败: {type(exc).__name__}",
                        source_id=node.id,
                    )
                )
                continue
            if not names.get("main_dir") or not names.get("file_name"):
                errors.append(
                    _issue(
                        "NAMING_INCOMPLETE",
                        "命名结果缺少主目录或文件名",
                        source_id=node.id,
                    )
                )
                continue
            try:
                _directory_segments(names["main_dir"])
                if names.get("season_dir"):
                    _directory_segments(names["season_dir"])
                _file_name(names["file_name"])
            except ValueError as exc:
                errors.append(
                    _issue(
                        "NAMING_INVALID_PATH",
                        str(exc),
                        source_id=node.id,
                    )
                )
                continue
            planned.append(
                _PlannedMedia(
                    node=node,
                    recognition=recognition,
                    rule=deepcopy(dict(matched_rule)),
                    names=names,
                )
            )
        return planned

    def _read_source_snapshot(
        self,
        client: P115DirectoryTreeClient,
        *,
        account_id: str,
        root_cid: str,
        root_name: str,
    ) -> tuple[tuple[P115SourceNode, ...], dict[str, Any]]:
        queue: deque[tuple[str, str, int]] = deque([(root_cid, root_name, 0)])
        nodes: list[P115SourceNode] = []
        seen_ids: set[str] = set()
        scanned_directories: set[str] = set()
        directory_evidence: list[dict[str, Any]] = []
        while queue:
            cid, path, depth = queue.popleft()
            if cid in scanned_directories:
                raise RuntimeError("source directory graph contains a cycle")
            if depth > _MAX_SOURCE_DEPTH:
                raise RuntimeError("source directory depth exceeds the safe limit")
            scanned_directories.add(cid)
            children, evidence = self._read_directory_pages(client, cid=cid, account_id=account_id)
            directory_evidence.append({"cid": cid, "path": path, **evidence})
            for item in children:
                node = _source_node(item, parent_id=cid, parent_path=path)
                if node.id in seen_ids:
                    raise RuntimeError("source snapshot contains a duplicate item ID")
                seen_ids.add(node.id)
                nodes.append(node)
                if len(nodes) > _MAX_SOURCE_NODES:
                    raise RuntimeError("source snapshot exceeds the safe node limit")
                if node.entry_kind == "directory":
                    queue.append((node.id, node.path, depth + 1))
        self._assert_account(account_id)
        evidence = {
            "complete": True,
            "directory_count": len(directory_evidence),
            "node_count": len(nodes),
            "directories": directory_evidence,
            "snapshot_hash": _hash_json([node.to_dict() for node in nodes]),
        }
        return tuple(nodes), evidence

    def _read_directory_pages(
        self,
        client: P115DirectoryTreeClient,
        *,
        cid: str,
        account_id: str,
    ) -> tuple[list[Mapping[str, Any]], dict[str, Any]]:
        offset = 0
        nonempty_pages = 0
        received = 0
        reported_total: int | None = None
        items: list[Mapping[str, Any]] = []
        page_hashes: list[str] = []
        seen_page_hashes: set[str] = set()
        while True:
            self._assert_account(account_id)
            response = client.list_page(cid=cid, limit=_PAGE_LIMIT, offset=offset)
            if not isinstance(response, Mapping) or response.get("state") is not True:
                raise RuntimeError("provider returned a failed directory page")
            data = response.get("data")
            if not isinstance(data, list) or any(not isinstance(item, Mapping) for item in data):
                raise RuntimeError("provider returned an invalid directory page")
            if len(data) > _PAGE_LIMIT:
                raise RuntimeError("provider page exceeds the requested limit")
            response_total = _optional_count(response.get("count"))
            if response_total is not None:
                if reported_total is None:
                    reported_total = response_total
                elif reported_total != response_total:
                    raise RuntimeError("provider directory count changed during pagination")
            if not data:
                if reported_total is not None and reported_total != received:
                    raise RuntimeError("provider directory pagination is incomplete")
                break
            if nonempty_pages >= _MAX_PAGES_PER_DIRECTORY:
                raise RuntimeError("provider directory pagination exceeds the safe page limit")
            nonempty_pages += 1
            page_hash = _hash_json([dict(item) for item in data])
            if page_hash in seen_page_hashes:
                raise RuntimeError("provider repeated an identical directory page")
            seen_page_hashes.add(page_hash)
            page_hashes.append(page_hash)
            items.extend(deepcopy(dict(item)) for item in data)
            received += len(data)
            if reported_total is not None and received > reported_total:
                raise RuntimeError("provider directory pagination exceeds the reported count")
            offset += len(data)
        return items, {
            "reported_count": reported_total,
            "received_count": received,
            "nonempty_pages": nonempty_pages,
            "page_hashes": page_hashes,
            "listing_hash": _hash_json(page_hashes),
        }

    def _assert_account(self, expected_account_id: str) -> None:
        current = str(self._credentials.load().account_id or "").strip()
        if current != expected_account_id:
            raise RuntimeError("115 account changed during dry-run snapshot")


def _source_node(
    item: Mapping[str, Any],
    *,
    parent_id: str,
    parent_path: str,
) -> P115SourceNode:
    item_id = str(item.get("fid") or item.get("file_id") or item.get("id") or "").strip()
    name = str(item.get("fn") or item.get("file_name") or item.get("n") or "").strip()
    if not item_id or not name:
        raise RuntimeError("provider item is missing an ID or name")
    category = item.get("fc") if item.get("fc") is not None else item.get("type")
    if str(category) == "0":
        entry_kind = "directory"
    elif str(category) == "1" or item.get("sha") or item.get("sha1") or item.get("pc"):
        entry_kind = "file"
    else:
        raise RuntimeError("provider item has an unknown entry kind")
    size = _nonnegative_int(item.get("fs") if item.get("fs") is not None else item.get("size"))
    normalized = {
        "id": item_id,
        "parent_id": parent_id,
        "name": name,
        "entry_kind": entry_kind,
        "sha1": str(item.get("sha") or item.get("sha1") or "").strip().upper(),
        "pick_code": str(item.get("pc") or item.get("pick_code") or "").strip(),
        "size": size,
        "updated_at": str(item.get("te") or item.get("upt") or item.get("updated_at") or "").strip(),
    }
    return P115SourceNode(
        **normalized,
        path=posixpath.join(parent_path, name),
        raw_hash=_hash_json(normalized),
    )


def _match_sorting_rule(
    rules: Sequence[Mapping[str, Any]],
    recognition: Mapping[str, Any],
    source_name: str,
) -> Mapping[str, Any] | None:
    extension = os.path.splitext(source_name)[1].lower().lstrip(".")
    for rule in rules:
        if not rule.get("enabled", True):
            continue
        media_type = str(rule.get("media_type") or "all")
        if media_type != "all" and media_type != recognition.get("media_type"):
            continue
        rule_extensions = _string_set(rule.get("file_extensions"), strip_dot=True)
        if rule_extensions and extension not in rule_extensions:
            continue
        configured: list[bool] = []
        _condition(configured, rule.get("genres"), _overlaps(rule.get("genres"), recognition.get("genre_ids")))
        _condition(
            configured,
            rule.get("countries"),
            _overlaps(rule.get("countries"), recognition.get("country_codes") or recognition.get("countries")),
        )
        _condition(
            configured,
            rule.get("languages"),
            _overlaps(rule.get("languages"), recognition.get("languages") or [recognition.get("lang_code")]),
        )
        _condition(
            configured,
            rule.get("studios"),
            _overlaps(rule.get("studios"), recognition.get("studios") or recognition.get("company_ids")),
        )
        _condition(
            configured,
            rule.get("keywords"),
            _overlaps(rule.get("keywords"), recognition.get("keywords") or recognition.get("keyword_ids")),
        )
        _condition(
            configured,
            rule.get("ratings"),
            str(recognition.get("rating_label") or "").casefold() in _string_set(rule.get("ratings")),
        )
        _condition(
            configured,
            rule.get("actors"),
            _actor_overlap(rule.get("actors"), recognition.get("actor_ids")),
        )
        watching_status = rule.get("watching_status")
        if watching_status == "watching":
            configured.append(str(recognition.get("watching_status") or "").lower() == "watching")
        year = _optional_int(recognition.get("year"))
        year_min = _optional_int(rule.get("year_min"))
        year_max = _optional_int(rule.get("year_max"))
        if year_min is not None or year_max is not None:
            configured.append(
                year is not None and (year_min is None or year >= year_min) and (year_max is None or year <= year_max)
            )
        runtime = _optional_int(recognition.get("runtime"))
        runtime_min = _optional_int(rule.get("runtime_min"))
        runtime_max = _optional_int(rule.get("runtime_max"))
        if runtime_min is not None or runtime_max is not None:
            configured.append(
                runtime is not None
                and (runtime_min is None or runtime >= runtime_min)
                and (runtime_max is None or runtime <= runtime_max)
            )
        minimum_rating = _optional_float(rule.get("min_rating"))
        if minimum_rating is not None and minimum_rating > 0:
            vote_average = _optional_float(recognition.get("vote_average"))
            configured.append(vote_average is not None and vote_average >= minimum_rating)
        if not configured:
            return rule
        if str(rule.get("match_mode") or "and") == "or":
            if any(configured):
                return rule
        elif all(configured):
            return rule
    return None


def _build_actions(
    media: Sequence[_PlannedMedia],
    *,
    account_id: str,
    input_fingerprint: str,
    sorting_revision: str,
    rename_revision: str,
    episode_revision: str,
    policy_revision_value: str,
    runtime_revision: str,
) -> tuple[P115PlanAction, ...]:
    actions: list[P115PlanAction] = []
    ensure_by_path: dict[tuple[str, str], str] = {}
    base_preconditions = {
        "account_id": account_id,
        "sorting_rules_revision": sorting_revision,
        "rename_config_revision": rename_revision,
        "episode_regex_revision": episode_revision,
        "policy_revision": policy_revision_value,
        "runtime_config_revision": runtime_revision,
    }

    def add(
        kind: str,
        *,
        subject: Mapping[str, Any],
        destination: Mapping[str, Any],
        dependencies: Sequence[str] = (),
        preconditions: Mapping[str, Any] | None = None,
        decision_basis: Mapping[str, Any] | None = None,
        expected_postcondition: Mapping[str, Any] | None = None,
    ) -> str:
        index = len(actions)
        semantic = {
            "kind": kind,
            "account_id": account_id,
            "subject": _effect_identity(subject),
            "destination": _effect_identity(destination),
        }
        action_id = _hash_json(
            {
                "input_fingerprint": input_fingerprint,
                "index": index,
                "semantic": semantic,
            }
        )
        effect_key = _hash_json(semantic)
        actions.append(
            P115PlanAction(
                action_index=index,
                action_id=action_id,
                kind=kind,
                subject=deepcopy(dict(subject)),
                destination=deepcopy(dict(destination)),
                dependencies=tuple(dependencies),
                preconditions={**base_preconditions, **dict(preconditions or {})},
                effect_key=effect_key,
                decision_basis=deepcopy(dict(decision_basis or {})),
                risk={"remote_mutation": True, "requires_readback": True},
                destructive=False,
                expected_postcondition=deepcopy(dict(expected_postcondition or {})),
            )
        )
        return action_id

    for planned in media:
        rule_cid = str(planned.rule.get("cid") or "")
        category_path = str(
            planned.rule.get("category_path") or planned.rule.get("dir_name") or planned.rule.get("name") or rule_cid
        ).strip("/")
        parent_action = ""
        target_directory_path = ""
        main_directory_parts = _directory_segments(planned.names["main_dir"])
        directory_parts = list(main_directory_parts)
        if planned.recognition.get("media_type") == "tv" and planned.names.get("season_dir"):
            directory_parts.extend(_directory_segments(planned.names["season_dir"]))
        for part_index, directory_name in enumerate(directory_parts):
            target_directory_path = posixpath.join(target_directory_path, directory_name)
            directory_key = (rule_cid, target_directory_path.casefold())
            directory_action = ensure_by_path.get(directory_key)
            if directory_action is None:
                subject = {"parent_action_id": parent_action} if parent_action else {"parent_cid": rule_cid}
                destination = {
                    **subject,
                    "target_root_cid": rule_cid,
                    "name": directory_name,
                    "path": target_directory_path,
                }
                directory_action = add(
                    "ENSURE_DIRECTORY",
                    subject=subject,
                    destination=destination,
                    dependencies=(parent_action,) if parent_action else (),
                    preconditions={"expected_entry_kind": "directory"},
                    decision_basis={
                        "sorting_rule_id": planned.rule.get("id"),
                        "sorting_rule_name": planned.rule.get("name"),
                        "sorting_category_path": category_path,
                        "season_number": (
                            planned.recognition.get("season_number")
                            if part_index >= len(main_directory_parts)
                            else None
                        ),
                    },
                    expected_postcondition={
                        "target_root_cid": rule_cid,
                        "path": target_directory_path,
                        "visible_child_name": directory_name,
                    },
                )
                ensure_by_path[directory_key] = directory_action
            parent_action = directory_action
        source_preconditions = {
            "source_id": planned.node.id,
            "source_parent_id": planned.node.parent_id,
            "source_name": planned.node.name,
            "source_sha1": planned.node.sha1,
            "source_size": planned.node.size,
            "source_raw_hash": planned.node.raw_hash,
            "media_info_snapshot_hash": planned.recognition["media_info_evidence"]["snapshot_hash"],
        }
        source_identity = {
            "file_id": planned.node.id,
            "parent_id": planned.node.parent_id,
            "name": planned.node.name,
            "sha1": planned.node.sha1,
            "size": planned.node.size,
        }
        move_action = add(
            "MOVE",
            subject=source_identity,
            destination={
                "parent_action_id": parent_action,
                "target_root_cid": rule_cid,
                "path": target_directory_path,
                "intermediate_name": planned.node.name,
            },
            dependencies=(parent_action,),
            preconditions={
                **source_preconditions,
                "destination_must_be_absent": True,
            },
            decision_basis={
                "recognition": planned.recognition,
                "sorting_rule_id": planned.rule.get("id"),
            },
            expected_postcondition={
                "parent_action_id": parent_action,
                "target_root_cid": rule_cid,
                "path": posixpath.join(target_directory_path, planned.node.name),
            },
        )
        target_name = planned.names["file_name"]
        if target_name != planned.node.name:
            rename_subject = {
                "file_id": planned.node.id,
                "origin_parent_id": planned.node.parent_id,
                "origin_name": planned.node.name,
                "current_name": planned.node.name,
                "source_parent_action_id": parent_action,
                "sha1": planned.node.sha1,
                "size": planned.node.size,
            }
            add(
                "RENAME",
                subject=rename_subject,
                destination={
                    "parent_action_id": parent_action,
                    "target_root_cid": rule_cid,
                    "name": target_name,
                    "path": posixpath.join(target_directory_path, target_name),
                },
                dependencies=(move_action,),
                preconditions={
                    "source_id": planned.node.id,
                    "source_name": planned.node.name,
                    "source_sha1": planned.node.sha1,
                    "source_size": planned.node.size,
                    "source_parent_action_id": parent_action,
                    "media_info_snapshot_hash": planned.recognition["media_info_evidence"]["snapshot_hash"],
                    "destination_must_be_absent": True,
                },
                decision_basis={"rename_config_revision": rename_revision},
                expected_postcondition={
                    "target_root_cid": rule_cid,
                    "path": posixpath.join(target_directory_path, target_name),
                    "name": target_name,
                },
            )
    return tuple(actions)


def _target_collision_issues(media: Sequence[_PlannedMedia]) -> list[dict[str, Any]]:
    targets: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for planned in media:
        rule_cid, directory_path, final_path = _planned_target(planned)
        directory_parts = _planned_directory_parts(planned)
        for part_index in range(len(directory_parts)):
            path = posixpath.join(*directory_parts[: part_index + 1])
            targets.setdefault((rule_cid, path.casefold()), []).append(("directory", planned.node.id))
        for phase, path in (
            ("move", posixpath.join(directory_path, planned.node.name)),
            ("final", final_path),
        ):
            targets.setdefault((rule_cid, path.casefold()), []).append((phase, planned.node.id))
    issues: list[dict[str, Any]] = []
    for (rule_cid, path), reservations in sorted(targets.items()):
        source_ids = sorted({source_id for _phase, source_id in reservations})
        phases = {phase for phase, _source_id in reservations}
        file_source_ids = {source_id for phase, source_id in reservations if phase != "directory"}
        directory_file_conflict = "directory" in phases and bool(file_source_ids)
        duplicate_file_target = len(file_source_ids) > 1
        if not directory_file_conflict and not duplicate_file_target:
            continue
        issues.append(
            _issue(
                "DUPLICATE_TARGET_PATH",
                "多个源文件会写入同一目标路径，计划已阻断",
                phases=sorted(phases),
                target_root_cid=rule_cid,
                target_path=path,
                source_ids=source_ids,
            )
        )
    return issues


def _planned_target(planned: _PlannedMedia) -> tuple[str, str, str]:
    rule_cid = str(planned.rule.get("cid") or "")
    parts = _planned_directory_parts(planned)
    directory_path = posixpath.join(*parts)
    file_name = _file_name(planned.names["file_name"])
    return rule_cid, directory_path, posixpath.join(directory_path, file_name)


def _planned_directory_parts(planned: _PlannedMedia) -> tuple[str, ...]:
    parts = list(_directory_segments(planned.names["main_dir"]))
    if planned.recognition.get("media_type") == "tv" and planned.names.get("season_dir"):
        parts.extend(_directory_segments(planned.names["season_dir"]))
    return tuple(parts)


def _directory_segments(value: object) -> tuple[str, ...]:
    raw = str(value or "").replace("\\", "/")
    parts = tuple(part.strip() for part in raw.split("/") if part.strip())
    if not parts or any(part in {".", ".."} for part in parts):
        raise ValueError("命名结果包含无效目录路径")
    return parts


def _file_name(value: object) -> str:
    name = str(value or "").strip()
    if not name or name in {".", ".."} or "/" in name or "\\" in name:
        raise ValueError("命名结果包含无效文件名")
    return name


def _effect_identity(value: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): deepcopy(item) for key, item in value.items() if not str(key).endswith("_action_id")}


def _runtime_config_snapshot(settings: Settings) -> dict[str, Any]:
    return {
        "p115_download_subs": bool(settings.p115_download_subs),
        "p115_extensions": sorted(str(value).lower().lstrip(".") for value in settings.p115_extensions),
        "p115_max_workers": int(settings.p115_max_workers),
        "p115_media_root_cid": str(settings.p115_media_root_cid),
        "p115_media_root_name": str(settings.p115_media_root_name),
        "p115_mediainfo_assisted_recognition": bool(settings.p115_mediainfo_assisted_recognition),
        "p115_min_video_size": int(settings.p115_min_video_size),
        "p115_save_path_cid": str(settings.p115_save_path_cid),
        "p115_save_path_name": str(settings.p115_save_path_name),
        "p115_unrecognized_cid": str(settings.p115_unrecognized_cid),
        "p115_unrecognized_name": str(settings.p115_unrecognized_name),
    }


def policy_revision(values: Mapping[str, Any]) -> str:
    return _hash_json({key: deepcopy(values.get(key)) for key in P115_PLANNING_POLICY_KEYS})


def runtime_config_revision(values: Mapping[str, Any] | None) -> str:
    source = values if isinstance(values, Mapping) else {}
    return _hash_json({key: deepcopy(source.get(key)) for key in P115_RUNTIME_CONFIG_KEYS})


def _filename_video_info(filename: str) -> dict[str, Any]:
    upper = filename.upper()
    info: dict[str, Any] = {}
    resolution = re.search(r"(?<!\d)(2160|1080|720|480)P\b", upper)
    if resolution:
        info["resolution"] = f"{resolution.group(1)}p"
    elif re.search(r"(?<![A-Z0-9])4K(?![A-Z0-9])", upper):
        info["resolution"] = "2160p"
    if re.search(r"(?:H[. ]?265|X[. ]?265|HEVC)", upper):
        info["codec"] = "HEVC"
    elif re.search(r"(?:H[. ]?264|X[. ]?264|AVC)", upper):
        info["codec"] = "AVC"
    elif re.search(r"(?<![A-Z0-9])AV1(?![A-Z0-9])", upper):
        info["codec"] = "AV1"
    audio = next(
        (
            label
            for pattern, label in (
                (r"TRUEHD[. ]*ATMOS|ATMOS", "TrueHD Atmos"),
                (r"TRUEHD", "TrueHD"),
                (r"DTS[-. ]?HD(?:[. ]?MA)?", "DTS-HD MA"),
                (r"DTS", "DTS"),
                (r"DDP|EAC3|DOLBY[. ]?DIGITAL\+", "DDP"),
                (r"AC3", "AC3"),
                (r"AAC", "AAC"),
                (r"FLAC", "FLAC"),
            )
            if re.search(pattern, upper)
        ),
        "",
    )
    channels = re.search(r"(?<!\d)(7\.1|5\.1|2\.0)(?!\d)", filename)
    if audio or channels:
        info["audio"] = " ".join(value for value in (audio, channels.group(1) if channels else "") if value)
    group = re.search(r"-([A-Za-z0-9][A-Za-z0-9._-]{2,})$", os.path.splitext(filename)[0])
    if group:
        info["group"] = group.group(1)
    return info


def _issue(code: str, message: str, **details: Any) -> dict[str, Any]:
    return {"code": code, "message": message, **details}


def _condition(results: list[bool], configured_value: object, result: bool) -> None:
    if configured_value:
        results.append(result)


def _overlaps(left: object, right: object) -> bool:
    return bool(_string_set(left) & _string_set(right))


def _actor_overlap(rule_actors: object, actor_ids: object) -> bool:
    if not isinstance(rule_actors, Sequence) or isinstance(rule_actors, (str, bytes)):
        return False
    wanted = {
        str(actor.get("id")) for actor in rule_actors if isinstance(actor, Mapping) and actor.get("id") is not None
    }
    return bool(wanted & _string_set(actor_ids))


def _string_set(value: object, *, strip_dot: bool = False) -> set[str]:
    if value is None:
        return set()
    values = value if isinstance(value, Sequence) and not isinstance(value, (str, bytes)) else [value]
    result = set()
    for item in values:
        text = str(item or "").strip().casefold()
        if strip_dot:
            text = text.lstrip(".")
        if text:
            result.add(text)
    return result


def _optional_count(value: object) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        raise RuntimeError("provider returned an invalid count")
    parsed = int(str(value))
    if parsed < 0:
        raise RuntimeError("provider returned a negative count")
    return parsed


def _nonnegative_int(value: object) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, bool):
        raise RuntimeError("provider returned an invalid numeric value")
    parsed = int(str(value))
    if parsed < 0:
        raise RuntimeError("provider returned a negative numeric value")
    return parsed


def _optional_int(value: object) -> int | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _optional_float(value: object) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _sanitize_mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {
        str(raw_key): _sanitize_value(raw_value) for raw_key, raw_value in value.items() if not _secret_key(raw_key)
    }


def _sanitize_value(value: object) -> Any:
    if isinstance(value, Mapping):
        return _sanitize_mapping(value)
    if isinstance(value, (list, tuple)):
        return [_sanitize_value(item) for item in value]
    return deepcopy(value)


def _secret_key(value: object) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", str(value).casefold())
    return any(re.sub(r"[^a-z0-9]", "", fragment) in normalized for fragment in _SECRET_KEY_FRAGMENTS)


def _hash_json(value: object) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        default=str,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "P115OrganizePlan",
    "P115OrganizePlanRepository",
    "P115OrganizePlanService",
    "P115PlanAction",
    "P115PlanCommitPreconditions",
    "P115PlanConflict",
    "P115PlanNamingGateway",
    "P115PlanRecognitionGateway",
    "P115PlanningSupportSnapshot",
    "P115SourceNode",
    "P115_ORGANIZE_PLANNER_VERSION",
    "P115_ORGANIZE_PLAN_SCHEMA_VERSION",
    "P115_PLANNING_POLICY_KEYS",
    "P115_RUNTIME_CONFIG_KEYS",
    "policy_revision",
    "runtime_config_revision",
]
