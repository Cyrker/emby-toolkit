from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Protocol

from .models import P115Result

P115_EPISODE_REGEX_RULES_KEY = "p115_episode_regex_rules"


@dataclass(frozen=True, slots=True)
class P115EpisodeRegexSnapshot:
    rules: tuple[dict[str, Any], ...]

    @property
    def revision(self) -> str:
        encoded = json.dumps(
            self.rules,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class P115EpisodeMatch:
    season: int
    episode: int
    rule_name: str


class P115EpisodeRegexRepository(Protocol):
    def load(self) -> P115EpisodeRegexSnapshot: ...

    def save(self, rules: Sequence[Mapping[str, Any]]) -> P115EpisodeRegexSnapshot: ...


class P115EpisodeRegexService:
    def __init__(self, repository: P115EpisodeRegexRepository) -> None:
        self._repository = repository

    def list_rules(self) -> P115Result:
        rules = [deepcopy(rule) for rule in self._repository.load().rules]
        return P115Result({"success": True, "data": rules})

    def save_rules(self, raw_payload: object) -> P115Result:
        payload = raw_payload or {}
        if isinstance(payload, list):
            raw_rules: object = payload
        elif isinstance(payload, Mapping):
            raw_rules = payload.get("rules", [])
        else:
            raw_rules = []
        rules, error = normalize_episode_regex_rules(raw_rules)
        if error:
            return P115Result({"success": False, "message": error}, 400)
        self._repository.save(rules)
        return P115Result(
            {
                "success": True,
                "message": "自定义季集号识别规则已保存",
                "data": rules,
            }
        )


def normalize_episode_regex_rules(raw_rules: object) -> tuple[list[dict[str, Any]], str | None]:
    if not isinstance(raw_rules, list):
        return [], "规则数据格式错误，必须是数组"

    clean_rules: list[dict[str, Any]] = []
    for index, raw_item in enumerate(raw_rules):
        if not isinstance(raw_item, Mapping):
            continue
        item = dict(raw_item)
        name = str(item.get("name") or f"规则{index + 1}").strip()
        pattern = str(item.get("pattern") or "").strip()
        mode = str(item.get("mode") or "episode_only").strip()
        enabled = bool(item.get("enabled", True))
        if not pattern:
            continue
        if mode not in ("season_episode", "episode_only"):
            return [], f"第 {index + 1} 条规则模式非法: {mode}"
        try:
            re.compile(pattern, re.IGNORECASE)
        except re.error as exc:
            return [], f"第 {index + 1} 条规则正则语法错误: {exc}"
        try:
            season_group = int(item.get("season_group") or 1)
            episode_group = int(item.get("episode_group") or 1)
            raw_default_season = item.get("default_season")
            default_season = 1 if raw_default_season in (None, "") else int(raw_default_season)
        except Exception:
            return [], f"第 {index + 1} 条规则分组序号或默认季号必须是整数"
        if season_group < 1 or episode_group < 1:
            return [], f"第 {index + 1} 条规则捕获组序号必须 >= 1"
        if default_season < 0:
            return [], f"第 {index + 1} 条规则默认季号不能小于 0"
        clean_rules.append(
            {
                "id": str(item.get("id") or f"episode_regex_{index + 1}"),
                "enabled": enabled,
                "name": name,
                "pattern": pattern,
                "mode": mode,
                "season_group": season_group,
                "episode_group": episode_group,
                "default_season": default_season,
            }
        )
    return clean_rules, None


def match_episode_regex(
    rules: Sequence[Mapping[str, Any]],
    original_name: str,
    *,
    relative_path: str = "",
) -> P115EpisodeMatch | None:
    if not rules or not original_name:
        return None
    for index, rule in enumerate(rules):
        if not rule.get("enabled", True):
            continue
        rule_name = str(rule.get("name") or f"规则{index + 1}").strip()
        pattern = str(rule.get("pattern") or "").strip()
        mode = str(rule.get("mode") or "episode_only").strip()
        if not pattern:
            continue
        try:
            matched = re.search(pattern, original_name, re.IGNORECASE)
            if matched is None and relative_path:
                matched = re.search(pattern, relative_path, re.IGNORECASE)
        except re.error:
            continue
        if matched is None:
            continue
        if mode == "season_episode":
            season = _safe_group_to_int(matched, rule.get("season_group") or 1)
            episode = _safe_group_to_int(matched, rule.get("episode_group") or 2)
            if season is not None and episode is not None:
                return P115EpisodeMatch(season, episode, rule_name)
            continue
        episode = _safe_group_to_int(matched, rule.get("episode_group") or 1)
        raw_default_season = rule.get("default_season")
        default_season = 1 if raw_default_season in (None, "") else int(str(raw_default_season))
        if episode is not None:
            return P115EpisodeMatch(default_season, episode, rule_name)
    return None


def _safe_group_to_int(match: re.Match[str], group_index: object) -> int | None:
    try:
        if not group_index:
            return None
        value = match.group(int(str(group_index)))
        if value is None or not str(value).strip():
            return None
        return int(str(value).strip())
    except Exception:
        return None


__all__ = [
    "P115EpisodeMatch",
    "P115EpisodeRegexRepository",
    "P115EpisodeRegexService",
    "P115EpisodeRegexSnapshot",
    "P115_EPISODE_REGEX_RULES_KEY",
    "match_episode_regex",
    "normalize_episode_regex_rules",
]
