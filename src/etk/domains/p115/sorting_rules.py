from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Protocol

from etk.platform import Settings

from .auth_directory import P115CredentialStore
from .models import P115Result

P115_SORTING_RULES_KEY = "p115_sorting_rules"
P115_SORTING_RULES_ACCOUNT_KEY = "p115_sorting_rules_account_id"


@dataclass(frozen=True, slots=True)
class P115SortingRulesSnapshot:
    rules: tuple[dict[str, Any], ...]
    account_id: str = ""

    @property
    def revision(self) -> str:
        payload = {"account_id": self.account_id, "rules": self.rules}
        encoded = json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        return hashlib.sha256(encoded).hexdigest()


class P115SortingRulesRepository(Protocol):
    def load(self) -> P115SortingRulesSnapshot: ...

    def save(self, rules: Sequence[Mapping[str, Any]], *, account_id: str) -> P115SortingRulesSnapshot: ...


class P115SortingRuleDirectoryGateway(Protocol):
    def is_available(self, *, expected_account_id: str) -> bool: ...

    def resolve_path(
        self,
        *,
        cid: str,
        expected_account_id: str,
    ) -> Sequence[Mapping[str, Any]] | None: ...


class P115SortingRulesService:
    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        credentials: P115CredentialStore,
        repository: P115SortingRulesRepository,
        directories: P115SortingRuleDirectoryGateway,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._settings = settings
        self._credentials = credentials
        self._repository = repository
        self._directories = directories
        self._clock = clock

    def list_rules(self) -> P115Result:
        snapshot = self._repository.load()
        rules = [deepcopy(rule) for rule in snapshot.rules]
        for rule in rules:
            if "id" not in rule:
                rule["id"] = str(int(self._clock() * 1000))
        return P115Result(rules)

    def save_rules(self, raw_rules: object) -> P115Result:
        try:
            rules = _rules(raw_rules)
            account_id = str(self._credentials.load().account_id or "").strip()

            if not account_id or not self._directories.is_available(expected_account_id=account_id):
                self._repository.save(rules, account_id=account_id)
                return P115Result({"status": "success", "message": "115 分类规则已保存"})

            previous = self._repository.load()
            previous_by_id = {
                str(rule.get("id")): rule
                for rule in previous.rules
                if rule.get("id") and previous.account_id == account_id
            }
            settings = self._settings()
            for rule in rules:
                rule_id = str(rule.get("id") or "")
                cid = str(rule.get("cid") or "").strip()
                old_rule = previous_by_id.get(rule_id)
                if old_rule is not None and str(old_rule.get("cid") or "") == cid and old_rule.get("category_path"):
                    rule["category_path"] = _strip_media_root_prefix(
                        old_rule.get("category_path"),
                        settings.p115_media_root_name,
                    )
                    continue

                resolved = None
                if account_id and cid and cid != "0":
                    try:
                        resolved = self._directories.resolve_path(
                            cid=cid,
                            expected_account_id=account_id,
                        )
                    except Exception:
                        resolved = None
                rule["category_path"] = _category_path(
                    rule,
                    resolved,
                    media_root_cid=str(settings.p115_media_root_cid or "0"),
                    media_root_name=str(settings.p115_media_root_name or ""),
                )

            self._repository.save(rules, account_id=account_id)
            return P115Result({"status": "success", "message": "115 分类规则已保存"})
        except Exception as exc:
            return P115Result({"status": "error", "message": str(exc)}, 500)


def _rules(raw: object) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    rules: list[dict[str, Any]] = []
    for index, value in enumerate(raw):
        if not isinstance(value, Mapping):
            raise TypeError(f"p115_sorting_rules[{index}] 必须是对象")
        rules.append(deepcopy(dict(value)))
    return rules


def _category_path(
    rule: Mapping[str, Any],
    path_nodes: Sequence[Mapping[str, Any]] | None,
    *,
    media_root_cid: str,
    media_root_name: str,
) -> str:
    fallback = str(rule.get("dir_name") or "")
    cid = str(rule.get("cid") or "")
    if not path_nodes:
        return _strip_media_root_prefix(rule.get("category_path") or fallback, media_root_name)

    start_index: int | None = None
    if media_root_cid == "0":
        start_index = 0 if cid == "0" else 1
    else:
        for index, node in enumerate(path_nodes):
            node_id = node.get("cid") or node.get("fid") or node.get("file_id") or node.get("id")
            if str(node_id or "") == media_root_cid:
                start_index = index + 1
                break
    if start_index is None or start_index >= len(path_nodes):
        return _strip_media_root_prefix(fallback, media_root_name)

    segments = []
    for node in path_nodes[start_index:]:
        name = node.get("file_name") or node.get("fn") or node.get("name") or node.get("n")
        if name:
            segments.append(str(name).strip())
    return _strip_media_root_prefix("/".join(segments) or fallback, media_root_name)


def _strip_media_root_prefix(value: object, media_root_name: object) -> str:
    path = str(value or "").strip().replace("\\", "/").strip("/")
    root = str(media_root_name or "").strip().replace("\\", "/").strip("/")
    if root and path == root:
        return ""
    if root and path.startswith(f"{root}/"):
        return path[len(root) + 1 :]
    return path


__all__ = [
    "P115SortingRuleDirectoryGateway",
    "P115SortingRulesRepository",
    "P115SortingRulesService",
    "P115SortingRulesSnapshot",
    "P115_SORTING_RULES_ACCOUNT_KEY",
    "P115_SORTING_RULES_KEY",
]
