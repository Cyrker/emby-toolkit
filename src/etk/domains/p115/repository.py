from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from datetime import datetime
from typing import Any

from sqlalchemy import text

from etk.platform.config.locking import DYNAMIC_CONFIG_LOCK_NAMESPACE, P115_AUTH_LOCK_KEY
from etk.platform.db import SqlAlchemyUnitOfWork

from .directory_sync import (
    P115DirectoryCacheEntry,
    P115DirectoryReconcileResult,
    P115DirectoryRoot,
)
from .episode_regex import (
    P115_EPISODE_REGEX_RULES_KEY,
    P115EpisodeRegexSnapshot,
)
from .organize_plan import (
    P115_PLANNING_POLICY_KEYS,
    P115OrganizePlan,
    P115PlanAction,
    P115PlanCommitPreconditions,
    P115PlanConflict,
    P115PlanningSupportSnapshot,
    policy_revision,
    runtime_config_revision,
)
from .rename_config import (
    P115_RENAME_CONFIG_KEY,
    P115RenameConfigSnapshot,
)
from .sorting_rules import (
    P115_SORTING_RULES_ACCOUNT_KEY,
    P115_SORTING_RULES_KEY,
    P115SortingRulesSnapshot,
)


class SqlAlchemyP115DirectoryCacheRepository:
    """Atomically upsert and reconcile one authoritative directory root."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def reconcile_children(
        self,
        root: P115DirectoryRoot,
        entries: Sequence[P115DirectoryCacheEntry],
        *,
        account_id: str,
    ) -> P115DirectoryReconcileResult:
        normalized_account_id = str(account_id or "").strip()
        if not normalized_account_id.isdigit():
            raise ValueError("directory cache reconciliation requires a stable numeric account ID")
        normalized = tuple(entries)
        if any(entry.parent_id != root.cid for entry in normalized):
            raise ValueError("directory cache entry parent does not match the reconciled root")
        if len({entry.id for entry in normalized}) != len(normalized):
            raise ValueError("directory cache entries contain duplicate IDs")
        if len({(entry.parent_id, entry.name) for entry in normalized}) != len(normalized):
            raise ValueError("directory cache schema cannot represent duplicate names under one parent")

        valid_ids = [entry.id for entry in normalized]
        with self._unit_of_work() as uow:
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(:namespace, :lock_key)"),
                {
                    "namespace": DYNAMIC_CONFIG_LOCK_NAMESPACE,
                    "lock_key": P115_AUTH_LOCK_KEY,
                },
            )
            persisted_account_id = str(
                uow.session.scalar(
                    text(
                        """
                        SELECT value_json ->> 'account_id'
                        FROM app_settings
                        WHERE setting_key = 'p115_auth_tokens'
                        """
                    )
                )
                or ""
            ).strip()
            if persisted_account_id != normalized_account_id:
                raise RuntimeError("115 账号在目录缓存提交前发生变化，事务已回滚")
            _locked_ids = tuple(
                uow.session.scalars(
                    text(
                        """
                        SELECT id
                        FROM p115_filesystem_cache
                        WHERE entry_kind = 'directory'
                          AND (
                              parent_id = :parent_id
                              OR account_id IS DISTINCT FROM :account_id
                          )
                        FOR UPDATE
                        """
                    ),
                    {"parent_id": root.cid, "account_id": normalized_account_id},
                )
            )
            conflict_cleaned = 0
            for entry in normalized:
                values = {
                    "id": entry.id,
                    "parent_id": entry.parent_id,
                    "name": entry.name,
                    "local_path": entry.local_path,
                    "account_id": normalized_account_id,
                }
                replaced = uow.session.execute(
                    text(
                        """
                        DELETE FROM p115_filesystem_cache
                        WHERE parent_id = :parent_id
                          AND name = :name
                          AND id <> :id
                          AND entry_kind = 'directory'
                        """
                    ),
                    values,
                )
                conflict_cleaned += _rowcount(replaced)
                updated = uow.session.execute(
                    text(
                        """
                        UPDATE p115_filesystem_cache
                        SET parent_id = :parent_id,
                            name = :name,
                            local_path = :local_path,
                            account_id = :account_id,
                            entry_kind = 'directory',
                            sha1 = NULL,
                            pick_code = NULL,
                            size = 0,
                            washing_level = NULL,
                            washing_snapshot_json = '{}'::jsonb,
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    values,
                )
                if _rowcount(updated):
                    continue
                uow.session.execute(
                    text(
                        """
                        INSERT INTO p115_filesystem_cache (
                            id,
                            parent_id,
                            name,
                            local_path,
                            account_id,
                            entry_kind
                        )
                        VALUES (
                            :id,
                            :parent_id,
                            :name,
                            :local_path,
                            :account_id,
                            'directory'
                        )
                        """
                    ),
                    values,
                )

            retired_accounts = uow.session.execute(
                text(
                    """
                    DELETE FROM p115_filesystem_cache
                    WHERE entry_kind = 'directory'
                      AND account_id IS DISTINCT FROM :account_id
                    """
                ),
                {"account_id": normalized_account_id},
            )

            if valid_ids:
                deleted = uow.session.execute(
                    text(
                        """
                        DELETE FROM p115_filesystem_cache
                        WHERE parent_id = :parent_id
                          AND account_id = :account_id
                          AND entry_kind = 'directory'
                          AND NOT (id = ANY(CAST(:valid_ids AS text[])))
                        """
                    ),
                    {
                        "parent_id": root.cid,
                        "account_id": normalized_account_id,
                        "valid_ids": valid_ids,
                    },
                )
            else:
                deleted = uow.session.execute(
                    text(
                        """
                        DELETE FROM p115_filesystem_cache
                        WHERE parent_id = :parent_id
                          AND account_id = :account_id
                          AND entry_kind = 'directory'
                        """
                    ),
                    {"parent_id": root.cid, "account_id": normalized_account_id},
                )
            cleaned = conflict_cleaned + _rowcount(retired_accounts) + _rowcount(deleted)
            uow.commit()
        return P115DirectoryReconcileResult(cached=len(normalized), cleaned=cleaned)


class SqlAlchemyP115SortingRulesRepository:
    """Persist rules and their owning account in one auth-serialized transaction."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def load(self) -> P115SortingRulesSnapshot:
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    """
                    SELECT setting_key, value_json
                    FROM app_settings
                    WHERE setting_key IN (:rules_key, :account_key)
                    """
                ),
                {
                    "rules_key": P115_SORTING_RULES_KEY,
                    "account_key": P115_SORTING_RULES_ACCOUNT_KEY,
                },
            ).mappings()
            values = {str(row["setting_key"]): deepcopy(row["value_json"]) for row in rows}
        return P115SortingRulesSnapshot(
            rules=tuple(_stored_rules(values.get(P115_SORTING_RULES_KEY))),
            account_id=str(values.get(P115_SORTING_RULES_ACCOUNT_KEY) or "").strip(),
        )

    def save(
        self,
        rules: Sequence[Mapping[str, Any]],
        *,
        account_id: str,
    ) -> P115SortingRulesSnapshot:
        with self._unit_of_work() as uow:
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(:namespace, :lock_key)"),
                {
                    "namespace": DYNAMIC_CONFIG_LOCK_NAMESPACE,
                    "lock_key": P115_AUTH_LOCK_KEY,
                },
            )
            snapshot = self.save_in_uow(uow, rules, account_id=account_id)
            uow.commit()
        return snapshot

    def save_in_uow(
        self,
        uow: SqlAlchemyUnitOfWork,
        rules: Sequence[Mapping[str, Any]],
        *,
        account_id: str,
    ) -> P115SortingRulesSnapshot:
        """Write into an existing transaction whose caller owns auth serialization."""
        normalized_account_id = str(account_id or "").strip()
        if normalized_account_id and not normalized_account_id.isdigit():
            raise ValueError("sorting rules account ID must be numeric when present")
        normalized_rules = tuple(deepcopy(dict(rule)) for rule in rules)
        persisted_account_id = str(
            uow.session.scalar(
                text(
                    """
                    SELECT value_json ->> 'account_id'
                    FROM app_settings
                    WHERE setting_key = 'p115_auth_tokens'
                    """
                )
            )
            or ""
        ).strip()
        if normalized_account_id and persisted_account_id != normalized_account_id:
            raise RuntimeError("115 账号在分类规则提交前发生变化，事务已回滚")
        _upsert_setting(uow, P115_SORTING_RULES_KEY, list(normalized_rules))
        _upsert_setting(uow, P115_SORTING_RULES_ACCOUNT_KEY, normalized_account_id)
        return P115SortingRulesSnapshot(normalized_rules, normalized_account_id)


class SqlAlchemyP115RenameConfigRepository:
    """Persist the independent 115 rename configuration as one JSON object."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def load(self) -> P115RenameConfigSnapshot:
        with self._unit_of_work() as uow:
            value = deepcopy(
                uow.session.scalar(
                    text(
                        """
                        SELECT value_json
                        FROM app_settings
                        WHERE setting_key = :key
                        """
                    ),
                    {"key": P115_RENAME_CONFIG_KEY},
                )
            )
        return P115RenameConfigSnapshot(dict(value) if isinstance(value, Mapping) else {})

    def save(self, config: Mapping[str, Any]) -> P115RenameConfigSnapshot:
        with self._unit_of_work() as uow:
            snapshot = self.save_in_uow(uow, config)
            uow.commit()
        return snapshot

    @staticmethod
    def save_in_uow(
        uow: SqlAlchemyUnitOfWork,
        config: Mapping[str, Any],
    ) -> P115RenameConfigSnapshot:
        normalized = deepcopy(dict(config))
        _upsert_setting(uow, P115_RENAME_CONFIG_KEY, normalized)
        return P115RenameConfigSnapshot(normalized)


class SqlAlchemyP115EpisodeRegexRepository:
    """Persist validated episode-recognition rules as one ordered JSON array."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def load(self) -> P115EpisodeRegexSnapshot:
        with self._unit_of_work() as uow:
            value = deepcopy(
                uow.session.scalar(
                    text(
                        """
                        SELECT value_json
                        FROM app_settings
                        WHERE setting_key = :key
                        """
                    ),
                    {"key": P115_EPISODE_REGEX_RULES_KEY},
                )
            )
        rules = (
            tuple(deepcopy(dict(rule)) for rule in value if isinstance(rule, Mapping))
            if isinstance(value, list)
            else ()
        )
        return P115EpisodeRegexSnapshot(rules)

    def save(self, rules: Sequence[Mapping[str, Any]]) -> P115EpisodeRegexSnapshot:
        normalized = tuple(deepcopy(dict(rule)) for rule in rules)
        with self._unit_of_work() as uow:
            _upsert_setting(uow, P115_EPISODE_REGEX_RULES_KEY, list(normalized))
            uow.commit()
        return P115EpisodeRegexSnapshot(normalized)


class SqlAlchemyP115OrganizePlanRepository:
    """Persist immutable Dry Run plans after rechecking all mutable input revisions."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def load_support_snapshot(self, sha1s: Sequence[str]) -> P115PlanningSupportSnapshot:
        normalized_sha1s = sorted({str(value or "").strip().upper() for value in sha1s if value})
        with self._unit_of_work() as uow:
            policy_rows = uow.session.execute(
                text(
                    """
                    SELECT setting_key, value_json
                    FROM app_settings
                    WHERE setting_key = ANY(CAST(:keys AS text[]))
                    """
                ),
                {"keys": list(P115_PLANNING_POLICY_KEYS)},
            ).mappings()
            policy = {key: None for key in P115_PLANNING_POLICY_KEYS}
            policy.update({str(row["setting_key"]): deepcopy(row["value_json"]) for row in policy_rows})
            dynamic_config = uow.session.scalar(
                text(
                    """
                    SELECT value_json
                    FROM app_settings
                    WHERE setting_key = 'dynamic_app_config'
                    """
                )
            )
            media_info: dict[str, dict[str, Any]] = {}
            if normalized_sha1s:
                media_rows = uow.session.execute(
                    text(
                        """
                        SELECT sha1, mediainfo_json
                        FROM p115_mediainfo_cache
                        WHERE UPPER(sha1) = ANY(CAST(:sha1s AS text[]))
                        """
                    ),
                    {"sha1s": normalized_sha1s},
                ).mappings()
                for row in media_rows:
                    sha1 = str(row["sha1"] or "").upper()
                    media_info[sha1] = _media_info_evidence(row["mediainfo_json"])
        return P115PlanningSupportSnapshot(
            policy=policy,
            policy_revision=policy_revision(policy),
            runtime_config_revision=runtime_config_revision(_mapping(dynamic_config)),
            media_info=media_info,
        )

    def save(
        self,
        plan: P115OrganizePlan,
        *,
        preconditions: P115PlanCommitPreconditions,
    ) -> P115OrganizePlan:
        with self._unit_of_work() as uow:
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(:namespace, :lock_key)"),
                {
                    "namespace": DYNAMIC_CONFIG_LOCK_NAMESPACE,
                    "lock_key": P115_AUTH_LOCK_KEY,
                },
            )
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:intent, 0))"),
                {"intent": plan.request_intent_id},
            )
            existing = (
                uow.session.execute(
                    text(
                        """
                    SELECT plan_id, request_hash, input_fingerprint
                    FROM p115_organize_plans
                    WHERE request_intent_id = :intent
                    """
                    ),
                    {"intent": plan.request_intent_id},
                )
                .mappings()
                .first()
            )
            if existing is not None:
                if (
                    str(existing["request_hash"]) != plan.request_hash
                    or str(existing["input_fingerprint"]) != plan.input_fingerprint
                ):
                    raise P115PlanConflict("request_intent_id 已用于不同的整理计划输入")
                loaded = _load_plan_with_session(uow, str(existing["plan_id"]))
                if loaded is None:
                    raise RuntimeError("已存在的整理计划无法读取")
                return loaded

            current = _locked_planning_settings(uow)
            _assert_plan_preconditions(current, preconditions)
            _assert_media_info_preconditions(uow, preconditions.media_info_revisions)
            uow.session.execute(
                text(
                    """
                    INSERT INTO p115_organize_plans (
                        plan_id,
                        schema_version,
                        planner_version,
                        request_intent_id,
                        request_hash,
                        input_fingerprint,
                        status,
                        account_id,
                        source_root_cid,
                        source_root_name,
                        sorting_rules_revision,
                        rename_config_revision,
                        episode_regex_revision,
                        policy_revision,
                        runtime_config_revision,
                        config_snapshot_json,
                        source_snapshot_json,
                        provider_evidence_json,
                        recognition_evidence_json,
                        warnings_json,
                        errors_json,
                        risk_summary_json,
                        created_at,
                        expires_at
                    )
                    VALUES (
                        :plan_id,
                        :schema_version,
                        :planner_version,
                        :request_intent_id,
                        :request_hash,
                        :input_fingerprint,
                        :status,
                        :account_id,
                        :source_root_cid,
                        :source_root_name,
                        :sorting_rules_revision,
                        :rename_config_revision,
                        :episode_regex_revision,
                        :policy_revision,
                        :runtime_config_revision,
                        CAST(:config_snapshot_json AS jsonb),
                        CAST(:source_snapshot_json AS jsonb),
                        CAST(:provider_evidence_json AS jsonb),
                        CAST(:recognition_evidence_json AS jsonb),
                        CAST(:warnings_json AS jsonb),
                        CAST(:errors_json AS jsonb),
                        CAST(:risk_summary_json AS jsonb),
                        :created_at,
                        :expires_at
                    )
                    """
                ),
                _plan_insert_values(plan),
            )
            for action in plan.actions:
                uow.session.execute(
                    text(
                        """
                        INSERT INTO p115_organize_plan_actions (
                            plan_id,
                            action_index,
                            action_id,
                            kind,
                            subject_json,
                            destination_json,
                            dependencies_json,
                            preconditions_json,
                            effect_key,
                            decision_basis_json,
                            risk_json,
                            destructive,
                            expected_postcondition_json,
                            deferred_slice
                        )
                        VALUES (
                            :plan_id,
                            :action_index,
                            :action_id,
                            :kind,
                            CAST(:subject_json AS jsonb),
                            CAST(:destination_json AS jsonb),
                            CAST(:dependencies_json AS jsonb),
                            CAST(:preconditions_json AS jsonb),
                            :effect_key,
                            CAST(:decision_basis_json AS jsonb),
                            CAST(:risk_json AS jsonb),
                            :destructive,
                            CAST(:expected_postcondition_json AS jsonb),
                            :deferred_slice
                        )
                        """
                    ),
                    _action_insert_values(plan.plan_id, action),
                )
            uow.commit()
        return plan

    def get(self, plan_id: str) -> P115OrganizePlan | None:
        with self._unit_of_work() as uow:
            return _load_plan_with_session(uow, plan_id)

    def assert_current(self, plan: P115OrganizePlan) -> None:
        with self._unit_of_work() as uow:
            assert_organize_plan_current(uow, plan)
            uow.commit()


def assert_organize_plan_current(
    uow: SqlAlchemyUnitOfWork,
    plan: P115OrganizePlan,
    *,
    require_unexpired: bool = False,
) -> None:
    uow.session.execute(
        text("SELECT pg_advisory_xact_lock(:namespace, :lock_key)"),
        {
            "namespace": DYNAMIC_CONFIG_LOCK_NAMESPACE,
            "lock_key": P115_AUTH_LOCK_KEY,
        },
    )
    persisted = (
        uow.session.execute(
            text(
                """
            SELECT plan_id, status, expires_at
            FROM p115_organize_plans
            WHERE plan_id = :plan_id
            FOR UPDATE
            """
            ),
            {"plan_id": plan.plan_id},
        )
        .mappings()
        .one_or_none()
    )
    if persisted is None or str(persisted["status"]) != "ready":
        raise RuntimeError("115 整理计划已不存在或不再可执行")
    if require_unexpired:
        expired = bool(
            uow.session.scalar(
                text("SELECT :expires_at <= NOW()"),
                {"expires_at": persisted["expires_at"]},
            )
        )
        if expired:
            raise RuntimeError("115 整理计划已过期，请重新生成 Dry Run")
    current = _locked_planning_settings(uow)
    preconditions = _execution_plan_preconditions(plan)
    _assert_plan_preconditions(current, preconditions)
    _assert_media_info_preconditions(uow, preconditions.media_info_revisions)


def _execution_plan_preconditions(plan: P115OrganizePlan) -> P115PlanCommitPreconditions:
    media_info_revisions: dict[str, str] = {}
    for action in plan.actions:
        sha1 = str(action.preconditions.get("source_sha1") or "").strip().upper()
        revision = str(action.preconditions.get("media_info_snapshot_hash") or "").strip()
        if not sha1 or not revision:
            continue
        previous = media_info_revisions.setdefault(sha1, revision)
        if previous != revision:
            raise RuntimeError("115 整理计划包含冲突的 MediaInfo 快照")
    return P115PlanCommitPreconditions(
        account_id=plan.account_id,
        sorting_rules_revision=plan.sorting_rules_revision,
        rename_config_revision=plan.rename_config_revision,
        episode_regex_revision=plan.episode_regex_revision,
        policy_revision=plan.policy_revision,
        runtime_config_revision=plan.runtime_config_revision,
        media_info_revisions=media_info_revisions,
    )


def _rowcount(result: object) -> int:
    return max(int(getattr(result, "rowcount", 0) or 0), 0)


def _stored_rules(value: object) -> list[dict[str, Any]]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return []
    if not isinstance(value, list):
        return []
    return [deepcopy(dict(rule)) for rule in value if isinstance(rule, Mapping)]


def _upsert_setting(uow: SqlAlchemyUnitOfWork, key: str, value: object) -> None:
    uow.session.execute(
        text(
            """
            INSERT INTO app_settings (setting_key, value_json, last_updated_at)
            VALUES (:key, CAST(:value AS jsonb), NOW())
            ON CONFLICT (setting_key) DO UPDATE SET
                value_json = EXCLUDED.value_json,
                last_updated_at = NOW()
            """
        ),
        {"key": key, "value": json.dumps(value, ensure_ascii=False)},
    )


def _locked_planning_settings(uow: SqlAlchemyUnitOfWork) -> dict[str, Any]:
    keys = [
        "p115_auth_tokens",
        P115_SORTING_RULES_KEY,
        P115_SORTING_RULES_ACCOUNT_KEY,
        P115_RENAME_CONFIG_KEY,
        P115_EPISODE_REGEX_RULES_KEY,
        "dynamic_app_config",
        *P115_PLANNING_POLICY_KEYS,
    ]
    rows = uow.session.execute(
        text(
            """
            SELECT setting_key, value_json
            FROM app_settings
            WHERE setting_key = ANY(CAST(:keys AS text[]))
            FOR UPDATE
            """
        ),
        {"keys": keys},
    ).mappings()
    values = {key: None for key in keys}
    values.update({str(row["setting_key"]): deepcopy(row["value_json"]) for row in rows})
    return values


def _assert_plan_preconditions(
    values: Mapping[str, Any],
    expected: P115PlanCommitPreconditions,
) -> None:
    auth = values.get("p115_auth_tokens")
    account_id = str(auth.get("account_id") or "").strip() if isinstance(auth, Mapping) else ""
    if account_id != expected.account_id:
        raise RuntimeError("115 账号在 Dry Run Plan 提交前发生变化")
    sorting = P115SortingRulesSnapshot(
        tuple(_stored_rules(values.get(P115_SORTING_RULES_KEY))),
        str(values.get(P115_SORTING_RULES_ACCOUNT_KEY) or "").strip(),
    )
    if sorting.revision != expected.sorting_rules_revision:
        raise RuntimeError("115 分类规则在 Dry Run Plan 提交前发生变化")
    raw_rename = values.get(P115_RENAME_CONFIG_KEY)
    rename = P115RenameConfigSnapshot(dict(raw_rename) if isinstance(raw_rename, Mapping) else {})
    if rename.revision != expected.rename_config_revision:
        raise RuntimeError("115 重命名配置在 Dry Run Plan 提交前发生变化")
    raw_episode = values.get(P115_EPISODE_REGEX_RULES_KEY)
    episode_rules = (
        tuple(deepcopy(dict(rule)) for rule in raw_episode if isinstance(rule, Mapping))
        if isinstance(raw_episode, list)
        else ()
    )
    episode = P115EpisodeRegexSnapshot(episode_rules)
    if episode.revision != expected.episode_regex_revision:
        raise RuntimeError("115 季集识别规则在 Dry Run Plan 提交前发生变化")
    current_policy = {key: deepcopy(values.get(key)) for key in P115_PLANNING_POLICY_KEYS}
    if policy_revision(current_policy) != expected.policy_revision:
        raise RuntimeError("115 整理策略在 Dry Run Plan 提交前发生变化")
    if runtime_config_revision(_mapping(values.get("dynamic_app_config"))) != expected.runtime_config_revision:
        raise RuntimeError("115 运行配置在 Dry Run Plan 提交前发生变化")


def _assert_media_info_preconditions(
    uow: SqlAlchemyUnitOfWork,
    expected: Mapping[str, str],
) -> None:
    normalized = {str(sha1).strip().upper(): str(revision or "") for sha1, revision in expected.items()}
    if not normalized:
        return
    rows = uow.session.execute(
        text(
            """
            SELECT sha1, mediainfo_json
            FROM p115_mediainfo_cache
            WHERE UPPER(sha1) = ANY(CAST(:sha1s AS text[]))
            FOR UPDATE
            """
        ),
        {"sha1s": sorted(normalized)},
    ).mappings()
    actual = {
        str(row["sha1"] or "").upper(): str(_media_info_evidence(row["mediainfo_json"])["snapshot_hash"])
        for row in rows
    }
    if {sha1: actual.get(sha1, "") for sha1 in normalized} != normalized:
        raise RuntimeError("115 MediaInfo 缓存在 Dry Run Plan 提交前发生变化")


def _plan_insert_values(plan: P115OrganizePlan) -> dict[str, Any]:
    return {
        "plan_id": plan.plan_id,
        "schema_version": plan.schema_version,
        "planner_version": plan.planner_version,
        "request_intent_id": plan.request_intent_id,
        "request_hash": plan.request_hash,
        "input_fingerprint": plan.input_fingerprint,
        "status": plan.status,
        "account_id": plan.account_id,
        "source_root_cid": plan.source_root_cid,
        "source_root_name": plan.source_root_name,
        "sorting_rules_revision": plan.sorting_rules_revision,
        "rename_config_revision": plan.rename_config_revision,
        "episode_regex_revision": plan.episode_regex_revision,
        "policy_revision": plan.policy_revision,
        "runtime_config_revision": plan.runtime_config_revision,
        "config_snapshot_json": _json(plan.config_snapshot),
        "source_snapshot_json": _json(list(plan.source_snapshot)),
        "provider_evidence_json": _json(plan.provider_evidence),
        "recognition_evidence_json": _json(plan.recognition_evidence),
        "warnings_json": _json(list(plan.warnings)),
        "errors_json": _json(list(plan.errors)),
        "risk_summary_json": _json(plan.risk_summary),
        "created_at": plan.created_at,
        "expires_at": plan.expires_at,
    }


def _action_insert_values(plan_id: str, action: P115PlanAction) -> dict[str, Any]:
    return {
        "plan_id": plan_id,
        "action_index": action.action_index,
        "action_id": action.action_id,
        "kind": action.kind,
        "subject_json": _json(action.subject),
        "destination_json": _json(action.destination),
        "dependencies_json": _json(list(action.dependencies)),
        "preconditions_json": _json(action.preconditions),
        "effect_key": action.effect_key,
        "decision_basis_json": _json(action.decision_basis),
        "risk_json": _json(action.risk),
        "destructive": action.destructive,
        "expected_postcondition_json": _json(action.expected_postcondition),
        "deferred_slice": action.deferred_slice,
    }


def _load_plan_with_session(
    uow: SqlAlchemyUnitOfWork,
    plan_id: str,
) -> P115OrganizePlan | None:
    row = (
        uow.session.execute(
            text("SELECT * FROM p115_organize_plans WHERE plan_id = :plan_id"),
            {"plan_id": plan_id},
        )
        .mappings()
        .first()
    )
    if row is None:
        return None
    action_rows = uow.session.execute(
        text(
            """
            SELECT *
            FROM p115_organize_plan_actions
            WHERE plan_id = :plan_id
            ORDER BY action_index
            """
        ),
        {"plan_id": plan_id},
    ).mappings()
    actions = tuple(
        P115PlanAction(
            action_index=int(action["action_index"]),
            action_id=str(action["action_id"]),
            kind=str(action["kind"]),
            subject=deepcopy(dict(action["subject_json"])),
            destination=deepcopy(dict(action["destination_json"])),
            dependencies=tuple(str(value) for value in action["dependencies_json"]),
            preconditions=deepcopy(dict(action["preconditions_json"])),
            effect_key=str(action["effect_key"]),
            decision_basis=deepcopy(dict(action["decision_basis_json"])),
            risk=deepcopy(dict(action["risk_json"])),
            destructive=bool(action["destructive"]),
            expected_postcondition=deepcopy(dict(action["expected_postcondition_json"])),
            deferred_slice=str(action["deferred_slice"]),
        )
        for action in action_rows
    )
    return P115OrganizePlan(
        plan_id=str(row["plan_id"]),
        schema_version=int(row["schema_version"]),
        planner_version=str(row["planner_version"]),
        request_intent_id=str(row["request_intent_id"]),
        request_hash=str(row["request_hash"]),
        input_fingerprint=str(row["input_fingerprint"]),
        status=str(row["status"]),
        account_id=str(row["account_id"]),
        source_root_cid=str(row["source_root_cid"]),
        source_root_name=str(row["source_root_name"]),
        sorting_rules_revision=str(row["sorting_rules_revision"]),
        rename_config_revision=str(row["rename_config_revision"]),
        episode_regex_revision=str(row["episode_regex_revision"]),
        policy_revision=str(row["policy_revision"]),
        runtime_config_revision=str(row["runtime_config_revision"]),
        config_snapshot=deepcopy(dict(row["config_snapshot_json"])),
        source_snapshot=tuple(deepcopy(dict(value)) for value in row["source_snapshot_json"]),
        provider_evidence=deepcopy(dict(row["provider_evidence_json"])),
        recognition_evidence=deepcopy(dict(row["recognition_evidence_json"])),
        warnings=tuple(deepcopy(dict(value)) for value in row["warnings_json"]),
        errors=tuple(deepcopy(dict(value)) for value in row["errors_json"]),
        risk_summary=deepcopy(dict(row["risk_summary_json"])),
        actions=actions,
        created_at=_datetime(row["created_at"]),
        expires_at=_datetime(row["expires_at"]),
    )


def _media_info_evidence(value: object) -> dict[str, Any]:
    media_source = _media_source(value)
    streams = media_source.get("MediaStreams")
    normalized_streams = (
        [stream for stream in streams if isinstance(stream, Mapping)] if isinstance(streams, list) else []
    )
    has_video = any(
        str(stream.get("Type") or stream.get("type") or "").casefold() == "video" for stream in normalized_streams
    )
    return {
        "snapshot_hash": hashlib.sha256(_json(value).encode()).hexdigest(),
        "has_video": has_video,
        "stream_count": len(normalized_streams),
        "video_info": _video_info_from_streams(normalized_streams),
    }


def _media_source(value: object) -> Mapping[str, Any]:
    if isinstance(value, list):
        for item in value:
            source = _media_source(item)
            if source:
                return source
        return {}
    if not isinstance(value, Mapping):
        return {}
    nested = value.get("MediaSourceInfo")
    if nested is not None:
        source = _media_source(nested)
        if source:
            return source
    return value if isinstance(value.get("MediaStreams"), list) else {}


def _video_info_from_streams(streams: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    video = next(
        (stream for stream in streams if str(stream.get("Type") or stream.get("type") or "").casefold() == "video"),
        None,
    )
    audio_streams = [
        stream for stream in streams if str(stream.get("Type") or stream.get("type") or "").casefold() == "audio"
    ]
    info: dict[str, Any] = {}
    if video is not None:
        width = _positive_int(video.get("Width") or video.get("width"))
        if width >= 3800:
            info["resolution"] = "2160p"
        elif width >= 1900:
            info["resolution"] = "1080p"
        elif width >= 1200:
            info["resolution"] = "720p"
        elif width > 0:
            info["resolution"] = "480p"
        codec_raw = str(video.get("Codec") or video.get("codec") or "").casefold()
        codec = {"h264": "AVC", "avc": "AVC", "h265": "HEVC", "hevc": "HEVC", "av1": "AV1"}.get(
            codec_raw,
            codec_raw.upper(),
        )
        bit_depth = _positive_int(video.get("BitDepth") or video.get("bit_depth"))
        if codec:
            info["codec"] = f"{codec} {bit_depth}bit" if bit_depth > 8 else codec
        video_range = " ".join(
            str(video.get(key) or "") for key in ("VideoRange", "ExtendedVideoType", "ExtendedVideoSubTypeDescription")
        ).casefold()
        has_dolby_vision = "dolbyvision" in video_range or "dovi" in video_range
        hdr = (
            "HDR10+"
            if "hdr10+" in video_range
            else "HDR10"
            if "hdr10" in video_range
            else "HDR"
            if "hdr" in video_range
            else ""
        )
        if has_dolby_vision and hdr:
            info["effect"] = f"{hdr} DoVi"
        elif has_dolby_vision:
            info["effect"] = "DoVi"
        elif hdr:
            info["effect"] = hdr
        frame_rate = video.get("RealFrameRate") or video.get("AverageFrameRate")
        try:
            if frame_rate not in (None, ""):
                info["fps"] = f"{round(float(str(frame_rate)))}fps"
        except ValueError:
            pass
    if audio_streams:
        primary = next((stream for stream in audio_streams if stream.get("IsDefault")), audio_streams[0])
        codec = str(primary.get("Codec") or primary.get("codec") or "").casefold()
        profile = str(primary.get("Profile") or primary.get("profile") or "").casefold()
        audio = {
            "truehd": "TrueHD Atmos" if "atmos" in profile else "TrueHD",
            "dts": "DTS-HD MA" if "ma" in profile else "DTS",
            "eac3": "DDP",
            "ac3": "AC3",
            "aac": "AAC",
            "flac": "FLAC",
        }.get(codec, codec.upper())
        channels = {8: "7.1", 6: "5.1", 2: "2.0"}.get(_positive_int(primary.get("Channels")))
        label = " ".join(value for value in (audio, channels or "") if value)
        if label:
            info["audio"] = label
        if len(audio_streams) > 1:
            info["audio_count"] = f"{len(audio_streams)}Audios"
    return info


def _positive_int(value: object) -> int:
    try:
        return max(int(str(value or 0)), 0)
    except (TypeError, ValueError):
        return 0


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _json(value: object) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        default=str,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _datetime(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("plan timestamp is not a datetime")
    return value


__all__ = [
    "SqlAlchemyP115DirectoryCacheRepository",
    "SqlAlchemyP115EpisodeRegexRepository",
    "SqlAlchemyP115OrganizePlanRepository",
    "SqlAlchemyP115RenameConfigRepository",
    "SqlAlchemyP115SortingRulesRepository",
]
