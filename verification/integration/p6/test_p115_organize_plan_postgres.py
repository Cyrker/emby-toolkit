from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import inspect, text

from etk.domains.p115 import (
    P115EpisodeRegexSnapshot,
    P115OrganizePlan,
    P115PlanAction,
    P115PlanCommitPreconditions,
    P115PlanConflict,
    P115RenameConfigSnapshot,
    P115SortingRulesSnapshot,
    SqlAlchemyP115OrganizePlanRepository,
)
from etk.platform.db import DatabaseRuntime

FIXED_NOW = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)


def test_postgres18_organize_plan_schema_has_the_frozen_constraints_and_indexes(
    p6_runtime: DatabaseRuntime,
) -> None:
    inspector = inspect(p6_runtime.engine)

    assert {"p115_organize_plans", "p115_organize_plan_actions"}.issubset(inspector.get_table_names())
    plan_columns = {column["name"]: column for column in inspector.get_columns("p115_organize_plans")}
    assert plan_columns["runtime_config_revision"]["nullable"] is False
    assert inspector.get_pk_constraint("p115_organize_plans")["constrained_columns"] == ["plan_id"]
    assert {tuple(item["column_names"]) for item in inspector.get_unique_constraints("p115_organize_plans")} >= {
        ("request_intent_id",),
    }
    assert {item["name"] for item in inspector.get_check_constraints("p115_organize_plans")} >= {
        "ck_p115_organize_plans_account_id",
        "ck_p115_organize_plans_episode_revision",
        "ck_p115_organize_plans_expiry",
        "ck_p115_organize_plans_input_fingerprint",
        "ck_p115_organize_plans_policy_revision",
        "ck_p115_organize_plans_rename_revision",
        "ck_p115_organize_plans_request_hash",
        "ck_p115_organize_plans_runtime_revision",
        "ck_p115_organize_plans_schema_version",
        "ck_p115_organize_plans_sorting_revision",
        "ck_p115_organize_plans_status",
    }
    assert {item["name"] for item in inspector.get_indexes("p115_organize_plans")} >= {
        "idx_p115_organize_plans_account_source",
        "idx_p115_organize_plans_status_expiry",
    }

    action_primary_key = inspector.get_pk_constraint("p115_organize_plan_actions")
    assert action_primary_key["name"] == "pk_p115_organize_plan_actions"
    assert action_primary_key["constrained_columns"] == ["plan_id", "action_index"]
    assert {item["name"] for item in inspector.get_unique_constraints("p115_organize_plan_actions")} >= {
        "uq_p115_organize_plan_action_id",
    }
    assert {item["name"] for item in inspector.get_check_constraints("p115_organize_plan_actions")} >= {
        "ck_p115_organize_plan_action_id",
        "ck_p115_organize_plan_action_index",
        "ck_p115_organize_plan_action_kind",
        "ck_p115_organize_plan_effect_key",
    }
    assert {item["name"] for item in inspector.get_indexes("p115_organize_plan_actions")} >= {
        "idx_p115_organize_plan_actions_effect",
    }
    foreign_keys = inspector.get_foreign_keys("p115_organize_plan_actions")
    assert len(foreign_keys) == 1
    assert foreign_keys[0]["constrained_columns"] == ["plan_id"]
    assert foreign_keys[0]["referred_table"] == "p115_organize_plans"
    assert foreign_keys[0]["referred_columns"] == ["plan_id"]
    assert foreign_keys[0].get("options") == {"ondelete": "CASCADE"}


def _seed_inputs(
    runtime: DatabaseRuntime,
) -> tuple[
    P115SortingRulesSnapshot,
    P115RenameConfigSnapshot,
    P115EpisodeRegexSnapshot,
]:
    rules = [{"id": "movies", "cid": "target", "media_type": "movie"}]
    rename = {"main_dir_template": "{{title}}", "movie_file_template": "{{title}}{{fileExt}}"}
    episode = [
        {
            "id": "episode",
            "enabled": True,
            "name": "Episode",
            "pattern": "EP(\\d+)",
            "mode": "episode_only",
            "season_group": 1,
            "episode_group": 1,
            "default_season": 1,
        }
    ]
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                VALUES
                    ('p115_sorting_rules', CAST(:rules AS jsonb), NOW()),
                    ('p115_sorting_rules_account_id', '"115001"'::jsonb, NOW()),
                    ('p115_rename_config', CAST(:rename AS jsonb), NOW()),
                    ('p115_episode_regex_rules', CAST(:episode AS jsonb), NOW())
                """
            ),
            {
                "rules": '[{"id":"movies","cid":"target","media_type":"movie"}]',
                "rename": '{"main_dir_template":"{{title}}","movie_file_template":"{{title}}{{fileExt}}"}',
                "episode": (
                    '[{"id":"episode","enabled":true,"name":"Episode",'
                    '"pattern":"EP(\\\\d+)","mode":"episode_only","season_group":1,'
                    '"episode_group":1,"default_season":1}]'
                ),
            },
        )
    return (
        P115SortingRulesSnapshot(tuple(rules), "115001"),
        P115RenameConfigSnapshot(rename),
        P115EpisodeRegexSnapshot(tuple(episode)),
    )


def _plan(
    *,
    sorting: P115SortingRulesSnapshot,
    rename: P115RenameConfigSnapshot,
    episode: P115EpisodeRegexSnapshot,
    policy_revision: str,
    runtime_config_revision: str,
    request_hash: str = "a" * 64,
) -> P115OrganizePlan:
    action = P115PlanAction(
        action_index=0,
        action_id="b" * 64,
        kind="ENSURE_DIRECTORY",
        subject={"parent_cid": "target"},
        destination={"parent_cid": "target", "name": "Movie", "path": "电影/Movie"},
        dependencies=(),
        preconditions={
            "account_id": "115001",
            "sorting_rules_revision": sorting.revision,
            "rename_config_revision": rename.revision,
            "episode_regex_revision": episode.revision,
        },
        effect_key="c" * 64,
        decision_basis={"sorting_rule_id": "movies"},
        risk={"remote_mutation": True, "requires_readback": True},
        destructive=False,
        expected_postcondition={"visible_child_name": "Movie"},
    )
    return P115OrganizePlan(
        plan_id="00000000-0000-5000-8000-000000000001",
        request_intent_id="fixture-plan-intent",
        request_hash=request_hash,
        input_fingerprint="d" * 64,
        status="ready",
        account_id="115001",
        source_root_cid="source",
        source_root_name="待整理",
        sorting_rules_revision=sorting.revision,
        rename_config_revision=rename.revision,
        episode_regex_revision=episode.revision,
        policy_revision=policy_revision,
        runtime_config_revision=runtime_config_revision,
        config_snapshot={"safe": True},
        source_snapshot=(
            {
                "id": "file-1",
                "parent_id": "source",
                "name": "Movie.mkv",
                "entry_kind": "file",
                "sha1": "E" * 40,
                "size": 1024,
            },
        ),
        provider_evidence={"complete": True, "snapshot_hash": "f" * 64},
        recognition_evidence={"file-1": {"title": "Movie", "media_type": "movie"}},
        warnings=(),
        errors=(),
        risk_summary={"external_writes_executed": 0, "requires_human_review": True},
        actions=(action,),
        created_at=FIXED_NOW,
        expires_at=FIXED_NOW + timedelta(hours=24),
    )


def test_postgres18_plan_repository_persists_and_reloads_immutable_document_and_actions(
    p6_runtime: DatabaseRuntime,
) -> None:
    sorting, rename, episode = _seed_inputs(p6_runtime)
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                    INSERT INTO p115_mediainfo_cache (sha1, mediainfo_json, hit_count)
                    VALUES (
                        :sha1,
                        CAST(:mediainfo AS jsonb),
                        7
                    )
                """
            ),
            {
                "sha1": "E" * 40,
                "mediainfo": (
                    '{"MediaSourceInfo":{"MediaStreams":['
                    '{"Type":"Video","Codec":"hevc","Width":3840,"BitDepth":10},'
                    '{"Type":"Audio","Codec":"truehd","Profile":"atmos","Channels":8}'
                    "]}}"
                ),
            },
        )
    repository = SqlAlchemyP115OrganizePlanRepository(p6_runtime.unit_of_work)
    support = repository.load_support_snapshot(["E" * 40])
    plan = _plan(
        sorting=sorting,
        rename=rename,
        episode=episode,
        policy_revision=support.policy_revision,
        runtime_config_revision=support.runtime_config_revision,
    )
    preconditions = P115PlanCommitPreconditions(
        account_id="115001",
        sorting_rules_revision=sorting.revision,
        rename_config_revision=rename.revision,
        episode_regex_revision=episode.revision,
        policy_revision=support.policy_revision,
        runtime_config_revision=support.runtime_config_revision,
        media_info_revisions={"E" * 40: support.media_info["E" * 40]["snapshot_hash"]},
    )

    saved = repository.save(plan, preconditions=preconditions)
    replay = repository.save(plan, preconditions=preconditions)
    loaded = repository.get(plan.plan_id)

    assert saved == plan
    assert replay == plan
    assert loaded == plan
    assert support.media_info["E" * 40] == {
        "snapshot_hash": support.media_info["E" * 40]["snapshot_hash"],
        "has_video": True,
        "stream_count": 2,
        "video_info": {
            "audio": "TrueHD Atmos 7.1",
            "codec": "HEVC 10bit",
            "resolution": "2160p",
        },
    }
    with p6_runtime.engine.connect() as connection:
        assert int(connection.scalar(text("SHOW server_version_num")) or 0) // 10_000 == 18
        assert connection.scalar(text("SELECT COUNT(*) FROM p115_organize_plans")) == 1
        assert connection.scalar(text("SELECT COUNT(*) FROM p115_organize_plan_actions")) == 1
        assert (
            connection.scalar(
                text("SELECT hit_count FROM p115_mediainfo_cache WHERE sha1 = :sha1"),
                {"sha1": "E" * 40},
            )
            == 7
        )


def test_postgres18_plan_repository_rejects_idempotency_conflicts(
    p6_runtime: DatabaseRuntime,
) -> None:
    sorting, rename, episode = _seed_inputs(p6_runtime)
    repository = SqlAlchemyP115OrganizePlanRepository(p6_runtime.unit_of_work)
    support = repository.load_support_snapshot([])
    original = _plan(
        sorting=sorting,
        rename=rename,
        episode=episode,
        policy_revision=support.policy_revision,
        runtime_config_revision=support.runtime_config_revision,
    )
    preconditions = P115PlanCommitPreconditions(
        account_id="115001",
        sorting_rules_revision=sorting.revision,
        rename_config_revision=rename.revision,
        episode_regex_revision=episode.revision,
        policy_revision=support.policy_revision,
        runtime_config_revision=support.runtime_config_revision,
        media_info_revisions={},
    )
    repository.save(original, preconditions=preconditions)
    conflicting = _plan(
        sorting=sorting,
        rename=rename,
        episode=episode,
        policy_revision=support.policy_revision,
        runtime_config_revision=support.runtime_config_revision,
        request_hash="e" * 64,
    )

    with pytest.raises(P115PlanConflict, match="不同的整理计划输入"):
        repository.save(conflicting, preconditions=preconditions)

    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM p115_organize_plans")) == 1
        assert connection.scalar(text("SELECT COUNT(*) FROM p115_organize_plan_actions")) == 1


def test_postgres18_plan_repository_rolls_back_when_config_revision_changes(
    p6_runtime: DatabaseRuntime,
) -> None:
    sorting, rename, episode = _seed_inputs(p6_runtime)
    repository = SqlAlchemyP115OrganizePlanRepository(p6_runtime.unit_of_work)
    support = repository.load_support_snapshot([])
    plan = _plan(
        sorting=sorting,
        rename=rename,
        episode=episode,
        policy_revision=support.policy_revision,
        runtime_config_revision=support.runtime_config_revision,
    )
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE app_settings
                SET value_json = '{"main_dir_template":"changed"}'::jsonb,
                    last_updated_at = NOW()
                WHERE setting_key = 'p115_rename_config'
                """
            )
        )

    with pytest.raises(RuntimeError, match="重命名配置.*发生变化"):
        repository.save(
            plan,
            preconditions=P115PlanCommitPreconditions(
                account_id="115001",
                sorting_rules_revision=sorting.revision,
                rename_config_revision=rename.revision,
                episode_regex_revision=episode.revision,
                policy_revision=support.policy_revision,
                runtime_config_revision=support.runtime_config_revision,
                media_info_revisions={},
            ),
        )

    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM p115_organize_plans")) == 0
        assert connection.scalar(text("SELECT COUNT(*) FROM p115_organize_plan_actions")) == 0


def test_postgres18_plan_repository_rolls_back_when_runtime_or_mediainfo_changes(
    p6_runtime: DatabaseRuntime,
) -> None:
    sorting, rename, episode = _seed_inputs(p6_runtime)
    sha1 = "F" * 40
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO p115_mediainfo_cache (sha1, mediainfo_json)
                VALUES (:sha1, '{"MediaSourceInfo":{"MediaStreams":[{"Type":"Video","Codec":"hevc"}]}}')
                """
            ),
            {"sha1": sha1},
        )
    repository = SqlAlchemyP115OrganizePlanRepository(p6_runtime.unit_of_work)
    support = repository.load_support_snapshot([sha1])
    plan = _plan(
        sorting=sorting,
        rename=rename,
        episode=episode,
        policy_revision=support.policy_revision,
        runtime_config_revision=support.runtime_config_revision,
    )
    preconditions = P115PlanCommitPreconditions(
        account_id="115001",
        sorting_rules_revision=sorting.revision,
        rename_config_revision=rename.revision,
        episode_regex_revision=episode.revision,
        policy_revision=support.policy_revision,
        runtime_config_revision=support.runtime_config_revision,
        media_info_revisions={sha1: support.media_info[sha1]["snapshot_hash"]},
    )
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE p115_mediainfo_cache
                SET mediainfo_json = '{"MediaSourceInfo":{"MediaStreams":[{"Type":"Video","Codec":"av1"}]}}'
                WHERE sha1 = :sha1
                """
            ),
            {"sha1": sha1},
        )

    with pytest.raises(RuntimeError, match="MediaInfo 缓存.*发生变化"):
        repository.save(plan, preconditions=preconditions)

    fresh_support = repository.load_support_snapshot([])
    fresh_plan = _plan(
        sorting=sorting,
        rename=rename,
        episode=episode,
        policy_revision=fresh_support.policy_revision,
        runtime_config_revision=fresh_support.runtime_config_revision,
    )
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                VALUES ('dynamic_app_config', CAST(:config AS jsonb), NOW())
                ON CONFLICT (setting_key) DO UPDATE SET
                    value_json = EXCLUDED.value_json,
                    last_updated_at = NOW()
                """
            ),
            {"config": '{"p115_min_video_size":20}'},
        )

    with pytest.raises(RuntimeError, match="运行配置.*发生变化"):
        repository.save(
            fresh_plan,
            preconditions=P115PlanCommitPreconditions(
                account_id="115001",
                sorting_rules_revision=sorting.revision,
                rename_config_revision=rename.revision,
                episode_regex_revision=episode.revision,
                policy_revision=fresh_support.policy_revision,
                runtime_config_revision=fresh_support.runtime_config_revision,
                media_info_revisions={},
            ),
        )

    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM p115_organize_plans")) == 0
        assert connection.scalar(text("SELECT COUNT(*) FROM p115_organize_plan_actions")) == 0
