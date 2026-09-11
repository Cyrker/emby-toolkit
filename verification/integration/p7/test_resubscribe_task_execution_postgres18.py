from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from pydantic import SecretStr
from sqlalchemy import text

from etk.domains.media.cleanup_projection import SqlAlchemyCleanupMediaProjection
from etk.domains.media.resubscribe_projection import SqlAlchemyResubscribeMediaProjection
from etk.domains.subscription.cleanup_adapters import (
    SafeLocalCleanupGateway,
    SettingsCleanupExecutionPolicy,
    SqlAlchemyCleanupMediaGateway,
)
from etk.domains.subscription.cleanup_execution import CleanupEffectResult, CleanupExecutionService
from etk.domains.subscription.cleanup_repository import SqlAlchemyCleanupRepository
from etk.domains.subscription.cleanup_runtime_repository import SqlAlchemyCleanupExecutionRepository
from etk.domains.subscription.resubscribe import (
    PostgresResubscribeOperationStager,
    ResubscribeBatchReservation,
    ResubscribeItemKey,
    ResubscribePolicySnapshot,
    SqlAlchemyResubscribeRepository,
)
from etk.domains.subscription.resubscribe_task_execution import (
    NativeResubscribeTaskService,
    ResubscribeExecutionItem,
    ResubscribeHDHiveDisposition,
    ResubscribeHDHiveResult,
    SqlAlchemyResubscribeExecutionRepository,
)
from etk.domains.subscription.runtime_repository import PostgresSubscriptionRuntimeRepository
from etk.platform import Settings
from etk.platform.db import DatabaseRuntime
from etk.platform.jobs import PostgresJobStore
from etk.platform.tasks import TaskOutcomeStatus

PREFIX = "p11-native-resubscribe-"


@pytest.fixture(autouse=True)
def _clean_rows(p7_runtime: DatabaseRuntime) -> Iterator[None]:
    def clean() -> None:
        with p7_runtime.engine.begin() as connection:
            connection.execute(
                text("DELETE FROM resubscribe_index WHERE tmdb_id LIKE :prefix"),
                {"prefix": f"{PREFIX}%"},
            )
            connection.execute(
                text("DELETE FROM media_metadata WHERE tmdb_id LIKE :prefix OR parent_series_tmdb_id LIKE :prefix"),
                {"prefix": f"{PREFIX}%"},
            )
            connection.execute(
                text("DELETE FROM resubscribe_rules WHERE name LIKE :prefix"),
                {"prefix": f"{PREFIX}%"},
            )

    clean()
    try:
        yield
    finally:
        clean()


class _Policy:
    def snapshot(self) -> ResubscribePolicySnapshot:
        return ResubscribePolicySnapshot(
            quota_date=date(2026, 9, 8),
            daily_cap=10,
            delay_seconds=0,
            language_keywords={},
            release_group_mapping={},
        )


class _Progress:
    def __init__(self) -> None:
        self.checkpoints = 0
        self.updates: list[tuple[int, str]] = []

    def checkpoint(self) -> None:
        self.checkpoints += 1

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))


class _CleanupProgress(_Progress):
    def control_state(self) -> Mapping[str, Any]:
        return {}


class _CleanupRecorder:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def stage_version_deletion(self, emby_id: str, **_values: Any) -> Mapping[str, Any]:
        self.calls.append(emby_id)
        return {
            "job_id": str(uuid4()),
            "execution_key": f"fixture:{emby_id}",
            "item_count": 1,
            "reused": False,
        }


class _CleanupSubmission:
    def __init__(self, repository: SqlAlchemyCleanupRepository) -> None:
        self._repository = repository
        self.submissions: list[Any] = []

    def stage_version_deletion(self, emby_id: str, **_values: Any) -> Mapping[str, Any]:
        submission = self._repository.stage_version_deletion(emby_id)
        self.submissions.append(submission)
        return {
            "job_id": str(submission.job_id),
            "execution_key": submission.execution_key,
            "item_count": submission.item_count,
            "reused": submission.reused,
        }


class _NoHDHive:
    def execute(self, *args: Any, **kwargs: Any) -> ResubscribeHDHiveResult:
        del args, kwargs
        raise AssertionError("HDHive must not be called")


class _ConfirmedHDHive:
    def __init__(self) -> None:
        self.calls = 0

    def execute(
        self,
        item: ResubscribeExecutionItem,
        *,
        occurrence: str,
        progress: Any,
    ) -> ResubscribeHDHiveResult:
        del item, occurrence, progress
        self.calls += 1
        return ResubscribeHDHiveResult(ResubscribeHDHiveDisposition.CONFIRMED, uuid4())


class _NoMoviePilot:
    def stage_replacements(self, keys: Sequence[ResubscribeItemKey]) -> ResubscribeBatchReservation:
        del keys
        raise AssertionError("MoviePilot must not be called")


class _ConfirmedProvider:
    def request(self, target: Mapping[str, Any]) -> CleanupEffectResult:
        del target
        return CleanupEffectResult.confirmed()

    def readback(self, target: Mapping[str, Any]) -> CleanupEffectResult:
        del target
        return CleanupEffectResult.confirmed({"absent": True})


def _ids() -> tuple[int, str]:
    suffix = uuid4().hex
    return 1_000_000 + int(suffix[:6], 16), suffix


def _insert_movie(
    runtime: DatabaseRuntime,
    *,
    rule_id: int,
    tmdb_id: str,
    action: str = "resubscribe",
    source: str = "moviepilot",
    enabled: bool = True,
    emby_ids: Sequence[str] = (),
    paths: Sequence[str] = (),
) -> None:
    assets = [
        {"emby_item_id": emby_id, "path": path, "release_group_raw": ["fixture"]}
        for emby_id, path in zip(emby_ids, paths, strict=True)
    ]
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO resubscribe_rules (
                    id, name, enabled, sort_order, rule_type, resubscribe_source,
                    custom_resubscribe_enabled, resubscribe_entire_season
                ) VALUES (
                    :rule_id, :name, :enabled, :rule_id, :action, :source, FALSE, FALSE
                )
                """
            ),
            {
                "rule_id": rule_id,
                "name": f"{PREFIX}rule-{rule_id}",
                "action": action,
                "source": source,
                "enabled": enabled,
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, in_library, subscription_status,
                    emby_item_ids_json, asset_details_json, file_sha1_json, file_pickcode_json
                ) VALUES (
                    :tmdb_id, 'Movie', :title, TRUE, 'SUBSCRIBED',
                    CAST(:emby_ids AS jsonb), CAST(:assets AS jsonb),
                    CAST(:sha1s AS jsonb), CAST(:pickcodes AS jsonb)
                )
                """
            ),
            {
                "tmdb_id": tmdb_id,
                "title": f"Movie {tmdb_id}",
                "emby_ids": json.dumps(list(emby_ids)),
                "assets": json.dumps(assets),
                "sha1s": json.dumps(["" for _value in emby_ids]),
                "pickcodes": json.dumps(["" for _value in emby_ids]),
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO resubscribe_index (
                    tmdb_id, item_type, season_number, status, reason, matched_rule_id
                ) VALUES (:tmdb_id, 'Movie', -1, 'needed', 'fixture', :rule_id)
                """
            ),
            {"tmdb_id": tmdb_id, "rule_id": rule_id},
        )


def _execution_repository(runtime: DatabaseRuntime) -> SqlAlchemyResubscribeExecutionRepository:
    return SqlAlchemyResubscribeExecutionRepository(
        runtime.unit_of_work,
        SqlAlchemyResubscribeMediaProjection(),
    )


def test_mixed_moviepilot_and_delete_rules_route_to_distinct_durable_owners(
    p7_runtime: DatabaseRuntime,
) -> None:
    moviepilot_rule, suffix = _ids()
    delete_rule, _ = _ids()
    moviepilot_tmdb = f"{PREFIX}mp-{suffix}"
    delete_tmdb = f"{PREFIX}delete-{suffix}"
    _insert_movie(p7_runtime, rule_id=moviepilot_rule, tmdb_id=moviepilot_tmdb)
    _insert_movie(
        p7_runtime,
        rule_id=delete_rule,
        tmdb_id=delete_tmdb,
        action="delete",
        emby_ids=(f"{suffix}-a", f"{suffix}-b"),
        paths=(f"/fixture/{suffix}-a.strm", f"/fixture/{suffix}-b.strm"),
    )
    policy = _Policy()
    cleanup = _CleanupRecorder()
    service = NativeResubscribeTaskService(
        _execution_repository(p7_runtime),
        SqlAlchemyResubscribeRepository(
            p7_runtime.unit_of_work,
            SqlAlchemyResubscribeMediaProjection(),
            PostgresResubscribeOperationStager(),
            policy,
        ),
        cleanup,
        _NoHDHive(),
        PostgresSubscriptionRuntimeRepository(p7_runtime.unit_of_work),
        policy,
    )

    outcome = service.execute("resubscribe-library", _Progress(), {})

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["moviepilot_staged"] == 1
    assert outcome.details["cleanup_staged"] == 2
    assert cleanup.calls == [f"{suffix}-a", f"{suffix}-b"]
    with p7_runtime.engine.connect() as connection:
        state = (
            connection.execute(
                text(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM subscription_operations
                         WHERE scope_key LIKE :scope) AS operations,
                        (SELECT COUNT(*) FROM subscription_quota_reservations
                         WHERE operation_id IN (
                             SELECT id FROM subscription_operations WHERE scope_key LIKE :scope
                         )) AS quotas,
                        (SELECT COUNT(*) FROM outbox_events
                         WHERE aggregate_type = 'subscription-operation'
                           AND aggregate_id IN (
                               SELECT CAST(id AS text) FROM subscription_operations WHERE scope_key LIKE :scope
                           )) AS outbox
                    """
                ),
                {"scope": f"%{moviepilot_tmdb}%"},
            )
            .mappings()
            .one()
        )
    assert dict(state) == {"operations": 1, "quotas": 1, "outbox": 1}


def test_disabled_rule_cannot_reach_any_resubscribe_effect(
    p7_runtime: DatabaseRuntime,
) -> None:
    rule_id, suffix = _ids()
    _insert_movie(
        p7_runtime,
        rule_id=rule_id,
        tmdb_id=f"{PREFIX}disabled-{suffix}",
        source="hdhive",
        enabled=False,
    )
    service = NativeResubscribeTaskService(
        _execution_repository(p7_runtime),
        _NoMoviePilot(),
        _CleanupRecorder(),
        _NoHDHive(),
        PostgresSubscriptionRuntimeRepository(p7_runtime.unit_of_work),
        _Policy(),
    )

    outcome = service.execute("resubscribe-library", _Progress(), {})

    assert outcome.status is TaskOutcomeStatus.SKIPPED
    assert outcome.details["requested"] == 0


def test_historical_hdhive_item_is_skipped_without_provider_or_quota_access(
    p7_runtime: DatabaseRuntime,
) -> None:
    rule_id, suffix = _ids()
    tmdb_id = f"{PREFIX}hdhive-{suffix}"
    _insert_movie(p7_runtime, rule_id=rule_id, tmdb_id=tmdb_id, source="hdhive")
    hdhive = _ConfirmedHDHive()
    policy = _Policy()
    service = NativeResubscribeTaskService(
        _execution_repository(p7_runtime),
        _NoMoviePilot(),
        _CleanupRecorder(),
        hdhive,
        PostgresSubscriptionRuntimeRepository(p7_runtime.unit_of_work),
        policy,
    )

    first = service.execute("resubscribe-library", _Progress(), {})
    replay = service.execute("resubscribe-library", _Progress(), {})

    assert first.status is TaskOutcomeStatus.SKIPPED
    assert first.details["retired_source_skipped"] == 1
    assert replay.status is TaskOutcomeStatus.SKIPPED
    assert hdhive.calls == 0
    with p7_runtime.engine.connect() as connection:
        status = connection.scalar(
            text("SELECT status FROM resubscribe_index WHERE tmdb_id = :tmdb_id"),
            {"tmdb_id": tmdb_id},
        )
        quota_count = connection.scalar(
            text(
                """
                SELECT COUNT(*)
                FROM subscription_quota_reservations
                """
            )
        )
    assert status == "needed"
    assert int(quota_count or 0) == 0


def test_delete_rule_uses_disposable_cleanup_owner_for_files_database_and_index(
    p7_runtime: DatabaseRuntime,
    tmp_path: Path,
) -> None:
    rule_id, suffix = _ids()
    tmdb_id = f"{PREFIX}cleanup-{suffix}"
    media = tmp_path / "Fixture.strm"
    subtitle = tmp_path / "Fixture.zh.srt"
    media.write_text("fixture", encoding="utf-8")
    subtitle.write_text("fixture subtitle", encoding="utf-8")
    emby_id = f"emby-{suffix}"
    _insert_movie(
        p7_runtime,
        rule_id=rule_id,
        tmdb_id=tmdb_id,
        action="delete",
        emby_ids=(emby_id,),
        paths=(str(media),),
    )
    cleanup_repository = SqlAlchemyCleanupRepository(
        p7_runtime.unit_of_work,
        SqlAlchemyCleanupMediaProjection(),
        SettingsCleanupExecutionPolicy(
            lambda: Settings(
                local_strm_root=str(tmp_path),
                emby_server_url="http://emby.fixture.invalid",
                emby_api_key=SecretStr("fixture-token"),
                p115_enable_sync_delete=False,
            )
        ),
    )
    cleanup = _CleanupSubmission(cleanup_repository)
    service = NativeResubscribeTaskService(
        _execution_repository(p7_runtime),
        _NoMoviePilot(),
        cleanup,
        _NoHDHive(),
        PostgresSubscriptionRuntimeRepository(p7_runtime.unit_of_work),
        _Policy(),
    )

    outcome = service.execute("resubscribe-library", _Progress(), {})

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert len(cleanup.submissions) == 1
    submission = cleanup.submissions[0]
    jobs = PostgresJobStore(p7_runtime.unit_of_work)
    job = jobs.get_job(submission.job_id)
    assert job is not None
    attempt_id = job.current_attempt.id
    claim = jobs.claim_attempt(
        job_id=submission.job_id,
        attempt_id=attempt_id,
        worker_id="fixture-cleanup",
        lease_seconds=60,
    )
    assert claim.claimed is True
    jobs.start_attempt(
        job_id=submission.job_id,
        attempt_id=attempt_id,
        worker_id="fixture-cleanup",
        lease_seconds=60,
    )
    cleanup_execution = CleanupExecutionService(
        SqlAlchemyCleanupExecutionRepository(p7_runtime.unit_of_work),
        local=SafeLocalCleanupGateway(),
        media=SqlAlchemyCleanupMediaGateway(p7_runtime.unit_of_work),
        p115=_ConfirmedProvider(),
        emby=_ConfirmedProvider(),
        lease_seconds=60,
    )
    result = cleanup_execution.execute(
        job_id=submission.job_id,
        execution_key=submission.execution_key,
        attempt_id=attempt_id,
        worker_id="fixture-cleanup",
        progress=_CleanupProgress(),
    )
    jobs.succeed_attempt(
        job_id=submission.job_id,
        attempt_id=attempt_id,
        worker_id="fixture-cleanup",
        result=result,
    )

    assert result["status"] == "confirmed"
    assert not media.exists() and not subtitle.exists()
    with p7_runtime.engine.connect() as connection:
        state = (
            connection.execute(
                text(
                    """
                    SELECT
                        (SELECT in_library FROM media_metadata
                         WHERE tmdb_id = :tmdb_id AND item_type = 'Movie') AS in_library,
                        (SELECT COUNT(*) FROM resubscribe_index
                         WHERE tmdb_id = :tmdb_id) AS index_rows,
                        (SELECT status FROM cleanup_execution_items
                         WHERE job_id = :job_id) AS cleanup_status
                    """
                ),
                {"tmdb_id": tmdb_id, "job_id": submission.job_id},
            )
            .mappings()
            .one()
        )
    assert dict(state) == {"in_library": False, "index_rows": 0, "cleanup_status": "confirmed"}
