from __future__ import annotations

from contextlib import nullcontext
from typing import Any

import pytest

from etk.platform.tasks import TaskOutcomeStatus
from etk.processes.p7_subscription_effects import DurableMoviePilotEffectIndeterminateError
from etk.processes.p7_subscription_tasks import _explicit_outcome
from etk.processes.worker import _reported_failure_is_retryable
from tasks import resubscribe


@pytest.fixture(autouse=True)
def _legacy_trace_logger(monkeypatch) -> None:
    monkeypatch.setattr(resubscribe.logger, "trace", lambda *_args, **_kwargs: None, raising=False)


class _Processor:
    def __init__(self, stops: list[bool] | None = None) -> None:
        self.config: dict[str, Any] = {}
        self.emby_url = "http://emby.fixture.invalid"
        self.emby_api_key = "fixture"
        self._stops = list(stops or [])

    def is_stop_requested(self) -> bool:
        return self._stops.pop(0) if self._stops else False


class _Cursor:
    def __init__(self, row: dict[str, Any]) -> None:
        self._row = row

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, *_args: object, **_kwargs: object) -> None:
        return None

    def fetchone(self) -> dict[str, Any]:
        return dict(self._row)

    def fetchall(self) -> list[dict[str, Any]]:
        return [dict(self._row)]


class _Connection:
    def __init__(self, row: dict[str, Any]) -> None:
        self._row = row

    def cursor(self) -> _Cursor:
        return _Cursor(self._row)


class _CleanupGateway:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[str] = []

    def stage_version_deletion(self, emby_id: str) -> dict[str, Any]:
        self.calls.append(emby_id)
        if self.error is not None:
            raise self.error
        return {
            "job_id": "00000000-0000-0000-0000-000000000001",
            "execution_key": "a" * 64,
            "item_count": 1,
            "reused": False,
            "resumed": False,
        }


def _forbid(label: str):
    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError(f"unexpected {label}")

    return forbidden


def test_update_cache_cancel_after_partial_scan_does_not_publish_or_purge(monkeypatch) -> None:
    rules = [{"id": 1, "name": "fixture", "enabled": True, "scope_rules": [{"field": "library"}]}]
    movies = [
        {"tmdb_id": "101", "title": "A", "asset_details_json": [{}]},
        {"tmdb_id": "102", "title": "B", "asset_details_json": [{}]},
    ]
    processor = _Processor([False, False, False, True])
    monkeypatch.setattr(resubscribe.time, "sleep", lambda *_args: None)
    monkeypatch.setattr(resubscribe.task_manager, "update_status_from_thread", lambda *_args: None)
    monkeypatch.setattr(resubscribe.resubscribe_db, "get_all_resubscribe_rules", lambda: rules)
    monkeypatch.setattr(resubscribe.resubscribe_db, "fetch_all_active_movies_for_analysis", lambda: movies)
    monkeypatch.setattr(resubscribe.resubscribe_db, "fetch_all_active_series_for_analysis", lambda: [])
    monkeypatch.setattr(resubscribe.resubscribe_db, "get_current_index_statuses", lambda: {})
    monkeypatch.setattr(
        resubscribe,
        "_fetch_candidates_for_rule",
        lambda _rule: [{"tmdb_id": "101"}, {"tmdb_id": "102"}],
    )
    monkeypatch.setattr(resubscribe, "_item_needs_resubscribe", lambda *_args: (True, "fixture"))
    monkeypatch.setattr(resubscribe.resubscribe_db, "batch_update_missing_info", _forbid("missing-info publish"))
    monkeypatch.setattr(resubscribe.resubscribe_db, "upsert_resubscribe_index_batch", _forbid("index publish"))
    monkeypatch.setattr(resubscribe.resubscribe_db, "get_all_resubscribe_index_keys", _forbid("purge read"))
    monkeypatch.setattr(resubscribe.resubscribe_db, "delete_resubscribe_index_by_keys", _forbid("purge"))

    result = resubscribe.task_update_resubscribe_cache(processor)

    assert result == {
        "ok": False,
        "status": "cancelled",
        "message": "任务已中止，未发布部分扫描结果。",
    }


def test_update_cache_cancel_during_index_publish_stops_before_purge(monkeypatch) -> None:
    processor = _Processor()
    cancelled = False

    def is_stop_requested() -> bool:
        return cancelled

    processor.is_stop_requested = is_stop_requested  # type: ignore[method-assign]
    rules = [{"id": 1, "name": "fixture", "enabled": True, "scope_rules": [{"field": "library"}]}]
    movie = {"tmdb_id": "101", "title": "A", "asset_details_json": [{}]}
    monkeypatch.setattr(resubscribe.time, "sleep", lambda *_args: None)
    monkeypatch.setattr(resubscribe.task_manager, "update_status_from_thread", lambda *_args: None)
    monkeypatch.setattr(resubscribe.task_manager, "persistent_execution_checkpoint", lambda: None)
    monkeypatch.setattr(resubscribe.resubscribe_db, "get_all_resubscribe_rules", lambda: rules)
    monkeypatch.setattr(resubscribe.resubscribe_db, "fetch_all_active_movies_for_analysis", lambda: [movie])
    monkeypatch.setattr(resubscribe.resubscribe_db, "fetch_all_active_series_for_analysis", lambda: [])
    monkeypatch.setattr(resubscribe.resubscribe_db, "get_current_index_statuses", lambda: {})
    monkeypatch.setattr(resubscribe, "_fetch_candidates_for_rule", lambda _rule: [{"tmdb_id": "101"}])
    monkeypatch.setattr(resubscribe, "_item_needs_resubscribe", lambda *_args: (True, "fixture"))

    def publish(_rows) -> None:
        nonlocal cancelled
        cancelled = True

    monkeypatch.setattr(resubscribe.resubscribe_db, "upsert_resubscribe_index_batch", publish)
    monkeypatch.setattr(resubscribe.resubscribe_db, "get_all_resubscribe_index_keys", _forbid("purge read"))
    monkeypatch.setattr(resubscribe.resubscribe_db, "delete_resubscribe_index_by_keys", _forbid("purge"))

    result = resubscribe.task_update_resubscribe_cache(processor)

    assert result == {
        "ok": False,
        "status": "cancelled",
        "message": "任务已中止，已停止后续发布和索引清理。",
    }


@pytest.mark.parametrize("stage_error", [None, RuntimeError("fixture stage failure")])
def test_persistent_delete_stages_cleanup_before_any_inline_effect(monkeypatch, stage_error) -> None:
    gateway = _CleanupGateway(error=stage_error)
    item = {
        "item_id": 1,
        "item_name": "Fixture",
        "tmdb_id": "101",
        "item_type": "Movie",
        "matched_rule_id": 1,
    }
    rule = {"id": 1, "rule_type": "delete"}
    monkeypatch.setattr(resubscribe.resubscribe_db, "get_all_needed_resubscribe_items", lambda: [item])
    monkeypatch.setattr(resubscribe.resubscribe_db, "get_all_resubscribe_rules", lambda: [rule])
    monkeypatch.setattr(resubscribe.settings_db, "get_setting", lambda *_args: {})
    monkeypatch.setattr(resubscribe.settings_db, "get_subscription_quota", lambda: 1)
    monkeypatch.setattr(resubscribe.task_manager, "update_status_from_thread", lambda *_args: None)
    monkeypatch.setattr(resubscribe.task_manager, "has_persistent_execution", lambda: True)
    monkeypatch.setattr(resubscribe.task_manager, "persistent_execution_checkpoint", lambda: None)
    monkeypatch.setattr(
        resubscribe.connection,
        "get_db_connection",
        lambda: nullcontext(
            _Connection(
                {
                    "asset_details_json": [
                        {"emby_item_id": "emby-old", "path": "/fixture/old.mkv"},
                    ]
                }
            )
        ),
    )
    monkeypatch.setattr(resubscribe.os, "remove", _forbid("local delete"))
    monkeypatch.setattr(resubscribe.maintenance_db, "cleanup_deleted_media_item", _forbid("metadata cleanup"))
    monkeypatch.setattr(resubscribe.resubscribe_db, "delete_resubscribe_index_by_keys", _forbid("index delete"))
    monkeypatch.setattr(resubscribe, "delete_115_files_by_webhook", _forbid("115 delete"))
    monkeypatch.setattr(resubscribe.emby, "notify_emby_file_changes", _forbid("Emby refresh"))

    with resubscribe.persistent_resubscribe_cleanup(gateway):
        result = resubscribe.task_resubscribe_library(_Processor())

    assert result is not None
    assert gateway.calls == ["emby-old"]
    assert result["deleted"] == 0
    if stage_error is None:
        assert result["status"] == "deferred"
        assert len(result["deferred_cleanup_jobs"]) == 1
        assert result["cleanup_failures"] == []
    else:
        assert result["status"] == "partial"
        assert result["deferred_cleanup_jobs"] == []
        assert result["cleanup_failures"] == [{"emby_id": "emby-old", "error": "RuntimeError"}]


def test_persistent_hdhive_resubscribe_is_deferred_without_inline_client(monkeypatch) -> None:
    item = {
        "item_id": 1,
        "item_name": "Fixture",
        "tmdb_id": "101",
        "item_type": "Movie",
        "matched_rule_id": 1,
    }
    rule = {"id": 1, "rule_type": "resubscribe", "resubscribe_source": "hdhive"}
    monkeypatch.setattr(resubscribe.resubscribe_db, "get_all_needed_resubscribe_items", lambda: [item])
    monkeypatch.setattr(resubscribe.resubscribe_db, "get_all_resubscribe_rules", lambda: [rule])
    monkeypatch.setattr(resubscribe.settings_db, "get_setting", lambda *_args: {})
    monkeypatch.setattr(resubscribe.settings_db, "get_subscription_quota", lambda: 1)
    monkeypatch.setattr(resubscribe.task_manager, "update_status_from_thread", lambda *_args: None)
    monkeypatch.setattr(resubscribe.task_manager, "has_persistent_execution", lambda: True)
    monkeypatch.setattr(resubscribe, "HDHiveClient", _forbid("HDHive client construction"))

    result = resubscribe.task_resubscribe_library(_Processor())

    assert result is not None
    assert result["status"] == "deferred"
    assert result["deferred_p9"] == [
        {
            "tmdb_id": "101",
            "item_type": "Movie",
            "season_number": None,
            "reason": "deferred_p9",
        }
    ]


def test_deferred_and_non_retryable_legacy_results_remain_visible_to_worker() -> None:
    deferred = _explicit_outcome(
        "resubscribe-library",
        {"ok": True, "status": "deferred", "message": "fixture child pending"},
    )
    actor_unknown = _explicit_outcome(
        "purge-ghost-actors",
        {"ok": False, "status": "partial", "message": "fixture unknown", "retryable": False},
    )

    assert deferred.status is TaskOutcomeStatus.SKIPPED
    assert actor_unknown.status is TaskOutcomeStatus.PARTIAL
    assert _reported_failure_is_retryable(actor_unknown) is False


def test_washing_priority_partial_failure_is_not_reported_as_succeeded() -> None:
    succeeded = _explicit_outcome(
        "recalculate_library_washing_priorities",
        {"processed": 3, "updated": 2, "errors": 0},
    )
    partial = _explicit_outcome(
        "recalculate_library_washing_priorities",
        {"processed": 3, "updated": 2, "errors": 1},
    )

    assert succeeded.status is TaskOutcomeStatus.SUCCEEDED
    assert partial.status is TaskOutcomeStatus.PARTIAL
    assert partial.details["legacy_result"] == {"processed": 3, "updated": 2, "errors": 1}


@pytest.mark.parametrize(
    "failure",
    [
        DurableMoviePilotEffectIndeterminateError("delete effect unknown"),
        TimeoutError("delete readback deadline"),
    ],
)
def test_persistent_resubscribe_never_creates_after_unknown_delete(monkeypatch, failure) -> None:
    item = {
        "item_id": "101-Movie",
        "item_name": "Fixture",
        "tmdb_id": "101",
        "item_type": "Movie",
        "season_number": None,
        "matched_rule_id": 1,
    }
    rule = {"id": 1, "rule_type": "resubscribe", "resubscribe_source": "moviepilot"}
    monkeypatch.setattr(resubscribe.resubscribe_db, "get_all_needed_resubscribe_items", lambda: [item])
    monkeypatch.setattr(resubscribe.resubscribe_db, "get_all_resubscribe_rules", lambda: [rule])
    monkeypatch.setattr(resubscribe.settings_db, "get_setting", lambda *_args: {})
    monkeypatch.setattr(resubscribe.settings_db, "get_subscription_quota", lambda: 1)
    monkeypatch.setattr(resubscribe.task_manager, "update_status_from_thread", lambda *_args: None)
    monkeypatch.setattr(resubscribe.task_manager, "has_persistent_execution", lambda: True)
    monkeypatch.setattr(resubscribe, "build_resubscribe_payload", lambda *_args: {"tmdbid": "101"})
    monkeypatch.setattr(
        resubscribe.moviepilot,
        "cancel_subscription",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(failure),
    )
    monkeypatch.setattr(
        resubscribe.moviepilot,
        "subscribe_with_custom_payload",
        _forbid("MoviePilot create after unknown delete"),
    )

    with pytest.raises(type(failure), match=str(failure)):
        resubscribe.task_resubscribe_library(_Processor())


@pytest.mark.parametrize(
    ("create_result", "expected_status"),
    [(True, "succeeded"), (False, "failed")],
)
def test_rejected_old_subscription_delete_uses_final_create_outcome(
    monkeypatch,
    create_result: bool,
    expected_status: str,
) -> None:
    item = {
        "item_id": "101-Movie",
        "item_name": "Fixture",
        "tmdb_id": "101",
        "item_type": "Movie",
        "season_number": None,
        "matched_rule_id": 1,
    }
    rule = {"id": 1, "rule_type": "resubscribe", "resubscribe_source": "moviepilot"}
    progress: list[int] = []
    updates: list[tuple[list[tuple[str, str, int]], str]] = []

    class Effects:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def delete_subscription(self, *_args, **_kwargs) -> bool:
            self.calls.append("delete")
            return False

        def create_subscription(self, *_args, **_kwargs) -> bool:
            self.calls.append("create")
            return create_result

    effects = Effects()
    monkeypatch.setattr(resubscribe.resubscribe_db, "get_all_needed_resubscribe_items", lambda: [item])
    monkeypatch.setattr(resubscribe.resubscribe_db, "get_all_resubscribe_rules", lambda: [rule])
    monkeypatch.setattr(
        resubscribe.resubscribe_db,
        "batch_update_resubscribe_index_status",
        lambda rows, status: updates.append((rows, status)),
    )
    monkeypatch.setattr(resubscribe.settings_db, "get_setting", lambda *_args: {})
    monkeypatch.setattr(resubscribe.settings_db, "get_subscription_quota", lambda: 1)
    monkeypatch.setattr(
        resubscribe.task_manager,
        "update_status_from_thread",
        lambda value, *_args: progress.append(value),
    )
    monkeypatch.setattr(resubscribe.task_manager, "has_persistent_execution", lambda: True)
    monkeypatch.setattr(resubscribe, "build_resubscribe_payload", lambda *_args: {"tmdbid": "101"})
    monkeypatch.setattr(resubscribe.time, "sleep", lambda *_args: None)

    with resubscribe.moviepilot.persistent_moviepilot_effects(effects):
        result = resubscribe.task_resubscribe_library(_Processor())

    assert result is not None and result["status"] == expected_status
    assert result["resubscribed"] == int(create_result)
    assert effects.calls == ["delete", "create"]
    assert updates == ([([("101", "Movie", -1)], "subscribed")] if create_result else [])
    assert result["provider_failures"] == (
        [] if create_result else [{"action": "create", "tmdb_id": "101", "item_type": "Movie", "season": None}]
    )
    if not create_result:
        assert result["retryable"] is False
    assert all(value >= 0 for value in progress)
