from __future__ import annotations

from typing import Any

import pytest

import config_manager  # noqa: F401  # Initialize legacy database modules in production order.
import constants
import task_manager
import watchlist_processor as watchlist_module
from etk.platform.tasks import TaskOutcomeStatus
from services.subscribe_assistant import manager as assistant_manager
from services.subscribe_assistant.config import AssistantConfig
from services.subscribe_assistant.engine import CompletionSignal
from tasks import watchlist as watchlist_tasks


class _Progress:
    def __init__(self) -> None:
        self.updates: list[tuple[int, str]] = []

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        return None

    def record_started_effect(self, label: str) -> None:
        del label
        return None

    def record_committed_effect(self, label: str) -> None:
        del label
        return None

    def record_abandoned_effect(self, label: str) -> None:
        del label
        return None

    def control_state(self) -> dict[str, Any]:
        return {}


def _processor(monkeypatch: pytest.MonkeyPatch) -> watchlist_module.WatchlistProcessor:
    monkeypatch.setattr(watchlist_module.logger, "trace", lambda *_args, **_kwargs: None, raising=False)
    processor = watchlist_module.WatchlistProcessor.__new__(watchlist_module.WatchlistProcessor)
    processor.config = {}
    processor._stop_event = watchlist_module.threading.Event()
    processor.progress_callback = None
    return processor


def _manager_with_failing_cleanup(monkeypatch: pytest.MonkeyPatch) -> assistant_manager.SubscribeAssistantManager:
    manager = assistant_manager.SubscribeAssistantManager(
        app_config={},
        assistant_config=AssistantConfig(
            enabled=True,
            best_version_type="completed_full",
            best_version_full_consistency_check_enabled=False,
            best_version_episode_to_full=False,
            subscription_cleanup_history_type="current",
            subscription_cleanup_history_scenes=["completed"],
        ),
    )
    monkeypatch.setattr(manager, "_local_progress_seasons", lambda _tmdb_id: {1})
    monkeypatch.setattr(manager, "_target_seasons_for_sync", lambda **kwargs: kwargs["valid_seasons"])
    monkeypatch.setattr(manager, "_completion_signal", lambda **_kwargs: CompletionSignal(scope_total=1))
    monkeypatch.setattr(manager, "_season_local_completed", lambda *_args: True)
    monkeypatch.setattr(
        manager,
        "_decide_subscription_state",
        lambda **_kwargs: {"mp_state": "R", "snapshot": True, "reason": "fixture"},
    )
    monkeypatch.setattr(manager, "_has_recent_manual_mp_change", lambda *_args: False)
    monkeypatch.setattr(manager, "_update_source_state", lambda *_args: None)
    monkeypatch.setattr(manager, "_target_total", lambda *_args: 1)
    monkeypatch.setattr(manager, "_remember_expected_mp_update", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(manager, "_mark_full_washing_started", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(manager, "_set_season_active_washing", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(manager, "_series_title", lambda *_args: "Fixture Series")
    monkeypatch.setattr(
        assistant_manager.moviepilot,
        "find_subscriptions",
        lambda *_args, **_kwargs: [{"id": 7, "season": 1, "best_version_full": 1, "name": "Fixture Series"}],
    )
    monkeypatch.setattr(assistant_manager.moviepilot, "update_subscription_status", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(assistant_manager, "_cleanup_mp_episode_residue_persistent", lambda *_args: False)
    monkeypatch.setattr(
        assistant_manager.store,
        "upsert_snapshot",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("cleanup failure must stop snapshot persistence")),
    )
    return manager


def test_cleanup_failure_reaches_watchlist_child_job_through_the_real_manager_chain(monkeypatch) -> None:
    manager = _manager_with_failing_cleanup(monkeypatch)
    processor = _processor(monkeypatch)
    monkeypatch.setattr(
        processor,
        "_get_series_to_process",
        lambda *_args, **_kwargs: [{"tmdb_id": "100", "item_name": "Fixture Series"}],
    )

    def process_one_series(series: dict[str, Any], **_kwargs: Any) -> None:
        processor._sync_status_to_moviepilot(
            tmdb_id=str(series["tmdb_id"]),
            series_name=str(series["item_name"]),
            series_details={"seasons": [{"season_number": 1, "episode_count": 1}]},
            final_status="Completed",
            all_tmdb_episodes=[{"season_number": 1, "episode_number": 1}],
        )

    monkeypatch.setattr(processor, "_process_one_series", process_one_series)
    monkeypatch.setattr(watchlist_module, "SubscribeAssistantManager", lambda _config: manager)

    progress = _Progress()
    with task_manager.persistent_execution(progress):
        with pytest.raises(ExceptionGroup) as caught:
            watchlist_tasks.task_process_watchlist(processor, tmdb_id="100")

    assert [str(error) for error in caught.value.exceptions] == [
        "MoviePilot episode residue cleanup failed for TMDb 100"
    ]
    assert processor.progress_callback is None


def test_persistent_watchlist_collects_every_concurrent_series_failure(monkeypatch) -> None:
    processor = _processor(monkeypatch)
    monkeypatch.setattr(
        processor,
        "_get_series_to_process",
        lambda *_args, **_kwargs: [
            {"tmdb_id": "100", "item_name": "Series A"},
            {"tmdb_id": "200", "item_name": "Series B"},
        ],
    )

    def fail_series(series: dict[str, Any], **_kwargs: Any) -> None:
        raise RuntimeError(f"series {series['tmdb_id']} failed")

    monkeypatch.setattr(processor, "_process_one_series", fail_series)

    progress = _Progress()
    with task_manager.persistent_execution(progress):
        with pytest.raises(ExceptionGroup, match="2") as caught:
            processor.run_regular_processing_task_concurrent(progress_callback=lambda *_args: None)

    assert {str(error) for error in caught.value.exceptions} == {
        "series 100 failed",
        "series 200 failed",
    }
    assert processor.progress_callback is None


def test_legacy_watchlist_keeps_manager_failure_best_effort(monkeypatch) -> None:
    processor = _processor(monkeypatch)
    monkeypatch.setattr(
        processor,
        "_get_series_to_process",
        lambda *_args, **_kwargs: [{"tmdb_id": "100", "item_name": "Fixture Series"}],
    )

    class FailingManager:
        @staticmethod
        def sync_series(**_kwargs: Any) -> None:
            raise RuntimeError("legacy cleanup failed")

    def process_one_series(series: dict[str, Any], **_kwargs: Any) -> None:
        processor._sync_status_to_moviepilot(
            tmdb_id=str(series["tmdb_id"]),
            series_name=str(series["item_name"]),
            series_details={"seasons": [{"season_number": 1}]},
            final_status="Watching",
        )

    monkeypatch.setattr(processor, "_process_one_series", process_one_series)
    monkeypatch.setattr(watchlist_module, "SubscribeAssistantManager", lambda _config: FailingManager())
    progress_updates: list[tuple[int, str]] = []

    processor.run_regular_processing_task_concurrent(
        progress_callback=lambda progress, message: progress_updates.append((progress, message))
    )

    assert progress_updates[-1] == (100, "追剧检查完成。")
    assert processor.progress_callback is None


def test_catalog_watchlist_scan_reports_partial_when_any_series_fails(monkeypatch) -> None:
    processor = _processor(monkeypatch)
    monkeypatch.setattr(watchlist_tasks.logger, "trace", lambda *_args, **_kwargs: None, raising=False)
    processor.config = {constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS: []}
    monkeypatch.setattr(watchlist_tasks.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(watchlist_tasks.watchlist_db, "batch_import_series_as_completed", lambda _ids: None)
    monkeypatch.setattr(
        watchlist_tasks.watchlist_db,
        "get_series_by_dynamic_condition",
        lambda **_kwargs: [
            {"tmdb_id": "100", "item_name": "Series A"},
            {"tmdb_id": "200", "item_name": "Series B"},
        ],
    )

    def process(series: dict[str, Any], **_kwargs: Any) -> None:
        if series["tmdb_id"] == "200":
            raise RuntimeError("fixture series failure")

    monkeypatch.setattr(processor, "_process_one_series", process)

    outcome = watchlist_tasks.task_add_all_series_to_watchlist(processor)

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details == {
        "processed": 2,
        "failed": 1,
        "failed_items": ["200"],
    }
