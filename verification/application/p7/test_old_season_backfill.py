from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any

import pytest

from etk.domains.subscription.watchlist import (
    OldSeasonBackfillResult,
    OldSeasonBackfillService,
)
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcomeStatus
from etk.processes.p7_subscription_tasks import P7SubscriptionTaskRouter


class Repository:
    def __init__(self, candidates: Sequence[Mapping[str, Any]]) -> None:
        self.candidates = tuple(dict(candidate) for candidate in candidates)
        self.marked: list[dict[str, Any]] = []
        self.sources: list[dict[str, Any]] = []
        self.error: Exception | None = None

    def list_missing_old_seasons(self) -> Sequence[Mapping[str, Any]]:
        return deepcopy(self.candidates)

    def mark_old_seasons_wanted(
        self,
        seasons: Sequence[Mapping[str, Any]],
        *,
        source: Mapping[str, Any],
        checkpoint: Callable[[], None],
    ) -> int:
        checkpoint()
        if self.error is not None:
            raise self.error
        self.marked.extend(deepcopy([dict(season) for season in seasons]))
        self.sources.append(deepcopy(dict(source)))
        return len(seasons)


class Progress:
    def __init__(self, *, cancel_at: int | None = None) -> None:
        self.cancel_at = cancel_at
        self.checkpoints = 0
        self.updates: list[tuple[int, str]] = []

    def checkpoint(self) -> None:
        self.checkpoints += 1
        if self.checkpoints == self.cancel_at:
            raise RuntimeError("fixture cancellation")

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))


def _candidate(**overrides: Any) -> dict[str, Any]:
    candidate = {
        "tmdb_id": "season-1",
        "item_type": "Season",
        "title": None,
        "original_title": "Original",
        "season_number": 1,
        "parent_series_tmdb_id": "series-1",
        "release_date": "2025-01-01",
        "poster_path": "/poster.jpg",
        "overview": "Fixture overview",
        "series_title": "Fixture Series",
    }
    candidate.update(overrides)
    return candidate


def test_old_season_backfill_empty_result_preserves_success_progress() -> None:
    repository = Repository([])
    progress = Progress()

    result = OldSeasonBackfillService(repository).execute(progress)

    assert result == OldSeasonBackfillResult(0, 0, "扫描完成：没有发现缺失的旧季。")
    assert progress.checkpoints == 1
    assert progress.updates == [
        (10, "正在分析数据库中的缺失旧季..."),
        (100, "扫描完成：没有发现缺失的旧季。"),
    ]
    assert repository.marked == []


def test_old_season_backfill_maps_exact_source_and_candidate_fields() -> None:
    repository = Repository([_candidate(), _candidate(tmdb_id="season-2", season_number=2, title="Season Two")])
    progress = Progress()

    result = OldSeasonBackfillService(repository).execute(progress)

    assert result.candidate_count == 2
    assert result.affected_count == 2
    assert result.message == "任务完成！已为 2 个旧季提交了订阅请求。"
    assert progress.checkpoints == 3
    assert progress.updates == [
        (10, "正在分析数据库中的缺失旧季..."),
        (50, "发现 2 个缺失旧季，正在提交请求..."),
        (100, "任务完成！已为 2 个旧季提交了订阅请求。"),
    ]
    assert repository.sources == [
        {
            "type": "scan_old_seasons_backfill",
            "reason": "backfill_old_season",
        }
    ]
    assert repository.marked[0] == {
        "tmdb_id": "season-1",
        "item_type": "Season",
        "title": "Fixture Series - 第 1 季",
        "original_title": "Original",
        "release_date": "2025-01-01",
        "poster_path": "/poster.jpg",
        "season_number": 1,
        "parent_series_tmdb_id": "series-1",
        "overview": "Fixture overview",
        "reason": "backfill_old_season: S1 Missing",
    }
    assert repository.marked[1]["title"] == "Season Two"


def test_old_season_backfill_cancellation_and_writer_failure_do_not_report_success() -> None:
    cancelled_repository = Repository([_candidate()])
    with pytest.raises(RuntimeError, match="fixture cancellation"):
        OldSeasonBackfillService(cancelled_repository).execute(Progress(cancel_at=2))
    assert cancelled_repository.marked == []

    failed_repository = Repository([_candidate()])
    failed_repository.error = RuntimeError("fixture database failure")
    failed_progress = Progress()
    with pytest.raises(RuntimeError, match="fixture database failure"):
        OldSeasonBackfillService(failed_repository).execute(failed_progress)
    assert failed_progress.updates[-1][0] == 50


def test_p7_router_executes_old_season_backfill_without_processor_or_compatibility_gateway() -> None:
    class Backfill:
        def execute(self, progress: object) -> OldSeasonBackfillResult:
            del progress
            return OldSeasonBackfillResult(3, 2, "fixture completed")

    task_router = P7SubscriptionTaskRouter(
        None,
        old_season_backfill=Backfill(),
    )
    message = TaskMessage.create(
        task_key="scan_old_seasons_backfill",
        processor=ProcessorIdentity.WATCHLIST,
        payload={},
    )

    outcome = task_router.execute(message, None, Progress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.message == "fixture completed"
    assert outcome.details == {
        "task_key": "scan_old_seasons_backfill",
        "native_handler": "P7SubscriptionTaskRouter",
        "compatibility_fallback": False,
        "candidate_count": 3,
        "affected_count": 2,
    }
