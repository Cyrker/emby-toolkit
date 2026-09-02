from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal
from uuid import UUID

import pytest

from etk.domains.collection.custom_tasks import (
    CustomCollectionCandidate,
    CustomCollectionLibrarySnapshot,
    CustomCollectionRefreshPlan,
    CustomCollectionRefreshResult,
    CustomCollectionSourceIndeterminate,
    CustomCollectionTaskService,
)
from etk.platform.tasks import TaskOutcomeStatus

_JOB_ID = UUID("00000000-0000-0000-0000-000000000401")


class Progress:
    def __init__(self) -> None:
        self.updates: list[tuple[int, str]] = []
        self.checkpoints = 0

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        self.checkpoints += 1


class Repository:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        self.rows = [dict(row) for row in rows]

    def list_active_for_execution(self) -> Sequence[Mapping[str, Any]]:
        return tuple(self.rows)

    def get_for_execution(self, collection_id: int) -> Mapping[str, Any] | None:
        return next((row for row in self.rows if row.get("id") == collection_id), None)


class Source:
    def __init__(
        self,
        *,
        listed: Sequence[CustomCollectionCandidate] | BaseException = (),
        recommended: Sequence[CustomCollectionCandidate] | BaseException = (),
    ) -> None:
        self.listed = listed
        self.recommended = recommended

    def fetch_list(self, definition: Mapping[str, Any]) -> Sequence[CustomCollectionCandidate]:
        del definition
        if isinstance(self.listed, BaseException):
            raise self.listed
        return self.listed

    def fetch_global_recommendation(self, definition: Mapping[str, Any]) -> Sequence[CustomCollectionCandidate]:
        del definition
        if isinstance(self.recommended, BaseException):
            raise self.recommended
        return self.recommended


class Tmdb:
    def __init__(self, values: Mapping[str, Mapping[str, Any]] | None = None) -> None:
        self.values = dict(values or {})

    def series_details(self, tmdb_id: str) -> Mapping[str, Any]:
        return self.values[tmdb_id]


class MediaIndex:
    def __init__(self, values: Mapping[tuple[str, str, int | None], str | None] | None = None) -> None:
        self.values = dict(values or {})

    def lookup(self, *, tmdb_id: str, media_type: str, season: int | None = None) -> str | None:
        return self.values.get((tmdb_id, media_type, season))


class Library:
    def __init__(self) -> None:
        self.filter_snapshot = CustomCollectionLibrarySnapshot((), 0)
        self.personal_snapshot = CustomCollectionLibrarySnapshot((), 0)
        self.placeholders: tuple[str, ...] = ()
        self.calls: list[tuple[str, str, str]] = []

    def query_filter(
        self,
        definition: Mapping[str, Any],
        *,
        user_id: str,
        execution_key: str = "",
    ) -> CustomCollectionLibrarySnapshot:
        del definition
        self.calls.append(("filter", user_id, execution_key))
        return self.filter_snapshot

    def query_personal_recommendation(
        self,
        definition: Mapping[str, Any],
        *,
        user_id: str,
        execution_key: str = "",
    ) -> CustomCollectionLibrarySnapshot:
        del definition
        self.calls.append(("personal", user_id, execution_key))
        return self.personal_snapshot

    def query_placeholders(
        self,
        definition: Mapping[str, Any],
        *,
        user_id: str,
        execution_key: str,
        limit: int = 9,
    ) -> tuple[str, ...]:
        del definition
        self.calls.append(("placeholder", user_id, execution_key))
        return self.placeholders[:limit]


class Operations:
    def __init__(
        self,
        status: Literal["confirmed", "pending", "rejected", "indeterminate"] = "confirmed",
    ) -> None:
        self.status: Literal["confirmed", "pending", "rejected", "indeterminate"] = status
        self.calls: list[tuple[CustomCollectionRefreshPlan, str]] = []

    def apply(self, plan: CustomCollectionRefreshPlan, *, idempotency_key: str) -> CustomCollectionRefreshResult:
        self.calls.append((plan, idempotency_key))
        return CustomCollectionRefreshResult(
            self.status,
            details={"operation_id": "fixture-operation"},
        )


def _row(
    collection_id: int,
    collection_type: str,
    *,
    definition: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": collection_id,
        "name": f"Fixture {collection_id}",
        "type": collection_type,
        "definition_json": dict(definition or {"item_type": ["Movie"]}),
        "emby_collection_id": None,
        "status": "active",
    }


def _service(
    rows: Sequence[Mapping[str, Any]],
    *,
    source: Source | None = None,
    tmdb: Tmdb | None = None,
    media_index: MediaIndex | None = None,
    library: Library | None = None,
    operations: Operations | None = None,
) -> tuple[CustomCollectionTaskService, Library, Operations]:
    selected_library = library or Library()
    selected_operations = operations or Operations()
    return (
        CustomCollectionTaskService(
            Repository(rows),
            source or Source(),
            tmdb or Tmdb(),
            media_index or MediaIndex(),
            selected_library,
            selected_operations,
            default_emby_user_id=lambda: "default-user",
        ),
        selected_library,
        selected_operations,
    )


def test_batch_alias_owner_reports_pending_as_partial() -> None:
    operations = Operations("pending")
    source = Source(
        listed=(CustomCollectionCandidate("101", "Movie", "List Item"),),
        recommended=(CustomCollectionCandidate("202", "Movie", "Recommended Item"),),
    )
    media = MediaIndex(
        {
            ("101", "Movie", None): "emby-101",
            ("202", "Movie", None): "emby-202",
        }
    )
    service, _library, selected_operations = _service(
        [_row(1, "list"), _row(2, "ai_recommendation_global"), _row(3, "filter")],
        source=source,
        media_index=media,
        operations=operations,
    )

    outcome = service.execute_all(Progress(), job_id=_JOB_ID)

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details["confirmed"] == 0
    assert [value["collection_id"] for value in outcome.details["pending"]] == [1, 2]
    assert [plan.collection_id for plan, _key in selected_operations.calls] == [1, 2]


def test_batch_and_single_share_the_same_frozen_plan_and_occurrence() -> None:
    row = _row(7, "list", definition={"item_type": ["Movie"], "limit": 10})
    source = Source(listed=(CustomCollectionCandidate("303", "Movie", "Same"),))
    media = MediaIndex({("303", "Movie", None): "emby-303"})
    service, _library, operations = _service([row], source=source, media_index=media)

    batch = service.execute_all(Progress(), job_id=_JOB_ID)
    single = service.execute_one(7, Progress(), job_id=_JOB_ID)

    assert batch.status is TaskOutcomeStatus.SUCCEEDED
    assert single.status is TaskOutcomeStatus.SUCCEEDED
    assert operations.calls[0] == operations.calls[1]
    assert operations.calls[0][1] == f"{_JOB_ID}:custom-collection:7"


def test_authoritative_empty_source_plans_delete_but_source_failure_never_mutates() -> None:
    empty_service, _library, empty_operations = _service([_row(8, "list")])

    outcome = empty_service.execute_one(8, Progress(), job_id=_JOB_ID)

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    plan = empty_operations.calls[0][0]
    assert plan.delete_empty_source is True
    assert plan.desired_member_ids == ()
    assert plan.source_snapshot["status"] == "empty"

    failed_service, _library, failed_operations = _service(
        [_row(9, "list")],
        source=Source(listed=CustomCollectionSourceIndeterminate("timeout")),
    )
    with pytest.raises(CustomCollectionSourceIndeterminate, match="timeout"):
        failed_service.execute_one(9, Progress(), job_id=_JOB_ID)
    assert failed_operations.calls == []


def test_missing_list_members_use_visible_placeholders_without_hiding_subscription_candidates() -> None:
    library = Library()
    library.placeholders = ("visible-a", "visible-b")
    service, selected_library, operations = _service(
        [_row(10, "list")],
        source=Source(listed=(CustomCollectionCandidate("404", "Movie", "Missing"),)),
        library=library,
    )

    outcome = service.execute_one(10, Progress(), job_id=_JOB_ID, emby_user_id="user-10")

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    plan = operations.calls[0][0]
    assert plan.desired_member_ids == ("visible-a", "visible-b")
    assert plan.in_library_count == 2
    assert plan.generated_items[0]["tmdb_id"] == "404"
    assert selected_library.calls == [("placeholder", "user-10", str(_JOB_ID))]


def test_series_subscription_candidate_uses_the_real_tmdb_season_identity() -> None:
    service, _library, operations = _service(
        [_row(11, "list", definition={"item_type": ["Series"]})],
        source=Source(listed=(CustomCollectionCandidate("505", "Series", "Show"),)),
        tmdb=Tmdb(
            {
                "505": {
                    "seasons": [
                        {"id": 50501, "season_number": 0},
                        {"id": 50511, "season_number": 1},
                    ]
                }
            }
        ),
    )

    service.execute_one(11, Progress(), job_id=_JOB_ID)

    item = operations.calls[0][0].generated_items[0]
    assert item["tmdb_id"] == "505"
    assert item["season"] == 1
    assert item["season_tmdb_id"] == "50511"


def test_filter_and_personal_queries_keep_the_effective_user_and_execution_key() -> None:
    library = Library()
    library.filter_snapshot = CustomCollectionLibrarySnapshot(("filter-item",), 1)
    library.personal_snapshot = CustomCollectionLibrarySnapshot(("personal-item",), 0)
    service, selected_library, operations = _service(
        [_row(12, "filter"), _row(13, "ai_recommendation")],
        library=library,
    )

    service.execute_one(12, Progress(), job_id=_JOB_ID, emby_user_id="visible-user")
    service.execute_one(13, Progress(), job_id=_JOB_ID)

    assert selected_library.calls == [
        ("filter", "visible-user", str(_JOB_ID)),
        ("personal", "default-user", str(_JOB_ID)),
    ]
    assert operations.calls[0][0].desired_member_ids == ("filter-item",)
    assert operations.calls[1][0].desired_member_ids == ("personal-item",)
