from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from etk.platform.tasks import TaskOutcome, TaskOutcomeStatus
from tasks import shared_resource_tasks, users


class Cursor:
    def __init__(self, expired_users):
        self._expired_users = expired_users
        self.statement = ""

    def execute(self, statement, _parameters=None) -> None:
        self.statement = str(statement)

    def fetchall(self):
        if "expiration_date < NOW()" in self.statement:
            return list(self._expired_users)
        return []


class Connection:
    def __init__(self, expired_users):
        self._cursor = Cursor(expired_users)
        self.commits = 0

    def cursor(self):
        return self._cursor

    def commit(self) -> None:
        self.commits += 1


def test_expired_user_catalog_task_reports_partial_provider_failure(monkeypatch) -> None:
    expired = [
        {"emby_user_id": "user-1", "name": "User One"},
        {"emby_user_id": "user-2", "name": "User Two"},
    ]
    connections = []

    @contextmanager
    def connection_factory():
        connection = Connection(expired)
        connections.append(connection)
        yield connection

    monkeypatch.setattr(users.connection, "get_db_connection", connection_factory)
    monkeypatch.setattr(
        users.emby,
        "set_user_disabled_status",
        lambda user_id, **_kwargs: user_id == "user-1",
    )
    processor = SimpleNamespace(
        config={"emby_server_url": "http://emby.invalid", "emby_api_key": "fixture"},
        is_stop_requested=lambda: False,
    )

    outcome = users.task_check_expired_users(processor)

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details == {
        "successful": 1,
        "failed": 1,
        "failed_user_ids": ["user-2"],
        "total": 2,
    }
    assert len(connections) == 2


@pytest.mark.parametrize(
    ("results", "expected_status"),
    [
        ([True, True], TaskOutcomeStatus.SUCCEEDED),
        ([True, False], TaskOutcomeStatus.PARTIAL),
        ([False, False], TaskOutcomeStatus.FAILED),
    ],
)
def test_share_all_library_catalog_task_reports_aggregate_outcome(
    monkeypatch,
    results,
    expected_status,
) -> None:
    candidates = [
        {"title": f"Fixture {index}", "expected_ok": expected} for index, expected in enumerate(results, start=1)
    ]
    monkeypatch.setattr(shared_resource_tasks, "_enabled", lambda: True)
    monkeypatch.setattr(shared_resource_tasks, "_task_status", lambda *_args: None)
    monkeypatch.setattr(
        shared_resource_tasks.shared_share_db,
        "all_library_share_candidates",
        lambda **_kwargs: {
            "items": candidates,
            "scanned": len(candidates),
            "skipped_existing": 0,
            "skipped_duplicate": 0,
            "skipped_season_candidate": 0,
            "timings": {},
        },
    )
    monkeypatch.setattr(
        shared_resource_tasks,
        "register_candidate_to_center",
        lambda candidate, **_kwargs: {
            "ok": candidate["expected_ok"],
            "message": "registered" if candidate["expected_ok"] else "center failed",
        },
    )

    outcome = shared_resource_tasks.share_all_library()

    assert isinstance(outcome, TaskOutcome)
    assert outcome.status is expected_status
    assert outcome.details["success"] == sum(results)
    assert outcome.details["failed"] == len(results) - sum(results)
