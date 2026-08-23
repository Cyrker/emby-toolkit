from __future__ import annotations

from contextlib import contextmanager

import pytest

import task_manager
from etk.platform.tasks import TaskOutcomeStatus
from tasks import p115


class Cursor:
    def __init__(self, statements, cached_by_parent):
        self.statements = statements
        self.cached_by_parent = cached_by_parent
        self.parent_cid = ""

    def execute(self, statement, parameters=None) -> None:
        normalized = " ".join(str(statement).split())
        self.statements.append((normalized, parameters))
        if normalized.startswith("SELECT id FROM p115_filesystem_cache"):
            self.parent_cid = str((parameters or ("",))[0])

    def fetchall(self):
        return [{"id": value} for value in self.cached_by_parent.get(self.parent_cid, ())]

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class Connection:
    def __init__(self, statements, cached_by_parent):
        self.cursor_instance = Cursor(statements, cached_by_parent)

    def cursor(self):
        return self.cursor_instance

    def commit(self) -> None:
        return None


def _install(monkeypatch, client, rules, cached_by_parent):
    statements = []
    updates = []

    @contextmanager
    def connection_factory():
        yield Connection(statements, cached_by_parent)

    monkeypatch.setattr(
        p115.P115Service,
        "get_client",
        classmethod(lambda _cls, expected_account_id=None: client),
    )
    monkeypatch.setattr(p115.settings_db, "get_setting", lambda _key: rules)
    monkeypatch.setattr(p115, "get_db_connection", connection_factory)
    monkeypatch.setattr(
        task_manager,
        "update_status_from_thread",
        lambda progress, message: updates.append((progress, message)),
    )
    return statements, updates


@pytest.mark.parametrize(
    "response",
    [
        TimeoutError("listing timeout"),
        {"state": False, "message": "provider rejected"},
        {"state": True, "data": None},
        {"state": True, "data": [None]},
    ],
)
def test_directory_sync_unknown_or_invalid_listing_never_cleans_cache(monkeypatch, response) -> None:
    class Client:
        @staticmethod
        def fs_files(_request):
            if isinstance(response, Exception):
                raise response
            return response

    statements, updates = _install(
        monkeypatch,
        Client(),
        [{"enabled": True, "cid": "root-a", "name": "Movies"}],
        {"root-a": {"cached-a", "cached-b"}},
    )

    outcome = p115.task_sync_115_directory_tree()

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert not any(statement.startswith("DELETE") for statement, _params in statements)
    assert updates[-1][0] == -1


def test_directory_sync_cleans_only_after_authoritative_complete_listing(monkeypatch) -> None:
    class Client:
        @staticmethod
        def fs_files(_request):
            return {"state": True, "data": []}

    statements, updates = _install(
        monkeypatch,
        Client(),
        [{"enabled": True, "cid": "root-a", "name": "Movies"}],
        {"root-a": {"cached-a", "cached-b"}},
    )

    outcome = p115.task_sync_115_directory_tree()

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    deletes = [(statement, params) for statement, params in statements if statement.startswith("DELETE")]
    assert len(deletes) == 1
    assert set(deletes[0][1][0]) == {"cached-a", "cached-b"}
    assert updates[-1][0] == 100


def test_directory_sync_reports_partial_and_skips_only_failed_root_cleanup(monkeypatch) -> None:
    class Client:
        @staticmethod
        def fs_files(request):
            if request["cid"] == "root-b":
                raise TimeoutError("second root timeout")
            return {"state": True, "data": []}

    statements, updates = _install(
        monkeypatch,
        Client(),
        [
            {"enabled": True, "cid": "root-a", "name": "Movies"},
            {"enabled": True, "cid": "root-b", "name": "Series"},
        ],
        {"root-a": {"cached-a"}, "root-b": {"cached-b"}},
    )

    outcome = p115.task_sync_115_directory_tree()

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    deletes = [(statement, params) for statement, params in statements if statement.startswith("DELETE")]
    assert len(deletes) == 1
    assert set(deletes[0][1][0]) == {"cached-a"}
    assert updates[-1][0] == -1
