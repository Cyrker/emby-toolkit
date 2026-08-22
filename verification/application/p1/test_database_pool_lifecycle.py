from __future__ import annotations

import threading
import time

import config_manager  # Import in the legacy runtime's supported order.
from database import connection


class FakeConnection:
    cursor_factory = None

    def rollback(self) -> None:
        pass


class FakePool:
    def __init__(self, events: list[str]):
        self.events = events
        self.connection = FakeConnection()

    def getconn(self) -> FakeConnection:
        self.events.append("get")
        return self.connection

    def putconn(self, value: FakeConnection) -> None:
        assert value is self.connection
        self.events.append("put")

    def closeall(self) -> None:
        self.events.append("close")


def test_close_pool_waits_for_the_owning_connection_context(monkeypatch) -> None:
    assert isinstance(config_manager.APP_CONFIG, dict)
    events: list[str] = []
    pool = FakePool(events)
    with connection._db_pool_condition:
        monkeypatch.setattr(connection, "_db_pool", pool)
        monkeypatch.setattr(connection, "_db_pool_active_contexts", 0)
        monkeypatch.setattr(connection, "_db_pool_closing", False)

    entered = threading.Event()
    release = threading.Event()
    close_result: list[bool] = []

    def lease_connection() -> None:
        with connection.get_db_connection() as current:
            assert current is pool.connection
            entered.set()
            assert release.wait(2)

    worker = threading.Thread(target=lease_connection, daemon=True)
    worker.start()
    assert entered.wait(2)

    closer = threading.Thread(target=lambda: close_result.append(connection.close_pool(timeout=2)), daemon=True)
    closer.start()
    time.sleep(0.05)
    assert closer.is_alive()
    assert events == ["get"]

    release.set()
    worker.join(2)
    closer.join(2)

    assert not worker.is_alive()
    assert not closer.is_alive()
    assert close_result == [True]
    assert events == ["get", "put", "close"]
    assert connection._db_pool is None
