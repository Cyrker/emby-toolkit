from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from etk.bootstrap.runtime_supervisor import (
    RuntimeReloadError,
    RuntimeRole,
    RuntimeState,
    RuntimeSupervisor,
)


class FakeAdapter:
    def __init__(
        self,
        events: list[Any],
        *,
        fail_start: bool = False,
        fail_reload: bool = False,
        fail_stop: bool = False,
    ):
        self.events = events
        self.fail_start = fail_start
        self.fail_reload = fail_reload
        self.fail_stop = fail_stop
        self._config: dict[str, Any] = {"revision": 1}

    @property
    def config(self) -> Mapping[str, Any]:
        return self._config

    def start(self, *, role: RuntimeRole, background: bool) -> None:
        self.events.append(("start", role, background))
        if self.fail_start:
            raise RuntimeError("start failed")

    def save_config_and_reload(self, new_config: Mapping[str, Any]) -> None:
        self.events.append(("reload", dict(new_config)))
        if self.fail_reload:
            raise RuntimeError("reload failed")
        self._config.update(new_config)

    def stop(self) -> None:
        self.events.append(("stop",))
        if self.fail_stop:
            raise RuntimeError("stop failed")


def test_supervisor_start_reload_stop_order_and_idempotence() -> None:
    events: list[Any] = []
    adapter = FakeAdapter(events)
    supervisor = RuntimeSupervisor(lambda: adapter)

    supervisor.start(role="api")
    supervisor.start(role=RuntimeRole.API)
    assert supervisor.state is RuntimeState.RUNNING
    assert supervisor.config == {"revision": 1}

    supervisor.save_config_and_reload({"revision": 2, "unknown": "preserved"})
    assert supervisor.config == {"revision": 2, "unknown": "preserved"}
    supervisor.stop()
    supervisor.stop()

    assert events == [
        ("start", RuntimeRole.API, True),
        ("reload", {"revision": 2, "unknown": "preserved"}),
        ("stop",),
    ]
    assert supervisor.state is RuntimeState.STOPPED


def test_supervisor_cleans_failed_start_and_can_retry() -> None:
    events: list[Any] = []
    adapters = iter((FakeAdapter(events, fail_start=True), FakeAdapter(events)))
    supervisor = RuntimeSupervisor(lambda: next(adapters))

    with pytest.raises(RuntimeError, match="start failed"):
        supervisor.start(role=RuntimeRole.PROXY)
    assert supervisor.state is RuntimeState.FAILED
    assert events == [("start", RuntimeRole.PROXY, False), ("stop",)]

    supervisor.start(role=RuntimeRole.PROXY)
    assert supervisor.state is RuntimeState.RUNNING
    supervisor.stop()


def test_reload_failure_marks_runtime_failed_until_stopped() -> None:
    events: list[Any] = []
    adapter = FakeAdapter(events, fail_reload=True)
    supervisor = RuntimeSupervisor(lambda: adapter)
    supervisor.start()

    with pytest.raises(RuntimeError, match="reload failed"):
        supervisor.save_config_and_reload({"revision": 2})
    assert supervisor.state is RuntimeState.FAILED
    supervisor.stop()
    assert supervisor.state is RuntimeState.STOPPED


def test_compensated_reload_failure_keeps_previous_runtime_available() -> None:
    events: list[Any] = []

    class CompensatingAdapter(FakeAdapter):
        def save_config_and_reload(self, new_config: Mapping[str, Any]) -> None:
            self.events.append(("reload", dict(new_config)))
            raise RuntimeReloadError("reload was rolled back", compensated=True)

    supervisor = RuntimeSupervisor(lambda: CompensatingAdapter(events))
    supervisor.start()

    with pytest.raises(RuntimeReloadError, match="rolled back"):
        supervisor.save_config_and_reload({"revision": 2})

    assert supervisor.state is RuntimeState.RUNNING
    assert supervisor.config == {"revision": 1}
    supervisor.stop()


def test_stop_failure_retains_adapter_for_cleanup_retry() -> None:
    events: list[Any] = []
    adapter = FakeAdapter(events, fail_stop=True)
    supervisor = RuntimeSupervisor(lambda: adapter)
    supervisor.start()

    with pytest.raises(RuntimeError, match="stop failed"):
        supervisor.stop()
    assert supervisor.state is RuntimeState.FAILED
    with pytest.raises(RuntimeError, match="must be stopped"):
        supervisor.start()

    adapter.fail_stop = False
    supervisor.stop()
    assert supervisor.state is RuntimeState.STOPPED
    assert events[-2:] == [("stop",), ("stop",)]
