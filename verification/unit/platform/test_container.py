from __future__ import annotations

import pytest

from etk.platform.config import Settings
from etk.platform.container import (
    AppContainer,
    CircularProviderDependencyError,
    ContainerClosedError,
    ProviderNotRegisteredError,
)


def test_container_providers_are_explicit_lazy_singletons() -> None:
    events: list[str] = []

    def database(_container: AppContainer) -> object:
        events.append("database")
        return object()

    def repository(container: AppContainer) -> dict[str, object]:
        events.append("repository")
        return {"database": container.database}

    def client(_container: AppContainer) -> object:
        events.append("client")
        return object()

    def use_case(container: AppContainer) -> tuple[object, object]:
        events.append("use-case")
        return (container.repository("media"), container.client("emby"))

    container = AppContainer(
        Settings(),
        database_provider=database,
        repository_providers={"media": repository},
        client_providers={"emby": client},
        use_case_providers={"sync-media": use_case},
    )

    assert events == []
    first = container.use_case("sync-media")
    second = container.use_case("sync-media")

    assert first is second
    assert events == ["use-case", "repository", "database", "client"]


def test_failed_provider_can_be_retried_and_cycles_are_reported() -> None:
    attempts = 0

    def retryable(_container: AppContainer) -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary failure")
        return "ready"

    container = AppContainer(Settings(), client_providers={"retryable": retryable})
    with pytest.raises(RuntimeError, match="temporary failure"):
        container.client("retryable")
    assert container.client("retryable") == "ready"

    circular = AppContainer(
        Settings(),
        repository_providers={"a": lambda current: current.client("b")},
        client_providers={"b": lambda current: current.repository("a")},
    )
    with pytest.raises(CircularProviderDependencyError, match=r"repository:a.*client:b.*repository:a"):
        circular.repository("a")


def test_container_closes_only_materialized_resources_in_reverse_order() -> None:
    events: list[str] = []

    class Resource:
        def __init__(self, name: str) -> None:
            self.name = name

        def close(self) -> None:
            events.append(f"close:{self.name}")

    container = AppContainer(
        Settings(),
        database_provider=lambda _container: Resource("database"),
        repository_providers={"used": lambda current: (current.database, Resource("repository"))[1]},
        client_providers={"unused": lambda _container: Resource("unused")},
    )

    container.repository("used")
    container.close()
    container.close()

    assert events == ["close:repository", "close:database"]
    with pytest.raises(ContainerClosedError):
        _ = container.database


def test_container_close_retries_only_failed_resources_and_stays_unavailable() -> None:
    events: list[str] = []

    class RetryableResource:
        def __init__(self) -> None:
            self.attempts = 0

        def close(self) -> None:
            self.attempts += 1
            events.append(f"close:retryable:{self.attempts}")
            if self.attempts == 1:
                raise TimeoutError("active lease")

    class SuccessfulResource:
        def close(self) -> None:
            events.append("close:successful")

    retryable = RetryableResource()
    container = AppContainer(
        Settings(),
        database_provider=lambda _container: retryable,
        client_providers={"successful": lambda _container: SuccessfulResource()},
    )
    _ = container.database
    container.client("successful")

    with pytest.raises(ExceptionGroup) as raised:
        container.close()

    assert isinstance(raised.value.exceptions[0], TimeoutError)
    assert container.closed is True
    assert events == ["close:successful", "close:retryable:1"]
    with pytest.raises(ContainerClosedError):
        _ = container.database

    container.close()
    container.close()

    assert events == ["close:successful", "close:retryable:1", "close:retryable:2"]


def test_missing_provider_is_a_clear_error() -> None:
    container = AppContainer(Settings())

    with pytest.raises(ProviderNotRegisteredError, match="database"):
        _ = container.database
    with pytest.raises(ProviderNotRegisteredError, match="repository"):
        container.repository("missing")
