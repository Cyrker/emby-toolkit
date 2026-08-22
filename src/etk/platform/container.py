from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from .config.settings import Settings

type ProviderFactory[T] = Callable[["AppContainer"], T]

_UNSET = object()


class ProviderNotRegisteredError(LookupError):
    pass


class CircularProviderDependencyError(RuntimeError):
    pass


class ContainerClosedError(RuntimeError):
    pass


@dataclass(slots=True)
class _LazyProvider:
    label: str
    factory: ProviderFactory[Any]
    value: Any = field(default=_UNSET)
    resolving: bool = False

    @property
    def materialized(self) -> bool:
        return self.value is not _UNSET


class AppContainer:
    """Explicit, lazy dependency graph for one application process."""

    def __init__(
        self,
        settings: Settings,
        *,
        database_provider: ProviderFactory[Any] | None = None,
        repository_providers: Mapping[str, ProviderFactory[Any]] | None = None,
        client_providers: Mapping[str, ProviderFactory[Any]] | None = None,
        use_case_providers: Mapping[str, ProviderFactory[Any]] | None = None,
    ) -> None:
        self.settings = settings
        self._lock = threading.RLock()
        self._close_lock = threading.Lock()
        self._closed = False
        self._resolution_stack: list[str] = []
        self._materialization_order: list[_LazyProvider] = []
        self._database: _LazyProvider | None = None
        self._repositories: dict[str, _LazyProvider] = {}
        self._clients: dict[str, _LazyProvider] = {}
        self._use_cases: dict[str, _LazyProvider] = {}

        if database_provider is not None:
            self.register_database(database_provider)
        for name, factory in (repository_providers or {}).items():
            self.register_repository(name, factory)
        for name, factory in (client_providers or {}).items():
            self.register_client(name, factory)
        for name, factory in (use_case_providers or {}).items():
            self.register_use_case(name, factory)

    @property
    def database(self) -> Any:
        with self._lock:
            provider = self._database
            if provider is None:
                raise ProviderNotRegisteredError("database provider is not registered")
            return self._resolve(provider)

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    def repository(self, name: str) -> Any:
        return self._resolve_named("repository", name, self._repositories)

    def client(self, name: str) -> Any:
        return self._resolve_named("client", name, self._clients)

    def use_case(self, name: str) -> Any:
        return self._resolve_named("use-case", name, self._use_cases)

    def register_database(self, factory: ProviderFactory[Any], *, replace: bool = False) -> None:
        with self._lock:
            self._ensure_open()
            if self._database is not None and not replace:
                raise ValueError("database provider is already registered")
            if self._database is not None and self._database.materialized:
                raise RuntimeError("cannot replace a materialized database provider")
            self._database = _LazyProvider("database", factory)

    def register_repository(
        self,
        name: str,
        factory: ProviderFactory[Any],
        *,
        replace: bool = False,
    ) -> None:
        self._register_named("repository", name, factory, self._repositories, replace=replace)

    def register_client(
        self,
        name: str,
        factory: ProviderFactory[Any],
        *,
        replace: bool = False,
    ) -> None:
        self._register_named("client", name, factory, self._clients, replace=replace)

    def register_use_case(
        self,
        name: str,
        factory: ProviderFactory[Any],
        *,
        replace: bool = False,
    ) -> None:
        self._register_named("use-case", name, factory, self._use_cases, replace=replace)

    def close(self) -> None:
        with self._close_lock:
            with self._lock:
                if self._closed and not self._materialization_order:
                    return
                self._closed = True
                materialized = list(reversed(self._materialization_order))

            errors: list[Exception] = []
            attempted_ids: set[int] = set()
            completed_ids: set[int] = set()
            for provider in materialized:
                value = provider.value
                identity = id(value)
                if identity in attempted_ids:
                    continue
                attempted_ids.add(identity)
                close = getattr(value, "close", None)
                if not callable(close):
                    completed_ids.add(identity)
                    continue
                try:
                    close()
                except Exception as exc:
                    errors.append(exc)
                else:
                    completed_ids.add(identity)

            with self._lock:
                self._materialization_order = [
                    provider for provider in self._materialization_order if id(provider.value) not in completed_ids
                ]
            if errors:
                raise ExceptionGroup("one or more container resources failed to close", errors)

    def _register_named(
        self,
        kind: str,
        name: str,
        factory: ProviderFactory[Any],
        registry: dict[str, _LazyProvider],
        *,
        replace: bool,
    ) -> None:
        normalized = _provider_name(name)
        with self._lock:
            self._ensure_open()
            current = registry.get(normalized)
            if current is not None and not replace:
                raise ValueError(f"{kind} provider {normalized!r} is already registered")
            if current is not None and current.materialized:
                raise RuntimeError(f"cannot replace materialized {kind} provider {normalized!r}")
            registry[normalized] = _LazyProvider(f"{kind}:{normalized}", factory)

    def _resolve_named(
        self,
        kind: str,
        name: str,
        registry: Mapping[str, _LazyProvider],
    ) -> Any:
        normalized = _provider_name(name)
        with self._lock:
            provider = registry.get(normalized)
            if provider is None:
                raise ProviderNotRegisteredError(f"{kind} provider {normalized!r} is not registered")
            return self._resolve(provider)

    def _resolve(self, provider: _LazyProvider) -> Any:
        self._ensure_open()
        if provider.materialized:
            return provider.value
        if provider.resolving:
            chain = " -> ".join([*self._resolution_stack, provider.label])
            raise CircularProviderDependencyError(f"circular provider dependency: {chain}")

        provider.resolving = True
        self._resolution_stack.append(provider.label)
        try:
            value = provider.factory(self)
        except BaseException:
            provider.resolving = False
            raise
        else:
            provider.value = value
            provider.resolving = False
            self._materialization_order.append(provider)
            return value
        finally:
            self._resolution_stack.pop()

    def _ensure_open(self) -> None:
        if self._closed:
            raise ContainerClosedError("application container is closed")


def _provider_name(name: str) -> str:
    normalized = str(name).strip()
    if not normalized:
        raise ValueError("provider name must not be empty")
    return normalized
