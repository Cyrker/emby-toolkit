from __future__ import annotations

from collections.abc import Sequence
from types import SimpleNamespace
from typing import Any

from pydantic import SecretStr

import etk.domains.media.embeddings_adapter as embeddings_adapter
from etk.domains.media.embeddings import MediaEmbeddingCandidate, MediaEmbeddingService
from etk.domains.media.embeddings_adapter import ConfiguredMediaEmbeddingProvider
from etk.platform import Settings
from etk.platform.tasks import TaskOutcomeStatus


class FakeRepository:
    def __init__(self, candidates: Sequence[MediaEmbeddingCandidate]) -> None:
        self.candidates = tuple(candidates)
        self.stored: list[tuple[MediaEmbeddingCandidate, tuple[float, ...]]] = []

    def count_pending(self) -> int:
        return len(self.candidates)

    def list_pending(
        self,
        *,
        after: tuple[str, str] | None,
        limit: int,
    ) -> Sequence[MediaEmbeddingCandidate]:
        values = [candidate for candidate in self.candidates if after is None or _identity(candidate) > after]
        return tuple(values[:limit])

    def store(self, candidate: MediaEmbeddingCandidate, embedding: Sequence[float]) -> bool:
        self.stored.append((candidate, tuple(embedding)))
        return candidate.tmdb_id != "changed"


class FakeProvider:
    def __init__(self, failures: set[str] | None = None) -> None:
        self.failures = failures or set()
        self.calls: list[str] = []

    def embed(self, text_value: str) -> Sequence[float] | None:
        self.calls.append(text_value)
        if text_value in self.failures:
            return None
        return (0.25, 0.75)


class FakeProgress:
    def __init__(self) -> None:
        self.checkpoints = 0
        self.updates: list[tuple[int, str]] = []

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        self.checkpoints += 1


def _identity(candidate: MediaEmbeddingCandidate) -> tuple[str, str]:
    return candidate.tmdb_id, candidate.item_type


def test_media_embedding_service_is_processor_free_and_fences_changed_rows() -> None:
    candidates = (
        MediaEmbeddingCandidate("1", "Movie", "one"),
        MediaEmbeddingCandidate("changed", "Series", "two"),
    )
    repository = FakeRepository(candidates)
    provider = FakeProvider()
    progress = FakeProgress()

    outcome = MediaEmbeddingService(repository, provider, enabled=lambda: True).execute(progress)

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details == {
        "pending": 2,
        "scanned": 2,
        "generated": 1,
        "failed": 0,
        "failures": [],
        "state_changed": 1,
    }
    assert provider.calls == ["one", "two"]
    assert progress.checkpoints == 3
    assert progress.updates[-1][0] == 100


def test_media_embedding_service_finishes_partial_without_reselecting_failures() -> None:
    repository = FakeRepository(
        (
            MediaEmbeddingCandidate("1", "Movie", "bad"),
            MediaEmbeddingCandidate("2", "Series", "good"),
        )
    )
    provider = FakeProvider({"bad"})

    outcome = MediaEmbeddingService(repository, provider, enabled=lambda: True).execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details["generated"] == 1
    assert outcome.details["failed"] == 1
    assert outcome.details["failures"] == [{"tmdb_id": "1", "item_type": "Movie", "error_type": "RuntimeError"}]
    assert provider.calls == ["bad", "good"]


def test_media_embedding_service_skips_when_disabled_or_complete() -> None:
    disabled_repository = FakeRepository((MediaEmbeddingCandidate("1", "Movie", "one"),))
    disabled_provider = FakeProvider()

    disabled = MediaEmbeddingService(
        disabled_repository,
        disabled_provider,
        enabled=lambda: False,
    ).execute(FakeProgress())
    complete = MediaEmbeddingService(
        FakeRepository(()),
        FakeProvider(),
        enabled=lambda: True,
    ).execute(FakeProgress())

    assert disabled.status is TaskOutcomeStatus.SKIPPED
    assert disabled_provider.calls == []
    assert complete.status is TaskOutcomeStatus.SKIPPED


def test_configured_embedding_provider_reuses_client_for_same_identity(monkeypatch: Any) -> None:
    clients: list[Any] = []

    class FakeEmbeddings:
        def create(self, *, input: str, model: str) -> Any:
            assert model == "text-embedding-3-small"
            return SimpleNamespace(data=[SimpleNamespace(embedding=[len(input), 1.0])])

    class FakeOpenAI:
        def __init__(self, **_kwargs: Any) -> None:
            clients.append(self)
            self.embeddings = FakeEmbeddings()

    monkeypatch.setattr(embeddings_adapter, "OpenAI", FakeOpenAI)
    settings = Settings(
        ai_provider="openai",
        ai_api_key=SecretStr("fixture-key"),
        ai_base_url="https://api.openai.example/v1",
    )
    provider = ConfiguredMediaEmbeddingProvider(lambda: settings)

    assert provider.embed("one") == (3.0, 1.0)
    assert provider.embed("second") == (6.0, 1.0)
    assert len(clients) == 1
