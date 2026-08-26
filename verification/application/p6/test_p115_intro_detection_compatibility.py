from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

from etk.compatibility.intro_detection import (
    LazyPersistentIntroDetectionGateway,
    clear_persistent_intro_detection_gateway,
    install_persistent_intro_detection_gateway,
)
from etk.domains.p115.intro_detection import (
    IntroDetectionPolicySnapshot,
    IntroEpisodeSnapshot,
    IntroTriggerMode,
)
from etk.domains.p115.intro_detection_compatibility import NativeP115IntroDetectionGateway


def _episode(number: int) -> IntroEpisodeSnapshot:
    return IntroEpisodeSnapshot.create(
        item_id=f"item-{number}",
        series_tmdb_id="series-42",
        season_number=1,
        episode_number=number,
        sha1=f"{number:040X}",
        pick_code=f"pick-{number}",
        source_library_id="library-a",
        runtime_seconds=1500,
        cache_revision=number,
    )


class _Submission:
    def __init__(self, *, created: bool = True) -> None:
        self.created = created
        self.intents: list[Any] = []

    def submit(self, intent: Any) -> Any:
        self.intents.append(intent)
        return SimpleNamespace(
            created=self.created,
            operation=SimpleNamespace(id=uuid4()),
        )


class _Source:
    def __init__(self, mode: IntroTriggerMode = IntroTriggerMode.IMPORT) -> None:
        self.policy = IntroDetectionPolicySnapshot.create(
            trigger_mode=mode,
            credits_enabled=True,
            library_ids=("library-a",),
        )
        self.season = (_episode(1), _episode(2), _episode(3))

    def snapshot(self) -> IntroDetectionPolicySnapshot:
        return self.policy

    def season_is_favorite(self, series_tmdb_id: str, season_number: int) -> bool:
        return series_tmdb_id == "series-42" and season_number == 1

    def resolve_item(self, item: Any, sha1: str = "") -> tuple[IntroEpisodeSnapshot, ...]:
        del item, sha1
        return self.season

    def resolve_episode_ids(
        self,
        episode_ids: Any,
        *,
        require_existing_detection: bool,
    ) -> tuple[tuple[tuple[IntroEpisodeSnapshot, ...], ...], int]:
        del require_existing_detection
        return ((self.season,), int("missing" in episode_ids))

    def resolve_favorite_item(self, item: Any) -> tuple[tuple[IntroEpisodeSnapshot, ...], ...]:
        del item
        return (self.season,)

    def list_library_seasons(self, *, limit: int, library_ids: Any) -> Any:
        assert limit == 50
        assert tuple(library_ids) == ("library-a",)
        return (self.season,)

    def list_favorite_seasons(self, *, limit: int, library_ids: Any) -> Any:
        del limit, library_ids
        return (self.season,)


def _gateway(source: _Source, submission: _Submission) -> NativeP115IntroDetectionGateway:
    return NativeP115IntroDetectionGateway(
        submission=submission,
        source=source,
        account_id=lambda: "115001",
    )


def test_lazy_gateway_resolves_provider_only_on_first_business_call() -> None:
    source = _Source()
    submission = _Submission()
    gateway = _gateway(source, submission)
    provider_calls: list[str] = []
    lazy_gateway = LazyPersistentIntroDetectionGateway(lambda: provider_calls.append("resolve") or gateway)

    assert provider_calls == []

    result = lazy_gateway.enqueue_item(
        {"Id": "item-1", "Type": "Episode", "SeriesId": "series-emby"},
        "A" * 40,
    )

    assert result["ok"] is True
    assert provider_calls == ["resolve"]
    assert len(submission.intents) == 1


def test_native_gateway_reserves_one_immutable_season_operation() -> None:
    source = _Source()
    submission = _Submission()
    gateway = _gateway(source, submission)

    result = gateway.enqueue_item(
        {"Id": "item-1", "Type": "Episode", "SeriesId": "series-emby"},
        "A" * 40,
    )

    assert result["ok"] is True
    assert result["queued"] is True
    assert result["created"] == 1
    assert len(submission.intents) == 1
    request = submission.intents[0].request
    assert [episode["episode_number"] for episode in request["episodes"]] == [1, 2, 3]
    serialized = repr(request).lower()
    assert "http://" not in serialized
    assert "https://" not in serialized


def test_native_gateway_backfill_is_idempotent_and_keeps_empty_external_payload_semantics() -> None:
    source = _Source()
    submission = _Submission(created=False)
    gateway = _gateway(source, submission)

    result = gateway.enqueue_active_backfill()

    assert result == {
        "ok": True,
        "queued": False,
        "completed": True,
        "count": 0,
        "scope": "library",
        "created": 0,
        "accepted": 1,
        "duplicates": 1,
        "skipped_seasons": 0,
        "candidates": 1,
        "operation_ids": result["operation_ids"],
    }
    assert len(result["operation_ids"]) == 1


def test_installed_gateway_prevents_legacy_intro_queue_or_daemon_start(monkeypatch) -> None:
    from handler import intro_detection_service

    calls: list[tuple[Any, str, str]] = []

    class _Gateway:
        def enqueue_item(self, item: Any, sha1: str = "", *, trigger: str = "import") -> dict[str, Any]:
            calls.append((item, sha1, trigger))
            return {"ok": True, "queued": True, "native": True}

    gateway = _Gateway()
    monkeypatch.setattr(
        intro_detection_service,
        "_start_worker",
        lambda: (_ for _ in ()).throw(AssertionError("native gateway must not start the legacy daemon")),
    )
    monkeypatch.setattr(
        intro_detection_service._queue,
        "put_nowait",
        lambda _payload: (_ for _ in ()).throw(AssertionError("native gateway must not use the legacy queue")),
    )
    install_persistent_intro_detection_gateway(cast(Any, gateway))
    try:
        result = intro_detection_service.enqueue_item(
            {"Id": "item-1", "Type": "Episode", "SeriesId": "series-1"},
            "A" * 40,
        )
    finally:
        clear_persistent_intro_detection_gateway(cast(Any, gateway))

    assert result == {"ok": True, "queued": True, "native": True}
    assert calls == [
        (
            {"Id": "item-1", "Type": "Episode", "SeriesId": "series-1"},
            "A" * 40,
            "import",
        )
    ]
