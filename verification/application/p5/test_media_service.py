from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import SimpleNamespace
from typing import Any

import pytest

from etk.domains.media.models import (
    ImagePolicyKey,
    ImagePolicyRecord,
    MediaIdentity,
    MediaItemType,
    MetadataAggregate,
    ServiceResult,
)
from etk.domains.media.service import ImagePolicyUnavailable, MediaService
from etk.platform import Settings


class FakeRepository:
    def __init__(self) -> None:
        self.identity: MediaIdentity | None = MediaIdentity(
            "100",
            "movie",
            MediaItemType.MOVIE,
        )
        self.aggregate: MetadataAggregate | None = MetadataAggregate(
            identity=self.identity,
            requested_type=MediaItemType.MOVIE,
            row={
                "tmdb_id": "100",
                "item_type": "Movie",
                "title": "Fixture Movie",
                "metadata_ready": True,
                "actors_ready": False,
                "genres_json": [],
                "keywords_json": [],
                "production_companies_json": [],
                "networks_json": [],
                "official_rating_json": {"US": "PG"},
                "emby_item_ids_json": ["item-100"],
            },
        )
        self.backfill_targets: tuple[MediaIdentity, ...] = ()
        self.policy: ImagePolicyRecord | None = None
        self.replaced: list[tuple[ImagePolicyKey, list[Mapping[str, Any]], list[Mapping[str, Any]]]] = []
        self.previous_images: tuple[Mapping[str, Any], ...] = ()
        self.bind_result: Mapping[str, Any] = {"updated": True, "tmdb_id": "100"}
        self.bind_calls: list[Mapping[str, Any]] = []
        self.event_reservations: dict[str, str] = {}
        self.released_events: list[tuple[str, str]] = []
        self.referenced_sources: set[str] = set()

    def resolve_by_path(
        self,
        path: str,
        requested_type: MediaItemType,
        *,
        season_number: int | None = None,
        episode_number: int | None = None,
    ) -> MediaIdentity | None:
        del path, requested_type, season_number, episode_number
        return self.identity

    def resolve_by_fingerprint(
        self,
        requested_type: MediaItemType,
        *,
        sha1: str = "",
        pick_code: str = "",
    ) -> MediaIdentity | None:
        del requested_type, sha1, pick_code
        return self.identity

    def resolve_by_emby_id(self, item_id: str) -> MediaIdentity | None:
        del item_id
        return self.identity

    def load_metadata(
        self,
        identity: MediaIdentity,
        requested_type: MediaItemType,
    ) -> MetadataAggregate | None:
        del identity, requested_type
        return self.aggregate

    def find_backfill_targets_by_emby_ids(
        self,
        item_ids: Sequence[str],
    ) -> tuple[MediaIdentity, ...]:
        del item_ids
        return self.backfill_targets

    def find_backfill_targets_by_library(
        self,
        library_id: str,
    ) -> tuple[MediaIdentity, ...]:
        del library_id
        return self.backfill_targets

    def load_setting(self, key: str) -> Any:
        del key
        return None

    def get_image_policy(self, key: ImagePolicyKey) -> ImagePolicyRecord | None:
        del key
        return self.policy

    def replace_image_policy(
        self,
        key: ImagePolicyKey,
        rules: Sequence[Mapping[str, Any]],
        images: Sequence[Mapping[str, Any]],
    ) -> tuple[Mapping[str, Any], ...]:
        self.replaced.append((key, list(rules), list(images)))
        return self.previous_images

    def source_is_referenced(self, source_url: str) -> bool:
        return source_url in self.referenced_sources

    def bind_emby_item(self, **_kwargs: Any) -> Mapping[str, Any]:
        self.bind_calls.append(dict(_kwargs))
        return self.bind_result

    def reserve_emby_event(self, event_key: str, ttl_seconds: int) -> str | None:
        del ttl_seconds
        if event_key in self.event_reservations:
            return None
        token = f"token-{len(self.event_reservations) + 1}"
        self.event_reservations[event_key] = token
        return token

    def release_emby_event(self, event_key: str, token: str) -> bool:
        self.released_events.append((event_key, token))
        return self.event_reservations.pop(event_key, None) == token


class FakeJobs:
    def __init__(self) -> None:
        self.submissions: list[tuple[str, Mapping[str, Any] | None, Mapping[str, Any]]] = []

    def submit_task(
        self,
        task_key: str,
        parameters: Mapping[str, Any] | None = None,
        **options: Any,
    ) -> Any:
        self.submissions.append((task_key, parameters, options))
        return SimpleNamespace()

    def current(self) -> Any:
        return None


class FakeImages:
    def __init__(self) -> None:
        self.archived_metadata: list[tuple[Mapping[str, Any], str]] = []
        self.live_calls = 0
        self.candidates: list[dict[str, Any]] = []
        self.archived_policy: list[dict[str, Any]] | None = None
        self.discarded: list[str] = []

    def screenshot_token(self, content_hash: str) -> str:
        return f"etk-cache://{content_hash}"

    def archive_metadata(
        self,
        payload: Mapping[str, Any],
        base_url: str,
    ) -> Mapping[str, Any]:
        self.archived_metadata.append((dict(payload), base_url))
        return payload

    def live_candidates(
        self,
        requested_type: MediaItemType,
        identity: MediaIdentity,
        *,
        include_all_languages: bool,
    ) -> list[dict[str, Any]]:
        del requested_type, identity, include_all_languages
        self.live_calls += 1
        return list(self.candidates)

    def archive_policy_images(
        self,
        images: Sequence[Mapping[str, Any]],
        base_url: str,
    ) -> list[dict[str, Any]]:
        del base_url
        return list(self.archived_policy) if self.archived_policy is not None else [dict(value) for value in images]

    def discard_unreferenced_sources(self, sources: Sequence[str]) -> None:
        self.discarded.extend(sources)

    def refresh_images(
        self,
        requested_type: MediaItemType,
        identity: MediaIdentity,
    ) -> int:
        del requested_type, identity
        return 2

    def schedule_missing_collection_cleanup(self, tmdb_id: str) -> None:
        del tmdb_id
        return None


class FakeEventGateway:
    def __init__(self) -> None:
        self.calls: list[Mapping[str, Any]] = []
        self.result = ServiceResult({"status": "handled"}, 202)

    def handle(
        self,
        payload: Mapping[str, Any],
        *,
        host_url: str,
        remote_addr: str,
    ) -> ServiceResult:
        del host_url, remote_addr
        self.calls.append(dict(payload))
        return self.result


class FakeEvents:
    def __init__(self) -> None:
        self.p5 = FakeEventGateway()
        self.future = FakeEventGateway()


def _service() -> tuple[MediaService, FakeRepository, FakeJobs, FakeImages, FakeEvents]:
    repository = FakeRepository()
    jobs = FakeJobs()
    images = FakeImages()
    events = FakeEvents()
    service = MediaService(
        repository=repository,
        settings=lambda: Settings(),
        jobs=jobs,
        images=images,
        p5_events=events.p5,
        future_events=events.future,
    )
    return service, repository, jobs, images, events


def test_bridge_metadata_uses_native_repository_and_preserves_no_store() -> None:
    service, _repository, _jobs, images, _events = _service()

    result = service.provide_bridge_metadata(
        "/fixture/movie.mkv",
        MediaItemType.MOVIE,
        base_url="http://fixture.invalid/",
    )

    assert result.status_code == 200
    assert result.headers == {"Cache-Control": "no-store"}
    assert result.payload["tmdb_id"] == "100"
    assert result.payload["official_rating"] == "PG"
    assert images.archived_metadata == [(result.payload, "http://fixture.invalid/")]


def test_bridge_metadata_not_found_does_not_archive() -> None:
    service, repository, _jobs, images, _events = _service()
    repository.identity = None

    result = service.provide_bridge_metadata(
        "/missing",
        MediaItemType.MOVIE,
        base_url="http://fixture.invalid/",
    )

    assert result == ServiceResult({"error": "metadata path identity not found"}, 404)
    assert images.archived_metadata == []


def test_item_backfill_submits_one_native_catalog_job_with_json_targets() -> None:
    service, repository, jobs, _images, _events = _service()
    repository.backfill_targets = (
        MediaIdentity("100", "movie", MediaItemType.MOVIE),
        MediaIdentity("200", "tv", MediaItemType.SERIES),
    )

    result = service.backfill_items(["one", "one", "two"])

    assert result == ServiceResult({"ok": True, "submitted": True, "count": 2}, 202)
    assert jobs.submissions == [
        (
            "backfill-media-metadata",
            {
                "targets": [
                    {"tmdb_id": "100", "media_type": "movie"},
                    {"tmdb_id": "200", "media_type": "tv"},
                ]
            },
            {
                "trigger_type": "system",
                "silent": True,
                "reject_when_busy": True,
            },
        )
    ]


def test_native_item_rebind_only_calls_the_repository_without_submitting_processing() -> None:
    service, repository, jobs, images, events = _service()

    result = service.bind_emby_item(
        item_id="movie-new",
        item_type=MediaItemType.MOVIE,
        item_path="/fixture/movie.mkv",
        sha1="A" * 40,
        pick_code="pc-movie",
        tmdb_id="100",
    )

    assert result == repository.bind_result
    assert repository.bind_calls == [
        {
            "item_id": "movie-new",
            "item_type": MediaItemType.MOVIE,
            "item_path": "/fixture/movie.mkv",
            "sha1": "A" * 40,
            "pick_code": "pc-movie",
            "tmdb_id": "100",
            "series_id": "",
            "season_id": "",
        }
    ]
    assert jobs.submissions == []
    assert images.live_calls == 0
    assert events.p5.calls == []
    assert events.future.calls == []


def test_unchanged_image_policy_reuses_archive_without_tmdb_lookup() -> None:
    service, repository, _jobs, images, _events = _service()
    key = ImagePolicyKey.create(MediaItemType.MOVIE, "100")
    repository.policy = ImagePolicyRecord(
        key,
        rules=({"type": "Primary", "limit": 1, "min_width": 0},),
        images=({"type": "Primary", "source_url": "manual", "manual": True, "index": 0},),
    )
    images.archived_policy = [
        {
            "type": "Primary",
            "source_url": "manual",
            "manual": True,
            "index": 0,
            "url": "http://fixture.invalid/cache/manual",
        }
    ]

    result = service.sync_image_policy(
        MediaItemType.MOVIE,
        path="/fixture/movie.mkv",
        tmdb_id="",
        season_number=None,
        episode_number=None,
        rules=[{"Type": "Primary", "Limit": 1}],
        force=False,
        base_url="http://fixture.invalid/",
    )

    assert result.status_code == 200
    assert images.live_calls == 0
    assert repository.replaced == []
    assert result.payload["images"][0]["url"] == "http://fixture.invalid/cache/manual"


def test_changed_image_policy_preserves_manual_and_discards_only_removed_sources() -> None:
    service, repository, _jobs, images, _events = _service()
    key = ImagePolicyKey.create(MediaItemType.MOVIE, "100")
    repository.policy = ImagePolicyRecord(
        key,
        rules=({"type": "Primary", "limit": 1, "min_width": 0},),
        images=({"type": "Primary", "source_url": "manual", "manual": True, "index": 0},),
    )
    repository.previous_images = (
        {"type": "Primary", "source_url": "manual"},
        {"type": "Backdrop", "source_url": "old-background"},
    )
    images.candidates = [
        {"type": "Primary", "source_url": "new-primary", "width": 1000},
        {"type": "Backdrop", "source_url": "new-background", "width": 1920},
    ]

    result = service.sync_image_policy(
        MediaItemType.MOVIE,
        path="/fixture/movie.mkv",
        tmdb_id="",
        season_number=None,
        episode_number=None,
        rules=[
            {"type": "Primary", "limit": 1},
            {"type": "Backdrop", "limit": 1},
        ],
        force=False,
        base_url="http://fixture.invalid/",
    )

    assert result.status_code == 200
    assert images.live_calls == 1
    stored = repository.replaced[0][2]
    assert [value["source_url"] for value in stored] == ["manual", "new-background"]
    assert images.discarded == ["old-background"]


def test_missing_tmdb_key_preserves_legacy_status_by_image_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, _repository, _jobs, images, _events = _service()

    def unavailable(*_args: Any, **_kwargs: Any) -> Any:
        raise ImagePolicyUnavailable("tmdb api key is not configured")

    monkeypatch.setattr(images, "live_candidates", unavailable)
    search = service.search_bridge_images(
        MediaItemType.MOVIE,
        path="/fixture/movie.mkv",
        tmdb_id="",
        season_number=None,
        episode_number=None,
        include_all_languages=False,
    )
    sync = service.sync_image_policy(
        MediaItemType.MOVIE,
        path="/fixture/movie.mkv",
        tmdb_id="",
        season_number=None,
        episode_number=None,
        rules=[{"type": "Primary", "limit": 1}],
        force=False,
        base_url="http://fixture.invalid/",
    )

    monkeypatch.setattr(images, "refresh_images", unavailable)
    refresh = service.refresh_bridge_images(
        MediaItemType.MOVIE,
        path="/fixture/movie.mkv",
        tmdb_id="",
        season_number=None,
        episode_number=None,
    )

    assert search == ServiceResult({"error": "tmdb api key is not configured"}, 503)
    assert sync == ServiceResult({"error": "tmdb api key is not configured"}, 502)
    assert refresh == ServiceResult(
        {"ok": False, "error": "tmdb api key is not configured"},
        503,
    )


def test_invalid_plugin_event_is_rejected_before_event_dispatch() -> None:
    service, _repository, _jobs, _images, events = _service()

    invalid = service.handle_emby_event(
        {"_etk_source": "wrong", "Event": "metadata.update"},
        host_url="http://fixture.invalid/",
        remote_addr="127.0.0.1",
    )
    valid = service.handle_emby_event(
        {"_etk_source": "ETKMediaInfoBridge", "Event": "metadata.update"},
        host_url="http://fixture.invalid/",
        remote_addr="127.0.0.1",
    )

    assert invalid == ServiceResult({"status": "invalid_plugin_event"}, 400)
    assert valid == ServiceResult({"status": "handled"}, 202)
    assert [value["Event"] for value in events.p5.calls] == ["metadata.update"]
    assert events.future.calls == []


@pytest.mark.parametrize(
    ("event_name", "gateway_name"),
    [
        ("image.update", "p5"),
        ("item.markplayed", "future"),
    ],
)
def test_identical_emby_event_replay_is_suppressed_before_owned_dispatch(
    event_name: str,
    gateway_name: str,
) -> None:
    service, _repository, _jobs, _images, events = _service()
    payload = {
        "_etk_source": "ETKMediaInfoBridge",
        "Event": event_name,
        "EventId": f"event-{gateway_name}",
        "User": {"Id": "user-1", "Name": "Fixture User"},
        "Item": {"Id": "item-1", "Type": "Movie", "Name": "Fixture Movie"},
    }

    first = service.handle_emby_event(
        payload,
        host_url="http://fixture.invalid/",
        remote_addr="127.0.0.1",
    )
    replay = service.handle_emby_event(
        payload,
        host_url="http://fixture.invalid/",
        remote_addr="127.0.0.1",
    )

    assert first == ServiceResult({"status": "handled"}, 202)
    assert replay == ServiceResult({"status": "duplicate_event_ignored"})
    gateway = getattr(events, gateway_name)
    assert [value["Event"] for value in gateway.calls] == [event_name]
    other_gateway = events.future if gateway_name == "p5" else events.p5
    assert other_gateway.calls == []


def test_failed_emby_event_releases_reservation_for_retry() -> None:
    service, repository, _jobs, _images, events = _service()
    events.p5.result = ServiceResult({"status": "failed"}, 503)
    payload = {
        "_etk_source": "ETKMediaInfoBridge",
        "Event": "metadata.update",
        "EventId": "retryable-event",
    }

    first = service.handle_emby_event(payload, host_url="http://fixture/", remote_addr="127.0.0.1")
    retry = service.handle_emby_event(payload, host_url="http://fixture/", remote_addr="127.0.0.1")

    assert first.status_code == retry.status_code == 503
    assert len(events.p5.calls) == 2
    assert events.future.calls == []
    assert len(repository.released_events) == 2


def test_unknown_emby_event_preserves_legacy_ignored_boundary_without_claiming_an_owner() -> None:
    service, _repository, _jobs, _images, events = _service()
    payload = {
        "_etk_source": "ETKMediaInfoBridge",
        "Event": "plugin.future-event",
        "EventId": "unknown-event-1",
    }

    result = service.handle_emby_event(
        payload,
        host_url="http://fixture.invalid/",
        remote_addr="127.0.0.1",
    )
    replay = service.handle_emby_event(
        payload,
        host_url="http://fixture.invalid/",
        remote_addr="127.0.0.1",
    )

    assert result == ServiceResult({"status": "event_ignored_not_in_trigger_list"})
    assert replay == ServiceResult({"status": "duplicate_event_ignored"})
    assert events.p5.calls == []
    assert events.future.calls == []


def test_image_archive_failure_does_not_publish_policy_or_cleanup_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository, _jobs, images, _events = _service()
    images.candidates = [{"type": "Primary", "source_url": "new", "width": 1000}]

    def fail_archive(
        values: Sequence[Mapping[str, Any]],
        base_url: str,
    ) -> list[dict[str, Any]]:
        del values, base_url
        raise RuntimeError("fixture archive failed")

    monkeypatch.setattr(images, "archive_policy_images", fail_archive)

    with pytest.raises(RuntimeError, match="fixture archive failed"):
        service.sync_image_policy(
            MediaItemType.MOVIE,
            path="/fixture/movie.mkv",
            tmdb_id="",
            season_number=None,
            episode_number=None,
            rules=[{"type": "Primary", "limit": 1}],
            force=False,
            base_url="http://fixture.invalid/",
        )

    assert repository.replaced == []
    assert images.discarded == []


def test_removed_image_source_is_kept_when_another_policy_references_it() -> None:
    service, repository, _jobs, images, _events = _service()
    key = ImagePolicyKey.create(MediaItemType.MOVIE, "100")
    repository.policy = ImagePolicyRecord(
        key,
        rules=({"type": "Primary", "limit": 1, "min_width": 0},),
        images=({"type": "Primary", "source_url": "shared-old", "index": 0},),
    )
    repository.previous_images = ({"type": "Primary", "source_url": "shared-old"},)
    repository.referenced_sources.add("shared-old")
    images.candidates = [{"type": "Primary", "source_url": "new", "width": 1000}]

    result = service.sync_image_policy(
        MediaItemType.MOVIE,
        path="/fixture/movie.mkv",
        tmdb_id="",
        season_number=None,
        episode_number=None,
        rules=[{"type": "Primary", "limit": 1, "min_width": 500}],
        force=True,
        base_url="http://fixture.invalid/",
    )

    assert result.status_code == 200
    assert images.discarded == []
