from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from urllib.parse import parse_qs
from uuid import UUID

import httpx
import pytest
from pydantic import SecretStr

from etk.domains.media import cast_editor_adapters as cast_editor_adapters_module
from etk.domains.media import event_adapters as event_adapters_module
from etk.domains.media import image_adapters as image_adapters_module
from etk.domains.media import image_editing_adapters as image_editing_adapters_module
from etk.domains.media import repository as media_repository_module
from etk.domains.media.cast_editor import CastTranslationCommand, ManualCastUpdateCommand
from etk.domains.media.cast_editor_adapters import (
    HttpxMediaCastEditorGateway,
    NativeCastUpdateOperation,
    SqlAlchemyCastEditorRepository,
)
from etk.domains.media.effect_execution import MediaEffectDeliveryIndeterminate, MediaEffectDeliveryRejected
from etk.domains.media.effects import MediaEffectProjection
from etk.domains.media.event_adapters import (
    HttpxP5MediaEventProvider,
    NativeMetadataEventOperation,
    NativeP5MediaEventGateway,
)
from etk.domains.media.event_effects import P5MediaEventEffectDelivery
from etk.domains.media.image_adapters import HttpxMediaImageGateway, StaticMediaMappingDefaults
from etk.domains.media.image_editing import ImageUpdateCommand
from etk.domains.media.image_editing_adapters import HttpxMediaImageEditingGateway
from etk.domains.media.image_editing_effects import ManualImageEffectDelivery
from etk.domains.media.image_proxy_adapters import HttpxMediaImageProxyGateway
from etk.domains.media.models import ImagePolicyKey, ImagePolicyRecord, MediaIdentity, MediaItemType, ServiceResult
from etk.domains.media.repository import SqlAlchemyMediaRepository
from etk.platform import Settings


def _settings(**values: Any) -> Settings:
    defaults = {
        "emby_server_url": "https://emby.fixture.invalid",
        "emby_api_key": SecretStr("fixture-emby-token"),
        "emby_user_id": "fixture-user",
        "tmdb_api_base_url": "https://tmdb.fixture.invalid/3",
        "tmdb_api_key": SecretStr("fixture-tmdb-token"),
    }
    defaults.update(values)
    return Settings(
        **defaults,
    )


def _client_factory(handler: Any) -> Any:
    transport = httpx.MockTransport(handler)
    return lambda: httpx.Client(transport=transport)


class FakeImageStore:
    def __init__(self, root: Path | None = None) -> None:
        self.root = root
        self.by_hash: dict[str, dict[str, Any]] = {}
        self.by_source: dict[str, dict[str, Any]] = {}
        self.discarded: list[tuple[str, ...]] = []
        self.fail_store = False

    @staticmethod
    def cache_token(content_hash: str) -> str:
        return f"etk-cache://{content_hash}"

    def get(self, content_hash: str) -> dict[str, Any] | None:
        return self.by_hash.get(content_hash)

    def get_source(self, source_url: str) -> dict[str, Any] | None:
        return self.by_source.get(source_url)

    def store(self, source_url: str, content: bytes, content_type: str) -> dict[str, Any]:
        if self.fail_store:
            raise OSError("fixture cache failure")
        import hashlib

        content_hash = hashlib.sha256(content).hexdigest()
        value = {
            "source_url": source_url,
            "content_hash": content_hash,
            "mime_type": content_type,
            "path": str((self.root or Path("/tmp")) / content_hash),
        }
        self.by_hash[content_hash] = value
        self.by_source[source_url] = value
        return value

    def discard_unreferenced_sources(self, sources: Sequence[str]) -> int:
        normalized = tuple(sources)
        self.discarded.append(normalized)
        for source in normalized:
            value = self.by_source.pop(source, None)
            if value is not None:
                self.by_hash.pop(str(value["content_hash"]), None)
        return len(normalized)


class FakeMediaRepository:
    def __init__(self) -> None:
        self.identity = MediaIdentity("100", "movie", MediaItemType.MOVIE)
        self.sync_result = True
        self.resolve_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self.image_updates: list[tuple[MediaIdentity, str, str]] = []
        self.policy: ImagePolicyRecord | None = None
        self.replacements: list[dict[str, Any]] = []
        self.metadata_items: list[tuple[str, Mapping[str, Any]]] = []
        self.recorded_screenshots: list[tuple[str, int, int, str]] = []
        self.cleared_screenshots: list[tuple[str, int, int]] = []
        self.record_screenshot_result = True
        self.clear_screenshot_result = True

    def resolve_by_path(self, *args: Any, **kwargs: Any) -> MediaIdentity | None:
        self.resolve_calls.append((args, kwargs))
        return self.identity

    def update_cached_image_path(self, identity: MediaIdentity, image_type: str, value: str) -> int:
        self.image_updates.append((identity, image_type, value))
        return 1

    def get_image_policy(self, key: ImagePolicyKey) -> ImagePolicyRecord | None:
        del key
        return self.policy

    def replace_image_policy(
        self,
        key: ImagePolicyKey,
        rules: Sequence[Mapping[str, Any]],
        images: Sequence[Mapping[str, Any]],
    ) -> tuple[Mapping[str, Any], ...]:
        del key
        self.replacements.append({"rules": list(rules), "images": list(images)})
        return tuple(self.policy.images if self.policy is not None else ())

    def sync_emby_metadata(self, item_id: str, item: Mapping[str, Any]) -> bool:
        self.metadata_items.append((item_id, item))
        return self.sync_result

    def original_language(self, identity: MediaIdentity) -> str:
        del identity
        return "zh"

    def replace_cached_images(
        self,
        requested_type: MediaItemType,
        identity: MediaIdentity,
        **values: Any,
    ) -> int:
        self.replacements.append({"requested_type": requested_type, "identity": identity, **values})
        return 1

    def record_episode_screenshot(
        self,
        series_tmdb_id: str,
        season_number: int,
        episode_number: int,
        content_hash: str,
    ) -> bool:
        self.recorded_screenshots.append((series_tmdb_id, season_number, episode_number, content_hash))
        return self.record_screenshot_result

    def clear_episode_screenshot(self, series_tmdb_id: str, season_number: int, episode_number: int) -> bool:
        self.cleared_screenshots.append((series_tmdb_id, season_number, episode_number))
        return self.clear_screenshot_result


def test_native_image_proxy_streams_and_replaces_caller_emby_api_key(tmp_path: Path) -> None:
    observed: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        observed.append(request)
        return httpx.Response(200, headers={"Content-Type": "image/jpeg"}, content=b"fixture-image")

    store = FakeImageStore(tmp_path)
    cached = store.store("fixture://cached", b"cached", "image/webp")
    gateway = HttpxMediaImageProxyGateway(
        lambda: _settings(),
        cast(Any, store),
        client_factory=_client_factory(respond),
    )

    cached_result = gateway.cached_image(str(cached["content_hash"]))
    external = gateway.fetch_external("https://images.fixture.invalid/poster.jpg")
    emby = gateway.fetch_emby(
        "Items/movie-1/Images/Primary",
        b"quality=90&api_key=caller-secret&tag=cover%2Fone",
    )

    assert cached_result is not None
    assert cached_result.content_type == "image/webp"
    assert b"".join(external.chunks) == b"fixture-image"
    assert b"".join(emby.chunks) == b"fixture-image"
    assert observed[0].headers["referer"] == "https://images.fixture.invalid/"
    assert observed[0].headers["user-agent"] == _settings().user_agent
    emby_query = parse_qs(observed[1].url.query.decode())
    assert emby_query == {
        "quality": ["90"],
        "tag": ["cover/one"],
        "api_key": ["fixture-emby-token"],
    }


def test_native_image_proxy_routes_external_images_and_emby_to_separate_clients(tmp_path: Path) -> None:
    routed: list[tuple[str, str]] = []

    def factory(label: str) -> Any:
        def respond(request: httpx.Request) -> httpx.Response:
            routed.append((label, request.url.host))
            return httpx.Response(200, headers={"Content-Type": "image/jpeg"}, content=b"fixture-image")

        return _client_factory(respond)

    gateway = HttpxMediaImageProxyGateway(
        lambda: _settings(),
        cast(Any, FakeImageStore(tmp_path)),
        client_factory=factory("direct"),
        external_client_factory=factory("external"),
    )

    external = gateway.fetch_external("https://images.fixture.invalid/poster.jpg")
    emby = gateway.fetch_emby("Items/movie-1/Images/Primary", b"")
    assert b"".join(external.chunks) == b"fixture-image"
    assert b"".join(emby.chunks) == b"fixture-image"
    assert routed == [
        ("external", "images.fixture.invalid"),
        ("direct", "emby.fixture.invalid"),
    ]


def test_native_image_editing_routes_emby_identity_and_tmdb_catalog_to_separate_clients() -> None:
    routed: list[tuple[str, str]] = []

    def direct(request: httpx.Request) -> httpx.Response:
        routed.append(("direct", request.url.host))
        return httpx.Response(
            200,
            json={"Id": "movie-1", "Type": "Movie", "ProviderIds": {"Tmdb": "100"}},
        )

    def external(request: httpx.Request) -> httpx.Response:
        routed.append(("external", request.url.host))
        return httpx.Response(200, json={"images": {"posters": [], "backdrops": [], "logos": []}})

    gateway = HttpxMediaImageEditingGateway(
        lambda: _settings(),
        FakeMediaRepository(),
        cast(Any, FakeImageStore()),
        client_factory=_client_factory(direct),
        external_client_factory=_client_factory(external),
    )

    catalog = gateway.tmdb_images("movie-1")
    assert catalog.posters == ()
    assert routed == [
        ("direct", "emby.fixture.invalid"),
        ("external", "tmdb.fixture.invalid"),
    ]


@pytest.mark.parametrize(
    ("write_result", "readback", "expected_status"),
    [
        (400, b"old-image", "rejected"),
        (503, b"old-image", "indeterminate"),
        (503, b"new-image", "confirmed"),
    ],
)
def test_native_image_editing_classifies_provider_write_from_readback(
    write_result: int,
    readback: bytes,
    expected_status: str,
) -> None:
    repository = FakeMediaRepository()
    store = FakeImageStore()
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(f"{request.method} {request.url.path}")
        if request.method == "GET" and "/Users/" in request.url.path:
            return httpx.Response(
                200,
                json={"Id": "movie-1", "Type": "Movie", "Path": "/library/movie.mkv"},
            )
        if request.method == "POST":
            return httpx.Response(write_result)
        return httpx.Response(200, headers={"Content-Type": "image/jpeg"}, content=readback)

    gateway = HttpxMediaImageEditingGateway(
        lambda: _settings(),
        repository,
        cast(Any, store),
        client_factory=_client_factory(respond),
    )
    outcome = gateway.update_image(
        ImageUpdateCommand(
            item_id="movie-1",
            image_type="poster",
            image_url="",
            image_bytes=b"new-image",
            content_type="image/jpeg",
        )
    )

    assert calls == [
        "GET /Users/fixture-user/Items/movie-1",
        "POST /Items/movie-1/Images/Primary",
        "GET /Items/movie-1/Images/Primary",
    ]
    if expected_status == "confirmed":
        assert outcome.success is True
        assert outcome.effect_status == ""
        assert len(repository.image_updates) == 1
        assert len(repository.replacements) == 1
    else:
        assert outcome.success is False
        assert outcome.effect_status == ("indeterminate" if expected_status == "indeterminate" else "")
        assert repository.image_updates == []


def test_native_image_editing_marks_confirmed_provider_write_indeterminate_when_local_cache_fails() -> None:
    repository = FakeMediaRepository()
    store = FakeImageStore()
    store.fail_store = True

    def respond(request: httpx.Request) -> httpx.Response:
        if "/Users/" in request.url.path:
            return httpx.Response(200, json={"Id": "movie-1", "Type": "Movie", "Path": "/movie.mkv"})
        if request.method == "POST":
            return httpx.Response(204)
        return httpx.Response(200, content=b"new-image")

    gateway = HttpxMediaImageEditingGateway(
        lambda: _settings(),
        repository,
        cast(Any, store),
        client_factory=_client_factory(respond),
    )
    outcome = gateway.update_image(ImageUpdateCommand("movie-1", "poster", "", b"new-image", "image/jpeg"))

    assert outcome.success is False
    assert outcome.effect_status == "indeterminate"
    assert "Emby 图片已更新" in outcome.message


class FakeJobs:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Mapping[str, Any], Mapping[str, Any]]] = []

    def submit_native_operation(self, key: str, parameters: Mapping[str, Any], **options: Any) -> Any:
        self.calls.append((key, dict(parameters), options))
        return SimpleNamespace(
            projection=SimpleNamespace(job=SimpleNamespace(id=UUID("00000000-0000-0000-0000-000000000555")))
        )


class FakeCastRepository:
    def __init__(self, *, fail_replace: bool = False) -> None:
        self.fail_replace = fail_replace
        self.replaced: list[tuple[Any, ...]] = []
        self.actors_result: tuple[dict[str, Any], ...] = (
            {
                "tmdb_id": 1001,
                "name": "Actor",
                "character": "Role",
                "profile_path": "/profile.jpg",
            },
        )

    def actors(self, _tmdb_id: str, _item_type: str) -> tuple[dict[str, Any], ...]:
        return self.actors_result

    def review(self, _item_id: str) -> Mapping[str, Any]:
        return {"score": 8.5, "error_message": "fixture-review"}

    def replace_cast(self, *args: Any, **kwargs: Any) -> None:
        self.replaced.append((*args, kwargs))
        if self.fail_replace:
            raise RuntimeError("fixture local projection failure")


class FakeTranslation:
    def translate(self, values: Sequence[str]) -> Mapping[str, str]:
        assert values == ("Actor", "Role")
        return {"Actor": "演员", "Role": "角色"}


def test_native_cast_editor_reads_translates_and_submits_fixed_operation() -> None:
    jobs = FakeJobs()
    repository = FakeCastRepository()

    def respond(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "Id": "movie-1",
                "Name": "Fixture Movie",
                "Type": "Movie",
                "ProviderIds": {"Tmdb": "100"},
                "People": [{"Id": "person-1", "ProviderIds": {"Tmdb": "1001"}}],
                "ProductionYear": 2026,
            },
        )

    gateway = HttpxMediaCastEditorGateway(
        lambda: _settings(ai_translate_actor_role=True),
        cast(Any, repository),
        lambda: cast(Any, jobs),
        FakeTranslation(),
        client_factory=_client_factory(respond),
    )

    editing = gateway.media_for_editing("movie-1")
    translated = gateway.translate_cast(
        CastTranslationCommand(cast=({"name": "Actor", "role": "Role"},), title="Fixture Movie", year=2026)
    )
    gateway.submit_manual_update(
        ManualCastUpdateCommand("movie-1", ({"tmdbId": 1001, "name": "Actor"},), "Fixture Movie")
    )

    assert editing["current_emby_cast"][0]["emby_person_id"] == "person-1"
    assert editing["search_links"]["google"].endswith("Fixture+Movie+2026")
    assert translated == [{"name": "演员", "role": "角色", "matchStatus": "已翻译"}]
    assert jobs.calls[0][0] == "media.cast-update"
    assert jobs.calls[0][1] == {
        "item_id": "movie-1",
        "cast": [{"tmdbId": 1001, "name": "Actor"}],
        "item_name": "Fixture Movie",
    }


class FakeProgress:
    def __init__(self) -> None:
        self.events: list[tuple[str, Any]] = []

    def checkpoint(self) -> None:
        self.events.append(("checkpoint", None))

    def update(self, progress: int, message: str) -> None:
        self.events.append(("update", (progress, message)))

    def record_started_effect(self, label: str) -> None:
        self.events.append(("started", label))

    def record_committed_effect(self, label: str) -> None:
        self.events.append(("committed", label))

    def record_abandoned_effect(self, label: str) -> None:
        self.events.append(("abandoned", label))


@pytest.mark.parametrize(
    ("post_status", "readback_matches", "expected_event"),
    [(400, False, "abandoned"), (503, False, None), (503, True, "committed")],
)
def test_native_cast_operation_effect_certainty(
    post_status: int,
    readback_matches: bool,
    expected_event: str | None,
) -> None:
    repository = FakeCastRepository()
    progress = FakeProgress()
    reads = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal reads
        if request.method == "POST":
            return httpx.Response(post_status)
        reads += 1
        people = (
            [{"Name": "Actor", "Role": "Role", "Type": "Actor", "ProviderIds": {"Tmdb": "1001"}}]
            if reads > 1 and readback_matches
            else []
        )
        return httpx.Response(
            200,
            json={
                "Id": "movie-1",
                "Name": "Movie",
                "Type": "Movie",
                "ProviderIds": {"Tmdb": "100"},
                "People": people,
            },
        )

    operation = NativeCastUpdateOperation(
        lambda: _settings(),
        cast(Any, repository),
        client_factory=_client_factory(respond),
    )
    parameters = {
        "item_id": "movie-1",
        "item_name": "Movie",
        "cast": [{"tmdbId": 1001, "name": "Actor", "role": "Role"}],
    }

    if expected_event == "committed":
        assert operation.execute(parameters, progress)["ok"] is True
        assert repository.replaced
    else:
        with pytest.raises(RuntimeError, match="拒绝|无法确认"):
            operation.execute(parameters, progress)
        assert repository.replaced == []
    labels = [event for event, _value in progress.events]
    assert "started" in labels
    if expected_event is None:
        assert "committed" not in labels and "abandoned" not in labels
    else:
        assert expected_event in labels


def test_native_cast_operation_records_committed_before_local_projection_failure() -> None:
    repository = FakeCastRepository(fail_replace=True)
    progress = FakeProgress()
    reads = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal reads
        if request.method == "POST":
            return httpx.Response(204)
        reads += 1
        return httpx.Response(
            200,
            json={
                "Id": "movie-1",
                "Name": "Movie",
                "Type": "Movie",
                "ProviderIds": {"Tmdb": "100"},
                "People": (
                    []
                    if reads == 1
                    else [{"Name": "Actor", "Role": "Role", "Type": "Actor", "ProviderIds": {"Tmdb": "1001"}}]
                ),
            },
        )

    operation = NativeCastUpdateOperation(
        lambda: _settings(),
        cast(Any, repository),
        client_factory=_client_factory(respond),
    )
    with pytest.raises(RuntimeError, match="local projection failure"):
        operation.execute(
            {
                "item_id": "movie-1",
                "item_name": "Movie",
                "cast": [{"tmdbId": 1001, "name": "Actor", "role": "Role"}],
            },
            progress,
        )

    labels = [event for event, _value in progress.events]
    assert labels.index("committed") < len(labels)
    assert "abandoned" not in labels


class FakeEventProvider:
    def __init__(self) -> None:
        self.item_calls: list[str] = []
        self.library: Mapping[str, Any] | None = {"ItemId": "library-1"}
        self.items: dict[str, dict[str, Any] | None] = {}
        self.image_result: tuple[bytes, str] | None = (b"image", "image/jpeg")

    def library_for_path(self, _path: str) -> Mapping[str, Any] | None:
        return self.library

    def item(self, item_id: str) -> dict[str, Any] | None:
        self.item_calls.append(item_id)
        return self.items.get(
            item_id,
            {"Id": item_id, "Name": "Fixture", "Type": "Movie", "Path": "/library/movie.mkv"},
        )

    def image(self, _item_id: str, _image_type: str) -> tuple[bytes, str] | None:
        return self.image_result


def test_httpx_p5_event_provider_handles_item_and_library_read_boundaries() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/Library/VirtualFolders":
            return httpx.Response(
                200,
                json=[
                    "invalid-folder",
                    {"ItemId": "library-root", "Locations": ["/library"]},
                    {"ItemId": "library-deep", "Locations": ["/library/deep", ""]},
                ],
            )
        item_id = request.url.path.rsplit("/", 1)[-1]
        if item_id == "missing":
            return httpx.Response(404)
        if item_id == "broken":
            return httpx.Response(503)
        if item_id == "not-object":
            return httpx.Response(200, json=[{"Id": item_id}])
        return httpx.Response(200, json={"Id": item_id, "Name": "Fixture"})

    provider = HttpxP5MediaEventProvider(lambda: _settings(), client_factory=_client_factory(respond))

    assert provider.item("") is None
    assert provider.item("missing") is None
    assert provider.item("broken") is None
    assert provider.item("not-object") is None
    assert provider.item("movie 1") == {"Id": "movie 1", "Name": "Fixture"}
    assert provider.library_for_path("/library/deep/movie.mkv") == {
        "ItemId": "library-deep",
        "Locations": ["/library/deep", ""],
    }
    assert requests[-1].url.params["api_key"] == "fixture-emby-token"


def test_httpx_p5_event_provider_rejects_invalid_or_unmatched_library_payloads() -> None:
    payload: Any = {"not": "a-list"}

    def respond(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    provider = HttpxP5MediaEventProvider(lambda: _settings(), client_factory=_client_factory(respond))
    assert provider.library_for_path("/library/movie.mkv") is None

    payload = [{"ItemId": "other", "Locations": ["/other"]}]
    assert provider.library_for_path("/library/movie.mkv") is None

    def invalid_json(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not-json")

    invalid_provider = HttpxP5MediaEventProvider(
        lambda: _settings(),
        client_factory=_client_factory(invalid_json),
    )
    assert invalid_provider.library_for_path("/library/movie.mkv") is None


def test_httpx_p5_event_provider_bounds_streamed_images(monkeypatch: pytest.MonkeyPatch) -> None:
    mode = "missing"

    def respond(_request: httpx.Request) -> httpx.Response:
        if mode == "missing":
            return httpx.Response(404)
        if mode == "declared-large":
            return httpx.Response(200, headers={"Content-Length": "5"}, content=b"12345")
        if mode == "streamed-large":
            return httpx.Response(200, content=b"12345")
        if mode == "empty":
            return httpx.Response(200, content=b"")
        if mode == "error":
            return httpx.Response(503)
        return httpx.Response(200, headers={"Content-Type": "image/png"}, content=b"1234")

    monkeypatch.setattr(event_adapters_module, "MAX_IMAGE_BYTES", 4)
    provider = HttpxP5MediaEventProvider(lambda: _settings(), client_factory=_client_factory(respond))

    assert provider.image("movie-1", "Backdrop") is None
    mode = "declared-large"
    assert provider.image("movie-1", "Primary") is None
    mode = "streamed-large"
    assert provider.image("movie-1", "Primary") is None
    mode = "empty"
    assert provider.image("movie-1", "Primary") is None
    mode = "error"
    assert provider.image("movie-1", "Primary") is None
    mode = "ok"
    assert provider.image("movie-1", "Primary") == (b"1234", "image/png")

    with provider._default_client() as default_client:
        assert default_client.follow_redirects is True


def _event_gateway(
    repository: FakeMediaRepository,
    provider: FakeEventProvider,
    *,
    store: FakeImageStore | None = None,
    jobs: FakeJobs | None = None,
) -> tuple[NativeP5MediaEventGateway, FakeJobs, FakeImageStore]:
    job_gateway = jobs or FakeJobs()
    image_store = store or FakeImageStore()
    return (
        NativeP5MediaEventGateway(
            lambda: _settings(libraries_to_process=["library-1"]),
            repository,
            lambda: cast(Any, job_gateway),
            cast(Any, provider),
            cast(Any, image_store),
        ),
        job_gateway,
        image_store,
    )


def test_native_p5_event_rejects_wrong_owner_and_ignores_invalid_targets(tmp_path: Path) -> None:
    repository = FakeMediaRepository()
    provider = FakeEventProvider()
    gateway, _jobs, _store = _event_gateway(repository, provider)

    with pytest.raises(ValueError, match="not owned by P5"):
        gateway.handle(
            {"Event": "playback.start", "Item": {"Id": "movie-1", "Type": "Movie"}},
            host_url="https://etk.fixture.invalid",
            remote_addr="127.0.0.1",
        )

    invalid = gateway.handle(
        {"Event": "metadata.update", "Item": {"Type": "Movie"}},
        host_url="https://etk.fixture.invalid",
        remote_addr="127.0.0.1",
    )
    assert invalid.payload["status"] == "event_ignored_no_id_or_wrong_type"

    provider.library = {"ItemId": "library-2"}
    ignored = gateway.handle(
        {
            "Event": "metadata.update",
            "Item": {"Id": "movie-1", "Type": "Movie", "Path": "/library/movie.mkv"},
        },
        host_url="https://etk.fixture.invalid",
        remote_addr="127.0.0.1",
    )
    assert ignored.payload["status"] == "ignored_library"

    provider.library = {"ItemId": "library-1"}
    foreign_strm = tmp_path / "foreign.strm"
    foreign_strm.write_text("https://foreign.fixture.invalid/video", encoding="utf-8")
    ignored_strm = gateway.handle(
        {
            "Event": "metadata.update",
            "Item": {"Id": "movie-1", "Type": "Movie", "Path": str(foreign_strm)},
        },
        host_url="https://etk.fixture.invalid",
        remote_addr="127.0.0.1",
    )
    assert ignored_strm.payload["status"] == "ignored_non_etk_strm"


def test_native_p5_episode_metadata_targets_series_and_requires_series_identity() -> None:
    repository = FakeMediaRepository()
    provider = FakeEventProvider()
    provider.items["episode-1"] = {"Id": "episode-1", "Type": "Episode"}
    gateway, jobs, _store = _event_gateway(repository, provider)
    payload = {
        "Event": "metadata.update",
        "Item": {"Id": "episode-1", "Name": "Episode", "Type": "Episode", "Path": "/library/e01.mkv"},
    }

    missing_series = gateway.handle(
        payload,
        host_url="https://etk.fixture.invalid",
        remote_addr="127.0.0.1",
    )
    assert missing_series.payload["status"] == "event_ignored_episode_no_series_id"

    provider.items["episode-1"] = {"Id": "episode-1", "Type": "Episode", "SeriesId": "series-1"}
    provider.items["series-1"] = {"Id": "series-1", "Type": "Series", "Name": "Fixture Series"}
    submitted = gateway.handle(
        payload,
        host_url="https://etk.fixture.invalid",
        remote_addr="127.0.0.1",
    )
    assert submitted.status_code == 202
    assert jobs.calls[-1][1] == {"item_id": "series-1", "item_name": "Fixture Series"}


def test_native_p5_image_event_observes_missing_inputs_and_tmdb_sources() -> None:
    repository = FakeMediaRepository()
    provider = FakeEventProvider()
    gateway, _jobs, store = _event_gateway(repository, provider)
    base_item = {"Id": "movie-1", "Name": "Movie", "Type": "Movie", "Path": "/library/movie.mkv"}

    no_type = gateway.handle(
        {"Event": "image.update", "Item": base_item, "Image": {}},
        host_url="https://etk.fixture.invalid",
        remote_addr="127.0.0.1",
    )
    assert no_type.payload["status"] == "image_update_observed"

    repository.identity = cast(Any, None)
    no_identity = gateway.handle(
        {"Event": "image.update", "Item": base_item, "Image": {"Type": "Primary"}},
        host_url="https://etk.fixture.invalid",
        remote_addr="127.0.0.1",
    )
    assert no_identity.payload["status"] == "image_update_identity_not_found"

    repository.identity = MediaIdentity("100", "movie", MediaItemType.MOVIE)
    cached = gateway.handle(
        {
            "Event": "image.update",
            "Item": base_item,
            "Image": {"Type": "Primary/0", "Url": "https://image.tmdb.org/t/p/w500/poster.jpg"},
        },
        host_url="https://etk.fixture.invalid/",
        remote_addr="127.0.0.1",
    )
    assert cached.payload == {"status": "image_update_cached", "item_id": "movie-1", "updated": 1}
    assert repository.image_updates[-1][2] == "/poster.jpg"
    assert repository.replacements[-1]["rules"] == [{"type": "Primary", "limit": 1, "min_width": 0}]
    assert repository.replacements[-1]["images"][0]["source_url"].endswith("/original/poster.jpg")
    assert store.by_hash == {}


def test_native_p5_image_event_archives_manual_image_and_replaces_existing_policy() -> None:
    repository = FakeMediaRepository()
    repository.identity = MediaIdentity("200", "tv", MediaItemType.EPISODE, 2, 3)
    key = ImagePolicyKey.create(MediaItemType.EPISODE, "200", season_number=2, episode_number=3)
    repository.policy = ImagePolicyRecord(
        key,
        ({"type": "Primary", "limit": 1, "min_width": 100},),
        (
            {"type": "Primary", "index": 0, "source_url": "fixture://old"},
            {"type": "Backdrop", "index": 0, "source_url": "fixture://keep"},
        ),
    )
    provider = FakeEventProvider()
    gateway, _jobs, store = _event_gateway(repository, provider)
    item = {
        "Id": "episode-1",
        "Name": "Episode",
        "Type": "Episode",
        "Path": "/library/e03.mkv",
        "ParentIndexNumber": "2",
        "IndexNumber": "3",
    }

    result = gateway.handle(
        {"Event": "image.update", "EventId": "event-1", "Item": item, "Image": {"Type": "Primary"}},
        host_url="https://etk.fixture.invalid/",
        remote_addr="127.0.0.1",
    )

    assert result.payload["status"] == "image_update_cached"
    assert repository.resolve_calls[-1][1] == {"season_number": 2, "episode_number": 3}
    cached_hash = next(iter(store.by_hash))
    assert repository.image_updates[-1][2] == f"etk-cache://{cached_hash}"
    replacement = repository.replacements[-1]
    assert replacement["rules"] == [{"type": "Primary", "limit": 1, "min_width": 100}]
    assert [image["type"] for image in replacement["images"]] == ["Backdrop", "Primary"]
    assert replacement["images"][-1]["url"] == f"https://etk.fixture.invalid/api/image_proxy?cache={cached_hash}"


def test_native_p5_image_event_handles_provider_and_archive_failures() -> None:
    repository = FakeMediaRepository()
    provider = FakeEventProvider()
    provider.image_result = None
    gateway, _jobs, _store = _event_gateway(repository, provider)
    payload = {
        "Event": "image.update",
        "Item": {"Id": "movie-1", "Type": "Movie", "Path": "/library/movie.mkv"},
        "Image": {"Type": "Primary"},
    }
    unavailable = gateway.handle(
        payload,
        host_url="https://etk.fixture.invalid",
        remote_addr="127.0.0.1",
    )
    assert unavailable.payload["status"] == "image_update_observed"

    provider.image_result = (b"image", "image/jpeg")
    failing_store = FakeImageStore()
    failing_store.fail_store = True
    failing_gateway, _jobs, _store = _event_gateway(repository, provider, store=failing_store)
    failed = failing_gateway.handle(
        payload,
        host_url="https://etk.fixture.invalid",
        remote_addr="127.0.0.1",
    )
    assert failed.payload["status"] == "image_update_archive_failed"


def test_metadata_event_operation_validates_input_and_authoritative_readback() -> None:
    repository = FakeMediaRepository()
    provider = FakeEventProvider()
    progress = FakeProgress()
    operation = NativeMetadataEventOperation(cast(Any, provider), repository)

    with pytest.raises(ValueError, match="requires item_id"):
        operation.execute({}, progress)

    provider.items["missing"] = None
    with pytest.raises(RuntimeError, match="unavailable"):
        operation.execute({"item_id": "missing"}, progress)

    repository.sync_result = False
    with pytest.raises(RuntimeError, match="unavailable or ambiguous"):
        operation.execute({"item_id": "movie-1"}, progress)


def test_media_event_helpers_validate_authorization_paths_and_numbers(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="authorization is unavailable"):
        event_adapters_module._emby_identity(_settings(emby_server_url=""))

    missing = tmp_path / "missing.strm"
    empty = tmp_path / "empty.strm"
    empty.write_text("", encoding="utf-8")
    foreign = tmp_path / "foreign.strm"
    foreign.write_text("https://foreign.fixture.invalid/video", encoding="utf-8")
    native = tmp_path / "native.strm"
    native.write_text("http://127.0.0.1/api/p115/virtual-play/pick-code", encoding="utf-8")
    assert event_adapters_module._is_etk_strm(str(missing)) is False
    assert event_adapters_module._is_etk_strm(str(empty)) is False
    assert event_adapters_module._is_etk_strm(str(foreign)) is False
    assert event_adapters_module._is_etk_strm(str(native)) is True

    assert event_adapters_module._tmdb_image_path("") == ""
    assert event_adapters_module._tmdb_image_path("https://images.fixture.invalid/poster.jpg") == ""
    proxied = "https://etk.fixture.invalid/api/image_proxy?url=https%3A%2F%2Fimage.tmdb.org%2Ft%2Fp%2Fw780%2Fback.jpg"
    assert event_adapters_module._tmdb_image_path(proxied) == "/back.jpg"
    assert event_adapters_module._optional_int("2") == 2
    assert event_adapters_module._optional_int("") is None
    assert event_adapters_module._optional_int("invalid") is None


def test_native_p5_event_submits_metadata_operation_and_operation_syncs_repository() -> None:
    repository = FakeMediaRepository()
    jobs = FakeJobs()
    provider = FakeEventProvider()
    gateway = NativeP5MediaEventGateway(
        lambda: _settings(libraries_to_process=["library-1"]),
        repository,
        lambda: cast(Any, jobs),
        cast(Any, provider),
        cast(Any, FakeImageStore()),
    )

    result = gateway.handle(
        {
            "Event": "metadata.update",
            "Item": {"Id": "movie-1", "Name": "Movie", "Type": "Movie", "Path": "/library/movie.mkv"},
        },
        host_url="https://etk.fixture.invalid",
        remote_addr="127.0.0.1",
    )
    operation = NativeMetadataEventOperation(cast(Any, provider), repository)
    progress = FakeProgress()
    operation_result = operation.execute({"item_id": "movie-1"}, progress)

    assert result.status_code == 202
    assert result.payload["job_id"] == "00000000-0000-0000-0000-000000000555"
    assert jobs.calls[0][0] == "media.metadata-event-sync"
    assert operation_result == {"ok": True, "item_id": "movie-1"}
    assert repository.metadata_items[0][0] == "movie-1"


def test_media_effect_deliveries_map_rejected_and_indeterminate_results() -> None:
    manual = ManualImageEffectDelivery(
        cast(
            Any,
            SimpleNamespace(
                update_image=lambda _command: SimpleNamespace(
                    success=False,
                    effect_status="indeterminate",
                    message="unknown",
                )
            ),
        )
    )
    event = P5MediaEventEffectDelivery(
        cast(
            Any,
            SimpleNamespace(
                handle=lambda *_args, **_kwargs: ServiceResult({"error": "bad event"}, 400),
            ),
        )
    )
    effect = cast(
        MediaEffectProjection,
        SimpleNamespace(
            payload={
                "item_id": "movie-1",
                "image_type": "poster",
                "image_bytes_base64": None,
                "event_payload": {"Event": "image.update"},
            }
        ),
    )

    with pytest.raises(MediaEffectDeliveryIndeterminate):
        manual.deliver(effect)
    with pytest.raises(MediaEffectDeliveryRejected):
        event.deliver(effect)


def test_native_media_image_gateway_sorts_tmdb_candidates_and_archives_bounded_source() -> None:
    repository = FakeMediaRepository()
    store = FakeImageStore()
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.host == "images.fixture.invalid":
            return httpx.Response(200, headers={"Content-Type": "image/jpeg"}, content=b"remote-image")
        assert request.url.path == "/3/movie/100/images"
        return httpx.Response(
            200,
            json={
                "posters": [
                    {"file_path": "/en.jpg", "iso_639_1": "en", "vote_average": 9, "vote_count": 10},
                    {"file_path": "/zh.jpg", "iso_639_1": "zh-CN", "vote_average": 8, "vote_count": 5},
                ],
                "backdrops": [],
                "logos": [],
            },
        )

    gateway = HttpxMediaImageGateway(
        lambda: _settings(media_image_archive_enabled=True),
        repository,
        cast(Any, store),
        cast(Any, FakeEventProvider()),
        lambda _tmdb_id: None,
        client_factory=_client_factory(respond),
    )
    candidates = gateway.live_candidates(MediaItemType.MOVIE, repository.identity, include_all_languages=False)
    archived = gateway.archive_policy_images(
        [{"type": "Primary", "source_url": "https://images.fixture.invalid/poster.jpg"}],
        "https://etk.fixture.invalid",
    )

    assert candidates[0]["source_url"].endswith("/zh.jpg")
    assert archived[0]["url"].startswith("https://etk.fixture.invalid/api/image_proxy?cache=")
    assert requests[-1].headers["accept"] == "image/*"


def _image_gateway(
    repository: FakeMediaRepository,
    store: FakeImageStore,
    handler: Any,
    *,
    emby_images: Any | None = None,
    cleanup: list[str] | None = None,
    settings: Settings | None = None,
) -> HttpxMediaImageGateway:
    cleanup_calls = cleanup if cleanup is not None else []
    return HttpxMediaImageGateway(
        lambda: settings or _settings(media_image_archive_enabled=True),
        repository,
        cast(Any, store),
        cast(Any, emby_images or FakeEventProvider()),
        lambda tmdb_id: cleanup_calls.append(tmdb_id),
        client_factory=_client_factory(handler),
    )


def test_media_image_archive_metadata_maps_tokens_and_migrates_emby_screenshot() -> None:
    repository = FakeMediaRepository()
    store = FakeImageStore()
    emby = FakeEventProvider()

    def unused(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("archive_metadata must not call remote HTTP")

    gateway = _image_gateway(repository, store, unused, emby_images=emby)
    token_hash = "a" * 64
    result = gateway.archive_metadata(
        {
            "item_type": "Episode",
            "series_tmdb_id": "200",
            "season_number": "2",
            "episode_number": 3,
            "_emby_item_ids": ["", "episode-1"],
            "images": {"primary": "", "backdrop": f"etk-cache://{token_hash}", "logo": None},
        },
        "https://etk.fixture.invalid/",
    )

    assert "_emby_item_ids" not in result
    assert result["images"]["backdrop"] == f"https://etk.fixture.invalid/api/image_proxy?cache={token_hash}"
    screenshot_hash = repository.recorded_screenshots[-1][-1]
    assert result["images"]["primary"].endswith(f"cache={screenshot_hash}")
    assert result["images"]["thumb"] == result["images"]["primary"]

    assert gateway.archive_metadata({"images": "invalid", "value": 1}, "https://etk.fixture.invalid") == {
        "images": "invalid",
        "value": 1,
    }


def test_media_image_archive_metadata_discards_obsolete_episode_screenshot() -> None:
    repository = FakeMediaRepository()
    store = FakeImageStore()

    def unused(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("archive_metadata must not call remote HTTP")

    gateway = _image_gateway(repository, store, unused)
    result = gateway.archive_metadata(
        {
            "item_type": "Episode",
            "series_tmdb_id": "200",
            "season_number": 2,
            "episode_number": 3,
            "_screenshot_hash": "b" * 64,
            "images": {"primary": "/official-still.jpg"},
        },
        "https://etk.fixture.invalid",
    )

    assert result["images"]["primary"] == "/official-still.jpg"
    assert repository.cleared_screenshots == [("200", 2, 3)]
    assert store.discarded == [("episode-screenshot://200/S02/E03",)]

    repository.clear_screenshot_result = False
    gateway.archive_metadata(
        {
            "item_type": "Episode",
            "series_tmdb_id": "200",
            "season_number": 2,
            "episode_number": 3,
            "_screenshot_hash": "c" * 64,
            "images": {"primary": "/replacement.jpg"},
        },
        "https://etk.fixture.invalid",
    )
    assert len(store.discarded) == 1


def test_media_image_archive_existing_episode_image_handles_incomplete_and_failed_candidates() -> None:
    repository = FakeMediaRepository()
    repository.record_screenshot_result = False
    store = FakeImageStore()
    calls = 0

    class EmbyImages:
        def image(self, _item_id: str, _image_type: str) -> tuple[bytes, str] | None:
            nonlocal calls
            calls += 1
            return None if calls == 1 else (b"fixture", "image/jpeg")

    def unused(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("archive_metadata must not call remote HTTP")

    gateway = _image_gateway(repository, store, unused, emby_images=EmbyImages())
    incomplete = gateway.archive_metadata(
        {"item_type": "Episode", "_emby_item_ids": ["episode-1"], "images": {}},
        "https://etk.fixture.invalid",
    )
    assert incomplete["images"] == {}

    not_recorded = gateway.archive_metadata(
        {
            "item_type": "Episode",
            "series_tmdb_id": "200",
            "season_number": 2,
            "episode_number": 3,
            "_emby_item_ids": ["episode-1", "episode-2"],
            "images": {},
        },
        "https://etk.fixture.invalid",
    )
    assert not_recorded["images"] == {}
    assert store.discarded[-1] == ("episode-screenshot://200/S02/E03",)


def test_media_image_live_candidates_cover_all_hierarchies_and_language_modes() -> None:
    repository = FakeMediaRepository()
    store = FakeImageStore()
    seen: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        if request.url.path == "/3/collection/400":
            return httpx.Response(200, json={"parts": ["invalid", {"original_language": "ja"}]})
        if request.url.path.endswith("/images"):
            return httpx.Response(
                200,
                json={
                    "posters": ["invalid", {"file_path": ""}, {"file_path": "/poster.jpg", "iso_639_1": "ja"}],
                    "backdrops": [{"file_path": "/backdrop.jpg", "iso_639_1": None}],
                    "logos": [{"file_path": "/logo.png", "iso_639_1": "en"}],
                    "stills": [{"file_path": "/still.jpg", "iso_639_1": "zh"}],
                },
            )
        raise AssertionError(request.url)

    gateway = _image_gateway(repository, store, respond)
    cases = [
        (MediaItemType.MOVIE, MediaIdentity("100", "movie", MediaItemType.MOVIE)),
        (MediaItemType.SERIES, MediaIdentity("200", "tv", MediaItemType.SERIES)),
        (MediaItemType.SEASON, MediaIdentity("200", "tv", MediaItemType.SEASON, 2)),
        (MediaItemType.EPISODE, MediaIdentity("200", "tv", MediaItemType.EPISODE, 2, 3)),
        (MediaItemType.BOXSET, MediaIdentity("400", "movie", MediaItemType.BOXSET)),
    ]

    results = {
        item_type: gateway.live_candidates(item_type, identity, include_all_languages=True)
        for item_type, identity in cases
    }

    assert {item["type"] for item in results[MediaItemType.MOVIE]} == {
        "Primary",
        "Disc",
        "Backdrop",
        "Art",
        "Banner",
        "Thumb",
        "Logo",
    }
    assert {item["type"] for item in results[MediaItemType.SEASON]} == {"Primary", "Disc"}
    assert {item["type"] for item in results[MediaItemType.EPISODE]} == {
        "Primary",
        "Art",
        "Backdrop",
        "Banner",
        "Thumb",
        "Disc",
    }
    assert "Logo" not in {item["type"] for item in results[MediaItemType.BOXSET]}
    assert "/3/collection/400" in seen


def test_media_image_live_candidates_classifies_not_found_and_missing_configuration() -> None:
    repository = FakeMediaRepository()
    store = FakeImageStore()
    response_status = 404

    def respond(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(response_status)

    missing_key = _image_gateway(
        repository,
        store,
        respond,
        settings=_settings(tmdb_api_key=SecretStr("")),
    )
    with pytest.raises(image_adapters_module.ImagePolicyUnavailable):
        missing_key.live_candidates(MediaItemType.MOVIE, repository.identity, include_all_languages=False)

    gateway = _image_gateway(repository, store, respond)
    episode = MediaIdentity("200", "tv", MediaItemType.EPISODE, 2, 3)
    assert gateway.live_candidates(MediaItemType.EPISODE, episode, include_all_languages=False) == []
    with pytest.raises(image_adapters_module.ImageCollectionNotFound):
        gateway.live_candidates(
            MediaItemType.BOXSET,
            MediaIdentity("400", "movie", MediaItemType.BOXSET),
            include_all_languages=False,
        )
    with pytest.raises(image_adapters_module.ImagePolicyError):
        gateway.live_candidates(MediaItemType.MOVIE, repository.identity, include_all_languages=False)


def test_media_image_refreshes_boxset_movie_series_season_and_episode() -> None:
    repository = FakeMediaRepository()
    store = FakeImageStore()

    def respond(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/3/collection/400":
            return httpx.Response(
                200,
                json={
                    "parts": [{"original_language": "ja"}],
                    "poster_path": "/collection-default.jpg",
                    "backdrop_path": "/collection-backdrop.jpg",
                },
            )
        if path == "/3/collection/400/images":
            return httpx.Response(200, json={"posters": [], "backdrops": []})
        if path in {"/3/movie/100", "/3/tv/200"}:
            return httpx.Response(
                200,
                json={
                    "poster_path": "/default.jpg",
                    "backdrop_path": "/default-back.jpg",
                    "images": {
                        "posters": [{"file_path": "/selected.jpg", "iso_639_1": "zh"}],
                        "backdrops": [{"file_path": "/selected-back.jpg", "iso_639_1": "zh"}],
                        "logos": [{"file_path": "/logo.png", "iso_639_1": "zh"}],
                    },
                    "seasons": ["invalid", {"season_number": ""}, {"season_number": 1}, {"season_number": 2}],
                },
            )
        if path == "/3/tv/200/season/1":
            return httpx.Response(404)
        if path == "/3/tv/200/season/2":
            return httpx.Response(
                200,
                json={"poster_path": "/season-default.jpg", "images": {"posters": [{"file_path": "/s2.jpg"}]}},
            )
        if path == "/3/tv/200/season/2/episode/3":
            return httpx.Response(
                200,
                json={"still_path": "/episode-default.jpg", "images": {"stills": [{"file_path": "/e3.jpg"}]}},
            )
        raise AssertionError(path)

    gateway = _image_gateway(repository, store, respond)
    assert (
        gateway.refresh_images(
            MediaItemType.BOXSET,
            MediaIdentity("400", "movie", MediaItemType.BOXSET),
        )
        == 1
    )
    assert gateway.refresh_images(MediaItemType.MOVIE, repository.identity) == 1
    assert (
        gateway.refresh_images(
            MediaItemType.SERIES,
            MediaIdentity("200", "tv", MediaItemType.SERIES),
        )
        == 1
    )
    assert (
        gateway.refresh_images(
            MediaItemType.SEASON,
            MediaIdentity("200", "tv", MediaItemType.SEASON, 2),
        )
        == 1
    )
    assert (
        gateway.refresh_images(
            MediaItemType.EPISODE,
            MediaIdentity("200", "tv", MediaItemType.EPISODE, 2, 3),
        )
        == 1
    )

    boxset, movie, series, season, episode = repository.replacements[-5:]
    assert boxset["root_images"] == {
        "poster": "/collection-default.jpg",
        "backdrop": "/collection-backdrop.jpg",
    }
    assert movie["root_images"]["logo"] == "/logo.png"
    assert series["season_posters"] == {2: "/s2.jpg"}
    assert season["season_posters"] == {2: "/s2.jpg"}
    assert episode["episode_still"] == "/e3.jpg"


def test_media_image_refresh_validates_hierarchy_and_provider_failures() -> None:
    repository = FakeMediaRepository()
    store = FakeImageStore()
    status_by_path: dict[str, int] = {}

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_by_path.get(request.url.path, 404))

    gateway = _image_gateway(repository, store, respond)
    with pytest.raises(image_adapters_module.ImagePolicyError, match="collection"):
        gateway.refresh_images(MediaItemType.BOXSET, MediaIdentity("400", "movie", MediaItemType.BOXSET))
    with pytest.raises(image_adapters_module.ImagePolicyError, match="media"):
        gateway.refresh_images(MediaItemType.MOVIE, repository.identity)
    with pytest.raises(ValueError, match="season number"):
        gateway.refresh_images(MediaItemType.SEASON, MediaIdentity("200", "tv", MediaItemType.SEASON))
    with pytest.raises(image_adapters_module.ImagePolicyError, match="season"):
        gateway.refresh_images(MediaItemType.SEASON, MediaIdentity("200", "tv", MediaItemType.SEASON, 2))
    with pytest.raises(ValueError, match="episode number"):
        gateway.refresh_images(MediaItemType.EPISODE, MediaIdentity("200", "tv", MediaItemType.EPISODE, 2))
    with pytest.raises(image_adapters_module.ImagePolicyError, match="episode"):
        gateway.refresh_images(MediaItemType.EPISODE, MediaIdentity("200", "tv", MediaItemType.EPISODE, 2, 3))


def test_media_image_policy_archive_handles_cache_tokens_disabled_mode_and_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = FakeMediaRepository()
    store = FakeImageStore()
    existing = store.store("https://images.fixture.invalid/existing.jpg", b"existing", "image/jpeg")
    valid_hash = str(existing["content_hash"])
    mode = "ok"

    def respond(_request: httpx.Request) -> httpx.Response:
        if mode == "too-large":
            return httpx.Response(200, headers={"Content-Length": "5", "Content-Type": "image/jpeg"}, content=b"12345")
        if mode == "not-image":
            return httpx.Response(200, headers={"Content-Type": "text/plain"}, content=b"text")
        if mode == "empty":
            return httpx.Response(200, headers={"Content-Type": "image/jpeg"}, content=b"")
        return httpx.Response(200, headers={"Content-Type": "application/octet-stream"}, content=b"png")

    monkeypatch.setattr(image_adapters_module, "MAX_IMAGE_BYTES", 4)
    gateway = _image_gateway(repository, store, respond)
    archived = gateway.archive_policy_images(
        [
            {"source_url": f"etk-cache://{valid_hash}"},
            {"source_url": "https://images.fixture.invalid/existing.jpg"},
            {"source_url": "https://images.fixture.invalid/new.png"},
            {"source_url": "fixture://not-http", "content_hash": "invalid"},
            {"source_url": ""},
        ],
        "https://etk.fixture.invalid/",
    )
    assert archived[0]["url"].endswith(f"cache={valid_hash}")
    assert archived[1]["url"].endswith(f"cache={valid_hash}")
    assert archived[2]["url"].startswith("https://etk.fixture.invalid/api/image_proxy?cache=")
    assert archived[3]["url"] == "fixture://not-http"
    assert "content_hash" not in archived[3]

    for failure_mode in ("too-large", "not-image", "empty"):
        mode = failure_mode
        value = gateway.archive_policy_images(
            [{"source_url": f"https://images.fixture.invalid/{failure_mode}"}],
            "https://etk.fixture.invalid",
        )[0]
        assert value["url"].endswith(f"/{failure_mode}")
        assert "content_hash" not in value

    disabled = _image_gateway(
        repository,
        store,
        respond,
        settings=_settings(media_image_archive_enabled=False),
    )
    value = disabled.archive_policy_images(
        [{"source_url": "https://images.fixture.invalid/plain.jpg", "thumbnail_source_url": "fixture://thumb"}],
        "https://etk.fixture.invalid",
    )[0]
    assert value["url"] == "https://images.fixture.invalid/plain.jpg"
    assert value["thumbnail_url"] == "fixture://thumb"


def test_media_image_gateway_helpers_and_cleanup_contract() -> None:
    repository = FakeMediaRepository()
    store = FakeImageStore()
    cleanup: list[str] = []

    def unused(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("helper contract must not call remote HTTP")

    gateway = _image_gateway(repository, store, unused, cleanup=cleanup)
    digest = "d" * 64
    assert gateway.screenshot_token(digest) == f"etk-cache://{digest}"
    gateway.discard_unreferenced_sources(("fixture://one",))
    gateway.schedule_missing_collection_cleanup(" 400 ")
    assert store.discarded[-1] == ("fixture://one",)
    assert cleanup == ["400"]

    assert gateway._images_endpoint(MediaItemType.MOVIE, repository.identity) == "/movie/100/images"
    assert (
        gateway._images_endpoint(
            MediaItemType.SERIES,
            MediaIdentity("200", "tv", MediaItemType.SERIES),
        )
        == "/tv/200/images"
    )
    with pytest.raises(ValueError, match="hierarchy is incomplete"):
        gateway._images_endpoint(MediaItemType.EPISODE, MediaIdentity("200", "tv", MediaItemType.EPISODE, 2))

    assert image_adapters_module._cache_hash("fixture://invalid") == ""
    assert image_adapters_module._cache_hash("etk-cache://not-a-hash") == ""
    assert image_adapters_module._mapping_sequence("invalid") == ()
    assert image_adapters_module._mapping_sequence([{"ok": True}, "invalid"]) == ({"ok": True},)
    assert image_adapters_module._mapping("invalid") == {}
    assert image_adapters_module._optional_int("invalid") is None
    assert image_adapters_module._optional_text(" ") is None
    assert StaticMediaMappingDefaults().keyword_mapping()
    assert StaticMediaMappingDefaults().studio_mapping()

    with gateway._default_client() as client:
        assert client.follow_redirects is True


def _image_editing_gateway(
    repository: FakeMediaRepository,
    store: FakeImageStore,
    handler: Any,
    *,
    settings: Settings | None = None,
) -> HttpxMediaImageEditingGateway:
    return HttpxMediaImageEditingGateway(
        lambda: settings or _settings(),
        repository,
        cast(Any, store),
        client_factory=_client_factory(handler),
    )


def test_media_image_editing_tmdb_catalog_validates_item_identity_and_payload() -> None:
    repository = FakeMediaRepository()
    store = FakeImageStore()
    item_payload: Any = None
    tmdb_payload: Any = {"images": {"posters": [], "backdrops": [], "logos": []}}
    tmdb_status = 200

    def respond(request: httpx.Request) -> httpx.Response:
        if "/Users/" in request.url.path:
            if item_payload is None:
                return httpx.Response(404)
            return httpx.Response(200, json=item_payload)
        return httpx.Response(tmdb_status, json=tmdb_payload)

    gateway = _image_editing_gateway(repository, store, respond)
    assert gateway.processor_ready() is True
    with pytest.raises(image_editing_adapters_module.EmbyMediaDetailsNotFoundError):
        gateway.tmdb_images("movie-1")

    item_payload = {"Id": "movie-1", "Type": "Movie", "ProviderIds": {}}
    with pytest.raises(image_editing_adapters_module.TmdbIdentityMissingError):
        gateway.tmdb_images("movie-1")

    item_payload = {"Id": "episode-1", "Type": "Episode", "ProviderIds": {"Tmdb": "100"}}
    with pytest.raises(image_editing_adapters_module.UnsupportedImageMediaTypeError):
        gateway.tmdb_images("episode-1")

    item_payload = {"Id": "movie-1", "Type": "Movie", "ProviderIds": {"Tmdb": "100"}}
    missing_key = _image_editing_gateway(
        repository,
        store,
        respond,
        settings=_settings(tmdb_api_key=SecretStr("")),
    )
    with pytest.raises(RuntimeError, match="TMDb API Key"):
        missing_key.tmdb_images("movie-1")

    tmdb_status = 503
    with pytest.raises(image_editing_adapters_module.TmdbImagesNotFoundError):
        gateway.tmdb_images("movie-1")

    tmdb_status = 200
    tmdb_payload = []
    with pytest.raises(image_editing_adapters_module.TmdbImagesNotFoundError):
        gateway.tmdb_images("movie-1")

    tmdb_payload = {"images": "invalid"}
    with pytest.raises(image_editing_adapters_module.TmdbImagesNotFoundError):
        gateway.tmdb_images("movie-1")

    item_payload = {"Id": "series-1", "Type": "Series", "ProviderIds": {"Tmdb": "200"}}
    tmdb_payload = {
        "images": {
            "posters": [
                {"file_path": ""},
                {"file_path": "/poster.jpg", "aspect_ratio": 0.66, "width": "1000", "height": 1500},
            ],
            "backdrops": [{"file_path": "/back.jpg"}],
            "logos": [{"file_path": "/logo.png"}],
        }
    }
    catalog = gateway.tmdb_images("series-1")
    assert catalog.posters[0].preview.endswith("/poster.jpg")
    assert catalog.posters[0].width == 1000
    assert catalog.backdrops[0].original.endswith("/back.jpg")
    assert catalog.logos[0].original.endswith("/logo.png")


def test_media_image_editing_rejects_invalid_target_and_image_inputs() -> None:
    repository = FakeMediaRepository()
    store = FakeImageStore()
    item_payload: Any = {"Id": "movie-1", "Type": "Movie", "Path": "/movie.mkv"}

    def respond(request: httpx.Request) -> httpx.Response:
        if "/Users/" in request.url.path:
            return httpx.Response(200, json=item_payload)
        raise AssertionError("invalid target must not reach provider write")

    gateway = _image_editing_gateway(repository, store, respond)
    unsupported = gateway.update_image(ImageUpdateCommand("movie-1", "disc", "", b"image", "image/jpeg"))
    assert unsupported.success is False and "不支持" in unsupported.message

    item_payload = []
    missing = gateway.update_image(ImageUpdateCommand("movie-1", "poster", "", b"image", "image/jpeg"))
    assert missing.success is False and "Emby 详情" in missing.message

    item_payload = {"Id": "boxset-1", "Type": "Boxset", "Path": "/boxset"}
    wrong_type = gateway.update_image(ImageUpdateCommand("boxset-1", "poster", "", b"image", "image/jpeg"))
    assert wrong_type.success is False and "不支持为" in wrong_type.message

    item_payload = {"Id": "movie-1", "Type": "Movie", "Path": ""}
    no_path = gateway.update_image(ImageUpdateCommand("movie-1", "poster", "", b"image", "image/jpeg"))
    assert no_path.success is False and "无法定位" in no_path.message

    item_payload = {"Id": "movie-1", "Type": "Movie", "Path": "/movie.mkv"}
    repository.identity = cast(Any, None)
    no_identity = gateway.update_image(ImageUpdateCommand("movie-1", "poster", "", b"image", "image/jpeg"))
    assert no_identity.success is False and "ETK 元数据" in no_identity.message

    repository.identity = MediaIdentity("100", "movie", MediaItemType.MOVIE)
    invalid_url = gateway.update_image(ImageUpdateCommand("movie-1", "poster", "fixture://invalid", b"", "image/jpeg"))
    assert invalid_url.success is False and "URL 无效" in invalid_url.message

    invalid_bytes = gateway.update_image(ImageUpdateCommand("movie-1", "poster", "", b"plain", "text/plain"))
    assert invalid_bytes.success is False and "可识别的图片" in invalid_bytes.message


def test_media_image_editing_downloads_bounded_remote_image_and_updates_episode_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = FakeMediaRepository()
    repository.identity = MediaIdentity("200", "tv", MediaItemType.EPISODE, 2, 3)
    key = ImagePolicyKey.create(MediaItemType.EPISODE, "200", season_number=2, episode_number=3)
    repository.policy = ImagePolicyRecord(
        key,
        ({"type": "Backdrop", "limit": 1, "min_width": 0},),
        (
            {"type": "Backdrop", "index": 0, "source_url": "fixture://old"},
            {"type": "Primary", "index": 0, "source_url": "fixture://keep"},
        ),
    )
    store = FakeImageStore()
    writes = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal writes
        if "/Users/" in request.url.path:
            return httpx.Response(
                200,
                json={
                    "Id": "episode-1",
                    "Type": "Episode",
                    "Path": "/series/S02/E03.mkv",
                    "ParentIndexNumber": "2",
                    "IndexNumber": "3",
                },
            )
        if request.url.host == "images.fixture.invalid":
            return httpx.Response(
                200,
                headers={"Content-Type": "application/octet-stream"},
                content=b"\x89PNG\r\n\x1a\n",
            )
        if request.method == "POST":
            writes += 1
            assert request.headers["content-type"] == "image/png"
            return httpx.Response(204)
        return httpx.Response(200, content=b"\x89PNG\r\n\x1a\n")

    monkeypatch.setattr(image_editing_adapters_module, "MAX_IMAGE_BYTES", 64)
    gateway = _image_editing_gateway(repository, store, respond)
    outcome = gateway.update_image(
        ImageUpdateCommand(
            "episode-1",
            "fanart",
            "https://images.fixture.invalid/backdrop",
            b"",
            "",
        )
    )

    assert outcome.success is True
    assert writes == 1
    assert repository.resolve_calls[-1][1] == {"season_number": 2, "episode_number": 3}
    replacement = repository.replacements[-1]
    assert [image["type"] for image in replacement["images"]] == ["Primary", "Backdrop"]
    assert replacement["rules"][-1]["type"] == "Backdrop"


def test_media_image_editing_classifies_transport_and_readback_uncertainty() -> None:
    repository = FakeMediaRepository()
    store = FakeImageStore()
    post_raises = True
    read_raises = False

    def respond(request: httpx.Request) -> httpx.Response:
        if "/Users/" in request.url.path:
            return httpx.Response(200, json={"Id": "movie-1", "Type": "Movie", "Path": "/movie.mkv"})
        if request.method == "POST" and post_raises:
            raise httpx.ConnectError("fixture transport failure", request=request)
        if request.method == "GET" and read_raises:
            raise httpx.ReadError("fixture readback failure", request=request)
        return httpx.Response(400 if request.method == "POST" else 404)

    gateway = _image_editing_gateway(repository, store, respond)
    uncertain = gateway.update_image(ImageUpdateCommand("movie-1", "poster", "", b"\xff\xd8\xffimage", "image/jpeg"))
    assert uncertain.effect_status == "indeterminate"

    post_raises = False
    read_raises = True
    rejected_but_unreadable = gateway.update_image(
        ImageUpdateCommand("movie-1", "poster", "", b"\xff\xd8\xffimage", "image/jpeg")
    )
    assert rejected_but_unreadable.effect_status == "indeterminate"


def test_media_image_editing_bounds_remote_content_and_helper_formats(monkeypatch: pytest.MonkeyPatch) -> None:
    repository = FakeMediaRepository()
    store = FakeImageStore()
    mode = "declared-large"

    def respond(request: httpx.Request) -> httpx.Response:
        if "/Users/" in request.url.path:
            return httpx.Response(200, json={"Id": "movie-1", "Type": "Movie", "Path": "/movie.mkv"})
        if mode == "declared-large":
            return httpx.Response(200, headers={"Content-Length": "5", "Content-Type": "image/png"}, content=b"12345")
        return httpx.Response(200, headers={"Content-Type": "image/png"}, content=b"12345")

    monkeypatch.setattr(image_editing_adapters_module, "MAX_IMAGE_BYTES", 4)
    gateway = _image_editing_gateway(repository, store, respond)
    command = ImageUpdateCommand("movie-1", "poster", "https://images.fixture.invalid/poster.png", b"", "")
    declared = gateway.update_image(command)
    assert declared.success is False and "32MB" in declared.message

    mode = "streamed-large"
    streamed = gateway.update_image(command)
    assert streamed.success is False and "32MB" in streamed.message

    assert image_editing_adapters_module._detect_mime(b"data", "image/avif; charset=binary", "") == "image/avif"
    assert image_editing_adapters_module._detect_mime(b"data", "", "https://fixture.invalid/image.gif") == "image/gif"
    assert image_editing_adapters_module._detect_mime(b"\xff\xd8\xff", "", "") == "image/jpeg"
    assert image_editing_adapters_module._detect_mime(b"\x89PNG\r\n\x1a\n", "", "") == "image/png"
    assert image_editing_adapters_module._detect_mime(b"RIFFxxxxWEBP", "", "") == "image/webp"
    assert image_editing_adapters_module._detect_mime(b"GIF87a", "", "") == "image/gif"
    assert image_editing_adapters_module._optional_int("invalid") is None

    with pytest.raises(TypeError, match="sequence"):
        image_editing_adapters_module._format_images("invalid")
    with pytest.raises(TypeError, match="object"):
        image_editing_adapters_module._format_images(["invalid"])

    unavailable = _image_editing_gateway(
        repository,
        store,
        respond,
        settings=_settings(emby_server_url=""),
    )
    assert unavailable.processor_ready() is False
    with unavailable._default_client() as client:
        assert client.follow_redirects is True


class FakeSqlResult:
    def __init__(
        self,
        *,
        row: Mapping[str, Any] | None = None,
        rows: Sequence[Mapping[str, Any]] = (),
        rowcount: int = 1,
    ) -> None:
        self.row = row
        self.rows = tuple(rows)
        self.rowcount = rowcount

    def mappings(self) -> FakeSqlResult:
        return self

    def one_or_none(self) -> Mapping[str, Any] | None:
        return self.row

    def all(self) -> list[Mapping[str, Any]]:
        return list(self.rows)

    def first(self) -> Mapping[str, Any] | None:
        return self.row

    def __iter__(self) -> Any:
        return iter(self.rows)


class FakeSqlSession:
    def __init__(self, results: Sequence[FakeSqlResult]) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, Mapping[str, Any] | None]] = []

    def execute(self, statement: Any, parameters: Mapping[str, Any] | None = None) -> FakeSqlResult:
        self.calls.append((str(statement), parameters))
        return self.results.pop(0)


class FakeSqlUow:
    def __init__(self, session: FakeSqlSession) -> None:
        self.session = session
        self.committed = False

    def __enter__(self) -> FakeSqlUow:
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def commit(self) -> None:
        self.committed = True


def test_cast_editor_repository_reads_sorted_people_and_review() -> None:
    actor_session = FakeSqlSession(
        [
            FakeSqlResult(
                row={
                    "actors_json": [
                        "invalid",
                        {"tmdb_id": "invalid"},
                        {"tmdb_id": 1002, "character": "Role 2", "order": 2},
                        {"tmdb_id": 1001, "character": "Role 1", "order": 1},
                    ]
                }
            ),
            FakeSqlResult(
                rows=[
                    {
                        "tmdb_person_id": 1001,
                        "primary_name": "Actor 1",
                        "original_name": "Original 1",
                        "profile_path": "/one.jpg",
                        "imdb_id": "nm1",
                        "douban_celebrity_id": "db1",
                    },
                    {
                        "tmdb_person_id": 1002,
                        "primary_name": "",
                        "original_name": "Actor 2",
                        "profile_path": "/two.jpg",
                        "imdb_id": "nm2",
                        "douban_celebrity_id": "db2",
                    },
                ]
            ),
        ]
    )
    actor_uow = FakeSqlUow(actor_session)
    repository = SqlAlchemyCastEditorRepository(lambda: cast(Any, actor_uow))
    actors = repository.actors("100", "Movie")
    assert [actor["tmdb_id"] for actor in actors] == [1001, 1002]
    assert actors[1]["name"] == "Actor 2"

    empty_session = FakeSqlSession([FakeSqlResult(row={"actors_json": "invalid"})])
    empty_repository = SqlAlchemyCastEditorRepository(lambda: cast(Any, FakeSqlUow(empty_session)))
    assert empty_repository.actors("100", "Movie") == ()

    review_session = FakeSqlSession([FakeSqlResult(row={"score": 7.5, "error_message": "review"})])
    review_repository = SqlAlchemyCastEditorRepository(lambda: cast(Any, FakeSqlUow(review_session)))
    assert review_repository.review("movie-1") == {"score": 7.5, "error_message": "review"}

    missing_session = FakeSqlSession([FakeSqlResult(row=None)])
    missing_repository = SqlAlchemyCastEditorRepository(lambda: cast(Any, FakeSqlUow(missing_session)))
    assert missing_repository.review("movie-1") == {}


def test_cast_editor_uses_media_cast_when_person_cache_is_missing() -> None:
    session = FakeSqlSession([
        FakeSqlResult(row={"actors_json": [
            {"tmdb_id": 2, "name": "演员乙", "profile_path": "/two.jpg", "order": 1},
            {"tmdb_id": 1, "name": "演员甲", "profile_path": "/one.jpg", "order": 0},
        ]}),
        FakeSqlResult(rows=[{"tmdb_person_id": 2, "original_name": "Original Two"}]),
    ])
    repository = SqlAlchemyCastEditorRepository(lambda: cast(Any, FakeSqlUow(session)))
    actors = repository.actors("100", "Series")
    assert [(a["name"], a["profile_path"]) for a in actors] == [("演员甲", "/one.jpg"), ("演员乙", "/two.jpg")]


def test_cast_editor_repository_replaces_valid_cast_and_rejects_missing_target() -> None:
    success_session = FakeSqlSession([FakeSqlResult(), FakeSqlResult(rowcount=1), FakeSqlResult(), FakeSqlResult()])
    success_uow = FakeSqlUow(success_session)
    repository = SqlAlchemyCastEditorRepository(lambda: cast(Any, success_uow))
    repository.replace_cast(
        "100",
        "Movie",
        (
            {"tmdbId": "invalid", "name": "Ignored"},
            {"tmdb_id": 1001, "Name": "", "Role": "Lead"},
        ),
        item_id="movie-1",
        item_name="Movie",
    )
    assert success_uow.committed is True
    insert_parameters = success_session.calls[0][1]
    assert insert_parameters == {"tmdb_person_id": 1001, "primary_name": "TMDb 1001"}
    update_parameters = success_session.calls[1][1]
    assert update_parameters is not None and '"character": "Lead"' in str(update_parameters["actors"])

    failure_session = FakeSqlSession([FakeSqlResult(rowcount=0)])
    failure_repository = SqlAlchemyCastEditorRepository(lambda: cast(Any, FakeSqlUow(failure_session)))
    with pytest.raises(RuntimeError, match="metadata row is unavailable"):
        failure_repository.replace_cast("100", "Movie", (), item_id="movie-1", item_name="Movie")


def test_media_cast_editor_handles_missing_items_actors_and_profile_variants() -> None:
    repository = FakeCastRepository()
    item_status = 404
    item_payload: Any = None

    def respond(_request: httpx.Request) -> httpx.Response:
        if item_status == 404:
            return httpx.Response(404)
        return httpx.Response(200, json=item_payload)

    gateway = HttpxMediaCastEditorGateway(
        lambda: _settings(),
        cast(Any, repository),
        lambda: cast(Any, FakeJobs()),
        cast(Any, SimpleNamespace(translate=lambda _values: {})),
        client_factory=_client_factory(respond),
    )
    assert gateway.processor_ready() is True
    assert gateway.media_for_editing("missing") is None

    item_status = 200
    item_payload = {
        "Id": "movie-1",
        "Type": "Movie",
        "Name": "Movie",
        "ProviderIds": {"Tmdb": "100"},
        "ImageTags": "invalid",
        "People": [],
    }
    repository.actors_result = ()
    assert gateway.media_for_editing("movie-1") is None

    repository.actors_result = (
        {"tmdb_id": 1001, "name": "One", "character": "饰 Role", "profile_path": "https://img/one.jpg"},
        {"tmdb_id": 1002, "name": "Two", "character": "配 Voice", "profile_path": ""},
    )
    value = gateway.media_for_editing("movie-1")
    assert value["image_tag"] is None
    assert value["current_emby_cast"][0]["imageUrl"] == "https://img/one.jpg"
    assert value["current_emby_cast"][0]["role"] == "Role"
    assert value["current_emby_cast"][1]["imageUrl"] is None

    assert gateway.preview_item_details("movie-1") == item_payload
    processed = gateway.process_preview_cast(item_payload)
    assert [actor.tmdb_id for actor in processed] == [1001, 1002]
    with gateway._default_client() as client:
        assert client.timeout.connect == pytest.approx(_settings().emby_api_timeout)


def test_media_cast_translation_handles_empty_disabled_and_already_translated_values() -> None:
    repository = FakeCastRepository()

    def respond(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    disabled = HttpxMediaCastEditorGateway(
        lambda: _settings(ai_translate_actor_role=False),
        cast(Any, repository),
        lambda: cast(Any, FakeJobs()),
        cast(Any, SimpleNamespace(translate=lambda _values: {})),
        client_factory=_client_factory(respond),
    )
    assert disabled.translate_cast(CastTranslationCommand(cast=(), title="", year=None)) == []
    untranslated = disabled.translate_cast(
        CastTranslationCommand(cast=({"name": "Actor", "role": "Role"},), title="Movie", year=2026)
    )
    assert untranslated[0]["matchStatus"] == "AI未启用"

    calls: list[tuple[str, ...]] = []
    enabled = HttpxMediaCastEditorGateway(
        lambda: _settings(ai_translate_actor_role=True),
        cast(Any, repository),
        lambda: cast(Any, FakeJobs()),
        cast(Any, SimpleNamespace(translate=lambda values: calls.append(tuple(values)) or {})),
        client_factory=_client_factory(respond),
    )
    translated = enabled.translate_cast(
        CastTranslationCommand(cast=({"name": "演员", "role": "饰 角色"},), title="电影", year=2026)
    )
    assert translated == [{"name": "演员", "role": "饰 角色"}]
    assert calls == [()]


def test_native_cast_operation_validates_parameters_item_and_transport_uncertainty() -> None:
    repository = FakeCastRepository()
    progress = FakeProgress()
    phase = "missing"

    def respond(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            raise httpx.ConnectError("fixture write failure", request=request)
        if phase == "missing":
            return httpx.Response(404)
        return httpx.Response(
            200,
            json={
                "Id": "movie-1",
                "Name": "Movie",
                "Type": "Movie",
                "ProviderIds": {"Tmdb": "100"},
                "People": [],
            },
        )

    operation = NativeCastUpdateOperation(
        lambda: _settings(),
        cast(Any, repository),
        client_factory=_client_factory(respond),
    )
    for parameters in ({}, {"item_id": "movie-1", "cast": "invalid"}, {"item_id": "movie-1", "cast": ["bad"]}):
        with pytest.raises(ValueError, match="parameters are invalid"):
            operation.execute(cast(Any, parameters), progress)

    with pytest.raises(RuntimeError, match="无法获取"):
        operation.execute({"item_id": "movie-1", "cast": []}, progress)

    phase = "available"
    with pytest.raises(RuntimeError, match="无法确认"):
        operation.execute(
            {"item_id": "movie-1", "cast": [{"tmdbId": 1001, "name": "Actor"}]},
            progress,
        )

    with operation._default_client() as client:
        assert client.timeout.connect == pytest.approx(_settings().emby_api_timeout)


def test_cast_editor_helpers_cover_identity_people_signature_and_role_rules() -> None:
    assert (
        cast_editor_adapters_module._read_item(
            _settings(emby_server_url=""),
            _client_factory(lambda _request: httpx.Response(500)),
            "movie-1",
        )
        is None
    )

    def invalid_json(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"invalid-json")

    assert cast_editor_adapters_module._read_item(_settings(), _client_factory(invalid_json), "movie-1") is None
    with pytest.raises(ValueError, match="supported TMDb identity"):
        cast_editor_adapters_module._item_identity({"Type": "Episode", "ProviderIds": {"Tmdb": "100"}})

    item = {
        "People": [
            "invalid",
            {"Id": "no-provider"},
            {"Id": "person-1", "ProviderIds": {"Tmdb": "1001", "Imdb": "nm1"}},
        ]
    }
    assert cast_editor_adapters_module._emby_people_by_tmdb(item) == {"1001": "person-1"}
    desired = cast_editor_adapters_module._desired_emby_people(
        item,
        (
            {"name": "missing-id"},
            {"tmdbId": 1001, "Name": "Actor", "Role": "Lead"},
            {"tmdb_id": 1002, "name": "Actor 2", "role": "Support", "embyPersonId": "person-2"},
        ),
    )
    assert desired[0]["ProviderIds"] == {"Tmdb": "1001", "Imdb": "nm1"}
    assert desired[0]["Id"] == "person-1"
    assert desired[1]["Id"] == "person-2"
    assert cast_editor_adapters_module._people_signature("invalid") == ()
    assert cast_editor_adapters_module._people_signature(
        desired + [{"Name": "Director", "Type": "Director", "ProviderIds": {"Tmdb": "9"}}]
    ) == (("1001", "Actor", "Lead"), ("1002", "Actor 2", "Support"))
    assert cast_editor_adapters_module._clean_role("as Hero") == "Hero"
    assert cast_editor_adapters_module._needs_translation("Actor") is True
    assert cast_editor_adapters_module._needs_translation("演员") is False


def _sql_media_repository(
    results: Sequence[FakeSqlResult],
) -> tuple[SqlAlchemyMediaRepository, FakeSqlUow, FakeSqlSession]:
    session = FakeSqlSession(results)
    uow = FakeSqlUow(session)
    return SqlAlchemyMediaRepository(lambda: cast(Any, uow)), uow, session


@pytest.mark.parametrize(
    ("requested_type", "identity", "root_images", "season_posters", "episode_still", "result_count"),
    [
        (
            MediaItemType.BOXSET,
            MediaIdentity("400", "movie", MediaItemType.BOXSET),
            {"poster": "/p.jpg", "backdrop": "/b.jpg"},
            None,
            None,
            1,
        ),
        (
            MediaItemType.MOVIE,
            MediaIdentity("100", "movie", MediaItemType.MOVIE),
            {"poster": "/p.jpg", "backdrop": "/b.jpg", "logo": "/l.png", "thumb": "/t.jpg"},
            None,
            None,
            1,
        ),
        (
            MediaItemType.SEASON,
            MediaIdentity("200", "tv", MediaItemType.SEASON, 2),
            None,
            {2: "/s2.jpg"},
            None,
            1,
        ),
        (
            MediaItemType.EPISODE,
            MediaIdentity("200", "tv", MediaItemType.EPISODE, 2, 3),
            None,
            None,
            "/e3.jpg",
            1,
        ),
    ],
)
def test_sql_media_repository_replaces_each_cached_image_hierarchy(
    requested_type: MediaItemType,
    identity: MediaIdentity,
    root_images: Mapping[str, str | None] | None,
    season_posters: Mapping[int, str | None] | None,
    episode_still: str | None,
    result_count: int,
) -> None:
    repository, uow, _session = _sql_media_repository([FakeSqlResult(rowcount=result_count)])
    assert (
        repository.replace_cached_images(
            requested_type,
            identity,
            root_images=root_images,
            season_posters=season_posters,
            episode_still=episode_still,
        )
        == result_count
    )
    assert uow.committed is True


def test_sql_media_repository_replaces_series_root_and_each_season_poster() -> None:
    repository, uow, session = _sql_media_repository(
        [
            FakeSqlResult(rowcount=1),
            FakeSqlResult(rowcount=0),
            FakeSqlResult(rowcount=1),
            FakeSqlResult(rowcount=0),
        ]
    )
    updated = repository.replace_cached_images(
        MediaItemType.SERIES,
        MediaIdentity("200", "tv", MediaItemType.SERIES),
        root_images={"poster": "/p.jpg", "backdrop": None, "logo": None, "thumb": None},
        season_posters={1: "/s1.jpg", 2: None},
    )
    assert updated == 2
    assert uow.committed is True
    assert session.calls[-1][1] == {"poster_path": None, "tmdb_id": "200", "season_number": 2}


def test_sql_media_repository_image_replacement_noop_does_not_commit() -> None:
    repository, uow, _session = _sql_media_repository([])
    assert (
        repository.replace_cached_images(
            MediaItemType.BOXSET,
            MediaIdentity("400", "movie", MediaItemType.BOXSET),
        )
        == 0
    )
    assert uow.committed is False


@pytest.mark.parametrize("method", ["record", "clear"])
@pytest.mark.parametrize("rowcount", [0, 1])
def test_sql_media_repository_episode_screenshot_writes_commit_only_when_changed(method: str, rowcount: int) -> None:
    repository, uow, _session = _sql_media_repository([FakeSqlResult(rowcount=rowcount)])
    if method == "record":
        changed = repository.record_episode_screenshot("200", 2, 3, "a" * 64)
    else:
        changed = repository.clear_episode_screenshot("200", 2, 3)
    assert changed is bool(rowcount)
    assert uow.committed is bool(rowcount)


@pytest.mark.parametrize(
    ("identity", "image_type", "rowcount"),
    [
        (MediaIdentity("100", "movie", MediaItemType.MOVIE), "Primary", 1),
        (MediaIdentity("200", "tv", MediaItemType.SERIES), "Backdrop", 1),
        (MediaIdentity("200", "tv", MediaItemType.SEASON, 2), "Logo", 0),
        (MediaIdentity("200", "tv", MediaItemType.EPISODE, 2, 3), "Thumb", 1),
    ],
)
def test_sql_media_repository_updates_cached_image_path_per_identity(
    identity: MediaIdentity,
    image_type: str,
    rowcount: int,
) -> None:
    repository, uow, session = _sql_media_repository([FakeSqlResult(rowcount=rowcount)])
    assert repository.update_cached_image_path(identity, image_type, "fixture://image") == rowcount
    assert uow.committed is bool(rowcount)
    assert session.calls[0][1] is not None and session.calls[0][1]["tmdb_id"] == identity.tmdb_id


def test_sql_media_repository_rejects_unsupported_cached_image_coordinates() -> None:
    repository, _uow, _session = _sql_media_repository([])
    assert (
        repository.update_cached_image_path(
            MediaIdentity("100", "movie", MediaItemType.MOVIE),
            "Disc",
            "fixture://image",
        )
        == 0
    )
    assert (
        repository.update_cached_image_path(
            MediaIdentity("400", "movie", MediaItemType.BOXSET),
            "Primary",
            "fixture://image",
        )
        == 0
    )


def test_sql_media_repository_syncs_authoritative_emby_metadata_and_normalizes_tags() -> None:
    target = {
        "tmdb_id": "100",
        "item_type": "Movie",
        "parent_series_tmdb_id": None,
        "season_number": None,
        "episode_number": None,
        "official_rating_json": {"CN": "保护级"},
    }
    repository, uow, session = _sql_media_repository([FakeSqlResult(rows=[target]), FakeSqlResult(rowcount=1)])
    changed = repository.sync_emby_metadata(
        "movie-1",
        {
            "Name": "Movie",
            "OriginalTitle": "Original",
            "Overview": "Overview",
            "PremiereDate": "2026-09-09T00:00:00Z",
            "ProductionYear": "2026",
            "OfficialRating": "PG-13",
            "CustomRating": "Custom",
            "Tags": ["Drama", "drama", ""],
            "TagItems": [{"Name": "Sci-Fi"}, {"Name": "DRAMA"}, "Fantasy"],
        },
    )
    assert changed is True
    assert uow.committed is True
    values = session.calls[-1][1]
    assert values is not None
    assert json.loads(str(values["official_rating_json"])) == {"CN": "保护级", "US": "PG-13"}
    assert json.loads(str(values["tags_json"])) == ["Drama", "Sci-Fi", "Fantasy"]


def test_sql_media_repository_sync_metadata_rejects_missing_or_ambiguous_identity() -> None:
    repository, _uow, _session = _sql_media_repository([])
    assert repository.sync_emby_metadata("", {}) is False

    missing, _uow, _session = _sql_media_repository([FakeSqlResult(rows=[])])
    assert missing.sync_emby_metadata("movie-1", {}) is False

    ambiguous, _uow, _session = _sql_media_repository([FakeSqlResult(rows=[{"tmdb_id": "1"}, {"tmdb_id": "2"}])])
    assert ambiguous.sync_emby_metadata("movie-1", {}) is False

    target = {
        "tmdb_id": "100",
        "item_type": "Movie",
        "parent_series_tmdb_id": None,
        "season_number": None,
        "episode_number": None,
        "official_rating_json": "invalid",
    }
    unchanged, uow, _session = _sql_media_repository([FakeSqlResult(rows=[target]), FakeSqlResult(rowcount=0)])
    assert unchanged.sync_emby_metadata("movie-1", {"Tags": []}) is False
    assert uow.committed is False


def test_media_repository_json_helpers_preserve_only_supported_shapes() -> None:
    assert media_repository_module._json_mapping({"ok": True}) == {"ok": True}
    assert media_repository_module._json_mapping("invalid") == {}
    assert media_repository_module._emby_tag_names(
        {"Tags": ["One", "one", None], "TagItems": [{"Name": "Two"}, {"Name": ""}]}
    ) == ["One", "Two"]
