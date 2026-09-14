from __future__ import annotations

import base64
import hashlib
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr

from etk.domains.collection.cover_task_execution import (
    COVER_IMAGE_EFFECT_TYPE,
    ConfiguredCoverRenderer,
    CoverSourceSnapshot,
    HttpxCoverRemoteAdapter,
    NativeCoverTaskService,
    PersistentCoverTaskEffects,
)
from etk.domains.media.effect_execution import (
    MediaEffectDeliveryIndeterminate,
    MediaEffectDeliveryRejected,
    MediaEffectDeliveryRetryable,
)
from etk.domains.media.effects import MediaEffectProjection, MediaEffectStatus
from etk.platform import Settings
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcome, TaskOutcomeStatus
from etk.processes.p8_effect_tasks import P8EffectTaskRouter

REPO_ROOT = Path(__file__).resolve().parents[3]


class _Progress:
    def __init__(self, *, fail_after: int | None = None) -> None:
        self.checkpoints = 0
        self.job_id = UUID("00000000-0000-0000-0000-000000000804")
        self._fail_after = fail_after
        self.started_effects: list[str] = []
        self.committed_effects: list[str] = []
        self.abandoned_effects: list[str] = []

    def checkpoint(self) -> None:
        self.checkpoints += 1
        if self._fail_after is not None and self.checkpoints > self._fail_after:
            raise RuntimeError("fixture cancellation")

    def update(self, progress: int, message: str) -> None:
        del progress, message
        return None

    def success_reported(self) -> bool:
        return False

    def record_started_effect(self, label: str) -> None:
        self.started_effects.append(label)

    def record_committed_effect(self, label: str) -> None:
        self.committed_effects.append(label)

    def record_abandoned_effect(self, label: str) -> None:
        self.abandoned_effects.append(label)


class _Repository:
    def __init__(self, *, enabled: bool = True) -> None:
        self.enabled = enabled

    def config(self) -> Mapping[str, object]:
        return {"enabled": self.enabled, "cover_style": "single_1", "exclude_libraries": ["ignored"]}

    def list_custom_targets(self) -> Sequence[Mapping[str, object]]:
        return ({"id": "custom-1", "name": "Custom", "item_count": 3},)


class _Remote:
    def __init__(self) -> None:
        self.configs: list[Mapping[str, object]] = []

    def list_libraries(self) -> Sequence[Mapping[str, object]]:
        return (
            {"Id": "movies", "Name": "Movies", "CollectionType": "movies", "item_count": 2},
            {"Id": "music", "Name": "Music", "CollectionType": "music", "item_count": 1},
            {"Id": "ignored", "Name": "Ignored", "CollectionType": "movies", "item_count": 1},
            {"Id": "other", "Name": "Other", "CollectionType": "other", "item_count": 1},
        )

    def source_images(
        self,
        target: Mapping[str, object],
        limit: int,
        *,
        config: Mapping[str, object],
    ) -> CoverSourceSnapshot:
        del target, limit
        self.configs.append(config)
        return CoverSourceSnapshot((b"source-image",), 7)


class _Renderer:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def render(self, **kwargs: object) -> object:
        self.calls.append(dict(kwargs))
        return SimpleNamespace(cache_key="cache-key", png=b"rendered-cover")


class _Effects:
    def __init__(self, status: MediaEffectStatus, *, existing: MediaEffectProjection | None = None) -> None:
        self.status = status
        self.existing = existing
        self.calls: list[dict[str, object]] = []
        self.reads: list[tuple[str, str]] = []
        self.executions: list[UUID] = []

    def read(self, effect_key: str, target_id: str) -> MediaEffectProjection | None:
        self.reads.append((effect_key, target_id))
        return self.existing

    def execute(self, effect_id: UUID) -> MediaEffectProjection:
        self.executions.append(effect_id)
        assert self.existing is not None
        return self.existing

    def apply(self, **kwargs: object) -> MediaEffectProjection:
        self.calls.append(dict(kwargs))
        return cast(MediaEffectProjection, SimpleNamespace(id=uuid4(), status=self.status))


def _settings() -> Settings:
    return Settings(
        emby_server_url="http://emby.test",
        emby_user_id="user-1",
        emby_api_key=SecretStr("fixture-token"),
    )


def _cover_effect(
    content: bytes,
    *,
    target_name: str = "Target",
    output_root: str | None = None,
) -> MediaEffectProjection:
    return cast(
        MediaEffectProjection,
        SimpleNamespace(
            payload={
                "target_id": "target-1",
                "target_name": target_name,
                "content_b64": base64.b64encode(content).decode("ascii"),
                "content_sha256": hashlib.sha256(content).hexdigest(),
                "byte_size": len(content),
                "output_root": output_root,
            }
        ),
    )


def test_native_cover_task_filters_libraries_and_stages_durable_effects() -> None:
    remote = _Remote()
    renderer = _Renderer()
    effects = _Effects(MediaEffectStatus.CONFIRMED)
    service = NativeCoverTaskService(_Repository(), remote, renderer, effects)
    progress = _Progress()

    outcome = service.execute("generate-all-covers", progress)

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["targets"] == 2
    assert outcome.details["updated"] == 2
    assert outcome.details["failed"] == 0
    assert len(effects.calls) == 2
    assert [call["item_count"] for call in renderer.calls] == [2, 1]
    assert all(str(call["effect_key"]).startswith("p8:cover:generate-all-covers:") for call in effects.calls)
    assert all(call["content"] == b"rendered-cover" for call in effects.calls)
    assert progress.started_effects == progress.committed_effects
    assert len(progress.committed_effects) == 2
    assert progress.abandoned_effects == []


def test_native_custom_cover_task_keeps_pending_effects_partial() -> None:
    effects = _Effects(MediaEffectStatus.INDETERMINATE)
    renderer = _Renderer()
    service = NativeCoverTaskService(_Repository(), _Remote(), renderer, effects)
    progress = _Progress()

    outcome = service.execute("generate-custom-collection-covers", progress)

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details["targets"] == 1
    assert outcome.details["updated"] == 0
    assert outcome.details["indeterminate"] == 1
    assert outcome.details["failed"] == 0
    assert renderer.calls[0]["item_count"] == 3
    assert len(progress.started_effects) == 1
    assert progress.committed_effects == []
    assert progress.abandoned_effects == []


def test_disabled_cover_task_is_skipped_without_remote_or_effect() -> None:
    effects = _Effects(MediaEffectStatus.CONFIRMED)
    service = NativeCoverTaskService(_Repository(enabled=False), _Remote(), _Renderer(), effects)

    outcome = service.execute("generate-all-covers", _Progress())

    assert outcome.status is TaskOutcomeStatus.SKIPPED
    assert effects.calls == []


def test_persistent_cover_effects_bounds_and_hashes_payload() -> None:
    class _Executor:
        def __init__(self) -> None:
            self.kwargs: dict[str, object] = {}

        def read(self, effect_key: str) -> None:
            del effect_key
            return None

        def execute(self, effect_id: UUID) -> object:
            del effect_id
            raise AssertionError("no existing effect")

        def reserve_and_execute(self, **kwargs: object) -> MediaEffectProjection:
            self.kwargs = dict(kwargs)
            return cast(MediaEffectProjection, SimpleNamespace(id=uuid4(), status=MediaEffectStatus.CONFIRMED))

    executor = _Executor()
    effects = PersistentCoverTaskEffects(executor)  # type: ignore[arg-type]
    effects.apply(
        effect_key="cover:key",
        target_id="target",
        target_name="Target",
        content=b"png",
        cache_key="cache",
        output_root=None,
    )

    assert executor.kwargs["effect_type"] == COVER_IMAGE_EFFECT_TYPE
    payload = executor.kwargs["payload"]
    assert isinstance(payload, Mapping)
    assert payload["byte_size"] == 3
    assert payload["content_sha256"] == hashlib.sha256(b"png").hexdigest()


def test_persistent_cover_effects_reject_cross_target_effect_key_reuse() -> None:
    existing = cast(
        MediaEffectProjection,
        SimpleNamespace(
            id=uuid4(),
            effect_type=COVER_IMAGE_EFFECT_TYPE,
            aggregate_type="emby_cover",
            aggregate_id="other-target",
        ),
    )

    class _Executor:
        def read(self, effect_key: str) -> MediaEffectProjection:
            assert effect_key == "cover:key"
            return existing

        def execute(self, effect_id: UUID) -> object:
            raise AssertionError(f"must not execute conflicting effect {effect_id}")

    effects = PersistentCoverTaskEffects(cast(Any, _Executor()))

    with pytest.raises(ValueError, match="different target"):
        effects.apply(
            effect_key="cover:key",
            target_id="target",
            target_name="Target",
            content=b"png",
            cache_key="cache",
            output_root=None,
        )


def test_p8_router_dispatches_cover_tasks_without_compatibility_fallback() -> None:
    class _CoverOwner:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def execute(self, task_key: str, progress: object) -> TaskOutcome:
            self.calls.append(task_key)
            progress.update(100, "done")  # type: ignore[attr-defined]
            return TaskOutcome.succeeded("done")

    owner = _CoverOwner()
    router = P8EffectTaskRouter(
        compatibility=None,
        user_sync=cast(Any, SimpleNamespace(execute=lambda _progress: TaskOutcome.succeeded("unused"))),
        cover_tasks=owner,
    )
    message = TaskMessage.create(task_key="generate-all-covers", processor=ProcessorIdentity.MEDIA)
    outcome = router.execute(message, processor=None, progress=_Progress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["compatibility_fallback"] is False
    assert owner.calls == ["generate-all-covers"]


def test_cancellation_after_render_stops_before_durable_cover_effect() -> None:
    effects = _Effects(MediaEffectStatus.CONFIRMED)
    service = NativeCoverTaskService(_Repository(), _Remote(), _Renderer(), effects)

    with pytest.raises(RuntimeError, match="fixture cancellation"):
        service.execute("generate-custom-collection-covers", _Progress(fail_after=1))

    assert effects.calls == []


def test_cancellation_after_confirmed_effect_is_not_swallowed() -> None:
    effects = _Effects(MediaEffectStatus.CONFIRMED)
    progress = _Progress(fail_after=2)
    service = NativeCoverTaskService(_Repository(), _Remote(), _Renderer(), effects)

    with pytest.raises(RuntimeError, match="fixture cancellation"):
        service.execute("generate-custom-collection-covers", progress)

    assert len(effects.calls) == 1
    assert progress.started_effects == progress.committed_effects
    assert progress.abandoned_effects == []


def test_cover_effect_exception_is_recorded_as_abandoned() -> None:
    class _FailingEffects(_Effects):
        def apply(self, **kwargs: object) -> MediaEffectProjection:
            self.calls.append(dict(kwargs))
            raise RuntimeError("fixture delivery failure")

    effects = _FailingEffects(MediaEffectStatus.CONFIRMED)
    progress = _Progress()
    service = NativeCoverTaskService(_Repository(), _Remote(), _Renderer(), effects)

    outcome = service.execute("generate-custom-collection-covers", progress)

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert progress.started_effects == progress.abandoned_effects
    assert progress.committed_effects == []


def test_cover_effect_identity_is_stable_per_job_and_changes_for_a_new_job() -> None:
    effects = _Effects(MediaEffectStatus.CONFIRMED)
    service = NativeCoverTaskService(_Repository(), _Remote(), _Renderer(), effects)
    first_job = _Progress()

    service.execute("generate-custom-collection-covers", first_job)
    service.execute("generate-custom-collection-covers", first_job)
    second_job = _Progress()
    second_job.job_id = UUID("00000000-0000-0000-0000-000000000805")
    service.execute("generate-custom-collection-covers", second_job)

    assert effects.calls[0]["effect_key"] == effects.calls[1]["effect_key"]
    assert effects.calls[2]["effect_key"] != effects.calls[0]["effect_key"]


def test_cover_retry_resumes_persisted_effect_before_source_query_or_render() -> None:
    existing = cast(
        MediaEffectProjection,
        SimpleNamespace(
            id=uuid4(),
            status=MediaEffectStatus.CONFIRMED,
            effect_type=COVER_IMAGE_EFFECT_TYPE,
            aggregate_type="emby_cover",
            aggregate_id="custom-1",
        ),
    )

    class _UnavailableRemote(_Remote):
        def source_images(
            self,
            target: Mapping[str, object],
            limit: int,
            *,
            config: Mapping[str, object],
        ) -> CoverSourceSnapshot:
            del target, limit, config
            raise AssertionError("persisted cover recovery must not query sources")

    renderer = _Renderer()
    effects = _Effects(MediaEffectStatus.CONFIRMED, existing=existing)
    progress = _Progress()
    service = NativeCoverTaskService(_Repository(), _UnavailableRemote(), renderer, effects)

    outcome = service.execute("generate-custom-collection-covers", progress)

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert renderer.calls == []
    assert effects.calls == []
    assert effects.executions == [existing.id]
    assert progress.started_effects == progress.committed_effects


def test_custom_collection_uses_emby_identity_and_legacy_badge_label() -> None:
    class _CustomRepository(_Repository):
        def list_custom_targets(self) -> Sequence[Mapping[str, object]]:
            return (
                {
                    "id": 7,
                    "emby_collection_id": "emby-custom",
                    "name": "Custom",
                    "type": "list",
                    "definition_json": {"url": "https://www.douban.com/doulist/7"},
                },
            )

    renderer = _Renderer()
    effects = _Effects(MediaEffectStatus.CONFIRMED)
    service = NativeCoverTaskService(_CustomRepository(), _Remote(), renderer, effects)

    outcome = service.execute("generate-custom-collection-covers", _Progress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert effects.calls[0]["target_id"] == "emby-custom"
    assert renderer.calls[0]["item_count"] == "豆列"


def test_httpx_cover_sources_preserve_sort_type_and_backdrop_selection() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/Users/user-1/Items":
            return httpx.Response(
                200,
                json={
                    "Items": [
                        {
                            "Id": "source-1",
                            "ImageTags": {"Primary": "primary-tag"},
                            "BackdropImageTags": ["backdrop-tag"],
                            "OfficialRating": "PG-13",
                        }
                    ],
                    "TotalRecordCount": 27,
                },
            )
        if request.url.path == "/Items/source-1/Images/Backdrop/0":
            return httpx.Response(200, content=b"backdrop", headers={"Content-Type": "image/jpeg"})
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    adapter = HttpxCoverRemoteAdapter(
        _settings,
        client_factory=lambda: httpx.Client(transport=transport),
    )

    snapshot = adapter.source_images(
        {
            "id": 7,
            "emby_collection_id": "emby-custom",
            "name": "Custom",
            "item_type": ["Movie"],
        },
        1,
        config={"cover_style": "single_1", "single_use_primary": False, "sort_by": "Latest"},
    )

    assert snapshot == CoverSourceSnapshot((b"backdrop",), 27)
    query = requests[0].url.params
    assert query["ParentId"] == "emby-custom"
    assert query["IncludeItemTypes"] == "Movie"
    assert query["SortBy"] == "DateCreated"
    assert query["SortOrder"] == "Descending"


def test_local_cover_sources_remain_usable_when_the_inventory_query_fails(tmp_path: Path) -> None:
    source = tmp_path / "Custom" / "1.png"
    source.parent.mkdir()
    source.write_bytes(b"local-cover")

    adapter = HttpxCoverRemoteAdapter(
        _settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(503))),
    )

    snapshot = adapter.source_images(
        {"id": "custom-1", "name": "Custom"},
        1,
        config={"covers_input": str(tmp_path)},
    )

    assert snapshot == CoverSourceSnapshot((b"local-cover",), None)


def test_tmdb_poster_fallback_precedes_an_empty_emby_collection() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.host == "image.tmdb.org"
        return httpx.Response(200, content=b"tmdb-cover", headers={"Content-Type": "image/jpeg"})

    adapter = HttpxCoverRemoteAdapter(
        _settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )

    snapshot = adapter.source_images(
        {
            "id": 7,
            "emby_collection_id": "emby-custom",
            "name": "Custom",
            "type": "list",
            "generated_media_info_json": [{"tmdb_id": "7"}],
            "tmdb_poster_paths": ["/poster.jpg"],
        },
        1,
        config={"sort_by": "Latest"},
    )

    assert snapshot == CoverSourceSnapshot((b"tmdb-cover",), None)
    assert [request.url.path for request in requests] == ["/t/p/w500/poster.jpg"]


def test_cover_adapter_routes_emby_inventory_and_tmdb_posters_to_separate_clients() -> None:
    routed: list[tuple[str, str]] = []

    def direct(request: httpx.Request) -> httpx.Response:
        routed.append(("direct", request.url.host))
        return httpx.Response(200, json=[])

    def external(request: httpx.Request) -> httpx.Response:
        routed.append(("external", request.url.host))
        return httpx.Response(200, content=b"tmdb-cover", headers={"Content-Type": "image/jpeg"})

    adapter = HttpxCoverRemoteAdapter(
        _settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(direct)),
        external_client_factory=lambda: httpx.Client(transport=httpx.MockTransport(external)),
    )

    assert adapter.list_libraries() == ()
    snapshot = adapter.source_images(
        {
            "id": 7,
            "emby_collection_id": "emby-custom",
            "name": "Custom",
            "type": "list",
            "generated_media_info_json": [{"tmdb_id": "7"}],
            "tmdb_poster_paths": ["/poster.jpg"],
        },
        1,
        config={"sort_by": "Latest"},
    )

    assert snapshot == CoverSourceSnapshot((b"tmdb-cover",), None)
    assert routed == [
        ("direct", "emby.test"),
        ("external", "image.tmdb.org"),
    ]


def test_cover_sources_apply_country_specific_safe_rating_limits() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/Users/user-1/Items":
            return httpx.Response(
                200,
                json={
                    "Items": [
                        {"Id": "adult", "BackdropImageTags": ["adult-tag"], "OfficialRating": "GB-18"},
                        {"Id": "safe", "BackdropImageTags": ["safe-tag"], "OfficialRating": "GB-12"},
                    ],
                    "TotalRecordCount": 2,
                },
            )
        assert request.url.path == "/Items/safe/Images/Backdrop/0"
        return httpx.Response(200, content=b"safe-cover", headers={"Content-Type": "image/jpeg"})

    adapter = HttpxCoverRemoteAdapter(
        _settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )

    snapshot = adapter.source_images(
        {"Id": "movies", "Name": "Movies", "CollectionType": "movies"},
        1,
        config={"max_safe_rating": 8},
    )

    assert snapshot == CoverSourceSnapshot((b"safe-cover",), 2)
    assert [request.url.path for request in requests] == [
        "/Users/user-1/Items",
        "/Items/safe/Images/Backdrop/0",
    ]


def test_cover_source_download_rejects_oversized_content_length_without_buffering() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/Users/user-1/Items":
            return httpx.Response(
                200,
                json={"Items": [{"Id": "source", "BackdropImageTags": ["tag"]}]},
            )
        return httpx.Response(
            200,
            headers={"Content-Type": "image/jpeg", "Content-Length": str(8 * 1024 * 1024 + 1)},
        )

    adapter = HttpxCoverRemoteAdapter(
        _settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )

    snapshot = adapter.source_images(
        {"Id": "movies", "Name": "Movies", "CollectionType": "movies"},
        1,
        config={},
    )

    assert snapshot.images == ()


def test_configured_cover_renderer_rejects_oversized_remote_fonts(tmp_path: Path) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"Content-Length": str(16 * 1024 * 1024 + 1)})

    renderer = ConfiguredCoverRenderer(
        default_zh_font=REPO_ROOT / "fonts" / "zh_font.ttf",
        default_en_font=REPO_ROOT / "fonts" / "en_font.ttf",
        cache_root=tmp_path / "fonts",
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )

    with pytest.raises(RuntimeError, match="font download failed"):
        renderer.render(
            style="single_1",
            sources=((REPO_ROOT / "verification/fixtures/images/p8/cover-source-1.png").read_bytes(),),
            title_zh="Fixture",
            title_en="",
            item_count=None,
            blur=False,
            seed=1,
            config={"zh_font_url": "https://fonts.invalid/zh.ttf"},
        )

    assert list((tmp_path / "fonts").iterdir()) == []


def test_cover_delivery_accepts_2xx_without_repeating_the_mutation_and_saves_output(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []
    content = b"rendered-cover"

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(404)
        assert request.content == base64.b64encode(content)
        return httpx.Response(204)

    adapter = HttpxCoverRemoteAdapter(
        _settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )

    result = adapter.deliver(_cover_effect(content, target_name="../Target", output_root=str(tmp_path)))

    assert [request.method for request in requests] == ["GET", "POST"]
    assert result["provider_response_confirmed"] is True
    assert result["output_saved"] is True
    assert (tmp_path / ".._Target.jpg").read_bytes() == content


def test_cover_delivery_recovers_a_lost_response_by_digest_readback() -> None:
    content = b"rendered-cover"
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "POST":
            raise httpx.ReadError("fixture response lost", request=request)
        if len(requests) == 1:
            return httpx.Response(404)
        return httpx.Response(200, content=content, headers={"Content-Type": "image/png"})

    adapter = HttpxCoverRemoteAdapter(
        _settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )

    result = adapter.deliver(_cover_effect(content))

    assert [request.method for request in requests] == ["GET", "POST", "GET"]
    assert result["recovered_by_readback"] is True


@pytest.mark.parametrize(
    ("initial_status", "post_status", "error_type", "expected_methods"),
    [
        (503, None, MediaEffectDeliveryRetryable, ["GET"]),
        (404, 400, MediaEffectDeliveryRejected, ["GET", "POST"]),
        (404, 503, MediaEffectDeliveryIndeterminate, ["GET", "POST", "GET"]),
    ],
)
def test_cover_delivery_classifies_readback_and_upload_failures(
    initial_status: int,
    post_status: int | None,
    error_type: type[Exception],
    expected_methods: list[str],
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "POST":
            assert post_status is not None
            return httpx.Response(post_status)
        status = initial_status if len(requests) == 1 else 404
        return httpx.Response(status)

    adapter = HttpxCoverRemoteAdapter(
        _settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )

    with pytest.raises(error_type):
        adapter.deliver(_cover_effect(b"rendered-cover"))

    assert [request.method for request in requests] == expected_methods


def test_cover_delivery_rejects_missing_authorization_before_http() -> None:
    adapter = HttpxCoverRemoteAdapter(
        lambda: Settings(),
        client_factory=lambda: (_ for _ in ()).throw(AssertionError("HTTP must not be attempted")),
    )

    with pytest.raises(MediaEffectDeliveryRejected, match="not configured"):
        adapter.deliver(_cover_effect(b"rendered-cover"))
