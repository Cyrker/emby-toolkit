from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from io import BytesIO
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import httpx
import pytest
from PIL import Image
from pydantic import SecretStr

import etk.domains.media.studio_images_adapter as studio_images_adapter
from etk.domains.media.effect_execution import (
    MediaEffectDeliveryIndeterminate,
    MediaEffectDeliveryRejected,
    MediaEffectDeliveryRetryable,
)
from etk.domains.media.effects import MediaEffectProjection, MediaEffectStatus
from etk.domains.media.studio_images import StudioImageService
from etk.domains.media.studio_images_adapter import HttpxStudioImageAdapter, studio_image_claim_lease_seconds
from etk.platform import Settings
from etk.platform.tasks import TaskOutcomeStatus


class FakeSettings:
    def __init__(self, value: Any) -> None:
        self.value = value

    def load(self, key: str) -> Any:
        assert key == "studio_mapping"
        return self.value


class FakeProvider:
    def __init__(self) -> None:
        self.studios: Sequence[Mapping[str, Any]] = (
            {"Id": "studio-1", "Name": "Fixture Studio", "ImageTags": {}},
            {"Id": "studio-2", "Name": "Fixture Alias", "ImageTags": {"Thumb": "thumb", "Primary": "old"}},
            {"Id": "ready", "Name": "Fixture Studio", "ImageTags": {"Thumb": "thumb"}},
            {"Id": "unmapped", "Name": "Unknown", "ImageTags": {}},
        )
        self.logo_calls: list[Mapping[str, Any]] = []

    def list_studios(self) -> Sequence[Mapping[str, Any]]:
        return self.studios

    def resolve_logo(self, mapping: Mapping[str, Any]) -> tuple[str, str] | None:
        self.logo_calls.append(mapping)
        return "/fixture.png", "company:1"


class FakeEffects:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.statuses = {
            "studio-1": MediaEffectStatus.CONFIRMED,
            "studio-2": MediaEffectStatus.CONFIRMED,
        }

    def reconcile(
        self,
        *,
        effect_key: str,
        item_id: str,
        logo_path: str | None,
        logo_source: str | None,
    ) -> MediaEffectProjection:
        self.calls.append(
            {
                "effect_key": effect_key,
                "item_id": item_id,
                "logo_path": logo_path,
                "logo_source": logo_source,
            }
        )
        return cast(
            MediaEffectProjection,
            SimpleNamespace(id=UUID(int=len(self.calls)), status=self.statuses[item_id]),
        )


class FakeProgress:
    def __init__(self, *, cancel_at_checkpoint: int | None = None) -> None:
        self.checkpoints = 0
        self.cancel_at_checkpoint = cancel_at_checkpoint
        self.updates: list[tuple[int, str]] = []
        self.started_effects: list[str] = []
        self.committed_effects: list[str] = []
        self.abandoned_effects: list[str] = []

    def checkpoint(self) -> None:
        self.checkpoints += 1
        if self.checkpoints == self.cancel_at_checkpoint:
            raise RuntimeError("fixture cancellation")

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def record_started_effect(self, label: str) -> None:
        self.started_effects.append(label)

    def record_committed_effect(self, label: str) -> None:
        self.committed_effects.append(label)

    def record_abandoned_effect(self, label: str) -> None:
        self.abandoned_effects.append(label)


def _mapping() -> list[dict[str, Any]]:
    return [
        {
            "label": "Fixture Studio",
            "en": ["Fixture Alias"],
            "company_ids": [1],
        }
    ]


def test_studio_image_service_uses_aliases_and_stable_effect_identity() -> None:
    provider = FakeProvider()
    effects = FakeEffects()
    progress = FakeProgress()
    service = StudioImageService(FakeSettings(_mapping()), provider, effects)

    outcome = service.execute(progress, operation_key="studio-images:job-1")

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details == {
        "studios": 4,
        "candidates": 2,
        "ready": 1,
        "unmapped": 1,
        "no_logo": 0,
        "confirmed": 2,
        "failed": 0,
        "indeterminate": 0,
        "effect_ids": [str(UUID(int=1)), str(UUID(int=2))],
    }
    assert provider.logo_calls == [_mapping()[0]]
    assert effects.calls[0]["logo_path"] == "/fixture.png"
    assert effects.calls[0]["effect_key"].startswith("studio-images:job-1:studio:studio-1:")
    assert effects.calls[1] == {
        "effect_key": effects.calls[1]["effect_key"],
        "item_id": "studio-2",
        "logo_path": None,
        "logo_source": None,
    }
    assert progress.started_effects == [call["effect_key"] for call in effects.calls]
    assert progress.committed_effects == progress.started_effects
    assert progress.abandoned_effects == []
    assert progress.updates[-1][0] == 100


def test_studio_image_service_reports_indeterminate_effect_as_partial() -> None:
    provider = FakeProvider()
    provider.studios = ({"Id": "studio-1", "Name": "Fixture Studio", "ImageTags": {}},)
    effects = FakeEffects()
    effects.statuses["studio-1"] = MediaEffectStatus.INDETERMINATE

    progress = FakeProgress()
    outcome = StudioImageService(FakeSettings(_mapping()), provider, effects).execute(
        progress,
        operation_key="studio-images:job-2",
    )

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details["indeterminate"] == 1
    assert len(progress.started_effects) == 1
    assert progress.committed_effects == []
    assert progress.abandoned_effects == []


def test_studio_image_service_reports_claimed_effect_as_indeterminate_not_failed() -> None:
    provider = FakeProvider()
    provider.studios = ({"Id": "studio-1", "Name": "Fixture Studio", "ImageTags": {}},)
    effects = FakeEffects()
    effects.statuses["studio-1"] = MediaEffectStatus.CLAIMED

    outcome = StudioImageService(FakeSettings(_mapping()), provider, effects).execute(
        FakeProgress(),
        operation_key="studio-images:job-claimed",
    )

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details["failed"] == 0
    assert outcome.details["indeterminate"] == 1


def test_studio_image_service_rechecks_cancellation_after_logo_resolution() -> None:
    provider = FakeProvider()
    provider.studios = ({"Id": "studio-1", "Name": "Fixture Studio", "ImageTags": {}},)
    effects = FakeEffects()

    with pytest.raises(RuntimeError, match="fixture cancellation"):
        StudioImageService(FakeSettings(_mapping()), provider, effects).execute(
            FakeProgress(cancel_at_checkpoint=3),
            operation_key="studio-images:job-cancelled",
        )

    assert len(provider.logo_calls) == 1
    assert effects.calls == []


def test_studio_image_service_checkpoints_after_committed_effect() -> None:
    provider = FakeProvider()
    provider.studios = ({"Id": "studio-1", "Name": "Fixture Studio", "ImageTags": {}},)
    effects = FakeEffects()
    progress = FakeProgress(cancel_at_checkpoint=4)

    with pytest.raises(RuntimeError, match="fixture cancellation"):
        StudioImageService(FakeSettings(_mapping()), provider, effects).execute(
            progress,
            operation_key="studio-images:job-post-effect-cancelled",
        )

    assert len(effects.calls) == 1
    assert progress.committed_effects == progress.started_effects
    assert len(progress.committed_effects) == 1


def test_studio_image_service_skips_without_effective_mapping() -> None:
    provider = FakeProvider()

    outcome = StudioImageService(FakeSettings({"items": []}), provider, FakeEffects()).execute(
        FakeProgress(),
        operation_key="studio-images:job-3",
    )

    assert outcome.status is TaskOutcomeStatus.SKIPPED
    assert provider.logo_calls == []


def _settings() -> Settings:
    return Settings(
        emby_server_url="https://emby.example",
        emby_user_id="user-fixture",
        emby_api_key=SecretStr("emby-token"),
        emby_api_timeout=5,
        tmdb_api_key=SecretStr("tmdb-token"),
        tmdb_api_base_url="https://tmdb.example/3",
    )


def _adapter(handler: Callable[[httpx.Request], httpx.Response]) -> HttpxStudioImageAdapter:
    transport = httpx.MockTransport(handler)
    return HttpxStudioImageAdapter(_settings, client_factory=lambda: httpx.Client(transport=transport))


def _effect(*, logo_path: str | None = "/fixture.png") -> MediaEffectProjection:
    return cast(
        MediaEffectProjection,
        SimpleNamespace(
            payload={
                "item_id": "studio-1",
                "logo_path": logo_path,
                "logo_source": "company:1",
            }
        ),
    )


def _png() -> bytes:
    output = BytesIO()
    Image.new("RGBA", (32, 16), (255, 255, 255, 255)).save(output, format="PNG")
    return output.getvalue()


def test_studio_image_adapter_confirms_lost_upload_by_readback_then_cleans_primary() -> None:
    tags = {"Primary": "old"}
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.url.host == "image.tmdb.org":
            return httpx.Response(200, content=_png(), headers={"content-type": "image/png"})
        if request.method == "GET":
            return httpx.Response(200, json={"Id": "studio-1", "ImageTags": dict(tags)})
        if request.method == "POST":
            tags["Thumb"] = "new"
            return httpx.Response(503, json={})
        if request.method == "DELETE":
            tags.pop("Primary", None)
            return httpx.Response(204)
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    result = _adapter(handler).deliver(_effect())

    assert result == {
        "item_id": "studio-1",
        "thumb_confirmed": True,
        "primary_deleted": True,
        "logo_source": "company:1",
    }
    assert [method for method, _path in calls].count("POST") == 1
    assert [method for method, _path in calls].count("DELETE") == 1


def test_studio_image_adapter_does_not_retry_unknown_primary_cleanup() -> None:
    delete_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal delete_calls
        if request.method == "GET":
            return httpx.Response(
                200,
                json={"Id": "studio-1", "ImageTags": {"Thumb": "thumb", "Primary": "old"}},
            )
        if request.method == "DELETE":
            delete_calls += 1
            return httpx.Response(503, json={})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    with pytest.raises(MediaEffectDeliveryIndeterminate, match="cleanup outcome is unknown"):
        _adapter(handler).deliver(_effect(logo_path=None))

    assert delete_calls == 1


def test_studio_image_adapter_merges_top_level_tags_when_image_tags_is_empty() -> None:
    reads = 0
    delete_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal delete_calls, reads
        if request.method == "GET":
            reads += 1
            payload = {
                "Id": "studio-1",
                "ImageTags": {},
                "ThumbImageTag": "thumb",
            }
            if reads == 1:
                payload["PrimaryImageTag"] = "old"
            return httpx.Response(200, json=payload)
        if request.method == "DELETE":
            delete_calls += 1
            return httpx.Response(204)
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    result = _adapter(handler).deliver(_effect(logo_path=None))

    assert result["thumb_confirmed"] is True
    assert result["primary_deleted"] is True
    assert delete_calls == 1


def test_studio_image_adapter_rejects_malformed_initial_readback_and_keeps_malformed_post_write_unknown() -> None:
    initial = _adapter(lambda _request: httpx.Response(200, json={"Id": "wrong", "ImageTags": {}}))
    with pytest.raises(MediaEffectDeliveryRejected, match="invalid item"):
        initial.deliver(_effect())

    reads = 0

    def malformed_after_delete(request: httpx.Request) -> httpx.Response:
        nonlocal reads
        if request.method == "GET":
            reads += 1
            if reads == 1:
                return httpx.Response(
                    200,
                    json={"Id": "studio-1", "ImageTags": {"Thumb": "thumb", "Primary": "old"}},
                )
            return httpx.Response(200, json={"Id": "studio-1"})
        if request.method == "DELETE":
            return httpx.Response(503, json={})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    with pytest.raises(MediaEffectDeliveryIndeterminate, match="readback is unavailable"):
        _adapter(malformed_after_delete).deliver(_effect(logo_path=None))


@pytest.mark.parametrize("status_code", [404, 401, 403])
def test_studio_image_adapter_rejects_authoritative_initial_read_failures(status_code: int) -> None:
    adapter = _adapter(lambda _request: httpx.Response(status_code, json={}))

    with pytest.raises(MediaEffectDeliveryRejected):
        adapter.deliver(_effect())


@pytest.mark.parametrize("status_code", [302, 408, 425, 429, 500])
def test_studio_image_adapter_retries_only_transient_initial_read_failures(status_code: int) -> None:
    adapter = _adapter(lambda _request: httpx.Response(status_code, json={}))

    with pytest.raises(MediaEffectDeliveryRetryable):
        adapter.deliver(_effect())


def test_studio_image_adapter_resolves_network_before_company_fallback() -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path.endswith("/network/1"):
            return httpx.Response(404, json={})
        if request.url.path.endswith("/company/2"):
            return httpx.Response(200, json={"logo_path": "/company.svg"})
        raise AssertionError(f"unexpected request: {request.url}")

    result = _adapter(handler).resolve_logo({"network_ids": [1], "company_ids": [2]})

    assert result == ("/company.svg", "company:2")
    assert paths == ["/3/network/1", "/3/company/2"]


def test_studio_image_adapter_rejects_svg_and_oversized_raster_before_decode_expansion() -> None:
    with pytest.raises(MediaEffectDeliveryRejected, match="SVG"):
        studio_images_adapter._studio_thumb(b"<svg/>", "image/svg+xml")

    oversized = BytesIO()
    Image.new("RGBA", (8_193, 1), (255, 255, 255, 255)).save(oversized, format="PNG")
    with pytest.raises(MediaEffectDeliveryRejected, match="dimensions"):
        studio_images_adapter._studio_thumb(oversized.getvalue(), "image/png")


def test_studio_image_claim_lease_covers_six_provider_calls_and_margin() -> None:
    assert studio_image_claim_lease_seconds(60) == 390
    assert studio_image_claim_lease_seconds(5) == 90
