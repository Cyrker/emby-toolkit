from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import httpx
import pytest
from pydantic import SecretStr

from etk.domains.identity.post_authorization import identity_authorization_revision
from etk.domains.media import final_refresh_effects
from etk.domains.media.bridge_client import HttpxEmbyBridgeClient
from etk.domains.media.editing import ChapterUpdate, MediaCommandOutcome, MediaCommandStatus
from etk.domains.media.editing_effects import BridgeMediaEffectDelivery, PersistentMediaInfoCommandGateway
from etk.domains.media.effect_execution import MediaEffectDeliveryRejected
from etk.domains.media.effects import (
    MediaEffectIntentReservation,
    MediaEffectProjection,
    media_effect_request_hash,
)
from etk.domains.media.models import MediaInfoEditContext
from etk.platform import Settings

EFFECT_ID = UUID("00000000-0000-4000-8000-000000000778")


def _settings(server: str, token: str, user_id: str) -> Settings:
    return Settings(
        emby_server_url=server,
        emby_api_key=SecretStr(token),
        emby_user_id=user_id,
    )


def _effect(effect_type: str, payload: dict[str, Any]) -> MediaEffectProjection:
    return cast(
        MediaEffectProjection,
        SimpleNamespace(
            effect_type=effect_type,
            payload=payload,
        ),
    )


def _settings_provider(settings: Settings):
    def load() -> Settings:
        return settings

    return load


def test_final_refresh_stage_persists_only_revision_and_uses_it_in_effect_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings_a = _settings("http://emby-a.fixture.invalid", "token-a", "user-a")
    settings_b = _settings("http://emby-b.fixture.invalid", "token-b", "user-b")
    revision_a = identity_authorization_revision(settings_a)
    revision_b = identity_authorization_revision(settings_b)
    reservations: list[dict[str, Any]] = []

    def reserve(_cursor: object, **options: Any) -> MediaEffectIntentReservation:
        reservations.append(options)
        return MediaEffectIntentReservation(EFFECT_ID, True)

    monkeypatch.setattr(final_refresh_effects, "reserve_media_effect_dbapi", reserve)

    for settings in (settings_a, settings_b):
        provider = _settings_provider(settings)
        final_refresh_effects.install_final_refresh_settings_provider(provider)
        try:
            final_refresh_effects.stage_final_metadata_refresh(
                cast(Any, object()),
                operation_id="job-778",
                item_id="movie-778",
                item_type="Movie",
                replace_all_metadata=True,
                replace_all_images=False,
                metadata_only=False,
            )
        finally:
            final_refresh_effects.clear_final_refresh_settings_provider(provider)

    assert reservations[0]["payload"]["emby_authorization_revision"] == revision_a
    assert reservations[1]["payload"]["emby_authorization_revision"] == revision_b
    assert reservations[0]["effect_key"] != reservations[1]["effect_key"]
    serialized = repr(reservations)
    for secret in (
        "http://emby-a.fixture.invalid",
        "http://emby-b.fixture.invalid",
        "token-a",
        "token-b",
        "user-a",
        "user-b",
    ):
        assert secret not in serialized


def test_final_refresh_authorization_drift_fails_closed_before_http() -> None:
    settings_a = _settings("http://emby-a.fixture.invalid", "token-a", "user-a")
    settings_b = _settings("http://emby-b.fixture.invalid", "token-b", "user-b")
    client_factories = 0

    def client_factory() -> httpx.Client:
        nonlocal client_factories
        client_factories += 1
        return httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(204, request=request)))

    delivery = final_refresh_effects.EmbyMetadataRefreshEffectDelivery(
        lambda: settings_b,
        client_factory=client_factory,
    )
    effect = _effect(
        final_refresh_effects.FINAL_METADATA_REFRESH_EFFECT,
        {
            "item_id": "movie-778",
            "item_type": "Movie",
            "replace_all_metadata": True,
            "replace_all_images": False,
            "metadata_only": False,
            "emby_authorization_revision": identity_authorization_revision(settings_a),
        },
    )

    with pytest.raises(MediaEffectDeliveryRejected, match="authorization changed"):
        delivery.deliver(effect)

    assert client_factories == 0


@pytest.mark.parametrize(
    ("effect_type", "payload"),
    [
        (
            "emby.mediainfo.apply",
            {
                "item_id": "movie-778",
                "sha1": "A" * 40,
                "mediainfo": {"MediaSourceInfo": {"MediaStreams": []}},
                "drop_conflicting_external_streams": False,
            },
        ),
        (
            "emby.chapters.update",
            {
                "item_id": "movie-778",
                "sha1": "A" * 40,
                "intro_start_ticks": 10,
                "intro_end_ticks": 20,
                "clear_intro": False,
                "clear_credits": True,
            },
        ),
    ],
)
def test_bridge_authorization_drift_fails_closed_before_http(
    effect_type: str,
    payload: dict[str, Any],
) -> None:
    settings_a = _settings("http://emby-a.fixture.invalid", "token-a", "user-a")
    settings_b = _settings("http://emby-b.fixture.invalid", "token-b", "user-b")
    client_factories = 0
    settings_reads = 0

    def current_settings() -> Settings:
        nonlocal settings_reads
        settings_reads += 1
        return settings_a if settings_reads == 1 else settings_b

    def client_factory() -> httpx.Client:
        nonlocal client_factories
        client_factories += 1
        return httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, request=request)))

    bridge = HttpxEmbyBridgeClient(current_settings, client_factory=client_factory)
    effect = _effect(
        effect_type,
        {
            **payload,
            "emby_authorization_revision": identity_authorization_revision(settings_a),
        },
    )

    with pytest.raises(MediaEffectDeliveryRejected, match="authorization changed"):
        BridgeMediaEffectDelivery(bridge).deliver(effect)

    assert client_factories == 0
    assert settings_reads == 2


def test_mediainfo_and_chapter_staging_include_revision_in_payload_and_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings_a = _settings("http://emby-a.fixture.invalid", "token-a", "user-a")
    revision_a = identity_authorization_revision(settings_a)

    class Bridge:
        def authorization_revision(self) -> str:
            return revision_a

    gateway = PersistentMediaInfoCommandGateway(cast(Any, lambda: None), cast(Any, Bridge()))
    context = MediaInfoEditContext(
        item_id="movie-778",
        sha1="A" * 40,
        mediainfo={"MediaSourceInfo": {"MediaStreams": []}},
        title="Fixture",
        item_type="Movie",
    )
    staged: list[dict[str, Any]] = []

    def stage(_context: MediaInfoEditContext, **options: Any) -> SimpleNamespace:
        staged.append(options)
        return SimpleNamespace(id=EFFECT_ID)

    monkeypatch.setattr(gateway, "_stage", stage)
    monkeypatch.setattr(
        gateway,
        "execute",
        lambda _effect_id: MediaCommandOutcome(MediaCommandStatus.CONFIRMED, str(EFFECT_ID)),
    )

    gateway.apply_mediainfo(context, context.mediainfo)
    gateway.apply_chapters(
        context,
        ChapterUpdate(
            mediainfo=context.mediainfo,
            intro_start_ticks=10,
            intro_end_ticks=20,
            credits_start_ticks=None,
            clear_intro=False,
            clear_credits=True,
        ),
    )

    assert [entry["payload"]["emby_authorization_revision"] for entry in staged] == [revision_a, revision_a]
    assert staged[0]["effect_type"] == "emby.mediainfo.apply"
    assert staged[1]["effect_type"] == "emby.chapters.update"
    payload_a = staged[0]["payload"]
    payload_b = {**payload_a, "emby_authorization_revision": "b" * 64}
    assert media_effect_request_hash(
        effect_type="emby.mediainfo.apply",
        aggregate_type="media_asset",
        aggregate_id=f"{context.item_id}:{context.sha1}",
        payload=payload_a,
    ) != media_effect_request_hash(
        effect_type="emby.mediainfo.apply",
        aggregate_type="media_asset",
        aggregate_id=f"{context.item_id}:{context.sha1}",
        payload=payload_b,
    )
    assert "http://emby-a.fixture.invalid" not in repr(staged)
    assert "token-a" not in repr(staged)
    assert "user-a" not in repr(staged)
