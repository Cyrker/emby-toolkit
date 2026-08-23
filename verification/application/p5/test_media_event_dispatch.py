from __future__ import annotations

from typing import Any

import pytest
from flask import Flask, jsonify, request

from etk.compatibility.flask_app import LazyCompatibilityApplication
from etk.domains.media.compatibility import (
    LegacyFutureDomainEventGateway,
    LegacyP5MediaEventGateway,
)
from etk.domains.media.event_dispatch import (
    FUTURE_DOMAIN_EVENT_NAMES,
    P5_MEDIA_EVENT_NAMES,
    EmbyEventOwner,
    classify_emby_event,
)
from etk.domains.media.models import ServiceResult


def _compatibility_app() -> LazyCompatibilityApplication:
    def factory() -> Flask:
        app = Flask("p5-event-fixture")

        @app.post("/api/emby/events")
        def events() -> Any:
            payload = request.get_json()
            return jsonify({"status": "legacy-forwarded", "event": payload["Event"]}), 202

        return app

    return LazyCompatibilityApplication(factory)


def test_event_catalog_has_exactly_two_p5_and_eleven_future_domain_owners() -> None:
    assert P5_MEDIA_EVENT_NAMES == {"metadata.update", "image.update"}
    assert len(FUTURE_DOMAIN_EVENT_NAMES) == 11
    assert P5_MEDIA_EVENT_NAMES.isdisjoint(FUTURE_DOMAIN_EVENT_NAMES)
    assert classify_emby_event("metadata.update") is EmbyEventOwner.P5_MEDIA
    assert classify_emby_event("playback.stop") is EmbyEventOwner.FUTURE_DOMAIN
    assert classify_emby_event("plugin.future-event") is EmbyEventOwner.LEGACY_UNKNOWN


def test_typed_compatibility_gateways_share_the_legacy_app_but_fail_closed_by_owner() -> None:
    compatibility_app = _compatibility_app()
    p5 = LegacyP5MediaEventGateway(compatibility_app)
    future = LegacyFutureDomainEventGateway(compatibility_app)

    p5_result = p5.handle(
        {"Event": "metadata.update", "Item": {"Id": "movie-1", "Type": "Movie"}},
        host_url="http://fixture.invalid/",
        remote_addr="127.0.0.1",
    )
    future_result = future.handle(
        {
            "Event": "playback.stop",
            "User": {"Id": "user-1"},
            "Item": {"Id": "movie-1", "Type": "Movie"},
            "PlaybackInfo": {"PositionTicks": 60, "RunTimeTicks": 100},
        },
        host_url="http://fixture.invalid/",
        remote_addr="127.0.0.1",
    )

    assert p5_result == ServiceResult(
        {"status": "legacy-forwarded", "event": "metadata.update"},
        202,
        headers={"Content-Type": "application/json"},
    )
    assert future_result == ServiceResult(
        {"status": "legacy-forwarded", "event": "playback.stop"},
        202,
        headers={"Content-Type": "application/json"},
    )
    with pytest.raises(ValueError, match="future-domain, not p5-media"):
        p5.handle(
            {"Event": "playback.stop"},
            host_url="http://fixture.invalid/",
            remote_addr="127.0.0.1",
        )
    with pytest.raises(ValueError, match="p5-media, not future-domain"):
        future.handle(
            {"Event": "image.update"},
            host_url="http://fixture.invalid/",
            remote_addr="127.0.0.1",
        )
