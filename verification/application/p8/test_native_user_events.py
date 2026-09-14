from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from etk.domains.identity.native_user_events import NativeUserEventEffectsAdapter
from etk.domains.media.effect_execution import MediaEffectDeliveryIndeterminate, MediaEffectDeliveryRetryable
from etk.platform import Settings


def adapter(**settings):
    calls = SimpleNamespace(
        favorite=Mock(return_value={"ok": True, "queued": True}),
        halfway=Mock(return_value={"ok": True, "queued": True}),
        cleanup=Mock(return_value=1),
        release=Mock(return_value=1),
        telegram=Mock(),
    )
    result = NativeUserEventEffectsAdapter(
        settings=lambda: Settings.model_validate(settings),
        favorite=calls.favorite,
        halfway=calls.halfway,
        cleanup=calls.cleanup,
        release_proxy=calls.release,
        telegram=calls.telegram,
        admin_chat_ids=lambda: ["admin", "admin"],
    )
    return result, calls


SNAPSHOT = {
    "Event": "playback.stop",
    "Item": {"Id": "episode", "Name": "分集"},
    "User": {"Id": "user", "Name": "用户"},
    "PlaySessionId": "session",
    "DeviceId": "device",
}


@pytest.mark.parametrize(
    "kind,method",
    [
        ("favorite-intro-detection", "favorite"),
        ("halfway-intro-detection", "halfway"),
        ("playback-cleanup", "cleanup"),
        ("proxy-concurrency-release", "release"),
    ],
)
def test_active_events_use_native_services(kind, method):
    service, calls = adapter()
    service.execute(kind, SNAPSHOT)
    assert getattr(calls, method).call_count == 1
    if method == "release":
        calls.release.assert_called_once_with(
            user_id="user", item_id="episode", play_session_id="session", device_id="device"
        )


@pytest.mark.parametrize("kind", ["playback-cleanup", "proxy-concurrency-release"])
def test_missing_identity_does_not_release_other_sessions(kind):
    service, calls = adapter()
    assert service.execute(kind, {"UserId": "user", "ItemId": "episode"})["skipped"]
    calls.cleanup.assert_not_called()
    calls.release.assert_not_called()


def test_disabled_notifications_and_retired_backlog_complete_without_external_calls():
    service, calls = adapter()
    assert service.execute("telegram-playback-notification", SNAPSHOT)["reason"] == "notification_disabled"
    assert service.execute("virtual-import-promotion", SNAPSHOT)["reason"] == "feature_retired"
    calls.telegram.send_message.assert_not_called()


def test_notification_targets_are_deduplicated_and_failures_not_reported_as_success():
    service, calls = adapter(
        telegram_notify_types=["playback"], telegram_bot_token="fixture", telegram_channel_id="channel"
    )
    confirmed = SimpleNamespace(disposition=SimpleNamespace(value="confirmed"), status_code=200)
    retryable = SimpleNamespace(disposition=SimpleNamespace(value="retryable"), status_code=429)
    calls.telegram.send_message.return_value = confirmed
    assert service.execute("telegram-playback-notification", SNAPSHOT)["sent"] == 2
    calls.telegram.send_message.side_effect = [confirmed, retryable]
    with pytest.raises(MediaEffectDeliveryIndeterminate):
        service.execute("telegram-playback-notification", SNAPSHOT)
    calls.telegram.send_message.side_effect = [retryable]
    with pytest.raises(MediaEffectDeliveryRetryable):
        service.execute("telegram-playback-notification", SNAPSHOT)
