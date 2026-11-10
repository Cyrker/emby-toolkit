from html.parser import HTMLParser
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from etk.domains.media.effect_execution import MediaEffectDeliveryIndeterminate
from etk.domains.media.library_notification_format import episode_ranges, library_card, tmdb_photo
from etk.domains.media.library_notifications import (
    PersistentLibraryResultNotification,
    TelegramLibraryNotificationDelivery,
)
from etk.integrations.http_policy import ProviderDisposition, ProviderResponse
from etk.platform import Settings
from etk.platform.tasks import TaskOutcome


def member(episode=1, **updates):
    return {
        "item_id": str(episode),
        "item_type": "Episode",
        "title": "示例剧集",
        "year": 2024,
        "season": 1,
        "episode": episode,
        "overview": "这是剧情简介。",
        "photo": "https://image.tmdb.org/t/p/w780/test.jpg",
        "assets": [{"emby_item_id": str(episode), "size_bytes": 1024**3}],
        "succeeded": True,
        "completed_at": "2026-09-14T12:00:00+08:00",
        "display_time": "2026-09-14 12:00:00",
        **updates,
    }


def test_card_combines_only_this_batch_and_deduplicates_retries():
    result = library_card([member(i) for i in range(1, 9)] + [member(1)])
    assert result["parse_mode"] == "HTML"
    assert result["photo"].endswith("test.jpg")
    assert "<b>示例剧集 (2024)</b> 🔄 已更新" in result["message"]
    assert "S01E01–E08" in result["message"]
    assert "8.00GB（8个文件）" in result["message"]
    assert "Emby ID" not in result["message"] and "确认" not in result["message"]


def test_episode_ranges_keep_gaps_and_seasons():
    assert episode_ranges([(2, 3), (2, 4), (2, 5), (2, 9), (2, 10), (3, 1), (0, 1), (None, 5)]) == (
        "S00E01, S02E03–E05, S02E09–E10, S03E01"
    )


def test_partial_card_never_claims_complete_success():
    card = library_card([member(1), member(2, succeeded=False)])
    assert "部分待处理" in card["message"] and "S01E02；请查看任务中心" in card["message"]
    assert "已更新" not in card["message"]


def test_retried_episode_success_keeps_other_pending_episodes_visible():
    card = library_card([
        member(218, season=5, succeeded=True, retry_success_count=1),
        member(219, season=5, succeeded=False),
    ])
    assert "部分待处理" in card["message"]
    assert "已成功入库（第 1 次重刮成功）" in card["message"]
    assert "S05E218" in card["message"]
    assert "S05E219；请查看任务中心" in card["message"]


def test_long_untrusted_text_is_escaped_and_caption_stays_bounded():
    card = library_card(
        [member(i, title="<script>&" * 200, overview="<b>&" * 500, succeeded=False) for i in range(1, 500, 2)]
    )
    assert len(card["message"]) <= 1024
    assert "<script>" not in card["message"]

    class Parser(HTMLParser):
        def handle_starttag(self, tag, attrs):
            assert tag in {"b", "code"} and not attrs

    Parser().feed(card["message"])


def test_movie_without_artwork_or_size_has_readable_fallback():
    card = library_card([member(item_type="Movie", season=None, episode=None, photo=None, assets=[], overview=None)])
    assert card["photo"] is None
    assert "入库成功" in card["message"] and "暂无剧情简介" in card["message"]
    assert "集数" not in card["message"] and "体积" not in card["message"]
    assert tmdb_photo({"backdrop_path": "/backdrop.jpg", "poster_path": "/poster.jpg"}).endswith("backdrop.jpg")
    assert tmdb_photo({"poster_path": "//private.invalid/secret"}) is None


def delivery(bot):
    return TelegramLibraryNotificationDelivery(
        settings=lambda: Settings(
            telegram_bot_token="test", telegram_notify_types=["library_new"], telegram_channel_id="1"
        ),
        telegram=bot,
        admin_chat_ids=lambda: ["1"],
    )


@pytest.mark.parametrize("photo", [None, "https://image.tmdb.org/t/p/w780/test.jpg"])
def test_card_delivery_uses_html_and_deduplicates_recipients(photo):
    bot = Mock()
    sent = ProviderResponse(
        ProviderDisposition.CONFIRMED, 200, {"ok": True, "result": {"message_id": 17}}
    )
    bot.send_message.return_value = bot.send_photo.return_value = sent
    effect = SimpleNamespace(effect_type="telegram.library.result", payload=library_card([member(photo=photo)]))
    kind = "caption" if photo else "text"
    assert delivery(bot).deliver(effect) == {
        "sent": 1,
        "messages": [{"chat_id": "1", "message_id": 17, "kind": kind}],
    }
    call = bot.send_photo if photo else bot.send_message
    assert call.call_count == 1 and call.call_args.kwargs == {"parse_mode": "HTML"}


def test_rejected_photo_falls_back_but_uncertain_send_does_not():
    bot = Mock()
    effect = SimpleNamespace(effect_type="telegram.library.result", payload=library_card([member()]))
    bot.send_photo.return_value = ProviderResponse(ProviderDisposition.REJECTED, 400)
    bot.send_message.return_value = ProviderResponse(
        ProviderDisposition.CONFIRMED, 200, {"ok": True, "result": {"message_id": 18}}
    )
    assert delivery(bot).deliver(effect) == {
        "sent": 1,
        "messages": [{"chat_id": "1", "message_id": 18, "kind": "text"}],
    }
    bot.reset_mock()
    bot.send_photo.return_value = ProviderResponse(ProviderDisposition.INDETERMINATE, None)
    with pytest.raises(MediaEffectDeliveryIndeterminate):
        delivery(bot).deliver(effect)
    bot.send_message.assert_not_called()


def test_retry_success_edits_the_original_photo_caption_and_text_message():
    bot = Mock()
    bot.edit_message_caption.return_value = ProviderResponse(ProviderDisposition.CONFIRMED, 200)
    bot.edit_message_text.return_value = ProviderResponse(ProviderDisposition.CONFIRMED, 200)
    batches = Mock()
    batches.mark_retry_success.return_value = {
        "updated": True,
        "members": [member(218, season=5, succeeded=True, retry_success_count=1)],
        "messages": [
            {"chat_id": "photo-chat", "message_id": 9, "kind": "caption"},
            {"chat_id": "text-chat", "message_id": 10, "kind": "text"},
        ],
    }
    settings = Settings(telegram_bot_token="test", telegram_notify_types=["library_new"])
    delivery_with_batches = TelegramLibraryNotificationDelivery(
        settings=lambda: settings,
        telegram=bot,
        admin_chat_ids=lambda: [],
        batches=batches,
    )
    effect = SimpleNamespace(
        effect_type="telegram.library.retry-success",
        payload={"member_key": "member-key", "retry_count": 1},
    )

    assert delivery_with_batches.deliver(effect) == {"updated": True, "edited": 2, "retry_count": 1}
    batches.mark_retry_success.assert_called_once_with("member-key", 1)
    assert "已成功入库（第 1 次重刮成功）" in bot.edit_message_caption.call_args.args[2]
    assert "S05E218" in bot.edit_message_text.call_args.args[2]


def test_callback_stages_episode_with_series_artwork_without_sending():
    executor = Mock()
    executor.read.return_value = None
    batches = Mock()
    batches.media.side_effect = [
        {
            "asset_details_json": [
                {"emby_item_id": "episode", "size_bytes": 1024},
                {"emby_item_id": "unrelated", "size_bytes": 9000},
            ]
        },
        {"backdrop_path": "/series.jpg"},
    ]
    batches.stage.return_value = SimpleNamespace(
        id="effect", aggregate_id="batch", status=SimpleNamespace(value="pending"), result=None
    )
    catalog = {
        "episode": {
            "Id": "episode",
            "Type": "Episode",
            "Name": "某一集",
            "SeriesId": "series",
            "ParentIndexNumber": 1,
            "IndexNumber": 2,
        },
        "series": {
            "Id": "series",
            "Type": "Series",
            "Name": "示例",
            "ProductionYear": 2024,
            "Overview": "整剧简介",
            "Path": "/media/series",
        },
    }
    notify = PersistentLibraryResultNotification(executor, catalog.get, batches=batches)
    result = notify("episode", TaskOutcome.succeeded("ok"), "operation")
    assert result["status"] == "pending"
    executor.reserve_and_execute.assert_not_called()
    saved = batches.stage.call_args.args[0]
    assert saved["title"] == "示例" and saved["overview"] == "整剧简介"
    assert saved["photo"].endswith("series.jpg") and len(saved["assets"]) == 1


def test_missing_cached_size_uses_already_read_emby_media_source_size():
    executor = Mock()
    executor.read.return_value = None
    batches = Mock()
    batches.media.return_value = {"asset_details_json": [{"emby_item_id": "movie", "path": "/media/film.strm"}]}
    item = {
        "Id": "movie",
        "Type": "Movie",
        "Name": "电影",
        "MediaSources": [{"Path": "/media/film.strm", "Size": 3 * 1024**3}],
    }
    service = PersistentLibraryResultNotification(executor, lambda _: item, batches=batches)
    saved = service._member("movie", TaskOutcome.succeeded("ok"))
    assert "3.00GB（1个文件）" in library_card([saved])["message"]
