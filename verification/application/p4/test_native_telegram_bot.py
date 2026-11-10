from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import httpx
import pytest

from etk.domains.media.effect_execution import MediaEffectDeliveryIndeterminate
from etk.domains.media.library_notifications import TelegramLibraryNotificationDelivery
from etk.integrations.http_policy import ProviderDisposition, ProviderResponse
from etk.integrations.telegram import HttpxTelegramBotAdapter
from etk.platform import Settings
from etk.processes.telegram_bot import NativeTelegramBot


def response(result):
    return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"ok": True, "result": result})


def fixture():
    settings = Settings(
        telegram_bot_token="fixture-token", tg_menu_tasks=["populate-metadata", "scan-organize-115", "missing"]
    )
    jobs = Mock()
    jobs.registry.definitions.return_value = [SimpleNamespace(key=k, name=k) for k in settings.get("tg_menu_tasks")[:2]]
    jobs.submit_task.return_value = SimpleNamespace(projection=SimpleNamespace(job=SimpleNamespace(id="job-fixture")))
    bot = Mock()
    bot.get_me.return_value = response({"username": "fixture_bot"})
    bot.send_message.return_value = response({"message_id": 1})
    state = Mock()
    state.load.return_value = None
    runtime = NativeTelegramBot(
        settings=lambda: settings, jobs=jobs, state=state, admin_chat_ids=lambda: ["1"], bot=bot
    )
    return runtime, bot, jobs, state


def message(command="/all_tasks", sender="1", chat_type="private", update_id=10):
    return {
        "update_id": update_id,
        "message": {"from": {"id": sender}, "chat": {"id": "1", "type": chat_type}, "text": command},
    }


@pytest.mark.parametrize("command", ["/all_tasks", "/all\\_tasks", "/tasks", "/all_tasks@fixture_bot"])
def test_menu_replies_and_filters_unapproved_tasks(command):
    runtime, bot, jobs, _ = fixture()
    runtime._username = "fixture_bot"
    runtime.handle_update(message(command))
    buttons = bot.send_message.call_args.kwargs["reply_markup"]["inline_keyboard"]
    assert buttons == [[{"text": "populate-metadata", "callback_data": "run_task_populate-metadata"}]]
    jobs.submit_task.assert_not_called()


@pytest.mark.parametrize("sender,chat_type", [("2", "private"), ("1", "group"), ("1", "channel")])
def test_sender_authorization_not_chat_or_channel_membership(sender, chat_type):
    runtime, bot, jobs, _ = fixture()
    runtime.handle_update(message(sender=sender, chat_type=chat_type))
    bot.send_message.assert_not_called()
    jobs.submit_task.assert_not_called()


def test_callback_submits_durable_idempotent_job_with_current_menu_permission():
    runtime, bot, jobs, _ = fixture()
    update = {
        "update_id": 11,
        "callback_query": {
            "id": "query",
            "from": {"id": "1"},
            "message": {"chat": {"id": "1", "type": "private"}},
            "data": "run_task_populate-metadata",
        },
    }
    runtime.handle_update(update)
    assert jobs.submit_task.call_args.args == ("populate-metadata", {"force_full_update": False})
    assert jobs.submit_task.call_args.kwargs["idempotency_key"] == "11"
    assert jobs.submit_task.call_args.kwargs["reject_when_busy"] is True
    jobs.reset_mock()
    update["callback_query"]["data"] = "run_task_scan-organize-115"
    runtime.handle_update(update)
    jobs.submit_task.assert_not_called()


def test_startup_discards_backlog_then_persists_offset_before_handling():
    runtime, bot, jobs, state = fixture()
    bot.get_updates.side_effect = [
        response([message("/populate_metadata", update_id=40)]),
        response([message(update_id=41)]),
        response([message(update_id=41)]),
    ]
    runtime.poll_once()
    jobs.submit_task.assert_not_called()
    bot.send_message.assert_not_called()
    assert state.save.call_args.args[1] == {"offset": 41}
    runtime.poll_once()
    assert state.save.call_args.args[1] == {"offset": 42}
    assert bot.send_message.call_count == 1
    runtime.poll_once()
    assert bot.send_message.call_count == 1
    runtime.stop()
    runtime.handle_update(message("/populate_metadata"))
    jobs.submit_task.assert_not_called()


def test_library_notification_obeys_setting_and_does_not_retry_unknown_send():
    settings = Settings(telegram_bot_token="fixture-token", telegram_channel_id="channel", telegram_notify_types=[])
    bot = Mock()
    delivery = TelegramLibraryNotificationDelivery(
        settings=lambda: settings, telegram=bot, admin_chat_ids=lambda: ["1"]
    )
    effect = SimpleNamespace(effect_type="telegram.library.result", payload={"message": "入库结果"})
    assert delivery.deliver(effect)["reason"] == "notification_disabled"
    bot.send_message.assert_not_called()
    settings = settings.model_copy(update={"telegram_notify_types": ["library_new"]})
    bot.send_message.side_effect = [response({}), ProviderResponse(ProviderDisposition.INDETERMINATE, None)]
    with pytest.raises(MediaEffectDeliveryIndeterminate):
        delivery.deliver(effect)
    assert bot.send_message.call_count == 2


def test_telegram_adapter_edits_text_and_photo_caption_by_message_id():
    client = MagicMock()
    client.__enter__.return_value = client
    client.request.side_effect = lambda *_args, **_kwargs: httpx.Response(
        200, json={"ok": True, "result": {"message_id": 42}}
    )
    adapter = HttpxTelegramBotAdapter(
        Settings(telegram_bot_token="fixture-token"), client_factory=lambda: client
    )

    text_result = adapter.edit_message_text("chat-1", 42, "updated text", parse_mode="HTML")
    caption_result = adapter.edit_message_caption("chat-1", 43, "updated caption", parse_mode="HTML")

    assert text_result.successful and caption_result.successful
    text_call, caption_call = client.request.call_args_list
    assert text_call.args[:2] == ("POST", "https://api.telegram.org/botfixture-token/editMessageText")
    assert text_call.kwargs["json"] == {
        "chat_id": "chat-1", "message_id": 42, "text": "updated text", "parse_mode": "HTML"
    }
    assert caption_call.args[:2] == ("POST", "https://api.telegram.org/botfixture-token/editMessageCaption")
    assert caption_call.kwargs["json"] == {
        "chat_id": "chat-1", "message_id": 43, "caption": "updated caption", "parse_mode": "HTML"
    }
