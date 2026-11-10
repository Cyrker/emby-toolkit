from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
from sqlalchemy import text

from etk.domains.ingestion import MonitorPolicy, PostgresIngestionMonitorLedger
from etk.domains.media.effect_execution import MediaEffectDeliveryRetryable, PersistentMediaEffectExecutor
from etk.domains.media.effects import PostgresMediaEffectLedger
from etk.domains.media.library_notification_format import library_card
from etk.domains.media.library_notification_repository import (
    SqlAlchemyLibraryNotificationRepository,
    library_notification_member_key,
)
from etk.domains.media.library_notifications import TelegramLibraryNotificationDelivery
from etk.integrations.http_policy import ProviderDisposition, ProviderResponse
from etk.platform import Settings


@pytest.fixture()
def repository(p6_runtime):
    with p6_runtime.engine.begin() as c:
        c.execute(text("DELETE FROM media_library_notification_batches"))
    return SqlAlchemyLibraryNotificationRepository(p6_runtime.unit_of_work)


def member(i=1, group="series"):
    return {
        "item_id": str(i),
        "group_key": group,
        "group_path": "/fixture/series",
        "item_type": "Episode",
        "title": "示例",
        "season": 1,
        "episode": i,
        "succeeded": True,
        "completed_at": datetime.now(UTC).isoformat(),
        "display_time": "2026-09-14 12:00:00",
        "photo": "https://image.tmdb.org/t/p/w780/fixture.jpg",
    }


def ready(runtime):
    with runtime.engine.begin() as c:
        c.execute(text("UPDATE media_library_notification_batches SET quiet_until=NOW()-INTERVAL '1 second'"))
        c.execute(
            text(
                "UPDATE media_effects SET available_at=NOW()-INTERVAL '1 second' "
                "WHERE effect_type='telegram.library.batch'"
            )
        )


def test_concurrent_completions_create_one_durable_batch_and_outbox(repository, p6_runtime):
    with ThreadPoolExecutor(max_workers=4) as pool:
        effects = list(pool.map(lambda i: repository.stage(member(i), operation_key=f"op-{i}"), range(1, 9)))
    assert len({e.id for e in effects}) == 1
    with p6_runtime.engine.connect() as c:
        assert (
            c.execute(text("SELECT count(*) FROM media_effects WHERE effect_type='telegram.library.batch'")).scalar()
            == 1
        )
        assert c.execute(text("SELECT count(*) FROM outbox_events WHERE topic='media.effect.requested'")).scalar() == 1
    with pytest.raises(MediaEffectDeliveryRetryable):
        repository.snapshot(effects[0].aggregate_id)
    restarted = SqlAlchemyLibraryNotificationRepository(p6_runtime.unit_of_work)
    assert restarted.stage(member(1), operation_key="op-1").id == effects[0].id
    ready(p6_runtime)
    assert len(restarted.snapshot(effects[0].aggregate_id)) == 8
    assert restarted.stage(member(9), operation_key="op-9").id != effects[0].id
    assert restarted.stage(member(1), operation_key="op-1").id == effects[0].id


def test_pending_same_series_blocks_delivery_but_other_series_does_not(repository, p6_runtime):
    effect = repository.stage(member(), operation_key="pending")
    ingress = PostgresIngestionMonitorLedger(p6_runtime.unit_of_work)
    policy = MonitorPolicy.create(enabled=True, extensions=(".strm",), exclude_paths=(), refresh_delay_seconds=0)
    ingress.record(policy.created("/fixture/series/ep2.strm", producer_key="pending-next", monitor_root="/fixture"))
    ready(p6_runtime)
    with pytest.raises(MediaEffectDeliveryRetryable):
        repository.snapshot(effect.aggregate_id)
    with p6_runtime.engine.begin() as c:
        c.execute(text("DELETE FROM ingestion_monitor_items"))
    ingress.record(policy.created("/fixture/other/ep2.strm", producer_key="pending-other", monitor_root="/fixture"))
    assert len(repository.snapshot(effect.aggregate_id)) == 1


@pytest.mark.parametrize("disposition", [ProviderDisposition.CONFIRMED, ProviderDisposition.INDETERMINATE])
def test_delivery_ledger_prevents_resend_even_after_restart(repository, p6_runtime, disposition):
    effect = repository.stage(member(), operation_key="send")
    repository.stage(member(2), operation_key="send2")
    ready(p6_runtime)
    bot = Mock()
    bot.send_photo.return_value = ProviderResponse(
        disposition, 200 if disposition is ProviderDisposition.CONFIRMED else None
    )
    settings = Settings(telegram_bot_token="fixture", telegram_notify_types=["library_new"])

    def executor():
        return PersistentMediaEffectExecutor(
            PostgresMediaEffectLedger(p6_runtime.unit_of_work),
            TelegramLibraryNotificationDelivery(
                settings=lambda: settings, telegram=bot, admin_chat_ids=lambda: ["1"], batches=repository
            ),
        )

    first = executor().execute(effect.id)
    second = executor().execute(effect.id)
    assert first.status == second.status
    assert first.status.value == disposition.value
    assert bot.send_photo.call_count == 1
    assert "S01E01–E02" in bot.send_photo.call_args.args[2]
    bot.send_message.assert_not_called()


def test_retry_success_updates_batch_and_reads_persisted_telegram_message_ids(repository, p6_runtime):
    effect = repository.stage({**member(218), "succeeded": False}, operation_key="retry-source")
    repository.stage({**member(219), "succeeded": False}, operation_key="other-source")
    ready(p6_runtime)
    bot = Mock()
    bot.send_photo.return_value = ProviderResponse(
        ProviderDisposition.CONFIRMED, 200, {"ok": True, "result": {"message_id": 77}}
    )
    settings = Settings(telegram_bot_token="fixture", telegram_notify_types=["library_new"])
    executor = PersistentMediaEffectExecutor(
        PostgresMediaEffectLedger(p6_runtime.unit_of_work),
        TelegramLibraryNotificationDelivery(
            settings=lambda: settings, telegram=bot, admin_chat_ids=lambda: ["1"], batches=repository
        ),
    )
    sent = executor.execute(effect.id)
    assert sent.status.value == "confirmed"

    updated = repository.mark_retry_success(library_notification_member_key("retry-source", "218"), 1)
    assert updated["updated"] is True
    assert updated["messages"] == [{"chat_id": "1", "message_id": 77, "kind": "caption"}]
    recovered = next(item for item in updated["members"] if item["item_id"] == "218")
    assert recovered["succeeded"] is True and recovered["retry_success_count"] == 1
    card = library_card(updated["members"])
    assert "已成功入库（第 1 次重刮成功）" in card["message"]
    assert "S01E219；请查看任务中心" in card["message"]
