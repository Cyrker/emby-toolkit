from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest

from etk.domains.media.effect_execution import MediaEffectDeliveryRejected, MediaEffectDeliveryRetryable
from etk.domains.media.metadata_quality import metadata_gaps
from etk.domains.media.metadata_retry import MetadataRetryDelivery, MetadataRetryScheduler
from etk.domains.media.reprocess import NativeMediaItemReprocessOperation, NativeMediaItemReprocessService
from etk.domains.media.reprocess_metadata import prepare_metadata
from etk.domains.media.task_execution import NativeMediaMetadataTaskService
from etk.domains.operations.jobs import TaskBusyError
from etk.platform.tasks import TaskOutcome


class Provider:
    def __init__(self, detail=None):
        self.detail = detail
        self.calls = 0

    def season_details(self, *_):
        return {
            "id": 2,
            "season_number": 1,
            "name": "第一季",
            "episodes": [{"id": 3, "season_number": 1, "episode_number": 3, "name": "第 3 集", "overview": ""}],
        }

    def episode_details(self, *_):
        self.calls += 1
        return self.detail or self.season_details()["episodes"][0]


ROOT = {"Id": "1", "Type": "Series", "ProviderIds": {"Tmdb": "100"}}
ITEM = {
    "Id": "3",
    "Type": "Episode",
    "SeriesId": "1",
    "Name": "第 3 集",
    "Path": "/fixture/show/S01E03.mp4",
    "ParentIndexNumber": 1,
    "IndexNumber": 3,
}


@pytest.mark.parametrize("title", ["第 3 集", "Episode 3", "EP03", ""])
def test_placeholder_and_blank_overview_are_incomplete(title):
    assert metadata_gaps("Episode", title, " ") == ["分集标题", "中文简介"]
    assert metadata_gaps("Season", "第 1 季", "") == []


def test_episode_detail_fills_stale_season_and_rejects_wrong_identity():
    provider = Provider({"id": 3, "season_number": 1, "episode_number": 3, "name": "正式标题", "overview": "中文剧情"})
    _, data = prepare_metadata(ITEM, ROOT, provider, {})
    assert data["name"] == "正式标题" and data["overview"] == "中文剧情"
    assert provider.calls == 1
    provider.detail["id"] = 99
    with pytest.raises(ValueError, match="分集身份"):
        prepare_metadata(ITEM, ROOT, provider, {})


def test_missing_source_is_partial_after_real_readback_and_incomplete_cache_is_refetched():
    provider = Provider()
    saved = []
    repo = SimpleNamespace(
        upsert_metadata=lambda item, data, **_: saved.append(data) or True,
        reprocess_cache=lambda *_: {"title": "第 3 集", "overview": "已有简介"},
    )
    catalog = SimpleNamespace(item=lambda id: ROOT if id == "1" else {**ITEM, "ProviderIds": {"Tmdb": "3"}})
    metadata = NativeMediaMetadataTaskService(repo, catalog, provider)
    assert metadata.cached_reprocess_metadata(ITEM, ROOT) is None
    messages = []
    progress = SimpleNamespace(checkpoint=lambda: None, update=lambda _, message: messages.append(message))
    service = NativeMediaItemReprocessService(catalog, metadata, None, refresh=lambda *_: TaskOutcome.succeeded())
    outcome = service.execute("3", progress, operation_key="fixture", restore_media_info=False, only_missing=True)
    assert outcome.status.value == "partial"
    assert outcome.details["metadata"]["verified"] == 1
    assert outcome.details["metadata"]["incomplete"][0]["missing"] == ["分集标题", "中文简介"]
    assert any("资料待补齐" in m for m in messages)
    assert not any("标题、简介已就绪" in m for m in messages)


def test_retry_schedule_is_bounded_and_excludes_unconfirmed_readbacks():
    calls = []

    def reserve(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(effect=SimpleNamespace(id=uuid4(), available_at=kwargs["available_at"]))

    scheduler = MetadataRetryScheduler(SimpleNamespace(reserve=reserve))
    outcome = TaskOutcome.partial(details={"metadata": {"incomplete": [{"item_id": "3"}], "failures": []}})
    for count, hours in enumerate((6, 24, 48)):
        result = scheduler(outcome, f"job-{count}", count)
        assert result["status"] == "scheduled"
        assert abs((calls[-1]["available_at"] - datetime.now(UTC)).total_seconds() - hours * 3600) < 5
        assert calls[-1]["payload"]["retry_count"] == count + 1
    assert scheduler(outcome, "job-3", 3)["status"] == "exhausted"
    outcome.details["metadata"]["failures"].append({"item_id": "3"})
    assert scheduler(outcome, "job-4", 0)["status"] == "not_needed"
    assert len(calls) == 3


def test_retry_delivery_is_idempotent_and_never_replays_media_streams_or_notifications():
    calls = []
    job_id = uuid4()

    def submit(*args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(projection=SimpleNamespace(job=SimpleNamespace(id=job_id)), reused=False)

    jobs = SimpleNamespace(get_idempotent=lambda **_: None, submit_native_operation=submit)
    delivery = MetadataRetryDelivery(jobs)
    effect = SimpleNamespace(id=uuid4(), payload={"item_id": "3", "retry_count": 1}, attempts=1)
    assert delivery.deliver(effect)["job_id"] == str(job_id)
    assert calls[0][0][1] == {
        "item_id": "3",
        "restore_media_info": False,
        "only_missing": True,
        "notify_library": False,
        "metadata_retry_count": 1,
    }
    jobs.get_idempotent = lambda **_: SimpleNamespace(job=SimpleNamespace(id=job_id))
    assert delivery.deliver(effect)["reused"]
    assert len(calls) == 1
    jobs.get_idempotent = lambda **_: None

    def busy(*_, **__):
        raise TaskBusyError("busy")

    jobs.submit_native_operation = busy
    with pytest.raises(MediaEffectDeliveryRetryable):
        delivery.deliver(effect)
    effect.attempts = 24
    with pytest.raises(MediaEffectDeliveryRejected):
        delivery.deliver(effect)


def test_operation_records_scheduled_retry_and_retains_partial_status():
    outcome = TaskOutcome.partial(details={"metadata": {"incomplete": [{"item_id": "3"}]}})
    operation = NativeMediaItemReprocessOperation(
        SimpleNamespace(execute=lambda *_a, **_k: outcome),
        schedule_retry=lambda *_: {"status": "scheduled", "items": []},
    )
    result = operation.execute({"item_id": "3"}, SimpleNamespace(checkpoint=lambda: None, update=lambda *_: None))
    assert result["status"] == "partial"
    assert result["details"]["metadata_retry"]["status"] == "scheduled"
