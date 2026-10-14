import json

import pytest
from sqlalchemy import text

from etk.domains.media.effects import PostgresMediaEffectLedger
from etk.domains.media.metadata_retry import MetadataRetryScheduler
from etk.domains.media.repository import SqlAlchemyMediaRepository
from etk.domains.media.task_execution import SqlAlchemyMediaMetadataTaskRepository
from etk.platform.jobs import PostgresJobStore
from etk.platform.tasks import TaskOutcome


@pytest.fixture
def runtime(p5_runtime):
    with p5_runtime.engine.begin() as c:
        c.execute(text("TRUNCATE job_runs CASCADE"))
    yield p5_runtime
    with p5_runtime.engine.begin() as c:
        c.execute(text("TRUNCATE job_runs CASCADE"))


def make_job(runtime, item_id, *, name="主动入库: 测试剧 S01E03", status="succeeded", outcome=None):
    job = (
        PostgresJobStore(runtime.unit_of_work)
        .create_job(
            task_key="native-operation:media.reprocess-item",
            payload={"parameters": {"item_id": item_id}, "display_name": name},
        )
        .job
    )
    with runtime.engine.begin() as c:
        c.execute(
            text(
                "UPDATE job_runs SET status=:status,result_json=CAST(:result AS jsonb),finished_at=NOW() WHERE id=:id"
            ),
            {"status": status, "result": json.dumps(outcome), "id": job.id},
        )
        if status == "failed":
            c.execute(text("UPDATE job_runs SET result_json=NULL WHERE id=:id"), {"id": job.id})
            c.execute(
                text("UPDATE job_attempts SET error_details_json=CAST(:result AS jsonb) WHERE job_id=:id"),
                {"id": job.id, "result": json.dumps({"task_outcome": outcome})},
            )
    return job


def save_episode(runtime, *, title="第 3 集", overview="", path="/fixture/测试剧/S01E03.mp4"):
    repo = SqlAlchemyMediaMetadataTaskRepository(runtime.unit_of_work)
    item = {
        "Id": "3",
        "Type": "Episode",
        "Path": path,
        "_series_tmdb_id": "100",
        "_season_number": 1,
        "_episode_number": 3,
        "_preserve_cached_images": True,
    }
    details = {"id": 33, "name": title, "overview": overview, "season_number": 1, "episode_number": 3}
    assert repo.upsert_metadata(item, details, preserve_people=False)
    return repo, item


def test_existing_local_tasks_are_visible_with_incomplete_cache_and_bounded_pagination(runtime):
    save_episode(runtime)
    make_job(runtime, "3", outcome={"details": {"metadata": {"selected": 1, "verified": 1}}})
    make_job(runtime, "4", name="主动入库: 第二部剧", status="failed", outcome={"message": "网络失败"})
    repo = SqlAlchemyMediaRepository(runtime.unit_of_work)
    result = repo.list_scrape_records(page=1, per_page=1, search="", status="")
    assert result["total"] == 2 and len(result["items"]) == 1
    result = repo.list_scrape_records(page=1, per_page=25, search="测试剧", status="incomplete")
    assert result["total"] == 1
    row = result["items"][0]
    assert row["source"] == "本地入库" and row["item_id"] == "3"
    assert row["job_status"] == "succeeded" and row["status"] == "incomplete"
    assert not row["completeness_checked"]
    assert repo.list_scrape_records(page=1, per_page=25, search="' OR true --", status="")["total"] == 0
    assert repo.list_scrape_records(page=3, per_page=1, search="", status="")["items"] == []


def test_partial_attempt_keeps_missing_fields_and_scheduled_retry(runtime):
    save_episode(runtime, path="/fixture/测试剧/S01E03.strm")
    make_job(
        runtime,
        "3",
        status="failed",
        outcome={
            "status": "partial",
            "details": {
                "metadata": {"selected": 1, "verified": 1, "incomplete": [{"item_id": "3", "missing": ["中文简介"]}]},
                "metadata_retry": {"status": "scheduled", "items": []},
            },
        },
    )
    row = SqlAlchemyMediaRepository(runtime.unit_of_work).list_scrape_records(
        page=1, per_page=25, search="", status="incomplete"
    )["items"][0]
    assert row["source"] == "STRM 入库" and row["retry"]["status"] == "scheduled"
    assert row["incomplete"][0]["missing"] == ["中文简介"]
    assert row["completeness_checked"]


def test_incomplete_provider_does_not_erase_existing_good_metadata(runtime):
    repo, item = save_episode(runtime, title="已确认标题", overview="已确认中文简介")
    assert repo.upsert_metadata(item, {"id": 33, "name": "第 3 集", "overview": ""}, preserve_people=False)
    with runtime.engine.connect() as c:
        row = c.execute(text("SELECT title,overview FROM media_metadata WHERE tmdb_id='33'")).mappings().one()
    assert dict(row) == {"title": "已确认标题", "overview": "已确认中文简介"}


def test_delayed_retry_is_durable_idempotent_and_does_not_create_an_active_root(runtime):
    scheduler = MetadataRetryScheduler(PostgresMediaEffectLedger(runtime.unit_of_work))
    outcome = TaskOutcome.partial(details={"metadata": {"incomplete": [{"item_id": "3"}]}})
    first = scheduler(outcome, "fixture", 0)
    assert scheduler(outcome, "fixture", 0) == first
    with runtime.engine.connect() as c:
        assert c.scalar(text("SELECT count(*) FROM media_effects WHERE effect_type='media.metadata.retry'")) == 1
        assert c.scalar(text("SELECT count(*) FROM job_runs")) == 0
        assert c.scalar(
            text(
                "SELECT available_at > NOW() + interval '5 hours' FROM media_effects "
                "WHERE effect_type='media.metadata.retry'"
            )
        )
