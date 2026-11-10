import json
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from etk.domains.media.repository import SqlAlchemyMediaRepository
from etk.domains.media.reprocess_identification import SqlAlchemyIdentificationRepository
from etk.domains.media.reprocess_metadata import ReprocessIdentificationError
from etk.domains.media.task_execution import SqlAlchemyMediaMetadataTaskRepository
from etk.domains.operations.review_reprocess import PersistentReviewJobGateway
from etk.platform.jobs import PostgresJobStore


@pytest.fixture
def runtime(p5_runtime):
    with p5_runtime.engine.begin() as c:
        c.execute(text("TRUNCATE job_runs CASCADE"))
    yield p5_runtime
    with p5_runtime.engine.begin() as c:
        c.execute(text("TRUNCATE job_runs CASCADE"))


def test_history_without_cache_and_selection_roundtrip_are_durable(runtime):
    store = PostgresJobStore(runtime.unit_of_work)
    old = store.create_job(task_key="native-operation:media.reprocess-item",
                           payload={"parameters": {"item_id": "123"}, "display_name": "补齐缺失元数据"}).job
    with runtime.engine.begin() as c:
        c.execute(text("UPDATE job_runs SET status='failed',finished_at=NOW() WHERE id=:id"), {"id": old.id})
        c.execute(text("UPDATE job_attempts SET error_details_json=CAST(:outcome AS jsonb) WHERE job_id=:id"), {
            "id": old.id, "outcome": json.dumps({"task_outcome": {
                "message": "TMDb 候选过多", "details": {"error_code": "TMDB_AMBIGUOUS"},
            }}),
        })
    repo = SqlAlchemyIdentificationRepository(runtime.unit_of_work)
    source = repo.source("123", str(old.id))
    assert source["outcome"]["details"]["error_code"] == "TMDB_AMBIGUOUS"
    assert repo.source("123", None)["id"] == old.id
    assert repo.source("different", str(old.id)) is None
    assert not repo.bindings({"Id": "123"})
    selection = {"tmdb_id": "2", "media_type": "movie", "snapshot": "a" * 64}
    gateway = PersistentReviewJobGateway(SimpleNamespace(store=store))
    values = dict(item_id="123", item_name="作品", reason=None, only_missing=True,
                  source_job_id=str(old.id), tmdb_selection=selection,
                  requested_by_user_id=None, trace_id=None, idempotency_key="same-confirmation")
    first = gateway.submit_one(**values)
    replay = gateway.submit_one(**values)
    assert replay.reused and replay.projection.job.id == first.projection.job.id
    assert first.projection.job.request_payload["parameters"]["tmdb_selection"] == selection
    assert gateway.replay_one("123", "same-confirmation", {
        "tmdb_selection": selection, "source_job_id": str(old.id), "only_missing": True,
    }).projection.job.id == first.projection.job.id
    records = SqlAlchemyMediaRepository(runtime.unit_of_work).list_scrape_records(
        page=1, per_page=25, search="", status="",
    )["items"]
    assert records[0]["tmdb_selection"] == selection
    assert records[1]["error_code"] == "TMDB_AMBIGUOUS" and records[1]["path"] == ""


def test_confirmed_choice_does_not_reassign_existing_cache_or_path(runtime):
    repo = SqlAlchemyMediaMetadataTaskRepository(runtime.unit_of_work)
    item = {"Id": "123", "Type": "Movie", "Path": "/fixture/movie.mkv", "ProviderIds": {"Tmdb": "1"}}
    assert repo.upsert_metadata(item, {"id": 1, "title": "已有作品"}, preserve_people=False)
    for identifier in ("123", "456"):
        changed = {**item, "Id": identifier, "ProviderIds": {"Tmdb": "2"}, "_confirmed_root_tmdb_id": "2"}
        with pytest.raises(ReprocessIdentificationError, match="缓存"):
            repo.upsert_metadata(changed, {"id": 2, "title": "其他作品"}, preserve_people=False)
    with runtime.engine.connect() as c:
        assert c.scalar(text("SELECT count(*) FROM media_metadata")) == 1
        assert c.scalar(text("SELECT title FROM media_metadata")) == "已有作品"


def test_historical_item_id_can_be_recovered_from_failed_outcome(runtime):
    store = PostgresJobStore(runtime.unit_of_work)
    old = store.create_job(task_key="p8-review-reprocess-one", payload={"parameters": {}}).job
    with runtime.engine.begin() as c:
        c.execute(text("UPDATE job_runs SET status='failed',finished_at=NOW() WHERE id=:id"), {"id": old.id})
        c.execute(text("UPDATE job_attempts SET error_details_json=CAST(:outcome AS jsonb) WHERE job_id=:id"), {
            "id": old.id, "outcome": json.dumps({"task_outcome": {
                "message": "TMDb 候选过多", "details": {"item_id": "123"},
            }}),
        })
    assert SqlAlchemyIdentificationRepository(runtime.unit_of_work).source("123", str(old.id))["id"] == old.id
