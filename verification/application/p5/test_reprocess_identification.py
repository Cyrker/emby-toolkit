from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from etk.domains.media.reprocess import NativeMediaItemReprocessService
from etk.domains.media.reprocess_identification import ReprocessIdentificationService, selection_value
from etk.domains.media.reprocess_metadata import ReprocessIdentificationError
from etk.domains.operations.review_reprocess import (
    NativeReviewReprocessExecutor,
    PersistentReviewJobGateway,
    ReviewReprocessService,
)
from etk.platform.jobs import IdempotencyConflictError
from etk.platform.tasks import TaskOutcome

JOB = "00000000-0000-0000-0000-000000000123"


@pytest.fixture
def fixture():
    item = {"Id": "123", "Name": "同名作品", "ProductionYear": 2020, "Type": "Movie", "ProviderIds": {}}
    source = {"id": JOB, "status": "failed", "parameters": {},
              "outcome": {"details": {"error_code": "TMDB_AMBIGUOUS"}}}
    rows = [{"id": 1, "title": "同名作品", "original_title": "Original One", "release_date": "2020-01-01"},
            {"id": 2, "title": "同名作品", "original_title": "Original Two", "release_date": "2020-02-01"}]
    catalog = SimpleNamespace(item=Mock(side_effect=lambda identifier: item if identifier == "123" else None),
                              descendants=Mock(return_value=[]))
    provider = SimpleNamespace(
        search=Mock(return_value={"results": rows, "total_pages": 2}),
        details=Mock(side_effect=lambda identifier, _: next(r for r in rows if str(r["id"]) == identifier)),
    )
    repo = SimpleNamespace(source=Mock(return_value=source), bindings=Mock(return_value=[]))
    service = ReprocessIdentificationService(catalog, provider, repo)
    return SimpleNamespace(
        item=item, source=source, rows=rows, catalog=catalog, provider=provider, repo=repo, service=service,
    )


def choice(f):
    return {"tmdb_id": "2", "media_type": "movie", "snapshot": f.service.preview("123", job_id=JOB)["snapshot"]}


def test_no_cache_or_saved_path_is_required_for_read_only_search(fixture):
    f = fixture
    before = deepcopy(f.item)
    preview = f.service.preview("123", job_id=JOB)
    assert preview["requires_selection"] and preview["target"]["path"] == ""
    assert [row["tmdb_id"] for row in preview["candidates"]] == ["1", "2"]
    assert preview["candidates"][1]["original_title"] == "Original Two"
    assert f.item == before
    assert not f.repo.bindings.called
    f.service.preview("123", job_id=JOB, query="新关键词", year=2021, page=2)
    f.provider.search.assert_called_with("新关键词", "movie", year=2021, page=2)


def test_context_can_open_before_search_and_source_must_be_finished(fixture):
    f = fixture
    assert f.service.preview("123", context_only=True)["requires_selection"]
    f.provider.search.assert_not_called()
    f.source["status"] = "running"
    with pytest.raises(ReprocessIdentificationError, match="尚未结束"):
        f.service.preview("123", context_only=True)


def test_episode_search_uses_current_root_series_identity(fixture):
    f = fixture
    root = {**f.item, "Id": "series", "Type": "Series", "Name": "所属电视剧"}
    episode = {**f.item, "Type": "Episode", "SeriesId": "series", "Name": "第 1 集"}
    f.catalog.item.side_effect = lambda identifier: root if identifier == "series" else episode
    result = f.service.preview("123")
    assert result["target"]["root_item_id"] == "series"
    assert result["target"]["media_type"] == "tv"
    f.provider.search.assert_called_with("所属电视剧", "tv", year=2020, page=1)


def test_legacy_ambiguous_tasks_are_rechecked_but_other_failures_are_unchanged(fixture):
    f = fixture
    f.source["outcome"] = {"message": "TMDb 没有唯一的片名及年份匹配；请人工确认并填写 ID"}
    assert f.service.preview("123")["requires_selection"]
    f.provider.search.return_value = {"results": [f.rows[0]], "total_pages": 1}
    assert f.service.preview("123") == {"requires_selection": False}
    f.source["outcome"] = {"details": {"error_code": "TMDB_NO_MATCH"}}
    f.catalog.item.reset_mock()
    assert f.service.preview("123") == {"requires_selection": False}
    f.catalog.item.assert_not_called()


@pytest.mark.parametrize("change", ["bound", "parent", "year", "type"])
def test_changed_identity_is_rejected_before_writes(fixture, change):
    f = fixture
    selected = choice(f)
    if change == "bound":
        f.item["ProviderIds"] = {"Tmdb": "99"}
    elif change == "parent":
        f.item["SeriesId"] = "different"
    elif change == "year":
        f.item["ProductionYear"] = 2021
    else:
        f.item["Type"] = "Series"
    with pytest.raises(ReprocessIdentificationError, match="变化"):
        f.service.validate("123", JOB, selected)


def test_wrong_type_invalid_manual_id_and_cache_reassignment_are_rejected(fixture):
    f = fixture
    selected = choice(f)
    with pytest.raises(ReprocessIdentificationError, match="类型"):
        f.service.validate("123", JOB, {**selected, "media_type": "tv"})
    with pytest.raises(ReprocessIdentificationError, match="正整数"):
        f.service.preview("123", manual_id="-1")
    f.repo.bindings.return_value = [{"tmdb_id": "99", "item_type": "Movie"}]
    with pytest.raises(ReprocessIdentificationError, match="缓存"):
        f.service.validate("123", JOB, selected)


def test_current_emby_identity_can_continue_normal_retry_and_missing_target_is_explicit(fixture):
    f = fixture
    f.item["ProviderIds"] = {"Tmdb": "2"}
    assert f.service.preview("123") == {"requires_selection": False}
    f.item["ProviderIds"] = {}
    f.catalog.item.return_value = None
    f.catalog.item.side_effect = None
    with pytest.raises(ReprocessIdentificationError, match="不存在"):
        f.service.preview("123")


def test_previous_choice_is_retained_after_later_failure(fixture):
    f = fixture
    f.source["parameters"]["tmdb_selection"] = choice(f)
    f.source["outcome"] = {"message": "后续刮削失败"}
    assert f.service.preview("123")["requires_selection"]
    f.item["ProviderIds"] = {"Tmdb": "9"}
    with pytest.raises(ReprocessIdentificationError, match="其他身份"):
        f.service.preview("123")


def test_selection_runs_original_metadata_refresh_and_readback_without_auto_identification(fixture):
    f = fixture
    f.item["Path"] = "/fixture/movie.mkv"
    selected = choice(f)
    metadata = SimpleNamespace(
        identify_reprocess_root=Mock(side_effect=AssertionError("不得再次自动识别")),
        cached_reprocess_metadata=Mock(return_value=None),
        prepare_reprocess_metadata=Mock(return_value={"Name": "所选作品", "Overview": "简介", "_tmdb_id": "2"}),
    )
    def refresh(item, _):
        f.item.update(Name="所选作品", Overview="简介", ProviderIds={"Tmdb": "2"})
        return TaskOutcome.succeeded()
    mediainfo = SimpleNamespace(execute=Mock())
    service = NativeMediaItemReprocessService(f.catalog, metadata, mediainfo, refresh=refresh, identification=f.service)
    progress = SimpleNamespace(checkpoint=lambda: None, update=lambda *_: None)
    outcome = service.execute("123", progress, operation_key="fixture", only_missing=True,
                              tmdb_selection=selected, source_job_id=JOB)
    assert outcome.status.value == "succeeded"
    assert outcome.details["metadata"]["verified"] == 1
    assert metadata.prepare_reprocess_metadata.call_args.args[1]["ProviderIds"]["Tmdb"] == "2"
    mediainfo.execute.assert_not_called()


def test_identity_change_during_preparation_is_not_overwritten_by_refresh(fixture):
    f = fixture
    selected = choice(f)
    def prepare(*_):
        f.item["ProviderIds"] = {"Tmdb": "999"}
        return {"Name": "作品", "_tmdb_id": "2"}
    metadata = SimpleNamespace(cached_reprocess_metadata=Mock(return_value=None), prepare_reprocess_metadata=prepare)
    refresh = Mock()
    service = NativeMediaItemReprocessService(
        f.catalog, metadata, SimpleNamespace(execute=Mock()), refresh=refresh, identification=f.service,
    )
    outcome = service.execute("123", SimpleNamespace(checkpoint=lambda: None, update=lambda *_: None),
                              operation_key="fixture", only_missing=True, tmdb_selection=selected, source_job_id=JOB)
    refresh.assert_not_called()
    assert outcome.status.value != "succeeded"
    assert "身份已变化" in outcome.details["metadata"]["failures"][0]["message"]


def test_submission_validates_choice_and_executor_keeps_it(fixture):
    f = fixture
    selected = choice(f)
    submitted = SimpleNamespace(projection=SimpleNamespace(job=SimpleNamespace(id="new-job")), reused=False)
    jobs = SimpleNamespace(replay_one=Mock(return_value=None), submit_one=Mock(return_value=submitted))
    service = ReviewReprocessService(SimpleNamespace(item=Mock()), jobs, identification=f.service)
    payload = {"source_job_id": JOB, "tmdb_selection": selected, "only_missing": True}
    result = service.submit_one("123", payload, requested_by_user_id="admin", trace_id=None, idempotency_key="request")
    assert result.status_code == 202
    values = jobs.submit_one.call_args.kwargs
    assert values["tmdb_selection"] == selected and values["source_job_id"] == JOB
    executor_target = SimpleNamespace(execute=Mock(return_value=TaskOutcome.failed("后续失败")))
    NativeReviewReprocessExecutor(f.repo, executor_target).execute(
        "p8-review-reprocess-one", values, object(), operation_key="retry",
    )
    assert executor_target.execute.call_args.kwargs["tmdb_selection"] == selected


def test_same_submission_replays_even_if_emby_has_changed_and_different_choice_conflicts():
    selected = {"tmdb_id": "2", "media_type": "movie", "snapshot": "a" * 64}
    intent = {"tmdb_selection": selected, "source_job_id": JOB, "only_missing": True}
    projection = SimpleNamespace(job=SimpleNamespace(request_payload={"parameters": intent}))
    store = SimpleNamespace(get_idempotent_job=Mock(return_value=projection))
    gateway = PersistentReviewJobGateway(SimpleNamespace(store=store))
    assert gateway.replay_one("123", "same", intent).reused
    with pytest.raises(IdempotencyConflictError):
        gateway.replay_one("123", "same", {**intent, "tmdb_selection": {**selected, "tmdb_id": "1"}})


@pytest.mark.parametrize("bad", [None, {}, {"tmdb_id": "1"},
                                {"tmdb_id": "-1", "media_type": "movie", "snapshot": "a" * 64},
                                {"tmdb_id": "1", "media_type": "tv", "snapshot": "bad"}])
def test_selection_schema_is_strict(bad):
    with pytest.raises(ReprocessIdentificationError):
        selection_value(bad)
