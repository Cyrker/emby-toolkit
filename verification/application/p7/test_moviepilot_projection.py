from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from etk.domains.p115.moviepilot_projection import NativeMoviePilotStrmProjection, _record_category, _relative_path
from etk.domains.p115.sorting_rules import P115SortingRulesSnapshot
from etk.domains.p115.strm_adapters import AtomicP115LocalStrmLibraryGateway
from etk.platform import Settings


def event() -> dict:
    return dict(
        file_id="101",
        account_id="100",
        parent_id="102",
        file_name="剧集.S01E01.mkv",
        path="/Movie/媒体库/电视剧/剧集/Season 01/剧集.S01E01.mkv",
        sha1="A" * 40,
        pick_code="fixture-pc",
        size=1024,
        tmdb_id="123",
        media_type="tv",
        season_number=1,
    )


def projection(tmp_path: Path, *, account_id="100"):
    settings = Settings(
        local_strm_root=str(tmp_path), etk_server_url="http://etk.example", p115_media_root_name="媒体库"
    )
    repository = Mock()
    ingress = Mock()
    remote = Mock()
    sorting_rules = Mock()
    sorting_rules.load.return_value = P115SortingRulesSnapshot(
        rules=({"cid": "200", "category_path": "电视剧", "enabled": True},), account_id=account_id
    )
    service = NativeMoviePilotStrmProjection(
        settings=lambda: settings,
        repository=repository,
        records=repository,
        sorting_rules=sorting_rules,
        ingestion=ingress,
        remote=remote,
        libraries=AtomicP115LocalStrmLibraryGateway(),
        coordinator=SimpleNamespace(hold=lambda scope: nullcontext()),
    )
    return service, repository, ingress, remote


def test_single_file_preserves_path_caches_before_write_and_retries_idempotently(tmp_path):
    service, repository, ingress, remote = projection(tmp_path)
    target = tmp_path / "电视剧/剧集/Season 01/剧集.S01E01.strm"
    order = []
    repository.persist_remote_file.side_effect = lambda **kw: order.append(("cache", target.exists()))
    def record_ingestion(*args, **kwargs):
        order.append(("ingestion", target.exists()))
        return ("recorded",)

    ingress.record_event.side_effect = record_ingestion
    repository.add_missing_record.side_effect = lambda record: order.append(("record", target.exists()))
    first = service.project(file=event(), event_key="a" * 64, assert_current=lambda: None)
    stamp = target.stat().st_mtime_ns
    second = service.project(file=event(), event_key="a" * 64, assert_current=lambda: None)
    assert first["changed"] is True and second["changed"] is False
    assert target.stat().st_mtime_ns == stamp
    assert order == [
        ("cache", False), ("ingestion", True), ("record", True),
        ("cache", True), ("ingestion", True), ("record", True),
    ]
    assert repository.add_missing_record.call_args.args[0] == {
        "file_id": "101", "pick_code": "fixture-pc", "original_name": "剧集.S01E01.mkv",
        "tmdb_id": "123", "media_type": "tv", "season_number": 1,
        "target_cid": "200", "category_name": "MP直出/电视剧",
    }
    assert ingress.record_event.call_args_list[0] == ingress.record_event.call_args_list[1]
    assert repository.persist_remote_file.call_args.kwargs["local_path"].endswith(".mkv")
    assert target.read_text().startswith("http://etk.example/api/p115/play/fixture-pc/")
    assert list(tmp_path.rglob("*.strm")) == [target]
    remote.open.assert_not_called()


def test_retry_recovers_after_file_write_before_ingestion_commit(tmp_path):
    service, repository, ingress, _ = projection(tmp_path)
    ingress.record_event.side_effect = [RuntimeError("database unavailable"), ("recorded",)]
    with pytest.raises(RuntimeError, match="database unavailable"):
        service.project(file=event(), event_key="a" * 64, assert_current=lambda: None)
    repository.add_missing_record.assert_not_called()
    result = service.project(file=event(), event_key="a" * 64, assert_current=lambda: None)
    assert result["changed"] is False
    assert result["ingestion_submitted"] is True
    repository.add_missing_record.assert_called_once()


def test_record_failure_is_retryable_without_rewriting_strm(tmp_path):
    service, repository, ingress, _ = projection(tmp_path)
    repository.add_missing_record.side_effect = [RuntimeError("record write failed"), True]
    with pytest.raises(RuntimeError, match="record write failed"):
        service.project(file=event(), event_key="a" * 64, assert_current=lambda: None)
    target = next(tmp_path.rglob("*.strm"))
    stamp = target.stat().st_mtime_ns
    result = service.project(file=event(), event_key="a" * 64, assert_current=lambda: None)
    assert result["changed"] is False
    assert target.stat().st_mtime_ns == stamp
    assert ingress.record_event.call_args_list[0] == ingress.record_event.call_args_list[1]
    assert repository.add_missing_record.call_count == 2


def test_skipped_ingestion_does_not_create_success_record(tmp_path):
    service, repository, ingress, _ = projection(tmp_path)
    ingress.record_event.return_value = ()
    result = service.project(file=event(), event_key="a" * 64, assert_current=lambda: None)
    assert result["ingestion_submitted"] is False
    repository.add_missing_record.assert_not_called()


@pytest.mark.parametrize("category_path", ["电视剧其他", "电影"])
def test_unmatched_category_is_not_replaced_by_parent_directory(tmp_path, category_path):
    service, repository, _, _ = projection(tmp_path)
    file = {**event(), "path": event()["path"].replace("电视剧/", f"{category_path}/")}
    service.project(file=file, event_key="a" * 64, assert_current=lambda: None)
    record = repository.add_missing_record.call_args.args[0]
    assert record["target_cid"] is None
    assert record["category_name"] == "MP直出/未匹配分类"


def test_category_matching_uses_longest_enabled_path_without_ambiguous_fallback():
    rules = P115SortingRulesSnapshot(rules=(
        {"cid": "200", "category_path": "媒体库/电视剧"},
        {"cid": "201", "category_path": "电视剧/剧集"},
        {"cid": "202", "category_path": "电视剧/剧集/Season 01", "enabled": False},
    ))
    relative = "电视剧/剧集/Season 01/剧集.S01E01.mkv"
    assert _record_category(relative, rules, "媒体库") == ("201", "MP直出/电视剧/剧集")
    ambiguous = P115SortingRulesSnapshot(rules=rules.rules + ({"cid": "203", "category_path": "电视剧/剧集"},))
    assert _record_category(relative, ambiguous, "媒体库") == (None, "MP直出/未匹配分类")


def test_other_account_rules_are_rejected_before_local_effects(tmp_path):
    service, repository, ingress, remote = projection(tmp_path, account_id="other-account")
    with pytest.raises(RuntimeError, match="另一个 115 账号"):
        service.project(file=event(), event_key="a" * 64, assert_current=lambda: None)
    repository.persist_remote_file.assert_not_called()
    repository.add_missing_record.assert_not_called()
    ingress.record_event.assert_not_called()
    remote.open.assert_not_called()


@pytest.mark.parametrize(
    "path",
    [
        "/Movie/媒体库/../剧集.S01E01.mkv",
        "/Movie/别的库/剧集.S01E01.mkv",
        "/Movie/媒体库/媒体库/剧集.S01E01.mkv",
        "/Movie/媒体库/other.mkv",
        "relative/媒体库/剧集.S01E01.mkv",
        "/Movie/媒体库//剧集.S01E01.mkv",
    ],
)
def test_invalid_mapping_is_rejected(path):
    with pytest.raises(ValueError):
        _relative_path({**event(), "path": path}, "媒体库")


def test_existing_unrelated_file_and_symlink_are_not_overwritten(tmp_path):
    service, repository, ingress, _ = projection(tmp_path)
    target = tmp_path / "电视剧/剧集/Season 01/剧集.S01E01.strm"
    target.parent.mkdir(parents=True)
    target.write_text("unrelated")
    with pytest.raises(ValueError, match="拒绝覆盖"):
        service.project(file=event(), event_key="a" * 64, assert_current=lambda: None)
    assert target.read_text() == "unrelated"
    target.unlink()
    target.symlink_to(tmp_path / "outside")
    with pytest.raises(RuntimeError):
        service.project(file=event(), event_key="a" * 64, assert_current=lambda: None)
    repository.persist_remote_file.assert_not_called()
    repository.add_missing_record.assert_not_called()
    ingress.record_event.assert_not_called()


def test_credential_drift_has_no_local_effects(tmp_path):
    service, repository, ingress, _ = projection(tmp_path)

    def reject():
        raise RuntimeError("credential changed")

    with pytest.raises(RuntimeError, match="credential changed"):
        service.project(file=event(), event_key="a" * 64, assert_current=reject)
    repository.persist_remote_file.assert_not_called()
    repository.add_missing_record.assert_not_called()
    ingress.record_event.assert_not_called()
    assert not list(tmp_path.rglob("*.strm"))


def test_subtitle_only_downloads_target_without_video_ingestion(tmp_path):
    service, repository, ingress, remote = projection(tmp_path)
    remote.open.return_value.download_subtitle.return_value = b"subtitle"
    file = {**event(), "file_name": "剧集.S01E01.srt", "path": event()["path"].replace(".mkv", ".srt")}
    service.project(file=file, event_key="a" * 64, assert_current=lambda: None)
    service.project(file=file, event_key="a" * 64, assert_current=lambda: None)
    assert (tmp_path / "电视剧/剧集/Season 01/剧集.S01E01.srt").read_bytes() == b"subtitle"
    assert remote.open.return_value.download_subtitle.call_count == 1
    ingress.record_event.assert_not_called()
    assert repository.persist_remote_file.call_count == 2
    assert repository.add_missing_record.call_count == 2
    assert repository.add_missing_record.call_args.args[0]["original_name"].endswith(".srt")


def test_failed_subtitle_download_has_no_success_record(tmp_path):
    service, repository, _, remote = projection(tmp_path)
    remote.open.return_value.download_subtitle.side_effect = RuntimeError("download failed")
    file = {**event(), "file_name": "剧集.S01E01.srt", "path": event()["path"].replace(".mkv", ".srt")}
    with pytest.raises(RuntimeError, match="download failed"):
        service.project(file=file, event_key="a" * 64, assert_current=lambda: None)
    repository.add_missing_record.assert_not_called()


def test_unsupported_file_does_not_create_record(tmp_path):
    service, repository, ingress, _ = projection(tmp_path)
    file = {**event(), "file_name": "剧集.nfo", "path": event()["path"].replace("剧集.S01E01.mkv", "剧集.nfo")}
    result = service.project(file=file, event_key="a" * 64, assert_current=lambda: None)
    assert result == {"skipped": True, "reason": "unsupported_extension"}
    repository.add_missing_record.assert_not_called()
    ingress.record_event.assert_not_called()
