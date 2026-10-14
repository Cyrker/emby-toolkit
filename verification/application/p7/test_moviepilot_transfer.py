from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock
from uuid import UUID

import pytest

from etk.domains.p115.media_probe import P115MediaProbeResult, P115MediaProbeStatus
from etk.domains.p115.models import P115CredentialSnapshot, P115Result
from etk.domains.subscription.moviepilot_transfer import (
    NativeMoviePilotTransferProcessor,
    moviepilot_credential_context,
)

_EVENT_KEY = "a" * 64
_PLAN_ID = UUID("11111111-1111-1111-1111-111111111111")
_EXECUTION_ID = UUID("22222222-2222-2222-2222-222222222222")
_JOB_ID = UUID("33333333-3333-3333-3333-333333333333")
_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_CREDENTIAL = P115CredentialSnapshot(
    account_id="115001",
    access_token="fixture-openapi-token",
)


class FakeCredentials:
    def __init__(self, snapshots: Sequence[P115CredentialSnapshot] | None = None) -> None:
        self.snapshots = tuple(snapshots or (_DEFAULT_CREDENTIAL,))
        self.calls = 0

    def load(self) -> P115CredentialSnapshot:
        value = self.snapshots[min(self.calls, len(self.snapshots) - 1)]
        self.calls += 1
        return value


class FakeRemoteClient:
    account_id = "115001"

    def __init__(
        self,
        children: Sequence[Mapping[str, Any]],
        *,
        file_info_item: Mapping[str, Any] | None = None,
    ) -> None:
        self.children = tuple(dict(value) for value in children)
        self.file_info_item = dict(file_info_item) if file_info_item is not None else None
        self.list_calls: list[str] = []
        self.info_calls: list[str] = []

    def list_children(self, parent_id: str) -> Sequence[Mapping[str, Any]]:
        self.list_calls.append(parent_id)
        return self.children

    def file_info(self, file_id: str) -> Mapping[str, Any]:
        self.info_calls.append(file_id)
        if self.file_info_item is not None:
            return dict(self.file_info_item)
        return next((value for value in self.children if value.get("fid") == file_id), {})


class FakeRemote:
    def __init__(self, client: FakeRemoteClient) -> None:
        self.client = client
        self.open_calls: list[dict[str, Any]] = []

    def open(self, **kwargs: Any) -> FakeRemoteClient:
        self.open_calls.append(dict(kwargs))
        return self.client


class FakeMediaInfo:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, str, object]] = []

    def enrich(self, request: Any, *, operation_key: str, progress: object) -> P115MediaProbeResult:
        self.calls.append((request, operation_key, progress))
        return P115MediaProbeResult(P115MediaProbeStatus.CONFIRMED, "confirmed")


class FakePlans:
    def __init__(self, *, ready: bool = True) -> None:
        self.ready = ready
        self.calls: list[dict[str, Any]] = []

    def create(self, raw_payload: object) -> P115Result:
        assert isinstance(raw_payload, Mapping)
        self.calls.append(dict(raw_payload))
        return P115Result(
            {
                "success": True,
                "data": {
                    "status": "ready" if self.ready else "blocked",
                    "plan_id": str(_PLAN_ID),
                    "errors": [] if self.ready else [{"code": "FIXTURE_BLOCKED"}],
                },
            }
        )


class FakeExecutions:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def prepare(
        self,
        plan_id: str,
        approved_intent_id: str,
        *,
        requested_by_user_id: str | None = None,
        trace_id: str | None = None,
    ) -> Any:
        del requested_by_user_id
        self.calls.append((plan_id, approved_intent_id, trace_id or ""))
        return SimpleNamespace(
            created=True,
            execution=SimpleNamespace(
                execution_id=_EXECUTION_ID,
                plan_id=_PLAN_ID,
                job_id=_JOB_ID,
            ),
        )


class FakeExecutionRepository:
    def __init__(self, existing: Any = None) -> None:
        self.existing = existing
        self.calls: list[str] = []

    def find_by_intent(self, approved_intent_id: str) -> Any:
        self.calls.append(approved_intent_id)
        return self.existing


class FakeJobs:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any], dict[str, Any]]] = []

    def submit_task(self, task_key: str, parameters: Mapping[str, Any], **kwargs: Any) -> Any:
        self.calls.append((task_key, dict(parameters), dict(kwargs)))
        return SimpleNamespace(
            reused=False,
            projection=SimpleNamespace(job=SimpleNamespace(id=_JOB_ID)),
        )


def _file(
    file_id: str,
    name: str,
    *,
    sha1: str,
    pick_code: str,
) -> dict[str, Any]:
    return {
        "fid": file_id,
        "fn": name,
        "sha1": sha1,
        "pc": pick_code,
        "fs": 20 * 1024 * 1024,
    }


def _event_file(
    credential: P115CredentialSnapshot = _DEFAULT_CREDENTIAL,
) -> dict[str, Any]:
    return {
        "file_id": "video",
        "parent_id": "source-root",
        "tmdb_id": "100",
        "title": "Fixture",
        "media_type": "tv",
        "season_number": 1,
        "episode_number": 2,
        **moviepilot_credential_context(credential, "openapi"),
    }


def _processor(
    *,
    children: Sequence[Mapping[str, Any]] = (),
    credentials: FakeCredentials | None = None,
    plans: FakePlans | None = None,
    existing_execution: Any = None,
    file_info_item: Mapping[str, Any] | None = None,
    passthrough_projection: Any = None,
    check_openapi: Any = None,
) -> tuple[
    NativeMoviePilotTransferProcessor,
    FakeRemote,
    FakeMediaInfo,
    FakePlans,
    FakeExecutions,
    FakeExecutionRepository,
    FakeJobs,
]:
    remote = FakeRemote(FakeRemoteClient(children, file_info_item=file_info_item))
    media_info = FakeMediaInfo()
    plan_service = plans or FakePlans()
    executions = FakeExecutions()
    repository = FakeExecutionRepository(existing_execution)
    jobs = FakeJobs()
    processor = NativeMoviePilotTransferProcessor(
        credentials=credentials or FakeCredentials(),
        remote=remote,
        media_info=media_info,
        plans=plan_service,
        executions=executions,
        execution_repository=repository,
        jobs=lambda: jobs,
        passthrough_projection=passthrough_projection,
        check_openapi=check_openapi,
    )
    return processor, remote, media_info, plan_service, executions, repository, jobs


def test_queued_transfer_uses_current_token_instead_of_old_event_fingerprint() -> None:
    current = P115CredentialSnapshot(account_id="115001", access_token="renewed-token")
    processor, remote, *_ = _processor(
        credentials=FakeCredentials((current,)),
        file_info_item=_file("video", "Fixture.S01E02.mkv", sha1="A" * 40, pick_code="pc-video"),
        passthrough_projection=SimpleNamespace(project=lambda **kw: {}),
    )
    processor.process(event_key=_EVENT_KEY, event_type="transfer.complete", passthrough=True,
        file={**_event_file(), "credential_fingerprint": "b" * 64, "file_name": "Fixture.S01E02.mkv"})
    assert remote.open_calls[0]["expected_credential_fingerprint"] not in {"b" * 64, ""}
    assert "credential_fingerprint" not in moviepilot_credential_context(current, "openapi")


@pytest.mark.parametrize("as_exception", [False, True])
def test_file_read_auth_failure_reopens_current_client_once_before_projection(as_exception) -> None:
    old = _DEFAULT_CREDENTIAL
    current = P115CredentialSnapshot(account_id="115001", access_token="renewed-token")
    check = Mock(side_effect=[(old, {"state": True}), (current, {"state": True})])
    projection = Mock(return_value={})
    processor, remote, *_ = _processor(
        credentials=FakeCredentials((old, current)), check_openapi=check,
        passthrough_projection=SimpleNamespace(project=projection),
    )
    failure = {"state": False, "code": 40140126, "message": "access_token 校验失败"}
    remote.client.file_info = Mock(side_effect=[
        RuntimeError(failure) if as_exception else failure,
        _file("video", "Fixture.S01E02.mkv", sha1="A" * 40, pick_code="pc-video"),
    ])
    processor.process(event_key=_EVENT_KEY, event_type="transfer.complete", passthrough=True,
        file={**_event_file(), "file_name": "Fixture.S01E02.mkv"})
    assert check.call_count == 2
    assert len(remote.open_calls) == 2
    fingerprints = [call["expected_credential_fingerprint"] for call in remote.open_calls]
    assert fingerprints[0] != fingerprints[1]
    projection.assert_called_once()


def test_authorization_failure_stops_before_read_or_projection_and_keeps_code() -> None:
    from etk.domains.subscription.moviepilot_transfer import MoviePilotTransferPreparationError

    check = Mock(return_value=(_DEFAULT_CREDENTIAL, {"state": False, "code": 40140126}))
    processor, remote, *_ = _processor(check_openapi=check)
    with pytest.raises(MoviePilotTransferPreparationError) as caught:
        processor.process(event_key=_EVENT_KEY, event_type="transfer.complete", file=_event_file(), passthrough=True)
    assert caught.value.details["stage"] == "authorization"
    assert caught.value.details["provider_code"] == "40140126"
    assert remote.open_calls == []


def test_expired_openapi_is_refreshed_and_saved_before_first_file_read() -> None:
    from etk.domains.p115.auth_directory import check_openapi_credentials

    old = P115CredentialSnapshot(account_id="115001", access_token="old", refresh_token="refresh-old")
    current = P115CredentialSnapshot(account_id="115001", access_token="new", refresh_token="refresh-new")
    credentials = FakeCredentials((old,))
    store = Mock()
    store.load.side_effect = credentials.load

    def save(access, refresh, **kwargs):
        assert (access, refresh, kwargs["expected_access_token"]) == ("new", "refresh-new", "old")
        credentials.snapshots = (current,)

    store.save_openapi.side_effect = save
    auth = Mock()
    auth.check_openapi.side_effect = lambda token: (
        {"state": False, "code": 40140126} if token == "old"
        else {"state": True, "data": {"user_id": "115001"}}
    )
    auth.refresh_openapi.return_value = {"state": True, "data": {
        "access_token": "new", "refresh_token": "refresh-new",
    }}
    processor, remote, *_ = _processor(
        credentials=credentials, check_openapi=lambda: check_openapi_credentials(store, auth),
        file_info_item=_file("video", "Fixture.S01E02.mkv", sha1="A" * 40, pick_code="pc-video"),
        passthrough_projection=SimpleNamespace(project=lambda **kwargs: {}),
    )
    processor.process(event_key=_EVENT_KEY, event_type="transfer.complete", passthrough=True,
        file={**_event_file(old), "file_name": "Fixture.S01E02.mkv"})
    auth.refresh_openapi.assert_called_once_with("refresh-old")
    store.save_openapi.assert_called_once()
    assert remote.client.info_calls == ["video"]


def test_unrelated_file_error_does_not_refresh_or_repeat_read() -> None:
    check = Mock(return_value=(_DEFAULT_CREDENTIAL, {"state": True}))
    processor, remote, *_ = _processor(check_openapi=check,
        file_info_item={"state": False, "code": 500},
        passthrough_projection=SimpleNamespace(project=lambda **kwargs: pytest.fail("must not project")))
    with pytest.raises(RuntimeError, match="file readback failed"):
        processor.process(event_key=_EVENT_KEY, event_type="transfer.complete", file=_event_file(), passthrough=True)
    assert check.call_count == 1
    assert remote.client.info_calls == ["video"]


def test_projection_unknown_outcome_is_not_converted_to_retryable_preparation_error() -> None:
    from etk.domains.media.effect_execution import MediaEffectDeliveryIndeterminate

    failure = MediaEffectDeliveryIndeterminate("projection outcome unknown")
    processor, *_ = _processor(
        file_info_item=_file("video", "Fixture.S01E02.mkv", sha1="A" * 40, pick_code="pc-video"),
        passthrough_projection=SimpleNamespace(project=Mock(side_effect=failure)),
    )
    with pytest.raises(MediaEffectDeliveryIndeterminate) as caught:
        processor.process(event_key=_EVENT_KEY, event_type="transfer.complete", passthrough=True,
            file={**_event_file(), "file_name": "Fixture.S01E02.mkv"})
    assert caught.value is failure


def test_repeated_file_read_auth_failure_has_a_bounded_retry() -> None:
    from etk.domains.subscription.moviepilot_transfer import MoviePilotTransferPreparationError

    check = Mock(return_value=(_DEFAULT_CREDENTIAL, {"state": True}))
    projection = Mock()
    processor, remote, *_ = _processor(check_openapi=check, passthrough_projection=SimpleNamespace(project=projection))
    remote.client.file_info = Mock(return_value={"state": False, "code": 40140126})
    with pytest.raises(MoviePilotTransferPreparationError) as caught:
        processor.process(event_key=_EVENT_KEY, event_type="transfer.complete", file=_event_file(), passthrough=True)
    assert remote.client.file_info.call_count == 2
    assert caught.value.details["stage"] == "file_info"
    assert caught.value.details["provider_code"] == "40140126"
    projection.assert_not_called()


def test_passthrough_projects_only_event_file_without_full_sync() -> None:
    calls = []

    def project(**kwargs: Any) -> dict[str, Any]:
        kwargs["assert_current"]()
        calls.append(kwargs)
        return {"changed": True, "ingestion_submitted": True}

    processor, remote, media_info, plans, executions, _repository, jobs = _processor(
        file_info_item=_file("video", "Fixture.S01E02.mkv", sha1="A" * 40, pick_code="pc-video"),
        passthrough_projection=SimpleNamespace(project=project),
    )

    first = processor.process(
        event_key=_EVENT_KEY,
        event_type="transfer.complete",
        file={**_event_file(), "file_name": "Fixture.S01E02.mkv"},
        passthrough=True,
    )

    assert first["reconciliation"] == "single-file-strm"
    assert jobs.calls == []
    assert remote.client.list_calls == []
    assert remote.client.info_calls == ["video"]
    assert len(media_info.calls) == 1
    assert len(calls) == 1
    assert calls[0]["file"]["sha1"] == "A" * 40
    assert plans.calls == []
    assert executions.calls == []


def test_episode_transfer_freezes_video_and_sidecar_but_not_another_episode() -> None:
    children = (
        _file("video", "Fixture.S01E02.mkv", sha1="A" * 40, pick_code="pc-video"),
        _file("subtitle", "Fixture.S01E02.zh.srt", sha1="B" * 40, pick_code="pc-subtitle"),
        _file("other", "Fixture.S01E03.mkv", sha1="C" * 40, pick_code="pc-other"),
    )
    processor, remote, media_info, plans, executions, _repository, _jobs = _processor(children=children)

    result = processor.process(
        event_key=_EVENT_KEY,
        event_type="transfer.complete",
        file=_event_file(),
        passthrough=False,
    )

    assert result["reconciliation"] == "p115-organize-plan"
    assert len(remote.open_calls) == 1
    assert remote.open_calls[0]["expected_account_id"] == "115001"
    assert remote.open_calls[0]["expected_backend"] == "openapi"
    assert len(remote.open_calls[0]["expected_credential_fingerprint"]) == 64
    assert [call[0].file_name for call in media_info.calls] == ["Fixture.S01E02.mkv"]
    assert plans.calls[0]["source_file_ids"] == ["video", "subtitle"]
    assert plans.calls[0]["recognition_hints"]["subtitle"]["sidecar"] is True
    assert executions.calls[0][0] == str(_PLAN_ID)
    assert executions.calls[0][1] == f"moviepilot-transfer:{_EVENT_KEY}"


@pytest.mark.parametrize(
    "changed", [{"fn": "Other.mkv"}, {"fid": "other"}, {"paths": [{"fid": "other", "fn": "Other"}]}]
)
def test_passthrough_rejects_changed_remote_identity(changed: dict[str, Any]) -> None:
    projection = SimpleNamespace(project=lambda **kwargs: pytest.fail("must not project"))
    processor, _, media_info, _, _, _, jobs = _processor(
        file_info_item={**_file("video", "Fixture.S01E02.mkv", sha1="A" * 40, pick_code="pc"), **changed},
        passthrough_projection=projection,
    )
    with pytest.raises(ValueError, match="changed"):
        processor.process(
            event_key=_EVENT_KEY,
            event_type="transfer.complete",
            file={**_event_file(), "file_name": "Fixture.S01E02.mkv"},
            passthrough=True,
        )
    assert media_info.calls == [] and jobs.calls == []


def test_passthrough_provider_error_is_retryable_not_identity_rejection() -> None:
    processor, _, _, _, _, _, jobs = _processor(
        file_info_item={"state": False, "code": 40140125, "data": []},
        passthrough_projection=SimpleNamespace(project=lambda **kwargs: pytest.fail("must not project")),
    )
    with pytest.raises(RuntimeError, match="readback failed"):
        processor.process(event_key=_EVENT_KEY, event_type="transfer.complete", file=_event_file(), passthrough=True)
    assert jobs.calls == []


def test_passthrough_openapi_readback_uses_real_season_parent_and_byte_size() -> None:
    calls = []
    target = {
        "state": True,
        "data": {
            "file_id": "video",
            "file_name": "Fixture.S01E02.mkv",
            "sha1": "A" * 40,
            "pick_code": "pc-video",
            "size": "1GB",
            "size_byte": 1024,
            "paths": [
                {"file_id": "0", "file_name": "根目录"},
                {"file_id": "source-root", "file_name": "媒体库"},
                {"file_id": "season", "file_name": "Season 01"},
            ],
        },
    }
    processor, _, _, _, _, _, _ = _processor(
        file_info_item=target,
        passthrough_projection=SimpleNamespace(project=lambda **kw: calls.append(kw) or {}),
    )
    processor.process(
        event_key=_EVENT_KEY,
        event_type="transfer.complete",
        file={**_event_file(), "file_name": "Fixture.S01E02.mkv", "path": "/媒体库/Season 01/Fixture.S01E02.mkv"},
        passthrough=True,
    )
    assert calls[0]["file"]["parent_id"] == "season"
    assert calls[0]["file"]["size"] == 1024


def test_file_info_fallback_keeps_target_video_in_the_selected_batch() -> None:
    video = _file("video", "Fixture.S01E02.mkv", sha1="A" * 40, pick_code="pc-video")
    subtitle = _file("subtitle", "Fixture.S01E02.zh.srt", sha1="B" * 40, pick_code="pc-subtitle")
    processor, _remote, _media_info, plans, _executions, _repository, _jobs = _processor(
        children=(subtitle,),
        file_info_item=video,
    )

    processor.process(
        event_key=_EVENT_KEY,
        event_type="transfer.complete",
        file=_event_file(),
        passthrough=False,
    )

    assert plans.calls[0]["source_file_ids"] == ["video", "subtitle"]


def test_movie_batch_does_not_match_a_longer_unrelated_stem() -> None:
    children = (
        _file("video", "Film.mkv", sha1="A" * 40, pick_code="pc-video"),
        _file("other", "Filmography.mkv", sha1="B" * 40, pick_code="pc-other"),
    )
    event = {**_event_file(), "media_type": "movie", "title": "Film"}
    processor, _remote, _media_info, plans, _executions, _repository, _jobs = _processor(children=children)

    processor.process(
        event_key=_EVENT_KEY,
        event_type="transfer.complete",
        file=event,
        passthrough=False,
    )

    assert plans.calls[0]["source_file_ids"] == ["video"]


def test_tv_event_without_episode_identity_keeps_only_the_target_name_family() -> None:
    children = (
        _file("video", "Fixture.mkv", sha1="A" * 40, pick_code="pc-video"),
        _file("subtitle", "Fixture.zh.srt", sha1="B" * 40, pick_code="pc-subtitle"),
        _file("other", "Another.mkv", sha1="C" * 40, pick_code="pc-other"),
    )
    event = {**_event_file(), "season_number": None, "episode_number": None}
    processor, _remote, _media_info, plans, _executions, _repository, _jobs = _processor(children=children)

    processor.process(
        event_key=_EVENT_KEY,
        event_type="transfer.complete",
        file=event,
        passthrough=False,
    )

    assert plans.calls[0]["source_file_ids"] == ["video", "subtitle"]


def test_existing_approved_intent_reuses_execution_without_provider_read() -> None:
    existing = SimpleNamespace(execution_id=_EXECUTION_ID, plan_id=_PLAN_ID, job_id=_JOB_ID)
    processor, remote, media_info, plans, executions, repository, _jobs = _processor(existing_execution=existing)

    result = processor.process(
        event_key=_EVENT_KEY,
        event_type="transfer.complete",
        file=_event_file(),
        passthrough=False,
    )

    assert result["job_reused"] is True
    assert repository.calls == [f"moviepilot-transfer:{_EVENT_KEY}"]
    assert remote.open_calls == []
    assert media_info.calls == []
    assert plans.calls == []
    assert executions.calls == []


def test_credential_drift_rejects_before_plan_or_execution_is_prepared() -> None:
    stable = P115CredentialSnapshot(account_id="115001", access_token="first-token")
    drifted = P115CredentialSnapshot(account_id="115001", access_token="second-token")
    credentials = FakeCredentials((stable, drifted))
    processor, _remote, _media_info, plans, executions, _repository, _jobs = _processor(
        children=(_file("video", "Fixture.S01E02.mkv", sha1="A" * 40, pick_code="pc-video"),),
        credentials=credentials,
    )

    with pytest.raises(RuntimeError, match="credential changed"):
        processor.process(
            event_key=_EVENT_KEY,
            event_type="transfer.complete",
            file=_event_file(stable),
            passthrough=False,
        )

    assert plans.calls == []
    assert executions.calls == []


def test_blocked_plan_never_prepares_an_execution() -> None:
    plans = FakePlans(ready=False)
    processor, _remote, _media_info, _plans, executions, _repository, _jobs = _processor(
        children=(_file("video", "Fixture.S01E02.mkv", sha1="A" * 40, pick_code="pc-video"),),
        plans=plans,
    )

    with pytest.raises(RuntimeError, match="Plan is not ready"):
        processor.process(
            event_key=_EVENT_KEY,
            event_type="transfer.complete",
            file=_event_file(),
            passthrough=False,
        )

    assert executions.calls == []


def test_worker_registers_exactly_one_native_moviepilot_delivery() -> None:
    source = (_REPO_ROOT / "src/etk/processes/worker.py").read_text(encoding="utf-8")
    coordinator_source = (_REPO_ROOT / "src/etk/processes/coordinator.py").read_text(encoding="utf-8")

    assert source.count("MoviePilotWebhookEffectDelivery(") == 1
    assert source.count("_moviepilot_transfer_processor(") == 2
    assert source.count("active_downloads=moviepilot_gateway.active_downloads") == 1
    assert source.count("MoviePilotFullWashingOperations(subscription_operations)") == 1
    assert "MoviePilotTransferJobProcessor" not in source
    assert coordinator_source.count("MOVIEPILOT_ASSISTANT_EVENT_TYPE") == 2
