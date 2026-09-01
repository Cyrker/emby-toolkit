from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest

from etk.domains.p115.media_probe import P115MediaProbeResult, P115MediaProbeStatus
from etk.domains.p115.models import P115CredentialSnapshot, P115Result
from etk.domains.subscription.moviepilot_transfer import (
    NativeMoviePilotTransferProcessor,
    freeze_moviepilot_credential_binding,
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
        **freeze_moviepilot_credential_binding(credential, "openapi"),
    }


def _processor(
    *,
    children: Sequence[Mapping[str, Any]] = (),
    credentials: FakeCredentials | None = None,
    plans: FakePlans | None = None,
    existing_execution: Any = None,
    file_info_item: Mapping[str, Any] | None = None,
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
    )
    return processor, remote, media_info, plan_service, executions, repository, jobs


def test_passthrough_submits_one_stable_full_sync_job() -> None:
    processor, remote, _media_info, plans, executions, _repository, jobs = _processor()

    first = processor.process(
        event_key=_EVENT_KEY,
        event_type="transfer.complete",
        file=_event_file(),
        passthrough=True,
    )

    assert first["reconciliation"] == "full-sync-strm"
    assert jobs.calls == [
        (
            "full-sync-strm",
            {"home_video_only": False},
            {
                "trigger_type": "system",
                "idempotency_scope": "moviepilot-transfer-passthrough",
                "idempotency_key": _EVENT_KEY,
                "max_attempts": 2,
                "queue_name": "native.serial",
            },
        )
    ]
    assert remote.open_calls == []
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


def test_both_worker_profiles_register_exactly_one_native_moviepilot_delivery() -> None:
    source = (_REPO_ROOT / "src/etk/processes/worker.py").read_text(encoding="utf-8")
    coordinator_source = (_REPO_ROOT / "src/etk/processes/coordinator.py").read_text(encoding="utf-8")

    assert source.count("MoviePilotWebhookEffectDelivery(") == 2
    assert source.count("_moviepilot_transfer_processor(") == 3
    assert source.count("active_downloads=moviepilot_gateway.active_downloads") == 2
    assert source.count("MoviePilotFullWashingOperations(subscription_operations)") == 2
    assert "MoviePilotTransferJobProcessor" not in source
    assert coordinator_source.count("MOVIEPILOT_ASSISTANT_EVENT_TYPE") == 3
