from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest
from pydantic import SecretStr

from etk.domains.identity import post_authorization
from etk.domains.identity.service import _post_authorization_projection
from etk.platform import Settings
from etk.platform.jobs import JobStatus

JOB_ID = UUID("00000000-0000-0000-0000-0000000001bb")


def _settings(token: str = "fixture-token") -> Settings:
    return Settings(
        emby_server_url="http://example.invalid",
        emby_api_key=SecretStr(token),
        emby_user_id="fixture-admin",
        emby_auth_mode="user_token",
    )


def _projection(
    status: JobStatus,
    *,
    attempt_no: int = 1,
    error_type: str | None = None,
    error: dict[str, Any] | None = None,
    error_details: dict[str, Any] | None = None,
):
    return SimpleNamespace(
        job=SimpleNamespace(
            id=JOB_ID,
            status=status,
            result=None,
            error_summary=error,
        ),
        current_attempt=SimpleNamespace(
            attempt_no=attempt_no,
            error_type=error_type,
            error_details=error_details,
        ),
    )


def test_submitter_persists_only_revision_and_identity(monkeypatch) -> None:
    captured = {}

    class Jobs:
        def submit_background_operation(self, function, args, kwargs, **options):
            captured.update({"function": function, "args": args, "kwargs": kwargs, **options})
            return SimpleNamespace(
                projection=_projection(JobStatus.QUEUED),
                reused=False,
                resumed=False,
            )

    submitter = post_authorization.PersistentPostAuthorizationSubmitter(cast(Any, lambda: Jobs()))

    result = submitter.submit(_settings())

    assert result.job_id == str(JOB_ID)
    assert result.status == "queued"
    assert result.reused is False
    assert result.resumed is False
    assert captured["function"] is post_authorization.run_identity_post_authorization_persistent
    assert captured["args"][1] == "fixture-admin"
    assert captured["kwargs"] == {}
    assert captured["max_attempts"] == 5
    assert captured["timeout_seconds"] == 900
    assert captured["idempotency_scope"] == "identity-post-authorization"
    assert "fixture-token" not in repr(captured)


def test_same_revision_explicitly_resumes_a_terminal_job_without_changing_its_identity() -> None:
    resumed_job_ids = []

    class Jobs:
        def submit_background_operation(self, *_args, **_kwargs):
            return SimpleNamespace(
                projection=_projection(JobStatus.FAILED, attempt_no=5, error_type="RuntimeError"),
                reused=True,
                resumed=False,
            )

        def resume_terminal(self, job_id):
            resumed_job_ids.append(job_id)
            return SimpleNamespace(
                projection=_projection(JobStatus.QUEUED, attempt_no=6),
                reused=True,
                resumed=True,
            )

    submitter = post_authorization.PersistentPostAuthorizationSubmitter(cast(Any, lambda: Jobs()))

    result = submitter.submit(_settings())

    assert resumed_job_ids == [JOB_ID]
    assert result.job_id == str(JOB_ID)
    assert result.status == "queued"
    assert result.reused is True
    assert result.resumed is True


def test_indeterminate_terminal_job_is_reported_without_blind_replay() -> None:
    class Jobs:
        def submit_background_operation(self, *_args, **_kwargs):
            return SimpleNamespace(
                projection=_projection(
                    JobStatus.FAILED,
                    attempt_no=5,
                    error_type="PersistentEffectIndeterminate",
                    error={"type": "PersistentEffectIndeterminate", "message": "fixture unknown effect"},
                ),
                reused=True,
                resumed=False,
            )

        def resume_terminal(self, _job_id):
            raise AssertionError("indeterminate effects must not be blindly resumed")

    submitter = post_authorization.PersistentPostAuthorizationSubmitter(cast(Any, lambda: Jobs()))

    result = submitter.submit(_settings())
    plugin_install, library_config = _post_authorization_projection(result)

    assert result.status == "failed"
    assert result.error_type == "PersistentEffectIndeterminate"
    assert plugin_install["status"] == "failed"
    assert "结果未知" in plugin_install["message"]
    assert library_config["status"] == "failed"


@pytest.mark.parametrize(
    ("error_type", "control"),
    [
        ("PersistentExecutionInterruptedAfterEffect", None),
        ("RuntimeError", {"committed_effects": ["fixture-write"], "inflight_effects": []}),
        ("RuntimeError", {"committed_effects": [], "inflight_effects": ["fixture-write"]}),
    ],
)
def test_effectful_terminal_job_is_never_automatically_resumed(error_type, control) -> None:
    class Jobs:
        def submit_background_operation(self, *_args, **_kwargs):
            return SimpleNamespace(
                projection=_projection(
                    JobStatus.FAILED,
                    attempt_no=5,
                    error_type=error_type,
                    error_details={"control": control} if control is not None else None,
                ),
                reused=True,
                resumed=False,
            )

        def resume_terminal(self, _job_id):
            raise AssertionError("effectful terminal Jobs must not be automatically resumed")

    submitter = post_authorization.PersistentPostAuthorizationSubmitter(cast(Any, lambda: Jobs()))

    result = submitter.submit(_settings())

    assert result.status == "failed"
    assert result.resumed is False


def test_worker_reads_current_persisted_authorization_and_propagates_failures(monkeypatch) -> None:
    settings = _settings()
    monkeypatch.setattr(post_authorization, "_load_persisted_identity_settings", lambda: settings)
    calls = []

    class Gateway:
        def install_plugin(self, authorization, received_settings):
            calls.append(("plugin", authorization, received_settings))
            return {"ok": True, "message": "fixture plugin ready"}

        def configure_libraries(self, authorization, plugin_install):
            calls.append(("libraries", authorization, plugin_install))
            return {"ok": False, "message": "fixture library failure"}

    monkeypatch.setattr(post_authorization, "LegacyEmbyIdentityGateway", Gateway)

    with pytest.raises(RuntimeError, match="fixture library failure"):
        post_authorization.run_identity_post_authorization_persistent(
            post_authorization.identity_authorization_revision(settings),
            "fixture-admin",
        )

    assert calls[0][0] == "plugin"
    assert calls[1][0] == "libraries"
    assert calls[0][1]["AccessToken"] == "fixture-token"


def test_worker_rejects_a_superseded_authorization_without_external_calls(monkeypatch) -> None:
    monkeypatch.setattr(post_authorization, "_load_persisted_identity_settings", lambda: _settings("new-token"))
    monkeypatch.setattr(
        post_authorization,
        "LegacyEmbyIdentityGateway",
        lambda: (_ for _ in ()).throw(AssertionError("stale Job must not call Emby")),
    )

    with pytest.raises(RuntimeError, match="授权已变化"):
        post_authorization.run_identity_post_authorization_persistent(
            post_authorization.identity_authorization_revision(_settings("old-token")),
            "fixture-admin",
        )
