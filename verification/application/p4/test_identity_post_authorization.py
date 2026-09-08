from __future__ import annotations

from collections.abc import Mapping
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
        def submit_native_operation(self, operation_key, parameters, **options):
            captured.update({"operation_key": operation_key, "parameters": parameters, **options})
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
    assert captured["operation_key"] == "identity.post-authorization"
    assert captured["parameters"]["expected_user_id"] == "fixture-admin"
    assert captured["display_name"] == "Emby 授权后配置"
    assert captured["trigger_type"] == "system"
    assert captured["timeout_seconds"] == 900
    assert captured["idempotency_scope"] == "identity-post-authorization"
    assert "fixture-token" not in repr(captured)


def test_same_revision_terminal_native_job_is_reported_without_blind_resume() -> None:
    class Jobs:
        def submit_native_operation(self, *_args, **_kwargs):
            return SimpleNamespace(
                projection=_projection(JobStatus.FAILED, error_type="RuntimeError"),
                reused=True,
                resumed=False,
            )

        def resume_terminal(self, _job_id):
            raise AssertionError("native operations must not be resumed")

    submitter = post_authorization.PersistentPostAuthorizationSubmitter(cast(Any, lambda: Jobs()))

    result = submitter.submit(_settings())

    assert result.job_id == str(JOB_ID)
    assert result.status == "failed"
    assert result.reused is True
    assert result.resumed is False


def test_indeterminate_terminal_job_is_reported_without_blind_replay() -> None:
    class Jobs:
        def submit_native_operation(self, *_args, **_kwargs):
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
        def submit_native_operation(self, *_args, **_kwargs):
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


class Progress:
    def __init__(self) -> None:
        self.job_id = JOB_ID
        self.events: list[tuple[Any, ...]] = []

    def checkpoint(self) -> None:
        self.events.append(("checkpoint",))

    def update(self, progress: int, message: str) -> None:
        self.events.append(("update", progress, message))

    def record_started_effect(self, label: str) -> None:
        self.events.append(("started", label))

    def record_committed_effect(self, label: str) -> None:
        self.events.append(("committed", label))

    def record_abandoned_effect(self, label: str) -> None:
        self.events.append(("abandoned", label))


def test_worker_reads_current_persisted_authorization_and_propagates_failures() -> None:
    settings = _settings()
    calls = []

    class Gateway:
        def install_plugin(self, authorization, received_settings, request_id):
            calls.append(("plugin", authorization, received_settings, request_id))
            return {"ok": True, "message": "fixture plugin ready"}

        def configure_libraries(self, authorization, plugin_install):
            calls.append(("libraries", authorization, plugin_install))
            return {"ok": False, "message": "fixture library failure"}

    progress = Progress()
    operation = post_authorization.NativeIdentityPostAuthorizationOperation(lambda: settings, cast(Any, Gateway()))

    with pytest.raises(RuntimeError, match="fixture library failure"):
        operation.execute(
            {
                "expected_revision": post_authorization.identity_authorization_revision(settings),
                "expected_user_id": "fixture-admin",
            },
            progress,
        )

    assert calls[0][0] == "plugin"
    assert calls[1][0] == "libraries"
    assert calls[0][1]["AccessToken"] == "fixture-token"
    assert calls[0][3] == str(JOB_ID)
    assert [event[0] for event in progress.events] == [
        "checkpoint",
        "started",
        "committed",
        "update",
        "checkpoint",
        "started",
    ]


def test_worker_rejects_a_superseded_authorization_without_external_calls() -> None:
    operation = post_authorization.NativeIdentityPostAuthorizationOperation(
        lambda: _settings("new-token"),
        cast(Any, object()),
    )

    with pytest.raises(RuntimeError, match="授权已变化"):
        operation.execute(
            {
                "expected_revision": post_authorization.identity_authorization_revision(_settings("old-token")),
                "expected_user_id": "fixture-admin",
            },
            Progress(),
        )


def _execute_authorization(
    settings: Settings,
    gateway: object,
    *,
    progress: Progress | None = None,
    expected_user_id: str | None = None,
) -> tuple[Mapping[str, Any], Progress]:
    selected_progress = progress or Progress()
    operation = post_authorization.NativeIdentityPostAuthorizationOperation(lambda: settings, cast(Any, gateway))
    result = operation.execute(
        {
            "expected_revision": post_authorization.identity_authorization_revision(settings),
            "expected_user_id": expected_user_id or settings.emby_user_id,
        },
        selected_progress,
    )
    return result, selected_progress


def test_worker_rejects_incomplete_parameters_mode_user_identity_and_authorization() -> None:
    settings = _settings()
    operation = post_authorization.NativeIdentityPostAuthorizationOperation(lambda: settings, cast(Any, object()))
    with pytest.raises(ValueError, match="parameters are incomplete"):
        operation.execute({}, Progress())

    wrong_mode = settings.model_copy(update={"emby_auth_mode": "api_key"})
    with pytest.raises(RuntimeError, match="授权模式无效"):
        _execute_authorization(wrong_mode, object())

    with pytest.raises(RuntimeError, match="授权用户已变化"):
        _execute_authorization(settings, object(), expected_user_id="another-user")

    missing_token = settings.model_copy(update={"emby_api_key": SecretStr("")})
    with pytest.raises(RuntimeError, match="授权不完整"):
        _execute_authorization(missing_token, object())

    progress = Progress()
    cast(Any, progress).job_id = ""
    with pytest.raises(RuntimeError, match="缺少持久化 Job identity"):
        _execute_authorization(settings, object(), progress=progress)


@pytest.mark.parametrize(
    ("certainty", "expected_event"),
    [
        ("confirmed-no-effect", "abandoned"),
        ("observed-complete", "committed"),
        ("unknown", None),
    ],
)
def test_worker_records_plugin_control_plane_certainty(
    certainty: str,
    expected_event: str | None,
) -> None:
    class Gateway:
        @staticmethod
        def install_plugin(*_args: Any, **_kwargs: Any) -> Mapping[str, Any]:
            raise post_authorization.ControlPlaneError("fixture", certainty=cast(Any, certainty))

    progress = Progress()
    with pytest.raises(post_authorization.ControlPlaneError, match="fixture"):
        _execute_authorization(_settings(), Gateway(), progress=progress)

    kinds = [event[0] for event in progress.events]
    assert kinds[:2] == ["checkpoint", "started"]
    if expected_event is None:
        assert kinds == ["checkpoint", "started"]
    else:
        assert kinds[-1] == expected_event


def test_worker_rejects_unconfirmed_plugin_install() -> None:
    gateway = SimpleNamespace(install_plugin=lambda *_args: {"ok": False, "message": "plugin rejected"})
    with pytest.raises(RuntimeError, match="plugin rejected"):
        _execute_authorization(_settings(), gateway)


@pytest.mark.parametrize(
    ("certainty", "expected_event"),
    [
        ("confirmed-no-effect", "abandoned"),
        ("observed-complete", "committed"),
        ("unknown", "started"),
    ],
)
def test_worker_records_library_configuration_certainty(
    certainty: str,
    expected_event: str,
) -> None:
    class Gateway:
        @staticmethod
        def install_plugin(*_args: Any) -> Mapping[str, Any]:
            return {"ok": True}

        @staticmethod
        def configure_libraries(*_args: Any) -> Mapping[str, Any]:
            return {"ok": False, "certainty": certainty, "message": "library rejected"}

    progress = Progress()
    with pytest.raises(RuntimeError, match="library rejected"):
        _execute_authorization(_settings(), Gateway(), progress=progress)

    library_events = [event[0] for event in progress.events if "identity-library" in str(event)]
    assert library_events[-1] == expected_event


def test_worker_completes_plugin_and_library_configuration() -> None:
    class Gateway:
        @staticmethod
        def install_plugin(*_args: Any) -> Mapping[str, Any]:
            return {"ok": True, "installed": True}

        @staticmethod
        def configure_libraries(*_args: Any) -> Mapping[str, Any]:
            return {"ok": True, "configured": 2}

    result, progress = _execute_authorization(_settings(), Gateway())

    assert result["ok"] is True
    assert result["plugin_install"]["installed"] is True
    assert result["library_config"]["configured"] == 2
    assert progress.events[-1] == ("update", 100, "Emby 授权后配置已完成")
