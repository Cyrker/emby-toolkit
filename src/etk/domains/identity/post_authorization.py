from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from etk.domains.operations.jobs import JobSubmissionService
from etk.platform.config import Settings
from etk.platform.control_plane import ControlPlaneError
from etk.platform.jobs import JobProjection

from .ports import EmbyIdentityGateway, PostAuthorizationSubmission


def identity_authorization_revision(settings: Settings) -> str:
    value = "\0".join(
        (
            settings.emby_server_url.strip().rstrip("/"),
            settings.emby_user_id.strip(),
            settings.emby_api_key.get_secret_value().strip(),
        )
    ).encode("utf-8")
    return hashlib.sha256(value).hexdigest()


class IdentityPostAuthorizationProgress(Protocol):
    job_id: Any

    def checkpoint(self) -> None: ...

    def update(self, progress: int, message: str) -> None: ...

    def record_started_effect(self, label: str) -> None: ...

    def record_committed_effect(self, label: str) -> None: ...

    def record_abandoned_effect(self, label: str) -> None: ...


class NativeIdentityPostAuthorizationOperation:
    def __init__(
        self,
        settings: Callable[[], Settings],
        gateway: EmbyIdentityGateway,
    ) -> None:
        self._settings = settings
        self._gateway = gateway

    def execute(
        self,
        parameters: Mapping[str, Any],
        progress: IdentityPostAuthorizationProgress,
    ) -> Mapping[str, Any]:
        expected_revision = str(parameters.get("expected_revision") or "").strip()
        expected_user_id = str(parameters.get("expected_user_id") or "").strip()
        if not expected_revision or not expected_user_id:
            raise ValueError("identity post-authorization parameters are incomplete")
        settings = self._settings()
        progress.checkpoint()
        return self._execute(settings, expected_revision, expected_user_id, progress)

    def _execute(
        self,
        settings: Settings,
        expected_revision: str,
        expected_user_id: str,
        progress: IdentityPostAuthorizationProgress,
    ) -> Mapping[str, Any]:
        if settings.emby_auth_mode != "user_token":
            raise RuntimeError("Emby 服务授权模式无效")
        if settings.emby_user_id.strip() != str(expected_user_id or "").strip():
            raise RuntimeError("Emby 服务授权用户已变化，拒绝执行旧授权任务")
        if identity_authorization_revision(settings) != str(expected_revision or "").strip():
            raise RuntimeError("Emby 服务授权已变化，拒绝执行旧授权任务")

        access_token = settings.emby_api_key.get_secret_value().strip()
        if not settings.emby_server_url.strip() or not access_token:
            raise RuntimeError("Emby 服务授权不完整")
        request_id = str(progress.job_id or "").strip()
        if not request_id:
            raise RuntimeError("Emby 授权后配置缺少持久化 Job identity")
        authorization = {
            "_normalized_url": settings.emby_server_url.strip().rstrip("/"),
            "AccessToken": access_token,
            "User": {"Id": settings.emby_user_id.strip()},
        }
        plugin_label = f"identity-plugin-install:{expected_revision}"
        progress.record_started_effect(plugin_label)
        try:
            plugin_install = dict(self._gateway.install_plugin(authorization, settings, request_id))
        except ControlPlaneError as exc:
            if exc.certainty == "confirmed-no-effect":
                progress.record_abandoned_effect(plugin_label)
            elif exc.certainty == "observed-complete":
                progress.record_committed_effect(plugin_label)
            raise
        if not plugin_install.get("ok"):
            raise RuntimeError(plugin_install.get("message") or "ETK 插件安装失败")
        progress.record_committed_effect(plugin_label)
        progress.update(50, "ETK 插件安装已确认")
        progress.checkpoint()
        library_label = f"identity-library-configuration:{expected_revision}"
        progress.record_started_effect(library_label)
        library_config = dict(self._gateway.configure_libraries(authorization, plugin_install))
        if not library_config.get("ok"):
            certainty = str(library_config.get("certainty") or "unknown")
            if certainty == "confirmed-no-effect":
                progress.record_abandoned_effect(library_label)
            elif certainty == "observed-complete":
                progress.record_committed_effect(library_label)
            raise RuntimeError(library_config.get("message") or "Emby 媒体库加速配置失败")
        progress.record_committed_effect(library_label)
        progress.update(100, "Emby 授权后配置已完成")
        return {
            "ok": True,
            "plugin_install": plugin_install,
            "library_config": library_config,
            "emby_user_id": settings.emby_user_id.strip(),
        }


class PersistentPostAuthorizationSubmitter:
    def __init__(self, jobs: Callable[[], JobSubmissionService]) -> None:
        self._jobs = jobs

    def submit(self, settings: Settings) -> PostAuthorizationSubmission:
        revision = identity_authorization_revision(settings)
        user_id = settings.emby_user_id.strip()
        if not user_id:
            raise ValueError("Emby 服务授权缺少用户 ID")
        jobs = self._jobs()
        submitted = jobs.submit_native_operation(
            "identity.post-authorization",
            {"expected_revision": revision, "expected_user_id": user_id},
            display_name="Emby 授权后配置",
            trigger_type="system",
            timeout_seconds=900,
            idempotency_scope="identity-post-authorization",
            idempotency_key=revision,
        )
        projection = submitted.projection
        return PostAuthorizationSubmission(
            job_id=str(projection.job.id),
            status=projection.job.status.value,
            reused=submitted.reused,
            resumed=submitted.resumed,
            result=dict(projection.job.result) if projection.job.result is not None else None,
            error=dict(projection.job.error_summary) if projection.job.error_summary is not None else None,
            error_type=_projection_error_type(projection),
        )


def _projection_error_type(projection: JobProjection) -> str | None:
    attempt_type = str(projection.current_attempt.error_type or "").strip()
    if attempt_type:
        return attempt_type
    summary_type = str((projection.job.error_summary or {}).get("type") or "").strip()
    return summary_type or None


__all__ = [
    "NativeIdentityPostAuthorizationOperation",
    "PersistentPostAuthorizationSubmitter",
    "identity_authorization_revision",
]
