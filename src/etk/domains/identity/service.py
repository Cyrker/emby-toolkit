from __future__ import annotations

import logging
import secrets
import threading
import time
from collections.abc import Callable, Mapping
from copy import deepcopy
from typing import Any

from etk.platform.config import Settings
from etk.platform.web import SessionState

from .models import (
    AuthCheckResponse,
    AuthorizationDetails,
    AuthUser,
    IdentityResult,
    LoginRequest,
    LoginResponse,
    ReauthorizeRequest,
    ReauthorizeResponse,
    RecoveryVerifyRequest,
    ServiceStatusResponse,
    SetupRequest,
    SetupResponse,
    StatusResponse,
    error_result,
)
from .ports import (
    ConfigurationWriter,
    EmbyIdentityGateway,
    IdentityRepository,
    PostAuthorizationSubmission,
    PostAuthorizationSubmitter,
)

logger = logging.getLogger(__name__)


class RecoveryTokenStore:
    def __init__(
        self,
        *,
        token_factory: Callable[[], str] | None = None,
        clock: Callable[[], float] = time.time,
        lifetime_seconds: float = 300.0,
    ) -> None:
        if lifetime_seconds <= 0:
            raise ValueError("recovery token lifetime must be positive")
        self._token_factory = token_factory or (lambda: secrets.token_hex(3).upper())
        self._clock = clock
        self._lifetime_seconds = lifetime_seconds
        self._lock = threading.Lock()
        self._tokens: dict[str, float] = {}

    def issue(self) -> str:
        with self._lock:
            self._clean_locked()
            token = self._token_factory().strip().upper()
            if not token:
                raise RuntimeError("recovery token factory returned an empty token")
            self._tokens[token] = self._clock() + self._lifetime_seconds
            return token

    def consume(self, token: str) -> bool:
        normalized = str(token or "").strip().upper()
        with self._lock:
            self._clean_locked()
            return self._tokens.pop(normalized, None) is not None

    def _clean_locked(self) -> None:
        now = self._clock()
        for token, expires_at in tuple(self._tokens.items()):
            if now > expires_at:
                del self._tokens[token]


class IdentityService:
    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        repository: IdentityRepository,
        gateway: EmbyIdentityGateway,
        configuration: ConfigurationWriter,
        post_authorization: PostAuthorizationSubmitter,
        recovery_tokens: RecoveryTokenStore | None = None,
    ) -> None:
        self._settings = settings
        self.repository = repository
        self._gateway = gateway
        self._configuration = configuration
        self._post_authorization_submitter = post_authorization
        self._recovery_tokens = recovery_tokens or RecoveryTokenStore()

    def check_status(self, session: SessionState) -> IdentityResult:
        if not self.is_configured():
            return IdentityResult(AuthCheckResponse(status="setup_required", message="系统未配置"))
        user_id = str(session.get("emby_user_id") or "").strip()
        if user_id:
            try:
                active = self.repository.is_active(user_id)
                is_admin = self.repository.is_admin(user_id) if active else False
            except Exception:
                logger.exception("Unable to verify current identity state")
                return error_result("身份状态暂时不可用", status_code=503)
            if not active:
                session.clear()
                return IdentityResult(AuthCheckResponse(status="login_required"))
            return IdentityResult(
                AuthCheckResponse(
                    status="logged_in",
                    user=AuthUser(
                        id=user_id,
                        name=session.get("emby_username"),
                        is_admin=is_admin,
                    ),
                )
            )
        return IdentityResult(AuthCheckResponse(status="login_required"))

    def login(self, request: LoginRequest, session: SessionState) -> IdentityResult:
        if not self.is_configured():
            return error_result(
                "系统尚未配置 Emby 连接",
                status_code=428,
                code="SETUP_REQUIRED",
            )
        settings = self._settings()
        try:
            projection_revision = self.repository.projection_revision()
        except Exception:
            logger.exception("Unable to snapshot identity projection before login")
            return error_result(
                "登录暂时不可用，请稍后重试",
                status_code=503,
                code="IDENTITY_STATE_UNAVAILABLE",
            )
        try:
            authorization = self._gateway.authenticate(
                settings.emby_server_url,
                request.username,
                request.password,
                device_id="my-emby-toolkit-auth-v4",
            )
        except Exception:
            logger.exception("Emby login gateway failed")
            authorization = None
        if not authorization:
            return error_result(
                "登录失败：用户名/密码错误，或无法连接 Emby 服务器",
                status_code=401,
            )
        user = _authorization_user(authorization)
        if not user.get("Id"):
            return error_result(
                "登录失败：用户名/密码错误，或无法连接 Emby 服务器",
                status_code=401,
            )
        try:
            registration_pending = self.repository.registration_pending(str(user.get("Name") or request.username))
        except Exception:
            logger.exception("Unable to verify registration ownership before login")
            return error_result(
                "登录暂时不可用，请稍后重试",
                status_code=503,
                code="REGISTRATION_STATE_UNAVAILABLE",
            )
        if registration_pending:
            return error_result(
                "注册流程尚未完成或状态尚未确认，请稍后重试",
                status_code=409,
                code="REGISTRATION_PENDING",
            )
        if not self._start_session(user, session, expected_revision=projection_revision):
            return error_result(
                "登录期间用户状态发生变化，请重试",
                status_code=409,
                code="IDENTITY_STATE_CHANGED",
            )
        user_id = str(user["Id"])
        try:
            can_subscribe = self.repository.subscription_permission(user_id)
        except Exception:
            logger.exception("Unable to read subscription permission after login")
            can_subscribe = False
        logger.info("Emby user logged in", extra={"emby_user_id": user_id})
        return IdentityResult(
            LoginResponse(
                user=AuthUser(
                    id=user_id,
                    name=str(user.get("Name") or ""),
                    is_admin=bool(session.get("emby_is_admin", False)),
                    allow_unrestricted_subscriptions=can_subscribe,
                    user_type="emby_user",
                )
            )
        )

    def request_recovery(self) -> IdentityResult:
        token = self._recovery_tokens.issue()
        logger.critical("=" * 60)
        logger.critical("【安全警告】收到重置连接配置的请求。")
        logger.critical("若这是您本人的操作，请在页面输入以下安全令牌以进入设置模式:")
        logger.critical("安全令牌:  %s", token)
        logger.critical("令牌有效期: 5 分钟")
        logger.critical("=" * 60)
        return IdentityResult(
            StatusResponse(
                status="ok",
                message="安全令牌已发送至服务器控制台日志(Docker Logs)，请查阅并输入。",
            )
        )

    def verify_recovery(self, request: RecoveryVerifyRequest, session: SessionState) -> IdentityResult:
        if self._recovery_tokens.consume(request.token):
            session["is_setup_mode"] = True
            return IdentityResult(StatusResponse(status="ok", message="验证成功"))
        return error_result("令牌无效或已过期", status_code=403)

    def setup(self, request: SetupRequest, session: SessionState) -> IdentityResult:
        if self.is_configured() and not session.get("is_setup_mode"):
            return error_result("系统已配置，且无重置权限", status_code=403)
        try:
            projection_revision = self.repository.projection_revision()
        except Exception:
            logger.exception("Unable to snapshot identity projection before setup")
            return error_result("身份状态暂时不可用", status_code=503)
        authorization, error = self._authorize_service(
            request.url,
            request.username,
            request.password,
        )
        if error or authorization is None:
            return error_result(error or "管理员认证失败", status_code=400)
        try:
            settings = self._save_authorization(authorization)
        except Exception:
            logger.exception("Saving Emby service authorization failed")
            return error_result("保存配置失败，请检查日志", status_code=500)
        user = _authorization_user(authorization)
        if not self._start_session(user, session, expected_revision=projection_revision):
            return error_result(
                "Emby 服务授权已保存，但授权期间用户状态发生变化；请刷新后重新登录",
                status_code=409,
            )
        try:
            plugin_install, library_config = self._post_authorization(settings)
        except Exception:
            logger.exception("Submitting post-authorization workflow failed")
            return error_result(
                "Emby 服务授权已保存，但插件与媒体库配置任务提交失败，请检查任务服务",
                status_code=503,
            )
        logger.info("Emby administrator service authorization saved without account password")
        return IdentityResult(
            SetupResponse(
                message=_authorization_message("Emby 服务授权成功", plugin_install, library_config),
                plugin_install=dict(plugin_install),
                library_config=dict(library_config),
                user=AuthUser(
                    id=str(session["emby_user_id"]),
                    name=str(session.get("emby_username") or ""),
                    is_admin=True,
                ),
            )
        )

    def service_status(self) -> IdentityResult:
        settings = self._settings()
        authorized = self.is_service_authorized()
        if authorized:
            try:
                result = self._gateway.test_connection(
                    settings.emby_server_url, settings.emby_api_key.get_secret_value()
                )
            except Exception:
                logger.exception("Emby service status check failed")
                authorized = False
            else:
                authorized = bool(result.get("success"))
        return IdentityResult(ServiceStatusResponse(authorized=authorized, url=settings.emby_server_url or ""))

    def reauthorize(self, request: ReauthorizeRequest) -> IdentityResult:
        url = request.url or self._settings().emby_server_url
        authorization, error = self._authorize_service(url, request.username, request.password)
        if error or authorization is None:
            return error_result(error or "管理员认证失败", status_code=400)
        try:
            settings = self._save_authorization(authorization)
        except Exception:
            logger.exception("Reauthorizing Emby service failed")
            return error_result("保存服务授权失败，请检查日志", status_code=500)
        try:
            plugin_install, library_config = self._post_authorization(settings)
        except Exception:
            logger.exception("Submitting reauthorization workflow failed")
            return error_result(
                "Emby 服务授权已更新，但插件与媒体库配置任务提交失败，请检查任务服务",
                status_code=503,
            )
        user = _authorization_user(authorization)
        return IdentityResult(
            ReauthorizeResponse(
                message=_authorization_message("Emby 服务授权已更新", plugin_install, library_config),
                plugin_install=dict(plugin_install),
                library_config=dict(library_config),
                authorization=AuthorizationDetails(
                    url=str(authorization["_normalized_url"]),
                    user_id=str(user["Id"]),
                    user_name=str(user.get("Name") or ""),
                ),
            )
        )

    @staticmethod
    def logout(session: SessionState) -> IdentityResult:
        session.clear()
        return IdentityResult(StatusResponse(status="ok"))

    def is_configured(self) -> bool:
        settings = self._settings()
        return bool(settings.emby_server_url.strip() and settings.emby_api_key.get_secret_value().strip())

    def is_service_authorized(self) -> bool:
        settings = self._settings()
        return bool(
            settings.emby_auth_mode == "user_token"
            and settings.emby_server_url.strip()
            and settings.emby_api_key.get_secret_value().strip()
            and settings.emby_user_id.strip()
        )

    def _authorize_service(
        self,
        url: str,
        username: str,
        password: str,
    ) -> tuple[dict[str, Any] | None, str | None]:
        normalized_url = str(url or "").strip().rstrip("/")
        normalized_username = str(username or "").strip()
        if not normalized_url or not normalized_username:
            return None, "Emby URL 和管理员用户名不能为空"
        try:
            raw = self._gateway.authenticate(
                normalized_url,
                normalized_username,
                password or "",
                device_id="emby-toolkit-service",
            )
        except Exception:
            logger.exception("Emby service authorization gateway failed")
            raw = None
        if not raw:
            return None, "管理员认证失败，请检查账号、密码和服务器地址"
        authorization = deepcopy(dict(raw))
        access_token = str(authorization.get("AccessToken") or "").strip()
        user = _authorization_user(authorization)
        user_id = str(user.get("Id") or "").strip()
        if not access_token or not user_id:
            return None, "Emby 认证响应缺少 AccessToken 或用户 ID"
        if not _user_policy(user).get("IsAdministrator", False):
            refreshed = self._gateway.get_user(normalized_url, access_token, user_id)
            if refreshed:
                user = dict(refreshed)
                authorization["User"] = user
        if not _user_policy(user).get("IsAdministrator", False):
            return None, "必须使用 Emby 管理员账号完成服务授权"
        test_result = self._gateway.test_connection(normalized_url, access_token)
        if not test_result.get("success"):
            return None, f"服务 Token 验证失败: {test_result.get('error')}"
        authorization["_normalized_url"] = normalized_url
        return authorization, None

    def _save_authorization(self, authorization: Mapping[str, Any]) -> Settings:
        user = _authorization_user(authorization)
        return self._configuration.save(
            {
                "emby_server_url": str(authorization["_normalized_url"]),
                "emby_api_key": str(authorization["AccessToken"]),
                "emby_user_id": str(user["Id"]),
                "emby_auth_mode": "user_token",
            }
        )

    def _post_authorization(
        self,
        settings: Settings,
    ) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
        submission = self._post_authorization_submitter.submit(settings)
        return _post_authorization_projection(submission)

    def _start_session(
        self,
        user: Mapping[str, Any],
        session: SessionState,
        *,
        expected_revision: int,
    ) -> bool:
        user_id = str(user.get("Id") or "").strip()
        if not user_id:
            return False
        try:
            if not self.repository.upsert_user(user, expected_revision=expected_revision):
                return False
            if not self.repository.is_active(user_id):
                return False
            is_admin = self.repository.is_admin(user_id)
        except Exception:
            logger.exception("Login-time Emby user synchronization failed")
            return False
        session.clear()
        session.update(
            {
                "_permanent": True,
                "emby_user_id": user_id,
                "emby_username": user.get("Name"),
                "emby_is_admin": is_admin,
            }
        )
        return True


def _post_authorization_projection(
    submission: PostAuthorizationSubmission,
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    job_id = submission.job_id
    if submission.status == "succeeded":
        result = dict(submission.result or {})
        plugin_value = result.get("plugin_install")
        library_value = result.get("library_config")
        plugin_install = (
            dict(plugin_value)
            if isinstance(plugin_value, Mapping)
            else {
                "ok": False,
                "status": "succeeded",
                "message": "插件任务已完成，但持久化结果缺少插件状态",
            }
        )
        library_config = (
            dict(library_value)
            if isinstance(library_value, Mapping)
            else {
                "ok": False,
                "status": "succeeded",
                "message": "插件任务已完成，但持久化结果缺少媒体库状态",
            }
        )
        plugin_install.setdefault("job_id", job_id)
        library_config.setdefault("job_id", job_id)
        return plugin_install, library_config

    if submission.status in {"queued", "running", "retry_wait", "cancel_requested"}:
        plugin_message = "ETK 插件安装任务已恢复" if submission.resumed else "ETK 插件安装任务已提交"
        plugin_install = {
            "ok": False,
            "status": "pending",
            "message": plugin_message,
            "job_id": job_id,
        }
        library_config = {
            "ok": False,
            "status": "pending",
            "message": "媒体库加速配置等待插件安装完成",
            "job_id": job_id,
        }
        return plugin_install, library_config

    error = dict(submission.error or {})
    message = str(error.get("message") or "插件与媒体库配置任务未完成")
    if submission.error_type == "PersistentEffectIndeterminate":
        message = f"{message}；外部写入结果未知，未自动重放"
    plugin_install = {
        "ok": False,
        "status": submission.status,
        "message": message,
        "job_id": job_id,
    }
    library_config = {
        "ok": False,
        "status": submission.status,
        "message": "媒体库加速配置任务未完成",
        "job_id": job_id,
    }
    return plugin_install, library_config


def _authorization_user(authorization: Mapping[str, Any]) -> dict[str, Any]:
    user = authorization.get("User")
    return dict(user) if isinstance(user, Mapping) else {}


def _user_policy(user: Mapping[str, Any]) -> Mapping[str, Any]:
    policy = user.get("Policy")
    return policy if isinstance(policy, Mapping) else {}


def _authorization_message(
    prefix: str,
    plugin_install: Mapping[str, Any],
    library_config: Mapping[str, Any],
) -> str:
    if plugin_install.get("status") == "pending" or library_config.get("status") == "pending":
        return f"{prefix}，ETK 插件与媒体库配置任务已提交"
    if not plugin_install.get("ok"):
        return f"{prefix}；ETK 插件自动安装失败：{plugin_install.get('message', '未知错误')}"
    message = f"{prefix}，{plugin_install.get('message', 'ETK 插件已就绪')}"
    if library_config.get("ok"):
        return f"{message}，{library_config.get('message', '媒体库加速配置已应用')}"
    return f"{message}；媒体库加速配置失败：{library_config.get('message', '未知错误')}"
