from __future__ import annotations

import hashlib
import socket
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID, uuid4

from pydantic import SecretStr

from etk.platform import Settings

from .effect_execution import (
    IdentityMutationIdempotencyConflictError,
    IdentityUserMutationCoordinator,
    RegistrationSagaCoordinator,
)
from .effect_ports import IdentityEmbyMutationPort, RegistrationSecretPort
from .operation_repository import IdentityOperationScopeBusyError, IdentityOperationStateError
from .operations import (
    IdentityOperation,
    IdentityOperationKind,
    IdentityOperationStatus,
    IdentityOperationStep,
)
from .users_repository import UserTemplateNameConflictError


@dataclass(frozen=True, slots=True)
class IdentityEffectsHttpResult:
    payload: Any
    status_code: int = 200


class IdentityEffectsRepository(Protocol):
    def create_template(
        self,
        *,
        name: str,
        description: str | None,
        policy: Mapping[str, Any],
        default_expiration_days: int,
        source_emby_user_id: str,
        configuration: Mapping[str, Any] | None,
        allow_unrestricted_subscriptions: bool,
        personal_play_account_only: bool,
        max_concurrent_streams: int,
    ) -> int: ...

    def template_effect(self, template_id: int) -> dict[str, Any] | None: ...

    def managed_user_projection(self) -> tuple[set[str], dict[str, dict[str, Any]]]: ...

    def managed_user_exists(self, user_id: str) -> bool: ...

    def adopt_remote_user(self, user: Mapping[str, Any]) -> bool: ...

    def profile_image_tag(self, user_id: str) -> str | None: ...


class IdentityEffectsService:
    def __init__(
        self,
        *,
        registration: RegistrationSagaCoordinator,
        mutations: IdentityUserMutationCoordinator,
        secrets: RegistrationSecretPort,
        repository: IdentityEffectsRepository,
        emby: IdentityEmbyMutationPort,
        settings: Callable[[], Settings],
        worker_id: str | None = None,
        lease_seconds: float = 90,
    ) -> None:
        self._registration = registration
        self._mutations = mutations
        self._secrets = secrets
        self._repository = repository
        self._emby = emby
        self._settings = settings
        self._worker_id = str(worker_id or f"identity-api:{socket.gethostname()}").strip()
        self._lease_seconds = float(lease_seconds)
        if not self._worker_id or self._lease_seconds <= 0:
            raise ValueError("identity effects service requires a worker id and positive lease")

    def create_template(self, payload: Any) -> IdentityEffectsHttpResult:
        if not isinstance(payload, Mapping):
            return IdentityEffectsHttpResult(
                {"status": "error", "message": "模板名称和源用户ID不能为空"},
                400,
            )
        name = str(payload.get("name") or "").strip()
        source_user_id = str(payload.get("source_emby_user_id") or "").strip()
        if not name or not source_user_id:
            return IdentityEffectsHttpResult(
                {"status": "error", "message": "模板名称和源用户ID不能为空"},
                400,
            )
        try:
            source = self._emby.get_user(source_user_id)
        except Exception:
            return IdentityEffectsHttpResult(
                {"status": "error", "message": "无法获取源用户的权限策略"},
                500,
            )
        policy = source.get("Policy") if isinstance(source, Mapping) else None
        if not isinstance(policy, Mapping):
            return IdentityEffectsHttpResult(
                {"status": "error", "message": "无法获取源用户的权限策略"},
                404,
            )
        raw_configuration = source.get("Configuration") if isinstance(source, Mapping) else None
        configuration = (
            dict(raw_configuration)
            if bool(payload.get("include_configuration")) and isinstance(raw_configuration, Mapping)
            else None
        )
        try:
            template_id = self._repository.create_template(
                name=name,
                description=_optional_text(payload.get("description")),
                policy=dict(policy),
                default_expiration_days=int(payload.get("default_expiration_days", 30)),
                source_emby_user_id=source_user_id,
                configuration=configuration,
                allow_unrestricted_subscriptions=bool(payload.get("allow_unrestricted_subscriptions", False)),
                personal_play_account_only=bool(payload.get("personal_play_account_only", False)),
                max_concurrent_streams=max(0, int(payload.get("max_concurrent_streams") or 0)),
            )
        except UserTemplateNameConflictError:
            return IdentityEffectsHttpResult({"status": "error", "message": "模板名称已被占用"}, 409)
        except (TypeError, ValueError):
            return IdentityEffectsHttpResult({"status": "error", "message": "模板参数无效"}, 400)
        except Exception:
            return IdentityEffectsHttpResult({"status": "error", "message": "模板创建失败"}, 500)
        return IdentityEffectsHttpResult(
            {"status": "ok", "message": "模板创建成功", "id": template_id},
            201,
        )

    def list_managed_users(self) -> IdentityEffectsHttpResult:
        try:
            remote_users = self._emby.list_users()
            source_ids, extended = self._repository.managed_user_projection()
        except Exception:
            return IdentityEffectsHttpResult({"error": "无法从 Emby 获取用户列表"}, 500)
        users: list[dict[str, Any]] = []
        for raw_user in remote_users:
            user = dict(raw_user)
            user_id = str(user.get("Id") or "").strip()
            if not user_id or user_id in source_ids:
                continue
            details = extended.get(user_id, {})
            policy = user.get("Policy")
            user["IsDisabled"] = bool(isinstance(policy, Mapping) and policy.get("IsDisabled", False))
            user["expiration_date"] = details.get("expiration_date")
            user["status_in_db"] = details.get("status")
            user["template_id"] = details.get("template_id")
            user["template_name"] = details.get("template_name")
            user["max_concurrent_streams"] = int(details.get("max_concurrent_streams") or 0)
            users.append(user)
        return IdentityEffectsHttpResult(users)

    def operation_status(
        self,
        operation_id: str,
        *,
        actor_user_id: str | None,
        is_admin: bool,
        invitation_token: str | None = None,
    ) -> IdentityEffectsHttpResult:
        try:
            parsed_operation_id = UUID(str(operation_id))
        except (TypeError, ValueError):
            return IdentityEffectsHttpResult({"status": "error", "message": "操作ID无效"}, 400)
        operation: IdentityOperation | None = None
        try:
            operation = self._mutations.read(parsed_operation_id)
        except Exception:
            pass
        if operation is None:
            try:
                operation = self._registration.read(parsed_operation_id)
            except Exception:
                return IdentityEffectsHttpResult({"status": "error", "message": "读取操作状态失败"}, 500)
        if operation is None:
            return IdentityEffectsHttpResult({"status": "error", "message": "操作不存在"}, 404)
        actor = _optional_text(actor_user_id)
        authorized = bool(is_admin) or (actor is not None and str(operation.request.get("user_id") or "") == actor)
        if not authorized and operation.kind is IdentityOperationKind.REGISTER:
            try:
                authorized = self._registration.registration_token_matches(
                    operation.id,
                    str(invitation_token or ""),
                )
            except Exception:
                return IdentityEffectsHttpResult({"status": "error", "message": "读取操作状态失败"}, 500)
        if not authorized:
            return IdentityEffectsHttpResult({"status": "error", "message": "无权读取该操作"}, 403)
        payload = _operation_status_payload(operation)
        if operation.status is IdentityOperationStatus.CONFIRMED:
            result = _safe_operation_result(operation, self._settings())
            if result:
                payload["operation"]["result"] = result
        return IdentityEffectsHttpResult(payload)

    def assign_template(
        self,
        user_id: str,
        payload: Any,
        *,
        idempotency_key: str | None = None,
    ) -> IdentityEffectsHttpResult:
        if not isinstance(payload, Mapping) or not payload.get("template_id"):
            return IdentityEffectsHttpResult(
                {"status": "error", "message": "必须提供新的模板ID"},
                400,
            )
        try:
            template_id = int(payload["template_id"])
        except (TypeError, ValueError):
            return IdentityEffectsHttpResult({"status": "error", "message": "模板参数无效"}, 400)
        replay = self._replay_idempotent_mutation(
            kind=IdentityOperationKind.ASSIGN_TEMPLATE,
            user_id=user_id,
            idempotency_key=idempotency_key,
            expected_request={"template_id": template_id},
            success_message="用户模板已成功切换并应用新配置",
            failure_message="切换模板失败",
        )
        if replay is not None:
            return replay
        denial = self._managed_user_denial(user_id, failure_message="切换模板失败")
        if denial is not None:
            return denial
        try:
            template = self._repository.template_effect(template_id)
        except Exception:
            return IdentityEffectsHttpResult({"status": "error", "message": "切换模板失败"}, 500)
        if template is None:
            return IdentityEffectsHttpResult({"status": "error", "message": "模板不存在"}, 404)
        request_id = _request_id(idempotency_key)
        try:
            reservation = self._mutations.reserve(
                kind=IdentityOperationKind.ASSIGN_TEMPLATE,
                user_id=user_id,
                request={
                    "request_id": request_id,
                    "template_id": template_id,
                    "snapshot_pending": True,
                },
                idempotency_key=idempotency_key,
                outbox_delay_seconds=self._lease_seconds,
            )
        except (IdentityMutationIdempotencyConflictError, IdentityOperationScopeBusyError):
            return IdentityEffectsHttpResult(
                {"status": "error", "message": "用户已有进行中或待确认的变更"},
                409,
            )
        except LookupError:
            return IdentityEffectsHttpResult({"status": "error", "message": "模板不存在"}, 404)
        except Exception:
            return IdentityEffectsHttpResult({"status": "error", "message": "切换模板失败"}, 500)
        if not reservation.created:
            return self._execute_reserved_mutation(
                reservation.operation,
                success_message="用户模板已成功切换并应用新配置",
                failure_message="切换模板失败",
            )
        try:
            remote = self._emby.get_user(user_id)
        except Exception:
            self._reject_prepared(reservation.operation.id, "template_snapshot_unavailable")
            return IdentityEffectsHttpResult({"status": "error", "message": "切换模板失败"}, 500)
        if remote is None:
            self._reject_prepared(reservation.operation.id, "template_user_not_found")
            return IdentityEffectsHttpResult({"status": "error", "message": "用户不存在"}, 404)
        policy = dict(template.get("emby_policy_json") or {})
        previous_policy = remote.get("Policy")
        previous_configuration = remote.get("Configuration")
        if not isinstance(previous_policy, Mapping):
            self._reject_prepared(reservation.operation.id, "template_policy_snapshot_unavailable")
            return IdentityEffectsHttpResult({"status": "error", "message": "切换模板失败"}, 500)
        template_configuration = template.get("emby_configuration_json")
        if isinstance(template_configuration, Mapping) and not isinstance(previous_configuration, Mapping):
            self._reject_prepared(reservation.operation.id, "template_configuration_snapshot_unavailable")
            return IdentityEffectsHttpResult({"status": "error", "message": "切换模板失败"}, 500)
        if "IsDisabled" in previous_policy:
            policy["IsDisabled"] = bool(previous_policy["IsDisabled"])
        request = {
            "user_id": user_id,
            "request_id": request_id,
            "template_id": template_id,
            "template_name": str(template.get("name") or ""),
            "template_policy": policy,
            "template_configuration": (
                dict(template_configuration) if isinstance(template_configuration, Mapping) else None
            ),
            "previous_policy": dict(previous_policy),
            "previous_configuration": (
                dict(previous_configuration) if isinstance(previous_configuration, Mapping) else {}
            ),
        }
        try:
            finalized = self._mutations.finalize_request(reservation.operation.id, request)
        except Exception:
            self._reject_prepared(reservation.operation.id, "template_snapshot_finalize_failed")
            return IdentityEffectsHttpResult({"status": "error", "message": "切换模板失败"}, 500)
        return self._execute_reserved_mutation(
            finalized,
            success_message="用户模板已成功切换并应用新配置",
            failure_message="切换模板失败",
        )

    def set_status(
        self,
        user_id: str,
        payload: Any,
        *,
        idempotency_key: str | None = None,
    ) -> IdentityEffectsHttpResult:
        if not isinstance(payload, Mapping):
            return IdentityEffectsHttpResult({"status": "error", "message": "更新用户状态失败"}, 400)
        disable = bool(payload.get("disable", False))
        replay = self._replay_idempotent_mutation(
            kind=IdentityOperationKind.SET_STATUS,
            user_id=user_id,
            idempotency_key=idempotency_key,
            expected_request={"disable": disable},
            success_message="用户状态已更新",
            failure_message="更新用户状态失败",
        )
        if replay is not None:
            return replay
        denial = self._managed_user_denial(user_id, failure_message="更新用户状态失败")
        if denial is not None:
            return denial
        request_id = _request_id(idempotency_key)
        try:
            reservation = self._mutations.reserve(
                kind=IdentityOperationKind.SET_STATUS,
                user_id=user_id,
                request={
                    "request_id": request_id,
                    "disable": disable,
                    "snapshot_pending": True,
                },
                idempotency_key=idempotency_key,
                outbox_delay_seconds=self._lease_seconds,
            )
        except (IdentityMutationIdempotencyConflictError, IdentityOperationScopeBusyError):
            return IdentityEffectsHttpResult(
                {"status": "error", "message": "用户已有进行中或待确认的变更"},
                409,
            )
        except Exception:
            return IdentityEffectsHttpResult({"status": "error", "message": "更新用户状态失败"}, 500)
        if not reservation.created:
            return self._execute_reserved_mutation(
                reservation.operation,
                success_message="用户状态已更新",
                failure_message="更新用户状态失败",
            )
        try:
            remote = self._emby.get_user(user_id)
        except Exception:
            self._reject_prepared(reservation.operation.id, "status_snapshot_unavailable")
            return IdentityEffectsHttpResult({"status": "error", "message": "更新用户状态失败"}, 500)
        if remote is None:
            self._reject_prepared(reservation.operation.id, "status_user_not_found")
            return IdentityEffectsHttpResult({"status": "error", "message": "用户不存在"}, 404)
        previous_policy = remote.get("Policy")
        if not isinstance(previous_policy, Mapping):
            self._reject_prepared(reservation.operation.id, "status_policy_unavailable")
            return IdentityEffectsHttpResult({"status": "error", "message": "更新用户状态失败"}, 500)
        desired_policy = dict(previous_policy)
        desired_policy["IsDisabled"] = disable
        request = {
            "user_id": user_id,
            "request_id": request_id,
            "disable": disable,
            "desired_policy": desired_policy,
            "previous_policy": dict(previous_policy),
        }
        try:
            finalized = self._mutations.finalize_request(reservation.operation.id, request)
        except Exception:
            self._reject_prepared(reservation.operation.id, "status_snapshot_finalize_failed")
            return IdentityEffectsHttpResult({"status": "error", "message": "更新用户状态失败"}, 500)
        return self._execute_reserved_mutation(
            finalized,
            success_message="用户状态已更新",
            failure_message="更新用户状态失败",
        )

    def delete_user(
        self,
        user_id: str,
        *,
        idempotency_key: str | None = None,
    ) -> IdentityEffectsHttpResult:
        replay = self._replay_idempotent_mutation(
            kind=IdentityOperationKind.DELETE_USER,
            user_id=user_id,
            idempotency_key=idempotency_key,
            expected_request={},
            success_message="用户已彻底删除",
            failure_message="删除用户失败",
        )
        if replay is not None:
            return replay
        denial = self._managed_user_denial(user_id, failure_message="删除用户失败")
        if denial is not None:
            return denial
        return self._run_mutation(
            kind=IdentityOperationKind.DELETE_USER,
            user_id=user_id,
            request={"request_id": _request_id(idempotency_key)},
            idempotency_key=idempotency_key,
            success_message="用户已彻底删除",
            failure_message="删除用户失败",
        )

    def upload_avatar(
        self,
        user_id: str,
        *,
        content: bytes,
        content_type: str,
        idempotency_key: str | None = None,
    ) -> IdentityEffectsHttpResult:
        if not content:
            return IdentityEffectsHttpResult({"status": "error", "message": "未找到文件"}, 400)
        if len(content) > 5 * 1024 * 1024:
            return IdentityEffectsHttpResult(
                {"status": "error", "message": "图片大小不能超过 5MB"},
                400,
            )
        normalized_content_type = str(content_type or "").partition(";")[0].strip().casefold()
        if normalized_content_type not in {"image/jpeg", "image/png", "image/webp"}:
            return IdentityEffectsHttpResult({"status": "error", "message": "图片格式不受支持"}, 400)
        denial = self._managed_user_denial(user_id, failure_message="上传到 Emby 服务器失败")
        if denial is not None:
            return denial
        try:
            reservation = self._mutations.reserve_avatar(
                user_id=user_id,
                content=content,
                content_type=normalized_content_type,
                previous_tag=self._repository.profile_image_tag(user_id),
                idempotency_key=_optional_text(idempotency_key),
            )
        except (
            IdentityMutationIdempotencyConflictError,
            IdentityOperationScopeBusyError,
            IdentityOperationStateError,
        ):
            return IdentityEffectsHttpResult(
                {"status": "error", "message": "用户已有进行中、待确认或冲突的头像变更"},
                409,
            )
        except ValueError:
            return IdentityEffectsHttpResult({"status": "error", "message": "图片格式不受支持"}, 400)
        except Exception:
            return IdentityEffectsHttpResult({"status": "error", "message": "上传到 Emby 服务器失败"}, 500)
        try:
            execution = self._mutations.execute(
                reservation.operation.id,
                worker_id=self._worker_id,
                lease_seconds=self._lease_seconds,
            )
        except Exception:
            execution = None
        try:
            operation = execution.operation if execution is not None else self._mutations.read(reservation.operation.id)
        except Exception:
            operation = None
        if operation is None:
            return IdentityEffectsHttpResult({"status": "error", "message": "上传到 Emby 服务器失败"}, 500)
        if not operation.terminal:
            return _pending_operation_result(operation, "头像变更已提交，正在等待 Emby 确认")
        if operation.status is not IdentityOperationStatus.CONFIRMED:
            return _failed_operation_result(operation, "上传到 Emby 服务器失败")
        result = operation.result or {}
        return IdentityEffectsHttpResult(
            {
                "status": "ok",
                "message": "头像上传成功",
                "new_tag": result.get("new_tag"),
            }
        )

    def register_with_invitation(
        self,
        *,
        username: str,
        password: SecretStr,
        invitation_token: str,
    ) -> IdentityEffectsHttpResult:
        normalized_username = str(username or "").strip()
        normalized_token = str(invitation_token or "").strip()
        if not normalized_username or not password.get_secret_value() or not normalized_token:
            return IdentityEffectsHttpResult(
                {"status": "error", "message": "用户名、密码和邀请码不能为空"},
                400,
            )
        try:
            reservation = self._registration.reserve(
                invitation_token=normalized_token,
                username=normalized_username,
            )
        except IdentityOperationScopeBusyError:
            return IdentityEffectsHttpResult(
                {"status": "error", "message": "该用户名正在注册或状态尚未确认"},
                409,
            )
        except LookupError:
            return IdentityEffectsHttpResult(
                {"status": "error", "message": "邀请链接无效或已被使用"},
                400,
            )
        except Exception:
            return IdentityEffectsHttpResult(
                {"status": "error", "message": "注册过程中发生服务器内部错误，请联系管理员。"},
                500,
            )

        try:
            with self._secrets.lease(password) as secret:
                execution = self._registration.execute_with_secret(
                    reservation.operation.id,
                    secret,
                    worker_id=self._worker_id,
                    lease_seconds=self._lease_seconds,
                )
        except Exception:
            execution = None
        try:
            operation = (
                execution.operation if execution is not None else self._registration.read(reservation.operation.id)
            )
        except Exception:
            operation = None
        if operation is None:
            return IdentityEffectsHttpResult(
                {"status": "error", "message": "注册状态暂时无法确认，请联系管理员"},
                500,
            )
        if operation.status is IdentityOperationStatus.CONFIRMED and operation.result is not None:
            settings = self._settings()
            redirect_url = settings.emby_public_url.strip() or settings.emby_server_url.strip()
            return IdentityEffectsHttpResult(
                {
                    "status": "ok",
                    "message": "注册成功！",
                    "data": {
                        "username": str(operation.result.get("username") or normalized_username),
                        "expiration_info": str(operation.result.get("expiration_info") or "永久有效"),
                        "redirect_url": redirect_url,
                        "template_description": str(operation.result.get("template_description") or ""),
                    },
                },
                201,
            )
        error_type = str((operation.error or {}).get("type") or "")
        if error_type == "username_conflict":
            return _failed_operation_result(operation, "该用户名已被占用", status_code=409)
        if not operation.terminal:
            requires_resubmission = not str(
                operation.state.get("remote_user_id") or ""
            ).strip() and operation.current_step in {
                IdentityOperationStep.RESERVED,
                IdentityOperationStep.PREFLIGHT_USERNAME,
            }
            return _pending_operation_result(
                operation,
                (
                    "注册暂未完成，请使用相同邀请码重新提交以继续"
                    if requires_resubmission
                    else "注册已提交，正在等待 Emby 确认"
                ),
                requires_resubmission=requires_resubmission,
            )
        if operation.status is IdentityOperationStatus.INDETERMINATE:
            return _failed_operation_result(
                operation,
                "注册状态暂时无法确认，请联系管理员",
            )
        return _failed_operation_result(
            operation,
            "注册失败，未保留未完成的 Emby 用户",
        )

    def _run_mutation(
        self,
        *,
        kind: IdentityOperationKind,
        user_id: str,
        request: Mapping[str, Any],
        idempotency_key: str | None,
        success_message: str,
        failure_message: str,
    ) -> IdentityEffectsHttpResult:
        try:
            reservation = self._mutations.reserve(
                kind=kind,
                user_id=user_id,
                request=request,
                idempotency_key=_optional_text(idempotency_key),
            )
        except (IdentityMutationIdempotencyConflictError, IdentityOperationScopeBusyError):
            return IdentityEffectsHttpResult(
                {"status": "error", "message": "用户已有进行中或待确认的变更"},
                409,
            )
        except Exception:
            return IdentityEffectsHttpResult({"status": "error", "message": failure_message}, 500)
        try:
            execution = self._mutations.execute(
                reservation.operation.id,
                worker_id=self._worker_id,
                lease_seconds=self._lease_seconds,
            )
        except Exception:
            execution = None
        try:
            operation = execution.operation if execution is not None else self._mutations.read(reservation.operation.id)
        except Exception:
            operation = None
        return self._mutation_operation_result(
            operation,
            success_message=success_message,
            failure_message=failure_message,
        )

    def _managed_user_denial(
        self,
        user_id: str,
        *,
        failure_message: str,
    ) -> IdentityEffectsHttpResult | None:
        try:
            if self._repository.managed_user_exists(user_id):
                return None
            remote = self._emby.get_user(user_id)
            if remote is None:
                return IdentityEffectsHttpResult({"status": "error", "message": "用户不存在"}, 404)
            if not self._repository.adopt_remote_user(remote):
                return IdentityEffectsHttpResult(
                    {"status": "error", "message": "该用户的注册仍在进行中或状态尚未确认"},
                    409,
                )
            return None
        except Exception:
            return IdentityEffectsHttpResult({"status": "error", "message": failure_message}, 500)

    def _execute_reserved_mutation(
        self,
        operation: IdentityOperation,
        *,
        success_message: str,
        failure_message: str,
    ) -> IdentityEffectsHttpResult:
        if operation.request.get("snapshot_pending") is True:
            return _pending_operation_result(
                operation,
                "用户变更已保留，正在获取写入前快照",
            )
        try:
            execution = self._mutations.execute(
                operation.id,
                worker_id=self._worker_id,
                lease_seconds=self._lease_seconds,
            )
        except Exception:
            execution = None
        try:
            current = execution.operation if execution is not None else self._mutations.read(operation.id)
        except Exception:
            current = None
        return self._mutation_operation_result(
            current,
            success_message=success_message,
            failure_message=failure_message,
        )

    @staticmethod
    def _mutation_operation_result(
        operation: IdentityOperation | None,
        *,
        success_message: str,
        failure_message: str,
    ) -> IdentityEffectsHttpResult:
        if operation is None:
            return IdentityEffectsHttpResult({"status": "error", "message": failure_message}, 500)
        if operation.status is IdentityOperationStatus.CONFIRMED:
            return IdentityEffectsHttpResult({"status": "ok", "message": success_message})
        if not operation.terminal:
            return _pending_operation_result(operation, f"{success_message}请求已提交，正在等待 Emby 确认")
        return _failed_operation_result(operation, failure_message)

    def _reject_prepared(self, operation_id: UUID, error_type: str) -> None:
        try:
            self._mutations.reject_ready(operation_id, error_type=error_type)
        except Exception:
            pass

    def _replay_idempotent_mutation(
        self,
        *,
        kind: IdentityOperationKind,
        user_id: str,
        idempotency_key: str | None,
        expected_request: Mapping[str, Any],
        success_message: str,
        failure_message: str,
    ) -> IdentityEffectsHttpResult | None:
        normalized_key = _optional_text(idempotency_key)
        if normalized_key is None:
            return None
        try:
            existing = self._mutations.find_idempotent(
                kind=kind,
                user_id=user_id,
                idempotency_key=normalized_key,
            )
        except Exception:
            return IdentityEffectsHttpResult({"status": "error", "message": failure_message}, 500)
        if existing is None:
            return None
        if any(existing.request.get(key) != value for key, value in expected_request.items()):
            return IdentityEffectsHttpResult(
                {"status": "error", "message": "Idempotency-Key 已用于不同请求"},
                409,
            )
        if existing.request.get("snapshot_pending") is True:
            return IdentityEffectsHttpResult(
                {"status": "error", "message": "用户已有进行中或待确认的变更"},
                409,
            )
        try:
            execution = self._mutations.execute(
                existing.id,
                worker_id=self._worker_id,
                lease_seconds=self._lease_seconds,
            )
        except Exception:
            execution = None
        try:
            operation = execution.operation if execution is not None else self._mutations.read(existing.id)
        except Exception:
            operation = None
        return self._mutation_operation_result(
            operation,
            success_message=success_message,
            failure_message=failure_message,
        )


def _optional_text(value: object) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _request_id(idempotency_key: str | None) -> str:
    normalized = _optional_text(idempotency_key)
    if normalized is None:
        return str(uuid4())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _operation_status_payload(operation: IdentityOperation) -> dict[str, Any]:
    error_type = str((operation.error or {}).get("type") or "") or None
    return {
        "status": (
            "ok"
            if operation.status is IdentityOperationStatus.CONFIRMED
            else ("error" if operation.terminal else "pending")
        ),
        "operation": {
            "id": str(operation.id),
            "kind": operation.kind.value,
            "operation_status": operation.status.value,
            "terminal": operation.terminal,
            "error_type": error_type,
        },
    }


def _pending_operation_result(
    operation: IdentityOperation,
    message: str,
    *,
    requires_resubmission: bool = False,
) -> IdentityEffectsHttpResult:
    payload = _operation_status_payload(operation)
    payload["status"] = "pending"
    payload["message"] = message
    payload["operation"]["status_url"] = f"/api/identity/operations/{operation.id}/status"
    payload["operation"]["requires_resubmission"] = requires_resubmission
    return IdentityEffectsHttpResult(payload, 503)


def _failed_operation_result(
    operation: IdentityOperation,
    message: str,
    *,
    status_code: int = 500,
) -> IdentityEffectsHttpResult:
    payload = _operation_status_payload(operation)
    payload["status"] = "error"
    payload["message"] = message
    payload["operation"]["status_url"] = f"/api/identity/operations/{operation.id}/status"
    return IdentityEffectsHttpResult(payload, status_code)


def _safe_operation_result(operation: IdentityOperation, settings: Settings) -> dict[str, Any]:
    result = dict(operation.result or {})
    if operation.kind is IdentityOperationKind.REGISTER:
        redirect_url = settings.emby_public_url.strip() or settings.emby_server_url.strip()
        return {
            "username": str(result.get("username") or operation.request.get("username") or ""),
            "expiration_info": str(result.get("expiration_info") or "永久有效"),
            "template_description": str(result.get("template_description") or ""),
            "redirect_url": redirect_url,
        }
    if operation.kind is IdentityOperationKind.UPLOAD_AVATAR:
        return {"new_tag": result.get("new_tag")}
    return {"message": str(result.get("message") or "操作已确认")}


__all__ = ["IdentityEffectsHttpResult", "IdentityEffectsService"]
