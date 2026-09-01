from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Protocol, cast

from etk.domains.sharing.execution import SharingDeliveryOutcome, SharingOperationDeliveryPort
from etk.domains.sharing.operations import SharingOperation, SharingOperationKind, sharing_operation_key
from etk.domains.sharing.service import SharingHttpResult, SharingService
from etk.integrations import (
    HDHivePort,
    ProviderDisposition,
    ProviderResponse,
    SharedCenterPort,
    TelegramUserBotCommandFailedError,
    TelegramUserBotCommandIndeterminateError,
    TelegramUserBotCommandTimeoutError,
    TelegramUserBotIdempotencyConflictError,
    TelegramUserBotPort,
)
from etk.platform.jobs import normalize_json_object

from .hdhive_policy import filter_hdhive_resources, normalize_filter_config, normalize_hdhive_media_type

PROVIDER_CONTROL_USE_CASE = "subscription.provider-control"

_HDHIVE_SETTING_KEY = "hdhive_config"
_TELEGRAM_SETTING_KEY = "tg_userbot_config"
_TELEGRAM_PASSWORD_MASK = "********"
_CHANNEL_SECRET_TTL_SECONDS = 7 * 24 * 60 * 60
_SUPPORTED_SHARED_SOURCE_KINDS = frozenset({"movie", "episode", "logical_episode", "logical_season", "season_hub"})
_TELEGRAM_DEFAULT_CONFIG: dict[str, Any] = {
    "enabled": False,
    "phone": "",
    "password": "",
    "channels": [],
    "monitor_types": ["movie", "tv"],
    "transfer_modes": ["subscribe"],
    "transfer_mode_channels": {},
    "transfer_keywords": [],
    "block_keywords": [],
}


class ProviderSettingsPort(Protocol):
    def load(self, key: str) -> Any: ...

    def save(self, key: str, value: Any) -> None: ...

    def merge_mapping(
        self,
        key: str,
        updates: Mapping[str, Any],
        *,
        remove_keys: Sequence[str] = (),
    ) -> Mapping[str, Any]: ...


class HDHiveRegistrationPort(Protocol):
    def register(self) -> ProviderResponse[dict[str, Any]]: ...


class HDHiveRegistrationOperationDelivery(SharingOperationDeliveryPort):
    """Persist relay instance credentials after a checkpointed one-shot registration."""

    def __init__(self, hdhive: HDHiveRegistrationPort, settings: ProviderSettingsPort) -> None:
        self._hdhive = hdhive
        self._settings = settings

    @staticmethod
    def supports(operation: SharingOperation) -> bool:
        request = operation.request if isinstance(operation.request, Mapping) else {}
        return request.get("provider") == "hdhive_admin" and request.get("action") == "register"

    def supports_authoritative_readback(self, operation: SharingOperation) -> bool:
        del operation
        return False

    def request(
        self,
        operation: SharingOperation,
        *,
        checkpoint: Callable[[], None],
    ) -> SharingDeliveryOutcome:
        del operation
        checkpoint()
        try:
            response = self._hdhive.register()
        except Exception as exc:
            return SharingDeliveryOutcome.indeterminate(
                {"type": "hdhive_registration_error", "error_class": type(exc).__name__}
            )
        if response.disposition is ProviderDisposition.REJECTED:
            return SharingDeliveryOutcome.rejected({"type": "hdhive_registration_rejected"})
        if response.disposition is not ProviderDisposition.CONFIRMED or not isinstance(response.payload, Mapping):
            return SharingDeliveryOutcome.indeterminate({"type": "hdhive_registration_not_confirmed"})
        data = response.payload.get("data")
        data = data if isinstance(data, Mapping) else response.payload
        instance_id = str(data.get("instance_id") or "").strip()
        instance_secret = str(data.get("instance_secret") or "").strip()
        if not instance_id or not instance_secret:
            return SharingDeliveryOutcome.indeterminate({"type": "hdhive_registration_credentials_missing"})
        try:
            self._settings.merge_mapping(
                _HDHIVE_SETTING_KEY,
                {
                    "relay_instance_id": instance_id,
                    "relay_instance_secret": instance_secret,
                },
                remove_keys=("instance_id", "instance_secret"),
            )
        except Exception as exc:
            return SharingDeliveryOutcome.indeterminate(
                {"type": "hdhive_registration_persistence_error", "error_class": type(exc).__name__}
            )
        return SharingDeliveryOutcome.confirmed({"registered": True, "instance_id_hash": _digest(instance_id)})

    def readback(self, operation: SharingOperation) -> SharingDeliveryOutcome:
        del operation
        return SharingDeliveryOutcome.indeterminate({"type": "authoritative_readback_unavailable"})


class ProviderControlService:
    """Native owner for Cloud, HDHive, and Telegram provider-control HTTP flows."""

    def __init__(
        self,
        *,
        settings: ProviderSettingsPort,
        hdhive: HDHivePort,
        center: SharedCenterPort,
        telegram: TelegramUserBotPort,
        sharing: SharingService,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._settings = settings
        self._hdhive = hdhive
        self._center = center
        self._telegram = telegram
        self._sharing = sharing
        self._clock = clock or (lambda: datetime.now(UTC))

    def hdhive_configuration(self) -> SharingHttpResult:
        config = self._hdhive_config()
        relay_status = self._safe_provider_payload(self._hdhive.ping)
        authorized = bool(relay_status.get("success") is not False and relay_status.get("has_access_token"))
        authorize_url = self._authorize_url()
        user_info: Mapping[str, Any] | None = None
        usage_today: Mapping[str, Any] | None = None
        vip_info: Mapping[str, Any] | None = None
        if authorized:
            user_info = self._provider_data(self._safe_provider_payload(self._hdhive.user_info))
            if _has_scope(relay_status, "meta"):
                usage_today = self._provider_data(self._safe_provider_payload(self._hdhive.usage_today))
            if _has_scope(relay_status, "vip") and str((user_info or {}).get("level") or "") in {
                "vip",
                "forever_vip",
                "lifetime_vip",
                "premium",
            }:
                vip_info = self._provider_data(self._safe_provider_payload(self._hdhive.vip_entitlements))
        unlock = config["unlock_limit"]
        filters = config["filter"]
        return SharingHttpResult(
            {
                "success": True,
                "authorize_url": authorize_url,
                "relay_status": relay_status or None,
                "authorized": authorized,
                "hdhive_checkin_mode": config["checkin_mode"],
                "unlock_limit_count": unlock["count"],
                "unlock_limit_window": unlock["window"],
                "hdhive_free_only": filters["free_only"],
                "hdhive_max_points": filters["max_points"],
                "hdhive_max_size_gb": filters["max_size_gb"],
                "hdhive_resolution": filters["resolution"],
                "hdhive_zh_sub_only": filters["zh_sub_only"],
                "hdhive_exclude_iso": filters["exclude_iso"],
                "user_info": dict(user_info) if user_info is not None else None,
                "usage_today": dict(usage_today) if usage_today is not None else None,
                "vip_info": dict(vip_info) if vip_info is not None else None,
            }
        )

    def telegram_configuration(self) -> SharingHttpResult:
        loaded = self._settings.load(_TELEGRAM_SETTING_KEY)
        config = {**_TELEGRAM_DEFAULT_CONFIG, **(dict(loaded) if isinstance(loaded, Mapping) else {})}
        config.pop("api_id", None)
        config.pop("api_hash", None)
        config["password"] = _TELEGRAM_PASSWORD_MASK if str(config.get("password") or "") else ""
        return SharingHttpResult({"success": True, "data": config})

    def save_telegram_configuration(self, payload: Mapping[str, Any]) -> SharingHttpResult:
        loaded = self._settings.load(_TELEGRAM_SETTING_KEY)
        current = dict(loaded) if isinstance(loaded, Mapping) else {}
        config = dict(payload)
        config.pop("api_id", None)
        config.pop("api_hash", None)
        if str(config.get("password") or "") == _TELEGRAM_PASSWORD_MASK:
            config["password"] = str(current.get("password") or "")
        self._settings.save(_TELEGRAM_SETTING_KEY, config)
        return SharingHttpResult({"success": True, "message": "频道订阅监听配置已保存生效"})

    def telegram_status(self, command_id: str = "") -> SharingHttpResult:
        normalized_command_id = str(command_id or "").strip()
        if normalized_command_id:
            try:
                command = self._telegram.get_command(normalized_command_id)
            except Exception:
                command = None
            if not isinstance(command, Mapping):
                return SharingHttpResult(
                    {
                        "success": False,
                        "accepted": False,
                        "status": "missing",
                        "command_id": normalized_command_id,
                        "message": "Telegram 操作记录不存在或已过期",
                    }
                )
            status = str(command.get("status") or "pending").strip().lower()
            active = status in {"pending", "claimed", "requesting"}
            succeeded = status == "succeeded"
            messages = {
                "succeeded": "Telegram 操作已完成",
                "failed": "Telegram 操作失败",
                "indeterminate": "Telegram 操作结果不确定，请勿直接重放",
            }
            return SharingHttpResult(
                {
                    "success": succeeded,
                    "accepted": active,
                    "status": status,
                    "command_id": normalized_command_id,
                    "message": messages.get(status, "Telegram 操作仍在处理中"),
                }
            )
        try:
            status = self._telegram.get_runtime_status()
        except Exception:
            return SharingHttpResult({"success": False, "message": "读取状态失败"}, 503)
        safe = {
            key: status[key]
            for key in ("status", "msg", "updated_at")
            if key in status and type(status[key]) in {bool, int, float, str, type(None)}
        }
        safe.setdefault("status", "stopped")
        return SharingHttpResult({"success": True, "data": safe})

    def telegram_send_code(self, *, idempotency_key: str) -> SharingHttpResult:
        return self._telegram_command(
            "send_code",
            {},
            idempotency_key=idempotency_key,
            success_message="验证码已发送到您的 Telegram 客户端",
        )

    def telegram_login(self, code: object, *, idempotency_key: str) -> SharingHttpResult:
        normalized_code = str(code or "").strip()
        if not normalized_code:
            return SharingHttpResult({"success": False, "message": "请输入验证码"})
        occurrence = f"telegram-control:login:{idempotency_key}"
        owner_key = _digest(occurrence)
        secret = {"code": normalized_code}
        reference = _json_hash({"kind": "telegram-login-code", "owner_key": owner_key})
        try:
            _reference, created = self._telegram.create_resource_secret(
                secret,
                reference=reference,
                owner_key=owner_key,
                ttl_seconds=10 * 60,
            )
        except RuntimeError:
            return SharingHttpResult(
                {"success": False, "status": "conflict", "message": "幂等键已用于其他 Telegram 登录请求"},
                409,
            )
        result = self._telegram_command(
            "login",
            {"code_ref": reference, "code_hash": _json_hash(secret)},
            idempotency_key=idempotency_key,
            success_message="登录成功！监听服务已启动。",
        )
        status = str(result.payload.get("status") or "") if isinstance(result.payload, Mapping) else ""
        if created and status not in {"pending", "claimed", "requesting"}:
            self._telegram.destroy_resource_secret(reference, owner_key=owner_key)
        return result

    def telegram_logout(self, *, idempotency_key: str) -> SharingHttpResult:
        return self._telegram_command(
            "logout",
            {},
            idempotency_key=idempotency_key,
            success_message="已注销并清除凭证",
        )

    def telegram_clear_session(self, *, idempotency_key: str) -> SharingHttpResult:
        result = self._telegram_command(
            "clear_session",
            {},
            idempotency_key=idempotency_key,
            success_message="未发现需要清理的 TG 登录会话文件",
        )
        if not isinstance(result.payload, Mapping) or not result.payload.get("success"):
            return result
        payload = dict(result.payload)
        data = payload.get("data")
        if isinstance(data, Mapping):
            deleted = [str(value) for value in data.get("deleted", []) if str(value).strip()]
            if deleted:
                payload["message"] = f"已清理 TG 登录会话文件：{'、'.join(deleted)}"
        return SharingHttpResult(payload, result.status_code, result.headers)

    def save_hdhive_configuration(self, payload: Mapping[str, Any]) -> SharingHttpResult:
        old = self._hdhive_config()
        mode = str(payload.get("hdhive_checkin_mode") or "normal").strip()
        filters = normalize_filter_config(
            {
                "free_only": payload.get("hdhive_free_only", False),
                "max_points": payload.get("hdhive_max_points", 10),
                "max_size_gb": payload.get("hdhive_max_size_gb", 120),
                "resolution": payload.get("hdhive_resolution", "All"),
                "zh_sub_only": payload.get("hdhive_zh_sub_only", True),
                "exclude_iso": payload.get("hdhive_exclude_iso", False),
            }
        )
        saved = {
            "relay_base_url": old["relay_base_url"],
            "relay_instance_id": old["relay_instance_id"],
            "relay_instance_secret": old["relay_instance_secret"],
            "checkin_mode": mode if mode in {"normal", "gambler"} else "normal",
            "unlock_limit": {
                "count": _bounded_int(payload.get("unlock_limit_count"), default=3, minimum=0, maximum=100),
                "window": _bounded_int(
                    payload.get("unlock_limit_window"),
                    default=60,
                    minimum=1,
                    maximum=86_400,
                ),
            },
            "filter": filters,
        }
        self._settings.save(_HDHIVE_SETTING_KEY, saved)
        result = self.hdhive_configuration()
        body = dict(result.payload)
        body["message"] = (
            "影巢配置保存成功！" if body.get("user_info") else "筛选配置已保存。若未授权，请点击“前往影巢授权”。"
        )
        return SharingHttpResult(body, result.status_code)

    def hdhive_authorize_url(
        self,
        *,
        principal_id: str,
        idempotency_key: str,
    ) -> SharingHttpResult:
        config = self._hdhive_config()
        if not config["relay_instance_id"] or not config["relay_instance_secret"]:
            registration = self._sharing.stage_provider_operation(
                kind=SharingOperationKind.REGISTER_DEVICE,
                scope_key="hdhive-instance-registration",
                principal_id=principal_id,
                idempotency_key=idempotency_key,
                request={"provider": "hdhive_admin", "action": "register", "payload": {}},
                accepted_message="影巢中转实例注册任务已提交",
            )
            if registration.status_code != 200:
                return registration
        url = self._authorize_url()
        if not url:
            return SharingHttpResult(
                {"success": False, "message": "生成影巢授权链接失败，请查看后端日志。"},
                500,
            )
        return SharingHttpResult({"success": True, "authorize_url": url})

    def hdhive_resources(
        self,
        *,
        tmdb_id: object,
        media_type: object,
        season: object,
    ) -> SharingHttpResult:
        normalized_tmdb = str(tmdb_id or "").strip()
        if not normalized_tmdb:
            return SharingHttpResult({"success": False, "message": "缺少 TMDB ID"}, 400)
        if not self._hdhive_authorized():
            return SharingHttpResult({"success": False, "message": "请先完成影巢授权"}, 401)
        kind = normalize_hdhive_media_type(media_type)
        response = self._hdhive.get_resources(normalized_tmdb, kind, season=_optional_positive_int(season))
        raw = _response_rows(response, ("resources", "items", "results", "data"))
        filtered = filter_hdhive_resources(
            raw,
            config=self._hdhive_config()["filter"],
            target_season=season,
            media_type=kind,
        )
        return SharingHttpResult({"success": True, "data": filtered, "total": len(raw), "filtered": len(filtered)})

    def hdhive_subscriptions(
        self,
        *,
        target_type: str | None,
        status: str,
        query: str | None,
        page: int,
        page_size: int,
    ) -> SharingHttpResult:
        if not self._hdhive_authorized():
            return SharingHttpResult({"success": False, "message": "请先完成影巢授权"}, 401)
        response = self._hdhive.list_subscriptions(
            target_type=target_type,
            status=status,
            query=query,
            page=page,
            page_size=page_size,
        )
        return _provider_http_result(response, rejected_status=500)

    def hdhive_subscription_check(self, *, target_type: str, target_key: str) -> SharingHttpResult:
        if not self._hdhive_authorized():
            return SharingHttpResult({"success": False, "message": "请先完成影巢授权"}, 401)
        if target_type not in {"publisher", "media_resource"} or not target_key:
            return SharingHttpResult({"success": False, "message": "缺少订阅目标信息"}, 400)
        return _provider_http_result(
            self._hdhive.check_subscription(target_type, target_key),
            rejected_status=400,
        )

    def create_hdhive_subscription(
        self,
        payload: Mapping[str, Any],
        *,
        principal_id: str,
        idempotency_key: str,
    ) -> SharingHttpResult:
        if not self._hdhive_authorized():
            return SharingHttpResult({"success": False, "message": "请先完成影巢授权"}, 401)
        target_type = str(payload.get("target_type") or "").strip()
        target_key = str(payload.get("target_key") or "").strip()
        target_id = _optional_positive_int(payload.get("target_id"))
        filters = payload.get("media_filters")
        if target_type not in {"publisher", "media_resource"}:
            return SharingHttpResult({"success": False, "message": "订阅类型仅支持发布者或资源"}, 400)
        if not target_key or target_id is None:
            return SharingHttpResult({"success": False, "message": "缺少订阅目标信息"}, 400)
        if filters is not None and not isinstance(filters, Mapping):
            return SharingHttpResult({"success": False, "message": "media_filters 必须是对象"}, 400)
        request = {
            "provider": "hdhive",
            "action": "create_subscription",
            "payload": {
                "target_type": target_type,
                "target_id": target_id,
                "target_key": target_key,
                "media_filters": dict(filters) if isinstance(filters, Mapping) else None,
            },
            "readback": {
                "provider": "hdhive",
                "action": "check_subscription",
                "query": {"target_type": target_type, "target_key": target_key},
                "match_fields": {"target_type": target_type, "target_key": target_key},
            },
        }
        return self._sharing.stage_provider_operation(
            kind=SharingOperationKind.SHARE_REQUEST,
            scope_key=f"hdhive-subscription:{target_type}:{_digest(target_key)}",
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            request=request,
            accepted_message="影巢订阅任务已提交",
        )

    def delete_hdhive_subscription(
        self,
        subscription_id: int,
        *,
        principal_id: str,
        idempotency_key: str,
    ) -> SharingHttpResult:
        if not self._hdhive_authorized():
            return SharingHttpResult({"success": False, "message": "请先完成影巢授权"}, 401)
        if subscription_id < 1:
            return SharingHttpResult({"success": False, "message": "订阅 ID 无效"}, 400)
        return self._sharing.stage_provider_operation(
            kind=SharingOperationKind.SHARE_REQUEST,
            scope_key=f"hdhive-subscription-id:{subscription_id}",
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            request={
                "provider": "hdhive",
                "action": "delete_subscription",
                "payload": {"subscription_id": subscription_id},
            },
            accepted_message="影巢取消订阅任务已提交",
        )

    def hdhive_checkin(
        self,
        payload: Mapping[str, Any],
        *,
        principal_id: str,
        idempotency_key: str,
    ) -> SharingHttpResult:
        if not self._hdhive_authorized():
            return SharingHttpResult({"success": False, "message": "请先完成影巢授权"}, 401)
        is_gambler = (
            bool(payload.get("is_gambler"))
            if "is_gambler" in payload
            else self._hdhive_config()["checkin_mode"] == "gambler"
        )
        return self._sharing.stage_provider_operation(
            kind=SharingOperationKind.HDHIVE_CHECKIN,
            scope_key=f"hdhive-checkin:{principal_id}",
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            request={
                "provider": "hdhive",
                "action": "checkin",
                "payload": {"is_gambler": is_gambler},
            },
            accepted_message="影巢签到任务已提交",
        )

    def clear_hdhive_authorization(
        self,
        *,
        principal_id: str,
        idempotency_key: str,
    ) -> SharingHttpResult:
        return self._sharing.stage_provider_operation(
            kind=SharingOperationKind.SHARE_REQUEST,
            scope_key=f"hdhive-authorization:{principal_id}",
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            request={
                "provider": "hdhive",
                "action": "clear_authorization",
                "payload": {},
                "readback": {
                    "provider": "hdhive",
                    "action": "ping",
                    "query": {},
                    "match_fields": {"has_access_token": False},
                },
            },
            accepted_message="影巢授权清理任务已提交",
        )

    def hdhive_download(
        self,
        payload: Mapping[str, Any],
        *,
        principal_id: str,
        idempotency_key: str,
    ) -> SharingHttpResult:
        if not self._hdhive_authorized():
            return SharingHttpResult({"success": False, "message": "请先完成影巢授权"}, 401)
        slug = str(payload.get("slug") or "").strip()
        if not slug:
            return SharingHttpResult({"success": False, "message": "缺少影巢资源 slug"}, 400)
        digest = _digest(slug)
        return self._sharing.stage_provider_operation(
            kind=SharingOperationKind.TRANSFER,
            scope_key=f"cloud-transfer:hdhive:{digest}",
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            request={
                "provider": "auto_subscription",
                "action": "hdhive_transfer",
                "payload": {
                    "slug": slug,
                    "slug_hash": digest,
                    "tmdb_id": str(payload.get("tmdb_id") or ""),
                    "media_type": normalize_hdhive_media_type(payload.get("media_type")),
                },
            },
            accepted_message="已提交影巢资源转存任务",
        )

    def cloud_resources(
        self,
        *,
        query: Mapping[str, Any],
        idempotency_key: str | None,
    ) -> SharingHttpResult:
        tmdb_id = str(query.get("tmdb_id") or "").strip()
        title = _strip_season_suffix(query.get("title") or query.get("query") or "")
        year = str(query.get("year") or "").strip()
        if not tmdb_id and not title:
            return SharingHttpResult({"success": False, "message": "缺少 TMDB ID 或搜索标题"}, 400)
        media_type = normalize_hdhive_media_type(query.get("media_type"))
        season = query.get("season")
        limit = _bounded_int(query.get("limit"), default=50, minimum=1, maximum=100)
        hdhive_limit = _bounded_int(query.get("hdhive_limit"), default=limit, minimum=1, maximum=100)
        channel_limit = _bounded_int(query.get("channel_limit"), default=limit, minimum=1, maximum=100)
        shared_limit = _bounded_int(query.get("shared_limit"), default=limit, minimum=1, maximum=100)
        warnings: list[str] = []
        resources: list[dict[str, Any]] = []
        seen: set[str] = set()

        shared_total = self._append_shared_resources(
            resources,
            seen,
            tmdb_id=tmdb_id,
            title=title,
            year=year,
            media_type=media_type,
            limit=shared_limit,
            warnings=warnings,
        )
        hdhive_total, hdhive_filtered = self._append_hdhive_resources(
            resources,
            seen,
            tmdb_id=tmdb_id,
            media_type=media_type,
            season=season,
            limit=hdhive_limit,
            warnings=warnings,
        )
        channel_total = self._append_channel_resources(
            resources,
            seen,
            tmdb_id=tmdb_id,
            title=title,
            year=year,
            media_type=media_type,
            limit=channel_limit,
            idempotency_key=idempotency_key,
            warnings=warnings,
        )
        return SharingHttpResult(
            {
                "success": True,
                "data": resources[:limit],
                "total": len(resources),
                "stats": {
                    "shared_pool_total": shared_total,
                    "hdhive_total": hdhive_total,
                    "hdhive_filtered": hdhive_filtered,
                    "channel_total": channel_total,
                    "shown": min(len(resources), limit),
                    "limit": limit,
                    "warnings": warnings,
                },
            }
        )

    def cloud_download(
        self,
        payload: Mapping[str, Any],
        *,
        principal_id: str,
        idempotency_key: str,
    ) -> SharingHttpResult:
        resource_value = payload.get("resource")
        resource = dict(resource_value) if isinstance(resource_value, Mapping) else {}
        source_type = (
            str(payload.get("source_type") or resource.get("source_type") or resource.get("_cloud_source") or "")
            .strip()
            .casefold()
        )
        source_kind = str(resource.get("source_kind") or "").strip()
        if source_type in {"shared_pool", "shared", "shared_center", "center", "共享池"} or source_kind in (
            _SUPPORTED_SHARED_SOURCE_KINDS
        ):
            mode = (
                str(
                    payload.get("mode")
                    or resource.get("preferred_transfer_mode")
                    or resource.get("transfer_mode")
                    or "rapid"
                )
                .strip()
                .casefold()
            )
            if mode not in {"rapid", "share"}:
                return SharingHttpResult({"success": False, "message": "共享池转存模式必须是 rapid 或 share"}, 400)
            source_id = str(resource.get("source_id") or resource.get("source_ref_id") or "").strip()
            if not source_kind or not source_id:
                return SharingHttpResult(
                    {"success": False, "message": "共享池资源缺少 source_kind/source_id，无法转存"},
                    400,
                )
            result = self._sharing.import_center_source(
                {"source": resource, "force_real": True, "mode": mode},
                principal_id=principal_id,
                idempotency_key=idempotency_key,
            )
            return _replace_message(result, "共享池分享转存任务已提交" if mode == "share" else "共享池转存任务已提交")

        slug = str(payload.get("slug") or resource.get("slug") or "").strip()
        if source_type in {"hdhive", "hive", "影巢"} or slug:
            return self.hdhive_download(
                {**resource, **dict(payload), "slug": slug},
                principal_id=principal_id,
                idempotency_key=idempotency_key,
            )
        if (
            source_type in {"channel", "tg", "telegram", "频道"}
            or resource.get("target_link")
            or resource.get("magnet_url")
        ):
            return self._stage_channel_download(
                resource,
                principal_id=principal_id,
                idempotency_key=idempotency_key,
            )
        return SharingHttpResult({"success": False, "message": "未知资源来源，无法执行转存"}, 400)

    def _stage_channel_download(
        self,
        resource: Mapping[str, Any],
        *,
        principal_id: str,
        idempotency_key: str,
    ) -> SharingHttpResult:
        if not str(resource.get("target_link") or resource.get("magnet_url") or "").strip():
            return SharingHttpResult({"success": False, "message": "频道资源缺少可转存链接"}, 400)
        operation_owner = sharing_operation_key(
            kind=SharingOperationKind.TRANSFER,
            principal_id=principal_id,
            idempotency_key=idempotency_key,
        )
        result = {"ok": True, "results": [normalize_json_object(dict(resource), field="channel resource")]}
        result_hash = _json_hash(result)
        candidate_hash = _json_hash(result["results"][0])
        reference = _json_hash({"kind": "cloud-channel-transfer", "operation_owner": operation_owner})
        _reference, created = self._telegram.create_resource_secret(
            result,
            reference=reference,
            owner_key=operation_owner,
            ttl_seconds=_CHANNEL_SECRET_TTL_SECONDS,
        )
        staged = self._sharing.stage_provider_operation(
            kind=SharingOperationKind.TRANSFER,
            scope_key=f"cloud-transfer:telegram:{candidate_hash}",
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            request={
                "provider": "auto_subscription",
                "action": "telegram_transfer",
                "payload": {
                    "candidate_index": 0,
                    "candidate_hash": candidate_hash,
                    "search_result_ref": reference,
                    "search_result_hash": result_hash,
                },
            },
            accepted_message="已提交频道资源转存任务",
        )
        if created and staged.status_code >= 400:
            self._telegram.destroy_resource_secret(reference, owner_key=operation_owner)
        return staged

    def _telegram_command(
        self,
        command_type: str,
        payload: Mapping[str, Any],
        *,
        idempotency_key: str,
        success_message: str,
    ) -> SharingHttpResult:
        occurrence = f"telegram-control:{command_type}:{idempotency_key}"
        command_id = ""
        try:
            command_id = self._telegram.enqueue_once(
                command_type,
                payload,
                idempotency_key=occurrence,
            )
            command = self._telegram.wait(command_id, timeout_seconds=45, poll_seconds=0.1)
        except TelegramUserBotIdempotencyConflictError:
            return SharingHttpResult(
                {"success": False, "status": "conflict", "message": "幂等键已用于其他 Telegram 操作"},
                409,
            )
        except TelegramUserBotCommandTimeoutError:
            return SharingHttpResult(
                {
                    "success": False,
                    "accepted": True,
                    "status": "pending",
                    "message": "Telegram 操作仍在处理中",
                    "command_id": command_id,
                },
                503,
            )
        except TelegramUserBotCommandIndeterminateError:
            return SharingHttpResult(
                {
                    "success": False,
                    "accepted": False,
                    "status": "indeterminate",
                    "message": "Telegram 操作结果不确定，请先检查状态，禁止直接重放",
                    "command_id": command_id,
                },
                503,
            )
        except TelegramUserBotCommandFailedError:
            return SharingHttpResult(
                {
                    "success": False,
                    "accepted": False,
                    "status": "failed",
                    "message": "Telegram 操作失败",
                    "command_id": command_id,
                }
            )
        except Exception:
            return SharingHttpResult({"success": False, "message": "Telegram 操作不可用"}, 503)
        result_value = command.get("result")
        result = dict(result_value) if isinstance(result_value, Mapping) else {}
        if command_type == "login" and result.get("success") is not True:
            return SharingHttpResult(
                {
                    "success": False,
                    "message": str(result.get("msg") or "登录失败"),
                    "command_id": command_id,
                    "status": "failed",
                    "data": {"need_2fa": bool(result.get("need_2fa"))},
                }
            )
        safe_result: dict[str, Any] = {}
        if command_type == "clear_session":
            for key in ("deleted", "missing"):
                values = result.get(key)
                if isinstance(values, list):
                    safe_result[key] = [str(value) for value in values if str(value).strip()]
        return SharingHttpResult(
            {
                "success": True,
                "message": success_message,
                "command_id": command_id,
                "status": "succeeded",
                **({"data": safe_result} if safe_result else {}),
            }
        )

    def _append_shared_resources(
        self,
        resources: list[dict[str, Any]],
        seen: set[str],
        *,
        tmdb_id: str,
        title: str,
        year: str,
        media_type: str,
        limit: int,
        warnings: list[str],
    ) -> int:
        try:
            self._center.validate_configuration()
            response = self._center.list_cloud_search_sources(
                q="" if tmdb_id else title,
                status=(
                    "alive,available" if media_type == "movie" else "alive,available,updating,inconsistent,incomplete"
                ),
                item_type="Movie" if media_type == "movie" else "Pack",
                tmdb_id=tmdb_id,
                order_by="latest",
                limit=max(limit, min(500, limit * 5)) if media_type == "tv" else limit,
                offset=0,
            )
            rows = _response_rows(response, ("items", "sources", "results", "data"))
            normalized = _shared_pool_rows(rows, fallback_year=year)
            normalized.sort(key=_shared_sort_key)
            _append_unique(resources, seen, normalized[:limit])
            return len(normalized)
        except Exception:
            warnings.append("共享池搜索失败，已跳过共享池资源。")
            return 0

    def _append_hdhive_resources(
        self,
        resources: list[dict[str, Any]],
        seen: set[str],
        *,
        tmdb_id: str,
        media_type: str,
        season: object,
        limit: int,
        warnings: list[str],
    ) -> tuple[int, int]:
        if not tmdb_id:
            return 0, 0
        try:
            if not self._hdhive_authorized():
                warnings.append("影巢未授权或连接不可用，已跳过影巢搜索。")
                return 0, 0
            response = self._hdhive.get_resources(
                tmdb_id,
                media_type,
                season=None if media_type == "tv" else _optional_positive_int(season),
            )
            rows = _response_rows(response, ("resources", "items", "results", "data"))
            filtered = (
                list(rows)
                if media_type == "tv"
                else filter_hdhive_resources(
                    rows,
                    config=self._hdhive_config()["filter"],
                    target_season=season,
                    media_type=media_type,
                )
            )
            normalized = [_normalize_hdhive_resource(item) for item in filtered]
            _append_unique(resources, seen, normalized[:limit])
            return len(rows), len(filtered)
        except Exception:
            warnings.append("影巢搜索失败，已跳过影巢资源。")
            return 0, 0

    def _append_channel_resources(
        self,
        resources: list[dict[str, Any]],
        seen: set[str],
        *,
        tmdb_id: str,
        title: str,
        year: str,
        media_type: str,
        limit: int,
        idempotency_key: str | None,
        warnings: list[str],
    ) -> int:
        if not title:
            warnings.append("未提供标题，已跳过频道历史搜索。")
            return 0
        payload = {
            "query": title,
            "media_type": media_type,
            "tmdb_id": tmdb_id,
            "year": year,
            "limit": limit,
            "extra_queries": [f"{title} {year}"] if year else [],
            "timeout": 35,
            "include_tmdb_query": False,
            "strict_title_match": True,
        }
        occurrence = str(idempotency_key or "").strip() or _json_hash(
            {
                "kind": "cloud-channel-search",
                "payload": payload,
                "bucket": int(self._clock().timestamp() // 30),
            }
        )
        owner = _json_hash({"kind": "cloud-channel-search-result", "occurrence": occurrence})
        try:
            command_id = self._telegram.enqueue_once(
                "search",
                payload,
                idempotency_key=f"cloud-search:{occurrence}",
                result_owner_key=owner,
            )
            command = self._telegram.wait(command_id, timeout_seconds=40, poll_seconds=0.1)
            result_value = command.get("result")
            result = dict(result_value) if isinstance(result_value, Mapping) else {}
            if result.get("ok") is not True:
                warnings.append("频道搜索未返回可用结果。")
                return 0
            reference = _required_hash(result.get("search_result_ref"), "search result reference")
            expected_hash = _required_hash(result.get("search_result_hash"), "search result hash")
            raw = self._telegram.consume_resource_secret(reference, owner)
            if _json_hash(raw) != expected_hash:
                warnings.append("频道搜索结果校验失败，已忽略。")
                return 0
            rows = _mapping_rows(raw, ("results", "items", "data"))
            normalized = [_normalize_channel_resource(item) for item in rows]
            _append_unique(resources, seen, normalized[:limit])
            return len(rows)
        except Exception:
            warnings.append("频道搜索失败，已跳过频道资源。")
            return 0

    def _hdhive_config(self) -> dict[str, Any]:
        loaded = self._settings.load(_HDHIVE_SETTING_KEY)
        source = dict(loaded) if isinstance(loaded, Mapping) else {}
        unlock_value = source.get("unlock_limit")
        unlock = unlock_value if isinstance(unlock_value, Mapping) else {}
        filter_value = source.get("filter")
        return {
            "relay_base_url": str(source.get("relay_base_url") or "").strip(),
            "relay_instance_id": str(source.get("relay_instance_id") or source.get("instance_id") or "").strip(),
            "relay_instance_secret": str(
                source.get("relay_instance_secret") or source.get("instance_secret") or ""
            ).strip(),
            "checkin_mode": source.get("checkin_mode")
            if source.get("checkin_mode") in {"normal", "gambler"}
            else "normal",
            "unlock_limit": {
                "count": _bounded_int(unlock.get("count"), default=3, minimum=0, maximum=100),
                "window": _bounded_int(unlock.get("window"), default=60, minimum=1, maximum=86_400),
            },
            "filter": normalize_filter_config(filter_value),
        }

    def _hdhive_authorized(self) -> bool:
        payload = self._safe_provider_payload(self._hdhive.ping)
        return bool(payload.get("success") is not False and payload.get("has_access_token"))

    def _authorize_url(self) -> str:
        payload = self._safe_provider_payload(self._hdhive.authorize_url)
        data = self._provider_data(payload)
        return str((data or payload).get("authorize_url") or "").strip()

    @staticmethod
    def _safe_provider_payload(call: Callable[[], ProviderResponse[dict[str, Any]]]) -> dict[str, Any]:
        try:
            response = call()
        except Exception:
            return {}
        if response.disposition is not ProviderDisposition.CONFIRMED or not isinstance(response.payload, Mapping):
            return {}
        return normalize_json_object(dict(response.payload), field="provider response")

    @staticmethod
    def _provider_data(payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
        value = payload.get("data")
        return value if isinstance(value, Mapping) else payload if payload else None


def _provider_http_result(
    response: ProviderResponse[dict[str, Any]],
    *,
    rejected_status: int,
) -> SharingHttpResult:
    payload = dict(response.payload) if isinstance(response.payload, Mapping) else {}
    if response.disposition is ProviderDisposition.CONFIRMED:
        return SharingHttpResult(payload or {"success": True})
    if response.disposition in {ProviderDisposition.REJECTED, ProviderDisposition.NOT_FOUND}:
        return SharingHttpResult(payload or {"success": False, "message": "影巢请求失败"}, rejected_status)
    return SharingHttpResult({"success": False, "message": "影巢服务暂时不可用"}, 503)


def _response_rows(
    response: ProviderResponse[dict[str, Any]],
    keys: Sequence[str],
) -> list[dict[str, Any]]:
    if response.disposition is not ProviderDisposition.CONFIRMED or not isinstance(response.payload, Mapping):
        return []
    return _mapping_rows(response.payload, keys)


def _mapping_rows(payload: Mapping[str, Any], keys: Sequence[str]) -> list[dict[str, Any]]:
    queue: list[Any] = [payload]
    visited: set[int] = set()
    while queue:
        value = queue.pop(0)
        if id(value) in visited:
            continue
        visited.add(id(value))
        if isinstance(value, list):
            rows = [dict(item) for item in value if isinstance(item, Mapping)]
            if rows:
                return rows
        if isinstance(value, Mapping):
            for key in keys:
                nested = value.get(key)
                if isinstance(nested, (Mapping, list)):
                    queue.append(nested)
    return []


def _shared_pool_rows(rows: Sequence[Mapping[str, Any]], *, fallback_year: str) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for raw in rows:
        parent = dict(raw)
        versions_value = parent.get("versions")
        versions = (
            [dict(item) for item in versions_value if isinstance(item, Mapping)]
            if isinstance(
                versions_value,
                list,
            )
            else []
        )
        candidates = versions or [parent]
        grouped: dict[str, list[dict[str, Any]]] = {}
        for candidate in candidates:
            merged = {**parent, **candidate}
            merged.pop("versions", None)
            identity = str(
                merged.get("manifest_hash")
                or merged.get("sha1")
                or merged.get("source_id")
                or merged.get("source_ref_id")
                or _json_hash(
                    {
                        "tmdb_id": merged.get("tmdb_id"),
                        "season_number": merged.get("season_number"),
                        "title": merged.get("title") or merged.get("name"),
                        "size": merged.get("size") or merged.get("total_size"),
                    }
                )
            )
            grouped.setdefault(identity, []).append(merged)
        total_versions = len(grouped)
        for index, candidates_for_version in enumerate(grouped.values(), start=1):
            item = max(
                candidates_for_version,
                key=lambda candidate: (
                    bool(candidate.get("source_id") or candidate.get("source_ref_id")),
                    int(candidate.get("size") or candidate.get("total_size") or 0),
                ),
            )
            item = _scrub_shared_pool_resource(item)
            source_kind = str(item.get("source_kind") or "").strip()
            source_id = str(item.get("source_id") or item.get("source_ref_id") or "").strip()
            title = str(item.get("title") or item.get("name") or item.get("file_name") or "共享池资源").strip()
            year = str(item.get("release_year") or item.get("year") or fallback_year or "").strip()
            if year and not re.search(rf"[（(]\s*{re.escape(year[:4])}\s*[）)]", title):
                title = f"{title}（{year[:4]}）"
            season = _optional_positive_int(item.get("season_number"))
            if season and not re.search(r"第\s*\d+\s*季", title):
                title = f"{_strip_season_suffix(title)}第 {season} 季"
            item.update(
                {
                    "source_type": "shared_pool",
                    "source_name": "共享池",
                    "_cloud_source": "shared_pool",
                    "unique_id": (
                        f"shared_pool:{source_kind}:{source_id}:{item.get('sha1') or item.get('manifest_hash') or ''}"
                    ),
                    "title": title,
                    "name": title,
                    "pan_type": "rapid115",
                    "already_owned": bool(item.get("is_mine")),
                    "unlock_points": 0,
                    "_shared_pool_version_index": index if total_versions > 1 else 0,
                    "_shared_pool_version_count": total_versions if total_versions > 1 else 0,
                    "_shared_pool_source_count": len(candidates_for_version),
                }
            )
            result.append(item)
    return result


def _scrub_shared_pool_resource(value: Mapping[str, Any]) -> dict[str, Any]:
    forbidden = {
        "access_token",
        "api_key",
        "authorization",
        "cookie",
        "password",
        "receive_code",
        "refresh_token",
        "secret",
        "share_code",
        "share_url",
        "token",
    }

    def scrub(current: Any) -> Any:
        if isinstance(current, Mapping):
            result: dict[str, Any] = {}
            for raw_key, item in current.items():
                key = str(raw_key)
                normalized = key.strip().casefold().replace("-", "_")
                if normalized in forbidden or normalized.endswith(("_token", "_secret", "_password")):
                    continue
                result[key] = scrub(item)
            return result
        if isinstance(current, list):
            return [scrub(item) for item in current]
        return current

    return cast(dict[str, Any], scrub(value))


def _normalize_hdhive_resource(value: Mapping[str, Any]) -> dict[str, Any]:
    item = dict(value)
    item.update(source_type="hdhive", source_name="影巢", _cloud_source="hdhive")
    user = item.get("user")
    if isinstance(user, Mapping):
        publisher_name = user.get("nickname") or user.get("name") or user.get("username") or user.get("display_name")
        publisher_id = user.get("id") or user.get("user_id")
        if publisher_name and not item.get("publisher_name"):
            item["publisher_name"] = publisher_name
        if publisher_id is not None and not item.get("publisher_id"):
            item["publisher_id"] = publisher_id
        if publisher_id is not None and not item.get("publisher_target_key"):
            item["publisher_target_key"] = f"publisher:{publisher_id}"
    if item.get("slug"):
        item["unique_id"] = f"hdhive:{item['slug']}"
    return item


def _normalize_channel_resource(value: Mapping[str, Any]) -> dict[str, Any]:
    item = dict(value)
    item.update(
        source_type="channel",
        source_name=item.get("source_channel") or "TG频道",
        _cloud_source="channel",
        already_owned=False,
        unlock_points=0,
    )
    if item.get("source_username") and item.get("message_id"):
        item["unique_id"] = f"channel:{item['source_username']}:{item['message_id']}"
    elif item.get("target_link"):
        item["unique_id"] = f"channel:{item['target_link']}"
    elif item.get("magnet_url"):
        item["unique_id"] = f"channel:{str(item['magnet_url'])[:80]}"
    return item


def _append_unique(
    destination: list[dict[str, Any]],
    seen: set[str],
    values: Sequence[Mapping[str, Any]],
) -> None:
    for value in values:
        item = dict(value)
        identity = _resource_identity(item)
        if identity and identity in seen:
            continue
        if identity:
            seen.add(identity)
        destination.append(item)


def _resource_identity(item: Mapping[str, Any]) -> str:
    for key in ("unique_id", "slug", "target_link", "magnet_url", "message_link"):
        if value := str(item.get(key) or "").strip():
            return f"{key}:{value}"
    if item.get("source_username") and item.get("message_id"):
        return f"channel:{item['source_username']}:{item['message_id']}"
    return ""


def _shared_sort_key(item: Mapping[str, Any]) -> tuple[int, int, int, str]:
    season = _optional_positive_int(item.get("season_number")) or 9_999
    version = _optional_positive_int(item.get("_shared_pool_version_index")) or 9_999
    kind = str(item.get("item_type") or item.get("display_type") or "").casefold()
    return (0 if kind in {"season", "pack", "series"} else 1, season, version, str(item.get("title") or ""))


def _has_scope(status: Mapping[str, Any], scope: str) -> bool:
    scopes = status.get("scopes")
    if isinstance(scopes, list):
        return scope in {str(value) for value in scopes}
    return scope in str(status.get("scope") or "").split()


def _replace_message(result: SharingHttpResult, message: str) -> SharingHttpResult:
    if not isinstance(result.payload, Mapping):
        return result
    payload = dict(result.payload)
    if payload.get("success"):
        payload["message"] = message
    return SharingHttpResult(payload, result.status_code, result.headers)


def _strip_season_suffix(value: object) -> str:
    return re.sub(
        r"\s*(?:第\s*\d+\s*季|S\d{1,3}|Season\s*\d{1,3})\s*$",
        "",
        str(value or "").strip(),
        flags=re.IGNORECASE,
    ).strip()


def _bounded_int(value: object, *, default: int, minimum: int, maximum: int) -> int:
    try:
        normalized = int(str(value).strip())
    except (TypeError, ValueError):
        normalized = default
    return max(minimum, min(normalized, maximum))


def _optional_positive_int(value: object) -> int | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        normalized = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return normalized if normalized > 0 else None


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _json_hash(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _required_hash(value: object, field: str) -> str:
    normalized = str(value or "").strip().casefold()
    if not re.fullmatch(r"[0-9a-f]{64}", normalized):
        raise ValueError(f"{field} is invalid")
    return normalized


__all__ = ["PROVIDER_CONTROL_USE_CASE", "ProviderControlService", "ProviderSettingsPort"]
