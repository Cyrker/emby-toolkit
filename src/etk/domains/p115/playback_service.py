from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import UUID

from .directory_sync import P115TaskProgress
from .playback import (
    PLAYBACK_LEASE_TTL_SECONDS,
    P115DirectLinkGateway,
    P115PlaybackConfigService,
    P115PlaybackNotFoundError,
    P115PlaybackRepository,
    P115PlaybackSourceGateway,
    P115PlaybackStateError,
    PlaybackArtifact,
    PlaybackArtifactStatus,
    PlaybackDirectResponse,
    PlaybackLeaseIntent,
    PlaybackMode,
    PlaybackOperation,
    PlaybackOperationIntent,
    PlaybackOperationKind,
    PlaybackOperationStatus,
    PlaybackPrepared,
    PlaybackPrepareRequest,
    account_backend,
    artifact_key,
    cleanup_delay,
    has_account_auth,
    human_bytes,
    lease_key,
    normalize_owner_type,
    normalize_sha1,
    operation_id,
    play_pool_business_date,
    pool_account_revision,
    pool_credential_fingerprint,
    safe_playback_json,
    sha256_json,
    sha256_text,
    speedtest_threshold_mbps,
    user_agent_hash,
)

_SERVER_USER_AGENT_MARKERS = ("emby", "jellyfin", "lavf", "kodi")
_SERVER_PROXY_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_IN_FLIGHT_OPERATION_STATUSES = frozenset(
    {
        PlaybackOperationStatus.CLAIMED,
        PlaybackOperationStatus.REQUESTING,
        PlaybackOperationStatus.AWAITING_READBACK,
        PlaybackOperationStatus.REMOTE_CONFIRMED,
    }
)
_OPERATION_WAIT_ATTEMPTS = 50
_OPERATION_WAIT_SECONDS = 0.1


class PlaybackOperationExecutor(Protocol):
    def execute(self, operation_id: UUID) -> PlaybackOperation | None: ...


class P115PlaybackService:
    """Native P6.9 owner for playback configuration, artifacts, leases, and reclamation."""

    def __init__(
        self,
        *,
        config: P115PlaybackConfigService,
        repository: P115PlaybackRepository,
        sources: P115PlaybackSourceGateway,
        direct_links: P115DirectLinkGateway,
        executor: PlaybackOperationExecutor,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._config = config
        self._repository = repository
        self._sources = sources
        self._direct_links = direct_links
        self._executor = executor
        self._sleep = sleep
        self._clock = clock

    def get_public_config(self) -> dict[str, Any]:
        return self._config.public_config()

    def is_pool_enabled(self) -> bool:
        return bool(self._config.snapshot().get("enabled"))

    def has_usable_pool(self, user_id: str = "", *, own_account_only: bool = False) -> bool:
        accounts = self._config.snapshot()["accounts"]
        active = self._repository.active_counts([account["id"] for account in accounts])
        return (
            self._config.select_account(
                user_id=str(user_id or "").strip(),
                own_account_only=bool(own_account_only),
                active_counts=active,
            )
            is not None
        )

    def save_pool_settings(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self._config.save_settings(payload)

    def add_account(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        values = dict(payload)
        if not str(values.get("cookie") or "").strip() and not str(values.get("access_token") or "").strip():
            raise ValueError("Cookie/OpenAPI 不能为空")
        account = self._config.upsert_account(values)
        account = self._ensure_pool_account(account)
        if not bool(values.get("_skip_auto_speedtest")):
            try:
                self.speedtest_account(account["id"], intent_key=f"account-create:{account['updated_at']}")
            except Exception as exc:
                self._config.patch_account(
                    account["id"],
                    {
                        "enabled": False,
                        "last_error": str(exc),
                        "last_speed_bps": 0,
                        "last_speed_at": self._now_iso(),
                    },
                )
        current = self._config.account(account["id"]) or account
        return self._config.public_account(current)

    def update_account(self, account_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        values = dict(payload)
        if not str(values.get("cookie") or "").strip() and not str(values.get("access_token") or "").strip():
            values["_skip_auto_speedtest"] = True
        account = self._config.upsert_account(values, account_id=str(account_id or ""))
        if has_account_auth(account) and (
            "cookie" in values or "access_token" in values or not str(account.get("temp_cid") or "").strip()
        ):
            account = self._ensure_pool_account(account)
        if has_account_auth(account) and not bool(values.get("_skip_auto_speedtest")):
            try:
                self.speedtest_account(account["id"], intent_key=f"account-update:{account['updated_at']}")
            except Exception as exc:
                self._config.patch_account(
                    account["id"],
                    {
                        "enabled": False,
                        "last_error": str(exc),
                        "last_speed_bps": 0,
                        "last_speed_at": self._now_iso(),
                    },
                )
        current = self._config.account(account["id"]) or account
        return self._config.public_account(current)

    def delete_account(self, account_id: str) -> bool:
        account = self._config.account(account_id)
        if account is None:
            return False
        self._repository.release_account_leases(
            account["id"],
            cleanup_delays=self._cleanup_delays(),
        )
        deleted = self._config.delete_account(account["id"])
        self.cleanup()
        return deleted

    def speedtest_account(self, account_id: str, intent_key: str = "") -> dict[str, Any]:
        account = self._config.account(account_id)
        if account is None:
            raise P115PlaybackNotFoundError("小号不存在")
        if not has_account_auth(account):
            raise P115PlaybackStateError("小号未配置 Cookie/OpenAPI")
        if not str(account.get("temp_cid") or "").strip():
            account = self._ensure_pool_account(account)
        source = self._sources.snapshot(
            PlaybackPrepareRequest(
                mode=PlaybackMode.MAINTENANCE,
                source="speedtest",
            )
        )
        source = self._source_snapshot(source)
        intent_identity = str(intent_key or "").strip() or self._clock().astimezone(UTC).strftime("manual:%Y%m%d%H%M")
        source_identity = sha256_json(source)
        backend = account_backend(account)
        credential_hash = pool_credential_fingerprint(account, backend)
        account_revision = pool_account_revision(account, backend)
        threshold = speedtest_threshold_mbps(self._config.snapshot().get("auto_speedtest_threshold_mbps"))
        configuration_revision = sha256_json(
            {
                "account_revision": account_revision,
                "speedtest_threshold_mbps": threshold,
            }
        )
        remote_name_digest = sha256_text(
            f"{account['id']}\0{intent_identity}\0{source_identity}\0{configuration_revision}"
        )
        source = {
            **source,
            "file_name": f"ETK-speedtest-{remote_name_digest[:32]}.bin",
        }
        request = {
            "account_kind": "pool",
            "target_cid": str(account.get("temp_cid") or ""),
            "source": source,
            "user_agent_hash": user_agent_hash(_SERVER_PROXY_USER_AGENT),
            "credential_hash": credential_hash,
            "account_revision": account_revision,
            "speedtest_threshold_mbps": threshold,
            "configuration_revision": configuration_revision,
        }
        operation = self._reserve_and_execute(
            PlaybackOperationIntent.create(
                semantic_key=(
                    f"speedtest:{account['id']}:{intent_identity}:{sha256_json(source)}:{configuration_revision}"
                ),
                kind=PlaybackOperationKind.SPEEDTEST,
                mode=PlaybackMode.MAINTENANCE,
                account_id=account["id"],
                provider_backend=backend,
                request=request,
            )
        )
        self._require_confirmed(operation, "小号测速")
        result = dict(operation.result or operation.remote_receipt or {})
        for field in ("bytes", "bps"):
            result[field] = max(0, int(result.get(field) or 0))
        result["seconds"] = float(result.get("seconds") or 0)
        result["speed_text"] = str(result.get("speed_text") or f"{human_bytes(result['bps'])}/s")
        return result

    def cleanup(self) -> int:
        self._repository.expire_leases(limit=500)
        removed = 0
        for artifact in self._repository.reclaimable_artifacts(limit=500):
            operation = self._reserve_reclamation(artifact)
            if operation.status is PlaybackOperationStatus.CONFIRMED:
                removed += 1
        return removed

    def save_user_account(
        self,
        user_id: str,
        username: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        owner = str(user_id or "").strip()
        if not owner:
            raise PermissionError("未登录")
        values = dict(payload)
        cookie = str(values.get("cookie") or "").strip()
        access_token = str(values.get("access_token") or "").strip()
        refresh_token = str(values.get("refresh_token") or "").strip()
        existing = self._config.account_by_owner("user", owner)
        if not cookie and not access_token and existing is None:
            raise ValueError("Cookie/OpenAPI 不能为空")
        update: dict[str, Any] = {
            "alias": str(values.get("alias") or username or "用户小号").strip() or "用户小号",
            "app_type": str(values.get("app_type") or "alipaymini").strip() or "alipaymini",
            "owner_type": "user",
            "owner_user_id": owner,
            "shared": bool(values.get("shared", False)),
            "_skip_auto_speedtest": True,
        }
        if cookie:
            update.update({"cookie": cookie, "enabled": True})
        if access_token:
            update.update(
                {
                    "access_token": access_token,
                    "refresh_token": refresh_token,
                    "enabled": True,
                }
            )
        account = self._config.upsert_account(update, account_id=str((existing or {}).get("id") or ""))
        reward: dict[str, Any] = {}
        if cookie or access_token:
            try:
                account = self._ensure_pool_account(account)
                speed = self.speedtest_account(
                    account["id"],
                    intent_key=f"user-account:{account['updated_at']}",
                )
                account = self._config.account(account["id"]) or account
                reward = self._config.grant_reward(
                    account,
                    notify_user=False,
                    speed_text=str(speed.get("speed_text") or ""),
                    error_text="" if account.get("enabled") else str(account.get("last_error") or "测速未达标"),
                )
            except Exception as exc:
                account = self._config.patch_account(
                    account["id"],
                    {
                        "enabled": False,
                        "last_error": str(exc),
                        "last_speed_bps": 0,
                        "last_speed_at": self._now_iso(),
                    },
                )
                reward = self._config.grant_reward(
                    account,
                    notify_user=False,
                    error_text=str(exc),
                )
        account = self._config.account(account["id"]) or account
        public = self._config.public_account(account)
        if reward.get("reward_days"):
            public["reward_days"] = reward["reward_days"]
        public["reward_summary"] = self._config.reward_summary(owner)
        return public

    def get_user_account(self, user_id: str) -> dict[str, Any]:
        owner = str(user_id or "").strip()
        account = self._config.account_by_owner("user", owner)
        result = self._config.public_account(account)
        result["reward_summary"] = self._config.reward_summary(owner)
        return result

    def delete_user_account(self, user_id: str) -> bool:
        account = self._config.account_by_owner("user", str(user_id or "").strip())
        if account is None:
            return False
        return self.delete_account(account["id"])

    def reward_summary(self, user_id: str) -> dict[str, Any]:
        return self._config.reward_summary(str(user_id or "").strip())

    def get_temp_config(self) -> dict[str, Any]:
        return self._public_temp_config(self._config.temp_config())

    def save_temp_config(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        primary = dict(self._sources.primary_account())
        account_id = str(primary.get("account_id") or "").strip()
        backend = str(primary.get("provider_backend") or "").strip()
        if not account_id or backend not in {"openapi", "cookie"}:
            raise P115PlaybackStateError("115 客户端未初始化，请先配置 115 Cookie/OpenAPI")
        current = self._config.temp_config()
        cleanup_cron = (
            str(payload.get("cleanup_cron") or "").strip()
            if "cleanup_cron" in payload
            else str(current.get("cleanup_cron") or "0 * * * *").strip()
        )
        request = {
            "account_kind": "primary",
            "temp_dir_name": "ETK临时目录",
            "cleanup_cron": cleanup_cron,
        }
        operation = self._ensure_temp_dir_operation(
            semantic_base=(f"ensure-temp:primary:{account_id}:{backend}:ETK临时目录:{sha256_text(cleanup_cron)}"),
            account_id=account_id,
            backend=backend,
            request=request,
        )
        self._require_confirmed(operation, "115 临时目录确认")
        accounts: list[dict[str, Any]] = []
        for account in self._config.snapshot()["accounts"]:
            if not has_account_auth(account):
                accounts.append(
                    {
                        "id": account["id"],
                        "alias": account.get("alias") or account["id"],
                        "success": False,
                        "message": "未配置 Cookie/OpenAPI",
                    }
                )
                continue
            try:
                ensured = self._ensure_pool_account(account)
                accounts.append(
                    {
                        "id": ensured["id"],
                        "alias": ensured.get("alias") or ensured["id"],
                        "success": True,
                        "cid": str(ensured.get("temp_cid") or ""),
                    }
                )
            except Exception as exc:
                accounts.append(
                    {
                        "id": account["id"],
                        "alias": account.get("alias") or account["id"],
                        "success": False,
                        "message": str(exc),
                    }
                )
        return {**self.get_temp_config(), "accounts": accounts}

    def prepare_play(
        self,
        request: PlaybackPrepareRequest,
        *,
        force_new: bool = False,
        failed_remote_pick_code: str = "",
    ) -> PlaybackPrepared | None:
        if request.mode not in {PlaybackMode.POOL, PlaybackMode.COPY, PlaybackMode.VIRTUAL}:
            raise ValueError("playback mode must be pool, copy, or virtual")
        if force_new and request.mode is not PlaybackMode.COPY:
            raise ValueError("force_new is only valid for copy playback")
        failed_pick_code = str(failed_remote_pick_code or "").strip()
        if force_new and not failed_pick_code:
            raise ValueError("force_new requires the failed copy artifact pick code")
        source = self._source_snapshot(self._sources.snapshot(request))
        if request.mode is PlaybackMode.POOL:
            accounts = self._config.snapshot()["accounts"]
            active = self._repository.active_counts([account["id"] for account in accounts])
            account = self._config.select_account(
                user_id=request.user_id,
                own_account_only=request.own_account_only,
                active_counts=active,
            )
            if account is None:
                return None
            if not str(account.get("temp_cid") or "").strip():
                account = self._ensure_pool_account(account)
            account_id = account["id"]
            backend = account_backend(account)
            target_cid = str(account.get("temp_cid") or "")
        else:
            primary = dict(self._sources.primary_account())
            account_id = str(primary.get("account_id") or "").strip()
            backend = str(primary.get("provider_backend") or "").strip()
            target_cid = str(self._config.temp_config().get("cid") or "").strip()
            if not account_id or backend not in {"openapi", "cookie"}:
                raise P115PlaybackStateError("115 Client not initialized")
            if not target_cid:
                raise P115PlaybackStateError("115 临时目录 CID 未配置")
        digest = artifact_key(
            mode=request.mode,
            account_id=account_id,
            provider_backend=backend,
            target_cid=target_cid,
            source_sha1=str(source.get("sha1") or ""),
            source_size=int(source.get("size") or 0),
            source_pick_code=str(source.get("pick_code") or request.source_pick_code or ""),
            source_fid=str(source.get("fid") or ""),
        )
        artifact = self._repository.artifact_by_key(digest)
        if artifact is not None and artifact.status is PlaybackArtifactStatus.INDETERMINATE:
            raise P115PlaybackStateError("播放临时文件状态不确定，必须先对账")
        if artifact is not None and artifact.status is PlaybackArtifactStatus.RECLAIM_PENDING:
            raise P115PlaybackStateError("播放临时文件正在回收，请稍后重试")
        lease_digest = lease_key(request, digest)
        if (
            force_new
            and artifact is not None
            and artifact.status is PlaybackArtifactStatus.AVAILABLE
            and artifact.remote_pick_code == failed_pick_code
        ):
            stale_lease = self._repository.reserve_lease(self._lease_intent(request, lease_digest, artifact)).lease
            self._repository.release_lease(stale_lease.id, cleanup_delay_seconds=0)
            reclamation = self._reserve_reclamation(artifact)
            self._require_confirmed(reclamation, "复制播放失效文件回收")
            artifact = self._repository.artifact(artifact.id)
            if artifact is None or artifact.status is not PlaybackArtifactStatus.RECLAIMED:
                raise P115PlaybackStateError("复制播放失效文件未完成持久化回收")
        if artifact is None or artifact.status is PlaybackArtifactStatus.RECLAIMED:
            artifact = self._prepare_artifact(
                request=request,
                source=source,
                account_id=account_id,
                backend=backend,
                target_cid=target_cid,
                digest=digest,
                previous=artifact,
            )
        if artifact.status is not PlaybackArtifactStatus.AVAILABLE:
            raise P115PlaybackStateError(f"播放临时文件状态为 {artifact.status.value}")
        lease = self._repository.reserve_lease(self._lease_intent(request, lease_digest, artifact)).lease
        if request.mode is PlaybackMode.POOL:
            self._config.apply_account_projection(
                lease_digest,
                account_id,
                patch={"last_used_at": self._clock().timestamp(), "last_error": "", "temp_cid": target_cid},
                play_count_delta=1,
            )
        operation = self._repository.read(artifact.operation_id)
        if operation is None:
            raise P115PlaybackStateError("播放临时文件缺少持久 operation")
        return PlaybackPrepared(operation=operation, artifact=artifact, lease=lease)

    def resolve_prepared(self, prepared: PlaybackPrepared, user_agent: str) -> str:
        lease = self._repository.lease(prepared.lease.id)
        artifact = self._repository.artifact(prepared.artifact.id)
        if lease is None or not lease.status.active:
            return ""
        if artifact is None or artifact.status is not PlaybackArtifactStatus.AVAILABLE:
            return ""
        return str(self._direct_links.resolve_artifact(artifact, str(user_agent or "").strip()) or "")

    def resolve_lease(self, lease_id: UUID | str, user_agent: str) -> str:
        parsed = UUID(str(lease_id))
        lease = self._repository.lease(parsed)
        if lease is None or not lease.status.active:
            return ""
        artifact = self._repository.artifact(lease.artifact_id)
        if artifact is None or artifact.status is not PlaybackArtifactStatus.AVAILABLE:
            return ""
        return str(self._direct_links.resolve_artifact(artifact, str(user_agent or "").strip()) or "")

    def release_lease(self, lease_id: UUID | str) -> bool:
        parsed = UUID(str(lease_id))
        lease = self._repository.lease(parsed)
        if lease is None:
            return False
        self._repository.release_lease(
            parsed,
            cleanup_delay_seconds=cleanup_delay(lease.mode),
        )
        self.cleanup()
        return True

    def release_for_stop(self, data: Mapping[str, Any]) -> int:
        raw_playback = data.get("PlaybackInfo")
        raw_session = data.get("Session")
        raw_user = data.get("User")
        raw_item = data.get("Item")
        playback = dict(raw_playback) if isinstance(raw_playback, Mapping) else {}
        session = dict(raw_session) if isinstance(raw_session, Mapping) else {}
        user = dict(raw_user) if isinstance(raw_user, Mapping) else {}
        item = dict(raw_item) if isinstance(raw_item, Mapping) else {}
        client_key = str(
            data.get("_etk_client_key") or data.get("DeviceId") or session.get("DeviceId") or session.get("Id") or ""
        ).strip()
        released = self._repository.release_matching(
            play_session_id=str(playback.get("PlaySessionId") or data.get("PlaySessionId") or "").strip(),
            item_id=str(item.get("Id") or data.get("ItemId") or "").strip(),
            owner_user_id=str(user.get("Id") or data.get("UserId") or "").strip(),
            client_key=client_key,
            cleanup_delays=self._cleanup_delays(),
        )
        if released:
            self.cleanup()
        return len(released)

    def resolve_virtual_play(self, virtual_info: Mapping[str, Any], **context: Any) -> str:
        prepared = self.prepare_play(
            PlaybackPrepareRequest(
                mode=PlaybackMode.VIRTUAL,
                virtual_id=int(virtual_info.get("virtual_id") or 0),
                sha1=str(virtual_info.get("sha1") or ""),
                file_name=str(virtual_info.get("filename") or context.get("display_name") or ""),
                item_id=str(context.get("item_id") or ""),
                play_session_id=str(context.get("play_session_id") or ""),
                user_id=str(context.get("user_id") or ""),
                client_key=str(context.get("client_key") or ""),
                user_agent=str(context.get("user_agent") or ""),
                source="virtual-play",
            )
        )
        if prepared is None:
            return ""
        return self.resolve_prepared(prepared, str(context.get("user_agent") or ""))

    def direct_play(self, pick_code: str, user_agent: str, range_header: str) -> PlaybackDirectResponse:
        client_ua = str(user_agent or "")
        server_proxy = not client_ua or any(marker in client_ua.casefold() for marker in _SERVER_USER_AGENT_MARKERS)
        request_ua = _SERVER_PROXY_USER_AGENT if server_proxy else client_ua
        real_url = ""
        last_error: Exception | None = None
        for attempt in range(2):
            try:
                real_url = str(self._direct_links.resolve_primary(str(pick_code or ""), request_ua) or "")
            except P115PlaybackNotFoundError as exc:
                return PlaybackDirectResponse("error", 500, body=str(exc))
            except Exception as exc:
                last_error = exc
            if real_url:
                break
            if attempt == 0:
                self._sleep(0.5)
        if not real_url:
            del last_error
            return PlaybackDirectResponse("error", 404, body="Failed to get download URL or Rate Limited")
        if not server_proxy:
            return PlaybackDirectResponse("redirect", 302, location=real_url)
        try:
            upstream = self._direct_links.stream(
                real_url,
                user_agent=request_ua,
                range_header=str(range_header or ""),
            )
        except Exception as exc:
            return PlaybackDirectResponse("error", 500, body=str(exc))
        return PlaybackDirectResponse("stream", upstream.status_code, stream=upstream)

    def daily_speedtest(self, progress: P115TaskProgress) -> Mapping[str, Any]:
        config = self._config.snapshot()
        if not config.get("enabled"):
            message = "小号池未启用，跳过"
            progress.update(100, message)
            return {"skipped": True, "reason": "小号池未启用", "results": [], "failed_count": 0, "message": message}
        accounts = [account for account in config["accounts"] if has_account_auth(account)]
        total = len(accounts)
        results: list[dict[str, Any]] = []
        progress.update(0, f"小号池每日测速启动，共 {total} 个账号")
        date_key = play_pool_business_date(self._clock())
        for index, account in enumerate(accounts, start=1):
            progress.checkpoint()
            progress.update(int(((index - 1) / max(total, 1)) * 95), f"正在测速：{account['alias']} ({index}/{total})")
            result: dict[str, Any] = {
                "account_id": account["id"],
                "alias": account.get("alias") or "小号",
                "owner_type": normalize_owner_type(account.get("owner_type")),
                "owner_name": self._config.public_account(account).get("owner_user_name") or "管理员",
                "success": False,
                "speed_text": "",
                "error": "",
                "reward_error": "",
                "reward_days": 0.0,
            }
            try:
                speed = self.speedtest_account(account["id"], intent_key=f"daily:{date_key}")
                fresh = self._config.account(account["id"]) or account
                result["success"] = bool(fresh.get("enabled"))
                result["speed_text"] = str(speed.get("speed_text") or "")
                if not fresh.get("enabled"):
                    result["error"] = str(fresh.get("last_error") or "测速未达标")
                reward_account = {
                    **fresh,
                    "owner_type": account.get("owner_type"),
                    "owner_user_id": account.get("owner_user_id"),
                    "shared": account.get("shared"),
                }
                try:
                    reward = self._config.grant_reward(
                        reward_account,
                        notify_user=True,
                        speed_text=result["speed_text"],
                        error_text=result["error"],
                    )
                    result["reward_days"] = float(reward.get("reward_days") or 0)
                    if reward.get("error"):
                        result["reward_error"] = "奖励持久化失败"
                except Exception:
                    result["reward_error"] = "奖励持久化失败"
            except Exception as exc:
                error = str(exc)
                fresh = self._config.patch_account(
                    account["id"],
                    {
                        "enabled": False,
                        "last_error": error,
                        "last_speed_bps": 0,
                        "last_speed_at": self._now_iso(),
                    },
                )
                reward_account = {
                    **fresh,
                    "owner_type": account.get("owner_type"),
                    "owner_user_id": account.get("owner_user_id"),
                    "shared": account.get("shared"),
                }
                try:
                    self._config.grant_reward(reward_account, notify_user=True, error_text=error)
                except Exception:
                    result["reward_error"] = "奖励状态持久化失败"
                result["error"] = error
            results.append(result)
        notification_error = ""
        try:
            self._config.notify_daily_results(results)
        except Exception:
            notification_error = "每日测速汇总通知失败"
        failed = sum(not bool(item.get("success")) or bool(item.get("reward_error")) for item in results)
        if notification_error:
            failed = max(1, failed)
        speedtest_passed = sum(bool(item.get("success")) for item in results)
        progress.update(100, f"小号池每日测速完成，通过 {speedtest_passed}/{total}")
        return {
            "ok": failed == 0,
            "status": "succeeded" if failed == 0 else "partial",
            "skipped": False,
            "failed_count": failed,
            "results": results,
            "notification_error": notification_error,
            "message": (
                f"小号池每日测速完成，测速通过 {speedtest_passed}/{total}，异常 {failed}"
                if failed
                else f"小号池每日测速完成，全部 {total} 个账号通过"
            ),
        }

    def _ensure_pool_account(self, account: Mapping[str, Any]) -> dict[str, Any]:
        if not has_account_auth(account):
            raise P115PlaybackStateError("小号未配置 Cookie/OpenAPI")
        backend = account_backend(account)
        credential_revision = pool_credential_fingerprint(account, backend)
        account_revision = pool_account_revision(account, backend)
        operation = self._ensure_temp_dir_operation(
            semantic_base=f"ensure-temp:pool:{account['id']}:{account_revision}:ETK临时目录",
            account_id=str(account["id"]),
            backend=backend,
            request={
                "account_kind": "pool",
                "temp_dir_name": "ETK临时目录",
                "configured_cid": str(account.get("temp_cid") or ""),
                "credential_hash": credential_revision,
                "account_revision": account_revision,
            },
        )
        self._require_confirmed(operation, "小号临时目录确认")
        return self._config.account(str(account["id"])) or dict(account)

    def _prepare_artifact(
        self,
        *,
        request: PlaybackPrepareRequest,
        source: Mapping[str, Any],
        account_id: str,
        backend: str,
        target_cid: str,
        digest: str,
        previous: PlaybackArtifact | None,
    ) -> PlaybackArtifact:
        generation = self._repository.latest_artifact_generation(digest)
        if previous is not None:
            minimum = previous.generation + (1 if previous.status is PlaybackArtifactStatus.RECLAIMED else 0)
            generation = max(generation, minimum)
        generation = max(1, generation)
        semantic = f"prepare:{digest}:{generation}"
        existing = self._repository.read(operation_id(sha256_text(semantic)))
        if existing is not None and existing.status is PlaybackOperationStatus.REJECTED:
            generation += 1
            semantic = f"prepare:{digest}:{generation}"
            existing = self._repository.read(operation_id(sha256_text(semantic)))
        if existing is None:
            intent = PlaybackOperationIntent.create(
                semantic_key=semantic,
                kind=PlaybackOperationKind.PREPARE,
                mode=request.mode,
                account_id=account_id,
                provider_backend=backend,
                request={
                    "artifact_key": digest,
                    "artifact_generation": generation,
                    "target_cid": target_cid,
                    "file_name": str(request.file_name or source.get("file_name") or ""),
                    "source": dict(source),
                },
                artifact_key=digest,
                artifact_generation=generation,
            )
            existing = self._reserve_and_execute(intent)
        elif not existing.status.terminal:
            existing = self._execute_or_wait(existing)
        if existing.status is PlaybackOperationStatus.INDETERMINATE:
            raise P115PlaybackStateError("播放临时文件远端结果不确定，禁止重复写入")
        self._require_confirmed(existing, "播放临时文件准备")
        artifact = self._repository.artifact_by_key(digest)
        if artifact is None or artifact.generation != generation:
            raise P115PlaybackStateError("播放临时文件 projection 未完成")
        if artifact.status is PlaybackArtifactStatus.RECLAIMED:
            raise P115PlaybackStateError("播放临时文件已被并发回收，请重试")
        return artifact

    def _lease_intent(
        self,
        request: PlaybackPrepareRequest,
        lease_digest: str,
        artifact: PlaybackArtifact,
    ) -> PlaybackLeaseIntent:
        return PlaybackLeaseIntent(
            lease_key=lease_digest,
            artifact_id=artifact.id,
            mode=request.mode,
            owner_user_id=str(request.user_id or "").strip(),
            item_id=str(request.item_id or "").strip(),
            play_session_id=str(request.play_session_id or "").strip(),
            client_key=str(request.client_key or "").strip(),
            user_agent_hash=user_agent_hash(request.user_agent),
            source=str(request.source or "").strip(),
            expires_at=self._clock() + timedelta(seconds=PLAYBACK_LEASE_TTL_SECONDS),
        )

    def _reserve_reclamation(self, artifact: PlaybackArtifact) -> PlaybackOperation:
        intent = PlaybackOperationIntent.create(
            semantic_key=f"reclaim:{artifact.id}:{artifact.state_revision}",
            kind=PlaybackOperationKind.RECLAIM,
            mode=artifact.mode,
            account_id=artifact.account_id,
            provider_backend=artifact.provider_backend,
            request={
                "artifact_id": str(artifact.id),
                "artifact_key": artifact.artifact_key,
                "artifact_generation": artifact.generation,
                "target_cid": artifact.target_cid,
                "remote_fid": artifact.remote_fid,
                "remote_pick_code": artifact.remote_pick_code,
                "source": {
                    "sha1": artifact.source_sha1,
                    "size": artifact.source_size,
                    "pick_code": artifact.source_pick_code,
                    "fid": artifact.source_fid,
                    "file_name": artifact.file_name,
                },
            },
            artifact_key=artifact.artifact_key,
            artifact_generation=artifact.generation,
        )
        return self._reserve_and_execute(intent)

    def _ensure_temp_dir_operation(
        self,
        *,
        semantic_base: str,
        account_id: str,
        backend: str,
        request: Mapping[str, Any],
    ) -> PlaybackOperation:
        for retry_generation in range(1, 1001):
            semantic_key = f"{semantic_base}:attempt:{retry_generation}"
            key = sha256_text(semantic_key)
            existing = self._repository.read(operation_id(key))
            if existing is not None:
                if existing.status is PlaybackOperationStatus.REJECTED:
                    continue
                if not existing.status.terminal:
                    return self._execute_or_wait(existing)
                return existing
            intent = PlaybackOperationIntent.create(
                semantic_key=semantic_key,
                kind=PlaybackOperationKind.ENSURE_TEMP_DIR,
                mode=PlaybackMode.MAINTENANCE,
                account_id=account_id,
                provider_backend=backend,
                request={**dict(request), "retry_generation": retry_generation},
            )
            reservation = self._repository.reserve(intent)
            operation = reservation.operation
            if not reservation.created and operation.status is PlaybackOperationStatus.REJECTED:
                continue
            if not operation.status.terminal:
                operation = self._execute_or_wait(operation)
            return operation
        raise P115PlaybackStateError("115 临时目录重试 generation 超过安全上限")

    def _reserve_and_execute(self, intent: PlaybackOperationIntent) -> PlaybackOperation:
        reservation = self._repository.reserve(intent)
        operation = reservation.operation
        if not operation.status.terminal:
            operation = self._execute_or_wait(operation)
        return operation

    def _execute_or_wait(self, operation: PlaybackOperation) -> PlaybackOperation:
        current = self._executor.execute(operation.id) or operation
        for _attempt in range(_OPERATION_WAIT_ATTEMPTS):
            if current.status not in _IN_FLIGHT_OPERATION_STATUSES:
                break
            self._sleep(_OPERATION_WAIT_SECONDS)
            current = self._repository.read(operation.id) or current
        return current

    @staticmethod
    def _require_confirmed(operation: PlaybackOperation, label: str) -> None:
        if operation.status is PlaybackOperationStatus.CONFIRMED:
            return
        error = operation.error or {}
        message = str(error.get("message") or error.get("type") or operation.status.value)
        if operation.status is PlaybackOperationStatus.INDETERMINATE:
            raise P115PlaybackStateError(f"{label}结果不确定: {message}")
        if operation.status in {PlaybackOperationStatus.READY, PlaybackOperationStatus.RETRYABLE}:
            raise P115PlaybackStateError(f"{label}已持久化等待 Worker 重试")
        if operation.status in {
            PlaybackOperationStatus.CLAIMED,
            PlaybackOperationStatus.REQUESTING,
            PlaybackOperationStatus.AWAITING_READBACK,
            PlaybackOperationStatus.REMOTE_CONFIRMED,
        }:
            raise P115PlaybackStateError(f"{label}正在处理: {operation.status.value}")
        raise P115PlaybackStateError(f"{label}失败: {message}")

    @staticmethod
    def _source_snapshot(value: Mapping[str, Any]) -> dict[str, Any]:
        source = safe_playback_json(value, field="playback source snapshot")
        source["sha1"] = normalize_sha1(source.get("sha1"))
        source["size"] = max(0, int(source.get("size") or 0))
        source["pick_code"] = str(source.get("pick_code") or "").strip()
        source["fid"] = str(source.get("fid") or "").strip()
        source["file_name"] = str(source.get("file_name") or "").strip()
        if not source["sha1"] and not source["pick_code"] and not source["fid"]:
            raise P115PlaybackNotFoundError("播放源缺少 SHA1/PickCode/FID 身份")
        if source["sha1"] and source["size"] <= 0:
            raise P115PlaybackNotFoundError("播放源缺少文件大小")
        return source

    @staticmethod
    def _public_temp_config(value: Mapping[str, Any]) -> dict[str, Any]:
        return {key: item for key, item in value.items() if not str(key).startswith("_")}

    @staticmethod
    def _cleanup_delays() -> dict[PlaybackMode, float]:
        return {mode: cleanup_delay(mode) for mode in PlaybackMode}

    def _now_iso(self) -> str:
        return self._clock().astimezone(UTC).isoformat()


__all__ = ["P115PlaybackService", "PlaybackOperationExecutor"]
