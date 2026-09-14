from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import suppress
from typing import Any

import httpx

from .playback import (
    P115DirectLinkGateway,
    P115PlaybackConfigService,
    P115PlaybackNotFoundError,
    P115PlaybackProvider,
    P115PlaybackSourceGateway,
    P115PlaybackStateError,
    P115PlaybackUserGateway,
    PlaybackArtifact,
    PlaybackMode,
    PlaybackOperation,
    PlaybackOperationKind,
    PlaybackPrepareRequest,
    PlaybackProviderDisposition,
    PlaybackProviderOutcome,
    PlaybackUpstreamStream,
    human_bytes,
    normalize_sha1,
    pool_credential_fingerprint,
    safe_playback_json,
)

_VIDEO_SAMPLE_NAME = "play-pool-speedtest.mkv"
_MAX_DIRECTORY_PAGES = 1000
_PAGE_SIZE = 1000


class LegacyP115PlaybackUserGateway(P115PlaybackUserGateway):
    """P11 adapter around existing user/group and reward projections."""

    def display_name(self, user_id: str) -> str:
        from handler.p115_play_pool import _display_user_name

        return str(_display_user_name(user_id) or "")

    def expand_allowed_user_ids(self, user_ids: Sequence[str]) -> Sequence[str]:
        from handler.p115_play_pool import _expand_allowed_user_ids

        return tuple(_expand_allowed_user_ids(list(user_ids)))

    def reward_summary(self, user_id: str) -> Mapping[str, Any]:
        from handler.p115_play_pool import public_user_reward

        return dict(public_user_reward(user_id))

    def grant_reward(
        self,
        account: Mapping[str, Any],
        *,
        notify_user: bool,
        speed_text: str,
        error_text: str,
    ) -> Mapping[str, Any]:
        from handler.p115_play_pool import _reward_user_cookie

        return dict(
            _reward_user_cookie(
                dict(account),
                notify_user=notify_user,
                speed_text=speed_text,
                error_text=error_text,
            )
        )

    def notify_daily_results(self, results: Sequence[Mapping[str, Any]]) -> None:
        from database import user_db
        from handler.telegram import escape_markdown, send_telegram_message

        lines = ["小号池每日测速结果"]
        for item in results:
            state = "通过" if item.get("success") else "失败"
            detail = item.get("speed_text") or item.get("error") or "-"
            reward = float(item.get("reward_days") or 0)
            reward_text = f"，奖励 +{reward:g} 天" if reward else ""
            lines.append(f"{state} {item.get('alias')}（{item.get('owner_name')}）：{detail}{reward_text}")
        message = escape_markdown("\n".join(lines))
        for chat_id in user_db.get_admin_telegram_chat_ids():
            send_telegram_message(chat_id, message, disable_notification=True)


class LegacyP115PlaybackSourceGateway(P115PlaybackSourceGateway):
    """Read stable source identity before a playback mutation is reserved."""

    def snapshot(self, request: PlaybackPrepareRequest) -> Mapping[str, Any]:
        if request.mode is PlaybackMode.VIRTUAL:
            return self._virtual(request)
        if request.mode is PlaybackMode.MAINTENANCE and request.source == "speedtest":
            return self._speedtest()
        if request.mode is PlaybackMode.POOL:
            from handler.p115_play_pool import _get_source_row_from_mediainfo_cache

            row = _get_source_row_from_mediainfo_cache(
                request.item_id,
                request.source_pick_code,
                request.file_name,
            )
            return self._bind_primary_holder(
                _source_snapshot(
                    row,
                    fallback_pick_code=request.source_pick_code,
                    fallback_name=request.file_name,
                )
            )
        if request.mode is PlaybackMode.COPY:
            from handler.p115_service import P115CacheManager

            row = P115CacheManager.get_file_cache_by_pickcode(request.source_pick_code) or {}
            return _source_snapshot(
                row,
                fallback_pick_code=request.source_pick_code,
                fallback_name=request.file_name,
            )
        raise ValueError(f"unsupported playback source mode: {request.mode.value}")

    def primary_account(self) -> Mapping[str, Any]:
        from handler.p115_service import get_115_api_priority, get_115_tokens

        access_token, _refresh_token, cookie, _app_type = get_115_tokens()
        from handler.p115_service import get_115_account_id

        account_id = str(get_115_account_id() or "").strip()
        priority = str(get_115_api_priority() or "openapi").strip().casefold()
        if priority == "cookie" and str(cookie or "").strip():
            backend = "cookie"
        elif str(access_token or "").strip():
            backend = "openapi"
        elif str(cookie or "").strip():
            backend = "cookie"
        else:
            backend = ""
        credential_hash = ""
        if backend:
            credential_hash = pool_credential_fingerprint(
                {
                    "access_token": access_token,
                    "cookie": cookie,
                    "app_type": _app_type,
                },
                backend,
            )
        return {
            "account_id": account_id,
            "provider_backend": backend,
            "credential_hash": credential_hash,
        }

    @staticmethod
    def _speedtest() -> Mapping[str, Any]:
        from routes.p115 import _p115_pick_speedtest_sample_from_library

        sample = _p115_pick_speedtest_sample_from_library() or {}
        pick_code = str(sample.get("pick_code") or "").strip()
        if not pick_code:
            raise P115PlaybackNotFoundError("未找到可用于测速的 115 样本文件")
        from handler.p115_play_pool import _get_source_row_from_mediainfo_cache

        row = _get_source_row_from_mediainfo_cache("", pick_code, _VIDEO_SAMPLE_NAME)
        return LegacyP115PlaybackSourceGateway._bind_primary_holder(
            _source_snapshot(row, fallback_pick_code=pick_code, fallback_name=_VIDEO_SAMPLE_NAME)
        )

    @staticmethod
    def _virtual(request: PlaybackPrepareRequest) -> Mapping[str, Any]:
        from database import shared_virtual_db

        virtual_id = int(request.virtual_id or 0)
        sha1 = normalize_sha1(request.sha1)
        row = shared_virtual_db.get_virtual_import(virtual_id)
        if not row:
            raise P115PlaybackNotFoundError("Virtual import not found")
        raw_files = row.get("files_json")
        files = raw_files if isinstance(raw_files, list) else []
        file_info = next(
            (dict(item) for item in files if isinstance(item, Mapping) and normalize_sha1(item.get("sha1")) == sha1),
            None,
        )
        if file_info is None:
            raise P115PlaybackNotFoundError("Virtual file not found")
        snapshot = _source_snapshot(file_info, fallback_name=request.file_name)
        snapshot.update(
            {
                "virtual_id": virtual_id,
                "source_kind": str(row.get("source_kind") or file_info.get("source_kind") or "")[:64],
                "source_id": str(
                    row.get("source_id") or file_info.get("source_id") or file_info.get("source_ref_id") or ""
                )[:255],
            }
        )
        return LegacyP115PlaybackSourceGateway._bind_primary_holder(
            safe_playback_json(snapshot, field="virtual playback source")
        )

    @staticmethod
    def _bind_primary_holder(source: Mapping[str, Any]) -> dict[str, Any]:
        snapshot = dict(source)
        holder = str(snapshot.get("holder_account_id") or "").strip()
        if not holder.isdigit():
            from handler.p115_service import get_115_account_id

            holder = str(get_115_account_id() or "").strip()
        if not holder.isdigit():
            raise P115PlaybackStateError("播放源缺少稳定的主号 holder 身份")
        snapshot["holder_account_id"] = holder
        return safe_playback_json(snapshot, field="playback holder source")


class LegacyP115PlaybackProvider(P115PlaybackProvider):
    """Pinned-backend compatibility adapter with authoritative, paginated readback."""

    def __init__(
        self,
        config: P115PlaybackConfigService,
        *,
        http_client_factory: Callable[..., httpx.Client] = httpx.Client,
    ) -> None:
        self._config = config
        self._http_client_factory = http_client_factory

    def execute(
        self,
        operation: PlaybackOperation,
        *,
        checkpoint: Callable[[Mapping[str, Any]], None],
    ) -> PlaybackProviderOutcome:
        request_sent = operation.provider_request_started_at is not None
        latest_session = dict(operation.provider_session) if operation.provider_session is not None else None

        def durable_checkpoint(value: Mapping[str, Any]) -> None:
            nonlocal latest_session, request_sent
            checkpoint(value)
            latest_session = dict(value)
            request_sent = True

        try:
            if operation.kind is PlaybackOperationKind.PREPARE:
                return self._prepare(operation, durable_checkpoint)
            if operation.kind is PlaybackOperationKind.ENSURE_TEMP_DIR:
                return self._ensure_temp_dir(operation, durable_checkpoint)
            if operation.kind is PlaybackOperationKind.SPEEDTEST:
                return self._speedtest(operation, durable_checkpoint)
            if operation.kind is PlaybackOperationKind.RECLAIM:
                return self._reclaim(operation, durable_checkpoint)
            return _rejected("unsupported_playback_operation", f"unsupported kind {operation.kind.value}")
        except Exception as exc:
            error_type = "playback_provider_exception"
            message = str(exc)[:500]
            if operation.kind is PlaybackOperationKind.SPEEDTEST:
                error_type = "speedtest_provider_exception"
                message = "测速执行失败"
            return PlaybackProviderOutcome(
                PlaybackProviderDisposition.INDETERMINATE,
                request_sent=request_sent,
                session=latest_session,
                error={"type": error_type, "message": message},
            )

    def readback(self, operation: PlaybackOperation) -> PlaybackProviderOutcome:
        try:
            if operation.kind is PlaybackOperationKind.PREPARE:
                found = self._readback_prepare(operation)
                if found:
                    return _confirmed(found, request_sent=True, readback={"found": True})
                return _indeterminate("playback_prepare_absent_after_unknown", "远端回读未找到播放临时文件")
            if operation.kind is PlaybackOperationKind.ENSURE_TEMP_DIR:
                cid = self._find_temp_dir(operation)
                if cid:
                    return _confirmed(
                        {"cid": cid, "created_remote": False},
                        request_sent=True,
                        readback={"found": True},
                    )
                return _indeterminate("temp_dir_absent_after_unknown", "远端回读未找到临时目录")
            if operation.kind in {PlaybackOperationKind.RECLAIM, PlaybackOperationKind.SPEEDTEST}:
                return self._readback_delete_phase(operation)
            return _rejected("unsupported_playback_readback", f"unsupported kind {operation.kind.value}")
        except Exception as exc:
            if operation.kind is PlaybackOperationKind.SPEEDTEST:
                return _indeterminate("speedtest_readback_failed", "测速清理回读失败")
            return _indeterminate("playback_readback_failed", str(exc))

    def _prepare(
        self,
        operation: PlaybackOperation,
        checkpoint: Callable[[Mapping[str, Any]], None],
    ) -> PlaybackProviderOutcome:
        existing = self._readback_prepare(operation)
        if existing:
            return _confirmed(existing, request_sent=False, readback={"found": True})
        source = _operation_source(operation)
        if operation.mode is PlaybackMode.COPY:
            return self._copy_prepare(operation, source, checkpoint)
        return self._rapid_prepare(operation, source, checkpoint)

    def _rapid_prepare(
        self,
        operation: PlaybackOperation,
        source: Mapping[str, Any],
        checkpoint: Callable[[Mapping[str, Any]], None],
    ) -> PlaybackProviderOutcome:
        sha1 = normalize_sha1(source.get("sha1"))
        size = int(source.get("size") or 0)
        file_name = str(operation.request.get("file_name") or source.get("file_name") or sha1).strip()
        target_cid = str(operation.request.get("target_cid") or "").strip()
        if not sha1 or size <= 0 or not target_cid:
            return _rejected("invalid_rapid_source", "秒传源缺少 SHA1、size 或 target_cid")
        client = self._client(operation)
        preid = normalize_sha1(source.get("preid"))
        if not preid and operation.mode is PlaybackMode.POOL:
            from handler.p115_play_pool import _ensure_source_preid_for_play_pool

            preid = normalize_sha1(
                _ensure_source_preid_for_play_pool(
                    dict(source),
                    str(source.get("pick_code") or ""),
                    sha1,
                    file_name,
                )
            )
        if not preid:
            return _rejected("rapid_preid_missing", "播放秒传需要源文件 preid")
        payload = {
            "cid": target_cid,
            "sha1": sha1,
            "size": size,
            "file_name": file_name,
            "pick_code": str(source.get("pick_code") or ""),
            "preid": preid,
            "_rapid_sign_backend": operation.provider_backend,
        }
        checkpoint(_provider_session(operation, phase="rapid_upload"))
        response = client.rapid_upload(payload)
        if isinstance(response, Mapping) and response.get("_rapid_sign_required"):
            main = _primary_client(operation.account_id if operation.mode is not PlaybackMode.POOL else "")
            if main is None:
                return _indeterminate("rapid_holder_unavailable", "秒传签名需要主号客户端")
            sign = main.rapid_sign_value(
                {
                    "sha1": sha1,
                    "pick_code": source.get("pick_code"),
                    "sign_check": response.get("_rapid_sign_check"),
                    "size": size,
                    "file_name": file_name,
                }
            )
            payload["sign_key"] = response.get("_rapid_sign_key")
            payload["sign_val"] = sign.get("sign_val")
            response = client.rapid_upload(payload)
        if not isinstance(response, Mapping) or not response.get("state"):
            return PlaybackProviderOutcome(
                PlaybackProviderDisposition.REJECTED,
                request_sent=True,
                session=_provider_session(operation, phase="rapid_upload"),
                error={
                    "type": "rapid_upload_rejected",
                    "message": str((response or {}).get("error_msg") or (response or {}).get("message") or response)[
                        :500
                    ],
                },
            )
        found = self._readback_prepare(operation)
        if not found:
            return _indeterminate("rapid_upload_readback_missing", "秒传成功但权威回读未找到临时文件")
        found["created_remote"] = True
        return _confirmed(found, request_sent=True, session=_provider_session(operation, phase="rapid_upload"))

    def _copy_prepare(
        self,
        operation: PlaybackOperation,
        source: Mapping[str, Any],
        checkpoint: Callable[[Mapping[str, Any]], None],
    ) -> PlaybackProviderOutcome:
        source_fid = str(source.get("fid") or "").strip()
        target_cid = str(operation.request.get("target_cid") or "").strip()
        if not source_fid or not target_cid:
            return _rejected("copy_source_incomplete", "复制播放缺少源 FID 或 target_cid")
        client = self._client(operation)
        checkpoint(_provider_session(operation, phase="copy"))
        if not hasattr(client, "fs_copy_backend"):
            return _rejected("copy_backend_unavailable", "115 客户端缺少固定后端复制能力")
        response = client.fs_copy_backend([source_fid], target_cid, backend=operation.provider_backend)
        if not isinstance(response, Mapping) or not response.get("state"):
            return PlaybackProviderOutcome(
                PlaybackProviderDisposition.REJECTED,
                request_sent=True,
                session=_provider_session(operation, phase="copy"),
                error={"type": "copy_rejected", "message": str(response)[:500]},
            )
        found = self._readback_prepare(operation)
        if not found:
            return _indeterminate("copy_readback_missing", "复制成功但权威回读未找到临时文件")
        found["created_remote"] = True
        return _confirmed(found, request_sent=True, session=_provider_session(operation, phase="copy"))

    def _ensure_temp_dir(
        self,
        operation: PlaybackOperation,
        checkpoint: Callable[[Mapping[str, Any]], None],
    ) -> PlaybackProviderOutcome:
        existing = self._find_temp_dir(operation)
        if existing:
            return _confirmed({"cid": existing, "created_remote": False}, request_sent=False)
        client = self._client(operation)
        name = str(operation.request.get("temp_dir_name") or "ETK临时目录")
        checkpoint(_provider_session(operation, phase="mkdir"))
        if str(operation.request.get("account_kind") or "") == "primary" and hasattr(client, "fs_mkdir_backend"):
            response = client.fs_mkdir_backend(name, "0", backend=operation.provider_backend)
        else:
            response = client.fs_mkdir(name, "0")
        cid = _response_id(response)
        if not cid:
            cid = self._find_temp_dir(operation)
        if cid:
            return _confirmed(
                {"cid": cid, "created_remote": True},
                request_sent=True,
                session=_provider_session(operation, phase="mkdir"),
            )
        if isinstance(response, Mapping) and not response.get("state"):
            return PlaybackProviderOutcome(
                PlaybackProviderDisposition.REJECTED,
                request_sent=True,
                session=_provider_session(operation, phase="mkdir"),
                error={"type": "mkdir_rejected", "message": str(response)[:500]},
            )
        return _indeterminate("mkdir_readback_missing", "创建临时目录后回读未找到 CID")

    def _speedtest(
        self,
        operation: PlaybackOperation,
        checkpoint: Callable[[Mapping[str, Any]], None],
    ) -> PlaybackProviderOutcome:
        session = dict(operation.provider_session or {})
        speed = session.get("speed") if isinstance(session.get("speed"), Mapping) else None
        found = self._readback_prepare(operation)
        if found is None:
            prepared = self._rapid_prepare(operation, _operation_source(operation), checkpoint)
            if prepared.disposition is not PlaybackProviderDisposition.CONFIRMED:
                return prepared
            found = dict(prepared.receipt or {})
        remote_fid = str(found.get("remote_fid") or "").strip()
        if not remote_fid:
            return _indeterminate("speedtest_remote_fid_missing", "测速临时文件缺少 remote_fid")
        if str(session.get("remote_fid") or "").strip() != remote_fid:
            checkpoint(
                {
                    **_provider_session(operation, phase="speedtest"),
                    "remote_fid": remote_fid,
                }
            )
        if speed is None:
            try:
                url = self._resolve_operation_url(
                    operation,
                    str(found.get("remote_pick_code") or ""),
                    "Mozilla/5.0",
                )
                if not url:
                    return _indeterminate("speedtest_direct_url_missing", "小号获取测速直链失败")
                speed = self._measure(url, user_agent="Mozilla/5.0")
            except Exception:
                return _indeterminate("speedtest_download_failed", "测速下载失败")
        checkpoint(
            {
                **_provider_session(operation, phase="delete"),
                "speed": dict(speed),
                "remote_fid": remote_fid,
            }
        )
        deleted = self._delete_remote(operation, remote_fid)
        if deleted is None:
            return _indeterminate("speedtest_delete_unknown", "测速临时文件删除结果未知")
        if not deleted:
            return PlaybackProviderOutcome(
                PlaybackProviderDisposition.RETRYABLE,
                request_sent=False,
                session={
                    **_provider_session(operation, phase="delete"),
                    "speed": dict(speed),
                    "remote_fid": remote_fid,
                },
                error={"type": "speedtest_delete_not_applied", "message": "测速临时文件仍存在"},
            )
        return _confirmed(
            dict(speed),
            request_sent=True,
            session={
                **_provider_session(operation, phase="delete"),
                "speed": dict(speed),
                "remote_fid": remote_fid,
            },
            readback={"deleted": True},
        )

    def _reclaim(
        self,
        operation: PlaybackOperation,
        checkpoint: Callable[[Mapping[str, Any]], None],
    ) -> PlaybackProviderOutcome:
        remote_fid = str(operation.request.get("remote_fid") or "").strip()
        if not remote_fid:
            return _rejected("reclaim_fid_missing", "播放临时文件回收缺少 remote_fid")
        if operation.request.get("created_remote") is False:
            return _confirmed(
                {"deleted": False, "released": True, "remote_fid": remote_fid},
                request_sent=False,
                readback={"owned_remote": False},
            )
        exists = self._remote_exists(operation, remote_fid)
        if exists is False:
            return _confirmed({"deleted": True, "remote_fid": remote_fid}, request_sent=False)
        if exists is None:
            return _indeterminate("reclaim_preflight_unknown", "无法确认播放临时文件当前状态")
        checkpoint(_provider_session(operation, phase="delete"))
        client = self._client(operation)
        if str(operation.request.get("account_kind") or "") == "primary" and hasattr(client, "fs_delete_backend"):
            response = client.fs_delete_backend([remote_fid], backend=operation.provider_backend)
        elif hasattr(client, "fs_delete_backend"):
            response = client.fs_delete_backend([remote_fid], backend=operation.provider_backend)
        else:
            response = client.fs_delete([remote_fid])
        exists = self._remote_exists(operation, remote_fid)
        if exists is False:
            return _confirmed(
                {"deleted": True, "remote_fid": remote_fid},
                request_sent=True,
                session=_provider_session(operation, phase="delete"),
                readback={"deleted": True},
            )
        if exists is True and isinstance(response, Mapping) and not response.get("state"):
            return PlaybackProviderOutcome(
                PlaybackProviderDisposition.REJECTED,
                request_sent=True,
                session=_provider_session(operation, phase="delete"),
                error={"type": "reclaim_rejected", "message": str(response)[:500]},
            )
        return _indeterminate("reclaim_delete_unknown", "播放临时文件删除后无法确认结果")

    def _readback_delete_phase(self, operation: PlaybackOperation) -> PlaybackProviderOutcome:
        remote_fid = str(
            operation.request.get("remote_fid") or (operation.provider_session or {}).get("remote_fid") or ""
        ).strip()
        if not remote_fid:
            return _indeterminate("delete_readback_fid_missing", "删除回读缺少固定 remote_fid")
        if operation.request.get("created_remote") is False:
            return _confirmed(
                {"deleted": False, "released": True, "remote_fid": remote_fid},
                request_sent=False,
                readback={"owned_remote": False},
            )
        exists = self._remote_exists(operation, remote_fid)
        if exists is None:
            return _indeterminate("delete_readback_unknown", "无法确认播放临时文件是否仍存在")
        if exists:
            return PlaybackProviderOutcome(
                PlaybackProviderDisposition.RETRYABLE,
                request_sent=False,
                session=operation.provider_session,
                readback={"deleted": False},
                error={"type": "delete_not_applied", "message": "权威回读确认播放临时文件仍存在"},
            )
        if operation.kind is PlaybackOperationKind.SPEEDTEST:
            speed = (operation.provider_session or {}).get("speed")
            if not isinstance(speed, Mapping):
                return _indeterminate("speedtest_result_missing", "测速文件已删除但测速结果未持久化")
            return _confirmed(dict(speed), request_sent=True, readback={"deleted": True})
        return _confirmed(
            {"deleted": True, "remote_fid": remote_fid},
            request_sent=True,
            readback={"deleted": True},
        )

    def _readback_prepare(self, operation: PlaybackOperation) -> dict[str, Any] | None:
        source = _operation_source(operation)
        target_cid = str(operation.request.get("target_cid") or "").strip()
        file_name = str(operation.request.get("file_name") or source.get("file_name") or "").strip()
        for item in self._list_directory(operation, target_cid):
            if not _matches_source(item, source, file_name=file_name):
                continue
            remote_fid = _item_id(item)
            remote_pick_code = _item_pick_code(item)
            if remote_fid and remote_pick_code:
                return {
                    "target_cid": target_cid,
                    "remote_fid": remote_fid,
                    "remote_pick_code": remote_pick_code,
                    "file_name": _item_name(item) or file_name,
                    "created_remote": False,
                }
        return None

    def _find_temp_dir(self, operation: PlaybackOperation) -> str:
        wanted = str(operation.request.get("temp_dir_name") or "ETK临时目录")
        for item in self._list_directory(operation, "0", search=wanted):
            if _item_name(item) == wanted and _item_id(item) and _item_is_directory(item):
                return _item_id(item)
        return ""

    def _list_directory(
        self,
        operation: PlaybackOperation,
        cid: str,
        *,
        search: str = "",
    ) -> Iterable[Mapping[str, Any]]:
        if not str(cid or "").strip():
            return ()
        client = self._client(operation)
        offset = 0
        items: list[Mapping[str, Any]] = []
        for _page in range(_MAX_DIRECTORY_PAGES):
            payload: dict[str, Any] = {
                "cid": str(cid),
                "limit": _PAGE_SIZE,
                "offset": offset,
                "show_dir": 1,
                "record_open_time": 0,
                "count_folders": 0,
            }
            if search:
                payload["search_value"] = search
            if hasattr(client, "fs_files_backend"):
                response = client.fs_files_backend(payload, backend=operation.provider_backend)
            else:
                response = client.fs_files(payload)
            page_items, count = _response_items(response)
            items.extend(page_items)
            received = len(page_items)
            offset += received
            if received < _PAGE_SIZE or (count is not None and offset >= count):
                break
            if received == 0:
                break
        else:
            raise RuntimeError("115 临时目录分页超过安全上限")
        return tuple(items)

    def _client(self, operation: PlaybackOperation) -> Any:
        account_kind = str(operation.request.get("account_kind") or "")
        if (
            operation.mode is PlaybackMode.POOL
            or account_kind == "pool"
            or operation.kind is PlaybackOperationKind.SPEEDTEST
        ):
            account = self._config.account(operation.account_id)
            if account is None:
                raise P115PlaybackNotFoundError("小号不存在")
            from handler.p115_play_pool_client import P115PlayPoolClient

            return P115PlayPoolClient(
                str(account.get("cookie") or "") if operation.provider_backend == "cookie" else "",
                str(account.get("app_type") or "alipaymini"),
                str(account.get("access_token") or "") if operation.provider_backend == "openapi" else "",
                str(account.get("refresh_token") or "") if operation.provider_backend == "openapi" else "",
                str(account.get("id") or ""),
                self._refresh_openapi_token,
            )
        client = _primary_client(operation.account_id)
        if client is None:
            raise P115PlaybackNotFoundError("115 Client not initialized")
        return client

    def _refresh_openapi_token(self, account_id: str, failed_token: str | None = None) -> dict[str, Any]:
        account = self._config.account(account_id)
        if account is None:
            return {"ok": False, "message": "小号账号不存在"}
        current_access = str(account.get("access_token") or "")
        if failed_token and current_access and current_access != str(failed_token):
            return {
                "ok": True,
                "access_token": current_access,
                "refresh_token": str(account.get("refresh_token") or ""),
                "reused": True,
            }
        refresh_token = str(account.get("refresh_token") or "")
        if not refresh_token:
            return {"ok": False, "message": "小号缺少 refresh_token"}
        try:
            with self._http_client_factory(timeout=10, follow_redirects=True, trust_env=False) as client:
                response = client.post(
                    "https://passportapi.115.com/open/refreshToken",
                    data={"refresh_token": refresh_token},
                )
                response.raise_for_status()
                payload = response.json()
        except Exception as exc:
            return {"ok": False, "message": str(exc)}
        payload_data = dict(payload) if isinstance(payload, Mapping) else {}
        raw_data = payload_data.get("data")
        data = dict(raw_data) if isinstance(raw_data, Mapping) else {}
        access = str(data.get("access_token") or "")
        rotated_refresh = str(data.get("refresh_token") or "")
        if not payload_data.get("state") or not access:
            return {"ok": False, "message": str(payload_data.get("message") or "小号 Token 续期失败")}
        updated = self._config.compare_and_swap_tokens(
            account_id,
            expected_access_token=current_access,
            access_token=access,
            refresh_token=rotated_refresh,
        )
        if updated is None:
            return {"ok": False, "message": "小号账号不存在"}
        return {
            "ok": True,
            "access_token": str(updated.get("access_token") or ""),
            "refresh_token": str(updated.get("refresh_token") or ""),
        }

    def _resolve_operation_url(self, operation: PlaybackOperation, pick_code: str, user_agent: str) -> str:
        client = self._client(operation)
        if hasattr(client, "resolve_download_url_backend"):
            return str(
                client.resolve_download_url_backend(
                    pick_code,
                    backend=operation.provider_backend,
                    user_agent=user_agent,
                )
                or ""
            )
        return str(client.resolve_download_url(pick_code, user_agent=user_agent) or "")

    def _measure(
        self,
        url: str,
        *,
        user_agent: str,
        max_bytes: int = 16 * 1024 * 1024,
        max_seconds: float = 8,
    ) -> dict[str, Any]:
        headers = {
            "User-Agent": user_agent or "Mozilla/5.0",
            "Accept": "*/*",
            "Range": f"bytes=0-{max_bytes - 1}",
        }
        started = time.monotonic()
        total = 0
        timeout = httpx.Timeout(connect=8, read=max_seconds + 4, write=10, pool=10)
        with self._http_client_factory(timeout=timeout, follow_redirects=True, trust_env=False) as client:
            with client.stream("GET", url, headers=headers) as response:
                response.raise_for_status()
                for chunk in response.iter_bytes(chunk_size=256 * 1024):
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total >= max_bytes or time.monotonic() - started >= max_seconds:
                        break
        elapsed = max(0.001, time.monotonic() - started)
        bps = int(total / elapsed)
        return {
            "bytes": total,
            "seconds": round(elapsed, 3),
            "bps": bps,
            "speed_text": f"{human_bytes(bps)}/s",
        }

    def _delete_remote(self, operation: PlaybackOperation, remote_fid: str) -> bool | None:
        client = self._client(operation)
        try:
            if hasattr(client, "fs_delete_backend"):
                client.fs_delete_backend([remote_fid], backend=operation.provider_backend)
            else:
                client.fs_delete([remote_fid])
        except Exception:
            return None
        return self._remote_exists(operation, remote_fid) is False

    def _remote_exists(self, operation: PlaybackOperation, remote_fid: str) -> bool | None:
        client = self._client(operation)
        try:
            if hasattr(client, "fs_get_info_backend"):
                response = client.fs_get_info_backend(remote_fid, backend=operation.provider_backend)
            else:
                response = client.fs_get_info(remote_fid)
        except Exception:
            return None
        if isinstance(response, Mapping):
            if response.get("state") and (_response_id(response) or response.get("data")):
                return True
            code = str(response.get("code") or response.get("errno") or "").casefold()
            message = str(response.get("message") or response.get("error_msg") or "").casefold()
            if code in {"404", "20018", "not_found", "file_not_found"} or any(
                marker in message for marker in ("not found", "不存在", "已删除")
            ):
                return False
        return None


class LegacyP115DirectLinkGateway(P115DirectLinkGateway):
    def __init__(
        self,
        config: P115PlaybackConfigService,
        *,
        client_factory: Callable[..., httpx.Client] = httpx.Client,
    ) -> None:
        self._config = config
        self._client_factory = client_factory

    def resolve_primary(self, pick_code: str, user_agent: str) -> str:
        client = _primary_client("")
        if client is None:
            raise P115PlaybackNotFoundError("115 Client not initialized")
        return str(client.resolve_download_url(pick_code, user_agent=user_agent) or "")

    def resolve_artifact(self, artifact: PlaybackArtifact, user_agent: str) -> str:
        if artifact.mode is PlaybackMode.POOL:
            from handler.p115_play_pool_client import P115PlayPoolClient

            account = self._config.account(artifact.account_id)
            if account is None:
                return ""
            client = P115PlayPoolClient(
                str(account.get("cookie") or "") if artifact.provider_backend == "cookie" else "",
                str(account.get("app_type") or "alipaymini"),
                str(account.get("access_token") or "") if artifact.provider_backend == "openapi" else "",
                str(account.get("refresh_token") or "") if artifact.provider_backend == "openapi" else "",
                str(account.get("id") or ""),
            )
            return str(client.resolve_download_url(artifact.remote_pick_code, user_agent=user_agent) or "")
        client = _primary_client(artifact.account_id)
        if client is None:
            return ""
        if hasattr(client, "resolve_download_url_backend"):
            return str(
                client.resolve_download_url_backend(
                    artifact.remote_pick_code,
                    backend=artifact.provider_backend,
                    user_agent=user_agent,
                )
                or ""
            )
        return str(client.resolve_download_url(artifact.remote_pick_code, user_agent=user_agent) or "")

    def stream(self, url: str, *, user_agent: str, range_header: str = "") -> PlaybackUpstreamStream:
        headers = {
            "User-Agent": user_agent,
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        }
        if range_header:
            headers["Range"] = range_header
        client = self._client_factory(timeout=10, follow_redirects=True, trust_env=False)
        response: httpx.Response | None = None
        try:
            request = client.build_request("GET", url, headers=headers)
            response = client.send(request, stream=True)
            excluded = {"content-encoding", "transfer-encoding", "connection", "host"}
            content_encoding = str(response.headers.get("content-encoding") or "").strip().casefold()
            if content_encoding and content_encoding != "identity":
                excluded.add("content-length")
            response_headers = {
                name: value for name, value in response.headers.items() if name.casefold() not in excluded
            }
        except BaseException:
            if response is not None:
                with suppress(Exception):
                    response.close()
            with suppress(Exception):
                client.close()
            raise

        def close() -> None:
            with suppress(Exception):
                response.close()
            with suppress(Exception):
                client.close()

        return PlaybackUpstreamStream(
            status_code=response.status_code,
            headers=response_headers,
            chunks=response.iter_bytes(chunk_size=8192),
            close=close,
        )


def _primary_client(expected_account_id: str) -> Any | None:
    from handler.p115_service import P115Service

    expected = str(expected_account_id or "").strip()
    return P115Service.get_client(expected_account_id=expected or None)


def _source_snapshot(
    value: Mapping[str, Any] | None,
    *,
    fallback_pick_code: str = "",
    fallback_name: str = "",
) -> dict[str, Any]:
    row = dict(value or {})
    raw_rapid = row.get("rapid_meta_json")
    rapid = dict(raw_rapid) if isinstance(raw_rapid, Mapping) else {}
    snapshot = {
        "sha1": normalize_sha1(row.get("sha1") or row.get("file_sha1")),
        "size": _integer(row.get("size") or row.get("file_size")),
        "preid": normalize_sha1(
            row.get("preid") or rapid.get("preid") or rapid.get("pre_sha1") or rapid.get("pre_sha1_128k")
        ),
        "pick_code": str(
            row.get("pick_code") or row.get("pickcode") or row.get("pc") or fallback_pick_code or ""
        ).strip(),
        "fid": str(row.get("fid") or row.get("file_id") or row.get("id") or "").strip(),
        "holder_account_id": str(
            row.get("holder_account_id") or row.get("local_sign_holder_account_id") or row.get("account_id") or ""
        ).strip(),
        "file_name": str(row.get("file_name") or row.get("name") or row.get("n") or fallback_name or "").strip(),
    }
    return safe_playback_json(snapshot, field="playback source snapshot")


def _operation_source(operation: PlaybackOperation) -> dict[str, Any]:
    value = operation.request.get("source")
    if not isinstance(value, Mapping):
        raise ValueError("playback operation source snapshot is missing")
    return dict(value)


def _provider_session(operation: PlaybackOperation, *, phase: str) -> dict[str, Any]:
    return {
        "account_id": operation.account_id,
        "backend": operation.provider_backend,
        "phase": phase,
        "kind": operation.kind.value,
    }


def _confirmed(
    receipt: Mapping[str, Any],
    *,
    request_sent: bool,
    session: Mapping[str, Any] | None = None,
    readback: Mapping[str, Any] | None = None,
) -> PlaybackProviderOutcome:
    return PlaybackProviderOutcome(
        PlaybackProviderDisposition.CONFIRMED,
        request_sent=request_sent,
        receipt=dict(receipt),
        session=dict(session) if session is not None else None,
        readback=dict(readback) if readback is not None else None,
    )


def _rejected(error_type: str, message: str) -> PlaybackProviderOutcome:
    return PlaybackProviderOutcome(
        PlaybackProviderDisposition.REJECTED,
        request_sent=False,
        error={"type": error_type, "message": str(message)[:500]},
    )


def _indeterminate(error_type: str, message: str) -> PlaybackProviderOutcome:
    return PlaybackProviderOutcome(
        PlaybackProviderDisposition.INDETERMINATE,
        request_sent=True,
        error={"type": error_type, "message": str(message)[:500]},
    )


def _response_items(value: Any) -> tuple[list[Mapping[str, Any]], int | None]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, Mapping)], len(value)
    if not isinstance(value, Mapping):
        raise RuntimeError("115 目录返回无效数据")
    if value.get("state") is False:
        raise RuntimeError(str(value.get("error_msg") or value.get("message") or "115 目录查询失败"))
    data: Any = value.get("data")
    count: int | None = _optional_integer(value.get("count") or value.get("total"))
    if isinstance(data, Mapping):
        count = _optional_integer(data.get("count") or data.get("total")) or count
        data = data.get("list") or data.get("items") or data.get("data") or []
    if data in (None, ""):
        data = value.get("list") or value.get("items") or []
    if not isinstance(data, list):
        raise RuntimeError("115 目录列表结构无效")
    return [item for item in data if isinstance(item, Mapping)], count


def _matches_source(item: Mapping[str, Any], source: Mapping[str, Any], *, file_name: str) -> bool:
    wanted_sha1 = normalize_sha1(source.get("sha1"))
    wanted_size = int(source.get("size") or 0)
    item_sha1 = normalize_sha1(item.get("sha1") or item.get("sha"))
    item_size = _integer(item.get("size") or item.get("file_size") or item.get("s"))
    if wanted_sha1 and item_sha1 == wanted_sha1 and (wanted_size <= 0 or item_size == wanted_size):
        return True
    return bool(file_name and _item_name(item) == file_name and (wanted_size <= 0 or item_size == wanted_size))


def _response_id(value: Any) -> str:
    if not isinstance(value, Mapping):
        return ""
    for key in ("cid", "file_id", "fid", "id"):
        if value.get(key) not in (None, "", [], {}):
            return str(value[key])
    data = value.get("data")
    return _response_id(data) if isinstance(data, Mapping) else ""


def _item_id(item: Mapping[str, Any]) -> str:
    return str(item.get("fid") or item.get("file_id") or item.get("id") or item.get("cid") or "").strip()


def _item_pick_code(item: Mapping[str, Any]) -> str:
    return str(item.get("pick_code") or item.get("pickcode") or item.get("pc") or "").strip()


def _item_name(item: Mapping[str, Any]) -> str:
    return str(item.get("name") or item.get("file_name") or item.get("n") or item.get("fn") or "").strip()


def _item_is_directory(item: Mapping[str, Any]) -> bool:
    value = item.get("fc") if item.get("fc") is not None else item.get("type")
    if value in (None, ""):
        return not bool(normalize_sha1(item.get("sha1")))
    return str(value) == "0"


def _integer(value: Any) -> int:
    try:
        return max(0, int(float(value or 0)))
    except (TypeError, ValueError, OverflowError):
        return 0


def _optional_integer(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return _integer(value)


__all__ = [
    "LegacyP115DirectLinkGateway",
    "LegacyP115PlaybackProvider",
    "LegacyP115PlaybackSourceGateway",
    "LegacyP115PlaybackUserGateway",
]
