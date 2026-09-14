from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from copy import deepcopy
from datetime import UTC, datetime
from ipaddress import ip_address
from typing import Any, Protocol
from urllib.parse import urljoin, urlsplit

import httpx
from sqlalchemy import text

from etk.domains.sharing.rapid_transfer import (
    NativeP115RapidTransferGateway,
    RapidTransferReadback,
    RapidUploadOutcome,
)
from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs.json_payload import canonical_json, normalize_json_object

from .auth_directory import P115CredentialStore
from .direct_url_transport import P115SdkDirectUrlGateway
from .directory_transport import HttpxP115DirectoryTreeGateway
from .maintenance_transport import (
    HttpxP115BoundedDownloadGateway,
    P115PinnedOperationsClient,
    P115PrimaryOperationsGateway,
    item_is_directory,
    provider_succeeded,
    response_ids,
)
from .media_info_adapters import BoundedRangeReader, PreIdCandidates, resolve_cached_preid
from .models import P115CredentialSnapshot
from .playback import (
    P115DirectLinkGateway,
    P115PlaybackConfigService,
    P115PlaybackNotFoundError,
    P115PlaybackProvider,
    P115PlaybackStateError,
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
    pool_account_revision,
    pool_credential_fingerprint,
    safe_playback_json,
)

_DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_SIGN_CHECK = re.compile(r"([0-9]{1,20})-([0-9]{1,20})")
_RANGE_HEADER = re.compile(
    r"bytes=(?:[0-9]+-[0-9]*|[0-9]*-[0-9]+)(?:,(?:[0-9]+-[0-9]*|[0-9]*-[0-9]+))*",
    re.IGNORECASE,
)
_MAX_SOURCE_ROWS = 50
_STREAM_RESPONSE_HEADERS = frozenset(
    {
        "accept-ranges",
        "cache-control",
        "content-disposition",
        "content-length",
        "content-range",
        "content-type",
        "etag",
        "last-modified",
    }
)
_PLAYBACK_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_MAX_PLAYBACK_REDIRECTS = 5

type PlaybackStateMutation = Callable[
    [dict[str, Any] | None, datetime],
    tuple[dict[str, Any] | None, Any, bool],
]


class P115PlaybackCredentialChanged(RuntimeError):
    pass


class SqlAlchemyP115PlaybackConfigBackend:
    """Serialize one playback setting through the native SQLAlchemy UoW."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def mutate(self, setting_key: str, mutation: PlaybackStateMutation) -> Any:
        key = str(setting_key or "").strip()
        if not key:
            raise ValueError("playback setting key is required")
        with self._unit_of_work() as uow:
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:setting_key, 0))"),
                {"setting_key": key},
            )
            current_time = uow.session.scalar(text("SELECT CURRENT_TIMESTAMP"))
            if not isinstance(current_time, datetime):
                raise TypeError("database current time must be a datetime")
            now = current_time.replace(tzinfo=UTC) if current_time.tzinfo is None else current_time.astimezone(UTC)
            raw = uow.session.scalar(
                text("SELECT value_json FROM app_settings WHERE setting_key = :setting_key"),
                {"setting_key": key},
            )
            current = _playback_state(raw)
            next_state, result, should_write = mutation(current, now)
            if should_write:
                if next_state is None:
                    uow.session.execute(
                        text("DELETE FROM app_settings WHERE setting_key = :setting_key"),
                        {"setting_key": key},
                    )
                else:
                    normalized = normalize_json_object(next_state, field="playback state")
                    uow.session.execute(
                        text(
                            """
                            INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                            VALUES (:setting_key, CAST(:value_json AS jsonb), CURRENT_TIMESTAMP)
                            ON CONFLICT (setting_key) DO UPDATE SET
                                value_json = EXCLUDED.value_json,
                                last_updated_at = CURRENT_TIMESTAMP
                            """
                        ),
                        {
                            "setting_key": key,
                            "value_json": canonical_json(normalized, field="playback state"),
                        },
                    )
            uow.commit()
            return result


class P115PoolPlaybackClient(Protocol):
    def find_directory(self, name: str) -> str: ...

    def create_directory(self, name: str) -> Mapping[str, Any]: ...

    def readback_source(self, source: Mapping[str, Any], target_cid: str) -> Mapping[str, Any] | None: ...

    def rapid_upload(
        self,
        source: Mapping[str, Any],
        target_cid: str,
        *,
        sign_key: str = "",
        sign_val: str = "",
    ) -> RapidUploadOutcome: ...

    def copy_source(self, source_fid: str, target_cid: str) -> Mapping[str, Any]: ...

    def resolve_download(self, pick_code: str, user_agent: str) -> str: ...

    def measure(self, url: str, user_agent: str) -> Mapping[str, Any]: ...

    def remote_exists(self, remote_fid: str, target_cid: str) -> bool | None: ...

    def delete_remote(self, remote_fid: str, target_cid: str) -> bool | None: ...


class P115PoolPlaybackClientFactory(Protocol):
    def open(self, operation: PlaybackOperation) -> P115PoolPlaybackClient: ...


class P115RapidSigner(Protocol):
    def sign(self, source: Mapping[str, Any], sign_check: str) -> str: ...

    def preid(self, source: Mapping[str, Any]) -> str: ...


class P115PlayPoolCredentialStore(P115CredentialStore):
    """Expose one local pool account as a credential-fenced provider identity."""

    def __init__(
        self,
        config: P115PlaybackConfigService,
        *,
        account_id: str,
        backend: str,
        expected_fingerprint: str,
        expected_account_revision: str,
    ) -> None:
        self._config = config
        self._account_id = str(account_id or "").strip()
        self._backend = str(backend or "").strip().casefold()
        self._expected_fingerprint = str(expected_fingerprint or "").strip().casefold()
        self._expected_account_revision = str(expected_account_revision or "").strip().casefold()
        if not self._account_id or self._backend not in {"openapi", "cookie"}:
            raise ValueError("pool credential identity is invalid")
        if re.fullmatch(r"[0-9a-f]{64}", self._expected_fingerprint) is None:
            raise ValueError("pool credential fingerprint is invalid")
        if re.fullmatch(r"[0-9a-f]{64}", self._expected_account_revision) is None:
            raise ValueError("pool account revision is invalid")

    def load(self) -> P115CredentialSnapshot:
        account = self._config.account(self._account_id)
        if account is None:
            raise P115PlaybackCredentialChanged("play-pool account no longer exists")
        actual = pool_credential_fingerprint(account, self._backend)
        if actual != self._expected_fingerprint:
            raise P115PlaybackCredentialChanged("play-pool credential changed after reservation")
        if pool_account_revision(account, self._backend) != self._expected_account_revision:
            raise P115PlaybackCredentialChanged("play-pool account configuration changed after reservation")
        return P115CredentialSnapshot(
            access_token=str(account.get("access_token") or "") if self._backend == "openapi" else "",
            refresh_token=str(account.get("refresh_token") or "") if self._backend == "openapi" else "",
            cookie=str(account.get("cookie") or "") if self._backend == "cookie" else "",
            app_type=str(account.get("app_type") or "alipaymini"),
            account_id=self._account_id,
        )

    def save_openapi(
        self,
        access_token: str | None,
        refresh_token: str | None,
        *,
        account_id: str,
        expected_access_token: str | None = None,
    ) -> None:
        if self._backend != "openapi" or str(account_id or "").strip() != self._account_id:
            raise P115PlaybackCredentialChanged("play-pool OpenAPI identity changed")
        current = self.load()
        expected = str(expected_access_token if expected_access_token is not None else current.access_token)
        updated = self._config.compare_and_swap_tokens(
            self._account_id,
            expected_access_token=expected,
            access_token=str(access_token or ""),
            refresh_token=str(refresh_token or ""),
        )
        if updated is None or str(updated.get("access_token") or "") != str(access_token or ""):
            raise P115PlaybackCredentialChanged("play-pool OpenAPI token compare-and-swap failed")

    def save_cookie(self, cookie: str, app_type: str) -> None:
        del cookie, app_type
        raise P115PlaybackCredentialChanged("play-pool Cookie replacement requires a new intent")

    @staticmethod
    def app_label(app_type: str) -> str:
        return str(app_type or "web")


class _PinnedPrimaryCredentialStore(P115CredentialStore):
    def __init__(
        self,
        credentials: P115CredentialStore,
        *,
        account_id: str,
        backend: str,
        expected_fingerprint: str,
    ) -> None:
        self._credentials = credentials
        self._account_id = str(account_id or "").strip()
        self._backend = str(backend or "").strip().casefold()
        self._expected_fingerprint = str(expected_fingerprint or "").strip().casefold()
        if not self._account_id or self._backend not in {"openapi", "cookie"}:
            raise ValueError("primary playback credential identity is invalid")
        if re.fullmatch(r"[0-9a-f]{64}", self._expected_fingerprint) is None:
            raise ValueError("primary playback credential fingerprint is invalid")

    def load(self) -> P115CredentialSnapshot:
        snapshot = self._credentials.load()
        if str(snapshot.account_id or "").strip() != self._account_id:
            raise P115PlaybackCredentialChanged("primary account changed after reservation")
        actual = pool_credential_fingerprint(
            {
                "access_token": snapshot.access_token,
                "cookie": snapshot.cookie,
                "app_type": snapshot.app_type,
            },
            self._backend,
        )
        if actual != self._expected_fingerprint:
            raise P115PlaybackCredentialChanged("primary credential changed after reservation")
        return snapshot

    def save_openapi(
        self,
        access_token: str | None,
        refresh_token: str | None,
        *,
        account_id: str,
        expected_access_token: str | None = None,
    ) -> None:
        del access_token, refresh_token, account_id, expected_access_token
        raise P115PlaybackCredentialChanged("primary OpenAPI replacement requires a new intent")

    def save_cookie(self, cookie: str, app_type: str) -> None:
        del cookie, app_type
        raise P115PlaybackCredentialChanged("primary Cookie replacement requires a new intent")

    def app_label(self, app_type: str) -> str:
        return self._credentials.app_label(app_type)


class SqlAlchemyP115PlaybackSourceGateway:
    """Resolve playback sources from native PostgreSQL projections without provider I/O."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        *,
        primary_credentials: P115CredentialStore | None = None,
        priority: Callable[[], str] | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._primary_credentials = primary_credentials
        self._priority = priority or (lambda: "openapi")

    def snapshot(self, request: PlaybackPrepareRequest) -> Mapping[str, Any]:
        if request.mode is PlaybackMode.VIRTUAL:
            return self._virtual(request)
        if request.mode is PlaybackMode.POOL:
            return self._pool(request)
        if request.mode is PlaybackMode.COPY:
            return self._copy(request)
        if request.mode is PlaybackMode.MAINTENANCE and request.source == "speedtest":
            return self._speedtest()
        raise ValueError(f"unsupported native playback source mode: {request.mode.value}")

    def _speedtest(self) -> Mapping[str, Any]:
        primary_account_id = self._primary_account_id()
        ambiguous = False
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    """
                    SELECT tmdb_id, item_type, title, parent_series_tmdb_id,
                           season_number, episode_number, file_pickcode_json,
                           file_sha1_json, asset_details_json
                    FROM media_metadata
                    WHERE in_library = TRUE
                      AND item_type IN ('Movie', 'Episode')
                      AND jsonb_typeof(file_pickcode_json) = 'array'
                      AND jsonb_typeof(file_sha1_json) = 'array'
                    ORDER BY last_updated_at DESC NULLS LAST, tmdb_id, item_type
                    LIMIT :limit
                    """
                ),
                {"limit": _MAX_SOURCE_ROWS},
            ).mappings()
            for row in rows:
                pick_codes = _json_list(row.get("file_pickcode_json"))
                sha1s = _json_list(row.get("file_sha1_json"))
                for index, raw_pick_code in enumerate(pick_codes):
                    pick_code = str(raw_pick_code or "").strip()
                    sha1 = normalize_sha1(sha1s[index] if index < len(sha1s) else "")
                    if not pick_code or not sha1:
                        continue
                    matches = list(
                        uow.session.execute(
                            text(
                                """
                                SELECT id, account_id, name, pick_code, sha1, size
                                FROM p115_filesystem_cache
                                WHERE entry_kind = 'file'
                                  AND UPPER(sha1) = :sha1
                                  AND (
                                    CAST(:primary_account_id AS text) = ''
                                    AND pick_code = :pick_code
                                    OR account_id = :primary_account_id
                                  )
                                ORDER BY updated_at DESC NULLS LAST, id
                                LIMIT 2
                                """
                            ),
                            {
                                "pick_code": pick_code,
                                "sha1": sha1,
                                "primary_account_id": primary_account_id,
                            },
                        ).mappings()
                    )
                    if len(matches) != 1:
                        ambiguous = ambiguous or len(matches) > 1
                        continue
                    source = self._complete_source(
                        uow,
                        dict(matches[0]),
                        fallback_name=_asset_name(row.get("asset_details_json"), index) or str(row.get("title") or ""),
                    )
                    if source:
                        return source

            if ambiguous:
                raise P115PlaybackStateError("测速源在 115 本地缓存中存在歧义，已拒绝选择")
            fallback_rows = (
                uow.session.execute(
                    text(
                        """
                    SELECT cache.id, cache.account_id, cache.name, cache.pick_code,
                           cache.sha1, cache.size,
                           COUNT(*) OVER (
                             PARTITION BY cache.account_id, UPPER(cache.sha1), cache.size
                           ) AS identity_count
                    FROM p115_filesystem_cache AS cache
                    JOIN p115_mediainfo_cache AS info
                      ON UPPER(info.sha1) = UPPER(cache.sha1)
                    WHERE cache.entry_kind = 'file'
                      AND cache.pick_code IS NOT NULL AND cache.pick_code <> ''
                      AND cache.sha1 ~* '^[0-9a-f]{40}$'
                      AND cache.size > 0
                      AND cache.account_id ~ '^[0-9]+$'
                      AND (CAST(:primary_account_id AS text) = '' OR cache.account_id = :primary_account_id)
                    ORDER BY cache.updated_at DESC NULLS LAST, cache.id
                    """
                    ),
                    {"primary_account_id": primary_account_id},
                )
                .mappings()
                .yield_per(100)
            )
            for row in fallback_rows:
                if int(row.get("identity_count") or 0) != 1:
                    ambiguous = True
                    continue
                source = self._complete_source(uow, dict(row), fallback_name=str(row.get("name") or ""))
                if source:
                    return source
            if ambiguous:
                raise P115PlaybackStateError("测速源在 115 本地缓存中存在歧义，已拒绝选择")
        raise P115PlaybackNotFoundError("未找到同时具备 PickCode、SHA1、size、preid 和 holder 的测速源")

    def _pool(self, request: PlaybackPrepareRequest) -> Mapping[str, Any]:
        account_id = self._interactive_primary_account_id()
        item_id = str(request.item_id or "").strip()
        requested_pick_code = str(request.source_pick_code or "").strip()
        if not item_id:
            raise P115PlaybackNotFoundError("小号播放缺少 Emby item identity")
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    """
                    WITH target_item AS (
                        SELECT tmdb_id, item_type
                        FROM media_metadata
                        WHERE emby_item_ids_json @> CAST(:item_ids AS jsonb)
                    )
                    SELECT m.emby_item_ids_json, m.file_pickcode_json,
                           m.file_sha1_json, m.asset_details_json, m.title
                    FROM media_metadata AS m
                    JOIN target_item AS target
                      ON (m.tmdb_id = target.tmdb_id AND m.item_type = target.item_type)
                      OR (m.parent_series_tmdb_id = target.tmdb_id
                          AND target.item_type = 'Series' AND m.item_type = 'Episode')
                    ORDER BY m.last_updated_at DESC NULLS LAST, m.tmdb_id, m.item_type
                    """
                ),
                {"item_ids": json.dumps([item_id], ensure_ascii=False)},
            ).mappings()
            candidates: list[dict[str, Any]] = []
            for row in rows:
                emby_ids = [str(value or "").strip() for value in _json_list(row.get("emby_item_ids_json"))]
                pick_codes = [str(value or "").strip() for value in _json_list(row.get("file_pickcode_json"))]
                sha1s = [normalize_sha1(value) for value in _json_list(row.get("file_sha1_json"))]
                assets = _json_list(row.get("asset_details_json"))
                if len({len(emby_ids), len(pick_codes), len(sha1s), len(assets)}) != 1:
                    raise P115PlaybackStateError("媒体物理资产数组未对齐，已拒绝选择播放源")
                for index, raw_asset in enumerate(assets):
                    if not isinstance(raw_asset, Mapping):
                        raise P115PlaybackStateError("媒体物理资产数组包含无效项，已拒绝选择播放源")
                    asset = dict(raw_asset)
                    candidates.append(
                        {
                            "pick_code": pick_codes[index] if index < len(pick_codes) else "",
                            "sha1": sha1s[index] if index < len(sha1s) else "",
                            "size": _positive_int(asset.get("size") or asset.get("file_size")),
                            "file_name": str(
                                asset.get("file_name")
                                or asset.get("name")
                                or str(asset.get("path") or "").replace("\\", "/").rsplit("/", 1)[-1]
                                or row.get("title")
                                or request.file_name
                                or ""
                            ).strip(),
                        }
                    )
            if requested_pick_code and any(item["pick_code"] == requested_pick_code for item in candidates):
                candidates = [item for item in candidates if item["pick_code"] == requested_pick_code]
            resolved: list[dict[str, Any]] = []
            for candidate in candidates:
                source = self._cached_interactive_source(
                    uow,
                    account_id=account_id,
                    pick_code=requested_pick_code or str(candidate["pick_code"]),
                    sha1=str(candidate["sha1"]),
                    fallback_size=int(candidate["size"]),
                    fallback_name=str(candidate["file_name"] or request.file_name),
                )
                if source:
                    resolved.append(source)
            if len(resolved) > 1:
                raise P115PlaybackStateError("播放源候选存在歧义，已拒绝选择")
            if resolved:
                return resolved[0]
        raise P115PlaybackNotFoundError("未找到与 Emby 媒体匹配的主号 115 播放源")

    def _copy(self, request: PlaybackPrepareRequest) -> Mapping[str, Any]:
        account_id = self._interactive_primary_account_id()
        pick_code = str(request.source_pick_code or "").strip()
        if not pick_code:
            raise P115PlaybackNotFoundError("复制播放缺少源 PickCode")
        with self._unit_of_work() as uow:
            source = self._cached_interactive_source(
                uow,
                account_id=account_id,
                pick_code=pick_code,
                sha1="",
                fallback_size=0,
                fallback_name=str(request.file_name or ""),
            )
        if not source:
            raise P115PlaybackNotFoundError("主号 115 缓存中未找到复制播放源")
        return source

    def _virtual(self, request: PlaybackPrepareRequest) -> Mapping[str, Any]:
        account_id = self._interactive_primary_account_id()
        virtual_id = _positive_int(request.virtual_id)
        sha1 = normalize_sha1(request.sha1)
        if virtual_id <= 0 or not sha1:
            raise P115PlaybackNotFoundError("虚拟播放源身份无效")
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text("SELECT * FROM shared_virtual_imports WHERE id = :virtual_id"),
                    {"virtual_id": virtual_id},
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            raise P115PlaybackNotFoundError("Virtual import not found")
        matches = [
            {str(key): value for key, value in item.items()}
            for item in _json_list(row.get("files_json"))
            if isinstance(item, Mapping) and normalize_sha1(item.get("sha1")) == sha1
        ]
        if len(matches) > 1:
            raise P115PlaybackStateError("Virtual file SHA1 存在歧义，已拒绝选择")
        if not matches:
            raise P115PlaybackNotFoundError("Virtual file not found")
        file_info = matches[0]
        status = str(row.get("status") or "").strip().casefold()
        if status in {"deleted", "disabled", "cancelled", "canceled", "failed"}:
            raise P115PlaybackNotFoundError("Virtual import is not playable")
        declared_holder = str(
            file_info.get("holder_account_id") or file_info.get("local_sign_holder_account_id") or ""
        ).strip()
        if declared_holder and declared_holder != account_id:
            raise P115PlaybackStateError("虚拟播放源 holder 与当前主号身份不一致")
        raw_rapid = file_info.get("rapid_meta_json")
        rapid: dict[str, Any] = (
            {str(key): value for key, value in raw_rapid.items()} if isinstance(raw_rapid, Mapping) else {}
        )
        source = {
            "fid": str(file_info.get("fid") or file_info.get("file_id") or rapid.get("fid") or "").strip(),
            "holder_account_id": account_id,
            "pick_code": str(
                file_info.get("pick_code")
                or file_info.get("pickcode")
                or file_info.get("pc")
                or rapid.get("pick_code")
                or ""
            ).strip(),
            "sha1": sha1,
            "size": _positive_int(file_info.get("size") or file_info.get("file_size")),
            "preid": normalize_sha1(
                file_info.get("preid") or rapid.get("preid") or rapid.get("pre_sha1") or rapid.get("pre_sha1_128k")
            ),
            "file_name": str(
                file_info.get("file_name") or file_info.get("name") or request.file_name or f"{sha1}.mkv"
            ).strip(),
            "virtual_id": virtual_id,
            "source_kind": str(row.get("source_kind") or file_info.get("source_kind") or "")[:64],
            "source_id": str(
                row.get("source_id") or file_info.get("source_id") or file_info.get("source_ref_id") or ""
            )[:255],
        }
        if not source["pick_code"] or source["size"] <= 0:
            raise P115PlaybackNotFoundError("Virtual file is missing rapid-transfer identity")
        if not source["source_kind"] or not source["source_id"]:
            raise P115PlaybackStateError("Virtual playback source provenance is incomplete")
        return safe_playback_json(source, field="virtual playback source")

    @staticmethod
    def _cached_interactive_source(
        uow: SqlAlchemyUnitOfWork,
        *,
        account_id: str,
        pick_code: str,
        sha1: str,
        fallback_size: int,
        fallback_name: str,
    ) -> dict[str, Any]:
        normalized_pick_code = str(pick_code or "").strip()
        normalized_sha1 = normalize_sha1(sha1)
        rows: list[dict[str, Any]] = []
        if normalized_pick_code:
            rows = [
                {str(key): value for key, value in row.items()}
                for row in uow.session.execute(
                    text(
                        """
                        SELECT id, account_id, name, pick_code, sha1, size
                        FROM p115_filesystem_cache
                        WHERE account_id = :account_id
                          AND (entry_kind = 'file' OR entry_kind IS NULL)
                          AND pick_code = :pick_code
                          AND (CAST(:sha1 AS text) = '' OR UPPER(sha1) = :sha1)
                        ORDER BY updated_at DESC NULLS LAST, id
                        LIMIT 2
                        """
                    ),
                    {"account_id": account_id, "pick_code": normalized_pick_code, "sha1": normalized_sha1},
                ).mappings()
            ]
        if not rows and not normalized_pick_code and normalized_sha1:
            rows = [
                {str(key): value for key, value in row.items()}
                for row in uow.session.execute(
                    text(
                        """
                        SELECT id, account_id, name, pick_code, sha1, size
                        FROM p115_filesystem_cache
                        WHERE account_id = :account_id
                          AND (entry_kind = 'file' OR entry_kind IS NULL)
                          AND UPPER(sha1) = :sha1
                        ORDER BY updated_at DESC NULLS LAST, id
                        LIMIT 2
                        """
                    ),
                    {"account_id": account_id, "sha1": normalized_sha1},
                ).mappings()
            ]
        if len(rows) > 1:
            raise P115PlaybackStateError("主号 115 播放源缓存存在歧义")
        if not rows:
            return {}
        row = dict(rows[0])
        if _positive_int(row.get("size")) <= 0 and fallback_size > 0:
            row["size"] = fallback_size
        return SqlAlchemyP115PlaybackSourceGateway._complete_source(
            uow,
            row,
            fallback_name=fallback_name,
            require_preid=False,
        )

    def primary_account(self) -> Mapping[str, Any]:
        if self._primary_credentials is None:
            return {}
        snapshot = self._primary_credentials.load()
        account_id = str(snapshot.account_id or "").strip()
        if not account_id.isdigit():
            raise P115PlaybackStateError("主号缺少稳定的 115 holder 身份")
        priority = str(self._priority() or "openapi").strip().casefold()
        if priority == "cookie" and snapshot.cookie:
            backend = "cookie"
        elif snapshot.access_token:
            backend = "openapi"
        elif snapshot.cookie:
            backend = "cookie"
        else:
            backend = ""
        credential_hash = ""
        if backend:
            credential_hash = pool_credential_fingerprint(
                {
                    "access_token": snapshot.access_token,
                    "cookie": snapshot.cookie,
                    "app_type": snapshot.app_type,
                },
                backend,
            )
        return {
            "account_id": account_id,
            "provider_backend": backend,
            "credential_hash": credential_hash,
        }

    def _primary_account_id(self) -> str:
        if self._primary_credentials is None:
            return ""
        account_id = str(self._primary_credentials.load().account_id or "").strip()
        if not account_id.isdigit():
            raise P115PlaybackStateError("主号缺少稳定的 115 holder 身份")
        return account_id

    def _interactive_primary_account_id(self) -> str:
        account_id = self._primary_account_id()
        if not account_id:
            raise P115PlaybackNotFoundError("主号客户端未初始化")
        return account_id

    @staticmethod
    def _complete_source(
        uow: SqlAlchemyUnitOfWork,
        row: Mapping[str, Any],
        *,
        fallback_name: str,
        require_preid: bool = True,
    ) -> dict[str, Any]:
        fid = str(row.get("id") or "").strip()
        holder_account_id = str(row.get("account_id") or "").strip()
        pick_code = str(row.get("pick_code") or "").strip()
        sha1 = normalize_sha1(row.get("sha1"))
        size = _positive_int(row.get("size"))
        if not fid or not holder_account_id.isdigit() or not pick_code or not sha1 or size <= 0:
            return {}
        raw = uow.session.scalar(
            text("SELECT raw_ffprobe_json FROM p115_mediainfo_cache WHERE UPPER(sha1) = :sha1"),
            {"sha1": sha1},
        )
        preid = resolve_cached_preid(PreIdCandidates(cached_raw=raw))
        if preid is None and require_preid:
            return {}
        return {
            "fid": fid,
            "holder_account_id": holder_account_id,
            "pick_code": pick_code,
            "sha1": sha1,
            "size": size,
            "preid": preid.value if preid is not None else "",
            "file_name": str(row.get("name") or fallback_name or f"{sha1}.mkv").strip(),
        }


class NativeP115PlaybackRapidSigner:
    """Compute one status=7 sign value from a primary-holder exact Range read."""

    def __init__(
        self,
        direct_urls: P115SdkDirectUrlGateway,
        credentials: P115CredentialStore,
        *,
        reader_factory: Callable[..., BoundedRangeReader] = BoundedRangeReader,
    ) -> None:
        self._direct_urls = direct_urls
        self._credentials = credentials
        self._reader_factory = reader_factory

    def sign(self, source: Mapping[str, Any], sign_check: str) -> str:
        match = _SIGN_CHECK.fullmatch(str(sign_check or "").strip())
        if match is None:
            raise ValueError("rapid sign_check is invalid")
        start, end = (int(value) for value in match.groups())
        if end < start or end - start + 1 > 16 * 1024 * 1024:
            raise ValueError("rapid sign_check exceeds the bounded Range limit")
        return self._range_sha1(source, start=start, length=end - start + 1)

    def preid(self, source: Mapping[str, Any]) -> str:
        size = _positive_int(source.get("size"))
        if size <= 0:
            raise P115PlaybackStateError("播放源缺少有效 size，无法计算 preid")
        return self._range_sha1(source, start=0, length=min(size, 128 * 1024))

    def _range_sha1(self, source: Mapping[str, Any], *, start: int, length: int) -> str:
        account_id = str(source.get("holder_account_id") or "").strip()
        if not account_id.isdigit():
            raise P115PlaybackStateError("播放源缺少稳定的主号 holder 身份")
        expected = self._credentials.load()
        if str(expected.account_id or "").strip() != account_id:
            raise P115PlaybackStateError("播放源 holder 与当前主号身份不一致")
        resolved = self._direct_urls.resolve(
            str(source.get("pick_code") or ""),
            expected_account_id=account_id,
            user_agent=_DEFAULT_USER_AGENT,
        )
        if resolved is None:
            raise P115PlaybackStateError("无法为播放源解析主号 holder 直链")
        reader = self._reader_factory(
            resolved.url,
            headers={"User-Agent": resolved.user_agent or _DEFAULT_USER_AGENT},
            max_requests=4,
            max_bytes=length,
            deadline_seconds=45,
            request_timeout_seconds=30,
            cancel=lambda: self._primary_credentials_changed(expected, resolved.backend),
        )
        value = hashlib.sha1(reader.read(start, length)).hexdigest().upper()
        if self._primary_credentials_changed(expected, resolved.backend):
            raise P115PlaybackCredentialChanged("primary credential changed during rapid signing")
        return value

    def _primary_credentials_changed(self, expected: P115CredentialSnapshot, backend: str) -> bool:
        current = self._credentials.load()
        if str(current.account_id or "").strip() != str(expected.account_id or "").strip():
            return True
        if backend == "openapi":
            return current.access_token != expected.access_token or current.refresh_token != expected.refresh_token
        return current.cookie != expected.cookie or current.app_type != expected.app_type


class NativeP115PoolPlaybackClientFactory:
    def __init__(
        self,
        config: P115PlaybackConfigService,
        *,
        primary_credentials: P115CredentialStore | None = None,
        downloads: HttpxP115BoundedDownloadGateway | None = None,
        http_client_factory: Callable[..., httpx.Client] = httpx.Client,
    ) -> None:
        self._config = config
        self._primary_credentials = primary_credentials
        self._downloads = downloads or HttpxP115BoundedDownloadGateway()
        self._http_client_factory = http_client_factory

    def open(self, operation: PlaybackOperation) -> P115PoolPlaybackClient:
        credential_hash = str(operation.request.get("credential_hash") or "").strip()
        account_kind = str(operation.request.get("account_kind") or "").strip().casefold()
        if account_kind == "pool":
            account_revision = str(operation.request.get("account_revision") or "").strip()
            store: P115CredentialStore = P115PlayPoolCredentialStore(
                self._config,
                account_id=operation.account_id,
                backend=operation.provider_backend,
                expected_fingerprint=credential_hash,
                expected_account_revision=account_revision,
            )
        elif account_kind == "primary":
            if self._primary_credentials is None:
                raise P115PlaybackNotFoundError("主号客户端未初始化")
            store = _PinnedPrimaryCredentialStore(
                self._primary_credentials,
                account_id=operation.account_id,
                backend=operation.provider_backend,
                expected_fingerprint=credential_hash,
            )
        else:
            raise P115PlaybackStateError("播放 operation 缺少固定的账号类型")
        store.load()

        def priority() -> str:
            return operation.provider_backend

        directory = HttpxP115DirectoryTreeGateway(
            credentials=store,
            priority=priority,
            client_factory=self._http_client_factory,
        )
        operations = P115PrimaryOperationsGateway(
            credentials=store,
            priority=priority,
            directory=directory,
        ).open(
            expected_account_id=operation.account_id,
            expected_backend=operation.provider_backend,
        )
        if operations is None:
            raise P115PlaybackNotFoundError("小号客户端未初始化")
        target_cid = str(operation.request.get("target_cid") or operation.request.get("configured_cid") or "")
        rapid = NativeP115RapidTransferGateway(
            credentials=store,
            priority=priority,
            target_cid=lambda: target_cid,
            directory=directory,
            client_factory=lambda: self._http_client_factory(timeout=30, follow_redirects=True, trust_env=False),
        )
        direct_urls = P115SdkDirectUrlGateway(
            credentials=store,
            priority=priority,
        )
        return _NativeP115PoolPlaybackClient(
            account_id=operation.account_id,
            backend=operation.provider_backend,
            credential_hash=credential_hash,
            credentials=store,
            operations=operations,
            rapid=rapid,
            direct_urls=direct_urls,
            downloads=self._downloads,
            source_kind="speedtest" if operation.kind is PlaybackOperationKind.SPEEDTEST else "playback",
        )


class _NativeP115PoolPlaybackClient:
    def __init__(
        self,
        *,
        account_id: str,
        backend: str,
        credential_hash: str,
        credentials: P115CredentialStore,
        operations: P115PinnedOperationsClient,
        rapid: NativeP115RapidTransferGateway,
        direct_urls: P115SdkDirectUrlGateway,
        downloads: HttpxP115BoundedDownloadGateway,
        source_kind: str,
    ) -> None:
        self._account_id = account_id
        self._backend = backend
        self._credential_hash = credential_hash
        self._credentials = credentials
        self._operations = operations
        self._rapid = rapid
        self._direct_urls = direct_urls
        self._downloads = downloads
        self._source_kind = source_kind

    def find_directory(self, name: str) -> str:
        wanted = str(name or "").strip()
        matches = [
            item
            for item in self._operations.list_children("0")
            if str(item.get("name") or item.get("file_name") or item.get("n") or "").strip() == wanted
            and item_is_directory(item)
        ]
        ids = {_item_id(item) for item in matches if _item_id(item)}
        if len(ids) > 1:
            raise P115PlaybackStateError("小号临时目录回读存在歧义")
        return next(iter(ids), "")

    def create_directory(self, name: str) -> Mapping[str, Any]:
        return self._operations.create_directory("0", str(name))

    def readback_source(self, source: Mapping[str, Any], target_cid: str) -> Mapping[str, Any] | None:
        identity = self._identity(source, target_cid)
        result: RapidTransferReadback = self._rapid.readback(identity)
        if result.status == "ambiguous" or result.ambiguous_count:
            raise P115PlaybackStateError("测速临时文件权威回读存在歧义")
        if result.status != "confirmed" or len(result.receipts) != 1:
            return None
        receipt = dict(result.receipts[0])
        return {
            "target_cid": str(receipt.get("target_cid") or target_cid),
            "remote_fid": str(receipt.get("fid") or ""),
            "remote_pick_code": str(receipt.get("pick_code") or ""),
            "file_name": str(receipt.get("file_name") or source.get("file_name") or ""),
            "created_remote": False,
        }

    def rapid_upload(
        self,
        source: Mapping[str, Any],
        target_cid: str,
        *,
        sign_key: str = "",
        sign_val: str = "",
    ) -> RapidUploadOutcome:
        return self._rapid.upload_init(
            self._identity(source, target_cid),
            source,
            sign_key=sign_key,
            sign_val=sign_val,
        )

    def copy_source(self, source_fid: str, target_cid: str) -> Mapping[str, Any]:
        return self._operations.copy_files([str(source_fid)], str(target_cid))

    def resolve_download(self, pick_code: str, user_agent: str) -> str:
        resolved = self._direct_urls.resolve_backend(
            pick_code,
            expected_account_id=self._account_id,
            expected_backend=self._backend,
            user_agent=user_agent,
        )
        return resolved.url if resolved is not None else ""

    def measure(self, url: str, user_agent: str) -> Mapping[str, Any]:
        self._assert_credentials()
        result = self._downloads.measure(
            url,
            user_agent=user_agent,
            max_bytes=16 * 1024 * 1024,
            max_seconds=8,
            checkpoint=self._assert_credentials,
        )
        self._assert_credentials()
        size = max(0, int(result.get("downloaded_bytes") or 0))
        seconds = max(0.001, float(result.get("elapsed_seconds") or 0))
        bps = int(size / seconds)
        return {
            "bytes": size,
            "seconds": seconds,
            "bps": bps,
            "speed_text": str(result.get("speed_text") or f"{human_bytes(bps)}/s"),
        }

    def _assert_credentials(self) -> None:
        self._credentials.load()

    def remote_exists(self, remote_fid: str, target_cid: str) -> bool | None:
        wanted = str(remote_fid or "").strip()
        if not wanted or not str(target_cid or "").strip():
            return None
        try:
            response = self._operations.file_info(wanted)
        except Exception:
            return None
        if _file_info_missing(response):
            return False
        if provider_succeeded(response) and wanted in response_ids(response):
            return True
        return None

    def delete_remote(self, remote_fid: str, target_cid: str) -> bool | None:
        try:
            response = self._operations.delete_files([str(remote_fid)])
        except Exception:
            return None
        exists = self.remote_exists(remote_fid, target_cid)
        if exists is False:
            return True
        if exists is True and response.get("state") is False:
            return False
        return None

    def _identity(self, source: Mapping[str, Any], target_cid: str) -> dict[str, Any]:
        return {
            "account_id": self._account_id,
            "backend": self._backend,
            "credential_hash": self._credential_hash,
            "target_cid": str(target_cid or "").strip(),
            "files": [
                {
                    "sha1": normalize_sha1(source.get("sha1")),
                    "size": _positive_int(source.get("size")),
                    "file_name": str(source.get("file_name") or ""),
                    "preid": normalize_sha1(source.get("preid")),
                    "source_kind": self._source_kind,
                    "source_id": str(source.get("fid") or source.get("sha1") or ""),
                }
            ],
        }


class NativeP115PlaybackProvider(P115PlaybackProvider):
    """Processor-free playback provider with readback-only crash recovery."""

    def __init__(self, clients: P115PoolPlaybackClientFactory, signer: P115RapidSigner) -> None:
        self._clients = clients
        self._signer = signer

    def execute(
        self,
        operation: PlaybackOperation,
        *,
        checkpoint: Callable[[Mapping[str, Any]], None],
    ) -> PlaybackProviderOutcome:
        if operation.kind not in {
            PlaybackOperationKind.PREPARE,
            PlaybackOperationKind.ENSURE_TEMP_DIR,
            PlaybackOperationKind.SPEEDTEST,
            PlaybackOperationKind.RECLAIM,
        }:
            return _rejected("unsupported_native_playback_operation", operation.kind.value)
        if operation.kind is PlaybackOperationKind.RECLAIM and operation.request.get("created_remote") is not True:
            remote_fid = str(operation.request.get("remote_fid") or "").strip()
            if not remote_fid:
                return _rejected("reclaim_fid_missing", "播放临时文件回收缺少 remote_fid")
            return _confirmed(
                {"deleted": False, "released": True, "remote_fid": remote_fid},
                request_sent=False,
                readback={"owned_remote": False},
            )
        request_sent = operation.provider_request_started_at is not None
        latest_session = dict(operation.provider_session or {}) or None

        def durable_checkpoint(value: Mapping[str, Any]) -> None:
            nonlocal request_sent, latest_session
            checkpoint(value)
            request_sent = True
            latest_session = dict(value)

        try:
            client = self._clients.open(operation)
            if operation.kind is PlaybackOperationKind.PREPARE:
                return self._prepare(operation, client, durable_checkpoint)
            if operation.kind is PlaybackOperationKind.ENSURE_TEMP_DIR:
                return self._ensure_temp_dir(operation, client, durable_checkpoint)
            if operation.kind is PlaybackOperationKind.SPEEDTEST:
                return self._speedtest(operation, client, durable_checkpoint)
            if operation.kind is PlaybackOperationKind.RECLAIM:
                return self._reclaim(operation, client, durable_checkpoint)
            return _rejected("unsupported_native_playback_operation", operation.kind.value)
        except P115PlaybackCredentialChanged as exc:
            if request_sent:
                return _indeterminate("pool_credential_changed_after_request", str(exc), session=latest_session)
            return _rejected("pool_credential_changed", str(exc))
        except Exception as exc:
            del exc
            return PlaybackProviderOutcome(
                PlaybackProviderDisposition.INDETERMINATE,
                request_sent=request_sent,
                session=latest_session,
                error={"type": "native_playback_provider_exception", "message": "原生小号维护执行失败"},
            )

    def readback(self, operation: PlaybackOperation) -> PlaybackProviderOutcome:
        if operation.kind not in {
            PlaybackOperationKind.PREPARE,
            PlaybackOperationKind.ENSURE_TEMP_DIR,
            PlaybackOperationKind.SPEEDTEST,
            PlaybackOperationKind.RECLAIM,
        }:
            return _rejected("unsupported_native_playback_readback", operation.kind.value)
        session = dict(operation.provider_session or {})
        if operation.kind is PlaybackOperationKind.RECLAIM and operation.request.get("created_remote") is not True:
            remote_fid = str(operation.request.get("remote_fid") or "").strip()
            if not remote_fid:
                return _indeterminate("reclaim_readback_fid_missing", "删除回读缺少固定 remote_fid", session=session)
            return _confirmed(
                {"deleted": False, "released": True, "remote_fid": remote_fid},
                request_sent=False,
                session=session,
                readback={"owned_remote": False},
            )
        phase = str(session.get("phase") or "").strip()
        try:
            client = self._clients.open(operation)
            if operation.kind is PlaybackOperationKind.PREPARE:
                return self._prepare_readback(operation, client, session)
            if operation.kind is PlaybackOperationKind.ENSURE_TEMP_DIR:
                cid = client.find_directory(str(operation.request.get("temp_dir_name") or "ETK临时目录"))
                if cid:
                    return _confirmed({"cid": cid, "created_remote": False}, request_sent=True)
                return _indeterminate("mkdir_readback_missing", "小号临时目录回读未确认", session=session)
            if operation.kind is PlaybackOperationKind.RECLAIM:
                return self._reclaim_readback(operation, client, session)
            if operation.kind is not PlaybackOperationKind.SPEEDTEST:
                return _rejected("unsupported_native_playback_readback", operation.kind.value)
            if phase in {"rapid_upload", "rapid_sign_finalize"}:
                source = _operation_source(operation)
                source_preid = normalize_sha1(source.get("preid")) or normalize_sha1(session.get("source_preid"))
                if not source_preid:
                    return _indeterminate(
                        "rapid_readback_preid_missing",
                        "测速恢复缺少冻结的 PREID",
                        session=session,
                    )
                source["preid"] = source_preid
                found = client.readback_source(source, _target_cid(operation))
                if found:
                    return PlaybackProviderOutcome(
                        PlaybackProviderDisposition.RETRYABLE,
                        request_sent=False,
                        session=session,
                        error={
                            "type": "rapid_readback_confirmed_resume",
                            "message": "测速临时文件已确认，继续后续阶段",
                        },
                    )
                return _indeterminate("rapid_readback_missing", "测速临时文件尚未权威确认", session=session)
            if phase == "speedtest":
                exists = client.remote_exists(str(session.get("remote_fid") or ""), _target_cid(operation))
                if exists is True:
                    return PlaybackProviderOutcome(
                        PlaybackProviderDisposition.RETRYABLE,
                        request_sent=False,
                        session=session,
                        error={"type": "speedtest_unknown_cleanup_resume", "message": "测速结果未知，继续清理临时文件"},
                    )
                return _indeterminate("speedtest_result_unknown", "测速结果无法恢复", session=session)
            if phase == "delete":
                return self._delete_readback(operation, client, session)
            return _indeterminate("speedtest_readback_phase_missing", "测速恢复阶段缺失", session=session)
        except Exception as exc:
            del exc
            return _indeterminate("native_playback_readback_failed", "原生小号维护回读失败", session=session)

    def _prepare(
        self,
        operation: PlaybackOperation,
        client: P115PoolPlaybackClient,
        checkpoint: Callable[[Mapping[str, Any]], None],
    ) -> PlaybackProviderOutcome:
        session = dict(operation.provider_session or {})
        phase = str(session.get("phase") or "").strip()
        if phase in {"copy", "rapid_upload", "rapid_sign_finalize"}:
            return self._prepare_readback(operation, client, session)
        if operation.provider_request_started_at is not None:
            return _indeterminate(
                "prepare_readback_phase_missing",
                "播放临时文件恢复阶段缺失",
                session=session,
            )
        source = _operation_source(operation)
        durable_preid = ""
        if not normalize_sha1(source.get("preid")):
            durable_preid = normalize_sha1(self._signer.preid(source))
            if not durable_preid:
                return _rejected("prepare_preid_missing", "播放源缺少有效的 PREID")
            source["preid"] = durable_preid
        existing = client.readback_source(source, _target_cid(operation))
        if existing is not None:
            return _prepare_confirmed(operation, existing, created_remote=False, request_sent=False)
        if operation.mode is PlaybackMode.COPY:
            return self._copy_prepare(operation, client, checkpoint, source, durable_preid=durable_preid)
        if operation.mode in {PlaybackMode.POOL, PlaybackMode.VIRTUAL}:
            return self._rapid_prepare(operation, client, checkpoint, source, durable_preid=durable_preid)
        return _rejected("prepare_mode_invalid", "播放临时文件模式不受支持")

    def _rapid_prepare(
        self,
        operation: PlaybackOperation,
        client: P115PoolPlaybackClient,
        checkpoint: Callable[[Mapping[str, Any]], None],
        source: Mapping[str, Any],
        *,
        durable_preid: str,
    ) -> PlaybackProviderOutcome:
        target_cid = _target_cid(operation)
        rapid_session = {**_session(operation, phase="rapid_upload"), "preflight_absent": True}
        if durable_preid:
            rapid_session["source_preid"] = durable_preid
        checkpoint(rapid_session)
        outcome = client.rapid_upload(source, target_cid)
        if outcome.status == "sign_required":
            signed_session = {**rapid_session, "phase": "rapid_sign_finalize"}
            checkpoint(signed_session)
            sign_val = self._signer.sign(source, outcome.sign_check)
            outcome = client.rapid_upload(
                source,
                target_cid,
                sign_key=outcome.sign_key,
                sign_val=sign_val,
            )
            rapid_session = signed_session
        if outcome.status == "rejected":
            return PlaybackProviderOutcome(
                PlaybackProviderDisposition.REJECTED,
                request_sent=True,
                session=rapid_session,
                error={"type": "rapid_prepare_rejected", "message": "115 明确拒绝创建播放临时文件"},
            )
        if outcome.status != "confirmed":
            return _indeterminate("rapid_prepare_unknown", "115 播放秒传结果未知", session=rapid_session)
        found = client.readback_source(source, target_cid)
        if found is None:
            return _indeterminate(
                "rapid_prepare_readback_missing",
                "播放秒传后权威回读未找到临时文件",
                session=rapid_session,
            )
        return _prepare_confirmed(operation, found, created_remote=True, request_sent=True, session=rapid_session)

    @staticmethod
    def _copy_prepare(
        operation: PlaybackOperation,
        client: P115PoolPlaybackClient,
        checkpoint: Callable[[Mapping[str, Any]], None],
        source: Mapping[str, Any],
        *,
        durable_preid: str,
    ) -> PlaybackProviderOutcome:
        source_fid = str(source.get("fid") or "").strip()
        target_cid = _target_cid(operation)
        if not source_fid:
            return _rejected("copy_source_incomplete", "复制播放缺少源 FID")
        copy_session = {**_session(operation, phase="copy"), "preflight_absent": True}
        if durable_preid:
            copy_session["source_preid"] = durable_preid
        checkpoint(copy_session)
        response = client.copy_source(source_fid, target_cid)
        if _provider_rejected(response):
            return PlaybackProviderOutcome(
                PlaybackProviderDisposition.REJECTED,
                request_sent=True,
                session=copy_session,
                error={"type": "copy_prepare_rejected", "message": "115 明确拒绝复制播放临时文件"},
            )
        found = client.readback_source(source, target_cid)
        if found is not None:
            return _prepare_confirmed(operation, found, created_remote=True, request_sent=True, session=copy_session)
        return _indeterminate(
            "copy_prepare_readback_missing",
            "复制后权威回读未找到播放临时文件",
            session=copy_session,
        )

    @staticmethod
    def _prepare_readback(
        operation: PlaybackOperation,
        client: P115PoolPlaybackClient,
        session: Mapping[str, Any],
    ) -> PlaybackProviderOutcome:
        source = _operation_source(operation)
        source_preid = normalize_sha1(source.get("preid")) or normalize_sha1(session.get("source_preid"))
        if not source_preid:
            return _indeterminate(
                "prepare_readback_preid_missing",
                "播放临时文件恢复缺少冻结的 PREID",
                session=session,
            )
        source["preid"] = source_preid
        found = client.readback_source(source, _target_cid(operation))
        if found is None:
            return _indeterminate(
                "prepare_readback_missing",
                "播放临时文件尚未权威确认",
                session=session,
            )
        return _prepare_confirmed(
            operation,
            found,
            created_remote=False,
            request_sent=True,
            session=session,
        )

    def _reclaim(
        self,
        operation: PlaybackOperation,
        client: P115PoolPlaybackClient,
        checkpoint: Callable[[Mapping[str, Any]], None],
    ) -> PlaybackProviderOutcome:
        session = dict(operation.provider_session or {})
        phase = str(session.get("phase") or "").strip()
        if phase == "delete":
            return self._reclaim_readback(operation, client, session)
        if operation.provider_request_started_at is not None:
            return _indeterminate(
                "reclaim_readback_phase_missing",
                "播放临时文件删除恢复阶段缺失",
                session=session,
            )
        remote_fid = str(operation.request.get("remote_fid") or "").strip()
        if not remote_fid:
            return _rejected("reclaim_fid_missing", "播放临时文件回收缺少 remote_fid")
        if operation.request.get("created_remote") is not True:
            return _confirmed(
                {"deleted": False, "released": True, "remote_fid": remote_fid},
                request_sent=False,
                readback={"owned_remote": False},
            )
        exists = client.remote_exists(remote_fid, _target_cid(operation))
        if exists is False:
            return _confirmed(
                {"deleted": True, "remote_fid": remote_fid},
                request_sent=False,
                readback={"deleted": True},
            )
        if exists is None:
            return PlaybackProviderOutcome(
                PlaybackProviderDisposition.RETRYABLE,
                request_sent=False,
                error={"type": "reclaim_preflight_unknown", "message": "无法确认播放临时文件当前状态"},
            )
        delete_session = {**_session(operation, phase="delete"), "remote_fid": remote_fid}
        checkpoint(delete_session)
        deleted = client.delete_remote(remote_fid, _target_cid(operation))
        if deleted is True:
            return _confirmed(
                {"deleted": True, "remote_fid": remote_fid},
                request_sent=True,
                session=delete_session,
                readback={"deleted": True},
            )
        if deleted is False:
            return PlaybackProviderOutcome(
                PlaybackProviderDisposition.REJECTED,
                request_sent=True,
                session=delete_session,
                error={"type": "reclaim_rejected", "message": "115 明确拒绝删除播放临时文件"},
            )
        return _indeterminate(
            "reclaim_delete_unknown",
            "播放临时文件删除结果未知",
            session=delete_session,
        )

    @staticmethod
    def _reclaim_readback(
        operation: PlaybackOperation,
        client: P115PoolPlaybackClient,
        session: Mapping[str, Any],
    ) -> PlaybackProviderOutcome:
        remote_fid = str(session.get("remote_fid") or operation.request.get("remote_fid") or "").strip()
        if not remote_fid:
            return _indeterminate("reclaim_readback_fid_missing", "删除回读缺少固定 remote_fid", session=session)
        if operation.request.get("created_remote") is not True:
            return _confirmed(
                {"deleted": False, "released": True, "remote_fid": remote_fid},
                request_sent=False,
                session=session,
                readback={"owned_remote": False},
            )
        exists = client.remote_exists(remote_fid, _target_cid(operation))
        if exists is False:
            return _confirmed(
                {"deleted": True, "remote_fid": remote_fid},
                request_sent=True,
                session=session,
                readback={"deleted": True},
            )
        if exists is True:
            return _retryable(
                "reclaim_delete_not_applied",
                "权威回读确认播放临时文件仍存在",
                session=session,
            )
        return _indeterminate(
            "reclaim_delete_readback_unknown",
            "无法确认播放临时文件是否已删除",
            session=session,
        )

    def _ensure_temp_dir(
        self,
        operation: PlaybackOperation,
        client: P115PoolPlaybackClient,
        checkpoint: Callable[[Mapping[str, Any]], None],
    ) -> PlaybackProviderOutcome:
        name = str(operation.request.get("temp_dir_name") or "ETK临时目录")
        phase = str((operation.provider_session or {}).get("phase") or "").strip()
        if operation.provider_request_started_at is not None and phase != "mkdir":
            return _indeterminate(
                "mkdir_readback_phase_missing",
                "小号临时目录恢复阶段缺失",
                session=operation.provider_session,
            )
        existing = client.find_directory(name)
        if existing:
            return _confirmed({"cid": existing, "created_remote": False}, request_sent=False)
        if phase == "mkdir":
            return _indeterminate(
                "mkdir_readback_missing",
                "小号临时目录创建结果无法确认",
                session=operation.provider_session,
            )
        session = _session(operation, phase="mkdir")
        checkpoint(session)
        response = client.create_directory(name)
        confirmed = client.find_directory(name)
        if confirmed:
            return _confirmed(
                {"cid": confirmed, "created_remote": True},
                request_sent=True,
                session=session,
            )
        if response.get("state") is False:
            return PlaybackProviderOutcome(
                PlaybackProviderDisposition.REJECTED,
                request_sent=True,
                session=session,
                error={"type": "mkdir_rejected", "message": "115 明确拒绝创建小号临时目录"},
            )
        return _indeterminate("mkdir_readback_missing", "小号临时目录创建结果无法确认", session=session)

    def _speedtest(
        self,
        operation: PlaybackOperation,
        client: P115PoolPlaybackClient,
        checkpoint: Callable[[Mapping[str, Any]], None],
    ) -> PlaybackProviderOutcome:
        source = _operation_source(operation)
        request_preid = normalize_sha1(source.get("preid"))
        target_cid = _target_cid(operation)
        session = dict(operation.provider_session or {})
        phase = str(session.get("phase") or "")
        if operation.provider_request_started_at is not None and phase not in {
            "rapid_upload",
            "rapid_sign_finalize",
            "speedtest",
            "delete",
        }:
            return _indeterminate(
                "speedtest_readback_phase_missing",
                "测速恢复阶段缺失",
                session=session,
            )
        if phase == "delete":
            return self._delete_readback(operation, client, session)
        if phase == "speedtest":
            remote_fid = str(session.get("remote_fid") or "")
            exists = client.remote_exists(remote_fid, target_cid)
            if exists is not True:
                return _indeterminate("speedtest_result_unknown", "测速结果无法恢复", session=session)
            cleanup_session = {**_session(operation, phase="delete"), "remote_fid": remote_fid, "speed_unknown": True}
            checkpoint(cleanup_session)
            client.delete_remote(remote_fid, target_cid)
            return _indeterminate(
                "speedtest_result_unknown",
                "测速结果未知，临时文件已进入只读清理核对",
                session=cleanup_session,
            )

        source_preid = request_preid or normalize_sha1(session.get("source_preid"))
        if not source_preid:
            if phase in {"rapid_upload", "rapid_sign_finalize"}:
                return _indeterminate(
                    "rapid_readback_preid_missing",
                    "测速恢复缺少冻结的 PREID",
                    session=session,
                )
            source_preid = normalize_sha1(self._signer.preid(source))
            if not source_preid:
                return _rejected("speedtest_preid_missing", "测速源缺少有效的 PREID")
        source["preid"] = source_preid

        found = client.readback_source(source, target_cid)
        if found is None:
            if phase in {"rapid_upload", "rapid_sign_finalize"}:
                return _indeterminate("rapid_readback_missing", "测速临时文件尚未权威确认", session=session)
            rapid_session = _session(operation, phase="rapid_upload")
            if not request_preid:
                rapid_session["source_preid"] = source_preid
            checkpoint(rapid_session)
            initial = client.rapid_upload(source, target_cid)
            if initial.status == "sign_required":
                signed_session = {**rapid_session, "phase": "rapid_sign_finalize"}
                checkpoint(signed_session)
                sign_val = self._signer.sign(source, initial.sign_check)
                initial = client.rapid_upload(
                    source,
                    target_cid,
                    sign_key=initial.sign_key,
                    sign_val=sign_val,
                )
                rapid_session = signed_session
            if initial.status == "rejected":
                return PlaybackProviderOutcome(
                    PlaybackProviderDisposition.REJECTED,
                    request_sent=True,
                    session=rapid_session,
                    error={"type": "rapid_upload_rejected", "message": "115 明确拒绝创建测速临时文件"},
                )
            if initial.status != "confirmed":
                return _indeterminate(
                    "rapid_upload_unknown",
                    "115 秒传结果未知",
                    session=rapid_session,
                )
            found = client.readback_source(source, target_cid)
            if found is None:
                return _indeterminate(
                    "rapid_upload_readback_missing",
                    "秒传后权威回读未找到测速文件",
                    session=rapid_session,
                )

        remote_fid = str(found.get("remote_fid") or "").strip()
        remote_pick_code = str(found.get("remote_pick_code") or "").strip()
        if not remote_fid or not remote_pick_code:
            return _indeterminate("speedtest_remote_identity_missing", "测速临时文件身份不完整")
        speed_session = {
            **_session(operation, phase="speedtest"),
            "remote_fid": remote_fid,
            "remote_pick_code": remote_pick_code,
        }
        checkpoint(speed_session)
        url = client.resolve_download(remote_pick_code, _DEFAULT_USER_AGENT)
        if not url:
            return _indeterminate("speedtest_direct_url_missing", "小号测速直链解析失败", session=speed_session)
        speed = _speed_receipt(client.measure(url, _DEFAULT_USER_AGENT))
        delete_session = {
            **_session(operation, phase="delete"),
            "remote_fid": remote_fid,
            "speed": speed,
        }
        checkpoint(delete_session)
        deleted = client.delete_remote(remote_fid, target_cid)
        if deleted is True:
            return _confirmed(speed, request_sent=True, session=delete_session, readback={"deleted": True})
        if deleted is False:
            return _retryable(
                "speedtest_delete_not_applied",
                "权威回读确认测速临时文件仍存在",
                session=delete_session,
            )
        return _indeterminate("speedtest_delete_unknown", "测速临时文件删除结果未知", session=delete_session)

    @staticmethod
    def _delete_readback(
        operation: PlaybackOperation,
        client: P115PoolPlaybackClient,
        session: Mapping[str, Any],
    ) -> PlaybackProviderOutcome:
        remote_fid = str(session.get("remote_fid") or "").strip()
        exists = client.remote_exists(remote_fid, _target_cid(operation))
        if exists is True:
            return _retryable(
                "speedtest_delete_not_applied",
                "权威回读确认测速临时文件仍存在",
                session=session,
            )
        if exists is None:
            return _indeterminate(
                "speedtest_delete_readback_unknown",
                "无法确认测速临时文件是否已删除",
                session=session,
            )
        speed = session.get("speed")
        if not isinstance(speed, Mapping):
            return _indeterminate("speedtest_result_missing", "测速文件已删除但测速结果未持久化", session=session)
        return _confirmed(dict(speed), request_sent=True, session=session, readback={"deleted": True})


class NativeP115DirectLinkGateway(P115DirectLinkGateway):
    """Credential-fenced direct URLs plus streaming without legacy client imports."""

    def __init__(
        self,
        config: P115PlaybackConfigService,
        credentials: P115CredentialStore,
        *,
        priority: Callable[[], str],
        client_factory: Callable[..., httpx.Client] = httpx.Client,
        timeout_seconds: float = 15.0,
    ) -> None:
        self._config = config
        self._credentials = credentials
        self._priority = priority
        self._client_factory = client_factory
        self._timeout_seconds = max(1.0, float(timeout_seconds))

    def resolve_primary(self, pick_code: str, user_agent: str) -> str:
        snapshot = self._credentials.load()
        account_id = str(snapshot.account_id or "").strip()
        if not account_id.isdigit():
            raise P115PlaybackNotFoundError("115 Client not initialized")
        resolved = P115SdkDirectUrlGateway(
            credentials=self._credentials,
            priority=self._priority,
        ).resolve(
            pick_code,
            expected_account_id=account_id,
            user_agent=user_agent,
        )
        return resolved.url if resolved is not None else ""

    def resolve_artifact(self, artifact: PlaybackArtifact, user_agent: str) -> str:
        receipt = artifact.remote_receipt if isinstance(artifact.remote_receipt, Mapping) else {}
        if artifact.mode not in {PlaybackMode.POOL, PlaybackMode.COPY, PlaybackMode.VIRTUAL}:
            return ""
        expected_kind = "pool" if artifact.mode is PlaybackMode.POOL else "primary"
        account_kind = str(receipt.get("account_kind") or "").strip().casefold()
        credential_hash = str(receipt.get("credential_hash") or "").strip().casefold()
        if account_kind != expected_kind or re.fullmatch(r"[0-9a-f]{64}", credential_hash) is None:
            return ""
        try:
            if account_kind == "pool":
                account_revision = str(receipt.get("account_revision") or "").strip().casefold()
                if re.fullmatch(r"[0-9a-f]{64}", account_revision) is None:
                    return ""
                store: P115CredentialStore = P115PlayPoolCredentialStore(
                    self._config,
                    account_id=artifact.account_id,
                    backend=str(artifact.provider_backend or "").strip().casefold(),
                    expected_fingerprint=credential_hash,
                    expected_account_revision=account_revision,
                )
            else:
                store = _PinnedPrimaryCredentialStore(
                    self._credentials,
                    account_id=artifact.account_id,
                    backend=str(artifact.provider_backend or "").strip().casefold(),
                    expected_fingerprint=credential_hash,
                )
            store.load()
        except (P115PlaybackCredentialChanged, P115PlaybackNotFoundError, P115PlaybackStateError, ValueError):
            return ""
        backend = str(artifact.provider_backend or "").strip().casefold()
        resolved = P115SdkDirectUrlGateway(
            credentials=store,
            priority=lambda: backend,
        ).resolve_backend(
            artifact.remote_pick_code,
            expected_account_id=artifact.account_id,
            expected_backend=backend,
            user_agent=user_agent,
        )
        return resolved.url if resolved is not None else ""

    def stream(self, url: str, *, user_agent: str, range_header: str = "") -> PlaybackUpstreamStream:
        normalized_url = _validate_playback_upstream_url(url)
        headers = {
            "User-Agent": str(user_agent or _DEFAULT_USER_AGENT).strip() or _DEFAULT_USER_AGENT,
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        }
        normalized_range = str(range_header or "")
        if normalized_range and len(normalized_range) <= 256 and _RANGE_HEADER.fullmatch(normalized_range):
            headers["Range"] = normalized_range
        client = self._client_factory(timeout=self._timeout_seconds, follow_redirects=False, trust_env=False)
        response: httpx.Response | None = None
        try:
            current_url = normalized_url
            for redirect_count in range(_MAX_PLAYBACK_REDIRECTS + 1):
                request = client.build_request("GET", current_url, headers=headers)
                current_response = client.send(request, stream=True)
                response = current_response
                if current_response.status_code not in _PLAYBACK_REDIRECT_STATUSES:
                    break
                location = _header_value(current_response.headers, "location")
                if not location:
                    break
                if redirect_count >= _MAX_PLAYBACK_REDIRECTS:
                    raise ValueError("115 playback redirect limit exceeded")
                next_url = _validate_playback_upstream_url(urljoin(current_url, location))
                with suppress(Exception):
                    current_response.close()
                response = None
                current_url = next_url
            if response is None:
                raise ValueError("115 playback upstream returned no response")
            active_response = response
            _validate_playback_upstream_url(str(getattr(active_response, "url", "") or current_url))
            response_headers = {
                name: value
                for name, value in active_response.headers.items()
                if name.casefold() in _STREAM_RESPONSE_HEADERS
                and not (
                    name.casefold() == "content-length"
                    and _header_value(active_response.headers, "content-encoding").casefold() not in {"", "identity"}
                )
            }
        except BaseException:
            if response is not None:
                with suppress(Exception):
                    response.close()
            with suppress(Exception):
                client.close()
            raise P115PlaybackStateError("115 playback upstream request failed") from None

        def close() -> None:
            with suppress(Exception):
                active_response.close()
            with suppress(Exception):
                client.close()

        def chunks():
            try:
                yield from active_response.iter_bytes(chunk_size=64 * 1024)
            except Exception:
                raise P115PlaybackStateError("115 playback upstream stream failed") from None
            finally:
                close()

        return PlaybackUpstreamStream(
            status_code=active_response.status_code,
            headers=response_headers,
            chunks=chunks(),
            close=close,
        )


def _validate_playback_upstream_url(value: str) -> str:
    normalized = str(value or "").strip()
    try:
        parsed = urlsplit(normalized)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("115 playback upstream URL is invalid") from exc
    del port
    if parsed.scheme.casefold() not in {"http", "https"} or not parsed.hostname:
        raise ValueError("115 playback upstream URL must use HTTP or HTTPS")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("115 playback upstream URL must not contain user information")
    host = parsed.hostname.casefold().rstrip(".")
    if host in {"localhost", "localhost.localdomain", "0.0.0.0", "::"} or host.endswith(
        (".localhost", ".local", ".internal")
    ):
        raise ValueError("115 playback upstream URL must not target a local host")
    try:
        address = ip_address(host)
    except ValueError:
        address = None
    if address is not None and (
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
    ):
        raise ValueError("115 playback upstream URL must not target a private host")
    return normalized


def _header_value(headers: Mapping[str, Any], wanted: str) -> str:
    target = str(wanted or "").casefold()
    for name, value in headers.items():
        if str(name).casefold() == target:
            return str(value or "").strip()
    return ""


class NativeOnlyP115DirectLinkGateway(P115DirectLinkGateway):
    """Daily speedtest wiring sentinel; interactive playback is owned by the API runtime."""

    @staticmethod
    def resolve_primary(pick_code: str, user_agent: str) -> str:
        del pick_code, user_agent
        raise P115PlaybackStateError("native-only Worker does not serve interactive playback")

    @staticmethod
    def resolve_artifact(artifact: PlaybackArtifact, user_agent: str) -> str:
        del artifact, user_agent
        raise P115PlaybackStateError("native-only Worker does not serve interactive playback")

    @staticmethod
    def stream(url: str, *, user_agent: str, range_header: str = "") -> PlaybackUpstreamStream:
        del url, user_agent, range_header
        raise P115PlaybackStateError("native-only Worker does not proxy playback streams")


def _operation_source(operation: PlaybackOperation) -> dict[str, Any]:
    source = operation.request.get("source")
    if not isinstance(source, Mapping):
        raise P115PlaybackStateError("playback operation source snapshot is missing")
    return deepcopy(dict(source))


def _target_cid(operation: PlaybackOperation) -> str:
    value = str(operation.request.get("target_cid") or "").strip()
    if not value:
        raise P115PlaybackStateError("playback operation target cid is missing")
    return value


def _session(operation: PlaybackOperation, *, phase: str) -> dict[str, Any]:
    return {
        "phase": phase,
        "account_id": operation.account_id,
        "backend": operation.provider_backend,
        "kind": operation.kind.value,
    }


def _prepare_confirmed(
    operation: PlaybackOperation,
    found: Mapping[str, Any],
    *,
    created_remote: bool,
    request_sent: bool,
    session: Mapping[str, Any] | None = None,
) -> PlaybackProviderOutcome:
    target_cid = str(found.get("target_cid") or operation.request.get("target_cid") or "").strip()
    remote_fid = str(found.get("remote_fid") or "").strip()
    remote_pick_code = str(found.get("remote_pick_code") or "").strip()
    file_name = str(found.get("file_name") or operation.request.get("file_name") or "").strip()
    if not target_cid or not remote_fid or not remote_pick_code or not file_name:
        return _indeterminate(
            "prepare_remote_identity_missing",
            "播放临时文件权威身份不完整",
            session=session,
            request_sent=request_sent,
        )
    return _confirmed(
        {
            "target_cid": target_cid,
            "remote_fid": remote_fid,
            "remote_pick_code": remote_pick_code,
            "file_name": file_name,
            "created_remote": created_remote,
        },
        request_sent=request_sent,
        session=session,
        readback={"found": True},
    )


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


def _indeterminate(
    error_type: str,
    message: str,
    *,
    session: Mapping[str, Any] | None = None,
    request_sent: bool = True,
) -> PlaybackProviderOutcome:
    return PlaybackProviderOutcome(
        PlaybackProviderDisposition.INDETERMINATE,
        request_sent=request_sent,
        session=dict(session) if session is not None else None,
        error={"type": error_type, "message": str(message)[:500]},
    )


def _retryable(
    error_type: str,
    message: str,
    *,
    session: Mapping[str, Any] | None = None,
) -> PlaybackProviderOutcome:
    return PlaybackProviderOutcome(
        PlaybackProviderDisposition.RETRYABLE,
        request_sent=True,
        session=dict(session) if session is not None else None,
        error={"type": error_type, "message": str(message)[:500]},
    )


def _item_id(item: Mapping[str, Any]) -> str:
    return str(item.get("fid") or item.get("file_id") or item.get("cid") or item.get("id") or "").strip()


def _file_info_missing(value: Mapping[str, Any]) -> bool:
    code = str(value.get("code") or value.get("errno") or value.get("errNo") or "").strip().casefold()
    message = str(value.get("message") or value.get("error_msg") or value.get("error") or "").casefold()
    return code in {"404", "20018", "not_found", "file_not_found"} or any(
        marker in message for marker in ("not found", "不存在", "已删除")
    )


def _provider_rejected(value: Mapping[str, Any]) -> bool:
    if value.get("state") is False or value.get("success") is False:
        return True
    code = str(value.get("code") or "").strip()
    return bool(code and code not in {"0", "200"})


def _positive_int(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _speed_receipt(value: Mapping[str, Any]) -> dict[str, Any]:
    size = _positive_int(value.get("bytes"))
    bps = _positive_int(value.get("bps"))
    try:
        seconds = max(0.001, float(value.get("seconds") or 0))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("speedtest duration is invalid") from exc
    speed_text = str(value.get("speed_text") or f"{human_bytes(bps)}/s").strip()
    if not speed_text or len(speed_text) > 128:
        raise ValueError("speedtest display value is invalid")
    return {
        "bytes": size,
        "seconds": seconds,
        "bps": bps,
        "speed_text": speed_text,
    }


def _playback_state(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, Mapping):
        raise TypeError("playback state must be a JSON object")
    return normalize_json_object(dict(value), field="playback state")


def _json_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _asset_name(value: Any, index: int) -> str:
    assets = _json_list(value)
    candidates: Sequence[Any] = ([assets[index]] if 0 <= index < len(assets) else []) + assets
    for raw in candidates:
        if not isinstance(raw, Mapping):
            continue
        name = raw.get("file_name") or raw.get("FileName") or raw.get("name") or raw.get("Name")
        if name:
            return str(name)
        path = raw.get("path") or raw.get("Path") or raw.get("file_path") or raw.get("FilePath")
        if path:
            return str(path).replace("\\", "/").rsplit("/", 1)[-1]
    return ""


__all__ = [
    "NativeP115DirectLinkGateway",
    "NativeOnlyP115DirectLinkGateway",
    "NativeP115PlaybackProvider",
    "NativeP115PlaybackRapidSigner",
    "NativeP115PoolPlaybackClientFactory",
    "P115PlayPoolCredentialStore",
    "P115PlaybackCredentialChanged",
    "P115PoolPlaybackClient",
    "P115PoolPlaybackClientFactory",
    "P115RapidSigner",
    "SqlAlchemyP115PlaybackConfigBackend",
    "SqlAlchemyP115PlaybackSourceGateway",
]
