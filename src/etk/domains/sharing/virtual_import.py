from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Protocol
from urllib.parse import quote

import httpx
from sqlalchemy import text

from etk.domains.p115.strm_sync import P115LocalDeleteAction, P115LocalStrmLibraryGateway
from etk.integrations import ProviderDisposition, ProviderResponse, SharedCenterPort
from etk.platform import Settings
from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs.json_payload import canonical_json, normalize_durable_json_object

from .execution import SharingDeliveryOutcome
from .operations import SharingOperation

_SHA1 = re.compile(r"[A-F0-9]{40}")
_SOURCE_KINDS = frozenset({"movie", "episode", "logical_episode", "logical_season", "season_hub"})
_SENSITIVE_KEYS = frozenset(
    {
        "access_token",
        "api_key",
        "authorization",
        "cookie",
        "ed2k",
        "full_url",
        "magnet",
        "magnet_url",
        "password",
        "receive_code",
        "refresh_token",
        "share_code",
        "secret",
        "target_link",
        "token",
        "url",
        "urls",
    }
)
_SENSITIVE_COMPACT_KEYS = frozenset(
    {
        "code",
        "extractcode",
        "p115sharecode",
        "passcode",
        "receivecode",
        "sharecode",
        "sharelink",
        "shareurl",
    }
)
_MAX_FILES = 2_000
_MAX_SOURCE_BYTES = 2 * 1024 * 1024
_MAX_SOURCE_STRING_BYTES = 4 * 1024
_INTENT_HASH_KEY = "_etk_virtual_intent_hash"
_BUNDLE_HASH_KEY = "_etk_virtual_bundle_hash"
_SENSITIVE_VALUE = re.compile(
    r"(?:https?|ftp|file)://|magnet:\?|ed2k://|115(?:cdn)?\.com/s/"
    r"|(?:^|[?&])(?:access_?token|api_?key|password|secret|token)="
    r"|(?:^|[\\/])(?:Users|home|tmp|var/folders|Volumes)[\\/]"
    r"|(?:^|\s)Bearer\s+",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class VirtualImportBundle:
    source_kind: str
    source_id: str
    source: dict[str, Any]
    files: tuple[dict[str, Any], ...]

    @property
    def state_hash(self) -> str:
        payload = {"source_kind": self.source_kind, "source_id": self.source_id, "files": list(self.files)}
        return hashlib.sha256(canonical_json(payload, field="virtual import state").encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class VirtualImportCreation:
    item: dict[str, Any]
    created: bool


@dataclass(frozen=True, slots=True)
class VirtualImportProjection:
    paths: tuple[str, ...]
    files: tuple[dict[str, Any], ...]


class VirtualImportProjectionReconcileRequired(RuntimeError):
    def __init__(self, message: str, projection: VirtualImportProjection) -> None:
        super().__init__(message)
        self.projection = projection


@dataclass(frozen=True, slots=True)
class VirtualImportSourceIntent:
    source_kind: str
    source_id: str
    source: dict[str, Any]

    @property
    def state_hash(self) -> str:
        return hashlib.sha256(canonical_json(self.source, field="virtual import source intent").encode()).hexdigest()


class VirtualImportProjector(Protocol):
    def project(self, virtual_id: int, bundle: VirtualImportBundle) -> VirtualImportProjection: ...

    def cleanup(self, item: Mapping[str, Any]) -> Mapping[str, Any]: ...


class VirtualImportRepository(Protocol):
    def create(
        self,
        bundle: VirtualImportBundle,
        projector: VirtualImportProjector,
    ) -> VirtualImportCreation: ...

    def get(self, virtual_id: int) -> dict[str, Any] | None: ...

    def active_by_source(self, source_kind: str, source_id: str) -> tuple[dict[str, Any], ...]: ...

    def remove(
        self,
        virtual_id: int,
        *,
        expected_state_hash: str,
        projector: VirtualImportProjector,
    ) -> Mapping[str, Any]: ...


class VirtualImportEmbyGateway(Protocol):
    def exact_item_ids(self, paths: Sequence[str]) -> tuple[str, ...]: ...

    def delete_items(self, item_ids: Sequence[str]) -> None: ...


class VirtualImportSourceResolver:
    def __init__(self, center: SharedCenterPort) -> None:
        self._center = center

    def resolve(self, raw_source: Mapping[str, Any]) -> VirtualImportBundle:
        source = _bounded_object(raw_source, field="center source")
        source_kind = _source_kind(source)
        if source_kind in {"completed_season", "deprecated_completed_season"}:
            raise ValueError("旧 completed_season 已停用，请刷新中心资源库后使用 logical_season。")
        if source_kind not in _SOURCE_KINDS:
            raise ValueError("中心源缺少受支持的 source_kind")
        source_id = _source_id(source, source_kind)
        if not source_id:
            raise ValueError("中心源缺少 source_id")

        raw_files = _embedded_files(source, source_kind)
        if not raw_files and source_kind in {"logical_season", "season_hub"}:
            response = self._center.list_display_children(
                source_kind=source_kind,
                source_id=source_id,
                source_ids=[source_id],
                hub_id=str(source.get("hub_id") or source_id),
                limit=_MAX_FILES,
                offset=0,
            )
            raw_files = _provider_files(response)
        if not raw_files and source_kind in {"movie", "episode", "logical_episode"}:
            raw_files = [source]
        if not raw_files:
            raise ValueError("中心返回的文件清单为空")
        if len(raw_files) > _MAX_FILES:
            raise ValueError(f"中心文件清单超过 {_MAX_FILES} 条")

        files: list[dict[str, Any]] = []
        seen: set[tuple[str, int]] = set()
        for index, raw_file in enumerate(raw_files):
            if not isinstance(raw_file, Mapping):
                raise ValueError(f"中心文件清单第 {index + 1} 项不是对象")
            item = _normalize_file(source, source_kind, source_id, raw_file)
            identity = (str(item["sha1"]), int(item["size"]))
            if identity in seen:
                continue
            seen.add(identity)
            files.append(item)
        if not files:
            raise ValueError("中心文件清单没有可用视频")
        normalized_source = dict(source)
        normalized_source["source_kind"] = source_kind
        normalized_source["source_id"] = source_id
        normalized_source["files"] = files
        normalized_source.pop("children", None)
        normalized_source.pop("pack_items", None)
        normalized_source.pop("best_asset_map", None)
        normalized_source = _bounded_object(normalized_source, field="normalized center source")
        return VirtualImportBundle(source_kind, source_id, normalized_source, tuple(files))


def freeze_virtual_import_source(raw_source: Mapping[str, Any]) -> VirtualImportSourceIntent:
    source = _bounded_object(raw_source, field="center source intent")
    source_kind = _source_kind(source)
    if source_kind in {"completed_season", "deprecated_completed_season"}:
        raise ValueError("旧 completed_season 已停用，请刷新中心资源库后使用 logical_season。")
    if source_kind not in _SOURCE_KINDS:
        raise ValueError("中心源缺少受支持的 source_kind")
    source_id = _source_id(source, source_kind)
    if not source_id:
        raise ValueError("中心源缺少 source_id")
    source["source_kind"] = source_kind
    source["source_id"] = source_id
    return VirtualImportSourceIntent(source_kind, source_id, source)


class SqlAlchemyVirtualImportRepository:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def create(
        self,
        bundle: VirtualImportBundle,
        projector: VirtualImportProjector,
    ) -> VirtualImportCreation:
        metadata = _metadata(bundle)
        source_payload = {**bundle.source, _BUNDLE_HASH_KEY: bundle.state_hash}
        scope = f"shared-virtual-import:{bundle.source_kind}:{bundle.source_id}"
        with self._unit_of_work() as uow:
            uow.session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"), {"scope": scope})
            existing = (
                uow.session.execute(
                    text(
                        """
                        SELECT * FROM shared_virtual_imports
                        WHERE source_kind = :source_kind AND source_id = :source_id
                          AND status IN ('creating', 'virtual', 'promoting', 'reconcile_required')
                        ORDER BY id DESC LIMIT 1 FOR UPDATE
                        """
                    ),
                    {"source_kind": bundle.source_kind, "source_id": bundle.source_id},
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None and str(existing.get("status") or "") != "creating":
                item = _row(existing)
                if _bundle_state_hash_from_row(item) != bundle.state_hash:
                    raise RuntimeError("同一中心源已有不同文件清单的虚拟入库记录")
                if str(item.get("status") or "") == "reconcile_required":
                    raise RuntimeError("该虚拟入库存在未完成的文件补偿，需先人工核对")
                return VirtualImportCreation(item, False)
            if existing is not None:
                raise RuntimeError("该虚拟入库正在创建中，请等待已有操作完成")
            inserted = (
                uow.session.execute(
                    text(
                        """
                        INSERT INTO shared_virtual_imports(
                            source_kind, source_id, tmdb_id, item_type, parent_series_tmdb_id,
                            season_number, episode_number, title, release_year, file_count,
                            total_size, status, strm_paths_json, source_payload_json, files_json,
                            updated_at
                        ) VALUES (
                            :source_kind, :source_id, :tmdb_id, :item_type, :parent_series_tmdb_id,
                            :season_number, :episode_number, :title, :release_year, :file_count,
                            :total_size, 'creating', '[]'::jsonb, CAST(:source_payload AS jsonb),
                            CAST(:files AS jsonb), NOW()
                        ) RETURNING *
                        """
                    ),
                    {
                        **metadata,
                        "source_kind": bundle.source_kind,
                        "source_id": bundle.source_id,
                        "source_payload": canonical_json(source_payload, field="virtual import source"),
                        "files": json.dumps(
                            list(bundle.files), ensure_ascii=False, separators=(",", ":"), sort_keys=True
                        ),
                    },
                )
                .mappings()
                .one()
            )
            virtual_id = int(inserted["id"])
            projection: VirtualImportProjection | None = None
            try:
                projection = projector.project(virtual_id, bundle)
                updated = (
                    uow.session.execute(
                        text(
                            """
                            UPDATE shared_virtual_imports
                            SET status = 'virtual', strm_paths_json = CAST(:paths AS jsonb),
                                files_json = CAST(:files AS jsonb),
                                file_count = :file_count, updated_at = NOW()
                            WHERE id = :id
                            RETURNING *
                            """
                        ),
                        {
                            "id": virtual_id,
                            "paths": json.dumps(list(projection.paths), ensure_ascii=False, separators=(",", ":")),
                            "files": json.dumps(
                                list(projection.files), ensure_ascii=False, separators=(",", ":"), sort_keys=True
                            ),
                            "file_count": len(projection.files),
                        },
                    )
                    .mappings()
                    .one()
                )
                uow.commit()
            except BaseException as exc:
                uow.rollback()
                reconcile_projection = (
                    exc.projection if isinstance(exc, VirtualImportProjectionReconcileRequired) else None
                )
                if projection is not None:
                    try:
                        projector.cleanup(
                            {
                                "id": virtual_id,
                                "strm_paths_json": list(projection.paths),
                                "files_json": list(projection.files),
                            }
                        )
                    except BaseException as cleanup_exc:
                        exc.add_note(f"virtual import rollback cleanup failed: {cleanup_exc}")
                        reconcile_projection = projection
                if reconcile_projection is not None:
                    try:
                        self._record_reconcile_required(
                            virtual_id,
                            bundle,
                            reconcile_projection,
                            reason="projection_compensation_failed",
                        )
                    except BaseException as reconcile_exc:
                        exc.add_note(f"virtual import reconcile record failed: {reconcile_exc}")
                raise
            item = _row(updated)
            item["strm_paths_json"] = list(projection.paths)
            item["files_json"] = list(projection.files)
            return VirtualImportCreation(item, True)

    def _record_reconcile_required(
        self,
        virtual_id: int,
        bundle: VirtualImportBundle,
        projection: VirtualImportProjection,
        *,
        reason: str,
    ) -> None:
        metadata = _metadata(bundle)
        source_payload = {
            **bundle.source,
            _BUNDLE_HASH_KEY: bundle.state_hash,
            "_etk_reconcile": {"type": reason, "virtual_id": virtual_id},
        }
        scope = f"shared-virtual-import:{bundle.source_kind}:{bundle.source_id}"
        with self._unit_of_work() as uow:
            uow.session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"), {"scope": scope})
            existing = uow.session.scalar(
                text(
                    """
                    SELECT id FROM shared_virtual_imports
                    WHERE source_kind = :source_kind AND source_id = :source_id
                      AND status IN ('creating', 'virtual', 'promoting', 'reconcile_required')
                    ORDER BY id DESC LIMIT 1
                    """
                ),
                {"source_kind": bundle.source_kind, "source_id": bundle.source_id},
            )
            if existing is not None:
                raise RuntimeError("virtual import reconcile identity is already occupied")
            uow.session.execute(
                text(
                    """
                    INSERT INTO shared_virtual_imports(
                        id, source_kind, source_id, tmdb_id, item_type, parent_series_tmdb_id,
                        season_number, episode_number, title, release_year, file_count,
                        total_size, status, strm_paths_json, source_payload_json, files_json,
                        updated_at
                    ) VALUES (
                        :id, :source_kind, :source_id, :tmdb_id, :item_type, :parent_series_tmdb_id,
                        :season_number, :episode_number, :title, :release_year, :file_count,
                        :total_size, 'reconcile_required', CAST(:paths AS jsonb),
                        CAST(:source_payload AS jsonb), CAST(:files AS jsonb), NOW()
                    )
                    """
                ),
                {
                    "id": virtual_id,
                    **metadata,
                    "source_kind": bundle.source_kind,
                    "source_id": bundle.source_id,
                    "paths": json.dumps(list(projection.paths), ensure_ascii=False, separators=(",", ":")),
                    "source_payload": canonical_json(source_payload, field="virtual reconcile source"),
                    "files": json.dumps(
                        list(projection.files), ensure_ascii=False, separators=(",", ":"), sort_keys=True
                    ),
                },
            )
            uow.commit()

    def get(self, virtual_id: int) -> dict[str, Any] | None:
        if isinstance(virtual_id, bool) or int(virtual_id) <= 0:
            return None
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text("SELECT * FROM shared_virtual_imports WHERE id = :id"),
                    {"id": int(virtual_id)},
                )
                .mappings()
                .one_or_none()
            )
        return _row(row) if row is not None else None

    def active_by_source(self, source_kind: str, source_id: str) -> tuple[dict[str, Any], ...]:
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT * FROM shared_virtual_imports
                        WHERE source_kind = :source_kind AND source_id = :source_id
                          AND status IN ('virtual', 'promoting', 'reconcile_required')
                        ORDER BY id ASC
                        """
                    ),
                    {"source_kind": source_kind, "source_id": source_id},
                )
                .mappings()
                .all()
            )
        return tuple(_row(row) for row in rows)

    def remove(
        self,
        virtual_id: int,
        *,
        expected_state_hash: str,
        projector: VirtualImportProjector,
    ) -> Mapping[str, Any]:
        scope = f"shared-virtual-import:id:{int(virtual_id)}"
        with self._unit_of_work() as uow:
            uow.session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"), {"scope": scope})
            row = (
                uow.session.execute(
                    text("SELECT * FROM shared_virtual_imports WHERE id = :id FOR UPDATE"),
                    {"id": int(virtual_id)},
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return {"removed": False, "absent": True}
            item = _row(row)
            if virtual_import_state_hash(item) != str(expected_state_hash or "").strip().lower():
                raise RuntimeError("虚拟入库记录状态已变化，拒绝删除")
            cleanup = dict(projector.cleanup(item))
            deleted = uow.session.execute(
                text("DELETE FROM shared_virtual_imports WHERE id = :id"),
                {"id": int(virtual_id)},
            )
            if int(getattr(deleted, "rowcount", 0) or 0) != 1:
                raise RuntimeError("虚拟入库记录删除未确认")
            uow.commit()
        return {"removed": True, "absent": False, **cleanup}


class AtomicVirtualImportProjector:
    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        libraries: P115LocalStrmLibraryGateway,
    ) -> None:
        self._settings = settings
        self._libraries = libraries

    def project(self, virtual_id: int, bundle: VirtualImportBundle) -> VirtualImportProjection:
        settings = self._settings()
        root = str(settings.local_strm_root or "").strip()
        base_url = str(settings.etk_server_url or "").strip().rstrip("/")
        if not root:
            raise RuntimeError("请先配置本地 STRM 根目录")
        if not base_url.startswith(("http://", "https://")):
            raise RuntimeError("STRM 链接地址必须以 http:// 或 https:// 开头")
        library = self._libraries.open(root)
        if library is None:
            raise RuntimeError("本地 STRM 根目录不是已存在的安全绝对目录")

        projected: list[dict[str, Any]] = []
        absolute_paths: list[str] = []
        written: list[tuple[str, str]] = []
        seen_paths: set[str] = set()
        try:
            for item in bundle.files:
                relative_path, safe_name = _virtual_path(bundle.source, item)
                strm_path = str(PurePosixPath(relative_path) / f"{Path(safe_name).stem}.strm")
                path_key = strm_path.casefold()
                if path_key in seen_paths:
                    raise ValueError("虚拟入库文件会写入重复 STRM 路径")
                seen_paths.add(path_key)
                content = f"{base_url}/api/p115/virtual-play/{virtual_id}/{item['sha1']}/{safe_name}"
                digest = hashlib.sha256(content.encode()).hexdigest()
                result = library.create_strm(strm_path, content)
                written.append((strm_path, digest))
                absolute_paths.append(result.absolute_path)
                projected.append(
                    {
                        **dict(item),
                        "virtual_relative_path": strm_path,
                        "virtual_file_name": safe_name,
                        "virtual_content_sha256": digest,
                    }
                )
        except BaseException as exc:
            try:
                self._delete_owned(library, written)
            except BaseException as cleanup_exc:
                projection = VirtualImportProjection(tuple(absolute_paths), tuple(projected))
                failure = VirtualImportProjectionReconcileRequired(
                    "虚拟入库投影失败且文件补偿未完成",
                    projection,
                )
                failure.add_note(f"projection error: {type(exc).__name__}")
                failure.add_note(f"virtual import projection cleanup failed: {cleanup_exc}")
                raise failure from exc
            raise
        return VirtualImportProjection(tuple(absolute_paths), tuple(projected))

    def cleanup(self, item: Mapping[str, Any]) -> Mapping[str, Any]:
        settings = self._settings()
        root = str(settings.local_strm_root or "").strip()
        library = self._libraries.open(root)
        if library is None:
            raise RuntimeError("本地 STRM 根目录不是已存在的安全绝对目录")
        files = _json_list(item.get("files_json"))
        owned: list[tuple[str, str]] = []
        for file_item in files:
            if not isinstance(file_item, Mapping):
                raise RuntimeError("虚拟入库 ownership 文件快照无效")
            path = str(file_item.get("virtual_relative_path") or "").strip()
            digest = str(file_item.get("virtual_content_sha256") or "").strip().lower()
            if not path or not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise RuntimeError("虚拟入库 ownership 文件快照不完整")
            owned.append((path, digest))
        deleted, deferred = self._delete_owned(library, owned)
        if deferred:
            raise RuntimeError("虚拟入库 STRM 身份发生变化，拒绝删除记录")
        return {
            "deleted_files": deleted,
            "absent_files": max(0, len(owned) - deleted),
            "deferred_files": deferred,
        }

    @staticmethod
    def _delete_owned(library: Any, values: Sequence[tuple[str, str]]) -> tuple[int, int]:
        actions: list[P115LocalDeleteAction] = []
        for path, digest in values:
            action = library.plan_delete(
                path,
                kind="file",
                source="cache_projection",
                expected_sha256=digest,
            )
            if action is not None:
                actions.append(action)
        if not actions:
            return 0, 0
        roots = sorted({action.path.split("/", 1)[0] for action in actions})
        result = library.reconcile(
            roots=roots,
            desired_paths=[],
            cleanup=False,
            allowed_deletions=actions,
        )
        return len(result.deleted_paths), len(result.deferred_paths)


class VirtualImportService:
    def __init__(
        self,
        *,
        resolver: VirtualImportSourceResolver,
        repository: VirtualImportRepository,
        projector: VirtualImportProjector,
        configuration: Callable[[], Mapping[str, Any]],
    ) -> None:
        self._resolver = resolver
        self._repository = repository
        self._projector = projector
        self._configuration = configuration

    def freeze(self, source: Mapping[str, Any]) -> VirtualImportSourceIntent:
        return freeze_virtual_import_source(source)

    def prepare(self, source: Mapping[str, Any]) -> VirtualImportBundle:
        bundle = self._resolver.resolve(source)
        config = dict(self._configuration() or {})
        title = str(bundle.source.get("title") or bundle.files[0].get("title") or bundle.source_id)
        if bool(config.get("p115_shared_block_clean_version_transfer")) and _feature_flag(
            bundle.source, "is_clean_version", "clean_version_meta_json"
        ):
            raise ValueError(f"已开启“不秒传纯净版”，跳过《{title}》。")
        if bool(config.get("p115_shared_block_short_drama_transfer")) and _feature_flag(
            bundle.source, "is_short_drama", "short_drama_meta_json"
        ):
            raise ValueError(f"已开启“不秒传短剧”，跳过《{title}》。")
        return bundle

    def create(self, source: Mapping[str, Any]) -> VirtualImportCreation:
        return self._repository.create(self.prepare(source), self._projector)

    def create_bundle(self, bundle: VirtualImportBundle) -> VirtualImportCreation:
        return self._repository.create(bundle, self._projector)

    def get(self, virtual_id: int) -> dict[str, Any] | None:
        return self._repository.get(virtual_id)

    def active_by_source(self, source_kind: str, source_id: str) -> tuple[dict[str, Any], ...]:
        return self._repository.active_by_source(source_kind, source_id)

    def cleanup(self, item: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._projector.cleanup(item)

    def remove(self, virtual_id: int, *, expected_state_hash: str) -> Mapping[str, Any]:
        return self._repository.remove(
            virtual_id,
            expected_state_hash=expected_state_hash,
            projector=self._projector,
        )


class HttpxEmbyVirtualItemGateway:
    """Resolve and delete only exact-path Emby items with one bounded request per path."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def exact_item_ids(self, paths: Sequence[str]) -> tuple[str, ...]:
        normalized_paths = tuple(dict.fromkeys(_absolute_media_path(path) for path in paths))
        if not normalized_paths:
            raise ValueError("虚拟入库记录缺少 STRM 路径")
        base_url, token, _user_id = self._configuration()
        found: list[str] = []
        with self._client_factory() as client:
            for path in normalized_paths:
                response = client.get(
                    f"{base_url}/Items",
                    headers={"Accept": "application/json", "X-Emby-Token": token},
                    params={"Recursive": "true", "Path": path, "Fields": "Id,Path,Name"},
                )
                if response.status_code == 404:
                    continue
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, Mapping) or not isinstance(payload.get("Items"), list):
                    raise RuntimeError("Emby Items 回读不是有效对象")
                matches = [
                    item
                    for item in payload["Items"]
                    if isinstance(item, Mapping) and _media_path(item.get("Path")) == _media_path(path)
                ]
                if len(matches) > 1:
                    raise RuntimeError("Emby 返回多个相同路径的虚拟项，拒绝自动删除")
                if not matches:
                    continue
                item_id = str(matches[0].get("Id") or "").strip()
                if not item_id:
                    raise RuntimeError("Emby 精确路径项缺少 Id")
                found.append(item_id)
        return tuple(dict.fromkeys(found))

    def delete_items(self, item_ids: Sequence[str]) -> None:
        normalized_ids = tuple(dict.fromkeys(str(value or "").strip() for value in item_ids))
        if any(not value for value in normalized_ids):
            raise ValueError("Emby item id 无效")
        base_url, token, user_id = self._configuration()
        with self._client_factory() as client:
            for item_id in normalized_ids:
                response = client.post(
                    f"{base_url}/Items/{quote(item_id, safe='')}/Delete",
                    headers={"X-Emby-Token": token},
                    params={"UserId": user_id},
                )
                if response.status_code in {200, 204, 404}:
                    continue
                response.raise_for_status()
                raise RuntimeError(f"Emby 拒绝删除虚拟项: HTTP {response.status_code}")

    def _configuration(self) -> tuple[str, str, str]:
        settings = self._settings()
        base_url = str(settings.emby_server_url or "").strip().rstrip("/")
        token = settings.emby_api_key.get_secret_value().strip()
        user_id = str(settings.emby_user_id or "").strip()
        if not base_url or not token or not user_id:
            raise RuntimeError("Emby 虚拟项删除配置不完整")
        return base_url, token, user_id

    def _default_client(self) -> httpx.Client:
        return httpx.Client(timeout=float(self._settings().emby_api_timeout))


class VirtualImportOperationDelivery:
    """Durable Worker owner for local virtual create and exact-path retirement."""

    def __init__(self, service: VirtualImportService, emby: VirtualImportEmbyGateway) -> None:
        self._service = service
        self._emby = emby

    @staticmethod
    def supports(operation: SharingOperation) -> bool:
        request = operation.request if isinstance(operation.request, Mapping) else {}
        return str(request.get("provider") or "") == "virtual_import" and str(request.get("action") or "") in {
            "create",
            "delete",
        }

    def request(
        self,
        operation: SharingOperation,
        *,
        checkpoint: Callable[[], None],
    ) -> SharingDeliveryOutcome:
        action, payload = _virtual_operation_parts(operation)
        if action == "create":
            source = _required_mapping(payload.get("source"), "virtual create source")
            intent = self._service.freeze(source)
            _assert_virtual_source_intent(payload, intent)
            bundle = self._service.prepare(intent.source)
            bundle = VirtualImportBundle(
                bundle.source_kind,
                bundle.source_id,
                {**bundle.source, _INTENT_HASH_KEY: intent.state_hash},
                bundle.files,
            )
            checkpoint()
            created = self._service.create_bundle(bundle)
            return SharingDeliveryOutcome.confirmed(_safe_virtual_creation(created))

        virtual_id = _positive_virtual_id(payload.get("virtual_id"))
        expected_hash = _required_hash(payload.get("state_hash"), "virtual state hash")
        row = self._service.get(virtual_id)
        if row is None:
            checkpoint()
            return SharingDeliveryOutcome.confirmed(
                {"authoritative_readback": True, "virtual_id": virtual_id, "removed": False, "absent": True}
            )
        if virtual_import_state_hash(row) != expected_hash:
            return SharingDeliveryOutcome.rejected({"type": "virtual_import_state_changed"})
        paths = _virtual_paths(row)
        item_ids = self._emby.exact_item_ids(paths)
        checkpoint()
        self._emby.delete_items(item_ids)
        remaining = self._emby.exact_item_ids(paths)
        if remaining:
            return SharingDeliveryOutcome.indeterminate(
                {"type": "virtual_emby_items_still_present", "remaining_count": len(remaining)}
            )
        cleanup = dict(self._service.cleanup(row))
        return SharingDeliveryOutcome.confirmed(
            {"authoritative_readback": True, "virtual_id": virtual_id, "removed": True, **cleanup}
        )

    def readback(self, operation: SharingOperation) -> SharingDeliveryOutcome:
        action, payload = _virtual_operation_parts(operation)
        if action == "create":
            source = _required_mapping(payload.get("source"), "virtual create source")
            intent = self._service.freeze(source)
            _assert_virtual_source_intent(payload, intent)
            rows = self._service.active_by_source(intent.source_kind, intent.source_id)
            if len(rows) == 1:
                if str(rows[0].get("status") or "") == "reconcile_required":
                    return SharingDeliveryOutcome.indeterminate({"type": "virtual_import_reconcile_required"})
                source_payload = _json_object(rows[0].get("source_payload_json"))
                if str(source_payload.get(_INTENT_HASH_KEY) or "") != intent.state_hash:
                    return SharingDeliveryOutcome.indeterminate({"type": "virtual_import_readback_identity_changed"})
                return SharingDeliveryOutcome.confirmed(
                    {"authoritative_readback": True, "virtual_id": int(rows[0]["id"]), "created": False}
                )
            return SharingDeliveryOutcome.indeterminate(
                {"type": "virtual_import_readback_ambiguous" if rows else "virtual_import_readback_absent"}
            )

        virtual_id = _positive_virtual_id(payload.get("virtual_id"))
        expected_hash = _required_hash(payload.get("state_hash"), "virtual state hash")
        row = self._service.get(virtual_id)
        if row is None:
            return SharingDeliveryOutcome.confirmed(
                {"authoritative_readback": True, "virtual_id": virtual_id, "removed": False, "absent": True}
            )
        if virtual_import_state_hash(row) != expected_hash:
            return SharingDeliveryOutcome.indeterminate({"type": "virtual_import_state_changed_after_request"})
        remaining = self._emby.exact_item_ids(_virtual_paths(row))
        if remaining:
            return SharingDeliveryOutcome.retryable(
                {"type": "virtual_emby_items_still_present", "remaining_count": len(remaining)}
            )
        cleanup = dict(self._service.cleanup(row))
        return SharingDeliveryOutcome.confirmed(
            {"authoritative_readback": True, "virtual_id": virtual_id, "removed": True, **cleanup}
        )


def virtual_import_state_hash(item: Mapping[str, Any]) -> str:
    payload = {
        "id": int(item.get("id") or 0),
        "source_kind": str(item.get("source_kind") or ""),
        "source_id": str(item.get("source_id") or ""),
        "status": str(item.get("status") or ""),
        "source_payload_json": _json_object(item.get("source_payload_json")),
        "files_json": _json_list(item.get("files_json")),
        "strm_paths_json": _json_list(item.get("strm_paths_json")),
    }
    return hashlib.sha256(canonical_json(payload, field="virtual import row state").encode()).hexdigest()


def _bundle_state_hash_from_row(item: Mapping[str, Any]) -> str:
    persisted = str(_json_object(item.get("source_payload_json")).get(_BUNDLE_HASH_KEY) or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{64}", persisted):
        return persisted
    payload = {
        "source_kind": str(item.get("source_kind") or ""),
        "source_id": str(item.get("source_id") or ""),
        "files": _json_list(item.get("files_json")),
    }
    return hashlib.sha256(canonical_json(payload, field="virtual import state").encode()).hexdigest()


def _source_kind(source: Mapping[str, Any]) -> str:
    value = str(source.get("source_kind") or source.get("kind") or "").strip().lower().replace("-", "_")
    if value:
        return value
    item_type = str(source.get("item_type") or source.get("display_type") or "").strip().lower()
    return {"movie": "movie", "episode": "episode"}.get(item_type, "")


def _source_id(source: Mapping[str, Any], source_kind: str) -> str:
    values = (
        source.get("source_id"),
        source.get("source_ref_id"),
        source.get("asset_id") if source_kind == "logical_episode" else None,
        source.get("hub_id") if source_kind == "season_hub" else None,
        source.get("id"),
    )
    return next((str(value).strip() for value in values if str(value or "").strip()), "")[:255]


def _embedded_files(source: Mapping[str, Any], source_kind: str) -> list[Mapping[str, Any]]:
    if source_kind == "logical_season" and isinstance(source.get("best_asset_map"), Mapping):
        values = source["best_asset_map"]
        return [
            item
            for _key, item in sorted(values.items(), key=lambda pair: _integer(pair[0]))
            if isinstance(item, Mapping)
        ]
    for key in ("files", "pack_items", "children", "items"):
        value = source.get(key)
        if isinstance(value, list) and value:
            return [item for item in value if isinstance(item, Mapping)]
    return []


def _provider_files(response: ProviderResponse[dict[str, Any]]) -> list[Mapping[str, Any]]:
    if not isinstance(response, ProviderResponse):
        raise TypeError("共享中心子项查询返回无效响应")
    if response.disposition is not ProviderDisposition.CONFIRMED:
        raise RuntimeError("共享中心子项查询未确认")
    payload = response.payload if isinstance(response.payload, Mapping) else {}
    containers = [payload]
    if isinstance(payload.get("data"), Mapping):
        containers.append(payload["data"])
    for container in containers:
        for key in ("files", "pack_items", "children", "items"):
            value = container.get(key)
            if isinstance(value, list) and value:
                return [item for item in value if isinstance(item, Mapping)]
    return []


def _normalize_file(
    source: Mapping[str, Any],
    source_kind: str,
    source_id: str,
    raw: Mapping[str, Any],
) -> dict[str, Any]:
    item = _bounded_object(raw, field="center source file")
    sha1 = str(item.get("sha1") or "").strip().upper()
    if not _SHA1.fullmatch(sha1):
        raise ValueError("中心文件缺少合法 SHA1")
    size = _integer(item.get("size") or item.get("file_size") or item.get("fs"))
    if size <= 0:
        raise ValueError("中心文件缺少合法 size")
    file_name = str(item.get("file_name") or item.get("name") or f"{sha1}.mkv").strip()
    if not file_name:
        raise ValueError("中心文件缺少文件名")
    child_kind = (
        "logical_episode"
        if source_kind == "logical_season"
        else ("episode" if source_kind == "season_hub" else source_kind)
    )
    child_id = _source_id(item, child_kind) or source_id
    normalized = {
        **item,
        "sha1": sha1,
        "size": size,
        "file_name": file_name[:512],
        "source_kind": child_kind,
        "source_id": child_id,
        "item_type": item.get("item_type")
        or source.get("item_type")
        or ("Movie" if child_kind == "movie" else "Episode"),
        "tmdb_id": item.get("tmdb_id") or source.get("tmdb_id"),
        "parent_series_tmdb_id": item.get("parent_series_tmdb_id")
        or item.get("series_tmdb_id")
        or source.get("parent_series_tmdb_id")
        or source.get("series_tmdb_id"),
        "season_number": (
            item.get("season_number") if item.get("season_number") not in (None, "") else source.get("season_number")
        ),
        "episode_number": item.get("episode_number"),
        "title": item.get("title") or source.get("title"),
        "release_year": item.get("release_year") or source.get("release_year") or source.get("year"),
    }
    return _bounded_object(normalized, field="normalized center source file")


def _metadata(bundle: VirtualImportBundle) -> dict[str, Any]:
    first = bundle.files[0]
    item_type = str(bundle.source.get("item_type") or first.get("item_type") or "")
    parent_tmdb = str(
        bundle.source.get("parent_series_tmdb_id")
        or bundle.source.get("series_tmdb_id")
        or first.get("parent_series_tmdb_id")
        or ""
    )
    tmdb_id = str(bundle.source.get("tmdb_id") or first.get("tmdb_id") or "")
    if item_type.casefold() != "movie":
        tmdb_id = parent_tmdb or tmdb_id
    return {
        "tmdb_id": tmdb_id,
        "item_type": item_type or ("Movie" if bundle.source_kind == "movie" else "Episode"),
        "parent_series_tmdb_id": parent_tmdb,
        "season_number": _optional_integer(bundle.source.get("season_number") or first.get("season_number")),
        "episode_number": _optional_integer(bundle.source.get("episode_number") or first.get("episode_number")),
        "title": str(bundle.source.get("title") or first.get("title") or first.get("file_name") or "")[:500],
        "release_year": _optional_integer(
            bundle.source.get("release_year") or bundle.source.get("year") or first.get("release_year")
        ),
        "file_count": len(bundle.files),
        "total_size": sum(int(item["size"]) for item in bundle.files),
    }


def _virtual_path(source: Mapping[str, Any], item: Mapping[str, Any]) -> tuple[str, str]:
    is_movie = str(item.get("item_type") or source.get("item_type") or "").casefold() == "movie"
    title = _safe_component(source.get("title") or item.get("title") or "共享资源")
    tmdb_id = str(
        source.get("tmdb_id")
        if is_movie
        else source.get("parent_series_tmdb_id")
        or source.get("series_tmdb_id")
        or item.get("parent_series_tmdb_id")
        or item.get("tmdb_id")
        or ""
    ).strip()
    year = _optional_integer(source.get("release_year") or source.get("year") or item.get("release_year"))
    root = title
    if year:
        root += f" ({year})"
    if tmdb_id:
        root += f" {{tmdb={_safe_component(tmdb_id)}}}"
    original = _safe_component(item.get("file_name") or item.get("name") or f"{item['sha1']}.mkv")
    suffix = Path(original).suffix or ".mkv"
    if is_movie:
        return str(PurePosixPath("电影") / root), original
    season = max(1, _integer(item.get("season_number") or source.get("season_number") or 1))
    episode = _optional_integer(item.get("episode_number"))
    filename = original if episode is None else f"{title} - S{season:02d}E{episode:02d}{suffix}"
    return str(PurePosixPath("剧集") / root / f"Season {season:02d}"), _safe_component(filename)


def _safe_component(value: Any) -> str:
    text_value = re.sub(r"[\\/:*?\"<>|\r\n\t]+", " ", str(value or "").strip())
    text_value = re.sub(r"\s+", " ", text_value).strip(" ._-")
    return (text_value or "未命名")[:180]


def _feature_flag(source: Mapping[str, Any], key: str, meta_key: str) -> bool:
    if _boolish(source.get(key)):
        return True
    meta = _json_object(source.get(meta_key))
    if _boolish(meta.get(key)):
        return True
    for nested_key in ("version_summary", "summary_json", "media_signature_json"):
        nested = _json_object(source.get(nested_key))
        if _boolish(nested.get(key)) or _boolish(_json_object(nested.get(meta_key)).get(key)):
            return True
    return False


def _bounded_object(value: Mapping[str, Any], *, field: str) -> dict[str, Any]:
    normalized = normalize_durable_json_object(dict(value), field=field)
    _reject_sensitive(normalized)
    encoded = canonical_json(normalized, field=field).encode()
    if len(encoded) > _MAX_SOURCE_BYTES:
        raise ValueError(f"{field} exceeds {_MAX_SOURCE_BYTES} bytes")
    return normalized


def _reject_sensitive(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized_key = str(key).strip().lower().replace("-", "_")
            compact_key = re.sub(r"[^a-z0-9]+", "", normalized_key)
            if (
                normalized_key in _SENSITIVE_KEYS
                or compact_key in _SENSITIVE_COMPACT_KEYS
                or normalized_key.endswith(("_token", "_secret", "_password"))
            ):
                raise ValueError("中心源包含敏感字段，不能进入持久操作")
            _reject_sensitive(item)
    elif isinstance(value, list):
        for item in value:
            _reject_sensitive(item)
    elif isinstance(value, str):
        if len(value.encode("utf-8")) > _MAX_SOURCE_STRING_BYTES:
            raise ValueError("中心源字符串超过持久化上限")
        stripped = value.strip()
        if stripped.startswith(("{", "[")):
            try:
                decoded = json.loads(stripped)
            except (TypeError, ValueError):
                pass
            else:
                if isinstance(decoded, Mapping | list):
                    _reject_sensitive(decoded)
        if _SENSITIVE_VALUE.search(value):
            raise ValueError("中心源包含 URL、临时路径或凭据形态值，不能进入持久操作")


def _virtual_operation_parts(operation: SharingOperation) -> tuple[str, dict[str, Any]]:
    request = operation.request if isinstance(operation.request, Mapping) else {}
    if str(request.get("provider") or "") != "virtual_import":
        raise ValueError("sharing operation is not a virtual import operation")
    action = str(request.get("action") or "").strip()
    if action not in {"create", "delete"}:
        raise ValueError("unsupported virtual import action")
    payload = _required_mapping(request.get("payload"), "virtual import payload")
    return action, _bounded_object(payload, field="virtual import operation payload")


def _assert_virtual_source_intent(payload: Mapping[str, Any], intent: VirtualImportSourceIntent) -> None:
    if str(payload.get("source_kind") or "") != intent.source_kind:
        raise ValueError("virtual source kind changed")
    if str(payload.get("source_id") or "") != intent.source_id:
        raise ValueError("virtual source id changed")
    if _required_hash(payload.get("source_state_hash"), "virtual source state hash") != intent.state_hash:
        raise ValueError("virtual source state hash changed")


def _safe_virtual_creation(value: VirtualImportCreation) -> dict[str, Any]:
    item = value.item
    return {
        "authoritative_readback": True,
        "created": bool(value.created),
        "virtual_id": int(item.get("id") or 0),
        "file_count": len(_json_list(item.get("files_json"))),
    }


def _virtual_paths(item: Mapping[str, Any]) -> tuple[str, ...]:
    values = tuple(str(value or "").strip() for value in _json_list(item.get("strm_paths_json")))
    if not values or any(not value for value in values):
        raise RuntimeError("虚拟入库记录缺少完整 STRM ownership 路径")
    return values


def _required_mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    return dict(value)


def _required_hash(value: Any, field: str) -> str:
    normalized = str(value or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", normalized):
        raise ValueError(f"{field} is invalid")
    return normalized


def _positive_virtual_id(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("virtual id is invalid")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("virtual id is invalid") from exc
    if normalized < 1:
        raise ValueError("virtual id is invalid")
    return normalized


def _absolute_media_path(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized or not os.path.isabs(normalized):
        raise ValueError("Emby 虚拟项路径必须是绝对路径")
    return os.path.normpath(normalized)


def _media_path(value: Any) -> str:
    return os.path.normcase(os.path.normpath(str(value or "").replace("\\", "/"))).casefold()


def _row(value: Any) -> dict[str, Any]:
    row = dict(value)
    for key in ("strm_paths_json", "files_json"):
        row[key] = _json_list(row.get(key))
    row["source_payload_json"] = _json_object(row.get("source_payload_json"))
    return row


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except ValueError:
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, Mapping) and isinstance(value.get("items"), list):
        return list(value["items"])
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except ValueError:
            return []
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, Mapping) and isinstance(parsed.get("items"), list):
            return list(parsed["items"])
    return []


def _integer(value: Any) -> int:
    try:
        if value in (None, "") or isinstance(value, bool):
            return 0
        return int(float(value))
    except (TypeError, ValueError, OverflowError):
        return 0


def _optional_integer(value: Any) -> int | None:
    integer = _integer(value)
    return integer if integer else None


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


__all__ = [
    "AtomicVirtualImportProjector",
    "HttpxEmbyVirtualItemGateway",
    "SqlAlchemyVirtualImportRepository",
    "VirtualImportBundle",
    "VirtualImportCreation",
    "VirtualImportProjection",
    "VirtualImportProjectionReconcileRequired",
    "VirtualImportRepository",
    "VirtualImportService",
    "VirtualImportSourceIntent",
    "VirtualImportSourceResolver",
    "VirtualImportOperationDelivery",
    "freeze_virtual_import_source",
    "virtual_import_state_hash",
]
