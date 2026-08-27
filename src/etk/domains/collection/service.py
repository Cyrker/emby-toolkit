from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from .repository import CollectionNameConflictError


@dataclass(frozen=True, slots=True)
class CollectionHttpResult:
    payload: Any
    status_code: int = 200
    headers: Mapping[str, str] | None = None


class CollectionSettingsStore(Protocol):
    def load(self, key: str) -> Any: ...

    def save(self, key: str, value: Any) -> None: ...

    def merge_mapping(
        self,
        key: str,
        updates: Mapping[str, Any],
        *,
        remove_keys: Sequence[str] = (),
    ) -> Mapping[str, Any]: ...


class CollectionRepository(Protocol):
    def list_custom(self) -> list[dict[str, Any]]: ...

    def get_custom(self, collection_id: int) -> dict[str, Any] | None: ...

    def create_custom(
        self,
        *,
        name: str,
        collection_type: str,
        definition: Any,
        allowed_user_ids: Sequence[str] | None,
    ) -> dict[str, Any]: ...

    def update_custom(
        self,
        collection_id: int,
        *,
        name: str,
        collection_type: str,
        definition: Any,
        status: str,
        allowed_user_ids: Sequence[str] | None,
    ) -> dict[str, Any] | None: ...

    def delete_custom(self, collection_id: int) -> dict[str, Any] | None: ...

    def update_custom_order(self, ordered_ids: Sequence[Any]) -> None: ...

    def list_native(self, *, active_only: bool = True) -> list[dict[str, Any]]: ...

    def get_native(self, tmdb_collection_id: str) -> dict[str, Any] | None: ...

    def upsert_native(
        self,
        details: Mapping[str, Any],
        existing_emby_id: str | None = None,
    ) -> dict[str, Any]: ...

    def deactivate_native(self, emby_collection_id: str) -> bool: ...


class CollectionUserProjection(Protocol):
    def expand_template_users(self, selected_user_ids: Sequence[str] | None) -> list[str]: ...

    def collection_user_options(self) -> list[dict[str, Any]]: ...


class CollectionMediaProjection(Protocol):
    def custom_status(self, collection: Mapping[str, Any]) -> dict[str, Any]: ...

    def collection_status(self, collections: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]: ...

    def local_filter_values(self, kind: str, query: str = "") -> Sequence[Any]: ...


class CollectionLocalGateway(Protocol):
    def defaults(self) -> Mapping[str, Any]: ...

    def cover_preview(self, payload: Mapping[str, Any]) -> str: ...


class CollectionEffectsGateway(Protocol):
    def collection_details(self, tmdb_collection_id: int) -> Mapping[str, Any] | None: ...

    def search_collections(self, query: str) -> Sequence[Mapping[str, Any]]: ...

    def archive_metadata_images(self, payload: Mapping[str, Any], host_url: str) -> None: ...

    def schedule_missing_collection_cleanup(self, tmdb_collection_id: str) -> None: ...

    def delete_native_collection(self, emby_collection_id: str) -> bool: ...

    def delete_custom_collection_effect(self, collection: Mapping[str, Any]) -> None: ...

    def cleanup_custom_placeholders(self, collection: Mapping[str, Any]) -> None: ...

    def fix_custom_match(self, collection_id: int, payload: Mapping[str, Any]) -> Mapping[str, Any] | None: ...

    def emby_libraries(self, *, cover: bool = False) -> Sequence[Mapping[str, Any]] | None: ...

    def tmdb_movie_genres(self) -> Sequence[Mapping[str, Any]] | None: ...

    def tmdb_tv_genres(self) -> Sequence[Mapping[str, Any]] | None: ...

    def search_tmdb_companies(self, query: str, search_type: str) -> Sequence[Mapping[str, Any]]: ...

    def search_tmdb_people(self, query: str) -> Sequence[Mapping[str, Any]]: ...


class CollectionProviderNotFoundError(LookupError):
    pass


class CollectionGatewayConfigurationError(RuntimeError):
    pass


COVER_DEFAULTS: dict[str, Any] = {
    "enabled": False,
    "transfer_monitor": True,
    "exclude_libraries": [],
    "sort_by": "Latest",
    "max_safe_rating": 8,
    "show_item_count": False,
    "badge_style": "badge",
    "badge_size_ratio": 0.12,
    "cover_style": "single_1",
    "tab": "style-tab",
    "title_config": (
        "# 配置封面标题（按媒体库名称对应）\n# 格式如下：\n#\n# 媒体库名称:\n#   - 中文标题\n#   - 英文标题\n#\n"
    ),
    "zh_font_path_local": "",
    "en_font_path_local": "",
    "zh_font_url": "",
    "en_font_url": "",
    "zh_font_size": 1.0,
    "en_font_size": 1.0,
    "blur_size": 50,
    "color_ratio": 0.8,
    "single_use_primary": False,
    "zh_font_path_multi_1_local": "",
    "en_font_path_multi_1_local": "",
    "zh_font_url_multi_1": "",
    "en_font_url_multi_1": "",
    "zh_font_size_multi_1": 1.0,
    "en_font_size_multi_1": 1.0,
    "blur_size_multi_1": 50,
    "color_ratio_multi_1": 0.8,
    "multi_1_blur": False,
    "multi_1_use_main_font": False,
    "multi_1_use_primary": True,
}


class CollectionService:
    def __init__(
        self,
        repository: CollectionRepository,
        settings: CollectionSettingsStore,
        gateway: CollectionLocalGateway,
        user_projection: CollectionUserProjection,
        media_projection: CollectionMediaProjection,
        effects_gateway: CollectionEffectsGateway | None = None,
    ) -> None:
        self._repository = repository
        self._settings = settings
        self._gateway = gateway
        self._user_projection = user_projection
        self._media_projection = media_projection
        self._effects_gateway = effects_gateway

    def _effects(self) -> CollectionEffectsGateway:
        if self._effects_gateway is None:
            raise RuntimeError("collection effects adapter is not configured")
        return self._effects_gateway

    def provider_metadata(self, tmdb_collection_id: str, host_url: str) -> CollectionHttpResult:
        value = str(tmdb_collection_id or "").strip()
        if not value.isdigit():
            return CollectionHttpResult({"error": "invalid tmdb collection id"}, 400)
        row = self._repository.get_native(value)
        if row is None or int(row.get("metadata_schema_version") or 0) < 1:
            try:
                details = self._effects().collection_details(int(value))
            except CollectionProviderNotFoundError:
                self._effects().schedule_missing_collection_cleanup(value)
                return CollectionHttpResult({"error": "tmdb collection no longer exists"}, 410)
            if details:
                row = self._repository.upsert_native(
                    details,
                    existing_emby_id=str((row or {}).get("emby_collection_id") or "") or None,
                )
        payload = _provider_payload(row)
        if payload is None:
            return CollectionHttpResult({"error": "collection metadata not found"}, 404)
        self._effects().archive_metadata_images(payload, host_url)
        return CollectionHttpResult(payload, headers={"cache-control": "no-store"})

    def provider_search(self, query: str) -> CollectionHttpResult:
        normalized = str(query or "").strip()
        if not normalized:
            return CollectionHttpResult([])
        key = normalized.casefold()
        merged: dict[str, dict[str, Any]] = {}
        local = sorted(
            self._repository.list_native(),
            key=lambda row: (
                str(row.get("name") or "").casefold() != key,
                not str(row.get("name") or "").casefold().startswith(key),
                str(row.get("name") or "").casefold(),
            ),
        )
        for row in local:
            if key not in str(row.get("name") or "").casefold():
                continue
            payload = _provider_payload(row)
            if payload:
                merged[payload["tmdb_id"]] = payload
        for row in self._effects().search_collections(normalized):
            payload = _provider_payload(row)
            if payload:
                merged.setdefault(payload["tmdb_id"], payload)
        return CollectionHttpResult(list(merged.values())[:20])

    def native_status(self) -> CollectionHttpResult:
        try:
            collections = self._repository.list_native()
            return CollectionHttpResult(self._media_projection.collection_status(collections))
        except Exception:
            return CollectionHttpResult({"error": "读取合集时发生服务器内部错误"}, 500)

    def delete_native(self, emby_collection_id: str) -> CollectionHttpResult:
        try:
            if not self._effects().delete_native_collection(emby_collection_id):
                return CollectionHttpResult({"error": "删除合集失败，请检查 Emby 日志"}, 500)
            self._repository.deactivate_native(emby_collection_id)
            return CollectionHttpResult({"message": "合集已成功从 Emby 删除"})
        except CollectionGatewayConfigurationError as exc:
            return CollectionHttpResult({"error": str(exc)}, 500)
        except Exception as exc:
            return CollectionHttpResult({"error": f"服务器内部错误: {exc}"}, 500)

    def native_settings(self) -> CollectionHttpResult:
        try:
            value = self._settings.load("native_collections_config")
            config = dict(value) if isinstance(value, Mapping) else {}
            config.pop("auto_complete_enabled", None)
            return CollectionHttpResult({"auto_sub_enabled": False, **config})
        except Exception:
            return CollectionHttpResult({"error": "获取设置失败"}, 500)

    def save_native_settings(self, payload: Any) -> CollectionHttpResult:
        if not isinstance(payload, Mapping):
            return CollectionHttpResult({"error": "无效的数据格式"}, 400)
        try:
            updates = {"auto_sub_enabled": bool(payload["auto_sub_enabled"])} if "auto_sub_enabled" in payload else {}
            self._settings.merge_mapping(
                "native_collections_config",
                updates,
                remove_keys=("auto_complete_enabled",),
            )
            return CollectionHttpResult({"message": "设置已保存"})
        except Exception:
            return CollectionHttpResult({"error": "保存设置失败"}, 500)

    def custom_users(self) -> CollectionHttpResult:
        try:
            return CollectionHttpResult(self._user_projection.collection_user_options())
        except Exception:
            return CollectionHttpResult({"error": "服务器内部错误"}, 500)

    def list_custom(self) -> CollectionHttpResult:
        try:
            rows = [_custom_list_item(row) for row in self._repository.list_custom()]
            return CollectionHttpResult(rows)
        except Exception:
            return CollectionHttpResult([])

    def create_custom(self, payload: Any) -> CollectionHttpResult:
        if not isinstance(payload, Mapping):
            return CollectionHttpResult({"error": "请求无效: 缺少 name, type, 或 definition"}, 400)
        name = payload.get("name")
        collection_type = payload.get("type")
        definition = payload.get("definition")
        if not name or not collection_type or not definition:
            return CollectionHttpResult({"error": "请求无效: 缺少 name, type, 或 definition"}, 400)
        allowed = payload.get("allowed_user_ids")
        expanded = self._user_projection.expand_template_users(_string_sequence(allowed))
        try:
            row = self._repository.create_custom(
                name=str(name),
                collection_type=str(collection_type),
                definition=definition,
                allowed_user_ids=expanded,
            )
            return CollectionHttpResult(row, 201)
        except CollectionNameConflictError:
            return CollectionHttpResult({"error": f"创建失败：名为 '{name}' 的合集已存在。"}, 409)
        except Exception:
            return CollectionHttpResult(
                {"error": "数据库操作失败，无法创建合集，请检查后端日志。"},
                500,
            )

    def update_custom(self, collection_id: int, payload: Any) -> CollectionHttpResult:
        if not isinstance(payload, Mapping):
            return CollectionHttpResult({"error": "请求无效: 缺少必要参数"}, 400)
        name = payload.get("name")
        collection_type = payload.get("type")
        definition = payload.get("definition")
        status = payload.get("status")
        if not all((name, collection_type, definition, status)):
            return CollectionHttpResult({"error": "请求无效: 缺少必要参数"}, 400)
        allowed = payload.get("allowed_user_ids")
        expanded = self._user_projection.expand_template_users(_string_sequence(allowed))
        try:
            row = self._repository.update_custom(
                collection_id,
                name=str(name),
                collection_type=str(collection_type),
                definition=definition,
                status=str(status),
                allowed_user_ids=expanded,
            )
            if row is None:
                return CollectionHttpResult({"error": "数据库操作失败，未找到或无法更新该合集"}, 404)
            return CollectionHttpResult(row)
        except CollectionNameConflictError:
            return CollectionHttpResult({"error": "数据库操作失败，未找到或无法更新该合集"}, 404)
        except Exception:
            return CollectionHttpResult({"error": "服务器内部错误，请检查后端日志"}, 500)

    def update_custom_order(self, payload: Any) -> CollectionHttpResult:
        ids = payload.get("ids") if isinstance(payload, Mapping) else None
        if not isinstance(ids, list):
            return CollectionHttpResult({"error": "请求无效: 需要一个ID列表。"}, 400)
        try:
            self._repository.update_custom_order(ids)
            return CollectionHttpResult({"message": "合集顺序已成功更新。"})
        except Exception:
            return CollectionHttpResult({"error": "服务器内部错误"}, 500)

    def delete_custom(self, collection_id: int) -> CollectionHttpResult:
        collection = self._repository.get_custom(collection_id)
        if collection is None:
            return CollectionHttpResult({"error": "未找到要删除的合集"}, 404)
        try:
            self._effects().delete_custom_collection_effect(collection)
            deleted = self._repository.delete_custom(collection_id)
            if deleted is None:
                return CollectionHttpResult({"error": "数据库删除操作失败，请查看日志。"}, 500)
            self._effects().cleanup_custom_placeholders(collection)
            return CollectionHttpResult({"message": f"自定义合集 '{collection.get('name')}' 已成功联动删除。"})
        except Exception:
            return CollectionHttpResult({"error": "服务器内部错误"}, 500)

    def custom_status(self, collection_id: int) -> CollectionHttpResult:
        try:
            collection = self._repository.get_custom(collection_id)
            if collection is None:
                return CollectionHttpResult({"error": "未找到合集"}, 404)
            return CollectionHttpResult(self._media_projection.custom_status(collection))
        except Exception:
            return CollectionHttpResult({"error": "服务器内部错误"}, 500)

    def fix_custom_match(self, collection_id: int, payload: Any) -> CollectionHttpResult:
        if not isinstance(payload, Mapping):
            return CollectionHttpResult({"error": "请求无效: 缺少 new_tmdb_id"}, 400)
        if not payload.get("new_tmdb_id"):
            return CollectionHttpResult({"error": "请求无效: 缺少 new_tmdb_id"}, 400)
        if not payload.get("old_tmdb_id") and not payload.get("old_title"):
            return CollectionHttpResult(
                {"error": "请求无效: 必须提供 old_tmdb_id 或 old_title 以定位项目"},
                400,
            )
        try:
            corrected = self._effects().fix_custom_match(collection_id, payload)
            if corrected is None:
                return CollectionHttpResult({"error": "修正失败，未找到对应的媒体项"}, 404)
            return CollectionHttpResult({"message": "修正成功！", "corrected_item": dict(corrected)})
        except Exception as exc:
            return CollectionHttpResult({"error": f"服务器内部错误: {exc}"}, 500)

    def mapping(self, key: str, default_key: str) -> CollectionHttpResult:
        value = self._settings.load(key)
        default = self._gateway.defaults()[default_key]
        if key == "release_group_mapping":
            return CollectionHttpResult(_release_group_mapping(value, default))
        if key == "junk_file_patterns":
            patterns = value if isinstance(value, list) else default
            return CollectionHttpResult(
                [{"label": item.get("label", "") if isinstance(item, Mapping) else str(item)} for item in patterns]
            )
        if key == "rating_mapping":
            return CollectionHttpResult(value if value else default)
        if key == "rating_priority":
            return CollectionHttpResult(value if value else default)
        if key == "unified_ratings":
            labels = [{"label": label} for label in default]
            return CollectionHttpResult(_ensure_list(value, labels))
        return CollectionHttpResult(_ensure_list(value, default))

    def save_mapping(self, key: str, payload: Any) -> CollectionHttpResult:
        if key in {"release_group_mapping", "junk_file_patterns", "rating_priority"} and not isinstance(payload, list):
            return CollectionHttpResult({"error": "数据格式错误，必须是列表"}, 400)
        if key == "rating_mapping" and not isinstance(payload, Mapping):
            return CollectionHttpResult({"error": "数据格式错误，必须是字典"}, 400)
        if key == "junk_file_patterns":
            patterns: list[str] = []
            for item in payload:
                pattern = item.get("label") if isinstance(item, Mapping) else item
                normalized = str(pattern or "").strip()
                if not normalized:
                    continue
                try:
                    re.compile(normalized)
                except re.error as exc:
                    return CollectionHttpResult({"error": f"无效正则：{normalized}（{exc}）"}, 400)
                patterns.append(normalized)
            payload = patterns
        self._settings.save(key, payload)
        messages = {
            "unified_ratings": "分级标签保存成功",
            "rating_mapping": "分级映射表保存成功",
            "rating_priority": "优先级策略保存成功",
        }
        return CollectionHttpResult({"message": messages.get(key, "保存成功")})

    def mapping_defaults(self, key: str) -> CollectionHttpResult:
        default = self._gateway.defaults()[key]
        if key == "release_group_mapping":
            default = _release_group_mapping(None, default)
        elif key == "junk_file_patterns":
            default = [{"label": value} for value in default]
        elif key == "unified_ratings":
            default = [{"label": value} for value in default]
        return CollectionHttpResult(default)

    def filter_options(self, kind: str) -> CollectionHttpResult:
        try:
            if kind in {"tags", "movie_genres", "tv_genres"}:
                return CollectionHttpResult(list(self._media_projection.local_filter_values(kind)))
            if kind == "keywords":
                values = self.mapping("keyword_mapping", "keyword_mapping").payload
                return CollectionHttpResult([{"label": item["label"], "value": item["label"]} for item in values])
            if kind == "studios":
                values = self.mapping("studio_mapping", "studio_mapping").payload
                return CollectionHttpResult([_studio_option(item) for item in values])
            if kind in {"languages", "countries"}:
                key = "language_mapping" if kind == "languages" else "country_mapping"
                values = self.mapping(key, key).payload
                return CollectionHttpResult(
                    [
                        {"label": item["label"], "value": item["value"]}
                        for item in values
                        if item.get("label") and item.get("value")
                    ]
                )
            if kind == "unified_ratings":
                values = self.mapping("unified_ratings", "unified_ratings").payload
                options = [item["label"] for item in values if isinstance(item, Mapping) and item.get("label")]
                return CollectionHttpResult(options or list(self._gateway.defaults()["unified_ratings"]))
            raise LookupError(kind)
        except Exception:
            if kind in {"movie_genres", "tv_genres", "countries"}:
                return CollectionHttpResult({"error": "服务器内部错误"}, 500)
            if kind == "unified_ratings":
                return CollectionHttpResult(list(self._gateway.defaults()["unified_ratings"]))
            return CollectionHttpResult([], 500)

    def tmdb_genres(self, media_type: str) -> CollectionHttpResult:
        try:
            values = self._effects().tmdb_movie_genres() if media_type == "movie" else self._effects().tmdb_tv_genres()
            if values is None:
                return CollectionHttpResult(
                    {"error": f"从 TMDb 获取{'电影' if media_type == 'movie' else '电视剧'}类型失败"}, 500
                )
            translations = self._gateway.defaults()["genre_translation"]
            return CollectionHttpResult(
                [{**dict(item), "name": translations.get(item.get("name"), item.get("name"))} for item in values]
            )
        except CollectionGatewayConfigurationError as exc:
            return CollectionHttpResult({"error": str(exc)}, 500)
        except Exception:
            return CollectionHttpResult({"error": "服务器内部错误"}, 500)

    def custom_libraries(self) -> CollectionHttpResult:
        try:
            values = self._effects().emby_libraries()
            if values is None:
                return CollectionHttpResult({"error": "无法从 Emby 获取媒体库列表"}, 500)
            return CollectionHttpResult(
                [
                    {"label": value.get("Name"), "value": value.get("Id")}
                    for value in values
                    if value.get("CollectionType") in {"movies", "tvshows", "mixed"}
                    or (not value.get("CollectionType") and value.get("Type") == "CollectionFolder")
                ]
            )
        except CollectionGatewayConfigurationError as exc:
            return CollectionHttpResult({"error": str(exc)}, 500)
        except Exception:
            return CollectionHttpResult({"error": "服务器内部错误"}, 500)

    def search_companies(self, query: str, search_type: str) -> CollectionHttpResult:
        if not query:
            return CollectionHttpResult([])
        try:
            return CollectionHttpResult(list(self._effects().search_tmdb_companies(query, search_type)))
        except Exception:
            return CollectionHttpResult({"error": "服务器内部错误"}, 500)

    def search_people(self, query: str) -> CollectionHttpResult:
        if not query:
            return CollectionHttpResult([])
        try:
            return CollectionHttpResult(list(self._effects().search_tmdb_people(query)))
        except Exception:
            return CollectionHttpResult({"error": "服务器内部错误"}, 500)

    def cover_config(self) -> CollectionHttpResult:
        try:
            value = self._settings.load("cover_generator_config")
            config = dict(value) if isinstance(value, Mapping) else {}
            return CollectionHttpResult({**COVER_DEFAULTS, **config})
        except Exception:
            return CollectionHttpResult({"error": "读取配置失败"}, 500)

    def save_cover_config(self, payload: Any) -> CollectionHttpResult:
        try:
            self._settings.save("cover_generator_config", payload)
            return CollectionHttpResult({"message": "配置已成功保存！"})
        except Exception:
            return CollectionHttpResult({"error": "保存配置失败"}, 500)

    def cover_libraries(self) -> CollectionHttpResult:
        try:
            values = self._effects().emby_libraries(cover=True)
            if not values:
                return CollectionHttpResult([])
            return CollectionHttpResult(
                [
                    {"label": value.get("Name"), "value": value.get("Id")}
                    for value in values
                    if value.get("Name") and value.get("Id")
                ]
            )
        except Exception:
            return CollectionHttpResult({"error": "获取媒体库列表失败"}, 500)

    def cover_preview(self, payload: Any) -> CollectionHttpResult:
        if not isinstance(payload, Mapping) or not payload.get("base_image"):
            return CollectionHttpResult({"error": "缺少基础图片数据"}, 400)
        try:
            return CollectionHttpResult({"image": self._gateway.cover_preview(payload)})
        except Exception:
            return CollectionHttpResult({"error": "生成预览失败"}, 500)


def _image_url(value: Any) -> str | None:
    if not value:
        return None
    path = str(value)
    if path.startswith(("http://", "https://")):
        return path
    return f"https://image.tmdb.org/t/p/original/{path.lstrip('/')}"


def _provider_payload(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    tmdb_id = str(row.get("tmdb_collection_id") or row.get("id") or "").strip()
    if not tmdb_id:
        return None
    backdrop = _image_url(row.get("backdrop_path"))
    return {
        "item_type": "BoxSet",
        "tmdb_id": tmdb_id,
        "name": row.get("name"),
        "overview": row.get("overview"),
        "actors_ready": False,
        "genres": [],
        "tags": [],
        "studios": [],
        "people": [],
        "images": {
            "primary": _image_url(row.get("poster_path")),
            "backdrop": backdrop,
            "logo": None,
            "thumb": backdrop,
        },
    }


def _custom_list_item(row: Mapping[str, Any]) -> dict[str, Any]:
    item = dict(row)
    definition = item.pop("definition_json", {})
    if isinstance(definition, str):
        try:
            definition = json.loads(definition)
            if isinstance(definition, str):
                definition = json.loads(definition)
        except (TypeError, json.JSONDecodeError):
            definition = {}
    item["definition"] = dict(definition) if isinstance(definition, Mapping) else {}
    item.pop("generated_media_info_json", None)
    timestamp = item.get("last_synced_at")
    if timestamp:
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except ValueError:
                timestamp = None
        if isinstance(timestamp, datetime):
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)
            item["last_synced_at"] = timestamp.astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")
    item.update(missing_count=0, health_status="ok")
    return item


def _ensure_list(value: Any, default: Any) -> list[dict[str, Any]]:
    source = value if value else default
    if isinstance(source, Mapping):
        converted: list[dict[str, Any]] = []
        for label, data in source.items():
            item: dict[str, Any] = {"label": str(label)}
            if isinstance(data, Mapping):
                item.update(dict(data))
            elif isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
                item["aliases"] = list(data)
            elif data is not None:
                item["value"] = data
            converted.append(item)
        return sorted(converted, key=lambda item: str(item.get("label") or ""))
    if isinstance(source, list):
        return [dict(item) for item in source if isinstance(item, Mapping)]
    return []


def _string_sequence(value: Any) -> Sequence[str] | None:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return None
    return [str(item) for item in value]


def _release_group_mapping(value: Any, default: Any) -> list[dict[str, Any]]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            value = None
    if isinstance(value, Mapping):
        values = [{"label": str(label), "aliases": _aliases(aliases)} for label, aliases in value.items() if str(label)]
        if values:
            return values
    if isinstance(value, list):
        values = []
        for item in value:
            if isinstance(item, str) and item.strip():
                values.append({"label": item.strip(), "aliases": []})
            elif isinstance(item, Mapping):
                label = str(item.get("label") or item.get("name") or "").strip()
                if label:
                    values.append({"label": label, "aliases": _aliases(item)})
        if values:
            return values
    if isinstance(default, Mapping):
        return [{"label": str(label), "aliases": _aliases(aliases)} for label, aliases in default.items()]
    return []


def _aliases(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        value = value.get("aliases") or value.get("en") or value.get("ids") or []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item or "").strip()]


def _studio_option(item: Mapping[str, Any]) -> dict[str, Any]:
    types: list[str] = []
    params: dict[str, str] = {}
    companies = item.get("company_ids") or item.get("ids")
    networks = item.get("network_ids")
    if companies:
        types.append("movie")
        params["with_companies"] = ",".join(str(value) for value in companies)
    if networks:
        types.append("tv")
        params["with_networks"] = ",".join(str(value) for value in networks)
    return {
        "label": item["label"],
        "value": item["label"],
        "types": types,
        "tmdb_params": params,
    }


__all__ = [
    "CollectionEffectsGateway",
    "CollectionGatewayConfigurationError",
    "CollectionHttpResult",
    "CollectionLocalGateway",
    "CollectionMediaProjection",
    "CollectionProviderNotFoundError",
    "CollectionRepository",
    "CollectionService",
    "CollectionSettingsStore",
    "CollectionUserProjection",
    "COVER_DEFAULTS",
]
