from __future__ import annotations

import base64
from collections.abc import Callable, Mapping, Sequence
from io import BytesIO
from pathlib import Path
from typing import Any

from PIL import Image

from etk.platform import Settings

from .service import CollectionGatewayConfigurationError, CollectionProviderNotFoundError


class LegacyCollectionGateway:
    """P11 seam around established provider and rendering implementations."""

    def __init__(self, settings: Callable[[], Settings]) -> None:
        self._settings = settings

    def defaults(self) -> Mapping[str, Any]:
        from utils import (
            DEFAULT_COUNTRY_MAPPING,
            DEFAULT_JUNK_FILE_PATTERNS,
            DEFAULT_KEYWORD_MAPPING,
            DEFAULT_LANGUAGE_MAPPING,
            DEFAULT_RATING_MAPPING,
            DEFAULT_RATING_PRIORITY,
            DEFAULT_RELEASE_GROUP_MAPPING,
            DEFAULT_STUDIO_MAPPING,
            GENRE_TRANSLATION_PATCH,
            UNIFIED_RATING_CATEGORIES,
        )

        return {
            "country_mapping": DEFAULT_COUNTRY_MAPPING,
            "junk_file_patterns": DEFAULT_JUNK_FILE_PATTERNS,
            "keyword_mapping": DEFAULT_KEYWORD_MAPPING,
            "language_mapping": DEFAULT_LANGUAGE_MAPPING,
            "rating_mapping": DEFAULT_RATING_MAPPING,
            "rating_priority": DEFAULT_RATING_PRIORITY,
            "release_group_mapping": DEFAULT_RELEASE_GROUP_MAPPING,
            "studio_mapping": DEFAULT_STUDIO_MAPPING,
            "unified_ratings": UNIFIED_RATING_CATEGORIES,
            "genre_translation": GENRE_TRANSLATION_PATCH,
        }

    def collection_details(self, tmdb_collection_id: int) -> Mapping[str, Any] | None:
        from handler import tmdb

        try:
            return tmdb.get_collection_details(
                tmdb_collection_id,
                self._settings().tmdb_api_key.get_secret_value(),
                raise_not_found=True,
            )
        except tmdb.TmdbNotFoundError as exc:
            raise CollectionProviderNotFoundError(tmdb_collection_id) from exc

    def search_collections(self, query: str) -> Sequence[Mapping[str, Any]]:
        from handler import tmdb

        return tmdb.search_collections(query, self._settings().tmdb_api_key.get_secret_value()) or []

    @staticmethod
    def archive_metadata_images(payload: Mapping[str, Any], host_url: str) -> None:
        from handler.media_image_cache import archive_metadata_images

        archive_metadata_images(dict(payload), host_url, archive_sources=True)

    @staticmethod
    def schedule_missing_collection_cleanup(tmdb_collection_id: str) -> None:
        from database import tmdb_collection_db
        from routes.tmdb_collections import _schedule_missing_collection_cleanup

        value = str(tmdb_collection_id)
        row = tmdb_collection_db.get_native_collection_by_tmdb_id(value)
        _schedule_missing_collection_cleanup(value, row)

    @staticmethod
    def collection_status() -> Sequence[Mapping[str, Any]]:
        from handler.tmdb_collections import assemble_all_collection_details

        return assemble_all_collection_details()

    def delete_native_collection(self, emby_collection_id: str) -> bool:
        from handler import emby

        settings = self._settings()
        base_url = settings.emby_server_url
        api_key = settings.emby_api_key.get_secret_value()
        user_id = settings.emby_user_id
        if not all((base_url, api_key, user_id)):
            raise CollectionGatewayConfigurationError("Emby 配置不完整，无法执行删除操作")
        from extensions import DELETING_COLLECTIONS

        DELETING_COLLECTIONS.add_for(emby_collection_id, 10)
        emby.empty_collection_in_emby(emby_collection_id, base_url, api_key, user_id)
        return bool(emby.delete_item(emby_collection_id, base_url, api_key, user_id))

    def delete_custom_collection_effect(self, collection: Mapping[str, Any]) -> None:
        from extensions import DELETING_COLLECTIONS
        from handler import emby

        emby_id = str(collection.get("emby_collection_id") or "")
        if emby_id:
            settings = self._settings()
            DELETING_COLLECTIONS.add_for(emby_id, 20)
            emby.empty_collection_in_emby(
                collection_id=emby_id,
                base_url=settings.emby_server_url,
                api_key=settings.emby_api_key.get_secret_value(),
                user_id=settings.emby_user_id,
            )

    @staticmethod
    def cleanup_custom_placeholders(collection: Mapping[str, Any]) -> None:
        from handler.poster_generator import cleanup_placeholder

        items = collection.get("generated_media_info_json")
        if isinstance(items, list):
            for item in items:
                if isinstance(item, Mapping) and item.get("tmdb_id"):
                    cleanup_placeholder(str(item["tmdb_id"]))

    @staticmethod
    def fix_custom_match(collection_id: int, payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
        from database.custom_collection_db import apply_and_persist_media_correction

        return apply_and_persist_media_correction(
            collection_id=collection_id,
            old_tmdb_id=str(payload.get("old_tmdb_id")) if payload.get("old_tmdb_id") else None,
            new_tmdb_id=str(payload["new_tmdb_id"]),
            season_number=payload.get("season_number"),
            old_title=payload.get("old_title"),
        )

    def emby_libraries(self, *, cover: bool = False) -> Sequence[Mapping[str, Any]] | None:
        del cover
        from handler import emby

        settings = self._settings()
        if not all((settings.emby_server_url, settings.emby_api_key.get_secret_value(), settings.emby_user_id)):
            raise CollectionGatewayConfigurationError("Emby 服务器配置不完整")
        return emby.get_emby_libraries(
            settings.emby_server_url,
            settings.emby_api_key.get_secret_value(),
            settings.emby_user_id,
        )

    def tmdb_movie_genres(self) -> Sequence[Mapping[str, Any]] | None:
        from handler.tmdb import get_movie_genres_tmdb

        api_key = self._settings().tmdb_api_key.get_secret_value()
        if not api_key:
            raise CollectionGatewayConfigurationError("TMDb API Key 未配置")
        return get_movie_genres_tmdb(api_key)

    def tmdb_tv_genres(self) -> Sequence[Mapping[str, Any]] | None:
        from handler.tmdb import get_tv_genres_tmdb

        api_key = self._settings().tmdb_api_key.get_secret_value()
        if not api_key:
            raise CollectionGatewayConfigurationError("TMDb API Key 未配置")
        return get_tv_genres_tmdb(api_key)

    def search_tmdb_companies(self, query: str, search_type: str) -> Sequence[Mapping[str, Any]]:
        from handler.tmdb import get_tv_details, search_companies_tmdb, search_tv_tmdb

        api_key = self._settings().tmdb_api_key.get_secret_value()
        if search_type != "network":
            return search_companies_tmdb(api_key, query) or []
        search = search_tv_tmdb(api_key, {"query": query}) or {}
        found: dict[Any, dict[str, Any]] = {}
        for show in list(search.get("results") or [])[:3]:
            details = get_tv_details(show["id"], api_key)
            for network in (details or {}).get("networks", []):
                if network.get("id") not in found:
                    found[network.get("id")] = {
                        "id": network.get("id"),
                        "name": network.get("name"),
                        "logo_path": network.get("logo_path"),
                        "origin_country": network.get("origin_country"),
                        "source_show": show.get("name"),
                    }
        return list(found.values())

    def search_tmdb_people(self, query: str) -> Sequence[Mapping[str, Any]]:
        from handler.tmdb import search_person_tmdb

        people = search_person_tmdb(query, self._settings().tmdb_api_key.get_secret_value()) or []
        return [
            {
                "id": person.get("id"),
                "name": person.get("name"),
                "profile_path": person.get("profile_path"),
                "department": person.get("known_for_department"),
                "known_for": "、".join(
                    filter(
                        None,
                        [item.get("title") or item.get("name", "") for item in person.get("known_for", [])],
                    )
                ),
            }
            for person in people
        ]

    @staticmethod
    def local_filter_values(kind: str, query: str = "") -> Sequence[Any]:
        from database import custom_collection_db

        handlers = {
            "tags": custom_collection_db.get_unique_tags,
            "movie_genres": custom_collection_db.get_movie_genres,
            "tv_genres": custom_collection_db.get_tv_genres,
            "studios": lambda: custom_collection_db.search_unique_studios(query),
        }
        return handlers[kind]()

    @staticmethod
    def cover_preview(payload: Mapping[str, Any]) -> str:
        import config_manager
        from services.cover_generator.styles.badge_drawer import draw_badge

        encoded = str(payload["base_image"])
        if "," in encoded:
            encoded = encoded.split(",", 1)[1]
        image = Image.open(BytesIO(base64.b64decode(encoded))).convert("RGBA")
        font_path = Path(config_manager.PERSISTENT_DATA_PATH) / "cover_generator" / "fonts" / "en_font.ttf"
        if not font_path.is_file():
            font_path = Path(__file__).resolve().parents[4] / "fonts" / "en_font.ttf"
        rendered = draw_badge(
            image=image,
            item_count=payload.get("item_count", 99),
            font_path=str(font_path),
            style=payload.get("badge_style", "badge"),
            size_ratio=payload.get("badge_size_ratio", 0.12),
        )
        output = BytesIO()
        rendered.save(output, format="PNG")
        return "data:image/png;base64," + base64.b64encode(output.getvalue()).decode("utf-8")


__all__ = ["LegacyCollectionGateway"]
