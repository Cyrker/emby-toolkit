from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.collection.router import router
from etk.domains.collection.service import (
    COVER_DEFAULTS,
    CollectionLocalGateway,
    CollectionMediaProjection,
    CollectionProviderNotFoundError,
    CollectionRepository,
    CollectionService,
    CollectionUserProjection,
)
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = json.loads((REPO_ROOT / "verification/fixtures/http/p8-collection-cover.json").read_text(encoding="utf-8"))
CONFIG_OWNERSHIP = REPO_ROOT / "verification/contracts/config-ownership.yaml"
SECRET_KEY = b"p8-collection-fixture-key"


class FakePermissions:
    def __init__(self) -> None:
        self.admins: set[str] = set()

    def is_admin(self, user_id: str) -> bool:
        return user_id in self.admins


class FakeSettings:
    def __init__(self) -> None:
        self.values: dict[str, Any] = {}
        self.loads: list[str] = []
        self.saves: list[tuple[str, Any]] = []
        self.fail_load_keys: set[str] = set()
        self.fail_save_keys: set[str] = set()

    def load(self, key: str) -> Any:
        self.loads.append(key)
        if key in self.fail_load_keys:
            raise RuntimeError("fixture settings load failure")
        return deepcopy(self.values.get(key))

    def save(self, key: str, value: Any) -> None:
        if key in self.fail_save_keys:
            raise RuntimeError("fixture settings save failure")
        copied = deepcopy(value)
        self.values[key] = copied
        self.saves.append((key, copied))

    def merge_mapping(
        self,
        key: str,
        updates: Mapping[str, Any],
        *,
        remove_keys: Sequence[str] = (),
    ) -> Mapping[str, Any]:
        current = self.values.get(key)
        merged = dict(current) if isinstance(current, Mapping) else {}
        for removed in remove_keys:
            merged.pop(removed, None)
        merged.update(updates)
        self.save(key, merged)
        return deepcopy(merged)


class FakeCollectionRepository:
    def __init__(self) -> None:
        self.custom: dict[int, dict[str, Any]] = {
            7: {
                "id": 7,
                "name": "Fixture Collection",
                "type": "filter",
                "definition_json": {"media_type": "Movie"},
                "status": "active",
                "allowed_user_ids": ["user-a"],
                "created_at": datetime(2026, 8, 27, tzinfo=UTC),
                "last_synced_at": datetime(2026, 8, 27, 1, tzinfo=UTC),
                "generated_media_info_json": [],
                "sort_order": 0,
            }
        }
        self.native: dict[str, dict[str, Any]] = {}
        self.expansion_inputs: list[Sequence[str] | None] = []
        self.order_updates: list[list[Any]] = []
        self.next_id = 8

    def list_custom(self) -> list[dict[str, Any]]:
        return [deepcopy(value) for value in self.custom.values()]

    def get_custom(self, collection_id: int) -> dict[str, Any] | None:
        value = self.custom.get(collection_id)
        return deepcopy(value) if value is not None else None

    def create_custom(
        self,
        *,
        name: str,
        collection_type: str,
        definition: Any,
        allowed_user_ids: Sequence[str] | None,
    ) -> dict[str, Any]:
        value = {
            "id": self.next_id,
            "name": name,
            "type": collection_type,
            "definition_json": deepcopy(definition),
            "status": "active",
            "allowed_user_ids": list(allowed_user_ids or ()),
            "created_at": datetime(2026, 8, 27, tzinfo=UTC),
            "last_synced_at": None,
            "generated_media_info_json": None,
            "sort_order": 0,
        }
        self.custom[self.next_id] = value
        self.next_id += 1
        return deepcopy(value)

    def update_custom(
        self,
        collection_id: int,
        *,
        name: str,
        collection_type: str,
        definition: Any,
        status: str,
        allowed_user_ids: Sequence[str] | None,
    ) -> dict[str, Any] | None:
        value = self.custom.get(collection_id)
        if value is None:
            return None
        value.update(
            name=name,
            type=collection_type,
            definition_json=deepcopy(definition),
            status=status,
            allowed_user_ids=list(allowed_user_ids or ()),
        )
        return deepcopy(value)

    def delete_custom(self, collection_id: int) -> dict[str, Any] | None:
        value = self.custom.pop(collection_id, None)
        return deepcopy(value) if value is not None else None

    def update_custom_order(self, ordered_ids: Sequence[Any]) -> None:
        self.order_updates.append(list(ordered_ids))

    def expand_template_users(self, selected_user_ids: Sequence[str] | None) -> list[str]:
        self.expansion_inputs.append(selected_user_ids)
        values = list(selected_user_ids or ())
        if "template-source" in values:
            values.append("template-child")
        return values

    @staticmethod
    def collection_user_options() -> list[dict[str, Any]]:
        return [{"label": "Fixture User", "value": "user-a", "is_template_source": False}]

    def list_native(self, *, active_only: bool = True) -> list[dict[str, Any]]:
        values = self.native.values()
        if active_only:
            values = (value for value in values if value.get("is_active"))
        return [deepcopy(value) for value in values]

    def get_native(self, tmdb_collection_id: str) -> dict[str, Any] | None:
        value = self.native.get(str(tmdb_collection_id))
        return deepcopy(value) if value is not None else None

    def upsert_native(
        self,
        details: Mapping[str, Any],
        existing_emby_id: str | None = None,
    ) -> dict[str, Any]:
        value = {
            "tmdb_collection_id": str(details["id"]),
            "emby_collection_id": existing_emby_id,
            "name": details.get("name"),
            "overview": details.get("overview"),
            "poster_path": details.get("poster_path"),
            "backdrop_path": details.get("backdrop_path"),
            "metadata_schema_version": 1,
            "is_active": bool(existing_emby_id),
            "last_checked_at": datetime(2026, 8, 27, tzinfo=UTC),
        }
        self.native[str(details["id"])] = value
        return deepcopy(value)

    def deactivate_native(self, emby_collection_id: str) -> bool:
        for value in self.native.values():
            if value.get("emby_collection_id") == emby_collection_id:
                value.update(emby_collection_id=None, is_active=False)
                return True
        return False


class FakeCollectionGateway:
    def __init__(self) -> None:
        self.missing_ids: set[int] = set()
        self.cleanup_requests: list[str] = []
        self.deleted_native: list[str] = []
        self.deleted_custom: list[int] = []

    @staticmethod
    def defaults() -> Mapping[str, Any]:
        return {
            "country_mapping": [{"label": "中国大陆", "value": "CN"}],
            "junk_file_patterns": ["sample"],
            "keyword_mapping": [{"label": "科幻", "ids": [1]}],
            "language_mapping": [{"label": "国语", "value": "zh"}],
            "rating_mapping": {"US": [{"code": "G", "label": "全年龄"}]},
            "rating_priority": ["ORIGIN", "US"],
            "release_group_mapping": {"Fixture": ["FIX"]},
            "studio_mapping": [{"label": "Fixture Studio", "company_ids": [1]}],
            "unified_ratings": ["全年龄", "未知"],
            "genre_translation": {"Sci-Fi & Fantasy": "科幻奇幻"},
        }

    def collection_details(self, tmdb_collection_id: int) -> Mapping[str, Any] | None:
        if tmdb_collection_id in self.missing_ids:
            raise CollectionProviderNotFoundError(tmdb_collection_id)
        return {
            "id": tmdb_collection_id,
            "name": "Fixture Native Collection",
            "overview": "Fixture overview",
            "poster_path": "/poster.jpg",
            "backdrop_path": "/backdrop.jpg",
            "parts": [{"id": 11}],
        }

    @staticmethod
    def search_collections(query: str) -> Sequence[Mapping[str, Any]]:
        return [{"id": 101, "name": f"{query} Collection", "poster_path": "/provider.jpg"}]

    @staticmethod
    def archive_metadata_images(payload: Mapping[str, Any], host_url: str) -> None:
        del payload, host_url

    def schedule_missing_collection_cleanup(self, tmdb_collection_id: str) -> None:
        self.cleanup_requests.append(tmdb_collection_id)

    @staticmethod
    def collection_status() -> Sequence[Mapping[str, Any]]:
        return [
            {
                "tmdb_collection_id": "101",
                "name": "Fixture Native Collection",
                "last_checked_at": datetime(2026, 8, 27, tzinfo=UTC),
            }
        ]

    def delete_native_collection(self, emby_collection_id: str) -> bool:
        self.deleted_native.append(emby_collection_id)
        return True

    def delete_custom_collection_effect(self, collection: Mapping[str, Any]) -> None:
        self.deleted_custom.append(int(collection["id"]))

    @staticmethod
    def cleanup_custom_placeholders(collection: Mapping[str, Any]) -> None:
        del collection

    @staticmethod
    def fix_custom_match(collection_id: int, payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
        return {"collection_id": collection_id, "tmdb_id": str(payload["new_tmdb_id"])}

    @staticmethod
    def emby_libraries(*, cover: bool = False) -> Sequence[Mapping[str, Any]] | None:
        del cover
        return [
            {"Name": "Movies", "Id": "library-movie", "CollectionType": "movies"},
            {"Name": "Folders", "Id": "library-folder", "Type": "CollectionFolder"},
            {"Name": "Ignored", "Id": "library-ignored", "CollectionType": "books"},
        ]

    @staticmethod
    def tmdb_movie_genres() -> Sequence[Mapping[str, Any]] | None:
        return [{"id": 1, "name": "Sci-Fi & Fantasy"}]

    @staticmethod
    def tmdb_tv_genres() -> Sequence[Mapping[str, Any]] | None:
        return [{"id": 2, "name": "Drama"}]

    @staticmethod
    def search_tmdb_companies(query: str, search_type: str) -> Sequence[Mapping[str, Any]]:
        return [{"id": 1, "name": query, "type": search_type}]

    @staticmethod
    def search_tmdb_people(query: str) -> Sequence[Mapping[str, Any]]:
        return [{"id": 2, "name": query}]

    @staticmethod
    def local_filter_values(kind: str, query: str = "") -> Sequence[Any]:
        del query
        return [f"fixture-{kind}"]

    @staticmethod
    def cover_preview(payload: Mapping[str, Any]) -> str:
        del payload
        return "data:image/png;base64,fixture"


class FakeCollectionMediaProjection:
    @staticmethod
    def custom_status(collection: Mapping[str, Any]) -> dict[str, Any]:
        value = deepcopy(dict(collection))
        value.update(media_items=[], missing_count=0, health_status="dynamic")
        return value

    @staticmethod
    def collection_status(collections: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        del collections
        return [
            {
                "tmdb_collection_id": "101",
                "name": "Fixture Native Collection",
                "last_checked_at": datetime(2026, 8, 27, tzinfo=UTC),
            }
        ]

    @staticmethod
    def local_filter_values(kind: str, query: str = "") -> Sequence[Any]:
        del query
        return [f"fixture-{kind}"]


def _build_app() -> tuple[
    FastAPI,
    FakePermissions,
    FakeSettings,
    FakeCollectionRepository,
    FakeCollectionGateway,
]:
    permissions = FakePermissions()
    settings = FakeSettings()
    repository = FakeCollectionRepository()
    gateway = FakeCollectionGateway()
    service = CollectionService(
        cast(CollectionRepository, repository),
        settings,
        cast(CollectionLocalGateway, gateway),
        cast(CollectionUserProjection, repository),
        cast(CollectionMediaProjection, FakeCollectionMediaProjection()),
    )
    container = AppContainer(
        Settings(auth_enabled=True),
        repository_providers={"identity": lambda _: permissions},
    )
    app = FastAPI()
    app.state.container = container
    app.state.collection_service = service
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return app, permissions, settings, repository, gateway


def _set_user(client: TestClient, user_id: str) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set("session", codec.dumps({"emby_user_id": user_id}))


def test_fixture_freezes_48_local_routes_request_bodies_and_nonempty_success_schemas() -> None:
    app, *_ = _build_app()
    paths = app.openapi()["paths"]
    expected = {(route["method"].lower(), route["path"]) for route in FIXTURE["routes"]}
    actual = {(method, path) for path, operations in paths.items() for method in operations}

    assert len(expected) == 48
    assert actual == expected
    for route in FIXTURE["routes"]:
        operation = paths[route["path"]][route["method"].lower()]
        assert "422" not in {str(value) for value in route["response_statuses"]}
        assert set(operation["responses"]) - {"422"} == {str(value) for value in route["response_statuses"]}
        assert ("requestBody" in operation) is route["request_body"]
        success = operation["responses"][str(next(code for code in route["response_statuses"] if code < 300))]
        assert success["content"]["application/json"]["schema"]


def test_public_login_and_admin_permission_boundaries_are_preserved() -> None:
    app, permissions, *_ = _build_app()
    with TestClient(app) as client:
        public = client.get("/api/custom_collections/config/keyword_mapping")
        login_denied = client.get("/api/custom_collections/config/keywords")
        admin_denied = client.get("/api/custom_collections")
        _set_user(client, "user")
        logged_in = client.get("/api/custom_collections/config/keywords")
        still_not_admin = client.get("/api/custom_collections")
        permissions.admins.add("user")
        admin = client.get("/api/custom_collections")

    assert public.status_code == 200
    assert public.json() == [{"label": "科幻", "ids": [1]}]
    assert login_denied.status_code == 401
    assert admin_denied.status_code == 403
    assert logged_in.status_code == 200
    assert logged_in.json() == [{"label": "科幻", "value": "科幻"}]
    assert still_not_admin.status_code == 403
    assert admin.status_code == 200


def test_cover_settings_preserve_defaults_pass_through_transport_and_ownership() -> None:
    app, permissions, settings, *_ = _build_app()
    settings.values["cover_generator_config"] = {
        "enabled": True,
        "cover_style": "fixture-style",
        "opaque": {"preserved": True},
    }
    with TestClient(app) as client:
        denied_get = client.get("/api/config/cover_generator")
        denied_post = client.post(
            "/api/config/cover_generator",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
        assert settings.loads == []
        assert settings.saves == []

        permissions.admins.add("admin")
        _set_user(client, "admin")
        loaded = client.get("/api/config/cover_generator")
        first_defaults = app.state.collection_service.cover_config().payload
        first_defaults["exclude_libraries"].append("mutated")
        assert app.state.collection_service.cover_config().payload["exclude_libraries"] == []
        saved = client.post(
            "/api/config/cover_generator",
            json={"enabled": False, "opaque": ["kept"]},
        )
        saved_list = client.post("/api/config/cover_generator", json=["list", 7])
        malformed = client.post(
            "/api/config/cover_generator",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
        unsupported = client.post(
            "/api/config/cover_generator",
            content=b"{}",
            headers={"Content-Type": "text/plain"},
        )

    assert denied_get.status_code == 403
    assert denied_post.status_code == 403
    assert loaded.status_code == 200
    assert loaded.json() == {
        **COVER_DEFAULTS,
        "enabled": True,
        "cover_style": "fixture-style",
        "opaque": {"preserved": True},
    }
    assert saved.json() == {"message": "配置已成功保存！"}
    assert saved_list.json() == {"message": "配置已成功保存！"}
    assert settings.saves == [
        ("cover_generator_config", {"enabled": False, "opaque": ["kept"]}),
        ("cover_generator_config", ["list", 7]),
    ]
    assert malformed.status_code == 500
    assert malformed.json() == {"error": "保存配置失败"}
    assert unsupported.status_code == 415

    manifest = json.loads(CONFIG_OWNERSHIP.read_text(encoding="utf-8"))
    contract = next(item for item in manifest["standalone_app_settings"] if item["key"] == "cover_generator_config")
    assert contract["status"] == "new"
    assert contract["new_owner"] == "etk.domains.system.repository.SqlAlchemyAppSettingsRepository"
    assert contract["validation_owner"] == "collection/cover-settings"
    assert contract["fixture"] == "verification/fixtures/http/p8-collection-cover.json"


def test_cover_settings_repository_failures_preserve_500_envelopes() -> None:
    app, permissions, settings, *_ = _build_app()
    permissions.admins.add("admin")
    with TestClient(app) as client:
        _set_user(client, "admin")
        settings.fail_load_keys.add("cover_generator_config")
        read = client.get("/api/config/cover_generator")
        settings.fail_load_keys.clear()
        settings.fail_save_keys.add("cover_generator_config")
        saved = client.post("/api/config/cover_generator", json={"enabled": True})

    assert read.status_code == 500
    assert read.json() == {"error": "读取配置失败"}
    assert saved.status_code == 500
    assert saved.json() == {"error": "保存配置失败"}


def test_cover_settings_corrupted_truthy_non_object_preserves_legacy_read_failure() -> None:
    app, permissions, settings, *_ = _build_app()
    permissions.admins.add("admin")
    with TestClient(app) as client:
        _set_user(client, "admin")
        settings.values["cover_generator_config"] = ["corrupt"]
        corrupt = client.get("/api/config/cover_generator")
        settings.values["cover_generator_config"] = []
        empty = client.get("/api/config/cover_generator")

    assert corrupt.status_code == 500
    assert corrupt.json() == {"error": "读取配置失败"}
    assert empty.status_code == 200
    assert empty.json() == COVER_DEFAULTS


def test_custom_create_does_not_expand_string_as_character_ids_and_serializes_datetime() -> None:
    app, permissions, _settings, repository, _gateway = _build_app()
    permissions.admins.add("admin")
    with TestClient(app) as client:
        _set_user(client, "admin")
        response = client.post(
            "/api/custom_collections",
            json={
                "name": "String Users",
                "type": "filter",
                "definition": {"media_type": "Movie"},
                "allowed_user_ids": "user-a",
            },
        )

    assert response.status_code == 201
    assert repository.expansion_inputs == [None]
    assert response.json()["allowed_user_ids"] == []
    assert response.json()["created_at"] == "Thu, 27 Aug 2026 00:00:00 GMT"


def test_custom_create_expands_template_users_and_empty_mapping_uses_defaults() -> None:
    app, permissions, settings, repository, _gateway = _build_app()
    permissions.admins.add("admin")
    settings.values["keyword_mapping"] = []
    with TestClient(app) as client:
        _set_user(client, "admin")
        created = client.post(
            "/api/custom_collections",
            json={
                "name": "Template Users",
                "type": "filter",
                "definition": {"media_type": "Series"},
                "allowed_user_ids": ["template-source"],
            },
        )
        mapping = client.get("/api/custom_collections/config/keyword_mapping")

    assert created.status_code == 201
    assert repository.expansion_inputs == [["template-source"]]
    assert created.json()["allowed_user_ids"] == ["template-source", "template-child"]
    assert mapping.json() == [{"label": "科幻", "ids": [1]}]


def test_datetime_status_is_legacy_compatible() -> None:
    app, permissions, _settings, _repository, _gateway = _build_app()
    permissions.admins.add("admin")
    with TestClient(app) as client:
        _set_user(client, "admin")
        status = client.get("/api/collections/status")

    assert status.status_code == 200
    assert status.json()[0]["last_checked_at"] == "Thu, 27 Aug 2026 00:00:00 GMT"


def test_manual_json_transport_preserves_400_415_and_never_returns_422() -> None:
    app, permissions, *_ = _build_app()
    permissions.admins.add("admin")
    with TestClient(app) as client:
        _set_user(client, "admin")
        malformed = client.post(
            "/api/custom_collections",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
        unsupported = client.post(
            "/api/custom_collections",
            content=b"{}",
            headers={"Content-Type": "text/plain"},
        )
        invalid_path = client.get("/api/custom_collections/not-an-int/status")

    assert malformed.status_code == 400 and malformed.headers["content-type"].startswith("text/html")
    assert unsupported.status_code == 415 and unsupported.headers["content-type"].startswith("text/html")
    assert invalid_path.status_code == 404 and invalid_path.headers["content-type"].startswith("text/html")
    assert all(response.status_code != 422 for response in (malformed, unsupported, invalid_path))
