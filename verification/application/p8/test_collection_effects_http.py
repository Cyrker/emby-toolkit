from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.collection.effects import (
    CollectionEffectSubmission,
    CollectionRemoteIndeterminate,
)
from etk.domains.collection.operations import (
    CollectionOperation,
    CollectionOperationConflictError,
    CollectionOperationKind,
    CollectionOperationScopeBusyError,
    CollectionOperationStateError,
    CollectionOperationStatus,
    CollectionOperationStep,
)
from etk.domains.collection.repository import (
    CollectionCorrectionSnapshot,
    CollectionMatchConflictError,
    CustomMatchCandidate,
)
from etk.domains.collection.router import effects_router
from etk.domains.collection.service import (
    CollectionEffectGateway,
    CollectionLocalGateway,
    CollectionMediaProjection,
    CollectionProviderNotFoundError,
    CollectionProviderUnavailableError,
    CollectionQueryGateway,
    CollectionRepository,
    CollectionService,
    CollectionUserProjection,
)
from etk.domains.media.subscription_projection import SubscriptionMediaKey
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

SECRET_KEY = b"p8-collection-effects-fixture"
EFFECT_ID = UUID("018f24aa-5be2-7f3a-b909-3f8d5e2f8f01")


class FakePermissions:
    def is_admin(self, user_id: str) -> bool:
        return user_id == "admin"

    @staticmethod
    def is_active(user_id: str) -> bool:
        return bool(user_id)


class FakeRepository:
    def __init__(self) -> None:
        self.native: dict[str, dict[str, Any]] = {}
        self.inspected: list[tuple[int, str | None, str | None]] = []
        self.applied: list[tuple[CustomMatchCandidate, CollectionCorrectionSnapshot]] = []
        self.candidate_item_type = "Series"
        self.conflict = False

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
        }
        self.native[str(details["id"])] = value
        return deepcopy(value)

    def inspect_custom_match(
        self,
        collection_id: int,
        *,
        old_tmdb_id: str | None,
        old_title: str | None,
    ) -> CustomMatchCandidate | None:
        self.inspected.append((collection_id, old_tmdb_id, old_title))
        return CustomMatchCandidate(
            collection_id=collection_id,
            collection_name="Fixture Custom Collection",
            old_tmdb_id=old_tmdb_id,
            old_title=old_title,
            item_type=self.candidate_item_type,
            season_number=None,
            collection_digest=b"fixture",
        )

    def apply_custom_match(
        self,
        candidate: CustomMatchCandidate,
        snapshot: CollectionCorrectionSnapshot,
    ) -> dict[str, Any]:
        if self.conflict:
            raise CollectionMatchConflictError("fixture drift")
        self.applied.append((candidate, snapshot))
        return dict(snapshot.corrected_item)

    @staticmethod
    def list_native(*, active_only: bool = True) -> list[dict[str, Any]]:
        del active_only
        return []


class FakeQueryGateway:
    def __init__(self) -> None:
        self.missing = False
        self.unavailable = False
        self.correction_missing = False
        self.correction_unavailable = False
        self.correction_calls: list[tuple[Any, ...]] = []

    def collection_details(self, tmdb_collection_id: int) -> Mapping[str, Any] | None:
        if self.missing:
            raise CollectionProviderNotFoundError(tmdb_collection_id)
        if self.unavailable:
            raise CollectionProviderUnavailableError("TMDb transport failed")
        return {
            "id": tmdb_collection_id,
            "name": "Fixture Collection",
            "poster_path": "/poster.jpg",
            "backdrop_path": "/backdrop.jpg",
            "parts": [],
        }

    @staticmethod
    def search_collections(query: str) -> Sequence[Mapping[str, Any]]:
        return ({"id": 12, "name": query},)

    @staticmethod
    def emby_libraries(*, cover: bool = False) -> Sequence[Mapping[str, Any]]:
        del cover
        return ()

    @staticmethod
    def tmdb_movie_genres() -> Sequence[Mapping[str, Any]]:
        return ()

    @staticmethod
    def tmdb_tv_genres() -> Sequence[Mapping[str, Any]]:
        return ()

    @staticmethod
    def search_tmdb_companies(query: str, search_type: str) -> Sequence[Mapping[str, Any]]:
        del query, search_type
        return ()

    @staticmethod
    def search_tmdb_people(query: str) -> Sequence[Mapping[str, Any]]:
        del query
        return ()

    def movie_details(self, tmdb_id: int) -> Mapping[str, Any]:
        self.correction_calls.append(("movie", tmdb_id))
        self._correction_error(tmdb_id)
        return {
            "id": tmdb_id,
            "title": "Fixture Movie",
            "original_title": "Fixture Movie Original",
            "release_date": "2020-01-01",
            "poster_path": "/movie.jpg",
            "backdrop_path": "/movie-backdrop.jpg",
            "overview": "Movie overview",
        }

    def series_details(self, tmdb_id: int) -> Mapping[str, Any]:
        self.correction_calls.append(("series", tmdb_id))
        self._correction_error(tmdb_id)
        return {
            "id": tmdb_id,
            "name": "Fixture Series",
            "original_name": "Fixture Series Original",
            "first_air_date": "2020-01-01",
            "poster_path": "/series.jpg",
            "backdrop_path": "/series-backdrop.jpg",
            "overview": "Series overview",
        }

    def season_details(self, series_tmdb_id: int, season_number: int) -> Mapping[str, Any]:
        self.correction_calls.append(("season", series_tmdb_id, season_number))
        self._correction_error(series_tmdb_id)
        return {
            "id": series_tmdb_id * 100 + season_number,
            "season_number": season_number,
            "name": f"Season {season_number}",
            "air_date": "2099-01-01",
            "poster_path": "/season.jpg",
            "overview": "Season overview",
        }

    def _correction_error(self, identity: int) -> None:
        if self.correction_missing:
            raise CollectionProviderNotFoundError(identity)
        if self.correction_unavailable:
            raise CollectionProviderUnavailableError("TMDb correction failed")


class FakeEffectGateway:
    def __init__(self) -> None:
        self.missing: list[str] = []
        self.images: list[dict[str, Any]] = []
        self.native: list[tuple[str, str, str | None]] = []
        self.custom: list[tuple[int, str, str | None]] = []
        self.native_error: BaseException | None = None
        self.custom_error: BaseException | None = None

    @staticmethod
    def _submission() -> CollectionEffectSubmission:
        return CollectionEffectSubmission(EFFECT_ID, "pending", True)

    def stage_native_delete(
        self,
        emby_collection_id: str,
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> CollectionEffectSubmission:
        if self.native_error is not None:
            raise self.native_error
        self.native.append((emby_collection_id, principal_id, idempotency_key))
        return self._submission()

    def stage_custom_delete(
        self,
        collection_id: int,
        *,
        principal_id: str,
        idempotency_key: str | None,
    ) -> CollectionEffectSubmission | None:
        if self.custom_error is not None:
            raise self.custom_error
        self.custom.append((collection_id, principal_id, idempotency_key))
        return self._submission()

    def stage_missing_cleanup(self, tmdb_collection_id: str) -> CollectionEffectSubmission:
        self.missing.append(tmdb_collection_id)
        return self._submission()

    def stage_image_archives(self, payload: Mapping[str, Any]) -> tuple[CollectionEffectSubmission, ...]:
        self.images.append(deepcopy(dict(payload)))
        return (self._submission(),)


class FakeLocalGateway:
    @staticmethod
    def defaults() -> Mapping[str, Any]:
        return {"genre_translation": {}}

    @staticmethod
    def cover_preview(payload: Mapping[str, Any]) -> str:
        del payload
        return ""


class FakeProjection:
    @staticmethod
    def custom_status(collection: Mapping[str, Any]) -> dict[str, Any]:
        return dict(collection)

    @staticmethod
    def collection_status(collections: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        return [dict(value) for value in collections]

    @staticmethod
    def local_filter_values(kind: str, query: str = "") -> Sequence[Any]:
        del kind, query
        return ()


class FakeSettingsStore:
    @staticmethod
    def load(key: str) -> Any:
        del key
        return None

    @staticmethod
    def save(key: str, value: Any) -> None:
        del key, value

    @staticmethod
    def merge_mapping(
        key: str,
        updates: Mapping[str, Any],
        *,
        remove_keys: Sequence[str] = (),
    ) -> Mapping[str, Any]:
        del key, remove_keys
        return dict(updates)


class FakeOperationRepository:
    def __init__(self, operation: CollectionOperation | None) -> None:
        self.operation = operation
        self.read_ids: list[UUID] = []

    def read(self, operation_id: UUID) -> CollectionOperation | None:
        self.read_ids.append(operation_id)
        if self.operation is None or self.operation.id != operation_id:
            return None
        return self.operation


class FakeOperationReconciler:
    def __init__(self, result: CollectionOperation | None) -> None:
        self.result = result
        self.error: BaseException | None = None
        self.calls: list[tuple[UUID, int, str]] = []

    def reconcile(
        self,
        operation_id: UUID,
        *,
        expected_state_revision: int,
        resolved_by: str,
    ) -> CollectionOperation | None:
        self.calls.append((operation_id, expected_state_revision, resolved_by))
        if self.error is not None:
            raise self.error
        return self.result


def _operation(
    status: CollectionOperationStatus,
    *,
    state_revision: int = 7,
    result: Mapping[str, Any] | None = None,
    error: Mapping[str, Any] | None = None,
) -> CollectionOperation:
    now = datetime.now(UTC)
    return CollectionOperation(
        id=EFFECT_ID,
        sequence=1,
        operation_key="a" * 64,
        request_hash=b"b" * 32,
        kind=CollectionOperationKind.NATIVE_DELETE,
        scope_key="emby-collection:fixture",
        request={"emby_collection_id": "fixture"},
        status=status,
        current_step=CollectionOperationStep.DELETE_SHELL,
        state_revision=state_revision,
        attempts=2,
        available_at=now,
        claimed_by=None,
        claim_token=None,
        claimed_at=None,
        lease_expires_at=None,
        step_started_at=now,
        state={},
        result=result,
        error=error,
        finished_at=now if status.terminal else None,
        created_at=now,
        updated_at=now,
    )


def _build_app(
    *,
    operation_repository: FakeOperationRepository | None = None,
    operation_reconciler: FakeOperationReconciler | None = None,
) -> tuple[FastAPI, FakeRepository, FakeQueryGateway, FakeEffectGateway]:
    repository = FakeRepository()
    queries = FakeQueryGateway()
    effects = FakeEffectGateway()
    service = CollectionService(
        cast(CollectionRepository, repository),
        FakeSettingsStore(),
        cast(CollectionLocalGateway, FakeLocalGateway()),
        cast(CollectionUserProjection, object()),
        cast(CollectionMediaProjection, FakeProjection()),
        query_gateway=cast(CollectionQueryGateway, queries),
        effect_gateway=cast(CollectionEffectGateway, effects),
        operation_repository=cast(Any, operation_repository),
        operation_reconciler=cast(Any, operation_reconciler),
    )
    container = AppContainer(
        Settings(auth_enabled=True),
        repository_providers={"identity": lambda _: FakePermissions()},
    )
    app = FastAPI()
    app.state.container = container
    app.state.collection_service = service
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(effects_router)
    return app, repository, queries, effects


def _admin(client: TestClient) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set("session", codec.dumps({"emby_user_id": "admin"}))


def test_authoritative_tmdb_404_stages_cleanup_but_transport_failure_does_not() -> None:
    app, _repository, queries, effects = _build_app()
    with TestClient(app) as client:
        queries.missing = True
        missing = client.get("/api/collections/provider/metadata/101")
        queries.missing = False
        queries.unavailable = True
        unavailable = client.get("/api/collections/provider/metadata/102")

    assert missing.status_code == 410
    assert missing.json()["effect_id"] == str(EFFECT_ID)
    assert effects.missing == ["101"]
    assert unavailable.status_code == 500
    assert unavailable.json() == {"error": "TMDb transport failed"}
    assert effects.missing == ["101"]


def test_provider_metadata_stages_archive_without_executing_provider_mutation() -> None:
    app, repository, _queries, effects = _build_app()
    with TestClient(app) as client:
        response = client.get("/api/collections/provider/metadata/101")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert repository.native["101"]["metadata_schema_version"] == 1
    assert effects.images[0]["images"]["primary"].endswith("/poster.jpg")


def test_delete_routes_only_stage_durable_effects_and_forward_idempotency_key() -> None:
    app, _repository, _queries, effects = _build_app()
    with TestClient(app) as client:
        _admin(client)
        native = client.delete(
            "/api/collections/emby-boxset",
            headers={"Idempotency-Key": "native-delete"},
        )
        custom = client.delete(
            "/api/custom_collections/7",
            headers={"Idempotency-Key": "custom-delete"},
        )

    assert native.status_code == 202
    assert custom.status_code == 202
    assert effects.native == [("emby-boxset", "collection-admin:admin", "native-delete")]
    assert effects.custom == [(7, "collection-admin:admin", "custom-delete")]
    assert native.json()["effect_status"] == "pending"
    assert custom.json()["effect_id"] == str(EFFECT_ID)
    assert native.headers["location"] == f"/api/collection-operations/{EFFECT_ID}/status"
    assert custom.json()["status_url"] == f"/api/collection-operations/{EFFECT_ID}/status"


def test_delete_routes_map_idempotency_conflict_and_busy_scope_to_http_409() -> None:
    app, _repository, _queries, effects = _build_app()
    with TestClient(app) as client:
        _admin(client)
        effects.native_error = CollectionOperationConflictError("fixture conflict")
        conflict = client.delete(
            "/api/collections/emby-boxset",
            headers={"Idempotency-Key": "duplicate"},
        )
        effects.native_error = None
        effects.custom_error = CollectionOperationScopeBusyError("fixture busy")
        busy = client.delete(
            "/api/custom_collections/7",
            headers={"Idempotency-Key": "busy"},
        )

    assert conflict.status_code == 409
    assert conflict.json() == {"error": "Idempotency-Key 已用于其他合集删除请求"}
    assert busy.status_code == 409
    assert busy.json() == {"error": "该合集已有未完成的操作"}


def test_collection_operation_status_maps_success_missing_and_invalid_identity() -> None:
    operation = _operation(CollectionOperationStatus.AWAITING_READBACK)
    operation_repository = FakeOperationRepository(operation)
    app, _repository, _queries, _effects = _build_app(operation_repository=operation_repository)
    with TestClient(app) as client:
        _admin(client)
        found = client.get(f"/api/collection-operations/{EFFECT_ID}/status")
        operation_repository.operation = None
        missing = client.get(f"/api/collection-operations/{EFFECT_ID}/status")
        invalid = client.get("/api/collection-operations/not-a-uuid/status")

    assert found.status_code == 200
    assert found.json()["status"] == "awaiting_readback"
    assert found.json()["state_revision"] == 7
    assert missing.status_code == 404
    assert invalid.status_code == 400


def test_collection_operation_reconciliation_maps_observations_and_failures() -> None:
    reconciler = FakeOperationReconciler(
        _operation(
            CollectionOperationStatus.CONFIRMED,
            state_revision=8,
            result={"provider_present": False},
        )
    )
    app, _repository, _queries, _effects = _build_app(operation_reconciler=reconciler)
    with TestClient(app) as client:
        _admin(client)
        absent = client.post(
            f"/api/collection-operations/{EFFECT_ID}/reconcile",
            json={"state_revision": 7},
        )
        reconciler.result = _operation(
            CollectionOperationStatus.REJECTED,
            state_revision=9,
            error={"type": "provider_write_not_observed", "provider_present": True},
        )
        present = client.post(
            f"/api/collection-operations/{EFFECT_ID}/reconcile",
            json={"state_revision": 8},
        )
        reconciler.error = CollectionOperationStateError("fixture revision conflict")
        conflict = client.post(
            f"/api/collection-operations/{EFFECT_ID}/reconcile",
            json={"state_revision": 8},
        )
        reconciler.error = CollectionRemoteIndeterminate("fixture readback unavailable")
        unavailable = client.post(
            f"/api/collection-operations/{EFFECT_ID}/reconcile",
            json={"state_revision": 8},
        )
        reconciler.error = None
        reconciler.result = None
        missing = client.post(
            f"/api/collection-operations/{EFFECT_ID}/reconcile",
            json={"state_revision": 8},
        )
        invalid = client.post(
            f"/api/collection-operations/{EFFECT_ID}/reconcile",
            json={"state_revision": -1},
        )

    assert absent.status_code == 200
    assert absent.json()["status"] == "confirmed"
    assert absent.json()["result"] == {"provider_present": False}
    assert present.status_code == 200
    assert present.json()["status"] == "rejected"
    assert present.json()["error"]["provider_present"] is True
    assert conflict.status_code == 409
    assert unavailable.status_code == 503
    assert missing.status_code == 404
    assert invalid.status_code == 400
    assert reconciler.calls[0] == (EFFECT_ID, 7, "collection-admin:admin")


def test_fix_match_season_uses_authoritative_provider_id_and_no_external_effect() -> None:
    app, repository, queries, effects = _build_app()
    with TestClient(app) as client:
        _admin(client)
        response = client.post(
            "/api/custom_collections/7/fix_match",
            json={"old_title": "Unknown", "new_tmdb_id": "202", "season_number": 2},
        )

    assert response.status_code == 200
    assert repository.inspected == [(7, None, "Unknown")]
    assert queries.correction_calls == [("series", 202), ("season", 202, 2)]
    candidate, snapshot = repository.applied[0]
    assert candidate.item_type == "Series"
    assert snapshot.collection_tmdb_id == "202"
    assert snapshot.target_key == SubscriptionMediaKey("20202", "Season")
    assert snapshot.parent_key == SubscriptionMediaKey("202", "Series")
    assert snapshot.target_record["parent_series_tmdb_id"] == "202"
    assert snapshot.target_status == "PENDING_RELEASE"
    assert response.json()["corrected_item"]["season"] == 2
    assert not effects.native and not effects.custom


def test_fix_match_movie_freezes_provider_snapshot_before_repository_apply() -> None:
    app, repository, queries, _effects = _build_app()
    repository.candidate_item_type = "Movie"
    with TestClient(app) as client:
        _admin(client)
        response = client.post(
            "/api/custom_collections/7/fix_match",
            json={"old_tmdb_id": "101", "new_tmdb_id": "303"},
        )

    assert response.status_code == 200
    assert queries.correction_calls == [("movie", 303)]
    _candidate, snapshot = repository.applied[0]
    assert snapshot.target_key == SubscriptionMediaKey("303", "Movie")
    assert snapshot.target_status == "WANTED"
    assert snapshot.corrected_item["title"] == "Fixture Movie"


def test_fix_match_provider_failure_and_snapshot_drift_do_not_apply() -> None:
    app, repository, queries, _effects = _build_app()
    with TestClient(app) as client:
        _admin(client)
        queries.correction_missing = True
        missing = client.post(
            "/api/custom_collections/7/fix_match",
            json={"old_title": "Unknown", "new_tmdb_id": "202", "season_number": 1},
        )
        queries.correction_missing = False
        repository.conflict = True
        drift = client.post(
            "/api/custom_collections/7/fix_match",
            json={"old_title": "Unknown", "new_tmdb_id": "202", "season_number": 1},
        )

    assert missing.status_code == 404
    assert missing.json() == {"error": "修正失败，TMDb 中未找到目标媒体"}
    assert drift.status_code == 409
    assert drift.json() == {"error": "合集项目已变化，请刷新后重试"}
    assert repository.applied == []


def test_fix_match_rejects_invalid_identity_and_movie_season_before_provider_read() -> None:
    app, repository, queries, _effects = _build_app()
    repository.candidate_item_type = "Movie"
    with TestClient(app) as client:
        _admin(client)
        bad_id = client.post(
            "/api/custom_collections/7/fix_match",
            json={"old_title": "Unknown", "new_tmdb_id": "not-an-id"},
        )
        movie_season = client.post(
            "/api/custom_collections/7/fix_match",
            json={"old_title": "Unknown", "new_tmdb_id": "303", "season_number": 1},
        )

    assert bad_id.status_code == 400
    assert movie_season.status_code == 400
    assert queries.correction_calls == []
