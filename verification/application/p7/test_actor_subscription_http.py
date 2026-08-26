from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.subscription.actor import (
    ACTOR_DEFAULT_CONFIG_KEY,
    ActorRefreshIntent,
    ActorSubscriptionService,
    DuplicateActorSubscriptionError,
)
from etk.domains.subscription.actor_router import router
from etk.platform.web import CookieSessionMiddleware, PermissionPolicy

REPO_ROOT = Path(__file__).resolve().parents[3]
CONTRACT = json.loads(
    (REPO_ROOT / "verification/fixtures/http/p7-actor-subscriptions.json").read_text(encoding="utf-8")
)


class FakePermissions:
    def is_admin(self, user_id: str) -> bool:
        return user_id == "fixture-admin"


class FakeSettings:
    def __init__(self, value: Any = None) -> None:
        self.value = deepcopy(value)
        self.saves: list[tuple[str, Any]] = []
        self.fail_save = False

    def load(self, key: str) -> Any:
        assert key == ACTOR_DEFAULT_CONFIG_KEY
        return deepcopy(self.value)

    def save(self, key: str, value: Any) -> None:
        assert key == ACTOR_DEFAULT_CONFIG_KEY
        if self.fail_save:
            raise RuntimeError("fixture settings failure")
        self.value = deepcopy(value)
        self.saves.append((key, deepcopy(value)))


class FakeSearch:
    def __init__(self) -> None:
        self.is_configured = True
        self.results: Any = deepcopy(CONTRACT["search"]["provider_results"])
        self.queries: list[str] = []

    def configured(self) -> bool:
        return self.is_configured

    def search(self, query: str) -> Any:
        self.queries.append(query)
        if isinstance(self.results, Exception):
            raise self.results
        return deepcopy(self.results)


class FakeRuntime:
    def __init__(self) -> None:
        self.ready = True

    def processor_ready(self) -> bool:
        return self.ready

    def detail_context(self) -> dict[str, Any]:
        detail = CONTRACT["detail"]
        return {
            "emby_server_url": detail["emby_server_url"],
            "emby_api_key_for_url": detail["emby_api_key_for_url"],
            "emby_server_id": detail["emby_server_id"],
        }


class FakeRefreshIntents:
    def __init__(self) -> None:
        self.intents: list[ActorRefreshIntent] = []
        self.accepted = True
        self.error: Exception | None = None

    def submit(self, intent: ActorRefreshIntent) -> bool:
        self.intents.append(intent)
        if self.error is not None:
            raise self.error
        return self.accepted


class FakeRepository:
    def __init__(self) -> None:
        self.subscriptions: dict[int, dict[str, Any]] = {}
        self.create_calls: list[dict[str, Any]] = []
        self.update_calls: list[tuple[int, dict[str, Any]]] = []
        self.delete_calls: list[int] = []
        self.duplicate = False
        self.next_id = 7

    def list_subscriptions(self) -> list[dict[str, Any]]:
        return [self._summary(value) for value in reversed(tuple(self.subscriptions.values()))]

    def get_subscription(self, subscription_id: int) -> dict[str, Any] | None:
        value = self.subscriptions.get(subscription_id)
        return deepcopy(value) if value is not None else None

    def create_subscription(self, **values: Any) -> int:
        self.create_calls.append(deepcopy(values))
        if self.duplicate:
            raise DuplicateActorSubscriptionError
        subscription_id = self.next_id
        config = deepcopy(dict(values["config"]))
        self.subscriptions[subscription_id] = {
            "id": subscription_id,
            "tmdb_person_id": values["tmdb_person_id"],
            "actor_name": values["actor_name"],
            "profile_path": values["profile_path"],
            "status": "active",
            "last_checked_at": None,
            "added_at": datetime(2025, 1, 1, tzinfo=UTC),
            "config": config,
            "tracked_media": [
                {
                    "tmdb_media_id": "501",
                    "media_type": "Movie",
                    "title": "Fixture Movie",
                    "release_date": date(2025, 1, 2),
                    "poster_path": "/fixture/poster.jpg",
                    "status": "WANTED",
                    "emby_item_ids_json": [],
                    "in_library": False,
                    "ignore_reason": None,
                    "season_number": None,
                    "parent_title": None,
                    "emby_item_id": None,
                }
            ],
        }
        return subscription_id

    def update_subscription(self, subscription_id: int, data: Any) -> bool:
        self.update_calls.append((subscription_id, deepcopy(dict(data))))
        subscription = self.subscriptions.get(subscription_id)
        if subscription is None:
            return False
        subscription["status"] = data.get("status", subscription["status"])
        subscription["config"].update(deepcopy(dict(data.get("config", data))))
        return True

    def delete_subscription(self, subscription_id: int) -> None:
        self.delete_calls.append(subscription_id)
        self.subscriptions.pop(subscription_id, None)

    @staticmethod
    def _summary(value: dict[str, Any]) -> dict[str, Any]:
        return {key: deepcopy(value.get(key)) for key in CONTRACT["summary"]}


def _service(
    *,
    repository: FakeRepository | None = None,
    settings: FakeSettings | None = None,
    search: FakeSearch | None = None,
    runtime: FakeRuntime | None = None,
    refresh: FakeRefreshIntents | None = None,
) -> tuple[
    ActorSubscriptionService,
    FakeRepository,
    FakeSettings,
    FakeSearch,
    FakeRuntime,
    FakeRefreshIntents,
]:
    repository = repository or FakeRepository()
    settings = settings or FakeSettings()
    search = search or FakeSearch()
    runtime = runtime or FakeRuntime()
    refresh = refresh or FakeRefreshIntents()
    return (
        ActorSubscriptionService(
            repository=repository,
            default_settings=settings,
            search=search,
            refresh_intents=refresh,
            runtime=runtime,
        ),
        repository,
        settings,
        search,
        runtime,
        refresh,
    )


def _client(
    service: ActorSubscriptionService,
    *,
    auth_enabled: bool = False,
) -> tuple[TestClient, LegacySessionCodec]:
    codec = LegacySessionCodec.from_secret_key(b"p7-actor-subscription-session")
    app = FastAPI()
    app.state.actor_subscription_service = service
    app.state.permission_policy = PermissionPolicy(FakePermissions(), auth_enabled=auth_enabled)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return TestClient(app), codec


def test_router_preserves_all_nine_legacy_method_path_pairs() -> None:
    actual = set()
    for route in router.routes:
        path = str(getattr(route, "path", "")).replace("{sub_id}", "7")
        if path.startswith("/api/actor-subscriptions"):
            actual.update((method, path) for method in (getattr(route, "methods", None) or set()))

    assert actual == {(item["method"], item["path"]) for item in CONTRACT["routes"]}


@pytest.mark.parametrize("route", CONTRACT["routes"], ids=lambda route: f"{route['method']} {route['path']}")
def test_every_actor_subscription_route_requires_admin(route: dict[str, str]) -> None:
    service, _repository, _settings, search, _runtime, refresh = _service()
    client, _codec = _client(service, auth_enabled=True)

    with client:
        response = client.request(
            route["method"],
            route["path"],
            json={} if route["method"] in {"POST", "PUT"} else None,
        )

    assert {"status": response.status_code, "body": response.json()} == CONTRACT["auth_denied"]
    assert search.queries == []
    assert refresh.intents == []


def test_default_config_get_save_and_create_fallback_preserve_legacy_schema() -> None:
    service, repository, settings, _search, _runtime, _refresh = _service()
    client, _codec = _client(service)

    with client:
        empty = client.get("/api/actor-subscriptions/default-config")
        saved = client.post("/api/actor-subscriptions/default-config", json=CONTRACT["saved_default_config"])
        current = client.get("/api/actor-subscriptions/default-config")
        created = client.post("/api/actor-subscriptions", json=CONTRACT["create_request"])

    assert empty.json() == CONTRACT["empty_default_config"]
    assert saved.json() == CONTRACT["messages"]["default_saved"]
    assert current.json() == CONTRACT["saved_default_config"]
    assert {"status": created.status_code, "body": created.json()} == CONTRACT["create_response"]
    assert settings.saves == [(ACTOR_DEFAULT_CONFIG_KEY, CONTRACT["saved_default_config"])]
    assert repository.create_calls[0]["config"] == CONTRACT["saved_default_config"]


def test_crud_responses_and_detail_runtime_context_match_frozen_contract() -> None:
    service, repository, settings, _search, _runtime, _refresh = _service(
        settings=FakeSettings(CONTRACT["saved_default_config"])
    )
    client, _codec = _client(service)

    with client:
        created = client.post("/api/actor-subscriptions", json=CONTRACT["create_request"])
        listed = client.get("/api/actor-subscriptions")
        detail = client.get("/api/actor-subscriptions/7")
        updated = client.put("/api/actor-subscriptions/7", json={"status": "paused"})
        deleted = client.delete("/api/actor-subscriptions/7")
        missing = client.get("/api/actor-subscriptions/7")

    assert {"status": created.status_code, "body": created.json()} == CONTRACT["create_response"]
    assert listed.json() == [CONTRACT["summary"]]
    assert detail.json() == CONTRACT["detail"]
    assert updated.json() == CONTRACT["messages"]["updated"]
    assert repository.update_calls == [(7, {"status": "paused"})]
    assert deleted.json() == CONTRACT["messages"]["deleted"]
    assert repository.delete_calls == [7]
    assert {"status": missing.status_code, "body": missing.json()} == {
        "status": 404,
        "body": CONTRACT["messages"]["missing"],
    }
    assert settings.value == CONTRACT["saved_default_config"]


def test_create_validation_duplicate_and_not_found_contracts() -> None:
    repository = FakeRepository()
    service, *_ = _service(repository=repository)
    client, _codec = _client(service)

    with client:
        invalid = client.post("/api/actor-subscriptions", json={"actor_name": "Fixture Actor"})
        repository.duplicate = True
        duplicate = client.post("/api/actor-subscriptions", json=CONTRACT["create_request"])
        update_missing = client.put("/api/actor-subscriptions/404", json={"status": "paused"})
        delete_missing = client.delete("/api/actor-subscriptions/404")

    assert {"status": invalid.status_code, "body": invalid.json()} == {
        "status": 400,
        "body": CONTRACT["messages"]["invalid_create"],
    }
    assert {"status": duplicate.status_code, "body": duplicate.json()} == {
        "status": 409,
        "body": CONTRACT["messages"]["duplicate"],
    }
    assert {"status": update_missing.status_code, "body": update_missing.json()} == {
        "status": 404,
        "body": CONTRACT["messages"]["missing"],
    }
    assert delete_missing.status_code == 200
    assert delete_missing.json() == CONTRACT["messages"]["deleted"]


def test_malformed_json_is_handled_manually_without_fastapi_422() -> None:
    service, *_ = _service()
    client, _codec = _client(service)

    with client:
        malformed_create = client.post(
            "/api/actor-subscriptions",
            content="{",
            headers={"content-type": "application/json"},
        )
        wrong_content_type = client.post(
            "/api/actor-subscriptions",
            content="{}",
            headers={"content-type": "text/plain"},
        )
        array_create = client.post("/api/actor-subscriptions", json=[])
        invalid_path = client.get("/api/actor-subscriptions/not-an-integer")
        malformed_update = client.put(
            "/api/actor-subscriptions/7",
            content="{",
            headers={"content-type": "application/json"},
        )

    assert malformed_create.status_code == 400
    assert wrong_content_type.status_code == 415
    assert array_create.status_code == 500
    assert invalid_path.status_code == 404
    assert all(
        response.status_code != 422 for response in (malformed_create, wrong_content_type, array_create, invalid_path)
    )
    assert {"status": malformed_update.status_code, "body": malformed_update.json()} == {
        "status": 500,
        "body": CONTRACT["messages"]["update_failed"],
    }


def test_default_config_malformed_json_uses_legacy_json_500_envelope() -> None:
    service, _repository, settings, *_ = _service()
    client, _codec = _client(service)
    settings.fail_save = True

    with client:
        malformed = client.post(
            "/api/actor-subscriptions/default-config",
            content="{",
            headers={"content-type": "application/json"},
        )
        failed = client.post("/api/actor-subscriptions/default-config", json={"start_year": 2000})

    expected = {"status": 500, "body": CONTRACT["messages"]["default_save_failed"]}
    assert {"status": malformed.status_code, "body": malformed.json()} == expected
    assert {"status": failed.status_code, "body": failed.json()} == expected


def test_search_readiness_validation_configuration_and_formatting_semantics() -> None:
    service, _repository, _settings, search, runtime, _refresh = _service()
    client, _codec = _client(service)

    with client:
        runtime.ready = False
        unavailable = client.get("/api/actor-subscriptions/search")
        runtime.ready = True
        missing = client.get("/api/actor-subscriptions/search")
        search.is_configured = False
        unconfigured = client.get("/api/actor-subscriptions/search?name=Fixture")
        search.is_configured = True
        found = client.get(f"/api/actor-subscriptions/search?name={CONTRACT['search']['query']}")

    assert {"status": unavailable.status_code, "body": unavailable.json()} == {
        "status": 503,
        "body": CONTRACT["messages"]["processor_unavailable"],
    }
    assert {"status": missing.status_code, "body": missing.json()} == {
        "status": 400,
        "body": CONTRACT["messages"]["missing_query"],
    }
    assert {"status": unconfigured.status_code, "body": unconfigured.json()} == {
        "status": 503,
        "body": CONTRACT["messages"]["tmdb_unconfigured"],
    }
    assert found.status_code == 200
    assert found.json() == CONTRACT["search"]["response"]
    assert search.queries == [CONTRACT["search"]["query"]]


def test_search_provider_failure_preserves_both_legacy_error_envelopes() -> None:
    service, _repository, _settings, search, _runtime, _refresh = _service()
    client, _codec = _client(service)

    with client:
        search.results = None
        rejected = client.get("/api/actor-subscriptions/search?name=Fixture")
        search.results = RuntimeError("fixture TMDb failure")
        crashed = client.get("/api/actor-subscriptions/search?name=Fixture")

    assert {"status": rejected.status_code, "body": rejected.json()} == {
        "status": 500,
        "body": CONTRACT["messages"]["tmdb_failed"],
    }
    assert crashed.status_code == 500
    assert crashed.json() == {"error": "搜索演员时发生未知的服务器错误"}


def test_refresh_submits_only_symbolic_intent_and_preserves_acceptance_of_false_return() -> None:
    repository = FakeRepository()
    repository.subscriptions[7] = deepcopy(CONTRACT["detail"])
    refresh = FakeRefreshIntents()
    refresh.accepted = False
    service, *_ = _service(repository=repository, refresh=refresh)
    client, _codec = _client(service)

    with client:
        response = client.post("/api/actor-subscriptions/7/refresh")

    assert response.status_code == 202
    assert response.json() == CONTRACT["messages"]["refresh_accepted"]
    assert refresh.intents == [ActorRefreshIntent(7, "Fixture Actor")]


def test_refresh_not_found_and_submission_exception_match_legacy_contract() -> None:
    repository = FakeRepository()
    refresh = FakeRefreshIntents()
    service, *_ = _service(repository=repository, refresh=refresh)
    client, _codec = _client(service)

    with client:
        missing = client.post("/api/actor-subscriptions/404/refresh")
        repository.subscriptions[7] = deepcopy(CONTRACT["detail"])
        refresh.error = RuntimeError("fixture intent failure")
        failed = client.post("/api/actor-subscriptions/7/refresh")

    assert {"status": missing.status_code, "body": missing.json()} == {
        "status": 404,
        "body": CONTRACT["messages"]["refresh_missing"],
    }
    assert failed.status_code == 500
    assert failed.headers["content-type"].startswith("text/html")
    assert "Internal Server Error" in failed.text
