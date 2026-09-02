from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from etk.domains.people.task_execution import NativePeopleTaskService
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcomeStatus
from etk.processes.p7_subscription_tasks import P7SubscriptionTaskRouter


class Progress:
    def __init__(self) -> None:
        self.checkpoints = 0
        self.updates: list[tuple[int, str]] = []

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        self.checkpoints += 1


class Catalog:
    def __init__(self) -> None:
        self.persons: list[dict[str, Any]] = [
            {"Id": "1", "Name": "John Doe", "ProviderIds": {"Tmdb": "100"}},
            {"Id": "2", "Name": "约翰", "ProviderIds": {"Tmdb": "100"}},
            {"Id": "3", "Name": "Ghost", "ProviderIds": {"Tmdb": "300"}},
        ]
        self.media: list[dict[str, Any]] = [
            {"Id": "media-1", "People": [{"Id": "1", "Name": "John Doe"}]},
            {"Id": "media-2", "People": [{"Id": "2", "Name": "约翰"}]},
            {"Id": "media-3", "People": [{"Id": "2", "Name": "约翰"}]},
        ]
        self.updated_people: list[tuple[str, Sequence[Mapping[str, Any]]]] = []
        self.deleted: list[str] = []

    def list_persons(self) -> Sequence[Mapping[str, Any]]:
        return tuple(dict(person) for person in self.persons if str(person["Id"]) not in self.deleted)

    def update_person_name(self, person_id: str, name: str) -> bool:
        for person in self.persons:
            if str(person["Id"]) == person_id:
                person["Name"] = name
        return True

    def list_media_items(self) -> Sequence[Mapping[str, Any]]:
        return tuple(dict(item) for item in self.media)

    def update_media_people(self, item_id: str, people: Sequence[Mapping[str, Any]]) -> bool:
        self.updated_people.append((item_id, tuple(dict(person) for person in people)))
        for item in self.media:
            if str(item["Id"]) == item_id:
                item["People"] = [dict(person) for person in people]
        return True

    def delete_person(self, person_id: str) -> bool:
        self.deleted.append(person_id)
        return True

    def person_exists(self, person_id: str) -> bool:
        return person_id not in self.deleted


class Provider:
    def details(self, person_id: str) -> Mapping[str, Any]:
        return {
            "id": int(person_id),
            "name": f"Person {person_id}",
            "original_name": f"Original {person_id}",
            "also_known_as": [f"Alias {person_id}"],
        }

    def credits(self, person_id: str) -> Sequence[Mapping[str, Any]]:
        assert person_id == "100"
        return ({"id": 2001}, {"id": 2002})


class Repository:
    def __init__(self) -> None:
        self.enrichment = ({"tmdb_person_id": 100, "primary_name": "John", "last_synced_at": None},)
        self.subscriptions = ({"id": 5, "tmdb_person_id": 100, "actor_name": "John", "status": "active"},)
        self.upserts: list[tuple[Mapping[str, Any], str | None]] = []
        self.scans: list[tuple[int, Sequence[str]]] = []
        self.reconciles: list[tuple[Mapping[str, Any], Sequence[Mapping[str, Any]], Sequence[str]]] = []

    def list_for_enrichment(self, *, force: bool) -> Sequence[Mapping[str, Any]]:
        assert force is True
        return self.enrichment

    def upsert(self, details: Mapping[str, Any], *, primary_name: str | None = None) -> bool:
        self.upserts.append((dict(details), primary_name))
        return True

    def list_actor_subscriptions(self, subscription_id: int | None = None) -> Sequence[Mapping[str, Any]]:
        if subscription_id is None:
            return self.subscriptions
        return tuple(row for row in self.subscriptions if row["id"] == subscription_id)

    def record_actor_scan(self, subscription_id: int, tmdb_ids: Sequence[str]) -> int:
        self.scans.append((subscription_id, tuple(tmdb_ids)))
        return 1

    def reconcile_actor_scan(
        self,
        subscription: Mapping[str, Any],
        works: Sequence[Mapping[str, Any]],
        all_work_ids: Sequence[str],
    ) -> Mapping[str, int]:
        self.reconciles.append((dict(subscription), tuple(dict(work) for work in works), tuple(all_work_ids)))
        return {"added": 1, "removed": 0, "scanned": 2}


class Translation:
    def translate(self, values: Sequence[str]) -> Mapping[str, str]:
        return {value: f"中文-{value}" for value in values}


def test_people_service_translates_enriches_and_tracks() -> None:
    catalog = Catalog()
    repository = Repository()
    service = NativePeopleTaskService(
        catalog,
        Provider(),
        repository,
        Translation(),
        actor_enabled=lambda: True,
    )

    translation = service.execute("actor-translation", Progress(), {})
    enrichment = service.execute("enrich-aliases", Progress(), {"force_full_update": True})
    tracking = service.execute("actor-tracking", Progress(), {"actor_name": "John"})

    assert translation.status is TaskOutcomeStatus.SUCCEEDED
    assert enrichment.status is TaskOutcomeStatus.SUCCEEDED
    assert tracking.status is TaskOutcomeStatus.SUCCEEDED
    assert repository.upserts[-1][0]["id"] == 100
    assert repository.scans == [(5, ())]


def test_merge_and_purge_use_fresh_reference_fences() -> None:
    catalog = Catalog()
    repository = Repository()
    service = NativePeopleTaskService(catalog, Provider(), repository)

    merge = service.execute("merge-duplicate-actors", Progress(), {})
    assert merge.status is TaskOutcomeStatus.SUCCEEDED
    assert catalog.updated_people[0][1][0]["Id"] == "2"
    assert catalog.deleted == ["1"]

    purge = service.execute("purge-ghost-actors", Progress(), {})
    assert purge.status is TaskOutcomeStatus.SUCCEEDED
    assert "3" in catalog.deleted


def test_actor_tracking_enriches_tv_order_and_seasons_when_supported() -> None:
    class TvProvider(Provider):
        def credits(self, person_id: str) -> Sequence[Mapping[str, Any]]:
            assert person_id == "100"
            return (
                {
                    "id": 2001,
                    "media_type": "tv",
                    "name": "中文剧集",
                    "first_air_date": "2024-01-01",
                    "poster_path": "/show.jpg",
                },
            )

        def work_details(self, work_id: str, media_type: str) -> Mapping[str, Any]:
            assert (work_id, media_type) == ("2001", "tv")
            return {
                "credits": {"cast": [{"id": 100, "order": 1}]},
                "seasons": [{"id": 3001, "season_number": 1, "name": "第一季", "air_date": "2024-01-01"}],
            }

    repository = Repository()
    result = NativePeopleTaskService(Catalog(), TvProvider(), repository).execute(
        "actor-tracking",
        Progress(),
        {"subscription_id": 5},
    )

    assert result.status is TaskOutcomeStatus.SUCCEEDED
    assert repository.reconciles
    work = repository.reconciles[0][1][0]
    assert work["order"] == 1
    assert work["seasons"][0]["season_number"] == 1


def test_p7_router_executes_people_owner_without_processor() -> None:
    catalog = Catalog()
    owner = NativePeopleTaskService(catalog, Provider(), Repository())
    router = P7SubscriptionTaskRouter(None, None, people_tasks=owner)
    message = TaskMessage.create(
        task_key="actor-tracking",
        processor=ProcessorIdentity.ACTOR,
        payload={"subscription_id": 5},
    )

    result = router.execute(message, None, Progress())

    assert result.status is TaskOutcomeStatus.SUCCEEDED
    assert result.details["compatibility_fallback"] is False
