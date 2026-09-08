from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any, Protocol
from urllib.parse import quote, quote_plus

import httpx
from sqlalchemy import text

from etk.domains.operations.jobs import JobSubmissionService
from etk.platform import Settings
from etk.platform.db import SqlAlchemyUnitOfWork

from .cast_editor import CastTranslationCommand, ManualCastUpdateCommand, ProcessedCastActor


class _Translation(Protocol):
    def translate(self, values: Sequence[str]) -> Mapping[str, str]: ...


class CastUpdateProgress(Protocol):
    def checkpoint(self) -> None: ...

    def update(self, progress: int, message: str) -> None: ...

    def record_started_effect(self, label: str) -> None: ...

    def record_committed_effect(self, label: str) -> None: ...

    def record_abandoned_effect(self, label: str) -> None: ...


class SqlAlchemyCastEditorRepository:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def actors(self, tmdb_id: str, item_type: str) -> tuple[dict[str, Any], ...]:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT actors_json
                        FROM media_metadata
                        WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                        """
                    ),
                    {"tmdb_id": str(tmdb_id), "item_type": str(item_type)},
                )
                .mappings()
                .one_or_none()
            )
            links = row.get("actors_json") if row is not None else None
            if not isinstance(links, list):
                return ()
            ids = [
                int(link["tmdb_id"])
                for link in links
                if isinstance(link, Mapping) and str(link.get("tmdb_id") or "").isdigit()
            ]
            people = {
                int(person["tmdb_person_id"]): dict(person)
                for person in uow.session.execute(
                    text(
                        """
                        SELECT tmdb_person_id, imdb_id, douban_celebrity_id,
                               primary_name, original_name, profile_path
                        FROM person_metadata
                        WHERE tmdb_person_id = ANY(CAST(:ids AS integer[]))
                        """
                    ),
                    {"ids": ids},
                ).mappings()
            }
        result: list[dict[str, Any]] = []
        for link in links:
            if not isinstance(link, Mapping) or not str(link.get("tmdb_id") or "").isdigit():
                continue
            person_id = int(link["tmdb_id"])
            person = people.get(person_id, {})
            result.append(
                {
                    "tmdb_id": person_id,
                    "name": person.get("primary_name") or person.get("original_name"),
                    "character": link.get("character"),
                    "order": link.get("order"),
                    "profile_path": person.get("profile_path"),
                    "imdb_id": person.get("imdb_id"),
                    "douban_id": person.get("douban_celebrity_id"),
                }
            )
        result.sort(key=lambda item: int(item.get("order") or 999))
        return tuple(result)

    def review(self, item_id: str) -> Mapping[str, Any]:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text("SELECT error_message, score FROM failed_log WHERE item_id = :item_id"),
                    {"item_id": str(item_id)},
                )
                .mappings()
                .one_or_none()
            )
        return dict(row) if row is not None else {}

    def replace_cast(
        self,
        tmdb_id: str,
        item_type: str,
        actors: Sequence[Mapping[str, Any]],
        *,
        item_id: str,
        item_name: str,
    ) -> None:
        links: list[dict[str, Any]] = []
        with self._unit_of_work() as uow:
            for order, actor in enumerate(actors):
                raw_tmdb_id = actor.get("tmdbId") or actor.get("tmdb_id")
                actor_tmdb_id = int(str(raw_tmdb_id)) if str(raw_tmdb_id or "").isdigit() else None
                if actor_tmdb_id is None:
                    continue
                name = str(actor.get("name") or actor.get("Name") or "").strip() or f"TMDb {actor_tmdb_id}"
                role = str(actor.get("role") or actor.get("Role") or "").strip()
                uow.session.execute(
                    text(
                        """
                        INSERT INTO person_metadata (tmdb_person_id, primary_name, last_updated_at)
                        VALUES (:tmdb_person_id, :primary_name, NOW())
                        ON CONFLICT (tmdb_person_id) DO UPDATE SET
                            primary_name = EXCLUDED.primary_name,
                            last_updated_at = NOW()
                        """
                    ),
                    {"tmdb_person_id": actor_tmdb_id, "primary_name": name},
                )
                links.append({"tmdb_id": actor_tmdb_id, "character": role, "order": order})
            updated = uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET actors_json = CAST(:actors AS jsonb), actors_ready = TRUE, last_updated_at = NOW()
                    WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                    """
                ),
                {
                    "actors": json.dumps(links, ensure_ascii=False),
                    "tmdb_id": str(tmdb_id),
                    "item_type": str(item_type),
                },
            )
            if int(getattr(updated, "rowcount", 0) or 0) != 1:
                raise RuntimeError("manual cast target metadata row is unavailable")
            uow.session.execute(
                text(
                    """
                    INSERT INTO processed_log (item_id, item_name, score, processed_at)
                    VALUES (:item_id, :item_name, 10.0, NOW())
                    ON CONFLICT (item_id) DO UPDATE SET
                        item_name = EXCLUDED.item_name,
                        score = EXCLUDED.score,
                        processed_at = NOW()
                    """
                ),
                {"item_id": str(item_id), "item_name": str(item_name)},
            )
            uow.session.execute(text("DELETE FROM failed_log WHERE item_id = :item_id"), {"item_id": str(item_id)})
            uow.commit()


class HttpxMediaCastEditorGateway:
    def __init__(
        self,
        settings: Callable[[], Settings],
        repository: SqlAlchemyCastEditorRepository,
        jobs: Callable[[], JobSubmissionService],
        translation: _Translation,
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._jobs = jobs
        self._translation = translation
        self._client_factory = client_factory or self._default_client

    def processor_ready(self) -> bool:
        settings = self._settings()
        return bool(
            settings.emby_server_url.strip()
            and settings.emby_api_key.get_secret_value().strip()
            and settings.emby_user_id.strip()
        )

    def media_for_editing(self, item_id: str) -> Any:
        item = self._item(item_id)
        if item is None:
            return None
        tmdb_id, item_type = _item_identity(item)
        actors = self._repository.actors(tmdb_id, item_type)
        if not actors:
            return None
        emby_people = _emby_people_by_tmdb(item)
        current_cast = []
        for actor in actors:
            profile_path = str(actor.get("profile_path") or "").strip()
            image_url = (
                profile_path
                if profile_path.startswith("http")
                else (f"https://image.tmdb.org/t/p/w185{profile_path}" if profile_path else None)
            )
            actor_tmdb_id = actor.get("tmdb_id")
            current_cast.append(
                {
                    "tmdbId": actor_tmdb_id,
                    "name": actor.get("name"),
                    "role": _clean_role(actor.get("character")),
                    "imageUrl": image_url,
                    "emby_person_id": emby_people.get(str(actor_tmdb_id)),
                }
            )
        review = self._repository.review(item_id)
        title = str(item.get("Name") or "")
        year = item.get("ProductionYear")
        query = quote_plus(" ".join(str(value) for value in (title, year) if value not in {None, ""}))
        return {
            "item_id": str(item_id),
            "item_name": title,
            "item_type": item_type,
            "image_tag": (item.get("ImageTags") or {}).get("Primary")
            if isinstance(item.get("ImageTags"), Mapping)
            else None,
            "original_score": review.get("score"),
            "review_reason": review.get("error_message"),
            "current_emby_cast": current_cast,
            "search_links": {
                "baidu": f"https://www.baidu.com/s?wd={query}",
                "wikipedia": f"https://zh.wikipedia.org/w/index.php?search={query}",
                "google": f"https://www.google.com/search?q={query}",
            },
        }

    def submit_manual_update(self, command: ManualCastUpdateCommand) -> bool:
        self._jobs().submit_native_operation(
            "media.cast-update",
            {
                "item_id": command.item_id,
                "cast": list(command.cast),
                "item_name": command.item_name,
            },
            display_name=f"手动更新: {command.item_name}",
            trigger_type="manual",
            timeout_seconds=max(120, int(self._settings().emby_api_timeout) + 60),
        )
        return True

    def translate_cast(self, command: CastTranslationCommand) -> Any:
        result = [dict(actor) for actor in command.cast if isinstance(actor, Mapping)]
        if not result:
            return []
        if not self._settings().ai_translate_actor_role:
            for actor in result:
                if _needs_translation(actor.get("name")) or _needs_translation(actor.get("role")):
                    actor["matchStatus"] = "AI未启用"
                    break
            return result
        values = tuple(
            dict.fromkeys(
                text_value
                for actor in result
                for text_value in (
                    str(actor.get("name") or "").strip(),
                    _clean_role(actor.get("role")),
                )
                if _needs_translation(text_value)
            )
        )
        translated = self._translation.translate(values)
        for actor in result:
            changed = False
            for field in ("name", "role"):
                original = _clean_role(actor.get(field)) if field == "role" else str(actor.get(field) or "").strip()
                replacement = str(translated.get(original) or original)
                if replacement != original:
                    actor[field] = replacement
                    changed = True
            if changed:
                actor["matchStatus"] = "已翻译"
        return result

    def preview_item_details(self, item_id: str) -> dict[str, Any] | None:
        return self._item(item_id)

    def process_preview_cast(self, item_details: dict[str, Any]) -> tuple[ProcessedCastActor, ...]:
        tmdb_id, item_type = _item_identity(item_details)
        emby_people = _emby_people_by_tmdb(item_details)
        return tuple(
            ProcessedCastActor(
                emby_person_id=emby_people.get(str(actor.get("tmdb_id") or "")),
                name=actor.get("name"),
                role=actor.get("character"),
                imdb_id=actor.get("imdb_id"),
                douban_id=actor.get("douban_id"),
                tmdb_id=actor.get("tmdb_id"),
            )
            for actor in self._repository.actors(tmdb_id, item_type)
        )

    def _item(self, item_id: str) -> dict[str, Any] | None:
        return _read_item(self._settings(), self._client_factory, item_id)

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(proxy=proxy or None, timeout=max(1.0, float(settings.emby_api_timeout)))


class NativeCastUpdateOperation:
    def __init__(
        self,
        settings: Callable[[], Settings],
        repository: SqlAlchemyCastEditorRepository,
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._client_factory = client_factory or self._default_client

    def execute(self, parameters: Mapping[str, Any], progress: CastUpdateProgress) -> Mapping[str, Any]:
        item_id = str(parameters.get("item_id") or "").strip()
        item_name = str(parameters.get("item_name") or f"未知项目(ID:{item_id})")
        raw_cast = parameters.get("cast")
        if not item_id or not isinstance(raw_cast, list) or any(not isinstance(actor, Mapping) for actor in raw_cast):
            raise ValueError("manual cast operation parameters are invalid")
        cast = tuple(dict(actor) for actor in raw_cast)
        progress.checkpoint()
        item = _read_item(self._settings(), self._client_factory, item_id)
        if item is None:
            raise RuntimeError("无法获取该媒体的 Emby 详情")
        tmdb_id, item_type = _item_identity(item)
        desired_people = _desired_emby_people(item, cast)
        label = f"media-cast-update:{item_id}"
        progress.record_started_effect(label)
        outcome = self._write_and_confirm(item_id, item, desired_people)
        if outcome == "rejected":
            progress.record_abandoned_effect(label)
            raise RuntimeError("Emby 拒绝演员表更新")
        if outcome != "confirmed":
            raise RuntimeError("Emby 演员表更新结果无法确认")
        progress.record_committed_effect(label)
        self._repository.replace_cast(tmdb_id, item_type, cast, item_id=item_id, item_name=item_name)
        progress.update(100, "手动演员表已更新")
        return {"ok": True, "item_id": item_id, "updated": len(cast)}

    def _write_and_confirm(
        self,
        item_id: str,
        item: Mapping[str, Any],
        desired_people: list[dict[str, Any]],
    ) -> str:
        settings = self._settings()
        base_url = settings.emby_server_url.strip().rstrip("/")
        token = settings.emby_api_key.get_secret_value().strip()
        payload = deepcopy(dict(item))
        payload["People"] = desired_people
        for key in (
            "MediaStreams",
            "MediaSources",
            "Chapters",
            "RecursiveItemCount",
            "ChildCount",
            "ImageTags",
            "SeriesTimerId",
            "RunTimeTicks",
        ):
            payload.pop(key, None)
        uncertain = False
        rejected = False
        try:
            with self._client_factory() as client:
                response = client.post(
                    f"{base_url}/Items/{quote(item_id, safe='')}",
                    params={"api_key": token},
                    headers={"X-Emby-Token": token, "Content-Type": "application/json"},
                    json=payload,
                )
            uncertain = response.status_code >= 500
            rejected = 400 <= response.status_code < 500
        except (httpx.TimeoutException, httpx.TransportError):
            uncertain = True
        observed = _read_item(self._settings(), self._client_factory, item_id)
        if observed is not None and _people_signature(observed.get("People")) == _people_signature(desired_people):
            return "confirmed"
        return "unknown" if uncertain or not rejected else "rejected"

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(proxy=proxy or None, timeout=max(1.0, float(settings.emby_api_timeout)))


def _read_item(
    settings: Settings,
    client_factory: Callable[[], httpx.Client],
    item_id: str,
) -> dict[str, Any] | None:
    base_url = settings.emby_server_url.strip().rstrip("/")
    token = settings.emby_api_key.get_secret_value().strip()
    user_id = settings.emby_user_id.strip()
    normalized_id = str(item_id or "").strip()
    if not base_url or not token or not user_id or not normalized_id:
        return None
    try:
        with client_factory() as client:
            response = client.get(
                f"{base_url}/Users/{quote(user_id, safe='')}/Items/{quote(normalized_id, safe='')}",
                params={"Fields": "ProviderIds,People,ImageTags,ProductionYear,Path,Type,Name"},
                headers={"X-Emby-Token": token, "Accept": "application/json"},
            )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload: Any = response.json()
    except (httpx.HTTPError, TypeError, ValueError):
        return None
    return dict(payload) if isinstance(payload, Mapping) else None


def _item_identity(item: Mapping[str, Any]) -> tuple[str, str]:
    providers = item.get("ProviderIds")
    tmdb_id = str(providers.get("Tmdb") or "").strip() if isinstance(providers, Mapping) else ""
    item_type = str(item.get("Type") or "").strip()
    if not tmdb_id or item_type not in {"Movie", "Series"}:
        raise ValueError("preview item does not have a supported TMDb identity")
    return tmdb_id, item_type


def _emby_people_by_tmdb(item: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(providers.get("Tmdb")): person.get("Id")
        for person in (item.get("People") or [])
        if isinstance(person, Mapping)
        and isinstance((providers := person.get("ProviderIds")), Mapping)
        and providers.get("Tmdb")
    }


def _desired_emby_people(item: Mapping[str, Any], cast: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    existing_by_tmdb = {
        str(providers.get("Tmdb")): dict(person)
        for person in (item.get("People") or [])
        if isinstance(person, Mapping)
        and isinstance((providers := person.get("ProviderIds")), Mapping)
        and providers.get("Tmdb")
    }
    result: list[dict[str, Any]] = []
    for actor in cast:
        tmdb_id = str(actor.get("tmdbId") or actor.get("tmdb_id") or "").strip()
        name = str(actor.get("name") or actor.get("Name") or "").strip()
        if not tmdb_id or not name:
            continue
        person = existing_by_tmdb.get(tmdb_id, {})
        desired = {
            "Name": name,
            "Role": str(actor.get("role") or actor.get("Role") or "").strip(),
            "Type": "Actor",
            "ProviderIds": {**dict(person.get("ProviderIds") or {}), "Tmdb": tmdb_id},
        }
        emby_id = actor.get("embyPersonId") or actor.get("emby_person_id") or person.get("Id")
        if emby_id:
            desired["Id"] = str(emby_id)
        result.append(desired)
    return result


def _people_signature(value: object) -> tuple[tuple[str, str, str], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(
        (
            str((person.get("ProviderIds") or {}).get("Tmdb") or ""),
            str(person.get("Name") or ""),
            str(person.get("Role") or ""),
        )
        for person in value
        if isinstance(person, Mapping) and str(person.get("Type") or "Actor") == "Actor"
    )


def _clean_role(value: object) -> str:
    role = str(value or "").strip()
    for prefix in ("饰 ", "配 ", "as "):
        if role.casefold().startswith(prefix.casefold()):
            role = role[len(prefix) :].strip()
    return role


def _needs_translation(value: object) -> bool:
    text_value = str(value or "").strip()
    return bool(text_value) and not any("\u4e00" <= char <= "\u9fff" for char in text_value)


__all__ = [
    "HttpxMediaCastEditorGateway",
    "NativeCastUpdateOperation",
    "SqlAlchemyCastEditorRepository",
]
