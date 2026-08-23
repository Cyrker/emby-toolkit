from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

from .cast_editor import CastTranslationCommand, ManualCastUpdateCommand, ProcessedCastActor


class LegacyMediaCastEditorGateway:
    """Lazy adapter around the legacy processor, Emby provider, and task queue."""

    @staticmethod
    def processor_ready() -> bool:
        import extensions

        return extensions.media_processor_instance is not None

    @staticmethod
    def _processor() -> Any:
        import extensions

        processor = extensions.media_processor_instance
        if processor is None:
            raise RuntimeError("media processor became unavailable")
        return processor

    def media_for_editing(self, item_id: str) -> Any:
        return self._processor().get_cast_for_editing(item_id)

    def submit_manual_update(self, command: ManualCastUpdateCommand) -> bool:
        import task_manager
        from tasks.media import task_manual_update

        return bool(
            task_manager.submit_task(
                task_manual_update,
                f"手动更新: {command.item_name}",
                processor_type="media",
                item_id=command.item_id,
                manual_cast_list=list(command.cast),
                item_name=command.item_name,
            )
        )

    def translate_cast(self, command: CastTranslationCommand) -> Any:
        return self._processor().translate_cast_list_for_editing(
            cast_list=list(command.cast),
            title=command.title,
            year=command.year,
        )

    def preview_item_details(self, item_id: str) -> dict[str, Any] | None:
        import handler.emby as emby

        processor = self._processor()
        details = emby.get_emby_item_details(
            item_id,
            processor.emby_url,
            processor.emby_api_key,
            processor.emby_user_id,
        )
        return cast(dict[str, Any] | None, details)

    def process_preview_cast(
        self,
        item_details: dict[str, Any],
    ) -> tuple[ProcessedCastActor, ...]:
        tmdb_id = str((item_details.get("ProviderIds") or {}).get("Tmdb") or "").strip()
        item_type = str(item_details.get("Type") or "").strip()
        if not tmdb_id or not item_type:
            raise ValueError("preview item does not have a TMDb identity")
        _payload, raw_cast = self._processor()._reconstruct_full_data_from_db(tmdb_id, item_type)
        if not isinstance(raw_cast, Sequence) or isinstance(raw_cast, str | bytes | bytearray):
            raise TypeError("processed cast must be a sequence")
        emby_by_tmdb = {
            str((person.get("ProviderIds") or {}).get("Tmdb")): person.get("Id")
            for person in item_details.get("People", [])
            if isinstance(person, Mapping) and (person.get("ProviderIds") or {}).get("Tmdb")
        }
        result: list[ProcessedCastActor] = []
        for actor in raw_cast:
            if not isinstance(actor, Mapping):
                raise TypeError("processed actor must be an object")
            result.append(
                ProcessedCastActor(
                    emby_person_id=actor.get("emby_person_id")
                    or emby_by_tmdb.get(str(actor.get("tmdb_id") or actor.get("id") or "")),
                    name=actor.get("name") or actor.get("Name"),
                    role=actor.get("character") or actor.get("Role"),
                    imdb_id=actor.get("imdb_id") or actor.get("ImdbId"),
                    douban_id=actor.get("douban_id") or actor.get("DoubanCelebrityId"),
                    tmdb_id=actor.get("tmdb_id") or actor.get("id") or actor.get("TmdbPersonId"),
                )
            )
        return tuple(result)


__all__ = ["LegacyMediaCastEditorGateway"]
