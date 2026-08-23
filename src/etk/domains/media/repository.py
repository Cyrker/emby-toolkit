from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import uuid4

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork

from .models import (
    ImagePolicyKey,
    ImagePolicyRecord,
    MediaIdentity,
    MediaInfoEditContext,
    MediaInfoEpisode,
    MediaItemType,
    MetadataAggregate,
)
from .policies import normalize_media_path

MEDIA_METADATA_SCHEMA_VERSION = 1


class MediaAssetAlignmentError(RuntimeError):
    pass


class MediaIdentityConflictError(RuntimeError):
    pass


class MediaIdentityNotFoundError(RuntimeError):
    pass


class MediaInfoFingerprintNotFoundError(RuntimeError):
    pass


class MediaInfoCacheNotFoundError(RuntimeError):
    pass


class MediaRepository(Protocol):
    def resolve_by_path(
        self,
        path: str,
        requested_type: MediaItemType,
        *,
        season_number: int | None = None,
        episode_number: int | None = None,
    ) -> MediaIdentity | None: ...

    def resolve_by_fingerprint(
        self,
        requested_type: MediaItemType,
        *,
        sha1: str = "",
        pick_code: str = "",
    ) -> MediaIdentity | None: ...

    def resolve_by_emby_id(self, item_id: str) -> MediaIdentity | None: ...

    def load_metadata(
        self,
        identity: MediaIdentity,
        requested_type: MediaItemType,
    ) -> MetadataAggregate | None: ...

    def find_backfill_targets_by_emby_ids(
        self,
        item_ids: Sequence[str],
    ) -> tuple[MediaIdentity, ...]: ...

    def find_backfill_targets_by_library(
        self,
        library_id: str,
    ) -> tuple[MediaIdentity, ...]: ...

    def load_setting(self, key: str) -> Any: ...

    def get_image_policy(self, key: ImagePolicyKey) -> ImagePolicyRecord | None: ...

    def replace_image_policy(
        self,
        key: ImagePolicyKey,
        rules: Sequence[Mapping[str, Any]],
        images: Sequence[Mapping[str, Any]],
    ) -> tuple[Mapping[str, Any], ...]: ...

    def source_is_referenced(self, source_url: str) -> bool: ...

    def bind_emby_item(
        self,
        *,
        item_id: str,
        item_type: MediaItemType,
        item_path: str,
        sha1: str,
        pick_code: str = "",
        tmdb_id: str = "",
        series_id: str = "",
        season_id: str = "",
    ) -> Mapping[str, Any]: ...

    def reserve_emby_event(self, event_key: str, ttl_seconds: int) -> str | None: ...

    def release_emby_event(self, event_key: str, token: str) -> bool: ...


class SqlAlchemyMediaRepository:
    """P5 media read/write owner on the PostgreSQL 18 Unit of Work."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def resolve_by_path(
        self,
        path: str,
        requested_type: MediaItemType,
        *,
        season_number: int | None = None,
        episode_number: int | None = None,
    ) -> MediaIdentity | None:
        normalized_path = normalize_media_path(path)
        if not normalized_path or requested_type is MediaItemType.BOXSET:
            return None
        item_types = ["Movie"] if requested_type is MediaItemType.MOVIE else ["Series", "Season", "Episode"]
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT
                            m.tmdb_id,
                            m.item_type,
                            m.parent_series_tmdb_id,
                            m.season_number,
                            m.episode_number,
                            asset.value AS asset
                        FROM media_metadata AS m
                        CROSS JOIN LATERAL jsonb_array_elements(
                            CASE
                                WHEN jsonb_typeof(m.asset_details_json) = 'array'
                                THEN m.asset_details_json
                                ELSE '[]'::jsonb
                            END
                        ) AS asset(value)
                        WHERE m.item_type = ANY(:item_types)
                          AND asset.value->>'path' IS NOT NULL
                          AND btrim(asset.value->>'path') <> ''
                          AND (
                              lower(asset.value->>'path') = lower(:path)
                              OR lower(asset.value->>'path') LIKE lower(:child_prefix)
                              OR lower(:path) LIKE lower(rtrim(asset.value->>'path', '/')) || '/%'
                          )
                        """
                    ),
                    {
                        "item_types": item_types,
                        "path": normalized_path,
                        "child_prefix": f"{normalized_path.rstrip('/')}/%",
                    },
                )
                .mappings()
                .all()
            )
        candidates = [dict(row) for row in rows]
        if not candidates:
            return None

        def score(row: Mapping[str, Any]) -> tuple[int, int, int, int, int]:
            asset = _mapping(row.get("asset"))
            asset_path = normalize_media_path(asset.get("path"))
            return (
                0 if asset_path.casefold() == normalized_path.casefold() else 1,
                0 if row.get("item_type") == requested_type.value else 1,
                0 if season_number is None or row.get("season_number") in {None, season_number} else 1,
                0 if episode_number is None or row.get("episode_number") in {None, episode_number} else 1,
                abs(len(asset_path) - len(normalized_path)),
            )

        scored = [(score(row), row) for row in candidates]
        best_score = min(candidate_score for candidate_score, _row in scored)
        best_rows = [row for candidate_score, row in scored if candidate_score == best_score]
        best_identities = {
            (
                str(row.get("tmdb_id") or ""),
                str(row.get("item_type") or ""),
                str(row.get("parent_series_tmdb_id") or ""),
                _optional_integer(row.get("season_number")),
                _optional_integer(row.get("episode_number")),
            )
            for row in best_rows
        }
        if len(best_identities) != 1:
            return None
        selected = best_rows[0]
        asset = _mapping(selected.get("asset"))
        selected_type = MediaItemType.parse(selected.get("item_type"))
        if selected_type is None:
            return None
        media_type = "movie" if selected_type is MediaItemType.MOVIE else "tv"
        root_tmdb_id = (
            selected.get("tmdb_id")
            if media_type == "movie"
            else selected.get("parent_series_tmdb_id") or selected.get("tmdb_id")
        )
        if not root_tmdb_id:
            return None
        return MediaIdentity(
            tmdb_id=str(root_tmdb_id),
            media_type=media_type,
            item_type=requested_type,
            season_number=_first_integer(season_number, selected.get("season_number"), asset.get("season_number")),
            episode_number=_first_integer(
                episode_number,
                selected.get("episode_number"),
                asset.get("episode_number"),
            ),
        )

    def resolve_by_fingerprint(
        self,
        requested_type: MediaItemType,
        *,
        sha1: str = "",
        pick_code: str = "",
    ) -> MediaIdentity | None:
        normalized_sha1 = str(sha1 or "").strip().upper()
        normalized_pick_code = str(pick_code or "").strip().casefold()
        if requested_type not in {MediaItemType.MOVIE, MediaItemType.EPISODE}:
            return None
        if not normalized_sha1 and not normalized_pick_code:
            return None
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT
                            tmdb_id,
                            item_type,
                            parent_series_tmdb_id,
                            season_number,
                            episode_number,
                            emby_item_ids_json,
                            asset_details_json,
                            file_sha1_json,
                            file_pickcode_json
                        FROM media_metadata
                        WHERE item_type = :item_type
                          AND (
                              (
                                  :sha1 <> ''
                                  AND file_sha1_json @> CAST(:sha1_json AS jsonb)
                              )
                              OR (
                                  :pick_code <> ''
                                  AND EXISTS (
                                      SELECT 1
                                      FROM jsonb_array_elements_text(
                                          COALESCE(file_pickcode_json, '[]'::jsonb)
                                      ) AS pc(value)
                                      WHERE lower(pc.value) = :pick_code
                                  )
                              )
                          )
                        """
                    ),
                    {
                        "item_type": requested_type.value,
                        "sha1": normalized_sha1,
                        "sha1_json": json.dumps([normalized_sha1]),
                        "pick_code": normalized_pick_code,
                    },
                )
                .mappings()
                .all()
            )
        identities: set[tuple[str, str, int | None, int | None]] = set()
        for raw_row in rows:
            row = dict(raw_row)
            if not row.get("tmdb_id") or _aligned_asset_arrays(row) is None:
                continue
            identities.add(
                (
                    str(row.get("tmdb_id")),
                    str(row.get("parent_series_tmdb_id") or ""),
                    _optional_integer(row.get("season_number")),
                    _optional_integer(row.get("episode_number")),
                )
            )
        if len(identities) != 1:
            return None
        tmdb_id, parent_tmdb_id, season_number, episode_number = identities.pop()
        root_tmdb_id = tmdb_id if requested_type is MediaItemType.MOVIE else parent_tmdb_id
        if not root_tmdb_id:
            return None
        return MediaIdentity(
            tmdb_id=root_tmdb_id,
            media_type="movie" if requested_type is MediaItemType.MOVIE else "tv",
            item_type=requested_type,
            season_number=season_number,
            episode_number=episode_number,
        )

    def resolve_by_emby_id(self, item_id: str) -> MediaIdentity | None:
        normalized_id = str(item_id or "").strip()
        if not normalized_id:
            return None
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, item_type, parent_series_tmdb_id, season_number, episode_number
                        FROM media_metadata
                        WHERE jsonb_exists(emby_item_ids_json, :item_id)
                        ORDER BY last_updated_at DESC NULLS LAST
                        """
                    ),
                    {"item_id": normalized_id},
                )
                .mappings()
                .all()
            )
        if len(rows) != 1:
            return None
        row = rows[0]
        item_type = MediaItemType.parse(row.get("item_type"))
        if item_type is None:
            return None
        media_type = "movie" if item_type is MediaItemType.MOVIE else "tv"
        root_tmdb_id = (
            row.get("tmdb_id") if media_type == "movie" else row.get("parent_series_tmdb_id") or row.get("tmdb_id")
        )
        if not root_tmdb_id:
            return None
        return MediaIdentity(
            tmdb_id=str(root_tmdb_id),
            media_type=media_type,
            item_type=item_type,
            season_number=_optional_integer(row.get("season_number")),
            episode_number=_optional_integer(row.get("episode_number")),
        )

    def load_metadata(
        self,
        identity: MediaIdentity,
        requested_type: MediaItemType,
    ) -> MetadataAggregate | None:
        if requested_type is MediaItemType.BOXSET:
            return None
        with self._unit_of_work() as uow:
            row = self._metadata_row(uow, identity, requested_type)
            if row is None and requested_type is MediaItemType.EPISODE:
                parent = self._root_row(uow, identity.tmdb_id, MediaItemType.SERIES)
                if parent is not None:
                    row = {
                        **parent,
                        "tmdb_id": None,
                        "item_type": MediaItemType.EPISODE.value,
                        "title": None,
                        "overview": None,
                        "season_number": identity.season_number,
                        "episode_number": identity.episode_number,
                        "poster_path": None,
                        "backdrop_path": None,
                        "logo_path": None,
                        "thumb_path": None,
                        "actors_ready": False,
                    }
            if row is None:
                return None
            metadata_row = dict(row)
            actors = self._actors(uow, metadata_row) if metadata_row.get("actors_ready") else ()
            directors = tuple(
                dict(value)
                for value in _json_list(metadata_row.get("directors_json"))
                if isinstance(value, Mapping) and value.get("name")
            )
            collections = self._collections(uow, identity.tmdb_id) if requested_type is MediaItemType.MOVIE else ()
        return MetadataAggregate(
            identity=identity,
            requested_type=requested_type,
            row=metadata_row,
            actors=actors,
            directors=directors,
            collections=collections,
        )

    def find_backfill_targets_by_emby_ids(
        self,
        item_ids: Sequence[str],
    ) -> tuple[MediaIdentity, ...]:
        normalized = tuple(dict.fromkeys(str(value).strip() for value in item_ids if str(value).strip()))
        if not normalized:
            return ()
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, item_type, parent_series_tmdb_id
                        FROM media_metadata
                        WHERE in_library IS TRUE
                          AND emby_item_ids_json ?| CAST(:item_ids AS text[])
                        """
                    ),
                    {"item_ids": list(normalized)},
                )
                .mappings()
                .all()
            )
            targets = self._root_targets(dict(row) for row in rows)
            result = tuple(target for target in targets if self._needs_backfill(uow, target))
        return result

    def find_backfill_targets_by_library(
        self,
        library_id: str,
    ) -> tuple[MediaIdentity, ...]:
        normalized_id = str(library_id or "").strip()
        if not normalized_id:
            return ()
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT DISTINCT root.tmdb_id, root.item_type
                        FROM media_metadata AS root
                        WHERE root.in_library IS TRUE
                          AND root.item_type IN ('Movie', 'Series')
                          AND (
                              EXISTS (
                                  SELECT 1
                                  FROM jsonb_array_elements(
                                      COALESCE(root.asset_details_json, '[]'::jsonb)
                                  ) AS asset
                                  WHERE asset->>'source_library_id' = :library_id
                              )
                              OR (
                                  root.item_type = 'Series'
                                  AND EXISTS (
                                      SELECT 1
                                      FROM media_metadata AS episode
                                      CROSS JOIN LATERAL jsonb_array_elements(
                                          COALESCE(episode.asset_details_json, '[]'::jsonb)
                                      ) AS asset
                                      WHERE episode.parent_series_tmdb_id = root.tmdb_id
                                        AND episode.item_type = 'Episode'
                                        AND episode.in_library IS TRUE
                                        AND asset->>'source_library_id' = :library_id
                                  )
                              )
                          )
                        """
                    ),
                    {"library_id": normalized_id},
                )
                .mappings()
                .all()
            )
            targets = self._root_targets(dict(row) for row in rows)
            result = tuple(target for target in targets if self._needs_backfill(uow, target))
        return result

    def resolve_media_info_context(self, item_id: str) -> MediaInfoEditContext:
        normalized_item_id = str(item_id or "").strip()
        if not normalized_item_id:
            raise MediaIdentityNotFoundError("Emby ItemID is required")
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT title, item_type, parent_series_tmdb_id,
                               season_number, episode_number,
                               emby_item_ids_json, asset_details_json,
                               file_sha1_json, file_pickcode_json
                        FROM media_metadata
                        WHERE emby_item_ids_json @> CAST(:item_ids AS jsonb)
                        ORDER BY item_type, tmdb_id
                        """
                    ),
                    {"item_ids": json.dumps([normalized_item_id])},
                )
                .mappings()
                .all()
            )
            if not rows:
                raise MediaIdentityNotFoundError("media record was not found for Emby ItemID")
            if len(rows) != 1:
                raise MediaIdentityConflictError("Emby ItemID matches more than one media record")
            row = dict(rows[0])
            aligned = _aligned_asset_arrays(row)
            if aligned is None:
                raise MediaAssetAlignmentError("media asset version arrays are not position-aligned")
            assets, emby_ids, sha1s, _pick_codes = aligned
            matched_indices = {
                index
                for index, (asset, emby_id) in enumerate(zip(assets, emby_ids, strict=True))
                if emby_id == normalized_item_id
                or str(_mapping(asset).get("emby_item_id") or "").strip() == normalized_item_id
            }
            if len(matched_indices) != 1:
                raise MediaIdentityConflictError("Emby ItemID does not identify exactly one asset version")
            target_index = matched_indices.pop()
            asset_item_id = str(_mapping(assets[target_index]).get("emby_item_id") or "").strip()
            slot_item_id = emby_ids[target_index]
            if asset_item_id and slot_item_id and asset_item_id != slot_item_id:
                raise MediaAssetAlignmentError("asset detail ItemID conflicts with the aligned ItemID slot")
            sha1 = sha1s[target_index]
            if not sha1:
                raise MediaInfoFingerprintNotFoundError("media asset does not have an SHA1 fingerprint")
            mediainfo = uow.session.scalar(
                text(
                    """
                    SELECT mediainfo_json
                    FROM p115_mediainfo_cache
                    WHERE upper(sha1) = :sha1
                    LIMIT 1
                    """
                ),
                {"sha1": sha1},
            )
        normalized_mediainfo = _mediainfo_value(mediainfo)
        if normalized_mediainfo is None:
            raise MediaInfoCacheNotFoundError("formatted MediaInfo cache was not found")
        return MediaInfoEditContext(
            item_id=normalized_item_id,
            sha1=sha1,
            mediainfo=normalized_mediainfo,
            title=str(row.get("title") or ""),
            item_type=str(row.get("item_type") or ""),
            parent_series_tmdb_id=str(row.get("parent_series_tmdb_id") or ""),
            season_number=_optional_integer(row.get("season_number")),
            episode_number=_optional_integer(row.get("episode_number")),
        )

    def list_series_media_info_episodes(self, series_id: str) -> tuple[MediaInfoEpisode, ...]:
        normalized_series_id = str(series_id or "").strip()
        if not normalized_series_id:
            raise MediaIdentityNotFoundError("Emby Series ItemID is required")
        with self._unit_of_work() as uow:
            series_rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id
                        FROM media_metadata
                        WHERE item_type = 'Series'
                          AND emby_item_ids_json @> CAST(:item_ids AS jsonb)
                        ORDER BY tmdb_id
                        """
                    ),
                    {"item_ids": json.dumps([normalized_series_id])},
                )
                .scalars()
                .all()
            )
            if not series_rows:
                raise MediaIdentityNotFoundError("series record was not found for Emby ItemID")
            if len(series_rows) != 1:
                raise MediaIdentityConflictError("Emby Series ItemID matches more than one media record")
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT title, season_number, episode_number, emby_item_ids_json
                        FROM media_metadata
                        WHERE parent_series_tmdb_id = :series_tmdb_id
                          AND item_type = 'Episode'
                          AND in_library = TRUE
                        ORDER BY season_number ASC NULLS LAST,
                                 episode_number ASC NULLS LAST,
                                 tmdb_id
                        """
                    ),
                    {"series_tmdb_id": str(series_rows[0])},
                )
                .mappings()
                .all()
            )
        episodes: list[MediaInfoEpisode] = []
        for row in rows:
            emby_id = next(
                (
                    str(value or "").strip()
                    for value in _json_list(row.get("emby_item_ids_json"))
                    if str(value or "").strip()
                ),
                "",
            )
            if not emby_id:
                continue
            episodes.append(
                MediaInfoEpisode(
                    emby_id=emby_id,
                    season_number=_optional_integer(row.get("season_number")),
                    episode_number=_optional_integer(row.get("episode_number")),
                    title=str(row.get("title") or ""),
                )
            )
        return tuple(episodes)

    def load_setting(self, key: str) -> Any:
        with self._unit_of_work() as uow:
            return uow.session.scalar(
                text("SELECT value_json FROM app_settings WHERE setting_key = :key"),
                {"key": str(key)},
            )

    def get_image_policy(self, key: ImagePolicyKey) -> ImagePolicyRecord | None:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT policy_json, images_json
                        FROM media_image_policy_cache
                        WHERE item_type = :item_type
                          AND tmdb_id = :tmdb_id
                          AND season_number = :season_number
                          AND episode_number = :episode_number
                        """
                    ),
                    _image_key_parameters(key),
                )
                .mappings()
                .first()
            )
        if row is None:
            return None
        return ImagePolicyRecord(
            key=key,
            rules=tuple(dict(value) for value in _json_list(row.get("policy_json")) if isinstance(value, Mapping)),
            images=tuple(dict(value) for value in _json_list(row.get("images_json")) if isinstance(value, Mapping)),
        )

    def replace_image_policy(
        self,
        key: ImagePolicyKey,
        rules: Sequence[Mapping[str, Any]],
        images: Sequence[Mapping[str, Any]],
    ) -> tuple[Mapping[str, Any], ...]:
        parameters = _image_key_parameters(key)
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT images_json
                        FROM media_image_policy_cache
                        WHERE item_type = :item_type
                          AND tmdb_id = :tmdb_id
                          AND season_number = :season_number
                          AND episode_number = :episode_number
                        FOR UPDATE
                        """
                    ),
                    parameters,
                )
                .mappings()
                .first()
            )
            previous = tuple(
                dict(value)
                for value in _json_list(row.get("images_json") if row else None)
                if isinstance(value, Mapping)
            )
            uow.session.execute(
                text(
                    """
                    INSERT INTO media_image_policy_cache (
                        item_type,
                        tmdb_id,
                        season_number,
                        episode_number,
                        policy_json,
                        images_json,
                        updated_at
                    ) VALUES (
                        :item_type,
                        :tmdb_id,
                        :season_number,
                        :episode_number,
                        CAST(:policy_json AS jsonb),
                        CAST(:images_json AS jsonb),
                        NOW()
                    )
                    ON CONFLICT (item_type, tmdb_id, season_number, episode_number)
                    DO UPDATE SET
                        policy_json = EXCLUDED.policy_json,
                        images_json = EXCLUDED.images_json,
                        updated_at = NOW()
                    """
                ),
                {
                    **parameters,
                    "policy_json": json.dumps(list(rules), ensure_ascii=False),
                    "images_json": json.dumps(list(images), ensure_ascii=False),
                },
            )
            uow.commit()
        return previous

    def source_is_referenced(self, source_url: str) -> bool:
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text(
                    """
                    SELECT EXISTS(
                        SELECT 1
                        FROM media_image_policy_cache
                        WHERE images_json @> CAST(:value AS jsonb)
                    )
                    """
                ),
                {"value": json.dumps([{"source_url": str(source_url or "").strip()}])},
            )
        return bool(value)

    def bind_emby_item(
        self,
        *,
        item_id: str,
        item_type: MediaItemType,
        item_path: str,
        sha1: str,
        pick_code: str = "",
        tmdb_id: str = "",
        series_id: str = "",
        season_id: str = "",
    ) -> Mapping[str, Any]:
        normalized_item_id = str(item_id or "").strip()
        raw_item_path = str(item_path or "").strip()
        normalized_path = normalize_media_path(raw_item_path).casefold()
        normalized_sha1 = str(sha1 or "").strip().upper()
        normalized_pick_code = str(pick_code or "").strip().casefold()
        normalized_tmdb_id = str(tmdb_id or "").strip()
        if (
            not normalized_item_id
            or item_type not in {MediaItemType.MOVIE, MediaItemType.EPISODE}
            or not (normalized_sha1 or normalized_pick_code)
        ):
            return {"updated": False, "reason": "invalid_identity"}

        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT
                            tmdb_id,
                            item_type,
                            title,
                            parent_series_tmdb_id,
                            season_number,
                            emby_item_ids_json,
                            asset_details_json,
                            file_sha1_json,
                            file_pickcode_json
                        FROM media_metadata
                        WHERE item_type = :item_type
                          AND (
                              (
                                  :sha1 <> ''
                                  AND file_sha1_json @> CAST(:sha1_json AS jsonb)
                              )
                              OR (
                                  :pick_code <> ''
                                  AND EXISTS (
                                      SELECT 1
                                      FROM jsonb_array_elements_text(
                                          COALESCE(file_pickcode_json, '[]'::jsonb)
                                      ) AS pc(value)
                                      WHERE lower(pc.value) = :pick_code
                                  )
                              )
                          )
                        FOR UPDATE
                        """
                    ),
                    {
                        "item_type": item_type.value,
                        "sha1": normalized_sha1,
                        "sha1_json": json.dumps([normalized_sha1]),
                        "pick_code": normalized_pick_code,
                    },
                )
                .mappings()
                .all()
            )
            matches: list[tuple[int, dict[str, Any], int, list[Any]]] = []
            unaligned_rows = False
            for raw_row in rows:
                row = dict(raw_row)
                aligned = _aligned_asset_arrays(row)
                if aligned is None:
                    unaligned_rows = True
                    continue
                assets, _emby_ids, sha1s, pick_codes = aligned
                for index in range(len(assets)):
                    asset = assets[index] if index < len(assets) and isinstance(assets[index], Mapping) else {}
                    path_matches = bool(
                        normalized_path and normalize_media_path(asset.get("path")).casefold() == normalized_path
                    )
                    sha1_matches = bool(normalized_sha1 and index < len(sha1s) and sha1s[index] == normalized_sha1)
                    pick_matches = bool(
                        normalized_pick_code and index < len(pick_codes) and pick_codes[index] == normalized_pick_code
                    )
                    if path_matches and (sha1_matches or pick_matches):
                        matches.append((0, row, index, assets))
                    elif (pick_matches and (not normalized_sha1 or sha1_matches)) or (
                        not normalized_pick_code and sha1_matches
                    ):
                        matches.append((1, row, index, assets))

            restored_identity = False
            if not matches and normalized_tmdb_id:
                fallback = (
                    uow.session.execute(
                        text(
                            """
                            SELECT
                                tmdb_id,
                                item_type,
                                title,
                                parent_series_tmdb_id,
                                season_number,
                                emby_item_ids_json,
                                asset_details_json,
                                file_sha1_json,
                                file_pickcode_json
                            FROM media_metadata
                            WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                            FOR UPDATE
                            """
                        ),
                        {"tmdb_id": normalized_tmdb_id, "item_type": item_type.value},
                    )
                    .mappings()
                    .first()
                )
                if fallback is not None:
                    row = dict(fallback)
                    aligned = _aligned_asset_arrays(row)
                    if aligned is None:
                        return {"updated": False, "reason": "unaligned_asset_versions"}
                    assets, _emby_ids, _sha1s, _pick_codes = aligned
                    matched_index = next(
                        (
                            index
                            for index, asset in enumerate(assets)
                            if isinstance(asset, Mapping)
                            and normalized_path
                            and normalize_media_path(asset.get("path")).casefold() == normalized_path
                        ),
                        None,
                    )
                    if matched_index is None:
                        matched_index = max(
                            len(_json_list(row.get("emby_item_ids_json"))),
                            len(assets),
                            len(_json_list(row.get("file_sha1_json"))),
                            len(_json_list(row.get("file_pickcode_json"))),
                        )
                    matches.append((0, row, matched_index, assets))
                    restored_identity = True
            if not matches:
                if unaligned_rows:
                    return {"updated": False, "reason": "unaligned_asset_versions"}
                return {"updated": False, "reason": "asset_not_found"}
            matches.sort(key=lambda value: value[0])
            best_score = matches[0][0]
            best_matches = [value for value in matches if value[0] == best_score]
            if len(best_matches) != 1:
                return {"updated": False, "reason": "ambiguous_asset"}

            _, row, index, assets = best_matches[0]
            emby_ids = [str(value or "").strip() for value in _json_list(row.get("emby_item_ids_json"))]
            sha1s = [str(value or "").strip().upper() for value in _json_list(row.get("file_sha1_json"))]
            pick_codes = [str(value or "").strip().casefold() for value in _json_list(row.get("file_pickcode_json"))]
            while len(emby_ids) <= index:
                asset_id = ""
                if len(assets) > len(emby_ids) and isinstance(assets[len(emby_ids)], Mapping):
                    asset_id = str(assets[len(emby_ids)].get("emby_item_id") or "").strip()
                emby_ids.append(asset_id)
            while len(assets) <= index:
                assets.append({})
            while len(sha1s) <= index:
                sha1s.append("")
            while len(pick_codes) <= index:
                pick_codes.append("")
            if normalized_item_id in emby_ids and emby_ids[index] != normalized_item_id:
                return {"updated": False, "reason": "item_id_conflict"}

            changed = emby_ids[index] != normalized_item_id
            emby_ids[index] = normalized_item_id
            asset = dict(assets[index]) if isinstance(assets[index], Mapping) else {}
            changed = changed or str(asset.get("emby_item_id") or "").strip() != normalized_item_id
            asset["emby_item_id"] = normalized_item_id
            if normalized_path and not asset.get("path"):
                asset["path"] = raw_item_path
                changed = True
            assets[index] = asset
            if restored_identity:
                if normalized_sha1 and sha1s[index] != normalized_sha1:
                    sha1s[index] = normalized_sha1
                    changed = True
                if normalized_pick_code and pick_codes[index] != normalized_pick_code:
                    pick_codes[index] = normalized_pick_code
                    changed = True

            uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET emby_item_ids_json = CAST(:emby_ids AS jsonb),
                        asset_details_json = CAST(:assets AS jsonb),
                        file_sha1_json = CAST(:sha1s AS jsonb),
                        file_pickcode_json = CAST(:pick_codes AS jsonb),
                        in_library = TRUE,
                        last_synced_at = NOW(),
                        last_updated_at = NOW()
                    WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                    """
                ),
                {
                    "emby_ids": json.dumps(emby_ids, ensure_ascii=False),
                    "assets": json.dumps(assets, ensure_ascii=False),
                    "sha1s": json.dumps(sha1s, ensure_ascii=False),
                    "pick_codes": json.dumps(pick_codes, ensure_ascii=False),
                    "tmdb_id": row["tmdb_id"],
                    "item_type": row["item_type"],
                },
            )
            parent_updates = 0
            if item_type is MediaItemType.EPISODE and row.get("parent_series_tmdb_id"):
                parent_tmdb_id = str(row["parent_series_tmdb_id"])
                if series_id:
                    result = uow.session.execute(
                        text(
                            """
                            UPDATE media_metadata
                            SET emby_item_ids_json = CAST(:ids AS jsonb),
                                in_library = TRUE,
                                last_synced_at = NOW(),
                                last_updated_at = NOW()
                            WHERE tmdb_id = :tmdb_id
                              AND item_type = 'Series'
                              AND emby_item_ids_json IS DISTINCT FROM CAST(:ids AS jsonb)
                            """
                        ),
                        {"ids": json.dumps([str(series_id)]), "tmdb_id": parent_tmdb_id},
                    )
                    parent_updates += int(getattr(result, "rowcount", 0) or 0)
                if season_id and row.get("season_number") is not None:
                    result = uow.session.execute(
                        text(
                            """
                            UPDATE media_metadata
                            SET emby_item_ids_json = CAST(:ids AS jsonb),
                                in_library = TRUE,
                                last_synced_at = NOW(),
                                last_updated_at = NOW()
                            WHERE parent_series_tmdb_id = :tmdb_id
                              AND item_type = 'Season'
                              AND season_number = :season_number
                              AND emby_item_ids_json IS DISTINCT FROM CAST(:ids AS jsonb)
                            """
                        ),
                        {
                            "ids": json.dumps([str(season_id)]),
                            "tmdb_id": parent_tmdb_id,
                            "season_number": row["season_number"],
                        },
                    )
                    parent_updates += int(getattr(result, "rowcount", 0) or 0)
            uow.commit()
        return {
            "updated": changed,
            "parent_updates": parent_updates,
            "tmdb_id": str(row["tmdb_id"]),
            "title": str(row.get("title") or ""),
            "identity_restored": restored_identity,
        }

    def reserve_emby_event(self, event_key: str, ttl_seconds: int) -> str | None:
        normalized_key = str(event_key or "").strip()
        ttl = int(ttl_seconds)
        if not normalized_key or ttl < 1:
            raise ValueError("Emby event reservation requires a key and positive TTL")
        setting_key = f"p5:emby-event:{normalized_key}"
        token = uuid4().hex
        with self._unit_of_work() as uow:
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:setting_key, 0))"),
                {"setting_key": setting_key},
            )
            uow.session.execute(
                text(
                    """
                    DELETE FROM app_settings
                    WHERE setting_key LIKE 'p5:emby-event:%'
                      AND last_updated_at < CURRENT_TIMESTAMP - INTERVAL '1 hour'
                    """
                )
            )
            now = uow.session.scalar(text("SELECT CURRENT_TIMESTAMP"))
            current = uow.session.scalar(
                text("SELECT value_json FROM app_settings WHERE setting_key = :setting_key"),
                {"setting_key": setting_key},
            )
            if isinstance(now, datetime) and isinstance(current, Mapping):
                expires_at = _timestamp(current.get("expires_at"))
                if expires_at is not None and expires_at > now.astimezone(UTC):
                    return None
            expires_at = (now if isinstance(now, datetime) else datetime.now(UTC)) + timedelta(seconds=ttl)
            uow.session.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES (
                        :setting_key,
                        CAST(:value_json AS jsonb),
                        CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (setting_key) DO UPDATE SET
                        value_json = EXCLUDED.value_json,
                        last_updated_at = CURRENT_TIMESTAMP
                    """
                ),
                {
                    "setting_key": setting_key,
                    "value_json": json.dumps(
                        {"token": token, "expires_at": expires_at.astimezone(UTC).isoformat()},
                        separators=(",", ":"),
                    ),
                },
            )
            uow.commit()
        return token

    def release_emby_event(self, event_key: str, token: str) -> bool:
        setting_key = f"p5:emby-event:{str(event_key or '').strip()}"
        normalized_token = str(token or "").strip()
        if not normalized_token:
            return False
        with self._unit_of_work() as uow:
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:setting_key, 0))"),
                {"setting_key": setting_key},
            )
            result = uow.session.execute(
                text(
                    """
                    DELETE FROM app_settings
                    WHERE setting_key = :setting_key
                      AND value_json->>'token' = :token
                    """
                ),
                {"setting_key": setting_key, "token": normalized_token},
            )
            uow.commit()
        return bool(int(getattr(result, "rowcount", 0) or 0))

    @staticmethod
    def _metadata_row(
        uow: SqlAlchemyUnitOfWork,
        identity: MediaIdentity,
        requested_type: MediaItemType,
    ) -> dict[str, Any] | None:
        if requested_type is MediaItemType.MOVIE and identity.media_type == "movie":
            return SqlAlchemyMediaRepository._root_row(uow, identity.tmdb_id, MediaItemType.MOVIE)
        if requested_type is MediaItemType.SEASON:
            statement = text(
                """
                SELECT *
                FROM media_metadata
                WHERE parent_series_tmdb_id = :tmdb_id
                  AND item_type = 'Season'
                  AND season_number = :season_number
                  AND metadata_ready IS TRUE
                ORDER BY last_updated_at DESC NULLS LAST
                LIMIT 1
                """
            )
            parameters = {"tmdb_id": identity.tmdb_id, "season_number": identity.season_number}
        elif requested_type is MediaItemType.EPISODE:
            statement = text(
                """
                SELECT *
                FROM media_metadata
                WHERE parent_series_tmdb_id = :tmdb_id
                  AND item_type = 'Episode'
                  AND season_number = :season_number
                  AND episode_number = :episode_number
                  AND metadata_ready IS TRUE
                ORDER BY last_updated_at DESC NULLS LAST
                LIMIT 1
                """
            )
            parameters = {
                "tmdb_id": identity.tmdb_id,
                "season_number": identity.season_number,
                "episode_number": identity.episode_number,
            }
        else:
            return SqlAlchemyMediaRepository._root_row(uow, identity.tmdb_id, MediaItemType.SERIES)
        row = uow.session.execute(statement, parameters).mappings().first()
        return dict(row) if row is not None else None

    @staticmethod
    def _root_row(
        uow: SqlAlchemyUnitOfWork,
        tmdb_id: str,
        item_type: MediaItemType,
    ) -> dict[str, Any] | None:
        row = (
            uow.session.execute(
                text(
                    """
                    SELECT *
                    FROM media_metadata
                    WHERE tmdb_id = :tmdb_id
                      AND item_type = :item_type
                      AND metadata_ready IS TRUE
                    """
                ),
                {"tmdb_id": tmdb_id, "item_type": item_type.value},
            )
            .mappings()
            .first()
        )
        return dict(row) if row is not None else None

    @staticmethod
    def _actors(
        uow: SqlAlchemyUnitOfWork,
        row: Mapping[str, Any],
    ) -> tuple[Mapping[str, Any], ...]:
        links = [dict(value) for value in _json_list(row.get("actors_json")) if isinstance(value, Mapping)]
        actor_ids = [value.get("tmdb_id") for value in links if value.get("tmdb_id") is not None]
        if not actor_ids:
            return ()
        people = (
            uow.session.execute(
                text(
                    """
                    SELECT tmdb_person_id, primary_name, profile_path
                    FROM person_metadata
                    WHERE tmdb_person_id = ANY(:actor_ids)
                    """
                ),
                {"actor_ids": actor_ids},
            )
            .mappings()
            .all()
        )
        by_id = {value.get("tmdb_person_id"): value for value in people}
        result: list[Mapping[str, Any]] = []
        for link in links:
            person = by_id.get(link.get("tmdb_id"))
            if person is not None:
                result.append(
                    {
                        "tmdb_id": person.get("tmdb_person_id"),
                        "name": person.get("primary_name"),
                        "profile_path": person.get("profile_path"),
                        "character": link.get("character"),
                        "order": link.get("order", 999),
                    }
                )
        return tuple(result)

    @staticmethod
    def _collections(
        uow: SqlAlchemyUnitOfWork,
        tmdb_id: str,
    ) -> tuple[Mapping[str, Any], ...]:
        row = (
            uow.session.execute(
                text(
                    """
                    SELECT tmdb_collection_id, name, all_tmdb_ids_json
                    FROM collections_info
                    WHERE all_tmdb_ids_json @> CAST(:tmdb_ids AS jsonb)
                    ORDER BY last_checked_at DESC NULLS LAST
                    LIMIT 1
                    """
                ),
                {"tmdb_ids": json.dumps([str(tmdb_id)])},
            )
            .mappings()
            .first()
        )
        if row is None or not row.get("tmdb_collection_id") or not row.get("name"):
            return ()
        return (
            {
                "tmdb_id": str(row["tmdb_collection_id"]),
                "name": row["name"],
                "member_tmdb_ids": [
                    str(value) for value in _json_list(row.get("all_tmdb_ids_json")) if value is not None
                ],
            },
        )

    @staticmethod
    def _root_targets(rows: Iterable[Mapping[str, Any]]) -> tuple[MediaIdentity, ...]:
        result: dict[tuple[str, str], MediaIdentity] = {}
        for row in rows:
            item_type = MediaItemType.parse(row.get("item_type"))
            if item_type is None:
                continue
            if item_type is MediaItemType.MOVIE:
                tmdb_id = row.get("tmdb_id")
                media_type = "movie"
                root_type = MediaItemType.MOVIE
            else:
                tmdb_id = row.get("parent_series_tmdb_id") or row.get("tmdb_id")
                media_type = "tv"
                root_type = MediaItemType.SERIES
            if tmdb_id:
                result[(str(tmdb_id), media_type)] = MediaIdentity(
                    str(tmdb_id),
                    media_type,
                    root_type,
                )
        return tuple(result.values())

    @staticmethod
    def _needs_backfill(
        uow: SqlAlchemyUnitOfWork,
        identity: MediaIdentity,
    ) -> bool:
        item_type = MediaItemType.SERIES if identity.media_type == "tv" else MediaItemType.MOVIE
        value = uow.session.scalar(
            text(
                """
                SELECT EXISTS(
                    SELECT 1
                    FROM media_metadata
                    WHERE tmdb_id = :tmdb_id
                      AND item_type = :item_type
                      AND in_library IS TRUE
                      AND (
                          metadata_schema_version < :schema_version
                          OR NULLIF(btrim(title), '') IS NULL
                          OR NULLIF(btrim(overview), '') IS NULL
                          OR release_year IS NULL
                          OR NULLIF(
                              btrim(COALESCE(
                                  official_rating_json->>'US',
                                  official_rating_json->>'us',
                                  ''
                              )),
                              ''
                          ) IS NULL
                          OR genres_json IS NULL
                          OR genres_json = '[]'::jsonb
                          OR (
                              item_type = 'Series'
                              AND EXISTS (
                                  SELECT 1
                                  FROM media_metadata AS episode
                                  WHERE episode.parent_series_tmdb_id = media_metadata.tmdb_id
                                    AND episode.item_type = 'Episode'
                                    AND episode.in_library IS TRUE
                                    AND episode.season_number = 0
                                    AND episode.tmdb_id LIKE media_metadata.tmdb_id || '-S0E%'
                              )
                          )
                      )
                )
                """
            ),
            {
                "tmdb_id": identity.tmdb_id,
                "item_type": item_type.value,
                "schema_version": MEDIA_METADATA_SCHEMA_VERSION,
            },
        )
        return bool(value)


def _image_key_parameters(key: ImagePolicyKey) -> dict[str, Any]:
    return {
        "item_type": key.item_type.value,
        "tmdb_id": key.tmdb_id,
        "season_number": key.season_number,
        "episode_number": key.episode_number,
    }


def _json_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _aligned_asset_arrays(
    row: Mapping[str, Any],
) -> tuple[list[Any], list[str], list[str], list[str]] | None:
    assets = _json_list(row.get("asset_details_json"))
    emby_ids = [str(value or "").strip() for value in _json_list(row.get("emby_item_ids_json"))]
    sha1s = [str(value or "").strip().upper() for value in _json_list(row.get("file_sha1_json"))]
    pick_codes = [str(value or "").strip().casefold() for value in _json_list(row.get("file_pickcode_json"))]
    if len({len(assets), len(emby_ids), len(sha1s), len(pick_codes)}) != 1:
        return None
    if any(not isinstance(asset, Mapping) for asset in assets):
        return None
    return assets, emby_ids, sha1s, pick_codes


def _mediainfo_value(
    value: Any,
) -> Mapping[str, Any] | tuple[Mapping[str, Any], ...] | None:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, list) and value and all(isinstance(item, Mapping) for item in value):
        return tuple(dict(item) for item in value)
    return None


def _optional_integer(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _first_integer(*values: Any) -> int | None:
    for value in values:
        parsed = _optional_integer(value)
        if parsed is not None:
            return parsed
    return None


def _timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value))
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


__all__ = [
    "MEDIA_METADATA_SCHEMA_VERSION",
    "MediaAssetAlignmentError",
    "MediaIdentityConflictError",
    "MediaIdentityNotFoundError",
    "MediaInfoCacheNotFoundError",
    "MediaInfoFingerprintNotFoundError",
    "MediaRepository",
    "SqlAlchemyMediaRepository",
]
