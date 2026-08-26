from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text

from etk.platform.config.locking import DYNAMIC_CONFIG_LOCK_NAMESPACE, P115_AUTH_LOCK_KEY
from etk.platform.db import SqlAlchemyUnitOfWork

from .fingerprints import (
    P115FileFingerprint,
    P115FingerprintAccountChanged,
    P115FingerprintConflict,
    P115FingerprintRepairRow,
    canonical_sha1,
)
from .media_info import (
    P115MediaInfoCacheRecord,
    P115MediaInfoRestoreTarget,
    P115MediaProbeItemStatus,
    P115MediaProbeReservation,
    P115MediaProbeTarget,
)


class SqlAlchemyP115MediaInfoRepository:
    """P6.6 owner for MediaInfo cache and account-bound fingerprint repair transactions."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def find_by_sha1(
        self,
        sha1: str,
        *,
        record_hit: bool = False,
    ) -> P115MediaInfoCacheRecord | None:
        normalized_sha1 = canonical_sha1(sha1)
        row: Mapping[str, Any] | None = None
        with self._unit_of_work() as uow:
            if record_hit:
                found = (
                    uow.session.execute(
                        text(
                            """
                            UPDATE p115_mediainfo_cache
                            SET hit_count = hit_count + 1
                            WHERE sha1 = :sha1
                            RETURNING sha1, mediainfo_json, raw_ffprobe_json,
                                      cache_revision, raw_schema_version,
                                      formatter_revision, probe_revision, hit_count
                            """
                        ),
                        {"sha1": normalized_sha1},
                    )
                    .mappings()
                    .one_or_none()
                )
                row = dict(found) if found is not None else None
                if found is not None:
                    uow.commit()
            else:
                found = (
                    uow.session.execute(
                        text(
                            """
                            SELECT sha1, mediainfo_json, raw_ffprobe_json,
                                   cache_revision, raw_schema_version,
                                   formatter_revision, probe_revision, hit_count
                            FROM p115_mediainfo_cache
                            WHERE sha1 = :sha1
                            """
                        ),
                        {"sha1": normalized_sha1},
                    )
                    .mappings()
                    .one_or_none()
                )
                row = dict(found) if found is not None else None
        return _cache_record(row) if row is not None else None

    def resolve_sha1_by_pick_code(self, pick_code: str, *, account_id: str) -> str | None:
        normalized_pick_code = str(pick_code or "").strip()
        normalized_account_id = str(account_id or "").strip()
        if not normalized_pick_code or not normalized_account_id.isdigit():
            return None
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT id, sha1
                        FROM p115_filesystem_cache
                        WHERE account_id = :account_id
                          AND entry_kind = 'file'
                          AND lower(pick_code) = lower(:pick_code)
                          AND sha1 IS NOT NULL
                        ORDER BY id
                        """
                    ),
                    {
                        "account_id": normalized_account_id,
                        "pick_code": normalized_pick_code,
                    },
                )
                .mappings()
                .all()
            )
        if len(rows) != 1:
            return None
        try:
            return canonical_sha1(rows[0].get("sha1"))
        except ValueError:
            return None

    def replace_formatted_if_revision(
        self,
        sha1: str,
        *,
        expected_cache_revision: int,
        formatted: object,
    ) -> P115MediaInfoCacheRecord | None:
        """Replace only the formatted cache value when its revision is unchanged."""
        normalized_sha1 = canonical_sha1(sha1)
        if isinstance(expected_cache_revision, bool) or expected_cache_revision < 0:
            raise ValueError("expected cache revision must be non-negative")
        formatted_json = json.dumps(formatted, ensure_ascii=False, separators=(",", ":"))
        with self._unit_of_work() as uow:
            found = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_mediainfo_cache
                        SET mediainfo_json = CAST(:formatted AS jsonb),
                            cache_revision = cache_revision + 1,
                            updated_at = NOW()
                        WHERE sha1 = :sha1
                          AND cache_revision = :expected_cache_revision
                        RETURNING sha1, mediainfo_json, raw_ffprobe_json,
                                  cache_revision, raw_schema_version,
                                  formatter_revision, probe_revision, hit_count
                        """
                    ),
                    {
                        "sha1": normalized_sha1,
                        "expected_cache_revision": expected_cache_revision,
                        "formatted": formatted_json,
                    },
                )
                .mappings()
                .one_or_none()
            )
            row = dict(found) if found is not None else None
            if found is not None:
                uow.commit()
        return _cache_record(row) if row is not None else None

    def list_restore_targets(self, *, account_id: str) -> tuple[P115MediaInfoRestoreTarget, ...]:
        normalized_account_id = str(account_id or "").strip()
        if not normalized_account_id.isdigit():
            raise ValueError("MediaInfo restore requires a stable numeric account ID")
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, item_type, title, original_language,
                               parent_series_tmdb_id, season_number, episode_number,
                               emby_item_ids_json, asset_details_json,
                               file_sha1_json, file_pickcode_json
                        FROM media_metadata
                        WHERE in_library = TRUE
                          AND item_type IN ('Movie', 'Episode')
                          AND asset_details_json IS NOT NULL
                        ORDER BY item_type, title NULLS LAST, tmdb_id
                        """
                    )
                )
                .mappings()
                .all()
            )
            pick_codes = tuple(
                dict.fromkeys(
                    _repair_text(value)
                    for row in rows
                    for value in _json_list(row.get("file_pickcode_json"))
                    if _repair_text(value)
                )
            )
            cache_rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT pick_code, name, size, sha1
                        FROM p115_filesystem_cache
                        WHERE account_id = :account_id
                          AND entry_kind = 'file'
                          AND pick_code = ANY(CAST(:pick_codes AS text[]))
                        ORDER BY id
                        """
                    ),
                    {"account_id": normalized_account_id, "pick_codes": list(pick_codes)},
                )
                .mappings()
                .all()
                if pick_codes
                else []
            )
        cache_by_pick_code: dict[str, tuple[str, int, str]] = {}
        ambiguous: set[str] = set()
        for cache_row in cache_rows:
            pick_code = str(cache_row.get("pick_code") or "").strip()
            try:
                cached_sha1 = canonical_sha1(cache_row.get("sha1"))
            except ValueError:
                ambiguous.add(pick_code)
                continue
            value = (
                str(cache_row.get("name") or "").strip(),
                int(cache_row.get("size") or 0),
                cached_sha1,
            )
            if pick_code in cache_by_pick_code and cache_by_pick_code[pick_code] != value:
                ambiguous.add(pick_code)
            else:
                cache_by_pick_code[pick_code] = value

        targets: list[P115MediaInfoRestoreTarget] = []
        for row in rows:
            assets = _json_list(row.get("asset_details_json"))
            emby_ids = [_repair_text(value) for value in _json_list(row.get("emby_item_ids_json"))]
            sha1s = [_repair_sha1(value) for value in _json_list(row.get("file_sha1_json"))]
            row_pick_codes = [_repair_text(value) for value in _json_list(row.get("file_pickcode_json"))]
            if len({len(assets), len(emby_ids), len(sha1s), len(row_pick_codes)}) != 1:
                raise P115FingerprintConflict("MediaInfo restore asset arrays are not position-aligned")
            for index, asset in enumerate(assets):
                if not isinstance(asset, Mapping):
                    raise P115FingerprintConflict("MediaInfo restore asset details contain a non-object slot")
                path = str(asset.get("path") or asset.get("Path") or "").strip()
                emby_item_id = emby_ids[index] or str(asset.get("emby_item_id") or "").strip()
                sha1 = sha1s[index]
                pick_code = row_pick_codes[index]
                if not path or not emby_item_id or not sha1 or not pick_code or pick_code in ambiguous:
                    continue
                cached_name, cached_size, cached_sha1 = cache_by_pick_code.get(pick_code, ("", 0, ""))
                if cached_sha1 and cached_sha1 != sha1:
                    continue
                file_name = cached_name or path.replace("\\", "/").rsplit("/", 1)[-1]
                file_size = cached_size or _asset_size(asset)
                targets.append(
                    P115MediaInfoRestoreTarget(
                        tmdb_id=str(row.get("tmdb_id") or "").strip(),
                        item_type=str(row.get("item_type") or "").strip(),
                        title=str(row.get("title") or "").strip(),
                        path=path,
                        file_name=file_name,
                        emby_item_id=emby_item_id,
                        sha1=sha1,
                        pick_code=pick_code,
                        file_size=file_size,
                        original_language=str(row.get("original_language") or "").strip(),
                        parent_series_tmdb_id=str(row.get("parent_series_tmdb_id") or "").strip(),
                        season_number=_optional_integer(row.get("season_number")),
                        episode_number=_optional_integer(row.get("episode_number")),
                    )
                )
        return tuple(targets)

    def reserve_probe(
        self,
        *,
        operation_key: str,
        provenance_key: str,
        source: str,
        account_id: str,
        target: P115MediaProbeTarget,
        lease_seconds: int = 120,
    ) -> P115MediaProbeReservation:
        normalized_operation = str(operation_key or "").strip()
        normalized_provenance = str(provenance_key or "").strip()
        normalized_source = str(source or "").strip()
        normalized_account = str(account_id or "").strip()
        if not normalized_operation or len(normalized_operation) > 255:
            raise ValueError("media probe operation key is invalid")
        if len(normalized_provenance) != 64 or any(
            character not in "0123456789abcdef" for character in normalized_provenance
        ):
            raise ValueError("media probe provenance key is invalid")
        if normalized_source not in {"strm_sync", "restore_mediainfo"}:
            raise ValueError("media probe source is invalid")
        if not normalized_account.isdigit() or lease_seconds < 1:
            raise ValueError("media probe claim settings are invalid")
        item_id = uuid4()
        claim_token = uuid4()
        with self._unit_of_work() as uow:
            uow.session.execute(
                text(
                    """
                    INSERT INTO p115_media_probe_items (
                        id, operation_key, provenance_key, source, account_id,
                        sha1, pick_code, file_name
                    )
                    VALUES (
                        :id, :operation_key, :provenance_key, :source, :account_id,
                        :sha1, :pick_code, :file_name
                    )
                    ON CONFLICT (operation_key, sha1, provenance_key) DO NOTHING
                    """
                ),
                {
                    "id": item_id,
                    "operation_key": normalized_operation,
                    "provenance_key": normalized_provenance,
                    "source": normalized_source,
                    "account_id": normalized_account,
                    "sha1": target.sha1,
                    "pick_code": target.pick_code,
                    "file_name": target.file_name,
                },
            )
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT id, source, account_id, pick_code, file_name,
                               status, attempts, claim_token, lease_expires_at,
                               lease_expires_at > NOW() AS lease_active,
                               expected_cache_revision, result_json, last_error_json
                        FROM p115_media_probe_items
                        WHERE operation_key = :operation_key
                          AND sha1 = :sha1
                          AND provenance_key = :provenance_key
                        FOR UPDATE
                        """
                    ),
                    {
                        "operation_key": normalized_operation,
                        "sha1": target.sha1,
                        "provenance_key": normalized_provenance,
                    },
                )
                .mappings()
                .one()
            )
            if (
                str(row["source"]) != normalized_source
                or str(row["account_id"]) != normalized_account
                or str(row["pick_code"]) != target.pick_code
                or str(row["file_name"]) != target.file_name
            ):
                raise P115FingerprintConflict("media probe operation identity conflicts with existing work")
            status = P115MediaProbeItemStatus(str(row["status"]))
            cache: Mapping[str, Any] | None = None
            cache_locked = False
            if status is P115MediaProbeItemStatus.CONFIRMED:
                cache_row = (
                    uow.session.execute(
                        text(
                            """
                            SELECT cache_revision, mediainfo_json IS NOT NULL AS has_formatted
                            FROM p115_mediainfo_cache
                            WHERE sha1 = :sha1
                            FOR UPDATE
                            """
                        ),
                        {"sha1": target.sha1},
                    )
                    .mappings()
                    .one_or_none()
                )
                cache = dict(cache_row) if cache_row is not None else None
                cache_locked = True
                result = _mapping_or_none(row.get("result_json"))
                result_revision = _optional_integer((result or {}).get("cache_revision"))
                if (
                    cache is not None
                    and bool(cache["has_formatted"])
                    and result_revision is not None
                    and int(cache["cache_revision"]) == result_revision
                ):
                    uow.commit()
                    return P115MediaProbeReservation(
                        id=str(row["id"]),
                        status=status,
                        attempts=int(row["attempts"]),
                        cache_revision=_optional_integer(row.get("expected_cache_revision")),
                        result=result,
                        error=_mapping_or_none(row.get("last_error_json")),
                    )
            elif status in {
                P115MediaProbeItemStatus.REJECTED,
                P115MediaProbeItemStatus.CANCELLED,
            }:
                uow.commit()
                return P115MediaProbeReservation(
                    id=str(row["id"]),
                    status=status,
                    attempts=int(row["attempts"]),
                    cache_revision=_optional_integer(row.get("expected_cache_revision")),
                    result=_mapping_or_none(row.get("result_json")),
                    error=_mapping_or_none(row.get("last_error_json")),
                )
            active_claim = status is P115MediaProbeItemStatus.PROBING and bool(row["lease_active"])
            if active_claim:
                uow.commit()
                return P115MediaProbeReservation(
                    id=str(row["id"]),
                    status=status,
                    attempts=int(row["attempts"]),
                    cache_revision=_optional_integer(row.get("expected_cache_revision")),
                    result=_mapping_or_none(row.get("result_json")),
                    error=_mapping_or_none(row.get("last_error_json")),
                )
            if not cache_locked:
                cache_row = (
                    uow.session.execute(
                        text(
                            """
                            SELECT cache_revision, mediainfo_json IS NOT NULL AS has_formatted
                            FROM p115_mediainfo_cache
                            WHERE sha1 = :sha1
                            FOR UPDATE
                            """
                        ),
                        {"sha1": target.sha1},
                    )
                    .mappings()
                    .one_or_none()
                )
                cache = dict(cache_row) if cache_row is not None else None
            cache_revision = int(cache["cache_revision"]) if cache is not None else None
            claimed = (
                uow.session.execute(
                    text(
                        """
                        UPDATE p115_media_probe_items
                        SET status = 'probing',
                            attempts = attempts + 1,
                            claim_token = :claim_token,
                            lease_expires_at = NOW() + make_interval(secs => :lease_seconds),
                            expected_cache_revision = :cache_revision,
                            result_json = NULL,
                            last_error_json = NULL,
                            updated_at = NOW(),
                            finished_at = NULL
                        WHERE id = :id
                        RETURNING id, attempts, expected_cache_revision
                        """
                    ),
                    {
                        "claim_token": claim_token,
                        "lease_seconds": int(lease_seconds),
                        "cache_revision": cache_revision,
                        "id": row["id"],
                    },
                )
                .mappings()
                .one()
            )
            uow.commit()
        return P115MediaProbeReservation(
            id=str(claimed["id"]),
            status=P115MediaProbeItemStatus.PROBING,
            attempts=int(claimed["attempts"]),
            claim_token=str(claim_token),
            cache_revision=_optional_integer(claimed.get("expected_cache_revision")),
        )

    def finish_probe(
        self,
        reservation: P115MediaProbeReservation,
        *,
        status: P115MediaProbeItemStatus,
        provider_backend: str = "",
        result: Mapping[str, Any] | None = None,
        error: Mapping[str, Any] | None = None,
    ) -> None:
        if not reservation.claimed:
            raise ValueError("media probe reservation is not owned by this execution")
        if status not in {
            P115MediaProbeItemStatus.RETRYABLE,
            P115MediaProbeItemStatus.REJECTED,
            P115MediaProbeItemStatus.CANCELLED,
        }:
            raise ValueError("media probe finish status must describe an incomplete probe")
        terminal = status in {P115MediaProbeItemStatus.REJECTED, P115MediaProbeItemStatus.CANCELLED}
        with self._unit_of_work() as uow:
            updated = uow.session.execute(
                text(
                    """
                    UPDATE p115_media_probe_items
                    SET status = :status,
                        provider_backend = NULLIF(:provider_backend, ''),
                        result_json = CAST(:result AS jsonb),
                        last_error_json = CAST(:error AS jsonb),
                        claim_token = NULL,
                        lease_expires_at = NULL,
                        updated_at = NOW(),
                        finished_at = CASE WHEN :terminal THEN NOW() ELSE NULL END
                    WHERE id = :id
                      AND status = 'probing'
                      AND claim_token = :claim_token
                    """
                ),
                {
                    "status": status.value,
                    "provider_backend": str(provider_backend or "").strip(),
                    "result": json.dumps(dict(result or {}), ensure_ascii=False) if result is not None else None,
                    "error": json.dumps(dict(error or {}), ensure_ascii=False) if error is not None else None,
                    "terminal": terminal,
                    "id": UUID(reservation.id),
                    "claim_token": UUID(reservation.claim_token),
                },
            )
            if int(getattr(updated, "rowcount", 0) or 0) != 1:
                raise RuntimeError("media probe claim changed before finish")
            uow.commit()

    def confirm_probe(
        self,
        reservation: P115MediaProbeReservation,
        *,
        account_id: str,
        sha1: str,
        pick_code: str,
        provider_backend: str,
        result: Mapping[str, Any] | None = None,
        formatted: Sequence[Mapping[str, Any]] | None = None,
        raw: Mapping[str, Any] | None = None,
        formatter_revision: str = "",
        probe_revision: str = "",
        raw_schema_version: int = 1,
    ) -> int:
        """Atomically confirm one persistent probe, with an optional cache CAS replacement."""
        if not reservation.claimed:
            raise ValueError("media probe reservation is not owned by this execution")
        normalized_account = str(account_id or "").strip()
        normalized_sha1 = canonical_sha1(sha1)
        normalized_pick_code = str(pick_code or "").strip()
        normalized_backend = str(provider_backend or "").strip().lower()
        if not normalized_account.isdigit() or not normalized_pick_code:
            raise ValueError("confirmed media probe identity is invalid")
        if normalized_backend not in {"openapi", "cookie", "cache", "raw"}:
            raise ValueError("confirmed media probe backend is invalid")
        generated = formatted is not None or raw is not None
        if generated != (formatted is not None and raw is not None):
            raise ValueError("generated media probe requires formatted and raw cache values")
        if isinstance(raw_schema_version, bool) or int(raw_schema_version) < 1:
            raise ValueError("raw MediaInfo schema version must be positive")
        formatted_json = (
            json.dumps([dict(value) for value in formatted], ensure_ascii=False, separators=(",", ":"))
            if formatted is not None
            else None
        )
        raw_json = json.dumps(dict(raw), ensure_ascii=False, separators=(",", ":")) if raw is not None else None
        formatter_tag = _cache_revision_tag(formatter_revision, "formatter revision") if generated else ""
        probe_tag = _cache_revision_tag(probe_revision, "probe revision") if generated else ""
        item_id = UUID(reservation.id)
        claim_token = UUID(reservation.claim_token)

        with self._unit_of_work() as uow:
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(:namespace, :lock_key)"),
                {"namespace": DYNAMIC_CONFIG_LOCK_NAMESPACE, "lock_key": P115_AUTH_LOCK_KEY},
            )
            persisted_account = str(
                uow.session.scalar(
                    text(
                        """
                        SELECT value_json ->> 'account_id'
                        FROM app_settings
                        WHERE setting_key = 'p115_auth_tokens'
                        """
                    )
                )
                or ""
            ).strip()
            if persisted_account != normalized_account:
                raise P115FingerprintAccountChanged("115 account changed before MediaInfo probe confirmation")
            work = (
                uow.session.execute(
                    text(
                        """
                        SELECT source, account_id, sha1, pick_code, expected_cache_revision
                        FROM p115_media_probe_items
                        WHERE id = :item_id
                          AND status = 'probing'
                          AND claim_token = :claim_token
                        FOR UPDATE
                        """
                    ),
                    {"item_id": item_id, "claim_token": claim_token},
                )
                .mappings()
                .one_or_none()
            )
            if work is None:
                raise P115FingerprintConflict("media probe claim changed before confirmation")
            if (
                str(work["account_id"]) != normalized_account
                or canonical_sha1(work["sha1"]) != normalized_sha1
                or str(work["pick_code"]) != normalized_pick_code
            ):
                raise P115FingerprintConflict("media probe identity changed before confirmation")
            expected_revision = _optional_integer(work.get("expected_cache_revision"))
            if expected_revision != reservation.cache_revision:
                raise P115FingerprintConflict("media probe cache expectation changed before confirmation")
            cache = (
                uow.session.execute(
                    text(
                        """
                        SELECT cache_revision, mediainfo_json IS NOT NULL AS has_formatted
                        FROM p115_mediainfo_cache
                        WHERE sha1 = :sha1
                        FOR UPDATE
                        """
                    ),
                    {"sha1": normalized_sha1},
                )
                .mappings()
                .one_or_none()
            )

            if not generated:
                if (
                    cache is None
                    or expected_revision is None
                    or int(cache["cache_revision"]) != expected_revision
                    or not bool(cache["has_formatted"])
                ):
                    raise P115FingerprintConflict("MediaInfo cache changed before reuse was confirmed")
                next_revision = expected_revision
            elif cache is None:
                if expected_revision is not None:
                    raise P115FingerprintConflict("MediaInfo cache disappeared before probe confirmation")
                next_revision = 1
                uow.session.execute(
                    text(
                        """
                        INSERT INTO p115_mediainfo_cache (
                            sha1, mediainfo_json, raw_ffprobe_json, cache_revision,
                            raw_schema_version, formatter_revision, probe_revision
                        ) VALUES (
                            :sha1, CAST(:formatted AS jsonb), CAST(:raw AS jsonb), :cache_revision,
                            :raw_schema_version, :formatter_revision, :probe_revision
                        )
                        """
                    ),
                    {
                        "sha1": normalized_sha1,
                        "formatted": formatted_json,
                        "raw": raw_json,
                        "cache_revision": next_revision,
                        "raw_schema_version": int(raw_schema_version),
                        "formatter_revision": formatter_tag,
                        "probe_revision": probe_tag,
                    },
                )
            else:
                actual_revision = int(cache["cache_revision"])
                if expected_revision is None or actual_revision != expected_revision:
                    raise P115FingerprintConflict("MediaInfo cache revision changed before probe confirmation")
                next_revision = actual_revision + 1
                updated = uow.session.execute(
                    text(
                        """
                        UPDATE p115_mediainfo_cache
                        SET mediainfo_json = CAST(:formatted AS jsonb),
                            raw_ffprobe_json = CAST(:raw AS jsonb),
                            cache_revision = :next_revision,
                            raw_schema_version = :raw_schema_version,
                            formatter_revision = :formatter_revision,
                            probe_revision = :probe_revision,
                            updated_at = NOW()
                        WHERE sha1 = :sha1
                          AND cache_revision = :expected_revision
                        """
                    ),
                    {
                        "sha1": normalized_sha1,
                        "formatted": formatted_json,
                        "raw": raw_json,
                        "next_revision": next_revision,
                        "expected_revision": expected_revision,
                        "raw_schema_version": int(raw_schema_version),
                        "formatter_revision": formatter_tag,
                        "probe_revision": probe_tag,
                    },
                )
                if int(getattr(updated, "rowcount", 0) or 0) != 1:
                    raise P115FingerprintConflict("MediaInfo cache revision changed during probe confirmation")

            result_payload = {
                **dict(result or {}),
                "cache_revision": next_revision,
                "source": str(work["source"]),
            }
            confirmed = uow.session.execute(
                text(
                    """
                    UPDATE p115_media_probe_items
                    SET status = 'confirmed',
                        provider_backend = :provider_backend,
                        result_json = CAST(:result AS jsonb),
                        last_error_json = NULL,
                        claim_token = NULL,
                        lease_expires_at = NULL,
                        updated_at = NOW(),
                        finished_at = NOW()
                    WHERE id = :item_id
                      AND status = 'probing'
                      AND claim_token = :claim_token
                    """
                ),
                {
                    "provider_backend": normalized_backend,
                    "result": json.dumps(result_payload, ensure_ascii=False, separators=(",", ":")),
                    "item_id": item_id,
                    "claim_token": claim_token,
                },
            )
            if int(getattr(confirmed, "rowcount", 0) or 0) != 1:
                raise P115FingerprintConflict("media probe claim changed during confirmation")
            uow.commit()
        return next_revision

    def list_repair_rows(self) -> tuple[P115FingerprintRepairRow, ...]:
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, item_type, title,
                               asset_details_json, file_sha1_json, file_pickcode_json,
                               last_updated_at
                        FROM media_metadata
                        WHERE in_library = TRUE
                          AND item_type IN ('Movie', 'Episode')
                          AND asset_details_json IS NOT NULL
                        ORDER BY item_type, title NULLS LAST, tmdb_id
                        """
                    )
                )
                .mappings()
                .all()
            )
        return tuple(_repair_row(dict(row)) for row in rows)

    def find_fingerprint(
        self,
        *,
        account_id: str,
        pick_code: str = "",
        sha1: str = "",
        file_id: str = "",
        local_paths: Sequence[str] = (),
    ) -> P115FileFingerprint | None:
        normalized_account_id = str(account_id or "").strip()
        normalized_pick_code = str(pick_code or "").strip()
        normalized_sha1 = canonical_sha1(sha1, allow_empty=True)
        normalized_file_id = str(file_id or "").strip()
        normalized_paths = tuple(
            dict.fromkeys(str(value or "").strip() for value in local_paths if str(value or "").strip())
        )
        if not normalized_account_id.isdigit():
            raise ValueError("fingerprint lookup requires a stable numeric account ID")
        if not (normalized_pick_code or normalized_sha1 or normalized_file_id or normalized_paths):
            return None
        lookups: list[tuple[str, dict[str, Any]]] = []
        if normalized_file_id:
            lookups.append(("id = :file_id", {"file_id": normalized_file_id}))
        if normalized_pick_code:
            lookups.append(("lower(pick_code) = lower(:pick_code)", {"pick_code": normalized_pick_code}))
        if normalized_paths:
            lookups.append(
                (
                    "local_path = ANY(CAST(:local_paths AS text[]))",
                    {"local_paths": list(normalized_paths)},
                )
            )
        if not lookups and normalized_sha1:
            lookups.append(("upper(sha1) = :sha1", {"sha1": normalized_sha1}))
        with self._unit_of_work() as uow:
            for predicate, values in lookups:
                rows = (
                    uow.session.execute(
                        text(
                            f"""
                            SELECT id, parent_id, name, sha1, pick_code, size,
                                   account_id, local_path
                            FROM p115_filesystem_cache
                            WHERE account_id = :account_id
                              AND entry_kind = 'file'
                              AND {predicate}
                            ORDER BY id
                            """
                        ),
                        {"account_id": normalized_account_id, **values},
                    )
                    .mappings()
                    .all()
                )
                if len(rows) > 1:
                    raise P115FingerprintConflict("fingerprint lookup is ambiguous")
                if rows:
                    try:
                        return _file_fingerprint(dict(rows[0]))
                    except ValueError:
                        return None
        return None

    def commit_repair(
        self,
        row: P115FingerprintRepairRow,
        asset_index: int,
        fingerprint: P115FileFingerprint,
        *,
        account_id: str,
    ) -> bool:
        normalized_account_id = str(account_id or "").strip()
        if not normalized_account_id.isdigit() or fingerprint.account_id != normalized_account_id:
            raise P115FingerprintConflict("fingerprint account does not match the repair transaction")
        if asset_index < 0 or asset_index >= len(row.assets):
            raise IndexError("fingerprint repair asset index is out of range")

        with self._unit_of_work() as uow:
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(:namespace, :lock_key)"),
                {
                    "namespace": DYNAMIC_CONFIG_LOCK_NAMESPACE,
                    "lock_key": P115_AUTH_LOCK_KEY,
                },
            )
            persisted_account_id = str(
                uow.session.scalar(
                    text(
                        """
                        SELECT value_json ->> 'account_id'
                        FROM app_settings
                        WHERE setting_key = 'p115_auth_tokens'
                        """
                    )
                )
                or ""
            ).strip()
            if persisted_account_id != normalized_account_id:
                raise P115FingerprintAccountChanged("115 account changed before fingerprint commit")

            locked = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, item_type, title,
                               asset_details_json, file_sha1_json, file_pickcode_json,
                               last_updated_at
                        FROM media_metadata
                        WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                        FOR UPDATE
                        """
                    ),
                    {"tmdb_id": row.tmdb_id, "item_type": row.item_type},
                )
                .mappings()
                .one_or_none()
            )
            if locked is None:
                raise P115FingerprintConflict("media row disappeared before fingerprint commit")
            current = _repair_row(dict(locked))
            if current.revision != row.revision:
                raise P115FingerprintConflict("media fingerprint slots changed before commit")
            current_sha1 = current.sha1s[asset_index]
            current_pick_code = current.pick_codes[asset_index]
            if current_sha1 and current_sha1 != fingerprint.sha1:
                raise P115FingerprintConflict("media SHA1 slot changed before commit")
            if current_pick_code and current_pick_code != fingerprint.pick_code:
                raise P115FingerprintConflict("media PickCode slot changed before commit")

            conflicts = (
                uow.session.execute(
                    text(
                        """
                        SELECT id, parent_id, name, sha1, pick_code, size,
                               account_id, local_path
                        FROM p115_filesystem_cache
                        WHERE id = :file_id
                           OR (parent_id = :parent_id AND name = :name)
                           OR (pick_code IS NOT NULL AND lower(pick_code) = lower(:pick_code))
                        FOR UPDATE
                        """
                    ),
                    {
                        "file_id": fingerprint.file_id,
                        "parent_id": fingerprint.parent_id,
                        "name": fingerprint.name,
                        "pick_code": fingerprint.pick_code,
                    },
                )
                .mappings()
                .all()
            )
            for conflict in conflicts:
                if str(conflict.get("id") or "") != fingerprint.file_id:
                    raise P115FingerprintConflict("fingerprint identity conflicts with another cache row")
                conflict_account = str(conflict.get("account_id") or "").strip()
                if conflict_account and conflict_account != normalized_account_id:
                    raise P115FingerprintConflict("fingerprint cache row belongs to another account")
                _assert_cache_values_compatible(dict(conflict), fingerprint)

            values = {
                "file_id": fingerprint.file_id,
                "parent_id": fingerprint.parent_id,
                "name": fingerprint.name,
                "sha1": fingerprint.sha1,
                "pick_code": fingerprint.pick_code,
                "size": fingerprint.size,
                "account_id": normalized_account_id,
                "local_path": fingerprint.local_path or None,
            }
            updated = uow.session.execute(
                text(
                    """
                    UPDATE p115_filesystem_cache
                    SET parent_id = :parent_id,
                        name = :name,
                        sha1 = :sha1,
                        pick_code = :pick_code,
                        size = :size,
                        account_id = :account_id,
                        entry_kind = 'file',
                        local_path = COALESCE(:local_path, local_path),
                        updated_at = NOW()
                    WHERE id = :file_id
                    """
                ),
                values,
            )
            if int(getattr(updated, "rowcount", 0) or 0) == 0:
                uow.session.execute(
                    text(
                        """
                        INSERT INTO p115_filesystem_cache (
                            id, parent_id, name, local_path, sha1, pick_code,
                            size, account_id, entry_kind
                        )
                        VALUES (
                            :file_id, :parent_id, :name, :local_path, :sha1, :pick_code,
                            :size, :account_id, 'file'
                        )
                        """
                    ),
                    values,
                )

            sha1s = list(current.sha1s)
            pick_codes = list(current.pick_codes)
            sha1s[asset_index] = fingerprint.sha1
            pick_codes[asset_index] = fingerprint.pick_code
            changed = sha1s != list(current.sha1s) or pick_codes != list(current.pick_codes)
            if changed:
                result = uow.session.execute(
                    text(
                        """
                        UPDATE media_metadata
                        SET file_sha1_json = CAST(:sha1s AS jsonb),
                            file_pickcode_json = CAST(:pick_codes AS jsonb),
                            last_updated_at = NOW()
                        WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                        """
                    ),
                    {
                        "sha1s": json.dumps(sha1s, ensure_ascii=False),
                        "pick_codes": json.dumps(pick_codes, ensure_ascii=False),
                        "tmdb_id": row.tmdb_id,
                        "item_type": row.item_type,
                    },
                )
                if int(getattr(result, "rowcount", 0) or 0) != 1:
                    raise P115FingerprintConflict("media row changed before fingerprint update")
            uow.commit()
        return True


def _cache_record(row: Mapping[str, Any]) -> P115MediaInfoCacheRecord:
    return P115MediaInfoCacheRecord.create(
        sha1=row.get("sha1"),
        formatted=row.get("mediainfo_json"),
        raw=row.get("raw_ffprobe_json"),
        cache_revision=row.get("cache_revision", 0),
        raw_schema_version=row.get("raw_schema_version", 1),
        formatter_revision=row.get("formatter_revision", "legacy-v1"),
        probe_revision=row.get("probe_revision", "legacy-v1"),
        hit_count=row.get("hit_count", 0),
    )


def _repair_row(row: Mapping[str, Any]) -> P115FingerprintRepairRow:
    assets = _json_list(row.get("asset_details_json"))
    sha1s = [_repair_sha1(value) for value in _json_list(row.get("file_sha1_json"))]
    pick_codes = [_repair_text(value) for value in _json_list(row.get("file_pickcode_json"))]
    revision = _repair_row_revision(
        tmdb_id=row.get("tmdb_id"),
        item_type=row.get("item_type"),
        assets=assets,
    )
    return P115FingerprintRepairRow.create(
        tmdb_id=row.get("tmdb_id"),
        item_type=row.get("item_type"),
        title=row.get("title"),
        assets=assets,
        sha1s=sha1s,
        pick_codes=pick_codes,
        revision=revision,
    )


def _repair_row_revision(
    *,
    tmdb_id: object,
    item_type: object,
    assets: Sequence[object],
) -> str:
    payload = {
        "tmdb_id": str(tmdb_id or ""),
        "item_type": str(item_type or ""),
        "assets": list(assets),
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()


def _file_fingerprint(row: Mapping[str, Any]) -> P115FileFingerprint:
    return P115FileFingerprint(
        file_id=str(row.get("id") or ""),
        parent_id=str(row.get("parent_id") or ""),
        name=str(row.get("name") or ""),
        sha1=str(row.get("sha1") or ""),
        pick_code=str(row.get("pick_code") or ""),
        size=int(row.get("size") or 0),
        account_id=str(row.get("account_id") or ""),
        local_path=str(row.get("local_path") or ""),
    )


def _assert_cache_values_compatible(row: Mapping[str, Any], value: P115FileFingerprint) -> None:
    comparisons = {
        "parent_id": value.parent_id,
        "name": value.name,
        "sha1": value.sha1,
        "pick_code": value.pick_code,
    }
    for field, expected in comparisons.items():
        current = str(row.get(field) or "").strip()
        if field == "sha1":
            current = current.upper()
        if current and current != expected:
            raise P115FingerprintConflict(f"fingerprint cache {field} conflicts with remote identity")


def _json_list(value: object) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _mapping_or_none(value: object) -> Mapping[str, Any] | None:
    return dict(value) if isinstance(value, Mapping) else None


def _repair_sha1(value: object) -> str:
    normalized = str(value or "").strip()
    if normalized.casefold() in {"", "none", "null", "[]", "{}"}:
        return ""
    return canonical_sha1(normalized)


def _repair_text(value: object) -> str:
    normalized = str(value or "").strip()
    return "" if normalized.casefold() in {"", "none", "null", "[]", "{}"} else normalized


def _optional_integer(value: object) -> int | None:
    try:
        return int(str(value)) if value is not None else None
    except (TypeError, ValueError):
        return None


def _cache_revision_tag(value: object, label: str) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > 64:
        raise ValueError(f"{label} is invalid")
    if any(not (character.isalnum() or character in "._:-") for character in normalized):
        raise ValueError(f"{label} is invalid")
    return normalized


def _asset_size(asset: Mapping[str, Any]) -> int:
    for key in ("size_bytes", "size", "Size", "file_size"):
        value = asset.get(key)
        if isinstance(value, bool):
            continue
        try:
            parsed = int(str(value))
        except (TypeError, ValueError):
            continue
        if parsed >= 0:
            return parsed
    return 0


__all__ = ["SqlAlchemyP115MediaInfoRepository"]
