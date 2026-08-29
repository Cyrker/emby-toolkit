from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork

from .daily_theme import DailyThemePoolMutationResult, DailyThemePublishResult, DailyThemeSnapshot
from .effects import MediaEffectStatus, SqlAlchemyMediaEffectRepository

_PUBLICATION_KEY = "recommendation_publication_state"
_RECEIPT_EFFECT_TYPE = "daily-theme.publication.committed"
_RECEIPT_KEY_PREFIX = "daily-theme-publication:"
_SNAPSHOT_KEYS = (
    "keyword_mapping",
    "recommendation_pool",
    "recommendation_theme_index",
    "recommendation_pool_page",
    _PUBLICATION_KEY,
)


class SqlAlchemyDailyThemeRepository:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def load_snapshot(self) -> DailyThemeSnapshot:
        with self._unit_of_work() as uow:
            values = _load_settings(uow)
        return DailyThemeSnapshot(
            values=deepcopy(values),
            fingerprint=_fingerprint(values),
            publication_revision=_publication_revision(values.get(_PUBLICATION_KEY)),
        )

    def load_receipt(self, publication_id: str) -> Mapping[str, Any] | None:
        with self._unit_of_work() as uow:
            return _load_receipt(uow, publication_id)

    def excluded_movie_ids(self, tmdb_ids: Sequence[str]) -> set[str]:
        identifiers = tuple(dict.fromkeys(str(value) for value in tmdb_ids if str(value).strip()))
        if not identifiers:
            return set()
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    """
                    SELECT tmdb_id, in_library, emby_item_ids_json, subscription_status
                    FROM media_metadata
                    WHERE item_type = 'Movie'
                      AND tmdb_id = ANY(CAST(:tmdb_ids AS text[]))
                    """
                ),
                {"tmdb_ids": list(identifiers)},
            ).mappings()
            return {
                str(row["tmdb_id"])
                for row in rows
                if bool(row.get("in_library"))
                or bool(row.get("emby_item_ids_json"))
                or str(row.get("subscription_status") or "").strip().upper() not in {"", "NONE", "IGNORED"}
            }

    def actor_names(self, actor_ids: Sequence[int]) -> Mapping[int, str]:
        identifiers = tuple(dict.fromkeys(int(value) for value in actor_ids if int(value) > 0))
        if not identifiers:
            return {}
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    """
                    SELECT tmdb_person_id, primary_name
                    FROM person_metadata
                    WHERE tmdb_person_id = ANY(CAST(:actor_ids AS integer[]))
                    """
                ),
                {"actor_ids": list(identifiers)},
            ).mappings()
            return {int(row["tmdb_person_id"]): str(row["primary_name"]) for row in rows if row.get("primary_name")}

    def publish(
        self,
        *,
        expected_fingerprint: str,
        pool: Sequence[Mapping[str, Any]],
        theme_index: int,
        pool_page: int,
        metadata: Mapping[str, Any],
    ) -> DailyThemePublishResult | None:
        publication_id = str(metadata.get("publication_id") or "").strip()
        if not publication_id:
            raise ValueError("daily-theme publication metadata requires publication_id")
        with self._unit_of_work() as uow:
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended('recommendation_publication_state', 0))")
            )
            uow.session.execute(text("LOCK TABLE app_settings IN SHARE ROW EXCLUSIVE MODE"))
            existing_receipt = _load_receipt(uow, publication_id)
            if existing_receipt is not None:
                return DailyThemePublishResult(
                    revision=_receipt_revision(existing_receipt),
                    receipt=existing_receipt,
                    replayed=True,
                )
            current = _load_settings(uow, for_update=True)
            if _fingerprint(current) != expected_fingerprint:
                return None
            current_revision = _publication_revision(current.get(_PUBLICATION_KEY))
            next_revision = current_revision + 1
            publication = {
                **dict(metadata),
                "revision": next_revision,
            }
            values: tuple[tuple[str, Any], ...] = (
                ("recommendation_pool", [dict(item) for item in pool]),
                ("recommendation_theme_index", theme_index),
                ("recommendation_pool_page", pool_page),
                (_PUBLICATION_KEY, publication),
            )
            _write_settings(uow, values)
            recorded = SqlAlchemyMediaEffectRepository(uow.session).record_confirmed(
                effect_key=_receipt_key(publication_id),
                effect_type=_RECEIPT_EFFECT_TYPE,
                aggregate_type="daily_theme_publication",
                aggregate_id=publication_id,
                payload={"publication_id": publication_id},
                result=publication,
            )
            receipt = recorded.effect.result
            if receipt is None:
                raise RuntimeError("daily-theme publication receipt is missing its result")
            if not recorded.created:
                return DailyThemePublishResult(
                    revision=_receipt_revision(receipt),
                    receipt=deepcopy(receipt),
                    replayed=True,
                )
            uow.commit()
            return DailyThemePublishResult(
                revision=next_revision,
                receipt=deepcopy(receipt),
            )


def remove_recommendation_in_uow(
    uow: SqlAlchemyUnitOfWork,
    *,
    tmdb_id: str,
    mutation_id: str,
    occurred_at: str,
) -> DailyThemePoolMutationResult | None:
    normalized_tmdb_id = str(tmdb_id).strip()
    normalized_mutation_id = str(mutation_id).strip()
    if not normalized_tmdb_id or not normalized_mutation_id:
        raise ValueError("daily-theme recommendation removal identity is incomplete")
    uow.session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended('recommendation_publication_state', 0))"))
    uow.session.execute(text("LOCK TABLE app_settings IN SHARE ROW EXCLUSIVE MODE"))
    current = _load_settings(uow, for_update=True)
    current_pool = current.get("recommendation_pool")
    if not isinstance(current_pool, list):
        return None
    pool = [item for item in current_pool if not isinstance(item, Mapping) or str(item.get("id")) != normalized_tmdb_id]
    if len(pool) == len(current_pool):
        return None
    current_revision = _publication_revision(current.get(_PUBLICATION_KEY))
    next_revision = current_revision + 1
    previous_publication = current.get(_PUBLICATION_KEY)
    publication = {
        "publication_id": normalized_mutation_id,
        "mutation": "portal_removal",
        "source_revision": current_revision,
        "source_fingerprint": _fingerprint(current),
        "theme_index": current.get("recommendation_theme_index"),
        "theme_label": (
            str(previous_publication.get("theme_label") or "").strip()
            if isinstance(previous_publication, Mapping)
            else ""
        ),
        "pool_page": current.get("recommendation_pool_page"),
        "pool_size": len(pool),
        "removed_tmdb_id": normalized_tmdb_id,
        "published_at": str(occurred_at),
        "revision": next_revision,
    }
    _write_settings(
        uow,
        (
            ("recommendation_pool", pool),
            (_PUBLICATION_KEY, publication),
        ),
    )
    return DailyThemePoolMutationResult(revision=next_revision, pool_size=len(pool))


def _load_settings(
    uow: SqlAlchemyUnitOfWork,
    *,
    for_update: bool = False,
) -> dict[str, Any]:
    suffix = " FOR UPDATE" if for_update else ""
    rows = uow.session.execute(
        text(
            f"""
            SELECT setting_key, value_json
            FROM app_settings
            WHERE setting_key = ANY(CAST(:keys AS text[])){suffix}
            """
        ),
        {"keys": list(_SNAPSHOT_KEYS)},
    ).mappings()
    found = {str(row["setting_key"]): deepcopy(row["value_json"]) for row in rows}
    return {key: found.get(key) for key in _SNAPSHOT_KEYS}


def _fingerprint(values: Mapping[str, Any]) -> str:
    canonical = {key: values.get(key) for key in _SNAPSHOT_KEYS}
    encoded = json.dumps(
        canonical,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _write_settings(uow: SqlAlchemyUnitOfWork, values: Sequence[tuple[str, Any]]) -> None:
    for key, value in values:
        uow.session.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                VALUES (:key, CAST(:value AS jsonb), NOW())
                ON CONFLICT (setting_key) DO UPDATE SET
                    value_json = EXCLUDED.value_json,
                    last_updated_at = NOW()
                """
            ),
            {
                "key": key,
                "value": json.dumps(value, ensure_ascii=False, separators=(",", ":")),
            },
        )


def _publication_revision(value: Any) -> int:
    if value is None:
        return 0
    if not isinstance(value, Mapping):
        raise ValueError("recommendation pool publication metadata must be an object")
    revision = value.get("revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
        raise ValueError("recommendation pool publication revision must be a non-negative integer")
    return revision


def _load_receipt(uow: SqlAlchemyUnitOfWork, publication_id: str) -> Mapping[str, Any] | None:
    row = (
        uow.session.execute(
            text(
                """
                SELECT status, result_json
                FROM media_effects
                WHERE effect_key = :effect_key
                """
            ),
            {"effect_key": _receipt_key(publication_id)},
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    if MediaEffectStatus(str(row["status"])) is not MediaEffectStatus.CONFIRMED:
        raise RuntimeError("daily-theme publication receipt is not confirmed")
    result = row["result_json"]
    if not isinstance(result, Mapping):
        raise RuntimeError("daily-theme publication receipt result is invalid")
    _receipt_revision(result)
    return deepcopy(dict(result))


def _receipt_key(publication_id: str) -> str:
    normalized = str(publication_id).strip()
    if not normalized:
        raise ValueError("daily-theme publication_id must not be empty")
    value = f"{_RECEIPT_KEY_PREFIX}{normalized}"
    if len(value) > 512:
        raise ValueError("daily-theme publication_id is too long")
    return value


def _receipt_revision(receipt: Mapping[str, Any]) -> int:
    revision = receipt.get("revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
        raise RuntimeError("daily-theme publication receipt revision is invalid")
    return revision


__all__ = ["SqlAlchemyDailyThemeRepository", "remove_recommendation_in_uow"]
