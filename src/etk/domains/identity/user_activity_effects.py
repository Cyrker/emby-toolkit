from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import text

from etk.domains.media.effect_execution import (
    MediaEffectDeliveryIndeterminate,
    MediaEffectDeliveryRetryable,
)
from etk.domains.media.effects import MediaEffectProjection
from etk.platform import Settings
from etk.platform.db import SqlAlchemyUnitOfWork

from .operation_repository import PostgresIdentityOperationRepository
from .operations import IdentityOperationIntent, IdentityOperationKind
from .repository import SqlAlchemyIdentityRepository

USER_EVENT_EFFECT_TYPES = frozenset(
    {
        "emby.event.user.refresh-user-policy",
        "emby.event.user.sync-template-policy",
        "emby.event.user.favorite-intro-detection",
        "emby.event.user.halfway-intro-detection",
        "emby.event.user.playback-cleanup",
        "emby.event.user.virtual-import-promotion",
        "emby.event.user.proxy-concurrency-release",
        "emby.event.user.telegram-playback-notification",
    }
)


class UserPolicyProvider(Protocol):
    def get_user(self, user_id: str) -> Mapping[str, Any] | None: ...


@dataclass(frozen=True, slots=True)
class TemplatePropagationTemplate:
    template_id: int
    template_name: str
    copies_configuration: bool
    user_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TemplatePropagationPlan:
    source_user_id: str
    templates: tuple[TemplatePropagationTemplate, ...]

    @property
    def target_user_ids(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                {
                    user_id
                    for template in self.templates
                    for user_id in template.user_ids
                    if user_id != self.source_user_id
                }
            )
        )


class SqlAlchemyTemplatePropagationRepository:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def plan(self, source_user_id: str) -> TemplatePropagationPlan:
        normalized = _required_text(source_user_id, "source_user_id")
        with self._unit_of_work() as uow:
            return self._plan_in_uow(uow, normalized, lock=False)

    def apply(
        self,
        plan: TemplatePropagationPlan,
        *,
        source_user: Mapping[str, Any],
        target_users: Mapping[str, Mapping[str, Any]],
        source_event_key: str,
    ) -> tuple[UUID, ...]:
        source_policy = source_user.get("Policy")
        if not isinstance(source_policy, Mapping):
            raise ValueError("source user policy snapshot is invalid")
        source_configuration = source_user.get("Configuration")
        if source_configuration is not None and not isinstance(source_configuration, Mapping):
            raise ValueError("source user configuration snapshot is invalid")
        operations = PostgresIdentityOperationRepository(self._unit_of_work)
        operation_ids: list[UUID] = []
        with self._unit_of_work() as uow:
            current = self._plan_in_uow(uow, plan.source_user_id, lock=True)
            if current != plan:
                raise RuntimeError("template propagation targets changed before commit")
            for template in current.templates:
                template_configuration = (
                    dict(source_configuration)
                    if template.copies_configuration and isinstance(source_configuration, Mapping)
                    else None
                )
                uow.session.execute(
                    text(
                        """
                        UPDATE user_templates
                        SET emby_policy_json = CAST(:policy AS jsonb),
                            emby_configuration_json = CAST(:configuration AS jsonb)
                        WHERE id = :template_id
                        """
                    ),
                    {
                        "template_id": template.template_id,
                        "policy": _json(source_policy),
                        "configuration": (
                            _json(template_configuration) if template_configuration is not None else None
                        ),
                    },
                )
                for user_id in template.user_ids:
                    if user_id == plan.source_user_id:
                        continue
                    target = target_users.get(user_id)
                    previous_policy = target.get("Policy") if isinstance(target, Mapping) else None
                    previous_configuration = target.get("Configuration") if isinstance(target, Mapping) else None
                    if not isinstance(previous_policy, Mapping):
                        raise ValueError(f"target user {user_id!r} policy snapshot is invalid")
                    if template_configuration is not None and not isinstance(
                        previous_configuration,
                        Mapping,
                    ):
                        raise ValueError(f"target user {user_id!r} configuration snapshot is invalid")
                    desired_policy = dict(source_policy)
                    if "IsDisabled" in previous_policy:
                        desired_policy["IsDisabled"] = bool(previous_policy["IsDisabled"])
                    request_id = (
                        f"p8-policy:{_required_text(source_event_key, 'source_event_key')}:"
                        f"{template.template_id}:{user_id}"
                    )
                    reservation = operations.reserve_in_uow(
                        uow,
                        IdentityOperationIntent.create(
                            kind=IdentityOperationKind.ASSIGN_TEMPLATE,
                            scope_key=f"user:{user_id}",
                            request={
                                "user_id": user_id,
                                "request_id": request_id,
                                "template_id": template.template_id,
                                "template_name": template.template_name,
                                "template_policy": desired_policy,
                                "template_configuration": template_configuration,
                                "previous_policy": dict(previous_policy),
                                "previous_configuration": (
                                    dict(previous_configuration) if isinstance(previous_configuration, Mapping) else {}
                                ),
                            },
                            idempotency_key=request_id,
                        ),
                    )
                    operation_ids.append(reservation.operation.id)
            uow.commit()
        return tuple(operation_ids)

    @staticmethod
    def _plan_in_uow(
        uow: SqlAlchemyUnitOfWork,
        source_user_id: str,
        *,
        lock: bool,
    ) -> TemplatePropagationPlan:
        suffix = " FOR UPDATE" if lock else ""
        template_rows = tuple(
            uow.session.execute(
                text(
                    """
                    SELECT id, name, emby_configuration_json IS NOT NULL AS copies_configuration
                    FROM user_templates
                    WHERE source_emby_user_id = :source_user_id
                    ORDER BY id
                    """
                    + suffix
                ),
                {"source_user_id": source_user_id},
            ).mappings()
        )
        template_ids = tuple(int(row["id"]) for row in template_rows)
        users_by_template: dict[int, list[str]] = {template_id: [] for template_id in template_ids}
        if template_ids:
            user_suffix = " FOR UPDATE" if lock else ""
            user_rows = uow.session.execute(
                text(
                    """
                    SELECT template_id, emby_user_id
                    FROM emby_users_extended
                    WHERE template_id = ANY(:template_ids)
                    ORDER BY template_id, emby_user_id
                    """
                    + user_suffix
                ),
                {"template_ids": list(template_ids)},
            )
            for row in user_rows:
                users_by_template[int(row.template_id)].append(str(row.emby_user_id))
        return TemplatePropagationPlan(
            source_user_id,
            tuple(
                TemplatePropagationTemplate(
                    int(row["id"]),
                    str(row["name"]),
                    bool(row["copies_configuration"]),
                    tuple(users_by_template[int(row["id"])]),
                )
                for row in template_rows
            ),
        )


class UserPolicyEffectService:
    def __init__(
        self,
        provider: UserPolicyProvider,
        identity: SqlAlchemyIdentityRepository,
        templates: SqlAlchemyTemplatePropagationRepository,
    ) -> None:
        self._provider = provider
        self._identity = identity
        self._templates = templates

    def refresh_user_policy(self, user_id: str) -> Mapping[str, Any]:
        normalized = _required_text(user_id, "user_id")
        remote = self._provider.get_user(normalized)
        if remote is None:
            raise ValueError("Emby user was not found")
        if not isinstance(remote.get("Policy"), Mapping):
            raise ValueError("Emby user policy snapshot is invalid")
        revision = self._identity.projection_revision()
        updated = self._identity.upsert_user(remote, expected_revision=revision)
        return {"user_id": normalized, "updated": updated}

    def sync_template_policy(self, user_id: str, *, source_event_key: str) -> Mapping[str, Any]:
        normalized = _required_text(user_id, "user_id")
        plan = self._templates.plan(normalized)
        if not plan.templates:
            return {"source_user_id": normalized, "templates": 0, "operations": []}
        source = self._provider.get_user(normalized)
        if source is None:
            raise ValueError("Emby template source user was not found")
        targets: dict[str, Mapping[str, Any]] = {}
        for target_user_id in plan.target_user_ids:
            target = self._provider.get_user(target_user_id)
            if target is None:
                raise ValueError(f"Emby template target user {target_user_id!r} was not found")
            targets[target_user_id] = target
        operation_ids = self._templates.apply(
            plan,
            source_user=source,
            target_users=targets,
            source_event_key=source_event_key,
        )
        return {
            "source_user_id": normalized,
            "templates": len(plan.templates),
            "operations": [str(operation_id) for operation_id in operation_ids],
        }


class LegacyUserEventEffectsAdapter:
    """Worker-only adapters for P6/P9-owned side effects retained until their stage migration."""

    def __init__(self, settings: Callable[[], Settings]) -> None:
        self._settings = settings

    def execute(self, kind: str, snapshot: Mapping[str, Any]) -> Mapping[str, Any]:
        event = _legacy_event(snapshot)
        if kind == "favorite-intro-detection":
            result = import_module("handler.intro_detection_service").enqueue_favorite_item(
                dict(_mapping(snapshot.get("Item")))
            )
            return _mapping_result(result, kind)
        if kind == "halfway-intro-detection":
            result = import_module("handler.intro_detection_service").enqueue_playback_halfway(
                dict(_mapping(snapshot.get("Item")))
            )
            return _mapping_result(result, kind)
        if kind == "playback-cleanup":
            result = import_module("handler.p115_copy_play").cleanup_for_playback_stop(event)
            return {"completed": True, "released": int(result or 0)}
        if kind == "virtual-import-promotion":
            import_module("routes.webhook")._maybe_promote_virtual_import_from_playback(event)
            return {"completed": True}
        if kind == "proxy-concurrency-release":
            import_module("reverse_proxy").clear_play_concurrency_for_playback_stop(event)
            return {"completed": True}
        if kind == "telegram-playback-notification":
            if "playback" not in self._settings().telegram_notify_types:
                return {"completed": True, "skipped": True, "reason": "notification_disabled"}
            import_module("handler.telegram").send_playback_notification(event)
            return {"completed": True, "skipped": False}
        raise ValueError(f"unsupported P8 user event effect kind: {kind}")


class P8UserEventEffectDelivery:
    effect_types = USER_EVENT_EFFECT_TYPES
    _policy_attempt_limit = 12

    def __init__(
        self,
        policy: UserPolicyEffectService,
        compatibility: LegacyUserEventEffectsAdapter,
    ) -> None:
        self._policy = policy
        self._compatibility = compatibility

    def deliver(self, effect: MediaEffectProjection) -> Mapping[str, Any]:
        prefix = "emby.event.user."
        if effect.effect_type not in self.effect_types or not effect.effect_type.startswith(prefix):
            raise ValueError("P8 user event effect type is invalid")
        kind = effect.effect_type.removeprefix(prefix)
        try:
            if kind == "refresh-user-policy":
                return self._policy.refresh_user_policy(_required_text(effect.payload.get("user_id"), "user_id"))
            if kind == "sync-template-policy":
                return self._policy.sync_template_policy(
                    _required_text(effect.payload.get("updated_user_id"), "updated_user_id"),
                    source_event_key=_required_text(
                        effect.payload.get("source_event_key"),
                        "source_event_key",
                    ),
                )
        except ValueError:
            raise
        except Exception as exc:
            if effect.attempts >= self._policy_attempt_limit:
                raise MediaEffectDeliveryIndeterminate(
                    "P8 user policy effect retry budget was exhausted",
                    error={"kind": kind, "attempts": effect.attempts},
                ) from exc
            raise MediaEffectDeliveryRetryable(
                "P8 user policy effect could not complete before provider write",
                error={"kind": kind, "attempts": effect.attempts},
            ) from exc
        snapshot = effect.payload.get("event_snapshot")
        if not isinstance(snapshot, Mapping):
            raise ValueError("P8 user event effect snapshot is invalid")
        return self._compatibility.execute(kind, snapshot)


def _legacy_event(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    event = {
        "Event": str(snapshot.get("Event") or ""),
        "UserId": str(snapshot.get("UserId") or ""),
        "ItemId": str(snapshot.get("ItemId") or ""),
        "PlaySessionId": str(snapshot.get("PlaySessionId") or ""),
        "DeviceId": str(snapshot.get("DeviceId") or ""),
        "User": dict(_mapping(snapshot.get("User"))),
        "Item": dict(_mapping(snapshot.get("Item"))),
        "UserData": dict(_mapping(snapshot.get("UserData"))),
        "PlaybackInfo": dict(_mapping(snapshot.get("PlaybackInfo"))),
        "Session": dict(_mapping(snapshot.get("Session"))),
    }
    event["_etk_webhook_remote_addr"] = str(snapshot.get("remote_addr") or "")
    return event


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _mapping_result(value: Any, kind: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise RuntimeError(f"{kind} did not return a result object")
    result = dict(value)
    if result.get("ok") is False:
        raise RuntimeError(f"{kind} reported failure")
    return result


def _required_text(value: Any, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return normalized


def _json(value: Mapping[str, Any] | None) -> str:
    return json.dumps(dict(value or {}), ensure_ascii=False, separators=(",", ":"))


__all__ = [
    "LegacyUserEventEffectsAdapter",
    "P8UserEventEffectDelivery",
    "SqlAlchemyTemplatePropagationRepository",
    "TemplatePropagationPlan",
    "TemplatePropagationTemplate",
    "USER_EVENT_EFFECT_TYPES",
    "UserPolicyEffectService",
    "UserPolicyProvider",
]
