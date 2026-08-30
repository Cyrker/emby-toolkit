"""Provider boundary for durable sharing operations.

The operation repository deliberately stores only a provider-neutral request.
This module is the only place that turns that request into a call to a sharing
provider.  Mutations receive the executor checkpoint immediately before the
call; recovery accepts only an explicitly declared, read-only readback.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from typing import Any

from etk.integrations import (
    HDHivePort,
    ProviderDisposition,
    ProviderResponse,
    SharedCenterPort,
    TelegramBotPort,
)
from etk.integrations.http_policy import ExternalAdapterError
from etk.platform.jobs.json_payload import canonical_json, normalize_durable_json_object, normalize_json_object

from .execution import SharingDeliveryOutcome, SharingOperationDeliveryPort
from .operations import SharingOperation

_READBACK_COLLECTION_KEYS = ("items", "sources", "subscriptions", "results", "data")


class SharingProviderOperationDelivery(SharingOperationDeliveryPort):
    """Dispatch a strictly allow-listed sharing operation to one provider.

    ``request`` uses the following durable shape::

        {"provider": "shared_center", "action": "register_source",
         "payload": {...},
         "readback": {"provider": "shared_center", "action": "list_sources",
                      "query": {...}}}

    The optional ``readback`` object is intentionally explicit.  A mutation
    without a supported readback is still executed once, but an uncertain
    result is terminalized as ``indeterminate`` by ``SharingOperationExecutor``.
    """

    def __init__(
        self,
        *,
        shared_center: SharedCenterPort,
        hdhive: HDHivePort,
        telegram: TelegramBotPort | None = None,
    ) -> None:
        self._shared_center = shared_center
        self._hdhive = hdhive
        self._telegram = telegram

    def supports_authoritative_readback(self, operation: SharingOperation) -> bool:
        spec = _readback_spec(operation)
        if spec is None:
            return False
        return _readback_callable(self._shared_center, self._hdhive, spec) is not None and _valid_match_spec(spec)

    def request(
        self,
        operation: SharingOperation,
        *,
        checkpoint: Callable[[], None],
    ) -> SharingDeliveryOutcome:
        provider, action, payload = _request_parts(operation)
        try:
            call = _request_callable(self._shared_center, self._hdhive, self._telegram, provider, action)
            if call is None:
                return SharingDeliveryOutcome.rejected(
                    {"type": "unsupported_sharing_provider_action", "provider": provider, "action": action}
                )
            invoke = _request_invocation(call, action, payload)
            if provider == "shared_center":
                validate = getattr(self._shared_center, "validate_configuration", None)
                if callable(validate):
                    validate()
            # No external mutation may happen before this durable transition.
            checkpoint()
            response = invoke()
        except ExternalAdapterError as exc:
            return _exception_outcome(exc)
        except (TypeError, ValueError) as exc:
            return SharingDeliveryOutcome.rejected(
                {"type": "invalid_sharing_provider_request", "error_class": type(exc).__name__}
            )
        return _response_outcome(response)

    def readback(self, operation: SharingOperation) -> SharingDeliveryOutcome:
        spec = _readback_spec(operation)
        if spec is None:
            return SharingDeliveryOutcome.indeterminate({"type": "authoritative_readback_unavailable"})
        provider = _text(spec.get("provider"), "readback.provider")
        action = _text(spec.get("action"), "readback.action")
        query = spec.get("query", {})
        if not isinstance(query, Mapping):
            raise ValueError("readback.query must be an object")
        if not _valid_match_spec(spec):
            return SharingDeliveryOutcome.indeterminate({"type": "invalid_authoritative_readback_spec"})
        call = _readback_callable(self._shared_center, self._hdhive, spec)
        if call is None:
            return SharingDeliveryOutcome.indeterminate(
                {"type": "unsupported_authoritative_readback", "provider": provider, "action": action}
            )
        try:
            response = _invoke_readback(call, action, query)
        except ExternalAdapterError as exc:
            return _exception_outcome(exc)
        outcome = _response_outcome(response)
        if outcome.disposition.value == "confirmed" and not _readback_matches(spec, response.payload):
            return SharingDeliveryOutcome.retryable(
                {"type": "provider_state_not_confirmed", "provider": provider, "action": action},
                retry_after_seconds=response.retry_after_seconds,
            )
        return outcome


def _request_parts(operation: SharingOperation) -> tuple[str, str, dict[str, Any]]:
    request = operation.request
    if not isinstance(request, Mapping):
        raise ValueError("sharing operation request must be an object")
    provider = _text(request.get("provider"), "provider")
    action = _text(request.get("action"), "action")
    payload = request.get("payload", {})
    if not isinstance(payload, Mapping):
        raise ValueError("sharing operation payload must be an object")
    normalized = normalize_durable_json_object(dict(payload), field="sharing provider payload")
    return provider, action, normalized


def _request_callable(
    center: SharedCenterPort,
    hdhive: HDHivePort,
    telegram: TelegramBotPort | None,
    provider: str,
    action: str,
) -> Callable[..., ProviderResponse[Any]] | None:
    if provider == "shared_center":
        return {
            "register_device": center.register_device,
            "register_source": center.register_source,
            "acquire_transfer_lease": center.acquire_transfer_lease,
            "report_transfer": center.report_transfer,
            "share_sync_heartbeat": center.share_sync_heartbeat,
            "run_maintenance": center.run_maintenance,
            "create_share_request": center.create_share_request,
        }.get(action)
    if provider == "hdhive":
        return {
            "unlock": hdhive.unlock,
            "checkin": hdhive.checkin,
            "create_subscription": hdhive.create_subscription,
            "delete_subscription": hdhive.delete_subscription,
        }.get(action)
    if provider == "telegram" and telegram is not None:
        return {
            "send_message": telegram.send_message,
            "send_photo": telegram.send_photo,
        }.get(action)
    return None


def _readback_callable(
    center: SharedCenterPort,
    hdhive: HDHivePort,
    spec: Mapping[str, Any],
) -> Callable[..., ProviderResponse[Any]] | None:
    provider = str(spec.get("provider") or "").strip()
    action = str(spec.get("action") or "").strip()
    if provider == "shared_center":
        return {
            "list_sources": center.list_sources,
            "list_share_requests": center.list_share_requests,
            "device_status": center.device_status,
        }.get(action)
    if provider == "hdhive":
        return {
            "check_subscription": hdhive.check_subscription,
            "get_resources": hdhive.get_resources,
        }.get(action)
    return None


def _request_invocation(
    call: Callable[..., ProviderResponse[Any]], action: str, payload: Mapping[str, Any]
) -> Callable[[], ProviderResponse[Any]]:
    if action == "register_source":
        source_kind = _text(payload.get("source_kind"), "source_kind")
        body = payload.get("body", payload)
        if not isinstance(body, Mapping):
            raise ValueError("register_source body must be an object")
        normalized_body = dict(body)
        return lambda: call(source_kind, normalized_body)
    if action == "register_device":
        name = str(payload.get("name") or "ETK Device")
        server_id_hash = _text(payload.get("server_id_hash"), "server_id_hash")
        return lambda: call(name=name, server_id_hash=server_id_hash)
    if action == "send_message":
        chat_id = _text(payload.get("chat_id"), "chat_id")
        message = _text(payload.get("text"), "text")
        disable_notification = bool(payload.get("disable_notification", False))
        reply_markup = payload.get("reply_markup")
        if reply_markup is not None and not isinstance(reply_markup, Mapping):
            raise ValueError("sharing operation reply_markup must be an object")
        normalized_markup = dict(reply_markup) if isinstance(reply_markup, Mapping) else None
        return lambda: call(
            chat_id,
            message,
            disable_notification=disable_notification,
            reply_markup=normalized_markup,
        )
    if action == "send_photo":
        chat_id = _text(payload.get("chat_id"), "chat_id")
        photo = _text(payload.get("photo"), "photo")
        caption = str(payload.get("caption") or "")
        disable_notification = bool(payload.get("disable_notification", False))
        return lambda: call(
            chat_id,
            photo,
            caption,
            disable_notification=disable_notification,
        )
    if action == "checkin":
        is_gambler = bool(payload.get("is_gambler", False))
        return lambda: call(is_gambler=is_gambler)
    if action == "unlock":
        slug = _text(payload.get("slug"), "slug")
        return lambda: call(slug)
    if action == "create_subscription":
        filters = payload.get("media_filters")
        if filters is not None and not isinstance(filters, Mapping):
            raise ValueError("sharing operation media_filters must be an object")
        target_type = _text(payload.get("target_type"), "target_type")
        target_id = _positive_int(payload.get("target_id"), "target_id")
        target_key = _text(payload.get("target_key"), "target_key")
        normalized_filters = dict(filters) if isinstance(filters, Mapping) else None
        return lambda: call(
            target_type,
            target_id,
            target_key,
            media_filters=normalized_filters,
        )
    if action == "delete_subscription":
        subscription_id = _positive_int(payload.get("subscription_id"), "subscription_id")
        return lambda: call(subscription_id)
    normalized_payload = dict(payload)
    return lambda: call(normalized_payload)


def _invoke_readback(
    call: Callable[..., ProviderResponse[Any]], action: str, query: Mapping[str, Any]
) -> ProviderResponse[Any]:
    if action in {"list_sources", "list_share_requests"}:
        return call(**dict(query))
    if action == "device_status":
        return call()
    if action == "check_subscription":
        return call(_text(query.get("target_type"), "target_type"), _text(query.get("target_key"), "target_key"))
    if action == "get_resources":
        return call(query.get("tmdb_id"), _text(query.get("media_type"), "media_type"), season=query.get("season"))
    raise ValueError(f"unsupported sharing readback action: {action}")


def _response_outcome(response: ProviderResponse[Any]) -> SharingDeliveryOutcome:
    if not isinstance(response, ProviderResponse):
        raise TypeError("sharing provider returned an invalid response")
    details = {"http_status": response.status_code}
    if response.disposition is ProviderDisposition.CONFIRMED:
        return SharingDeliveryOutcome.confirmed(_safe_payload(response.payload, details))
    if response.disposition is ProviderDisposition.REJECTED or response.disposition is ProviderDisposition.NOT_FOUND:
        return SharingDeliveryOutcome.rejected(details)
    if response.disposition is ProviderDisposition.RETRYABLE:
        return SharingDeliveryOutcome.retryable(details, retry_after_seconds=response.retry_after_seconds)
    return SharingDeliveryOutcome.indeterminate(details)


def _exception_outcome(exc: ExternalAdapterError) -> SharingDeliveryOutcome:
    error = {"type": "sharing_provider_error", "error_class": type(exc).__name__}
    if exc.disposition is ProviderDisposition.REJECTED:
        return SharingDeliveryOutcome.rejected(error)
    if exc.disposition is ProviderDisposition.RETRYABLE:
        return SharingDeliveryOutcome.retryable(error)
    return SharingDeliveryOutcome.indeterminate(error)


def _safe_payload(value: Any, details: dict[str, Any]) -> dict[str, Any]:
    if isinstance(value, Mapping):
        normalized = normalize_json_object(dict(value), field="sharing provider result")
        details["provider_result_hash"] = hashlib.sha256(
            canonical_json(normalized, field="sharing provider result").encode("utf-8")
        ).hexdigest()
        safe = {
            key: normalized[key]
            for key in (
                "code",
                "errno",
                "accepted",
                "cost",
                "device_id",
                "group_id",
                "id",
                "lease_id",
                "operation_id",
                "source_id",
                "status",
                "subscription_id",
            )
            if key in normalized and type(normalized[key]) in {bool, int, str}
        }
        if safe:
            details["provider_result"] = safe
    return details


def _readback_spec(operation: SharingOperation) -> Mapping[str, Any] | None:
    value = operation.request.get("readback") if isinstance(operation.request, Mapping) else None
    return value if isinstance(value, Mapping) else None


def _readback_matches(spec: Mapping[str, Any], payload: Any) -> bool:
    key, expected, collection_key = _match_spec(spec)
    if isinstance(payload, Mapping):
        if _mapping_matches(payload, key, expected):
            return True
        collection_key = collection_key or next(
            (candidate for candidate in _READBACK_COLLECTION_KEYS if candidate in payload), None
        )
        if collection_key is not None:
            items = payload.get(collection_key)
            if isinstance(items, list):
                return any(isinstance(item, Mapping) and _mapping_matches(item, key, expected) for item in items)
        return False
    if isinstance(payload, list):
        return any(isinstance(item, Mapping) and _mapping_matches(item, key, expected) for item in payload)
    return False


def _mapping_matches(value: Mapping[str, Any], key: str, expected: Any) -> bool:
    current: Any = value
    for segment in key.split("."):
        if not isinstance(current, Mapping) or segment not in current:
            return False
        current = current[segment]
    return current == expected


def _valid_match_spec(spec: Mapping[str, Any]) -> bool:
    try:
        _match_spec(spec)
    except ValueError:
        return False
    return True


def _match_spec(spec: Mapping[str, Any]) -> tuple[str, Any, str | None]:
    key = spec.get("match_key")
    if not isinstance(key, str) or not key.strip():
        raise ValueError("readback.match_key must be a non-empty string")
    if any(not segment for segment in key.strip().split(".")):
        raise ValueError("readback.match_key contains an empty path segment")
    if "match_value" not in spec:
        raise ValueError("readback.match_value is required")
    collection_key = spec.get("match_list_key")
    if collection_key is None:
        collection_key = next((candidate for candidate in _READBACK_COLLECTION_KEYS if candidate in spec), None)
    if collection_key is not None:
        if not isinstance(collection_key, str) or not collection_key.strip():
            raise ValueError("readback.match_list_key must be a non-empty string")
        if collection_key not in _READBACK_COLLECTION_KEYS:
            raise ValueError("readback.match_list_key is unsupported")
    return key.strip(), spec["match_value"], collection_key


def _text(value: object, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"sharing operation {field} must not be empty")
    return normalized


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"sharing operation {field} must be a positive integer")
    try:
        normalized = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"sharing operation {field} must be a positive integer") from exc
    if normalized < 1:
        raise ValueError(f"sharing operation {field} must be a positive integer")
    return normalized


__all__ = ["SharingProviderOperationDelivery"]
