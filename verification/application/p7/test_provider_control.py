from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from etk.domains.sharing.service import SharingHttpResult
from etk.domains.subscription.provider_control import HDHiveRegistrationOperationDelivery, ProviderControlService
from etk.integrations import ProviderDisposition, ProviderResponse


def _hash(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()


class SettingsStore:
    def __init__(self, values: Mapping[str, Any] | None = None) -> None:
        self.values = dict(values or {})

    def load(self, key: str) -> Any:
        return self.values.get(key)

    def save(self, key: str, value: Any) -> None:
        self.values[key] = value

    def merge_mapping(
        self,
        key: str,
        updates: Mapping[str, Any],
        *,
        remove_keys: Sequence[str] = (),
    ) -> Mapping[str, Any]:
        current = dict(self.values.get(key) or {})
        for removed in remove_keys:
            current.pop(removed, None)
        current.update(updates)
        self.values[key] = current
        return current


class HDHive:
    def __init__(self) -> None:
        self.mutations: list[str] = []

    def register(self) -> ProviderResponse[dict[str, Any]]:
        self.mutations.append("register")
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {
                "success": True,
                "data": {
                    "instance_id": "fixture-instance",
                    "instance_secret": "fixture-instance-secret",
                },
            },
        )

    @staticmethod
    def ping() -> ProviderResponse[dict[str, Any]]:
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {"success": True, "has_access_token": True, "scopes": ["meta", "vip"]},
        )

    @staticmethod
    def authorize_url() -> ProviderResponse[dict[str, Any]]:
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {"success": True, "data": {"authorize_url": "https://hdhive.fixture/authorize"}},
        )

    @staticmethod
    def user_info() -> ProviderResponse[dict[str, Any]]:
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {"success": True, "data": {"level": "vip", "nickname": "Fixture"}},
        )

    @staticmethod
    def usage_today() -> ProviderResponse[dict[str, Any]]:
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"success": True, "data": {"count": 1}})

    @staticmethod
    def vip_entitlements() -> ProviderResponse[dict[str, Any]]:
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {"success": True, "data": {"active": True}},
        )

    @staticmethod
    def get_resources(
        tmdb_id: str,
        media_type: str,
        *,
        season: int | None = None,
    ) -> ProviderResponse[dict[str, Any]]:
        del tmdb_id, media_type, season
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {
                "success": True,
                "data": [
                    {
                        "slug": "fixture-hdhive",
                        "title": "Fixture 1080p 中字",
                        "unlock_points": 1,
                        "share_size": "2 GB",
                    }
                ],
            },
        )

    @staticmethod
    def list_subscriptions(**kwargs: Any) -> ProviderResponse[dict[str, Any]]:
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"success": True, "query": kwargs})

    @staticmethod
    def check_subscription(target_type: str, target_key: str) -> ProviderResponse[dict[str, Any]]:
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {"success": True, "data": {"target_type": target_type, "target_key": target_key}},
        )


class Center:
    @staticmethod
    def validate_configuration() -> None:
        return None

    @staticmethod
    def list_cloud_search_sources(**query: Any) -> ProviderResponse[dict[str, Any]]:
        del query
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {
                "success": True,
                "items": [
                    {
                        "source_kind": "movie",
                        "source_id": "center-fixture",
                        "tmdb_id": "42",
                        "title": "Fixture Movie",
                        "release_year": 2026,
                        "sha1": "A" * 40,
                        "share_channel": {
                            "channel_id": "channel-fixture",
                            "status": "valid",
                            "share_code": "provider-share-secret",
                            "receive_code": "provider-receive-secret",
                        },
                    }
                ],
            },
        )


class Telegram:
    def __init__(self) -> None:
        self.commands: list[tuple[str, dict[str, Any], str, str | None]] = []
        self.secrets: dict[str, dict[str, Any]] = {}
        self.command_states: dict[str, dict[str, Any]] = {}
        self.owner = ""

    def enqueue_once(
        self,
        command_type: str,
        payload: Mapping[str, Any] | None = None,
        *,
        idempotency_key: str,
        result_owner_key: str | None = None,
    ) -> str:
        self.commands.append((command_type, dict(payload or {}), idempotency_key, result_owner_key))
        self.owner = str(result_owner_key or "")
        self.command_states["fixture-command"] = {"status": "pending", "payload": {"secret": "hidden"}}
        return "fixture-command"

    def wait(self, command_id: str, **kwargs: Any) -> Mapping[str, Any]:
        del command_id, kwargs
        if self.commands[-1][0] == "search":
            raw = {
                "ok": True,
                "results": [
                    {
                        "source_chat_id": "-10042",
                        "message_id": "7",
                        "source_channel": "Fixture Channel",
                        "title": "Fixture Movie",
                        "target_link": "https://115.com/s/fixture",
                    }
                ],
            }
            self.secrets["a" * 64] = raw
            return {
                "status": "succeeded",
                "result": {
                    "ok": True,
                    "search_result_ref": "a" * 64,
                    "search_result_hash": _hash(raw),
                },
            }
        return {"status": "succeeded", "result": {"success": True}}

    def consume_resource_secret(self, reference: str, owner_key: str) -> Mapping[str, Any]:
        assert owner_key == self.owner or owner_key
        return self.secrets[reference]

    def create_resource_secret(
        self,
        payload: Mapping[str, Any],
        *,
        reference: str,
        owner_key: str,
        ttl_seconds: float,
    ) -> tuple[str, bool]:
        del owner_key, ttl_seconds
        self.secrets[reference] = dict(payload)
        return reference, True

    def destroy_resource_secret(self, reference: str, *, owner_key: str | None = None) -> bool:
        del owner_key
        return self.secrets.pop(reference, None) is not None

    def get_command(self, command_id: str) -> Mapping[str, Any] | None:
        return self.command_states.get(command_id)

    @staticmethod
    def get_runtime_status() -> Mapping[str, Any]:
        return {"status": "authorized", "updated_at": "2026-09-01T00:00:00+00:00", "secret": "hidden"}


class Sharing:
    def __init__(self) -> None:
        self.operations: list[dict[str, Any]] = []

    def stage_provider_operation(self, **kwargs: Any) -> SharingHttpResult:
        self.operations.append(kwargs)
        return SharingHttpResult(
            {
                "success": True,
                "accepted": True,
                "operation_id": str(uuid4()),
                "status": "ready",
                "idempotency_key": kwargs["idempotency_key"],
            },
            202,
        )

    def import_center_source(self, payload: Mapping[str, Any], **kwargs: Any) -> SharingHttpResult:
        self.operations.append({"payload": dict(payload), **kwargs})
        return SharingHttpResult(
            {"success": True, "accepted": True, "operation_id": str(uuid4()), "status": "ready"},
            202,
        )


def _service(settings: SettingsStore | None = None) -> tuple[ProviderControlService, HDHive, Telegram, Sharing]:
    hdhive = HDHive()
    telegram = Telegram()
    sharing = Sharing()
    service = ProviderControlService(
        settings=settings or SettingsStore(),
        hdhive=hdhive,  # type: ignore[arg-type]
        center=Center(),  # type: ignore[arg-type]
        telegram=telegram,  # type: ignore[arg-type]
        sharing=sharing,  # type: ignore[arg-type]
        clock=lambda: datetime(2026, 9, 1, tzinfo=UTC),
    )
    return service, hdhive, telegram, sharing


def test_provider_control_masks_telegram_password_and_preserves_masked_save() -> None:
    settings = SettingsStore(
        {"tg_userbot_config": {"enabled": True, "phone": "+8613800000000", "password": "fixture-secret"}}
    )
    service, _hdhive, _telegram, _sharing = _service(settings)

    loaded = service.telegram_configuration().payload["data"]
    assert loaded["password"] == "********"
    assert "api_id" not in loaded
    service.save_telegram_configuration(
        {"enabled": True, "phone": "+8613800000000", "password": "********", "api_id": 1, "api_hash": "x"}
    )

    assert settings.values["tg_userbot_config"] == {
        "enabled": True,
        "phone": "+8613800000000",
        "password": "fixture-secret",
    }


def test_cloud_search_aggregates_shared_hdhive_and_opaque_channel_results() -> None:
    settings = SettingsStore(
        {
            "hdhive_config": {
                "filter": {
                    "free_only": False,
                    "max_points": 10,
                    "max_size_gb": 120,
                    "resolution": "All",
                    "zh_sub_only": True,
                    "exclude_iso": False,
                }
            }
        }
    )
    service, _hdhive, telegram, _sharing = _service(settings)

    result = service.cloud_resources(
        query={"tmdb_id": "42", "title": "Fixture Movie", "year": "2026", "media_type": "movie"},
        idempotency_key="fixture-search",
    )

    assert result.status_code == 200
    assert [item["source_type"] for item in result.payload["data"]] == ["shared_pool", "hdhive", "channel"]
    assert result.payload["stats"] == {
        "shared_pool_total": 1,
        "hdhive_total": 1,
        "hdhive_filtered": 1,
        "channel_total": 1,
        "shown": 3,
        "limit": 50,
        "warnings": [],
    }
    assert telegram.commands[0][0] == "search"
    assert telegram.commands[0][3] is not None
    shared = result.payload["data"][0]
    assert shared["share_channel"] == {"channel_id": "channel-fixture", "status": "valid"}
    assert "provider-share-secret" not in json.dumps(shared)
    assert "provider-receive-secret" not in json.dumps(shared)


def test_cloud_download_preserves_explicit_shared_pool_share_mode() -> None:
    service, _hdhive, _telegram, sharing = _service()

    result = service.cloud_download(
        {
            "source_type": "shared_pool",
            "mode": "share",
            "resource": {
                "source_kind": "logical_season",
                "source_id": "logical-season-fixture",
                "title": "Fixture Series",
            },
        },
        principal_id="fixture-admin",
        idempotency_key="share-transfer-1",
    )

    assert result.status_code == 202
    assert sharing.operations[-1]["payload"]["mode"] == "share"
    assert sharing.operations[-1]["payload"]["force_real"] is True
    assert sharing.operations[-1]["idempotency_key"] == "share-transfer-1"


def test_hdhive_mutations_only_stage_durable_operations() -> None:
    service, hdhive, _telegram, sharing = _service()

    checkin = service.hdhive_checkin({}, principal_id="fixture-admin", idempotency_key="checkin-1")
    download = service.hdhive_download(
        {"slug": "fixture-slug", "tmdb_id": "42", "media_type": "movie"},
        principal_id="fixture-admin",
        idempotency_key="download-1",
    )

    assert checkin.status_code == 202
    assert download.status_code == 202
    assert [item["request"]["action"] for item in sharing.operations] == ["checkin", "hdhive_transfer"]
    assert hdhive.mutations == []


def test_telegram_login_externalizes_code_before_durable_enqueue() -> None:
    service, _hdhive, telegram, _sharing = _service()

    result = service.telegram_login("12345", idempotency_key="login-1")

    assert result.payload["success"] is True
    command_type, payload, key, owner = telegram.commands[0]
    assert command_type == "login"
    assert key == "telegram-control:login:login-1"
    assert owner is None
    assert "code" not in payload
    assert payload["code_ref"]
    assert payload["code_hash"]
    assert all("12345" not in json.dumps(command, ensure_ascii=False) for command in telegram.commands)


def test_telegram_status_allowlists_runtime_fields() -> None:
    service, _hdhive, _telegram, _sharing = _service()

    assert service.telegram_status().payload == {
        "success": True,
        "data": {"status": "authorized", "updated_at": "2026-09-01T00:00:00+00:00"},
    }


def test_telegram_status_reads_safe_durable_command_state_without_result_payload() -> None:
    service, _hdhive, telegram, _sharing = _service()
    telegram.command_states["fixture-pending"] = {
        "status": "requesting",
        "payload": {"code_ref": "secret-reference"},
        "result": {"telegram_session": "secret-session"},
        "error": "provider detail",
    }

    assert service.telegram_status("fixture-pending").payload == {
        "success": False,
        "accepted": True,
        "status": "requesting",
        "command_id": "fixture-pending",
        "message": "Telegram 操作仍在处理中",
    }
    assert service.telegram_status("expired-command").payload == {
        "success": False,
        "accepted": False,
        "status": "missing",
        "command_id": "expired-command",
        "message": "Telegram 操作记录不存在或已过期",
    }


def test_hdhive_registration_checkpoints_once_and_persists_credentials_outside_operation_result() -> None:
    settings = SettingsStore({"hdhive_config": {"checkin_mode": "normal"}})
    hdhive = HDHive()
    delivery = HDHiveRegistrationOperationDelivery(hdhive, settings)
    checkpoints: list[bool] = []

    outcome = delivery.request(object(), checkpoint=lambda: checkpoints.append(True))  # type: ignore[arg-type]

    assert checkpoints == [True]
    assert hdhive.mutations == ["register"]
    assert settings.values["hdhive_config"] == {
        "checkin_mode": "normal",
        "relay_instance_id": "fixture-instance",
        "relay_instance_secret": "fixture-instance-secret",
    }
    assert outcome.result == {
        "registered": True,
        "instance_id_hash": hashlib.sha256(b"fixture-instance").hexdigest(),
    }
    assert "secret" not in json.dumps(outcome.result)


def test_hdhive_registration_ambiguity_and_persistence_failure_are_indeterminate() -> None:
    class IndeterminateHDHive(HDHive):
        def register(self) -> ProviderResponse[dict[str, Any]]:
            self.mutations.append("register")
            return ProviderResponse(ProviderDisposition.INDETERMINATE, 503, None)

    settings = SettingsStore()
    hdhive = IndeterminateHDHive()
    checkpoints: list[bool] = []
    outcome = HDHiveRegistrationOperationDelivery(hdhive, settings).request(
        object(),  # type: ignore[arg-type]
        checkpoint=lambda: checkpoints.append(True),
    )

    assert checkpoints == [True]
    assert hdhive.mutations == ["register"]
    assert outcome.disposition.value == "indeterminate"
    assert settings.values == {}

    class FailingSettings(SettingsStore):
        def merge_mapping(
            self,
            key: str,
            updates: Mapping[str, Any],
            *,
            remove_keys: Sequence[str] = (),
        ) -> Mapping[str, Any]:
            del key, updates, remove_keys
            raise OSError("fixture persistence failure")

    confirmed_hdhive = HDHive()
    persistence_outcome = HDHiveRegistrationOperationDelivery(confirmed_hdhive, FailingSettings()).request(
        object(),  # type: ignore[arg-type]
        checkpoint=lambda: None,
    )

    assert confirmed_hdhive.mutations == ["register"]
    assert persistence_outcome.disposition.value == "indeterminate"
    assert persistence_outcome.error == {
        "type": "hdhive_registration_persistence_error",
        "error_class": "OSError",
    }
