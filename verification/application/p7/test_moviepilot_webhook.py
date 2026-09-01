from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

from etk.bootstrap.app_factory import create_app
from etk.domains.media.effect_execution import (
    MediaEffectDeliveryIndeterminate,
    MediaEffectDeliveryRejected,
    MediaEffectDeliveryRetryable,
)
from etk.domains.p115.models import P115CredentialSnapshot
from etk.domains.subscription.moviepilot_transfer import freeze_moviepilot_credential_binding
from etk.domains.subscription.moviepilot_webhook import (
    MOVIEPILOT_ASSISTANT_EVENT_TYPE,
    MOVIEPILOT_TRANSFER_EVENT_TYPE,
    MoviePilotWebhookEffectDelivery,
    MoviePilotWebhookService,
)
from etk.domains.subscription.moviepilot_webhook_router import router
from etk.domains.subscription.operations import SubscriptionOperationStatus
from etk.platform import AppContainer, Settings
from etk.processes.worker import (
    _moviepilot_full_washing_guard,
    _moviepilot_full_washing_timeout_hours,
)

_CREDENTIAL = P115CredentialSnapshot(
    account_id="115001",
    access_token="fixture-openapi-token",
)
_REPO_ROOT = Path(__file__).resolve().parents[3]


class _Credentials:
    def load(self) -> P115CredentialSnapshot:
        return _CREDENTIAL


class _Settings:
    def __init__(self, *, assistant_enabled: bool = True) -> None:
        self.assistant_enabled = assistant_enabled

    def load(self, key: str) -> Any:
        values = {
            "mp_config": {
                "moviepilot_url": "https://moviepilot.fixture",
                "subscribe_assistant": {"enabled": self.assistant_enabled},
            },
            "watchlist_config": {},
            "subscription_strategy_config": {"subscription_sources": ["mp"]},
        }
        return values.get(key)


class _AppSettings:
    def __init__(self, values: dict[str, Any]) -> None:
        self.values = values

    def load(self, key: str) -> Any:
        return self.values.get(key)


class _Ledger:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def reserve(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        return SimpleNamespace(effect=SimpleNamespace(id="effect-fixture"))


class _Assistant:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def apply(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {"handled": True, "subscribe_id": 7}

    def full_washing_started_at(self, **kwargs: Any) -> float:
        self.calls.append(kwargs)
        return 0

    def prepare_full_washing_timeout(self, **kwargs: Any) -> bool:
        self.calls.append(kwargs)
        return True

    def finalize_full_washing_timeout(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {"handled": True, "completion_timed_out": True}

    def finalize_full_washing_failure(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {"handled": True, "completion_failed": True}


class _Transfer:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def process(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {"job_id": "job-fixture", "job_reused": False}


class _DeferredAssistant:
    def __init__(self, *, started_at: float | None = None) -> None:
        self.started_at = started_at

    def apply(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        result: dict[str, Any] = {
            "handled": True,
            "completion_deferred": True,
            "reconciliation": "consistency_failed",
        }
        if self.started_at is not None:
            result["full_washing_started_at"] = self.started_at
        return result

    def full_washing_started_at(self, **kwargs: Any) -> float:
        del kwargs
        return float(self.started_at or 0)

    def finalize_full_washing_failure(self, **kwargs: Any) -> dict[str, Any]:
        return {
            "handled": True,
            "completion_failed": True,
            "phase": kwargs["phase"],
            "operation_id": kwargs["operation_id"],
            "operation_status": kwargs["operation_status"].value,
        }


class _DatabaseUnavailableAssistant:
    def full_washing_started_at(self, **kwargs: Any) -> float:
        del kwargs
        return 0

    def apply(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        raise SQLAlchemyError("database unavailable")


class _TimeoutFinalizeUnavailableAssistant(_DeferredAssistant):
    def prepare_full_washing_timeout(self, **kwargs: Any) -> bool:
        del kwargs
        return True

    def finalize_full_washing_timeout(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        raise SQLAlchemyError("database unavailable after provider confirmation")


class _FullWashing:
    def __init__(self, status: SubscriptionOperationStatus = SubscriptionOperationStatus.READY) -> None:
        self.status = status

    def ensure_restart(self, **kwargs: Any) -> tuple[str, Any]:
        del kwargs
        return (
            "resume",
            SimpleNamespace(id="operation-fixture", status=self.status),
        )

    def ensure_timeout(self, **kwargs: Any) -> tuple[str, Any]:
        del kwargs
        raise AssertionError("timeout must remain disabled")


class _ConfirmedTimeout:
    def ensure_timeout(self, **kwargs: Any) -> tuple[str, Any]:
        del kwargs
        return (
            "timeout-delete",
            SimpleNamespace(id="operation-fixture", status=SubscriptionOperationStatus.CONFIRMED),
        )


def _service(*, passthrough: bool = False, assistant_enabled: bool = True) -> tuple[MoviePilotWebhookService, _Ledger]:
    ledger = _Ledger()
    service = MoviePilotWebhookService(
        ledger,  # type: ignore[arg-type]
        _Settings(assistant_enabled=assistant_enabled),
        lambda: Settings(p115_mp_classify=passthrough),
        _Credentials(),
    )
    return service, ledger


def _transfer_payload(*, event_type: str = "transfer.complete", item_type: str = "file") -> dict[str, Any]:
    return {
        "type": event_type,
        "event_id": "mp-transfer-fixture",
        "data": {
            "transferinfo": {
                "target_item": {
                    "fileid": "11501",
                    "name": "Fixture.S01E02.mkv",
                    "type": item_type,
                    "pickcode": "pc-fixture",
                    "path": "/Fixture/Fixture.S01E02.mkv",
                },
                "target_diritem": {"fileid": "11000"},
            },
            "mediainfo": {"tmdb_id": "123", "type": "电视剧", "title": "Fixture"},
            "meta": {"begin_season": 1, "begin_episode": 2},
        },
    }


def test_legacy_emby_marker_is_removed_without_reserving_an_effect() -> None:
    service, ledger = _service()

    result = service.handle({"Event": "metadata.update", "type": "transfer.complete"})

    assert result.status_code == 200
    assert result.payload == {"status": "emby_webhook_removed"}
    assert ledger.calls == []


def test_assistant_event_reserves_one_durable_effect_and_disabled_configuration_is_ignored() -> None:
    service, ledger = _service()
    payload = {
        "type": "subscribe.added",
        "event_id": "subscribe-added-fixture",
        "data": {"subscribe_id": 7, "subscribe_info": {"id": 7, "tmdbid": 123}},
    }

    result = service.handle(payload)
    disabled, disabled_ledger = _service(assistant_enabled=False)
    ignored = disabled.handle(payload)

    assert result.payload == {"status": "subscribe_assistant_processed"}
    assert len(ledger.calls) == 1
    assert ledger.calls[0]["effect_type"] == MOVIEPILOT_ASSISTANT_EVENT_TYPE
    assert ledger.calls[0]["aggregate_id"] == "subscription:7"
    assert ignored.payload == {"status": "subscribe_assistant_ignored"}
    assert disabled_ledger.calls == []


def test_worker_full_washing_guard_uses_mp_then_watchlist_and_defaults_enabled() -> None:
    assert (
        _moviepilot_full_washing_guard(
            _AppSettings(
                {
                    "mp_config": {
                        "subscribe_assistant": {
                            "best_version_full_consistency_check_enabled": False,
                        }
                    },
                    "watchlist_config": {
                        "subscribe_assistant": {
                            "best_version_full_consistency_check_enabled": True,
                        }
                    },
                }
            )
        )
        is False
    )
    assert (
        _moviepilot_full_washing_guard(
            _AppSettings(
                {
                    "mp_config": {},
                    "watchlist_config": {
                        "subscribe_assistant": {
                            "best_version_full_consistency_check_enabled": "off",
                        }
                    },
                }
            )
        )
        is False
    )
    assert _moviepilot_full_washing_guard(_AppSettings({})) is True
    assert (
        _moviepilot_full_washing_guard(
            _AppSettings(
                {
                    "mp_config": {
                        "subscribe_assistant": {
                            "best_version_full_consistency_check_enabled": "yes",
                        }
                    }
                }
            )
        )
        is True
    )
    assert (
        _moviepilot_full_washing_timeout_hours(
            _AppSettings(
                {
                    "mp_config": {
                        "subscribe_assistant": {"full_washing_timeout_hours": 0},
                    }
                }
            )
        )
        == 0
    )


def test_download_effect_accepts_subscribe_prefix_without_persisting_tracker_urls() -> None:
    service, ledger = _service()
    payload = {
        "type": "download.added",
        "event_id": "download-added-fixture",
        "data": {
            "hash": "ABCDEF",
            "source": 'Subscribe|{"id":7,"tmdbid":"123","season":1}',
            "context": {
                "torrent_info": {
                    "title": "Fixture.S01",
                    "site_name": "Fixture",
                    "page_url": "https://tracker.invalid/item?token=secret-value",
                    "enclosure": "https://tracker.invalid/download?passkey=secret-value",
                }
            },
        },
    }

    result = service.handle(payload)

    assert result.payload == {"status": "subscribe_assistant_processed"}
    stored = ledger.calls[0]["payload"]["payload"]
    assert stored["data"]["source"] == {"id": 7, "tmdbid": "123", "season": 1}
    rendered = json.dumps(stored, ensure_ascii=False)
    assert "secret-value" not in rendered
    assert "page_url" not in rendered
    assert "enclosure" not in rendered


def test_transfer_event_preserves_legacy_envelopes_and_freezes_normalized_file() -> None:
    buffered, buffered_ledger = _service()
    passthrough, passthrough_ledger = _service(passthrough=True)

    first = buffered.handle(_transfer_payload())
    second = passthrough.handle(_transfer_payload(event_type="transfer.subtitle.complete"))
    ignored = buffered.handle(_transfer_payload(item_type="dir"))
    missing = buffered.handle({"type": "transfer.complete", "data": {}})

    assert first.payload == {"status": "processing_single_file"}
    assert second.payload == {"status": "processing_single_file_passthrough"}
    assert ignored.payload == {"status": "ignored_not_file"}
    assert missing.payload == {"status": "ignored_missing_data"}
    assert buffered_ledger.calls[0]["effect_type"] == MOVIEPILOT_TRANSFER_EVENT_TYPE
    assert buffered_ledger.calls[0]["payload"]["payload"]["file"] == {
        "file_id": "11501",
        "file_name": "Fixture.S01E02.mkv",
        "file_type": "file",
        "pick_code": "pc-fixture",
        "parent_id": "11000",
        "tmdb_id": "123",
        "media_type": "tv",
        "title": "Fixture",
        "season_number": 1,
        "episode_number": 2,
        "path": "/Fixture/Fixture.S01E02.mkv",
        **freeze_moviepilot_credential_binding(_CREDENTIAL, "openapi"),
    }
    assert passthrough_ledger.calls[0]["payload"]["payload"]["passthrough"] is True


def test_effect_delivery_dispatches_assistant_and_transfer_without_legacy_manager() -> None:
    assistant = _Assistant()
    transfer = _Transfer()
    delivery = MoviePilotWebhookEffectDelivery(assistant, transfer)
    common = {
        "event_key": "a" * 64,
        "event_type": "subscribe.added",
        "payload_digest": "b" * 64,
        "payload": {"data": {"subscribe_id": 7}},
    }

    assistant_result = delivery.deliver(
        SimpleNamespace(effect_type=MOVIEPILOT_ASSISTANT_EVENT_TYPE, payload=common, attempts=1)  # type: ignore[arg-type]
    )
    transfer_result = delivery.deliver(
        SimpleNamespace(
            effect_type=MOVIEPILOT_TRANSFER_EVENT_TYPE,
            payload={
                **common,
                "event_type": "transfer.complete",
                "payload": {"file": {"file_id": "11501"}, "passthrough": True},
            },
            attempts=1,
        )  # type: ignore[arg-type]
    )

    assert assistant_result == {"handled": True, "subscribe_id": 7}
    assert transfer_result == {"job_id": "job-fixture", "job_reused": False}
    assert assistant.calls[0]["event_key"] == "a" * 64
    assert transfer.calls == [
        {
            "event_key": "a" * 64,
            "event_type": "transfer.complete",
            "file": {"file_id": "11501"},
            "passthrough": True,
        }
    ]


def test_full_washing_delivery_stays_retryable_until_native_reconciliation_finishes() -> None:
    delivery = MoviePilotWebhookEffectDelivery(
        _DeferredAssistant(),  # type: ignore[arg-type]
        _Transfer(),
        _FullWashing(),  # type: ignore[arg-type]
    )
    effect = SimpleNamespace(
        effect_type=MOVIEPILOT_ASSISTANT_EVENT_TYPE,
        payload={
            "event_key": "a" * 64,
            "event_type": "subscribe.complete",
            "payload_digest": "b" * 64,
            "payload": {"data": {"subscribe_id": 7}},
        },
        attempts=1,
    )

    with pytest.raises(MediaEffectDeliveryRetryable, match="restart is pending"):
        delivery.deliver(effect)  # type: ignore[arg-type]


def test_full_washing_timeout_zero_preserves_disabled_compatibility_semantics() -> None:
    delivery = MoviePilotWebhookEffectDelivery(
        _DeferredAssistant(started_at=1),  # type: ignore[arg-type]
        _Transfer(),
        _FullWashing(),  # type: ignore[arg-type]
        full_washing_timeout_hours=lambda: 0,
    )
    effect = SimpleNamespace(
        effect_type=MOVIEPILOT_ASSISTANT_EVENT_TYPE,
        payload={
            "event_key": "a" * 64,
            "event_type": "subscribe.complete",
            "payload_digest": "b" * 64,
            "payload": {"data": {"subscribe_id": 7}},
        },
        attempts=1,
    )

    with pytest.raises(MediaEffectDeliveryRetryable, match="restart is pending"):
        delivery.deliver(effect)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (SubscriptionOperationStatus.REJECTED, MediaEffectDeliveryRejected),
        (SubscriptionOperationStatus.CANCELLED, MediaEffectDeliveryRejected),
        (SubscriptionOperationStatus.INDETERMINATE, MediaEffectDeliveryIndeterminate),
    ],
)
def test_full_washing_terminal_restart_operation_is_not_retried(
    status: SubscriptionOperationStatus,
    error_type: type[Exception],
) -> None:
    delivery = MoviePilotWebhookEffectDelivery(
        _DeferredAssistant(),  # type: ignore[arg-type]
        _Transfer(),
        _FullWashing(status),  # type: ignore[arg-type]
    )
    effect = SimpleNamespace(
        effect_type=MOVIEPILOT_ASSISTANT_EVENT_TYPE,
        payload={
            "event_key": "a" * 64,
            "event_type": "subscribe.complete",
            "payload_digest": "b" * 64,
            "payload": {"data": {"subscribe_id": 7}},
        },
        attempts=1,
    )

    with pytest.raises(error_type):
        delivery.deliver(effect)  # type: ignore[arg-type]


def test_assistant_database_failure_remains_retryable() -> None:
    delivery = MoviePilotWebhookEffectDelivery(
        _DatabaseUnavailableAssistant(),  # type: ignore[arg-type]
        _Transfer(),
    )
    effect = SimpleNamespace(
        effect_type=MOVIEPILOT_ASSISTANT_EVENT_TYPE,
        payload={
            "event_key": "a" * 64,
            "event_type": "subscribe.complete",
            "payload_digest": "b" * 64,
            "payload": {"data": {"subscribe_id": 7}},
        },
        attempts=1,
    )

    with pytest.raises(MediaEffectDeliveryRetryable, match="persistence is temporarily unavailable"):
        delivery.deliver(effect)  # type: ignore[arg-type]


def test_confirmed_timeout_finalization_database_failure_remains_retryable() -> None:
    delivery = MoviePilotWebhookEffectDelivery(
        _TimeoutFinalizeUnavailableAssistant(started_at=1),  # type: ignore[arg-type]
        _Transfer(),
        _ConfirmedTimeout(),  # type: ignore[arg-type]
        full_washing_timeout_hours=lambda: 1,
    )
    effect = SimpleNamespace(
        effect_type=MOVIEPILOT_ASSISTANT_EVENT_TYPE,
        payload={
            "event_key": "a" * 64,
            "event_type": "subscribe.complete",
            "payload_digest": "b" * 64,
            "payload": {"data": {"subscribe_id": 7}},
        },
        attempts=1,
    )

    with pytest.raises(MediaEffectDeliveryRetryable, match="persistence is temporarily unavailable"):
        delivery.deliver(effect)  # type: ignore[arg-type]


def test_native_http_route_accepts_silent_json_and_exposes_conflict_contract() -> None:
    service, ledger = _service()
    container = AppContainer(
        Settings(),
        use_case_providers={"subscription.moviepilot-webhook": lambda _: service},
    )
    app = FastAPI()
    app.state.container = container
    app.include_router(router)

    with TestClient(app) as client:
        transfer = client.post("/webhook/emby", json=_transfer_payload())
        unknown = client.post("/webhook/emby", content="not-json", headers={"Content-Type": "application/json"})

    assert transfer.status_code == 200
    assert transfer.json() == {"status": "processing_single_file"}
    assert unknown.status_code == 200
    assert unknown.json() == {"status": "event_ignored_not_in_trigger_list"}
    operation = app.openapi()["paths"]["/webhook/emby"]["post"]
    assert set(operation["responses"]) == {"200", "409", "500", "503"}
    assert len(ledger.calls) == 1


def test_combined_app_and_route_manifest_use_the_native_moviepilot_owner(tmp_path: Path) -> None:
    app = create_app(start_runtime=False, data_dir=tmp_path / "config")
    try:
        route_index, route = next(
            (index, nested)
            for index, value in enumerate(app.routes)
            for nested in getattr(getattr(value, "original_router", None), "routes", ())
            if isinstance(nested, APIRoute) and nested.path == "/webhook/emby" and "POST" in (nested.methods or set())
        )
        assert route.endpoint.__module__ == "etk.domains.subscription.moviepilot_webhook_router"
        compatibility_index = next(
            index
            for index, value in enumerate(app.routes)
            if isinstance(value, APIRoute) and value.path == "/{path:path}"
        )
        assert route_index < compatibility_index
    finally:
        app.state.container.close()

    ownership = json.loads((_REPO_ROOT / "verification/contracts/route-ownership.yaml").read_text(encoding="utf-8"))
    decorator = next(value for value in ownership["routes"] if value["path"] == "/webhook/emby")
    binding = next(
        value
        for value in ownership["method_bindings"]
        if value["path"] == "/webhook/emby" and value["method"] == "POST"
    )
    for value in (decorator, binding):
        assert value["status"] == "new"
        assert value["new_owner"] == "etk.domains.subscription.moviepilot_webhook_router"
        assert value["fixture"] == "verification/fixtures/http/p7-moviepilot-webhook.json"
