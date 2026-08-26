from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import httpx
from pydantic import SecretStr

from etk.domains.p115.bridge_compatibility import (
    EmbeddedP115SharedCleanupGateway,
    HttpxP115BridgeEmbyGateway,
    P115DeepDeleteJobGateway,
)
from etk.domains.p115.deep_delete import P115_DEEP_DELETE_OPERATION
from etk.platform import Settings

SHA1 = "A" * 40


def _settings() -> Settings:
    return Settings(
        emby_server_url="http://emby.fixture.invalid",
        emby_api_key=SecretStr("fixture-token"),
        emby_api_timeout=3,
    )


def test_httpx_bridge_gateway_verifies_item_identity_without_exposing_token() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "Items": [
                    {
                        "Id": "100",
                        "Type": "Movie",
                        "Path": "/fixture/movie.strm",
                        "MediaSources": [{"Path": "/api/p115/play/pc-fixture/movie.mkv"}],
                        "Chapters": [
                            {"MarkerType": "IntroStart", "StartPositionTicks": 0},
                            {"MarkerType": "IntroEnd", "StartPositionTicks": 100},
                        ],
                    }
                ]
            },
        )

    gateway = HttpxP115BridgeEmbyGateway(
        _settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(respond)),
    )

    item = gateway.verified_item(SHA1, "100", expected_pick_code="pc-fixture")

    assert item is not None and item["Id"] == "100"
    assert len(requests) == 1
    assert requests[0].headers["X-Emby-Token"] == "fixture-token"
    assert "fixture-token" not in str(requests[0].url)
    assert "Chapters" in requests[0].url.params["Fields"].split(",")
    assert item["Chapters"][1]["MarkerType"] == "IntroEnd"


class _Jobs:
    def __init__(self) -> None:
        self.submit_call: dict[str, Any] | None = None
        self.projection = SimpleNamespace(
            job=SimpleNamespace(
                id="00000000-0000-0000-0000-000000000067",
                request_payload={
                    "positional_parameters": [{"schema_version": 1, "snapshot": {"delete_token": "fixture-token"}}]
                },
            )
        )

    def submit_background_operation(self, function, args, kwargs, **options):
        self.submit_call = {
            "function": function,
            "args": args,
            "kwargs": kwargs,
            "options": options,
        }
        return SimpleNamespace(projection=self.projection)

    def get_idempotent(self, *, scope: str, key: str):
        assert (scope, key) == ("deep-delete", "fixture-token")
        return self.projection


def test_deep_delete_job_gateway_maps_internal_symbol_to_domain_process_adapter() -> None:
    jobs = _Jobs()
    gateway = P115DeepDeleteJobGateway(lambda: jobs)  # type: ignore[arg-type]
    payload = {"schema_version": 1, "snapshot": {"delete_token": "fixture-token"}}

    submitted = gateway.submit(
        P115_DEEP_DELETE_OPERATION,
        payload,
        max_attempts=5,
        timeout_seconds=900,
        idempotency_scope="deep-delete",
        idempotency_key="fixture-token",
    )
    replay = gateway.lookup(idempotency_scope="deep-delete", idempotency_key="fixture-token")

    assert submitted.job_id == "00000000-0000-0000-0000-000000000067"
    assert replay == submitted
    assert jobs.submit_call is not None
    assert jobs.submit_call["function"].__module__ == "etk.processes.p6_deep_delete"
    assert jobs.submit_call["function"].__name__ == "execute_p115_deep_delete_operation"
    assert jobs.submit_call["args"] == (payload,)
    assert jobs.submit_call["options"] == {
        "max_attempts": 5,
        "timeout_seconds": 900,
        "idempotency_scope": "deep-delete",
        "idempotency_key": "fixture-token",
    }


def test_embedded_shared_gateway_exposes_the_real_p5_receipt() -> None:
    receipt = {"ok": True, "removed": 2, "provider": "fixture"}

    result = EmbeddedP115SharedCleanupGateway().cleanup(
        {"delete_token": "fixture-token"},
        {"ok": True, "shared_cleanup": receipt},
        operation_id="fixture-token",
    )

    assert result == receipt
