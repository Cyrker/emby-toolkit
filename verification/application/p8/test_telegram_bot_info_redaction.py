from __future__ import annotations

import logging
from typing import Any, cast

import httpx

from etk.domains.identity.effect_adapters import HttpxTelegramBotInfoGateway
from etk.domains.identity.users import (
    TelegramBotInfoPort,
    UsersMediaProjection,
    UsersRepository,
    UsersService,
)
from etk.platform import Settings

TOKEN = "123456:fixture-secret-token"


def _settings(token: str = TOKEN) -> Settings:
    return Settings.model_validate({"telegram_bot_token": token})


def _service(port: TelegramBotInfoPort, *, token: str = TOKEN) -> UsersService:
    return UsersService(
        repository=cast(UsersRepository, object()),
        media=cast(UsersMediaProjection, object()),
        settings=lambda: _settings(token),
        telegram=port,
    )


class RaisingPort:
    @staticmethod
    def bot_username() -> str | None:
        raise RuntimeError(f"https://api.telegram.org/bot{TOKEN}/getMe failed")


def test_service_returns_a_fixed_failure_envelope_without_provider_exception() -> None:
    result = _service(RaisingPort()).telegram_bot_info()

    assert result.status_code == 200
    assert result.payload == {"bot_username": None, "error": "Telegram 服务暂时不可用"}
    assert TOKEN not in repr(result.payload)


def test_http_adapter_never_logs_the_token_bearing_url(caplog: Any) -> None:
    def fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(f"failed at {request.url}", request=request)

    adapter = HttpxTelegramBotInfoGateway(
        lambda: _settings(),
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(fail)),
    )

    with caplog.at_level(logging.WARNING):
        assert adapter.bot_username() is None

    assert TOKEN not in caplog.text
    assert f"bot{TOKEN}" not in caplog.text
    assert "api.telegram.org" not in caplog.text


def test_missing_token_does_not_call_the_port() -> None:
    class UnexpectedPort:
        @staticmethod
        def bot_username() -> str | None:
            raise AssertionError("port must not be called")

    result = _service(UnexpectedPort(), token="").telegram_bot_info()

    assert result.payload == {"bot_username": None, "error": "Bot Token未配置"}
