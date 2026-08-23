from __future__ import annotations

from importlib import import_module
from typing import Any

from etk.domains.media.bridge_client import BridgeWriteResult

import_module("config_manager")
emby = import_module("handler.emby")


class FakeBridge:
    def __init__(self) -> None:
        self.operations: list[str] = []

    def _result(self, operation: str, *, configured: bool = True) -> BridgeWriteResult:
        self.operations.append(operation)
        return BridgeWriteResult(operation, 200, {"Configured": configured, "operation": operation}, configured)

    def configure_origin(self, etk_url: str) -> BridgeWriteResult:
        del etk_url
        return self._result("configure-origin")

    def configure_discovery_url(self, discovery_url: str) -> BridgeWriteResult:
        del discovery_url
        return self._result("configure-discovery-url")

    def apply_mediainfo(self, item_id: str, raw: Any, **kwargs: Any) -> BridgeWriteResult:
        del item_id, raw, kwargs
        return self._result("apply-mediainfo")

    def apply_intro(self, item_id: str, start: int, end: int) -> BridgeWriteResult:
        del item_id, start, end
        return self._result("apply-intro")

    def apply_credits(self, item_id: str, start: int) -> BridgeWriteResult:
        del item_id, start
        return self._result("apply-credits")

    def update_chapters(self, item_id: str, **kwargs: Any) -> BridgeWriteResult:
        del item_id, kwargs
        return self._result("update-chapters")


def test_all_legacy_bridge_wrappers_delegate_to_the_p5_single_attempt_client(monkeypatch) -> None:
    bridge = FakeBridge()
    monkeypatch.setattr(emby, "_p5_bridge_client", lambda base_url, api_key: bridge)

    assert emby.configure_etk_plugin_origin("http://emby", "token", "http://etk") is True
    assert emby.configure_etk_plugin_discovery_url("http://emby", "token", "http://discovery") is True
    assert emby.apply_etk_mediainfo(
        "item",
        {"MediaSourceInfo": {"MediaStreams": []}},
        "http://emby",
        "token",
    ) == {"Configured": True, "operation": "apply-mediainfo"}
    assert emby.apply_etk_intro_chapters("item", 10, 20, "http://emby", "token") == {
        "Configured": True,
        "operation": "apply-intro",
    }
    assert emby.apply_etk_credits_chapter("item", 30, "http://emby", "token") == {
        "Configured": True,
        "operation": "apply-credits",
    }
    assert emby.update_etk_chapters(
        "item",
        "http://emby",
        "token",
        clear_intro=True,
    ) == {"Configured": True, "operation": "update-chapters"}
    assert bridge.operations == [
        "configure-origin",
        "configure-discovery-url",
        "apply-mediainfo",
        "apply-intro",
        "apply-credits",
        "update-chapters",
    ]
