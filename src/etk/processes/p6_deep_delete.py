from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from etk.compatibility.upload_monitor import persistent_upload_source_cleanup
from etk.domains.p115.bridge_compatibility import (
    EmbeddedP115SharedCleanupGateway,
    LegacyP115DeleteReadbackGateway,
    LegacyP115MediaCleanupReceiptGateway,
)
from etk.domains.p115.deep_delete import P115DeepDeleteExecutionService


def execute_p115_deep_delete_operation(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    """Worker-importable P6 operation; compatibility effects remain explicit until P11."""

    upload_sources = persistent_upload_source_cleanup()
    if upload_sources is None:
        raise RuntimeError("persistent P115 upload source cleanup gateway is not installed")
    return P115DeepDeleteExecutionService(
        p115=LegacyP115DeleteReadbackGateway(),
        upload_sources=upload_sources,
        media=LegacyP115MediaCleanupReceiptGateway(),
        shared=EmbeddedP115SharedCleanupGateway(),
    ).execute(payload)


__all__ = ["execute_p115_deep_delete_operation"]
