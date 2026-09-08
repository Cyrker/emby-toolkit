from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from etk.domains.p115.deep_delete import P115DeepDeleteExecutionService
from etk.processes.native_operations import NativeOperationProgress


class NativeP115DeepDeleteOperation:
    def __init__(self, service: P115DeepDeleteExecutionService) -> None:
        self._service = service

    def execute(
        self,
        parameters: Mapping[str, Any],
        progress: NativeOperationProgress,
    ) -> Mapping[str, Any]:
        snapshot = parameters.get("snapshot")
        token = str(snapshot.get("delete_token") or "") if isinstance(snapshot, Mapping) else ""
        if not token:
            raise ValueError("native P115 deep-delete operation has no delete token")
        progress.checkpoint()
        label = f"p115-deep-delete:{token}"
        progress.record_started_effect(label)
        result = self._service.execute(parameters)
        progress.record_committed_effect(label)
        progress.update(100, "115 深度删除已完成")
        return result


__all__ = ["NativeP115DeepDeleteOperation"]
