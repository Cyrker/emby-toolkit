from __future__ import annotations

from typing import Any

import pytest

import constants
import task_manager
from tasks import p115, subscriptions


def test_scan_and_organize_aggregates_root_failures_and_raises(monkeypatch) -> None:
    statuses: list[tuple[int, str]] = []
    config = {
        constants.CONFIG_OPTION_115_SAVE_PATH_CID: "1",
        constants.CONFIG_OPTION_115_SAVE_PATH_NAME: "待整理",
        constants.CONFIG_OPTION_115_UNRECOGNIZED_CID: "2",
        constants.CONFIG_OPTION_115_UNRECOGNIZED_NAME: "未识别",
        constants.CONFIG_OPTION_115_MAX_WORKERS: 1,
        constants.CONFIG_OPTION_115_EXTENSIONS: [],
    }

    class Client:
        @staticmethod
        def fs_files(request: dict[str, Any]) -> dict[str, Any]:
            if str(request.get("cid")) == "1":
                return {
                    "state": True,
                    "data": [{"fid": "10", "fn": "broken.txt", "fc": "1"}],
                }
            return {"state": True, "data": []}

        @staticmethod
        def fs_move(_fids, _target_cid) -> dict[str, Any]:
            return {"state": False, "message": "move failed"}

    monkeypatch.setattr(p115, "get_config", lambda: config)
    monkeypatch.setattr(
        p115.P115Service,
        "get_client",
        classmethod(lambda _cls, expected_account_id=None: Client()),
    )
    monkeypatch.setattr(p115.P115CacheManager, "get_transfer_context", lambda *_args: None)
    monkeypatch.setattr(
        task_manager,
        "update_status_from_thread",
        lambda progress, message: statuses.append((progress, message)),
    )

    with pytest.raises(RuntimeError, match="1 个根目录失败"):
        p115.task_scan_and_organize_115()

    assert statuses[-1][0] == -1
    assert "broken.txt" in statuses[-1][1]


def test_auto_subscribe_reports_failure_and_raises(monkeypatch) -> None:
    statuses: list[tuple[int, str]] = []
    monkeypatch.setattr(subscriptions.logger, "trace", lambda *_args, **_kwargs: None, raising=False)
    monkeypatch.setattr(subscriptions.settings_db, "get_setting", lambda _key: {})
    monkeypatch.setattr(
        subscriptions.request_db,
        "get_movies_to_revive",
        lambda: (_ for _ in ()).throw(RuntimeError("subscription exploded")),
    )
    monkeypatch.setattr(
        subscriptions.task_manager,
        "update_status_from_thread",
        lambda progress, message: statuses.append((progress, message)),
    )

    with pytest.raises(RuntimeError, match="subscription exploded"):
        subscriptions.task_auto_subscribe(object())

    assert statuses[-1] == (-1, "错误: subscription exploded")
