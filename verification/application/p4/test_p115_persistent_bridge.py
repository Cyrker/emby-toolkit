from __future__ import annotations

from contextlib import contextmanager, nullcontext
from types import SimpleNamespace
from uuid import UUID

import pytest
from flask import Flask

from etk.platform.jobs import JobStatus
from handler import p115_service
from routes import p115 as p115_routes

JOB_ID = UUID("00000000-0000-0000-0000-000000000177")


class FakeHandle:
    def __init__(self, projection) -> None:
        self._projection = projection
        self.cancelled = False
        self.job_id = JOB_ID

    def projection(self):
        return self._projection

    def kill(self) -> None:
        self.cancelled = True


def test_quick_deploy_terminal_success_reloads_the_api_runtime(monkeypatch) -> None:
    runtime = SimpleNamespace(calls=[])
    runtime.save_config_and_reload = lambda values: runtime.calls.append(values)
    monkeypatch.setattr("etk.bootstrap.runtime_supervisor.get_runtime_supervisor", lambda: runtime)
    projection = SimpleNamespace(
        job=SimpleNamespace(
            status=JobStatus.SUCCEEDED,
            progress=100,
            progress_message="done",
            result={"config": {"monitor_enabled": True}, "tree": {}},
            error_summary=None,
        )
    )

    events = list(p115_routes._quick_deploy_persistent_events(FakeHandle(projection)))

    assert runtime.calls == [{"monitor_enabled": True}]
    assert events[-1]["success"] is True
    assert events[-1]["data"]["tree"] == {}


def test_quick_deploy_cancels_when_dispatch_never_starts(monkeypatch) -> None:
    monkeypatch.setattr(p115_routes, "_QUICK_DEPLOY_START_TIMEOUT_SECONDS", 0)
    projection = SimpleNamespace(
        job=SimpleNamespace(
            status=JobStatus.QUEUED,
            progress=0,
            progress_message="",
            result=None,
            error_summary=None,
        )
    )
    handle = FakeHandle(projection)

    events = list(p115_routes._quick_deploy_persistent_events(handle))

    assert handle.cancelled
    assert events == [
        {
            "type": "done",
            "success": False,
            "message": "一键部署任务未在限定时间内开始，请检查 Coordinator 和 Worker 状态",
        }
    ]


def test_quick_deploy_uses_the_client_intent_and_desired_state_hash(monkeypatch) -> None:
    captured = {}

    def submit(function, **options):
        captured.update({"function": function, **options})
        return FakeHandle(None)

    monkeypatch.setattr(p115_routes, "submit_persistent_background", submit)

    p115_routes._submit_quick_deploy_persistent("intent-1", "desired-hash")

    assert captured["function"] is p115_routes._quick_deploy_persistent_operation
    assert captured["args"] == ("desired-hash",)
    assert captured["max_attempts"] == 3
    assert captured["timeout_seconds"] == p115_routes._QUICK_DEPLOY_TIMEOUT_SECONDS
    assert captured["idempotency_key"] == "intent-1"
    assert captured["idempotency_scope"] == "quick-deploy"


def test_quick_deploy_desired_hash_ignores_credential_rotation(monkeypatch) -> None:
    config = {
        p115_routes.constants.CONFIG_OPTION_EMBY_SERVER_URL: "http://emby.test",
        p115_routes.constants.CONFIG_OPTION_LOCAL_STRM_ROOT: "/fixture/media",
        p115_routes.constants.CONFIG_OPTION_ETK_SERVER_URL: "http://etk.test",
    }
    credentials = [
        {
            "account_id": "115001",
            "openapi_account_id": "115001",
            "cookie_account_id": "115001",
            "access_token": "first",
            "refresh_token": "refresh-1",
            "cookie": "UID=115001_a",
        }
    ]
    monkeypatch.setattr(p115_routes, "get_config", lambda: config)
    monkeypatch.setattr(p115_routes.settings_db, "get_setting", lambda _key: credentials[0])

    before = p115_routes._quick_deploy_desired_state_hash()
    credentials[0] = {
        "account_id": "115001",
        "openapi_account_id": "115001",
        "cookie_account_id": "115001",
        "access_token": "second",
        "refresh_token": "refresh-2",
        "cookie": "UID=115001_b",
    }

    assert p115_routes._quick_deploy_desired_state_hash() == before


def test_quick_deploy_desired_hash_changes_with_the_deployment_target(monkeypatch) -> None:
    config = {
        p115_routes.constants.CONFIG_OPTION_EMBY_SERVER_URL: "http://emby.test",
        p115_routes.constants.CONFIG_OPTION_LOCAL_STRM_ROOT: "/fixture/media",
        p115_routes.constants.CONFIG_OPTION_ETK_SERVER_URL: "http://etk.test",
    }
    monkeypatch.setattr(p115_routes, "get_config", lambda: config)
    monkeypatch.setattr(
        p115_routes.settings_db,
        "get_setting",
        lambda _key: {"account_id": "115001"},
    )

    before = p115_routes._quick_deploy_desired_state_hash()
    config[p115_routes.constants.CONFIG_OPTION_LOCAL_STRM_ROOT] = "/fixture/changed"

    assert p115_routes._quick_deploy_desired_state_hash() != before


def test_quick_deploy_desired_hash_changes_when_the_115_account_changes(monkeypatch) -> None:
    config = {
        p115_routes.constants.CONFIG_OPTION_EMBY_SERVER_URL: "http://emby.test",
        p115_routes.constants.CONFIG_OPTION_LOCAL_STRM_ROOT: "/fixture/media",
        p115_routes.constants.CONFIG_OPTION_ETK_SERVER_URL: "http://etk.test",
    }
    authorization = [{"account_id": "115001"}]
    monkeypatch.setattr(p115_routes, "get_config", lambda: config)
    monkeypatch.setattr(p115_routes.settings_db, "get_setting", lambda _key: authorization[0])

    before = p115_routes._quick_deploy_desired_state_hash()
    authorization[0] = {"account_id": "115002"}

    assert p115_routes._quick_deploy_desired_state_hash() != before


def test_quick_deploy_requires_a_stable_115_account_identity(monkeypatch) -> None:
    monkeypatch.setattr(p115_routes, "get_config", lambda: {})
    monkeypatch.setattr(p115_routes.settings_db, "get_setting", lambda _key: {})

    with pytest.raises(ValueError, match="稳定的 115 账号身份"):
        p115_routes._quick_deploy_desired_state_hash()


def test_p115_auth_storage_keeps_stable_identity_across_rotation_and_changes_accounts(monkeypatch) -> None:
    stored: dict[str, object] = {}
    monkeypatch.setattr(p115_service.settings_db, "get_setting", lambda _key: dict(stored))
    monkeypatch.setattr(p115_service.settings_db, "p115_auth_write_lock", nullcontext)
    monkeypatch.setattr(
        p115_service.settings_db,
        "save_setting",
        lambda _key, value: stored.update(dict(value)),
    )

    p115_service.save_115_tokens("access-1", "refresh-1", account_id="115001")
    p115_service.save_115_tokens(
        "access-2",
        "refresh-2",
        account_id=p115_service.get_115_account_id(),
    )

    assert stored["account_id"] == "115001"
    assert stored["openapi_account_id"] == "115001"
    p115_service.save_115_tokens(
        None,
        None,
        "UID=115002_fixture; CID=fixture",
        "web",
        credential_type="cookie",
    )
    assert stored["account_id"] == "115002"
    assert stored["cookie_account_id"] == "115002"
    assert stored["access_token"] is None
    assert stored["refresh_token"] is None
    assert stored["openapi_account_id"] is None


def test_p115_auth_storage_keeps_same_account_backends_and_replaces_the_opposite_account(monkeypatch) -> None:
    stored: dict[str, object] = {}
    monkeypatch.setattr(p115_service.settings_db, "get_setting", lambda _key: dict(stored))
    monkeypatch.setattr(p115_service.settings_db, "p115_auth_write_lock", nullcontext)
    monkeypatch.setattr(
        p115_service.settings_db,
        "save_setting",
        lambda _key, value: stored.clear() or stored.update(dict(value)),
    )

    p115_service.save_115_tokens(
        None,
        None,
        "UID=115001_fixture; CID=fixture",
        "web",
        credential_type="cookie",
    )
    p115_service.save_115_tokens(
        "access-1",
        "refresh-1",
        account_id="115001",
        credential_type="openapi",
    )
    assert stored["cookie"] == "UID=115001_fixture; CID=fixture"
    assert stored["cookie_account_id"] == "115001"
    assert stored["openapi_account_id"] == "115001"
    assert p115_service.get_115_account_id() == "115001"

    p115_service.save_115_tokens(
        "access-2",
        "refresh-2",
        account_id="115002",
        credential_type="openapi",
    )
    assert stored["account_id"] == "115002"
    assert stored["openapi_account_id"] == "115002"
    assert stored["cookie"] is None
    assert stored["cookie_account_id"] is None


def test_p115_account_identity_fails_closed_for_an_unbound_mixed_bundle(monkeypatch) -> None:
    monkeypatch.setattr(
        p115_service.settings_db,
        "get_setting",
        lambda _key: {
            "account_id": "115002",
            "access_token": "account-a-token",
            "refresh_token": "account-a-refresh",
            "cookie": "UID=115002_fixture; CID=fixture",
        },
    )

    assert p115_service.get_115_account_id() is None


def test_unidentified_cookie_is_rejected_without_erasing_the_openapi_account(monkeypatch) -> None:
    stored = {
        "account_id": "115001",
        "openapi_account_id": "115001",
        "cookie_account_id": None,
        "access_token": "account-a-token",
        "refresh_token": "account-a-refresh",
        "cookie": None,
        "app_type": "web",
    }
    monkeypatch.setattr(p115_service.settings_db, "get_setting", lambda _key: dict(stored))
    monkeypatch.setattr(p115_service.settings_db, "p115_auth_write_lock", nullcontext)
    monkeypatch.setattr(
        p115_service.settings_db,
        "save_setting",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("invalid Cookie must not persist")),
    )

    with pytest.raises(ValueError, match="Cookie.*稳定账号身份"):
        p115_service.save_115_tokens(
            None,
            None,
            "CID=fixture-without-uid",
            "web",
            credential_type="cookie",
        )

    assert stored["access_token"] == "account-a-token"


def test_unidentified_openapi_rotation_cannot_inherit_the_previous_account(monkeypatch) -> None:
    stored = {
        "account_id": "115001",
        "openapi_account_id": "115001",
        "cookie_account_id": None,
        "access_token": "account-a-token",
        "refresh_token": "account-a-refresh",
        "cookie": None,
        "app_type": "web",
    }
    monkeypatch.setattr(p115_service.settings_db, "get_setting", lambda _key: dict(stored))
    monkeypatch.setattr(p115_service.settings_db, "p115_auth_write_lock", nullcontext)
    monkeypatch.setattr(
        p115_service.settings_db,
        "save_setting",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unidentified Token must not persist")),
    )

    with pytest.raises(ValueError, match="OpenAPI Token.*稳定账号身份"):
        p115_service.save_115_tokens(
            "account-b-token",
            "account-b-refresh",
            credential_type="openapi",
        )

    assert stored["access_token"] == "account-a-token"
    assert stored["openapi_account_id"] == "115001"


def test_qrcode_authorization_fails_closed_when_account_identity_is_missing(monkeypatch) -> None:
    monkeypatch.setitem(p115_routes._qrcode_data, "access_token", "fixture-token")
    monkeypatch.setitem(p115_routes._qrcode_data, "refresh_token", "fixture-refresh")
    monkeypatch.setattr(
        p115_routes,
        "_check_qrcode_status",
        lambda: {"status": "success", "user_info": {}},
    )
    monkeypatch.setattr(p115_service.settings_db, "get_setting", lambda _key: {})
    monkeypatch.setattr(p115_service.settings_db, "p115_auth_write_lock", nullcontext)
    monkeypatch.setattr(
        p115_service.settings_db,
        "save_setting",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unidentified Token must not persist")),
    )
    app = Flask(__name__)

    with app.test_request_context():
        response, status = p115_routes.check_qrcode_status.__wrapped__()

    assert status == 500
    assert response.get_json()["success"] is False
    assert response.get_json()["status"] == "error"


def test_status_response_cannot_rebind_a_replaced_openapi_credential(monkeypatch) -> None:
    stored = {
        "account_id": "115001",
        "openapi_account_id": "115001",
        "cookie_account_id": None,
        "access_token": "account-a-token",
        "refresh_token": "account-a-refresh",
        "cookie": None,
        "app_type": "web",
    }

    class OpenAPI:
        access_token = "account-a-token"

        def get_user_info(self):
            stored.update(
                {
                    "account_id": "115002",
                    "openapi_account_id": "115002",
                    "access_token": "account-b-token",
                    "refresh_token": "account-b-refresh",
                }
            )
            return {"state": True, "data": {"user_id": "115001"}}

    monkeypatch.setattr(
        p115_service,
        "get_115_tokens",
        lambda: ("account-a-token", "account-a-refresh", None, "web"),
    )
    monkeypatch.setattr(
        p115_service.P115Service,
        "get_openapi_client",
        classmethod(lambda _cls: OpenAPI()),
    )
    monkeypatch.setattr(p115_service.settings_db, "get_setting", lambda _key: dict(stored))
    monkeypatch.setattr(p115_service.settings_db, "p115_auth_write_lock", nullcontext)
    monkeypatch.setattr(
        p115_service.settings_db,
        "save_setting",
        lambda _key, value: stored.clear() or stored.update(dict(value)),
    )
    app = Flask(__name__)

    with app.test_request_context():
        response = p115_routes.get_115_status.__wrapped__()

    assert response.get_json()["data"]["valid"] is True
    assert stored["account_id"] == "115002"
    assert stored["openapi_account_id"] == "115002"
    assert stored["access_token"] == "account-b-token"


def test_quick_deploy_holds_the_auth_lock_and_binds_the_client_account(monkeypatch) -> None:
    events = []

    @contextmanager
    def auth_lock():
        events.append("lock-enter")
        try:
            yield
        finally:
            events.append("lock-exit")

    monkeypatch.setattr(p115_routes.settings_db, "p115_auth_write_lock", auth_lock)
    monkeypatch.setattr(p115_routes, "_quick_deploy_desired_state_hash", lambda: "desired-hash")
    monkeypatch.setattr(p115_routes, "get_115_account_id", lambda: "115001")
    monkeypatch.setattr(
        p115_routes,
        "_quick_deploy_payload",
        lambda _progress, **kwargs: events.append(("payload", kwargs)) or {"ok": True},
    )

    assert p115_routes._quick_deploy_persistent_operation("desired-hash") == {"ok": True}
    assert events == [
        "lock-enter",
        ("payload", {"expected_account_id": "115001"}),
        "lock-exit",
    ]


def test_mkdir_rejects_a_cross_account_cached_cid_before_creating(monkeypatch) -> None:
    saved = []

    class OpenAPI:
        access_token = "fixture-token"

        def fs_search(self, _payload):
            return {"state": True, "data": []}

        def fs_files(self, _payload):
            return {"state": True, "data": []}

        def fs_mkdir(self, name, parent_cid):
            assert (name, parent_cid) == ("ETK媒体库", "0")
            return {"state": True, "cid": "account-b-cid", "data": {}}

    monkeypatch.setattr(
        p115_service,
        "get_115_tokens",
        lambda: ("fixture-token", "fixture-refresh", None, "web"),
    )
    monkeypatch.setattr(p115_service.settings_db, "p115_auth_write_lock", nullcontext)
    monkeypatch.setattr(p115_service, "get_115_account_id", lambda: "115002")
    monkeypatch.setattr(
        p115_service.P115Service,
        "get_openapi_client",
        classmethod(lambda _cls: OpenAPI()),
    )
    monkeypatch.setattr(
        p115_service.P115Service,
        "get_cookie_client",
        classmethod(lambda _cls: None),
    )
    monkeypatch.setattr(p115_service.P115CacheManager, "get_cid", lambda *_args: "account-a-cid")
    monkeypatch.setattr(
        p115_service.P115CacheManager,
        "save_cid",
        lambda cid, parent, name: saved.append((cid, parent, name)),
    )
    monkeypatch.setattr(p115_service.time, "sleep", lambda _seconds: None)

    client = p115_service.P115Service.get_client(expected_account_id="115002")
    assert client is not None
    result = client.fs_mkdir("ETK媒体库", "0")

    assert result["cid"] == "account-b-cid"
    assert result.get("_from_cache") is None
    assert saved == [("account-b-cid", "0", "ETK媒体库")]


def test_quick_deploy_greenfield_host_fails_before_starting_a_stream_without_gateway(monkeypatch) -> None:
    monkeypatch.setattr(p115_routes, "persistent_background_gateway_installed", lambda: False)
    monkeypatch.setattr(
        p115_routes,
        "_quick_deploy_payload",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("payload must not run")),
    )
    monkeypatch.setattr(
        p115_routes,
        "spawn",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("spawn must not run")),
    )
    app = Flask(__name__)
    app.config["ETK_COMPATIBILITY_HOST"] = True

    for query_string in ({}, {"stream": "1"}):
        with app.test_request_context(query_string=query_string):
            response, status = p115_routes.quick_deploy_115.__wrapped__()

        assert status == 503
        assert response.is_json
        assert response.get_json()["success"] is False


@pytest.mark.parametrize("background", [False, "false", 0])
def test_washing_recalculate_never_executes_in_the_api(monkeypatch, background) -> None:
    from tasks import p115 as p115_tasks

    captured = []
    monkeypatch.setattr(
        p115_routes.settings_db,
        "get_washing_priority_recalculate_state",
        lambda: {"media_types": {"movie": True}, "revision": 7},
    )
    monkeypatch.setattr(
        p115_tasks,
        "task_recalculate_library_washing_priorities",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("API must not run the task")),
    )
    monkeypatch.setattr(
        p115_tasks,
        "submit_washing_priority_recalculate_task",
        lambda **kwargs: captured.append(kwargs) or True,
    )
    app = Flask(__name__)

    with app.test_request_context(json={"background": background, "item_type": "movie", "limit": 5}):
        response = p115_routes.trigger_washing_priority_recalculate.__wrapped__()

    assert response.get_json()["success"] is True
    assert captured == [
        {
            "item_type": "movie",
            "limit": 5,
            "affected_scope": {"movie": True},
            "scope_revision": 7,
        }
    ]


def test_quick_deploy_stream_submission_failure_is_reported_before_ndjson_headers(monkeypatch) -> None:
    monkeypatch.setattr(p115_routes, "persistent_background_gateway_installed", lambda: True)
    monkeypatch.setattr(p115_routes, "_quick_deploy_idempotency_key", lambda: "intent-1")
    monkeypatch.setattr(p115_routes, "_quick_deploy_desired_state_hash", lambda: "desired-hash")
    monkeypatch.setattr(
        p115_routes,
        "_submit_quick_deploy_persistent",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("coordinator unavailable")),
    )
    app = Flask(__name__)
    app.config["ETK_COMPATIBILITY_HOST"] = True

    with app.test_request_context(query_string={"stream": "1"}):
        response, status = p115_routes.quick_deploy_115.__wrapped__()

    assert status == 503
    assert response.is_json
    assert "coordinator unavailable" in response.get_json()["message"]


def _stub_quick_deploy_dependencies(monkeypatch) -> list[dict[str, object]]:
    config = {
        p115_routes.constants.CONFIG_OPTION_EMBY_SERVER_URL: "http://emby.test",
        p115_routes.constants.CONFIG_OPTION_EMBY_API_KEY: "fixture-key",
        p115_routes.constants.CONFIG_OPTION_LOCAL_STRM_ROOT: "/fixture/media",
        p115_routes.constants.CONFIG_OPTION_ETK_SERVER_URL: "http://etk.test",
        p115_routes.constants.CONFIG_OPTION_MONITOR_PATHS: ["/api-added"],
        p115_routes.constants.CONFIG_OPTION_115_API_PRIORITY: "proapi",
        p115_routes.constants.CONFIG_OPTION_115_MIN_VIDEO_SIZE: 42,
        p115_routes.constants.CONFIG_OPTION_115_EXTENSIONS: ["fixture"],
    }
    monkeypatch.setattr(p115_routes.P115Service, "get_client", lambda **_kwargs: object())
    monkeypatch.setattr(p115_routes, "get_config", lambda: config)
    monkeypatch.setattr(p115_routes.config_manager, "is_emby_service_authorized", lambda: True)
    monkeypatch.setattr(p115_routes.emby, "test_connection", lambda *_args: {"success": True})
    monkeypatch.setattr(p115_routes, "is_etk_plugin_installed", lambda *_args: True)
    monkeypatch.setattr(
        p115_routes,
        "_p115_ensure_folder",
        lambda _client, _parent, name: {"cid": f"cid-{name}", "name": name, "created": False},
    )
    monkeypatch.setattr(p115_routes, "_p115_create_local_mirror", lambda *_args: [])
    monkeypatch.setattr(p115_routes, "_p115_deploy_sorting_rules", lambda *_args: [])
    monkeypatch.setattr(p115_routes, "_p115_deploy_washing_groups", lambda *_args: [])
    monkeypatch.setattr(p115_routes.settings_db, "save_setting", lambda *_args: None)
    monkeypatch.setattr(p115_routes.settings_db, "save_washing_priority_config", lambda *_args: None)
    saved: list[dict[str, object]] = []

    def save(values, monitor_path):
        committed = dict(values)
        committed[p115_routes.constants.CONFIG_OPTION_MONITOR_PATHS] = ["/api-added", monitor_path]
        saved.append(committed)
        return committed

    monkeypatch.setattr(p115_routes.config_manager, "save_config_with_monitor_path", save)
    return saved


def test_quick_deploy_does_not_publish_dynamic_config_before_all_libraries_converge(monkeypatch) -> None:
    saved = _stub_quick_deploy_dependencies(monkeypatch)
    monkeypatch.setattr(
        p115_routes,
        "_p115_deploy_emby_libraries",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("fixture Emby failure")),
    )

    with pytest.raises(RuntimeError, match="fixture Emby failure"):
        p115_routes._quick_deploy_payload()

    assert saved == []


def test_quick_deploy_publishes_one_complete_dynamic_config_snapshot(monkeypatch) -> None:
    saved = _stub_quick_deploy_dependencies(monkeypatch)
    libraries = [{"id": f"library-{index}", "name": f"Library {index}"} for index in range(1, 10)]
    monkeypatch.setattr(p115_routes, "_p115_deploy_emby_libraries", lambda *_args: libraries)

    result = p115_routes._quick_deploy_payload()

    assert len(saved) == 1
    assert saved[0][p115_routes.constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS] == [
        f"library-{index}" for index in range(1, 10)
    ]
    assert saved[0][p115_routes.constants.CONFIG_OPTION_MONITOR_PATHS] == ["/api-added", "/fixture/media"]
    assert p115_routes.constants.CONFIG_OPTION_115_API_PRIORITY not in saved[0]
    assert p115_routes.constants.CONFIG_OPTION_115_MIN_VIDEO_SIZE not in saved[0]
    assert p115_routes.constants.CONFIG_OPTION_115_EXTENSIONS not in saved[0]
    assert result["config"] == saved[0]


def test_deep_delete_uses_a_retryable_idempotent_persistent_job(monkeypatch) -> None:
    snapshot = {
        "created_at": p115_routes.time.time(),
        "item_id": "123",
        "item_name": "Fixture",
        "item_type": "Movie",
        "series_id": "",
        "pickcodes": ["pc-fixture"],
        "p115_file_snapshots": [],
    }
    captured = {}
    monkeypatch.setattr(
        p115_routes,
        "_claim_deep_delete_snapshot",
        lambda token: snapshot if token == "token" else None,
    )

    def submit(function, **options):
        captured.update({"function": function, **options})
        return SimpleNamespace(job_id=JOB_ID)

    monkeypatch.setattr(p115_routes, "submit_persistent_background", submit)
    app = Flask(__name__)
    with app.test_request_context(json={"token": "token"}):
        response, status = p115_routes._commit_deep_delete_response()

    assert status == 202
    assert response.get_json()["job_id"] == str(JOB_ID)
    assert captured["function"] is p115_routes._process_deep_delete_snapshot
    assert captured["max_attempts"] == 5
    assert captured["timeout_seconds"] == 900
    assert captured["idempotency_key"] == "token"
    assert captured["idempotency_scope"] == "deep-delete"


def test_deep_delete_retry_recovers_the_job_after_the_commit_response_is_lost(monkeypatch) -> None:
    snapshot = {
        "item_id": "123",
        "item_name": "Fixture",
        "item_type": "Movie",
        "pickcodes": ["pc-fixture"],
    }
    projection = SimpleNamespace(
        job=SimpleNamespace(
            request_payload={"positional_parameters": [snapshot]},
        )
    )
    handle = FakeHandle(projection)
    monkeypatch.setattr(p115_routes, "_claim_deep_delete_snapshot", lambda _token: None)
    monkeypatch.setattr(p115_routes, "persistent_background_gateway_installed", lambda: True)

    def lookup(**options):
        if options == {
            "idempotency_scope": "deep-delete",
            "idempotency_key": "token",
        }:
            return handle
        return None

    monkeypatch.setattr(
        p115_routes,
        "lookup_persistent_background",
        lookup,
    )

    app = Flask(__name__)
    with app.test_request_context(json={"token": "token"}):
        response, status = p115_routes._commit_deep_delete_response()

    assert status == 202
    assert response.get_json() == {
        "ok": True,
        "accepted": True,
        "job_id": str(JOB_ID),
        "pickcode_count": 1,
        "local_only": False,
    }
