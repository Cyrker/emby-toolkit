from __future__ import annotations

import hashlib
import json
import logging
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from etk.compatibility import tg_resource_effects
from etk.compatibility.tg_control_bridge import (
    normalize_telegram_resource_action,
    telegram_resource_action_key,
)
from etk.platform.jobs import normalize_json_object

logger = logging.getLogger(__name__)
_EFFECT_SETTLE_SECONDS = 10


class TelegramResourceEffectPendingError(RuntimeError):
    pass


class HDHiveUnlockPendingError(TelegramResourceEffectPendingError):
    pass


def task_execute_telegram_resource_action(processor=None, *, action=None) -> dict[str, Any]:
    """Execute one frozen Telegram action only after a persistent Worker claims it."""

    del processor
    normalized = normalize_telegram_resource_action(action or {})
    action_name = normalized["action"]
    payload = normalized["payload"]
    handlers = {
        "shared_pool_transfer": _run_shared_pool_transfer,
        "hdhive_transfer": _run_hdhive_transfer,
        "subscription": _run_subscription,
        "p115_share_import": _run_p115_share_import,
        "p115_offline_add": _run_p115_offline_add,
    }
    result = handlers[action_name](payload, normalized)
    return normalize_json_object(
        {
            "ok": True,
            "action": action_name,
            "chat_id": normalized["chat_id"],
            "update_id": normalized["update_id"],
            "source_identity": normalized["source_identity"],
            "result": result,
        },
        field="telegram_resource_action_result",
    )


def _run_shared_pool_transfer(payload: Mapping[str, Any], action: Mapping[str, Any]) -> dict[str, Any]:
    source = payload.get("source")
    if not isinstance(source, Mapping):
        raise TypeError("Telegram shared-pool action requires a source object")
    mode = str(payload.get("mode") or "rapid").strip().casefold()
    from tasks.shared_resource_tasks import run_shared_center_transfer_persistent

    result = run_shared_center_transfer_persistent(
        "cloud_download",
        dict(source),
        mode=mode,
        event_id=f"telegram:{action['update_id']}",
    )
    if not isinstance(result, Mapping) or result.get("ok") is not True:
        message = result.get("message") if isinstance(result, Mapping) else None
        raise RuntimeError(str(message or "Telegram shared-pool transfer reported failure"))
    raw_transfer = result.get("result")
    transfer: Mapping[str, Any] = raw_transfer if isinstance(raw_transfer, Mapping) else {}
    return {
        "message": str(transfer.get("message") or "共享池资源转存完成"),
        "success_count": _safe_int(transfer.get("success_count")),
        "total": _safe_int(transfer.get("total")),
        "transfer_mode": str(transfer.get("transfer_mode") or mode),
    }


def _run_hdhive_transfer(payload: Mapping[str, Any], _action: Mapping[str, Any]) -> dict[str, Any]:
    slug = str(payload.get("slug") or "").strip()
    if not slug:
        raise ValueError("Telegram HDHive action requires slug")
    from tasks.hdhive import task_download_from_hdhive

    result = task_download_from_hdhive(
        api_key=None,
        slug=slug,
        tmdb_id=payload.get("tmdb_id"),
        media_type=payload.get("media_type"),
        title=payload.get("title"),
        operation_id=str(payload.get("delivery_key") or telegram_resource_action_key(_action)),
    )
    _raise_for_reported_failure(result, fallback="HDHive 转存任务报告失败")
    return {"message": "影巢资源转存完成", "slug_hash": hashlib.sha256(slug.encode("utf-8")).hexdigest()}


def _run_subscription(payload: Mapping[str, Any], action: Mapping[str, Any]) -> dict[str, Any]:
    tmdb_id = str(payload.get("tmdb_id") or "").strip()
    media_type = str(payload.get("media_type") or "").strip().casefold()
    if not tmdb_id or media_type not in {"movie", "tv"}:
        raise ValueError("Telegram subscription action requires tmdb_id and movie/tv media_type")
    api_key = _tmdb_api_key()
    if not api_key:
        raise RuntimeError("TMDb API Key 未配置，无法执行 Telegram 搜索订阅")

    tmdb_items: list[dict[str, Any]] = []
    if media_type == "movie":
        tmdb_items.append({"tmdb_id": tmdb_id, "media_type": "Movie", "season": None})
    else:
        from handler.tmdb import get_tv_details

        details = get_tv_details(int(tmdb_id), api_key)
        for season in (details or {}).get("seasons") or []:
            season_number = _safe_int(season.get("season_number"))
            if season_number > 0:
                tmdb_items.append({"tmdb_id": tmdb_id, "media_type": "Series", "season": season_number})
        if not tmdb_items:
            tmdb_items.append({"tmdb_id": tmdb_id, "media_type": "Series", "season": 1})

    from tasks.helpers import process_subscription_items_and_update_db

    processed = process_subscription_items_and_update_db(
        tmdb_items=tmdb_items,
        tmdb_to_emby_item_map={},
        subscription_source={
            "type": "telegram_search",
            "user_id": action["chat_id"],
            "name": _telegram_emby_username(action["chat_id"]),
        },
        tmdb_api_key=api_key,
    )
    if processed is None:
        raise RuntimeError("Telegram subscription operation returned no result")
    return {
        "message": "订阅请求已处理",
        "processed_ids": sorted(str(value) for value in processed),
        "tmdb_id": tmdb_id,
        "media_type": media_type,
    }


def _run_p115_share_import(payload: Mapping[str, Any], action: Mapping[str, Any]) -> dict[str, Any]:
    return _execute_p115_secret_effect(
        "p115_share_import",
        payload,
        action,
        organize_reason="tg_share_import",
    )


def _run_p115_offline_add(payload: Mapping[str, Any], action: Mapping[str, Any]) -> dict[str, Any]:
    return _execute_p115_secret_effect(
        "p115_offline_add",
        payload,
        action,
        organize_reason="tg_offline_download",
    )


def execute_channel_p115_effect(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Execute one secret-backed channel transfer with the same durable effect contract."""

    resource_kind = str(payload.get("resource_kind") or "").strip()
    action_name = {
        "p115_share": "p115_share_import",
        "p115_offline": "p115_offline_add",
    }.get(resource_kind)
    if action_name is None:
        raise ValueError(f"unsupported Telegram channel P115 resource kind: {resource_kind!r}")
    return _execute_p115_secret_effect(
        action_name,
        payload,
        None,
        organize_reason="tg_channel_share" if action_name == "p115_share_import" else "tg_channel_offline",
    )


def execute_hdhive_p115_effect(
    *,
    unlock_secret_ref: str,
    unlock_owner_key: str,
    operation_id: str,
    semantic_source_hash: str,
) -> dict[str, Any]:
    secret = tg_resource_effects.consume_secret(unlock_secret_ref, unlock_owner_key)
    resource_kind = str(secret.payload.get("resource_kind") or "").strip()
    action_name = {
        "p115_share": "p115_share_import",
        "p115_offline": "p115_offline_add",
    }.get(resource_kind)
    if action_name is None:
        raise RuntimeError("HDHive unlock receipt has no supported P115 resource")
    payload = {
        "delivery_key": _digest_identity("hdhive-p115-delivery", operation_id),
        "secret_ref": unlock_secret_ref,
        "semantic_source_hash": semantic_source_hash,
    }
    return _execute_p115_secret_effect(
        action_name,
        payload,
        None,
        organize_reason="hdhive_download",
        secret_owner_key=unlock_owner_key,
        destroy_secret_on_complete=False,
    )


def acquire_hdhive_unlock_receipt(slug: str, operation_id: str) -> dict[str, Any]:
    """Perform at most one debit-capable HDHive POST and persist its raw result out of the Job."""

    slug_value = str(slug or "").strip()
    if not slug_value:
        raise ValueError("HDHive unlock requires slug")
    operation_key = (
        str(operation_id or "").strip().casefold()
        if re.fullmatch(r"[0-9a-fA-F]{64}", str(operation_id or "").strip())
        else _digest_identity("hdhive-operation", str(operation_id or slug_value))
    )
    from handler.hdhive_client import HDHiveClient, HDHiveUnlockIndeterminateError

    client = HDHiveClient()
    authorization_checked = False
    if not str(client.instance_id or "").strip():
        authorization_checked = True
        if not client.ping():
            raise RuntimeError("HDHive relay is not authorized")
    instance_hash = hashlib.sha256(str(client.instance_id or "unregistered").encode("utf-8")).hexdigest()
    slug_hash = hashlib.sha256(slug_value.encode("utf-8")).hexdigest()
    effect_key = _digest_identity("hdhive-unlock", f"{instance_hash}:{slug_hash}")
    existing = tg_resource_effects.read_effect(effect_key)
    if existing is not None and existing.status == "rejected":
        raise RuntimeError("HDHive unlock was explicitly rejected; automatic POST replay is disabled")
    if existing is not None and existing.status == "succeeded":
        if existing.evidence.get("downstream_succeeded") is True:
            tg_resource_effects.destroy_secret(effect_key, effect_key)
            return {
                "completed": True,
                "effect_key": effect_key,
                "operation_key": operation_key,
                "semantic_source_hash": slug_hash,
            }
        secret = tg_resource_effects.consume_secret(effect_key, effect_key)
        return {
            "completed": False,
            "effect_key": effect_key,
            "operation_key": operation_key,
            "semantic_source_hash": slug_hash,
            "secret_ref": secret.reference,
            "resource_kind": str(secret.payload.get("resource_kind") or ""),
        }

    if existing is not None:
        # The secret is written with the deterministic effect key before the
        # success receipt. Recover that narrow crash window without another POST.
        try:
            secret = tg_resource_effects.consume_secret(effect_key, effect_key)
        except tg_resource_effects.TelegramResourceSecretExpiredError as exc:
            raise HDHiveUnlockPendingError(
                "HDHive unlock outcome is indeterminate; automatic POST replay is disabled"
            ) from exc
        recovered = tg_resource_effects.mark_effect_succeeded(
            effect_key,
            "hdhive_unlock",
            {"secret_ref": effect_key, "resource_kind": secret.payload.get("resource_kind"), "recovered": True},
        )
        return {
            "completed": False,
            "effect_key": effect_key,
            "operation_key": operation_key,
            "semantic_source_hash": slug_hash,
            "secret_ref": effect_key,
            "resource_kind": str(recovered.evidence.get("resource_kind") or ""),
        }

    if not authorization_checked and not client.ping():
        raise RuntimeError("HDHive relay is not authorized")
    effect, created = tg_resource_effects.prepare_effect(
        effect_key,
        "hdhive_unlock",
        {"slug_hash": slug_hash, "relay_instance_hash": instance_hash},
    )
    if not created:
        raise HDHiveUnlockPendingError("HDHive unlock outcome is pending reconciliation")
    try:
        unlock_data = client.unlock_resource(slug_value)
    except HDHiveUnlockIndeterminateError as exc:
        raise HDHiveUnlockPendingError(
            "HDHive unlock response is indeterminate; automatic POST replay is disabled"
        ) from exc
    if not unlock_data:
        tg_resource_effects.mark_effect_rejected(effect.key, "hdhive_unlock", {"explicit_rejection": True})
        raise RuntimeError("HDHive unlock was explicitly rejected")
    secret_payload = _normalize_hdhive_unlock_secret(unlock_data)
    secret_ref, _created = tg_resource_effects.create_secret(
        secret_payload,
        reference=effect_key,
        owner_key=effect_key,
    )
    tg_resource_effects.mark_effect_succeeded(
        effect_key,
        "hdhive_unlock",
        {"secret_ref": secret_ref, "resource_kind": secret_payload["resource_kind"]},
    )
    return {
        "completed": False,
        "effect_key": effect_key,
        "operation_key": operation_key,
        "semantic_source_hash": slug_hash,
        "secret_ref": secret_ref,
        "resource_kind": secret_payload["resource_kind"],
    }


def complete_hdhive_unlock_receipt(effect_key: str) -> None:
    tg_resource_effects.mark_effect_succeeded(
        effect_key,
        "hdhive_unlock",
        {"downstream_succeeded": True},
    )
    tg_resource_effects.destroy_secret(effect_key, effect_key)


def _normalize_hdhive_unlock_secret(unlock_data: Any) -> dict[str, Any]:
    if not isinstance(unlock_data, Mapping):
        raise RuntimeError("HDHive unlock returned an invalid payload")

    def as_text(value: Any) -> str:
        if isinstance(value, list):
            return "\n".join(str(item or "") for item in value)
        return str(value or "")

    combined = "\n".join((as_text(unlock_data.get("url")), as_text(unlock_data.get("full_url"))))
    urls: list[str] = []
    for value in re.findall(
        r"(magnet:\?xt=urn:btih:[a-zA-Z0-9]+.*?|ed2k://\|file\|.*?\|/)",
        combined,
        re.IGNORECASE,
    ):
        clean = str(value or "").strip()
        if clean and clean not in urls:
            urls.append(clean)
    if urls:
        return {"resource_kind": "p115_offline", "urls": urls}

    match = re.search(r"(?i)(?:115\.com|115cdn\.com|anxia\.com)/s/([a-z0-9]+)", combined)
    if not match:
        raise RuntimeError("HDHive unlock returned no supported P115 resource")
    receive_code = str(unlock_data.get("access_code") or "").strip()
    if not receive_code:
        password = re.search(r"(?i)(?:pwd|password|code)=([a-z0-9]+)", combined)
        receive_code = password.group(1) if password else ""
    return {
        "resource_kind": "p115_share",
        "share_code": match.group(1),
        "receive_code": receive_code,
    }


def _execute_p115_secret_effect(
    action_name: str,
    payload: Mapping[str, Any],
    action: Mapping[str, Any] | None,
    *,
    organize_reason: str,
    secret_owner_key: str | None = None,
    destroy_secret_on_complete: bool = True,
) -> dict[str, Any]:
    del organize_reason
    delivery_key = str(payload.get("delivery_key") or "").strip().casefold()
    if not delivery_key and action is not None:
        delivery_key = telegram_resource_action_key(action)
    semantic_source_hash = str(payload.get("semantic_source_hash") or "").strip().casefold()
    secret_ref = str(payload.get("secret_ref") or "").strip().casefold()
    if not re.fullmatch(r"[0-9a-f]{64}", delivery_key):
        raise ValueError("Telegram P115 action has no stable delivery_key")
    if not re.fullmatch(r"[0-9a-f]{64}", semantic_source_hash):
        raise ValueError("Telegram P115 action has no semantic_source_hash")
    if not re.fullmatch(r"[0-9a-f]{64}", secret_ref):
        raise ValueError("Telegram P115 action has no opaque secret_ref")

    binding, client, target_cid = _resolve_p115_effect_binding(
        delivery_key,
        action_name,
        semantic_source_hash,
    )
    effect_key = binding.effect_key
    existing = tg_resource_effects.read_effect(effect_key)
    if existing is not None and existing.status == "succeeded":
        if destroy_secret_on_complete:
            tg_resource_effects.destroy_secret(secret_ref, secret_owner_key or delivery_key)
        return {
            "message": "115 外部操作已完成对账",
            "organize_submitted": False,
            "organize_plan_required": True,
            "reconciled": True,
            "effect_key": effect_key,
        }
    if existing is not None and existing.status == "rejected":
        if destroy_secret_on_complete:
            tg_resource_effects.destroy_secret(secret_ref, secret_owner_key or delivery_key)
        raise RuntimeError("115 外部操作已被 provider 明确拒绝，禁止自动重放")

    secret = tg_resource_effects.consume_secret(secret_ref, secret_owner_key or delivery_key)
    secret_payload = secret.payload
    if action_name == "p115_share_import":
        share_code = str(secret_payload.get("share_code") or "").strip()
        receive_code = str(secret_payload.get("receive_code") or "").strip()
        if not share_code:
            raise ValueError("Telegram 115 share secret is incomplete")

        def share_evidence_factory():
            return _prepare_share_import_evidence(
                client,
                share_code,
                receive_code,
                target_cid,
                binding.account_id,
            )

        def share_reconcile(evidence):
            return _reconcile_share_import(client, target_cid, evidence)

        def share_write():
            return client.share_import(share_code, receive_code, target_cid)

        evidence_factory = share_evidence_factory
        reconcile = share_reconcile
        write = share_write
    else:
        urls = [
            str(value).strip()
            for value in secret_payload.get("urls", [])
            if str(value or "").strip().startswith(("magnet:?", "ed2k://"))
        ]
        if not urls:
            raise ValueError("Telegram offline secret is incomplete")

        def offline_evidence_factory():
            return {
                "source_hashes": sorted(hashlib.sha256(url.encode("utf-8")).hexdigest() for url in urls),
                "account_id": binding.account_id,
                "target_cid": target_cid,
            }

        def offline_reconcile(evidence):
            return _offline_reconcile_status(client, urls, target_cid, evidence)

        def offline_write():
            request: dict[str, Any] = {"wp_path_id": target_cid}
            request.update({f"url[{index}]": url for index, url in enumerate(urls)})
            return client.offline_add_urls(request)

        evidence_factory = offline_evidence_factory
        reconcile = offline_reconcile
        write = offline_write

    effect, created = _prepare_resource_effect(effect_key, action_name, evidence_factory)
    if not created:
        _require_reconcile_age(effect)
        reconcile_status = reconcile(effect.evidence)
        if reconcile_status == "confirmed":
            tg_resource_effects.mark_effect_succeeded(effect_key, action_name, {"reconciled": True})
            if destroy_secret_on_complete:
                tg_resource_effects.destroy_secret(secret_ref, secret_owner_key or delivery_key)
            return {
                "message": "115 外部操作已完成对账",
                "organize_submitted": False,
                "organize_plan_required": True,
                "reconciled": True,
                "effect_key": effect_key,
            }
        if reconcile_status != "absent" or action_name == "p115_share_import":
            raise TelegramResourceEffectPendingError("115 外部操作结果无法权威确认，禁止自动重放")

    try:
        result = write()
    except Exception:
        raise TelegramResourceEffectPendingError("115 外部操作调用结果不确定，保留 prepared 等待对账") from None
    outcome = _p115_write_outcome(result)
    if outcome == "unknown":
        raise TelegramResourceEffectPendingError("115 外部操作响应不确定，保留 prepared 等待对账")
    if outcome == "rejected":
        tg_resource_effects.mark_effect_rejected(effect_key, action_name, _safe_provider_evidence(result))
        if destroy_secret_on_complete:
            tg_resource_effects.destroy_secret(secret_ref, secret_owner_key or delivery_key)
        raise RuntimeError("115 外部操作被 provider 明确拒绝")

    success_evidence = _safe_provider_evidence(result)
    tg_resource_effects.mark_effect_succeeded(effect_key, action_name, success_evidence)
    if destroy_secret_on_complete:
        tg_resource_effects.destroy_secret(secret_ref, secret_owner_key or delivery_key)
    return {
        "message": "115 分享链接转存完成" if action_name == "p115_share_import" else "115 离线下载已提交",
        "organize_submitted": False,
        "organize_plan_required": True,
        "effect_key": effect_key,
        **({"receive_title": success_evidence["receive_title"]} if success_evidence.get("receive_title") else {}),
    }


def _resolve_p115_effect_binding(delivery_key: str, action_name: str, semantic_source_hash: str):
    existing = tg_resource_effects.read_effect_binding(delivery_key)
    if existing is not None:
        if existing.action != action_name or existing.semantic_source_hash != semantic_source_hash:
            raise RuntimeError("Telegram resource delivery changed semantic effect identity")
        client, target_cid = _p115_client_and_target(
            expected_account_id=existing.account_id,
            expected_target_cid=existing.target_cid,
        )
        return existing, client, target_cid

    client, target_cid = _p115_client_and_target()
    account_id = str(getattr(client, "account_id", "") or "").strip()
    if not account_id:
        raise RuntimeError("115 client has no stable account_id")
    binding, _created = tg_resource_effects.bind_effect_delivery(
        delivery_key,
        action_name,
        semantic_source_hash,
        account_id,
        target_cid,
    )
    return binding, client, target_cid


def _p115_client_and_target(
    *,
    expected_account_id: str | None = None,
    expected_target_cid: str | None = None,
) -> tuple[Any, str]:
    import constants
    from config_manager import APP_CONFIG
    from handler.p115_service import P115Service

    client = P115Service.get_client(expected_account_id=expected_account_id)
    if client is None:
        raise RuntimeError("115 客户端未初始化")
    target_cid = str(APP_CONFIG.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID, "0") or "0")
    if expected_target_cid is not None and target_cid != str(expected_target_cid):
        raise RuntimeError(f"115 目标目录在任务开始后发生变化：expected={expected_target_cid}, current={target_cid}")
    return client, target_cid


def _prepare_resource_effect(
    effect_key: str,
    action: str,
    evidence_factory,
):
    existing = tg_resource_effects.read_effect(effect_key)
    if existing is not None:
        return existing, False
    evidence = normalize_json_object(dict(evidence_factory()), field="Telegram resource effect evidence")
    return tg_resource_effects.prepare_effect(effect_key, action, evidence)


def _require_reconcile_age(effect) -> None:
    age_seconds = (datetime.now(UTC) - effect.prepared_at).total_seconds()
    if age_seconds < _EFFECT_SETTLE_SECONDS:
        raise TelegramResourceEffectPendingError("115 远端结果仍在稳定窗口内，等待持久任务重试")


def _offline_reconcile_status(
    client: Any,
    urls: list[str],
    target_cid: str,
    evidence: Mapping[str, Any],
) -> str:
    if str(evidence.get("target_cid") or "") != str(target_cid):
        return "unknown"
    reconcile = getattr(client, "offline_task_reconcile", None)
    if not callable(reconcile):
        return "unknown"
    statuses: list[str] = []
    for url in urls:
        try:
            result = reconcile(url, target_cid)
        except Exception:
            return "unknown"
        if not isinstance(result, Mapping):
            return "unknown"
        status = str(result.get("status") or "unknown").strip().casefold()
        statuses.append(status if status in {"confirmed", "absent", "unknown"} else "unknown")
    if statuses and all(status == "confirmed" for status in statuses):
        return "confirmed"
    if statuses and all(status == "absent" for status in statuses):
        return "absent"
    return "unknown"


def _prepare_share_import_evidence(
    client: Any,
    share_code: str,
    receive_code: str,
    target_cid: str,
    account_id: str,
) -> dict[str, Any]:
    source_items = _collect_share_snapshot_items(client, share_code, receive_code)
    target_items = _collect_target_items(client, target_cid)
    source_fingerprints = {_item_fingerprint(item) for item in source_items}
    source_fingerprints.discard("")
    if not source_fingerprints:
        raise TelegramResourceEffectPendingError("115 分享源清单为空，无法建立可靠转存对账")
    baseline_ids = {
        _item_id(item) for item in target_items if _item_fingerprint(item) in source_fingerprints and _item_id(item)
    }
    return {
        "source_fingerprints": sorted(source_fingerprints),
        "baseline_item_ids": sorted(baseline_ids),
        "account_id": str(account_id),
        "target_cid": str(target_cid),
    }


def _reconcile_share_import(client: Any, target_cid: str, evidence: Mapping[str, Any]) -> str:
    source_fingerprints = {
        str(value) for value in evidence.get("source_fingerprints", []) if isinstance(value, str) and value
    }
    baseline_ids = {str(value) for value in evidence.get("baseline_item_ids", []) if str(value or "").strip()}
    if not source_fingerprints or str(evidence.get("target_cid") or "") != str(target_cid):
        return "unknown"
    try:
        current_items = _collect_target_items(client, target_cid)
    except TelegramResourceEffectPendingError:
        return "unknown"
    for item in current_items:
        item_id = _item_id(item)
        if item_id and item_id not in baseline_ids and _item_fingerprint(item) in source_fingerprints:
            return "confirmed"
    # A list miss is only read-side non-observation. 115 may still be applying an
    # accepted share import, so it can never authorize an automatic replay.
    return "unknown"


def _collect_share_snapshot_items(client: Any, share_code: str, receive_code: str) -> list[Mapping[str, Any]]:
    collected = []
    limit = 500
    for offset in range(0, limit * 20, limit):
        try:
            response = client.share_info(
                share_code,
                receive_code=receive_code,
                cid=0,
                limit=limit,
                offset=offset,
            )
        except Exception:
            raise TelegramResourceEffectPendingError("115 分享源清单查询异常") from None
        items = _response_items(response)
        if items is None:
            raise TelegramResourceEffectPendingError("115 分享源清单查询失败")
        collected.extend(items)
        if len(items) < limit:
            return collected
    raise TelegramResourceEffectPendingError("115 分享源清单分页未完整")


def _collect_target_items(client: Any, target_cid: str) -> list[Mapping[str, Any]]:
    collected = []
    limit = 1000
    for offset in range(0, limit * 20, limit):
        try:
            response = client.fs_files({"cid": str(target_cid), "limit": limit, "offset": offset, "show_dir": 1})
        except Exception as exc:
            raise TelegramResourceEffectPendingError("115 目标目录清单查询异常") from exc
        items = _response_items(response)
        if items is None:
            raise TelegramResourceEffectPendingError("115 目标目录清单查询失败")
        collected.extend(items)
        if len(items) < limit:
            return collected
    raise TelegramResourceEffectPendingError("115 目标目录清单分页未完整")


def _response_items(response: Any) -> list[Mapping[str, Any]] | None:
    if not isinstance(response, Mapping) or response.get("state") is not True:
        return None
    data = response.get("data")
    candidates = [data, response]
    for candidate in candidates:
        if isinstance(candidate, list):
            return [item for item in candidate if isinstance(item, Mapping)]
        if isinstance(candidate, Mapping):
            for key in ("list", "items", "files", "data"):
                items = candidate.get(key)
                if isinstance(items, list):
                    return [item for item in items if isinstance(item, Mapping)]
    return []


def _item_fingerprint(item: Mapping[str, Any]) -> str:
    name = str(item.get("name") or item.get("file_name") or item.get("fn") or item.get("n") or "").strip()
    if not name:
        return ""
    value = {
        "name": name.casefold(),
        "size": str(item.get("size") or item.get("file_size") or item.get("fs") or item.get("s") or ""),
        "sha1": str(item.get("sha1") or item.get("sha") or item.get("file_sha1") or "").strip().upper(),
        "kind": _item_kind(item),
    }
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _item_id(item: Mapping[str, Any]) -> str:
    return str(item.get("fid") or item.get("file_id") or item.get("id") or item.get("cid") or "").strip()


def _item_kind(item: Mapping[str, Any]) -> str:
    for key in ("fc", "file_category", "type"):
        value = item.get(key)
        if value is not None:
            return str(value)
    if item.get("cid") is not None and not any(item.get(key) for key in ("fid", "file_id", "id")):
        return "0"
    return "1" if any(item.get(key) for key in ("fid", "file_id", "id")) else ""


def _p115_write_outcome(result: Any) -> str:
    if not isinstance(result, Mapping):
        return "unknown"
    state = result.get("state")
    if (
        state is True
        or (isinstance(state, (int, float)) and not isinstance(state, bool) and state == 1)
        or (isinstance(state, str) and state.strip().casefold() in {"1", "true"})
    ):
        return "accepted"
    text = str(
        result.get("error_msg") or result.get("error") or result.get("message") or result.get("msg") or ""
    ).casefold()
    deterministic_markers = (
        "参数错误",
        "格式错误",
        "链接无效",
        "分享不存在",
        "分享已失效",
        "提取码错误",
        "访问码错误",
        "invalid parameter",
        "invalid link",
        "not found",
        "unsupported",
    )
    uncertain_markers = (
        "频繁",
        "稍后",
        "处理中",
        "超时",
        "timeout",
        "rate",
        "busy",
        "pending",
        "unknown",
    )
    if (
        text
        and any(marker in text for marker in deterministic_markers)
        and not any(marker in text for marker in uncertain_markers)
    ):
        return "rejected"
    return "unknown"


def _safe_provider_evidence(result: Any) -> dict[str, Any]:
    if not isinstance(result, Mapping):
        return {}
    evidence: dict[str, Any] = {}
    for key in ("errno", "code", "errNo", "err_no"):
        value = result.get(key)
        if value not in (None, ""):
            evidence["provider_code"] = str(value)[:64]
            break
    data = result.get("data")
    if isinstance(data, Mapping):
        receive_title = str(data.get("receive_title") or "").strip()
        if receive_title:
            evidence["receive_title"] = receive_title[:500]
    return evidence


def _digest_identity(namespace: str, value: str) -> str:
    encoded = json.dumps(
        {"namespace": str(namespace), "value": str(value)},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _tmdb_api_key() -> str:
    import constants
    from config_manager import APP_CONFIG

    return str(APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY) or "").strip()


def _telegram_emby_username(chat_id: str) -> str:
    from database.connection import get_db_connection

    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT u.name
                    FROM emby_users u
                    JOIN emby_users_extended e ON u.id = e.emby_user_id
                    WHERE e.telegram_chat_id = %s
                    """,
                    (str(chat_id),),
                )
                row = cursor.fetchone()
        if isinstance(row, Mapping) and row.get("name"):
            return str(row["name"])
        if isinstance(row, (list, tuple)) and row and row[0]:
            return str(row[0])
    except Exception:
        logger.warning("Unable to resolve the Emby username for Telegram chat %s", chat_id, exc_info=True)
    return "TG 搜索"


def _raise_for_reported_failure(result: Any, *, fallback: str) -> None:
    if not result:
        raise RuntimeError(fallback)
    if isinstance(result, Mapping):
        flags = [result[key] for key in ("ok", "success", "state") if key in result]
        if flags and any(value is False for value in flags):
            raise RuntimeError(str(result.get("message") or result.get("error_msg") or fallback))


def _raise_for_p115_failure(result: Any, *, fallback: str) -> None:
    if not isinstance(result, Mapping) or not result.get("state"):
        message = result.get("error_msg") or result.get("message") if isinstance(result, Mapping) else None
        raise RuntimeError(str(message or fallback))


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


__all__ = [
    "HDHiveUnlockPendingError",
    "TelegramResourceEffectPendingError",
    "acquire_hdhive_unlock_receipt",
    "complete_hdhive_unlock_receipt",
    "execute_channel_p115_effect",
    "execute_hdhive_p115_effect",
    "task_execute_telegram_resource_action",
]
