from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, cast

from etk.domains.p115.auth_directory import P115CredentialStore
from etk.domains.p115.maintenance_transport import (
    P115PrimaryOperationsGateway,
    item_is_directory,
    item_name,
    provider_succeeded,
    response_id,
)
from etk.integrations import ProviderDisposition, SharedCenterPort
from etk.platform import Settings
from etk.platform.jobs.json_payload import canonical_json

from .execution import SharingDeliveryOutcome
from .operations import SharingOperation
from .repository import PostgresSharingOperationRepository
from .virtual_import import VirtualImportBundle, VirtualImportService, freeze_virtual_import_source

_SHA1 = re.compile(r"[A-F0-9]{40}")
_MAX_TREE_ITEMS = 10_000
_MAX_TREE_DIRECTORIES = 256


@dataclass(frozen=True, slots=True)
class ShareTarget:
    status: str
    folder_cid: str = ""


@dataclass(frozen=True, slots=True)
class ShareImportOutcome:
    status: str
    reason: str = ""


@dataclass(frozen=True, slots=True)
class ShareTransferReadback:
    status: str
    receipts: tuple[dict[str, Any], ...] = ()
    missing_count: int = 0
    ambiguous_count: int = 0


class P115LogicalSeasonShareGateway(Protocol):
    def find_target(self, identity: Mapping[str, Any]) -> ShareTarget: ...

    def create_target(self, identity: Mapping[str, Any]) -> ShareTarget: ...

    def import_share(
        self,
        identity: Mapping[str, Any],
        *,
        share_code: str,
        receive_code: str,
    ) -> ShareImportOutcome: ...

    def readback(self, identity: Mapping[str, Any]) -> ShareTransferReadback: ...


class NativeP115LogicalSeasonShareGateway:
    """Cookie-pinned share import with authoritative directory readback."""

    def __init__(
        self,
        *,
        credentials: P115CredentialStore,
        priority: Callable[[], str],
        target_cid: Callable[[], str],
        operations: P115PrimaryOperationsGateway | None = None,
    ) -> None:
        self._credentials = credentials
        self._target_cid = target_cid
        self._operations = operations or P115PrimaryOperationsGateway(
            credentials=credentials,
            priority=priority,
        )

    def find_target(self, identity: Mapping[str, Any]) -> ShareTarget:
        client = self._client(identity)
        matches = [
            item
            for item in client.list_children(_text(identity.get("base_target_cid"), "base target cid", 128))
            if item_is_directory(item) and item_name(item) == _text(identity.get("folder_name"), "folder name", 255)
        ]
        self._assert_identity(identity)
        if not matches:
            return ShareTarget("missing")
        if len(matches) != 1:
            return ShareTarget("ambiguous")
        folder_cid = response_id(matches[0])
        return ShareTarget("found", folder_cid) if folder_cid else ShareTarget("ambiguous")

    def create_target(self, identity: Mapping[str, Any]) -> ShareTarget:
        client = self._client(identity)
        response = client.create_directory(
            _text(identity.get("base_target_cid"), "base target cid", 128),
            _text(identity.get("folder_name"), "folder name", 255),
        )
        if not provider_succeeded(response):
            return ShareTarget("rejected")
        return self.find_target(identity)

    def import_share(
        self,
        identity: Mapping[str, Any],
        *,
        share_code: str,
        receive_code: str,
    ) -> ShareImportOutcome:
        client = self._client(identity)
        response = client.import_share(
            str(share_code or "").strip(),
            str(receive_code or "").strip(),
            _text(identity.get("target_folder_cid"), "target folder cid", 128),
        )
        self._assert_identity(identity)
        return _share_import_outcome(response)

    def readback(self, identity: Mapping[str, Any]) -> ShareTransferReadback:
        client = self._client(identity)
        root = _text(identity.get("target_folder_cid"), "target folder cid", 128)
        candidates = _walk_files(client.list_children, root)
        self._assert_identity(identity)

        receipts: list[dict[str, Any]] = []
        missing = 0
        ambiguous = 0
        for expected in _identity_files(identity):
            sha1 = _sha1(expected.get("sha1"))
            size = _positive_int(expected.get("size"), "file size")
            matches = [item for item in candidates if _item_sha1(item) == sha1 and _item_size(item) == size]
            if not matches:
                missing += 1
                continue
            if len(matches) != 1:
                ambiguous += 1
                continue
            item = matches[0]
            file_id = response_id(item)
            if not file_id:
                ambiguous += 1
                continue
            receipts.append(
                {
                    "fid": file_id,
                    "pick_code": _item_pick_code(item),
                    "sha1": sha1,
                    "size": size,
                    "file_name": item_name(item) or str(expected.get("file_name") or ""),
                    "target_cid": root,
                    "backend": "cookie",
                    "account_id": str(identity.get("account_id") or ""),
                    "evidence": "authoritative_share_target_listing",
                }
            )
        status = (
            "confirmed"
            if len(receipts) == len(_identity_files(identity))
            else ("ambiguous" if ambiguous else "partial")
        )
        return ShareTransferReadback(status, tuple(receipts), missing, ambiguous)

    def _client(self, identity: Mapping[str, Any]) -> Any:
        self._assert_identity(identity)
        client = self._operations.open(
            expected_account_id=_text(identity.get("account_id"), "account id", 128),
            expected_backend="cookie",
        )
        if client is None:
            raise RuntimeError("115 Cookie share client is unavailable")
        return client

    def _assert_identity(self, identity: Mapping[str, Any]) -> None:
        snapshot = self._credentials.load()
        if str(snapshot.account_id or "").strip() != _text(identity.get("account_id"), "account id", 128):
            raise RuntimeError("115 account changed during share transfer")
        if not str(snapshot.cookie or "").strip():
            raise RuntimeError("115 Cookie credential is unavailable")
        if _share_credential_fingerprint(snapshot) != _hash64(identity.get("credential_hash"), "credential hash"):
            raise RuntimeError("115 credential changed during share transfer")
        if _text(self._target_cid(), "115 save path cid", 128) != _text(
            identity.get("base_target_cid"), "base target cid", 128
        ):
            raise RuntimeError("115 target changed during share transfer")


class LogicalSeasonShareTransferOperationDelivery:
    """One-checkpoint logical-season share transfer with readback-only recovery."""

    def __init__(
        self,
        *,
        repository: PostgresSharingOperationRepository,
        virtual_imports: VirtualImportService,
        credentials: P115CredentialStore,
        settings: Callable[[], Settings],
        center: SharedCenterPort,
        p115: P115LogicalSeasonShareGateway,
    ) -> None:
        self._repository = repository
        self._virtual_imports = virtual_imports
        self._credentials = credentials
        self._settings = settings
        self._center = center
        self._p115 = p115

    @staticmethod
    def supports(operation: SharingOperation) -> bool:
        request = operation.request if isinstance(operation.request, Mapping) else {}
        return str(request.get("provider") or "") == "p115" and str(request.get("action") or "") == (
            "transfer_logical_season_share"
        )

    def supports_authoritative_readback(self, operation: SharingOperation) -> bool:
        return self.supports(operation)

    def request(
        self,
        operation: SharingOperation,
        *,
        checkpoint: Callable[[], None],
    ) -> SharingDeliveryOutcome:
        payload = _transfer_payload(operation)
        intent = freeze_virtual_import_source(cast(Mapping[str, Any], payload["source"]))
        if intent.source_kind != "logical_season":
            raise ValueError("share transfer requires a logical_season source")
        if intent.state_hash != _hash64(payload.get("source_state_hash"), "source state hash"):
            raise ValueError("logical season share source state changed")
        bundle = self._virtual_imports.prepare(intent.source)
        self._validate_manifest(bundle)
        channel = self._channel(bundle.source_id, str(payload.get("expected_channel_id") or ""))
        identity = self._prepare_identity(bundle, intent.state_hash, channel_id=channel["channel_id"])
        identity["operation_id"] = str(operation.id)
        self._save_prewrite_state(operation, identity)
        checkpointed = False

        def begin_provider_writes() -> None:
            nonlocal checkpointed
            checkpoint()
            checkpointed = True

        target = self._p115.find_target(identity)
        if target.status == "ambiguous":
            return SharingDeliveryOutcome.rejected({"type": "share_target_folder_ambiguous"})
        if target.status == "found":
            identity.update(target_folder_cid=target.folder_cid, phase="target_prepared")
            self._save_prewrite_state(operation, identity)
            existing = self._p115.readback(identity)
            if existing.status == "confirmed":
                begin_provider_writes()
                identity["phase"] = "confirmed_existing"
                self._save_request_state(operation, identity)
                return SharingDeliveryOutcome.confirmed(_share_transfer_result(identity, existing, existing=True))
            if existing.status == "ambiguous" or existing.receipts:
                return SharingDeliveryOutcome.rejected(
                    {
                        "type": (
                            "share_target_files_ambiguous"
                            if existing.status == "ambiguous"
                            else "share_target_files_partial"
                        )
                    }
                )
        else:
            begin_provider_writes()
            identity["phase"] = "target_create_pending"
            self._save_request_state(operation, identity)
            target = self._p115.create_target(identity)
            if target.status != "found" or not target.folder_cid:
                return SharingDeliveryOutcome.indeterminate({"type": "share_target_folder_not_confirmed"})
            identity.update(target_folder_cid=target.folder_cid, phase="target_prepared")
            self._save_request_state(operation, identity)

        if not checkpointed:
            begin_provider_writes()

        identity["phase"] = "import_requesting"
        self._save_request_state(operation, identity)
        imported = self._p115.import_share(
            identity,
            share_code=channel["share_code"],
            receive_code=channel["receive_code"],
        )
        if imported.status == "rejected":
            if imported.reason in {"source_dead", "pending_review"}:
                identity.update(
                    phase="import_rejected",
                    channel_failure_status="expired" if imported.reason == "source_dead" else "pending_review",
                )
                self._save_request_state(operation, identity)
            return SharingDeliveryOutcome.rejected(
                {"type": "share_import_rejected", "reason": imported.reason or "provider_rejected"}
            )
        identity["phase"] = "awaiting_readback"
        self._save_request_state(operation, identity)
        readback = self._p115.readback(identity)
        if readback.status != "confirmed":
            return SharingDeliveryOutcome.indeterminate(
                {
                    "type": "share_transfer_readback_incomplete",
                    "missing_count": readback.missing_count,
                    "ambiguous_count": readback.ambiguous_count,
                }
            )
        identity["phase"] = "target_confirmed"
        self._save_request_state(operation, identity)
        return SharingDeliveryOutcome.confirmed(_share_transfer_result(identity, readback, existing=False))

    def readback(self, operation: SharingOperation) -> SharingDeliveryOutcome:
        identity = dict(operation.state) if isinstance(operation.state, Mapping) else {}
        if str(identity.get("kind") or "") != "logical_season_share_transfer":
            return SharingDeliveryOutcome.indeterminate({"type": "share_transfer_readback_identity_missing"})
        if not str(identity.get("target_folder_cid") or "").strip():
            target = self._p115.find_target(identity)
            if target.status != "found" or not target.folder_cid:
                return SharingDeliveryOutcome.retryable({"type": "share_target_folder_not_observed"})
            identity.update(target_folder_cid=target.folder_cid, phase="awaiting_readback")
        readback = self._p115.readback(identity)
        if readback.status == "confirmed":
            identity["phase"] = "target_confirmed"
            return SharingDeliveryOutcome.confirmed(_share_transfer_result(identity, readback, existing=False))
        return SharingDeliveryOutcome.retryable(
            {
                "type": "share_transfer_readback_incomplete",
                "missing_count": readback.missing_count,
                "ambiguous_count": readback.ambiguous_count,
            }
        )

    def _validate_manifest(self, bundle: VirtualImportBundle) -> None:
        response = self._center.logical_season_manifest(bundle.source_id)
        if response.disposition is not ProviderDisposition.CONFIRMED or not isinstance(response.payload, Mapping):
            raise RuntimeError("Shared Center logical season manifest is unavailable")
        remote = _manifest_files(response.payload)
        if not remote:
            raise ValueError("Shared Center logical season manifest is empty or invalid")
        expected = {(_sha1(item.get("sha1")), _positive_int(item.get("size"), "file size")) for item in bundle.files}
        if remote != expected:
            raise ValueError("Shared Center logical season manifest changed")

    def _channel(self, source_id: str, expected_channel_id: str) -> dict[str, str]:
        response = self._center.get_logical_season_share_channel(source_id)
        if response.disposition is not ProviderDisposition.CONFIRMED or not isinstance(response.payload, Mapping):
            raise RuntimeError("Shared Center logical season share channel is unavailable")
        channel = _provider_item(response.payload)
        status = str(channel.get("status") or "").strip().casefold()
        channel_id = str(channel.get("channel_id") or "").strip()
        share_code = str(channel.get("share_code") or "").strip()
        receive_code = str(channel.get("receive_code") or "").strip()
        if status != "valid" or not channel_id or not share_code:
            raise ValueError("Shared Center logical season share channel is not valid")
        if expected_channel_id and expected_channel_id != channel_id:
            raise ValueError("Shared Center logical season share channel changed")
        return {
            "channel_id": channel_id,
            "share_code": share_code,
            "receive_code": receive_code,
        }

    def _prepare_identity(
        self,
        bundle: VirtualImportBundle,
        source_state_hash: str,
        *,
        channel_id: str,
    ) -> dict[str, Any]:
        snapshot = self._credentials.load()
        account_id = _text(snapshot.account_id, "115 account id", 128)
        if not str(snapshot.cookie or "").strip():
            raise RuntimeError("115 Cookie credential is required for share transfer")
        settings = self._settings()
        base_target_cid = _text(settings.p115_save_path_cid, "115 save path cid", 128)
        files = [_transfer_file(item) for item in bundle.files]
        manifest_hash = hashlib.sha256(canonical_json({"files": files}, field="share manifest").encode()).hexdigest()
        return {
            "schema_version": 1,
            "kind": "logical_season_share_transfer",
            "account_id": account_id,
            "backend": "cookie",
            "credential_hash": _share_credential_fingerprint(snapshot),
            "base_target_cid": base_target_cid,
            "target_folder_cid": "",
            "folder_name": _season_folder_name(bundle),
            "source_kind": bundle.source_kind,
            "source_id": bundle.source_id,
            "source_state_hash": source_state_hash,
            "channel_id": channel_id,
            "files": files,
            "files_manifest_hash": manifest_hash,
            "phase": "prepared",
        }

    def _save_prewrite_state(self, operation: SharingOperation, identity: Mapping[str, Any]) -> None:
        if operation.claim_token is None:
            raise RuntimeError("sharing transfer operation is not claimed")
        self._repository.save_prewrite_state(operation.id, operation.claim_token, identity)

    def _save_request_state(self, operation: SharingOperation, identity: Mapping[str, Any]) -> None:
        if operation.claim_token is None:
            raise RuntimeError("sharing transfer operation is not claimed")
        self._repository.save_request_state(operation.id, operation.claim_token, identity)


def _transfer_payload(operation: SharingOperation) -> dict[str, Any]:
    request = operation.request if isinstance(operation.request, Mapping) else {}
    payload = request.get("payload")
    if not isinstance(payload, Mapping) or not isinstance(payload.get("source"), Mapping):
        raise ValueError("logical season share transfer payload is invalid")
    return dict(payload)


def _transfer_file(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "sha1": _sha1(value.get("sha1")),
        "size": _positive_int(value.get("size"), "file size"),
        "file_name": _text(value.get("file_name") or value.get("name"), "file name", 512),
        "season_number": _optional_int(value.get("season_number")),
        "episode_number": _optional_int(value.get("episode_number")),
    }


def _identity_files(value: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    files = value.get("files")
    if not isinstance(files, list) or not files or any(not isinstance(item, Mapping) for item in files):
        raise ValueError("logical season share file manifest is invalid")
    return tuple(_transfer_file(cast(Mapping[str, Any], item)) for item in files)


def _season_folder_name(bundle: VirtualImportBundle) -> str:
    first = bundle.files[0]
    title = _safe_component(bundle.source.get("title") or first.get("title") or bundle.source_id)
    year = _optional_int(bundle.source.get("release_year") or bundle.source.get("year") or first.get("release_year"))
    tmdb_id = str(
        bundle.source.get("parent_series_tmdb_id")
        or bundle.source.get("series_tmdb_id")
        or bundle.source.get("tmdb_id")
        or first.get("parent_series_tmdb_id")
        or first.get("tmdb_id")
        or ""
    ).strip()
    season = max(1, _optional_int(bundle.source.get("season_number") or first.get("season_number")) or 1)
    root = title
    if year:
        root += f" ({year})"
    if tmdb_id:
        root += f" {{tmdb={_safe_component(tmdb_id)}}}"
    return f"{root} - Season {season:02d}"[:255]


def _share_transfer_result(
    identity: Mapping[str, Any],
    readback: ShareTransferReadback,
    *,
    existing: bool,
) -> dict[str, Any]:
    return {
        "authoritative_readback": True,
        "transfer_confirmed": True,
        "existing": existing,
        "transfer_mode": "share",
        "share_channel_id": str(identity.get("channel_id") or ""),
        "source_kind": str(identity.get("source_kind") or ""),
        "source_id": str(identity.get("source_id") or ""),
        "account_id": str(identity.get("account_id") or ""),
        "backend": "cookie",
        "target_cid": str(identity.get("target_folder_cid") or ""),
        "base_target_cid": str(identity.get("base_target_cid") or ""),
        "standard_folder_name": str(identity.get("folder_name") or ""),
        "files_manifest_hash": str(identity.get("files_manifest_hash") or ""),
        "receipts": list(readback.receipts),
        "success_count": len(readback.receipts),
        "total_count": len(_identity_files(identity)),
    }


def _manifest_files(payload: Mapping[str, Any]) -> set[tuple[str, int]]:
    candidates: list[Mapping[str, Any]] = []
    queue: list[Any] = [payload]
    seen = 0
    while queue and seen < 10_000:
        value = queue.pop()
        seen += 1
        if isinstance(value, Mapping):
            if _item_sha1(value) and _item_size(value) > 0:
                candidates.append(value)
            for key in ("item", "data", "manifest", "files", "items", "children", "best_asset_map", "assets"):
                nested = value.get(key)
                if isinstance(nested, Mapping):
                    queue.extend(nested.values() if key in {"best_asset_map", "assets"} else [nested])
                elif isinstance(nested, list):
                    queue.extend(nested)
    return {(_item_sha1(item), _item_size(item)) for item in candidates if _item_sha1(item) and _item_size(item)}


def _provider_item(payload: Mapping[str, Any]) -> dict[str, Any]:
    for value in (
        payload.get("item"),
        payload.get("channel"),
        payload.get("data"),
        payload,
    ):
        if isinstance(value, Mapping):
            nested = value.get("item")
            return dict(nested) if isinstance(nested, Mapping) else dict(value)
    return {}


def _walk_files(
    list_children: Callable[[str], Sequence[Mapping[str, Any]]],
    root: str,
) -> tuple[dict[str, Any], ...]:
    directories = [root]
    files: list[dict[str, Any]] = []
    visited: set[str] = set()
    while directories:
        if len(visited) >= _MAX_TREE_DIRECTORIES or len(files) >= _MAX_TREE_ITEMS:
            raise RuntimeError("115 share target tree exceeded the readback bound")
        current = directories.pop()
        if current in visited:
            continue
        visited.add(current)
        for raw in list_children(current):
            item = dict(raw)
            item_id = response_id(item)
            if item_is_directory(item):
                if item_id:
                    directories.append(item_id)
            else:
                files.append(item)
    return tuple(files)


def _share_import_outcome(value: Mapping[str, Any]) -> ShareImportOutcome:
    text = json.dumps(dict(value), ensure_ascii=False, default=str).casefold()
    code = str(value.get("errno") or value.get("code") or value.get("errNo") or value.get("err_no") or "")
    already = code == "4100024" or any(
        token in text
        for token in ("4100024", "已经转存过", "已接收过", "重复接收", "already received", "already saved")
    )
    local_issue = (
        already
        or code == "4200041"
        or any(
            token in text
            for token in ("空间不足", "超过限制", "频繁", "限制接收", "限制转存", "quota", "too many", "rate limit")
        )
    )
    source_dead = not local_issue and (
        code == "4100005"
        or any(
            token in text
            for token in (
                "分享已取消",
                "分享已失效",
                "分享不存在",
                "提取码错误",
                "访问码错误",
                "源文件不存在",
                "share not found",
                "expired",
                "cancelled",
                "canceled",
            )
        )
    )
    pending = any(token in text for token in ("审核", "处理中", "pending", "review", "processing"))
    if provider_succeeded(value) and not already:
        return ShareImportOutcome("confirmed")
    if source_dead:
        return ShareImportOutcome("rejected", "source_dead")
    if pending:
        return ShareImportOutcome("rejected", "pending_review")
    if local_issue:
        return ShareImportOutcome("rejected", "local_account_issue")
    return ShareImportOutcome("indeterminate", "unknown")


def _share_credential_fingerprint(snapshot: Any) -> str:
    return hashlib.sha256(f"cookie\0{snapshot.app_type}\0{snapshot.cookie}".encode()).hexdigest()


def _item_sha1(value: Mapping[str, Any]) -> str:
    normalized = str(value.get("sha1") or value.get("file_sha1") or value.get("sha") or "").strip().upper()
    return normalized if _SHA1.fullmatch(normalized) else ""


def _item_size(value: Mapping[str, Any]) -> int:
    try:
        raw = value.get("size") or value.get("file_size") or value.get("s")
        return max(0, int(raw or 0))
    except (TypeError, ValueError):
        return 0


def _item_pick_code(value: Mapping[str, Any]) -> str:
    return str(value.get("pick_code") or value.get("pickcode") or value.get("pc") or "").strip()


def _sha1(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if not _SHA1.fullmatch(normalized):
        raise ValueError("logical season share sha1 is invalid")
    return normalized


def _hash64(value: Any, field: str) -> str:
    normalized = str(value or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", normalized):
        raise ValueError(f"{field} is invalid")
    return normalized


def _text(value: Any, field: str, limit: int) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > limit:
        raise ValueError(f"{field} is invalid")
    return normalized


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} is invalid")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} is invalid") from exc
    if parsed < 1:
        raise ValueError(f"{field} is invalid")
    return parsed


def _optional_int(value: Any) -> int | None:
    try:
        if value in (None, "") or isinstance(value, bool):
            return None
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _safe_component(value: Any) -> str:
    normalized = re.sub(r"[\\/:*?\"<>|\r\n\t]+", " ", str(value or "").strip())
    normalized = re.sub(r"\s+", " ", normalized).strip(" ._-")
    return (normalized or "未命名")[:180]


__all__ = [
    "LogicalSeasonShareTransferOperationDelivery",
    "NativeP115LogicalSeasonShareGateway",
    "P115LogicalSeasonShareGateway",
    "ShareImportOutcome",
    "ShareTarget",
    "ShareTransferReadback",
]
